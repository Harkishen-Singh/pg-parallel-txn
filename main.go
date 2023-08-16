package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/promscale/pkg/log"
)

const WAL_SCAN_INTERVAL = 10 * time.Second

func main() {
	uri := flag.String("uri", "", "Database URI to write data.")
	walPath := flag.String("wal_dir", "work_dir", "Path of dir where WAL segments.")
	level := flag.String("level", "info", "Log level to use from [ 'error', 'warn', 'info', 'debug' ].")
	numWorkers := flag.Int("num_workers", 20, "Number of parallel workers.")
	maxConn := flag.Int("max_conn", 20, "Maximum number of connections in the pool.")
	flag.Parse()

	logCfg := log.Config{
		Format: "logfmt",
		Level:  *level,
	}
	if err := log.Init(logCfg); err != nil {
		panic(err)
	}

	if *uri == "" {
		log.Fatal("msg", "please provide a database URI using the -uri flag.")
	}

	// state, err := LoadOrCreateState()
	// if err != nil {
	// 	log.Fatal("msg", "error loading state file", "error", err.Error())
	// }

	pool := getPgxPool(uri, 1, int32(*maxConn))
	defer pool.Close()
	testConn(pool)

	absWalDir, err := filepath.Abs(*walPath)
	if err != nil {
		panic(err)
	}

	files, err := os.ReadDir(absWalDir)
	if err != nil {
		log.Fatal("msg", "error reading WAL path", "error", err.Error())
	}

	sqlFiles := []string{}
	for _, file := range files {
		if file.Type().IsRegular() && strings.HasSuffix(file.Name(), ".sql") {
			sqlFiles = append(sqlFiles, filepath.Join(absWalDir, file.Name()))
		}
	}

	pendingSQLFiles, err := sortFilesByCreationTime(sqlFiles)
	if err != nil {
		panic(err)
	}

	var skipTxns atomic.Bool
	skipTxns.Store(false)
	activeParallelIngest := new(sync.WaitGroup)
	parallelTxnChannel := make(chan *txn, *numWorkers)
	for i := 0; i < *numWorkers; i++ {
		w := NewWorker(i, pool, parallelTxnChannel, activeParallelIngest)
		go w.Run()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT)
	go func() {
		<-sigChan
		log.Info("msg", "waiting for parallel workers to complete before shutdown")
		skipTxns.Store(true)
		activeParallelIngest.Wait()
		close(parallelTxnChannel)
		log.Info("msg", "Shutting down")
		os.Exit(0)
	}()

	stmts := []string{}
	isTxnOpen := false // Helps to capture commits that are spread over multiple files.
	beginMetadata := BeginMetadata{}
	commitMetadata := CommitMetadata{}
	for _, filePath := range pendingSQLFiles {
		totalTxns := getTotalTxns(filePath)
		txnCount := 0

		replayFile := func(filePath string) {
			log.Info("msg", fmt.Sprintf("replaying txns from: %s", filePath))

			file, err := os.Open(filePath)
			if err != nil {
				panic(err)
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			scanner.Split(bufio.ScanLines)
		stop_scanning:
			for scanner.Scan() {
				line := scanner.Text()
				switch {
				case line[:6] == "BEGIN;":
					stmts = []string{}
					isTxnOpen = true
					beginMetadata = GetBeginMetadata(line)
				case line[:7] == "COMMIT;":
					if skipTxns.Load() {
						log.Debug("msg", "skipping txns")
						break stop_scanning
					}
					isTxnOpen = false

					commitMetadata = GetCommitMetadata(line)
					if beginMetadata.XID != commitMetadata.XID {
						panic(fmt.Sprintf(
							"falty txn. Begin.commitLSN: %s does not match Commit.LSN: %s",
							beginMetadata.CommitLSN,
							commitMetadata.LSN,
						))
					}

					txnCount++
					if len(stmts) > 0 {
						performTxn(
							pool,
							commitMetadata.XID,
							stmts,
							parallelTxnChannel,
							activeParallelIngest,
							txnCount,
							totalTxns,
						)
					}

				// Ignore statements.
				case line[:12] == "-- KEEPALIVE":
					continue
				case line[:13] == "-- SWITCH WAL":
					continue

				default:
					stmts = append(stmts, line)
				}
			}
			if err := scanner.Err(); err != nil {
				log.Fatal("msg", "error scanning file", "error", err.Error(), "file", filePath)
			}
		}
		start := time.Now()
		replayFile(filePath)
		// Let's wait for previous batch to complete before moving to the next batch.
		log.Info("msg", "waiting for scheduled txns to complete", "total_txn_count", txnCount)
		if isTxnOpen {
			log.Info("msg",
				fmt.Sprintf("found a txn (xid:%d) that stretches beyond current file. Holding its contents till the previous batch completes", commitMetadata.XID))
		}
		activeParallelIngest.Wait()
		log.Info("msg", "completed replaying file", "time-taken", time.Since(start), "file", filePath)
	}
}

func performTxn(
	pool *pgxpool.Pool,
	xid int,
	stmts []string,
	parallelTxnChannel chan<- *txn,
	activeParallelIngest *sync.WaitGroup,
	txnCount int,
	totalTxns int,
) {
	t := &txn{
		stmts: stmts,
	}
	if isInsertOnly(stmts) {
		parallelTxnChannel <- t
		log.Debug(
			"msg", "execute parallel txn",
			"xid", xid,
			"num_stmts", len(stmts),
			"progress", fmt.Sprintf("%d/%d", txnCount, totalTxns))
	} else {
		// Wait for scheduled parallel txns to complete.
		log.Debug("msg", fmt.Sprintf(
			"received a serial txn type (xid:%d); waiting for scheduled parallel txns to complete", xid,
		))
		activeParallelIngest.Wait()
		log.Debug(
			"msg", "execute serial txn",
			"xid", xid,
			"num_stmts", len(stmts),
			"progress", fmt.Sprintf("%d/%d", txnCount, totalTxns))
		// Now all parallel txns have completed. Let's do the serial txn.
		if err := doSerialInsert(pool, t); err != nil {
			log.Fatal("msg", "error executing a serial txn", "xid", xid, "err", err.Error())
		}
	}
}

func isInsertOnly(stmts []string) bool {
	for _, s := range stmts {
		if s[:11] != "INSERT INTO" {
			return false
		}
	}
	return true
}

func getTotalTxns(file string) int {
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	totalTxns := 0
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		line := scanner.Text()
		if line[:7] == "COMMIT;" {
			totalTxns++
		}
	}
	return totalTxns
}

func doSerialInsert(conn *pgxpool.Pool, t *txn) error {
	txn, err := conn.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("error starting a txn: %w", err)
	}
	defer txn.Rollback(context.Background())

	batch := &pgx.Batch{}
	for _, stmt := range t.stmts {
		batch.Queue(stmt)
	}

	r := txn.SendBatch(context.Background(), batch)
	_, err = r.Exec()
	if err != nil {
		return fmt.Errorf("error executing batch results: %w", err)
	}
	err = r.Close()
	if err != nil {
		return fmt.Errorf("error closing rows from batch: %w", err)
	}

	if err := txn.Commit(context.Background()); err != nil {
		return fmt.Errorf("error commiting a txn: %w", err)
	}

	return nil
}

func getPgxPool(uri *string, min, max int32) *pgxpool.Pool {
	cfg, err := pgxpool.ParseConfig(*uri)
	if err != nil {
		log.Fatal("Error parsing config", err.Error())
	}
	cfg.MinConns = min
	cfg.MaxConns = max
	dbpool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		log.Fatal("Unable to connect to database", err.Error())
	}
	return dbpool
}

func testConn(conn *pgxpool.Pool) bool {
	var t int
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&t); err != nil {
		panic(err)
	}
	log.Info("msg", "connected to the database")
	return true
}
