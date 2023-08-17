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
	sourceUri := flag.String("source_uri", "", "Source database URI to update LSN pointer.")
	targetUri := flag.String("target_uri", "", "Target database URI to write data.")
	walPath := flag.String("wal_dir", "work_dir", "Path of dir where WAL segments.")
	level := flag.String("level", "info", "Log level to use from [ 'error', 'warn', 'info', 'debug' ].")
	numWorkers := flag.Int("num_workers", 20, "Number of parallel workers.")
	maxConn := flag.Int("max_conn", 20, "Maximum number of connections in the pool.")
	proceedLSNAfterXids := flag.Int64("proceed_lsn_after", PROCEED_AFTER_BATCH, "Proceed LSN marker in Source DB after '-proceed_lsn_after' txns. "+
		"A higher number causes less interruption in parallelism, but risks more duplicate data in case of a crash. "+
		"If 0, LSN pointer proceeds after a batch completes. The size of a typical batch is the number of txns in a WAL file.")
	noProceed := flag.Bool("no_lsn_proceed", false, "Development only. Do not proceed LSN. Source db uri is not needed.")
	sortingMethod := flag.String("file_sorting_method", "change_time", "Method to use for sorting WAL files to apply in order. "+
		"Valid: [change_time, hexadecimal]")
	flag.Parse()

	logCfg := log.Config{
		Format: "logfmt",
		Level:  *level,
	}
	if err := log.Init(logCfg); err != nil {
		panic(err)
	}

	if *targetUri == "" {
		log.Fatal("msg", "please provide database URIs for '-target_uri' flags")
	}

	// state, err := LoadOrCreateState()
	// if err != nil {
	// 	log.Fatal("msg", "error loading state file", "error", err.Error())
	// }

	pool := getPgxPool(targetUri, 1, int32(*maxConn))
	defer pool.Close()
	testConn(pool)
	log.Info("msg", "connected to target database")

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
	if len(sqlFiles) == 0 {
		log.Info("msg", "No files to replay")
	}

	var pendingSQLFiles []string
	switch *sortingMethod {
	case "change_time":
		pendingSQLFiles, err = sortFilesByChangeTime(sqlFiles)
	case "hexadecimal":
		pendingSQLFiles, err = sortFilesByName(sqlFiles)
	}
	if err != nil {
		panic(err)
	}
	log.Info("msg", fmt.Sprintf("Found %d files to be replayed", len(pendingSQLFiles)))

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
		log.Info("msg", "Waiting for parallel workers to complete before shutdown")
		skipTxns.Store(true)
		activeParallelIngest.Wait()
		close(parallelTxnChannel)
		log.Info("msg", "Shutting down")
		os.Exit(0)
	}()

	// Setup LSN proceeder.
	lsnp := NewNoopProceeder()
	if !*noProceed {
		sourceConn, err := pgx.Connect(context.Background(), *sourceUri)
		if err != nil {
			log.Fatal("msg", "unable to connect to source database", "err", err.Error())
		}
		defer sourceConn.Close(context.Background())
		testConn(sourceConn)
		log.Info("msg", "connected to source database")
		lsnp = NewLSNProceeder(sourceConn, *proceedLSNAfterXids, activeParallelIngest)
	}

	stmts := []string{}
	isTxnOpen := false // Helps to capture commits that are spread over multiple files.
	beginMetadata := BeginMetadata{}
	commitMetadata := CommitMetadata{}
	for _, filePath := range pendingSQLFiles {
		totalTxns := getTotalTxns(filePath)
		txnCount := int64(0)

		replayFile := func(filePath string) {
			log.Info("Replaying", getFileName(filePath), "total_txns", totalTxns)

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
						// This serves as an important check for txns that are spread over multiple files.
						// Though we read the WAL files in order in which they are created, we need a
						// reliable way to check if the txn we constructed has correct contents or not.
						//
						// When commits are spread over files, the Begin_txn_id is in a different file than
						// Commit_txn_id. Hence, the xid of Begin & Commit txn_id must be same.
						panic(fmt.Sprintf(
							"FATAL: Falty txn constructed. Begin.XID (%d) does not match Commit.XID (%d). File: %s",
							beginMetadata.XID,
							commitMetadata.XID,
							getFileName(filePath),
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
					lsnp.IncrementTxn(commitMetadata.LSN)
				case line[:3] == "-- ":
					// Ignore all comments.
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
		if isTxnOpen {
			log.Debug("msg",
				fmt.Sprintf("found a txn (xid:%d) that stretches beyond current file. Holding its contents till the previous batch completes", commitMetadata.XID))
		}
		log.Info("msg", "Waiting for batch to complete")
		activeParallelIngest.Wait()
		if *proceedLSNAfterXids == 0 {
			// Proceed LSN after batch completes is activated.
			lsnp.Proceed()
		}
		log.Info("Done", time.Since(start).String())
	}
	log.Info("msg", "Shutting down")
}

func performTxn(
	pool *pgxpool.Pool,
	xid int64,
	stmts []string,
	parallelTxnChannel chan<- *txn,
	activeParallelIngest *sync.WaitGroup,
	txnCount int64,
	totalTxns int64,
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

func getTotalTxns(file string) int64 {
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	totalTxns := int64(0)
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

func testConn(conn interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}) bool {
	var t int
	if err := conn.QueryRow(context.Background(), "SELECT 1").Scan(&t); err != nil {
		panic(err)
	}
	return true
}

func getFileName(path string) string {
	return filepath.Base(path)
}
