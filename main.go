package main

import (
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

	commitqueue "github.com/Harkishen-Singh/pg-parallel-txn/commit_queue"
	"github.com/Harkishen-Singh/pg-parallel-txn/common"
	"github.com/Harkishen-Singh/pg-parallel-txn/format"
	"github.com/Harkishen-Singh/pg-parallel-txn/progress"
	"github.com/Harkishen-Singh/pg-parallel-txn/sort"
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
	sortingMethod := flag.String("file_sorting_method", "hexadecimal", "Method to use for sorting WAL files to apply in order. "+
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
		log.Fatal("msg", "Please provide database URIs for '-target_uri' flags")
	}

	pool := getPgxPool(targetUri, 1, int32(*maxConn))
	defer pool.Close()
	testConn(pool)
	log.Info("msg", "Connected to target database")

	progressTracker, err := progress.NewTracker(pool)
	if err != nil {
		log.Fatal("msg", "Error creating progress tracker in target db", "err", err.Error())
	}
	performCatchup := false
	if progressTracker.LSN != "" {
		performCatchup = true
	}

	skipTxns := new(atomic.Bool)
	skipTxns.Store(false)
	activeIngests := new(sync.WaitGroup)
	parallelTxnChannel := make(chan *txn, *numWorkers)
	commitQ := commitqueue.New(100_000)
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()
	for i := 0; i < *numWorkers; i++ {
		w := &Worker{
			ctx:      rootCtx,
			id:       i,
			conn:     pool,
			incoming: parallelTxnChannel,
			active:   activeIngests,
			commitQ:  commitQ,
		}
		go w.Run()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT)
	go func() {
		<-sigChan
		log.Info("msg", "Waiting for parallel workers to complete before shutdown")
		skipTxns.Store(true)
		activeIngests.Wait()
		close(parallelTxnChannel)
		log.Info("msg", "Shutting down")
		os.Exit(0)
	}()

	// Setup LSN proceeder.
	lsnp := NewNoopProceeder()
	if !*noProceed {
		sourceConn, err := pgx.Connect(context.Background(), *sourceUri)
		if err != nil {
			log.Fatal("msg", "Unable to connect to source database", "err", err.Error())
		}
		defer sourceConn.Close(context.Background())
		testConn(sourceConn)
		log.Info("msg", "Connected to source database")
		lsnp = NewLSNProceeder(sourceConn, *proceedLSNAfterXids, activeIngests)
		format.CompleteMapping(sourceConn)
	}

	absWalDir, err := filepath.Abs(*walPath)
	if err != nil {
		panic(err)
	}

	replayer := &Replayer{
		pool:                 pool,
		lsnp:                 lsnp,
		skipTxns:             skipTxns,
		parallelTxn:          parallelTxnChannel,
		activeIngests:        activeIngests,
		commitQ:              commitQ,
		proceedLSNAfterBatch: *proceedLSNAfterXids == 0,
		migrationProgress:    migrationProgress,
		performCatchup:       performCatchup,
	}

	for {
		walFiles := getWALFiles(absWalDir)
		switch *sortingMethod {
		case "change_time":
			walFiles, err = sort.SortFilesByChangeTime(walFiles)
		case "hexadecimal":
			walFiles = sort.SortFilesByName(walFiles)
		}
		if err != nil {
			panic(err)
		}
		pendingSQLFiles := []string{}
		if replayer.performCatchup {
			pendingSQLFiles = trimFiles(walFiles, "TODO", 2)
		} else {
			pendingSQLFiles = trimFiles(walFiles, "TODO", 0)
		}
		if len(pendingSQLFiles) > 0 {
			log.Info("msg", fmt.Sprintf("Found %d files to be replayed", len(pendingSQLFiles)))
			replayer.Replay(pendingSQLFiles)
		} else {
			log.Info("msg", "No files to replay")
			// Wait for scan interval before next scan.
			<-time.After(WAL_SCAN_INTERVAL)
		}
	}
}

func getWALFiles(walDir string) []string {
	log.Debug("msg", "Scanning for WAL files")
	files, err := os.ReadDir(walDir)
	if err != nil {
		log.Fatal("msg", "Error reading WAL path", "error", err.Error())
	}

	sqlFiles := []string{}
	for _, file := range files {
		if file.Type().IsRegular() && strings.HasSuffix(file.Name(), ".sql") {
			sqlFiles = append(sqlFiles, filepath.Join(walDir, file.Name()))
		}
	}
	return sqlFiles
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

// trimFiles finds the location of 'upto', subtracts the 'buffer' from the location
// and removes all files before it.
// Note: files must be sorted in their expected order, otherwise trim would remove
// the wrong files.
func trimFiles(files []string, upto string, buffer int) []string {
	pos := 0
	for i, f := range files {
		if common.FileNameWithoutExtension(f) == common.FileNameWithoutExtension(upto) {
			pos = i+1 // Exclude the current file as well.
			break
		}
	}
	pos -= buffer
	if pos < 0 {
		pos = 0
	}
	files = files[pos:]
	return files
}
