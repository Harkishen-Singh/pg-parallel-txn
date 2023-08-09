package main

import (
	"bufio"
	"context"
	"flag"
	"os"
	"strings"
	"sync"
	"time"

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

	if *uri == "" {
		log.Fatal("msg", "please provide a database URI using the -uri flag.")
	}

	logCfg := log.Config{
		Format: "logfmt",
		Level:  *level,
	}
	if err := log.Init(logCfg); err != nil {
		panic(err)
	}

	pool := getPgxPool(uri, 1, int32(*maxConn))
	defer pool.Close()
	testConn(pool)

	activeParallelIngest := new(sync.WaitGroup)
	incomingTxn := make(chan *txnStatements, *numWorkers)
	workers := []*worker{}
	for i := 0; i < *numWorkers; i++ {
		w := NewWorker(i, pool, incomingTxn, activeParallelIngest)
		workers = append(workers, w)
	}

	files, err := os.ReadDir(*walPath)
	if err != nil {
		log.Fatal("msg", "error reading WAL path", "error", err.Error())
	}

	sqlFiles := []string{}
	for _, file := range files {
		if file.Type().IsRegular() && strings.HasPrefix(file.Name(), ".sql") {
			sqlFiles = append(sqlFiles, file.Name())
		}
	}

	
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

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
