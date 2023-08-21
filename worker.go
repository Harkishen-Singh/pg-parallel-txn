package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/promscale/pkg/log"
)

// txn contains all the statements that are part of
// a transaction in the order in which they are received.
// These statements do not include BEGIN; & COMMIT;, rather
// the ones between them.
type txn struct {
	stmts []string
}

type worker struct {
	id                   int
	conn                 *pgxpool.Pool
	incomingTxn          <-chan *txn
	activeParallelIngest *sync.WaitGroup
}

func NewWorker(id int, pool *pgxpool.Pool, incomingTxn <-chan *txn, activeParallelIngest *sync.WaitGroup) *worker {
	return &worker{
		id:                   id,
		conn:                 pool,
		incomingTxn:          incomingTxn,
		activeParallelIngest: activeParallelIngest,
	}
}

func (w *worker) Run() {
	log.Info("msg", fmt.Sprintf("starting worker %d", w.id))
	for {
		stmts, ok := <-w.incomingTxn
		if !ok {
			log.Info("msg", fmt.Sprintf("shutting down worker %d", w.id))
			return
		}
		w.activeParallelIngest.Add(1)
		if err := doBatch(w.conn, stmts); err != nil {
			log.Fatal("msg", "error doBatch", "err", err.Error())
		}
		w.activeParallelIngest.Done()
	}
}

var not_allowed_schemas = []string{"_timescaledb_catalog"}

func doBatch(conn *pgxpool.Pool, stmts *txn) error {
	txn, err := conn.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer txn.Rollback(context.Background())

	batch := &pgx.Batch{}
	for _, stmt := range stmts.stmts {
		if strings.Contains(stmt, not_allowed_schemas[0]) {
			log.Warn("msg", "Skipping txn since it contained not permitted statements")
		}
		batch.Queue(stmt)
	}

	r := txn.SendBatch(context.Background(), batch)
	if _, err := r.Exec(); err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	if err = r.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	if err := txn.Commit(context.Background()); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
