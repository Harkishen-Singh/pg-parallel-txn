package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/promscale/pkg/log"
)

// txnStatements contains all the statements that are part of
// a transaction in the order in which they are received.
// These statements do not include BEGIN; & COMMIT;, rather
// the ones between them.
type txnStatements struct {
	stmts []string
}

type worker struct {
	id                   int
	conn                 *pgxpool.Pool
	incomingTxn          <-chan *txnStatements
	activeParallelIngest *sync.WaitGroup
}

func NewWorker(id int, pool *pgxpool.Pool, incomingTxn <-chan *txnStatements, activeParallelIngest *sync.WaitGroup) *worker {
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
		perform := func() {
			txn, err := w.conn.Begin(context.Background())
			if err != nil {
				log.Error("msg", "error starting a txn", "error", err.Error())
				return
			}

			batch := &pgx.Batch{}
			for _, stmt := range stmts.stmts {
				batch.Queue(stmt)
			}

			r := txn.SendBatch(context.Background(), batch)
			rows, err := r.Query()
			if err != nil {
				log.Error("msg", "error querying batch results", "error", err.Error())
				return
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				log.Error("msg", "error closing rows from batch", "error", err.Error())
				return
			}

			if err := txn.Commit(context.Background()); err != nil {
				log.Error("msg", "error commiting a txn", "error", err.Error(), "txn-statements", stmts.stmts)
				return
			}
		}
		perform()
		w.activeParallelIngest.Done()
	}
}
