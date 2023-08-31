package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	commitqueue "github.com/Harkishen-Singh/pg-parallel-txn/commit_queue"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/promscale/pkg/log"
)

const commitQueueCheckDuration = time.Millisecond * 10

type Worker struct {
	ctx      context.Context
	id       int
	conn     *pgxpool.Pool
	incoming <-chan *txn
	active   *sync.WaitGroup
	commitQ  *commitqueue.CommitQueue
}

func (w *Worker) Run() {
	log.Info("msg", fmt.Sprintf("starting worker %d", w.id))
	for {
		stmts, ok := <-w.incoming
		if !ok {
			log.Info("msg", fmt.Sprintf("shutting down worker %d", w.id))
			return
		}
		w.active.Add(1)
		perform := func() {
			batchCtx, batchCancel := context.WithCancel(w.ctx)
			defer batchCancel()
			if err := doBatch(batchCtx, w.conn, w.commitQ, stmts); err != nil {
				log.Fatal("msg", "error doBatch", "err", err.Error())
			}
		}
		perform()
		w.active.Done()
	}
}

var schemasNotAllowed = []string{"_timescaledb_catalog"}

func doBatch(
	ctx context.Context,
	conn *pgxpool.Pool,
	commitQ *commitqueue.CommitQueue,
	t *txn,
) error {
	newTxn, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer newTxn.Rollback(ctx)

	batch := &pgx.Batch{}
	for _, stmt := range t.stmts {
		if strings.Contains(stmt, schemasNotAllowed[0]) {
			log.Warn("msg", "Skipping txn since it contained not permitted statements")
		}
		batch.Queue(stmt)
	}

	r := newTxn.SendBatch(ctx, batch)
	if _, err := r.Exec(); err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	if err = r.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	// Wait for my commit turn.
	for {
		if xid := commitQ.Peek(); xid == uint64(t.begin.XID) {
			break
		}
		<-time.After(commitQueueCheckDuration)
	}

	if err := newTxn.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	commitQ.Dequeue()
	return nil
}
