package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	commitqueue "github.com/Harkishen-Singh/pg-parallel-txn/commit_queue"
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
