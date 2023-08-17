package main

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/timescale/promscale/pkg/log"
)

const PROCEED_LSN = "update pgcopydb.sentinel set replay_lsn = $1"

type LSNProceeder interface {
	IncrementTxn(commitLSN string)
	Proceed()
}

type proceedLSN struct {
	conn                  *pgx.Conn
	parallelIngest        *sync.WaitGroup
	numXidsToProceedAfter int64
	currentXid            int64
	lastCommitLSN         string
}

func NewLSNProceeder(conn *pgx.Conn, numXidsToProceedAfter int64, parallelIngest *sync.WaitGroup) LSNProceeder {
	return &proceedLSN{
		conn:                  conn,
		numXidsToProceedAfter: numXidsToProceedAfter,
		parallelIngest:        parallelIngest,
	}
}

func (p *proceedLSN) IncrementTxn(commitLSN string) {
	p.currentXid++
	p.lastCommitLSN = commitLSN
	if p.currentXid > p.numXidsToProceedAfter {
		log.Info("msg", "Proceeding LSN. Waiting for parallel txns to complete")
		p.parallelIngest.Wait()
		log.Info("msg", "Proceeding LSN", "LSN", commitLSN)
		if err := p.proceed(commitLSN); err != nil {
			log.Fatal("msg", "could not proceed LSN in Source DB", "err", err.Error())
		}
		p.currentXid = 0
	}
}

func (p *proceedLSN) Proceed() {
	if err := p.proceed(p.lastCommitLSN); err != nil {
		log.Fatal("msg", "manual proceed: could not proceed LSN in Source DB", "err", err.Error())
	}
}

func (p *proceedLSN) proceed(newLSN string) error {
	_, err := p.conn.Exec(context.Background(), PROCEED_LSN, newLSN)
	return err
}

type noopProceeder struct{}

func NewNoopProceeder() LSNProceeder {
	return &noopProceeder{}
}

func (n noopProceeder) IncrementTxn(string) {}
func (n noopProceeder) Proceed()            {}
