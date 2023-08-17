package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/promscale/pkg/log"
)

type Replayer struct {
	pool                 *pgxpool.Pool
	lsnp                 LSNProceeder
	skipTxns             *atomic.Bool
	parallelTxn          chan<- *txn
	activeIngests        *sync.WaitGroup
	proceedLSNAfterBatch bool
}

// Replay all the SQL files in order against the target and proceed the LSN in source database.
func (r *Replayer) Replay(pendingSQLFilesInOrder []string) {
	stmts := []string{}
	isTxnOpen := false // Helps to capture commits that are spread over multiple files.
	beginMetadata := BeginMetadata{}
	commitMetadata := CommitMetadata{}
	for fileCount, filePath := range pendingSQLFilesInOrder {
		totalTxns := getTotalTxns(filePath)
		txnCount := int64(0)

		replayFile := func(filePath string) {
			log.Info(
				"Replaying", getFileName(filePath),
				"total_txns", totalTxns,
				"progress", fmt.Sprintf("%d/%d", fileCount, len(pendingSQLFilesInOrder)))

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
					if r.skipTxns.Load() {
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
						r.performTxn(
							commitMetadata.XID,
							stmts,
							txnCount,
							totalTxns,
						)
					}
					r.lsnp.IncrementTxn(commitMetadata.LSN)
				case line[:3] == "-- ":
					// Ignore all comments.
					continue
				default:
					stmts = append(stmts, line)
				}
			}
			if err := scanner.Err(); err != nil {
				log.Fatal("msg", "Error scanning file", "error", err.Error(), "file", filePath)
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
		r.activeIngests.Wait()
		if r.proceedLSNAfterBatch {
			// Proceed LSN after batch completes is activated.
			r.lsnp.Proceed()
		}
		log.Info("Done", time.Since(start).String())
	}
}

func (r *Replayer) performTxn(
	xid int64,
	stmts []string,
	txnCount int64,
	totalTxns int64,
) {
	t := &txn{
		stmts: stmts,
	}
	if isInsertOnly(stmts) {
		r.parallelTxn <- t
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
		r.activeIngests.Wait()
		log.Debug(
			"msg", "execute serial txn",
			"xid", xid,
			"num_stmts", len(stmts),
			"progress", fmt.Sprintf("%d/%d", txnCount, totalTxns))
		// Now all parallel txns have completed. Let's do the serial txn.
		if err := r.doSerialInsert(t); err != nil {
			log.Fatal("msg", "Error executing a serial txn", "xid", xid, "err", err.Error())
		}
	}
}

func (r *Replayer) doSerialInsert(t *txn) error {
	txn, err := r.pool.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("error starting a txn: %w", err)
	}
	defer txn.Rollback(context.Background())

	batch := &pgx.Batch{}
	for _, stmt := range t.stmts {
		batch.Queue(stmt)
	}

	result := txn.SendBatch(context.Background(), batch)
	_, err = result.Exec()
	if err != nil {
		return fmt.Errorf("error executing batch results: %w", err)
	}
	err = result.Close()
	if err != nil {
		return fmt.Errorf("error closing rows from batch: %w", err)
	}

	if err := txn.Commit(context.Background()); err != nil {
		return fmt.Errorf("error commiting a txn: %w", err)
	}

	return nil
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
