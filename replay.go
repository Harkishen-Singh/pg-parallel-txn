package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Harkishen-Singh/pg-parallel-txn/commit_queue"
	"github.com/Harkishen-Singh/pg-parallel-txn/common"
	"github.com/Harkishen-Singh/pg-parallel-txn/format"
	"github.com/Harkishen-Singh/pg-parallel-txn/progress"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/timescale/promscale/pkg/log"
)

type Replayer struct {
	ctx  context.Context
	pool *pgxpool.Pool
	// lsnp                  LSNProceeder // todo: remove lsn proceeder and make it an independent routine
	skipTxns              *atomic.Bool
	parallelTxn           chan<- *txn
	activeIngests         *sync.WaitGroup
	proceedLSNAfterBatch  bool
	commitQ               *commitqueue.CommitQueue
	activeTxn             *txn
	tracker               *progress.Tracker
	performCatchupOnStart bool
}

// Replay all the SQL files in order against the target and proceed the LSN in source database.
func (r *Replayer) Replay(pendingSQLFilesInOrder []string) {
	commitMetadata := CommitMetadata{}
	for fileCount, pendingFile := range pendingSQLFilesInOrder {
		totalTxns := getTotalTxns(pendingFile)
		txnCount := int64(0)

		replayFile := func(filePath string) {
			filePath = format.Format(filePath)
			log.Info(
				"Replaying", common.GetFileName(filePath),
				"total_txns", totalTxns,
				"progress", fmt.Sprintf("%d/%d", fileCount, len(pendingSQLFilesInOrder)))

			file, err := os.Open(filePath)
			if err != nil {
				panic(err)
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			scanner.Split(bufio.ScanLines)
			t := r.activeTxn
			if t == nil {
				// No previous txn exists.
				t = &txn{
					currentFilePath: filePath,
					stmts:           make([]string, 0),
				}
			}
			for {
				err := r.readNextTxn(scanner, t)
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Fatal("msg", "Error getting next txn", "err", err.Error())
				}
				if r.performCatchupOnStart {
					// We need to catchup to the previous progress, and then start replaying the txns,
					// otherwise we will repeat the transactions that have already been applied.
					current_lsn, current_file := t.commit.LSN, filePath
					last_lsn, last_file := r.tracker.LastProgressDetails()
					if current_lsn == last_lsn && common.FileNameWithoutExtension(current_file) == common.FileNameWithoutExtension(last_file) {
						log.Info("msg", "found the txn upto which we had replayed previously. Resuming replay from the next txn")
						r.performCatchupOnStart = false
					}
					t.refresh()
					continue
				}
				r.commitQ.Enqueue(uint64(t.begin.XID))
				txnCount++
				if isInsertOnly(t.stmts) {
					r.parallelTxn <- t
					log.Debug("msg", "execute parallel txn", "xid", t.begin.XID, "num_stmts", len(t.stmts), "progress", fmt.Sprintf("%d/%d", txnCount, totalTxns))
				} else {
					log.Debug("msg", fmt.Sprintf("received a serial txn type (xid:%d); waiting for scheduled parallel txns to complete", t.begin.XID))
					r.activeIngests.Wait()
					if !r.commitQ.IsEmpty() {
						panic("queue should have been empty after all active ingests are 0. Something is wrong in the workers")
					}
					log.Debug("msg", "execute serial txn", "xid", t.begin.XID, "num_stmts", len(t.stmts), "progress", fmt.Sprintf("%d/%d", txnCount, totalTxns))
					if err := r.doSerialInsert(t); err != nil {
						log.Fatal("msg", "Error executing a serial txn", "xid", t.begin.XID, "err", err.Error())
					}
				}
				t.refresh()
			}
			if t.begin != nil && t.commit == nil {
				// An open txn found. We need to carry this over to the next file.
				// This occurs when we found BEGIN; but not COMMIT;.
				// Common in last txn in a file, whose remaining part is in another file.
				r.activeTxn = t
			}
			if err := scanner.Err(); err != nil {
				log.Fatal("msg", "Error scanning file", "err", err.Error(), "file", filePath)
			}
		}
		start := time.Now()
		replayFile(pendingFile)
		// Let's wait for previous batch to complete before moving to the next batch.
		if len(r.activeTxn.stmts) > 0 {
			log.Debug("msg",
				fmt.Sprintf("found a txn (xid:%d) that stretches beyond current file. Holding its contents till the previous batch completes", commitMetadata.XID))
		}
		log.Info("msg", "Waiting for batch to complete")
		r.activeIngests.Wait() // Todo: this wait is optional. We can survive without this wait as well.
		log.Info("Done", time.Since(start).String())
	}
	log.Info("msg", "Replaying of SQL files batch completed", "num_files_replayed", len(pendingSQLFilesInOrder))
}

// txn contains all the statements that are part of
// a transaction in the order in which they are received.
// These statements do not include BEGIN; & COMMIT;, rather
// the ones between them.
type txn struct {
	currentFilePath string
	begin           *BeginMetadata
	commit          *CommitMetadata
	stmts           []string
}

func (t *txn) refresh() {
	t.begin = nil
	t.commit = nil
	t.stmts = t.stmts[:0]
}

func (r *Replayer) readNextTxn(scanner *bufio.Scanner, t *txn) error {
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line[:6] == "BEGIN;":
			if t.begin != nil {
				return fmt.Errorf(
					"faulty txn: Cannot start a new txn when a txn is already open. File=>%s xid=>%d lsn=>%s",
					common.GetFileName(t.currentFilePath),
					t.begin.XID,
					t.begin.LSN,
				)
			}
			*t.begin = GetBeginMetadata(line)
		case line[:7] == "COMMIT;":
			if t.begin == nil {
				return fmt.Errorf(
					"incomplete txn: Received COMMIT; when BEGIN; was not received. Skipping this txn. commit.xid: %d, commit.lsn: %s",
					t.commit.XID,
					t.commit.LSN)
			}
			*t.commit = GetCommitMetadata(line)
			if t.begin.XID != t.commit.XID {
				// This serves as an important check for txns that are spread over multiple files.
				// Though we read the WAL files in order in which they are created, we need a
				// reliable way to check if the txn we constructed has correct contents or not.
				//
				// When commits are spread over files, the Begin_txn_id is in a different file than
				// Commit_txn_id. Hence, the xid of Begin & Commit txn_id must be same.
				return fmt.Errorf(
					"FATAL: Faulty txn constructed. Begin.XID (%d) does not match Commit.XID (%d). File: %s",
					t.begin.XID,
					t.commit.XID,
					common.GetFileName(t.currentFilePath),
				)
			}
			return nil
		case line[:3] == "-- ":
			// Ignore all comments.
			continue
		default:
			if strings.Contains(line, "_timescaledb_catalog") {
				continue
			}
			t.stmts = append(t.stmts, line)
		}
	}
	return io.EOF
}

func (r *Replayer) doSerialInsert(t *txn) error {
	if err := doBatch(r.ctx, r.tracker, r.pool, r.commitQ, t); err != nil {
		return fmt.Errorf("doBatch: %w", err)
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
