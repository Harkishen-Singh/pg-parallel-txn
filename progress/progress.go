package progress

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const replicationOriginName = "pg-parallel-txn-progress"

type Tracker struct {
	mux       sync.RWMutex
	conn      *pgxpool.Pool
	LSN, File string
}

func NewTracker(conn *pgxpool.Pool) (*Tracker, error) {
	tr := &Tracker{conn: conn}
	exists, err := originExists(tr.conn)
	if err != nil {
		return nil, fmt.Errorf("progress table exists: %w", err)
	}
	if !exists {
		if err = createOrigin(tr.conn); err != nil {
			return nil, fmt.Errorf("create progress table: %w", err)
		}
		if err = tr.markAsCurrent(); err != nil {
			return nil, fmt.Errorf("setup after createOrigin: %w", err)
		}
		return tr, nil
	}
	if err = tr.markAsCurrent(); err != nil {
		return nil, fmt.Errorf("setup after fetching details of last txn: %w", err)
	}
	last_lsn, last_file, err := lastTxnDetails(conn)
	if err != nil {
		return nil, fmt.Errorf("get last txn details: %w", err)
	}
	tr.File = last_file
	tr.LSN = last_lsn
	return tr, nil
}

func (tr *Tracker) HasAnyProgress() bool {
	tr.mux.RLock()
	defer tr.mux.RUnlock()
	return tr.LSN != ""
}

func (tr *Tracker) LastProgressDetails() (lsn, walfile string) {
	tr.mux.RLock()
	defer tr.mux.RUnlock()
	return tr.LSN, tr.File
}

func (tr *Tracker) markAsCurrent() error {
	tr.mux.Lock()
	defer tr.mux.Unlock()
	_, err := tr.conn.Exec(context.Background(), `select pg_replication_origin_session_setup($1::text)`, replicationOriginName)
	return err
}

func createOrigin(conn *pgxpool.Pool) error {
	_, err := conn.Exec(context.Background(), `select pg_replication_origin_create($1::text)`, replicationOriginName)
	return err
}

// Advance proceeds the LSN pointer in replication origin in the same txn by using the transactional connection.
// Todo(harkishen): Make proceeding LSN pointer part of the pgx.SendBatch() that contains the txn data.
func (tr *Tracker) Advance(txConn *pgx.Conn, lsn, file string) error {
	tr.mux.Lock()
	defer tr.mux.Unlock()
	if _, err := txConn.Exec(context.Background(), `select pg_replication_origin_advance($1::text, $2::pg_lsn)`, replicationOriginName, lsn); err != nil {
		return fmt.Errorf("error advancing progress: %w", err)
	}
	tr.File = file
	tr.LSN = lsn
	return nil
}

func originExists(conn *pgxpool.Pool) (bool, error) {
	originID := new(int32)
	err := conn.QueryRow(context.Background(), `select pg_replication_origin_oid($1::text)`, replicationOriginName).Scan(&originID)
	return originID != nil, err
}

const lastTxnDetailsSQL = `
select
	a.lsn::text as lsn,
	pg_walfile_name(a.lsn)::text walfile
from (
	select pg_replication_origin_session_progress(false) as lsn
) a`

func lastTxnDetails(conn *pgxpool.Pool) (lsn, file string, err error) {
	err = conn.QueryRow(context.Background(), lastTxnDetailsSQL).Scan(&lsn, &file)
	return
}
