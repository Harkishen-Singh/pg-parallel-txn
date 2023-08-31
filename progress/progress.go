package progress

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Tracker struct {
	conn *pgxpool.Pool
	LSN, File string
	XID       uint64
}

func NewTracker(conn *pgxpool.Pool) (*Tracker, error) {
	tr := &Tracker{conn: conn}
	exists, err := progressTableExists(tr.conn)
	if err != nil {
		return nil, fmt.Errorf("progress table exists: %w", err)
	}
	if !exists {
		if err = createProgressTable(tr.conn); err != nil {
			return nil, fmt.Errorf("create progress table: %w", err)
		}
		return tr, nil
	}
	last_xid, last_lsn, last_file, err := lastTxnDetails(conn)
	if err != nil {
		return nil, fmt.Errorf("get last txn details: %w", err)
	}
	tr.File = last_file
	tr.LSN = last_lsn
	tr.XID = last_xid
	return tr, nil
}

const createProgressTableSQL = `
create table public._timescale_pg_parallel_txn_progress (
	time timestamptz not null,
	xid bigint not null,
	lsn text not null,
	file text not null,
	primary key (time, xid) -- This guards from duplicate txns.
)`

func createProgressTable(conn *pgxpool.Pool) error {
	_, err := conn.Exec(context.Background(), createProgressTableSQL)
	return err
}

const advanceProgressSQL = `insert into public._timescale_pg_parallel_txn_progress (time, xid, lsn, file) values (current_timestamp, $1, $2, $3)`

func (tr *Tracker) Advance(txConn *pgx.Conn, xid uint64, lsn, file string) error {
	_, err := txConn.Exec(context.Background(), advanceProgressSQL, xid, lsn, file)
	if err != nil {
		return fmt.Errorf("error advancing progress: %w", err)
	}
	tr.File = file
	tr.LSN = lsn
	tr.XID = xid
	return err
}

const tableExistSQL = `
select exists (
	select from information_schema.tables
	where  table_schema = 'public'
	and    table_name   = '_timescale_pg_parallel_txn_progress'
)`

func progressTableExists(conn *pgxpool.Pool) (bool, error) {
	var exists bool
	err := conn.QueryRow(context.Background(), tableExistSQL).Scan(&exists)
	return exists, err
}

// If 2 txns were commited at the same time, then the xid that is larger is the latter one that was received from pgcopydb
const lastTxnDetailsSQL = `select xid::bigint, lsn::text, file::text from public._timescale_pg_parallel_txn_progress order by time, xid limit 1`

func lastTxnDetails(conn *pgxpool.Pool) (xid uint64, lsn, file string, err error) {
	err = conn.QueryRow(context.Background(), lastTxnDetailsSQL).Scan(&xid, &lsn, &file)
	return
}
