package progress

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const createProgressTable = `
create table public._timescale_pg_parallel_txn_progress (
	time timestamptz not null,
	xid bigint not null,
	lsn text not null,
	primary key (time, xid) -- This guards from duplicate txns.
)`

func CreateProgressTable(conn *pgxpool.Pool) error {
	_, err := conn.Exec(context.Background(), createProgressTable)
	return err
}

const advanceProgress = `insert into public._timescale_pg_parallel_txn_progress (time, xid, lsn) values (current_timestamp, $1, $2)`

func Advance(conn *pgx.Conn, xid uint64, lsn string) error {
	_, err := conn.Exec(context.Background(), advanceProgress, time.Now())
	return err
}

const tableExists = `
select exists (
	select from information_schema.tables
	where  table_schema = 'public'
	and    table_name   = '_timescale_pg_parallel_txn_progress'
)`

func TableExists(conn *pgxpool.Pool) (bool, error) {
	var exists bool
	err := conn.QueryRow(context.Background(), tableExists).Scan(&exists)
	return exists, err
}

// If 2 txns were commited at the same time, then the xid that is larger is the latter one that was received from pgcopydb
const lastTxnDetails = `select xid::bigint, lsn::text from public._timescale_pg_parallel_txn_progress order by time, xid limit 1`

func LastTxnDetails(conn *pgxpool.Pool) (xid uint64, lsn string, err error) {
	err = conn.QueryRow(context.Background(), lastTxnDetails).Scan(&xid, &lsn)
	return
}
