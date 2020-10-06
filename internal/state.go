package sslr

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
)

type tableState struct {
	lastSeenXmin uint64
}

func setupStateTable(conn *pgx.Conn) error {
	q := `--sql
	create table if not exists __sslr_state (
		table_name varchar(512) primary key,
		last_seen_xmin bigint
	)
	;`

	_, err := conn.Exec(context.Background(), q)
	return err
}

func getTableState(conn *pgx.Conn, table string) (tableState, error) {
	var state tableState

	err := setupStateTable(conn)
	if err != nil {
		return state, fmt.Errorf("failed to setup state table: %w", err)
	}

	q := `--sql
	select last_seen_xmin
	from __sslr_state
	where table_name = $1
	;`
	row := conn.QueryRow(context.Background(), q, table)
	err = row.Scan(&state.lastSeenXmin)
	if err == pgx.ErrNoRows {
		return state, nil
	}
	if err != nil {
		return state, fmt.Errorf("failed to load table state: %w", err)
	}

	return state, nil
}

func setTableState(conn *pgx.Conn, table string, state tableState) error {
	err := setupStateTable(conn)
	if err != nil {
		return fmt.Errorf("failed to setup state table: %w", err)
	}

	q := `--sql
	insert into __sslr_state (table_name, last_seen_xmin) values($1, $2)
	on conflict (table_name)
	do update set last_seen_xmin = $2
	;`

	_, err = conn.Exec(context.Background(), q, table, state.lastSeenXmin)
	if err != nil {
		return fmt.Errorf("failed to set table state: %w", err)
	}
	return nil
}
