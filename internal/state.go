package sslr

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
)

type tableState struct {
	lastSeenXmin uint64
}

func (job *Job) setupStateTable() error {

	q := fmt.Sprintf(`--sql
	create table if not exists %s (
		table_name varchar(512) primary key,
		last_seen_xmin bigint
	)
	;`, job.cfg.StateTableName)

	_, err := job.target.Exec(context.Background(), q)
	return err
}

func (job *Job) getTableState(table string) (tableState, error) {
	var state tableState

	err := job.setupStateTable()
	if err != nil {
		return state, fmt.Errorf("failed to setup state table: %w", err)
	}

	q := fmt.Sprintf(`--sql
	select last_seen_xmin
	from %s
	where table_name = $1
	;`, job.cfg.StateTableName)

	row := job.target.QueryRow(context.Background(), q, table)
	err = row.Scan(&state.lastSeenXmin)
	if err == pgx.ErrNoRows {
		return state, nil
	}
	if err != nil {
		return state, fmt.Errorf("failed to load table state: %w", err)
	}

	return state, nil
}

func (job *Job) setTableState(table string, state tableState) error {
	err := job.setupStateTable()
	if err != nil {
		return fmt.Errorf("failed to setup state table: %w", err)
	}

	q := fmt.Sprintf(`--sql
	insert into %s (table_name, last_seen_xmin) values($1, $2)
	on conflict (table_name)
	do update set last_seen_xmin = $2
	;`, job.cfg.StateTableName)

	_, err = job.target.Exec(context.Background(), q, table, state.lastSeenXmin)
	if err != nil {
		return fmt.Errorf("failed to set table state: %w", err)
	}
	return nil
}
