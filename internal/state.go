package sslr

import (
	"fmt"

	"github.com/jackc/pgx/v4"
)

type tableState struct {
	lastSeenXmin uint64
	whereClause  string
}

func (ts tableState) empty() bool {
	return ts.lastSeenXmin == 0 && ts.whereClause == ""
}

func (job *Job) setupStateTable() error {

	q := fmt.Sprintf(`--sql
	create table if not exists %s (
		table_name varchar(512) primary key,
		last_seen_xmin bigint,
		where_clause varchar
	)
	;`, job.cfg.StateTableName)

	_, err := job.target.Exec(job.ctx, q)
	return err
}

func (job *Job) getTableState(table string) (tableState, error) {
	var state tableState

	err := job.setupStateTable()
	if err != nil {
		return state, fmt.Errorf("failed to setup state table: %w", err)
	}

	q := fmt.Sprintf(`--sql
	select last_seen_xmin, coalesce(where_clause, '')
	from %s
	where table_name = $1
	;`, job.cfg.StateTableName)

	row := job.target.QueryRow(job.ctx, q, table)
	err = row.Scan(&state.lastSeenXmin, &state.whereClause)
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
	insert into %s (table_name, last_seen_xmin, where_clause) values($1, $2, $3)
	on conflict (table_name)
	do update set last_seen_xmin = $2, where_clause = $3
	;`, job.cfg.StateTableName)

	_, err = job.target.Exec(job.ctx, q, table, state.lastSeenXmin, state.whereClause)
	if err != nil {
		return fmt.Errorf("failed to set table state: %w", err)
	}
	return nil
}

func (job *Job) setTableStateXmin(table string, xmin uint64) error {
	state, err := job.getTableState(table)
	if err != nil {
		return err
	}

	state.lastSeenXmin = xmin
	err = job.setTableState(table, state)
	if err != nil {
		return err
	}
	return nil
}
