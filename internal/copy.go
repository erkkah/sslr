package sslr

import (
	"fmt"
	"strings"

	"github.com/erkkah/letarette/pkg/logger"
	"github.com/jackc/pgx/v4"
)

func (job *Job) copyFullTable(table string, where string) error {
	var whereClause string
	if len(where) > 0 {
		whereClause = "where " + where
	}
	q := fmt.Sprintf("select * from %s %s", table, whereClause)
	rows, err := job.source.Query(job.ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()

	var columnNames []string
	columns := rows.FieldDescriptions()
	for _, column := range columns {
		columnNames = append(columnNames, string(column.Name))
	}

	tx, err := job.target.Begin(job.ctx)
	if err != nil {
		return err
	}

	defer func() {
		if tx != nil {
			tx.Rollback(job.ctx)
		}
	}()

	_, err = tx.Exec(job.ctx, fmt.Sprintf("delete from %s", table))
	if err != nil {
		return fmt.Errorf("failed to delete old data: %w", err)
	}

	logger.Info.Printf("Running streaming copy")
	identifier := strings.Split(table, ".")
	updatedRows, err := tx.CopyFrom(job.ctx, identifier, columnNames, newReportingSource(rows))
	if err != nil {
		return err
	}

	err = tx.Commit(job.ctx)
	if err != nil {
		return err
	}
	job.updatedRows += uint32(updatedRows)
	tx = nil
	return nil
}

type reportingSource struct {
	wrapped  pgx.CopyFromSource
	rowsRead uint32
}

func newReportingSource(source pgx.CopyFromSource) pgx.CopyFromSource {
	return &reportingSource{
		source,
		0,
	}
}

func (r *reportingSource) Next() bool {
	hasNext := r.wrapped.Next()
	if hasNext {
		r.rowsRead++

		var reportInterval uint32
		switch {
		case r.rowsRead > 1000000:
			reportInterval = 500000
			break
		case r.rowsRead > 100000:
			reportInterval = 100000
			break
		default:
			reportInterval = 10000
		}

		if r.rowsRead%reportInterval == 0 {
			logger.Info.Printf("Read %v rows", r.rowsRead)
		}
	} else {
		logger.Info.Printf("Done reading, %v rows in total", r.rowsRead)
	}
	return hasNext
}

func (r *reportingSource) Values() ([]interface{}, error) {
	return r.wrapped.Values()
}

func (r *reportingSource) Err() error {
	return r.wrapped.Err()
}
