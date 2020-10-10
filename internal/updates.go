package sslr

import (
	"context"
	"fmt"
	"strings"

	"github.com/erkkah/letarette/pkg/logger"
	"github.com/jackc/pgx/v4"
)

type updateRange struct {
	fullTable bool
	startXmin uint64
	endXmin   uint64
}

func (u updateRange) empty() bool {
	return u.startXmin > u.endXmin
}

func (job *Job) getUpdateRange(table string, where string) (updateRange, error) {
	var resultRange updateRange

	if _, ok := job.forceSync[table]; ok {
		resultRange.fullTable = true
	} else {
		state, err := job.getTableState(table)
		if err != nil {
			return resultRange, err
		}
		if state.lastSeenXmin == 0 {
			resultRange.fullTable = true
		} else {
			resultRange.startXmin = state.lastSeenXmin + 1
		}
	}

	var whereClause string
	if len(where) > 0 {
		whereClause = "where " + where
	}
	q := fmt.Sprintf("select count(*), max(xmin::text::bigint) from %s %s", table, whereClause)
	row := job.source.QueryRow(context.Background(), q)

	var sourceLength uint64
	err := row.Scan(&sourceLength, &resultRange.endXmin)
	if err != nil {
		return resultRange, err
	}

	if !resultRange.fullTable {
		targetLength, err := getTableLength(job.target, table, where)
		if err != nil {
			return resultRange, err
		}

		if float64(targetLength) < float64(sourceLength)*job.cfg.FullCopyThreshold {
			resultRange.fullTable = true
		}
	}

	return resultRange, nil
}

func (job *Job) updateTableRange(table string, primaryKeys []string, updRange updateRange, where string) error {
	logger.Debug.Printf("Updating table %s from %v to %v", table, updRange.startXmin, updRange.endXmin)
	throttle := newThrottle("updates", job.cfg.ThrottlePercentage)
	xmin := updRange.startXmin
	offset := 0

	var whereClause string
	if len(where) > 0 {
		whereClause = "and " + where
	}

	var keySorting []string

	for _, key := range primaryKeys {
		keySorting = append(keySorting, fmt.Sprintf("%s asc", key))
	}

	orderClause := strings.Join(keySorting, ", ")

	for xmin <= updRange.endXmin {
		throttle.start()
		q := fmt.Sprintf(`--sql 
		select
			xmin, *
		from
			%[1]s
		where
			xmin::text::bigint >= $1
			%[3]s
		order by
			xmin::text::bigint asc,
			%[2]s
		offset
			$2
		limit
			$3
		;`, table, orderClause, whereClause)

		logger.Info.Printf("Reading from source")

		rows, err := job.source.Query(context.Background(), q, xmin, offset, job.cfg.UpdateChunkSize)
		if err != nil {
			return fmt.Errorf("query execution failure: %w", err)
		}
		rowsErr := rows.Err()
		if rowsErr != nil && rowsErr != pgx.ErrNoRows {
			return fmt.Errorf("row failure: %w", rowsErr)
		}
		defer rows.Close()

		var columnNames []string
		columns := rows.FieldDescriptions()
		for _, column := range columns[1:] {
			columnNames = append(columnNames, string(column.Name))
		}

		var rowValues [][]interface{}
		lastCompleteXmin := uint64(0)

		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				return err
			}

			lastUpdatedXmin := uint64(values[0].(uint32))
			if lastUpdatedXmin == xmin {
				offset++
			} else {
				lastCompleteXmin = xmin
				xmin = lastUpdatedXmin
				offset = 1
			}
			rowValues = append(rowValues, values[1:])
		}
		throttle.end()

		if len(rowValues) > 0 {
			logger.Info.Printf("Writing %d rows to target", len(rowValues))
			err = applyUpdates(job.target, table, primaryKeys, columnNames, rowValues)
			if err != nil {
				return fmt.Errorf("failed to apply updates: %w", err)
			}
			job.updatedRows += uint32(len(rowValues))
			throttle.wait()
		} else {
			lastCompleteXmin = xmin
			xmin++
		}

		if lastCompleteXmin != 0 {
			err = job.setTableState(table, tableState{lastCompleteXmin})
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func applyUpdates(target *pgx.Conn, table string, primaryKeys []string, columns []string, values [][]interface{}) error {
	ctx := context.Background()
	tx, err := target.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			tx.Rollback(ctx)
		}
	}()

	var primaryColumnIndices = make([]int, len(primaryKeys))

	for i, primaryKey := range primaryKeys {
		for j, col := range columns {
			if col == primaryKey {
				primaryColumnIndices[i] = j
				break
			}
		}
	}

	var keys PrimaryKeySetSlice
	for _, row := range values {
		var rowKeys PrimaryKeySet
		for _, keyIndex := range primaryColumnIndices {
			rowKeys = append(rowKeys, PrimaryKey{row[keyIndex]})
		}
		keys = append(keys, rowKeys)
	}

	err = deleteRows(tx, table, primaryKeys, keys)
	if err != nil {
		return err
	}

	identifier := strings.Split(table, ".")
	rowsCopied, err := tx.CopyFrom(ctx, identifier, columns, pgx.CopyFromRows(values))
	if err != nil {
		return err
	}
	if rowsCopied != int64(len(values)) {
		return fmt.Errorf("unexpected row count, %d != %d", rowsCopied, len(values))
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	tx = nil
	return nil
}

func deleteRows(target pgx.Tx, table string, primaryKeys []string, keys PrimaryKeySetSlice) error {
	keyList := strings.Join(primaryKeys, ", ")

	d := fmt.Sprintf(`--sql
	delete from %[1]s
	where array[[%[2]s]] <@ $1
	;`, table, keyList)

	tag, err := target.Exec(context.Background(), d, keys.StringValues())
	if err != nil {
		return err
	}
	logger.Debug.Printf("Deleted %d rows", tag.RowsAffected())
	return nil
}

func getTableLength(conn *pgx.Conn, table string, where string) (uint64, error) {
	var whereClause string
	if len(where) > 0 {
		whereClause = "where " + where
	}

	q := fmt.Sprintf(`--sql 
	select
		count(*)
	from
		%[1]s
	%[2]s
	;`, table, whereClause)

	row := conn.QueryRow(context.Background(), q)
	var count uint64
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
