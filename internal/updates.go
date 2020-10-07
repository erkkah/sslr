package sslr

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/erkkah/letarette/pkg/logger"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
)

type updateRange struct {
	startXmin uint64
	endXmin   uint64
}

func (u updateRange) empty() bool {
	return u.startXmin > u.endXmin
}

func (job *Job) getUpdateRange(table string) (updateRange, error) {
	var resultRange updateRange

	state, err := job.getTableState(table)
	if err != nil {
		return resultRange, err
	}
	resultRange.startXmin = state.lastSeenXmin + 1

	row := job.source.QueryRow(context.Background(), fmt.Sprintf("select max(xmin::text::bigint) from %s", table))
	err = row.Scan(&resultRange.endXmin)
	if err != nil {
		return resultRange, err
	}

	return resultRange, nil
}

func (job *Job) updateTable(table string, primaryKey string, updRange updateRange) error {
	logger.Info.Printf("Updating table %s from %v to %v", table, updRange.startXmin, updRange.endXmin)
	throttle := newThrottle(job.cfg.ThrottlePercentage)
	xmin := updRange.startXmin
	offset := 0

	for xmin <= updRange.endXmin {
		logger.Debug.Printf("Updating from %v:%v", xmin, offset)
		throttle.start()
		q := fmt.Sprintf(`--sql 
		select
			xmin, *
		from
			%[1]s
		where
			xmin::text::bigint >= $1
		order by
			xmin::text::bigint asc,
			%[2]s
		offset
			$2
		limit
			$3
		;`, table, primaryKey)
		rows, err := job.source.Query(context.Background(), q, xmin, offset, job.cfg.UpdateChunkSize)
		if err != nil {
			return fmt.Errorf("query execution failure: %w", err)
		}
		rowsErr := rows.Err()
		if rowsErr != nil && rowsErr != pgx.ErrNoRows {
			return fmt.Errorf("row failure: %w", rowsErr)
		}

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

			fixInfiniteDates(columns, values)
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
		throttle.wait()

		if len(rowValues) > 0 {
			err = applyUpdates(job.target, table, primaryKey, columnNames, rowValues)
			if err != nil {
				return fmt.Errorf("failed to apply updates: %w", err)
			}
			job.updatedRows += uint32(len(rowValues))
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

func fixInfiniteDates(columns []pgproto3.FieldDescription, values []interface{}) {
	for i, any := range values {
		switch columns[i].DataTypeOID {
		case 1114: // timestamp
			fallthrough
		case 1184: // timestamptz
			if infinity, ok := any.(pgtype.InfinityModifier); ok {
				if infinity == pgtype.Infinity {
					values[i] = time.Unix(math.MaxInt32*100, 0)
				} else if infinity == pgtype.NegativeInfinity {
					values[i] = time.Unix(0, 0)
				}
			}
		}
	}
}

func applyUpdates(target *pgx.Conn, table string, primaryKey string, columns []string, values [][]interface{}) error {
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

	primaryColumnIndex := 0
	for i, col := range columns {
		if col == primaryKey {
			primaryColumnIndex = i
			break
		}
	}

	var keys []int32
	for _, value := range values {
		keys = append(keys, value[primaryColumnIndex].(int32))
	}

	err = deleteRows(tx, table, primaryKey, keys)
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

func deleteRows(target pgx.Tx, table string, primaryKey string, keys []int32) error {
	d := fmt.Sprintf(`--sql
	delete from %[1]s
	where %[2]s in (
		select * from unnest($1::int[])
	)
	;`, table, primaryKey)
	_, err := target.Exec(context.Background(), d, keys)
	return err
}
