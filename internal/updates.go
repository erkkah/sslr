package sslr

import (
	"context"
	"fmt"
	"strings"

	"github.com/erkkah/letarette/pkg/logger"
	"github.com/jackc/pgx/v4"
)

type updateRange struct {
	startXmin uint64
	endXmin   uint64
}

func getUpdateRange(conn *pgx.Conn, table string, lastSeenXmin uint64) (updateRange, error) {
	row := conn.QueryRow(context.Background(), fmt.Sprintf("select max(xmin::text::bigint) from %s", table))
	var resultRange updateRange
	err := row.Scan(&resultRange.endXmin)
	if err != nil {
		return resultRange, err
	}
	resultRange.startXmin = lastSeenXmin
	return resultRange, nil
}

func updateTable(source *pgx.Conn, target *pgx.Conn, table string, updRange updateRange, chunkSize uint32) error {
	start := updRange.startXmin
	for start <= updRange.endXmin {
		logger.Debug.Printf("Updating table %s from %v", table, start)
		q := fmt.Sprintf(`--sql 
		select
			xmin, *
		from
			%s
		where
			xmin::text::bigint >= $1
		order by
			xmin::text::bigint asc
		limit
			$2
		;`, table)
		rows, err := source.Query(context.Background(), q, start, chunkSize)
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
		var lastUpdatedXmin uint64
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				return err
			}

			lastUpdatedXmin = uint64(values[0].(uint32))
			rowValues = append(rowValues, values[1:])
		}
		err = applyUpdates(target, table, columnNames, rowValues)
		if err != nil {
			return fmt.Errorf("failed to apply updates: %w", err)
		}
		start = lastUpdatedXmin + 1
		err = setTableState(target, table, tableState{lastUpdatedXmin})
		if err != nil {
			return err
		}
	}

	return nil
}

func applyUpdates(target *pgx.Conn, table string, columns []string, values [][]interface{}) error {
	identifier := strings.Split(table, ".")
	rowsCopied, err := target.CopyFrom(context.Background(), identifier, columns, pgx.CopyFromRows(values))
	if err != nil {
		return err
	}
	if rowsCopied != int64(len(values)) {
		return fmt.Errorf("unexpected row count, %d != %d", rowsCopied, len(values))
	}
	return nil
}
