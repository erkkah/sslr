package sslr

import (
	"context"
	"fmt"
	"strings"

	"github.com/erkkah/letarette/pkg/logger"
	"github.com/jackc/pgx/v4"
)

func (job *Job) syncDeletedRows(table string) error {
	primaryKey, err := job.getPrimaryKey(table)
	if err != nil {
		return err
	}

	keyRange, err := getPrimaryKeyRange(job.source, table, primaryKey)
	if err != nil {
		return fmt.Errorf("failed to get primary key range: %w", err)
	}

	chunkSize := job.cfg.DeleteChunkSize
	startKey := roundDownToEven(keyRange.min, chunkSize)
	throttle := newThrottle(job.cfg.ThrottlePercentage)

	for ; startKey < keyRange.max; startKey += chunkSize {
		throttle.start()
		job.syncDeletedRowRange(table, primaryKey, startKey, startKey+chunkSize)
		throttle.end()
		throttle.wait()
	}

	return nil
}

func (job *Job) syncDeletedRowRange(table string, primaryKey string, startKey uint32, endKey uint32) error {
	sourceHash, err := getKeyHash(job.source, table, primaryKey, startKey, endKey)
	if err != nil {
		return err
	}
	targetHash, err := getKeyHash(job.target, table, primaryKey, startKey, endKey)
	if err != nil {
		return err
	}
	if sourceHash != targetHash {
		chunkSize := endKey - startKey
		if chunkSize <= job.cfg.MinDeleteChunkSize {
			logger.Debug.Printf("Updating (%v - %v)", startKey, endKey)
			err = job.updateChangedRange(table, primaryKey, startKey, endKey)
			if err != nil {
				return err
			}
		} else {
			nextChunkSize := chunkSize / 2
			err = job.syncDeletedRowRange(table, primaryKey, startKey, startKey+nextChunkSize)
			if err != nil {
				return err
			}
			err = job.syncDeletedRowRange(table, primaryKey, startKey+nextChunkSize, endKey)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (job *Job) updateChangedRange(table string, primaryKey string, startKey uint32, endKey uint32) error {
	ctx := context.Background()
	tx, err := job.target.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if tx != nil {
			tx.Rollback(ctx)
		}
	}()

	q := fmt.Sprintf(`--sql
	select
		*
	from
		%[1]s
	where
		%[2]s >= $1
	and
		%[2]s <= $2
	;`, table, primaryKey)

	rows, err := job.source.Query(ctx, q, startKey, endKey)
	if err != nil {
		return err
	}
	defer rows.Close()

	rowErr := rows.Err()
	if rowErr == pgx.ErrNoRows {
		return nil
	}
	if rowErr != nil {
		return rowErr
	}

	var columnNames []string
	columns := rows.FieldDescriptions()
	for _, column := range columns[1:] {
		columnNames = append(columnNames, string(column.Name))
	}

	d := fmt.Sprintf(`--sql 
	delete from %[1]s
	where
	where
		%[2]s >= $1
	and
		%[2]s <= $2
	;`, table, primaryKey)

	_, err = tx.Exec(ctx, d, startKey, endKey)
	if err != nil {
		return err
	}

	identifier := strings.Split(table, ".")
	updatedRows, err := tx.CopyFrom(ctx, identifier, columnNames, rows)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	job.updatedRows += uint32(updatedRows)
	tx = nil
	return nil
}

func getKeyHash(conn *pgx.Conn, table string, primaryKey string, startKey, endKey uint32) (string, error) {
	q := `--sql 
	select
		md5(array_agg(id)::varchar) as hash
	from (
		select
			%[1]s as id
		from
			%[2]s
		where
			%[1]s >= $1
			and
			%[1]s < $2
		order by
			1
	) as t
	;`
	row := conn.QueryRow(context.Background(), fmt.Sprintf(q, primaryKey, table), startKey, endKey)
	var hash string
	err := row.Scan(&hash)
	if err != nil {
		return "", err
	}
	return hash, nil
}

func roundDownToEven(num uint32, chunkSize uint32) uint32 {
	return num - (num % chunkSize)
}

func roundUpToEven(num uint64, chunkSize uint64) uint64 {
	return num + (chunkSize - (num % chunkSize))
}

type primaryKeyRange struct {
	min   uint32
	max   uint32
	count uint32
}

func getPrimaryKeyRange(conn *pgx.Conn, table string, primaryKey string) (primaryKeyRange, error) {
	q := `--sql
		select min(%[1]s), max(%[1]s), count(*)
		from %[2]s
	;`
	row := conn.QueryRow(context.Background(), fmt.Sprintf(q, primaryKey, table))
	var result primaryKeyRange
	err := row.Scan(&result.min, &result.max, &result.count)
	if err != nil {
		return result, fmt.Errorf("failed to load primary key range - primary key not numeric? (%v@%v): %v", primaryKey, table, err)
	}

	return result, nil
}
