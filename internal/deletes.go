package sslr

import (
	"context"
	"fmt"
	"strings"

	"github.com/erkkah/letarette/pkg/logger"
	"github.com/jackc/pgx/v4"
)

func (job *Job) syncDeletedRows(table string, where string) error {
	primaryKey, err := job.getPrimaryKey(table)
	if err != nil {
		return err
	}

	keyRange, err := getPrimaryKeyRange(job.source, table, primaryKey, where)
	if err != nil {
		return fmt.Errorf("failed to get primary key range: %w", err)
	}

	chunkSize := job.cfg.DeleteChunkSize
	if keyRange.count < chunkSize {
		chunkSize = keyRange.count
	}
	startKey := keyRange.min
	throttle := newThrottle("deletes", job.cfg.ThrottlePercentage)

	for {
		throttle.start()
		endKey, err := job.syncDeletedRowRange(table, primaryKey, startKey, chunkSize, where)
		if err != nil {
			return err
		}
		if endKey == startKey {
			break
		}
		startKey = endKey
		throttle.end()
		throttle.wait()
	}

	return nil
}

func (job *Job) syncDeletedRowRange(table string, primaryKey string, startKey PrimaryKey, chunkSize uint32, where string) (endKey PrimaryKey, err error) {
	endKey, err = getKeyAtOffset(job.source, table, primaryKey, startKey, chunkSize, where)
	if err != nil {
		return
	}
	sourceHash, err := getKeyHash(job.source, table, primaryKey, startKey, endKey, where)
	if err != nil {
		return
	}
	targetHash, err := getKeyHash(job.target, table, primaryKey, startKey, endKey, where)
	if err != nil {
		return
	}
	logger.Debug.Printf("Start key: %v, end key: %v, chunk size: %v", startKey, endKey, chunkSize)
	logger.Debug.Printf("Source hash: %s, target hash: %s", sourceHash, targetHash)
	if sourceHash != targetHash {
		if chunkSize <= job.cfg.MinDeleteChunkSize {
			logger.Debug.Printf("Updating (%v - %v)", startKey, endKey)
			err = job.updateChangedRange(table, primaryKey, startKey, endKey, where)
			if err != nil {
				return
			}
		} else {
			nextChunkSize := chunkSize / 2
			var midKey PrimaryKey
			midKey, err = job.syncDeletedRowRange(table, primaryKey, startKey, nextChunkSize, where)
			if err != nil {
				return
			}
			_, err = job.syncDeletedRowRange(table, primaryKey, midKey, nextChunkSize, where)
			if err != nil {
				return
			}
		}
	}
	return endKey, nil
}

func getKeyAtOffset(conn *pgx.Conn, table string, primaryKey string, startKey PrimaryKey, offset uint32, where string) (PrimaryKey, error) {
	var whereClause string
	if len(where) > 0 {
		whereClause = "and " + where
	}
	q := fmt.Sprintf(`--sql
	select
		%[2]s
	from
	(
		select
			%[2]s
		from
			%[1]s
		where
			%[2]s >= $1
			%[3]s
		order by %[2]s
		limit $2
	) ids
	order by
		%[2]s desc
	limit 1
	;`, table, primaryKey, whereClause)

	ctx := context.Background()
	row := conn.QueryRow(ctx, q, startKey.value, offset)
	var key PrimaryKey
	err := row.Scan(&key)
	return key, err
}

func (job *Job) updateChangedRange(table string, primaryKey string, startKey PrimaryKey, endKey PrimaryKey, where string) error {
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

	var whereClause string
	if len(where) > 0 {
		whereClause = "and " + where
	}

	q := fmt.Sprintf(`--sql
	select
		*
	from
		%[1]s
	where
		%[2]s >= $1
		and
		%[2]s <= $2
		%[3]s
	;`, table, primaryKey, whereClause)

	rows, err := job.source.Query(ctx, q, startKey.value, endKey.value)
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
	for _, column := range columns {
		columnNames = append(columnNames, string(column.Name))
	}

	d := fmt.Sprintf(`--sql 
	delete from %[1]s
	where
		%[2]s >= $1
	and
		%[2]s <= $2
	;`, table, primaryKey)

	_, err = tx.Exec(ctx, d, startKey.value, endKey.value)
	if err != nil {
		return err
	}

	identifier := strings.Split(table, ".")
	rowsRead, err := tx.CopyFrom(ctx, identifier, columnNames, rows)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		return err
	}
	job.updatedRows += uint32(rowsRead)
	tx = nil
	return nil
}

func getKeyHash(conn *pgx.Conn, table string, primaryKey string, startKey PrimaryKey, endKey PrimaryKey, where string) (string, error) {
	var whereClause string
	if len(where) > 0 {
		whereClause = "and " + where
	}
	q := `--sql 
	select
		coalesce(md5(array_agg(id)::varchar), '') as hash
	from (
		select
			%[1]s as id
		from
			%[2]s
		where
			%[1]s >= $1
			and
			%[1]s < $2
			%[3]s
		order by
			1
	) as t
	;`
	row := conn.QueryRow(context.Background(), fmt.Sprintf(q, primaryKey, table, whereClause), startKey.value, endKey.value)
	var hash string
	err := row.Scan(&hash)
	if err != nil {
		return "", err
	}
	return hash, nil
}

func getPrimaryKeyRange(conn *pgx.Conn, table string, primaryKey string, where string) (primaryKeyRange, error) {
	var whereClause string
	if len(where) > 0 {
		whereClause = "where " + where
	}
	q := `--sql
		select min(%[1]s), max(%[1]s), count(*)
		from %[2]s
		%[3]s
	;`
	row := conn.QueryRow(context.Background(), fmt.Sprintf(q, primaryKey, table, whereClause))
	var result primaryKeyRange
	err := row.Scan(&result.min, &result.max, &result.count)
	if err != nil {
		return result, fmt.Errorf("failed to load primary key range (%v@%v): %v", primaryKey, table, err)
	}

	return result, nil
}
