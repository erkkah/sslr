package sslr

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/erkkah/letarette/pkg/logger"
	"github.com/jackc/pgx/v4"
)

func (job *Job) syncDeletedRows(table string, where string) error {
	primaryKeys, err := job.getPrimaryKeys(table)
	if err != nil {
		return err
	}

	keyRange, err := getPrimaryKeyRange(job.source, table, primaryKeys, where)
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
		endKey, err := job.syncDeletedRowRange(table, primaryKeys, startKey, chunkSize, where)
		if err != nil {
			return err
		}

		if endKey.Equals(startKey) {
			break
		}
		startKey = endKey
		throttle.end()
		throttle.wait()
	}

	return nil
}

func (job *Job) syncDeletedRowRange(table string, primaryKeys []string, startKey PrimaryKeySet, chunkSize uint32, where string) (endKey PrimaryKeySet, err error) {
	endKey, err = getKeyAtOffset(job.source, table, primaryKeys, startKey, chunkSize, where)
	if err != nil {
		err = fmt.Errorf("failed to get key at offset: %w", err)
		return
	}
	sourceHash, err := getKeyHash(job.source, table, primaryKeys, startKey, endKey, where)
	if err != nil {
		err = fmt.Errorf("failed to get source key hash: %w", err)
		return
	}
	targetHash, err := getKeyHash(job.target, table, primaryKeys, startKey, endKey, where)
	if err != nil {
		err = fmt.Errorf("failed to get target key hash: %w", err)
		return
	}
	logger.Debug.Printf("Start key: %v, end key: %v, chunk size: %v", startKey, endKey, chunkSize)
	logger.Debug.Printf("Source hash: %s, target hash: %s", sourceHash, targetHash)
	if sourceHash != targetHash {
		if chunkSize <= job.cfg.MinDeleteChunkSize {
			logger.Debug.Printf("Updating (%v - %v)", startKey, endKey)
			err = job.updateChangedRange(table, primaryKeys, startKey, endKey, where)
			if err != nil {
				err = fmt.Errorf("failed to update changed range: %w", err)
				return
			}
		} else {
			nextChunkSize := chunkSize / 2
			var midKey PrimaryKeySet
			midKey, err = job.syncDeletedRowRange(table, primaryKeys, startKey, nextChunkSize, where)
			if err != nil {
				return
			}
			_, err = job.syncDeletedRowRange(table, primaryKeys, midKey, nextChunkSize, where)
			if err != nil {
				return
			}
		}
	}
	return endKey, nil
}

func getKeyAtOffset(conn *pgx.Conn, table string, primaryKeys []string, startKey PrimaryKeySet, offset uint32, where string) (PrimaryKeySet, error) {
	var result PrimaryKeySet

	if len(primaryKeys) != len(startKey) {
		return result, errors.New("Key length mismatch")
	}

	var extraWhereClause string
	if len(where) > 0 {
		extraWhereClause = "and " + where
	}

	keyList := strings.Join(primaryKeys, ",")

	var filtering []string
	var queryParameters = []interface{}{offset}
	for i, keyValue := range startKey {
		// "2+i" since query parameters after "offset" start at 2
		filtering = append(filtering, fmt.Sprintf("%s >= $%d", primaryKeys[i], 2+i))
		queryParameters = append(queryParameters, keyValue.value)
	}
	whereClause := strings.Join(filtering, " and ")

	var minSorting []string
	var maxSorting []string
	for _, key := range primaryKeys {
		minSorting = append(minSorting, fmt.Sprintf("%s asc", key))
		maxSorting = append(maxSorting, fmt.Sprintf("%s desc", key))
	}
	minOrderClause := strings.Join(minSorting, ",")
	maxOrderClause := strings.Join(maxSorting, ",")

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
			%[3]s
			%[4]s
		order by %[5]s
		limit $1
	) ids
	order by
		%[6]s
	limit 1
	;`, table, keyList, whereClause, extraWhereClause, minOrderClause, maxOrderClause)

	ctx := context.Background()
	rows, err := conn.Query(ctx, q, queryParameters...)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	if rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return result, err
		}

		result = make(PrimaryKeySet, len(primaryKeys))
		for i := range result {
			result[i].value = values[i]
		}
	} else {
		return result, errors.New("Unexpected empty resultset")
	}

	return result, nil
}

func (job *Job) updateChangedRange(table string, primaryKeys []string, startKey PrimaryKeySet, endKey PrimaryKeySet, where string) error {
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

	var extraWhereClause string
	if len(where) > 0 {
		extraWhereClause = "and " + where
	}

	whereClause, queryParameters := whereClauseFromKeyRange(primaryKeys, startKey, endKey)

	baseQuery := fmt.Sprintf(`
	--sql
	from
		%[1]s
	where
		%[2]s
		%[3]s
	;`, table, whereClause, extraWhereClause)

	q := "select * " + baseQuery

	rows, err := job.source.Query(ctx, q, queryParameters...)
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

	d := "delete " + baseQuery

	_, err = tx.Exec(ctx, d, queryParameters...)
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

func getKeyHash(conn *pgx.Conn, table string, primaryKeys []string, startKey PrimaryKeySet, endKey PrimaryKeySet, where string) (string, error) {
	var extraWhereClause string
	if len(where) > 0 {
		extraWhereClause = "and " + where
	}

	keyList := strings.Join(primaryKeys, ",")

	whereClause, queryParameters := whereClauseFromKeyRange(primaryKeys, startKey, endKey)

	q := fmt.Sprintf(`--sql 
	select
		coalesce(md5(array_agg(id)::varchar), '') as hash
	from (
		select
			(%[1]s)::varchar as id
		from
			%[2]s
		where
			%[3]s
			%[4]s
		order by
			%[1]s
	) as t
	;`, keyList, table, whereClause, extraWhereClause)
	row := conn.QueryRow(context.Background(), q, queryParameters...)
	var hash string
	err := row.Scan(&hash)
	if err != nil {
		return "", err
	}
	return hash, nil
}

func getPrimaryKeyRange(conn *pgx.Conn, table string, primaryKeys []string, where string) (primaryKeyRange, error) {
	var whereClause string
	if len(where) > 0 {
		whereClause = "where " + where
	}

	var minSorting []string

	for _, key := range primaryKeys {
		minSorting = append(minSorting, fmt.Sprintf("%s asc", key))
	}

	minOrderClause := strings.Join(minSorting, ",")
	keyList := strings.Join(primaryKeys, ",")
	var result primaryKeyRange

	q := fmt.Sprintf(`--sql
		select
			%[2]s, (select count(*) from %[1]s) as cnt
		from
			%[1]s
		%[3]s
		order by
		%[4]s
		limit 1
	;`, table, keyList, whereClause, minOrderClause)

	rows, err := conn.Query(context.Background(), q)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	if !rows.Next() {
		return result, errors.New("Unexpected empty resultset")
	}

	values, err := rows.Values()
	if err != nil {
		return result, fmt.Errorf("failed to load primary key range (%v@%v): %v", primaryKeys, table, err)
	}

	result.min = make(PrimaryKeySet, len(primaryKeys))
	for i := range primaryKeys {
		result.min[i].value = values[i]
	}
	result.count, err = integerValue(values[len(primaryKeys)])
	if err != nil {
		return result, fmt.Errorf("failed to convert count to int: %w", err)
	}

	return result, nil
}

func integerValue(unknown interface{}) (result uint32, err error) {
	str := fmt.Sprintf("%v", unknown)
	_, err = fmt.Sscanf(str, "%d", &result)
	return
}

// whereClauseFromKeyRange creates a string with "where" filters, and corresponding list of query parameters
// from a set of primary key names and their limit values.
//
// The returned filter represents the closed interval [startKey, endKey].
// We use a closed interval and accept overlapping endpoints, since we cannot easily increment
// multi-column string-valued keys.
func whereClauseFromKeyRange(primaryKeys []string, startKey, endKey PrimaryKeySet) (string, []interface{}) {
	var startFiltering []string
	var queryParameters []interface{}
	for i, keyValue := range startKey {
		startFiltering = append(startFiltering, fmt.Sprintf("%s >= $%d", primaryKeys[i], i+1))
		queryParameters = append(queryParameters, keyValue.value)
	}

	var endFiltering []string

	parameterOffset := 1 + len(queryParameters)
	for i, keyValue := range endKey {
		endFiltering = append(endFiltering, fmt.Sprintf("%s <= $%d", primaryKeys[i], i+parameterOffset))
		queryParameters = append(queryParameters, keyValue.value)
	}
	whereClause := strings.Join(startFiltering, " and ")
	whereClause += " and "
	whereClause += strings.Join(endFiltering, " and ")

	return whereClause, queryParameters
}
