package sslr

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4"
)

// PrimaryKey wraps integer and string valued keys
type PrimaryKey struct {
	value interface{}
}

// PrimaryKeySet is a list of primary key values for a single row
type PrimaryKeySet []PrimaryKey

// Equals compares two keysets
func (pks PrimaryKeySet) Equals(other PrimaryKeySet) bool {
	if len(pks) != len(other) {
		return false
	}

	for i := range pks {
		if pks[i] != other[i] {
			return false
		}
	}

	return true
}

// Scan implements Scanner interface
func (pk *PrimaryKey) Scan(value interface{}) error {
	switch value.(type) {
	case int64, string:
		pk.value = value
	default:
		return fmt.Errorf("Unhandled primary key type: %v", value)
	}
	return nil
}

// Value implements Valuer interface
func (pk *PrimaryKey) Value() (driver.Value, error) {
	switch pk.value.(type) {
	case int64:
		return driver.Int32.ConvertValue(pk.value)
	case string:
		return driver.String.ConvertValue(pk.value)
	default:
		return nil, fmt.Errorf("Unhandled primary key type: %v", pk.value)
	}
}

// PrimaryKeySetSlice wraps a slice of PrimaryKeySet for easy conversion
type PrimaryKeySetSlice []PrimaryKeySet

// StringValues converts a slice of PrimaryKey slices to a slice of string slices.
// Or - converts rows of multiple primary key values into their string-valued counterparts
func (rows PrimaryKeySetSlice) StringValues() [][]string {
	var result [][]string
	for _, row := range rows {
		var keys []string
		for _, key := range row {
			keys = append(keys, fmt.Sprintf("%v", key))
		}
		result = append(result, keys)
	}
	return result
}

func (pk PrimaryKey) String() string {
	return fmt.Sprintf("%v", pk.value)
}

type primaryKeyRange struct {
	min   PrimaryKeySet
	count uint32
}

func extractTableSchema(conn *pgx.Conn, tablePath string) (string, error) {
	namespace, table := splitTablePath(tablePath)

	row := conn.QueryRow(context.Background(),
		`--sql
    select
        'create table ' || relname || '(' ||
        array_to_string(
            array_agg(
                column_name || ' ' || type || ' ' || not_null
            )
            , ','
        ) || ');'
    from
    (
        select 
                n.nspname || '.' || c.relname as relname, a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
                case 
                    when a.attnotnull
                        then 'not null'
                    else 'null'
                end
            as not_null
        from
            pg_class c,
            pg_attribute a,
            pg_type t,
            pg_catalog.pg_namespace n
        where
            c.relname = $2
            and n.nspname = $1
            and a.attnum > 0
            and a.attrelid = c.oid
            and a.atttypid = t.oid
            and n.oid = c.relnamespace
        order by a.attnum
    ) as "schema"
    group by
        relname;
    `, namespace, table)

	var schema string
	err := row.Scan(&schema)
	if err != nil {
		return "", fmt.Errorf("Failed to scan schema: %w", err)
	}
	return schema, nil
}

type tableIndex struct {
	indexName string
	primary   bool
	columns   []string
}

func extractTableIndices(conn *pgx.Conn, tablePath string) ([]tableIndex, error) {
	q := `--sql
    select
        i.relname as "indexName",
        ix.indisprimary as "primary",
        array_agg(a.attname)::text[] as "columns"
    from
        pg_class t,
        pg_class i,
        pg_index ix,
        pg_attribute a,
        pg_catalog.pg_namespace n
    where
        t.oid = ix.indrelid
        and i.oid = ix.indexrelid
        and a.attrelid = t.oid
        and a.attnum = ANY(ix.indkey)
        and t.relkind = 'r'
        and n.oid = t.relnamespace
        and n.nspname = $1
        and t.relname = $2
    group by
    	1, 2
    order by
        1, 2
    ;`

	var result []tableIndex

	namespace, table := splitTablePath(tablePath)
	rows, err := conn.Query(context.Background(), q, namespace, table)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	for rows.Next() {
		var index tableIndex
		err = rows.Scan(&index.indexName, &index.primary, &index.columns)
		if err != nil {
			return result, err
		}
		result = append(result, index)
	}

	return result, nil
}

func objectExists(conn *pgx.Conn, tablePath string) (bool, error) {
	row := conn.QueryRow(context.Background(), `select to_regclass($1) is not null`, tablePath)
	var exists bool
	err := row.Scan(&exists)
	return exists, err
}

func splitTablePath(path string) (string, string) {
	table := path
	namespace := "public"
	if strings.Contains(path, ".") {
		parts := strings.SplitN(path, ".", 2)
		namespace = parts[0]
		table = parts[1]
	}
	return namespace, table
}

func createTable(conn *pgx.Conn, table string, schema string) error {
	namespace, _ := splitTablePath(table)
	_, err := conn.Exec(context.Background(), fmt.Sprintf("create schema if not exists %s", namespace))
	if err != nil {
		return err
	}
	_, err = conn.Exec(context.Background(), schema)
	if err != nil {
		return err
	}

	return nil
}

func applyIndices(conn *pgx.Conn, table string, indices []tableIndex) error {
	for _, index := range indices {
		columns := strings.Join(index.columns, ",")
		var directive string
		if index.primary {
			directive = "unique"
		}
		q := fmt.Sprintf("create %s index concurrently if not exists %s on %s (%s)", directive, index.indexName, table, columns)
		_, err := conn.Exec(context.Background(), q)
		if err != nil {
			return fmt.Errorf("failed to create index: %w", err)
		}
	}

	return nil
}
