package sslr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/erkkah/letarette/pkg/logger"
	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"
)

// Job is my friend
type Job struct {
	cfg         Config
	primaryKeys map[string][]string
	columns     map[string][]string
	forceSync   map[string]bool
	source      *pgx.Conn
	target      *pgx.Conn
	start       time.Time
	updatedRows uint32
}

// NewJob creates a new job
func NewJob(config Config) (*Job, error) {
	job := Job{
		cfg:         config,
		primaryKeys: make(map[string][]string),
		columns:     make(map[string][]string),
		forceSync:   make(map[string]bool),
	}
	return &job, nil
}

// Run runs a single SSLR sync job
func (job *Job) Run() error {
	logger.Info.Printf("Starting job with throttle at %.2f%%", job.cfg.ThrottlePercentage)
	logger.Info.Printf("Changes are synced in chunks of %v", job.cfg.UpdateChunkSize)
	logger.Info.Printf("Deletions are synced in chunks of %v", job.cfg.DeleteChunkSize)
	job.start = time.Now()

	logger.Info.Printf("Connecting")
	err := job.connect()
	if err != nil {
		return err
	}

	logger.Info.Printf("Validating tables")
	err = job.validateTables()
	if err != nil {
		return err
	}

	logger.Info.Printf("Updating tables")
	err = job.updateTables()
	if err != nil {
		return err
	}

	logger.Info.Printf("Done")
	logger.Info.Printf("%v row(s) updated in %v", job.updatedRows, time.Since(job.start))
	return nil
}

func (job *Job) connect() error {
	pq.EnableInfinityTs(time.Unix(0, 0), time.Unix(math.MaxInt32*100, 0))

	var err error
	job.source, err = pgx.Connect(context.Background(), job.cfg.SourceConnection)
	if err != nil {
		return err
	}
	job.target, err = pgx.Connect(context.Background(), job.cfg.TargetConnection)
	if err != nil {
		return err
	}
	return nil
}

var errSchemaMismatch = errors.New("schema mismatch")

func (job *Job) validateTable(table string) error {
	schema, err := extractTableSchema(job.source, table)
	if err != nil {
		return err
	}

	targetExists, err := objectExists(job.target, table)
	if err != nil {
		return err
	}
	if targetExists {
		targetSchema, err := extractTableSchema(job.target, table)
		if err != nil {
			return err
		}
		if targetSchema != schema {
			logger.Debug.Printf("Schemas differ:\nsource: %s\ntarget: %s", schema, targetSchema)
			return errSchemaMismatch
		}
	} else {
		err = createTable(job.target, table, schema)

		if err != nil {
			return fmt.Errorf("failed to create target table: %w", err)
		}
	}

	indices, err := extractTableIndices(job.source, table)
	if err != nil {
		return err
	}

	err = applyIndices(job.target, table, indices)
	if err != nil {
		return fmt.Errorf("failed to create indices: %w", err)
	}

	for _, index := range indices {
		if index.primary {
			job.primaryKeys[table] = index.columns
		}
	}

	return nil
}

func (job *Job) validateTables() error {
	for _, table := range job.cfg.SourceTables {
		err := job.validateTable(table)
		if err == errSchemaMismatch && job.cfg.ResyncOnSchemaChange {
			logger.Info.Printf("Schema for table %q has changed, marking for re-synk", table)
			job.forceSync[table] = true
			continue
		}
		if err != nil {
			return err
		}
	}

	for table := range job.cfg.FilteredSourceTables {
		err := job.validateTable(table)
		if err == errSchemaMismatch && job.cfg.ResyncOnSchemaChange {
			logger.Info.Printf("Schema for table %q has changed, marking for re-synk", table)
			job.forceSync[table] = true
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (job *Job) getPrimaryKey(table string) (string, error) {
	primaryKeys := job.primaryKeys[table]
	if len(primaryKeys) != 1 {
		return "", fmt.Errorf("table %q must have exactly one primary key column, found %v", table, len(primaryKeys))
	}
	return primaryKeys[0], nil
}

func (job *Job) updateTables() error {

	for _, table := range job.cfg.SourceTables {
		err := job.updateTable(table, "")
		if err != nil {
			return err
		}
	}

	for table, filter := range job.cfg.FilteredSourceTables {
		err := job.updateTable(table, filter.Where)
		if err != nil {
			return err
		}
	}

	return nil
}

func (job *Job) updateTable(table string, where string) error {
	primaryKey, err := job.getPrimaryKey(table)
	if err != nil {
		return err
	}

	var updateRange updateRange

	if job.cfg.SyncUpdates {
		logger.Info.Printf("Fetching update range for table %s", table)
		updateRange, err = job.getUpdateRange(table, where)
		if err != nil {
			return fmt.Errorf("failed to get update range: %w", err)
		}

		if updateRange.fullTable {
			logger.Info.Printf("Performing full table sync for stale / empty table")
			err = job.copyFullTable(table, where)
			if err != nil {
				return err
			}
			err = job.setTableState(table, tableState{
				lastSeenXmin: updateRange.endXmin,
			})
			if err != nil {
				return err
			}
			return nil
		}

		if !updateRange.empty() {
			logger.Info.Printf("Updating table %s", table)
			err = job.updateTableRange(table, primaryKey, updateRange, where)
			if err != nil {
				return err
			}
		}
	}

	if job.cfg.SyncDeletes {
		logger.Info.Printf("Syncing deletions for table %s", table)
		err = job.syncDeletedRows(table, where)
		if err != nil {
			logger.Error.Printf("Failed to sync deletions for table %s: %v", table, err)
		}
	}

	return nil
}

func (job *Job) copyFullTable(table string, where string) error {
	ctx := context.Background()

	var whereClause string
	if len(where) > 0 {
		whereClause = "where " + where
	}
	q := fmt.Sprintf("select * from %s %s", table, whereClause)
	rows, err := job.source.Query(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()

	var columnNames []string
	columns := rows.FieldDescriptions()
	for _, column := range columns {
		columnNames = append(columnNames, string(column.Name))
	}

	tx, err := job.target.Begin(ctx)
	if err != nil {
		return err
	}

	defer func() {
		if tx != nil {
			tx.Rollback(ctx)
		}
	}()

	_, err = tx.Exec(ctx, fmt.Sprintf("delete from %s", table))
	if err != nil {
		return fmt.Errorf("failed to delete old data: %w", err)
	}

	logger.Info.Printf("Running streaming copy")
	identifier := strings.Split(table, ".")
	updatedRows, err := tx.CopyFrom(ctx, identifier, columnNames, newReportingSource(rows))
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
		if r.rowsRead%10000 == 0 {
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
