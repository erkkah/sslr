package sslr

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/erkkah/letarette/pkg/logger"
	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"
)

// Job represents an active sync job
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

// NewJob creates a new job from a config
func NewJob(config Config) (*Job, error) {
	job := Job{
		cfg:         config,
		primaryKeys: make(map[string][]string),
		columns:     make(map[string][]string),
		forceSync:   make(map[string]bool),
	}
	return &job, nil
}

// Run performs a full sync operation according to the job's configuration
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

func (job *Job) getPrimaryKeys(table string) ([]string, error) {
	primaryKeys := job.primaryKeys[table]
	if len(primaryKeys) < 1 {
		return primaryKeys, fmt.Errorf("table %v does not have a primary key", table)
	}
	sort.StringSlice(primaryKeys).Sort()
	return primaryKeys, nil
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
	primaryKeys, err := job.getPrimaryKeys(table)
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
			err = job.updateTableRange(table, primaryKeys, updateRange, where)
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
