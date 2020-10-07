package sslr

import (
	"context"
	"fmt"
	"math"
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

func (job *Job) validateTables() error {
	for _, table := range job.cfg.SourceTables {
		schema, err := extractTableSchema(job.source, table)
		if err != nil {
			return err
		}

		logger.Debug.Printf("%s\n", schema)

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
				return fmt.Errorf("schema mismatch, table=%q", table)
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
		logger.Debug.Printf("%v\n", indices)

		err = applyIndices(job.target, table, indices)
		if err != nil {
			return fmt.Errorf("failed to create indices: %w", err)
		}

		for _, index := range indices {
			if index.primary {
				job.primaryKeys[table] = index.columns
			}
		}
	}
	return nil
}

func (job *Job) getPrimaryKey(table string) (string, error) {
	primaryKeys := job.primaryKeys[table]
	if len(primaryKeys) != 1 {
		return "", fmt.Errorf("table must have exactly one primary key column, found %v", len(primaryKeys))
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
	updateRange, err := job.getUpdateRange(table, where)
	if err != nil {
		return fmt.Errorf("failed to get update range: %w", err)
	}
	if !updateRange.empty() {
		logger.Info.Printf("Updating table %s", table)
		err = job.updateTableRange(table, primaryKey, updateRange, where)
		if err != nil {
			return err
		}
	}

	logger.Info.Printf("Syncing deletions for table %s", table)
	err = job.syncDeletedRows(table, where)
	if err != nil {
		logger.Error.Printf("Failed to sync deletions for table %s: %v", table, err)
	}

	return nil
}
