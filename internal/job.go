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
	}
	return &job, nil
}

// Run runs a single SSLR sync job
func (job *Job) Run() error {
	logger.Info.Printf("Starting job with throttle at %.2f%%", job.cfg.ThrottlePercentage)
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
	logger.Info.Printf("%v rows updated in %v", job.updatedRows, time.Since(job.start))
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

func (job *Job) updateTables() error {
	for _, table := range job.cfg.SourceTables {
		updateRange, err := job.getUpdateRange(table)
		if err != nil {
			return fmt.Errorf("failed to get update range: %w", err)
		}
		if updateRange.empty() {
			continue
		}
		logger.Info.Printf("Updating table %s", table)
		err = job.updateTable(table, updateRange)
		if err != nil {
			return err
		}
	}

	return nil
}
