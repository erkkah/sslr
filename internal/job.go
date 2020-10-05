package sslr

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
)

// Job is my friend
type Job struct {
	cfg    Config
	source *pgx.Conn
	target *pgx.Conn
}

// NewJob creates a new job
func NewJob(config Config) (*Job, error) {
	job := Job{cfg: config}
	return &job, nil
}

// Run runs a single SSLR sync job
func (job *Job) Run() error {

	err := job.connect()
	if err != nil {
		return err
	}
	err = job.validateTables()
	if err != nil {
		return err
	}
	return nil
}

func (job *Job) connect() error {
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
		fmt.Printf("%s\n", schema)

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
		fmt.Printf("%v\n", indices)

		err = applyIndices(job.target, table, indices)
		if err != nil {
			return fmt.Errorf("failed to create indices: %w", err)
		}
	}
	return nil
}
