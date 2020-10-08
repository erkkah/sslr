[![Go Report Card](https://goreportcard.com/badge/github.com/erkkah/sslr)](https://goreportcard.com/report/github.com/erkkah/sslr)
[![Generic badge](https://img.shields.io/badge/Hack-Yep!-gold.svg)](https://shields.io/)


# SSLR - Simple Stupid Logical Replication

**A simple, stupid logical replication tool for PostgreSQL databases.**

With so many existing solutions for replicating PostgreSQL databases, why did I create SSLR?

Well, maybe you are in a situation like mine:

- you need to keep a read replica updated, but don't have the possibility to set up a _real_ replica
- you want to replicate a subset of tables from a database
- you want to filter which rows to replicate
- you want a minimal-config solution for getting a usable copy of your production data
- you want to replicate without causing unnecessary load on the source database
- you have somewhat loose referential consistency requirements

SSLR is a small tool that provides throttled, filtered replication using a regular read-only connection. No PostgreSQL server-side configuration needed.

## Getting started

Download the tool, and create a file called `sslr.json` containing your replication config.

A minimal config looks like this:

```yaml
{
    "source": "postgres://postgres:super-secret@localhost:2345/test?default_transaction_read_only=true",
    "target": "postgres://postgres:super-secret@localhost:2345/sslr",

    "tables": [
        "timestamps"
    ],
}
```

Then just run the tool:

```console
$ sslr
[INFO] 2020/10/08 14:31:43 Starting job with throttle at 75.00%
[INFO] 2020/10/08 14:31:43 Changes are synced in chunks of 10000
[INFO] 2020/10/08 14:31:43 Deletions are synced in chunks of 50000
[INFO] 2020/10/08 14:31:43 Connecting
[INFO] 2020/10/08 14:31:43 Validating tables
[INFO] 2020/10/08 14:31:43 Updating tables
[INFO] 2020/10/08 14:31:43 Fetching update range for table timestamps
[INFO] 2020/10/08 14:31:43 Syncing deletions for table timestamps
[INFO] 2020/10/08 14:31:43 Done
[INFO] 2020/10/08 14:31:43 0 row(s) updated in 160.154866ms
```

## Replication options

SSLR is meant to be run regularly to poll the source database and update the target database. All operations are performed in chunks, and can be throttled to limit the load of the source database.

In many cases, the basic configuration described above works just fine. Depending on the source data structure, table size and optional filtering, you might need to tweak a couple of options to increase performance.

### Chunking

Updates are fetched in chunks of `updateChunkSize` rows. Each source transaction, represented by the source row `xmin` value, has to be synced as a whole before being committed. The row-count of a transaction can be higher than the chunk size, it will just take more chunks to sync the whole transaction.

Deletes are scanned for using the initial chunk size of `deleteChunkSize`. If a chunk is found to contain changes, it is repeatedly split in half to track down the `minDeleteChunkSize` sized chunks containing the actual changes.

### Throttling

The throttle level is set as the maximum allowed time spent in the source database as a percentage of total execution time.

It's pretty crude, but works well enough to make SSLR a nice database citizen.

If you can fully load the source database while running a sync job, set the throttle percentage value to 100 for unthrottled operation.

### Job splitting

A replication job runs through all tables one at a time. Replicating many large tables will lead to long complete sync cycles. In those cases, it might make sense to split the sync job into several different SSLR configurations.

As long as the same table is not synced by more than one job, jobs can run in parallel.

Since scanning for deleted rows can be slow for large tables, you can split your replication jobs into update-only and delete-only jobs and run several updates before running deletes. Or - possibly running delete jobs only once per week, depending on your needs.

### Logging

To get more feedback while tweaking options, use the `LOG_LEVEL` environment variable to set log level to `debug`.

### Documented full configuration example

```yaml
{
    "/* Connection URLS ":"*/",
    "source": "postgres://postgres:super-secret@localhost:2345/test?default_transaction_read_only=true",
    "target": "postgres://postgres:super-secret@localhost:2345/sslr",

    "/* List of tables to sync ":"*/",
    "tables": [
        "timestamps"
    ],

    "/* Everything below is optional ":"*/",

    "/* Filtered tables, with 'where' clause ":"*/",
    "filteredTables": {
        "people": {
            "where": "name like 'bob%'"
        }
    },

    "/* New or updated rows will be applied using this chunk size ":"*/",
    "updateChunkSize": 10000,

    "/* Source table will be scanned for deleted rows starting at this chunk size ":"*/",
    "deleteChunkSize": 50000,

    "/* Chunk size for when to stop divide and conquer while scanning for deleted rows ":"*/",
    "minDeleteChunkSize": 250,

    "/* Max source database utilization as a percentage of total execution time ":"*/",
    "throttlePercentage": 75,

    "/* Full table copy will be performed if the target table has less than (fullCopyThreshold * source_rows) rows ":"*/",
    "fullCopyThreshold": 0.5,

    "/* Sync added and updated rows ":"*/",
    "syncUpdates": true,

    "/* Sync deleted rows ":"*/",
    "syncDeletes": true,

    "/* Perform full table copy instead of exiting when schema changes are detected ":"*/",
    "resyncOnSchemaChange": false,

    "/* Name of SSLR state table in the target database":"*/",
    "stateTable": "__sslr_state"
}
```

## How does it work?

SSLR is based on per-table `xmin` tracking and primary key hashing. Source table updates are discovered by tracking last seen `xmin`. Source table deletes are discovered by comparing hashes on ranges of primary keys in a divide and conquer fashion.

The current state (highest synced `xmin` per table) is stored in the target database.

### Algorithm

For each table:

- Create destination

  - Extract create statements from source table
  - Create target table if needed using extracted statements
  - If target exists, compare current structure

    - Resync full table or abort if the structure has changed

- Update

  - Get highest seen target `xmin` from state table
  - Get current highest source `xmin`
  - Pull needed changes and update in chunks

- Delete

  - Generate list of hashes for primary keys in chunks
  - Compare hashes between source and target
  - Where they differ, split the list and repeat until minimum list size is found
  - Update the resulting ranges of ids in transactions:

    - Delete all in-range rows in target
    - Re-add all in-range rows from source to target

## Known issues

- Since replication is done table by table, there are moments of referential inconsistency in the target database
- As the target is meant for reading only, no triggers, constraints, et.c. except for the primary key are copied to the target
- Deletions for tables with non-numerical primary key are not handled
- Multi-column primary keys are not supported
- `xmin` wrapping is not handled
- Full table copying is not throttled
