# SSLR - Simple Stupid Logical Replication

## What?

* SSLR is based on per-table `xmin` tracking and primary key hashing.
* Source table updates are discovered by tracking last seen xmin.
* Source table deletes are discovered by comparing hashes on ranges of primary keys in a divide and conquer fashion.
* Since replication is done table by table, there are moments of referential inconsistency in the target database.

## How?

* SSLR is run regularly to poll the source database and update the target database.
* All operations are performed in chunks, and can be throttled to limit the load of the source database.
* Updates are applied by deleting a range of rows, and then re-adding the changed range from the source, all in one transaction.

## Config

* Source database (connection url?)
* Target database (connection url?)
* Tables to sync
* Chunk sizes
* Throttle level

## Algorithm

For each table:

* Create destination, if needed
    * Extract create statements from source
    * If target exists, compare current structure
        * Warn or abort if the structure has changed
    * Create target using extracted statements
* Update
    * Get highest target xmin
    * Get current highest source xmin
    * Pull changes and update in chunks
* Delete
    * Generate list of hashes for primary keys in chunks
    * Compare hashes between source and target
    * Where they differ, divide the list until minimum list size is found
    * Update the resulting ranges of ids in transactions:
        * Delete all rows in target
        * Re-sync all rows from source to target
* The current state (xmin per table) is kept in the target database

## Known issues

The current implementation does not support:

* tracking deletions for tables with non-numerical primary key
* multi-column primary keys
* xmin wrapping

Other:

* Infinity valued timestamps in the source are mapped to silly low and high values in the target
