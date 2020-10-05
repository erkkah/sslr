# SSLR - Simple Stupid Logical Replication

## What?

* SSLR is based on per-table `xmin` tracking and primary key hashing.
* Source table updates are discovered by tracking last seen xmin.
* Source table deletes are discovered by comparing hashes on ranges of primary keys in a divide and conquer fashion.
* Since replication is done table by table, there are moments of referential inconsistency in the target database.

## How?

* The SSLR service regularly polls the source database and updates the target database
* All operations are performed in chunks, and can be throttled to limit the load of the source database.
* Updates are applied using "upserts"
* Deletes are applied by deleting a range of rows, and then re-adding the changed range from the source, all in one transaction.
* The source query can be filtered to only sync a subset of a table's contents.

## Config

The SSLR service's main configuration is the replication specification:

* Source database (connection url?)
* Target database (connection url?)
* List of
    * Source table
    * Optional
        * Destination table
        * Columns
        * Primary key
        * filter (where statement)

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
* The current state (updating / deleting / xmin, et.c.) is kept in the target database

## Notes

* txid_current cannot be used on a replica
    * `select txid_snapshot_xmin(txid_current_snapshot());`

**List indices:**

```sql
select
    t.relname as table_name,
    i.relname as index_name,
    array_agg(a.attname) as column_names
from
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a
where
    t.oid = ix.indrelid
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relkind = 'r'
group by
	1, 2
order by
	1, 2
```

**List "create table" statements:**

```sql
SELECT                                          
  'CREATE TABLE ' || relname || E'\n(\n' ||
  array_to_string(
    array_agg(
      '    ' || column_name || ' ' ||  type || ' '|| not_null
    )
    , E',\n'
  ) || E'\n);\n'
from
(
  SELECT 
    c.relname, a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
    case 
      when a.attnotnull
    then 'NOT NULL' 
    else 'NULL' 
    END as not_null 
  FROM pg_class c,
   pg_attribute a,
   pg_type t
   WHERE c.relname = 'tablename'
   AND a.attnum > 0
   AND a.attrelid = c.oid
   AND a.atttypid = t.oid
 ORDER BY a.attnum
) as tabledefinition
group by relname;
```

```sql
SELECT * FROM test
WHERE  xmin::text = (txid_current() % (2^32)::bigint)::text;
```

### Definitions

* xmin - Earliest transaction ID (txid) that is still active. All earlier transactions will either be committed and visible, or rolled back and dead.
* xmax - First as-yet-unassigned txid. All txids greater than or equal to this are not yet started as of the time of the snapshot, and thus invisible.
* xip_list - Active txids at the time of the snapshot. The list includes only those active txids between xmin and xmax; there might be active txids higher than xmax. A txid that is xmin <= txid < xmax and not in this list was already completed at the time of the snapshot, and thus either visible or dead according to its commit status. The list does not include txids of subtransactions.

https://www.postgresql.org/docs/12/functions-info.html
