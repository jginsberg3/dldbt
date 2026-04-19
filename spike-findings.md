# Phase 0 Spike Findings

**Verdict: core assumption validated.** A DuckLake schema can be shallow-copied
by inserting duplicate `ducklake_data_file` rows that point at the same physical
parquet files as another schema. Every Phase 0 invariant held.

Stack: DuckDB 1.5.2 with `ducklake` + `httpfs` extensions, Postgres 16 catalog,
MinIO 2025 S3-compatible storage.

## Invariants

| ID | Status | What we checked |
|----|--------|-----------------|
| I1 | pass | `main_schema.events` queryable after insert |
| I2 | pass | Catalog rows visible via psycopg (1 `ducklake_data_file` row) |
| I3 | pass | `feature_x.events` returns identical data to `main_schema.events` after direct catalog INSERTs |
| I4 | pass | Dropping `feature_x` leaves `main_schema.events` intact |
| I5 | pass | Writing a new table to `feature_x` creates new parquet files |
| I6 | pass | `main_schema` cannot see `feature_x.extra` |
| I7 | pass | Parquet files shared with `main_schema` survive the `feature_x` drop |

## What the shallow copy actually looks like

Against an existing `main_schema.events` (table_id = 2) with one parquet data
file, the copy is a transactional direct-INSERT into the catalog DB:

1. Create an empty `feature_x.events` through normal DDL (lets DuckLake allocate
   the schema_id, table_id, column rows, etc.).
2. Read the `ducklake_data_file` rows for `main_schema.events` (table_id = 2).
3. Re-insert each row with:
   - a fresh `data_file_id`,
   - `table_id` = feature_x's events table (id = 4),
   - `path` rewritten to an absolute `s3://...` URL,
   - `path_is_relative` set to `false`.

After that, re-attaching DuckDB is enough; the ducklake extension reads the
updated catalog and treats feature_x.events as holding the same 3 rows.

## Gotchas uncovered (important for Phase 1+)

### 1. Inlined data is the default for small inserts

DuckLake writes small inserts straight into catalog-side `ducklake_inlined_data_<tid>_<sv>`
tables, leaving `ducklake_data_file` empty. A naive "duplicate the data_file
rows" branch operation would silently copy nothing.

Two ways to force parquet writes:
- `ATTACH 'ducklake:postgres:…' AS lake (DATA_PATH '…', DATA_INLINING_ROW_LIMIT 0)`
  (what this spike uses).
- Insert more rows than the inlining row limit.

**Phase 1 implication:** `dldbt` must either (a) configure the lake to disable
inlining globally, or (b) branch both parquet data_files *and* inlined_data rows.
The simpler path is to set `DATA_INLINING_ROW_LIMIT 0` in our managed ATTACH so
all data lives in parquet — that also matches the "content-addressable Parquet"
design goal.

### 2. `ducklake_data_file.path` is resolved relative to the *owning* table's path

Paths are stored as `ducklake-<ulid>.parquet` with `path_is_relative = true`.
DuckLake resolves the absolute URL as `data_path + schema.path + table.path + path`.
Blindly copying the path column under a new `table_id` makes DuckLake look for
the file under *the new table's prefix*, which doesn't exist (404 from MinIO).

**Phase 1 implication:** the shallow-copy routine must resolve the original
absolute path and store it on the duplicated row with `path_is_relative = false`,
**or** insert a pointer that preserves the owning-schema prefix. Absolute paths
are simpler; the storage layer already tolerates them.

### 3. `DROP SCHEMA feature_x CASCADE` does not hard-delete parquet

I7 held: after CASCADE, all parquet files shared with `main_schema` stayed on S3
(and `main_schema.events` remained readable). DuckLake relies on a separate
deletion mechanism (`ducklake_files_scheduled_for_deletion` + vacuum) rather
than immediate unlink. Good news for cheap branch drops. GC (Phase 7) must
still be careful to check reference counts across all active branches before
scheduling deletions.

### 4. Concrete DuckLake catalog shape (as of DuckDB 1.5.2)

Tables we actually touched and their relevant columns:

- `ducklake_schema(schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative)`
- `ducklake_table(table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative)`
- `ducklake_data_file(data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, mapping_id, partial_max)`
- `ducklake_snapshot(snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)`
- `ducklake_snapshot_changes(snapshot_id, changes_made, author, commit_message, commit_extra_info)`
- `ducklake_metadata(key, value, scope, scope_id)` — `data_path` lives here
- `ducklake_inlined_data_tables(table_id, table_name, schema_version)` — catalog-side inlined-data table registry
- `ducklake_inlined_data_<tid>_<sv>(row_id, begin_snapshot, end_snapshot, …columns…)` — actual inlined rows

The `CatalogAdapter` interface in the implementation plan should lean on these
names. The column set is stable enough to write migrations against, but the
`ducklake_inlined_data_*` table names are dynamic and must be discovered at
runtime.

### 5. `begin_snapshot` / `end_snapshot` on data_file rows must be respected

The duplicated rows inherit `begin_snapshot` from the original (valid because
that snapshot is ≤ the current one). For merge (Phase 6), the plan should
explicitly pick `begin_snapshot = <current snapshot at time of merge>` so that
time-travel queries against older snapshots still see only main's pre-merge
state.

## Answers to the open questions from the plan

> Q1. Does DuckLake enforce any schema-level ownership of data files that would block shallow copies?

No. Data files are referenced by `table_id` only; nothing prevents multiple
table rows referring to the same physical parquet.

> Q2. How does `ducklake_add_data_files` behave across schemas? Can we reuse it for merge?

Not tested in this spike — we went straight to `INSERT INTO ducklake_data_file`.
Worth exploring in Phase 1: it might give us a higher-level API that also updates
snapshot/stat bookkeeping for free.

> Q3. What's the granularity of snapshot_id — per-catalog, per-schema, or per-table?

Per-catalog (per-attached-ducklake). Every DDL/DML bumps the global snapshot_id
and rolls it into `ducklake_snapshot_changes` with a single string describing
the change (e.g. `created_schema:"main_schema"`, `created_table:"feature_x"."events"`,
`inlined_insert:2`). This is strictly more coarse-grained than the plan's
`branch_state(ducklake_snapshot)` column implies; tracking a single snapshot_id
per branch is correct.

## Fallback plan (not needed)

Phase 0 also had a fallback — branch at the DuckLake-attach level instead of
schema-level. The direct-INSERT approach works, so we stay with schema-as-branch
for now. If DuckLake changes the catalog in a way that breaks this (e.g. enforces
a unique constraint on `(path, some_owner_id)`), we can migrate to the fallback
without reworking `dldbt`'s higher layers because the `CatalogAdapter` interface
hides it.

## How to reproduce

```bash
docker compose up -d
uv run python spike.py
```

The spike is idempotent — `reset_catalog()` drops all `ducklake_*` tables
before each run. Teardown is `docker compose down -v`.
