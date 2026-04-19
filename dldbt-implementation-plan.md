# dldbt — Implementation Plan

Working codename: **dldbt** (DuckLake + git). Rename later.

A tool that makes git branches map to DuckLake schemas, dbt runs respect the current branch, and merging a branch to `main` reuses the data it already computed. Closest prior art: SQLMesh Virtual Data Environments + lakeFS branching, but co-designed with dbt-duckdb and the DuckLake catalog.

---

## Core design decisions (locked)

1. **Fingerprint-based model identity.** Each model gets a content hash = f(rendered SQL, upstream fingerprints, config that affects output). Two models with the same fingerprint share physical Parquet files. This is the foundation — every other feature depends on it.
2. **Time-travel sources at branch creation.** Sources are frozen to the DuckLake snapshot at the moment the branch was created. `dldbt sync` pulls the latest main state (updated sources + any newly-materialized tables on main) into the feature branch.
3. **Rebase before merge.** Merging a feature branch requires it to be rebased on current main first. No 3-way Parquet merges. Ever.
4. **Single-user first, multi-user-ready.** Every catalog mutation is transactional and keyed by branch; design doesn't preclude concurrent branches, we just don't harden it yet.
5. **Sit beside DuckLake, not fight it.** When DuckLake ships native branching in v2.0, `dldbt` should migrate onto those primitives without a rewrite. Keep our abstractions shallow.

---

## Stack

- **Python 3.12+**, managed with `uv`
- **typer** for CLI, **rich** for output
- **duckdb** Python client for DuckLake operations
- **psycopg[binary]** for Postgres catalog (primary target); SQLite fallback for local dev
- **pydantic v2** for config and models
- **sqlglot** for SQL parsing in fingerprinting (you already know it well)
- **pytest** + **pytest-docker** for integration tests against a real DuckLake
- **ruff** for lint/format, **mypy** for typing

No Go in phase 1. You could rewrite the CLI in Go later if perf matters, but Python's integration with dbt-duckdb (same process, shared DuckDB connection) is too useful to give up early.

---

## Repository layout

```
dldbt/
├── pyproject.toml
├── README.md
├── src/dldbt/
│   ├── __init__.py
│   ├── cli.py                 # typer entry point
│   ├── config.py              # pydantic config models
│   ├── catalog/               # DuckLake catalog ops
│   │   ├── connection.py      # catalog DB connection mgmt
│   │   ├── schema_ops.py      # create/drop/copy branch schemas
│   │   ├── snapshots.py       # snapshot id ↔ branch tracking
│   │   └── metadata.py        # low-level catalog queries
│   ├── fingerprint/
│   │   ├── hasher.py          # compute model fingerprints
│   │   ├── dag.py             # walk dbt DAG, propagate hashes
│   │   └── store.py           # persist fingerprints in catalog
│   ├── git_ops/               # "git" is a reserved namespace in Python
│   │   ├── hooks.py           # install/run hooks
│   │   ├── branch.py          # branch name resolution + sanitization
│   │   └── state.py           # git commit ↔ branch tracking
│   ├── dbt_ops/
│   │   ├── manifest.py        # read/write dbt manifests
│   │   ├── macros/            # jinja macros we inject
│   │   │   └── generate_schema_name.sql
│   │   ├── defer.py           # build --state args
│   │   └── runner.py          # wrap dbt commands
│   ├── merge/
│   │   ├── rebase.py          # pull main into feature
│   │   ├── promote.py         # feature → main
│   │   └── diff.py            # fingerprint diffs
│   └── gc.py                  # orphan parquet cleanup
├── tests/
│   ├── unit/
│   ├── integration/
│   │   ├── docker-compose.yml # postgres catalog + minio
│   │   └── fixtures/          # tiny dbt projects
│   └── conftest.py
└── docs/
    ├── architecture.md
    └── catalog-schema.md
```

---

## Catalog metadata schema

All `dldbt` bookkeeping lives in a separate schema in the catalog DB, e.g. `dldbt_meta`. DuckLake's own tables stay untouched.

```sql
-- Branches we know about
CREATE TABLE dldbt_meta.branches (
    name               TEXT PRIMARY KEY,       -- sanitized ducklake schema name
    git_branch         TEXT NOT NULL,          -- original git branch name
    created_at         TIMESTAMPTZ NOT NULL,
    created_from       TEXT NOT NULL,          -- parent branch (usually 'main')
    base_snapshot_id   BIGINT NOT NULL,        -- ducklake snapshot at creation
    last_git_commit    TEXT,                   -- most recent git commit synced
    status             TEXT NOT NULL           -- 'active' | 'merged' | 'abandoned'
);

-- Which DuckLake snapshot each branch currently points at
CREATE TABLE dldbt_meta.branch_state (
    branch             TEXT NOT NULL REFERENCES dldbt_meta.branches(name),
    ducklake_snapshot  BIGINT NOT NULL,
    git_commit         TEXT NOT NULL,
    updated_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (branch, updated_at)
);

-- Per-model fingerprints. The heart of the system.
CREATE TABLE dldbt_meta.model_fingerprints (
    branch             TEXT NOT NULL,
    model_name         TEXT NOT NULL,          -- fully-qualified dbt model id
    fingerprint        TEXT NOT NULL,          -- hex hash
    data_location      TEXT,                   -- where the parquet files live
    materialized_at    TIMESTAMPTZ,
    non_deterministic  BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (branch, model_name)
);

-- Frozen source references (time-travel anchors)
CREATE TABLE dldbt_meta.source_freeze (
    branch             TEXT NOT NULL,
    source_name        TEXT NOT NULL,
    frozen_snapshot_id BIGINT NOT NULL,
    PRIMARY KEY (branch, source_name)
);
```

Index `model_fingerprints(fingerprint)` — you'll query by hash constantly to find "is this already materialized somewhere?"

---

## Phase 0 — Validate the core assumption (1–2 days, do this first)

**Question:** Can we actually make a "shallow copy" of a DuckLake schema by inserting catalog rows that point at the same Parquet files as an existing schema?

If yes → the rest of the plan is engineering. If no → we need a different design (probably: a branch isn't a schema but a "view" over another schema).

**Spike:**

```bash
# Write a throwaway Python script that:
# 1. Spins up ducklake with Postgres catalog + local filesystem storage
# 2. Creates `main` schema, loads a tiny parquet table
# 3. Directly INSERTs catalog rows duplicating that table under schema `feature_x`
# 4. Verifies: queries against feature_x return the same data as main
# 5. Verifies: dropping schema `feature_x` doesn't delete the parquet file
# 6. Creates a new table in feature_x, writes new parquet
# 7. Verifies: main still sees its old data, only its old data
```

**Claude Code prompt for this phase:**

> Set up a minimal Python project (uv, pyproject.toml, ruff, pytest) named `dldbt-spike`. Write a script that uses Docker Compose to bring up Postgres + MinIO, attaches DuckLake with Postgres catalog and MinIO storage, creates a test table, and then attempts to create a "shallow branch" by inserting duplicate catalog rows under a new schema. The script should print the result of each step and verify the 7 invariants listed in Phase 0 of `dldbt-implementation-plan.md`. Do not worry about CLI structure yet — the goal is to confirm the core mechanic works.

**If the spike fails**, the fallback is to branch at the ducklake-level instead of schema-level (i.e. each branch is its own `ATTACH '...' AS branch_xyz (TYPE ducklake)` pointing at a forked catalog). Document which approach won in `docs/architecture.md`.

---

## Phase 1 — Branch primitives (3–5 days)

Ship a CLI that works end-to-end even if nothing else does.

**Deliverables:**
- `dldbt init` — creates `dldbt_meta` schema, registers the DuckLake
- `dldbt branch create <name>` — shallow-copies `main` into `branch_<name>`
- `dldbt branch list` — with status, size, commit
- `dldbt branch drop <name>` — removes the branch schema; **does not** delete shared parquet files
- `dldbt branch show <name>` — tables, snapshot id, git commit
- Config file `.dldbt.yml` in the dbt project root: catalog connection string, storage location, main branch name

**Tests:**
- Unit: branch name sanitization (git `feature/foo-bar` → ducklake `feature_foo_bar`), config parsing
- Integration: full create → query → drop cycle against a real ducklake

**Exit criteria:** a manual `dldbt branch create test` followed by `duckdb` queries against `test` schema returns identical data to `main`.

---

## Phase 2 — Git integration (2–3 days)

**Deliverables:**
- `dldbt install-hooks` — writes `.git/hooks/post-checkout` and `post-merge` that call `dldbt`
- Hook logic:
  - `post-checkout`: if switching to a branch we don't have a schema for, create it (unless configured to skip)
  - `post-merge` on main: trigger promotion check (phase 6)
- `.dldbtignore`-style config to skip certain branch patterns (`main`, `release/*`)
- Handle detached HEAD, new empty branches, branch renames

**Tests:**
- Integration: init a dummy git repo, create/switch branches, verify dldbt state tracks it

---

## Phase 3 — dbt integration, no fingerprinting yet (3–5 days)

Get the "dbt runs land in the right schema" path working with dbt's own `--defer --state`.

**Deliverables:**
- Jinja macro `generate_schema_name.sql` that reads `DLDBT_BRANCH` env var
- `dldbt dbt run [dbt args...]` wrapper that:
  1. Ensures branch schema exists
  2. Sets `DLDBT_BRANCH` env var
  3. Adds `--defer --state <path>` pointing at main's last manifest
  4. Invokes dbt
  5. Captures new manifest, stores it keyed by branch+commit
- `dldbt dbt compile`, `dldbt dbt test`, `dldbt dbt build` — same wrapper pattern

**Tests:**
- Integration fixture: 3-model dbt project. Run on main, branch it, modify one model, run on branch, verify only the modified model (and its downstream) materialized new parquet in the branch schema.

**Exit criteria:** a `git checkout -b feature/x && dldbt dbt build` does the right thing without any manual setup.

---

## Phase 4 — Fingerprinting (1–2 weeks, the hardest phase)

This is where you diverge meaningfully from dbt's built-in state tracking.

**Fingerprint inputs (canonical order):**
1. Rendered SQL of the model (post-Jinja, post-macro expansion)
2. Normalized SQL AST (via sqlglot) — so whitespace/comment changes don't invalidate
3. Model config fields that affect output: `materialized`, `partition_by`, `cluster_by`, `unique_key`, `on_schema_change`
4. Fingerprints of all upstream refs (recursive — this is the DAG part)
5. Source freeze snapshot ids for any sources this model reads
6. Version of any macro the model transitively invokes (walk the manifest's `depends_on.macros`)

Concatenate, hash with SHA-256, hex-encode. This is the fingerprint.

**Non-determinism escape hatch:**
Model config option `non_deterministic: true` disables fingerprint-based sharing. The model rebuilds every time. Add this to the dbt schema.

**"Which models need to rebuild" algorithm:**
```
For each model M in dbt DAG (topological order):
    new_fp = compute_fingerprint(M)
    existing = lookup_fingerprint(branch=current, model=M)
    main_fp = lookup_fingerprint(branch=main, model=M)

    if new_fp == main_fp and branch != main:
        # Model unchanged from main — reuse main's data via shallow copy
        ensure_shallow_copy(M, from=main, to=current)
    elif new_fp == existing:
        # Already materialized in this branch, nothing to do
        pass
    else:
        # Needs rebuild
        queue_for_materialization(M)
        invalidate_downstream(M)
```

**Deliverables:**
- `dldbt.fingerprint.hasher.compute()` with unit tests covering every input field
- DAG walker that propagates fingerprints
- `dldbt status` command — show modified vs unchanged models (like `git status`)
- Replace phase 3's reliance on `--defer` with fingerprint-driven model selection
- `dldbt dbt run` now passes dbt a custom model selector based on fingerprint diff

**Tests:**
- Unit: same model, different whitespace → same fingerprint
- Unit: changed upstream → changed fingerprint for downstream
- Unit: changed macro → changed fingerprint for every model that uses it
- Unit: `non_deterministic: true` → always "modified"
- Integration: modify one staging model in a 10-model project, verify only 1 model + its downstream are selected for rebuild

---

## Phase 5 — Source freeze (3–5 days)

**Deliverables:**
- On `branch create`, snapshot the current ducklake snapshot_id for every source table, write to `source_freeze`
- In the branch schema, create views for each source that time-travel to the frozen snapshot:
  ```sql
  CREATE VIEW branch_x.raw_events AS
  SELECT * FROM main.raw_events AT (VERSION => 12345);
  ```
- dbt's source resolution happens naturally through these views

**Tests:**
- Integration: create branch, insert new rows into a source on main, verify branch queries don't see them

---

## Phase 6 — Merge (1–2 weeks)

Two operations:

### `dldbt sync` (main → feature, aka "pull" / "rebase")

1. Refuse if branch has uncommitted fingerprint changes (dirty state)
2. Update `source_freeze` to current main snapshot
3. Re-walk fingerprints: any model whose inputs changed (because sources updated, or main introduced new models) is now stale
4. For each stale model, the next `dldbt dbt run` rebuilds it
5. Update `branches.base_snapshot_id` and `branches.last_git_commit`

### `dldbt merge <branch>` (feature → main)

1. Refuse if branch's base is not current main (must `dldbt sync` first)
2. Compute diff: new models, modified models, unchanged models
3. Open transaction on catalog DB:
   - For each modified model: update main's catalog row to point at branch's parquet files
   - For each new model: insert catalog row into main
   - Update main's snapshot
   - Update `model_fingerprints` for main
4. Mark branch as `merged` in `branches` table
5. Commit transaction

**Deliverables:**
- Both commands with clear dry-run modes (`--plan` flag that prints what would change)
- Rollback path for failed merges
- Integration tests for: clean merge, merge-after-sync, refuse-merge-without-sync

**Tests:**
- Integration: feature branch modifies model A, main gets new data in source B, `dldbt sync` picks up new source data, rebuilds only models depending on B, merge succeeds
- Integration: simulate crash mid-merge, verify main's catalog is not corrupted

---

## Phase 7 — Garbage collection (3–5 days)

**Deliverables:**
- `dldbt gc` — find parquet files not referenced by any active branch's metadata, delete
- Reference counting: a parquet file is "live" if at least one row in any branch's catalog references it
- `--dry-run` mode
- Configurable grace period (don't delete files created in the last N hours)

---

## Phase 8 — Observability and polish (ongoing)

- `dldbt status` — staged vs unstaged model changes, divergence from main
- `dldbt diff <branch>` — which tables differ, count/row-level stats (optional: full data diff using duckdb EXCEPT queries)
- `dldbt log <branch>` — snapshot history
- `dldbt doctor` — sanity checks: orphaned metadata, broken references, etc.

---

## Cross-cutting concerns

### Testing strategy

- **Unit tests** cover pure logic: fingerprinting, branch name sanitization, DAG walking
- **Integration tests** use `pytest-docker` to spin up Postgres + MinIO per test session. Each test gets a fresh ducklake; teardown drops everything.
- **Fixture projects:** keep 2–3 minimal dbt projects under `tests/integration/fixtures/` that exercise different DAG shapes (linear, diamond, seeds-only, incremental model)
- **Golden-file tests for fingerprints:** freeze expected hashes for a fixed set of models so you catch accidental changes to the hash function

### Multi-user readiness (design now, implement later)

- Every catalog mutation wrapped in a Postgres transaction. Never mix reads and writes without `SELECT ... FOR UPDATE` on the relevant branch row.
- Branch-level advisory locks (`pg_advisory_lock(hashtext(branch_name))`) around merge/sync/gc operations
- Catalog schema migrations via a simple numbered-SQL-files approach (think Flyway-lite). Don't use Alembic — overkill.
- Treat the config file as read-only per process; don't share mutable state in memory across threads.

### When DuckLake v2.0 branching ships

- Keep catalog operations behind an interface (`CatalogAdapter`). Default impl uses raw SQL; a future `DuckLakeNativeBranchAdapter` swaps in when the feature lands.
- Fingerprinting and dbt integration are independent of how branches are physically stored. They survive the migration.

---

## Suggested first week, concretely

| Day | Work |
|-----|------|
| 1   | Phase 0 spike. Learn whether shallow-copy-via-metadata-insert actually works. |
| 2   | Project scaffold, CI, docker-compose for integration tests |
| 3   | Phase 1: `dldbt init` + `dldbt branch create` + `dldbt branch drop` |
| 4   | Phase 1: `branch list`, `branch show`, config file, round-trip test |
| 5   | Phase 2: git hooks, branch name sanitization, hook installer |

That gets you to "create a git branch, see a matching DuckLake schema magically appear, query it." From there phase 3 unblocks actual dbt work and you have a working tool (even if primitive) by end of week 2.

---

## First Claude Code prompt (ready to paste)

> I'm building `dldbt`, a Python CLI that integrates DuckLake, dbt, and git — it maps git branches to DuckLake schemas, uses content-addressable fingerprinting for model identity, and makes branch-merge a cheap metadata operation. Full design: see `dldbt-implementation-plan.md` (attached).
>
> We're starting with Phase 0: a throwaway spike to validate the core assumption that we can shallow-copy a DuckLake schema by inserting catalog rows that reference the same Parquet files. Please:
>
> 1. Set up a minimal Python project using `uv` with `ruff`, `pytest`, `duckdb`, `psycopg[binary]`
> 2. Create a `docker-compose.yml` with Postgres (as DuckLake catalog) and MinIO (as object storage)
> 3. Write `spike.py` that:
>    - Connects DuckDB + loads the ducklake extension
>    - Attaches a ducklake with Postgres catalog + MinIO storage
>    - Creates schema `main` with a small Parquet-backed table
>    - Reads the catalog DB directly via psycopg and prints the rows describing that table
>    - INSERTs duplicate rows under schema `feature_x` pointing at the same parquet files
>    - Verifies (a) the branch table queries identically, (b) dropping the branch schema doesn't delete the parquet, (c) writing to the branch creates new parquet that main doesn't see
>
> Output: a working spike script plus a short `spike-findings.md` reporting which invariants held. Don't build the real CLI yet — this is strictly to validate the mechanic.

---

## Open questions worth resolving during Phase 0–1

1. Does DuckLake's ducklake extension enforce any schema-level ownership of data files that would block shallow copies? (Phase 0 answers this.)
2. How does `ducklake_add_data_files` behave across schemas? Can we reuse it for merge?
3. What's the granularity of snapshot_id — per-catalog, per-schema, or per-table? (Affects how we track branch state.)
4. How do we want to handle dbt snapshots specifically? Defer until phase 5+, flag as "unsupported" initially.