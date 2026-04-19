# dldbt

Map git branches to DuckLake schemas so dbt builds respect the current branch
and feature branches reuse the parquet main already produced. See
[`dldbt-implementation-plan.md`](dldbt-implementation-plan.md) for the full
design and phase plan.

Status: Phases 0–3 implemented. Phase 4+ still open.

## What it does

- One DuckLake schema per git branch, backed by the same Postgres catalog and
  S3-compatible storage.
- Branch creation is a catalog-level shallow copy: new rows in
  `ducklake_data_file` point at main's existing parquet, so there's no data
  movement.
- `dldbt dbt <cmd>` sets `DLDBT_BRANCH` so a bundled `generate_schema_name`
  macro lands models in the branch's schema, and auto-injects
  `--defer --state <main_manifest> --select state:modified+` so feature
  branches only rebuild what changed.
- Optional post-checkout git hook auto-creates the schema for a new branch.

## Quick start

```bash
# 1. Bring up Postgres (catalog) + MinIO (storage)
docker compose up -d

# 2. Write a config (see .dldbt.yml.example)
cp .dldbt.yml.example .dldbt.yml

# 3. Initialize the catalog + generate profiles.yml
uv run dldbt init --project path/to/dbt_project

# 4. Drop the schema-naming macro into the dbt project
uv run dldbt install-macro --project path/to/dbt_project

# 5. Build on a feature branch — only changed models + downstream rebuild
git checkout -b feature/x
uv run dldbt dbt build --project path/to/dbt_project
```

## CLI

| command | what it does |
| --- | --- |
| `dldbt init` | Initialize the DuckLake catalog; write profiles.yml if a dbt_project.yml is present. |
| `dldbt install-macro` | Copy the `generate_schema_name` macro into a dbt project's `macros/`. |
| `dldbt generate-profile` | Write profiles.yml matching the current config. |
| `dldbt dbt <subcommand> [args...]` | Run dbt with `DLDBT_BRANCH` set to the current git branch's schema. Wraps `run`/`build`/`test`/`compile` with `--defer --state ... --select state:modified+` on feature branches (pass `--full` to skip). |
| `dldbt branch create <name>` | Shallow-copy main into a new branch schema. |
| `dldbt branch drop <name>` | Drop a branch schema (shared parquet stays). |
| `dldbt branch list` / `show` | Inspect registered branches. |
| `dldbt install-hooks` | Install post-checkout / post-merge git hooks. |

## Config

`.dldbt.yml` at the project root. See
[`.dldbt.yml.example`](.dldbt.yml.example) for the full shape. Minimum:

```yaml
catalog:
  dsn: "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake"
storage:
  data_path: "s3://bucket/lake/"
  s3:
    endpoint: "localhost:9000"
    access_key_id: "..."
    secret_access_key: "..."
    url_style: "path"
    use_ssl: false
```

`auto_create.enabled: true` turns on hook-driven branch creation on
`git checkout`; `skip_patterns` lists branch globs the hook should ignore
(manual `dldbt branch create` still works for those names).

## Development

```bash
# Unit tests (no services needed)
uv run pytest tests/unit/

# Full suite including docker-backed integration tests
docker compose up -d
uv run pytest

# Teardown
docker compose down -v
```

Lint + type-check: `uv run ruff check src/ tests/` and
`uv run mypy src/dldbt/`.
