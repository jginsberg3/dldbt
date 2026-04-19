# dldbt (codename)

A tool that maps git branches to DuckLake schemas so dbt runs respect the
current branch and merging reuses the data it already computed. See
[`dldbt-implementation-plan.md`](dldbt-implementation-plan.md) for the full design.

## Phase 0 spike

This repository currently contains only the Phase 0 spike, which validates the
core assumption that a DuckLake schema can be shallow-copied by inserting
catalog rows that point at the same Parquet files as an existing schema.

### Run it

```bash
# 1. Bring up Postgres (catalog) and MinIO (storage)
docker compose up -d

# 2. Run the spike
uv run python spike.py
```

Results print to stdout and are summarized in `spike-findings.md`.

### Clean up

```bash
docker compose down -v
```
