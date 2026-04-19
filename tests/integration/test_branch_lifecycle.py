"""Phase 1 integration test: full init → branch create → query → drop cycle.

These tests are marked `integration` and require docker compose to be running
(postgres + minio). See conftest.py for the auto-skip guard.
"""

from __future__ import annotations

import duckdb
import pytest

from dldbt.catalog.ducklake_pg import DuckLakePgAdapter
from dldbt.config import Config
from dldbt.errors import BranchAlreadyExistsError

pytestmark = pytest.mark.integration


def _query(config: Config, sql: str) -> list[tuple]:
    """Open a fresh DuckDB session against the lake and run a query."""
    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    s3 = config.storage.s3
    assert s3 is not None
    con.execute(
        f"""
        CREATE OR REPLACE SECRET q_s3 (
            TYPE s3, REGION 'us-east-1', URL_STYLE 'path', USE_SSL false,
            ENDPOINT '{s3.endpoint}',
            KEY_ID '{s3.access_key_id}',
            SECRET '{s3.secret_access_key}'
        )
        """
    )
    con.execute(
        f"""
        ATTACH 'ducklake:postgres:{config.catalog.dsn}' AS q_lake
        (DATA_PATH '{config.storage.data_path}', DATA_INLINING_ROW_LIMIT 0)
        """
    )
    con.execute("USE q_lake")
    rows = con.execute(sql).fetchall()
    con.close()
    return rows


EXPECTED_EVENTS = [
    (1, "alpha", "2026-04-01 00:00:00"),
    (2, "beta", "2026-04-02 00:00:00"),
    (3, "gamma", "2026-04-03 00:00:00"),
]


def _normalize(rows: list[tuple]) -> list[tuple]:
    return [(r[0], r[1], r[2].strftime("%Y-%m-%d %H:%M:%S")) for r in rows]


def test_init_is_idempotent(seeded_main: Config) -> None:
    with DuckLakePgAdapter(seeded_main) as adapter:
        adapter.init()
        adapter.init()  # second call must not crash
        main = adapter.get_branch("main")
    assert main is not None
    assert main.git_branch == "main"
    assert main.status == "active"


def test_create_branch_shares_data(seeded_main: Config) -> None:
    """Phase 1 exit criterion: `branch create` followed by a DuckDB query
    against the branch schema returns identical rows to main."""
    with DuckLakePgAdapter(seeded_main) as adapter:
        adapter.init()
        n = adapter.shallow_copy_schema("main", "feature_alpha")
        adapter.register_branch(
            name="feature_alpha",
            git_branch="feature/alpha",
            created_from="main",
            base_snapshot_id=adapter.current_snapshot_id(),
            last_git_commit=None,
        )
    assert n == 1

    main_rows = _normalize(
        _query(seeded_main, "SELECT * FROM main.events ORDER BY id")
    )
    branch_rows = _normalize(
        _query(seeded_main, "SELECT * FROM feature_alpha.events ORDER BY id")
    )
    assert main_rows == EXPECTED_EVENTS
    assert branch_rows == EXPECTED_EVENTS


def test_create_branch_rejects_existing_name(seeded_main: Config) -> None:
    with DuckLakePgAdapter(seeded_main) as adapter:
        adapter.init()
        adapter.shallow_copy_schema("main", "dup")
        with pytest.raises(BranchAlreadyExistsError):
            adapter.shallow_copy_schema("main", "dup")


def test_drop_branch_leaves_main_intact(seeded_main: Config) -> None:
    with DuckLakePgAdapter(seeded_main) as adapter:
        adapter.init()
        adapter.shallow_copy_schema("main", "feature_beta")
        adapter.register_branch(
            name="feature_beta",
            git_branch="feature/beta",
            created_from="main",
            base_snapshot_id=adapter.current_snapshot_id(),
            last_git_commit=None,
        )
        adapter.drop_schema("feature_beta")
        adapter.update_branch_status("feature_beta", "abandoned")
        remaining = {b.name: b.status for b in adapter.list_branches()}

    assert remaining["main"] == "active"
    assert remaining["feature_beta"] == "abandoned"
    main_rows = _normalize(
        _query(seeded_main, "SELECT * FROM main.events ORDER BY id")
    )
    assert main_rows == EXPECTED_EVENTS


def test_list_tables_reports_records_after_branch(seeded_main: Config) -> None:
    with DuckLakePgAdapter(seeded_main) as adapter:
        adapter.init()
        adapter.shallow_copy_schema("main", "feature_gamma")
        main_tables = {t.table_name: t for t in adapter.list_tables("main")}
        branch_tables = {t.table_name: t for t in adapter.list_tables("feature_gamma")}
    assert set(branch_tables) == {"events"}
    # Shallow copy aliases the same data_file rows, so record_count and
    # file_size_bytes reported from ducklake_data_file must match main.
    assert branch_tables["events"].record_count == main_tables["events"].record_count
    assert branch_tables["events"].file_size_bytes == main_tables["events"].file_size_bytes
    assert branch_tables["events"].record_count == 3


def test_writes_to_branch_do_not_touch_main(seeded_main: Config) -> None:
    with DuckLakePgAdapter(seeded_main) as adapter:
        adapter.init()
        adapter.shallow_copy_schema("main", "feature_delta")

    # Write a new table into the branch via DuckDB (simulating dbt).
    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    s3 = seeded_main.storage.s3
    assert s3 is not None
    con.execute(
        f"""
        CREATE OR REPLACE SECRET w_s3 (
            TYPE s3, REGION 'us-east-1', URL_STYLE 'path', USE_SSL false,
            ENDPOINT '{s3.endpoint}',
            KEY_ID '{s3.access_key_id}',
            SECRET '{s3.secret_access_key}'
        )
        """
    )
    con.execute(
        f"""
        ATTACH 'ducklake:postgres:{seeded_main.catalog.dsn}' AS w_lake
        (DATA_PATH '{seeded_main.storage.data_path}', DATA_INLINING_ROW_LIMIT 0)
        """
    )
    con.execute("USE w_lake")
    con.execute("CREATE TABLE feature_delta.scratch (id INTEGER, v VARCHAR)")
    con.execute("INSERT INTO feature_delta.scratch VALUES (1, 'only on branch')")
    con.close()

    # main should not resolve feature_delta.scratch
    with pytest.raises(duckdb.Error):
        _query(seeded_main, "SELECT * FROM main.scratch")

    # branch should see both tables now
    branch_rows = _normalize(
        _query(seeded_main, "SELECT * FROM feature_delta.events ORDER BY id")
    )
    assert branch_rows == EXPECTED_EVENTS
    scratch = _query(seeded_main, "SELECT * FROM feature_delta.scratch")
    assert scratch == [(1, "only on branch")]
