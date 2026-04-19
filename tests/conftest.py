from __future__ import annotations

import os
import socket
import uuid
from collections.abc import Iterator

import duckdb
import psycopg
import pytest

from dlgit.config import CatalogConfig, Config, S3Settings, StorageConfig

PG_DSN = os.environ.get(
    "DLGIT_TEST_PG_DSN",
    "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake",
)
S3_ENDPOINT = os.environ.get("DLGIT_TEST_S3_ENDPOINT", "localhost:9000")
S3_BUCKET = os.environ.get("DLGIT_TEST_S3_BUCKET", "ducklake")


def _service_up(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Skip integration tests automatically if Postgres/MinIO aren't running."""
    if not any("integration" in item.keywords for item in items):
        return
    pg_ok = _service_up("localhost", 5432)
    s3_host, s3_port = S3_ENDPOINT.split(":")
    s3_ok = _service_up(s3_host, int(s3_port))
    if pg_ok and s3_ok:
        return
    reason = (
        f"integration services unavailable "
        f"(pg={pg_ok}, s3={s3_ok}); run `docker compose up -d`"
    )
    skip = pytest.mark.skip(reason=reason)
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def integration_prefix() -> str:
    """A unique storage prefix per test so they don't step on each other."""
    return f"lake-test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def integration_config(integration_prefix: str) -> Config:
    return Config(
        catalog=CatalogConfig(dsn=PG_DSN),
        storage=StorageConfig(
            data_path=f"s3://{S3_BUCKET}/{integration_prefix}/",
            s3=S3Settings(
                endpoint=S3_ENDPOINT,
                access_key_id="minioadmin",
                secret_access_key="minioadmin",
                url_style="path",
                use_ssl=False,
            ),
        ),
        main_branch="main",
        lake_alias="test_lake",
    )


@pytest.fixture
def clean_catalog() -> Iterator[None]:
    """Wipe any ducklake_* and dlgit_meta state from Postgres before the test."""
    _reset_catalog()
    yield
    _reset_catalog()


def _reset_catalog() -> None:
    with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'ducklake_%'
            """
        )
        for (t,) in cur.fetchall():
            cur.execute(f'DROP TABLE IF EXISTS public."{t}" CASCADE')
        cur.execute("DROP SCHEMA IF EXISTS dlgit_meta CASCADE")


@pytest.fixture
def seeded_main(
    clean_catalog: None, integration_config: Config
) -> Iterator[Config]:
    """Bring the lake up with main.events populated via a direct DuckDB session.

    Tests use this as a pre-state: data already lives in main, and dlgit init
    has not been run yet."""
    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    s3 = integration_config.storage.s3
    assert s3 is not None
    con.execute(
        f"""
        CREATE OR REPLACE SECRET test_s3 (
            TYPE s3,
            REGION 'us-east-1',
            URL_STYLE 'path',
            USE_SSL false,
            ENDPOINT '{s3.endpoint}',
            KEY_ID '{s3.access_key_id}',
            SECRET '{s3.secret_access_key}'
        )
        """
    )
    con.execute(
        f"""
        ATTACH 'ducklake:postgres:{integration_config.catalog.dsn}' AS seed_lake
        (DATA_PATH '{integration_config.storage.data_path}', DATA_INLINING_ROW_LIMIT 0)
        """
    )
    con.execute("USE seed_lake")
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.execute(
        "CREATE TABLE main.events (id INTEGER, name VARCHAR, ts TIMESTAMP)"
    )
    con.execute(
        """
        INSERT INTO main.events VALUES
          (1, 'alpha', TIMESTAMP '2026-04-01 00:00:00'),
          (2, 'beta',  TIMESTAMP '2026-04-02 00:00:00'),
          (3, 'gamma', TIMESTAMP '2026-04-03 00:00:00')
        """
    )
    con.close()
    yield integration_config
