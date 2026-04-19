"""Postgres-backed DuckLake implementation of the CatalogAdapter interface.

The shallow-copy mechanic is the one validated by the Phase 0 spike:

  1. Create the destination schema + empty target tables via DDL so DuckLake
     allocates new schema_id / table_id / column rows.
  2. For each source table, INSERT duplicate rows into ducklake_data_file
     pointing at the same parquet files. The duplicate rows carry absolute
     s3:// paths with path_is_relative = false, because DuckLake would
     otherwise resolve the file path under the destination schema's prefix.
  3. Bump the ducklake_snapshot counter so the next DuckDB attach observes
     the new catalog state.

See spike-findings.md for the rationale behind each of these.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import duckdb
import psycopg

from dlgit.catalog.adapter import BranchRecord, TableInfo
from dlgit.config import Config
from dlgit.errors import (
    BranchAlreadyExistsError,
    BranchNotFoundError,
    NotInitializedError,
)

DLGIT_META_SCHEMA = "dlgit_meta"


@dataclass(frozen=True)
class _SourceTable:
    schema_id: int
    table_id: int
    table_name: str
    schema_path: str
    table_path: str


class DuckLakePgAdapter:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._pg: psycopg.Connection | None = None
        self._duck: duckdb.DuckDBPyConnection | None = None

    # --- context manager ------------------------------------------------
    def __enter__(self) -> DuckLakePgAdapter:
        self._pg = psycopg.connect(self.config.catalog.dsn)
        self._duck = self._new_duckdb()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._duck is not None:
            self._duck.close()
            self._duck = None
        if self._pg is not None:
            self._pg.close()
            self._pg = None

    # --- connections ----------------------------------------------------
    @property
    def pg(self) -> psycopg.Connection:
        if self._pg is None:
            raise RuntimeError("adapter not open; use `with DuckLakePgAdapter(config):`")
        return self._pg

    @property
    def duck(self) -> duckdb.DuckDBPyConnection:
        if self._duck is None:
            raise RuntimeError("adapter not open; use `with DuckLakePgAdapter(config):`")
        return self._duck

    def _new_duckdb(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect()
        con.execute("INSTALL ducklake")
        con.execute("LOAD ducklake")
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        s3 = self.config.storage.s3
        if s3 is not None:
            parts = [
                "TYPE s3",
                f"REGION '{_esc(s3.region)}'",
                f"URL_STYLE '{_esc(s3.url_style)}'",
                f"USE_SSL {'true' if s3.use_ssl else 'false'}",
            ]
            if s3.endpoint:
                parts.append(f"ENDPOINT '{_esc(s3.endpoint)}'")
            if s3.access_key_id:
                parts.append(f"KEY_ID '{_esc(s3.access_key_id)}'")
            if s3.secret_access_key:
                parts.append(f"SECRET '{_esc(s3.secret_access_key)}'")
            con.execute(
                "CREATE OR REPLACE SECRET dlgit_s3 ({})".format(", ".join(parts))
            )
        dsn = _esc(self.config.catalog.dsn)
        data_path = _esc(self.config.storage.data_path)
        con.execute(
            f"ATTACH 'ducklake:postgres:{dsn}' AS {self.config.lake_alias} "
            f"(DATA_PATH '{data_path}', DATA_INLINING_ROW_LIMIT 0)"
        )
        con.execute(f"USE {self.config.lake_alias}")
        return con

    def _reconnect_duckdb(self) -> None:
        """After direct catalog INSERTs the cached DuckLake state is stale;
        reopen the DuckDB connection to pick up the new rows."""
        if self._duck is not None:
            self._duck.close()
        self._duck = self._new_duckdb()

    # --- init / introspection ------------------------------------------
    def init(self) -> None:
        # Attaching in _new_duckdb already created the ducklake_* tables.
        with self.pg.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {DLGIT_META_SCHEMA}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {DLGIT_META_SCHEMA}.branches (
                    name              TEXT PRIMARY KEY,
                    git_branch        TEXT NOT NULL,
                    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    created_from      TEXT NOT NULL,
                    base_snapshot_id  BIGINT NOT NULL,
                    last_git_commit   TEXT,
                    status            TEXT NOT NULL DEFAULT 'active'
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {DLGIT_META_SCHEMA}.model_fingerprints (
                    branch             TEXT NOT NULL,
                    model_name         TEXT NOT NULL,
                    fingerprint        TEXT NOT NULL,
                    data_location      TEXT,
                    materialized_at    TIMESTAMPTZ,
                    non_deterministic  BOOLEAN NOT NULL DEFAULT FALSE,
                    PRIMARY KEY (branch, model_name)
                )
                """
            )
            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS model_fingerprints_fingerprint_idx
                ON {DLGIT_META_SCHEMA}.model_fingerprints (fingerprint)
                """
            )
        self.pg.commit()

        # Register the main branch if not already there. DuckLake provides a
        # default schema named 'main' on attach, which we adopt as the root.
        main = self.config.main_branch
        if self.get_branch(main) is None:
            self.register_branch(
                name=main,
                git_branch=main,
                created_from=main,
                base_snapshot_id=self.current_snapshot_id(),
                last_git_commit=None,
            )

    def is_initialized(self) -> bool:
        with self.pg.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS(
                  SELECT 1 FROM information_schema.tables
                  WHERE table_schema = %s AND table_name = 'branches'
                )
                """,
                (DLGIT_META_SCHEMA,),
            )
            return bool(cur.fetchone()[0])

    def _require_initialized(self) -> None:
        if not self.is_initialized():
            raise NotInitializedError(
                "dlgit_meta is missing. Run `dlgit init` first."
            )

    # --- branch registry ------------------------------------------------
    def register_branch(
        self,
        *,
        name: str,
        git_branch: str,
        created_from: str,
        base_snapshot_id: int,
        last_git_commit: str | None,
    ) -> BranchRecord:
        self._require_initialized()
        with self.pg.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {DLGIT_META_SCHEMA}.branches
                  (name, git_branch, created_from, base_snapshot_id, last_git_commit)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING name, git_branch, created_at, created_from,
                          base_snapshot_id, last_git_commit, status
                """,
                (name, git_branch, created_from, base_snapshot_id, last_git_commit),
            )
            row = cur.fetchone()
        self.pg.commit()
        return _to_branch_record(row)

    def list_branches(self) -> list[BranchRecord]:
        self._require_initialized()
        with self.pg.cursor() as cur:
            cur.execute(
                f"""
                SELECT name, git_branch, created_at, created_from,
                       base_snapshot_id, last_git_commit, status
                FROM {DLGIT_META_SCHEMA}.branches
                ORDER BY created_at
                """
            )
            return [_to_branch_record(r) for r in cur.fetchall()]

    def get_branch(self, name: str) -> BranchRecord | None:
        self._require_initialized()
        with self.pg.cursor() as cur:
            cur.execute(
                f"""
                SELECT name, git_branch, created_at, created_from,
                       base_snapshot_id, last_git_commit, status
                FROM {DLGIT_META_SCHEMA}.branches
                WHERE name = %s
                """,
                (name,),
            )
            row = cur.fetchone()
        return _to_branch_record(row) if row else None

    def update_branch_status(self, name: str, status: str) -> None:
        self._require_initialized()
        with self.pg.cursor() as cur:
            cur.execute(
                f"UPDATE {DLGIT_META_SCHEMA}.branches SET status = %s WHERE name = %s",
                (status, name),
            )
            if cur.rowcount == 0:
                raise BranchNotFoundError(name)
        self.pg.commit()

    # --- snapshot -------------------------------------------------------
    def current_snapshot_id(self) -> int:
        with self.pg.cursor() as cur:
            cur.execute("SELECT MAX(snapshot_id) FROM public.ducklake_snapshot")
            row = cur.fetchone()
        if row is None or row[0] is None:
            return 0
        return int(row[0])

    # --- shallow copy ---------------------------------------------------
    def shallow_copy_schema(self, src: str, dst: str) -> int:
        """See module docstring. Raises BranchAlreadyExistsError if dst exists."""
        self._require_initialized()
        if self._schema_exists(dst):
            raise BranchAlreadyExistsError(f"schema {dst!r} already exists")
        src_tables = self._list_source_tables(src)

        # 1. Create dst schema and empty tables via DDL.
        self.duck.execute(f'CREATE SCHEMA "{dst}"')
        for t in src_tables:
            self.duck.execute(
                f'CREATE TABLE "{dst}"."{t.table_name}" AS '
                f'SELECT * FROM "{src}"."{t.table_name}" WHERE 0=1'
            )

        # 2. Release the DuckDB connection so we can mutate the catalog
        # without fighting its cached state. Then reopen afterwards.
        self._duck.close() if self._duck else None
        self._duck = None

        # 3. Duplicate data_file rows for each table.
        dst_tables = self._list_source_tables(dst)  # uses psycopg, no duckdb needed
        dst_by_name = {t.table_name: t for t in dst_tables}
        total_files = 0
        try:
            with self.pg.cursor() as cur:
                data_path = self._ducklake_data_path(cur)
                for src_table in src_tables:
                    dst_table = dst_by_name[src_table.table_name]
                    n = self._duplicate_data_files(
                        cur, src_table, dst_table, data_path
                    )
                    total_files += n
                if total_files > 0:
                    self._record_snapshot_change(
                        cur, f'branched_schema:"{src}" -> "{dst}"'
                    )
            self.pg.commit()
        except Exception:
            self.pg.rollback()
            raise
        finally:
            self._duck = self._new_duckdb()

        return len(src_tables)

    def drop_schema(self, name: str) -> None:
        """Drop `name` and its tables. For tables created via shallow copy we
        first delete the aliased ducklake_data_file rows (path_is_relative =
        false) directly, so DROP SCHEMA CASCADE can't schedule shared parquet
        for deletion."""
        self._require_initialized()
        if not self._schema_exists(name):
            raise BranchNotFoundError(f"schema {name!r} does not exist")

        # Close duck so catalog surgery has no contenders.
        if self._duck is not None:
            self._duck.close()
            self._duck = None

        try:
            with self.pg.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM public.ducklake_data_file d
                    USING public.ducklake_table t, public.ducklake_schema s
                    WHERE d.table_id = t.table_id
                      AND t.schema_id = s.schema_id
                      AND s.schema_name = %s
                      AND d.path_is_relative = false
                    """,
                    (name,),
                )
            self.pg.commit()
        except Exception:
            self.pg.rollback()
            self._duck = self._new_duckdb()
            raise

        self._duck = self._new_duckdb()
        self.duck.execute(f'DROP SCHEMA IF EXISTS "{name}" CASCADE')

    # --- listing --------------------------------------------------------
    def list_tables(self, schema: str) -> list[TableInfo]:
        # Compute stats from ducklake_data_file directly, because branches
        # created via shallow_copy_schema do not get their own
        # ducklake_table_stats rows — they only inherit data_file references.
        with self.pg.cursor() as cur:
            cur.execute(
                """
                SELECT s.schema_name, t.table_name,
                       COALESCE(SUM(d.record_count), 0) AS record_count,
                       COALESCE(SUM(d.file_size_bytes), 0) AS file_size_bytes
                FROM public.ducklake_table t
                JOIN public.ducklake_schema s ON t.schema_id = s.schema_id
                LEFT JOIN public.ducklake_data_file d
                  ON d.table_id = t.table_id AND d.end_snapshot IS NULL
                WHERE s.schema_name = %s
                  AND t.end_snapshot IS NULL
                GROUP BY s.schema_name, t.table_name
                ORDER BY t.table_name
                """,
                (schema,),
            )
            return [
                TableInfo(
                    schema_name=r[0],
                    table_name=r[1],
                    record_count=int(r[2]) if r[2] is not None else None,
                    file_size_bytes=int(r[3]) if r[3] is not None else None,
                )
                for r in cur.fetchall()
            ]

    # --- internals ------------------------------------------------------
    def _schema_exists(self, name: str) -> bool:
        with self.pg.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM public.ducklake_schema
                WHERE schema_name = %s AND end_snapshot IS NULL
                """,
                (name,),
            )
            return cur.fetchone() is not None

    def _list_source_tables(self, schema: str) -> list[_SourceTable]:
        with self.pg.cursor() as cur:
            cur.execute(
                """
                SELECT s.schema_id, t.table_id, t.table_name, s.path, t.path
                FROM public.ducklake_table t
                JOIN public.ducklake_schema s ON t.schema_id = s.schema_id
                WHERE s.schema_name = %s
                  AND t.end_snapshot IS NULL
                  AND s.end_snapshot IS NULL
                ORDER BY t.table_name
                """,
                (schema,),
            )
            return [
                _SourceTable(
                    schema_id=r[0],
                    table_id=r[1],
                    table_name=r[2],
                    schema_path=r[3],
                    table_path=r[4],
                )
                for r in cur.fetchall()
            ]

    def _ducklake_data_path(self, cur: psycopg.Cursor) -> str:
        cur.execute(
            "SELECT value FROM public.ducklake_metadata WHERE key = 'data_path'"
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("ducklake_metadata has no data_path entry")
        return str(row[0])

    def _duplicate_data_files(
        self,
        cur: psycopg.Cursor,
        src: _SourceTable,
        dst: _SourceTable,
        data_path: str,
    ) -> int:
        df_cols = _columns(cur, "ducklake_data_file")
        path_idx = df_cols.index("path")
        rel_idx = df_cols.index("path_is_relative")
        id_idx = df_cols.index("data_file_id")
        tid_idx = df_cols.index("table_id")

        cur.execute(
            f'SELECT {_quote_cols(df_cols)} '
            f'FROM public.ducklake_data_file '
            f'WHERE table_id = %s AND end_snapshot IS NULL',
            (src.table_id,),
        )
        rows = cur.fetchall()
        if not rows:
            return 0

        cur.execute(
            'SELECT COALESCE(MAX(data_file_id), 0) FROM public.ducklake_data_file'
        )
        next_id = int(cur.fetchone()[0]) + 1

        insert_sql = (
            f"INSERT INTO public.ducklake_data_file ({_quote_cols(df_cols)}) "
            f"VALUES ({', '.join(['%s'] * len(df_cols))})"
        )
        new_rows = []
        for row in rows:
            row = list(row)
            original_path = row[path_idx]
            if row[rel_idx]:
                row[path_idx] = (
                    f"{data_path}{src.schema_path}{src.table_path}{original_path}"
                )
                row[rel_idx] = False
            row[id_idx] = next_id
            next_id += 1
            row[tid_idx] = dst.table_id
            new_rows.append(tuple(row))
        cur.executemany(insert_sql, new_rows)
        return len(new_rows)

    def _record_snapshot_change(
        self, cur: psycopg.Cursor, change: str
    ) -> None:
        """Insert a new ducklake_snapshot row + snapshot_changes entry so
        DuckDB's next attach sees our catalog mutation."""
        snap_cols = _columns(cur, "ducklake_snapshot")
        cur.execute(
            f"SELECT {_quote_cols(snap_cols)} FROM public.ducklake_snapshot "
            f"ORDER BY snapshot_id DESC LIMIT 1"
        )
        latest = cur.fetchone()
        if latest is None:
            return
        now = datetime.now(UTC)
        new_row: list[Any] = []
        for col, val in zip(snap_cols, latest, strict=True):
            if col == "snapshot_id":
                new_row.append(int(val) + 1)
            elif col == "snapshot_time":
                new_row.append(now)
            elif col in ("next_catalog_id", "next_file_id"):
                new_row.append(int(val) + 1 if val is not None else 1)
            else:
                new_row.append(val)
        cur.execute(
            f"INSERT INTO public.ducklake_snapshot ({_quote_cols(snap_cols)}) "
            f"VALUES ({', '.join(['%s'] * len(snap_cols))})",
            tuple(new_row),
        )
        new_snap_id = new_row[snap_cols.index("snapshot_id")]

        change_cols = _columns(cur, "ducklake_snapshot_changes")
        record = {c: None for c in change_cols}
        record["snapshot_id"] = new_snap_id
        record["changes_made"] = change
        cur.execute(
            f"INSERT INTO public.ducklake_snapshot_changes ({_quote_cols(change_cols)}) "
            f"VALUES ({', '.join(['%s'] * len(change_cols))})",
            tuple(record[c] for c in change_cols),
        )


def _columns(cur: psycopg.Cursor, table: str) -> list[str]:
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    )
    return [r[0] for r in cur.fetchall()]


def _quote_cols(cols: list[str]) -> str:
    return ", ".join(f'"{c}"' for c in cols)


def _to_branch_record(row: tuple | None) -> BranchRecord:
    assert row is not None
    return BranchRecord(
        name=row[0],
        git_branch=row[1],
        created_at=row[2],
        created_from=row[3],
        base_snapshot_id=row[4],
        last_git_commit=row[5],
        status=row[6],
    )


def _esc(s: str) -> str:
    # The DSN / paths are treated as single-quoted SQL string literals by
    # DuckDB. Escape embedded single quotes.
    return s.replace("'", "''")
