"""
Phase 0 spike for dlgit — validate that a DuckLake schema can be
"shallow copied" by inserting catalog rows that point at the same Parquet
files as an existing schema.

See `dldbt-implementation-plan.md` Phase 0.

Invariants tested:
  I1. main_schema.events loads and returns the inserted rows.
  I2. Catalog rows describing main_schema.events are readable via psycopg.
  I3. feature_x.events, populated by duplicating catalog rows, returns
      identical data to main_schema.events.
  I4. Dropping feature_x leaves main_schema.events intact.
  I5. Creating a new table in feature_x writes new parquet files.
  I6. main_schema does not see feature_x's new table.
  I7. After dropping feature_x, the parquet files shared with
      main_schema remain on disk.

This is a throwaway spike. It prints every meaningful step, dumps the
catalog at key moments, and writes spike-findings.md at the end.
"""

from __future__ import annotations

import os
import sys
import traceback
from dataclasses import dataclass, field

import duckdb
import psycopg


PG_DSN = os.environ.get(
    "DLGIT_SPIKE_PG_DSN",
    "host=localhost port=5432 dbname=ducklake user=ducklake password=ducklake",
)
S3_ENDPOINT = os.environ.get("DLGIT_SPIKE_S3_ENDPOINT", "localhost:9000")
S3_BUCKET = os.environ.get("DLGIT_SPIKE_S3_BUCKET", "ducklake")
S3_PREFIX = os.environ.get("DLGIT_SPIKE_S3_PREFIX", "lake")
S3_KEY = os.environ.get("DLGIT_SPIKE_S3_KEY", "minioadmin")
S3_SECRET = os.environ.get("DLGIT_SPIKE_S3_SECRET", "minioadmin")
LAKE = "my_lake"


@dataclass
class Invariant:
    id: str
    description: str
    status: str = "pending"
    detail: str = ""


@dataclass
class Report:
    invariants: list[Invariant] = field(default_factory=list)

    def add(self, id_: str, description: str) -> Invariant:
        inv = Invariant(id=id_, description=description)
        self.invariants.append(inv)
        return inv

    def write(self, path: str) -> None:
        lines = ["# Phase 0 Spike Findings", ""]
        lines.append("Ran against: DuckLake via DuckDB + Postgres catalog + MinIO (S3) storage.")
        lines.append("")
        lines.append("| ID | Status | Description |")
        lines.append("|----|--------|-------------|")
        for inv in self.invariants:
            lines.append(f"| {inv.id} | {inv.status} | {inv.description} |")
        lines.append("")
        for inv in self.invariants:
            lines.append(f"## {inv.id} — {inv.description}")
            lines.append(f"**Status:** {inv.status}")
            lines.append("")
            if inv.detail:
                lines.append("```")
                lines.append(inv.detail.rstrip())
                lines.append("```")
                lines.append("")
        with open(path, "w") as f:
            f.write("\n".join(lines))


def info(msg: str) -> None:
    print(msg, flush=True)


def section(title: str) -> None:
    print(f"\n=== {title} ===", flush=True)


def reset_catalog(pg: psycopg.Connection) -> None:
    with pg.cursor() as cur:
        cur.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'ducklake_%'
            """
        )
        tables = [r[0] for r in cur.fetchall()]
    with pg.cursor() as cur:
        for t in tables:
            cur.execute(f'DROP TABLE IF EXISTS public."{t}" CASCADE')
    pg.commit()
    info(f"Reset: dropped {len(tables)} existing ducklake_* tables")


def connect_duckdb() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL ducklake")
    con.execute("LOAD ducklake")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute(
        f"""
        CREATE OR REPLACE SECRET minio (
            TYPE s3,
            KEY_ID '{S3_KEY}',
            SECRET '{S3_SECRET}',
            ENDPOINT '{S3_ENDPOINT}',
            URL_STYLE 'path',
            USE_SSL false,
            REGION 'us-east-1'
        )
        """
    )
    # DATA_INLINING_ROW_LIMIT 0 forces every insert to produce a parquet file
    # instead of being stored directly in the catalog's ducklake_inlined_data_*
    # tables. That's what we want to validate — shallow copy at the file level.
    con.execute(
        f"""
        ATTACH 'ducklake:postgres:{PG_DSN}' AS {LAKE}
        (DATA_PATH 's3://{S3_BUCKET}/{S3_PREFIX}/', DATA_INLINING_ROW_LIMIT 0)
        """
    )
    con.execute(f"USE {LAKE}")
    return con


def ducklake_tables(pg: psycopg.Connection) -> list[str]:
    with pg.cursor() as cur:
        cur.execute(
            """
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public' AND tablename LIKE 'ducklake_%'
            ORDER BY tablename
            """
        )
        return [r[0] for r in cur.fetchall()]


def table_columns(pg: psycopg.Connection, table: str) -> list[str]:
    with pg.cursor() as cur:
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        return [r[0] for r in cur.fetchall()]


def dump_table(pg: psycopg.Connection, table: str, limit: int = 20) -> None:
    with pg.cursor() as cur:
        cur.execute(f'SELECT * FROM public."{table}" LIMIT {limit}')
        cols = [d.name for d in cur.description]
        rows = cur.fetchall()
    info(f"  {table} ({len(rows)} row(s)) cols={cols}")
    for r in rows:
        info(f"    {r}")


def dump_catalog(pg: psycopg.Connection, label: str) -> None:
    section(f"Catalog snapshot: {label}")
    for t in ducklake_tables(pg):
        dump_table(pg, t)


def list_s3_files() -> list[str]:
    con = duckdb.connect()
    try:
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute(
            f"""
            CREATE OR REPLACE SECRET minio (
                TYPE s3,
                KEY_ID '{S3_KEY}',
                SECRET '{S3_SECRET}',
                ENDPOINT '{S3_ENDPOINT}',
                URL_STYLE 'path',
                USE_SSL false,
                REGION 'us-east-1'
            )
            """
        )
        rows = con.execute(
            f"SELECT file FROM glob('s3://{S3_BUCKET}/{S3_PREFIX}/**')"
        ).fetchall()
        return [r[0] for r in rows]
    except duckdb.Error as e:
        info(f"glob error: {e}")
        return []
    finally:
        con.close()


def duplicate_data_files(pg: psycopg.Connection) -> dict[str, object]:
    """Insert ducklake_data_file rows for feature_x.events pointing at the
    same parquet files as main_schema.events. Returns diagnostic info."""
    diag: dict[str, object] = {}
    with pg.cursor() as cur:
        # Discover columns of the tables we touch dynamically.
        schema_cols = table_columns(pg, "ducklake_schema")
        table_cols = table_columns(pg, "ducklake_table")
        df_cols = table_columns(pg, "ducklake_data_file")
        diag["ducklake_schema_columns"] = schema_cols
        diag["ducklake_table_columns"] = table_cols
        diag["ducklake_data_file_columns"] = df_cols

        # Find id columns for schema and table — first column is typically PK.
        schema_name_col = _pick(schema_cols, ["schema_name", "name"])
        schema_id_col = _pick(schema_cols, ["schema_id", "id"])
        schema_path_col = _pick(schema_cols, ["path"])
        table_name_col = _pick(table_cols, ["table_name", "name"])
        table_id_col = _pick(table_cols, ["table_id", "id"])
        table_schema_fk = _pick(table_cols, ["schema_id"])
        table_path_col = _pick(table_cols, ["path"])

        cur.execute(
            f"""
            SELECT t."{table_id_col}", s."{schema_name_col}", t."{table_name_col}",
                   s."{schema_path_col}", t."{table_path_col}"
            FROM public.ducklake_table t
            JOIN public.ducklake_schema s
              ON t."{table_schema_fk}" = s."{schema_id_col}"
            WHERE t."{table_name_col}" = 'events'
            ORDER BY t."{table_id_col}"
            """
        )
        tbl_rows = cur.fetchall()
        diag["events_tables"] = tbl_rows
        info(f"  events table rows: {tbl_rows}")
        by_schema = {(r[1], r[2]): r for r in tbl_rows}
        main_row = by_schema[("main_schema", "events")]
        feat_row = by_schema[("feature_x", "events")]
        main_id = main_row[0]
        feat_id = feat_row[0]
        main_schema_path = main_row[3]
        main_table_path = main_row[4]

        # data_path comes from ducklake_metadata
        cur.execute(
            "SELECT value FROM public.ducklake_metadata WHERE key = 'data_path'"
        )
        data_path = cur.fetchone()[0]
        diag["data_path"] = data_path
        info(
            f"  resolving original files under "
            f"{data_path}{main_schema_path}{main_table_path}"
        )

        # Grab main's data_file rows.
        df_table_fk = _pick(df_cols, ["table_id"])
        df_id_col = df_cols[0]
        df_path_col = _pick(df_cols, ["path"])
        df_path_is_rel_col = _pick(df_cols, ["path_is_relative"])
        cur.execute(
            f'SELECT * FROM public.ducklake_data_file WHERE "{df_table_fk}" = %s',
            (main_id,),
        )
        main_dfs = cur.fetchall()
        diag["main_data_file_count"] = len(main_dfs)
        info(f"  {len(main_dfs)} data_file row(s) for main_schema.events")
        if not main_dfs:
            raise RuntimeError("no data_file rows to duplicate")

        cur.execute(
            f'SELECT COALESCE(MAX("{df_id_col}"), 0) FROM public.ducklake_data_file'
        )
        next_id = int(cur.fetchone()[0]) + 1

        col_list = ", ".join(f'"{c}"' for c in df_cols)
        placeholders = ", ".join(["%s"] * len(df_cols))
        insert_sql = (
            f"INSERT INTO public.ducklake_data_file ({col_list}) "
            f"VALUES ({placeholders})"
        )

        fk_idx = df_cols.index(df_table_fk)
        id_idx = df_cols.index(df_id_col)
        path_idx = df_cols.index(df_path_col)
        rel_idx = df_cols.index(df_path_is_rel_col)
        new_rows = []
        for row in main_dfs:
            original_path = row[path_idx]
            is_relative = row[rel_idx]
            # Resolve to an absolute URL so the duplicate row under feature_x's
            # table_id still points at the file in main_schema's prefix.
            if is_relative:
                absolute_path = (
                    f"{data_path}{main_schema_path}{main_table_path}{original_path}"
                )
            else:
                absolute_path = original_path
            row = list(row)
            row[id_idx] = next_id
            next_id += 1
            row[fk_idx] = feat_id
            row[path_idx] = absolute_path
            row[rel_idx] = False
            new_rows.append(tuple(row))

        cur.executemany(insert_sql, new_rows)
        diag["duplicated_rows"] = len(new_rows)
        diag["duplicated_paths"] = [r[path_idx] for r in new_rows]
        info(f"  Inserted {len(new_rows)} duplicate data_file row(s)")
        info(f"  pointing at: {diag['duplicated_paths']}")

        # Bump the snapshot so DuckLake sees a newer catalog state.
        try:
            _bump_snapshot(cur)
        except Exception as e:
            info(f"  snapshot bump skipped: {e!r}")
            diag["snapshot_bump_error"] = repr(e)
    pg.commit()
    return diag


def _pick(cols: list[str], candidates: list[str]) -> str:
    for c in candidates:
        if c in cols:
            return c
    raise KeyError(f"none of {candidates} in {cols}")


def _bump_snapshot(cur: psycopg.Cursor) -> None:
    """Insert a new snapshot row modeled on the highest existing snapshot.
    Column set is discovered at runtime to tolerate DuckLake schema drift."""
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'ducklake_snapshot'
        ORDER BY ordinal_position
        """
    )
    cols = [r[0] for r in cur.fetchall()]
    if not cols:
        raise RuntimeError("ducklake_snapshot does not exist")
    col_list = ", ".join(f'"{c}"' for c in cols)
    cur.execute(f"SELECT {col_list} FROM public.ducklake_snapshot ORDER BY 1 DESC LIMIT 1")
    latest = cur.fetchone()
    if latest is None:
        raise RuntimeError("ducklake_snapshot is empty")
    # Bump any obviously-integer columns that look like ids or counters,
    # set any timestamp-looking column to NOW().
    new_row = list(latest)
    for i, (c, v) in enumerate(zip(cols, latest)):
        if isinstance(v, int) and ("id" in c or "next" in c or "version" in c):
            new_row[i] = v + 1
        elif hasattr(v, "tzinfo") or "time" in c.lower():
            import datetime as _dt
            new_row[i] = _dt.datetime.now(_dt.timezone.utc)
    placeholders = ", ".join(["%s"] * len(cols))
    cur.execute(
        f"INSERT INTO public.ducklake_snapshot ({col_list}) VALUES ({placeholders})",
        tuple(new_row),
    )


def collect_main_parquet_paths(pg: psycopg.Connection) -> list[str]:
    try:
        df_cols = table_columns(pg, "ducklake_data_file")
        table_cols = table_columns(pg, "ducklake_table")
        schema_cols = table_columns(pg, "ducklake_schema")
    except Exception:
        return []
    path_col = next(
        (c for c in ["path", "data_file_path", "file_path", "location"] if c in df_cols),
        None,
    )
    if path_col is None:
        return []
    table_id_col = _pick(table_cols, ["table_id", "id"])
    schema_id_col = _pick(schema_cols, ["schema_id", "id"])
    table_schema_fk = _pick(table_cols, ["schema_id"])
    schema_name_col = _pick(schema_cols, ["schema_name", "name"])
    df_table_fk = _pick(df_cols, ["table_id"])
    with pg.cursor() as cur:
        cur.execute(
            f"""
            SELECT d."{path_col}"
            FROM public.ducklake_data_file d
            JOIN public.ducklake_table t ON d."{df_table_fk}" = t."{table_id_col}"
            JOIN public.ducklake_schema s ON t."{table_schema_fk}" = s."{schema_id_col}"
            WHERE s."{schema_name_col}" = 'main_schema'
            """
        )
        return [r[0] for r in cur.fetchall()]


def main() -> int:
    report = Report()
    i1 = report.add("I1", "main_schema.events is queryable after insert")
    i2 = report.add("I2", "catalog rows for main_schema.events visible via psycopg")
    i3 = report.add("I3", "feature_x.events returns identical data after shallow copy")
    i4 = report.add("I4", "dropping feature_x leaves main_schema.events intact")
    i5 = report.add("I5", "new table in feature_x creates new parquet files")
    i6 = report.add("I6", "main_schema does not see feature_x's new table")
    i7 = report.add("I7", "parquet shared with main survives feature_x drop")

    pg = psycopg.connect(PG_DSN)
    con: duckdb.DuckDBPyConnection | None = None
    try:
        reset_catalog(pg)

        section("Attach DuckLake (fresh)")
        con = connect_duckdb()
        info(f"attached {LAKE} at s3://{S3_BUCKET}/{S3_PREFIX}/")

        section("Create main_schema + events table and populate")
        con.execute("CREATE SCHEMA main_schema")
        con.execute(
            "CREATE TABLE main_schema.events (id INTEGER, name VARCHAR, ts TIMESTAMP)"
        )
        con.execute(
            """
            INSERT INTO main_schema.events VALUES
              (1, 'alpha', TIMESTAMP '2026-04-01 00:00:00'),
              (2, 'beta',  TIMESTAMP '2026-04-02 00:00:00'),
              (3, 'gamma', TIMESTAMP '2026-04-03 00:00:00')
            """
        )
        main_rows = con.execute(
            "SELECT * FROM main_schema.events ORDER BY id"
        ).fetchall()
        info(f"main_schema.events rows: {main_rows}")
        if len(main_rows) == 3:
            i1.status, i1.detail = "pass", f"rows = {main_rows}"
        else:
            i1.status, i1.detail = "fail", f"expected 3 rows, got {len(main_rows)}"

        dump_catalog(pg, "after main_schema.events populated")
        with pg.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.ducklake_data_file")
            cnt = cur.fetchone()[0]
        if cnt >= 1:
            i2.status, i2.detail = "pass", f"ducklake_data_file has {cnt} row(s)"
        else:
            i2.status, i2.detail = "fail", "ducklake_data_file is empty"

        section("Create feature_x schema with empty events table (same DDL)")
        con.execute("CREATE SCHEMA feature_x")
        con.execute(
            "CREATE TABLE feature_x.events (id INTEGER, name VARCHAR, ts TIMESTAMP)"
        )

        dump_catalog(pg, "after feature_x.events created (empty)")

        section("Shallow copy: duplicate data_file rows main -> feature_x")
        # Close the DuckDB connection so it doesn't hold any stale catalog state.
        con.close()
        con = None
        try:
            diag = duplicate_data_files(pg)
            info(f"duplicate diag: {diag}")
        except Exception as e:
            info(f"duplicate_data_files raised: {e!r}")
            traceback.print_exc()
            i3.status = "error"
            i3.detail = f"duplicate failed: {e!r}"

        dump_catalog(pg, "after duplicating data_file rows")

        section("Re-attach DuckDB and query feature_x.events")
        con = connect_duckdb()
        if i3.status != "error":
            try:
                branch_rows = con.execute(
                    "SELECT * FROM feature_x.events ORDER BY id"
                ).fetchall()
                info(f"feature_x.events rows: {branch_rows}")
                if branch_rows == main_rows and len(branch_rows) == 3:
                    i3.status, i3.detail = "pass", f"rows = {branch_rows}"
                else:
                    i3.status, i3.detail = "fail", f"expected {main_rows}, got {branch_rows}"
            except Exception as e:
                i3.status, i3.detail = "error", repr(e)
                traceback.print_exc()

        section("Write a new table into feature_x (not main)")
        files_before_extra = set(list_s3_files())
        try:
            con.execute("CREATE TABLE feature_x.extra (id INTEGER, label VARCHAR)")
            con.execute("INSERT INTO feature_x.extra VALUES (10, 'x'), (20, 'y')")
            files_after_extra = set(list_s3_files())
            new_files = files_after_extra - files_before_extra
            info(f"new files after feature_x.extra: {sorted(new_files)}")
            if new_files:
                i5.status, i5.detail = "pass", f"new parquet: {sorted(new_files)}"
            else:
                i5.status, i5.detail = "fail", "no new parquet files produced"
        except Exception as e:
            i5.status, i5.detail = "error", repr(e)
            traceback.print_exc()

        # I6: main_schema must not see feature_x.extra
        try:
            con.execute("SELECT * FROM main_schema.extra").fetchall()
            i6.status, i6.detail = "fail", "main_schema.extra resolved (should not exist)"
        except duckdb.Error as e:
            i6.status, i6.detail = "pass", f"main cannot see feature_x.extra: {e}"

        section("Collect parquet paths referenced by main_schema before drop")
        main_paths = collect_main_parquet_paths(pg)
        info(f"main_schema parquet paths: {main_paths}")
        s3_before_drop = set(list_s3_files())

        section("Drop schema feature_x CASCADE")
        try:
            con.execute("DROP SCHEMA feature_x CASCADE")
        except Exception as e:
            info(f"drop schema error: {e!r}")

        try:
            main_after = con.execute(
                "SELECT * FROM main_schema.events ORDER BY id"
            ).fetchall()
            if main_after == main_rows:
                i4.status, i4.detail = "pass", f"main unchanged: {main_after}"
            else:
                i4.status, i4.detail = "fail", f"expected {main_rows}, got {main_after}"
        except Exception as e:
            i4.status, i4.detail = "error", repr(e)
            traceback.print_exc()

        s3_after_drop = set(list_s3_files())
        info(f"S3 files: {len(s3_before_drop)} before drop, {len(s3_after_drop)} after")
        missing = [
            p for p in main_paths
            if not any(p.endswith(f) or f.endswith(p.rsplit('/', 1)[-1]) for f in s3_after_drop)
        ]
        if main_paths and not missing:
            i7.status, i7.detail = "pass", (
                f"all {len(main_paths)} main parquet file(s) present after drop"
            )
        elif not main_paths:
            i7.status, i7.detail = "error", "could not collect main parquet paths"
        else:
            i7.status, i7.detail = "fail", f"missing: {missing}"

    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        pg.close()

    section("Results")
    for inv in report.invariants:
        info(f"  [{inv.status.upper():5}] {inv.id}: {inv.description}")
        if inv.detail:
            info(f"    {inv.detail}")

    report.write("spike-results.md")
    info("\nWrote spike-results.md (machine-generated; narrative in spike-findings.md)")

    failed = [i for i in report.invariants if i.status != "pass"]
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
