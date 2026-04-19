from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class BranchRecord:
    name: str
    git_branch: str
    created_at: datetime
    created_from: str
    base_snapshot_id: int
    last_git_commit: str | None
    status: str  # 'active' | 'merged' | 'abandoned'


@dataclass(frozen=True)
class TableInfo:
    schema_name: str
    table_name: str
    record_count: int | None
    file_size_bytes: int | None


class CatalogAdapter(Protocol):
    """Interface between dlgit and whatever DuckLake-like catalog is in use.

    A Phase 1 implementation targets the ducklake extension with a Postgres
    catalog and any object store DuckDB can reach. A future implementation
    can front DuckLake's native branching (when it ships) without changing
    callers.
    """

    # --- setup ---
    def init(self) -> None:
        """Ensure the catalog is ready for dlgit. Creates dlgit_meta schema
        and registers the main branch. Idempotent."""

    def is_initialized(self) -> bool:
        """True if `dlgit init` has been run against this catalog."""

    # --- branch registry ---
    def register_branch(
        self,
        *,
        name: str,
        git_branch: str,
        created_from: str,
        base_snapshot_id: int,
        last_git_commit: str | None,
    ) -> BranchRecord: ...

    def list_branches(self) -> list[BranchRecord]: ...

    def get_branch(self, name: str) -> BranchRecord | None: ...

    def update_branch_status(self, name: str, status: str) -> None: ...

    # --- snapshot state ---
    def current_snapshot_id(self) -> int: ...

    # --- branch operations (the Phase 0 mechanic) ---
    def shallow_copy_schema(self, src: str, dst: str) -> int:
        """Create `dst` as a shallow copy of `src`: every table in `src`
        reappears in `dst` referencing the same parquet files. Returns the
        number of tables copied."""

    def drop_schema(self, name: str) -> None:
        """Drop the schema and all its tables. Does NOT delete parquet
        files shared with other schemas; parquet GC is a separate phase."""

    def list_tables(self, schema: str) -> list[TableInfo]: ...

    def close(self) -> None: ...
