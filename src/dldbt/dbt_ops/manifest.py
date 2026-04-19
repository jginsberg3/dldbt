"""Local manifest storage.

dbt writes its build artifacts (manifest.json, run_results.json, ...) under
`<dbt_project>/target/`. For `--defer --state` on a feature branch to work,
we need a persistent pointer to main's last manifest *directory* (dbt wants
a directory containing manifest.json, not the file itself).

Phase 3 stores manifests locally under `.dldbt/manifests/<branch>/latest/`.
A shared (S3-backed) variant is planned for a later phase once multi-user
support lands."""

from __future__ import annotations

import shutil
from pathlib import Path

DLDBT_DIR = ".dldbt"
MANIFESTS_SUBDIR = "manifests"
LATEST_NAME = "latest"


def manifests_root(project_root: Path) -> Path:
    return project_root / DLDBT_DIR / MANIFESTS_SUBDIR


def branch_manifest_dir(project_root: Path, branch: str) -> Path:
    """Path to the directory dbt should read when passed as `--state`."""
    return manifests_root(project_root) / branch / LATEST_NAME


def save_manifest(
    *, project_root: Path, branch: str, target_dir: Path
) -> Path | None:
    """Copy dbt's `target/` artifacts into .dldbt/manifests/<branch>/latest/.

    Returns the destination path, or None if `target_dir/manifest.json`
    doesn't exist (e.g. the dbt run failed before writing anything)."""
    src_manifest = target_dir / "manifest.json"
    if not src_manifest.exists():
        return None
    dst = branch_manifest_dir(project_root, branch)
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True, exist_ok=True)
    # Copy the whole target dir so run_results.json + graph files travel too;
    # dbt's `--state` reader pulls from several files, not just manifest.
    for name in ("manifest.json", "run_results.json", "graph.gpickle"):
        src = target_dir / name
        if src.exists():
            shutil.copy2(src, dst / name)
    return dst
