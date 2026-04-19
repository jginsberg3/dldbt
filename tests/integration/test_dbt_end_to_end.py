"""Phase 3 end-to-end: the dldbt exit criterion.

Flow:
  1. Copy the dbt_mini fixture into tmp_path; `git init` + configure identity.
  2. Write a .dldbt.yml and run `dldbt init` (writes profiles.yml).
  3. Run `dldbt install-macro` so generate_schema_name lands in the project.
  4. Build on main: `dldbt dbt seed` + `dldbt dbt build` — baseline manifest.
  5. `dldbt branch create feature/x` — shallow-copy into feat_x schema.
  6. `git checkout -b feature/x` so current_git_branch() returns the right thing.
  7. Edit mid_events.sql, then `dldbt dbt build`.
  8. Parse run_results.json: only mid_events + fct_events should be listed.

This exercises the whole Phase 3 surface against real docker services."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from dldbt.config import Config

pytestmark = pytest.mark.integration

FIXTURE = Path(__file__).parent / "fixtures" / "dbt_mini"


def _run(
    cmd: list[str], *, cwd: Path, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess:
    result = subprocess.run(
        cmd, cwd=cwd, env=env, capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        msg = (
            f"command failed: {' '.join(cmd)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        raise AssertionError(msg)
    return result


def _write_config(project: Path, config: Config) -> Path:
    data = {
        "catalog": {"dsn": config.catalog.dsn},
        "storage": {
            "data_path": config.storage.data_path,
            "s3": {
                "endpoint": config.storage.s3.endpoint,
                "access_key_id": config.storage.s3.access_key_id,
                "secret_access_key": config.storage.s3.secret_access_key,
                "url_style": config.storage.s3.url_style,
                "use_ssl": config.storage.s3.use_ssl,
            },
        },
        "main_branch": config.main_branch,
        "lake_alias": config.lake_alias,
    }
    path = project / ".dldbt.yml"
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    return path


def _git_init(project: Path) -> None:
    _run(["git", "init", "-q", "-b", "main"], cwd=project)
    _run(["git", "config", "user.email", "test@dldbt.invalid"], cwd=project)
    _run(["git", "config", "user.name", "dldbt test"], cwd=project)
    _run(["git", "add", "."], cwd=project)
    _run(["git", "commit", "-q", "-m", "initial"], cwd=project)


def _run_results(project: Path) -> dict:
    return json.loads((project / "target" / "run_results.json").read_text())


def _executed_models(project: Path) -> set[str]:
    """Names of models that were actually executed (not 'skipped') in the last
    dbt run. dbt's run_results reports a `status` per node; skipped defer nodes
    have status='skipped'."""
    rr = _run_results(project)
    executed: set[str] = set()
    for r in rr["results"]:
        if r.get("status") == "skipped":
            continue
        uid = r["unique_id"]
        # unique_id like 'model.dldbt_mini.mid_events'
        if uid.startswith("model."):
            executed.add(uid.rsplit(".", 1)[-1])
    return executed


def test_feature_branch_rebuilds_only_modified_and_downstream(
    tmp_path: Path,
    clean_catalog: None,
    integration_config: Config,
) -> None:
    project = tmp_path / "dbt_mini"
    shutil.copytree(FIXTURE, project)
    _write_config(project, integration_config)
    _git_init(project)

    # 1. init dldbt (creates main schema + writes profiles.yml)
    _run(["dldbt", "init", "--project", str(project)], cwd=project)
    assert (project / "profiles.yml").exists()

    # 2. install the schema macro
    _run(["dldbt", "install-macro", "--project", str(project)], cwd=project)
    assert (project / "macros" / "dldbt_schema.sql").exists()

    # 3. seed + build on main
    _run(["dldbt", "dbt", "seed", "--project", str(project)], cwd=project)
    _run(["dldbt", "dbt", "build", "--project", str(project)], cwd=project)
    manifest_dir = project / ".dldbt" / "manifests" / "main" / "latest"
    assert (manifest_dir / "manifest.json").exists(), (
        "main build should have saved a manifest for defer"
    )

    # 4. create feature branch schema + switch git onto it
    _run(["dldbt", "branch", "create", "feature/x"], cwd=project)
    _run(["git", "checkout", "-q", "-b", "feature/x"], cwd=project)

    # 5. modify the middle model
    mid = project / "models" / "mid_events.sql"
    mid.write_text(
        "select\n"
        "    id,\n"
        "    upper(name) as name_upper,\n"
        "    lower(name) as name_lower,\n"
        "    event_ts\n"
        "from {{ ref('stg_events') }}\n"
    )

    # 6. rebuild on the feature branch — defer should kick in
    _run(["dldbt", "dbt", "build", "--project", str(project)], cwd=project)

    executed = _executed_models(project)
    assert "mid_events" in executed, (
        f"modified model should rebuild; got {executed}"
    )
    assert "fct_events" in executed, (
        f"downstream of modified should rebuild; got {executed}"
    )
    assert "stg_events" not in executed, (
        f"upstream of modified should be deferred / skipped; got {executed}"
    )


def test_full_flag_forces_rebuild_of_everything(
    tmp_path: Path,
    clean_catalog: None,
    integration_config: Config,
) -> None:
    project = tmp_path / "dbt_mini"
    shutil.copytree(FIXTURE, project)
    _write_config(project, integration_config)
    _git_init(project)

    _run(["dldbt", "init", "--project", str(project)], cwd=project)
    _run(["dldbt", "install-macro", "--project", str(project)], cwd=project)
    _run(["dldbt", "dbt", "seed", "--project", str(project)], cwd=project)
    _run(["dldbt", "dbt", "build", "--project", str(project)], cwd=project)

    _run(["dldbt", "branch", "create", "feature/y"], cwd=project)
    _run(["git", "checkout", "-q", "-b", "feature/y"], cwd=project)

    # Touch a model so state:modified+ would otherwise skip the others.
    # Keep the output schema stable so downstream still compiles.
    (project / "models" / "mid_events.sql").write_text(
        "select id, upper(name) as name_upper, event_ts "
        "from {{ ref('stg_events') }}\n"
    )

    # --full should bypass the selector and rebuild everything
    _run(
        ["dldbt", "dbt", "build", "--full", "--project", str(project)],
        cwd=project,
    )

    executed = _executed_models(project)
    assert {"stg_events", "mid_events", "fct_events"}.issubset(executed), (
        f"--full should rebuild all models; got {executed}"
    )
