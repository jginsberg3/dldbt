"""Subprocess wrapper around the `dbt` CLI.

The wrapper's job is to:
  1. Ensure a branch schema exists (caller does this before invoking).
  2. Set DLDBT_BRANCH so our generate_schema_name macro lands models in
     the branch schema.
  3. On non-main branches with a known main manifest, add
     `--defer --state <main_manifest> --select state:modified+` so only
     changed models + their downstream rebuild. Callers can pass `--full`
     to skip the auto-injection.
  4. After a successful run, copy `target/manifest.json` into
     `.dldbt/manifests/<branch>/latest/`.

Only `run`, `build`, `test`, `compile` participate in this auto-wiring.
Other dbt subcommands pass through with just DLDBT_BRANCH set."""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

from dldbt.dbt_ops.manifest import branch_manifest_dir, save_manifest
from dldbt.errors import DldbtError

WRAPPED_SUBCOMMANDS = frozenset({"run", "build", "test", "compile"})

# dbt CLI flags we should not stomp on if the user already supplied them.
_STATE_FLAGS = {"--state", "--defer-state"}
_SELECT_FLAGS = {"-s", "--select", "--models", "-m", "--exclude"}


class DbtRunnerError(DldbtError):
    pass


@dataclass(frozen=True)
class DbtRunPlan:
    argv: list[str]
    env: dict[str, str]
    target_dir: Path
    subcommand: str
    deferred_against: Path | None  # manifest dir we passed as --state, if any


def build_run_plan(
    *,
    subcommand: str,
    user_args: list[str],
    project_root: Path,
    branch_schema: str,
    is_main_branch: bool,
    main_manifest_dir: Path | None,
    full: bool,
) -> DbtRunPlan:
    """Assemble argv + env for a dbt invocation.

    `project_root` is the dbt project dir (where dbt_project.yml lives).
    `branch_schema` goes into DLDBT_BRANCH.
    `main_manifest_dir` is the .dldbt/manifests/<main>/latest/ path; we use
    it for `--defer --state` on feature branches when it exists."""
    argv: list[str] = ["dbt", subcommand, *user_args]
    env = dict(os.environ)
    env["DLDBT_BRANCH"] = branch_schema

    # target-path so we always know where to find the resulting manifest.
    target_dir = project_root / "target"

    deferred_against: Path | None = None
    should_wire_defer = (
        subcommand in WRAPPED_SUBCOMMANDS
        and not is_main_branch
        and not full
        and main_manifest_dir is not None
        and main_manifest_dir.exists()
    )
    if should_wire_defer:
        assert main_manifest_dir is not None
        if not _user_set_any(user_args, _STATE_FLAGS):
            argv += ["--defer", "--state", str(main_manifest_dir)]
            deferred_against = main_manifest_dir
        if not _user_set_any(user_args, _SELECT_FLAGS):
            argv += ["--select", "state:modified+"]

    return DbtRunPlan(
        argv=argv,
        env=env,
        target_dir=target_dir,
        subcommand=subcommand,
        deferred_against=deferred_against,
    )


def execute(
    plan: DbtRunPlan,
    *,
    project_root: Path,
    branch_schema: str,
    profiles_dir: Path | None = None,
) -> int:
    """Run the planned dbt invocation; save manifest on success."""
    if shutil.which(plan.argv[0]) is None:
        raise DbtRunnerError(
            "dbt is not on PATH. Install dbt-duckdb in the project's venv."
        )
    argv = list(plan.argv)
    if profiles_dir is not None and "--profiles-dir" not in argv:
        argv += ["--profiles-dir", str(profiles_dir)]
    proc = subprocess.run(argv, cwd=project_root, env=plan.env, check=False)
    if proc.returncode == 0 and plan.subcommand in WRAPPED_SUBCOMMANDS:
        save_manifest(
            project_root=project_root,
            branch=branch_schema,
            target_dir=plan.target_dir,
        )
    return proc.returncode


def resolve_main_manifest_dir(
    project_root: Path, main_branch_schema: str
) -> Path | None:
    path = branch_manifest_dir(project_root, main_branch_schema)
    return path if path.exists() else None


def _user_set_any(args: list[str], flag_names: set[str]) -> bool:
    """True if the user already passed any of these flags (or the
    `--foo=bar` form)."""
    for token in args:
        if token in flag_names:
            return True
        if "=" in token and token.split("=", 1)[0] in flag_names:
            return True
    return False
