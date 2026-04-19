from __future__ import annotations

from pathlib import Path

from dldbt.dbt_ops.runner import build_run_plan


def _plan(
    *,
    subcommand: str = "run",
    user_args: list[str] | None = None,
    is_main: bool = False,
    main_manifest_dir: Path | None = Path("/tmp/.dldbt/manifests/main/latest"),
    full: bool = False,
):
    return build_run_plan(
        subcommand=subcommand,
        user_args=list(user_args or []),
        project_root=Path("/tmp/proj"),
        branch_schema="feat_x",
        is_main_branch=is_main,
        main_manifest_dir=main_manifest_dir,
        full=full,
    )


def test_feature_branch_auto_injects_defer_and_select(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "main_manifest"
    manifest_dir.mkdir()
    plan = _plan(main_manifest_dir=manifest_dir)
    assert "--defer" in plan.argv
    assert "--state" in plan.argv
    assert str(manifest_dir) in plan.argv
    assert "--select" in plan.argv
    assert "state:modified+" in plan.argv
    assert plan.deferred_against == manifest_dir
    assert plan.env["DLDBT_BRANCH"] == "feat_x"


def test_main_branch_no_defer_injection(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "main_manifest"
    manifest_dir.mkdir()
    plan = _plan(is_main=True, main_manifest_dir=manifest_dir)
    assert "--defer" not in plan.argv
    assert "--state" not in plan.argv
    assert plan.deferred_against is None


def test_full_flag_skips_injection(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "main_manifest"
    manifest_dir.mkdir()
    plan = _plan(full=True, main_manifest_dir=manifest_dir)
    assert "--defer" not in plan.argv
    assert "state:modified+" not in plan.argv


def test_no_manifest_dir_skips_injection() -> None:
    plan = _plan(main_manifest_dir=None)
    assert "--defer" not in plan.argv
    assert plan.deferred_against is None


def test_missing_manifest_dir_skips_injection(tmp_path: Path) -> None:
    # Path passed but the directory does not exist on disk
    plan = _plan(main_manifest_dir=tmp_path / "does_not_exist")
    assert "--defer" not in plan.argv


def test_user_select_flag_is_respected(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "m"
    manifest_dir.mkdir()
    plan = _plan(
        user_args=["--select", "my_model+"],
        main_manifest_dir=manifest_dir,
    )
    # defer still injected (user didn't touch --state)
    assert "--defer" in plan.argv
    assert "--state" in plan.argv
    # but our auto-select should NOT be appended on top of the user's
    assert plan.argv.count("--select") == 1
    assert "state:modified+" not in plan.argv


def test_user_state_flag_is_respected(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "m"
    manifest_dir.mkdir()
    plan = _plan(
        user_args=["--state", "/elsewhere"],
        main_manifest_dir=manifest_dir,
    )
    # --state present (from user) but we did not add --defer on top
    assert "--defer" not in plan.argv
    assert plan.deferred_against is None


def test_user_equals_form_flag_is_respected(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "m"
    manifest_dir.mkdir()
    plan = _plan(
        user_args=["--select=my_model"],
        main_manifest_dir=manifest_dir,
    )
    assert "state:modified+" not in plan.argv


def test_non_wrapped_subcommand_passes_through_with_env_only() -> None:
    plan = _plan(subcommand="seed")
    assert plan.env["DLDBT_BRANCH"] == "feat_x"
    assert "--defer" not in plan.argv
    assert plan.argv[:2] == ["dbt", "seed"]


def test_argv_preserves_user_args_order(tmp_path: Path) -> None:
    plan = _plan(
        subcommand="build",
        user_args=["--threads", "4"],
        main_manifest_dir=None,
    )
    # user flags must come before any injected ones (they come after `dbt build`)
    assert plan.argv[:4] == ["dbt", "build", "--threads", "4"]
