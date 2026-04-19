"""Phase 2 integration tests: install hooks into a real git repo and drive
a full checkout lifecycle against the live docker catalog."""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml

from dldbt.catalog.ducklake_pg import DuckLakePgAdapter
from dldbt.config import Config

pytestmark = pytest.mark.integration


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(cwd), *args], check=True)


def _write_config(project_root: Path, config: Config, auto_create: bool) -> Path:
    """Serialize the runtime Config (plus auto_create overrides) to
    .dldbt.yml under project_root."""
    body: dict = {
        "catalog": {"dsn": config.catalog.dsn},
        "storage": {"data_path": config.storage.data_path},
        "main_branch": config.main_branch,
        "protected_branches": list(config.protected_branches),
        "lake_alias": config.lake_alias,
        "auto_create": {
            "enabled": auto_create,
            "skip_patterns": ["main", "master", "release/*"],
        },
    }
    if config.storage.s3 is not None:
        s3 = config.storage.s3
        body["storage"]["s3"] = {
            "endpoint": s3.endpoint,
            "region": s3.region,
            "access_key_id": s3.access_key_id,
            "secret_access_key": s3.secret_access_key,
            "url_style": s3.url_style,
            "use_ssl": s3.use_ssl,
        }
    path = project_root / ".dldbt.yml"
    path.write_text(yaml.safe_dump(body))
    return path


@pytest.fixture
def git_env() -> dict[str, str]:
    """Minimum env needed for `git commit` to succeed in CI-ish shells."""
    return {
        **os.environ,
        "GIT_AUTHOR_NAME": "dldbt-test",
        "GIT_AUTHOR_EMAIL": "test@dldbt.invalid",
        "GIT_COMMITTER_NAME": "dldbt-test",
        "GIT_COMMITTER_EMAIL": "test@dldbt.invalid",
    }


@pytest.fixture
def repo(tmp_path: Path, git_env: dict[str, str]) -> Iterator[Path]:
    root = tmp_path / "project"
    root.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main", str(root)], check=True)
    # An initial commit so checkout can move HEAD later.
    (root / "README").write_text("seed\n")
    subprocess.run(
        ["git", "-C", str(root), "add", "README"], check=True
    )
    subprocess.run(
        ["git", "-C", str(root), "commit", "-q", "-m", "init"],
        env=git_env,
        check=True,
    )
    yield root


def _run_dldbt(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    """Invoke the installed dldbt CLI from the given cwd."""
    bin_path = shutil.which("dldbt")
    assert bin_path, "dldbt not on PATH — activate the venv"
    return subprocess.run(
        [bin_path, *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


def test_install_hooks_writes_both(repo: Path) -> None:
    result = _run_dldbt(["install-hooks"], cwd=repo)
    assert result.returncode == 0, result.stderr
    for name in ("post-checkout", "post-merge"):
        h = repo / ".git" / "hooks" / name
        assert h.exists()
        assert h.stat().st_mode & stat.S_IXUSR
        assert "@dldbt-managed" in h.read_text()


def test_checkout_with_auto_create_disabled_does_nothing(
    repo: Path, seeded_main: Config, git_env: dict[str, str]
) -> None:
    _write_config(repo, seeded_main, auto_create=False)
    _run_dldbt(["init"], cwd=repo)
    _run_dldbt(["install-hooks"], cwd=repo)

    subprocess.run(
        ["git", "-C", str(repo), "checkout", "-q", "-b", "feature/off"],
        env=git_env,
        check=True,
    )

    with DuckLakePgAdapter(seeded_main) as adapter:
        names = {b.name for b in adapter.list_branches()}
    assert "feature_off" not in names


def test_checkout_with_auto_create_creates_schema(
    repo: Path, seeded_main: Config, git_env: dict[str, str]
) -> None:
    _write_config(repo, seeded_main, auto_create=True)
    _run_dldbt(["init"], cwd=repo)
    _run_dldbt(["install-hooks"], cwd=repo)

    result = subprocess.run(
        ["git", "-C", str(repo), "checkout", "-q", "-b", "feature/auto"],
        env=git_env,
        check=True,
        capture_output=True,
        text=True,
    )
    # Hook output surfaces via the hook's own stdout (git passes it through
    # under -q, but it's visible under default verbosity).
    assert result.returncode == 0

    with DuckLakePgAdapter(seeded_main) as adapter:
        names = {b.name for b in adapter.list_branches()}
        assert "feature_auto" in names
        branch = adapter.get_branch("feature_auto")
        assert branch is not None
        assert branch.git_branch == "feature/auto"
        tables = {t.table_name for t in adapter.list_tables("feature_auto")}
    assert tables == {"events"}


def test_checkout_matching_skip_pattern_is_no_op(
    repo: Path, seeded_main: Config, git_env: dict[str, str]
) -> None:
    _write_config(repo, seeded_main, auto_create=True)
    _run_dldbt(["init"], cwd=repo)
    _run_dldbt(["install-hooks"], cwd=repo)

    # release/* is in the default skip list.
    subprocess.run(
        ["git", "-C", str(repo), "checkout", "-q", "-b", "release/2026.04"],
        env=git_env,
        check=True,
    )

    with DuckLakePgAdapter(seeded_main) as adapter:
        names = {b.name for b in adapter.list_branches()}
    assert "release_2026_04" not in names


def test_checkout_is_idempotent(
    repo: Path, seeded_main: Config, git_env: dict[str, str]
) -> None:
    _write_config(repo, seeded_main, auto_create=True)
    _run_dldbt(["init"], cwd=repo)
    _run_dldbt(["install-hooks"], cwd=repo)

    for _ in range(2):
        subprocess.run(
            ["git", "-C", str(repo), "checkout", "-q", "-B", "feature/twice"],
            env=git_env,
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(repo), "checkout", "-q", "main"],
            env=git_env,
            check=True,
        )
    # No crash and only one row in the registry.
    with DuckLakePgAdapter(seeded_main) as adapter:
        matches = [b for b in adapter.list_branches() if b.name == "feature_twice"]
    assert len(matches) == 1


def test_detached_head_checkout_skips(
    repo: Path, seeded_main: Config, git_env: dict[str, str]
) -> None:
    _write_config(repo, seeded_main, auto_create=True)
    _run_dldbt(["init"], cwd=repo)
    _run_dldbt(["install-hooks"], cwd=repo)

    sha = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    subprocess.run(
        ["git", "-C", str(repo), "checkout", "-q", "--detach", sha],
        env=git_env,
        check=True,
    )
    # No crash, and nothing new registered.
    with DuckLakePgAdapter(seeded_main) as adapter:
        names = {b.name for b in adapter.list_branches()}
    assert names == {"main"}
