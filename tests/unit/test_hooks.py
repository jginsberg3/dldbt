from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

from dldbt.git_ops import hooks as hook_ops


@pytest.fixture
def temp_git_repo(tmp_path: Path) -> Path:
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(tmp_path)], check=True
    )
    return tmp_path


def test_find_git_hooks_dir_inside_repo(temp_git_repo: Path) -> None:
    hooks = hook_ops.find_git_hooks_dir(temp_git_repo)
    assert hooks.is_absolute()
    assert hooks.name == "hooks"
    assert hooks.parent == (temp_git_repo / ".git").resolve()


def test_find_git_hooks_dir_outside_repo(tmp_path: Path) -> None:
    with pytest.raises(hook_ops.HookInstallError, match="not inside a git"):
        hook_ops.find_git_hooks_dir(tmp_path)


def test_render_post_checkout_contains_marker_and_guards(tmp_path: Path) -> None:
    script = hook_ops.render_post_checkout("/opt/dldbt/bin/dldbt", tmp_path)
    assert script.startswith("#!/usr/bin/env bash\n")
    assert hook_ops.HOOK_MARKER in script
    assert 'if [ "$3" != "1" ]; then exit 0; fi' in script
    assert "__post-checkout" in script
    assert "'/opt/dldbt/bin/dldbt'" in script


def test_render_post_merge_has_no_branch_guard(tmp_path: Path) -> None:
    script = hook_ops.render_post_merge("/opt/dldbt/bin/dldbt", tmp_path)
    assert 'if [ "$3" != "1" ]' not in script
    assert "__post-merge" in script


def test_render_escapes_single_quotes(tmp_path: Path) -> None:
    odd = "/tmp/has 'quote'/dldbt"
    script = hook_ops.render_post_checkout(odd, tmp_path)
    # Sanity: bash can parse the quoted literal without errors.
    subprocess.run(["bash", "-n", "-c", script], check=True)


def test_install_hook_fresh(tmp_path: Path) -> None:
    hooks_dir = tmp_path / "hooks"
    result = hook_ops.install_hook(
        hooks_dir, "post-checkout", "#!/bin/sh\n# @dldbt-managed\n:\n"
    )
    assert result.action == "installed"
    assert result.path.exists()
    assert result.path.stat().st_mode & stat.S_IXUSR
    assert hook_ops.is_executable(result.path)


def test_install_hook_updates_our_own(tmp_path: Path) -> None:
    hooks_dir = tmp_path / "hooks"
    hook_ops.install_hook(
        hooks_dir, "post-checkout", "#!/bin/sh\n# @dldbt-managed\nold\n"
    )
    result = hook_ops.install_hook(
        hooks_dir, "post-checkout", "#!/bin/sh\n# @dldbt-managed\nnew\n"
    )
    assert result.action == "updated"
    assert "new" in result.path.read_text()


def test_install_hook_skips_foreign(tmp_path: Path) -> None:
    hooks_dir = tmp_path / "hooks"
    hooks_dir.mkdir()
    existing = hooks_dir / "post-checkout"
    existing.write_text("#!/bin/sh\necho pre-existing user hook\n")
    result = hook_ops.install_hook(
        hooks_dir, "post-checkout", "#!/bin/sh\n# @dldbt-managed\n:\n"
    )
    assert result.action == "skipped_foreign"
    assert "user hook" in existing.read_text()


def test_install_hook_force_overwrites_foreign(tmp_path: Path) -> None:
    hooks_dir = tmp_path / "hooks"
    hooks_dir.mkdir()
    existing = hooks_dir / "post-checkout"
    existing.write_text("#!/bin/sh\necho user hook\n")
    result = hook_ops.install_hook(
        hooks_dir,
        "post-checkout",
        "#!/bin/sh\n# @dldbt-managed\n:\n",
        force=True,
    )
    assert result.action == "updated"
    assert "dldbt-managed" in existing.read_text()


def test_install_all_hooks_roundtrip(temp_git_repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Create a fake 'dldbt' on PATH so resolve_dldbt_binary succeeds.
    fake_bin = temp_git_repo / "fakebin"
    fake_bin.mkdir()
    fake_dldbt = fake_bin / "dldbt"
    fake_dldbt.write_text("#!/bin/sh\nexit 0\n")
    fake_dldbt.chmod(fake_dldbt.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("PATH", f"{fake_bin}{os.pathsep}{os.environ['PATH']}")

    results = hook_ops.install_all_hooks(project_root=temp_git_repo)
    names = {r.hook for r in results}
    assert names == {"post-checkout", "post-merge"}
    for r in results:
        assert r.action == "installed"
        assert r.path.exists()
        assert hook_ops.is_executable(r.path)


def test_is_branch_checkout() -> None:
    assert hook_ops.is_branch_checkout("a" * 40, "b" * 40, "1") is True
    assert hook_ops.is_branch_checkout("a" * 40, "b" * 40, "0") is False
    # `git checkout -b` produces prev == new; this should still be treated
    # as a branch checkout so the hook can create the new branch.
    assert hook_ops.is_branch_checkout("a" * 40, "a" * 40, "1") is True


def test_current_git_branch(temp_git_repo: Path) -> None:
    # Fresh repo with no commits has an 'unborn' HEAD: symbolic-ref works.
    assert hook_ops.current_git_branch(temp_git_repo) == "main"


def test_current_git_branch_detached(temp_git_repo: Path) -> None:
    # Commit and then detach.
    subprocess.run(
        ["git", "-C", str(temp_git_repo), "commit",
         "--allow-empty", "-m", "init", "-q"],
        env={**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
             "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"},
        check=True,
    )
    sha = subprocess.run(
        ["git", "-C", str(temp_git_repo), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    subprocess.run(
        ["git", "-C", str(temp_git_repo), "checkout", "-q", "--detach", sha],
        check=True,
    )
    assert hook_ops.current_git_branch(temp_git_repo) is None
