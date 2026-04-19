"""Write git hooks into a repo's .git/hooks/ directory.

The hooks are thin bash shims that exec the dldbt CLI. We pin the absolute
path to dldbt discovered at install time so the hook still works when the
user's shell PATH doesn't include the project venv (the common case when
git is invoked from an IDE or outside an activated shell)."""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path

from dldbt.errors import DldbtError

HOOK_MARKER = "# @dldbt-managed"

POST_CHECKOUT_HOOK = "post-checkout"
POST_MERGE_HOOK = "post-merge"


class HookInstallError(DldbtError):
    pass


@dataclass(frozen=True)
class HookInstallResult:
    hook: str
    path: Path
    action: str  # 'installed' | 'updated' | 'skipped_foreign'


def find_git_hooks_dir(start: Path) -> Path:
    """Resolve the hooks dir for the repo containing `start`.

    Uses `git rev-parse --git-path hooks` so it handles worktrees and
    custom `core.hooksPath` settings correctly.
    """
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--git-path", "hooks"],
            cwd=start,
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError as e:
        raise HookInstallError("git is not installed or not on PATH") from e
    except subprocess.CalledProcessError as e:
        raise HookInstallError(
            f"not inside a git repository: {start}\n{e.stderr.strip()}"
        ) from e
    # `git rev-parse --git-path` returns a path relative to cwd.
    hooks = (start / out.stdout.strip()).resolve()
    return hooks


def resolve_dldbt_binary() -> str:
    """Absolute path to the dldbt binary we should bake into the hook."""
    found = shutil.which("dldbt")
    if not found:
        raise HookInstallError(
            "could not find `dldbt` on PATH. Activate your venv (or run "
            "`uv sync`) before installing hooks."
        )
    return str(Path(found).resolve())


def render_post_checkout(dldbt_bin: str, project_root: Path) -> str:
    return _render(
        dldbt_bin=dldbt_bin,
        project_root=project_root,
        subcommand="__post-checkout",
        pass_args='"$1" "$2" "$3"',
        extra_guard='if [ "$3" != "1" ]; then exit 0; fi',
    )


def render_post_merge(dldbt_bin: str, project_root: Path) -> str:
    return _render(
        dldbt_bin=dldbt_bin,
        project_root=project_root,
        subcommand="__post-merge",
        pass_args='"$1"',
        extra_guard="",
    )


def _render(
    *,
    dldbt_bin: str,
    project_root: Path,
    subcommand: str,
    pass_args: str,
    extra_guard: str,
) -> str:
    guard = f"{extra_guard}\n" if extra_guard else ""
    return f"""#!/usr/bin/env bash
{HOOK_MARKER}
# Installed by `dldbt install-hooks`. Delete or overwrite to disable.
set -eu
{guard}DLDBT_BIN={_shell_quote(dldbt_bin)}
if [ ! -x "$DLDBT_BIN" ]; then
    # dldbt moved or venv was deleted; skip rather than break the checkout.
    exit 0
fi
cd {_shell_quote(str(project_root))}
exec "$DLDBT_BIN" {subcommand} {pass_args}
"""


def _shell_quote(value: str) -> str:
    # POSIX-safe single-quote escape: close quote, insert escaped quote, reopen.
    return "'" + value.replace("'", "'\\''") + "'"


def install_hook(
    hooks_dir: Path, hook_name: str, body: str, *, force: bool = False
) -> HookInstallResult:
    path = hooks_dir / hook_name
    action: str
    if path.exists():
        existing = path.read_text()
        if HOOK_MARKER in existing or force:
            action = "updated"
        else:
            return HookInstallResult(hook=hook_name, path=path, action="skipped_foreign")
    else:
        action = "installed"
    hooks_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    mode = path.stat().st_mode
    path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return HookInstallResult(hook=hook_name, path=path, action=action)


def install_all_hooks(
    *,
    project_root: Path,
    force: bool = False,
) -> list[HookInstallResult]:
    dldbt_bin = resolve_dldbt_binary()
    hooks_dir = find_git_hooks_dir(project_root)
    project_root = project_root.resolve()
    results = [
        install_hook(
            hooks_dir,
            POST_CHECKOUT_HOOK,
            render_post_checkout(dldbt_bin, project_root),
            force=force,
        ),
        install_hook(
            hooks_dir,
            POST_MERGE_HOOK,
            render_post_merge(dldbt_bin, project_root),
            force=force,
        ),
    ]
    return results


def current_git_branch(cwd: Path) -> str | None:
    """The short name of the currently checked-out branch, or None for
    detached HEAD / non-repo."""
    try:
        out = subprocess.run(
            ["git", "symbolic-ref", "--short", "-q", "HEAD"],
            cwd=cwd,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None
    if out.returncode != 0:
        return None
    name = out.stdout.strip()
    return name or None


def is_branch_checkout(prev_head: str, new_head: str, flag: str) -> bool:
    """git's post-checkout passes '1' as the third arg for branch checkouts,
    '0' for file checkouts. prev_head and new_head often equal each other for
    `git checkout -b` (new branch starts at current HEAD), so we don't filter
    on that; idempotency is handled by checking the branch registry."""
    del prev_head, new_head
    return flag == "1"


def assert_executable(path: Path) -> None:
    """Test helper: raises if `path` doesn't have the user-execute bit set."""
    st = path.stat()
    if not st.st_mode & stat.S_IXUSR:
        raise AssertionError(f"{path} is not executable (mode={oct(st.st_mode)})")


# Re-export for tests that want to check the os.access variant.
def is_executable(path: Path) -> bool:
    return os.access(path, os.X_OK)
