from __future__ import annotations

import fnmatch
from collections.abc import Iterable


def branch_matches_any(git_branch: str, patterns: Iterable[str]) -> bool:
    """True if `git_branch` matches any glob in `patterns` (fnmatch syntax).

    Matching is case-sensitive and applies to the raw git ref name, not the
    sanitized schema name. A pattern of "main" matches exactly "main"; a
    pattern of "release/*" matches "release/2026.04" but not "release/a/b".
    """
    return any(fnmatch.fnmatchcase(git_branch, p) for p in patterns)
