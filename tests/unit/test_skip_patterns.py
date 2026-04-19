from __future__ import annotations

import pytest

from dldbt.git_ops.match import branch_matches_any


@pytest.mark.parametrize(
    ("branch", "patterns", "expected"),
    [
        ("main", ["main"], True),
        ("main", ["master"], False),
        ("main", ["main", "master"], True),
        # fnmatch's '*' matches slashes, so nested paths under release/ still
        # match. Document that explicitly here.
        ("release/2026.04", ["release/*"], True),
        ("release/2026/04", ["release/*"], True),
        ("feature/foo", ["release/*"], False),
        ("feature/foo", ["feat*"], True),
        ("feature/foo", ["FEATURE/*"], False),  # case sensitive
        ("hotfix-123", ["hotfix-*"], True),
        ("anything", [], False),
    ],
)
def test_branch_matches_any(
    branch: str, patterns: list[str], expected: bool
) -> None:
    assert branch_matches_any(branch, patterns) is expected
