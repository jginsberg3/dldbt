from __future__ import annotations

import pytest

from dlgit.errors import InvalidBranchNameError
from dlgit.git_ops.branch import sanitize_branch_name


@pytest.mark.parametrize(
    ("git_name", "expected"),
    [
        ("main", "main"),
        ("feature/foo-bar", "feature_foo_bar"),
        ("FEATURE/Foo-BAR", "feature_foo_bar"),
        ("release/2026.04.19", "release_2026_04_19"),
        ("user/alice/experiment_1", "user_alice_experiment_1"),
        ("123-hotfix", "b_123_hotfix"),
        ("--prefix--", "prefix"),
        ("hot__fix", "hot_fix"),
        ("a/b/c", "a_b_c"),
    ],
)
def test_sanitize_branch_name(git_name: str, expected: str) -> None:
    assert sanitize_branch_name(git_name) == expected


@pytest.mark.parametrize("bad", ["", "   ", "///", "----"])
def test_sanitize_rejects_empty_or_all_punctuation(bad: str) -> None:
    with pytest.raises(InvalidBranchNameError):
        sanitize_branch_name(bad)


def test_sanitize_is_idempotent() -> None:
    once = sanitize_branch_name("feature/foo")
    twice = sanitize_branch_name(once)
    assert once == twice == "feature_foo"
