from __future__ import annotations

import re

from dldbt.errors import InvalidBranchNameError

_NON_IDENT_CHARS = re.compile(r"[^a-z0-9_]")
_MULTI_UNDERSCORE = re.compile(r"_+")


def sanitize_branch_name(git_branch: str) -> str:
    """Map a git branch name to a valid DuckLake schema identifier.

    Rules:
      - lowercase
      - replace every non-alphanumeric char with '_'
      - collapse runs of '_'
      - trim leading/trailing '_'
      - if the result starts with a digit, prefix 'b_'
      - empty input → InvalidBranchNameError

    Collisions (e.g. 'feature/foo' and 'feature-foo' both → 'feature_foo') are
    possible; the catalog enforces uniqueness via the branches PK.
    """
    if git_branch is None or not git_branch.strip():
        raise InvalidBranchNameError("branch name is empty")
    lowered = git_branch.strip().lower()
    replaced = _NON_IDENT_CHARS.sub("_", lowered)
    collapsed = _MULTI_UNDERSCORE.sub("_", replaced).strip("_")
    if not collapsed:
        raise InvalidBranchNameError(f"branch name {git_branch!r} sanitizes to empty")
    if collapsed[0].isdigit():
        collapsed = f"b_{collapsed}"
    return collapsed
