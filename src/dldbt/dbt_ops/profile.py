"""Render a dbt-duckdb profiles.yml from a dldbt Config.

dldbt wants to be the single source of truth for catalog + storage config.
Rather than ask users to keep .dldbt.yml and profiles.yml in sync, we
generate the profile. dbt-duckdb 1.10+ has native DuckLake attach support
(TYPE ducklake, DATA_PATH ...) which we use directly."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from dldbt.config import Config

DEFAULT_TARGET_NAME = "dldbt"


def render_profile(config: Config, *, profile_name: str) -> dict[str, Any]:
    """Build the profiles.yml dict for a given dbt profile name.

    `profile_name` is the top-level key in profiles.yml; it must match the
    `profile:` field in the user's dbt_project.yml.
    """
    s3 = config.storage.s3
    settings: dict[str, Any] = {}
    if s3 is not None:
        if s3.endpoint:
            settings["s3_endpoint"] = s3.endpoint
        settings["s3_region"] = s3.region
        settings["s3_url_style"] = s3.url_style
        settings["s3_use_ssl"] = s3.use_ssl
        if s3.access_key_id:
            settings["s3_access_key_id"] = s3.access_key_id
        if s3.secret_access_key:
            settings["s3_secret_access_key"] = s3.secret_access_key

    # Note: when TYPE ducklake is set we pass the DSN as `postgres:...`
    # NOT `ducklake:postgres:...` — the ducklake: prefix is for the legacy
    # single-arg ATTACH form. Prefix + TYPE together sends DuckLake down a
    # re-init code path that trips on PRIMARY KEY constraints.
    #
    # `is_ducklake: true` is the explicit hint dbt-duckdb looks for when the
    # path does not use the `ducklake:` scheme. Without it, dbt doesn't know
    # the attachment is DuckLake-backed and its default macros try CASCADE
    # drops (not supported in DuckLake).
    attach_entry: dict[str, Any] = {
        "path": f"postgres:{config.catalog.dsn}",
        "alias": config.lake_alias,
        "type": "ducklake",
        "is_ducklake": True,
        "options": {
            "data_path": config.storage.data_path,
            # Matches the dldbt default: keep all data as external parquet so
            # branch shallow-copy stays cheap.
            "data_inlining_row_limit": 0,
        },
    }

    output: dict[str, Any] = {
        "type": "duckdb",
        "path": ":memory:",
        "extensions": ["ducklake", "httpfs"],
        "attach": [attach_entry],
        "database": config.lake_alias,
        "schema": config.main_branch,
    }
    if settings:
        output["settings"] = settings

    return {
        profile_name: {
            "target": DEFAULT_TARGET_NAME,
            "outputs": {DEFAULT_TARGET_NAME: output},
        }
    }


def write_profile(
    config: Config, *, profile_name: str, profiles_dir: Path
) -> Path:
    """Write profiles.yml under `profiles_dir`; returns the written path."""
    profiles_dir.mkdir(parents=True, exist_ok=True)
    path = profiles_dir / "profiles.yml"
    body = render_profile(config, profile_name=profile_name)
    if path.exists():
        existing = yaml.safe_load(path.read_text()) or {}
        if not isinstance(existing, dict):
            existing = {}
        existing.update(body)
        body = existing
    path.write_text(yaml.safe_dump(body, sort_keys=False))
    return path


def detect_profile_name(dbt_project_dir: Path) -> str | None:
    """Read the `profile:` key out of dbt_project.yml, or None if not found."""
    proj = dbt_project_dir / "dbt_project.yml"
    if not proj.exists():
        return None
    try:
        data = yaml.safe_load(proj.read_text()) or {}
    except yaml.YAMLError:
        return None
    if not isinstance(data, dict):
        return None
    name = data.get("profile")
    return str(name) if name else None
