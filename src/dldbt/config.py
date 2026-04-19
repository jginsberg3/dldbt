from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, ValidationError

from dldbt.errors import ConfigError

DEFAULT_CONFIG_FILENAME = ".dldbt.yml"


class S3Settings(BaseModel):
    endpoint: str | None = None
    region: str = "us-east-1"
    access_key_id: str | None = None
    secret_access_key: str | None = None
    url_style: Literal["path", "vhost"] = "path"
    use_ssl: bool = True


class StorageConfig(BaseModel):
    # e.g. 's3://my-bucket/lake/' or '/var/lake/'
    data_path: str
    s3: S3Settings | None = None


class CatalogConfig(BaseModel):
    # Postgres DSN in libpq keyword=value form.
    dsn: str


class AutoCreateConfig(BaseModel):
    # Opt-in flag: when true, the post-checkout git hook creates a matching
    # dldbt schema after a git branch checkout. Off by default so an
    # install-hooks run is not immediately destructive.
    enabled: bool = False
    # Git branch patterns that the auto-create hook should ignore. Glob syntax
    # via fnmatch (e.g. "release/*"). Applies only to hook-driven creation;
    # `dldbt branch create` still works for these names.
    skip_patterns: list[str] = Field(
        default_factory=lambda: ["main", "master", "release/*"]
    )


class Config(BaseModel):
    catalog: CatalogConfig
    storage: StorageConfig
    main_branch: str = "main"
    # Branches whose schema should never be managed by dldbt (e.g. trunk names).
    protected_branches: list[str] = Field(default_factory=lambda: ["main", "master"])
    # Name used for the ATTACHed ducklake inside DuckDB. Internal, rarely changed.
    lake_alias: str = "dldbt_lake"
    auto_create: AutoCreateConfig = Field(default_factory=AutoCreateConfig)


def load_config(path: str | Path = DEFAULT_CONFIG_FILENAME) -> Config:
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"config file not found: {p}")
    try:
        with p.open("r") as f:
            raw = yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ConfigError(f"could not parse {p}: {e}") from e
    try:
        return Config.model_validate(raw)
    except ValidationError as e:
        raise ConfigError(f"invalid config in {p}:\n{e}") from e
