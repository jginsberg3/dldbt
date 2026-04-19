from __future__ import annotations

from pathlib import Path

import pytest

from dlgit.config import Config, load_config
from dlgit.errors import ConfigError


def _write(path: Path, body: str) -> Path:
    path.write_text(body)
    return path


def test_load_minimal(tmp_path: Path) -> None:
    p = _write(
        tmp_path / ".dlgit.yml",
        """
        catalog:
          dsn: "host=localhost port=5432 dbname=ducklake user=u password=p"
        storage:
          data_path: "s3://bucket/lake/"
        """,
    )
    cfg = load_config(p)
    assert isinstance(cfg, Config)
    assert cfg.main_branch == "main"
    assert cfg.storage.data_path == "s3://bucket/lake/"
    assert cfg.storage.s3 is None
    assert cfg.protected_branches == ["main", "master"]


def test_load_full(tmp_path: Path) -> None:
    p = _write(
        tmp_path / ".dlgit.yml",
        """
        catalog:
          dsn: "host=pg dbname=l"
        storage:
          data_path: "s3://b/lake/"
          s3:
            endpoint: "localhost:9000"
            access_key_id: "minio"
            secret_access_key: "minio-secret"
            url_style: "path"
            use_ssl: false
        main_branch: "trunk"
        protected_branches: ["trunk", "prod"]
        lake_alias: "my_lake"
        """,
    )
    cfg = load_config(p)
    assert cfg.main_branch == "trunk"
    assert cfg.protected_branches == ["trunk", "prod"]
    assert cfg.lake_alias == "my_lake"
    assert cfg.storage.s3 is not None
    assert cfg.storage.s3.endpoint == "localhost:9000"
    assert cfg.storage.s3.use_ssl is False


def test_missing_file(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.yml")


def test_invalid_yaml(tmp_path: Path) -> None:
    p = _write(tmp_path / ".dlgit.yml", "not: valid: yaml: here")
    with pytest.raises(ConfigError):
        load_config(p)


def test_missing_required_fields(tmp_path: Path) -> None:
    p = _write(tmp_path / ".dlgit.yml", "storage:\n  data_path: /tmp\n")
    with pytest.raises(ConfigError, match="catalog"):
        load_config(p)
