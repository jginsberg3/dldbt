from __future__ import annotations

from pathlib import Path

import yaml

from dldbt.config import CatalogConfig, Config, S3Settings, StorageConfig
from dldbt.dbt_ops.profile import (
    DEFAULT_TARGET_NAME,
    detect_profile_name,
    render_profile,
    write_profile,
)


def _config(*, s3: S3Settings | None = None, data_path: str = "s3://b/lake/") -> Config:
    return Config(
        catalog=CatalogConfig(dsn="host=pg dbname=l user=u password=p"),
        storage=StorageConfig(data_path=data_path, s3=s3),
    )


def test_render_minimal_local_storage() -> None:
    cfg = _config(data_path="/tmp/lake/")
    body = render_profile(cfg, profile_name="my_proj")
    assert "my_proj" in body
    out = body["my_proj"]
    assert out["target"] == DEFAULT_TARGET_NAME
    target = out["outputs"][DEFAULT_TARGET_NAME]
    assert target["type"] == "duckdb"
    assert target["path"] == ":memory:"
    assert "ducklake" in target["extensions"]
    assert "httpfs" in target["extensions"]
    # No s3 settings → no settings block
    assert "settings" not in target
    attach = target["attach"][0]
    assert attach["type"] == "ducklake"
    assert attach["alias"] == cfg.lake_alias
    assert attach["path"].startswith("postgres:")
    assert not attach["path"].startswith("ducklake:postgres:")
    assert attach["options"]["data_path"] == "/tmp/lake/"
    assert attach["options"]["data_inlining_row_limit"] == 0


def test_render_with_s3_settings() -> None:
    s3 = S3Settings(
        endpoint="minio:9000",
        region="us-east-1",
        access_key_id="ak",
        secret_access_key="sk",
        url_style="path",
        use_ssl=False,
    )
    cfg = _config(s3=s3)
    body = render_profile(cfg, profile_name="p")
    settings = body["p"]["outputs"][DEFAULT_TARGET_NAME]["settings"]
    assert settings["s3_endpoint"] == "minio:9000"
    assert settings["s3_access_key_id"] == "ak"
    assert settings["s3_secret_access_key"] == "sk"
    assert settings["s3_use_ssl"] is False
    assert settings["s3_url_style"] == "path"


def test_render_s3_without_credentials_omits_keys() -> None:
    s3 = S3Settings(endpoint="minio:9000")
    cfg = _config(s3=s3)
    settings = render_profile(cfg, profile_name="p")["p"]["outputs"][
        DEFAULT_TARGET_NAME
    ]["settings"]
    assert "s3_access_key_id" not in settings
    assert "s3_secret_access_key" not in settings
    assert settings["s3_endpoint"] == "minio:9000"


def test_write_profile_creates_file(tmp_path: Path) -> None:
    cfg = _config(data_path="/tmp/lake/")
    out = tmp_path / "proj"
    path = write_profile(cfg, profile_name="proj", profiles_dir=out)
    assert path == out / "profiles.yml"
    assert path.exists()
    loaded = yaml.safe_load(path.read_text())
    assert "proj" in loaded


def test_write_profile_preserves_other_profiles(tmp_path: Path) -> None:
    existing = tmp_path / "profiles.yml"
    existing.write_text(
        yaml.safe_dump({"other": {"target": "dev", "outputs": {"dev": {}}}})
    )
    cfg = _config(data_path="/tmp/lake/")
    write_profile(cfg, profile_name="mine", profiles_dir=tmp_path)
    loaded = yaml.safe_load(existing.read_text())
    assert "other" in loaded
    assert "mine" in loaded


def test_detect_profile_name(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text(
        yaml.safe_dump({"name": "demo", "profile": "my_profile", "version": "1.0"})
    )
    assert detect_profile_name(tmp_path) == "my_profile"


def test_detect_profile_name_missing_file(tmp_path: Path) -> None:
    assert detect_profile_name(tmp_path) is None


def test_detect_profile_name_bad_yaml(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text(":::not valid yaml:::")
    assert detect_profile_name(tmp_path) is None
