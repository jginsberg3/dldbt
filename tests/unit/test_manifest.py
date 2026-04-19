from __future__ import annotations

from pathlib import Path

from dldbt.dbt_ops.manifest import (
    branch_manifest_dir,
    manifests_root,
    save_manifest,
)


def test_path_helpers(tmp_path: Path) -> None:
    assert manifests_root(tmp_path) == tmp_path / ".dldbt" / "manifests"
    assert (
        branch_manifest_dir(tmp_path, "feat_x")
        == tmp_path / ".dldbt" / "manifests" / "feat_x" / "latest"
    )


def test_save_manifest_copies_artifacts(tmp_path: Path) -> None:
    project = tmp_path / "proj"
    target = project / "target"
    target.mkdir(parents=True)
    (target / "manifest.json").write_text('{"nodes": {}}')
    (target / "run_results.json").write_text('{"results": []}')
    (target / "graph.gpickle").write_bytes(b"\x80\x04")

    dst = save_manifest(project_root=project, branch="feat_x", target_dir=target)
    assert dst is not None
    assert (dst / "manifest.json").read_text() == '{"nodes": {}}'
    assert (dst / "run_results.json").read_text() == '{"results": []}'
    assert (dst / "graph.gpickle").read_bytes() == b"\x80\x04"


def test_save_manifest_noop_when_no_manifest(tmp_path: Path) -> None:
    project = tmp_path / "proj"
    target = project / "target"
    target.mkdir(parents=True)
    # no manifest.json written
    dst = save_manifest(project_root=project, branch="feat_x", target_dir=target)
    assert dst is None
    assert not (project / ".dldbt").exists()


def test_save_manifest_overwrites_existing(tmp_path: Path) -> None:
    project = tmp_path / "proj"
    target = project / "target"
    target.mkdir(parents=True)
    (target / "manifest.json").write_text('{"v": 1}')
    save_manifest(project_root=project, branch="b", target_dir=target)

    # second run with updated content
    (target / "manifest.json").write_text('{"v": 2}')
    dst = save_manifest(project_root=project, branch="b", target_dir=target)
    assert dst is not None
    assert (dst / "manifest.json").read_text() == '{"v": 2}'
