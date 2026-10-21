"""Rollback checks operate only in pytest's temporary workspace and result tree."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools import g4_sl_variants_rollback as rollback


@pytest.fixture
def study(tmp_path):
    workspace = tmp_path / "workspace"
    research = tmp_path / "research"
    run = research / "test_run"
    run.mkdir(parents=True)
    files = []
    for relative in sorted(rollback.ALLOWED_FILES):
        path = workspace / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"Original {relative}\n", encoding="utf-8")
        files.append(path)
    manifest = {
        "workspace_root": str(workspace),
        "new_repository_files": [str(path) for path in files],
        "new_repository_file_sha256": {str(path): rollback.sha256(path) for path in files},
        "production_or_latest_files_changed": False,
        "pre_run_pointer_sha256": {},
    }
    (run / "rollback.json").write_text(json.dumps(manifest), encoding="utf-8")
    (run / "summary.json").write_text("{\"keep_results\": true}", encoding="utf-8")
    return run, files, dict(workspace_root=workspace, research_root=research)


def test_preview_does_not_change_files_and_archive_restore_roundtrip(study):
    run, files, roots = study
    plan = rollback.archive_run(run, **roots)
    assert not plan["applied"]
    assert all(path.exists() for path in files)
    assert not (run / "rollback_archive").exists()
    archived = rollback.archive_run(run, apply=True, **roots)
    receipt = Path(archived["receipt"])
    assert all(not path.exists() for path in files)
    assert (run / "summary.json").exists()
    preview = rollback.restore_run(run, receipt, **roots)
    assert not preview["applied"]
    assert len(preview["files"]) == len(files)
    assert all(not path.exists() for path in files)
    restored = rollback.restore_run(run, receipt, apply=True, **roots)
    assert restored["applied"]
    assert all(path.exists() for path in files)
    assert len(rollback.restore_run(run, receipt, **roots)["already_present"]) == len(files)


def test_changed_user_file_refuses_entire_archive(study):
    run, files, roots = study
    files[-1].write_text("later user edit", encoding="utf-8")
    with pytest.raises(rollback.RollbackError, match="changed"):
        rollback.archive_run(run, apply=True, **roots)
    assert all(path.exists() for path in files)
    assert not (run / "rollback_archive").exists()


@pytest.mark.parametrize("relative", ["../outside.txt", "configs/strategies/active_profile.json"])
def test_manifest_cannot_move_unapproved_or_outside_files(study, relative):
    run, files, roots = study
    target = (roots["workspace_root"] / relative).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("protected", encoding="utf-8")
    manifest = json.loads((run / "rollback.json").read_text(encoding="utf-8"))
    manifest["new_repository_files"].append(str(target))
    manifest["new_repository_file_sha256"][str(target)] = rollback.sha256(target)
    (run / "rollback.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(rollback.RollbackError):
        rollback.archive_run(run, apply=True, **roots)
    assert target.read_text(encoding="utf-8") == "protected"
    assert all(path.exists() for path in files)


def test_restore_refuses_overwrite_without_partial_moves(study):
    run, files, roots = study
    receipt = Path(rollback.archive_run(run, apply=True, **roots)["receipt"])
    files[-1].write_text("new user file", encoding="utf-8")
    with pytest.raises(rollback.RollbackError, match="overwrite"):
        rollback.restore_run(run, receipt, apply=True, **roots)
    assert all(not path.exists() for path in files[:-1])
    assert files[-1].read_text(encoding="utf-8") == "new user file"


def test_restore_refuses_tampered_archive(study):
    run, files, roots = study
    receipt = Path(rollback.archive_run(run, apply=True, **roots)["receipt"])
    contents = json.loads(receipt.read_text(encoding="utf-8"))
    Path(contents["files"][-1]["archive"]).write_text("changed archive", encoding="utf-8")
    with pytest.raises(rollback.RollbackError, match="changed"):
        rollback.restore_run(run, receipt, apply=True, **roots)
    assert all(not path.exists() for path in files)


def test_restore_refuses_receipt_path_escape(study):
    run, files, roots = study
    receipt = Path(rollback.archive_run(run, apply=True, **roots)["receipt"])
    contents = json.loads(receipt.read_text(encoding="utf-8"))
    contents["files"][0]["archive"] = str(run / "summary.json")
    receipt.write_text(json.dumps(contents), encoding="utf-8")
    with pytest.raises(rollback.RollbackError, match="outside"):
        rollback.restore_run(run, receipt, apply=True, **roots)
    assert all(not path.exists() for path in files)


def test_restore_recovers_interrupted_archive(study, monkeypatch):
    run, files, roots = study
    move = rollback.shutil.move
    calls = []

    def interrupted(source, destination):
        calls.append(source)
        if len(calls) == 3:
            raise OSError("simulated interruption")
        return move(source, destination)

    monkeypatch.setattr(rollback.shutil, "move", interrupted)
    with pytest.raises(OSError, match="interruption"):
        rollback.archive_run(run, apply=True, **roots)
    receipt = next((run / "rollback_archive").glob("*/receipt.json"))
    assert json.loads(receipt.read_text(encoding="utf-8"))["state"] == "interrupted"
    monkeypatch.setattr(rollback.shutil, "move", move)
    rollback.restore_run(run, receipt, apply=True, **roots)
    assert all(path.exists() for path in files)


def test_manifest_production_changes_are_not_supported(study):
    run, files, roots = study
    manifest_path = run / "rollback.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["production_or_latest_files_changed"] = True
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(rollback.RollbackError, match="production/latest"):
        rollback.archive_run(run, apply=True, **roots)
    assert all(path.exists() for path in files)
