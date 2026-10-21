"""Every generic historical entry point uses the full current candidate replay."""
from datetime import date
import json

import pytest

from research import g3_afternoon_backtest as afternoon
from research import g3_current_history as history
from research import g3_selected_backtest as selected
from research import g3_target_cutoff_backtest as current


@pytest.mark.parametrize("entrypoint", ["selected", "history"])
def test_current_builders_preserve_cutoff_and_source_arguments(tmp_path, monkeypatch, entrypoint):
    root, output = tmp_path, tmp_path / "requested"
    through, base, roots = date(2026, 10, 14), object(), object()
    calls, result = [], object()

    def build(*args, **kwargs):
        calls.append((args, kwargs))
        return result

    monkeypatch.setattr(current, "build", build)
    if entrypoint == "selected":
        actual = selected.run(output, through, root=root, base=base, roots=roots)
    else:
        actual = history.build(root, output, through, base=base, roots=roots)
    assert actual is result
    assert calls == [((root, output, through), {"base": base, "roots": roots})]
    assert not output.exists()


def test_selected_run_infers_only_a_canonical_explicit_session(tmp_path, monkeypatch):
    output = tmp_path / "backtests/runs/2026-10-14/fresh"
    calls = []
    monkeypatch.setattr(current, "build", lambda *args, **kwargs: calls.append(args))
    selected.run(output, root=tmp_path)
    assert calls == [(tmp_path, output, date(2026, 10, 14))]
    with pytest.raises(ValueError, match="explicit through date"):
        selected.run(tmp_path / "ambiguous", root=tmp_path)
    assert len(calls) == 1


@pytest.mark.parametrize("entrypoint", [selected, history])
def test_generic_verification_routes_current_schema_to_current_verifier(tmp_path, monkeypatch, entrypoint):
    (tmp_path / "manifest.json").write_text(json.dumps({"schema_version": current.SCHEMA}))
    calls, result = [], object()
    monkeypatch.setattr(current, "verified_package",
                        lambda *args: calls.append(args) or result)
    assert entrypoint.verified_package(tmp_path, tmp_path.parent) is result
    assert calls == [(tmp_path, tmp_path.parent)]


def test_generic_verification_keeps_explicit_archived_routes(tmp_path, monkeypatch):
    calls, result = [], object()
    monkeypatch.setattr(history, "verified_legacy_package",
                        lambda *args: calls.append(("extension", args)) or result)
    monkeypatch.setattr(history, "verify_base",
                        lambda *args: calls.append(("afternoon", args)) or result)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"schema_version": history.SCHEMA}))
    assert history.verified_package(tmp_path, tmp_path.parent) is result
    manifest.write_text(json.dumps({"variant_id": afternoon.VERSION}))
    assert selected.verified_package(tmp_path, tmp_path.parent) is result
    assert calls == [("extension", (tmp_path, tmp_path.parent)), ("afternoon", (tmp_path,))]


def test_generic_publish_uses_current_profile_guard(tmp_path, monkeypatch):
    calls, result = [], object()
    monkeypatch.setattr(current, "publish", lambda *args: calls.append(args) or result)
    assert history.publish(tmp_path, tmp_path / "run") is result
    assert calls == [(tmp_path, tmp_path / "run")]


def test_current_base_resolves_current_package_without_old_generation(tmp_path, monkeypatch):
    run = tmp_path / "backtests/runs/2026-10-14/current"
    run.mkdir(parents=True)
    (run / "manifest.json").write_text(json.dumps({"schema_version": current.SCHEMA}))
    summary = dict(strategy_version=current.contract.STRATEGY_VERSION,
                   strategy_fingerprint=current.contract.strategy_fingerprint())
    history.replay.atomic_json(tmp_path / "latest/current_backtest.json", dict(
        run_dir=run.relative_to(tmp_path).as_posix(),
        manifest_sha256=history.replay.sha256(run / "manifest.json"), **summary))
    monkeypatch.setattr(current, "verified_package",
                        lambda *args: (summary, {"schema_version": current.SCHEMA}))
    assert history.current_base(tmp_path) == run


def test_archived_afternoon_generation_fails_before_reading_or_writing(tmp_path, monkeypatch):
    output = tmp_path / "unexpected"
    monkeypatch.setattr(afternoon, "configuration",
                        lambda: pytest.fail("Generation guard must precede input reads"))
    with pytest.raises(ValueError, match="verification-only.*fno_v13_v10_g_3_backtest"):
        afternoon.run(output)
    assert not output.exists()
