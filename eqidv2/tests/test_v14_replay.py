from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from v14 import baseline, data, features, replay, research, selection


def _profile(root: Path, variant_name="interval_rvol_1.5_cmf_0.2", *, policy=replay.STRICT_PATH_POLICY,
             slot_policy=None) -> Path:
    source = root / "runs/selecting_study"
    source.mkdir(parents=True)
    research.preregister(source)
    recorded = json.loads((source / "protocol.json").read_text())
    recorded["protocol"]["version"] = (
        replay.PREFIX_PROTOCOL_VERSION if policy == replay.EVIDENCE_VERSION else replay.STRICT_PROTOCOL_VERSION)
    recorded["protocol"].pop("path_policy", None)
    recorded["protocol"].pop("slot_selection_policy", None)
    if slot_policy is not None:
        recorded["protocol"]["slot_selection_policy"] = slot_policy
    if policy == replay.EVIDENCE_VERSION:
        recorded["protocol"]["path_policy"] = policy
    research.write_json(source / "protocol.json", recorded)
    config = baseline.frozen_config()
    variant = next(v for v in features.variants() if v.name == variant_name)
    profile = dict(version="V14", baseline=config["version"], selected_variant=variant.to_dict(),
        selection=dict(name=variant.name, qualified=True), common_feature_coverage=research.COMMON_FEATURES,
        **{key: config[key] for key in ("capital_per_entry_rupees", "portfolio_capital_rupees", "leverage_factor", "cost_bps")},
        execution_authority=False, state="FROZEN_RESEARCH_CANDIDATE", output_dir=str(source),
        protocol_sha256=research.sha256(source / "protocol.json"), data_manifest_sha256="original_study_manifest_hash")
    if policy == replay.EVIDENCE_VERSION:
        profile["path_policy"] = policy
    if slot_policy is not None:
        profile["slot_selection_policy"] = slot_policy
    research.write_json(source / "selected_profile.json", profile)
    research.write_json(root / "selected_profile.json", profile)
    research.write_json(source / "summary.json", dict(status="COMPLETE_RESEARCH"))
    return root / "selected_profile.json"


def _minutes() -> tuple[pd.DataFrame, str]:
    days = pd.bdate_range("2026-09-01", periods=23)
    frames = []
    for day_no, day in enumerate(days):
        stamps = pd.date_range(day.strftime("%Y-%m-%d") + " 09:16", periods=375, freq="min", tz="Asia/Kolkata")
        base = 150. if day_no == len(days) - 1 else 100. + day_no
        close = np.full(375, base * 1.06)
        close[:10] = base
        close[10:15] = base * (1. + np.arange(1, 6) * .0024)
        close[15], close[16:24] = base * 1.015, base * 1.016
        opening = np.r_[base, close[:-1]]
        volume = np.full(375, 100.)
        volume[10:16] = 3000. if day_no == len(days) - 1 else 1000.
        frames.append(pd.DataFrame(dict(ts=stamps, open=opening,
            high=np.maximum(opening, close) + .01, low=np.minimum(opening, close) - .01,
            close=close, volume=volume)))
    return pd.concat(frames, ignore_index=True), days[-1].date().isoformat()


def _staging(root: Path, minute: pd.DataFrame, day: str):
    source = root / "data/1m/CASH.parquet"
    source.parent.mkdir(parents=True)
    minute.to_parquet(source, index=False)
    manifest = dict(study_sessions=[day], symbols=["CASH"], symbol_count=1, limitations=[],
        files=[dict(symbol="CASH", sha256=research.sha256(source))])
    research.write_json(root / "data/manifest.json", manifest)
    research.write_json(root / "data/universe_manifest.json", dict(sessions={day: {"symbols": ["CASH"]}}))
    return manifest


def _dataset(output: Path, pool: pd.DataFrame, manifest: dict, day: str):
    pd.DataFrame(dict(day=[day], nifty_first_bar_return_pct=[.1])).to_csv(output / "nifty_context.csv", index=False)
    pd.DataFrame(dict(day=[day], presence_fraction=[1.])).to_csv(output / "session_data_coverage.csv", index=False)
    return pool, manifest, [day]


@pytest.mark.parametrize("policy,later_gap", [(replay.STRICT_PATH_POLICY, False),
                                             (replay.EVIDENCE_VERSION, False),
                                             (replay.EVIDENCE_VERSION, True)])
def test_actual_cash_features_and_execution_frozen_profile_smoke(tmp_path, monkeypatch, policy, later_gap):
    profile_path = _profile(tmp_path, policy=policy)
    profile_before = profile_path.read_bytes()
    minute, day = _minutes()
    if later_gap:
        minute = minute.loc[minute.ts.ne(pd.Timestamp(day + " 14:00", tz="Asia/Kolkata"))].copy()
    manifest = _staging(tmp_path, minute, day)
    pool = features.build_features(minute, "CASH")
    pool = pool.loc[pool.day.eq(day)].copy()
    pool["nifty_first_bar_return_pct"] = .1
    pool = features.strict_signals(pool).reset_index(drop=True)
    pool["common_feature_coverage"] = np.isfinite(pool[research.COMMON_FEATURES]).all(axis=1)
    pool["sid"] = np.arange(len(pool))
    monkeypatch.setattr(data, "prepare_data", lambda *args, **kwargs: tmp_path / "data/manifest.json")
    monkeypatch.setattr(research, "build_dataset", lambda root, output: _dataset(output, pool, manifest, day))
    monkeypatch.setattr(research, "choose_variant", lambda *args: pytest.fail("A replay must never reselect a profile"))
    output = tmp_path / "runs/replayed"
    result = replay.replay_selected(tmp_path, output, start_date=day, end_date=day)
    assert result["profile_reselected"] is False
    assert result["selected_variant"]["name"] == "interval_rvol_1.5_cmf_0.2"
    assert result["metrics"]["trades"] == 1
    assert result["metrics"]["net_pnl"] == pytest.approx(9750.)
    assert result["path_policy"] == policy
    assert result["slot_selection_policy"] == selection.FROZEN_G3_SELECTION
    evidence = json.loads((output / "path_evidence.json").read_text())
    assert evidence["path_policy"] == policy
    assert len(evidence["orders"]) == 1
    proof = next(iter(evidence["orders"].values()))
    assert proof["resolved"]
    if policy == replay.EVIDENCE_VERSION:
        assert proof["reason"] == "RESOLVED_TARGET"
        assert proof["required_minutes"] < proof["input_minutes"]
        assert proof["full_session_observed"] is not later_gap
    assert profile_path.read_bytes() == profile_before
    assert (output / "selected_profile.json").read_bytes() == profile_before
    assert (output / "prior_data_manifest.json").exists()
    assert (output / "input_data_manifest.json").exists()
    assert (output / "pullback_report.html").is_file()
    assert (output / "report.html").is_file()
    ledger = pd.read_csv(output / "trades.csv")
    monitored = pd.read_csv(output / "trades_with_pullbacks.csv")
    pd.testing.assert_series_equal(ledger.portfolio_net_profit_rupees, monitored.portfolio_net_profit_rupees)
    if later_gap:
        assert monitored.monitor_status.iloc[0] == "MONITOR_UNAVAILABLE"
    with pytest.raises(FileExistsError, match="immutable"):
        replay.replay_selected(tmp_path, output, start_date=day, end_date=day)


def test_profile_tamper_fails_before_data_preparation(tmp_path, monkeypatch):
    path = _profile(tmp_path)
    profile = json.loads(path.read_text())
    profile["selected_variant"]["threshold"] = 99.
    research.write_json(path, profile)
    monkeypatch.setattr(data, "prepare_data", lambda *args, **kwargs: pytest.fail("Do not prepare data for invalid profile"))
    with pytest.raises(ValueError, match="differs from its completed"):
        replay.replay_selected(tmp_path, tmp_path / "new", start_date="2026-10-01", end_date="2026-10-01")


@pytest.mark.parametrize("policy,gap_time", [(replay.STRICT_PATH_POLICY, "14:00"),
                                           (replay.EVIDENCE_VERSION, "09:34")])
def test_incomplete_selected_path_excludes_day_without_refill(tmp_path, monkeypatch, policy, gap_time):
    _profile(tmp_path, "no_oi_control", policy=policy)
    minute, day = _minutes()
    # Selected stock has a later missing minute. A lower-ranked complete stock
    # could exist, but selection is performed once and is never refilled.
    missing = pd.Timestamp(day + " " + gap_time, tz="Asia/Kolkata")
    minute = minute.loc[minute.ts.ne(missing)].copy()
    manifest = _staging(tmp_path, minute, day)
    signal = pd.Timestamp(day + " 09:30", tz="Asia/Kolkata")
    pool = pd.DataFrame([dict(sid=0, day=day, tradingsymbol="CASH", signal_ts=signal,
        confirmation_ts=signal + pd.Timedelta(minutes=1), side="LONG", hhmm_int=930,
        price_change_pct=.8, volume_ratio=3., body_ratio=.8, wick_ratio=.1,
        traded_value=1e7, v9_1m_volume_ratio=1.1, trigger=100., common_feature_coverage=True)])
    calls = []
    native_select = selection.select_orders
    def select(*args, **kwargs):
        calls.append(1)
        return native_select(*args, **kwargs)
    monkeypatch.setattr(selection, "select_orders", select)
    monkeypatch.setattr(data, "prepare_data", lambda *args, **kwargs: tmp_path / "data/manifest.json")
    monkeypatch.setattr(research, "build_dataset", lambda root, output: _dataset(output, pool, manifest, day))
    result = replay.replay_selected(tmp_path, tmp_path / "new", start_date=day, end_date=day)
    assert calls == [1]
    assert result["excluded_days"] == [day]
    assert result["evaluated_sessions"] == 0
    assert result["metrics"]["trades"] == 0
    assert result["status"] == "COMPLETE_REPLAY_NO_EVALUABLE_SESSIONS"


def test_v2_profile_protocol_disagreement_and_data_code_drift_are_rejected(tmp_path):
    path = _profile(tmp_path, policy=replay.EVIDENCE_VERSION)
    profile = json.loads(path.read_text())
    source = Path(profile["output_dir"])
    profile["path_policy"] = replay.STRICT_PATH_POLICY
    research.write_json(path, profile)
    research.write_json(source / "selected_profile.json", profile)
    with pytest.raises(ValueError, match="must agree"):
        replay._verified_profile(path)
    profile["path_policy"] = replay.EVIDENCE_VERSION
    research.write_json(path, profile)
    research.write_json(source / "selected_profile.json", profile)
    recorded = json.loads((source / "protocol.json").read_text())
    code = recorded["source_sha256"].copy()
    code["data.py"] = "0" * 64
    research.write_json(source / "executed_source_hashes.json", code)
    with pytest.raises(ValueError, match="data.py"):
        replay._verified_profile(path)


@pytest.mark.parametrize("slot_policy,expected_trades", [(None, 1), (selection.UNLIMITED_SELECTION, 2)])
def test_replay_uses_recorded_caps_not_current_v14_default(tmp_path, monkeypatch, slot_policy, expected_trades):
    path = _profile(tmp_path, policy=replay.EVIDENCE_VERSION, slot_policy=slot_policy)
    original_profile = path.read_bytes()
    minute, day = _minutes()
    manifest = _staging(tmp_path, minute, day)
    peer = tmp_path / "data/1m/PEER.parquet"
    minute.to_parquet(peer, index=False)
    manifest["files"].append(dict(symbol="PEER", sha256=research.sha256(peer)))
    manifest["symbols"].append("PEER")
    manifest["symbol_count"] = 2
    research.write_json(tmp_path / "data/manifest.json", manifest)
    native = features.build_features(minute, "CASH")
    native = native.loc[native.day.eq(day)].copy()
    native["nifty_first_bar_return_pct"] = .1
    native = features.strict_signals(native).reset_index(drop=True)
    native["common_feature_coverage"] = np.isfinite(native[research.COMMON_FEATURES]).all(axis=1)
    pool = pd.concat([native, native.assign(tradingsymbol="PEER")], ignore_index=True)
    pool["sid"] = np.arange(len(pool))
    monkeypatch.setattr(data, "prepare_data", lambda *args, **kwargs: tmp_path / "data/manifest.json")
    monkeypatch.setattr(research, "build_dataset", lambda root, output: _dataset(output, pool, manifest, day))
    result = replay.replay_selected(tmp_path, tmp_path / "new", start_date=day, end_date=day)
    assert result["metrics"]["trades"] == expected_trades
    assert result["metrics"]["net_pnl"] == pytest.approx(9750. * expected_trades)
    resolved_policy = slot_policy or selection.FROZEN_G3_SELECTION
    assert result["slot_selection_policy"] == resolved_policy
    assert result["lineage"]["slot_selection_policy"] == resolved_policy
    recorded = json.loads((tmp_path / "new/replay_protocol.json").read_text())
    assert recorded["slot_selection_policy"] == resolved_policy
    if slot_policy is not None:
        assert "selection.py" in result["lineage"]["checked_code_sha256"]
    else:
        assert "selection.py" not in result["lineage"]["checked_code_sha256"]
    assert path.read_bytes() == original_profile


@pytest.mark.parametrize("profile_policy", [None, selection.FROZEN_G3_SELECTION, "UNKNOWN_POLICY"])
def test_profile_and_protocol_slot_policy_must_agree(tmp_path, profile_policy):
    path = _profile(tmp_path, slot_policy=selection.UNLIMITED_SELECTION)
    profile = json.loads(path.read_text())
    if profile_policy is None:
        profile.pop("slot_selection_policy")
    else:
        profile["slot_selection_policy"] = profile_policy
    research.write_json(path, profile)
    research.write_json(Path(profile["output_dir"]) / "selected_profile.json", profile)
    with pytest.raises(ValueError, match="agree on slot selection policy"):
        replay._verified_profile(path)


@pytest.mark.parametrize("slot_policy,requires_wrapper_hash", [(None, False), (selection.UNLIMITED_SELECTION, True)])
def test_only_new_uncapped_profiles_bind_the_wrapper_hash(tmp_path, slot_policy, requires_wrapper_hash):
    path = _profile(tmp_path, slot_policy=slot_policy)
    source = Path(json.loads(path.read_text())["output_dir"])
    code = json.loads((source / "protocol.json").read_text())["source_sha256"]
    code["selection.py"] = "0" * 64
    research.write_json(source / "executed_source_hashes.json", code)
    if requires_wrapper_hash:
        with pytest.raises(ValueError, match="selection.py"):
            replay._verified_profile(path)
    else:
        _, _, lineage = replay._verified_profile(path)
        assert lineage["slot_selection_policy"] == selection.FROZEN_G3_SELECTION


@pytest.mark.parametrize("invalid_policy", [None, "UNKNOWN_POLICY"])
def test_explicit_invalid_policy_is_not_treated_as_an_omitted_legacy_field(tmp_path, invalid_policy):
    path = _profile(tmp_path, slot_policy=selection.UNLIMITED_SELECTION)
    profile = json.loads(path.read_text())
    source = Path(profile["output_dir"])
    protocol_path = source / "protocol.json"
    recorded = json.loads(protocol_path.read_text())
    recorded["protocol"]["slot_selection_policy"] = invalid_policy
    research.write_json(protocol_path, recorded)
    profile["slot_selection_policy"] = invalid_policy
    profile["protocol_sha256"] = research.sha256(protocol_path)
    research.write_json(path, profile)
    research.write_json(source / "selected_profile.json", profile)
    with pytest.raises(ValueError, match="Unsupported frozen slot selection policy"):
        replay._verified_profile(path)
