"""Evidence remains as recorded and never becomes a hypothetical trade."""
from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import datetime
from pathlib import Path

import pytest

import dashboard_flow_live_evidence as evidence

DAY = "2026-10-12"
NOW = datetime.fromisoformat(DAY + "T12:00:00+05:30")


def identity(**changes):
    return dict(strategy_version=evidence.STRATEGY_VERSION, strategy_fingerprint=evidence.FINGERPRINT,
                session_date=DAY, **changes)


def feature(symbol="TEST", **changes):
    raw = identity(tradingsymbol=symbol, signal_end="09:25", signal_ts=DAY + "T09:25:00+05:30",
        base_long_pass=True, base_short_pass=False, base_side="LONG", first_failed_gate="",
        failed_gates="[]", gate_oi_increasing=True, gate_base_volume=True)
    raw.update(changes)
    return raw


def write_json(root, path, value):
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(value), encoding="utf-8")
    return target


def scanner(root, *, rows=None, candidates=None, **changes):
    raw = identity(signal_end="09:25", confirmation_end="09:26", state="SUCCESS",
        published_at_ist=DAY + "T09:25:10+05:30", feature_evaluations=rows if rows is not None else [feature()],
        candidates=candidates or [], contracts_expected=1, contracts_missing_slot=0)
    raw.update(changes)
    return write_json(root, Path("fno_oi/v13_v10_g_3_live/scanner_5m") / DAY / "slot_0925.json", raw)


def confirmation(root, *, selected=False, rejected=False, **changes):
    raw = identity(signal_end="09:25", confirmation_end="09:26", state="SUCCESS", scanner_complete=True,
        published_at_ist=DAY + "T09:26:10+05:30", selected_signal_ids=[signal_id()] if selected else [],
        directional_confirmed=0 if rejected else 1,
        candidate_rejections={"TEST": "confirmation_volume_below_1.10_or_missing"} if rejected else {},
        feature_evaluations=[feature(confirmation_ts=DAY + "T09:26:00+05:30",
            gate_confirmation_present=True, gate_confirmation_direction=True,
            gate_confirmation_volume=not rejected, setup_filter_pass=not rejected,
            v9_1m_volume_ratio=.8 if rejected else 2.5, minimum_confirmation_volume_ratio=1.1)])
    raw.update(changes)
    return write_json(root, Path("fno_oi/v13_v10_g_3_live/confirmation_1m") / DAY / "slot_0926.json", raw)


def signal_id():
    return evidence.canonical_signal_id(evidence.STRATEGY_VERSION, datetime.fromisoformat(DAY).date(), "09:25", "09:26", "LONG", "TEST")


def order(root, *, mode="PAPER", namespace="", **changes):
    raw = identity(mode=mode, tradingsymbol="TEST", side="LONG", signal_end="09:25", signal_id=signal_id(),
        updated_at_ist=DAY + "T10:00:00+05:30", status="CLOSED", status_reason="TARGET",
        entry_at_ist=DAY + "T09:27:00+05:30", entry_price=100, quantity=10)
    raw.update(changes)
    path = Path("fno_oi/v13_v10_g_3_live/orders") / mode
    if namespace:
        path /= namespace
    return write_json(root, path / DAY / "order.json", raw)


def load(root, **kwargs):
    return evidence.build_live_evidence(root, DAY, now=kwargs.pop("now", NOW), **kwargs)


def test_absence_is_unknown_and_no_strategy_slots_are_not_errors(tmp_path):
    result = load(tmp_path)
    assert result["state"] == "NO_RECORDED_EVIDENCE"
    assert len(result["slots"]) == 76
    assert result["slots"]["09:15"]["state"] == "NOT_STRATEGY_SLOT"
    assert result["slots"]["09:25"]["state"] == "MISSING_EVIDENCE"
    assert result["slots"]["09:25"]["counts"]["selected"] is None
    assert result["slots"]["09:25"]["counts"]["paper_orders"] is None
    assert result["read_only"] is True


def test_recorded_pipeline_gap_is_not_inferred_missed_trade(tmp_path):
    path = scanner(tmp_path)
    before = path.read_bytes()
    slot = load(tmp_path)["slots"]["09:25"]
    assert slot["counts"]["base_pass"] == slot["counts"]["base_not_published"] == 1
    assert slot["counts"]["published"] == 0
    assert slot["rows"]["TEST"]["decision"] == "BASE_PASSED_NOT_PUBLISHED"
    assert slot["rows"]["TEST"]["selected"] is None
    assert "not proven missed trades" in slot["notes"][0]
    assert path.read_bytes() == before


def test_empty_success_is_zero_not_missing(tmp_path):
    scanner(tmp_path, rows=[])
    confirmation(tmp_path, feature_evaluations=[], directional_confirmed=0)
    slot = load(tmp_path)["slots"]["09:25"]
    assert slot["state"] == "RECORDED"
    assert slot["counts"]["evaluated"] == slot["counts"]["published"] == slot["counts"]["selected"] == 0


@pytest.mark.parametrize("change", [dict(strategy_fingerprint="old-G"), dict(strategy_version="V13-V10-G"),
    dict(session_date="2026-10-09"), dict(signal_end="09:30"), dict(published_at_ist=DAY + "T12:01:00+05:30"),
    dict(published_at_ist=DAY + "T09:24:00+05:30"), dict(feature_evaluations="bad")])
def test_invalid_or_future_scanner_is_omitted(tmp_path, change):
    scanner(tmp_path, **change)
    result = load(tmp_path)
    assert result["slots"]["09:25"]["rows"] == {}
    assert result["slots"]["09:25"]["counts"]["evaluated"] is None
    assert result["warnings"]


def test_confirmation_volume_rejection_not_direction_success(tmp_path):
    scanner(tmp_path, candidates=[dict(tradingsymbol="TEST", side="LONG", signal_end="09:25")])
    confirmation(tmp_path, rejected=True)
    row = load(tmp_path)["slots"]["09:25"]["rows"]["TEST"]
    assert row["published"] is True
    assert row["confirmation_gates"]["gate_confirmation_direction"] is True
    assert row["confirmed"] is False and row["selected"] is False
    assert row["decision"] == "CONFIRMATION_REJECTED"
    assert "volume_below" in row["reasons"][0]


def test_subsequent_selected_evidence_has_explicit_timing(tmp_path):
    scanner(tmp_path, candidates=[dict(tradingsymbol="TEST", side="LONG", signal_end="09:25")])
    confirmation(tmp_path, selected=True)
    result = load(tmp_path)
    slot = result["slots"]["09:25"]
    assert slot["rows"]["TEST"]["selected"] is True
    assert slot["rows"]["TEST"]["confirmed"] is True
    assert slot["confirmation"]["timing"] == "SUBSEQUENT_OUTCOME"
    assert slot["rows"]["TEST"]["confirmation_at_ist"] == DAY + "T09:26:00+05:30"
    early = load(tmp_path, now=datetime.fromisoformat(DAY + "T09:25:30+05:30"))
    assert early["slots"]["09:25"]["counts"]["selected"] is None


def test_confirmation_only_retains_missing_scanner_counters(tmp_path):
    confirmation(tmp_path, selected=True)
    slot = load(tmp_path)["slots"]["09:25"]
    assert slot["state"] == "PARTIAL_EVIDENCE"
    assert slot["counts"]["base_pass"] is None
    assert slot["counts"]["published"] is None
    assert slot["counts"]["selected"] == 1
    assert slot["rows"]["TEST"]["selected"] is True


def test_cache_observes_atomic_replacement(tmp_path):
    path = scanner(tmp_path)
    assert load(tmp_path)["slots"]["09:25"]["counts"]["base_pass"] == 1
    raw = json.loads(path.read_text())
    raw["feature_evaluations"] = []
    replacement = path.with_suffix(".tmp")
    replacement.write_text(json.dumps(raw))
    replacement.replace(path)
    assert load(tmp_path)["slots"]["09:25"]["counts"]["base_pass"] == 0


def test_paper_and_live_orders_are_explicit_and_duplicate_snapshots_not_pooled(tmp_path):
    order(tmp_path)
    order(tmp_path, mode="LIVE", namespace="live_kite_qty1", quantity=500)
    order(tmp_path, mode="LIVE", quantity=500)
    slot = load(tmp_path)["slots"]["09:25"]
    assert slot["counts"]["paper_orders"] == slot["counts"]["live_orders"] == 1
    rows = slot["rows"]["TEST"]["orders"]
    assert [(r["mode"], r["quantity"]) for r in rows] == [("PAPER", 10), ("LIVE", 500)]
    assert all(r["filled"] and r["timing"] == "LATEST_RECORDED_OUTCOME" for r in rows)


def test_conflicting_orders_are_omitted_not_counted_twice(tmp_path):
    order(tmp_path, mode="LIVE", quantity=1)
    order(tmp_path, mode="LIVE", namespace="live_kite_qty1", quantity=500)
    result = load(tmp_path)
    assert result["slots"]["09:25"]["orders_state"]["LIVE"] == "INCOMPLETE"
    assert result["slots"]["09:25"]["rows"] == {}
    assert result["warnings"]


def test_order_wrong_identity_is_not_accepted(tmp_path):
    order(tmp_path, signal_id="random-id")
    result = load(tmp_path)
    assert not result["slots"]["09:25"]["rows"]
    assert result["warnings"]


def history(root, *, selected=True, excluded=False):
    backtest = root / "backtesting_result_v13_v10_g_3"
    run = backtest / "history/runs/2026-10-12/example"
    run.mkdir(parents=True)
    raw = dict(strategy_version=evidence.STRATEGY_VERSION, strategy_fingerprint=evidence.FINGERPRINT,
        day=DAY, signal_timestamp=DAY + "T09:25:00+05:30", confirmation_timestamp=DAY + "T09:26:00+05:30",
        tradingsymbol="TEST", side="LONG", signal_id=signal_id(), v9_selected=selected,
        v9_decision="SELECTED" if selected else "FILTER_REJECTED", portfolio_executed=selected)
    artifacts = {}
    for kind in ("selection_audit", "portfolio_trades"):
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=list(raw))
        writer.writeheader()
        writer.writerow(raw)
        content = stream.getvalue().encode()
        (run / (kind + ".csv")).write_bytes(content)
        artifacts[kind] = dict(path=kind + ".csv", sha256=hashlib.sha256(content).hexdigest())
    manifest = dict(strategy_version=evidence.STRATEGY_VERSION, strategy_fingerprint=evidence.FINGERPRINT,
        schema_version="fno_g3_production_run_v1", role="cumulative_history", session_date=DAY,
        complete=True, execution_authority=False, state="SUCCESS", days=[DAY], artifacts=artifacts,
        coverage=dict(excluded_sessions=[dict(day=DAY, state="BLOCKED_SOURCE_INELIGIBLE")] if excluded else []))
    path = write_json(root, run / "run_manifest.json", manifest)
    write_json(root, backtest / "latest/current_history.json", dict(run_dir=run.relative_to(backtest).as_posix(),
        role="cumulative_history", strategy_version=evidence.STRATEGY_VERSION, strategy_fingerprint=evidence.FINGERPRINT,
        session_date=DAY, manifest_sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
    return run


def test_hash_verified_backtest_separate_from_actual_runtime(tmp_path):
    history(tmp_path)
    scanner(tmp_path)
    result = load(tmp_path)
    slot = result["slots"]["09:25"]
    row = slot["rows"]["TEST"]
    assert row["decision"] == "BASE_PASSED_NOT_PUBLISHED"
    assert row["selected"] is None
    assert row["backtest"][0]["selected"] is True
    assert row["backtest"][0]["timing"] == "RETROSPECTIVE_BACKTEST"
    assert slot["counts"]["backtest_selected"] == slot["counts"]["backtest_fills"] == 1
    assert slot["counts"]["selected"] is None


def test_corrupt_historical_artifact_rejects_all_backtest_evidence(tmp_path):
    run = history(tmp_path)
    with (run / "portfolio_trades.csv").open("a") as stream:
        stream.write("tampered")
    result = load(tmp_path)
    assert result["history"]["state"] == "INVALID_BACKTEST_EVIDENCE"
    assert result["slots"]["09:25"]["rows"] == {}
    assert result["slots"]["09:25"]["counts"]["backtest_selected"] is None


def test_excluded_session_does_not_become_zero_trade_success(tmp_path):
    history(tmp_path, excluded=True)
    result = load(tmp_path)
    assert result["history"]["state"] == "EXCLUDED_SESSION"
    assert result["slots"]["09:25"]["counts"]["backtest_selected"] is None


def test_future_and_bad_session_inputs(tmp_path):
    result = load(tmp_path, now=datetime.fromisoformat(DAY + "T09:10:00+05:30"))
    assert all(slot["state"] == "FUTURE" for slot in result["slots"].values())
    with pytest.raises(ValueError):
        evidence.build_live_evidence(tmp_path, "../../secret")
    with pytest.raises(ValueError):
        evidence.build_live_evidence(tmp_path, DAY, now=datetime(2026, 10, 12))
