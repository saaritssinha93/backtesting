import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from v14 import baseline, improvement_study, selection
from v14.improvement_study import evaluation_sets, select_candidate
from v14.improvement_spec import variants


def test_unknown_excludes_whole_day_for_all_common_arms_without_refill():
    days = ["2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06"]
    selected = {"a": pd.DataFrame({"sid": [1, 2, 3], "day": days[:3]}),
                "b": pd.DataFrame({"sid": [4, 5, 6], "day": days[:3]})}
    unknown = pd.DataFrame({"sid": [2], "day": [days[1]]})
    scopes, own = evaluation_sets(days, selected, unknown, [days[2]], [], days[:2])
    assert scopes["common_all_dates"] == [days[0], days[3]]  # no-order day retained
    assert scopes["matched_original_dates"] == [days[0]]
    assert own["b"] == [days[0], days[1], days[3]]
    assert selected["a"].sid.tolist() == [1, 2, 3]


def sample_metrics():
    return pd.DataFrame([dict(scope=scope, variant=v.name, split=split,
        additional_bps=stress, net_pnl=100., trades=25, max_drawdown=10.)
        for scope in ["common_all_dates", "matched_original_dates", "own_coverage_only"]
        for v in variants() for split in ["TRAIN", "VALIDATION", "TEST", "ALL"]
        for stress in [0., 5.]])


def test_selection_never_uses_seen_test_or_different_coverage_or_stress():
    metrics = sample_metrics()
    before = select_candidate(metrics)
    assert before["name"] == "no_oi_control"
    ignored = (metrics.split.isin(["TEST", "ALL"]) | metrics.scope.ne("common_all_dates")
               | metrics.additional_bps.ne(0))
    metrics.loc[ignored, ["net_pnl", "trades", "max_drawdown"]] = [-1e12, 0, 1e12]
    assert select_candidate(metrics) == before
    assert not before["fresh_out_of_sample"]
    assert not before["default_profile_promoted"]


def test_selection_requires_train_and_validation_qualification():
    metrics = sample_metrics()
    target = metrics.variant.eq("capacity_guard") & metrics.split.eq("VALIDATION")
    metrics.loc[target, "net_pnl"] = 10000.
    assert select_candidate(metrics)["name"] == "capacity_guard"
    metrics.loc[metrics.variant.eq("capacity_guard") & metrics.split.eq("TRAIN"), "trades"] = 19
    assert select_candidate(metrics)["name"] == "no_oi_control"
    metrics.loc[metrics.split.eq("TRAIN"), "net_pnl"] = -1.
    failed = select_candidate(metrics)
    assert failed["name"] == "capacity_guard"
    assert not failed["qualified"]
    assert failed["qualified_candidates"] == 0


def test_duplicate_partition_rejected():
    metrics = sample_metrics()
    metrics = pd.concat([metrics, metrics.iloc[:1]], ignore_index=True)
    with pytest.raises(ValueError, match="one train/validation"):
        select_candidate(metrics)


def test_archived_policy_defaults_to_caps_and_honors_explicit_profile(tmp_path):
    assert improvement_study.archived_selection_policy(tmp_path) == selection.FROZEN_G3_SELECTION
    profile = tmp_path / "selected_profile.json"
    profile.write_text("{}")
    assert improvement_study.archived_selection_policy(tmp_path) == selection.FROZEN_G3_SELECTION
    profile.write_text(json.dumps(dict(slot_selection_policy=selection.UNLIMITED_SELECTION)))
    assert improvement_study.archived_selection_policy(tmp_path) == selection.UNLIMITED_SELECTION
    profile.write_text(json.dumps(dict(slot_selection_policy="unknown")))
    with pytest.raises(ValueError, match="Unsupported archived"):
        improvement_study.archived_selection_policy(tmp_path)


@pytest.mark.parametrize("unknown_extra", [False, True])
def test_new_unlimited_study_reconciles_separate_capped_archive(tmp_path, monkeypatch, unknown_extra):
    root, source, output = (tmp_path / name for name in ("root", "source", "output"))
    source.mkdir()
    (root / "data/1m").mkdir(parents=True)
    day = "2026-10-08"
    stamp = pd.Timestamp(day + " 09:30", tz=baseline.IST)
    rows = pd.DataFrame([dict(sid=sid, day=day, tradingsymbol=f"CASH{sid}", signal_ts=stamp,
        confirmation_ts=stamp + pd.Timedelta(minutes=1), side="LONG", hhmm_int=930,
        price_change_pct=.8, volume_ratio=3., body_ratio=.8, wick_ratio=.1,
        traded_value=2e7, v9_1m_volume_ratio=1.2, trigger=100., close=100., high=101.,
        low=99., ema9=100., confirmation_close=100., confirmation_volume=100000.,
        interval_rvol=2., cmf10=.5, common_feature_coverage=True) for sid in range(1, 4)])
    rows.to_parquet(source / "feature_ledger.parquet", index=False)
    pd.DataFrame(dict(day=[day], nifty_first_bar_return_pct=[0.])).to_csv(source / "nifty_context.csv", index=False)
    pd.DataFrame(dict(day=[day], presence_fraction=[1.])).to_csv(source / "session_data_coverage.csv", index=False)
    (source / "evaluation_coverage.json").write_text(json.dumps(dict(evaluated_days=[day])))
    (source / "summary.json").write_text("{}")
    (source / "selected_profile.json").write_text("{}")
    files = []
    for symbol in rows.tradingsymbol:
        raw = root / "data/1m" / f"{symbol}.parquet"
        raw.write_bytes(b"verified-fixture")
        files.append(dict(symbol=symbol, sha256=improvement_study.sha256(raw)))
    manifest = dict(study_sessions=[day], files=files)
    times = pd.date_range(stamp + pd.Timedelta(minutes=2), stamp.normalize() + pd.Timedelta(hours=15, minutes=15), freq="min")
    path = dict(timestamp_ns=times.asi8, open=np.full(len(times), 100.),
        high=np.full(len(times), 104.), low=np.full(len(times), 99.9), close=np.full(len(times), 100.))
    capped = baseline.select_orders(rows)
    assert len(capped) == 1
    capped.to_csv(source / "selected_no_oi_control.csv", index=False)
    archived = baseline.simulate(capped, {int(sid): path for sid in capped.sid}, allow_observed_prefix=True)
    archived.to_csv(source / "trades_no_oi_control.csv", index=False)
    source_hashes = {p.name: improvement_study.sha256(p) for p in source.iterdir()}
    monkeypatch.setattr(improvement_study, "verify_source", lambda *args: manifest)
    monkeypatch.setattr(improvement_study, "clean_minutes", lambda value: value)
    monkeypatch.setattr(improvement_study.data, "load_symbol_1m", lambda symbol, **kwargs: symbol)
    missing_first = {key: value[1:] for key, value in path.items()}
    monkeypatch.setattr(improvement_study.baseline, "native_paths",
        lambda symbol, *args: missing_first if unknown_extra and symbol == "CASH3" else path)
    summary = improvement_study.run(root, source, output)
    new_orders = pd.read_csv(output / "selected_no_oi_control.csv")
    assert len(new_orders) == 3 and "max_entries" not in new_orders
    assert summary["slot_selection_policy"] == selection.UNLIMITED_SELECTION
    assert summary["archived_control_slot_selection_policy"] == selection.FROZEN_G3_SELECTION
    assert summary["evaluation_sets"]["common_all_dates"] == ([] if unknown_extra else [day])
    new_ledger = pd.read_csv(output / "trades_no_oi_control.csv")
    assert len(new_ledger) == (0 if unknown_extra else 3)
    replay = pd.read_csv(output / "archived_control_replay.csv")
    assert len(replay) == len(archived) == 1
    reconciliation = json.loads((output / "control_reconciliation.json").read_text())
    assert reconciliation["separate_historical_policy_replay"] and reconciliation["executed_and_pnl_match"]
    assert json.loads((output / "research_candidate.json").read_text())["slot_selection_policy"] == selection.UNLIMITED_SELECTION
    assert json.loads((output / "executed_source_hashes.json").read_text())["selection.py"] == improvement_study.sha256(Path(selection.__file__))
    assert {p.name: improvement_study.sha256(p) for p in source.iterdir()} == source_hashes
