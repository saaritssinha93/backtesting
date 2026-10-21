"""Raw afternoon inputs retain five setups and reject ten backtest exclusions."""
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import dashboard_flow as flow
import fno_v13_v10_g_3_contract as production
import fno_v13_v10_g_3_daily_replay as replay
import fno_v13_replay_source as source
from tests.test_fno_v13_v10_g_3_daily_replay import DAY, _minute_frame, raw


# Approved values are explicit so a later accidental contract edit cannot also
# change the expectation. Volumes include a small margin above each threshold.
APPROVED = [
    ("12:35", "LONG", .225, .37), ("12:35", "SHORT", .1, 1.24),
    ("12:45", "LONG", .1, .23), ("12:45", "SHORT", .825, .49),
    ("12:50", "LONG", .625, .26), ("12:50", "SHORT", .825, .14),
    ("12:55", "SHORT", .425, .14), ("13:00", "SHORT", .825, .88),
    ("13:05", "LONG", .1, .18), ("13:15", "LONG", .625, .17),
    ("13:15", "SHORT", 1.325, .33), ("13:20", "SHORT", .1, 1.01),
    ("13:25", "LONG", .225, .41), ("13:25", "SHORT", 1.225, .76),
    ("13:30", "LONG", .825, 1.17),
]


@pytest.mark.parametrize("clock,side,confirmation_ratio,target", APPROVED)
def test_afternoon_raw_inputs_execute_only_targets_above_cutoff(
        raw, tmp_path, clock, side, confirmation_ratio, target):
    roots, equity, _ = raw
    previous = pd.read_parquet(equity)
    previous = previous.loc[previous.date.dt.date.ne(DAY)]
    today = _minute_frame(DAY, move=False)
    signal = pd.Timestamp(f"{DAY} {clock}", tz="Asia/Kolkata")
    confirmation = signal + pd.Timedelta(minutes=1)
    direction = 1. if side == "LONG" else -1.
    base = 100 + direction * np.arange(len(today)) * .001
    today[["open", "high", "low", "close"]] = np.column_stack(
        [base - direction * .0005, base + .01, base - .01, base])
    for offset, price in enumerate(100 + direction * np.array([1.2, 1.4, 1.6, 1.8, 2.])):
        stamp = signal - pd.Timedelta(minutes=4-offset)
        today.loc[today.date.eq(stamp), ["open", "high", "low", "close", "volume"]] = (
            [price-.2, price+.05, price-.21, price, 200.] if side == "LONG"
            else [price+.2, price+.21, price-.05, price, 200.])
    today.loc[today.date.eq(confirmation), ["open", "high", "low", "close", "volume"]] = (
        [102., 102.25, 101.99, 102.2, confirmation_ratio * 125.] if side == "LONG"
        else [98.05, 98.06, 97.94, 97.95, confirmation_ratio * 125.])
    after = today.date.gt(confirmation)
    today.loc[after, ["open", "high", "low", "close", "volume"]] = (
        [102.3, 102.4, 102.3, 102.35, 100.] if side == "LONG"
        else [97.90, 97.94, 97.85, 97.90, 100.])
    target_path = today.date.gt(confirmation + pd.Timedelta(minutes=1))
    today.loc[target_path, ["open", "high", "low", "close"]] = (
        [107., 107.2, 107., 107.1] if side == "LONG" else [91., 91.2, 90.9, 91.1])
    pd.concat([previous, today], ignore_index=True).to_parquet(equity, index=False)

    directory = tmp_path / f"afternoon-{clock.replace(':', '')}-{side}"
    result = replay.replay_day(DAY, directory, roots=roots)
    assert result["complete"], result["coverage"]
    assert result["strategy_version"] == "V13-V10-G-3-BACKTEST-TARGET-GT075-RESTORED3-V13"
    ledger = pd.read_csv(directory / "portfolio_trades.csv")
    setup_id = confirmation.strftime("%H%M") + "_" + side
    added = ledger.loc[ledger.setup_id.eq(setup_id)]
    if target <= .75:
        assert added.empty
        assert replay.contract.setup_for(clock, side) is None
        # Prove these unchanged raw inputs really qualify under production;
        # an accidentally non-qualifying fixture must not pass the cutoff test.
        dataset = source.build_day_features(DAY, roots=roots, signal_clocks=production.SIGNAL_TO_CONFIRMATION)
        original_audit = production.selection_audit(dataset["signals"])
        old = original_audit.loc[original_audit.setup_id.eq(setup_id) & original_audit.v9_selected.eq(True)]
        assert len(old) == 1
        assert production.setup_for(clock, side).target_pct == target
        return
    assert len(added) == 1
    row = added.iloc[0]
    assert row.v9_1m_volume_ratio == pytest.approx(confirmation_ratio)
    assert row.native_target_pct == target
    assert row.native_stop_pct == 1.25
    assert row.filled and row.portfolio_executed
    assert row.gross_return_pct == pytest.approx(target)
    assert row.net_return_pct == pytest.approx(target - .05)
    assert pd.Timestamp(row.confirmation_ts) == confirmation
    assert pd.Timestamp(row.entry_ts) > confirmation
    provenance = json.loads((directory / "source_manifest.json").read_text())
    assert any(Path(item["path"]).name == "fno_v13_v10_g_3_afternoon_setups.py"
               for item in provenance["code"])
    features = pd.read_csv(directory / "feature_ledger.csv")
    assert int(features.hhmm_int.max()) == 1330


def test_missing_1330_oi_excludes_stock_instead_of_silently_shortening_day(raw, tmp_path):
    roots, _, _ = raw
    path = roots.futures_5m / "EXAMPLE30JANFUT_5minute.parquet"
    frame = pd.read_parquet(path)
    frame = frame.loc[~frame.timestamp.eq(pd.Timestamp(f"{DAY} 13:30", tz="Asia/Kolkata"))]
    frame.to_parquet(path, index=False)
    result = replay.replay_day(DAY, tmp_path / "missing-afternoon-oi", roots=roots)
    assert result["state"] == "BLOCKED_INCOMPLETE_DATA"
    assert result["metrics"] is None
    assert result["coverage"]["excluded_stocks"][0]["symbol"] == "EXAMPLE"
    assert any(row["reason"] == "NO_ELIGIBLE_STOCKS"
               for row in result["coverage"]["blocking_problems"])


def test_dashboard_describes_cutoff_backtest_separately_from_production(tmp_path, monkeypatch):
    monkeypatch.setattr(flow, "DATA_ROOT", tmp_path)
    path = tmp_path / "daily_results.csv"
    path.write_text("day,trades,wins,losses,net_pnl\n2026-10-13,0,0,0,0\n")
    metadata = dict(strategy="V13-V10-G-3", kind="backtest", verified=True,
        run_name="afternoon", id="afternoon", period_start="2026-10-13", period_end="2026-10-13",
        sessions=1, active_setup_count=51, current_full_strategy=True,
        strategy_version=replay.contract.STRATEGY_VERSION, strategy_fingerprint=replay.contract.strategy_fingerprint(),
        strategy_matches_active=False, strategy_matches_backtest=True, role="backtest_history")
    monkeypatch.setattr(flow, "_verified_g3_current_backtest", lambda: [(metadata, path)])
    for name in ("_verified_g3_runs", "_verified_g3_0920_long_backtest", "_verified_g3_0920_daily",
                 "_verified_g3_research_runs"):
        monkeypatch.setattr(flow, name, lambda: [])
    result = flow.load_flow_data(strategy="V13-V10-G-3")
    copy = next(item for item in result["warnings"] if item.startswith("Current G-3 historical backtest:"))
    assert "51 setups" in copy and "targets <=0.75%" in copy
    assert "09:50 LONG (0.75%)" in copy and "10 other setups" in copy
    assert "PAPER/LIVE retain their separate 61-setup" in copy
    assert "share the same" not in copy
