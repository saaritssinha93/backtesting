"""Read-only numerical comparison of new production vs preserved G3 evidence."""
from __future__ import annotations
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import numpy as np
import pandas as pd
import backtesting_result_v13_v10_g_3_daily as publisher
import fno_v13_v10_g_3_daily_replay as replay


def compare(left, right, keys, numbers, clocks=(), texts=()):
    left, right = left.copy(), right.copy()
    for field in clocks:
        left[field] = pd.to_datetime(left[field], utc=True, errors="coerce").astype(str)
        right[field] = pd.to_datetime(right[field], utc=True, errors="coerce").astype(str)
    left = left.sort_values(keys, kind="stable").reset_index(drop=True)
    right = right.sort_values(keys, kind="stable").reset_index(drop=True)
    assert len(left) == len(right), (len(left), len(right))
    assert left[keys].astype(str).equals(right[keys].astype(str)), "Trade identity mismatch"
    deltas = {}
    for field in numbers:
        a = pd.to_numeric(left[field], errors="coerce").to_numpy(float)
        b = pd.to_numeric(right[field], errors="coerce").to_numpy(float)
        np.testing.assert_allclose(a, b, atol=1e-7, rtol=0, equal_nan=True, err_msg=field)
        deltas[field] = float(np.nanmax(np.abs(a - b))) if len(a) and np.isfinite(a - b).any() else 0.
    for field in clocks:
        assert left[field].equals(right[field]), field
    for field in texts:
        assert left[field].fillna("").astype(str).equals(right[field].fillna("").astype(str)), field
    return dict(rows=len(left), keys=keys, maximum_absolute_deltas=deltas, timestamps_exact=True)


def main():
    root = publisher.DEFAULT_OUTPUT_ROOT
    history_dir, history = publisher._read_verified_pointer(root, "current_history.json", "cumulative_history")
    daily_dir, daily = publisher._read_verified_pointer(root, "current_run.json", "production_daily")
    ledger = pd.read_csv(history_dir / history["artifacts"]["portfolio_trades"]["path"])
    frozen_root = replay.runtime_dir("fno_oi", "strategy_research", "v13_corrected_v10_g_3",
                                   "frozen_20261008_long110_nextminute_v1")
    replay.contract.verify_frozen_config(frozen_root / "frozen_config.json")
    assert replay.sha256(frozen_root / "trades.csv") == replay.contract.FROZEN_LEDGER_SHA256
    frozen = pd.read_csv(frozen_root / "trades.csv")
    frozen_days = pd.read_csv(frozen_root / "daily_results.csv").day.astype(str).tolist()
    prefix = ledger.loc[ledger.day.astype(str).isin(frozen_days)]
    frozen_check = compare(prefix, frozen, ["day", "setup_id", "tradingsymbol", "side"],
        ["entry_price", "exit_price", "native_stop_pct", "native_target_pct", "portfolio_net_profit_rupees"],
        ["entry_ts", "exit_ts"], ["exit_reason"])
    keys = ["day", "setup_id", "tradingsymbol", "side"]
    a, b = prefix.sort_values(keys).reset_index(drop=True), frozen.sort_values(keys).reset_index(drop=True)
    assert a.filled.equals(b.filled)
    assert a.portfolio_executed.equals(b.portfolio_executed)
    extended_root = root / "runs" / "2026-10-10" / "20261010T_through_20261009_pullbacks"
    extended = pd.read_csv(extended_root / "trades_with_pullbacks.csv").rename(columns={
        "entry_time_ist": "entry_ts", "exit_time_ist": "exit_ts", "net_pnl": "portfolio_net_profit_rupees"})
    for column in ("entry_ts", "exit_ts"):
        stamps = pd.to_datetime(extended[column])
        extended[column] = stamps.dt.tz_localize("Asia/Kolkata") if stamps.dt.tz is None else stamps
    extended_days = pd.read_csv(extended_root / "daily_results.csv").day.astype(str).tolist()
    executed = ledger.loc[ledger.portfolio_executed.eq(True)]
    common_executed = executed.loc[executed.day.astype(str).isin(extended_days)]
    added_days = sorted(set(history["days"]) - set(extended_days))
    added = ledger.loc[ledger.day.astype(str).isin(added_days)]
    extended_check = compare(common_executed, extended, ["day", "tradingsymbol", "side", "entry_ts"],
        ["entry_price", "exit_price", "portfolio_net_profit_rupees", "active_stop_pct_at_exit"], ["entry_ts", "exit_ts"], ["exit_reason"])
    payload = dict(status="PASS", strategy_version=history["strategy_version"], strategy_fingerprint=history["strategy_fingerprint"],
        commands=["python backtesting_result_v13_v10_g_3_daily.py --date 2026-10-09",
                  "python backtesting_result_v13_v10_g_3_daily.py --history-through 2026-10-09 --jobs 2"],
        tests="python -m pytest tests/test_fno_v13_v10_g_3_daily_replay.py -q",
        latest_daily=dict(run_dir=str(daily_dir), manifest_sha256=replay.sha256(daily_dir / "run_manifest.json"),
                          metrics=daily["metrics"], coverage=daily["coverage"]),
        current_history=dict(run_dir=str(history_dir), manifest_sha256=replay.sha256(history_dir / "run_manifest.json"),
                             metrics=history["metrics"], coverage=history["coverage"]),
        frozen_accepted_46_session_comparison=dict(sessions=len(frozen_days), selected_orders=len(prefix),
            fills=int(prefix.portfolio_executed.eq(True).sum()), verification=frozen_check,
            frozen_ledger_sha256=replay.sha256(frozen_root / "trades.csv")),
        extended_48_session_comparison=dict(sessions=len(extended_days), fills=len(common_executed), verification=extended_check,
            archived_ledger_sha256=replay.sha256(extended_root / "trades_with_pullbacks.csv")),
        newly_usable_sessions=dict(days=added_days, selected_orders=len(added), fills=int(added.portfolio_executed.eq(True).sum()),
            net_profit_rupees=float(added.portfolio_net_profit_rupees.sum()),
            note="New independent raw reconstruction under disclosed per-stock eligibility; not part of archived46/48 samples"),
        original_archives_modified=False, completed_legacy_trade_outputs_used_as_execution_input=False)
    output = Path(__file__).with_name("production_backtest_evidence.json")
    replay.atomic_json(output, payload)
    print(json.dumps({"status": "PASS", "evidence": str(output), "history": history["metrics"],
                      "frozen46": {"selected": len(prefix), "fills": int(prefix.portfolio_executed.eq(True).sum())},
                      "extended48": {"fills": len(common_executed)}, "added_days": added_days}, indent=2))


if __name__ == "__main__":
    main()
