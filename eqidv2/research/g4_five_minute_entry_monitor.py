"""Causal five-minute reversal observation for all selected G4 entry orders.

Minute one is observed with no active order. A clean order can fill from minute
two onward; while still pending, reversal closes in minutes one to five cancel
it. A trigger touch takes precedence over that same minute's later close.
No production configuration or result pointer is changed.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_bounded_exit_backtest as bounded
from research import g4_delayed_entry_research as prior
from research import g4_failed_breakout_research as sealed
from research import g4_g3_mirror_backtest as base
from research import g4_iterative_dataset as dataset
from research.g3_sl_target_simulator import simulate_exit

VERSION = "G4-FIVE-MINUTE-DYNAMIC-ENTRY-MONITOR-V1"
MODES = ("none", "range_break", "midpoint", "two_adverse_closes")


def monitored_entry(row, path, mode: str, monitor_bars: int = 5):
    """Return (native fill, prior close cancellation index), with no lookahead.

    The original ten-minute trigger deadline remains fixed. The first possible
    fill is bar index one; index zero is observed only after its close.
    """
    if mode not in MODES or type(monitor_bars) is not int or not 1 <= monitor_bars <= 9:
        raise ValueError("Unknown mode or invalid observation window")
    side = row["side"]
    if side not in ("LONG", "SHORT"):
        raise ValueError("Unknown G4 side")
    sign = 1 if side == "LONG" else -1
    low, high = float(row["confirmation_low"]), float(row["confirmation_high"])
    reference = float(row["confirmation_close"])
    if not np.isfinite([low, high, reference]).all() or not 0 < low <= reference <= high:
        raise ValueError("Invalid confirmation candle")
    closes = np.asarray(path["close"], dtype=float)
    if len(closes) < 10 or not np.isfinite(closes[:10]).all():
        raise ValueError("Original ten-minute entry path is required")
    found = prior.delayed_entry(row, path, delay_bars=1)
    fill_index = int(found[0]) if found is not None else 10
    adverse_run = 0
    for index in range(min(fill_index, monitor_bars)):
        close = float(closes[index])
        if mode == "range_break":
            reversed_now = close < low if sign == 1 else close > high
        elif mode == "midpoint":
            reversed_now = sign * (close - (low + high) / 2) < 0
        elif mode == "two_adverse_closes":
            adverse_run = adverse_run + 1 if sign * (close - reference) < 0 else 0
            reversed_now = adverse_run >= 2
        else:
            reversed_now = False
        if reversed_now:
            return None, index
    return found, None


def replay(original: pd.DataFrame, paths: dict, mode: str) -> pd.DataFrame:
    """Simulate native entries/exits, then allocate the full G4 portfolio."""
    engine = sealed.bounded.history.simulator.g.v9
    records = []
    for source in original.to_dict("records"):
        row = source.copy()
        path = paths[int(row["sid"])]
        found, cancelled_at = monitored_entry(row, path, mode)
        row.update(filled=False, position_open=False, entry_ts=pd.NaT,
            entry_price=np.nan, entry_path_index=np.nan, entry_gap_through=False,
            entry_overshoot_bps=np.nan, exit_ts=pd.NaT, exit_price=np.nan,
            exit_path_index=np.nan, gross_return_pct=np.nan, net_return_pct=np.nan,
            cost_pct=np.nan, exit_reason="UNFILLED",
            monitor_cancelled=cancelled_at is not None,
            monitor_cancel_index=cancelled_at)
        if found is not None:
            index, entry, trigger, gap, overshoot = found
            row.update(filled=True, entry_path_index=index, entry_price=entry,
                trigger_used=trigger, entry_gap_through=gap,
                entry_overshoot_bps=overshoot,
                entry_ts=pd.Timestamp(int(path["timestamp_ns"][index]), tz="UTC")
                    .tz_convert(base.production.IST))
            outcome = simulate_exit(path, index, entry, row["side"] == "LONG",
                1.2, float(row["native_target_pct"]),
                tighten_after_minutes=120, tightened_stop_pct=1.)
            row.update(outcome)
            row.update(cost_pct=.05, net_return_pct=outcome["gross_return_pct"]-.05)
        records.append(row)
    work = pd.DataFrame(records)
    settings = engine.V9Config(portfolio_capital_rupees=1000000.,
        capital_per_entry_rupees=100000., leverage_factor=5.,
        max_positions=10, cost_bps=5.)
    sized = engine.v5.apply_fixed_capital_model(work, 100000., 5.)
    ledger, _ = engine.v6.apply_portfolio_constraints(
        sized, settings.portfolio_config())
    ledger["day"] = ledger.day.astype(str).str[:10]
    ledger["strategy_version"] = VERSION + "-" + mode
    ledger["scope"] = "BACKTEST_ONLY"
    ledger["execution_authority"] = False
    return ledger


def run(output: Path):
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to((base.G4_ROOT / "research").resolve()):
        raise ValueError("Require a fresh isolated G4 research directory")
    source = prior.DEFAULT_DATASET
    original, _contexts, paths, proof = dataset.load(source)
    if len(proof["days"]) != 53 or len(original) != 543:
        raise ValueError("Unexpected sealed G4 cohort")
    days = list(proof["days"])
    reference = prior.replay(original, paths, delay_bars=1,
        require_hold=False, scenario="wait_1_bar")
    periods = {"all": days, "october_15": ["2026-10-15"],
        "october_16": ["2026-10-16"]}
    comparison, daily, changes, ledgers = [], [], [], {}
    for mode in MODES:
        book = replay(original, paths, mode)
        if mode == "none":
            bounded.history._same_native(book, reference)
            bounded.history._same_allocation(book, reference)
        ledgers[mode] = book
        delta = prior._changes(original, book, mode)
        delta["monitor_cancelled"] = book.monitor_cancelled.to_numpy()
        delta["monitor_cancel_index"] = book.monitor_cancel_index.to_numpy()
        changes.append(delta)
        for period, cohort in periods.items():
            metrics = bounded.stats(book, cohort)
            baseline = bounded.stats(original, cohort)
            subset = delta.loc[delta.day_baseline.isin(cohort)]
            comparison.append(dict(mode=mode, period=period, **metrics,
                net_change_rs=metrics["net_pnl_rs"]-baseline["net_pnl_rs"],
                cancelled_orders=int(subset.monitor_cancelled.sum()),
                skipped_baseline_winners=int((
                    subset.portfolio_net_profit_rupees_baseline.gt(0) &
                    ~subset.portfolio_executed_variant.eq(True)).sum()),
                skipped_baseline_losers=int((
                    subset.portfolio_net_profit_rupees_baseline.lt(0) &
                    ~subset.portfolio_executed_variant.eq(True)).sum())))
        daily.extend(dict(mode=mode, day=day, **bounded.stats(book, [day]))
            for day in days)
        print(mode, comparison[-len(periods)]["net_pnl_rs"], flush=True)
    output.mkdir(parents=True)
    pd.DataFrame(comparison).to_csv(output / "comparison.csv", index=False)
    pd.DataFrame(daily).to_csv(output / "daily_comparison.csv", index=False)
    pd.concat(changes, ignore_index=True).to_csv(output / "trade_changes.csv", index=False)
    for mode, book in ledgers.items():
        folder = output / mode
        folder.mkdir()
        book.to_csv(folder / "portfolio_trades.csv", index=False)
        saved = base.read_frozen_csv(folder / "portfolio_trades.csv")
        expected, observed = bounded.stats(book, days), bounded.stats(saved, days)
        if any(not np.isclose(observed[key], value, rtol=0, atol=1e-6)
               for key, value in expected.items()):
            raise ValueError("Saved ledger does not reconcile")
    summary = dict(variant_id=VERSION, scope="BACKTEST_ONLY",
        execution_authority=False, default_configuration_changed=False,
        source_dataset=str(source), source_manifest_sha256=base.sha256(source / "manifest.json"),
        method="Every selected G4 order observes minute one, can fill from minute two if clean, and cancels on a reversal close while still pending during minutes one to five. Same-minute trigger fills precede close cancellation. Original minute-ten expiry, gap fills, bounded exits, costs and full allocation are replayed; no candidate replacement.",
        modes={"none":"One-bar wait without reversal monitoring",
            "range_break":"Close across opposite confirmation-candle extreme",
            "midpoint":"Close across confirmation-candle midpoint",
            "two_adverse_closes":"Two successive closes adverse to confirmation close"},
        comparison=comparison,
        limitations=["Retrospective, previously examined 53 sessions; no untouched validation.",
            "A reversal after an order fills cannot retrospectively reject that fill; it needs a separately modeled exit rule.",
            "The 90-second confirmation-publication deadline is distinct from the ten-minute entry-trigger expiry in the inspected G3 runtime. G4 live order timing is not validated.",
            "Minute OHLC does not establish intraminute order sequence or broker latency; no candidate replacement after cancellation."])
    (output / "summary.json").write_text(json.dumps(summary, indent=2)+"\n", encoding="utf-8")
    code = (Path(__file__), Path(prior.__file__), Path(sealed.__file__),
        Path(dataset.__file__), Path(bounded.__file__))
    manifest = dict(files={p.relative_to(output).as_posix(): base.sha256(p)
        for p in output.rglob("*") if p.is_file()},
        code_sha256={str(p.resolve()): base.sha256(p) for p in code})
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2)+"\n", encoding="utf-8")
    return pd.DataFrame(comparison)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    output = args.output_dir or (base.G4_ROOT / "research" /
        datetime.now().strftime("%Y%m%dT%H%M%S_dynamic_5m_entry_v1"))
    result = run(output)
    print(result.to_string(index=False), flush=True)
    print(output, flush=True)


if __name__ == "__main__":
    main()
