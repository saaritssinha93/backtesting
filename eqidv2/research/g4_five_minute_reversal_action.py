"""Research-only five-minute G4 reversal response without an entry wait.

Native orders may fill immediately. A reversal close cancels only a still-
pending order; an already-filled trade can exit no sooner than the next open,
with native stop/target precedence. No live G4 setting is changed.
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
from research import g4_failed_breakout_exit as failed
from research import g4_failed_breakout_research as sealed
from research import g4_g3_mirror_backtest as base
from research import g4_iterative_dataset as dataset
from research.g3_sl_target_simulator import simulate_exit

VERSION = "G4-FIVE-MINUTE-NO-WAIT-REVERSAL-ACTION-V1"
MODES = ("none", "range_break", "midpoint", "adverse_03_2closes")
MONITOR_BARS = 5


def _signal(row, path, index: int, mode: str, reference: float, adverse_count: int):
    sign = 1 if row["side"] == "LONG" else -1
    close = float(path["close"][index])
    low, high = float(row["confirmation_low"]), float(row["confirmation_high"])
    if mode == "range_break":
        return (close < low if sign == 1 else close > high), 0
    if mode == "midpoint":
        return sign * (close - (low + high) / 2) < 0, 0
    if mode == "adverse_03_2closes":
        count = adverse_count + 1 if sign * (close / reference - 1) * 100 <= -.3 else 0
        return count >= 2, count
    return False, 0


def evaluate_order(row, path, mode: str) -> dict:
    """Decide from completed closes in first five post-confirmation minutes."""
    if mode not in MODES or row["side"] not in ("LONG", "SHORT"):
        raise ValueError("Invalid G4 monitoring mode or side")
    if len(path["close"]) < 10:
        raise ValueError("Complete original entry window required")
    found = prior.delayed_entry(row, path, delay_bars=0)
    fill_index = int(found[0]) if found is not None else 10
    adverse = 0
    for index in range(min(fill_index, MONITOR_BARS)):
        reversal, adverse = _signal(row, path, index, mode,
                                    float(row["trigger"]), adverse)
        if reversal:
            return dict(found=None, outcome=None, monitor_action="CANCEL",
                        signal_index=index)
    if found is None:
        return dict(found=None, outcome=None, monitor_action="NONE",
                    signal_index=None)
    entry_index, entry, _trigger, _gap, _overshoot = found
    is_long = row["side"] == "LONG"
    native = simulate_exit(path, entry_index, entry, is_long, 1.2,
        float(row["native_target_pct"]), tighten_after_minutes=120,
        tightened_stop_pct=1.)
    native_index = int(native["exit_path_index"])
    adverse = 0
    for index in range(entry_index, min(MONITOR_BARS, len(path["close"]))):
        # The native intrabar stop or target has already resolved this candle.
        if index >= native_index:
            break
        reversal, adverse = _signal(row, path, index, mode, entry, adverse)
        if not reversal or index + 1 >= len(path["close"]):
            continue
        following = index + 1
        opening = float(path["open"][following])
        sign = 1 if is_long else -1
        stop = entry * (1 - sign * 1.2 / 100)
        target = entry * (1 + sign * float(row["native_target_pct"]) / 100)
        # A resting stop gap or already-marketable target wins at that open.
        if (opening <= stop if is_long else opening >= stop):
            break
        if (opening >= target if is_long else opening <= target):
            break
        outcome = failed._open_result(native, path, entry_index, entry,
            is_long, following, opening, "FIVE_MINUTE_REVERSAL_EXIT")
        return dict(found=found, outcome=outcome, monitor_action="EARLY_EXIT",
                    signal_index=index)
    return dict(found=found, outcome=native, monitor_action="NONE",
                signal_index=None)


def replay(original: pd.DataFrame, paths: dict, mode: str) -> pd.DataFrame:
    engine = sealed.bounded.history.simulator.g.v9
    records = []
    for source in original.to_dict("records"):
        row = source.copy()
        path = paths[int(row["sid"])]
        decision = evaluate_order(row, path, mode)
        found = decision["found"]
        row.update(filled=False, position_open=False, entry_ts=pd.NaT,
            entry_price=np.nan, entry_path_index=np.nan, entry_gap_through=False,
            entry_overshoot_bps=np.nan, exit_ts=pd.NaT, exit_price=np.nan,
            exit_path_index=np.nan, gross_return_pct=np.nan, net_return_pct=np.nan,
            cost_pct=np.nan, exit_reason="UNFILLED",
            monitor_action=decision["monitor_action"],
            monitor_signal_index=decision["signal_index"])
        if found is not None:
            index, entry, trigger, gap, overshoot = found
            row.update(filled=True, entry_path_index=index, entry_price=entry,
                trigger_used=trigger, entry_gap_through=gap,
                entry_overshoot_bps=overshoot,
                entry_ts=pd.Timestamp(int(path["timestamp_ns"][index]), tz="UTC")
                    .tz_convert(base.production.IST))
            outcome = decision["outcome"]
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
    periods = {"all": days, "october_15": ["2026-10-15"],
        "october_16": ["2026-10-16"]}
    comparison, daily, changes, ledgers = [], [], [], {}
    for mode in MODES:
        book = replay(original, paths, mode)
        if mode == "none":
            sealed.parity(book, original)
        ledgers[mode] = book
        delta = prior._changes(original, book, mode)
        delta["monitor_action"] = book.monitor_action.to_numpy()
        delta["monitor_signal_index"] = book.monitor_signal_index.to_numpy()
        changes.append(delta)
        for period, cohort in periods.items():
            metrics = bounded.stats(book, cohort)
            baseline = bounded.stats(original, cohort)
            subset = delta.loc[delta.day_baseline.isin(cohort)]
            comparison.append(dict(mode=mode, period=period, **metrics,
                net_change_rs=metrics["net_pnl_rs"]-baseline["net_pnl_rs"],
                cancelled_orders=int(subset.monitor_action.eq("CANCEL").sum()),
                early_exits=int(subset.monitor_action.eq("EARLY_EXIT").sum()),
                baseline_losers_not_traded=int((
                    subset.portfolio_net_profit_rupees_baseline.lt(0) &
                    ~subset.portfolio_executed_variant.eq(True)).sum()),
                baseline_winners_not_traded=int((
                    subset.portfolio_net_profit_rupees_baseline.gt(0) &
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
        method="Native immediate entry, original ten-minute expiry. First five post-confirmation completed closes cancel unfilled orders or request next-open exits for filled trades. Same-minute fill and native stop/target precedence retained. Full portfolio and costs recalculated; no candidate replacement.",
        modes={"none":"Exact current G4 baseline",
            "range_break":"Close across opposite confirmation-candle extreme",
            "midpoint":"Close across confirmation-candle midpoint",
            "adverse_03_2closes":"Two successive closes at least 0.30 percent adverse to trigger while pending or actual entry after fill"},
        comparison=comparison,
        limitations=["Retrospective, previously examined 53 sessions; no untouched validation.",
            "A filled entry cannot be rejected retroactively; reversal exits at following open, subject to native stop and target priority.",
            "Minute OHLC does not establish intraminute order sequence, broker latency or queue position.",
            "The 90-second confirmation-publication deadline is distinct from the ten-minute entry-trigger expiry in the inspected G3 runtime; G4 live integration is not validated.",
            "No candidate replacement after pending cancellation."])
    (output / "summary.json").write_text(json.dumps(summary, indent=2)+"\n", encoding="utf-8")
    code = (Path(__file__), Path(prior.__file__), Path(failed.__file__),
        Path(sealed.__file__), Path(dataset.__file__), Path(bounded.__file__))
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
        datetime.now().strftime("%Y%m%dT%H%M%S_no_wait_5m_reversal_v1"))
    result = run(output)
    print(result.to_string(index=False), flush=True)
    print(output, flush=True)


if __name__ == "__main__":
    main()
