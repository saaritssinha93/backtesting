"""Research only: adverse depth and close-observed recovery timing on sealed paths."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "outputs/v13_sl_tradeoff_20261004/pullback_analysis.json"
spec = importlib.util.spec_from_file_location("sl_sweep", ROOT / ".codex_tmp/v13_g2_sl_sweep.py")
sweep = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(sweep)


def stamp(value):
    return str(pd.Timestamp(int(value), tz="UTC").tz_convert("Asia/Kolkata"))


def describe(values):
    values = np.asarray(list(values), dtype=float)
    if not len(values):
        return {"count": 0}
    return {"count": len(values), "minimum": float(values.min()), "median": float(np.median(values)),
            "p75": float(np.quantile(values, .75)), "p90": float(np.quantile(values, .9)),
            "maximum": float(values.max())}


def intervals(mask, timestamps):
    runs = []
    start = None
    for index, below in enumerate(mask):
        if below and start is None:
            start = index
        elif not below and start is not None:
            runs.append({"start": stamp(timestamps[start]), "end": stamp(timestamps[index]),
                         "duration_minutes": float((timestamps[index] - timestamps[start]) / 60e9),
                         "recovered": True})
            start = None
    if start is not None:
        runs.append({"start": stamp(timestamps[start]), "end": stamp(timestamps[-1]),
                     "duration_minutes": float((timestamps[-1] - timestamps[start]) / 60e9),
                     "recovered": False})
    return runs


def underwater_timer_exit(path, entry_i, native_exit_i, entry, is_long, target_pct, stop_pct, minutes):
    """Signal on completed closes, execute at following open with resting-order priority."""
    direction = 1.0 if is_long else -1.0
    stop_level = entry * (1.0 - direction * stop_pct / 100.0)
    target_level = entry * (1.0 + direction * target_pct / 100.0)
    underwater_since = None
    # Native exits terminate the path before that bar's close. A timer signal on
    # the preceding bar may instead execute at its next open, before later extrema.
    for i in range(entry_i, native_exit_i):
        close_return = (float(path["close"][i]) / entry - 1.0) * 100.0 * direction
        if close_return >= 0:
            underwater_since = None
            continue
        if underwater_since is None:
            underwater_since = int(path["timestamp_ns"][i])
        if (int(path["timestamp_ns"][i]) - underwater_since) / 60e9 < minutes:
            continue
        next_i = i + 1
        opening = float(path["open"][next_i])
        stop_at_open = opening <= stop_level if is_long else opening >= stop_level
        target_at_open = opening >= target_level if is_long else opening <= target_level
        if stop_at_open:
            price, reason = opening, "STOP_GAP_BEFORE_TIMER"
        elif target_at_open:
            price, reason = target_level, "TARGET_AT_OPEN_BEFORE_TIMER"
        else:
            price, reason = opening, "TIME_UNDERWATER_NEXT_OPEN"
        return next_i, price, reason, i
    return None


def validate_synthetic_timer():
    def make_path(bars):
        return {"timestamp_ns": np.arange(1, len(bars) + 1, dtype=np.int64) * 60_000_000_000,
                **{name: np.asarray([bar[i] for bar in bars], dtype=float)
                   for i, name in enumerate(("open", "high", "low", "close"))}}
    path = make_path([(100, 100.1, 99.8, 99.9), (99.9, 100, 99.3, 99.4), (99.2, 100, 99.1, 99.5)])
    assert underwater_timer_exit(path, 0, 2, 100, True, 2, 1.25, 1) == (2, 99.2, "TIME_UNDERWATER_NEXT_OPEN", 1)
    # Same completed close is 99.4; the fill must use next open 99.2.
    assert path["close"][1] != underwater_timer_exit(path, 0, 2, 100, True, 2, 1.25, 1)[1]
    path["open"][2] = 98
    assert underwater_timer_exit(path, 0, 2, 100, True, 2, 1.25, 1) == (2, 98.0, "STOP_GAP_BEFORE_TIMER", 1)
    path["open"][2] = 103
    assert underwater_timer_exit(path, 0, 2, 100, True, 2, 1.25, 1) == (2, 102.0, "TARGET_AT_OPEN_BEFORE_TIMER", 1)
    short = make_path([(100, 100.2, 99.9, 100.1), (100.1, 100.7, 100, 100.6), (100.2, 100.4, 99.9, 100)])
    assert underwater_timer_exit(short, 0, 2, 100, False, 2, 1.25, 1) == (2, 100.2, "TIME_UNDERWATER_NEXT_OPEN", 1)
    assert underwater_timer_exit(short, 0, 1, 100, False, 2, 1.25, 1) is None
    return 6


def main():
    synthetic_checks = validate_synthetic_timer()
    published, segments, days = sweep.prepared_segments()
    paths_by_key = {}
    ledgers = {}
    for sl in (0.5, 0.75, 1.0, 1.1, 1.2, 1.25, 3.0):
        frames = []
        for segment, orders, paths in segments:
            frame, _, _ = sweep.ext._simulate(orders, paths, published["base"], stop_pct=sl)
            frame["analysis_key"] = [f"{segment}|{sid}" for sid in frame.sid]
            for sid, path in paths.items():
                paths_by_key[f"{segment}|{sid}"] = path
            frames.append(frame)
        trades = pd.concat(frames, ignore_index=True, sort=False)
        ledger, _ = sweep.g2.g.v9.v6.apply_portfolio_constraints(trades, published["base"].portfolio_config())
        ledgers[sl] = ledger.loc[ledger.portfolio_executed.eq(True)].set_index("analysis_key", drop=False)
    assert len(ledgers[3.0]) == 85
    assert not ledgers[3.0].exit_reason.eq("STOP").any(), "3% does not act as no-stop in these paths"
    for ledger in ledgers.values():
        assert set(ledger.index) == set(ledgers[3.0].index)

    analyses = []
    for key, row in ledgers[3.0].iterrows():
        path = paths_by_key[key]
        entry_i, end_i = int(row.entry_path_index), int(row.exit_path_index)
        entry = float(row.entry_price)
        direction = 1.0 if row.side == "LONG" else -1.0
        ts = path["timestamp_ns"][entry_i:end_i + 1]
        close = (path["close"][entry_i:end_i + 1] / entry - 1.0) * 100.0 * direction
        adverse_field = "low" if direction > 0 else "high"
        adverse = (path[adverse_field][entry_i:end_i + 1] / entry - 1.0) * 100.0 * direction
        good_runs = intervals(close < 0, ts)
        crossings = []
        for threshold in (0.5, .75, 1.0, 1.1, 1.2, 1.25):
            hits = np.flatnonzero(adverse <= -threshold + 1e-12)
            if not len(hits):
                crossings.append({"adverse_threshold_pct": threshold, "crossed": False})
                continue
            hit = int(hits[0])
            crossing = {"adverse_threshold_pct": threshold, "crossed": True, "first_cross_ts": stamp(ts[hit]),
                        "minutes_after_entry": float((ts[hit] - ts[0]) / 60e9),
                        "entry_minute": hit == 0,
                        "close_same_bar_pct": float(close[hit]),
                        "worst_adverse_from_cross_pct": float(-adverse[hit:].min()),
                        "additional_adverse_beyond_threshold_pct": float(max(0, -adverse[hit:].min() - threshold))}
            for name, level in (("breakeven", 0.0), ("net_positive", float(row.cost_pct))):
                # Use strictly later closes to avoid inventing within-minute high/low ordering.
                recoveries = np.flatnonzero(close[hit + 1:] >= level + (1e-10 if name == "net_positive" else 0.0))
                recovery = hit + 1 + int(recoveries[0]) if len(recoveries) else None
                crossing[f"{name}_recovered"] = recovery is not None
                crossing[f"{name}_recovery_ts"] = stamp(ts[recovery]) if recovery is not None else None
                crossing[f"minutes_cross_to_{name}"] = float((ts[recovery] - ts[hit]) / 60e9) if recovery is not None else None
            crossings.append(crossing)
        item = {"key": key, "day": str(row.day)[:10], "symbol": row.tradingsymbol, "side": row.side,
                "setup": str(row.setup_id),
                "entry_ts": str(row.entry_ts), "entry_price": entry, "exit_ts": str(row.exit_ts),
                "eventual_exit": row.exit_reason, "eventual_net_rupees": float(row.portfolio_net_profit_rupees),
                "eventual_net_return_pct": float(row.net_return_pct),
                "eventual_positive": bool(row.portfolio_net_profit_rupees > 0),
                "holding_minutes": float(row.holding_minutes),
                "maximum_adverse_pct": float(max(0.0, -adverse.min())),
                "maximum_adverse_excluding_entry_bar_pct": float(max(0.0, -adverse[1:].min())) if len(adverse) > 1 else None,
                "maximum_adverse_excluding_entry_and_exit_bars_pct": float(max(0.0, -adverse[1:-1].min())) if len(adverse) > 2 else None,
                "minutes_close_underwater_count": int((close < 0).sum()),
                "max_observed_close_underwater_run_minutes": max((r["duration_minutes"] for r in good_runs), default=0.0),
                "underwater_runs": good_runs, "crossings": crossings,
                "stop_variants": {str(sl): {"exit_ts": str(ledger.loc[key, "exit_ts"]),
                                           "exit_reason": str(ledger.loc[key, "exit_reason"]),
                                           "net_rupees": float(ledger.loc[key, "portfolio_net_profit_rupees"])}
                                  for sl, ledger in ledgers.items()}}
        analyses.append(item)

    groups = {}
    for name, selected in (("eventual_positive", [a for a in analyses if a["eventual_positive"]]),
                           ("eventual_negative", [a for a in analyses if not a["eventual_positive"]])):
        groups[name] = {"count": len(selected),
                        "maximum_adverse_pct": describe(a["maximum_adverse_pct"] for a in selected),
                        "max_close_underwater_run_minutes": describe(a["max_observed_close_underwater_run_minutes"] for a in selected),
                        "holding_minutes": describe(a["holding_minutes"] for a in selected)}
    stopped1 = [a for a in analyses if a["stop_variants"]["1.0"]["exit_reason"] == "STOP"]
    rescued125 = [a for a in stopped1 if a["stop_variants"]["1.25"]["net_rupees"] > 0]
    threshold_summary = []
    for threshold in (.5, .75, 1.0, 1.1, 1.2, 1.25):
        crossed = [(a, c) for a in analyses for c in a["crossings"] if c["adverse_threshold_pct"] == threshold and c["crossed"]]
        threshold_summary.append({"adverse_threshold_pct": threshold, "trades_crossed": len(crossed),
            "eventual_winners": sum(a["eventual_positive"] for a, c in crossed),
            "eventual_losers": sum(not a["eventual_positive"] for a, c in crossed),
            "recovered_breakeven_close": sum(c["breakeven_recovered"] for a, c in crossed),
            "recovered_net_positive_close": sum(c["net_positive_recovered"] for a, c in crossed),
            "winner_minutes_to_breakeven_after_cross": describe(c["minutes_cross_to_breakeven"] for a, c in crossed if a["eventual_positive"] and c["breakeven_recovered"]),
            "loser_minutes_to_breakeven_after_cross": describe(c["minutes_cross_to_breakeven"] for a, c in crossed if not a["eventual_positive"] and c["breakeven_recovered"])})

    # Signal after N elapsed minutes of consecutive underwater closes, then fill
    # the following open; resting gap-through stop/target orders take precedence.
    timed = []
    for stop in (1.0, 1.1, 1.2, 1.25):
        baseline = ledgers[stop]
        for minutes in (30, 60, 90, 120, 180):
            rows = []
            changed = []
            for key, original in baseline.iterrows():
                row = original.copy()
                path = paths_by_key[key]
                start_i, end_i = int(row.entry_path_index), int(row.exit_path_index)
                entry = float(row.entry_price)
                direction = 1.0 if row.side == "LONG" else -1.0
                timed_exit = underwater_timer_exit(path, start_i, end_i, entry, row.side == "LONG",
                                                   float(row.native_target_pct), stop, minutes)
                if timed_exit is not None:
                    i, exit_price, reason, signal_i = timed_exit
                    ret = float((exit_price / entry - 1.0) * 100.0 * direction)
                    bar_end = pd.Timestamp(int(path["timestamp_ns"][i]), tz="UTC").tz_convert("Asia/Kolkata")
                    row["exit_ts"] = bar_end - pd.Timedelta(minutes=1)
                    row["exit_reason"] = reason
                    row["exit_path_index"] = i
                    row["exit_price"] = exit_price
                    row["holding_minutes"] = float((row["exit_ts"] - row.entry_ts).total_seconds() / 60)
                    row["gross_return_pct"] = ret
                    row["net_return_pct"] = ret - float(row.cost_pct)
                    row["portfolio_gross_profit_rupees"] = float(row.exposure_per_entry_rupees) * ret / 100
                    row["portfolio_net_profit_rupees"] = row.portfolio_gross_profit_rupees - float(row.portfolio_cost_rupees)
                    changed.append({"key": key, "symbol": row.tradingsymbol, "day": str(row.day)[:10],
                                    "baseline_net": float(original.portfolio_net_profit_rupees),
                                    "timed_net": float(row.portfolio_net_profit_rupees), "time_exit": str(row.exit_ts),
                                    "exit_reason": reason, "exit_event": "OPEN", "exit_bar_end_ts": str(bar_end),
                                    "signal_close_ts": stamp(path["timestamp_ns"][signal_i]),
                                    "signal_close_price": float(path["close"][signal_i]), "execution_price": exit_price})
                rows.append(row)
            result = pd.DataFrame(rows)
            pnl = result.portfolio_net_profit_rupees.astype(float)
            daily = result.assign(_day=pd.to_datetime(result.day).dt.strftime("%Y-%m-%d")).groupby("_day").portfolio_net_profit_rupees.sum()
            day_pnl = daily.reindex([str(day) for day in days], fill_value=0.0)
            cum = np.r_[0.0, day_pnl.cumsum().to_numpy(float)]
            timed.append({"stop_pct": stop, "max_underwater_minutes": minutes,
                          "changed_trades": len(changed), "wins_cut_to_losses": sum(c["baseline_net"] > 0 and c["timed_net"] < 0 for c in changed),
                          "wins": int((pnl > 0).sum()), "win_rate_pct": float((pnl > 0).mean() * 100),
                          "net_rupees": float(pnl.sum()), "average_loss_rupees": float(pnl[pnl < 0].mean()),
                          "worst_loss_rupees": float(pnl.min()),
                          "max_daily_drawdown_rupees": float((np.maximum.accumulate(cum) - cum).max()),
                          "positive_active_days": int((day_pnl > 0).sum()), "changes": changed})

    output = {"window": [str(days[0]), str(days[-1])], "sessions": len(days), "fills": len(analyses),
        "synthetic_timer_checks_passed": synthetic_checks,
        "no_stop_reference": {"stop_pct": 3.0, "stop_exits": 0, "net_rupees": float(ledgers[3.0].portfolio_net_profit_rupees.sum())},
        "methodology": ["Uses sealed same-selected-order full 1-minute paths, actual native entry fill/index, retained target and 15:15 exit.",
            "Adverse excursions run only until native first retained target or 15:15; no post-exit recovery is used.",
            "Intrabar adverse depth includes entry/target bars as the official simulator does; OHLC order within those bars is unknown. Sensitivities exclude boundary bars.",
            "Recovery time uses a strictly later completed 1-minute close at breakeven or above 0.05% gross for net-positive after 5bps costs. This is conservative and does not assert intrabar chronology.",
            "Close-underwater runs are elapsed minutes from first negative close to first nonnegative close/end; count of underwater closes is also reported.",
            "Simple time rules signal on completed closes and execute at the following minute open. Resting stop gaps fill at that adverse open, then target gaps at the retained target take precedence; otherwise the timer fills at that open. Native stops/targets before the signal prevent timers. No same-signal-close fills are used.",
            "Timestamps are minute-end labels. A timer's actual open execution timestamp equals execution-candle end minus one minute; signal-close and execution-bar-end timestamps and both prices are retained. Native entries retain conservative source bar-end time labels.",
            "Simple time variants retain identical 85 fills and have no additional slippage allowance beyond next-open and stop-gap execution; they have not been selected or validated out of sample."],
        "outcome_groups": groups, "threshold_summary": threshold_summary,
        "one_pct_stopped_count": len(stopped1), "one_pct_stopped_eventual_winners": sum(a["eventual_positive"] for a in stopped1),
        "one_pct_stopped": stopped1, "one_25_pct_rescued_winners": rescued125,
        "simple_causal_time_rules": timed, "all_trade_paths": analyses}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(output, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(OUT), "outcome_groups": groups, "threshold_summary": threshold_summary,
                      "rescued125": [{k: a[k] for k in ("day", "symbol", "side", "entry_ts", "exit_ts", "maximum_adverse_pct", "crossings", "stop_variants")} for a in rescued125],
                      "time_rule_summary": [{k: v for k,v in t.items() if k != "changes"} for t in timed]}, indent=2))


if __name__ == "__main__":
    main()
