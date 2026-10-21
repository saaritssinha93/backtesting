"""Descriptive, ex-post path diagnostics for the pinned original G4 cohort.

This module neither searches exit thresholds nor identifies tradable labels.
Only full completed post-entry minutes preceding the native exit are observed.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from research import g4_failed_breakout_research as sealed
from research import g4_g3_mirror_backtest as base


VERSION = "G4-ORIGINAL-COHORT-DESCRIPTIVE-PATHS-V1"
DEFAULT_SOURCE = base.G4_ROOT / "research/20261016T153442_one_minute_trade_monitor_v1"
EXPECTED_MANIFEST = "5fe22bca247160a8f0d69589dfd91d62025391f4f384b6e8d9bf8cb7cd1b7cae"
MINUTE_NS = 60_000_000_000
THRESHOLDS = (.2, .4, .6, .8)
HORIZONS = (5, 10, 15, 30, 60)


def load_paths(path):
    result = {}
    with np.load(path, allow_pickle=False) as archive:
        for key in archive.files:
            sid, field = key.split("_", 1)
            result.setdefault(int(sid), {})[field] = archive[key]
    return result


def eligible_minutes(row, path):
    """Return full post-entry candle indices, strictly before native exit."""
    stamps = np.asarray(path["timestamp_ns"])
    if len(stamps) == 0 or np.any(np.diff(stamps) != MINUTE_NS):
        raise ValueError("Contiguous minute paths required")
    if any(len(path[field]) != len(stamps) for field in ("open", "high", "low", "close")):
        raise ValueError("Path arrays have different lengths")
    entry_index = int(row["entry_path_index"])
    entry = pd.Timestamp(row["entry_ts"])
    cutoff = pd.Timestamp(row["exit_ts"])
    if not 0 <= entry_index < len(stamps) or int(stamps[entry_index]) != entry.value:
        raise ValueError("Entry candle identity differs from native ledger")
    indices = np.arange(len(stamps))
    return indices[(indices > entry_index) & (stamps - MINUTE_NS >= entry.value) & (stamps < cutoff.value)]


def trade_diagnostics(native, paths):
    """Describe the originally allocated trades, including no-observation cases."""
    records = []
    for row in native.loc[native.portfolio_executed.eq(True)].to_dict("records"):
        path = paths[int(row["sid"])]
        indices = eligible_minutes(row, path)
        entry = float(row["entry_price"])
        sign = 1 if row["side"] == "LONG" else -1
        if row["side"] not in ("LONG", "SHORT"):
            raise ValueError("Unknown side")
        closes = sign * (np.asarray(path["close"], float)[indices] / entry - 1.) * 100
        adverse = np.asarray(path["low" if sign == 1 else "high"], float)[indices]
        adverse = sign * (adverse / entry - 1.) * 100
        elapsed = (np.asarray(path["timestamp_ns"])[indices] - pd.Timestamp(row["entry_ts"]).value) / MINUTE_NS
        pnl = float(row["portfolio_net_profit_rupees"])
        record = dict(sid=int(row["sid"]), day=str(row["day"])[:10], setup_id=row["setup_id"],
            tradingsymbol=row["tradingsymbol"], side=row["side"], exit_reason=row["exit_reason"],
            original_net_rs=pnl, outcome="WIN" if pnl > 1e-8 else "LOSS" if pnl < -1e-8 else "FLAT",
            eligible_completed_minutes=len(indices),
            best_completed_close_profit_pct=float(max(0., closes.max())) if len(closes) else np.nan,
            worst_completed_close_adverse_pct=float(max(0., -closes.min())) if len(closes) else np.nan,
            worst_full_minute_adverse_pct=float(max(0., -adverse.min())) if len(adverse) else np.nan,
            first_positive_close_minutes=float(elapsed[np.flatnonzero(closes > 0)[0]]) if np.any(closes > 0) else np.nan)
        for threshold in THRESHOLDS:
            hits = np.flatnonzero(closes >= threshold - 1e-10)
            key = f"{threshold:.1f}pct"
            record["reached_" + key] = bool(len(hits))
            record["first_" + key + "_minutes"] = float(elapsed[hits[0]]) if len(hits) else np.nan
        for horizon in HORIZONS:
            at = np.flatnonzero(elapsed == horizon)
            active = len(at) == 1
            prefix = closes[elapsed <= horizon]
            record[f"active_at_{horizon}m"] = bool(active)
            record[f"best_close_by_{horizon}m_pct"] = float(max(0., prefix.max())) if active else np.nan
            record[f"gross_at_{horizon}m_pct"] = float(closes[at[0]]) if active else np.nan
        records.append(record)
    return pd.DataFrame(records)


def summarize(native, trades):
    threshold_rows, followthrough_rows, quantiles = [], [], []
    known = trades.eligible_completed_minutes.gt(0)
    for outcome in ("WIN", "LOSS"):
        cohort = trades.loc[trades.outcome.eq(outcome)]
        eligible = cohort.loc[cohort.eligible_completed_minutes.gt(0)]
        for threshold in THRESHOLDS:
            hits = eligible["reached_" + f"{threshold:.1f}pct"]
            first = eligible.loc[hits, "first_" + f"{threshold:.1f}pct_minutes"]
            threshold_rows.append(dict(outcome=outcome, close_profit_threshold_pct=threshold,
                original_trades=len(cohort), trades_with_eligible_minutes=len(eligible),
                reached_trades=int(hits.sum()), reached_pct=100 * hits.mean() if len(hits) else np.nan,
                original_net_of_reached_trades_rs=float(eligible.loc[hits, "original_net_rs"].sum()),
                median_first_reach_minutes=float(first.median()) if len(first) else np.nan))
    for name, cohort in (("original_winners", trades.loc[trades.outcome.eq("WIN")]),
                         ("target_winners", trades.loc[trades.outcome.eq("WIN") & trades.exit_reason.eq("TARGET")]),
                         ("original_losers", trades.loc[trades.outcome.eq("LOSS")])):
        for field in ("worst_completed_close_adverse_pct", "worst_full_minute_adverse_pct", "first_positive_close_minutes"):
            values = cohort[field].dropna()
            quantiles.append(dict(cohort=name, measure=field, observed_trades=len(values),
                **{f"p{int(q*100)}": float(values.quantile(q)) if len(values) else np.nan for q in (.25, .50, .75, .90, .95)}))
    for horizon in HORIZONS:
        active = trades[f"active_at_{horizon}m"]
        for label, threshold in (("no_positive_close", 0.), ("no_0.2pct_close", .2)):
            best = trades[f"best_close_by_{horizon}m_pct"]
            condition = best.le(0.) if threshold == 0 else best.lt(threshold - 1e-10)
            selected = trades.loc[active & condition]
            followthrough_rows.append(dict(horizon_minutes=horizon, condition=label,
                still_open_original_trades=int(active.sum()), matching_trades=len(selected),
                eventual_winners=int(selected.outcome.eq("WIN").sum()),
                eventual_losers=int(selected.outcome.eq("LOSS").sum()),
                original_winner_profit_at_risk_rs=float(selected.loc[selected.outcome.eq("WIN"), "original_net_rs"].sum()),
                original_loser_loss_amount_rs=float(-selected.loc[selected.outcome.eq("LOSS"), "original_net_rs"].sum())))
    losses = trades.loc[trades.outcome.eq("LOSS")].copy()
    losses["observed_profit_bucket"] = pd.cut(losses.best_completed_close_profit_pct,
        [-np.inf, 0., .2 - 1e-10, .4 - 1e-10, .6 - 1e-10, .8 - 1e-10, np.inf],
        labels=["never_positive_completed_close", "positive_below_0.2pct", "0.2_to_0.4pct", "0.4_to_0.6pct", "0.6_to_0.8pct", "at_least_0.8pct"]).astype(object)
    losses.loc[losses.eligible_completed_minutes.eq(0), "observed_profit_bucket"] = "no_eligible_completed_minutes"
    buckets = losses.groupby("observed_profit_bucket", dropna=False).agg(
        original_losing_trades=("sid", "size"), original_net_rs=("original_net_rs", "sum")).reset_index()
    buckets["loss_amount_rs"] = -buckets.original_net_rs
    buckets["share_of_original_loss_amount_pct"] = 100 * buckets.loss_amount_rs / -losses.original_net_rs.sum()
    setup = native.groupby("setup_id").agg(selected_orders=("sid", "size"), native_fills=("filled", "sum"),
        originally_allocated=("portfolio_executed", "sum")).reset_index()
    by_setup = trades.groupby("setup_id").agg(
        original_winners=("outcome", lambda x: int(x.eq("WIN").sum())),
        original_losers=("outcome", lambda x: int(x.eq("LOSS").sum())),
        observed_sessions=("day", "nunique"), original_net_rs=("original_net_rs", "sum"))
    setup = setup.merge(by_setup, on="setup_id", how="left")
    winner_profit = float(trades.loc[trades.outcome.eq("WIN"), "original_net_rs"].sum())
    summary = dict(version=VERSION, purpose="DESCRIPTIVE_EX_POST_ONLY_NOT_TRADABLE_LABELS",
        selected_orders=len(native), original_executed_trades=len(trades),
        original_winners=int(trades.outcome.eq("WIN").sum()), original_losers=int(trades.outcome.eq("LOSS").sum()),
        original_winner_combined_net_profit_rs=winner_profit,
        maximum_five_percent_winner_profit_loss_rs=.05 * winner_profit,
        minimum_original_winner_combined_profit_at_95pct_rs=.95 * winner_profit,
        original_loss_amount_rs=float(-losses.original_net_rs.sum()), original_net_rs=float(trades.original_net_rs.sum()),
        no_eligible_completed_minutes=int((~known).sum()),
        setup_count=len(setup), setups_fewer_than_10_allocated=int(setup.originally_allocated.lt(10).sum()),
        setups_fewer_than_30_allocated=int(setup.originally_allocated.lt(30).sum()),
        setups_with_zero_original_losers=int(setup.original_losers.fillna(0).eq(0).sum()),
        setups_with_fewer_than_five_original_losers=int(setup.original_losers.fillna(0).lt(5).sum()),
        limitations=["Outcome groups use the original eventual net result and cannot be known at decision time.",
            "Entry candle and native exit candle are excluded; no-observation trades are reported separately.",
            "Completed-close profit differs from an intraminute high/low excursion.",
            "Fixed horizons describe trades still active at that horizon, not a simulated exit's obtainable profit.",
            "Historical strategy development already inspected these sessions; chronological partitions are not untouched holdouts.",
            "Setup counts and 52 correlated sessions provide limited evidence for individual setup rules."])
    return summary, dict(profit_thresholds=pd.DataFrame(threshold_rows), loss_profit_buckets=buckets,
        failed_followthrough=pd.DataFrame(followthrough_rows), excursion_quantiles=pd.DataFrame(quantiles), setup_samples=setup)


def run(output, source=DEFAULT_SOURCE):
    source, output = Path(source).resolve(), Path(output).resolve()
    if base.sha256(source / "manifest.json") != EXPECTED_MANIFEST:
        raise ValueError("Pinned original monitor manifest changed")
    monitor = sealed.package(source)
    original_source = Path(monitor["source_run"])
    if base.sha256(original_source / "manifest.json") != monitor["source_manifest_sha256"]:
        raise ValueError("Original G4 source manifest changed")
    sealed.package(original_source)
    native = base.read_frozen_csv(source / "baseline/portfolio_trades.csv")
    original = base.read_frozen_csv(original_source / "g4_bounded_exit/portfolio_trades.csv")
    sealed.parity(native, original)
    paths = load_paths(source / "historical_paths.npz")
    trades = trade_diagnostics(native, paths)
    summary, tables = summarize(native, trades)
    summary.update(first_date=monitor["first_date"], last_date=monitor["last_date"], sessions=monitor["sessions"],
        source_monitor=str(source), source_monitor_manifest_sha256=EXPECTED_MANIFEST,
        original_source=str(original_source), original_source_manifest_sha256=monitor["source_manifest_sha256"],
        historical_paths_sha256=base.sha256(source / "historical_paths.npz"))
    output.mkdir(parents=True, exist_ok=True)
    trades.to_csv(output / "trade_diagnostics.csv", index=False)
    for name, table in tables.items():
        table.to_csv(output / (name + ".csv"), index=False)
    (output / "summary.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return summary, tables


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=base.ROOT / ".codex_tmp/g4_iterative_diagnostics")
    args = parser.parse_args()
    summary, tables = run(args.output_dir)
    print(json.dumps(summary, indent=2))
    print(tables["profit_thresholds"].to_string(index=False))
    print(tables["loss_profit_buckets"].to_string(index=False))
    print(tables["failed_followthrough"].to_string(index=False))
    print(tables["excursion_quantiles"].to_string(index=False))


if __name__ == "__main__":
    main()
