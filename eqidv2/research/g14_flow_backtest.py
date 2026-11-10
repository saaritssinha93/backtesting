"""V14 V5 research: open-anchored VWAP, prior-clock RVOL and pressure proxy.

The three added entry conditions are isolated arms on the frozen V14 V3
combined prefilter candidate pool. Each arm reselects before quota, then
replays exact fills, inherited G7 permissions, exits and daily allocation.
No volume-delta claim is made from OHLCV data.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_indicator_lab_20261103 import selection as lab
from research import g14_avwap_features as avwap
from research import g14_rvol_features as rvol
from research import g14_momentum_backtest as previous
from research import g14_oi_day_or_backtest as parent
from research import g14_oi_day_or_permissions as permission
from research import g14_oi_day_or_selection as selector
from research import g14_six_feature_execution as execution
from research import slot_win70_backtest as source
from research.g8_exit_engine import _flags, metrics


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs/strategies/v13_v10_g_14_flow_v5_research.json"
ARMS = ("v3_combined", "avwap_side", "tod_rvol_ready", "tod_rvol_one", "pressure3_positive")
EXPECTED_ARMS = {
    "v3_combined": {"rule": "NO_ADDITIONAL_VOLUME_GATE"},
    "avwap_side": {"rule": "SIGNED_SIGNAL_CLOSE_DISTANCE_FROM_OPEN_ANCHORED_AVWAP_GT", "threshold_pct": 0.},
    "tod_rvol_ready": {"rule": "SAME_CLOCK_RVOL_READY_MATCHED_CONTROL",
                       "min_prior_sessions": 5, "warmup_policy": "BYPASS_EXTRA_RVOL_GATE_FIRST5_REPRESENTED_SESSIONS"},
    "tod_rvol_one": {"rule": "SAME_CLOCK_PRIOR20_MEDIAN_RVOL_GE", "threshold": 1.,
                     "min_prior_sessions": 5, "warmup_policy": "BYPASS_EXTRA_RVOL_GATE_FIRST5_REPRESENTED_SESSIONS"},
    "pressure3_positive": {"rule": "SIGNED_VOLUME_WEIGHTED_CLOSE_LOCATION_LAST_UP_TO3_COMPLETED_BARS_GT", "threshold": 0.},
}
FEATURES = (
    "flow_avwap", "flow_avwap_distance_signed_pct", "flow_avwap_slope_signed_pct",
    "flow_avwap_ready", "flow_avwap_slope_ready", "flow_avwap_source", "flow_anchor_clock",
    "flow_pressure3_signed", "flow_pressure3_ready", "flow_feature_ts",
    "flow_tod_rvol", "flow_tod_history_count", "flow_tod_ready",
    "flow_tod_current_valid", "flow_tod_prior_median_volume", "flow_tod_source",
)


def config() -> dict:
    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    if cfg["parent_config_sha256"] != previous.sha(selector.CONFIG):
        raise ValueError("V14 V3 configuration changed")
    if tuple(cfg["arms"]) != ARMS or cfg["arms"] != EXPECTED_ARMS:
        raise ValueError("V14 V5 research arms changed")
    if cfg["cost_bps_round_trip"] != list(parent.COSTS):
        raise ValueError("V14 V5 cost assumption changed")
    return cfg


def volume_mask(frame: pd.DataFrame, arm: str, *, day_index: int, cfg: dict) -> pd.Series:
    """Added signal-time gate by raw candidate row; unknown fails closed."""
    if arm not in ARMS:
        raise ValueError(arm)
    if arm == "v3_combined":
        return pd.Series(True, index=frame.index)
    needed = set(FEATURES) - set(frame)
    if needed:
        raise ValueError(f"V14 V5 missing flow features: {sorted(needed)}")
    rule = cfg["arms"][arm]
    if arm == "avwap_side":
        value = pd.to_numeric(frame.flow_avwap_distance_signed_pct, errors="coerce")
        mask = frame.flow_avwap_ready.eq(True) & np.isfinite(value) & value.gt(rule["threshold_pct"])
    elif arm in ("tod_rvol_ready", "tod_rvol_one"):
        value = pd.to_numeric(frame.flow_tod_rvol, errors="coerce")
        count = pd.to_numeric(frame.flow_tod_history_count, errors="coerce")
        ready = frame.flow_tod_ready.eq(True) & count.ge(rule["min_prior_sessions"])
        if day_index < rule["min_prior_sessions"]:
            # The source cohort starts July 29 and has no point-in-time
            # same-clock history earlier than the first represented day.
            mask = pd.Series(True, index=frame.index)
        else:
            mask = ready if arm == "tod_rvol_ready" else (
                ready & np.isfinite(value) & value.ge(rule["threshold"]))
    elif arm == "pressure3_positive":
        value = pd.to_numeric(frame.flow_pressure3_signed, errors="coerce")
        mask = frame.flow_pressure3_ready.eq(True) & np.isfinite(value) & value.gt(rule["threshold"])
    else:
        raise AssertionError(arm)
    return mask.fillna(False).astype(bool)


def select_day(candidates: pd.DataFrame, enriched: pd.DataFrame, arm: str,
               specs: dict, cfg: dict, day_index: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    if len(candidates) != len(enriched) or not candidates.sid.equals(enriched.sid):
        raise ValueError("V14 V5 flow features changed source candidate identities/order")
    prepared = lab.prepare(candidates)
    oi = selector.oi_day_or_branches(candidates).set_index("sid").oi_day_or_pass
    gate = volume_mask(enriched, arm, day_index=day_index, cfg=cfg)
    by_sid = pd.Series(gate.to_numpy(bool), index=enriched.sid)
    oi_gate, flow_gate = prepared.sid.map(oi), prepared.sid.map(by_sid)
    if oi_gate.isna().any() or flow_gate.isna().any():
        raise ValueError("Prepared V14 candidate lacks OI or flow feature decision")
    selected, audit = lab.select(
        prepared, variant=f"g14_v5_{arm}",
        removed_gates=("body", "max_wick", "oi_lower"),
        extra_mask=oi_gate.astype(bool) & flow_gate.astype(bool),
        all_slots=True, use_core=True,
    )
    feature_index = enriched.set_index("sid")
    for output in (selected, audit):
        for name in FEATURES:
            output[name] = output.sid.map(feature_index[name])
        output["v14_v5_arm"] = arm
        output["native_stop_pct"] = output.setup_id.map({key: spec["stop_pct"] for key, spec in specs.items()})
        output["native_target_pct"] = output.setup_id.map({key: spec["target_pct"] for key, spec in specs.items()})
        output["stop_pct"] = output.native_stop_pct
        output["target_pct"] = output.native_target_pct
    if selected.sid.duplicated().any():
        raise ValueError("Duplicate V14 V5 selected SID")
    return selected, audit


def aggregate(daily: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for arm in ARMS:
        for cost in parent.COSTS:
            group = daily.loc[daily.arm.eq(arm) & daily.cost_bps.eq(cost)].sort_values("day")
            for period in ("all", "development", "assessment"):
                part = group if period == "all" else group.loc[
                    group.day.le("2026-09-30") if period == "development" else group.day.gt("2026-09-30")]
                pnl = part.net_pnl_rs.to_numpy(float)
                equity = np.r_[0., pnl.cumsum()]
                n, wins = int(part.trades.sum()), int(part.wins.sum())
                gain = float(part.gross_winner_profit_rs.sum())
                loss = float(part.negative_trade_sum_abs_rs.sum())
                rows.append(dict(arm=arm, cost_bps=cost, period=period, sessions=len(part),
                                 selected_orders=int(part.selected_orders.sum()),
                                 native_fills=int(part.native_fills.sum()), trades=n, wins=wins,
                                 losses=int(part.losses.sum()), win_rate_pct=100*wins/n if n else 0.,
                                 net_pnl_rs=float(pnl.sum()), profit_factor=gain/loss if loss else np.nan,
                                 losing_days=int((pnl < -1e-8).sum()),
                                 worst_day_rs=float(pnl.min()) if len(pnl) else 0.,
                                 max_daily_close_drawdown_rs=float(np.max(np.maximum.accumulate(equity)-equity)),
                                 negative_trade_sum_abs_rs=loss))
    return pd.DataFrame(rows)


def run(out: Path) -> dict:
    out = Path(out).resolve()
    if out.exists() or not out.is_relative_to(parent.OUTROOT.resolve()):
        raise ValueError("Use a new isolated V14 run folder")
    cfg = config()
    previous._verify_parent_result()
    days, input_manifest, _ = parent.inputs()
    _, source_days, base_config = source.source_inputs()
    if source_days != days[:len(source_days)]:
        raise ValueError("G7 allocation source calendar changed")
    _, _, specs = selector._settings()
    rvol_state = rvol.PriorClockRvol()
    out.mkdir(parents=True)
    daily_rows, books, selected_rows, permission_rows, counts = [], [], [], [], []
    for position, day in enumerate(days):
        candidates = pd.read_parquet(parent.INPUTS / "candidates_by_day" / f"{day}.parquet")
        minute = pd.read_parquet(parent.LAB / day / "minutes.parquet")
        enriched = avwap.enrich_day(candidates, day)
        enriched = rvol_state.enrich_day(enriched, day)
        if not candidates.sid.equals(enriched.sid):
            raise ValueError("V14 V5 causal feature enrichment changed source order")
        for arm in ARMS:
            selected, audit = select_day(candidates, enriched, arm, specs, cfg, position)
            selected = selected.copy().reset_index(drop=True)
            if len(selected):
                selected["day"] = day
            if arm == "v3_combined":
                original, _ = selector.select_day(candidates, "combined")
                if set(original.sid) != set(selected.sid):
                    raise ValueError(f"V14 V3 selection parity differs: {day}")
            if len(selected):
                selected_rows.append(selected[["sid", "day", "setup_id", "tradingsymbol",
                                               "side", "signal_ts", *FEATURES, "v14_v5_arm"]].copy())
            ready = selected.execution_ready.eq(True) if "execution_ready" in selected else pd.Series(True, index=selected.index)
            executable = selected.loc[ready].copy().reset_index(drop=True)
            allowed = None
            if len(executable):
                allowed, market_audit = permission.allowed_day(executable, day, minute)
                market_audit["arm"] = arm
                permission_rows.append(market_audit)
            counts.append(dict(day=day, arm=arm, selected=len(selected),
                               execution_ready=len(executable),
                               feature_pass=int(volume_mask(enriched, arm, day_index=position, cfg=cfg).sum()),
                               flow_ready=int(enriched.flow_tod_ready.sum()) if arm in ("tod_rvol_ready", "tod_rvol_one") else np.nan,
                               global_warmup=position < cfg["arms"]["tod_rvol_one"]["min_prior_sessions"]))
            per_cost = execution.simulate_allocated(
                executable, minute, parent.COSTS, allowed=allowed,
                base_config=base_config, policy=f"g14_v5_{arm}_strict")
            for cost, book in per_cost.items():
                row = dict(metrics(book, [day]), day=day) if len(book) else parent.zero(day)
                row.update(arm=arm, cost_bps=cost)
                daily_rows.append(row)
                if len(book):
                    book = book.copy()
                    book["arm"] = arm
                    book["cost_bps"] = cost
                    books.append(book)
        if (position + 1) % 5 == 0 or position + 1 == len(days):
            print(f"{day} [{position+1}/{len(days)}]", flush=True)
    daily = pd.DataFrame(daily_rows)
    previous._parent_parity(daily)
    daily["period"] = np.where(daily.day.le(cfg["development_through"]), "development", "assessment")
    overall = aggregate(daily)
    orders = pd.concat(books, ignore_index=True, sort=False) if books else pd.DataFrame()
    daily.to_csv(out / "daywise_by_cost.csv", index=False)
    overall.to_csv(out / "overall_by_period_and_cost.csv", index=False)
    pd.DataFrame(counts).to_csv(out / "selection_counts.csv", index=False)
    if selected_rows:
        pd.concat(selected_rows, ignore_index=True).to_csv(out / "selected_order_features.csv", index=False)
    if permission_rows:
        pd.concat(permission_rows, ignore_index=True).to_csv(out / "permission_audit.csv", index=False)
    orders.to_csv(out / "all_orders.csv", index=False)
    if len(orders):
        done = orders.loc[_flags(orders.portfolio_executed, "executed")].copy()
        done.to_csv(out / "executed_trades.csv", index=False)
        done["slot"] = pd.to_datetime(done.signal_ts, utc=True).dt.tz_convert(parent.IST).dt.strftime("%H:%M")
        slots = done.groupby(["arm", "cost_bps", "slot", "side"]).agg(
            trades=("sid", "size"), wins=("portfolio_net_profit_rupees", lambda x: int((x > 0).sum())),
            net_pnl_rs=("portfolio_net_profit_rupees", "sum")).reset_index()
        slots["win_rate_pct"] = 100 * slots.wins / slots.trades
        slots.to_csv(out / "slot_side_results.csv", index=False)
    for cost in parent.COSTS:
        part = daily.loc[daily.cost_bps.eq(cost)].pivot(
            index="day", columns="arm", values=["trades", "wins", "net_pnl_rs"])
        part.columns = [f"{arm}_{field}" for field, arm in part.columns]
        for arm in ARMS[1:]:
            part[f"{arm}_minus_v3_rs"] = part[f"{arm}_net_pnl_rs"] - part["v3_combined_net_pnl_rs"]
        part.to_csv(out / f"daywise_{int(cost)}bps_comparison.csv", index_label="day")
    summary = dict(variant_id=cfg["variant_id"], scope="BACKTEST_ONLY",
                   execution_authority=False, sessions=len(days), first_day=days[0], last_day=days[-1],
                   parent_parity="V14 V3 strict selected counts, fills, trades, wins and daily P&L match",
                   config_sha256=previous.sha(CONFIG),
                   candidate_manifest_sha256=previous.sha(parent.INPUTS / "manifest.json"),
                   parent_artifact_manifest_sha256=previous.sha(previous.PARENT_RESULT / "artifact_manifest.json"),
                   limitations=["These 63 sessions were previously researched; the later period is not a virgin holdout.",
                                "AVWAP and pressure are bar-based proxies, not trade-level flow.",
                                "True aggressor buy-minus-sell volume delta is not available in these frozen sources.",
                                "Same-clock RVOL has five-session warmup from the represented calendar; native volume gate retained during global warmup.",
                                "One-minute bar fills, stop-first ambiguity and fixed costs are modeled."],
                   source_manifest=input_manifest)
    parent.dump(out / "summary.json", summary)
    code_files = (Path(__file__), CONFIG, ROOT / "research/g14_avwap_features.py",
                  ROOT / "research/g14_rvol_features.py", ROOT / "research/g14_momentum_backtest.py",
                  selector.CONFIG, ROOT / "research/g14_oi_day_or_selection.py",
                  ROOT / "research/g14_oi_day_or_permissions.py",
                  ROOT / "research/g14_six_feature_execution.py")
    parent.dump(out / "artifact_manifest.json", dict(
        files={p.name: previous.sha(p) for p in out.iterdir() if p.is_file() and p.name != "artifact_manifest.json"},
        code={str(p.relative_to(ROOT)): previous.sha(p) for p in code_files}))
    print(overall.loc[overall.cost_bps.eq(9.) & overall.period.eq("all")].to_string(index=False), flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    out = args.output_dir or parent.OUTROOT / (
        datetime.now(parent.IST).strftime("%Y%m%dT%H%M%S") + "_flow_v5_strict")
    run(out)
    print(out.resolve())


if __name__ == "__main__":
    main()
