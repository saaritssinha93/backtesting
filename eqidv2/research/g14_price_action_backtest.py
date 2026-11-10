"""V14 V6 research: three fixed opening price-action entry patterns.

Evaluate each pattern on both the original unchanged-rule full-pool selector
and the relaxed V3 selector. Signal-time failed-breakout veto reselects before
quota; next-minute confirmation patterns veto after native selection without
retroactive quota replacement. All arms receive strict G7 market permissions,
new native fills/exits and fresh daily portfolio allocation.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_indicator_lab_20261103 import selection as lab
from research import g14_momentum_backtest as previous
from research import g14_oi_day_or_backtest as parent
from research import g14_oi_day_or_permissions as permission
from research import g14_oi_day_or_selection as selector
from research import g14_price_action_features as price_action
from research import g14_six_feature_execution as execution
from research import slot_win70_backtest as source
from research.g8_exit_engine import _flags, metrics


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs/strategies/v13_v10_g_14_price_action_v6_research.json"
PARENT_RESULT = previous.PARENT_RESULT
ARMS = (
    "original_control", "original_failed_breakout_veto",
    "original_confirmation_breakout", "original_touch_hold",
    "v3_control", "v3_failed_breakout_veto",
    "v3_confirmation_breakout", "v3_touch_hold",
)
PRICE_COLUMNS = (
    "pa_prior_ready", "pa_failed_breakout", "pa_confirmation_ready",
    "pa_confirmation_breakout", "pa_touch_hold",
    "pa_signal_feature_ts", "pa_confirmation_feature_ts", "pa_prior_signal_ts",
)
EXPECTED_HYPOTHESES = {
    "failed_breakout_veto": {
        "timing": "COMPLETED_5M_SIGNAL_BEFORE_PICKER_QUOTA",
        "rule": "REJECT_IF_CURRENT_BAR_BREAKS_PRIOR_EXACT_5M_EXTREME_BUT_CLOSES_BACK_INSIDE",
        "opening_0920": "BASE_RULE_ONLY", "missing_prior_after_0920": "REJECT",
    },
    "confirmation_breakout": {
        "timing": "COMPLETED_1M_CONFIRMATION_AFTER_PICKER_NO_QUOTA_REFILL",
        "rule": "LONG_CONFIRM_CLOSE_GT_SIGNAL_HIGH_OR_SHORT_CONFIRM_CLOSE_LT_SIGNAL_LOW",
    },
    "touch_hold": {
        "timing": "COMPLETED_1M_CONFIRMATION_AFTER_PICKER_NO_QUOTA_REFILL",
        "rule": "CONFIRM_BAR_RANGE_TOUCHES_SIGNAL_CLOSE_AND_CLOSES_IN_TRADE_DIRECTION",
        "interpretation": "OHLC_RANGE_PROXY_NOT_A_SEQUENCED_INTRABAR_RETEST",
    },
}


def config() -> dict:
    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    if cfg["v3_config_sha256"] != previous.sha(selector.CONFIG):
        raise ValueError("V14 V3 overlay changed")
    if tuple(cfg["arms"]) != ARMS:
        raise ValueError("V14 V6 arm list changed")
    if cfg["cost_bps_round_trip"] != list(parent.COSTS):
        raise ValueError("V14 V6 cost specification changed")
    if cfg["hypotheses"] != EXPECTED_HYPOTHESES:
        raise ValueError("V14 V6 price-action hypothesis changed")
    return cfg


def _base(arm: str) -> tuple[str, str]:
    if arm not in ARMS:
        raise ValueError(f"Unknown V14 V6 arm {arm}")
    base = "original" if arm.startswith("original_") else "v3"
    rule = arm[len(base) + 1:]
    return base, rule


def _native_select(candidates: pd.DataFrame, base: str) -> pd.DataFrame:
    name = "baseline_reconstructed" if base == "original" else "combined"
    selected, _ = selector.select_day(candidates, name)
    return selected


def select_day(candidates: pd.DataFrame, enriched: pd.DataFrame, arm: str,
               specs: dict) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Return accepted orders, confirmation audit and selection counts."""
    if len(candidates) != len(enriched) or not candidates.sid.equals(enriched.sid):
        raise ValueError("V14 V6 feature builder changed source SID order")
    missing = set(PRICE_COLUMNS) - set(enriched)
    if missing:
        raise ValueError(f"Missing price-action features: {sorted(missing)}")
    base, rule = _base(arm)
    feature_index = enriched.set_index("sid")
    if rule == "failed_breakout_veto":
        prepared = lab.prepare(candidates)
        stamp = pd.to_datetime(prepared.signal_ts, utc=True).dt.tz_convert(parent.IST)
        opening = stamp.dt.strftime("%H:%M").eq("09:20")
        prior = prepared.sid.map(feature_index.pa_prior_ready)
        failed = prepared.sid.map(feature_index.pa_failed_breakout)
        if prior.isna().any() or failed.isna().any():
            raise ValueError("V14 V6 prepared SID lacks prior-bar decision")
        price_gate = opening | (prior.astype(bool) & ~failed.astype(bool))
        if base == "v3":
            oi = selector.oi_day_or_branches(candidates).set_index("sid").oi_day_or_pass
            oi_gate = prepared.sid.map(oi)
            if oi_gate.isna().any():
                raise ValueError("V14 V6 candidate lacks OI OR decision")
            extra = price_gate & oi_gate.astype(bool)
            removed = ("body", "max_wick", "oi_lower")
        else:
            extra = price_gate
            removed = ()
        selected, _ = lab.select(
            prepared, variant=f"g14_v6_{arm}", removed_gates=removed,
            extra_mask=extra, all_slots=True, use_core=True)
        for output in (selected,):
            output["native_stop_pct"] = output.setup_id.map(
                {key: spec["stop_pct"] for key, spec in specs.items()})
            output["native_target_pct"] = output.setup_id.map(
                {key: spec["target_pct"] for key, spec in specs.items()})
            output["stop_pct"] = output.native_stop_pct
            output["target_pct"] = output.native_target_pct
        before_filter = selected.copy()
        reserved = len(selected)
        confirmation_rejects = 0
    else:
        selected = _native_select(candidates, base)
        before_filter = selected.copy()
        reserved = len(selected)
        confirmation_rejects = 0
        if rule in ("confirmation_breakout", "touch_hold") and len(selected):
            ready = selected.sid.map(feature_index.pa_confirmation_ready)
            passed = selected.sid.map(feature_index[
                "pa_confirmation_breakout" if rule == "confirmation_breakout" else "pa_touch_hold"])
            if ready.isna().any() or passed.isna().any():
                raise ValueError("V14 V6 selected SID lacks confirmation decision")
            keep = ready.astype(bool) & passed.astype(bool)
            confirmation_rejects = int((~keep).sum())
            selected = selected.loc[keep].copy().reset_index(drop=True)
    selected = selected.copy()
    for output in (selected, before_filter):
        for name in PRICE_COLUMNS:
            output[name] = output.sid.map(feature_index[name])
        output["v14_v6_arm"] = arm
    if selected.sid.duplicated().any():
        raise ValueError("Duplicate V14 V6 selected SID")
    audit = before_filter[["sid", "day", "setup_id", "tradingsymbol", "side",
                           "signal_ts", "confirmation_ts", *PRICE_COLUMNS,
                           "v14_v6_arm"]].copy()
    audit["accepted_after_confirmation"] = audit.sid.isin(selected.sid)
    audit["confirmation_veto"] = ~audit.accepted_after_confirmation
    return selected, audit, dict(reserved_before_confirmation=reserved,
                                 confirmation_rejects=confirmation_rejects,
                                 accepted_after_confirmation=len(selected))


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
                rows.append(dict(arm=arm, cost_bps=cost, period=period,
                                 sessions=len(part), selected_orders=int(part.selected_orders.sum()),
                                 native_fills=int(part.native_fills.sum()), trades=n, wins=wins,
                                 losses=int(part.losses.sum()), win_rate_pct=100*wins/n if n else np.nan,
                                 net_pnl_rs=float(pnl.sum()), profit_factor=gain/loss if loss else np.nan,
                                 losing_days=int((pnl < -1e-8).sum()),
                                 worst_day_rs=float(pnl.min()) if len(pnl) else 0.,
                                 max_daily_close_drawdown_rs=float(np.max(np.maximum.accumulate(equity)-equity)),
                                 negative_trade_sum_abs_rs=loss))
    return pd.DataFrame(rows)


def _parent_parity(daily: pd.DataFrame) -> None:
    archived = pd.read_csv(PARENT_RESULT / "daywise_by_cost.csv")
    for new, old in (("original_control", "baseline_reconstructed"),
                     ("v3_control", "combined")):
        left = daily.loc[daily.arm.eq(new)].sort_values(["day", "cost_bps"]).reset_index(drop=True)
        right = archived.loc[archived.arm.eq(old)].sort_values(["day", "cost_bps"]).reset_index(drop=True)
        if len(left) != 63 * len(parent.COSTS) or len(left) != len(right):
            raise ValueError(f"V14 V6 parent coverage mismatch: {new}")
        for column in ("day", "cost_bps", "selected_orders", "native_fills", "trades", "wins"):
            if left[column].astype(str).tolist() != right[column].astype(str).tolist():
                raise ValueError(f"V14 V6 parent parity mismatch: {new} {column}")
        if not np.allclose(left.net_pnl_rs, right.net_pnl_rs, rtol=0, atol=1e-6):
            raise ValueError(f"V14 V6 parent P&L mismatch: {new}")


def _wilson_lower(wins: int, trades: int, z: float = 1.959963984540054) -> float:
    if trades <= 0:
        return np.nan
    p = wins / trades
    den = 1 + z*z/trades
    return 100 * (p + z*z/(2*trades) - z*np.sqrt(p*(1-p)/trades + z*z/(4*trades*trades))) / den


def _slot_result(done: pd.DataFrame, days: list[str]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if done.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    frame = done.copy()
    frame["slot"] = pd.to_datetime(frame.signal_ts, utc=True).dt.tz_convert(parent.IST).dt.strftime("%H:%M")
    frame["period"] = np.where(frame.day.le("2026-09-30"), "development", "assessment")
    cells = pd.MultiIndex.from_product([
        ARMS, parent.COSTS, ("all", "development", "assessment"),
        ("09:20", "09:25", "09:30", "09:35", "09:40", "09:45"), ("LONG", "SHORT")],
        names=["arm", "cost_bps", "period", "slot", "side"])
    values = []
    for arm, cost, period, slot, side in cells:
        part = frame.loc[frame.arm.eq(arm) & frame.cost_bps.eq(cost) & frame.slot.eq(slot)
                         & frame.side.eq(side)]
        if period != "all":
            part = part.loc[part.period.eq(period)]
        pnl = part.portfolio_net_profit_rupees.to_numpy(float)
        n, wins = len(part), int((pnl > 0).sum())
        values.append(dict(arm=arm, cost_bps=cost, period=period, slot=slot, side=side,
                           trades=n, wins=wins, losses=int((pnl < 0).sum()),
                           win_rate_pct=100*wins/n if n else np.nan,
                           wilson95_lower_pct=_wilson_lower(wins, n),
                           net_pnl_rs=float(pnl.sum()),
                           profitable=bool(n and pnl.sum() > 0),
                           observed_gt70_with10=bool(n >= 10 and wins/n > .70)))
    slot_side = pd.DataFrame(values)
    slot_time = slot_side.groupby(["arm", "cost_bps", "period", "slot"], sort=False).agg(
        trades=("trades", "sum"), wins=("wins", "sum"), net_pnl_rs=("net_pnl_rs", "sum")).reset_index()
    slot_time["win_rate_pct"] = np.where(slot_time.trades.gt(0),
                                           100*slot_time.wins/slot_time.trades, np.nan)
    coverage = slot_side.groupby(["arm", "cost_bps", "period"], sort=False).agg(
        cells_with_trades=("trades", lambda x: int((x > 0).sum())),
        cells_with_10_trades=("trades", lambda x: int((x >= 10).sum())),
        cells_observed_gt70=("win_rate_pct", lambda x: int((x > 70).sum())),
        cells_gt70_with10=("observed_gt70_with10", "sum"),
        min_cell_trades=("trades", "min"),
        min_wilson95_lower_pct=("wilson95_lower_pct", "min")).reset_index()
    coverage["all_12_gt70_with10"] = coverage.cells_gt70_with10.eq(12)
    return slot_side, slot_time, coverage


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
    out.mkdir(parents=True)
    daily_rows, books, selected_rows, permission_rows, counts = [], [], [], [], []
    for position, day in enumerate(days, 1):
        candidates = pd.read_parquet(parent.INPUTS / "candidates_by_day" / f"{day}.parquet")
        minutes = pd.read_parquet(parent.LAB / day / "minutes.parquet")
        enriched = price_action.enrich_day(candidates, day)
        if not candidates.sid.equals(enriched.sid):
            raise ValueError("V14 V6 price features changed source candidate order")
        for arm in ARMS:
            selected, audit, selection = select_day(candidates, enriched, arm, specs)
            selected = selected.copy().reset_index(drop=True)
            if len(selected):
                selected["day"] = day
            if len(audit):
                selected_rows.append(audit)
            ready = selected.execution_ready.eq(True) if "execution_ready" in selected else pd.Series(True, index=selected.index)
            executable = selected.loc[ready].copy().reset_index(drop=True)
            allowed = None
            if len(executable):
                allowed, market_audit = permission.allowed_day(executable, day, minutes)
                market_audit["arm"] = arm
                permission_rows.append(market_audit)
            counts.append(dict(day=day, arm=arm, selected=len(selected),
                               execution_ready=len(executable), **selection))
            per_cost = execution.simulate_allocated(
                executable, minutes, parent.COSTS, allowed=allowed,
                base_config=base_config, policy=f"g14_v6_{arm}_strict")
            for cost, book in per_cost.items():
                row = dict(metrics(book, [day]), day=day) if len(book) else parent.zero(day)
                row.update(arm=arm, cost_bps=cost)
                daily_rows.append(row)
                if len(book):
                    book = book.copy()
                    book["arm"] = arm
                    book["cost_bps"] = cost
                    books.append(book)
        if position % 5 == 0 or position == len(days):
            print(f"{day} [{position}/{len(days)}]", flush=True)
    daily = pd.DataFrame(daily_rows)
    _parent_parity(daily)
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
        slot_side, slot_time, coverage = _slot_result(done, days)
        slot_side.to_csv(out / "slot_side_results.csv", index=False)
        slot_time.to_csv(out / "slot_time_results.csv", index=False)
        coverage.to_csv(out / "slot_70_coverage.csv", index=False)
    for cost in parent.COSTS:
        part = daily.loc[daily.cost_bps.eq(cost)].pivot(
            index="day", columns="arm", values=["trades", "wins", "net_pnl_rs"])
        part.columns = [f"{arm}_{field}" for field, arm in part.columns]
        for arm in ARMS:
            base = "original_control" if arm.startswith("original_") else "v3_control"
            if arm != base:
                part[f"{arm}_minus_own_control_rs"] = part[f"{arm}_net_pnl_rs"] - part[f"{base}_net_pnl_rs"]
        part.to_csv(out / f"daywise_{int(cost)}bps_comparison.csv", index_label="day")
    summary = dict(variant_id=cfg["variant_id"], scope="BACKTEST_ONLY",
                   execution_authority=False, sessions=len(days), first_day=days[0], last_day=days[-1],
                   parent_parity="Original full-pool and V3 control daily selected/fill/trade/win/P&L match sealed V3 strict result",
                   config_sha256=previous.sha(CONFIG),
                   candidate_manifest_sha256=previous.sha(parent.INPUTS / "manifest.json"),
                   parent_artifact_manifest_sha256=previous.sha(PARENT_RESULT / "artifact_manifest.json"),
                   limitations=["All 63 sessions were previously researched; Oct-Nov is not a virgin holdout.",
                                "Observed 70% on a tiny slot sample cannot establish a 70% true win probability.",
                                "Confirmation OHLC touch-and-hold does not establish intrabar sequence.",
                                "One-minute bar fills, stop-first ambiguity and fixed costs are modeled."],
                   source_manifest=input_manifest)
    parent.dump(out / "summary.json", summary)
    code_files = (Path(__file__), CONFIG, ROOT / "research/g14_price_action_features.py",
                  ROOT / "research/g14_momentum_backtest.py", selector.CONFIG,
                  ROOT / "research/g14_oi_day_or_selection.py",
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
        datetime.now(parent.IST).strftime("%Y%m%dT%H%M%S") + "_price_action_v6_strict")
    run(out)
    print(out.resolve())


if __name__ == "__main__":
    main()
