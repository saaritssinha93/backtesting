"""Exploratory V14 V4 morning momentum filters on frozen full-pool data.

All arms keep the V14 V3 combined OI/body/wick rule. Additional momentum
filters act at the completed five-minute signal, before stock picker/quotas.
The parent V3 strict arm is replayed and checked against its sealed result.
"""
from __future__ import annotations

import argparse
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_indicator_lab_20261103 import selection as lab
from research import g14_momentum_features as features
from research import g14_oi_day_or_backtest as parent
from research import g14_oi_day_or_permissions as permission
from research import g14_oi_day_or_selection as selector
from research import g14_six_feature_execution as execution
from research import slot_win70_backtest as source
from research.g8_exit_engine import _flags, metrics


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs/strategies/v13_v10_g_14_momentum_v4_research.json"
PARENT_RESULT = parent.OUTROOT / "oi_day_or_no_body_wick_v3_strict_fixed"
ARMS = (
    "v3_combined", "nifty_session_005", "nifty_session_010", "nifty_bar_005",
    "nifty_both_align", "stock_relative_open", "stock_two_bar",
    "nifty_or_stock_relative",
)
EXPECTED_ARMS = {
    "v3_combined": {"rule": "NO_ADDITIONAL_MOMENTUM_GATE"},
    "nifty_session_005": {"rule": "SIGNED_NIFTY_FUTURES_FROM_OPEN_GE", "threshold_pct": .05},
    "nifty_session_010": {"rule": "SIGNED_NIFTY_FUTURES_FROM_OPEN_GE", "threshold_pct": .10},
    "nifty_bar_005": {"rule": "SIGNED_NIFTY_FUTURES_CURRENT_COMPLETED_5M_GE", "threshold_pct": .05},
    "nifty_both_align": {"rule": "SIGNED_NIFTY_SESSION_AND_CURRENT_BAR_GE_ZERO"},
    "stock_relative_open": {"rule": "SIGNED_STOCK_MINUS_NIFTY_FUTURES_FROM_OPEN_GE", "threshold_pct": .0},
    "stock_two_bar": {"rule": "LATER_SLOTS_CURRENT_AND_IMMEDIATELY_PREVIOUS_STOCK_BAR_DIRECTIONAL_RETURN_GT_ZERO", "opening_0920": "NATIVE_RULE_ONLY"},
    "nifty_or_stock_relative": {"rule": "SIGNED_NIFTY_SESSION_GE_005_OR_SIGNED_STOCK_MINUS_NIFTY_OPEN_GE_020", "nifty_threshold_pct": .05, "relative_threshold_pct": .20},
}
FIELDS = (
    "mom_nifty_session_signed_pct", "mom_nifty_bar_signed_pct",
    "mom_stock_bar_signed_pct", "mom_stock_prev_bar_signed_pct",
    "mom_stock_relative_open_signed_pct",
)


def sha(path: Path) -> str:
    digest = sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def config() -> dict:
    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    if cfg["parent_config_sha256"] != sha(selector.CONFIG):
        raise ValueError("V14 V3 parent config changed")
    if tuple(cfg["arms"]) != ARMS or cfg["arms"] != EXPECTED_ARMS:
        raise ValueError("V14 V4 arm specification changed")
    if cfg["cost_bps_round_trip"] != list(parent.COSTS):
        raise ValueError("V14 V4 cost specification changed")
    return cfg


def _number(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        raise ValueError(f"Missing causal momentum feature {column}")
    return pd.to_numeric(frame[column], errors="coerce")


def momentum_mask(frame: pd.DataFrame, arm: str, cfg: dict) -> pd.Series:
    """Return feature gate by candidate row, missing evidence false."""
    if arm not in ARMS:
        raise ValueError(f"Unknown momentum arm {arm}")
    rule = cfg["arms"][arm]
    if arm == "v3_combined":
        return pd.Series(True, index=frame.index)
    for name in FIELDS:
        if name not in frame:
            raise ValueError(f"Missing feature {name}")
    ses = _number(frame, "mom_nifty_session_signed_pct")
    bar = _number(frame, "mom_nifty_bar_signed_pct")
    stock = _number(frame, "mom_stock_bar_signed_pct")
    prior = _number(frame, "mom_stock_prev_bar_signed_pct")
    relative = _number(frame, "mom_stock_relative_open_signed_pct")
    # The inherited G7 Nifty-futures permission needs an exact completed bar
    # even for a stock-only filter. A missing market bar cannot be silently
    # accepted and then cause a different selection after quota allocation.
    valid_nifty = np.isfinite(ses) & np.isfinite(bar)
    if arm == "nifty_session_005":
        gate = ses.ge(rule["threshold_pct"])
    elif arm == "nifty_session_010":
        gate = ses.ge(rule["threshold_pct"])
    elif arm == "nifty_bar_005":
        gate = bar.ge(rule["threshold_pct"])
    elif arm == "nifty_both_align":
        gate = ses.ge(0) & bar.ge(0)
    elif arm == "stock_relative_open":
        gate = relative.ge(rule["threshold_pct"])
    elif arm == "stock_two_bar":
        opening = pd.to_datetime(frame.signal_ts, utc=True).dt.tz_convert(parent.IST).dt.strftime("%H:%M").eq("09:20")
        gate = opening | (stock.gt(0) & prior.gt(0))
    elif arm == "nifty_or_stock_relative":
        gate = ses.ge(rule["nifty_threshold_pct"]) | relative.ge(rule["relative_threshold_pct"])
    else:
        raise AssertionError(arm)
    return (valid_nifty & gate).fillna(False).astype(bool)


def select_day(candidates: pd.DataFrame, enriched: pd.DataFrame, arm: str,
               specs: dict, cfg: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply momentum before original core preference, picker and quotas."""
    if len(candidates) != len(enriched) or not candidates.sid.equals(enriched.sid):
        raise ValueError("Feature builder changed candidate row identity/order")
    prepared = lab.prepare(candidates)
    branch = selector.oi_day_or_branches(candidates).set_index("sid").oi_day_or_pass
    oi_gate = prepared.sid.map(branch)
    feature_gate = momentum_mask(enriched, arm, cfg)
    by_sid = pd.Series(feature_gate.to_numpy(bool), index=enriched.sid)
    momentum_gate = prepared.sid.map(by_sid)
    if oi_gate.isna().any() or momentum_gate.isna().any():
        raise ValueError("Prepared candidate SID lacks OI or momentum decision")
    extra = oi_gate.astype(bool) & momentum_gate.astype(bool)
    selected, audit = lab.select(
        prepared, variant=f"g14_v4_{arm}",
        removed_gates=("body", "max_wick", "oi_lower"),
        extra_mask=extra, all_slots=True, use_core=True,
    )
    for output in (selected, audit):
        for name in FIELDS:
            output[name] = output.sid.map(enriched.set_index("sid")[name])
        output["v14_v4_arm"] = arm
        output["native_stop_pct"] = output.setup_id.map({k: v["stop_pct"] for k, v in specs.items()})
        output["native_target_pct"] = output.setup_id.map({k: v["target_pct"] for k, v in specs.items()})
        output["stop_pct"] = output.native_stop_pct
        output["target_pct"] = output.native_target_pct
    if selected.sid.duplicated().any():
        raise ValueError("Duplicate V14 V4 selected SID")
    return selected, audit


def _parent_parity(daily: pd.DataFrame) -> None:
    archived = pd.read_csv(PARENT_RESULT / "daywise_by_cost.csv")
    original = archived.loc[archived.arm.eq("combined")].sort_values(["day", "cost_bps"])
    new = daily.loc[daily.arm.eq("v3_combined")].sort_values(["day", "cost_bps"])
    if len(original) != 63 * len(parent.COSTS) or len(new) != len(original):
        raise ValueError("V14 V3 parity has incomplete day/cost coverage")
    for column in ("day", "cost_bps", "selected_orders", "native_fills", "trades", "wins"):
        if original[column].reset_index(drop=True).astype(str).tolist() != new[column].reset_index(drop=True).astype(str).tolist():
            raise ValueError(f"V14 V3 parity mismatch: {column}")
    if not np.allclose(original.net_pnl_rs, new.net_pnl_rs, rtol=0, atol=1e-6):
        raise ValueError("V14 V3 strict daily P&L parity mismatch")


def _verify_parent_result() -> None:
    manifest = json.loads((PARENT_RESULT / "artifact_manifest.json").read_text(encoding="utf-8"))
    for name, digest in manifest["files"].items():
        if sha(PARENT_RESULT / name) != digest:
            raise ValueError(f"V14 V3 parent result changed: {name}")
    for relative, digest in manifest["code"].items():
        if sha(ROOT / relative) != digest:
            raise ValueError(f"V14 V3 parent code changed: {relative}")


def run(out: Path) -> dict:
    out = Path(out).resolve()
    if out.exists() or not out.is_relative_to(parent.OUTROOT.resolve()):
        raise ValueError("Use a new isolated folder inside the V14 run root")
    cfg = config()
    _verify_parent_result()
    days, input_manifest, _ = parent.inputs()
    _, source_days, base_config = source.source_inputs()
    if source_days != days[:len(source_days)]:
        raise ValueError("G7 allocation source calendar changed")
    _, _, specs = selector._settings()
    out.mkdir(parents=True)
    daily_rows, books, selected_rows, permission_rows, select_counts = [], [], [], [], []
    for index, day in enumerate(days, 1):
        candidates = pd.read_parquet(parent.INPUTS / "candidates_by_day" / f"{day}.parquet")
        minute = pd.read_parquet(parent.LAB / day / "minutes.parquet")
        enriched = features.enrich_day(candidates, day)
        for arm in ARMS:
            selected, audit = select_day(candidates, enriched, arm, specs, cfg)
            selected = selected.copy().reset_index(drop=True)
            if len(selected):
                selected["day"] = day
            if arm == "v3_combined":
                original, _ = selector.select_day(candidates, "combined")
                if set(selected.sid) != set(original.sid):
                    raise ValueError(f"V3 control selection differs: {day}")
            if len(selected):
                selected_rows.append(selected[["sid", "day", "setup_id", "tradingsymbol", "side",
                                               "signal_ts", *FIELDS, "v14_v4_arm"]].copy())
            ready = selected.execution_ready.eq(True) if "execution_ready" in selected else pd.Series(True, index=selected.index)
            executable = selected.loc[ready].copy().reset_index(drop=True)
            allowed = None
            if len(executable):
                allowed, market_audit = permission.allowed_day(executable, day, minute)
                market_audit["arm"] = arm
                permission_rows.append(market_audit)
            select_counts.append(dict(day=day, arm=arm, selected=len(selected),
                                      execution_ready=len(executable),
                                      feature_pass=int(momentum_mask(enriched, arm, cfg).sum()),
                                      oi_day_or_pass=int(selector.oi_day_or_mask(candidates).sum())))
            per_cost = execution.simulate_allocated(
                executable, minute, parent.COSTS, allowed=allowed,
                base_config=base_config, policy=f"g14_v4_{arm}_strict",
            )
            for cost, book in per_cost.items():
                row = dict(metrics(book, [day]), day=day) if len(book) else parent.zero(day)
                row.update(arm=arm, cost_bps=cost)
                daily_rows.append(row)
                if len(book):
                    book = book.copy()
                    book["arm"] = arm
                    book["cost_bps"] = cost
                    books.append(book)
        if index % 5 == 0 or index == len(days):
            print(f"{day} [{index}/{len(days)}]", flush=True)
    daily = pd.DataFrame(daily_rows)
    _parent_parity(daily)
    daily["period"] = np.where(daily.day.le(cfg["development_through"]), "development", "assessment")
    overall = aggregate(daily)
    orders = pd.concat(books, ignore_index=True, sort=False) if books else pd.DataFrame()
    daily.to_csv(out / "daywise_by_cost.csv", index=False)
    overall.to_csv(out / "overall_by_period_and_cost.csv", index=False)
    pd.DataFrame(select_counts).to_csv(out / "selection_counts.csv", index=False)
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
            trades=("sid", "size"),
            wins=("portfolio_net_profit_rupees", lambda x: int((x > 0).sum())),
            net_pnl_rs=("portfolio_net_profit_rupees", "sum"),
        ).reset_index()
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
                   parent_parity="V14 V3 strict selected counts, fills, trades, wins, and daily P&L match",
                   config_sha256=sha(CONFIG), candidate_manifest_sha256=sha(parent.INPUTS / "manifest.json"),
                   parent_artifact_manifest_sha256=sha(PARENT_RESULT / "artifact_manifest.json"),
                   development_through=cfg["development_through"],
                   limitations=["Previously researched sessions; October-November is not a virgin holdout.",
                                "Eight prespecified arms make any best-arm claim exploratory.",
                                "09:20 opening stock OI/price comparisons use the available four-minute proxy.",
                                "One-minute bar fills, stop-first ambiguity, and fixed round-trip costs are modeled."],
                   source_manifest=input_manifest)
    parent.dump(out / "summary.json", summary)
    code_files = (Path(__file__), CONFIG, ROOT / "research/g14_momentum_features.py",
                  selector.CONFIG, ROOT / "research/g14_oi_day_or_selection.py",
                  ROOT / "research/g14_oi_day_or_permissions.py",
                  ROOT / "research/g14_six_feature_execution.py")
    parent.dump(out / "artifact_manifest.json", dict(
        files={p.name: sha(p) for p in out.iterdir() if p.is_file() and p.name != "artifact_manifest.json"},
        code={str(p.relative_to(ROOT)): sha(p) for p in code_files}))
    print(overall.loc[overall.cost_bps.eq(9.) & overall.period.eq("all")].to_string(index=False), flush=True)
    return summary


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
                                 losses=int(part.losses.sum()), win_rate_pct=100*wins/n if n else 0.,
                                 net_pnl_rs=float(pnl.sum()), profit_factor=gain/loss if loss else np.nan,
                                 losing_days=int((pnl < -1e-8).sum()),
                                 worst_day_rs=float(pnl.min()) if len(pnl) else 0.,
                                 max_daily_close_drawdown_rs=float(np.max(np.maximum.accumulate(equity)-equity)),
                                 negative_trade_sum_abs_rs=loss))
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    out = args.output_dir or parent.OUTROOT / (
        datetime.now(parent.IST).strftime("%Y%m%dT%H%M%S") + "_momentum_v4_strict")
    run(out)
    print(out.resolve())


if __name__ == "__main__":
    main()
