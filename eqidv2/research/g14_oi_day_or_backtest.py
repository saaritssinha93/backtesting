"""Backtest-only V14 V3 OI-day OR and body/wick ablations from the full pool.

The archived V14 V1 starts after G4/G6/G7 ancestor selections.  Its book is
reported for context; the exact change is measured against a same-source,
unchanged-rule full-pool control.  No live/paper configuration is changed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from research import g14_oi_day_or_selection as selector
from research import g14_six_feature_execution as execution
from research import slot_win70_backtest as source
from research.g8_exit_engine import _flags, metrics


ROOT = Path(__file__).resolve().parents[1]
DATA = Path("C:/TradingData/eqidv2")
INPUTS = DATA / "backtesting_result_v13_v10_g_14/six_feature_source"
LAB = ROOT / "g3_indicator_lab_20261103/inputs"
BASELINE = DATA / "backtesting_result_v13_v10_g_14/runs/2026-11-03/morning12_config_complete_v1"
OUTROOT = DATA / "backtesting_result_v13_v10_g_14/runs/2026-11-03"
IST = ZoneInfo("Asia/Kolkata")
COSTS = (5., 9., 15.)


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def dump(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str,
                               allow_nan=False) + "\n", encoding="utf-8")


def inputs() -> tuple[list[str], dict, dict]:
    manifest = read(INPUTS / "manifest.json")
    frozen = read(LAB / "manifest.json")
    days = frozen["days"]
    if len(days) != 63 or days[-1] != "2026-11-02":
        raise ValueError("Expected 63 frozen sessions ending November 2")
    if manifest["days"] != len(days) or set(manifest["per_day_sha256"]) != {d + ".parquet" for d in days}:
        raise ValueError("V14 V3 candidate manifest/calendar mismatch")
    if sha(LAB / "manifest.json") != manifest["source_input_manifest_sha256"]:
        raise ValueError("Underlying full-pool manifest changed")
    lab_sessions = {x["day"]: x for x in frozen["sessions"]}
    for day in days:
        cp = INPUTS / "candidates_by_day" / f"{day}.parquet"
        mp = LAB / day / "minutes.parquet"
        if sha(cp) != manifest["per_day_sha256"][cp.name]:
            raise ValueError(f"Candidate source changed: {day}")
        if sha(mp) != lab_sessions[day]["minutes_sha256"]:
            raise ValueError(f"Minute source changed: {day}")
    old_manifest = read(BASELINE / "manifest.json")
    for name, digest in old_manifest["files"].items():
        if sha(BASELINE / name) != digest:
            raise ValueError(f"Archived V14 V1 result changed: {name}")
    baseline = pd.read_csv(BASELINE / "daywise_by_cost.csv")
    baseline = baseline.loc[baseline.strategy.eq("V13-V10-G-14")].copy()
    baseline["arm"] = "archived_v14_v1"
    if sorted(baseline.day.unique()) != days or set(baseline.cost_bps) != set(COSTS):
        raise ValueError("Archived V14 V1 comparison period differs")
    return days, manifest, baseline


def zero(day: str) -> dict:
    return dict(day=day, sessions=1, selected_orders=0, native_fills=0,
                trades=0, wins=0, losses=0, breakeven_trades=0,
                win_rate_pct=0., net_pnl_rs=0., gross_pnl_rs=0., cost_rs=0.,
                profitable_days=0, losing_days=0, flat_days=1,
                worst_day_rs=0., best_day_rs=0., losing_day_sum_abs_rs=0.,
                negative_trade_sum_abs_rs=0., negative_trade_pnl_rs=0.,
                gross_winner_profit_rs=0., daily_close_drawdown_rs=0.,
                max_drawdown_rs=0., average_net_trade_rs=0., profit_factor=np.nan,
                stop_exits=0, target_exits=0)


def aggregate(daily: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for arm in ("archived_v14_v1", *selector.ARMS):
        for cost in COSTS:
            group = daily.loc[daily.arm.eq(arm) & daily.cost_bps.eq(cost)].sort_values("day")
            for period in ("all", "development", "assessment"):
                part = group if period == "all" else group.loc[
                    group.day.le("2026-09-30") if period == "development"
                    else group.day.gt("2026-09-30")]
                pnl = part.net_pnl_rs.to_numpy(float)
                equity = np.r_[0., pnl.cumsum()]
                n, wins = int(part.trades.sum()), int(part.wins.sum())
                gain = float(part.gross_winner_profit_rs.sum())
                loss = float(part.negative_trade_sum_abs_rs.sum())
                rows.append(dict(arm=arm, cost_bps=cost, period=period,
                                 sessions=len(part), selected_orders=int(part.selected_orders.sum()),
                                 native_fills=int(part.native_fills.sum()),
                                 trades=n, wins=wins, losses=int(part.losses.sum()),
                                 win_rate_pct=100*wins/n if n else 0., net_pnl_rs=float(pnl.sum()),
                                 profit_factor=gain/loss if loss else np.nan,
                                 losing_days=int((pnl < -1e-8).sum()),
                                 worst_day_rs=float(pnl.min()) if len(pnl) else 0.,
                                 max_daily_close_drawdown_rs=float(np.max(np.maximum.accumulate(equity)-equity)),
                                 negative_trade_sum_abs_rs=loss))
    return pd.DataFrame(rows)


def run(out: Path, *, market_mode: str = "off") -> dict:
    out = Path(out).resolve()
    if out.exists() or not out.is_relative_to(OUTROOT.resolve()):
        raise ValueError("Use a fresh isolated folder inside V14 run root")
    if market_mode not in ("off", "strict"):
        raise ValueError("market_mode must be off or strict")
    days, input_manifest, old_daily = inputs()
    _, source_calendar, base_config = source.source_inputs()
    if source_calendar != days[:len(source_calendar)]:
        raise ValueError("G7 base allocation configuration changed")
    permission = None
    if market_mode == "strict":
        from research import g14_oi_day_or_permissions as permission
    out.mkdir(parents=True)
    daily, books, selection_counts, audit_counts, permissions = [], [], [], [], []
    for index, day in enumerate(days, 1):
        candidates = pd.read_parquet(INPUTS / "candidates_by_day" / f"{day}.parquet")
        minute = pd.read_parquet(LAB / day / "minutes.parquet")
        for arm in selector.ARMS:
            selected, audit = selector.select_day(candidates, arm)
            selected = selected.copy().reset_index(drop=True)
            if len(selected):
                selected["day"] = day
            for reason, count in audit.v9_decision.value_counts(dropna=False).items():
                audit_counts.append(dict(day=day, arm=arm, decision=str(reason), rows=int(count)))
            ready = selected.execution_ready.eq(True) if "execution_ready" in selected else pd.Series(True, index=selected.index)
            # Rank and reserve the slot quota from signal evidence first.  A
            # missing next-minute confirmation skips that order, without using
            # the future observation to select a replacement at signal time.
            executable = selected.loc[ready].copy().reset_index(drop=True)
            allowed = None
            if market_mode == "strict" and len(executable):
                allowed, gate_audit = permission.allowed_day(executable, day, minute)
                gate_audit["arm"] = arm
                permissions.append(gate_audit)
            selection_counts.append(dict(day=day, arm=arm, selected=len(selected),
                                         execution_ready=len(executable), market_mode=market_mode))
            per_cost = execution.simulate_allocated(
                executable, minute, COSTS, allowed=allowed,
                base_config=base_config, policy=f"g14_v3_{arm}_{market_mode}")
            for cost, book in per_cost.items():
                row = dict(metrics(book, [day]), day=day) if len(book) else zero(day)
                row.update(arm=arm, cost_bps=cost, market_mode=market_mode)
                daily.append(row)
                if len(book):
                    book = book.copy()
                    book["arm"] = arm
                    book["cost_bps"] = cost
                    book["market_mode"] = market_mode
                    books.append(book)
        if index % 5 == 0 or index == len(days):
            latest = selection_counts[-len(selector.ARMS):]
            print(f"{day} [{index}/{len(days)}] " + " ".join(
                f"{row['arm']}={row['selected']}" for row in latest), flush=True)
    daily = pd.concat([old_daily, pd.DataFrame(daily)], ignore_index=True, sort=False)
    daily["period"] = np.where(daily.day.le("2026-09-30"), "development", "assessment")
    overall = aggregate(daily)
    orders = pd.concat(books, ignore_index=True, sort=False) if books else pd.DataFrame()
    daily.to_csv(out / "daywise_by_cost.csv", index=False)
    overall.to_csv(out / "overall_by_period_and_cost.csv", index=False)
    pd.DataFrame(selection_counts).to_csv(out / "selection_counts.csv", index=False)
    pd.DataFrame(audit_counts).to_csv(out / "selection_decision_counts.csv", index=False)
    orders.to_csv(out / "all_orders.csv", index=False)
    if permissions:
        pd.concat(permissions, ignore_index=True, sort=False).to_csv(out / "permission_audit.csv", index=False)
    if len(orders):
        executed = orders.loc[_flags(orders.portfolio_executed, "executed")].copy()
        executed.to_csv(out / "executed_trades.csv", index=False)
        executed["slot"] = pd.to_datetime(executed.signal_ts, utc=True).dt.tz_convert(IST).dt.strftime("%H:%M")
        slots = executed.groupby(["arm", "cost_bps", "slot", "side"]).agg(
            trades=("sid", "size"),
            wins=("portfolio_net_profit_rupees", lambda x: int((x > 0).sum())),
            net_pnl_rs=("portfolio_net_profit_rupees", "sum")).reset_index()
        slots["win_rate_pct"] = 100*slots.wins/slots.trades
        slots.to_csv(out / "slot_side_results.csv", index=False)
    for cost in COSTS:
        part = daily.loc[daily.cost_bps.eq(cost)].pivot(
            index="day", columns="arm", values=["trades", "wins", "net_pnl_rs"])
        part.columns = [f"{arm}_{field}" for field, arm in part.columns]
        for arm in selector.ARMS[1:]:
            part[f"{arm}_minus_reconstructed_rs"] = (
                part[f"{arm}_net_pnl_rs"] - part["baseline_reconstructed_net_pnl_rs"])
        part.to_csv(out / f"daywise_{int(cost)}bps_comparison.csv", index_label="day")
    summary = dict(
        variant_id="V13-V10-G-14-OI-DAY-OR-NO-BODY-WICK-V3-RESEARCH",
        scope="BACKTEST_ONLY", execution_authority=False,
        market_mode=market_mode, sessions=len(days), first_day=days[0], last_day=days[-1],
        v3_config_sha256=sha(selector.CONFIG), v1_config_sha256=sha(selector.BASE_CONFIG),
        candidate_manifest_sha256=sha(INPUTS / "manifest.json"), archived_v1=str(BASELINE),
        comparison_contract=("Causal effect measured against same-source reconstructed control; "
                             "archived V14 V1 differs because it starts from G4/G6/G7 selected ancestry."),
        limitations=[
            "Previously researched 63-day history; no virgin holdout.",
            "09:20 OI delta is an observed four-minute proxy, not exact five-minute change.",
            "Full-pool V14 V1 reconstruction includes candidates absent in G4/G6/G7 ancestor selections.",
            ("Market-off disables inherited Nifty-futures and V4 low-efficiency "
             "postselection veto for all four same-source arms; not an exact V14 V1 replacement.")
            if market_mode == "off" else "Market permissions independently reconstructed from dated source."],
        source_manifest=input_manifest)
    dump(out / "summary.json", summary)
    code_files = (Path(__file__), selector.CONFIG,
                  ROOT / "research/g14_oi_day_or_selection.py",
                  ROOT / "research/g14_six_feature_execution.py")
    if market_mode == "strict":
        code_files += (ROOT / "research/g14_oi_day_or_permissions.py",)
    dump(out / "artifact_manifest.json", dict(
        files={p.name: sha(p) for p in out.iterdir() if p.is_file() and p.name != "artifact_manifest.json"},
        code={str(p.relative_to(ROOT)): sha(p) for p in code_files}))
    print(overall.loc[overall.cost_bps.eq(9.) & overall.period.eq("all")].to_string(index=False), flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--market-mode", choices=("off", "strict"), default="off")
    args = parser.parse_args()
    out = args.output_dir or OUTROOT / (datetime.now(IST).strftime("%Y%m%dT%H%M%S") +
                                        f"_oi_day_or_v3_{args.market_mode}")
    run(out, market_mode=args.market_mode)
    print(out.resolve())


if __name__ == "__main__":
    main()
