"""Replay native FNO V6 on the V8/V9 ten-session rollover snapshots.

This launcher is deliberately diagnostic-only.  It uses the immutable source
snapshots already authenticated by the V8/V9 rolling diagnostic, applies the
literal V6 BEST_NET setup book and legacy execution model, and writes to an
isolated run directory without changing canonical V6 artifacts or caches.
"""

from __future__ import annotations

import argparse
import json
import math
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common
import fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6 as v6
import fno_oi_ema_confirm_sweep as signal_builder
import fno_v5_hybrid_backtest as replay


SOURCE_ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / "v8_v9_last_10_backtests"
    / "rolling_diagnostic"
    / "composite_results"
)
OUTPUT_ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / "v6_last_10_rollover_diagnostic"
)
SESSION_DATES = (
    date(2026, 8, 12),
    date(2026, 8, 13),
    date(2026, 8, 14),
    date(2026, 8, 17),
    date(2026, 8, 18),
    date(2026, 8, 19),
    date(2026, 8, 20),
    date(2026, 8, 21),
    date(2026, 8, 24),
    date(2026, 8, 25),
)
NOTIONAL_TARGET_RS = 50_000.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cost-bps", type=float, default=15.0)
    parser.add_argument("--square-off", default="1530")
    parser.add_argument("--max-forward-bars", type=int, default=400)
    parser.add_argument("--composite-dir", type=Path, default=None)
    parser.add_argument("--out-root", type=Path, default=OUTPUT_ROOT)
    return parser.parse_args()


def latest_composite() -> Path:
    choices = sorted(
        (path for path in SOURCE_ROOT.glob("composite_*") if path.is_dir()),
        key=lambda path: path.name,
    )
    if not choices:
        raise FileNotFoundError(f"No V8/V9 rolling composite found below {SOURCE_ROOT}")
    return choices[-1]


def load_composite(path: Path) -> dict[str, Any]:
    summary_path = path.resolve() / "summary.json"
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "fno_v8_v9_rolling_diagnostic_composite_v1":
        raise ValueError(f"Unsupported V8/V9 rolling composite: {summary_path}")
    observed = tuple(pd.Timestamp(day).date() for day in payload.get("sessions", ()))
    if observed != SESSION_DATES:
        raise AssertionError(
            f"Expected the frozen ten-session schedule, observed {observed}"
        )
    return payload


def v8_children(composite: dict[str, Any]) -> dict[str, dict[str, Any]]:
    children = composite["strategies"]["V8_COMBINED"]["children"]
    result = {str(child["block"]): child for child in children}
    if set(result) != {"AUG", "SEP_ROLLOVER"}:
        raise AssertionError(f"Unexpected rolling blocks: {sorted(result)}")
    return result


def load_validated_snapshot(
    child: dict[str, Any], *, contract_filter: str
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any]]:
    snapshot_ref = child["source_snapshot"]
    snapshot = provenance.load_source_snapshot(snapshot_ref["manifest_path"])
    if snapshot["snapshot_fingerprint"] != snapshot_ref["snapshot_fingerprint"]:
        raise AssertionError("Composite and physical snapshot fingerprints differ")
    identity = child["universe"]
    mapped, universe_record = provenance.load_backtest_universe(
        universe_path=snapshot["universe_path"],
        universe_date=identity["master_date"],
        contract_month_contains=contract_filter,
        require_persisted_mapping=True,
        expected_file_sha256=identity["file_sha256"],
        expected_universe_sha256=identity["universe_sha256"],
        expected_mapped_universe_sha256=identity["mapped_universe_sha256"],
        expected_mapped_symbol_set_sha256=identity["mapped_symbol_set_sha256"],
    )
    snapshot, inventory = provenance.validate_source_snapshot(
        snapshot,
        mapped,
        universe_record,
        require_complete_sources=True,
    )
    return mapped, universe_record, snapshot, inventory


def replay_block(
    child: dict[str, Any],
    *,
    contract_filter: str,
    days: tuple[date, ...],
    cost_bps: float,
    square_off: str,
    max_forward_bars: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    mapped, universe_record, snapshot, inventory = load_validated_snapshot(
        child, contract_filter=contract_filter
    )
    print(
        f"[V6] building {contract_filter} signals for {len(days)} sessions "
        f"from {len(mapped)} mapped stocks",
        flush=True,
    )
    signals, paths = signal_builder.build_signal_table(
        set(days),
        square_off=square_off,
        max_forward_bars=max_forward_bars,
        mapped_universe=mapped,
        confirmation_policy=signal_builder.CONFIRMATION_POLICY_V6_STRICT,
        futures_5m_root=Path(snapshot["futures_5m_root"]),
        equity_1m_root=Path(snapshot["equity_1m_root"]),
    )
    if signals.empty:
        raise RuntimeError(f"V6 produced no broad signals for {contract_filter}")
    signals["day"] = pd.to_datetime(signals["day"]).dt.date
    observed_days = set(signals["day"])
    absent = sorted(set(days) - observed_days)
    if absent:
        print(
            f"[V6] no broad qualifying signal on {', '.join(map(str, absent))}",
            flush=True,
        )
    v6.v5.validate_cash_equity_signal_contract(signals, paths)
    orders = replay.replay_setups(
        signals,
        paths,
        cost_bps=cost_bps,
        setups=v6.ACTIVE_SETUPS,
    )
    if orders.empty:
        raise RuntimeError(f"V6 selected no orders for {contract_filter}")
    orders["contract"] = contract_filter
    orders["notional_target_rs"] = NOTIONAL_TARGET_RS
    trigger = pd.to_numeric(orders["trigger"], errors="raise")
    orders["qty_proxy"] = np.floor(NOTIONAL_TARGET_RS / trigger).astype(int)
    orders["net_pnl_rs"] = (
        orders["qty_proxy"]
        * trigger
        * pd.to_numeric(orders["net_return_pct"], errors="coerce")
        / 100.0
    )
    daily = replay.build_daily_curve(orders, list(days), split_day=days[0])
    daily["contract"] = contract_filter
    pnl_by_day = orders.groupby("day", sort=True)["net_pnl_rs"].sum()
    daily["net_pnl_rs"] = daily["day"].map(pnl_by_day).fillna(0.0)
    lineage = {
        "contract": contract_filter,
        "days": [day.isoformat() for day in days],
        "mapped_stocks": int(len(mapped)),
        "universe": universe_record,
        "snapshot_manifest": snapshot["manifest_path"],
        "snapshot_fingerprint": snapshot["snapshot_fingerprint"],
        "source_fingerprint": inventory["source_fingerprint"],
        "broad_signals": int(len(signals)),
        "orders": int(len(orders)),
        "fills": int(orders["filled"].sum()),
    }
    return orders, daily, lineage


def profit_factor(values: pd.Series) -> float:
    numbers = pd.to_numeric(values, errors="coerce").dropna().to_numpy(float)
    gains = float(numbers[numbers > 0].sum())
    losses = float(-numbers[numbers < 0].sum())
    if losses:
        return gains / losses
    return math.inf if gains else math.nan


def max_drawdown(values: pd.Series) -> float:
    cumulative = pd.to_numeric(values, errors="raise").cumsum().to_numpy(float)
    peaks = np.maximum.accumulate(np.concatenate(([0.0], cumulative)))[1:]
    return float(np.max(peaks - cumulative)) if cumulative.size else 0.0


def build_report(
    daily: pd.DataFrame,
    summary: dict[str, Any],
    *,
    run_dir: Path,
    composite_dir: Path,
) -> str:
    lines = [
        "# FnO V6 Last 10 Sessions - Rolling Diagnostic",
        "",
        f"Generated: `{summary['generated_at_ist']}`",
        "",
        (
            "Native V6 BEST_NET strict-confirmation replay on the same immutable "
            "AUG/SEP source snapshots and ten-session contract schedule used by "
            "the V8/V9 rolling diagnostic."
        ),
        "",
        (
            f"Economics: {summary['cost_bps']:g} bps round-trip cost, "
            f"Rs {NOTIONAL_TARGET_RS:,.0f} target cash-equity exposure per fill, "
            "and a requested 15:30 square-off using the last real cash bar."
        ),
        "",
        "Metric | Value",
        "--- | ---:",
        f"Sessions | {summary['sessions']}",
        f"Orders / fills | {summary['orders']} / {summary['fills']}",
        f"Wins / losses | {summary['wins']} / {summary['losses']}",
        f"Trade PF | {summary['profit_factor']:.3f}",
        f"Net return sum | {summary['net_return_percentage_points']:+.3f} pts",
        f"Sizing-proxy net P&L | Rs {summary['net_pnl_rs']:+,.2f}",
        f"Positive / negative / flat days | {summary['positive_days']} / {summary['negative_days']} / {summary['flat_days']}",
        f"Maximum cumulative daily drawdown | {summary['max_drawdown_percentage_points']:.3f} pts",
        "",
        "## Daily results",
        "",
        "Date | Contract | Orders | Fills | Return pts | Net P&L (Rs)",
        "--- | --- | ---: | ---: | ---: | ---:",
    ]
    for row in daily.itertuples(index=False):
        lines.append(
            f"{row.day} | {row.contract} | {int(row.selections)} | "
            f"{int(row.fills)} | {float(row.portfolio_net_return_pct):+.3f} | "
            f"{float(row.net_pnl_rs):+,.2f}"
        )
    lines += [
        "",
        "## Validity",
        "",
        "- Research-only rollover diagnostic; not promotion-grade or a canonical V6 headline.",
        "- AUG is used through Aug 21; the retrospectively reconstructed SEP universe is used on Aug 24-25.",
        "- The cash feed's last real bar is 15:15, so open trades use last-real-bar sensitivity rather than a genuine 15:30 close.",
        "- V6 uses exact trigger fills, no finite entry expiry, independent orders and no shared capital ledger; results are not execution-parity with V8/V9.",
        "",
        "## Artifacts",
        "",
        f"- Run directory: `{run_dir}`",
        f"- V8/V9 source composite: `{composite_dir}`",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    if args.cost_bps < 0:
        raise ValueError("cost-bps cannot be negative")
    v6.validate_configuration()
    composite_dir = (args.composite_dir or latest_composite()).resolve()
    composite = load_composite(composite_dir)
    children = v8_children(composite)
    aug_days = SESSION_DATES[:8]
    sep_days = SESSION_DATES[8:]
    aug_orders, aug_daily, aug_lineage = replay_block(
        children["AUG"],
        contract_filter="26AUG",
        days=aug_days,
        cost_bps=args.cost_bps,
        square_off=args.square_off,
        max_forward_bars=args.max_forward_bars,
    )
    sep_orders, sep_daily, sep_lineage = replay_block(
        children["SEP_ROLLOVER"],
        contract_filter="26SEP",
        days=sep_days,
        cost_bps=args.cost_bps,
        square_off=args.square_off,
        max_forward_bars=args.max_forward_bars,
    )
    orders = pd.concat((aug_orders, sep_orders), ignore_index=True, sort=False)
    daily = pd.concat((aug_daily, sep_daily), ignore_index=True, sort=False)
    daily = daily.sort_values("day", kind="stable").reset_index(drop=True)
    filled = orders.loc[orders["filled"]].copy()
    returns = pd.to_numeric(filled["net_return_pct"], errors="raise")
    day_returns = pd.to_numeric(daily["portfolio_net_return_pct"], errors="raise")
    generated_at = common.now_ist()
    summary: dict[str, Any] = {
        "schema_version": "fno_v6_last_10_rollover_diagnostic_v1",
        "generated_at_ist": generated_at.isoformat(timespec="microseconds"),
        "classification": "RETROSPECTIVELY_RECONSTRUCTED_ROLLOVER_DIAGNOSTIC",
        "research_only": True,
        "promotion_eligible": False,
        "headline_valid": False,
        "strategy_version": v6.STRATEGY_VERSION,
        "sessions": int(len(daily)),
        "orders": int(len(orders)),
        "fills": int(len(filled)),
        "wins": int((returns > 0).sum()),
        "losses": int((returns < 0).sum()),
        "flat_trades": int((returns == 0).sum()),
        "profit_factor": profit_factor(returns),
        "net_return_percentage_points": float(returns.sum()),
        "net_pnl_rs": float(filled["net_pnl_rs"].sum()),
        "max_drawdown_percentage_points": max_drawdown(day_returns),
        "positive_days": int((day_returns > 0).sum()),
        "negative_days": int((day_returns < 0).sum()),
        "flat_days": int((day_returns == 0).sum()),
        "cost_bps": float(args.cost_bps),
        "slippage_bps": 0.0,
        "requested_square_off": "15:30",
        "observed_cash_last_bar": "15:15",
        "eod_policy": "LAST_REAL_BAR_SENSITIVITY",
        "notional_target_rs": NOTIONAL_TARGET_RS,
        "source_composite": str(composite_dir),
        "source_composite_fingerprint": composite["composite_fingerprint"],
        "blocks": [aug_lineage, sep_lineage],
    }
    stamp = generated_at.strftime("%Y%m%dT%H%M%S%f%z")
    run_dir = Path(args.out_root).resolve() / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=False)
    daily_path = run_dir / "daily.csv"
    orders_path = run_dir / "orders.csv"
    summary_path = run_dir / "summary.json"
    report_path = run_dir / "report.md"
    common.atomic_write_csv(daily, daily_path)
    common.atomic_write_csv(orders, orders_path)
    common.atomic_write_json(summary_path, summary)
    common.atomic_write_text(
        report_path,
        build_report(
            daily,
            summary,
            run_dir=run_dir,
            composite_dir=composite_dir,
        ),
    )
    manifest = {
        "schema_version": "fno_v6_last_10_rollover_artifact_manifest_v1",
        "source_code": provenance.artifact_record(Path(__file__).resolve()),
        "artifacts": {
            "daily": provenance.artifact_record(daily_path),
            "orders": provenance.artifact_record(orders_path),
            "summary": provenance.artifact_record(summary_path),
            "report": provenance.artifact_record(report_path),
        },
    }
    manifest["manifest_fingerprint"] = common.canonical_json_sha256(manifest)
    common.atomic_write_json(run_dir / "manifest.json", manifest)
    common.atomic_write_json(
        Path(args.out_root).resolve() / "latest.json",
        {
            "schema_version": summary["schema_version"],
            "run_dir": str(run_dir),
            "summary": summary,
        },
    )
    print(json.dumps({"run_dir": str(run_dir), **summary}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
