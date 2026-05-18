"""
AVWAP ID 5-min v3 backtester.

v3 keeps the profitable v2 cascade result fixed, then researches only the
remaining setup families. It greedily adds filtered pockets from a resolved
native all-setups trade file when the pocket has acceptable standalone PF and
the combined v3 PF does not fall below the configured floor.

Default inputs:
  - v2 baseline:
    C:\\TradingData\\eqidv2\\outputs_ID_v2_5min\\avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv
  - expansion pool:
    outputs_avwap_ID_5min_v2_all_setups_ungated\\trades.csv

Output:
  C:\\TradingData\\eqidv2\\outputs_ID_v3_5min
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


CAPITAL_PER_TRADE = 10_000.0
LEVERAGE = 5.0
EFFECTIVE_NOTIONAL = CAPITAL_PER_TRADE * LEVERAGE

BASELINE_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v2_5min")
OUT_ROOT = Path(r"C:\TradingData\eqidv2\outputs_ID_v3_5min")
DEFAULT_EXPANSION_CSV = Path("outputs_avwap_ID_5min_v2_all_setups_ungated/trades.csv")

V2_FIXED_SETUPS = {
    "A_MOD_BREAK_C1_HIGH",
    "A_MOD_BREAK_C1_LOW",
    "A_MOD_CLOSE_CONTINUATION_BREAK",
    "B_AVWAP_RECLAIM_REVERSAL",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
    "C_OR_BREAKDOWN",
    "C_OR_BREAKOUT",
    "D_EMA20_BOUNCE",
    "D_EMA20_REJECTION",
    "E_VWAP_BAND_FADE",
    "G_HIGHER_HIGH_BREAK",
}

MINE_FEATURES = [
    "quality_score",
    "rs_pct",
    "market_ret_pct",
    "vol_ratio",
    "close_loc",
    "body_pct",
    "vwap_dist_atr",
    "atr_pct",
    "day_value_so_far_rs",
    "_signal_hour",
    "bars_held",
]

QUANTILES = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95]


def _pf(pnl: pd.Series) -> float:
    s = pd.to_numeric(pnl, errors="coerce").fillna(0.0)
    gains = float(s[s > 0].sum())
    losses = float(-s[s <= 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else 0.0
    return gains / losses


def _latest_baseline_csv() -> Path:
    files = sorted(
        BASELINE_DIR.glob("avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise SystemExit(f"no v2 baseline CSV found in {BASELINE_DIR}")
    return files[0]


def _normalize_baseline(df: pd.DataFrame, source: str) -> pd.DataFrame:
    out = df.copy()
    if "trade_date" not in out.columns:
        out["trade_date"] = pd.to_datetime(out["entry_time_ist"], errors="coerce").dt.date.astype(str)
    if "pnl_pct_price" in out.columns:
        pnl = pd.to_numeric(out["pnl_pct_price"], errors="coerce").fillna(0.0) / 100.0 * EFFECTIVE_NOTIONAL
    elif "pnl_rs" in out.columns:
        pnl = pd.to_numeric(out["pnl_rs"], errors="coerce").fillna(0.0)
    else:
        raise SystemExit("baseline CSV has no pnl_pct_price/pnl_rs")
    out["pnl_rs_v3"] = pnl
    out["v3_source"] = source
    out["v3_rule"] = "fixed_v2_baseline"
    out["capital_per_trade_rs"] = CAPITAL_PER_TRADE
    out["leverage"] = LEVERAGE
    out["notional_exposure_rs"] = EFFECTIVE_NOTIONAL
    return out


def _normalize_expansion(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "trade_date" not in out.columns:
        if "date" in out.columns:
            out["trade_date"] = out["date"].astype(str)
        else:
            out["trade_date"] = pd.to_datetime(out["entry_ts"], errors="coerce").dt.date.astype(str)
    if "signal_ts" in out.columns:
        sig = pd.to_datetime(out["signal_ts"], errors="coerce")
        out["_signal_hour"] = sig.dt.hour + sig.dt.minute / 60.0
    elif "signal_time_ist" in out.columns:
        sig = pd.to_datetime(out["signal_time_ist"], errors="coerce")
        out["_signal_hour"] = sig.dt.hour + sig.dt.minute / 60.0
    else:
        out["_signal_hour"] = np.nan

    if "net_pnl_rs" in out.columns:
        pnl = pd.to_numeric(out["net_pnl_rs"], errors="coerce").fillna(0.0)
    elif "pnl_pct_price" in out.columns:
        pnl = pd.to_numeric(out["pnl_pct_price"], errors="coerce").fillna(0.0) / 100.0 * EFFECTIVE_NOTIONAL
    elif "pnl_rs" in out.columns:
        pnl = pd.to_numeric(out["pnl_rs"], errors="coerce").fillna(0.0)
    else:
        raise SystemExit("expansion CSV has no net_pnl_rs/pnl_pct_price/pnl_rs")
    out["pnl_rs_v3"] = pnl
    out["v3_source"] = "v3_expansion"
    out["capital_per_trade_rs"] = CAPITAL_PER_TRADE
    out["leverage"] = LEVERAGE
    out["notional_exposure_rs"] = EFFECTIVE_NOTIONAL
    return out


def _candidate_rules(g: pd.DataFrame) -> Iterable[tuple[str, list[tuple[str, str, float]], pd.DataFrame]]:
    yield "whole_setup", [], g
    for feat in MINE_FEATURES:
        if feat not in g.columns:
            continue
        vals = pd.to_numeric(g[feat], errors="coerce").dropna()
        if len(vals) < 6:
            continue
        thresholds = sorted(set(vals.quantile(QUANTILES).round(5)))
        for threshold in thresholds:
            series = pd.to_numeric(g[feat], errors="coerce")
            yield f"{feat}<={threshold}", [(feat, "<=", float(threshold))], g[series <= threshold]
            yield f"{feat}>={threshold}", [(feat, ">=", float(threshold))], g[series >= threshold]


def _rule_text(conditions: list[tuple[str, str, float]]) -> str:
    if not conditions:
        return "whole_setup"
    return " AND ".join(f"{c} {op} {v:g}" for c, op, v in conditions)


def _mine_additions(
    baseline: pd.DataFrame,
    expansion_pool: pd.DataFrame,
    min_pocket_pf: float,
    min_total_pf: float,
    min_trades: int,
    max_steps: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    pool = expansion_pool[~expansion_pool["setup"].astype(str).isin(V2_FIXED_SETUPS)].copy()
    pool = pool.reset_index(drop=True)
    used = pd.Series(False, index=pool.index)
    combined = baseline.copy()
    chosen_rows = []

    for step in range(max_steps):
        best = None
        for setup, g0 in pool.loc[~used].groupby("setup", sort=True):
            for _, conditions, g in _candidate_rules(g0):
                if len(g) < min_trades:
                    continue
                pocket_pf = _pf(g["pnl_rs_v3"])
                if pocket_pf < min_pocket_pf:
                    continue
                trial_pnl = pd.concat([combined["pnl_rs_v3"], g["pnl_rs_v3"]], ignore_index=True)
                total_pf = _pf(trial_pnl)
                if total_pf < min_total_pf:
                    continue
                score = (len(g), float(g["pnl_rs_v3"].sum()), pocket_pf, total_pf)
                if best is None or score > best[0]:
                    best = (score, setup, conditions, g)

        if best is None:
            break

        score, setup, conditions, g = best
        rule = _rule_text(conditions)
        add = g.copy()
        add["v3_rule"] = rule
        combined = pd.concat([combined, add], ignore_index=True, sort=False)
        used.loc[g.index] = True
        chosen_rows.append({
            "step": step + 1,
            "side": str(g["side"].mode().iloc[0]) if "side" in g.columns and not g.empty else "",
            "setup": setup,
            "rule": rule,
            "trades": int(len(g)),
            "pocket_pf": float(score[2]),
            "pocket_pnl_rs": float(g["pnl_rs_v3"].sum()),
            "combined_pf_after": float(score[3]),
            "combined_trades_after": int(len(combined)),
            "combined_pnl_rs_after": float(combined["pnl_rs_v3"].sum()),
        })

    return combined, pd.DataFrame(chosen_rows)


def _metrics(trades: pd.DataFrame) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    pnl = pd.to_numeric(trades["pnl_rs_v3"], errors="coerce").fillna(0.0)
    work = trades.assign(pnl_rs_v3=pnl)
    daily = work.groupby("trade_date", sort=True)["pnl_rs_v3"].sum().reset_index()
    daily["cum_pnl_rs"] = daily["pnl_rs_v3"].cumsum()
    daily["drawdown_rs"] = daily["cum_pnl_rs"] - daily["cum_pnl_rs"].cummax()
    by_setup = (
        work.groupby(["side", "setup"], sort=True)
        .agg(
            trades=("setup", "count"),
            win_rate_pct=("pnl_rs_v3", lambda s: float((pd.to_numeric(s, errors="coerce") > 0).mean() * 100.0)),
            pnl_rs=("pnl_rs_v3", "sum"),
        )
        .reset_index()
    )
    side = work["side"].astype(str).str.upper()
    return {
        "trades": int(len(work)),
        "trading_days": int(daily["trade_date"].nunique()),
        "avg_trades_per_day": float(len(work) / max(daily["trade_date"].nunique(), 1)),
        "win_rate_pct": float((pnl > 0).mean() * 100.0),
        "profit_factor": float(_pf(pnl)),
        "net_pnl_rs": float(pnl.sum()),
        "day_win_rate_pct": float((daily["pnl_rs_v3"] > 0).mean() * 100.0),
        "max_drawdown_rs": float(daily["drawdown_rs"].min()),
        "long_trades": int((side == "LONG").sum()),
        "short_trades": int((side == "SHORT").sum()),
        "long_pnl_rs": float(work.loc[side == "LONG", "pnl_rs_v3"].sum()),
        "short_pnl_rs": float(work.loc[side == "SHORT", "pnl_rs_v3"].sum()),
    }, daily, by_setup


def _write_summary(out_dir: Path, baseline_summary: dict, summary: dict, by_setup: pd.DataFrame, chosen: pd.DataFrame) -> None:
    lines = [
        "=" * 86,
        "AVWAP ID 5-min v3 expansion backtest",
        "v2 fixed baseline + mined pockets from non-v2 setups",
        "=" * 86,
        "V2 baseline:",
        f"  Trades              : {baseline_summary['trades']:,}",
        f"  Profit factor       : {baseline_summary['profit_factor']:.3f}",
        f"  Net PnL             : Rs {baseline_summary['net_pnl_rs']:,.2f}",
        f"  LONG trades/PnL     : {baseline_summary['long_trades']:,} / Rs {baseline_summary['long_pnl_rs']:,.2f}",
        f"  SHORT trades/PnL    : {baseline_summary['short_trades']:,} / Rs {baseline_summary['short_pnl_rs']:,.2f}",
        "",
        "V3 expanded:",
        f"  Trades              : {summary['trades']:,}",
        f"  Trading days        : {summary['trading_days']:,}",
        f"  Avg trades/day      : {summary['avg_trades_per_day']:.2f}",
        f"  Win rate            : {summary['win_rate_pct']:.2f}%",
        f"  Profit factor       : {summary['profit_factor']:.3f}",
        f"  Net PnL             : Rs {summary['net_pnl_rs']:,.2f}",
        f"  Day win rate        : {summary['day_win_rate_pct']:.2f}%",
        f"  Max drawdown        : Rs {summary['max_drawdown_rs']:,.2f}",
        f"  LONG trades/PnL     : {summary['long_trades']:,} / Rs {summary['long_pnl_rs']:,.2f}",
        f"  SHORT trades/PnL    : {summary['short_trades']:,} / Rs {summary['short_pnl_rs']:,.2f}",
        "",
        "Delta vs v2:",
        f"  Trades              : {summary['trades'] - baseline_summary['trades']:+,}",
        f"  PF                  : {summary['profit_factor'] - baseline_summary['profit_factor']:+.3f}",
        f"  Net PnL             : Rs {summary['net_pnl_rs'] - baseline_summary['net_pnl_rs']:+,.2f}",
        "",
        "Accepted non-v2 pockets:",
    ]
    if chosen.empty:
        lines.append("  none")
    else:
        for _, r in chosen.iterrows():
            lines.append(
                f"  {int(r['step']):>2d}. {r['setup']:<34s} "
                f"n={int(r['trades']):>4d} pf={float(r['pocket_pf']):>5.2f} "
                f"pnl=Rs {float(r['pocket_pnl_rs']):>9,.2f} rule={r['rule']}"
            )
    lines.extend(["", "By setup:",])
    for _, r in by_setup.sort_values("pnl_rs", ascending=False).iterrows():
        lines.append(
            f"  {r['side']:<5s} {r['setup']:<34s} "
            f"n={int(r['trades']):>5d} win={float(r['win_rate_pct']):>6.2f}% "
            f"pnl=Rs {float(r['pnl_rs']):>11,.2f}"
        )
    lines.append("=" * 86)
    text = "\n".join(lines)
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="AVWAP ID 5-min v3 expansion miner")
    ap.add_argument("--baseline_csv", type=str, default="", help="defaults to latest v2 cascade CSV")
    ap.add_argument("--expansion_csv", type=str, default=str(DEFAULT_EXPANSION_CSV))
    ap.add_argument("--out", type=str, default=str(OUT_ROOT))
    ap.add_argument("--min_pocket_pf", type=float, default=1.30)
    ap.add_argument("--min_total_pf", type=float, default=1.62)
    ap.add_argument("--min_trades", type=int, default=3)
    ap.add_argument("--max_steps", type=int, default=30)
    args = ap.parse_args()

    baseline_csv = Path(args.baseline_csv) if args.baseline_csv else _latest_baseline_csv()
    expansion_csv = Path(args.expansion_csv)
    if not expansion_csv.exists():
        raise SystemExit(f"missing expansion CSV: {expansion_csv}")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    baseline = _normalize_baseline(pd.read_csv(baseline_csv, low_memory=False), "fixed_v2_baseline")
    expansion = _normalize_expansion(pd.read_csv(expansion_csv, low_memory=False))
    baseline_summary, baseline_daily, baseline_by_setup = _metrics(baseline)

    combined, chosen = _mine_additions(
        baseline=baseline,
        expansion_pool=expansion,
        min_pocket_pf=float(args.min_pocket_pf),
        min_total_pf=float(args.min_total_pf),
        min_trades=max(1, int(args.min_trades)),
        max_steps=max(1, int(args.max_steps)),
    )
    summary, daily, by_setup = _metrics(combined)

    combined.to_csv(out_dir / "trades.csv", index=False)
    daily.to_csv(out_dir / "daily.csv", index=False)
    by_setup.to_csv(out_dir / "by_setup.csv", index=False)
    chosen.to_csv(out_dir / "accepted_pockets.csv", index=False)
    baseline_daily.to_csv(out_dir / "baseline_daily.csv", index=False)
    baseline_by_setup.to_csv(out_dir / "baseline_by_setup.csv", index=False)
    _write_summary(out_dir, baseline_summary, summary, by_setup, chosen)
    print(f"\nwrote {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
