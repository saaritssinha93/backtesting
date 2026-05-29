"""
AVWAP ID 5-min v6 backtester.

v6 references the v5 trade set, then applies setup-specific exits found by
`v5_exit_resolver.py`. Every trade is re-resolved on 1-minute OHLC bars via
`v17D_exit_resolver.resolve`.

Default input:
  C:\\TradingData\\eqidv2\\outputs_ID_v5_5min\\trades.csv

Default output:
  C:\\TradingData\\eqidv2\\outputs_ID_v6_5min
"""

from __future__ import annotations

import argparse
import math
import subprocess
import sys
import time
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

import v17D_exit_resolver as er


V5_TRADES_CSV = Path(r"C:\TradingData\eqidv2\outputs_ID_v5_5min\trades.csv")
OUT_ROOT = Path(r"C:\TradingData\eqidv2\outputs_ID_v6_5min")
DATA_1M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")

CAPITAL_PER_TRADE = 10_000.0
LEVERAGE = 5.0
EFFECTIVE_NOTIONAL = CAPITAL_PER_TRADE * LEVERAGE
DEFAULT_COST_BPS = 16.0
STOP_EXTRA_BPS = 3.0
MIN_SL_PCT = 0.70


# Setup-specific exits from the latest v5 1-minute exit sweep.
# Values are percentages, not decimals.
SETUP_EXIT_RULES: dict[str, tuple[float, float]] = {
    "A_MOD_BREAK_C1_HIGH": (0.70, 1.00),
    "A_MOD_BREAK_C1_LOW": (0.70, 1.50),
    "A_MOD_CLOSE_CONTINUATION_BREAK": (0.70, 1.50),
    "A_PULLBACK_C2_THEN_BREAK_C2_HIGH": (0.70, 0.90),
    "A_PULLBACK_C2_THEN_BREAK_C2_LOW": (0.85, 1.00),
    "B_AVWAP_RECLAIM_REVERSAL": (0.70, 1.50),
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK": (0.70, 1.50),
    "B_HUGE_PULLBACK_HOLD_BREAK": (0.70, 1.10),
    "B_HUGE_RED_FAILED_BOUNCE": (0.70, 1.50),
    "C_OR_BREAKDOWN": (0.70, 1.30),
    "C_OR_BREAKOUT": (1.20, 1.50),
    "D_AVWAP_LOSE_REVERSAL": (1.00, 1.50),
    "D_EMA20_BOUNCE": (0.70, 1.50),
    "D_EMA20_REJECTION": (0.75, 1.30),
    "E_FAILED_OR_BREAKDOWN_TRAP_LONG": (0.75, 1.00),
    "E_FAILED_OR_BREAKOUT_TRAP_SHORT": (0.75, 1.00),
    "E_GAP_HOLD_CONTINUATION_LONG": (0.80, 1.20),
    "E_GAP_HOLD_CONTINUATION_SHORT": (0.80, 1.20),
    "E_OPENING_DRIVE_CONTINUATION_LONG": (0.75, 1.00),
    "E_OPENING_DRIVE_CONTINUATION_SHORT": (0.75, 1.00),
    "E_ORB_BREAKOUT_LONG": (0.80, 1.20),
    "E_ORB_BREAKOUT_SHORT": (0.80, 1.20),
    "E_ORB_RETEST_HOLD_LONG": (0.70, 1.00),
    "E_ORB_RETEST_HOLD_SHORT": (0.70, 1.00),
    "E_RS_FIRST_HOUR_BREAK_LONG": (0.80, 1.20),
    "E_RS_FIRST_HOUR_BREAK_SHORT": (0.80, 1.20),
    "E_VWAP_BAND_FADE": (0.70, 0.60),
    "E_VWAP_LOSE_EARLY_SHORT": (0.70, 1.00),
    "E_VWAP_RECLAIM_EARLY_LONG": (0.70, 1.00),
    "G_HIGHER_HIGH_BREAK": (0.90, 1.50),
    "G_LOWER_LOW_BREAK": (0.85, 0.90),
    "L_BB_SQUEEZE_LONG": (0.75, 0.75),
    "L_DOUBLE_BOTTOM_VWAP": (0.70, 0.80),
    "L_PRESSURE_BURST_VWAP": (1.10, 0.90),
    "L_TREND_PULLBACK": (0.70, 0.90),
    "S_BB_SQUEEZE_SHORT": (1.00, 1.50),
    "S_LIQUIDITY_SWEEP_REVERSAL": (0.70, 1.50),
    "S_MACD_HIST_FLIP": (0.70, 1.50),
}


def _pf(pnl: pd.Series) -> float:
    s = pd.to_numeric(pnl, errors="coerce").fillna(0.0)
    gains = float(s[s > 0].sum())
    losses = float(-s[s <= 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else 0.0
    return gains / losses


def _normalise_ts(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tz is None:
        return ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("Asia/Kolkata")


def _normalise_trades(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["entry_time_v6"] = pd.NaT
    if "entry_time_ist" in out.columns:
        out["entry_time_v6"] = pd.to_datetime(out["entry_time_ist"], errors="coerce")
    if "entry_ts" in out.columns:
        out["entry_time_v6"] = out["entry_time_v6"].fillna(pd.to_datetime(out["entry_ts"], errors="coerce"))

    if "entry_price" in out.columns:
        out["entry_price_v6"] = pd.to_numeric(out["entry_price"], errors="coerce")
    else:
        out["entry_price_v6"] = np.nan
    if "entry_px" in out.columns:
        out["entry_price_v6"] = out["entry_price_v6"].fillna(pd.to_numeric(out["entry_px"], errors="coerce"))

    if "trade_date" not in out.columns:
        out["trade_date"] = pd.to_datetime(out["entry_time_v6"], errors="coerce").dt.date.astype(str)

    out = out.dropna(subset=["ticker", "side", "setup", "entry_time_v6", "entry_price_v6"]).copy()
    out["ticker"] = out["ticker"].astype(str).str.upper()
    out["side"] = out["side"].astype(str).str.upper()
    out["setup"] = out["setup"].astype(str)
    out["trade_date"] = out["trade_date"].astype(str).str[:10]
    out["entry_time_v6"] = out["entry_time_v6"].map(_normalise_ts)
    return out.dropna(subset=["entry_time_v6"]).reset_index(drop=True)


@lru_cache(maxsize=None)
def _load_1m(ticker: str) -> pd.DataFrame | None:
    path = DATA_1M_DIR / f"{str(ticker).upper()}_stocks_indicators_1min.parquet"
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["date", "high", "low", "close"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if getattr(df["date"].dt, "tz", None) is None:
        df["date"] = df["date"].dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
    else:
        df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
    df = df.dropna(subset=["date"]).sort_values("date")
    return df.set_index("date")


def _net_pnl_rs(price_pnl_pct: float, outcome: str, cost_bps: float) -> tuple[float, float, float]:
    extra_bps = STOP_EXTRA_BPS if str(outcome).upper() == "SL" else 0.0
    gross = price_pnl_pct / 100.0 * EFFECTIVE_NOTIONAL
    cost = EFFECTIVE_NOTIONAL * ((cost_bps + extra_bps) / 10_000.0)
    return gross - cost, gross, cost


def _resolve_trade(row: pd.Series, cost_bps: float) -> dict | None:
    setup = str(row["setup"])
    if setup not in SETUP_EXIT_RULES:
        raise ValueError(f"missing v6 exit rule for setup: {setup}")
    sl_pct, target_pct = SETUP_EXIT_RULES[setup]
    if sl_pct < MIN_SL_PCT:
        raise ValueError(f"SL for {setup} is below {MIN_SL_PCT:.2f}%: {sl_pct}")

    bars = _load_1m(str(row["ticker"]))
    if bars is None or bars.empty:
        return None
    res = er.resolve(
        bars=bars,
        side=str(row["side"]),
        entry_price=float(row["entry_price_v6"]),
        entry_time_ist=row["entry_time_v6"],
        sl_pct=sl_pct,
        tgt_pct=target_pct,
    )
    if res is None:
        return None

    net, gross, cost = _net_pnl_rs(res.pnl_pct_price, res.outcome, cost_bps)
    rec = row.to_dict()
    rec.update({
        "v6_sl_pct": sl_pct,
        "v6_target_pct": target_pct,
        "v6_outcome": res.outcome,
        "v6_exit_price": float(res.exit_price),
        "v6_exit_time_ist": res.exit_time_ist,
        "v6_bars_held": int(res.bars_held),
        "v6_pnl_pct_price": float(res.pnl_pct_price),
        "v6_gross_pnl_rs": float(gross),
        "v6_cost_rs": float(cost),
        "v6_net_pnl_rs": float(net),
        "capital_per_trade_rs": CAPITAL_PER_TRADE,
        "leverage": LEVERAGE,
        "notional_exposure_rs": EFFECTIVE_NOTIONAL,
    })
    return rec


def _metrics(trades: pd.DataFrame) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    pnl = pd.to_numeric(trades["v6_net_pnl_rs"], errors="coerce").fillna(0.0)
    daily = trades.assign(_pnl=pnl).groupby("trade_date", sort=True)["_pnl"].sum().reset_index()
    daily["cum_pnl_rs"] = daily["_pnl"].cumsum()
    daily["drawdown_rs"] = daily["cum_pnl_rs"] - daily["cum_pnl_rs"].cummax()
    by_setup = (
        trades.groupby(["side", "setup"], sort=True)
        .agg(
            trades=("setup", "count"),
            win_rate_pct=("v6_net_pnl_rs", lambda s: float((pd.to_numeric(s, errors="coerce") > 0).mean() * 100.0)),
            target_rate_pct=("v6_outcome", lambda s: float((s.astype(str) == "TARGET").mean() * 100.0)),
            sl_rate_pct=("v6_outcome", lambda s: float((s.astype(str) == "SL").mean() * 100.0)),
            eod_rate_pct=("v6_outcome", lambda s: float((s.astype(str) == "EOD").mean() * 100.0)),
            pnl_rs=("v6_net_pnl_rs", "sum"),
            sl_pct=("v6_sl_pct", "first"),
            target_pct=("v6_target_pct", "first"),
        )
        .reset_index()
    )
    side = trades["side"].astype(str).str.upper()
    return {
        "trades": int(len(trades)),
        "trading_days": int(daily["trade_date"].nunique()),
        "avg_trades_per_day": float(len(trades) / max(daily["trade_date"].nunique(), 1)),
        "win_rate_pct": float((pnl > 0).mean() * 100.0),
        "target_rate_pct": float((trades["v6_outcome"].astype(str) == "TARGET").mean() * 100.0),
        "sl_rate_pct": float((trades["v6_outcome"].astype(str) == "SL").mean() * 100.0),
        "eod_rate_pct": float((trades["v6_outcome"].astype(str) == "EOD").mean() * 100.0),
        "profit_factor": float(_pf(pnl)),
        "net_pnl_rs": float(pnl.sum()),
        "day_win_rate_pct": float((daily["_pnl"] > 0).mean() * 100.0),
        "max_drawdown_rs": float(daily["drawdown_rs"].min()),
        "long_trades": int((side == "LONG").sum()),
        "short_trades": int((side == "SHORT").sum()),
        "long_pnl_rs": float(trades.loc[side == "LONG", "v6_net_pnl_rs"].sum()),
        "short_pnl_rs": float(trades.loc[side == "SHORT", "v6_net_pnl_rs"].sum()),
    }, daily, by_setup


def _summary_text(summary: dict, by_setup: pd.DataFrame, input_trades: Path) -> str:
    lines = [
        "=" * 100,
        "AVWAP ID 5-min v6 backtest",
        "v5 trades re-resolved on 1-minute bars with setup-specific SL/target exits",
        f"Input: {input_trades}",
        "=" * 100,
        f"Trades              : {summary['trades']:,}",
        f"Trading days        : {summary['trading_days']:,}",
        f"Avg trades/day      : {summary['avg_trades_per_day']:.2f}",
        f"Win rate            : {summary['win_rate_pct']:.2f}%",
        f"Target / SL / EOD   : {summary['target_rate_pct']:.2f}% / {summary['sl_rate_pct']:.2f}% / {summary['eod_rate_pct']:.2f}%",
        f"Profit factor       : {summary['profit_factor']:.3f}",
        f"Net PnL             : Rs {summary['net_pnl_rs']:,.2f}",
        f"Day win rate        : {summary['day_win_rate_pct']:.2f}%",
        f"Max drawdown        : Rs {summary['max_drawdown_rs']:,.2f}",
        f"LONG trades/PnL     : {summary['long_trades']:,} / Rs {summary['long_pnl_rs']:,.2f}",
        f"SHORT trades/PnL    : {summary['short_trades']:,} / Rs {summary['short_pnl_rs']:,.2f}",
        "",
        "By setup:",
    ]
    for _, r in by_setup.sort_values("pnl_rs", ascending=False).iterrows():
        lines.append(
            f"  {r['side']:<5s} {r['setup']:<36s} "
            f"n={int(r['trades']):>5d} SL={float(r['sl_pct']):>4.2f}% TGT={float(r['target_pct']):>4.2f}% "
            f"win={float(r['win_rate_pct']):>6.2f}% pnl=Rs {float(r['pnl_rs']):>11,.2f} "
            f"T/SL/EOD={float(r['target_rate_pct']):.1f}/{float(r['sl_rate_pct']):.1f}/{float(r['eod_rate_pct']):.1f}%"
        )
    lines.append("=" * 100)
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="AVWAP ID v6 setup-specific exit backtest")
    ap.add_argument("--v5_trades", type=str, default=str(V5_TRADES_CSV))
    ap.add_argument("--out", type=str, default=str(OUT_ROOT))
    ap.add_argument("--cost_bps", type=float, default=DEFAULT_COST_BPS)
    ap.add_argument("--refresh_v5", action="store_true", help="run avwap_5min_ID_v5_backtesting.py before resolving exits")
    args = ap.parse_args()

    if args.refresh_v5:
        subprocess.run([sys.executable, "avwap_5min_ID_v5_backtesting.py"], check=True)

    v5_path = Path(args.v5_trades)
    if not v5_path.exists():
        raise SystemExit(f"missing v5 trades CSV: {v5_path}")
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    trades = _normalise_trades(pd.read_csv(v5_path, low_memory=False))
    missing = sorted(set(trades["setup"].astype(str)) - set(SETUP_EXIT_RULES))
    if missing:
        raise SystemExit(f"missing setup exit rules: {missing}")

    print(f"[v6] resolving {len(trades)} v5 trades across {trades['setup'].nunique()} setups")
    t0 = time.time()
    rows = []
    misses = 0
    for i, (_, row) in enumerate(trades.iterrows(), 1):
        rec = _resolve_trade(row, cost_bps=float(args.cost_bps))
        if rec is None:
            misses += 1
        else:
            rows.append(rec)
        if i % 500 == 0 or i == len(trades):
            print(f"  [v6 {i:4d}/{len(trades)}] resolved={len(rows)} misses={misses} elapsed={time.time()-t0:.1f}s")

    if not rows:
        raise SystemExit("no v6 trades resolved")

    out = pd.DataFrame(rows)
    out.to_csv(out_dir / "trades.csv", index=False)
    summary, daily, by_setup = _metrics(out)
    daily.rename(columns={"_pnl": "net_pnl_rs"}).to_csv(out_dir / "daily.csv", index=False)
    by_setup.to_csv(out_dir / "by_setup.csv", index=False)
    pd.DataFrame(
        [{"setup": k, "sl_pct": v[0], "target_pct": v[1]} for k, v in sorted(SETUP_EXIT_RULES.items())]
    ).to_csv(out_dir / "setup_exit_rules.csv", index=False)

    text = _summary_text(summary, by_setup, v5_path)
    print(text)
    (out_dir / "summary.txt").write_text(text + f"\n\n1-min misses: {misses}\n", encoding="utf-8")
    print(f"[v6] wrote {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
