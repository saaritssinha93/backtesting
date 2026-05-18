"""
Quick 1-minute exit sweep for AVWAP ID v5.

Reads v5 trades, re-walks actual 1-minute OHLC bars from each entry time, and
tests target/SL combinations per setup. Stop-loss values below 0.70% are
rejected. Uses the existing v17D_exit_resolver.resolve path for exit ordering:
if SL and target are touched in the same 1-minute bar, SL wins.

Default input:
  C:\\TradingData\\eqidv2\\outputs_ID_v5_5min\\trades.csv

Default output:
  C:\\TradingData\\eqidv2\\outputs_ID_v5_5min\\exit_sweep_1min
"""

from __future__ import annotations

import argparse
import math
import time
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

import v17D_exit_resolver as er


TRADES_CSV = Path(r"C:\TradingData\eqidv2\outputs_ID_v5_5min\trades.csv")
DATA_1M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
OUT_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v5_5min\exit_sweep_1min")

CAPITAL_PER_TRADE = 10_000.0
LEVERAGE = 5.0
EFFECTIVE_NOTIONAL = CAPITAL_PER_TRADE * LEVERAGE
DEFAULT_COST_BPS = 16.0
STOP_EXTRA_BPS = 3.0
MIN_SL_PCT = 0.70

DEFAULT_TARGETS = [0.60, 0.70, 0.75, 0.80, 0.90, 1.00, 1.10, 1.20, 1.30, 1.50]
DEFAULT_STOPS = [0.70, 0.75, 0.80, 0.85, 0.90, 1.00, 1.10, 1.20]


def _pf(pnl: pd.Series) -> float:
    s = pd.to_numeric(pnl, errors="coerce").fillna(0.0)
    gains = float(s[s > 0].sum())
    losses = float(-s[s <= 0].sum())
    if losses <= 0:
        return math.inf if gains > 0 else 0.0
    return gains / losses


def _parse_grid(raw: str, default: list[float]) -> list[float]:
    if not raw:
        return default
    vals = [float(x.strip()) for x in raw.split(",") if x.strip()]
    return sorted(set(vals))


def _normalise_ts(value) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tz is None:
        return ts.tz_localize("Asia/Kolkata")
    return ts.tz_convert("Asia/Kolkata")


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


def _normalise_trades(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["entry_time_v5"] = pd.NaT
    if "entry_time_ist" in out.columns:
        out["entry_time_v5"] = pd.to_datetime(out["entry_time_ist"], errors="coerce")
    if "entry_ts" in out.columns:
        entry_ts = pd.to_datetime(out["entry_ts"], errors="coerce")
        out["entry_time_v5"] = out["entry_time_v5"].fillna(entry_ts)

    if "entry_price" in out.columns:
        out["entry_price_v5"] = pd.to_numeric(out["entry_price"], errors="coerce")
    else:
        out["entry_price_v5"] = np.nan
    if "entry_px" in out.columns:
        out["entry_price_v5"] = out["entry_price_v5"].fillna(pd.to_numeric(out["entry_px"], errors="coerce"))

    if "trade_date" not in out.columns:
        out["trade_date"] = pd.to_datetime(out["entry_time_v5"], errors="coerce").dt.date.astype(str)

    out = out.dropna(subset=["ticker", "side", "setup", "entry_time_v5", "entry_price_v5"]).copy()
    out["ticker"] = out["ticker"].astype(str).str.upper()
    out["side"] = out["side"].astype(str).str.upper()
    out["setup"] = out["setup"].astype(str)
    out["entry_time_v5"] = out["entry_time_v5"].map(_normalise_ts)
    out = out.dropna(subset=["entry_time_v5"])
    return out.reset_index(drop=True)


def _net_pnl_rs(price_pnl_pct: float, outcome: str, cost_bps: float) -> float:
    extra = STOP_EXTRA_BPS if str(outcome).upper() == "SL" else 0.0
    gross = price_pnl_pct / 100.0 * EFFECTIVE_NOTIONAL
    cost = EFFECTIVE_NOTIONAL * ((cost_bps + extra) / 10_000.0)
    return gross - cost


def _resolve_one(row: pd.Series, sl_pct: float, tgt_pct: float, cost_bps: float) -> dict | None:
    bars = _load_1m(str(row["ticker"]))
    if bars is None or bars.empty:
        return None
    res = er.resolve(
        bars=bars,
        side=str(row["side"]),
        entry_price=float(row["entry_price_v5"]),
        entry_time_ist=row["entry_time_v5"],
        sl_pct=float(sl_pct),
        tgt_pct=float(tgt_pct),
    )
    if res is None:
        return None
    pnl_rs = _net_pnl_rs(res.pnl_pct_price, res.outcome, cost_bps)
    return {
        "trade_date": str(row["trade_date"])[:10],
        "ticker": row["ticker"],
        "side": row["side"],
        "setup": row["setup"],
        "entry_time_v5": row["entry_time_v5"],
        "entry_price_v5": float(row["entry_price_v5"]),
        "sl_pct": float(sl_pct),
        "target_pct": float(tgt_pct),
        "outcome": res.outcome,
        "exit_price": float(res.exit_price),
        "exit_time_ist": res.exit_time_ist,
        "bars_held": int(res.bars_held),
        "pnl_pct_price": float(res.pnl_pct_price),
        "net_pnl_rs": float(pnl_rs),
    }


def _metrics(df: pd.DataFrame) -> dict:
    pnl = pd.to_numeric(df["net_pnl_rs"], errors="coerce").fillna(0.0)
    daily = df.assign(_pnl=pnl).groupby("trade_date", sort=True)["_pnl"].sum()
    cum = daily.cumsum()
    dd = cum - cum.cummax()
    return {
        "trades": int(len(df)),
        "win_rate_pct": float((pnl > 0).mean() * 100.0),
        "target_rate_pct": float((df["outcome"].astype(str) == "TARGET").mean() * 100.0),
        "sl_rate_pct": float((df["outcome"].astype(str) == "SL").mean() * 100.0),
        "eod_rate_pct": float((df["outcome"].astype(str) == "EOD").mean() * 100.0),
        "profit_factor": float(_pf(pnl)),
        "net_pnl_rs": float(pnl.sum()),
        "avg_pnl_rs": float(pnl.mean()) if len(pnl) else 0.0,
        "day_win_rate_pct": float((daily > 0).mean() * 100.0) if len(daily) else 0.0,
        "max_drawdown_rs": float(dd.min()) if len(dd) else 0.0,
    }


def _score_row(row: pd.Series) -> tuple:
    pf = float(row["profit_factor"])
    pf_score = min(pf, 10.0) if math.isfinite(pf) else 10.0
    return (
        pf_score,
        float(row["net_pnl_rs"]),
        float(row["win_rate_pct"]),
        -float(row["max_drawdown_rs"]),
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="V5 1-minute per-setup SL/target exit sweep")
    ap.add_argument("--trades", type=str, default=str(TRADES_CSV))
    ap.add_argument("--out", type=str, default=str(OUT_DIR))
    ap.add_argument("--targets", type=str, default="")
    ap.add_argument("--stops", type=str, default="")
    ap.add_argument("--cost_bps", type=float, default=DEFAULT_COST_BPS)
    ap.add_argument("--min_trades", type=int, default=1)
    ap.add_argument("--write_best_trades", action="store_true")
    args = ap.parse_args()

    targets = _parse_grid(args.targets, DEFAULT_TARGETS)
    stops = _parse_grid(args.stops, DEFAULT_STOPS)
    bad_stops = [s for s in stops if s < MIN_SL_PCT]
    if bad_stops:
        raise SystemExit(f"SL cannot be lower than {MIN_SL_PCT:.2f}%; bad values: {bad_stops}")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    trades = _normalise_trades(pd.read_csv(args.trades, low_memory=False))
    print(f"[v5-exit-sweep] trades={len(trades)} setups={trades['setup'].nunique()} targets={targets} stops={stops}")

    rows = []
    best_trade_frames = []
    t0 = time.time()
    grouped = list(trades.groupby(["side", "setup"], sort=True))
    for gi, ((side, setup), g) in enumerate(grouped, 1):
        if len(g) < int(args.min_trades):
            continue
        setup_results = []
        for sl_pct in stops:
            for tgt_pct in targets:
                resolved = []
                misses = 0
                for _, row in g.iterrows():
                    rec = _resolve_one(row, sl_pct=sl_pct, tgt_pct=tgt_pct, cost_bps=float(args.cost_bps))
                    if rec is None:
                        misses += 1
                    else:
                        resolved.append(rec)
                if not resolved:
                    continue
                rdf = pd.DataFrame(resolved)
                m = _metrics(rdf)
                m.update({
                    "side": side,
                    "setup": setup,
                    "sl_pct": float(sl_pct),
                    "target_pct": float(tgt_pct),
                    "misses": int(misses),
                })
                setup_results.append((m, rdf))
                rows.append(m)

        if setup_results:
            best_m, best_df = max(setup_results, key=lambda item: _score_row(pd.Series(item[0])))
            best_trade_frames.append(best_df.assign(best_for_setup=True))
            print(
                f"  [{gi:2d}/{len(grouped)}] {side:<5s} {setup:<34s} "
                f"n={best_m['trades']:>4d} best SL={best_m['sl_pct']:.2f}% TGT={best_m['target_pct']:.2f}% "
                f"PF={best_m['profit_factor']:.3f} pnl=Rs {best_m['net_pnl_rs']:,.2f}"
            )

    results = pd.DataFrame(rows)
    if results.empty:
        raise SystemExit("no sweep results produced")

    results = results.sort_values(["side", "setup", "profit_factor", "net_pnl_rs"], ascending=[True, True, False, False])
    results.to_csv(out_dir / "all_setup_exit_sweep.csv", index=False)

    best_rows = []
    for (side, setup), g in results.groupby(["side", "setup"], sort=True):
        best_rows.append(max([r for _, r in g.iterrows()], key=_score_row))
    best = pd.DataFrame(best_rows).sort_values(["profit_factor", "net_pnl_rs"], ascending=[False, False])
    best.to_csv(out_dir / "best_exit_combo_by_setup.csv", index=False)

    if args.write_best_trades and best_trade_frames:
        pd.concat(best_trade_frames, ignore_index=True).to_csv(out_dir / "best_combo_trades.csv", index=False)

    lines = [
        "=" * 100,
        "V5 1-minute exit sweep: best SL/target by setup",
        f"Input trades: {args.trades}",
        f"SL grid: {stops}  Target grid: {targets}  Cost: {args.cost_bps:.1f} bps + {STOP_EXTRA_BPS:.1f} bps on SL",
        "=" * 100,
    ]
    for _, r in best.sort_values("net_pnl_rs", ascending=False).iterrows():
        lines.append(
            f"{r['side']:<5s} {r['setup']:<36s} "
            f"n={int(r['trades']):>5d} SL={float(r['sl_pct']):>4.2f}% TGT={float(r['target_pct']):>4.2f}% "
            f"PF={float(r['profit_factor']):>6.3f} win={float(r['win_rate_pct']):>6.2f}% "
            f"pnl=Rs {float(r['net_pnl_rs']):>11,.2f} "
            f"T/SL/EOD={float(r['target_rate_pct']):.1f}/{float(r['sl_rate_pct']):.1f}/{float(r['eod_rate_pct']):.1f}%"
        )
    lines.append("=" * 100)
    text = "\n".join(lines)
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")
    print(f"[v5-exit-sweep] wrote {out_dir} in {time.time()-t0:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
