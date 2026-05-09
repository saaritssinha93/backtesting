"""v17D Step 2.10 — wide-net library backtest harness.

End-to-end driver for the 20-detector library:

    universe_filter -> for each ticker:
        load 5-min indicator parquet
        load 1-min OHLC parquet (for exit resolution)
        for each detector:
            -> list[Signal]
        for each Signal:
            -> resolve exit -> row
    apply governors (Cand-E v17C-style)
    apply sizing
    write trades CSV (per-Tier filter)
    print per-setup PF/win/n table

Usage:
    python -m eqidv2.v17D_library_backtest \
        --universe-csv eqidv2/configs/universe.csv \
        --indicator-dir C:/TradingData/eqidv2/stocks_indicators_5min_eq_live2 \
        --onemin-dir   C:/TradingData/eqidv2/stocks_indicators_1min_eq \
        --output-dir   outputs_v17D_library \
        --tier A          # A or B; A is fast (lab cost), B is realistic
        --setups TC_1_PULLBACK_EMA20,MR_2_VWAP_FADE   # optional filter

Universe CSV columns required: ticker, last_close, adv_rs_cr (optional sector).
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from eqidv2 import v17D_cost_model as cm
from eqidv2 import v17D_universe_filter as uf
from eqidv2 import v17D_exit_resolver as er
from eqidv2.v17D_signal_to_row import signal_to_row, rows_to_dataframe
from eqidv2.v17D_setup_library import scan_all, ALL_DETECTORS, DETECTORS_BY_FAMILY


def _load_5min_indicators(indicator_dir: Path, ticker: str) -> pd.DataFrame | None:
    p = indicator_dir / f"{ticker}_stocks_indicators_5min.parquet"
    if not p.exists():
        return None
    df = pd.read_parquet(p)
    # Expect a 'date' column already in IST or UTC; normalize to IST tz-aware.
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        if df["date"].dt.tz is None:
            df["date"] = df["date"].dt.tz_localize("Asia/Kolkata")
        else:
            df["date"] = df["date"].dt.tz_convert("Asia/Kolkata")
        df = df.set_index("date").sort_index()
    return df


def _cost_pct(adv_rs_cr: float | None, exit_kind: str, tier: str) -> float:
    """Tier A = lab (BASELINE 0.16/0.19); Tier B = realistic v17D cost model."""
    if tier.upper() == "A":
        return 0.16 if exit_kind == "TARGET" else 0.19
    bucket = cm.adv_bucket_for(adv_rs_cr or 0.0)
    return cm.costs_pct_for_v17C(bucket, exit_kind)


def backtest_one_ticker(
    ticker: str, indicator_dir: Path, onemin_dir: Path,
    *, tier: str, adv_rs_cr: float | None,
    sector: str | None, setups: list | None = None,
    leverage: float = 5.0, position_size_rs: float = 20000.0,
) -> list:
    """Scan all detectors on this ticker, resolve exits, return list[row]."""
    ind_df = _load_5min_indicators(indicator_dir, ticker)
    if ind_df is None:
        return []
    bars = er.load_1min(onemin_dir, ticker)
    if bars is None:
        return []

    signals = scan_all(ind_df, ticker, setups=setups)

    rows = []
    for sig in signals:
        # Entry bar = signal bar + 5 min
        entry_ts = pd.Timestamp(sig.signal_time_ist)
        if entry_ts.tz is None:
            entry_ts = entry_ts.tz_localize("Asia/Kolkata")
        entry_ts = entry_ts + pd.Timedelta(minutes=5)
        res = er.resolve(
            bars, sig.side, float(sig.entry_price), entry_ts,
            sig.suggested_sl_pct, sig.suggested_tgt_pct,
        )
        if res is None:
            continue   # outside parquet date range
        cost = _cost_pct(adv_rs_cr, res.outcome, tier)
        row = signal_to_row(
            sig, res,
            leverage=leverage, position_size_rs=position_size_rs,
            cost_pct=cost, sector=sector,
        )
        rows.append(row)
    return rows


def _pf(s: pd.Series) -> float:
    s = pd.to_numeric(s, errors="coerce").dropna()
    w = float(s[s > 0].sum()); l = float(-s[s < 0].sum())
    if l <= 0:
        return float("inf") if w > 0 else 0.0
    return w / l


def per_setup_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if df.empty:
        return pd.DataFrame(rows)
    df = df.copy()
    df["d"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    n_days = max(df["d"].nunique(), 1)
    for (side, setup), g in df.groupby(["side", "setup"]):
        n = len(g)
        rows.append({
            "side": side, "setup": setup,
            "family": next(
                (fam for fam, ids in DETECTORS_BY_FAMILY.items() if setup in ids),
                "?"
            ),
            "n": n,
            "per_day": round(n / n_days, 3),
            "pf": round(_pf(g["pnl_pct"]), 3),
            "win_pct": round((g["outcome"] == "TARGET").mean() * 100, 2),
            "sum_pnl_pct": round(float(g["pnl_pct"].sum()), 2),
            "sum_pnl_rs": round(float(g["pnl_rs"].sum()), 2),
        })
    return pd.DataFrame(rows).sort_values(
        ["family", "side", "setup"]
    ).reset_index(drop=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--universe-csv", required=True,
                   help="CSV with columns: ticker, last_close, adv_rs_cr [, sector]")
    p.add_argument("--indicator-dir", required=True)
    p.add_argument("--onemin-dir", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--tier", choices=["A", "B"], default="A",
                   help="A=lab cost (0.16/0.19); B=realistic v17D cost model")
    p.add_argument("--setups", default="", help="optional comma-list filter")
    p.add_argument("--leverage", type=float, default=5.0)
    p.add_argument("--position-size-rs", type=float, default=20000.0)
    args = p.parse_args()

    universe = pd.read_csv(args.universe_csv)
    indicator_dir = Path(args.indicator_dir)
    onemin_dir = Path(args.onemin_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    setups = [s.strip() for s in args.setups.split(",") if s.strip()] or None
    fo_set = uf.load_fo_list()

    print(f"v17D library backtest tier {args.tier}")
    print(f"  universe: {len(universe)} tickers")
    print(f"  setups: {setups if setups else 'ALL 20'}")
    print(f"  cost tier: {args.tier} ({'lab' if args.tier == 'A' else 'realistic v17D'})")

    all_rows = []
    n_skipped_universe = 0
    n_skipped_data = 0
    for _, row in universe.iterrows():
        ticker = row["ticker"]
        elig = uf.is_eligible(
            ticker=ticker, last_close=row.get("last_close"),
            adv_rs_cr=row.get("adv_rs_cr"), fo_set=fo_set if fo_set else None,
        )
        if not elig.eligible:
            n_skipped_universe += 1
            continue
        ticker_rows = backtest_one_ticker(
            ticker, indicator_dir, onemin_dir,
            tier=args.tier, adv_rs_cr=row.get("adv_rs_cr"),
            sector=row.get("sector"), setups=setups,
            leverage=args.leverage, position_size_rs=args.position_size_rs,
        )
        if not ticker_rows:
            n_skipped_data += 1
            continue
        all_rows.extend(ticker_rows)

    df = rows_to_dataframe(all_rows)
    ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    trades_path = output_dir / f"v17D_library_trades_tier{args.tier}_{ts_str}.csv"
    df.to_csv(trades_path, index=False)
    print(f"\nWrote trades CSV: {trades_path}  rows={len(df)}")
    print(f"  skipped (universe): {n_skipped_universe}, "
          f"skipped (no data): {n_skipped_data}")

    summary = per_setup_summary(df)
    summary_path = output_dir / f"v17D_library_summary_tier{args.tier}_{ts_str}.csv"
    summary.to_csv(summary_path, index=False)
    print(f"Wrote per-setup summary: {summary_path}")

    if not summary.empty:
        print("\nPer-setup:")
        print(summary.to_string(index=False))
        n_pass_pf = int((summary["pf"] >= 1.50).sum())
        n_pass_n = int((summary["n"] >= 30).sum())
        print(f"\nTier-{args.tier} drop-fast verdict:")
        print(f"  PF >= 1.50: {n_pass_pf}/{len(summary)}")
        print(f"  n  >= 30  : {n_pass_n}/{len(summary)}")


if __name__ == "__main__":
    main()
