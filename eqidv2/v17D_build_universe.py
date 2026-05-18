"""Build a universe.csv for v17D_library_backtest.

Reads every daily parquet in the daily indicators dir, computes:
    last_close    = most recent close
    adv_rs_cr     = mean(volume * close) over last 20 sessions, in Rs crores
    sector        = blank (sector ETF map handled separately)

Writes CSV with columns: ticker, last_close, adv_rs_cr.
Tickers with insufficient data (< 20 rows) or missing columns are skipped.

Usage:
    python -m eqidv2.v17D_build_universe \
        --daily-dir C:/TradingData/eqidv2/stocks_indicators_daily_eq \
        --output    eqidv2/configs/universe.csv
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def build(daily_dir: Path, lookback: int = 20) -> pd.DataFrame:
    rows = []
    skipped = 0
    for p in sorted(daily_dir.glob("*_stocks_indicators_daily.parquet")):
        ticker = p.name.replace("_stocks_indicators_daily.parquet", "")
        try:
            df = pd.read_parquet(p, columns=["date", "close", "volume"])
        except Exception:
            skipped += 1
            continue
        if len(df) < lookback or df["close"].isna().all() or df["volume"].isna().all():
            skipped += 1
            continue
        df = df.sort_values("date").tail(lookback)
        last_close = float(df["close"].iloc[-1])
        adv_rs_cr = float((df["close"] * df["volume"]).mean() / 1e7)
        rows.append({
            "ticker": ticker,
            "last_close": round(last_close, 2),
            "adv_rs_cr": round(adv_rs_cr, 2),
        })
    print(f"Built universe: {len(rows)} tickers (skipped {skipped})")
    return pd.DataFrame(rows)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--daily-dir", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--lookback", type=int, default=20)
    args = p.parse_args()
    df = build(Path(args.daily_dir), lookback=args.lookback)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.output, index=False)
    print(f"Wrote {args.output}")
    print(f"  ADV summary: median={df['adv_rs_cr'].median():.1f} cr, "
          f"p25={df['adv_rs_cr'].quantile(0.25):.1f}, "
          f"p75={df['adv_rs_cr'].quantile(0.75):.1f}, "
          f"max={df['adv_rs_cr'].max():.1f}")
    print(f"  Eligible (>= 50 cr ADV, Rs 50-5000): "
          f"{((df['adv_rs_cr'] >= 50) & (df['last_close'] >= 50) & (df['last_close'] <= 5000)).sum()}")


if __name__ == "__main__":
    main()
