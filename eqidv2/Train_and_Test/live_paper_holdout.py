"""live_paper_holdout.py — the TRUEST out-of-sample check: the actual live conf
paper trades, aggregated per-setup + total (net of cost).

The live v7 5-min stack trades the 16-setup final_setup_conf in PAPER mode every
day and writes per-day fills to:
    C:\\TradingData\\eqidv2\\live_signals\\paper_trades_<YYYY-MM-DD>_id_5min_v7.csv

This reads those files for a date window and reports net_pnl_rs PF / win% / count
by setup and overall. Use it as the real holdout that NO backtest pool can fake —
it reflects exactly what the live book did, overlay leaks and all.

Run from anywhere:
  py -3.12 Train_and_Test\\live_paper_holdout.py [--start 2026-06-16] [--end 2026-06-22]
  (default window = conf-live era, 2026-06-16 .. today)
"""
from __future__ import annotations

import argparse
import glob
from pathlib import Path

import numpy as np
import pandas as pd

LIVE_DIR = Path(r"C:\TradingData\eqidv2\live_signals")
PATTERN = "paper_trades_*_id_5min_v7.csv"
CONF_LIVE_START = "2026-06-16"   # date the 16-setup conf went live in paper


def _pf(net: np.ndarray) -> float:
    g = net[net > 0].sum()
    loss = -net[net < 0].sum()
    return float(g / loss) if loss > 0 else float("inf")


def load_live(start: str, end: str) -> pd.DataFrame:
    frames = []
    for f in sorted(glob.glob(str(LIVE_DIR / PATTERN))):
        day = Path(f).name.split("paper_trades_")[1][:10]
        if not (start <= day <= end):
            continue
        try:
            d = pd.read_csv(f, on_bad_lines="skip", low_memory=False)  # ragged-row tolerant
        except Exception:
            continue
        d["_day"] = day
        frames.append(d)
    if not frames:
        raise SystemExit(f"[holdout] no live paper trades in {start}..{end} under {LIVE_DIR}")
    df = pd.concat(frames, ignore_index=True, sort=False)
    df["net"] = pd.to_numeric(df.get("net_pnl_rs"), errors="coerce")
    df = df.dropna(subset=["net"])
    # resolved trades only (a real exit, or any non-zero P&L)
    resolved = df["outcome"].astype(str).str.upper().isin(
        ["TARGET", "SL", "EOD", "TIME", "TRAIL", "STOP", "EXIT"])
    return df[resolved | df["net"].ne(0)].copy()


def report(df: pd.DataFrame, label: str) -> None:
    net = df["net"].to_numpy()
    print(f"\n=== {label}: n={len(df)} days={df['_day'].nunique()} "
          f"net=Rs {net.sum():,.0f} win%={100*(net>0).mean():.0f} PF={_pf(net):.2f} ===")
    rows = []
    for setup, g in df.groupby("setup"):
        n = g["net"].to_numpy()
        rows.append({"setup": setup, "n": len(g), "net_rs": round(n.sum(), 0),
                     "win%": round(100*(n > 0).mean(), 1), "PF": round(_pf(n), 2)})
    out = pd.DataFrame(rows).sort_values("net_rs")
    with pd.option_context("display.width", 200, "display.max_rows", None):
        print(out.to_string(index=False))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=CONF_LIVE_START)
    ap.add_argument("--end", default=pd.Timestamp.today().strftime("%Y-%m-%d"))
    args = ap.parse_args()
    df = load_live(args.start, args.end)
    report(df, f"LIVE PAPER {args.start}..{args.end}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
