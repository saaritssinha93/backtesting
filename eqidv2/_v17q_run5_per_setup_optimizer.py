# -*- coding: utf-8 -*-
"""
RUN 5 -- per-setup parameter optimizer.

Goal: keep ALL setups firing in v17q (no whitelist) but find each setup's
own per-feature filter that maximizes its standalone PF subject to a
minimum sample size. Aggregate the per-setup filtered subsets and report
combined metrics.

For each (side, setup), tries a compact grid:
  - RSI band: side-natural ranges + "no filter"
  - ADX min:  20, 25, 30, no filter
  - QS  min:  3, 5, 7,  no filter
  - Hour cap: <=11.5, <=13.0, <=14.0, no filter
  - NIFTY:    side-aligned mode (LONG_ONLY/BOTH for L; SHORT_ONLY/BOTH for S) or no filter
  - atr_pct band: [0.003, 0.012], [0.004, 0.020], no filter

Per setup, picks the filter combo with the best composite score
(PF * sample_factor - drawdown_penalty) subject to n >= MIN_TRADES.

Outputs:
  run5_per_setup_filter_grid.csv  -- every (setup, filter) combination tested
  run5_per_setup_best_filters.csv -- one row per setup with chosen filter
  run5_pro_selected_trades.csv    -- aggregate trade set with per-setup filters applied
  run5_pro_daily_pnl_curve.csv    -- daily P&L + drawdown for the aggregate
  run5_pro_long_short_breakdown.csv -- LONG / SHORT / ALL summary
"""
from __future__ import annotations

import sys
import itertools
from pathlib import Path
import pandas as pd
import numpy as np

OUT_DIR = Path(r"C:/TradingData/eqidv2/outputs_v17q_5min")
RUN5_CSV = OUT_DIR / "avwap_longshort_trades_v16_5min_ALL_DAYS_20260427_143701.csv"

# Tunables
MIN_TRADES_PER_SETUP = 15      # require at least this many post-filter trades to consider
ABSOLUTE_MIN_TRADES = 8        # below this, the setup is completely dropped
PF_FLOOR = 1.05                # require PF >= this to keep the setup
DROP_IF_PF_BELOW = 0.95        # drop the setup entirely if even the best PF is below this


# ============================================================================
# Metric helpers
# ============================================================================
def metrics(df: pd.DataFrame) -> dict:
    n = len(df)
    if n == 0:
        return dict(n=0, win_rate=0.0, pf=0.0, sum_pnl_p=0.0, sum_pnl_lev=0.0,
                    max_dd_pct=0.0, day_count=0, day_win_rate=0.0, sharpe=0.0)
    pnl_p = pd.to_numeric(df.get("pnl_pct_price", df.get("pnl_pct", 0.0)), errors="coerce").fillna(0.0)
    pnl_l = pd.to_numeric(df.get("pnl_pct", 0.0), errors="coerce").fillna(0.0)
    wins = pnl_p[pnl_p > 0].sum()
    losses = abs(pnl_p[pnl_p < 0].sum())
    pf = (wins / losses) if losses > 0 else float("inf")
    d = df.copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce").dt.date
    daily = d.groupby("trade_date")["pnl_pct_price"].apply(lambda s: pd.to_numeric(s, errors="coerce").fillna(0.0).sum())
    day_count = int(len(daily))
    day_win_rate = float((daily > 0).sum() / day_count * 100.0) if day_count else 0.0
    cum = daily.cumsum()
    max_dd = float((cum.cummax() - cum).max()) if len(cum) else 0.0
    if day_count > 1:
        std = float(daily.std(ddof=1))
        sharpe = float(daily.mean() / std * np.sqrt(252)) if std > 0 else 0.0
    else:
        sharpe = 0.0
    return dict(
        n=n,
        win_rate=float((pnl_p > 0).mean() * 100),
        pf=pf,
        sum_pnl_p=float(pnl_p.sum()),
        sum_pnl_lev=float(pnl_l.sum()),
        max_dd_pct=max_dd,
        day_count=day_count,
        day_win_rate=day_win_rate,
        sharpe=sharpe,
    )


def score(m: dict) -> float:
    """Per-setup composite. Reward PF & win rate, penalize tiny samples."""
    if m["n"] < MIN_TRADES_PER_SETUP:
        return -1e6
    pf = min(m["pf"], 3.0)
    pf_score = (pf - 1.0) * 100
    win_score = (m["win_rate"] - 50) * 0.6
    dd_pen = -m["max_dd_pct"] * 0.5
    sample_bonus = min(np.log10(m["n"]) * 8, 20)
    return pf_score + win_score + dd_pen + sample_bonus


# ============================================================================
# Filter grid
# ============================================================================
RSI_BANDS_LONG = [
    None,
    (40, 100),
    (45, 100),
    (50, 100),
    (55, 100),
    (60, 100),
    (50, 80),
    (50, 75),
    (55, 75),
    (45, 70),
    (60, 80),
]

RSI_BANDS_SHORT = [
    None,
    (0, 60),
    (0, 55),
    (0, 50),
    (0, 45),
    (0, 40),
    (20, 50),
    (25, 50),
    (30, 50),
    (15, 45),
    (20, 45),
]

ADX_MINS = [None, 20, 25, 30]
QS_MINS = [None, 3, 5, 7]
HOUR_CAPS = [None, 11.5, 13.0, 14.0, 14.5]
NIFTY_OPTS = [None, "side_aligned", "strict_directional"]
ATR_BANDS = [None, (0.003, 0.012), (0.004, 0.020), (0.0025, 0.025)]


def apply_filter(df: pd.DataFrame, side: str, rsi_band, adx_min, qs_min, hour_cap, nifty_opt, atr_band) -> pd.Series:
    rsi = pd.to_numeric(df.get("rsi_signal", np.nan), errors="coerce")
    adx = pd.to_numeric(df.get("adx_signal", np.nan), errors="coerce")
    qs = pd.to_numeric(df.get("quality_score", np.nan), errors="coerce")
    atr_pct = pd.to_numeric(df.get("atr_pct_signal", np.nan), errors="coerce")
    et = pd.to_datetime(df.get("entry_time_ist"), errors="coerce", utc=True)
    hr = et.dt.tz_convert("Asia/Kolkata").dt.hour + et.dt.tz_convert("Asia/Kolkata").dt.minute / 60.0
    nctx = df.get("nifty_context_mode", pd.Series("BOTH", index=df.index)).astype(str)

    keep = pd.Series(True, index=df.index)
    if rsi_band is not None:
        keep &= rsi.between(rsi_band[0], rsi_band[1], inclusive="left")
    if adx_min is not None:
        keep &= (adx >= adx_min)
    if qs_min is not None:
        keep &= (qs >= qs_min)
    if hour_cap is not None:
        keep &= (hr < hour_cap)
    if nifty_opt == "side_aligned":
        if side == "LONG":
            keep &= nctx.isin(["LONG_ONLY", "BOTH"])
        else:
            keep &= nctx.isin(["SHORT_ONLY", "BOTH"])
    elif nifty_opt == "strict_directional":
        if side == "LONG":
            keep &= nctx.eq("LONG_ONLY")
        else:
            keep &= nctx.eq("SHORT_ONLY")
    if atr_band is not None:
        keep &= atr_pct.between(atr_band[0], atr_band[1])
    return keep


def filter_label(rsi_band, adx_min, qs_min, hour_cap, nifty_opt, atr_band) -> str:
    parts = []
    parts.append(f"RSI={rsi_band}" if rsi_band is not None else "RSI=any")
    parts.append(f"ADX>={adx_min}" if adx_min is not None else "ADX=any")
    parts.append(f"QS>={qs_min}" if qs_min is not None else "QS=any")
    parts.append(f"hr<{hour_cap}" if hour_cap is not None else "hr=any")
    parts.append(f"nifty={nifty_opt}" if nifty_opt is not None else "nifty=any")
    parts.append(f"atr={atr_band}" if atr_band is not None else "atr=any")
    return " | ".join(parts)


# ============================================================================
# Per-setup search
# ============================================================================
def search_setup(df: pd.DataFrame, side: str, setup: str) -> tuple[dict, list[dict]]:
    sub = df[(df["side"] == side) & (df["setup"] == setup)].copy()
    if len(sub) < ABSOLUTE_MIN_TRADES:
        return None, []
    sub = sub.reset_index(drop=True)

    rsi_grid = RSI_BANDS_LONG if side == "LONG" else RSI_BANDS_SHORT
    grid = list(itertools.product(rsi_grid, ADX_MINS, QS_MINS, HOUR_CAPS, NIFTY_OPTS, ATR_BANDS))

    results = []
    best = None
    best_s = -1e9
    for rsi_b, adx_m, qs_m, hr_c, ni, atr_b in grid:
        keep = apply_filter(sub, side, rsi_b, adx_m, qs_m, hr_c, ni, atr_b)
        kept = sub.loc[keep]
        m = metrics(kept)
        s = score(m)
        row = dict(side=side, setup=setup,
                   rsi_band=str(rsi_b), adx_min=adx_m, qs_min=qs_m, hour_cap=hr_c,
                   nifty=ni, atr_band=str(atr_b),
                   filter_label=filter_label(rsi_b, adx_m, qs_m, hr_c, ni, atr_b),
                   score=s, **m)
        results.append(row)
        if s > best_s:
            best_s = s
            best = row
    return best, results


# ============================================================================
# Main
# ============================================================================
def main() -> int:
    df = pd.read_csv(RUN5_CSV)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.reset_index(drop=True)
    print(f"Loaded Run 5 CSV: {len(df)} trades")

    setups = sorted(df.groupby(["side", "setup"]).size().index.tolist())
    print(f"Setups present: {len(setups)}")

    all_grid_rows = []
    best_per_setup = []
    DROPPED = []

    for side, setup in setups:
        best, rows = search_setup(df, side, setup)
        all_grid_rows.extend(rows)
        if best is None:
            DROPPED.append((side, setup, "n<8"))
            continue
        # Decide if setup is keepable
        if best["pf"] < DROP_IF_PF_BELOW:
            DROPPED.append((side, setup, f"best PF {best['pf']:.2f} < {DROP_IF_PF_BELOW:.2f}"))
            continue
        best_per_setup.append(best)
        print(f"  KEEP  {side:6s} {setup:35s} -> "
              f"PF={best['pf']:.2f} win={best['win_rate']:.1f}% n={best['n']:>4d}  filter: {best['filter_label']}")

    for side, setup, reason in DROPPED:
        print(f"  DROP  {side:6s} {setup:35s} -- {reason}")

    # Save grid
    grid_df = pd.DataFrame(all_grid_rows)
    grid_csv = OUT_DIR / "run5_per_setup_filter_grid.csv"
    grid_df.to_csv(grid_csv, index=False)
    print(f"\nWrote {grid_csv} ({len(grid_df)} rows)")

    # Save chosen filters
    chosen = pd.DataFrame(best_per_setup)
    chosen_csv = OUT_DIR / "run5_per_setup_best_filters.csv"
    chosen.to_csv(chosen_csv, index=False)
    print(f"Wrote {chosen_csv} ({len(chosen)} kept setups)")

    # Build aggregate trade set: for each kept setup, apply its chosen filter
    keep_masks = []
    for row in best_per_setup:
        side, setup = row["side"], row["setup"]
        sub = df[(df["side"] == side) & (df["setup"] == setup)]
        rsi_b = eval(row["rsi_band"]) if row["rsi_band"] != "None" else None
        atr_b = eval(row["atr_band"]) if row["atr_band"] != "None" else None
        keep = apply_filter(sub, side, rsi_b, row["adx_min"], row["qs_min"],
                             row["hour_cap"], row["nifty"], atr_b)
        kept_idx = sub.index[keep].tolist()
        keep_masks.extend(kept_idx)

    selected = df.loc[df.index.isin(keep_masks)].copy()
    sel_csv = OUT_DIR / "run5_pro_selected_trades.csv"
    selected.to_csv(sel_csv, index=False)
    print(f"Wrote {sel_csv} ({len(selected)} trades)")

    # Aggregate metrics
    print("\n=== Aggregate (all kept setups, per-setup filters) ===")
    m = metrics(selected)
    for k, v in m.items():
        if isinstance(v, float):
            print(f"  {k:18s}: {v:.3f}")
        else:
            print(f"  {k:18s}: {v}")

    # Long-Short breakdown
    breakdown = []
    for side in ("LONG", "SHORT", "ALL"):
        sub = selected if side == "ALL" else selected[selected["side"] == side]
        m_s = metrics(sub)
        breakdown.append(dict(side=side, **m_s))
    bd = pd.DataFrame(breakdown)
    bd_csv = OUT_DIR / "run5_pro_long_short_breakdown.csv"
    bd.to_csv(bd_csv, index=False)
    print(f"\nWrote {bd_csv}")
    print(bd.to_string(index=False))

    # Daily P&L curve
    sel = selected.copy()
    sel["trade_date"] = pd.to_datetime(sel["trade_date"]).dt.date
    daily_p = sel.groupby("trade_date")["pnl_pct_price"].sum()
    daily_l = sel.groupby("trade_date")["pnl_pct"].sum()
    daily_full = pd.DataFrame({
        "trade_date": daily_p.index,
        "daily_pnl_pct_price": daily_p.values,
        "daily_pnl_pct_levered": daily_l.values,
    })
    daily_full["cum_pnl_price"] = daily_full["daily_pnl_pct_price"].cumsum()
    daily_full["cum_pnl_lev"] = daily_full["daily_pnl_pct_levered"].cumsum()
    daily_full["high_water_lev"] = daily_full["cum_pnl_lev"].cummax()
    daily_full["drawdown_lev"] = daily_full["cum_pnl_lev"] - daily_full["high_water_lev"]
    daily_csv = OUT_DIR / "run5_pro_daily_pnl_curve.csv"
    daily_full.to_csv(daily_csv, index=False)
    print(f"\nWrote {daily_csv} ({len(daily_full)} days)")

    # Per-setup verdict
    print("\n=== Final per-setup verdict ===")
    for row in best_per_setup:
        print(f"  {row['side']:6s} {row['setup']:35s} -> "
              f"PF={row['pf']:.2f} win={row['win_rate']:.1f}% n={row['n']:>4d}  filter: {row['filter_label']}")

    print("\n=== Summary ===")
    print(f"Setups in universe   : {len(setups)}")
    print(f"Setups KEPT          : {len(best_per_setup)}")
    print(f"Setups DROPPED       : {len(DROPPED)}")
    print(f"Aggregate trade count: {len(selected)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
