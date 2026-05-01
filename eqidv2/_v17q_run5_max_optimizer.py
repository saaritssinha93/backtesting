# -*- coding: utf-8 -*-
"""
RUN 5 MAX -- volume-targeted per-setup filter optimizer (fast version).

For each (side, setup), pre-compute metrics at 5 graded looseness levels
(L0=strictest = RUN5_PRO, L4=loosest = no filter). Then pick the per-setup
level mix that targets ~2000 aggregate trades while maximizing aggregate PF.

Levels per setup (loosen one constraint at a time):
  L0: full RUN5_PRO filter
  L1: relax ADX min by ~5
  L2: also relax QS / RSI / hour cap
  L3: also relax atr_pct band
  L4: only hard whitelist (setup name)

Outputs:
  run5_max_per_setup_filters.csv
  run5_max_selected_trades.csv
  run5_max_daily_pnl_curve.csv
  run5_max_long_short_breakdown.csv
"""
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd
import numpy as np

OUT_DIR = Path(r"C:/TradingData/eqidv2/outputs_v17q_5min")
RUN5_CSV = OUT_DIR / "avwap_longshort_trades_v16_5min_ALL_DAYS_20260427_143701.csv"

TARGET_TOTAL_TRADES = 2000
HARD_PF_FLOOR = 0.85   # marginal-trade PF floor; trades below this won't be added


def metrics(df):
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
    return dict(n=n,
                win_rate=float((pnl_p > 0).mean() * 100),
                pf=pf,
                sum_pnl_p=float(pnl_p.sum()),
                sum_pnl_lev=float(pnl_l.sum()),
                max_dd_pct=max_dd,
                day_count=day_count,
                day_win_rate=day_win_rate,
                sharpe=sharpe)


# RUN5_PRO baseline filters (strictest level L0)
PRO_FILTERS = {
    ("LONG", "B_HUGE_C1_CLOSE_RECLAIM_BREAK"): dict(rsi=(50, 75), adx_min=30, qs_min=None, hour_cap=11.5, atr_pct=(0.003, 0.012)),
    ("LONG", "B_AVWAP_RECLAIM_REVERSAL"):      dict(rsi=(50, 75), adx_min=30, qs_min=5,    hour_cap=None, atr_pct=None),
    ("LONG", "A_MOD_BREAK_C1_HIGH"):           dict(rsi=None,    adx_min=30, qs_min=7,    hour_cap=None, atr_pct=(0.003, 0.012)),
    ("LONG", "C_OR_BREAKOUT"):                 dict(rsi=(45, 100), adx_min=30, qs_min=3,  hour_cap=None, atr_pct=None),
    ("LONG", "G_HIGHER_HIGH_BREAK"):           dict(rsi=(50, 75), adx_min=30, qs_min=3,    hour_cap=None, atr_pct=None),
    ("SHORT", "A_MOD_BREAK_C1_LOW"):           dict(rsi=(30, 50), adx_min=None, qs_min=None, hour_cap=13.0, atr_pct=(0.003, 0.012)),
    ("SHORT", "G_LOWER_LOW_BREAK"):            dict(rsi=(30, 50), adx_min=30, qs_min=None, hour_cap=None, atr_pct=(0.003, 0.012)),
    ("SHORT", "D_EMA20_REJECTION"):            dict(rsi=(0, 45),  adx_min=30, qs_min=None, hour_cap=11.5, atr_pct=(0.003, 0.012)),
    ("SHORT", "C_OR_BREAKDOWN"):               dict(rsi=(20, 45), adx_min=30, qs_min=None, hour_cap=None, atr_pct=(0.004, 0.020)),
    ("SHORT", "D_AVWAP_LOSE_REVERSAL"):        dict(rsi=(25, 50), adx_min=None, qs_min=None, hour_cap=None, atr_pct=(0.004, 0.020)),
}

# Add the dropped setups too -- with default-loose initial filters
EXTRA_SETUPS = {
    ("LONG",  "A_MOD_CLOSE_CONTINUATION_BREAK"): dict(rsi=(45, 80), adx_min=25, qs_min=3,  hour_cap=None, atr_pct=(0.003, 0.012)),
    ("LONG",  "D_EMA20_BOUNCE"):                  dict(rsi=(45, 80), adx_min=25, qs_min=3,  hour_cap=None, atr_pct=(0.003, 0.012)),
}
PRO_FILTERS.update(EXTRA_SETUPS)


def loosen(filt: dict, level: int) -> dict:
    """Return filt loosened by the given level (0=unchanged, 4=most loose)."""
    f = dict(filt)
    if level >= 1:
        # Relax ADX by 5 (or remove if was 22)
        if f.get("adx_min") is not None:
            new_adx = max(0, f["adx_min"] - 5)
            f["adx_min"] = new_adx if new_adx >= 18 else None
    if level >= 2:
        # Relax QS by 2 (or remove if was 2)
        if f.get("qs_min") is not None:
            new_qs = f["qs_min"] - 2
            f["qs_min"] = new_qs if new_qs >= 1 else None
        # Widen RSI by 5 each side
        if f.get("rsi") is not None:
            lo, hi = f["rsi"]
            f["rsi"] = (max(0, lo - 5), min(100, hi + 5))
        # Drop hour cap
        f["hour_cap"] = None
    if level >= 3:
        # Widen atr_pct
        if f.get("atr_pct") is not None:
            lo, hi = f["atr_pct"]
            f["atr_pct"] = (max(0.001, lo * 0.7), hi * 1.3)
        # Widen RSI further
        if f.get("rsi") is not None:
            lo, hi = f["rsi"]
            f["rsi"] = (max(0, lo - 10), min(100, hi + 10))
        # Drop ADX entirely
        f["adx_min"] = None
    if level >= 4:
        # Strip everything except setup
        f = dict(rsi=None, adx_min=None, qs_min=None, hour_cap=None, atr_pct=None)
    return f


def apply_filter(sub: pd.DataFrame, side: str, filt: dict) -> pd.Series:
    rsi = pd.to_numeric(sub.get("rsi_signal", np.nan), errors="coerce")
    adx = pd.to_numeric(sub.get("adx_signal", np.nan), errors="coerce")
    qs = pd.to_numeric(sub.get("quality_score", np.nan), errors="coerce")
    atr_pct = pd.to_numeric(sub.get("atr_pct_signal", np.nan), errors="coerce")
    et = pd.to_datetime(sub.get("entry_time_ist"), errors="coerce", utc=True)
    hr = et.dt.tz_convert("Asia/Kolkata").dt.hour + et.dt.tz_convert("Asia/Kolkata").dt.minute / 60.0

    keep = pd.Series(True, index=sub.index)
    if filt.get("rsi") is not None:
        keep &= rsi.between(filt["rsi"][0], filt["rsi"][1], inclusive="left")
    if filt.get("adx_min") is not None:
        keep &= (adx >= filt["adx_min"])
    if filt.get("qs_min") is not None:
        keep &= (qs >= filt["qs_min"])
    if filt.get("hour_cap") is not None:
        keep &= (hr < filt["hour_cap"])
    if filt.get("atr_pct") is not None:
        keep &= atr_pct.between(filt["atr_pct"][0], filt["atr_pct"][1], inclusive="both")
    return keep


def filter_label(filt):
    parts = [
        f"RSI={filt.get('rsi')}",
        f"ADX>={filt.get('adx_min')}",
        f"QS>={filt.get('qs_min')}",
        f"hr<{filt.get('hour_cap')}",
        f"atr={filt.get('atr_pct')}",
    ]
    return " | ".join(parts)


def main():
    df = pd.read_csv(RUN5_CSV)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.reset_index(drop=True)
    print(f"Loaded {len(df)} trades")

    # Build per-setup, per-level metrics table.
    print("\n=== Per-setup, per-level metrics ===")
    print(f"{'side':6s} {'setup':35s} {'L':>1s} {'n':>5s} {'pf':>6s} {'win':>6s} {'dd':>6s}")
    print("-" * 80)

    per_setup_levels = {}  # (side, setup) -> [(level, filt, n, pf, m, kept_idx_set), ...]
    for (side, setup), base_filt in PRO_FILTERS.items():
        sub = df[(df["side"] == side) & (df["setup"] == setup)].copy()
        if sub.empty:
            continue
        levels = []
        for L in range(5):
            f = loosen(base_filt, L)
            keep = apply_filter(sub, side, f)
            kept = sub.loc[keep]
            m = metrics(kept)
            levels.append((L, f, m, set(kept.index.tolist())))
            print(f"{side:6s} {setup:35s} {L} {m['n']:>5d} {m['pf']:>6.2f} {m['win_rate']:>5.1f}% {m['max_dd_pct']:>5.2f}%")
        per_setup_levels[(side, setup)] = levels

    # Now optimize: for each setup, pick a level. Score the combination by:
    # - total trade count distance from TARGET_TOTAL_TRADES
    # - aggregate PF (higher better)
    # - day-win and DD (secondary)
    # Greedy approach: start with all L0 (RUN5_PRO baseline). While total < target,
    # for each setup, compute marginal (added trades, PF of added trades) when
    # going to the next level. Pick the setup-level upgrade with the BEST marginal
    # PF on its added trades. Repeat.

    chosen_level = {key: 0 for key in per_setup_levels}

    def aggregate_with(chosen_level):
        keep_idxs = set()
        for key, L in chosen_level.items():
            level_data = per_setup_levels[key][L]
            keep_idxs |= level_data[3]
        sel = df.loc[df.index.isin(keep_idxs)]
        return sel

    # Greedy upgrade
    print("\n=== Greedy upgrade pass ===")
    while True:
        sel = aggregate_with(chosen_level)
        cur_n = len(sel)
        if cur_n >= TARGET_TOTAL_TRADES:
            break
        # Find best marginal: for each setup that can be upgraded, compute the
        # delta trades and the PF of those delta trades.
        best = None  # (key, marginal_pf, delta_n, marginal_added_idx)
        for key, cur_L in chosen_level.items():
            if cur_L == 4:
                continue
            cur_idx = per_setup_levels[key][cur_L][3]
            nxt_idx = per_setup_levels[key][cur_L + 1][3]
            added = nxt_idx - cur_idx
            if not added:
                continue
            added_df = df.loc[df.index.isin(added)]
            am = metrics(added_df)
            if am["pf"] < HARD_PF_FLOOR:
                continue
            # Score the upgrade: prefer high PF + higher count
            score = am["pf"] + min(am["n"] / 100.0, 0.5)
            if best is None or score > best[3]:
                best = (key, am["pf"], len(added), score, am)
        if best is None:
            break
        key, pf_added, delta_n, _, am = best
        chosen_level[key] += 1
        new_n = cur_n + delta_n
        print(f"  upgrade {key[0]} {key[1]} L{chosen_level[key]-1}->L{chosen_level[key]} "
              f"+{delta_n} trades (PF on added={pf_added:.2f}, win={am['win_rate']:.1f}%) total={new_n}")
        if new_n >= TARGET_TOTAL_TRADES:
            break

    # Capture trade-off curve at multiple stops
    print("\n=== Sweeping marginal-PF floor to map volume-vs-quality curve ===")
    curve_rows = []
    for floor in (1.00, 0.90, 0.80, 0.70, 0.60, 0.50, 0.40, 0.0):
        cl = {key: 0 for key in per_setup_levels}
        # Greedy upgrade with this floor
        for _ in range(200):
            cur = aggregate_with(cl)
            if len(cur) >= 4459:
                break
            best = None
            for key, cur_L in cl.items():
                if cur_L == 4:
                    continue
                cur_idx = per_setup_levels[key][cur_L][3]
                nxt_idx = per_setup_levels[key][cur_L + 1][3]
                added = nxt_idx - cur_idx
                if not added:
                    continue
                added_df = df.loc[df.index.isin(added)]
                am = metrics(added_df)
                if am["pf"] < floor:
                    continue
                score = am["pf"] + min(am["n"] / 100.0, 0.5)
                if best is None or score > best[1]:
                    best = (key, score)
            if best is None:
                break
            cl[best[0]] += 1
        sel_at_floor = aggregate_with(cl)
        m_at = metrics(sel_at_floor)
        curve_rows.append(dict(marginal_pf_floor=floor, **m_at))
        print(f"  floor={floor:.2f}: n={m_at['n']:>5d}, PF={m_at['pf']:.3f}, "
              f"win={m_at['win_rate']:.1f}%, day-win={m_at['day_win_rate']:.1f}%, "
              f"DD={m_at['max_dd_pct']:.2f}%")
    curve_df = pd.DataFrame(curve_rows)
    curve_csv = OUT_DIR / "run5_max_volume_quality_curve.csv"
    curve_df.to_csv(curve_csv, index=False)
    print(f"Wrote {curve_csv}")

    # Pick the floor whose n is closest to target
    curve_df["gap"] = (curve_df["n"] - TARGET_TOTAL_TRADES).abs()
    best_row = curve_df.sort_values("gap").iloc[0]
    chosen_floor = float(best_row["marginal_pf_floor"])
    print(f"\nClosest floor to target {TARGET_TOTAL_TRADES}: floor={chosen_floor}, n={int(best_row['n'])}")

    # Re-run greedy with chosen floor to get the actual chosen levels
    chosen_level = {key: 0 for key in per_setup_levels}
    for _ in range(200):
        cur = aggregate_with(chosen_level)
        if len(cur) >= 4459:
            break
        best = None
        for key, cur_L in chosen_level.items():
            if cur_L == 4:
                continue
            cur_idx = per_setup_levels[key][cur_L][3]
            nxt_idx = per_setup_levels[key][cur_L + 1][3]
            added = nxt_idx - cur_idx
            if not added:
                continue
            added_df = df.loc[df.index.isin(added)]
            am = metrics(added_df)
            if am["pf"] < chosen_floor:
                continue
            score = am["pf"] + min(am["n"] / 100.0, 0.5)
            if best is None or score > best[1]:
                best = (key, score)
        if best is None:
            break
        chosen_level[best[0]] += 1

    final_sel = aggregate_with(chosen_level)
    final_m = metrics(final_sel)
    print(f"\n>>> Final: n={final_m['n']}, target was {TARGET_TOTAL_TRADES}, "
          f"chosen marginal-PF floor = {chosen_floor}")

    # Save deliverables
    chosen_rows = []
    for key, L in chosen_level.items():
        side, setup = key
        level_data = per_setup_levels[key][L]
        f = level_data[1]
        m = level_data[2]
        chosen_rows.append(dict(
            side=side, setup=setup, level=L,
            n=m["n"], pf=m["pf"], win_rate=m["win_rate"], max_dd_pct=m["max_dd_pct"],
            day_win_rate=m["day_win_rate"], sum_pnl_p=m["sum_pnl_p"],
            filter_label=filter_label(f),
        ))
    chosen_df = pd.DataFrame(chosen_rows).sort_values("n", ascending=False)
    chosen_csv = OUT_DIR / "run5_max_per_setup_filters.csv"
    chosen_df.to_csv(chosen_csv, index=False)
    print(f"Wrote {chosen_csv}")

    sel_csv = OUT_DIR / "run5_max_selected_trades.csv"
    final_sel.to_csv(sel_csv, index=False)
    print(f"Wrote {sel_csv}")

    bd_rows = []
    for side in ("LONG", "SHORT", "ALL"):
        sub = final_sel if side == "ALL" else final_sel[final_sel["side"] == side]
        bd_rows.append(dict(side=side, **metrics(sub)))
    bd = pd.DataFrame(bd_rows)
    bd_csv = OUT_DIR / "run5_max_long_short_breakdown.csv"
    bd.to_csv(bd_csv, index=False)
    print(f"Wrote {bd_csv}")

    sel = final_sel.copy()
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
    daily_csv = OUT_DIR / "run5_max_daily_pnl_curve.csv"
    daily_full.to_csv(daily_csv, index=False)
    print(f"Wrote {daily_csv}")

    print("\n=== Per-setup chosen filters (sorted by trade count) ===")
    print(chosen_df[["side", "setup", "level", "n", "pf", "win_rate", "max_dd_pct", "filter_label"]].to_string(index=False))

    print("\n=== AGGREGATE METRICS ===")
    for k, v in final_m.items():
        if isinstance(v, float):
            print(f"  {k:18s}: {v:.3f}")
        else:
            print(f"  {k:18s}: {v}")

    print("\n=== LONG / SHORT BREAKDOWN ===")
    print(bd.to_string(index=False))

    return 0


if __name__ == "__main__":
    sys.exit(main())
