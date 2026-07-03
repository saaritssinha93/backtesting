r"""redesign_scan.py — regenerate A_MOD_BREAK_C1_LOW-intent events FROM RAW 5-MIN DATA
with redesigned detection variants (out-of-the-box recovery, same core idea:
"a moderate red impulse bar breaks the prior bar's low below session VWAP -> continuation
short"). RESEARCH-ONLY; writes only under this campaign's pools/redesigned/.

The original scanner hardcodes three incidental gates (ADX>=19.12, RSI>=23.22,
atr_pct<=0.0063 i.e. ONLY very-low-volatility names) and fires on EVERY qualifying bar.
Phase 1/2 proved that population is a uniform loser. This scan:

  1. re-detects the CORE event from raw OHLCV (red bar, close_loc<=0.40, range in
     [0.60,2.20]xATR, close<prev bar low, close<session VWAP, vol_ratio>=1.5,
     bar>=3rd of session, slot 09:30..15:00) with the incidental gates REMOVED
     (they become searchable features instead of hardcoded filters — including
     letting HIGH-volatility names in, which the production scanner excludes);
  2. adds structural variant FLAGS on each event:
       flag_fresh_low   bar makes a NEW session low (fresh-low continuation)
       flag_confirm2    prior bar was also a red prior-low break (2-bar persistence)
       flag_deep        close breaks prev low by >= 0.35 ATR (deep flow)
       flag_first       first CORE event of the day for the symbol
       nifty_below_ema20 market alignment (NIFTY50 index below its 5-min EMA20)
  3. builds a separate RETEST-REJECT detector (break, pullback to the broken level
     within 4 bars, red rejection close back below it) = later, better-priced entry;
  4. attaches the full 36-feature causal dictionary (reuses enrich_features.
     compute_ticker_features) + break geometry + NIFTY context to every event.

Outputs (tt.load_pool-compatible):
  pools/redesigned/AMOD_RX2/historical_all_available_pre_dedupe_live_candidates.csv
  pools/redesigned/AMOD_RETEST/historical_all_available_pre_dedupe_live_candidates.csv

Run:  py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_BREAK_C1_LOW\scripts\redesign_scan.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
WORK = _HERE.parent
TT_DIR = WORK.parent.parent
OLD_SCRIPTS = TT_DIR / "setup_pf_1_4_full_loop" / "A_MOD_BREAK_C1_LOW" / "scripts"
for _p in (str(OLD_SCRIPTS),):
    if _p not in sys.path:
        sys.path.insert(0, _p)
from enrich_features import compute_ticker_features, ENRICHED_FEATS  # noqa: E402

DATA_ROOT = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
NIFTY_PARQ = DATA_ROOT / "NIFTY50_INDEX_stocks_indicators_5min.parquet"
OUT_ROOT = WORK / "pools" / "redesigned"
DATE_LO, DATE_HI = "2026-03-01", "2026-06-30"
SLOT_LO, SLOT_HI = "09:30", "15:00"

CORE_VOL_RATIO_MIN = 1.5
IMPULSE_LO, IMPULSE_HI = 0.60, 2.20
CLOSE_LOC_MAX = 0.40
DEEP_ATR = 0.35
RETEST_LOOKAHEAD = 4   # bars after the break during which a retest-reject may fire


def _nifty_context() -> pd.DataFrame:
    df = pd.read_parquet(NIFTY_PARQ, columns=["date", "open", "close", "EMA_20"])
    dt = pd.to_datetime(df["date"])
    if dt.dt.tz is not None:
        dt = dt.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    day = dt.dt.normalize()
    first_open = df["open"].groupby(day).transform("first")
    out = pd.DataFrame({
        "_sig": dt,
        "nifty_day_ret_pct": (df["close"] - first_open) / first_open * 100.0,
        "nifty_below_ema20": (df["close"] < df["EMA_20"]).astype(int),
    })
    out["regime"] = np.select(
        [out["nifty_day_ret_pct"] < -0.30, out["nifty_day_ret_pct"] > 0.30],
        ["BEAR", "BULL"], default="NEUTRAL")
    return out


def scan_ticker(tk: str, nifty: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    p = DATA_ROOT / f"{tk}_stocks_indicators_5min.parquet"
    if not p.exists():
        return pd.DataFrame(), pd.DataFrame()
    try:
        df = pd.read_parquet(p, columns=["date", "open", "high", "low", "close", "volume",
                                         "RSI", "ATR", "EMA_20", "EMA_50", "ADX"])
    except Exception:
        return pd.DataFrame(), pd.DataFrame()
    df = df.sort_values("date").reset_index(drop=True)
    dt = pd.to_datetime(df["date"])
    if dt.dt.tz is not None:
        dt = dt.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    dstr = dt.dt.strftime("%Y-%m-%d")
    in_win = (dstr >= DATE_LO) & (dstr <= DATE_HI)
    if not in_win.any():
        return pd.DataFrame(), pd.DataFrame()

    o, h, l, c, v = (pd.to_numeric(df[k], errors="coerce") for k in ("open", "high", "low", "close", "volume"))
    day = dt.dt.normalize()
    same_day_prev = day.eq(day.shift(1))
    prev_low = l.shift(1).where(same_day_prev)
    prev_red_break = ((c.shift(1) < o.shift(1)) & (c.shift(1) < l.shift(2).where(day.eq(day.shift(2))))).where(same_day_prev, False)

    atr = pd.to_numeric(df["ATR"], errors="coerce")
    tr_rng = pd.concat([(h - l), (h - c.shift(1)).abs(), (l - c.shift(1)).abs()], axis=1).max(axis=1)
    atr = atr.fillna(tr_rng.rolling(14, min_periods=5).mean())
    atr_safe = atr.replace(0, np.nan)
    rng = (h - l)
    rng_safe = rng.replace(0, np.nan)
    vol20 = v.rolling(20, min_periods=10).mean()
    vol_ratio = v / vol20.replace(0, np.nan)
    close_loc = (c - l) / rng_safe
    body_pct = (c - o).abs() / rng_safe
    tp = (h + l + c) / 3
    cum_tpv = (tp * v).groupby(day).cumsum()
    cum_v = v.groupby(day).cumsum().replace(0, np.nan)
    svwap = cum_tpv / cum_v
    bar_no = day.groupby(day).cumcount()
    day_low_prev = l.groupby(day).cummin().shift(1).where(same_day_prev)
    hhmm = dt.dt.strftime("%H:%M")

    core = ((c < o) & (close_loc <= CLOSE_LOC_MAX)
            & (rng >= IMPULSE_LO * atr) & (rng <= IMPULSE_HI * atr)
            & (c < prev_low) & (c < svwap)
            & (vol_ratio >= CORE_VOL_RATIO_MIN)
            & (bar_no >= 2) & (hhmm >= SLOT_LO) & (hhmm <= SLOT_HI) & in_win)
    core = core.fillna(False)

    feats = compute_ticker_features(df)          # 36 causal features, same bar index
    fdt = pd.to_datetime(feats["date"])
    if fdt.dt.tz is not None:
        fdt = fdt.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)

    break_depth_atr = (prev_low - c) / atr_safe

    def _event_frame(idx: pd.Series, setup: str, reason: str) -> pd.DataFrame:
        if not idx.any():
            return pd.DataFrame()
        ev = pd.DataFrame({
            "ticker": tk, "side": "SHORT", "setup": setup,
            "signal_time_ist": dt[idx].dt.tz_localize("Asia/Kolkata").map(lambda x: x.isoformat()),
            "_sig": dt[idx].values,
            "signal_open": o[idx].values, "signal_high": h[idx].values,
            "signal_low": l[idx].values, "signal_close": c[idx].values,
            "signal_volume": v[idx].values,
            "vol_ratio": vol_ratio[idx].values,
            "atr_pct": (atr_safe[idx] / c[idx]).values,
            "body_pct": body_pct[idx].values,
            "close_loc": close_loc[idx].values,
            "vwap_dist_atr": ((svwap[idx] - c[idx]) / atr_safe[idx]).values,   # +ve = depth below VWAP
            "break_depth_atr": break_depth_atr[idx].values,
            "quality_score": np.clip(break_depth_atr[idx].values * 100.0, 0, 500),
            "reason": reason,
            "flag_fresh_low": (l[idx] < day_low_prev[idx]).astype(int).values,
            "flag_confirm2": prev_red_break[idx].astype(int).values,
            "flag_deep": (break_depth_atr[idx] >= DEEP_ATR).astype(int).values,
        })
        fsel = feats.loc[idx.values, list(ENRICHED_FEATS)].reset_index(drop=True)
        return pd.concat([ev.reset_index(drop=True), fsel], axis=1)

    master = _event_frame(core, "AMOD_RX2", "rx2_prior_low_break_core")
    if not master.empty:
        master = master.sort_values("_sig").reset_index(drop=True)
        _d = pd.to_datetime(master["_sig"]).dt.strftime("%Y-%m-%d")
        master["flag_first"] = (~_d.duplicated(keep="first")).astype(int)

    # ---- retest-reject events -----------------------------------------------------
    rt_rows = []
    core_idx = np.where(core.to_numpy())[0]
    dt_np = dt.to_numpy()
    for i in core_idx:
        level = prev_low.iloc[i]
        if not np.isfinite(level):
            continue
        for j in range(i + 1, min(i + 1 + RETEST_LOOKAHEAD, len(df))):
            if day.iloc[j] != day.iloc[i]:
                break
            if hhmm.iloc[j] > SLOT_HI:
                break
            if h.iloc[j] >= level and c.iloc[j] < level and c.iloc[j] < o.iloc[j]:
                rt_rows.append(j)
                break
    retest = pd.DataFrame()
    if rt_rows:
        sel = pd.Series(False, index=df.index)
        sel.iloc[sorted(set(rt_rows))] = True
        retest = _event_frame(sel, "AMOD_RETEST", "rx2_break_retest_reject")
        if not retest.empty:
            retest["flag_first"] = 1  # retests are already sparse; keep column parity
    return master, retest


def main() -> int:
    nifty = _nifty_context()
    tickers = sorted(p.name.split("_stocks_indicators_5min")[0]
                     for p in DATA_ROOT.glob("*_stocks_indicators_5min.parquet")
                     if not p.name.startswith("NIFTY"))
    print(f"[rx2] scanning {len(tickers)} tickers {DATE_LO}..{DATE_HI}")
    m_frames, r_frames = [], []
    t0 = time.time()
    for i, tk in enumerate(tickers, 1):
        try:
            m, r = scan_ticker(tk, nifty)
        except Exception as e:
            print(f"[rx2] {tk} ERROR {type(e).__name__}: {e}")
            continue
        if not m.empty:
            m_frames.append(m)
        if not r.empty:
            r_frames.append(r)
        if i % 200 == 0:
            print(f"[rx2] {i}/{len(tickers)} | master {sum(len(x) for x in m_frames)} "
                  f"retest {sum(len(x) for x in r_frames)} | {time.time()-t0:.0f}s", flush=True)
    for name, frames in (("AMOD_RX2", m_frames), ("AMOD_RETEST", r_frames)):
        df = pd.concat(frames, ignore_index=True)
        df["_sig"] = pd.to_datetime(df["_sig"])
        df = df.merge(nifty, on="_sig", how="left")
        df["market_ret_pct"] = df["nifty_day_ret_pct"]
        df["regime"] = df["regime"].fillna("NEUTRAL")
        out_dir = OUT_ROOT / name
        out_dir.mkdir(parents=True, exist_ok=True)
        df.drop(columns=["_sig"]).to_csv(out_dir / "historical_all_available_pre_dedupe_live_candidates.csv",
                                         index=False)
        print(f"[rx2] {name}: {len(df)} events / {df['ticker'].nunique()} tickers -> {out_dir}")
        if name == "AMOD_RX2":
            print(f"[rx2]   flags: fresh_low {int(df['flag_fresh_low'].sum())}, "
                  f"confirm2 {int(df['flag_confirm2'].sum())}, deep {int(df['flag_deep'].sum())}, "
                  f"first {int(df['flag_first'].sum())}, nifty_below_ema20 {int(df['nifty_below_ema20'].fillna(0).sum())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
