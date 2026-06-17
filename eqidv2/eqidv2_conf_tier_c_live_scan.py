"""Live causal detectors for the 4 Tier-C final_setup_conf setups that the native
v7 candidate scanner does NOT emit:

    E_ORB_RETEST_HOLD_LONG   (research_v11_tier123_new_setups.py, Tier 1)
    V_RECLAIM_PULLBACK_LONG  (research_v11_tier123_new_setups.py, Tier 1)
    P_PDH_BREAK_RETEST_LONG  (research_v11_tier123_new_setups.py, Tier 3)
    L_RS_LEADER_VWAP_HOLD    (new_setups_scan_v11.py, structural RS-leader scan)

In the v11 backtest these came from pre-computed research CSVs. This module ports
their detection to a per-slot LIVE scan so the same candidates are produced
on-line. The detection logic is copied VERBATIM from the research scripts so the
live candidates match the trained ones:
  - tier123 conditions  : research_v11_tier123_new_setups.py::_scan_ticker
                          (E_ORB_RETEST L480, V_RECLAIM L490, P_PDH L552) + its
                          per-day "prior break / vwap washout-reclaim" STATE.
  - L_RS_LEADER         : new_setups_scan_v11.py::_scan_ticker (L191-202).

CAUSALITY: only bars up to and including the current slot bar are loaded; the
per-day state flags are replayed from bars [4 .. i-1] (exactly as the research
loop accumulates them), and detection fires only on bar i (the just-completed
5-min signal bar). prev-day levels come from prior sessions only. No future bar
is read. This matches how the native candidate_scan fires.

Shared helpers (VWAP-corrected prepare, market context, candidate schema, quality)
are reused from eqidv2_v11_live_overlay so the emitted rows carry the same columns
the conf gate + entry engine expect.

Only referenced when EQIDV2_USE_FINAL_SETUP_CONF is set (the scanner calls this in
conf mode). Stand-alone import has no side effects and no broker access.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

import avwap_5min_ID_v7_candidate_scan as candidate_scan
import eqidv2_v11_live_overlay as ov

IST_TZ = ov.IST_TZ
_safe_float = ov._safe_float
_normalise_ts = ov._normalise_ts

TIER_C_SETUPS = (
    "E_ORB_RETEST_HOLD_LONG",
    "V_RECLAIM_PULLBACK_LONG",
    "P_PDH_BREAK_RETEST_LONG",
    "L_RS_LEADER_VWAP_HOLD",
)


def _prev_day_levels(prepared_full: pd.DataFrame) -> pd.DataFrame:
    """Attach prev_day_high / prev_day_low / prev_day_close_calc (prior sessions
    only) — mirrors research_v11_tier123_new_setups._prev_day_levels."""
    out = prepared_full.drop(
        columns=["prev_day_high", "prev_day_low", "prev_day_close_calc"], errors="ignore"
    )
    daily = out.groupby("date_only").agg(
        _dh=("high", "max"), _dl=("low", "min"), _dc=("close", "last")
    )
    daily["prev_day_high"] = daily["_dh"].shift(1)
    daily["prev_day_low"] = daily["_dl"].shift(1)
    daily["prev_day_close_calc"] = daily["_dc"].shift(1)
    levels = daily[["prev_day_high", "prev_day_low", "prev_day_close_calc"]]
    return out.merge(levels, left_on="date_only", right_index=True, how="left")


def _scan_conf_tier_c_ticker_slot(
    ticker: str,
    slot: pd.Timestamp,
    market_ctx: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[dict]:
    df = candidate_scan._load_live_5m(ticker)
    if df is None or df.empty or "date" not in df.columns:
        return []
    slot_ts = _normalise_ts(slot).floor("min")

    prepared_full = ov._tier123_prepare_5m(df)              # VWAP-corrected, full history
    prepared_full = _prev_day_levels(prepared_full)
    # causal slice: this session, up to and including the slot bar
    day_start = slot_ts.normalize()
    day_df = prepared_full.loc[
        (prepared_full["date"] >= day_start) & (prepared_full["date"] <= slot_ts)
    ].copy().reset_index(drop=True)
    # NB: research _scan_ticker uses `len(full_day) < 15` to skip low-data DAYS.
    # Here day_df is bars-up-to-slot, so an early slot legitimately has few bars;
    # the real per-setup minimums are the index guards below (i>=4 tier123, i>=10
    # L_RS). A small floor only avoids degenerate indicator windows.
    if len(day_df) < 5:
        return []
    idxs = np.where(day_df["date"].dt.floor("min").eq(slot_ts))[0]
    if len(idxs) == 0:
        return []
    i = int(idxs[-1])
    if i < 4:
        return []

    row = day_df.iloc[i]
    prev = day_df.iloc[i - 1]
    ts = _normalise_ts(row["date"])
    minute = ts.hour * 60 + ts.minute
    if minute < 570 or minute > 900:
        return []

    close = _safe_float(row.get("close"))
    open_px = _safe_float(row.get("open"))
    high = _safe_float(row.get("high"))
    low = _safe_float(row.get("low"))
    atr = _safe_float(row.get("ATR"))
    vwap = _safe_float(row.get("VWAP"))
    ema20 = _safe_float(row.get("EMA_20"))
    ema50 = _safe_float(row.get("EMA_50"))
    adx = _safe_float(row.get("ADX"))
    rsi = _safe_float(row.get("RSI"))
    vol_ratio = _safe_float(row.get("vol_ratio"))
    close_loc = _safe_float(row.get("close_loc"))
    vwap_dist = _safe_float(row.get("vwap_dist_atr"))
    rng = _safe_float(row.get("range"))

    day_open = _safe_float(day_df.iloc[0].get("open"))
    market_ret, regime = ov._tier123_bar_context(market_ctx or {}, slot_ts.strftime("%Y-%m-%d"), ts)
    stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
    rs_pct = stock_ret - market_ret
    above_vwap = np.isfinite(vwap) and close > vwap
    long_struct = close > open_px and close_loc >= 0.55

    or_high, or_low = candidate_scan.v2._opening_range(day_df)
    prev_day_high = _safe_float(row.get("prev_day_high"))

    # ---- replay per-day STATE over bars [4 .. i-1] (research _scan_ticker) ----
    prior_broke_high = prior_broke_pdh = False
    washout_below = reclaimed_vwap = False
    H = day_df["high"].to_numpy(dtype=float)
    L = day_df["low"].to_numpy(dtype=float)
    C = day_df["close"].to_numpy(dtype=float)
    Vw = pd.to_numeric(day_df.get("VWAP"), errors="coerce").to_numpy(dtype=float)
    Vd = pd.to_numeric(day_df.get("vwap_dist_atr"), errors="coerce").to_numpy(dtype=float)
    for j in range(4, i):
        above_j = np.isfinite(Vw[j]) and C[j] > Vw[j]
        if np.isfinite(or_high) and H[j] > or_high:
            prior_broke_high = True
        if np.isfinite(prev_day_high) and H[j] > prev_day_high:
            prior_broke_pdh = True
        if np.isfinite(Vd[j]) and Vd[j] <= -0.50:
            washout_below = True
        if washout_below and above_j:
            reclaimed_vwap = True

    rows: List[dict] = []

    # ---- L_RS_LEADER_VWAP_HOLD (new_setups_scan_v11 L191-202; loop starts i>=10) ----
    if i >= 10 and np.isfinite(atr) and atr > 0 and np.isfinite(vwap):
        ema20_slope = (ema20 - _safe_float(day_df["EMA_20"].iloc[i - 3])) if (i >= 3 and np.isfinite(_safe_float(day_df["EMA_20"].iloc[i - 3]))) else np.nan
        prev_high_1 = _safe_float(day_df["high"].iloc[i - 1])
        if (585 <= minute <= 840
                and rs_pct >= 0.75 and stock_ret >= 0.30
                and np.isfinite(ema20) and np.isfinite(ema50) and close > ema20 and ema20 >= ema50
                and np.isfinite(ema20_slope) and ema20_slope > 0
                and low <= vwap + 0.30 * atr and close > vwap
                and close > open_px and close_loc >= 0.60 and close > prev_high_1
                and np.isfinite(vol_ratio) and vol_ratio >= 1.3
                and np.isfinite(adx) and adx >= 20 and np.isfinite(rsi) and 50 <= rsi <= 72
                and regime in {"BULL", "TREND", "NEUTRAL"}):
            rows.append(ov._tier123_candidate(
                str(ticker).upper(), "L_RS_LEADER_VWAP_HOLD", "LONG", row,
                rs_pct, market_ret, regime, "rs_leader_vwap_test_hold_continuation"))

    # ---- tier123 setups: strict liquidity/quality bar gate (research L423-476) ----
    tier123_bar_ok = (
        np.isfinite(atr) and atr > 0
        and np.isfinite(vol_ratio) and vol_ratio >= 1.0
        and close >= 25 and _safe_float(row.get("day_value_so_far_rs"), 0.0) >= 20_000_000
        and not (np.isfinite(rng) and rng > 3.5 * atr)
    )
    if tier123_bar_ok:
        # E_ORB_RETEST_HOLD_LONG (research L479-480)
        if (minute <= 630 and prior_broke_high and long_struct and above_vwap
                and low <= or_high + 0.45 * atr and close > or_high
                and rs_pct >= 0.0 and regime != "BEAR" and abs(vwap_dist) <= 2.5):
            rows.append(ov._tier123_candidate(
                str(ticker).upper(), "E_ORB_RETEST_HOLD_LONG", "LONG", row,
                rs_pct, market_ret, regime, "orb_break_retest_hold_long"))
        # V_RECLAIM_PULLBACK_LONG (research L489-490)
        if (minute <= 690 and reclaimed_vwap and long_struct
                and low >= vwap - 0.35 * atr and low <= vwap + 0.45 * atr
                and close > max(_safe_float(prev.get("high")), vwap)
                and vol_ratio >= 1.25 and rs_pct >= 0.0 and regime != "BEAR" and vwap_dist <= 1.8):
            rows.append(ov._tier123_candidate(
                str(ticker).upper(), "V_RECLAIM_PULLBACK_LONG", "LONG", row,
                rs_pct, market_ret, regime, "vwap_reclaim_first_pullback_hold_long"))
        # P_PDH_BREAK_RETEST_LONG (research L551-552)
        if (np.isfinite(prev_day_high) and prior_broke_pdh and long_struct
                and low <= prev_day_high + 0.35 * atr and close > prev_day_high
                and above_vwap and rs_pct >= 0.0 and regime != "BEAR"):
            rows.append(ov._tier123_candidate(
                str(ticker).upper(), "P_PDH_BREAK_RETEST_LONG", "LONG", row,
                rs_pct, market_ret, regime, "previous_day_high_break_retest_long"))

    for r in rows:
        r["candidate_family"] = "CONF_TIER_C_LIVE"
        r["selection_mode"] = "final_setup_conf_tier_c"
        r["scan_session"] = "conf_tier_c_live"
    return rows


def scan_conf_tier_c_live_slot(
    slot: pd.Timestamp,
    tickers: List[str],
    *,
    market_ctx: Optional[Dict[str, Dict[str, Any]]] = None,
    max_workers: int = 8,
) -> pd.DataFrame:
    """Scan all tickers for the 4 Tier-C conf setups at this slot; return a
    candidate DataFrame (same schema as the native scanner / tier123 overlay)."""
    if not tickers:
        return pd.DataFrame()
    slot_ts = _normalise_ts(slot).floor("min")
    rows: List[dict] = []

    def _one(tk: str) -> List[dict]:
        try:
            return _scan_conf_tier_c_ticker_slot(str(tk).upper(), slot_ts, market_ctx)
        except Exception:
            return []

    workers = max(1, int(max_workers))
    if workers == 1:
        for tk in tickers:
            rows.extend(_one(tk))
    else:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="v7-conf-tierc") as ex:
            for res in ex.map(_one, [str(t).upper() for t in tickers]):
                rows.extend(res)

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    if {"candidate_id", "signal_time_ist", "ticker"}.issubset(out.columns):
        out = candidate_scan._dedupe_candidate_frame(out)
    return out.reset_index(drop=True)
