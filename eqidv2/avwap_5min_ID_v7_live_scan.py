"""
avwap_5min_ID_v7_live_scan.py — live scan adapter for the ID 5min v7 persistent
scanner. Replaces the v15_new shard fan-out with a direct, parity-oriented
wrapper around the avwap_5min_ID_v2 setup detection.

Pipeline per slot:
  1. Hybrid data load per ticker:
       history  = stocks_indicators_5min_eq_live2/<TICKER>_stocks_indicators_5min.parquet
       live     = stocks_indicators_5min_eq_live/<TICKER>_stocks_indicators_5min.parquet
       merged   = upsert(live into history) by 'date', sorted, dedup-last
     v2 stays unchanged; only this adapter sees the merged view.
  2. No-look-ahead: trim to bars with date <= slot_ist.
  3. v2._prepare_5m computes derived features (VWAP, vol_ratio, vwap_dist_atr,
     BB bands, prev_day_high/low, opening range, etc.) on the merged frame.
  4. v2._scan_day runs the setup detection for today's bars.
  5. Slot filter: keep only candidates whose signal_ts == slot_ist (the bar
     that just closed). Exclude setups in v7's EXCLUDED_SETUPS.
  6. SL/TGT remap: v6.SETUP_EXIT_RULES (per-setup [SL%, TGT%]) replaces v2's
     fixed defaults at signal-emit time. This is what v7 backtester does
     before mining, so live signals match v7 PnL accounting.

Public surface:
  scan_slot(slot_ist, tickers, market_ctx) -> (short_df, long_df)
  build_market_context_once() -> market_ctx
  scan_ticker_live(ticker, slot_ist, market_ctx) -> list[Candidate]
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

import avwap_5min_ID_v2_backtesting as v2
import avwap_5min_ID_v6_backtesting as v6

# v2 doesn't expose a top-level IST symbol; we use a local string-based tz that
# pandas resolves the same way (Asia/Kolkata, +05:30 fixed offset for our use).
IST_TZ = "Asia/Kolkata"


HIST_5M_DIR = Path(
    os.getenv(
        "EQIDV2_ID5MIN_V7_HIST_5M_DIR",
        r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2",
    )
)
LIVE_5M_DIR = Path(
    os.getenv(
        "EQIDV2_ID5MIN_V7_LIVE_5M_DIR",
        r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live",
    )
)

# Matches avwap_5min_ID_v7_backtesting.EXCLUDED_SETUPS — v7 drops these
# unconditionally before mining.
EXCLUDED_SETUPS = {
    "B_HUGE_PULLBACK_HOLD_BREAK",
    "S_LIQUIDITY_SWEEP_REVERSAL",
}

# Parallel scan: number of worker processes for scan_slot. 0/1 = sequential.
DEFAULT_SCAN_WORKERS = max(1, int(os.getenv("EQIDV2_ID5MIN_V7_SCAN_WORKERS", "8")))
# Emission window: at each scan, emit candidates whose signal bar is within
# this many minutes before the latest available bar (and thus has a successor
# bar for entry). A window (not a single bar) makes emission robust to a
# slightly-late 5-min fetcher — if the current slot's bar isn't written yet,
# the previous bar still fires, and when the late bar arrives next slot it
# fires too. CSV dedup (signal_id) prevents double-writing.
EMIT_WINDOW_MIN = int(os.getenv("EQIDV2_ID5MIN_V7_EMIT_WINDOW_MIN", "15"))


def _read_one(fp: Path) -> Optional[pd.DataFrame]:
    if not fp.exists():
        return None
    try:
        df = v2._read_ohlcv(fp)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    return df


def _merge_history_and_live(ticker: str) -> Optional[pd.DataFrame]:
    """Read 5-min bars from the LIVE store (_eq_live) ONLY.

    2026-05-20: switched from a hist+live merge to live-only because:
      * _eq_live2 (history) is not refreshed during the live session, so it
        had no today bars and a stale NIFTY market context.
      * the merge left today's VWAP NaN: history carries a (rolling) VWAP
        column, so the merged frame's VWAP was non-all-NaN, and
        v2._prepare_5m only recomputes VWAP when it is absent or all-NaN.
    Reading _eq_live only means the VWAP column is ABSENT (the minimal live
    fetcher does not emit it), so v2._prepare_5m recomputes a proper session
    VWAP for every bar including today. _eq_live is a rolling ~10-day window
    which is enough history for v2's intraday/prev-day rolling features.
    """
    fp_live = LIVE_5M_DIR / f"{ticker}_stocks_indicators_5min.parquet"
    df_live = _read_one(fp_live)
    if df_live is None or "date" not in df_live.columns:
        return None

    df_live = df_live.copy()
    df_live["date"] = pd.to_datetime(df_live["date"], errors="coerce")
    df_live = df_live.dropna(subset=["date"])
    if df_live.empty:
        return None

    df_live = (
        df_live.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )
    return df_live


def _ensure_ist_ts(ts: Any) -> pd.Timestamp:
    t = pd.Timestamp(ts)
    if t.tz is None:
        t = t.tz_localize(IST_TZ)
    else:
        t = t.tz_convert(IST_TZ)
    return t


def scan_ticker_live(
    ticker: str,
    slot_ist: Any,
    market_ctx: Dict[str, Dict[str, Any]],
) -> List["v2.Candidate"]:
    df = _merge_history_and_live(ticker)
    if df is None or df.empty:
        return []

    slot_ts = _ensure_ist_ts(slot_ist)

    df = df[df["date"] <= slot_ts].copy()
    if df.empty:
        return []

    if "date_only" not in df.columns:
        df["date_only"] = df["date"].dt.tz_convert(IST_TZ).dt.date

    # NOTE 2026-05-19 (parity diagnosis): do NOT drop the parquet's pre-computed
    # VWAP column. It is a rolling/cumulative VWAP, NOT a session VWAP — but
    # v2/v5/v7 backtester deliberately uses this rolling VWAP everywhere
    # (`above_vwap`, `vwap_dist_atr`). Force-recomputing session VWAP here
    # makes vwap_dist_atr near-zero across the day, killing every VWAP-gated
    # setup (C_OR_BREAKOUT, E_VWAP_BAND_FADE, etc.) — parity drops to 0%.
    # The parquet VWAP is the "ground truth" for setup parity even though it's
    # numerically unusual.

    try:
        prepared = v2._prepare_5m(df)
    except Exception:
        return []

    today = slot_ts.date()
    day_df = prepared[prepared["date_only"] == today].reset_index(drop=True)
    if day_df.empty:
        return []

    try:
        candidates = v2._scan_day(day_df, ticker, market_ctx)
    except Exception:
        return []

    # PARITY FIX (2026-05-19): v2._scan_day needs the bar AFTER the signal
    # bar (to set entry_ts = next bar's timestamp, entry_px = next bar's open).
    # In live, at slot T the just-closed bar T has no successor yet, so its
    # candidates get silently dropped. The signal we *can* emit at slot T is
    # for the bar (T - 5min): its successor T is now observable and serves as
    # the entry bar. This matches v2/v5/v7 backtester semantics exactly and is
    # the inherent live-vs-backtest 5-min lag (signal bar closes 5 min before
    # the signal is emitted with a known entry price).
    # Emit candidates for any bar within the last EMIT_WINDOW_MIN that has a
    # successor bar present (signal_ts < latest_bar). v2._scan_day only emits a
    # candidate when its next bar exists, so every returned candidate already
    # has a resolvable entry. Using a window (not a single == slot-5min bar)
    # makes this robust to a late fetcher: if the current slot's bar isn't
    # written yet, the prior bar still fires; CSV dedup avoids re-writes.
    latest_bar = pd.Timestamp(day_df["date"].max())
    if latest_bar.tz is None:
        latest_bar = latest_bar.tz_localize(IST_TZ)
    window_start = latest_bar - pd.Timedelta(minutes=EMIT_WINDOW_MIN)
    out: List["v2.Candidate"] = []
    for c in candidates:
        if str(c.setup) in EXCLUDED_SETUPS:
            continue
        c_ts = pd.Timestamp(c.signal_ts)
        if c_ts.tz is None:
            c_ts = c_ts.tz_localize(IST_TZ)
        if window_start <= c_ts < latest_bar:
            out.append(c)
    return out


def _apply_v6_exits(c: "v2.Candidate") -> Tuple[float, float]:
    rule = v6.SETUP_EXIT_RULES.get(str(c.setup))
    entry = float(c.entry_px) if float(c.entry_px) > 0 else float(c.signal_close)
    if rule is None or entry <= 0:
        return float(c.sl_px), float(c.target_px)
    sl_pct, tgt_pct = rule
    side = str(c.side).upper()
    if side == "LONG":
        sl_price = entry * (1.0 - sl_pct / 100.0)
        tgt_price = entry * (1.0 + tgt_pct / 100.0)
    else:  # SHORT
        sl_price = entry * (1.0 + sl_pct / 100.0)
        tgt_price = entry * (1.0 - tgt_pct / 100.0)
    return float(sl_price), float(tgt_price)


def _finite_or_none(x: Any) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if np.isfinite(v) else None


def candidates_to_dataframe(candidates: Iterable["v2.Candidate"]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for c in candidates:
        sl_price, tgt_price = _apply_v6_exits(c)
        bar_ts = pd.Timestamp(c.signal_ts)
        if bar_ts.tz is None:
            bar_ts = bar_ts.tz_localize(IST_TZ)
        # Format with colon in offset (e.g. +05:30 not +0530) to match the
        # backtester's signal_ts string format. Python's %z drops the colon
        # by default; the slice trick inserts it.
        offset = bar_ts.strftime("%z")
        bar_time_str = f"{bar_ts.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"
        diag = {
            "atr_pct": _finite_or_none(c.atr_pct),
            "close": _finite_or_none(c.signal_close),
            "vol_ratio": _finite_or_none(c.vol_ratio),
            "vwap_dist_atr": _finite_or_none(c.vwap_dist_atr),
            "rs_pct": _finite_or_none(c.rs_pct),
            "market_ret_pct": _finite_or_none(c.market_ret_pct),
            "body_pct": _finite_or_none(c.body_pct),
            "close_loc": _finite_or_none(c.close_loc),
            "day_value_so_far_rs": _finite_or_none(c.day_value_so_far_rs),
            "regime": str(c.regime) if c.regime is not None else "",
            "reason": str(c.reason) if c.reason is not None else "",
        }
        entry_price = float(c.entry_px) if float(c.entry_px) > 0 else float(c.signal_close)
        rows.append(
            {
                "ticker": str(c.ticker).upper(),
                "side": str(c.side).upper(),
                "setup": str(c.setup),
                "bar_time_ist": bar_time_str,
                "entry_price": entry_price,
                "sl_price": sl_price,
                "target_price": tgt_price,
                "score": float(c.quality_score) if np.isfinite(c.quality_score) else 0.0,
                "diagnostics_json": json.dumps(diag, default=str),
            }
        )
    return pd.DataFrame(rows)


_MARKET_CTX_CACHE: Dict[str, Dict[str, Any]] = {}


def build_market_context_once() -> Dict[str, Dict[str, Any]]:
    global _MARKET_CTX_CACHE
    if not _MARKET_CTX_CACHE:
        # 2026-05-20: point v2 at the LIVE store so _load_market_context reads
        # the NIFTY context from today's live data (NIFTYBEES is present in
        # _eq_live). v2.DATA_ROOT_5M defaults to _eq_live2 (stale history with
        # no today bars). Apply v5's overrides (noisy advanced shorts on,
        # mined-filter off) to match the backtester's setup universe.
        v2.DATA_ROOT_5M = LIVE_5M_DIR
        v2._init_worker({
            "ENABLE_NOISY_ADVANCED_SHORTS": True,
            "ENABLE_NATIVE_V2_MINED_FILTER": False,
        })
        _MARKET_CTX_CACHE = v2._load_market_context()
    return _MARKET_CTX_CACHE


def invalidate_market_context() -> None:
    global _MARKET_CTX_CACHE
    _MARKET_CTX_CACHE = {}


# --- Parallel scan worker plumbing (ProcessPool) ---------------------------
# Each worker process builds its own market context once (in the initializer)
# so the large NIFTY context dict is never pickled per task. Workers read the
# live store directly; v2 is pointed at LIVE_5M_DIR with v5's overrides.
_WORKER_MARKET_CTX: Optional[Dict[str, Dict[str, Any]]] = None


def _worker_init() -> None:
    global _WORKER_MARKET_CTX
    v2.DATA_ROOT_5M = LIVE_5M_DIR
    v2._init_worker({
        "ENABLE_NOISY_ADVANCED_SHORTS": True,
        "ENABLE_NATIVE_V2_MINED_FILTER": False,
    })
    try:
        _WORKER_MARKET_CTX = v2._load_market_context()
    except Exception:
        _WORKER_MARKET_CTX = {}


def _worker_scan(payload: Tuple[str, str]) -> List[Dict[str, Any]]:
    ticker, slot_iso = payload
    global _WORKER_MARKET_CTX
    if _WORKER_MARKET_CTX is None:
        _worker_init()
    try:
        cands = scan_ticker_live(ticker, pd.Timestamp(slot_iso), _WORKER_MARKET_CTX or {})
    except Exception:
        return []
    if not cands:
        return []
    return candidates_to_dataframe(cands).to_dict("records")


def scan_slot(
    slot_ist: Any,
    tickers: Iterable[str],
    market_ctx: Optional[Dict[str, Dict[str, Any]]] = None,
    max_workers: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    slot_ts = _ensure_ist_ts(slot_ist)
    tickers = [str(t).strip().upper() for t in tickers if str(t).strip()]
    workers = int(max_workers if max_workers is not None else DEFAULT_SCAN_WORKERS)

    rows: List[Dict[str, Any]] = []

    if workers <= 1:
        # Sequential fallback (no process spawn).
        if market_ctx is None:
            market_ctx = build_market_context_once()
        for tkr in tickers:
            try:
                c_list = scan_ticker_live(tkr, slot_ts, market_ctx)
            except Exception:
                c_list = []
            if c_list:
                rows.extend(candidates_to_dataframe(c_list).to_dict("records"))
    else:
        slot_iso = slot_ts.isoformat()
        payloads = [(t, slot_iso) for t in tickers]
        with ProcessPoolExecutor(max_workers=workers, initializer=_worker_init) as ex:
            for res in ex.map(_worker_scan, payloads, chunksize=24):
                if res:
                    rows.extend(res)

    if not rows:
        return pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(rows)
    short_df = df.loc[df["side"].str.upper().eq("SHORT")].reset_index(drop=True)
    long_df = df.loc[df["side"].str.upper().eq("LONG")].reset_index(drop=True)
    return short_df, long_df
