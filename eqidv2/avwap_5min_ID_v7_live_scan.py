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
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
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
# Keep the historical batching default for parity/stability, but make it
# independently tunable so live-like p95 benchmarks can select a smaller
# straggler-resistant chunk without changing worker count.
DEFAULT_SCAN_CHUNKSIZE = max(1, int(os.getenv("EQIDV2_ID5MIN_V7_SCAN_CHUNKSIZE", "24")))
SCAN_CACHE_ENABLED = os.getenv("EQIDV2_ID5MIN_V7_SCAN_CACHE_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
# Per process: 128 comfortably covers a normal 24-worker shard (~54 tickers)
# without allowing task migration to multiply the whole universe into every
# worker's memory.
SCAN_CACHE_MAX_TICKERS = max(
    1,
    int(os.getenv("EQIDV2_ID5MIN_V7_SCAN_CACHE_MAX_TICKERS", "128")),
)
ENTRY_WINDOW_START = pd.Timestamp(os.getenv("EQIDV2_ID5MIN_V7_ENTRY_WINDOW_START", "09:30")).time()
ENTRY_WINDOW_END = pd.Timestamp(os.getenv("EQIDV2_ID5MIN_V7_ENTRY_WINDOW_END", "14:00")).time()
ENTRY_SIGNAL_TO_ENTRY_LAG_MIN = int(os.getenv("EQIDV2_ID5MIN_V7_ENTRY_LAG_MIN", "1"))
# Emission window: at each scan, emit candidates whose signal bar is within
# this many minutes before the latest available bar (and thus has a successor
# bar for entry). A window (not a single bar) makes emission robust to a
# slightly-late 5-min fetcher — if the current slot's bar isn't written yet,
# the previous bar still fires, and when the late bar arrives next slot it
# fires too. CSV dedup (signal_id) prevents double-writing.
EMIT_WINDOW_MIN = int(os.getenv("EQIDV2_ID5MIN_V7_EMIT_WINDOW_MIN", "15"))


@dataclass
class _TickerFrameCache:
    """Process-local, exact-input cache for one ticker.

    A cache entry is reusable only while the live Parquet fingerprint is
    unchanged.  Prepared data has an additional cutoff key, so caching never
    substitutes an incrementally approximated indicator calculation for
    v2._prepare_5m.
    """

    fingerprint: Tuple[int, int, int]
    frame: pd.DataFrame
    prepared_key: Optional[Tuple[Any, ...]] = None
    prepared: Optional[pd.DataFrame] = None


# This dictionary is intentionally process-local.  ProcessPool workers do not
# share mutable pandas objects, which avoids locks and cross-process copies.
_LOCAL_FRAME_CACHE: Dict[str, _TickerFrameCache] = {}
_LAST_SCAN_TELEMETRY: Dict[str, Any] = {}


def _new_ticker_telemetry() -> Dict[str, Any]:
    return {
        "raw_cache_hits": 0,
        "raw_cache_misses": 0,
        "prepared_cache_hits": 0,
        "prepared_cache_misses": 0,
        "unchanged_frame_hits": 0,
        "file_read_seconds": 0.0,
        "prepare_seconds": 0.0,
        "strategy_seconds": 0.0,
        "unstable_file_reads": 0,
        "ticker_errors": 0,
        "ticker_elapsed_seconds": 0.0,
    }


def _bump_telemetry(
    telemetry: Optional[Dict[str, Any]],
    key: str,
    amount: float | int = 1,
) -> None:
    if telemetry is not None:
        telemetry[key] = telemetry.get(key, 0) + amount


def _file_fingerprint(fp: Path) -> Optional[Tuple[int, int, int]]:
    """Return a strong-enough local version key without opening the Parquet."""
    try:
        stat = fp.stat()
    except OSError:
        return None
    return (int(stat.st_mtime_ns), int(stat.st_ctime_ns), int(stat.st_size))


def _remember_frame(
    ticker: str,
    fingerprint: Tuple[int, int, int],
    frame: pd.DataFrame,
) -> _TickerFrameCache:
    if ticker not in _LOCAL_FRAME_CACHE and len(_LOCAL_FRAME_CACHE) >= SCAN_CACHE_MAX_TICKERS:
        # Dict insertion order gives a tiny, dependency-free FIFO bound.  A
        # normal worker owns only ~universe/workers entries, so eviction is a
        # safeguard for sequential/research use rather than the live path.
        _LOCAL_FRAME_CACHE.pop(next(iter(_LOCAL_FRAME_CACHE)))
    entry = _TickerFrameCache(fingerprint=fingerprint, frame=frame)
    _LOCAL_FRAME_CACHE[ticker] = entry
    return entry


def _fmt_ist(ts: Any) -> str:
    out = _ensure_ist_ts(ts)
    offset = out.strftime("%z")
    return f"{out.strftime('%Y-%m-%d %H:%M:%S')}{offset[:3]}:{offset[3:]}"


def _candidate_entry_ts(c: "v2.Candidate") -> pd.Timestamp:
    raw = getattr(c, "entry_ts", None)
    if raw is None:
        raw = pd.Timestamp(c.signal_ts) + pd.Timedelta(minutes=ENTRY_SIGNAL_TO_ENTRY_LAG_MIN)
    return _ensure_ist_ts(raw).floor("min")


def _entry_window_ok(entry_ts: Any) -> bool:
    t = _ensure_ist_ts(entry_ts).time()
    return ENTRY_WINDOW_START <= t <= ENTRY_WINDOW_END


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


def _load_normalized_live(
    ticker: str,
    telemetry: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[pd.DataFrame], Optional[_TickerFrameCache]]:
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
    fingerprint_before = _file_fingerprint(fp_live)
    cached = _LOCAL_FRAME_CACHE.get(ticker) if SCAN_CACHE_ENABLED else None
    if (
        fingerprint_before is not None
        and cached is not None
        and cached.fingerprint == fingerprint_before
    ):
        _bump_telemetry(telemetry, "raw_cache_hits")
        return cached.frame, cached

    _bump_telemetry(telemetry, "raw_cache_misses")
    read_started = time.perf_counter()
    df_live = _read_one(fp_live)
    _bump_telemetry(
        telemetry,
        "file_read_seconds",
        time.perf_counter() - read_started,
    )
    if df_live is None or "date" not in df_live.columns:
        return None, None

    df_live = df_live.copy()
    df_live["date"] = pd.to_datetime(df_live["date"], errors="coerce")
    df_live = df_live.dropna(subset=["date"])
    if df_live.empty:
        return None, None

    df_live = (
        df_live.sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )
    fingerprint_after = _file_fingerprint(fp_live)
    if (
        SCAN_CACHE_ENABLED
        and fingerprint_before is not None
        and fingerprint_before == fingerprint_after
    ):
        # Some feed paths rewrite a file even when no new bar was available.
        # Preserve the prepared cache only after an exact DataFrame equality
        # check; shape-only/latest-row shortcuts could silently miss a revised
        # historical candle.
        if cached is not None and cached.frame.equals(df_live):
            cached.fingerprint = fingerprint_after
            _bump_telemetry(telemetry, "unchanged_frame_hits")
            return cached.frame, cached
        return df_live, _remember_frame(ticker, fingerprint_after, df_live)

    # Use the successfully decoded frame for this call, but never cache a file
    # version that changed while it was being read.
    if fingerprint_before != fingerprint_after:
        _bump_telemetry(telemetry, "unstable_file_reads")
    _LOCAL_FRAME_CACHE.pop(ticker, None)
    return df_live, None


def _merge_history_and_live(ticker: str) -> Optional[pd.DataFrame]:
    """Compatibility wrapper returning the normalized live-only frame."""
    frame, _ = _load_normalized_live(ticker)
    return frame


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
    *,
    telemetry: Optional[Dict[str, Any]] = None,
) -> List["v2.Candidate"]:
    ticker_started = time.perf_counter()
    df, cache_entry = _load_normalized_live(ticker, telemetry)
    if df is None or df.empty:
        _bump_telemetry(
            telemetry,
            "ticker_elapsed_seconds",
            time.perf_counter() - ticker_started,
        )
        return []

    slot_ts = _ensure_ist_ts(slot_ist)

    df = df[df["date"] <= slot_ts].copy()
    if df.empty:
        _bump_telemetry(
            telemetry,
            "ticker_elapsed_seconds",
            time.perf_counter() - ticker_started,
        )
        return []

    # Trim to 3 calendar days before _prepare_5m: reduces compute from ~10 days to ~2-3
    # trading days (~9x speedup). 3 days safely covers the previous trading day on Mondays.
    _trim_start = (slot_ts - pd.Timedelta(days=3)).normalize()
    df = df[df["date"] >= _trim_start].copy()
    if df.empty:
        _bump_telemetry(
            telemetry,
            "ticker_elapsed_seconds",
            time.perf_counter() - ticker_started,
        )
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

    # Prepared frames are reused only for the exact same stable Parquet
    # fingerprint and exact same input cutoff.  No indicator is updated
    # approximately: every cache miss still calls the parity implementation
    # v2._prepare_5m over the complete trimmed input.
    prepared_key: Optional[Tuple[Any, ...]] = None
    if cache_entry is not None:
        prepared_key = (
            int(_trim_start.value),
            int(pd.Timestamp(df["date"].iloc[-1]).value),
            int(len(df)),
        )

    if (
        prepared_key is not None
        and cache_entry is not None
        and cache_entry.prepared_key == prepared_key
        and cache_entry.prepared is not None
    ):
        prepared = cache_entry.prepared
        _bump_telemetry(telemetry, "prepared_cache_hits")
    else:
        _bump_telemetry(telemetry, "prepared_cache_misses")
        prepare_started = time.perf_counter()
        try:
            prepared = v2._prepare_5m(df)
        except Exception:
            _bump_telemetry(telemetry, "ticker_errors")
            _bump_telemetry(
                telemetry,
                "prepare_seconds",
                time.perf_counter() - prepare_started,
            )
            _bump_telemetry(
                telemetry,
                "ticker_elapsed_seconds",
                time.perf_counter() - ticker_started,
            )
            return []
        _bump_telemetry(
            telemetry,
            "prepare_seconds",
            time.perf_counter() - prepare_started,
        )
        if cache_entry is not None and prepared_key is not None:
            cache_entry.prepared_key = prepared_key
            cache_entry.prepared = prepared

    today = slot_ts.date()
    day_df = prepared[prepared["date_only"] == today].reset_index(drop=True)
    if day_df.empty:
        _bump_telemetry(
            telemetry,
            "ticker_elapsed_seconds",
            time.perf_counter() - ticker_started,
        )
        return []

    strategy_started = time.perf_counter()
    try:
        candidates = v2._scan_day(day_df, ticker, market_ctx)
    except Exception:
        _bump_telemetry(telemetry, "ticker_errors")
        _bump_telemetry(
            telemetry,
            "strategy_seconds",
            time.perf_counter() - strategy_started,
        )
        _bump_telemetry(
            telemetry,
            "ticker_elapsed_seconds",
            time.perf_counter() - ticker_started,
        )
        return []
    _bump_telemetry(
        telemetry,
        "strategy_seconds",
        time.perf_counter() - strategy_started,
    )

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
        if not _entry_window_ok(_candidate_entry_ts(c)):
            continue
        if window_start <= c_ts < latest_bar:
            out.append(c)
    _bump_telemetry(
        telemetry,
        "ticker_elapsed_seconds",
        time.perf_counter() - ticker_started,
    )
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
        entry_ts = _candidate_entry_ts(c)
        bar_time_str = _fmt_ist(bar_ts)
        entry_time_str = _fmt_ist(entry_ts)
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
                "entry_time_ist": entry_time_str,
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
_WORKER_LAST_SLOT_ISO: Optional[str] = None

# Persistent ProcessPool — reused across slots to avoid Windows spawn overhead (~5-15s/slot).
_SCAN_POOL: Optional[ProcessPoolExecutor] = None
_SCAN_POOL_WORKERS: int = 0
_SCAN_POOL_DAY: Optional[str] = None


def shutdown_scan_pool(*, wait: bool = True) -> None:
    """Shut down the persistent pool (mainly for controlled restarts/tests)."""
    global _SCAN_POOL, _SCAN_POOL_WORKERS, _SCAN_POOL_DAY
    pool = _SCAN_POOL
    _SCAN_POOL = None
    _SCAN_POOL_WORKERS = 0
    _SCAN_POOL_DAY = None
    if pool is not None:
        try:
            pool.shutdown(wait=wait, cancel_futures=True)
        except Exception:
            pass


def reset_scan_caches(*, shutdown_pool_workers: bool = True) -> None:
    """Clear exact-input caches without changing any strategy configuration.

    Worker caches live inside their processes.  The default pool shutdown is
    therefore required to guarantee they are cleared as well; a later scan or
    prewarm call creates a fresh pool.
    """
    global _LAST_SCAN_TELEMETRY
    _LOCAL_FRAME_CACHE.clear()
    invalidate_market_context()
    _LAST_SCAN_TELEMETRY = {}
    if shutdown_pool_workers:
        shutdown_scan_pool(wait=True)


def get_last_scan_telemetry() -> Dict[str, Any]:
    """Return a copy of the most recent parent-process slot telemetry."""
    return dict(_LAST_SCAN_TELEMETRY)


def _replace_scan_pool(workers: int, today_str: str) -> None:
    global _SCAN_POOL, _SCAN_POOL_WORKERS, _SCAN_POOL_DAY
    shutdown_scan_pool(wait=False)
    _SCAN_POOL = ProcessPoolExecutor(max_workers=workers, initializer=_worker_init)
    _SCAN_POOL_WORKERS = workers
    _SCAN_POOL_DAY = today_str


def _get_scan_pool(workers: int, today_str: str) -> ProcessPoolExecutor:
    global _SCAN_POOL, _SCAN_POOL_WORKERS, _SCAN_POOL_DAY
    if _SCAN_POOL is None or _SCAN_POOL_WORKERS != workers or _SCAN_POOL_DAY != today_str:
        _replace_scan_pool(workers, today_str)
    return _SCAN_POOL


def _worker_init() -> None:
    global _WORKER_MARKET_CTX, _WORKER_LAST_SLOT_ISO
    v2.DATA_ROOT_5M = LIVE_5M_DIR
    v2._init_worker({
        "ENABLE_NOISY_ADVANCED_SHORTS": True,
        "ENABLE_NATIVE_V2_MINED_FILTER": False,
    })
    # Defer the market file read until the worker knows which slot it is
    # preparing.  This avoids the old initializer + first-task double read.
    _WORKER_MARKET_CTX = {}
    _WORKER_LAST_SLOT_ISO = None
    _LOCAL_FRAME_CACHE.clear()


def _refresh_worker_market_context(slot_iso: str) -> None:
    global _WORKER_MARKET_CTX, _WORKER_LAST_SLOT_ISO
    if _WORKER_MARKET_CTX is None:
        _worker_init()
    if _WORKER_LAST_SLOT_ISO == slot_iso:
        return
    try:
        _WORKER_MARKET_CTX = v2._load_market_context()
    except Exception:
        # Preserve the last valid context on a transient read error.
        if _WORKER_MARKET_CTX is None:
            _WORKER_MARKET_CTX = {}
    _WORKER_LAST_SLOT_ISO = slot_iso


def _worker_prewarm(slot_iso: str) -> int:
    """Initialize one pool worker and return its PID for prewarm telemetry."""
    _refresh_worker_market_context(slot_iso)
    return os.getpid()


def prewarm_scan_pool(
    slot_ist: Any,
    max_workers: Optional[int] = None,
) -> Dict[str, Any]:
    """Eagerly start scanner processes before the first decision-critical slot."""
    slot_ts = _ensure_ist_ts(slot_ist)
    workers = int(max_workers if max_workers is not None else DEFAULT_SCAN_WORKERS)
    started = time.perf_counter()
    if workers <= 1:
        build_market_context_once()
        return {
            "workers_requested": workers,
            "worker_pids_seen": 1,
            "seconds": time.perf_counter() - started,
        }

    pool = _get_scan_pool(workers, slot_ts.date().isoformat())
    # CPython starts max_workers eagerly when work is first submitted.  More
    # than one tiny task per worker lets the returned PID count expose whether
    # the host actually scheduled all workers during the prewarm.
    pids = list(
        pool.map(
            _worker_prewarm,
            [slot_ts.isoformat()] * (workers * 2),
            chunksize=1,
        )
    )
    result = {
        "workers_requested": workers,
        "worker_pids_seen": len(set(pids)),
        "seconds": time.perf_counter() - started,
    }
    print(
        "[v7_live_scan] prewarm "
        f"requested={workers} pids_seen={result['worker_pids_seen']} "
        f"total={result['seconds']:.3f}s",
        flush=True,
    )
    return result


def _worker_scan(
    payload: Tuple[str, str],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    ticker, slot_iso = payload
    telemetry = _new_ticker_telemetry()
    _refresh_worker_market_context(slot_iso)
    try:
        cands = scan_ticker_live(
            ticker,
            pd.Timestamp(slot_iso),
            _WORKER_MARKET_CTX or {},
            telemetry=telemetry,
        )
    except Exception:
        telemetry["ticker_errors"] += 1
        return [], telemetry
    if not cands:
        return [], telemetry
    return candidates_to_dataframe(cands).to_dict("records"), telemetry


_SUM_TELEMETRY_KEYS = (
    "raw_cache_hits",
    "raw_cache_misses",
    "prepared_cache_hits",
    "prepared_cache_misses",
    "unchanged_frame_hits",
    "file_read_seconds",
    "prepare_seconds",
    "strategy_seconds",
    "unstable_file_reads",
    "ticker_errors",
)


def _summarize_slot_telemetry(
    ticker_telemetry: List[Dict[str, Any]],
    *,
    slot_ts: pd.Timestamp,
    ticker_count: int,
    workers: int,
    chunksize: int,
    pool_get_seconds: float,
    scan_wall_seconds: float,
    total_wall_seconds: float,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "slot_ist": slot_ts.isoformat(),
        "ticker_count": ticker_count,
        "workers": workers,
        "chunksize": chunksize,
        "pool_get_seconds": pool_get_seconds,
        "scan_wall_seconds": scan_wall_seconds,
        "total_wall_seconds": total_wall_seconds,
    }
    for key in _SUM_TELEMETRY_KEYS:
        summary[key] = sum(float(item.get(key, 0)) for item in ticker_telemetry)
        if key.endswith(("hits", "misses", "reads", "errors")):
            summary[key] = int(summary[key])

    elapsed = [
        float(item.get("ticker_elapsed_seconds", 0.0))
        for item in ticker_telemetry
        if float(item.get("ticker_elapsed_seconds", 0.0)) >= 0.0
    ]
    summary["ticker_p50_seconds"] = float(np.percentile(elapsed, 50)) if elapsed else 0.0
    summary["ticker_p95_seconds"] = float(np.percentile(elapsed, 95)) if elapsed else 0.0
    summary["ticker_max_seconds"] = max(elapsed, default=0.0)
    raw_lookups = summary["raw_cache_hits"] + summary["raw_cache_misses"]
    prepared_lookups = summary["prepared_cache_hits"] + summary["prepared_cache_misses"]
    summary["raw_cache_hit_rate"] = (
        summary["raw_cache_hits"] / raw_lookups if raw_lookups else 0.0
    )
    summary["prepared_cache_hit_rate"] = (
        summary["prepared_cache_hits"] / prepared_lookups if prepared_lookups else 0.0
    )
    return summary


def scan_slot(
    slot_ist: Any,
    tickers: Iterable[str],
    market_ctx: Optional[Dict[str, Dict[str, Any]]] = None,
    max_workers: Optional[int] = None,
    chunksize: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    global _LAST_SCAN_TELEMETRY
    overall_started = time.perf_counter()
    slot_ts = _ensure_ist_ts(slot_ist)
    tickers = [str(t).strip().upper() for t in tickers if str(t).strip()]
    workers = int(max_workers if max_workers is not None else DEFAULT_SCAN_WORKERS)
    effective_chunksize = max(
        1,
        int(chunksize if chunksize is not None else DEFAULT_SCAN_CHUNKSIZE),
    )

    rows: List[Dict[str, Any]] = []
    ticker_telemetry: List[Dict[str, Any]] = []
    pool_get_seconds = 0.0
    scan_started = time.perf_counter()

    if workers <= 1:
        # Sequential fallback (no process spawn).
        if market_ctx is None:
            market_ctx = build_market_context_once()
        for tkr in tickers:
            one_telemetry = _new_ticker_telemetry()
            try:
                c_list = scan_ticker_live(
                    tkr,
                    slot_ts,
                    market_ctx,
                    telemetry=one_telemetry,
                )
            except Exception:
                one_telemetry["ticker_errors"] += 1
                c_list = []
            ticker_telemetry.append(one_telemetry)
            if c_list:
                rows.extend(candidates_to_dataframe(c_list).to_dict("records"))
    else:
        slot_iso = slot_ts.isoformat()
        payloads = [(t, slot_iso) for t in tickers]
        today_str = slot_ts.date().isoformat()
        _t0 = time.perf_counter()
        pool = _get_scan_pool(workers, today_str)
        _t1 = time.perf_counter()
        pool_get_seconds = _t1 - _t0
        scan_started = _t1
        try:
            for res, one_telemetry in pool.map(
                _worker_scan,
                payloads,
                chunksize=effective_chunksize,
            ):
                ticker_telemetry.append(one_telemetry)
                if res:
                    rows.extend(res)
        except Exception as exc:
            print(
                f"[v7_live_scan] pool error ({type(exc).__name__}); sequential fallback this slot",
                flush=True,
            )
            _replace_scan_pool(workers, today_str)
            # A broken map can yield a partial prefix.  Restart the result and
            # telemetry lists before the full sequential retry so candidates
            # and timing counters are never double-counted.
            rows = []
            ticker_telemetry = []
            if market_ctx is None:
                market_ctx = build_market_context_once()
            for tkr in tickers:
                one_telemetry = _new_ticker_telemetry()
                try:
                    c_list = scan_ticker_live(
                        tkr,
                        slot_ts,
                        market_ctx,
                        telemetry=one_telemetry,
                    )
                except Exception:
                    one_telemetry["ticker_errors"] += 1
                    c_list = []
                ticker_telemetry.append(one_telemetry)
                if c_list:
                    rows.extend(candidates_to_dataframe(c_list).to_dict("records"))

    finished = time.perf_counter()
    _LAST_SCAN_TELEMETRY = _summarize_slot_telemetry(
        ticker_telemetry,
        slot_ts=slot_ts,
        ticker_count=len(tickers),
        workers=workers,
        chunksize=effective_chunksize,
        pool_get_seconds=pool_get_seconds,
        scan_wall_seconds=finished - scan_started,
        total_wall_seconds=finished - overall_started,
    )
    print(
        "[v7_live_scan] "
        f"n={len(tickers)} workers={workers} chunksize={effective_chunksize} "
        f"pool_get={_LAST_SCAN_TELEMETRY['pool_get_seconds']:.3f}s "
        f"scan={_LAST_SCAN_TELEMETRY['scan_wall_seconds']:.3f}s "
        f"total={_LAST_SCAN_TELEMETRY['total_wall_seconds']:.3f}s "
        f"raw_cache={_LAST_SCAN_TELEMETRY['raw_cache_hits']}/"
        f"{_LAST_SCAN_TELEMETRY['raw_cache_misses']} "
        f"unchanged_frames={_LAST_SCAN_TELEMETRY['unchanged_frame_hits']} "
        f"prepared_cache={_LAST_SCAN_TELEMETRY['prepared_cache_hits']}/"
        f"{_LAST_SCAN_TELEMETRY['prepared_cache_misses']} "
        f"io_cpu={_LAST_SCAN_TELEMETRY['file_read_seconds']:.3f}s "
        f"prepare_cpu={_LAST_SCAN_TELEMETRY['prepare_seconds']:.3f}s "
        f"strategy_cpu={_LAST_SCAN_TELEMETRY['strategy_seconds']:.3f}s "
        f"ticker_p95={_LAST_SCAN_TELEMETRY['ticker_p95_seconds']:.3f}s "
        f"ticker_max={_LAST_SCAN_TELEMETRY['ticker_max_seconds']:.3f}s "
        f"errors={_LAST_SCAN_TELEMETRY['ticker_errors']}",
        flush=True,
    )

    if not rows:
        return pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(rows)
    short_df = df.loc[df["side"].str.upper().eq("SHORT")].reset_index(drop=True)
    long_df = df.loc[df["side"].str.upper().eq("LONG")].reset_index(drop=True)
    return short_df, long_df
