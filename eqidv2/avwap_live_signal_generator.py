# -*- coding: utf-8 -*-
"""
AVWAP V11 Live Signal Generator (15m)

Scans the *latest completed* 15-minute candle for all tickers and emits
entry signals for AVWAP V11 (combined LONG + SHORT) using the **same
signal-selection logic** as:

- avwap_common.py (configs, helpers, indicators)
- avwap_long_strategy.py (LONG setup rules)
- avwap_short_strategy.py (SHORT setup rules)
- avwap_combined_runner.py (overall orchestration concepts)

ML meta-label gating (ml_meta_filter.py) is kept intact:
- ML is used only to FILTER + SIZE trades (take/skip + conf multiplier).

Output:
- Appends to: live_signals/signals_YYYY-MM-DD.csv (same style as avwap_live*.py)
- Logs to console every run.

Notes:
- This is "live entry" mode: it only evaluates whether an entry triggers on the
  latest completed candle; it does NOT simulate exits.
- The backtest runner (avwap_ml_backtest_runner.py) still does full exit resolution.

Author: ChatGPT rewrite (Feb 2026)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

# -----------------------------
# Imports (prefer package layout)
# -----------------------------
try:
    from avwap_v11_refactored.avwap_common import (
        IST,
        StrategyConfig,
        compute_quality_score_long,
        compute_quality_score_short,
        compute_day_avwap,
        default_long_config,
        default_short_config,
        entry_buffer,
        in_signal_window,
        prepare_indicators,
        volume_filter_pass,
        list_tickers_15m,
    )
    from avwap_v11_refactored import avwap_long_strategy as long_mod
    from avwap_v11_refactored import avwap_short_strategy as short_mod
except Exception:  # pragma: no cover
    # Fallback: same-folder modules
    from avwap_common import (  # type: ignore
        IST,
        StrategyConfig,
        compute_quality_score_long,
        compute_quality_score_short,
        compute_day_avwap,
        default_long_config,
        default_short_config,
        entry_buffer,
        in_signal_window,
        prepare_indicators,
        volume_filter_pass,
        list_tickers_15m,
    )
    import avwap_long_strategy as long_mod  # type: ignore
    import avwap_short_strategy as short_mod  # type: ignore

try:
    from ml_meta_filter import MetaLabelFilter
except Exception:  # pragma: no cover
    MetaLabelFilter = None  # type: ignore


# -----------------------------
# Constants / paths
# -----------------------------
DEFAULT_OUT_DIR = "live_signals"
DEFAULT_STATE_DIR = "logs"
DEFAULT_STATE_FILE = "eqidv2_avwap_live_state_v11.json"

# NSE cash market close (15m candles end at 15:30)
MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)
BARS_PER_DAY_15M = int(((MARKET_CLOSE.hour * 60 + MARKET_CLOSE.minute) - (MARKET_OPEN.hour * 60 + MARKET_OPEN.minute)) / 15) + 1  # 26


# -----------------------------
# Data structures
# -----------------------------
@dataclass
class LiveSignal:
    ticker: str
    side: str  # "LONG" or "SHORT"
    day: str   # YYYY-MM-DD (IST)
    bar_time_ist: str  # candle end timestamp in IST (ISO-ish)
    setup: str
    impulse: str
    entry: float
    sl: float
    target: float
    quality_score: float

    # diagnostics for ML / reporting
    adx: float = float("nan")
    rsi: float = float("nan")
    k: float = float("nan")
    avwap_dist_atr: float = float("nan")
    ema_gap_atr: float = float("nan")
    atr_pct: float = float("nan")


# -----------------------------
# Utilities
# -----------------------------
def now_ist() -> datetime:
    return datetime.now(IST)


def _safe_float(x, default=np.nan) -> float:
    try:
        v = float(x)
        if np.isfinite(v):
            return v
    except Exception:
        pass
    return float(default)


def _normalize_date_col(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "date" not in df.columns:
        # try common alternatives
        for alt in ["Date", "datetime", "timestamp", "time"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "date"})
                break
    if "date" not in df.columns:
        return df

    s = pd.to_datetime(df["date"], errors="coerce")
    if getattr(s.dt, "tz", None) is None:
        # assume IST if naive (your pipelines usually store IST end timestamps)
        s = s.dt.tz_localize(IST)
    else:
        s = s.dt.tz_convert(IST)
    df = df.copy()
    df["date"] = s
    return df


def read_parquet_tail(path: str, tail_rows: int = 220, engine: str = "pyarrow") -> pd.DataFrame:
    """Read only the tail rows from a parquet file (fast enough for live scanning)."""
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    try:
        import pyarrow.parquet as pq  # type: ignore
    except Exception:
        # fallback to full read
        df = pd.read_parquet(p, engine=engine)
        return df.tail(tail_rows).reset_index(drop=True)

    pf = pq.ParquetFile(str(p))
    rg = pf.num_row_groups
    if rg == 0:
        return pd.DataFrame()

    dfs: List[pd.DataFrame] = []
    got = 0
    for i in range(rg - 1, -1, -1):
        t = pf.read_row_group(i)
        dfi = t.to_pandas()
        dfs.append(dfi)
        got += len(dfi)
        if got >= tail_rows:
            break

    df = pd.concat(reversed(dfs), ignore_index=True)
    if len(df) > tail_rows:
        df = df.tail(tail_rows).reset_index(drop=True)
    return df


def last_completed_15m_slot(now: datetime, buffer_seconds: int) -> datetime:
    """
    Latest candle END timestamp considered "complete".
    Example: at 12:15:07 with buffer=7 => slot is 12:15:00.
             at 12:15:03 with buffer=7 => slot is 12:00:00 (not complete yet).
    """
    n = now.astimezone(IST)
    # If within buffer seconds of boundary, treat boundary as not complete.
    if n.second < buffer_seconds:
        n = n - timedelta(minutes=1)
    floormin = (n.minute // 15) * 15
    slot = n.replace(minute=floormin, second=0, microsecond=0)
    return slot


def next_15m_boundary(now: datetime, buffer_seconds: int) -> datetime:
    """Next time the scheduler should wake up: next 15m boundary + buffer."""
    n = now.astimezone(IST)
    # next boundary
    minute = ((n.minute // 15) + 1) * 15
    hour = n.hour
    day = n.date()
    if minute >= 60:
        minute -= 60
        hour += 1
        if hour >= 24:
            hour = 0
            day = (n + timedelta(days=1)).date()
    nxt = datetime(day.year, day.month, day.day, hour, minute, 0, tzinfo=IST)
    return nxt + timedelta(seconds=buffer_seconds)


def _bars_left_after_entry(entry_ts: datetime) -> int:
    """Bars remaining after the entry candle until MARKET_CLOSE (15m)."""
    t = entry_ts.astimezone(IST).time()
    mins = (t.hour * 60 + t.minute) - (MARKET_OPEN.hour * 60 + MARKET_OPEN.minute)
    if mins < 0:
        return 0
    pos = mins // 15  # 0-index bar number
    total_minus1 = BARS_PER_DAY_15M - 1
    return max(0, total_minus1 - pos)


def _close_confirm_ok(side: str, close_val: float, trigger: float, cfg: StrategyConfig) -> bool:
    if not getattr(cfg, "require_entry_close_confirm", True):
        return True
    if side.upper() == "LONG":
        return close_val > trigger
    return close_val < trigger


# -----------------------------
# Persistent state (caps + de-dup)
# -----------------------------
def _state_path(state_dir: str) -> Path:
    Path(state_dir).mkdir(parents=True, exist_ok=True)
    return Path(state_dir) / DEFAULT_STATE_FILE


def _load_state(state_dir: str) -> Dict:
    p = _state_path(state_dir)
    if not p.exists():
        return {"signals": {}}  # signals[day][ticker][side] = count
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"signals": {}}


def _save_state(state_dir: str, st: Dict) -> None:
    p = _state_path(state_dir)
    p.write_text(json.dumps(st, indent=2, sort_keys=True), encoding="utf-8")


def _count_today(st: Dict, day: str, ticker: str, side: str) -> int:
    return int(st.get("signals", {}).get(day, {}).get(ticker, {}).get(side, 0))


def _inc_today(st: Dict, day: str, ticker: str, side: str) -> None:
    st.setdefault("signals", {}).setdefault(day, {}).setdefault(ticker, {})[side] = _count_today(st, day, ticker, side) + 1


def _stable_signal_id(ticker: str, side: str, bar_ts: str, setup: str) -> str:
    raw = f"{ticker}|{side}|{bar_ts}|{setup}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _read_seen_signal_ids(csv_path: Path) -> Set[str]:
    if not csv_path.exists():
        return set()
    try:
        df = pd.read_csv(csv_path)
        if "signal_id" in df.columns:
            return set(df["signal_id"].astype(str).tolist())
    except Exception:
        pass
    return set()


def _append_signals_to_csv(csv_path: Path, rows: List[Dict]) -> None:
    if not rows:
        return
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df_new = pd.DataFrame(rows)
    if csv_path.exists():
        try:
            df_old = pd.read_csv(csv_path)
            df_all = pd.concat([df_old, df_new], ignore_index=True)
        except Exception:
            df_all = df_new
    else:
        df_all = df_new
    df_all.to_csv(csv_path, index=False)


# -----------------------------
# V11 live entry detection (selection logic from strategies)
# -----------------------------
def _prepare_today_df(df_tail: pd.DataFrame, cfg_any: StrategyConfig) -> Tuple[pd.DataFrame, str]:
    """
    Build a clean df_day with indicator columns and AVWAP for the most recent day in df_tail.
    Returns (df_day, day_str).
    """
    if df_tail.empty:
        return pd.DataFrame(), ""

    df = _normalize_date_col(df_tail)
    df = df.dropna(subset=["date"]).copy()
    if df.empty:
        return pd.DataFrame(), ""

    df = df.sort_values("date").reset_index(drop=True)

    # Restrict to market hours broadly (09:15 to 15:30). Strategy windows further filter signals.
    def _in_mkt(ts: pd.Timestamp) -> bool:
        t = ts.astimezone(IST).time()
        return (t >= MARKET_OPEN) and (t <= MARKET_CLOSE)

    df = df[df["date"].apply(_in_mkt)].copy()
    if df.empty:
        return pd.DataFrame(), ""

    # Ensure core OHLC columns exist
    for c in ["open", "high", "low", "close"]:
        if c not in df.columns:
            # try uppercase variants
            if c.upper() in df.columns:
                df = df.rename(columns={c.upper(): c})
    for c in ["open", "high", "low", "close"]:
        if c not in df.columns:
            return pd.DataFrame(), ""

    # Indicators: compute if missing (safe even if they already exist)
    df = prepare_indicators(df, cfg_any)

    # most recent day in this tail window
    last_day = str(df["day"].iloc[-1])
    df_day = df[df["day"] == last_day].copy().reset_index(drop=True)
    if df_day.empty:
        return pd.DataFrame(), ""

    if "AVWAP" not in df_day.columns:
        df_day["AVWAP"] = compute_day_avwap(df_day)

    return df_day, last_day


def _find_entry_idx_for_slot(df_day: pd.DataFrame, slot_ts: datetime) -> Optional[int]:
    if df_day.empty:
        return None
    slot_ts = slot_ts.astimezone(IST)
    # exact match on end timestamp is preferred
    m = (df_day["date"] == slot_ts)
    if bool(m.any()):
        return int(np.where(m.to_numpy())[0][-1])
    return None


def _make_long_signal(
    ticker: str,
    day: str,
    entry_ts: datetime,
    entry_price: float,
    setup: str,
    impulse: str,
    quality: float,
    diag: Dict[str, float],
    cfg: StrategyConfig,
) -> LiveSignal:
    return LiveSignal(
        ticker=ticker,
        side="LONG",
        day=day,
        bar_time_ist=entry_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
        setup=setup,
        impulse=impulse,
        entry=float(entry_price),
        sl=float(entry_price * (1.0 - cfg.stop_pct)),
        target=float(entry_price * (1.0 + cfg.target_pct)),
        quality_score=float(quality),
        adx=float(diag.get("adx", np.nan)),
        rsi=float(diag.get("rsi", np.nan)),
        k=float(diag.get("k", np.nan)),
        avwap_dist_atr=float(diag.get("avwap_dist_atr", np.nan)),
        ema_gap_atr=float(diag.get("ema_gap_atr", np.nan)),
        atr_pct=float(diag.get("atr_pct", np.nan)),
    )


def _make_short_signal(
    ticker: str,
    day: str,
    entry_ts: datetime,
    entry_price: float,
    setup: str,
    impulse: str,
    quality: float,
    diag: Dict[str, float],
    cfg: StrategyConfig,
) -> LiveSignal:
    return LiveSignal(
        ticker=ticker,
        side="SHORT",
        day=day,
        bar_time_ist=entry_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
        setup=setup,
        impulse=impulse,
        entry=float(entry_price),
        sl=float(entry_price * (1.0 + cfg.stop_pct)),
        target=float(entry_price * (1.0 - cfg.target_pct)),
        quality_score=float(quality),
        adx=float(diag.get("adx", np.nan)),
        rsi=float(diag.get("rsi", np.nan)),
        k=float(diag.get("k", np.nan)),
        avwap_dist_atr=float(diag.get("avwap_dist_atr", np.nan)),
        ema_gap_atr=float(diag.get("ema_gap_atr", np.nan)),
        atr_pct=float(diag.get("atr_pct", np.nan)),
    )


def _latest_long_signal_for_ticker(
    ticker: str,
    df_day: pd.DataFrame,
    day: str,
    entry_idx: int,
    cfg: StrategyConfig,
    st: Dict,
) -> Optional[LiveSignal]:
    if entry_idx < 4:
        return None
    entry_ts = df_day.at[entry_idx, "date"]
    if not in_signal_window(entry_ts, cfg):
        return None

    # cap per ticker/day for LONG
    if _count_today(st, day, ticker, "LONG") >= cfg.max_trades_per_ticker_per_day:
        return None

    bars_left = _bars_left_after_entry(entry_ts)
    if bars_left < cfg.min_bars_left_after_entry:
        return None

    best: Optional[LiveSignal] = None
    best_q = -1e9

    # Candidate impulse indices near the entry (for MOD setups) and farther (for HUGE setups).
    # We'll loop a small recent window to keep runtime bounded.
    start_i = max(2, entry_idx - 12)
    for i in range(start_i, entry_idx):
        c1 = df_day.iloc[i]
        signal_ts = c1["date"]
        if not in_signal_window(signal_ts, cfg):
            continue

        impulse = long_mod.classify_green_impulse(c1, cfg)
        if impulse not in ("MODERATE", "HUGE"):
            continue

        # Basic impulse stats
        atr1 = _safe_float(c1.get("ATR15", np.nan))
        if not np.isfinite(atr1) or atr1 <= 0:
            continue
        if not volume_filter_pass(c1, cfg):
            continue
        if not long_mod._trend_filter_long(df_day, i, cfg):
            continue

        adx1 = _safe_float(df_day.at[i, "ADX15"])
        rsi1 = _safe_float(df_day.at[i, "RSI15"])
        k1 = _safe_float(df_day.at[i, "K15"])
        atr_pct = _safe_float(df_day.at[i, "ATR_PCT"])

        adx_prev2 = _safe_float(df_day.at[i - 2, "ADX15"])
        adx_slope2 = adx1 - adx_prev2

        close1 = _safe_float(df_day.at[i, "close"])
        ema20 = _safe_float(df_day.at[i, "EMA20"])
        ema_gap_atr = (close1 - ema20) / atr1 if np.isfinite(ema20) and atr1 > 0 else 0.0

        # -------------
        # SETUP A (MODERATE): break C1 high on the entry candle
        # -------------
        if impulse == "MODERATE":
            if i + 1 == entry_idx:
                high1 = _safe_float(c1["high"])
                trigger = high1 + entry_buffer(high1, cfg)
                high_e = _safe_float(df_day.at[entry_idx, "high"])
                close_e = _safe_float(df_day.at[entry_idx, "close"])
                if high_e > trigger and _close_confirm_ok("LONG", close_e, trigger, cfg):
                    atr_entry = _safe_float(df_day.at[entry_idx, "ATR15"])
                    ok_support, avwap_dist_atr = long_mod.avwap_support_pass(
                        df_day, i, entry_idx, atr_entry, cfg
                    )
                    if ok_support:
                        q = compute_quality_score_long(
                            adx1, adx_slope2, avwap_dist_atr, ema_gap_atr, impulse
                        )
                        diag = {
                            "adx": adx1,
                            "rsi": rsi1,
                            "k": k1,
                            "atr_pct": atr_pct,
                            "ema_gap_atr": ema_gap_atr,
                            "avwap_dist_atr": avwap_dist_atr,
                        }
                        sig = _make_long_signal(
                            ticker, day, entry_ts, trigger, "A_MOD_BREAK_C1_HIGH", impulse, q, diag, cfg
                        )
                        if q > best_q:
                            best, best_q = sig, q

            # Option 2: small red pullback then break C2 high on entry candle
            if cfg.enable_setup_a_pullback_c2_break and i + 2 == entry_idx and (i + 1 < len(df_day)):
                c2 = df_day.iloc[i + 1]
                c2o, c2c = _safe_float(c2["open"]), _safe_float(c2["close"])
                c2_body = abs(c2c - c2o)
                c2_atr = _safe_float(c2.get("ATR15", atr1))
                c2_avwap = _safe_float(c2.get("AVWAP", np.nan))

                c2_small_red = (c2c < c2o) and np.isfinite(c2_atr) and (c2_body <= cfg.small_counter_max_atr * c2_atr)
                c2_above_avwap = np.isfinite(c2_avwap) and (c2c > c2_avwap)
                if c2_small_red and c2_above_avwap:
                    high2 = _safe_float(c2["high"])
                    trigger2 = high2 + entry_buffer(high2, cfg)
                    high_e = _safe_float(df_day.at[entry_idx, "high"])
                    close_e = _safe_float(df_day.at[entry_idx, "close"])
                    if high_e > trigger2 and _close_confirm_ok("LONG", close_e, trigger2, cfg):
                        atr_entry = _safe_float(df_day.at[entry_idx, "ATR15"])
                        ok_support, avwap_dist_atr = long_mod.avwap_support_pass(
                            df_day, i, entry_idx, atr_entry, cfg
                        )
                        if ok_support:
                            q = compute_quality_score_long(
                                adx1, adx_slope2, avwap_dist_atr, ema_gap_atr, impulse
                            )
                            diag = {
                                "adx": adx1,
                                "rsi": rsi1,
                                "k": k1,
                                "atr_pct": atr_pct,
                                "ema_gap_atr": ema_gap_atr,
                                "avwap_dist_atr": avwap_dist_atr,
                            }
                            sig = _make_long_signal(
                                ticker, day, entry_ts, trigger2, "A_PULLBACK_C2_THEN_BREAK_C2_HIGH", impulse, q, diag, cfg
                            )
                            if q > best_q:
                                best, best_q = sig, q

        # -------------
        # SETUP B (HUGE): pullback holds, then break pullback high on entry candle
        # -------------
        if impulse == "HUGE":
            pull_end = min(i + 3, entry_idx)  # entry_idx is last
            if pull_end <= i:
                continue
            pull = df_day.iloc[i + 1 : pull_end + 1].copy()
            if pull.empty:
                continue

            open1 = _safe_float(c1["open"])
            mid_body = (open1 + close1) / 2.0

            pull_atr = pd.to_numeric(pull["ATR15"], errors="coerce").fillna(atr1)
            pull_body = (pd.to_numeric(pull["close"], errors="coerce") - pd.to_numeric(pull["open"], errors="coerce")).abs()
            pull_red = pd.to_numeric(pull["close"], errors="coerce") < pd.to_numeric(pull["open"], errors="coerce")
            pull_small = pull_body <= (cfg.small_counter_max_atr * pull_atr)

            if not bool((pull_red & pull_small).any()):
                continue

            lows = pd.to_numeric(pull["low"], errors="coerce")
            closes = pd.to_numeric(pull["close"], errors="coerce")
            avwaps = pd.to_numeric(pull["AVWAP"], errors="coerce")

            hold_mid = bool((lows > mid_body).fillna(False).all())
            hold_avwap = bool((closes > avwaps).fillna(False).all())
            if not (hold_mid or hold_avwap):
                continue

            pull_high = float(pd.to_numeric(pull["high"], errors="coerce").max())
            if not np.isfinite(pull_high):
                continue

            trigger = pull_high + entry_buffer(pull_high, cfg)

            # in scan_one_day, it breaks out of the entry loop when close <= avwap.
            # Here, enforce: no close <= avwap from (pull_end+1 .. entry_idx) inclusive.
            if pull_end + 1 <= entry_idx:
                seg = df_day.iloc[pull_end + 1 : entry_idx + 1]
                if not seg.empty and ("AVWAP" in seg.columns):
                    closes_seg = pd.to_numeric(seg["close"], errors="coerce")
                    avwap_seg = pd.to_numeric(seg["AVWAP"], errors="coerce")
                    if bool((closes_seg <= avwap_seg).fillna(False).any()):
                        continue

            high_e = _safe_float(df_day.at[entry_idx, "high"])
            close_e = _safe_float(df_day.at[entry_idx, "close"])
            if high_e > trigger and _close_confirm_ok("LONG", close_e, trigger, cfg):
                atr_entry = _safe_float(df_day.at[entry_idx, "ATR15"])
                ok_support, avwap_dist_atr = long_mod.avwap_support_pass(df_day, i, entry_idx, atr_entry, cfg)
                if ok_support:
                    q = compute_quality_score_long(adx1, adx_slope2, avwap_dist_atr, ema_gap_atr, impulse)
                    diag = {
                        "adx": adx1,
                        "rsi": rsi1,
                        "k": k1,
                        "atr_pct": atr_pct,
                        "ema_gap_atr": ema_gap_atr,
                        "avwap_dist_atr": avwap_dist_atr,
                    }
                    sig = _make_long_signal(
                        ticker, day, entry_ts, trigger, "B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK", impulse, q, diag, cfg
                    )
                    if q > best_q:
                        best, best_q = sig, q

    return best


def _latest_short_signal_for_ticker(
    ticker: str,
    df_day: pd.DataFrame,
    day: str,
    entry_idx: int,
    cfg: StrategyConfig,
    st: Dict,
) -> Optional[LiveSignal]:
    if entry_idx < 4:
        return None
    entry_ts = df_day.at[entry_idx, "date"]
    if not in_signal_window(entry_ts, cfg):
        return None

    if _count_today(st, day, ticker, "SHORT") >= cfg.max_trades_per_ticker_per_day:
        return None

    bars_left = _bars_left_after_entry(entry_ts)
    if bars_left < cfg.min_bars_left_after_entry:
        return None

    best: Optional[LiveSignal] = None
    best_q = -1e9

    start_i = max(2, entry_idx - 12)
    for i in range(start_i, entry_idx):
        c1 = df_day.iloc[i]
        signal_ts = c1["date"]
        if not in_signal_window(signal_ts, cfg):
            continue

        impulse = short_mod.classify_red_impulse(c1, cfg)
        if impulse not in ("MODERATE", "HUGE"):
            continue

        atr1 = _safe_float(c1.get("ATR15", np.nan))
        if not np.isfinite(atr1) or atr1 <= 0:
            continue
        if not volume_filter_pass(c1, cfg):
            continue
        if not short_mod._trend_filter_short(df_day, i, cfg):
            continue

        adx1 = _safe_float(df_day.at[i, "ADX15"])
        rsi1 = _safe_float(df_day.at[i, "RSI15"])
        k1 = _safe_float(df_day.at[i, "K15"])
        atr_pct = _safe_float(df_day.at[i, "ATR_PCT"])

        close1 = _safe_float(df_day.at[i, "close"])
        ema20 = _safe_float(df_day.at[i, "EMA20"])
        ema_gap_atr = (ema20 - close1) / atr1 if np.isfinite(ema20) and atr1 > 0 else 0.0
        avwap1 = _safe_float(df_day.at[i, "AVWAP"])
        avwap_dist_atr_imp = (avwap1 - close1) / atr1 if np.isfinite(avwap1) and atr1 > 0 else np.nan

        # Quality score for short doesn't include slope term
        q_base = compute_quality_score_short(adx1, avwap_dist_atr_imp, ema_gap_atr, impulse)

        # -------------
        # SETUP A (MODERATE): break C1 low on entry candle
        # -------------
        if impulse == "MODERATE":
            # Option 1: break C1 low on C2 (entry_idx = i+1)
            if i + 1 == entry_idx:
                low1 = _safe_float(c1["low"])
                trigger1 = low1 - entry_buffer(low1, cfg)
                low_e = _safe_float(df_day.at[entry_idx, "low"])
                close_e = _safe_float(df_day.at[entry_idx, "close"])
                if low_e < trigger1 and _close_confirm_ok("SHORT", close_e, trigger1, cfg):
                    if short_mod.avwap_rejection_pass(df_day, i, entry_idx, cfg) and short_mod.avwap_distance_pass(df_day, entry_idx, cfg):
                        diag = {
                            "adx": adx1,
                            "rsi": rsi1,
                            "k": k1,
                            "atr_pct": atr_pct,
                            "ema_gap_atr": ema_gap_atr,
                            "avwap_dist_atr": avwap_dist_atr_imp,
                        }
                        sig = _make_short_signal(
                            ticker, day, entry_ts, trigger1, "A_MOD_BREAK_C1_LOW", impulse, q_base, diag, cfg
                        )
                        if q_base > best_q:
                            best, best_q = sig, q_base

            # Option 2: small green pullback C2, then break C2 low on C3 (entry_idx = i+2)
            if i + 2 == entry_idx and (i + 1 < len(df_day)):
                c2 = df_day.iloc[i + 1]
                c2o, c2c = _safe_float(c2["open"]), _safe_float(c2["close"])
                c2_body = abs(c2c - c2o)
                c2_atr = _safe_float(c2.get("ATR15", atr1))
                c2_avwap = _safe_float(c2.get("AVWAP", np.nan))

                c2_small_green = (c2c > c2o) and np.isfinite(c2_atr) and (c2_body <= cfg.small_counter_max_atr * c2_atr)
                c2_below_avwap = np.isfinite(c2_avwap) and (c2c < c2_avwap)
                if c2_small_green and c2_below_avwap:
                    low2 = _safe_float(c2["low"])
                    trigger2 = low2 - entry_buffer(low2, cfg)
                    low_e = _safe_float(df_day.at[entry_idx, "low"])
                    close_e = _safe_float(df_day.at[entry_idx, "close"])
                    if low_e < trigger2 and _close_confirm_ok("SHORT", close_e, trigger2, cfg):
                        if short_mod.avwap_rejection_pass(df_day, i, entry_idx, cfg) and short_mod.avwap_distance_pass(df_day, entry_idx, cfg):
                            diag = {
                                "adx": adx1,
                                "rsi": rsi1,
                                "k": k1,
                                "atr_pct": atr_pct,
                                "ema_gap_atr": ema_gap_atr,
                                "avwap_dist_atr": avwap_dist_atr_imp,
                            }
                            sig = _make_short_signal(
                                ticker, day, entry_ts, trigger2, "A_PULLBACK_C2_THEN_BREAK_C2_LOW", impulse, q_base, diag, cfg
                            )
                            if q_base > best_q:
                                best, best_q = sig, q_base

        # -------------
        # SETUP B (HUGE): bounce fails, break bounce low on entry candle
        # -------------
        if impulse == "HUGE":
            bounce_end = min(i + 3, entry_idx)
            if bounce_end <= i:
                continue
            bounce = df_day.iloc[i + 1 : bounce_end + 1].copy()
            if bounce.empty:
                continue

            closes = pd.to_numeric(bounce["close"], errors="coerce")
            opens = pd.to_numeric(bounce["open"], errors="coerce")
            bounce_atr = pd.to_numeric(bounce.get("ATR15", atr1), errors="coerce").fillna(atr1)
            bounce_body = (closes - opens).abs()
            bounce_green = closes > opens
            bounce_small = bounce_body <= (cfg.small_counter_max_atr * bounce_atr)

            if not bool((bounce_green & bounce_small).any()):
                continue

            if cfg.require_avwap_rule and cfg.avwap_touch:
                avwaps = pd.to_numeric(bounce["AVWAP"], errors="coerce")
                highs = pd.to_numeric(bounce["high"], errors="coerce")
                touch_fail = bool(((highs >= avwaps) & (closes < avwaps)).fillna(False).any())
                if not touch_fail:
                    continue

            bounce_low = float(pd.to_numeric(bounce["low"], errors="coerce").min())
            if not np.isfinite(bounce_low):
                continue
            trigger_b = bounce_low - entry_buffer(bounce_low, cfg)

            # Similar to scan_one_day: break if close >= avwap (invalidate)
            if bounce_end + 1 <= entry_idx:
                seg = df_day.iloc[bounce_end + 1 : entry_idx + 1]
                if not seg.empty and ("AVWAP" in seg.columns):
                    closes_seg = pd.to_numeric(seg["close"], errors="coerce")
                    avwap_seg = pd.to_numeric(seg["AVWAP"], errors="coerce")
                    if bool((closes_seg >= avwap_seg).fillna(False).any()):
                        continue

            low_e = _safe_float(df_day.at[entry_idx, "low"])
            close_e = _safe_float(df_day.at[entry_idx, "close"])
            if low_e < trigger_b and _close_confirm_ok("SHORT", close_e, trigger_b, cfg):
                if short_mod.avwap_distance_pass(df_day, entry_idx, cfg) and short_mod.avwap_rejection_pass(df_day, i, entry_idx, cfg):
                    diag = {
                        "adx": adx1,
                        "rsi": rsi1,
                        "k": k1,
                        "atr_pct": atr_pct,
                        "ema_gap_atr": ema_gap_atr,
                        "avwap_dist_atr": avwap_dist_atr_imp,
                    }
                    sig = _make_short_signal(
                        ticker, day, entry_ts, trigger_b, "B_HUGE_RED_FAILED_BOUNCE", impulse, q_base, diag, cfg
                    )
                    if q_base > best_q:
                        best, best_q = sig, q_base

    return best


def detect_latest_signals_for_ticker(
    ticker: str,
    parquet_path: str,
    tail_rows: int,
    cfg_long: StrategyConfig,
    cfg_short: StrategyConfig,
    st: Dict,
) -> List[LiveSignal]:
    df_tail = read_parquet_tail(parquet_path, tail_rows=tail_rows, engine=cfg_long.parquet_engine)
    if df_tail.empty:
        return []
    # prepare df_day using long cfg (indicators same)
    df_day, day = _prepare_today_df(df_tail, cfg_long)
    if df_day.empty or not day:
        return []

    slot_ts = last_completed_15m_slot(now_ist(), buffer_seconds=0)  # buffer is handled by scheduler loop
    entry_idx = _find_entry_idx_for_slot(df_day, slot_ts)
    if entry_idx is None:
        return []

    out: List[LiveSignal] = []
    s_long = _latest_long_signal_for_ticker(ticker, df_day, day, entry_idx, cfg_long, st)
    if s_long is not None:
        out.append(s_long)
    s_short = _latest_short_signal_for_ticker(ticker, df_day, day, entry_idx, cfg_short, st)
    if s_short is not None:
        out.append(s_short)
    return out


# -----------------------------
# ML gate + scan loop
# -----------------------------
def _apply_ml_gate(
    signals: List[LiveSignal],
    ml_filter: Optional["MetaLabelFilter"],
    ml_threshold: float,
    disable_ml: bool,
) -> List[Tuple[LiveSignal, Dict]]:
    """
    Returns list of (signal, ml_info_dict) where ml_info includes p_win and conf_mult.
    If ML disabled or missing model, p_win will be NaN and conf_mult=1.0.
    """
    out: List[Tuple[LiveSignal, Dict]] = []
    for sig in signals:
        info = {"p_win": np.nan, "conf_mult": 1.0, "ml_taken": True}
        if disable_ml or ml_filter is None:
            out.append((sig, info))
            continue

        try:
            x = {
                "quality_score": sig.quality_score,
                "atr_pct": sig.atr_pct,
                "rsi": sig.rsi,
                "adx": sig.adx,
                "side": sig.side,
            }
            p = float(ml_filter.predict_pwin(x))
            cm = float(ml_filter.confidence_multiplier(p))
            take = bool(cm > 0.0) if ml_threshold is None else bool(p >= ml_threshold)
            # If you want ML gate to be exactly args.ml_threshold, we set it on ml_filter.cfg too.
            info = {"p_win": p, "conf_mult": cm, "ml_taken": take}
        except Exception:
            # keep defaults
            pass
        if info.get("ml_taken", True):
            out.append((sig, info))
    return out


def scan_latest_slot_all_tickers(
    cfg_long: StrategyConfig,
    cfg_short: StrategyConfig,
    args: argparse.Namespace,
) -> List[Dict]:
    """
    Scans ALL tickers once for the latest completed slot and returns row dicts to append to CSV.
    """
    st = _load_state(args.state_dir)
    out_rows: List[Dict] = []

    # Determine slot we are scanning (using buffer to ensure candle complete)
    slot_ts = last_completed_15m_slot(now_ist(), buffer_seconds=args.buffer_seconds)
    day_str = slot_ts.astimezone(IST).strftime("%Y-%m-%d")
    slot_str = slot_ts.astimezone(IST).strftime("%H:%M")

    print(f"[RUN ] slot={slot_str}  ts={slot_ts.strftime('%Y-%m-%d %H:%M:%S%z')}")

    # List tickers from files
    tickers = list_tickers_15m(args.dir_15m, args.suffix)
    if not tickers:
        print(f"[WARN] No tickers found in dir={args.dir_15m} suffix={args.suffix}")
        return []

    # Optional side filter
    side_filter = (args.side or "BOTH").upper()

    # ML filter
    ml_filter = None
    if (not args.no_ml) and (MetaLabelFilter is not None):
        try:
            ml_filter = MetaLabelFilter(model_path=args.model_path, features_path=args.features_path)
            # keep threshold consistent with CLI
            try:
                ml_filter.cfg.pwin_threshold = float(args.ml_threshold)
            except Exception:
                pass
        except Exception as e:
            print(f"[WARN] ML filter unavailable: {e}")
            ml_filter = None

    # Per-run candidate collection (to apply topN per run)
    candidates: List[LiveSignal] = []
    for t in tickers:
        path = os.path.join(args.dir_15m, f"{t}{args.suffix}")
        try:
            # use slot_ts inside (buffer handled by loop); we also need exact slot in df
            df_tail = read_parquet_tail(path, tail_rows=args.tail_rows, engine=cfg_long.parquet_engine)
            if df_tail.empty:
                continue
            df_day, day = _prepare_today_df(df_tail, cfg_long)
            if df_day.empty:
                continue
            entry_idx = _find_entry_idx_for_slot(df_day, slot_ts)
            if entry_idx is None:
                continue

            # LONG
            if side_filter in ("BOTH", "LONG"):
                sL = _latest_long_signal_for_ticker(t, df_day, day, entry_idx, cfg_long, st)
                if sL is not None:
                    candidates.append(sL)
            # SHORT
            if side_filter in ("BOTH", "SHORT"):
                sS = _latest_short_signal_for_ticker(t, df_day, day, entry_idx, cfg_short, st)
                if sS is not None:
                    candidates.append(sS)
        except Exception as e:
            print(f"[WARN] {t}: scan error: {e}")
            continue

    if not candidates:
        print("[INFO] No entry signals on this slot.")
        return []

    # TopN per run (not per day). Keeps behavior practical for live.
    if args.topn_per_run and args.topn_per_run > 0:
        if side_filter == "BOTH":
            longs = [s for s in candidates if s.side == "LONG"]
            shorts = [s for s in candidates if s.side == "SHORT"]
            longs.sort(key=lambda s: s.quality_score, reverse=True)
            shorts.sort(key=lambda s: s.quality_score, reverse=True)
            candidates = longs[: args.topn_per_run] + shorts[: args.topn_per_run]
        else:
            candidates.sort(key=lambda s: s.quality_score, reverse=True)
            candidates = candidates[: args.topn_per_run]

    # Apply ML gate (take/skip + conf multiplier)
    gated = _apply_ml_gate(candidates, ml_filter, args.ml_threshold, args.no_ml)

    # Build CSV rows (dedup by signal_id)
    out_csv = Path(args.out_dir) / f"signals_{day_str}.csv"
    seen_ids = _read_seen_signal_ids(out_csv)

    created_ts = now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
    accepted = 0
    for sig, ml in gated:
        signal_id = _stable_signal_id(sig.ticker, sig.side, sig.bar_time_ist, sig.setup)
        if signal_id in seen_ids:
            continue

        row = {
            "date": day_str,
            "ticker": sig.ticker,
            "side": sig.side,
            "bar_time_ist": sig.bar_time_ist,
            "entry": sig.entry,
            "sl": sig.sl,
            "target": sig.target,
            "setup": sig.setup,
            "impulse": sig.impulse,
            "quality_score": sig.quality_score,
            "adx": sig.adx,
            "rsi": sig.rsi,
            "k": sig.k,
            "avwap_dist_atr": sig.avwap_dist_atr,
            "ema_gap_atr": sig.ema_gap_atr,
            "atr_pct": sig.atr_pct,
            "p_win": ml.get("p_win", np.nan),
            "conf_mult": ml.get("conf_mult", 1.0),
            "signal_id": signal_id,
            "created_ts_ist": created_ts,
        }
        out_rows.append(row)
        seen_ids.add(signal_id)
        accepted += 1

        # Mark in state (per ticker/day cap)
        _inc_today(st, day_str, sig.ticker, sig.side)

    _save_state(args.state_dir, st)

    if accepted == 0:
        print("[INFO] All candidate signals were duplicates (already written).")
    else:
        print(f"[OK  ] Signals emitted: {accepted} -> {out_csv.as_posix()}")

    return out_rows


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--dir-15m", dest="dir_15m", default="stocks_indicators_15min_eq", help="Directory with per-ticker 15m parquet")
    p.add_argument("--suffix", default="_stocks_indicators_15min.parquet", help="Parquet suffix (e.g. _stocks_indicators_15min.parquet)")
    p.add_argument("--out-dir", dest="out_dir", default=DEFAULT_OUT_DIR, help="Output dir for live_signals CSVs")
    p.add_argument("--state-dir", dest="state_dir", default=DEFAULT_STATE_DIR, help="Directory for state json (caps/dedup)")
    p.add_argument("--tail-rows", dest="tail_rows", type=int, default=260, help="Tail rows per ticker to load (speed/accuracy tradeoff)")
    p.add_argument("--buffer-seconds", dest="buffer_seconds", type=int, default=7, help="Seconds after boundary to consider candle 'complete'")
    p.add_argument("--sleep-seconds", dest="sleep_seconds", type=int, default=1, help="Loop idle sleep granularity")
    p.add_argument("--run-once", action="store_true", help="Run once and exit")
    p.add_argument("--dry-run", action="store_true", help="Do not write CSV (only print)")
    p.add_argument("--side", default="BOTH", choices=["BOTH", "LONG", "SHORT"], help="Which side(s) to scan")

    # ML gate
    p.add_argument("--no-ml", action="store_true", help="Disable ML gate")
    p.add_argument("--ml-threshold", dest="ml_threshold", type=float, default=0.60, help="Min p_win to accept a trade")
    p.add_argument("--model-path", dest="model_path", default="models/meta_label_model.pkl", help="Path to trained ML model (if used)")
    p.add_argument("--features-path", dest="features_path", default="models/meta_label_features.json", help="Feature spec json (if used)")
    p.add_argument("--topn-per-run", dest="topn_per_run", type=int, default=30, help="Keep top-N signals per run (per side if BOTH)")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()

    # Build configs (selection logic lives in these configs)
    cfg_long = default_long_config()
    cfg_short = default_short_config()

    # Ensure dir exists
    Path(args.out_dir).mkdir(parents=True, exist_ok=True)
    Path(args.state_dir).mkdir(parents=True, exist_ok=True)

    print("[LIVE] AVWAP V11 Live Signal Generator (15m)")
    print(f"       dir_15m={args.dir_15m}  suffix={args.suffix}")
    print(f"       out_dir={args.out_dir}  state_dir={args.state_dir}")
    print(f"       ML={'OFF' if args.no_ml else 'ON'}  threshold={args.ml_threshold}")
    print(f"       TopN/run={args.topn_per_run}  side={args.side}")
    print(f"       Buffer={args.buffer_seconds}s  tail_rows={args.tail_rows}")

    if args.run_once:
        rows = scan_latest_slot_all_tickers(cfg_long, cfg_short, args)
        if rows and (not args.dry_run):
            out_csv = Path(args.out_dir) / f"signals_{now_ist().strftime('%Y-%m-%d')}.csv"
            _append_signals_to_csv(out_csv, rows)
        return

    # Loop mode
    while True:
        now = now_ist()
        nxt = next_15m_boundary(now, args.buffer_seconds)
        # Sleep until next boundary
        while now_ist() < nxt:
            time.sleep(args.sleep_seconds)

        # Scan once after boundary+buffer
        try:
            rows = scan_latest_slot_all_tickers(cfg_long, cfg_short, args)
            if rows and (not args.dry_run):
                day_str = now_ist().strftime("%Y-%m-%d")
                out_csv = Path(args.out_dir) / f"signals_{day_str}.csv"
                _append_signals_to_csv(out_csv, rows)
        except KeyboardInterrupt:
            print("\n[STOP] user interrupt")
            break
        except Exception as e:
            print(f"[ERROR] scan failed: {e}")


if __name__ == "__main__":
    main()
