# -*- coding: utf-8 -*-
"""
avwap_live_signal_generator.py — Live AVWAP Signal Scanner & CSV Writer
========================================================================

Runs continuously during market hours (9:15 AM – 3:30 PM IST).
Every 15 minutes (aligned to candle closes), scans the latest parquet data
for AVWAP entry signals and appends them to a daily CSV file.

Output CSV: live_signals/signals_YYYY-MM-DD.csv
Each row contains:
  - signal_id          : unique identifier for deduplication
  - logtime_ist        : time when signal was generated
  - ticker, side       : LONG/SHORT
  - entry_price, sl_price, target_price
  - quality_score, atr_pct_signal, rsi_signal, adx_signal (if available)
  - p_win, ml_threshold, confidence_multiplier
  - quantity           : position size in shares (scaled by confidence multiplier)
  - notes              : small debugging text (e.g., model/heuristic)

ML usage
--------
This script uses ml_meta_filter.MetaLabelFilter:
- If eqidv2/models/meta_model.pkl + eqidv2/models/meta_features.json exist:
    -> real model is used (predict_proba)
- Else:
    -> heuristic fallback is used
Then:
- If p_win < threshold -> confidence_multiplier = 0 -> signal is skipped
- Else quantity is scaled by confidence_multiplier

IMPORTANT
---------
This file fixes a common bug:
- run_single_scan passed meta_filter to scan_all_tickers,
  but scan_all_tickers did not accept meta_filter.
This rewrite makes meta_filter explicit and always available.

"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

# ML meta filter (must exist in your repo)
from ml_meta_filter import MetaFilterConfig, MetaLabelFilter

# ----------------------------
# Constants / Defaults
# ----------------------------
IST = pytz.timezone("Asia/Kolkata")

DEFAULT_15M_DATA_DIR = "stocks_indicators_15min_eq"
DEFAULT_SIGNALS_DIR = "live_signals"

DEFAULT_POSITION_SIZE_RS = 50_000.0   # base margin/capital per trade (notional = * leverage)
DEFAULT_LEVERAGE = 5.0               # intraday leverage multiplier (used for quantity sizing)
DEFAULT_ML_THRESHOLD = 0.60

# market times
MARKET_OPEN = (9, 15)
MARKET_CLOSE = (15, 30)
CUTOFF_NEW_TRADES = (15, 10)  # stop generating new signals after this (safety)

# scanning cadence
SCAN_EVERY_MINUTES = 15

# columns to write (stable schema)
SIGNAL_COLUMNS = [
    "signal_id",
    "logtime_ist",
    "ticker",
    "side",
    "entry_price",
    "sl_price",
    "target_price",
    "quality_score",
    "atr_pct_signal",
    "rsi_signal",
    "adx_signal",
    "p_win",
    "ml_threshold",
    "confidence_multiplier",
    "quantity",
    "notes",
]


# ----------------------------
# Data structures
# ----------------------------
@dataclass
class LiveSignal:
    signal_id: str
    logtime_ist: str
    ticker: str
    side: str
    entry_price: float
    sl_price: float
    target_price: float
    quality_score: float
    atr_pct_signal: float
    rsi_signal: float
    adx_signal: float
    p_win: float
    ml_threshold: float
    confidence_multiplier: float
    quantity: int
    notes: str


# ----------------------------
# Helpers
# ----------------------------
def now_ist() -> datetime:
    return datetime.now(IST)


def is_market_time(dt: datetime) -> bool:
    """Return True if dt is within market hours (inclusive)."""
    h, m = dt.hour, dt.minute
    open_h, open_m = MARKET_OPEN
    close_h, close_m = MARKET_CLOSE
    if (h, m) < (open_h, open_m):
        return False
    if (h, m) > (close_h, close_m):
        return False
    return True


def is_after_cutoff(dt: datetime) -> bool:
    """No new trades after cutoff time."""
    ch, cm = CUTOFF_NEW_TRADES
    return (dt.hour, dt.minute) >= (ch, cm)


def align_to_next_15m(dt: datetime) -> datetime:
    """Sleep until the next 15m boundary (approx)."""
    minute = (dt.minute // SCAN_EVERY_MINUTES + 1) * SCAN_EVERY_MINUTES
    nxt = dt.replace(second=5, microsecond=0)  # small delay to allow file writes
    if minute >= 60:
        nxt = (nxt + timedelta(hours=1)).replace(minute=0)
    else:
        nxt = nxt.replace(minute=minute)
    return nxt


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def list_tickers(data_dir: str) -> List[str]:
    """Tickers inferred from parquet files in data_dir."""
    d = Path(data_dir)
    if not d.exists():
        return []
    tickers = []
    for fp in d.glob("*.parquet"):
        tickers.append(fp.stem)
    tickers = sorted(set(tickers))
    return tickers


def read_latest_row_for_date(parquet_path: Path, today: date) -> Optional[pd.Series]:
    """
    Read parquet and pick the latest row for 'today' (IST).
    Supports 'datetime' or 'date' column, or datetime index.
    """
    try:
        df = pd.read_parquet(parquet_path)
    except Exception:
        return None
    if df is None or df.empty:
        return None

    # normalize datetime column
    cols_lower = {c.lower(): c for c in df.columns}
    if "datetime" in cols_lower:
        dtc = cols_lower["datetime"]
        ts = pd.to_datetime(df[dtc], errors="coerce")
    elif "date" in cols_lower:
        dc = cols_lower["date"]
        ts = pd.to_datetime(df[dc], errors="coerce")
    elif isinstance(df.index, pd.DatetimeIndex):
        ts = pd.to_datetime(df.index, errors="coerce")
    else:
        return None

    # convert/localize to IST
    try:
        if getattr(ts.dt, "tz", None) is None:
            ts = ts.dt.tz_localize(IST)
        else:
            ts = ts.dt.tz_convert(IST)
    except Exception:
        # per-row fallback
        ts = ts.apply(lambda x: x.tz_localize(IST) if x.tzinfo is None else x.tz_convert(IST))

    df = df.copy()
    df["_dt"] = ts
    df = df.dropna(subset=["_dt"]).sort_values("_dt")

    day_mask = df["_dt"].dt.date == today
    ddf = df[day_mask]
    if ddf.empty:
        return None

    return ddf.iloc[-1]


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return float(default)
        if isinstance(x, str) and x.strip() == "":
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def build_signal_features(row: pd.Series, side: str) -> Dict[str, Any]:
    """
    Build the minimal feature dict expected by MetaLabelFilter.
    It can accept keys:
      quality_score, atr_pct/atr_pct_signal, rsi/rsi_signal, adx/adx_signal, side
    """
    return {
        "quality_score": safe_float(row.get("quality_score", 0.0), 0.0),
        "atr_pct_signal": safe_float(row.get("atr_pct_signal", row.get("atr_pct", 0.0)), 0.0),
        "rsi_signal": safe_float(row.get("rsi_signal", row.get("rsi", 50.0)), 50.0),
        "adx_signal": safe_float(row.get("adx_signal", row.get("adx", 20.0)), 20.0),
        "side": side,
    }


def compute_qty(
    entry_price: float,
    position_size_rs: float,
    leverage: float,
    confidence_multiplier: float,
) -> int:
    """
    Notional sizing:
      notional = position_size_rs * leverage * confidence_multiplier
      qty = floor(notional / entry_price)

    (If you later want risk-based sizing using stop_distance, change here.)
    """
    if entry_price <= 0:
        return 0
    notional = float(position_size_rs) * float(leverage) * float(confidence_multiplier)
    qty = int(max(0, np.floor(notional / float(entry_price))))
    return qty


def write_signals_csv(csv_path: Path, signals: List[LiveSignal]) -> None:
    ensure_dir(csv_path.parent)

    file_exists = csv_path.exists()
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SIGNAL_COLUMNS)
        if not file_exists:
            w.writeheader()
        for s in signals:
            row = asdict(s)
            # ensure all columns exist
            out = {k: row.get(k, "") for k in SIGNAL_COLUMNS}
            w.writerow(out)


def load_strategy_modules(paths: Optional[List[str]]) -> Optional[dict]:
    """
    Optional: dynamic import of strategy modules, if you keep AVWAP detection logic in separate files.
    If not needed, returns None.
    """
    if not paths:
        return None
    mods = {}
    for p in paths:
        pth = Path(p)
        if not pth.exists():
            continue
        # dynamic import
        name = pth.stem + "_" + uuid.uuid4().hex[:6]
        import importlib.util
        spec = importlib.util.spec_from_file_location(name, str(pth))
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            mods[pth.stem] = mod
    return mods or None


# ----------------------------
# Rule-based detection
# ----------------------------
def detect_avwap_signal_builtin(row: pd.Series) -> Optional[Tuple[str, float, float, float, float]]:
    """
    Minimal built-in AVWAP-style detector, used if no external strategy module is supplied.
    Returns:
      (side, entry_price, sl_price, target_price, quality_score)
    or None if no signal.

    NOTE:
    If you already have your own detection in imported modules, that should be preferred.
    This built-in is intentionally conservative and simple.
    """
    close = safe_float(row.get("close", np.nan), np.nan)
    vwap = safe_float(row.get("VWAP", row.get("vwap", np.nan)), np.nan)
    avwap = safe_float(row.get("AVWAP", row.get("avwap", np.nan)), np.nan)

    if not np.isfinite(close) or not np.isfinite(avwap):
        return None

    # simple directional bias
    # LONG: price above AVWAP and recent RSI/ADX suggest trend
    rsi = safe_float(row.get("RSI", row.get("rsi_signal", 50.0)), 50.0)
    adx = safe_float(row.get("ADX", row.get("adx_signal", 20.0)), 20.0)

    atr = safe_float(row.get("ATR", np.nan), np.nan)
    if not np.isfinite(atr) or atr <= 0:
        return None

    # simplistic rejection proxy: distance from AVWAP
    dist_atr = (close - avwap) / atr

    if dist_atr >= 0.25 and rsi >= 52 and adx >= 18:
        side = "LONG"
        entry = close
        sl = close - 0.6 * atr
        tgt = close + 1.0 * atr
        q = float(min(1.0, max(0.0, 0.4 + 0.2 * dist_atr + 0.01 * (adx - 18))))
        return side, entry, sl, tgt, q

    if dist_atr <= -0.25 and rsi <= 48 and adx >= 18:
        side = "SHORT"
        entry = close
        sl = close + 0.6 * atr
        tgt = close - 1.0 * atr
        q = float(min(1.0, max(0.0, 0.4 + 0.2 * abs(dist_atr) + 0.01 * (adx - 18))))
        return side, entry, sl, tgt, q

    return None


def detect_signal(row: pd.Series, strategy_modules: Optional[dict]) -> Optional[Tuple[str, float, float, float, float, str]]:
    """
    Attempt to detect signal using an external strategy module first,
    otherwise fallback to builtin detector.

    Returns (side, entry, sl, tgt, quality_score, notes)
    """
    # If you have a module with a function like: detect_live_signal(row_dict) -> dict
    if strategy_modules:
        for name, mod in strategy_modules.items():
            fn = getattr(mod, "detect_live_signal", None)
            if callable(fn):
                try:
                    out = fn(row.to_dict())
                    if out and out.get("side") in ("LONG", "SHORT"):
                        side = str(out["side"]).upper()
                        entry = safe_float(out.get("entry_price"), np.nan)
                        sl = safe_float(out.get("sl_price"), np.nan)
                        tgt = safe_float(out.get("target_price"), np.nan)
                        q = safe_float(out.get("quality_score", row.get("quality_score", 0.0)), 0.0)
                        if np.isfinite(entry) and np.isfinite(sl) and np.isfinite(tgt):
                            return side, float(entry), float(sl), float(tgt), float(q), f"module:{name}"
                except Exception:
                    # ignore module errors; continue to next
                    pass

    # fallback
    out = detect_avwap_signal_builtin(row)
    if out is None:
        return None
    side, entry, sl, tgt, q = out
    return side, entry, sl, tgt, q, "builtin"


# ----------------------------
# Scanner
# ----------------------------
def scan_all_tickers(
    data_dir: str,
    today: date,
    position_size_rs: float,
    leverage: float,
    ml_threshold: float,
    signals_dir: str,
    strategy_modules: Optional[dict] = None,
    meta_filter: Optional[MetaLabelFilter] = None,
) -> List[LiveSignal]:
    """
    Scan all tickers in data_dir for today's signals.
    Uses external module detection if available, else built-in detection.
    Applies ML meta-filter (threshold gating + sizing).
    """
    tickers = list_tickers(data_dir)
    if not tickers:
        return []

    # Ensure meta_filter exists
    if meta_filter is None:
        meta_filter = MetaLabelFilter(MetaFilterConfig(pwin_threshold=ml_threshold))
    else:
        # respect CLI override threshold
        meta_filter.cfg.pwin_threshold = float(ml_threshold)

    signals: List[LiveSignal] = []
    ts = now_ist()

    for tkr in tickers:
        row = read_latest_row_for_date(Path(data_dir) / f"{tkr}.parquet", today)
        if row is None:
            continue

        sig = detect_signal(row, strategy_modules=strategy_modules)
        if sig is None:
            continue

        side, entry, sl, tgt, qscore, notes0 = sig

        # build features for ML p_win
        feat = build_signal_features(row, side=side)
        # keep quality_score from detector if it was computed there
        feat["quality_score"] = float(qscore)

        # Predict p_win and decide multiplier
        p_win = float(meta_filter.predict_pwin(feat))
        conf_mult = float(meta_filter.confidence_multiplier(p_win))

        # Gate: skip if multiplier <= 0
        if conf_mult <= 0.0:
            continue

        qty = compute_qty(entry_price=entry, position_size_rs=position_size_rs, leverage=leverage, confidence_multiplier=conf_mult)
        if qty <= 0:
            continue

        signals.append(LiveSignal(
            signal_id=f"{today.isoformat()}_{tkr}_{side}_{uuid.uuid4().hex[:8]}",
            logtime_ist=ts.strftime("%Y-%m-%d %H:%M:%S"),
            ticker=tkr,
            side=side,
            entry_price=float(entry),
            sl_price=float(sl),
            target_price=float(tgt),
            quality_score=float(qscore),
            atr_pct_signal=safe_float(row.get("atr_pct_signal", row.get("atr_pct", 0.0)), 0.0),
            rsi_signal=safe_float(row.get("rsi_signal", row.get("RSI", row.get("rsi", 50.0))), 50.0),
            adx_signal=safe_float(row.get("adx_signal", row.get("ADX", row.get("adx", 20.0))), 20.0),
            p_win=p_win,
            ml_threshold=float(ml_threshold),
            confidence_multiplier=conf_mult,
            quantity=int(qty),
            notes=f"{notes0}|ml:{'model' if meta_filter.model is not None else 'heur'}",
        ))

    # Write to CSV (daily)
    out_csv = Path(signals_dir) / f"signals_{today.isoformat()}.csv"
    if signals:
        write_signals_csv(out_csv, signals)

    return signals


def run_single_scan(args: argparse.Namespace, meta_filter: MetaLabelFilter, strategy_modules: Optional[dict]) -> None:
    dt = now_ist()
    today = dt.date()

    if not is_market_time(dt):
        return
    if is_after_cutoff(dt):
        return

    sigs = scan_all_tickers(
        data_dir=args.data_dir,
        today=today,
        position_size_rs=float(args.position_size),
        leverage=float(args.leverage),
        ml_threshold=float(args.ml_threshold),
        signals_dir=args.signals_dir,
        strategy_modules=strategy_modules,
        meta_filter=meta_filter,
    )

    if args.verbose:
        print(f"[{dt.strftime('%H:%M:%S')}] signals={len(sigs)}  (model={'YES' if meta_filter.model is not None else 'NO'})")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=DEFAULT_15M_DATA_DIR, help="15m indicator parquet folder")
    ap.add_argument("--signals-dir", default=DEFAULT_SIGNALS_DIR, help="Output folder for live signals CSV")
    ap.add_argument("--position-size", type=float, default=DEFAULT_POSITION_SIZE_RS, help="Base margin/capital per trade (Rs)")
    ap.add_argument("--leverage", type=float, default=DEFAULT_LEVERAGE, help="Intraday leverage multiplier")
    ap.add_argument("--ml-threshold", type=float, default=DEFAULT_ML_THRESHOLD, help="p_win threshold for ML gating")
    ap.add_argument("--strategy-module", action="append", default=[], help="Optional: path(s) to strategy module(s) with detect_live_signal()")
    ap.add_argument("--once", action="store_true", help="Run a single scan and exit")
    ap.add_argument("--verbose", action="store_true", help="Print scan summary every run")
    args = ap.parse_args()

    ensure_dir(Path(args.signals_dir))

    # Load ML filter
    cfg = MetaFilterConfig(pwin_threshold=float(args.ml_threshold))
    meta_filter = MetaLabelFilter(cfg)

    if args.verbose:
        mp = Path(cfg.model_path)
        fp = Path(cfg.feature_path)
        print(f"ML threshold: {cfg.pwin_threshold}")
        print(f"Model files: {mp} ({'FOUND' if mp.exists() else 'MISSING'}), {fp} ({'FOUND' if fp.exists() else 'MISSING'})")
        print(f"Using real model: {'YES' if meta_filter.model is not None else 'NO (heuristic fallback)'}")

    # Optional strategy modules
    strategy_modules = load_strategy_modules(args.strategy_module)

    if args.once:
        run_single_scan(args, meta_filter, strategy_modules)
        return

    # Continuous loop during market time
    while True:
        dt = now_ist()

        if not is_market_time(dt):
            # Sleep until near market open
            tomorrow = dt.date() if (dt.hour, dt.minute) < MARKET_OPEN else (dt.date() + timedelta(days=1))
            next_open = IST.localize(datetime(tomorrow.year, tomorrow.month, tomorrow.day, MARKET_OPEN[0], MARKET_OPEN[1], 0))
            sleep_s = max(30.0, (next_open - dt).total_seconds())
            if args.verbose:
                print(f"Outside market hours. Sleeping {int(sleep_s)}s until open...")
            time.sleep(min(sleep_s, 300))  # wake periodically
            continue

        # In market time
        try:
            run_single_scan(args, meta_filter, strategy_modules)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"[ERROR] scan failed: {e}", file=sys.stderr)

        # sleep until next boundary
        nxt = align_to_next_15m(dt)
        sleep_s = max(5.0, (nxt - now_ist()).total_seconds())
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
