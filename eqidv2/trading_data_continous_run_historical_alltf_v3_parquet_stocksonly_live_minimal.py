# -*- coding: utf-8 -*-
"""
Unified Zerodha (KiteConnect) data fetch + indicator generator
**LIVE MINIMAL profile** for intraday 5-minute and 15-minute timeframes
(Parquet storage).

This is a trimmed / rewritten version of:
  trading_data_continous_run_historical_alltf_v3_parquet_stocksonly.py

What was removed:
- daily / weekly / 1h / 3h modes
- 3h resampling pipeline
- daily/weekly cutoff logic
- all mode names except: 5min, 15min
- related directories and warmup settings

What remains:
- ETF universe loader (filtered_stocks_MIS_v2.py or stocks_tickers.txt)
- Kite session setup (api_key.txt + access_token.txt)
- Trading calendar helpers (weekends + optional holidays file)
- Robust missing/freshness detection for intraday candles
- Incremental fetching with warmup re-stabilization
- Minimal live/backtest indicator computation:
  RSI, ATR, EMA_20, EMA_50, EMA_200, Stoch_%K, Stoch_%D, ADX,
  and causal session-open AVWAP for 5-minute bars
- Parquet outputs + optional legacy CSV migration (read-only or delete after write)
- Reports for missing files / newly appended rows

Outputs (Parquet):
- stocks_indicators_5min_eq / <TICKER>_stocks_indicators_5min.parquet
- stocks_indicators_15min_eq / <TICKER>_stocks_indicators_15min.parquet

Usage examples:
- Fetch only 5-min:
    python trading_data_continous_run_historical_5m_15m_parquet.py 5min
- Fetch only 15-min:
    python trading_data_continous_run_historical_5m_15m_parquet.py 15min
- Fetch both:
    python trading_data_continous_run_historical_5m_15m_parquet.py all

Notes:
- Intraday timestamps can be stored as candle "end" (recommended) or "start".
- By default, the script skips tickers that are already "fresh".
"""

import os
import sys
import time as _time
import json
import re
import argparse
import logging
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, date, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import pytz
from kiteconnect import KiteConnect, exceptions as kexc
from requests import exceptions as reqexc
from eqidv2_runtime_paths import CACHE_15M_DIR
from eqidv2_runtime_paths import CACHE_5MIN_DIR
from eqidv2_runtime_paths import DATA_5M_DIR
from eqidv2_runtime_paths import DATA_15M_DIR
from eqidv2_runtime_paths import runtime_dir

# ========= GLOBAL CONFIG =========

IST_TZ = pytz.timezone("Asia/Kolkata")

# Directories (only intraday)
DIRS = {
    "5min":   {"cache": str(CACHE_5MIN_DIR),  "out": str(DATA_5M_DIR)},
    "15min":  {"cache": str(CACHE_15M_DIR),   "out": str(DATA_15M_DIR)},
}
for cfg in DIRS.values():
    os.makedirs(cfg["cache"], exist_ok=True)
    os.makedirs(cfg["out"], exist_ok=True)

VALID_MODES = ("5min", "15min")
DEFAULT_MAX_WORKERS = 6
DEFAULT_FETCH_RETRY_BASE_SEC = float(os.getenv("EQIDV2_FETCH_RETRY_BASE_SEC", "0.8"))
DEFAULT_FETCH_RATE_LIMIT_BACKOFF_BASE_SEC = float(os.getenv("EQIDV2_FETCH_RATE_LIMIT_BACKOFF_BASE_SEC", "2.0"))
DEFAULT_FETCH_TIMEOUT_BACKOFF_BASE_SEC = float(os.getenv("EQIDV2_FETCH_TIMEOUT_BACKOFF_BASE_SEC", "1.2"))
DEFAULT_FETCH_PACE_SEC = float(os.getenv("EQIDV2_FETCH_PACE_SEC", "0.50"))
DEFAULT_KITE_REQUEST_TIMEOUT_SEC = float(
    os.getenv("EQIDV2_KITE_REQUEST_TIMEOUT_SEC", os.getenv("EQIDV2_5M_KITE_TIMEOUT_SEC", "12"))
)
DEFAULT_LOG_UPDATED_TICKERS = str(os.getenv("EQIDV2_LOG_UPDATED_TICKERS", "0")).strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_LOG_UPDATED_TICKERS_TOP_N = max(0, int(os.getenv("EQIDV2_LOG_UPDATED_TICKERS_TOP_N", "8")))
DEFAULT_SAVE_NEW_ROWS_REPORTS = str(os.getenv("EQIDV2_SAVE_NEW_ROWS_REPORTS", "0")).strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_VERIFY_SAMPLE_SIZE = max(0, int(os.getenv("EQIDV2_VERIFY_SAMPLE_SIZE", "0")))
DEFAULT_LOG_INDICATOR_QUALITY = str(os.getenv("EQIDV2_LOG_INDICATOR_QUALITY", "0")).strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_DOWNCAST_NUMERIC = str(os.getenv("EQIDV2_DOWNCAST_NUMERIC", "0")).strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_PARQUET_COMPRESSION = str(os.getenv("EQIDV2_PARQUET_COMPRESSION", "none")).strip().lower()
DEFAULT_ENFORCE_5MIN_SESSION_COMPLETENESS = str(
    os.getenv("EQIDV2_5M_ENFORCE_SESSION_COMPLETENESS", "1")
).strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_5M_LIVE_SLIM_MODE = str(
    os.getenv("EQIDV2_5M_LIVE_SLIM_MODE", "0")
).strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_5M_LIVE_SLIM_CALENDAR_DAYS = max(
    7,
    int(os.getenv("EQIDV2_5M_LIVE_SLIM_CALENDAR_DAYS", "21")),
)
DEFAULT_5M_SYNTHETIC_GAP_FILL = str(
    os.getenv("EQIDV2_5M_SYNTHETIC_GAP_FILL", "1")
).strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_5M_PROVISIONAL_DUPLICATE_RETRY = str(
    os.getenv("EQIDV2_5M_PROVISIONAL_DUPLICATE_RETRY", "1")
).strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_5M_PROVISIONAL_SETTLE_SEC = max(
    0.0,
    float(os.getenv("EQIDV2_5M_PROVISIONAL_SETTLE_SEC", "18")),
)
DEFAULT_5M_PROVISIONAL_RETRY_ATTEMPTS = max(
    1,
    int(os.getenv("EQIDV2_5M_PROVISIONAL_RETRY_ATTEMPTS", "3")),
)
DEFAULT_5M_PROVISIONAL_RETRY_INTERVAL_SEC = max(
    0.0,
    float(os.getenv("EQIDV2_5M_PROVISIONAL_RETRY_INTERVAL_SEC", "2")),
)
DEFAULT_FNO_5M_FROM_1M = str(
    os.getenv("EQIDV2_FNO_5M_FROM_1M", "0")
).strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_FNO_UNIVERSE_PATH = Path(
    os.getenv(
        "EQIDV2_FNO_UNIVERSE_PATH",
        str(runtime_dir("fno_oi") / "universe" / "latest_near_month.parquet"),
    )
)
DEFAULT_SESSION_COMPLETENESS_LOG_LIMIT = max(
    1,
    int(os.getenv("EQIDV2_5M_SESSION_COMPLETENESS_LOG_LIMIT", "6")),
)

# Market timing (IST)
MARKET_OPEN_TIME = time(9, 15)
MARKET_CLOSE_TIME_INTRADAY = time(15, 30)      # last intraday candle end (5m/15m)

# Candle-end timestamps for intraday
DEFAULT_INTRADAY_TIMESTAMP = "end"  # "end" or "start"

# Incremental fetch warmup bars (retained only for compatibility/documentation).
# The live 5-minute and 15-minute flows are intentionally append-only.
WARMUP_BARS = {
    "5min":  600,
    "15min": 400,
}

_FNO_EQUITY_SYMBOLS: set[str] = set()
_FNO_UNIVERSE_MTIME_NS: int | None = None

# Token cache
SCRIPT_ROOT = Path(__file__).resolve().parent
TOKENS_CACHE_FILE = str(SCRIPT_ROOT / "stocks_tokens_cache.json")
TOKENS_CACHE_MAX_AGE_DAYS = 7
INVALID_SYMBOLS_FILE = str(SCRIPT_ROOT / "stocks_invalid_symbols.json")

# Optional NSE holidays file (one date per line or CSV column "date")
HOLIDAYS_FILE_DEFAULT = "nse_holidays.csv"

# ========= STORAGE (PARQUET) =========
MIGRATE_LEGACY_CSV = True
DELETE_LEGACY_CSV = False


class InvalidInstrumentTokenError(RuntimeError):
    """Raised when Kite rejects a cached instrument token as invalid."""


def _load_invalid_symbol_map() -> dict[str, dict[str, str]]:
    try:
        if not os.path.exists(INVALID_SYMBOLS_FILE):
            return {}
        raw = json.loads(Path(INVALID_SYMBOLS_FILE).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {}
        out: dict[str, dict[str, str]] = {}
        for key, value in raw.items():
            sym = str(key).strip().upper()
            if not sym:
                continue
            meta = value if isinstance(value, dict) else {"reason": str(value)}
            out[sym] = {str(k): str(v) for k, v in meta.items()}
        return out
    except Exception:
        return {}


def _load_invalid_symbols() -> set[str]:
    return set(_load_invalid_symbol_map().keys())


def _save_invalid_symbol_map(data: dict[str, dict[str, str]]) -> None:
    Path(INVALID_SYMBOLS_FILE).write_text(
        json.dumps(data, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _remove_symbols_from_token_cache(symbols: list[str] | set[str]) -> None:
    syms_u = {str(sym).strip().upper() for sym in symbols if str(sym).strip()}
    if not syms_u or not os.path.exists(TOKENS_CACHE_FILE):
        return
    try:
        cache = json.loads(Path(TOKENS_CACHE_FILE).read_text(encoding="utf-8"))
        if not isinstance(cache, dict):
            return
        changed = False
        for sym in syms_u:
            if sym in cache:
                cache.pop(sym, None)
                changed = True
        if changed:
            Path(TOKENS_CACHE_FILE).write_text(json.dumps(cache, indent=2), encoding="utf-8")
    except Exception:
        pass


def _quarantine_symbols(symbols: list[str] | set[str], logger: logging.Logger, reason: str) -> None:
    syms_u = sorted({str(sym).strip().upper() for sym in symbols if str(sym).strip()})
    if not syms_u:
        return
    try:
        data = _load_invalid_symbol_map()
        now_ist = datetime.now(IST_TZ).strftime("%Y-%m-%d %H:%M:%S%z")
        changed = False
        for sym in syms_u:
            prev = data.get(sym) or {}
            next_meta = {
                "reason": str(reason).strip() or "invalid_symbol",
                "noted_at": now_ist,
            }
            if prev != next_meta:
                data[sym] = next_meta
                changed = True
        if changed:
            _save_invalid_symbol_map(data)
        _remove_symbols_from_token_cache(syms_u)
        logger.warning(
            "Quarantined %d symbol(s) from live fetch: %s",
            len(syms_u),
            ", ".join(syms_u[:20]),
        )
    except Exception as exc:
        logger.warning("Failed to quarantine invalid symbols %s: %s", ", ".join(syms_u[:20]), exc)


def _filter_quarantined_symbols(
    tickers: list[str],
    token_map: dict[str, int],
    logger: logging.Logger,
) -> tuple[list[str], dict[str, int]]:
    invalid_symbols = _load_invalid_symbols()
    if not invalid_symbols:
        return tickers, token_map
    filtered_tickers = [t for t in tickers if t.upper() not in invalid_symbols]
    removed = sorted({t.upper() for t in tickers if t.upper() in invalid_symbols})
    if removed:
        logger.warning(
            "Skipping %d quarantined symbol(s) from live fetch universe: %s",
            len(removed),
            ", ".join(removed[:20]),
        )
    filtered_token_map = {k: v for k, v in token_map.items() if k.upper() not in invalid_symbols}
    return filtered_tickers, filtered_token_map


def _is_invalid_token_error(ex: Exception) -> bool:
    text = str(ex).strip().lower()
    return "invalid token" in text


# ========= LOGGING =========

def setup_logger() -> logging.Logger:
    logger = logging.getLogger("stocks_fetcher_live_minimal")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler("stocks_fetcher_live_minimal_run.log", mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


# ========= ETF UNIVERSE =========

import importlib
from types import ModuleType
from typing import Optional

def _normalize_ticker_list(obj) -> list[str]:
    if obj is None:
        return []
    if isinstance(obj, dict):
        arr = list(obj.keys())
    elif isinstance(obj, (set, list, tuple)):
        arr = list(obj)
    else:
        if isinstance(obj, str):
            arr = re.split(r"[\s,;]+", obj.strip())
        else:
            try:
                arr = list(obj)  # type: ignore
            except Exception:
                arr = [obj]

    tickers: list[str] = []
    for x in arr:
        s = str(x).strip().upper()
        if not s:
            continue
        s = s.replace("NSE:", "").replace("BSE:", "")
        tickers.append(s)

    return sorted(set(tickers))


def load_stocks_universe(logger: logging.Logger) -> tuple[list[str], dict[str, int]]:
    """
    Universe loader (ETF-ready):
    - Preferred: filtered_fno_MIS_v2.py with either:
        - stocks_tokens = {SYMBOL: TOKEN, ...}
        - selected_stocks = [...]
    - Legacy fallback: filtered_stocks_MIS_v2.py
    - Fallback: stocks_tickers.txt (one symbol per line)
    """
    cwd = Path.cwd().resolve()
    script_dir = Path(__file__).resolve().parent
    parent_dir = script_dir.parent

    for p in (str(script_dir), str(parent_dir)):
        if p not in sys.path:
            sys.path.insert(0, p)

    token_map: dict[str, int] = {}
    mod: Optional[ModuleType] = None
    module_name = ""

    for candidate_module in ("filtered_fno_MIS_v2", "filtered_stocks_MIS_v2"):
        try:
            mod = importlib.import_module(candidate_module)
            module_name = candidate_module
            break
        except Exception:
            mod = None

    if mod is not None:
        if hasattr(mod, "stocks_tokens") and isinstance(getattr(mod, "stocks_tokens"), dict):
            raw = getattr(mod, "stocks_tokens")
            try:
                token_map = {str(k).strip().upper(): int(v) for k, v in raw.items() if str(k).strip()}
                tickers = sorted(token_map.keys())
                if tickers:
                    logger.info("Loaded %d symbols from %s.stocks_tokens", len(tickers), module_name)
                    return _filter_quarantined_symbols(tickers, token_map, logger)
            except Exception:
                pass

        if hasattr(mod, "selected_stocks"):
            ss = getattr(mod, "selected_stocks")
            if isinstance(ss, dict):
                tickers = _normalize_ticker_list(ss)
                try:
                    if ss and all(isinstance(v, (int, float)) for v in ss.values()):
                        token_map = {str(k).strip().upper(): int(v) for k, v in ss.items() if str(k).strip()}
                        tickers = sorted(token_map.keys())
                except Exception:
                    pass
                if tickers:
                    logger.info("Loaded %d symbols from %s.selected_stocks", len(tickers), module_name)
                    return _filter_quarantined_symbols(tickers, token_map, logger)

            tickers = _normalize_ticker_list(ss)
            if tickers:
                logger.info("Loaded %d symbols from %s.selected_stocks", len(tickers), module_name)
                return _filter_quarantined_symbols(tickers, token_map, logger)

    for base in (cwd, script_dir, parent_dir):
        f = base / "stocks_tickers.txt"
        if f.exists():
            arr = [x.strip().upper() for x in f.read_text(encoding="utf-8", errors="ignore").splitlines() if x.strip()]
            tickers = _normalize_ticker_list(arr)
            if tickers:
                logger.info("Loaded %d symbols from %s", len(tickers), str(f))
                return _filter_quarantined_symbols(tickers, token_map, logger)

    raise RuntimeError(
        "Could not load symbols.\n"
        "Fix options:\n"
        "  1) Ensure filtered_fno_MIS_v2.py is importable and define either:\n"
        "       - stocks_tokens = {SYMBOL: TOKEN, ...}   OR\n"
        "       - selected_stocks = [SYMBOL, ...] / {SYMBOL, ...} / {SYMBOL: TOKEN, ...}\n"
        "  2) Or keep legacy filtered_stocks_MIS_v2.py importable.\n"
        "  3) Or create stocks_tickers.txt (one symbol per line) in cwd / script dir / parent dir.\n\n"
        f"Diagnostics:\n  cwd={cwd}\n  script_dir={script_dir}\n  parent_dir={parent_dir}"
    )


# ========= KITE SESSION =========

def setup_kite_session() -> KiteConnect:
    with open("access_token.txt", "r", encoding="utf-8") as f:
        access_token = f.read().strip()
    with open("api_key.txt", "r", encoding="utf-8") as f:
        api_key = f.read().split()[0]
    kite_local = KiteConnect(api_key=api_key, timeout=DEFAULT_KITE_REQUEST_TIMEOUT_SEC)
    kite_local.set_access_token(access_token)
    return kite_local


# ========= HOLIDAYS =========

def _read_holidays(path: str) -> set[date]:
    holidays: set[date] = set()
    if not path or not os.path.exists(path):
        return holidays

    try:
        if path.lower().endswith(".csv"):
            df = pd.read_csv(path)
            if "date" in df.columns:
                ds = pd.to_datetime(df["date"], errors="coerce").dropna()
                holidays.update(ds.dt.date.tolist())
            else:
                ds = pd.to_datetime(df.iloc[:, 0], errors="coerce").dropna()
                holidays.update(ds.dt.date.tolist())
        else:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if not s:
                        continue
                    d = pd.to_datetime(s, errors="coerce")
                    if pd.notna(d):
                        holidays.add(d.date())
    except Exception:
        return set()

    return holidays


# ========= TRADING CALENDAR HELPERS =========

def _is_trading_day(d: date, holidays: set[date]) -> bool:
    if d.weekday() >= 5:
        return False
    if d in holidays:
        return False
    return True

def _prev_trading_day(d: date, holidays: set[date]) -> date:
    x = d - timedelta(days=1)
    while not _is_trading_day(x, holidays):
        x -= timedelta(days=1)
    return x

def _round_down_session_anchored(ts: datetime, step_min: int) -> datetime:
    if ts.tzinfo is None:
        ts = IST_TZ.localize(ts)

    anchor = IST_TZ.localize(datetime(ts.year, ts.month, ts.day, 9, 15, 0))
    if ts <= anchor:
        return anchor.replace(second=0, microsecond=0)

    delta_min = int((ts - anchor).total_seconds() // 60)
    steps = max(0, delta_min // step_min)
    out = anchor + timedelta(minutes=steps * step_min)
    return out.replace(second=0, microsecond=0)

def last_completed_intraday_end(now_ist: datetime, step_min: int, holidays: set[date]) -> datetime:
    if now_ist.tzinfo is None:
        now_ist = IST_TZ.localize(now_ist)

    d = now_ist.date()

    if not _is_trading_day(d, holidays):
        d = _prev_trading_day(d, holidays)
        close_dt = IST_TZ.localize(datetime(d.year, d.month, d.day, 15, 30, 0))
        return _round_down_session_anchored(close_dt, step_min)

    if now_ist.time() < MARKET_OPEN_TIME:
        d = _prev_trading_day(d, holidays)
        close_dt = IST_TZ.localize(datetime(d.year, d.month, d.day, 15, 30, 0))
        return _round_down_session_anchored(close_dt, step_min)

    if now_ist.time() >= MARKET_CLOSE_TIME_INTRADAY:
        close_dt = IST_TZ.localize(datetime(d.year, d.month, d.day, 15, 30, 0))
        return _round_down_session_anchored(close_dt, step_min)

    return _round_down_session_anchored(now_ist, step_min)


# ========= START DATE PER MODE =========

def get_start_date(mode: str, now_ist: datetime) -> datetime:
    if now_ist.tzinfo is None:
        now_ist = IST_TZ.localize(now_ist)

    if mode.lower().strip() == "5min" and DEFAULT_5M_LIVE_SLIM_MODE:
        anchor = (now_ist - timedelta(days=DEFAULT_5M_LIVE_SLIM_CALENDAR_DAYS)).date()
        return IST_TZ.localize(datetime(anchor.year, anchor.month, anchor.day, 0, 0, 0))

    # Keep your original intraday start anchor behaviour
    return IST_TZ.localize(datetime(2025, 8, 25, 0, 0, 0))


# ========= DATA HELPERS =========

def _to_ist(series_dt: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series_dt, errors="coerce")
    if getattr(dt.dt, "tz", None) is None:
        return dt.dt.tz_localize(IST_TZ)
    return dt.dt.tz_convert(IST_TZ)

def _ensure_parquet_engine():
    try:
        import pyarrow  # noqa: F401
    except Exception as e:
        raise RuntimeError(
            "Parquet storage requires 'pyarrow'.\n"
            "Install it once:  pip install pyarrow\n"
            f"Original import error: {e}"
        ) from e

def _read_last_ts_fast_parquet(path: str):
    try:
        _ensure_parquet_engine()
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(path)
        md = pf.metadata
        if md is None or md.num_rows <= 0:
            return None

        last_rg = md.num_row_groups - 1
        if last_rg < 0:
            return None

        table = pf.read_row_group(last_rg, columns=["date"])
        if table.num_rows <= 0:
            return None

        col = table.column(0)
        val = col[col.length() - 1].as_py()
        ts = pd.to_datetime(val, errors="coerce")
        if pd.isna(ts):
            return None

        if ts.tzinfo is None:
            ts = ts.tz_localize(IST_TZ)
        else:
            ts = ts.tz_convert(IST_TZ)
        return ts
    except Exception:
        return None

def _read_last_ts_fast_csv(path: str):
    """Legacy CSV tail reader (kept only for migration)."""
    try:
        with open(path, "rb") as f:
            header = f.readline().decode("utf-8", errors="ignore").strip()
            if not header:
                return None
            cols = [c.strip().strip('"') for c in header.split(",")]
            if "date" not in cols:
                return None
            date_idx = cols.index("date")

            f.seek(0, os.SEEK_END)
            file_size = f.tell()
            if file_size <= 0:
                return None

            block = 8192
            data = b""
            offset = 0
            while file_size - offset > 0 and len(data.splitlines()) < 8:
                read_sz = min(block, file_size - offset)
                offset += read_sz
                f.seek(file_size - offset)
                chunk = f.read(read_sz)
                data = chunk + data

            lines = data.splitlines()
            for raw in reversed(lines):
                if not raw.strip():
                    continue
                try:
                    line = raw.decode("utf-8", errors="ignore")
                except Exception:
                    continue
                parts = [p.strip().strip('"') for p in line.split(",")]
                if len(parts) <= date_idx:
                    continue
                ds = parts[date_idx]
                ts = pd.to_datetime(ds, errors="coerce")
                if pd.isna(ts):
                    continue
                if ts.tzinfo is None:
                    ts = ts.tz_localize(IST_TZ)
                else:
                    ts = ts.tz_convert(IST_TZ)
                return ts
    except Exception:
        return None
    return None

def _read_last_ts_from_store(path: str):
    ext = str(Path(path).suffix).lower()
    if ext == ".parquet":
        return _read_last_ts_fast_parquet(path)
    if ext == ".csv":
        return _read_last_ts_fast_csv(path)
    ts = _read_last_ts_fast_parquet(path)
    if ts is not None:
        return ts
    return _read_last_ts_fast_csv(path)

def _intraday_end_shift_minutes(interval: str) -> int:
    return {"minute": 1, "5minute": 5, "15minute": 15}.get(interval, 0)

def _maybe_convert_existing_intraday_to_end(df: pd.DataFrame, step_min: int) -> pd.DataFrame:
    if df.empty or "date" not in df.columns:
        return df
    s = pd.to_datetime(df["date"], errors="coerce")
    if s.isna().all():
        return df
    s = _to_ist(s)
    min_ts = s.min()
    if "opening_snapshot" in df.columns:
        opening_snapshot = (
            pd.to_numeric(df["opening_snapshot"], errors="coerce").fillna(0).ne(0)
            | df["opening_snapshot"].astype(str).str.strip().str.lower().isin({"true", "yes", "on"})
        )
        if opening_snapshot.any():
            return df
    if (min_ts.hour, min_ts.minute) == (9, 15):
        s = s + pd.Timedelta(minutes=step_min)
        df = df.copy()
        df["date"] = s
    return df


# ========= PER-TICKER FRESHNESS (FAST SKIP) =========

def expected_last_stamp(mode: str, now_ist: datetime, holidays: set[date], intraday_ts: str) -> dict:
    mode = mode.lower().strip()

    if now_ist.tzinfo is None:
        now_ist = IST_TZ.localize(now_ist)

    step_map = {"5min": 5, "15min": 15}
    step = step_map.get(mode, 0)

    exp_end = last_completed_intraday_end(now_ist, step, holidays)

    if intraday_ts.lower() == "start":
        exp_end = exp_end - timedelta(minutes=step)

    return {"kind": "ts", "value": exp_end, "step_min": step}

def _legacy_csv_path_for(parquet_path: str) -> str:
    return str(Path(parquet_path).with_suffix(".csv"))

def _resolve_existing_store_path(target_parquet_path: str) -> str:
    if os.path.exists(target_parquet_path):
        return target_parquet_path
    if MIGRATE_LEGACY_CSV:
        legacy = _legacy_csv_path_for(target_parquet_path)
        if os.path.exists(legacy):
            return legacy
    return target_parquet_path

def ticker_is_fresh(mode: str, out_path: str, now_ist: datetime, holidays: set[date], intraday_ts: str) -> bool:
    existing_path = _resolve_existing_store_path(out_path)
    if not os.path.exists(existing_path):
        return False

    last_ts = _read_last_ts_from_store(existing_path)
    return _ticker_is_fresh_from_last_ts(
        mode,
        existing_path,
        last_ts,
        now_ist,
        holidays,
        intraday_ts,
    )


def _ticker_is_fresh_from_last_ts(
    mode: str,
    existing_path: str,
    last_ts,
    now_ist: datetime,
    holidays: set[date],
    intraday_ts: str,
) -> bool:
    """Evaluate freshness without rereading a last timestamp already in hand."""
    if last_ts is None:
        return False

    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize(IST_TZ)
    else:
        last_ts = last_ts.tz_convert(IST_TZ)

    spec = expected_last_stamp(mode, now_ist, holidays, intraday_ts)
    exp_ts: datetime = spec["value"]
    if exp_ts.tzinfo is None:
        exp_ts = IST_TZ.localize(exp_ts)

    tol = timedelta(seconds=1)
    step_min = int(spec.get("step_min", 0) or 0)
    step_td = timedelta(minutes=step_min) if step_min > 0 else timedelta(0)

    if last_ts >= (exp_ts - tol):
        if mode.lower().strip() == "5min" and DEFAULT_ENFORCE_5MIN_SESSION_COMPLETENESS:
            missing_session = _missing_5min_session_stamps_from_store(existing_path, exp_ts)
            if missing_session:
                return False
        return True
    if step_min > 0:
        if (last_ts + step_td) >= (exp_ts - tol):
            return True
        if (last_ts - step_td) >= (exp_ts - tol):
            return True

    return False

def missing_spec(mode: str, out_path: str, now_ist: datetime, holidays: set[date], intraday_ts: str) -> dict:
    existing_path = _resolve_existing_store_path(out_path)

    spec = expected_last_stamp(mode, now_ist, holidays, intraday_ts)

    if not os.path.exists(existing_path):
        return {"kind": "file_missing", "last_ts": None, "expected": spec}

    last_ts = _read_last_ts_from_store(existing_path)
    if last_ts is None:
        return {"kind": "rows_missing", "last_ts": None, "expected": spec}

    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize(IST_TZ)
    else:
        last_ts = last_ts.tz_convert(IST_TZ)

    if _ticker_is_fresh_from_last_ts(
        mode,
        existing_path,
        last_ts,
        now_ist,
        holidays,
        intraday_ts,
    ):
        return {"kind": "fresh", "last_ts": last_ts, "expected": spec}

    return {"kind": "rows_missing", "last_ts": last_ts, "expected": spec}


# ========= FETCHERS =========

def fetch_historical_generic(
    kite: KiteConnect,
    token: int,
    start_dt_ist: datetime,
    end_dt_ist: datetime,
    interval: str,
    chunk_days: int,
    step_td: timedelta,
    logger: logging.Logger,
    intraday_ts: str
) -> pd.DataFrame:
    end = end_dt_ist if end_dt_ist.tzinfo else IST_TZ.localize(end_dt_ist)
    s = start_dt_ist if start_dt_ist.tzinfo else IST_TZ.localize(start_dt_ist)

    chunk = timedelta(days=chunk_days)
    frames = []

    MAX_RETRIES = 4
    SLEEP_BETWEEN_CALLS = DEFAULT_FETCH_PACE_SEC

    def _is_rate_limited_error(ex: Exception) -> bool:
        text = str(ex).strip().lower()
        return (
            "too many requests" in text
            or "rate limit" in text
            or "http 429" in text
            or " 429 " in f" {text} "
        )

    def _timeout_backoff(attempt: int) -> float:
        return max(
            DEFAULT_FETCH_RETRY_BASE_SEC * (2 ** (attempt - 1)),
            DEFAULT_FETCH_TIMEOUT_BACKOFF_BASE_SEC * (2 ** (attempt - 1)),
        )

    while s < end:
        e = min(s + chunk, end)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                raw = kite.historical_data(token, s, e, interval)
                df = pd.DataFrame(raw)
                if df.empty:
                    break

                df["date"] = _to_ist(df["date"])

                if intraday_ts.lower() == "end":
                    shift_min = _intraday_end_shift_minutes(interval)
                    if shift_min > 0:
                        df["date"] = df["date"] + pd.Timedelta(minutes=shift_min)

                frames.append(df)
                break
            except (kexc.NetworkException, kexc.DataException, kexc.TokenException, kexc.InputException) as ex:
                if _is_invalid_token_error(ex):
                    logger.warning("Failed chunk %s → %s (%s): %s", s, e, interval, ex)
                    raise InvalidInstrumentTokenError(str(ex)) from ex
                backoff_sec = DEFAULT_FETCH_RETRY_BASE_SEC * (2 ** (attempt - 1))
                if _is_rate_limited_error(ex):
                    backoff_sec = max(
                        backoff_sec,
                        DEFAULT_FETCH_RATE_LIMIT_BACKOFF_BASE_SEC * (2 ** (attempt - 1)),
                    )
                if attempt == MAX_RETRIES:
                    logger.warning("Failed chunk %s â†’ %s (%s): %s", s, e, interval, ex)
                else:
                    _time.sleep(backoff_sec)
            except (reqexc.Timeout, reqexc.ConnectionError, TimeoutError) as ex:
                backoff_sec = _timeout_backoff(attempt)
                if attempt == MAX_RETRIES:
                    logger.warning("Failed chunk %s â†’ %s (%s): %s", s, e, interval, ex)
                else:
                    _time.sleep(backoff_sec)

        if SLEEP_BETWEEN_CALLS > 0 and e < end:
            _time.sleep(SLEEP_BETWEEN_CALLS)

        s = e + step_td

    if not frames:
        return pd.DataFrame()

    out = (
        pd.concat(frames, ignore_index=True)
          .drop_duplicates(subset="date")
          .sort_values("date")
          .reset_index(drop=True)
    )
    out = out[out["date"] <= end_dt_ist].reset_index(drop=True)
    return out

def fetch_historical_5min_df(kite, token, start_dt_ist, end_dt_ist, logger, intraday_ts):
    return fetch_historical_generic(kite, token, start_dt_ist, end_dt_ist, "5minute", 60, timedelta(minutes=5), logger, intraday_ts)


def _refresh_fno_equity_symbols(logger: logging.Logger) -> set[str]:
    """Refresh the mapped cash-equity subset once per universe revision."""
    global _FNO_EQUITY_SYMBOLS, _FNO_UNIVERSE_MTIME_NS
    if not DEFAULT_FNO_5M_FROM_1M:
        _FNO_EQUITY_SYMBOLS = set()
        _FNO_UNIVERSE_MTIME_NS = None
        return set()
    try:
        mtime_ns = DEFAULT_FNO_UNIVERSE_PATH.stat().st_mtime_ns
        if mtime_ns == _FNO_UNIVERSE_MTIME_NS and _FNO_EQUITY_SYMBOLS:
            return set(_FNO_EQUITY_SYMBOLS)
        universe = pd.read_parquet(
            DEFAULT_FNO_UNIVERSE_PATH, columns=["equity_symbol"]
        )
        symbols = {
            str(value).strip().upper()
            for value in universe["equity_symbol"].dropna()
            if str(value).strip()
        }
        if not symbols:
            raise RuntimeError("mapped equity set is empty")
        _FNO_EQUITY_SYMBOLS = symbols
        _FNO_UNIVERSE_MTIME_NS = mtime_ns
        logger.info(
            "[5MIN][FNO] Exact 1-minute aggregation enabled for %d mapped equities",
            len(symbols),
        )
    except Exception as exc:
        _FNO_EQUITY_SYMBOLS = set()
        _FNO_UNIVERSE_MTIME_NS = None
        logger.error(
            "[5MIN][FNO] Cannot load exact-aggregation universe %s: %s",
            DEFAULT_FNO_UNIVERSE_PATH,
            exc,
        )
    return set(_FNO_EQUITY_SYMBOLS)


def _aggregate_exact_minute_targets(
    minute: pd.DataFrame,
    targets: list[pd.Timestamp],
) -> pd.DataFrame:
    """Aggregate only targets backed by all five exact end-labelled minutes."""
    if minute is None or minute.empty or not targets:
        return pd.DataFrame()
    ohlcv_columns = ("open", "high", "low", "close", "volume")
    work = minute.copy()
    work["date"] = _to_ist(work["date"]).dt.floor("min")
    for column in ohlcv_columns:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work = work.drop_duplicates("date", keep="last").sort_values("date")
    rows: list[dict[str, object]] = []
    for raw_target in sorted(set(targets)):
        target = pd.Timestamp(raw_target).floor("min")
        expected = pd.date_range(
            target - pd.Timedelta(minutes=4),
            target,
            freq="1min",
        )
        bucket = work.loc[work["date"].isin(expected)].copy()
        if len(bucket) != 5 or set(bucket["date"]) != set(expected):
            continue
        values = bucket[list(ohlcv_columns)].to_numpy(dtype=float)
        if not np.isfinite(values).all():
            continue
        rows.append(
            {
                "date": target,
                "open": float(bucket.iloc[0]["open"]),
                "high": float(bucket["high"].max()),
                "low": float(bucket["low"].min()),
                "close": float(bucket.iloc[-1]["close"]),
                "volume": float(bucket["volume"].sum()),
                "gap_filled": 0,
                "source_1m_count": 5,
                "provisional_stale": 0,
                "opening_snapshot": False,
            }
        )
    return pd.DataFrame(rows)


def _fetch_exact_fno_5min_rows(
    ticker: str,
    kite: KiteConnect,
    token: int,
    requested_stamps: list[pd.Timestamp],
    logger: logging.Logger,
) -> pd.DataFrame:
    """Fetch mapped FnO equity minutes and return causal five-minute rows."""
    if not requested_stamps:
        return pd.DataFrame()
    requested = sorted({pd.Timestamp(stamp).floor("min") for stamp in requested_stamps})
    session_open = requested[0].normalize() + pd.Timedelta(hours=9, minutes=15)
    first_end = session_open + pd.Timedelta(minutes=5)
    real_targets = [stamp for stamp in requested if stamp >= first_end]
    fetch_targets = list(real_targets)
    if session_open in requested and first_end not in fetch_targets:
        fetch_targets.append(first_end)
    if not fetch_targets:
        return pd.DataFrame()

    latest_target = max(fetch_targets)
    retry_attempts = DEFAULT_5M_PROVISIONAL_RETRY_ATTEMPTS
    if latest_target > pd.Timestamp(datetime.now(IST_TZ)).floor("min"):
        retry_attempts = 1
    aggregated = pd.DataFrame()
    required_targets = set(fetch_targets)
    for attempt in range(1, retry_attempts + 1):
        minute = fetch_historical_generic(
            kite,
            int(token),
            (min(fetch_targets) - pd.Timedelta(minutes=5)).to_pydatetime(),
            latest_target.to_pydatetime(),
            "minute",
            55,
            timedelta(minutes=1),
            logger,
            "end",
        )
        refreshed = _aggregate_exact_minute_targets(minute, fetch_targets)
        if not refreshed.empty:
            aggregated = (
                pd.concat([aggregated, refreshed], ignore_index=True, sort=False)
                .drop_duplicates("date", keep="last")
                .sort_values("date")
                .reset_index(drop=True)
            )
        available_targets = set(aggregated.get("date", pd.Series(dtype="object")))
        if required_targets.issubset(available_targets):
            if attempt > 1:
                logger.info(
                    "[5MIN][FNO] %s exact minute targets settled on attempt %d",
                    ticker,
                    attempt,
                )
            break
        if attempt < retry_attempts:
            _time.sleep(DEFAULT_5M_PROVISIONAL_RETRY_INTERVAL_SEC)
    if aggregated.empty:
        logger.warning(
            "[5MIN][FNO] %s exact minute aggregation returned no complete target",
            ticker,
        )
        return aggregated

    output = aggregated.loc[aggregated["date"].isin(real_targets)].copy()
    if session_open in requested:
        opening_source = aggregated.loc[aggregated["date"].eq(first_end)]
        if not opening_source.empty:
            opening = opening_source.tail(1).copy()
            opening["date"] = session_open
            opening["source_1m_count"] = 0
            opening["opening_snapshot"] = True
            output = pd.concat([opening, output], ignore_index=True, sort=False)
    return output.sort_values("date").reset_index(drop=True)

def fetch_historical_15min_df(kite, token, start_dt_ist, end_dt_ist, logger, intraday_ts):
    return fetch_historical_generic(kite, token, start_dt_ist, end_dt_ist, "15minute", 120, timedelta(minutes=15), logger, intraday_ts)


# ========= INDICATORS =========
# (Copied as-is from your original script to preserve feature parity.)

def calculate_rsi(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def calculate_atr(df, period=14):
    prev_close = df["close"].shift(1)
    tr = pd.concat([(df["high"] - df["low"]),
                    (df["high"] - prev_close).abs(),
                    (df["low"] - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()

def calculate_macd(close, fast=12, slow=26, signal=9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - signal_line
    return macd, signal_line, hist

def calculate_bollinger_bands(close, period=20, up=2, dn=2):
    sma = close.rolling(period, min_periods=period).mean()
    std = close.rolling(period, min_periods=period).std()
    return sma + up * std, sma - dn * std

def _ma(x, window, kind="sma"):
    if kind == "ema":
        return x.ewm(span=window, adjust=False).mean()
    return x.rolling(window, min_periods=window).mean()

def calculate_stochastic_fast(df, k_period=14, d_period=3):
    low_min = df["low"].rolling(k_period, min_periods=k_period).min()
    high_max = df["high"].rolling(k_period, min_periods=k_period).max()
    rng = high_max - low_min
    k = pd.Series(0.0, index=df.index)
    valid = rng > 0
    k.loc[valid] = 100.0 * (df["close"].loc[valid] - low_min.loc[valid]) / rng.loc[valid]
    k = k.clip(0.0, 100.0)
    d = k.rolling(d_period, min_periods=d_period).mean().clip(0.0, 100.0)
    return k, d

def calculate_stochastic_slow(df, k_period=14, k_smooth=3, d_period=3, ma_kind="sma"):
    k_fast, _ = calculate_stochastic_fast(df, k_period=k_period, d_period=1)
    k_slow = _ma(k_fast, k_smooth, kind=ma_kind).clip(0.0, 100.0)
    d = _ma(k_slow, d_period, kind=ma_kind).clip(0.0, 100.0)
    return k_slow, d

def calculate_adx(df, period=14):
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    prev_high = high.shift(1)
    prev_low = low.shift(1)
    prev_close = close.shift(1)

    tr = pd.Series(
        np.maximum.reduce([
            (high - low).to_numpy(),
            (high - prev_close).abs().to_numpy(),
            (low - prev_close).abs().to_numpy(),
        ]),
        index=df.index,
    )

    up_move = high - prev_high
    down_move = prev_low - low

    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=df.index)

    alpha = 1.0 / float(period)
    atr = tr.ewm(alpha=alpha, adjust=False).mean()
    plus_dm_sm = plus_dm.ewm(alpha=alpha, adjust=False).mean()
    minus_dm_sm = minus_dm.ewm(alpha=alpha, adjust=False).mean()

    eps = 1e-10
    plus_di = 100.0 * (plus_dm_sm / (atr + eps))
    minus_di = 100.0 * (minus_dm_sm / (atr + eps))
    dx = 100.0 * (plus_di - minus_di).abs() / ((plus_di + minus_di) + eps)
    adx = dx.ewm(alpha=alpha, adjust=False).mean()
    return adx.clip(0, 100)

def calculate_vwap(df):
    # SESSION VWAP: reset each trading day, typical-price weighted. (Was a GLOBAL cumsum() that never
    # reset per day -> VWAP anchored to the first bar of history and drifting far from price, e.g.
    # 360ONE ~1106 vs price ~920. The backtest/live read-path recomputes session VWAP anyway; this
    # makes the STORED parquet VWAP column correct for the future too.)
    _tp = (df["high"] + df["low"] + df["close"]) / 3.0
    _vol = pd.to_numeric(df["volume"], errors="coerce").clip(lower=0).fillna(0.0)
    _d = pd.to_datetime(df["date"], errors="coerce")
    try:
        _day = _d.dt.tz_convert(IST_TZ).dt.date
    except (TypeError, AttributeError, NameError):
        _day = _d.dt.date
    _pv_cum = (_tp * _vol).groupby(_day).cumsum()
    _vol_cum = _vol.groupby(_day).cumsum()
    return _pv_cum / _vol_cum.where(_vol_cum != 0)


def calculate_anchored_vwap_5min(df: pd.DataFrame) -> pd.Series:
    """Return causal session-open AVWAP from completed, real 5-minute bars.

    The live store's 09:15 row is an opening snapshot, not a completed bar, so
    it must never seed the anchor. Synthetic/partial rows are similarly
    ineligible. Their AVWAP value is NaN, but later eligible bars continue the
    same session cumulative sums without any contribution from those rows.
    """
    typical = (
        pd.to_numeric(df["high"], errors="coerce")
        + pd.to_numeric(df["low"], errors="coerce")
        + pd.to_numeric(df["close"], errors="coerce")
    ) / 3.0
    volume = pd.to_numeric(df["volume"], errors="coerce").clip(lower=0)
    finite_typical = typical.notna() & np.isfinite(typical)
    finite_volume = volume.notna() & np.isfinite(volume)
    volume = volume.where(finite_volume, 0.0)

    timestamps = pd.to_datetime(df["date"], errors="coerce")
    try:
        timestamps_ist = timestamps.dt.tz_convert(IST_TZ)
    except (TypeError, AttributeError):
        timestamps_ist = timestamps.dt.tz_localize(IST_TZ)
    session = timestamps_ist.dt.date

    opening_snapshot = (
        (timestamps_ist.dt.hour == MARKET_OPEN_TIME.hour)
        & (timestamps_ist.dt.minute == MARKET_OPEN_TIME.minute)
    ).fillna(False)
    if "opening_snapshot" in df.columns:
        stored_opening = df["opening_snapshot"]
        stored_opening = (
            pd.to_numeric(stored_opening, errors="coerce").fillna(0).ne(0)
            | stored_opening.astype(str).str.strip().str.lower().isin({"true", "yes", "on"})
        )
        opening_snapshot |= stored_opening

    ineligible = opening_snapshot | ~finite_typical | ~finite_volume | timestamps_ist.isna()
    if "gap_filled" in df.columns:
        ineligible |= pd.to_numeric(df["gap_filled"], errors="coerce").fillna(0).ne(0)
    if "source_1m_count" in df.columns:
        ineligible |= pd.to_numeric(df["source_1m_count"], errors="coerce").fillna(0).ne(5)

    eligible = ~ineligible
    eligible_volume = volume.where(eligible, 0.0)
    cumulative_volume = eligible_volume.groupby(session).cumsum()
    cumulative_pv = (typical.fillna(0.0) * eligible_volume).groupby(session).cumsum()
    avwap = cumulative_pv / cumulative_volume.where(cumulative_volume > 0)
    return avwap.where(eligible)

def calculate_ema(close, span):
    return close.ewm(span=span, adjust=False).mean()

def calculate_cci(df, period=20):
    tp = (df["high"] + df["low"] + df["close"]) / 3
    sma = tp.rolling(period, min_periods=period).mean()
    mad = tp.rolling(period, min_periods=period).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
    return (tp - sma) / (0.015 * mad + 1e-10)

def calculate_mfi(df, period=14):
    tp = (df["high"] + df["low"] + df["close"]) / 3
    mf = tp * df["volume"]
    pos_mf = mf.where(tp.diff() > 0, 0)
    neg_mf = mf.where(tp.diff() < 0, 0)
    pos_sum = pos_mf.rolling(period, min_periods=period).sum()
    neg_sum = neg_mf.rolling(period, min_periods=period).sum().abs()
    return 100 - (100 / (1 + pos_sum / (neg_sum + 1e-10)))

def calculate_obv(df):
    close = df["close"].values
    volume = df["volume"].values
    direction = np.sign(np.diff(close, prepend=close[0]))
    direction[0] = 0
    return pd.Series(np.cumsum(direction * volume), index=df.index)

def add_standard_indicators(df):
    df["RSI"] = calculate_rsi(df["close"])
    df["ATR"] = calculate_atr(df)
    df["EMA_20"] = calculate_ema(df["close"], 20)
    df["EMA_50"] = calculate_ema(df["close"], 50)
    df["EMA_200"] = calculate_ema(df["close"], 200)
    return df


# ========= CHANGE FEATURES (intraday only) =========

def add_change_features_intraday(df: pd.DataFrame) -> pd.DataFrame:
    df["date_only"] = df["date"].dt.tz_convert(IST_TZ).dt.date
    df["Intra_Change"] = df.groupby("date_only")["close"].pct_change().mul(100.0)

    last_close_per_day = df.groupby("date_only", sort=True)["close"].last()
    prev_day_last_close = last_close_per_day.shift(1)

    df["Prev_Day_Close"] = df["date_only"].map(prev_day_last_close)
    df["Daily_Change"] = (df["close"] - df["Prev_Day_Close"]) / (df["Prev_Day_Close"] + 1e-10) * 100.0
    return df


# ========= TOKEN CACHE =========

def load_or_fetch_tokens(kite: KiteConnect, symbols: list[str], logger: logging.Logger, refresh: bool = False) -> dict[str, int]:
    invalid_symbols = _load_invalid_symbols()
    syms_u = sorted(
        {
            t.upper().strip()
            for t in symbols
            if t.strip() and t.upper().strip() not in invalid_symbols
        }
    )
    if not syms_u:
        return {}

    if (not refresh) and os.path.exists(TOKENS_CACHE_FILE):
        try:
            st = os.stat(TOKENS_CACHE_FILE)
            age_days = (datetime.now() - datetime.fromtimestamp(st.st_mtime)).days
            if age_days <= TOKENS_CACHE_MAX_AGE_DAYS:
                cache = json.loads(Path(TOKENS_CACHE_FILE).read_text(encoding="utf-8"))
                if isinstance(cache, dict) and all(t in cache for t in syms_u):
                    return {t: int(cache[t]) for t in syms_u}
        except Exception:
            pass

    logger.info("Fetching NSE instruments for token map (this can take time)...")
    ins = pd.DataFrame(kite.instruments("NSE"))
    tokens = ins[ins["tradingsymbol"].isin(syms_u)][["tradingsymbol", "instrument_token"]]
    mp = dict(zip(tokens["tradingsymbol"], tokens["instrument_token"]))
    missing_requested = sorted([t for t in syms_u if t not in mp])

    try:
        existing = {}
        if os.path.exists(TOKENS_CACHE_FILE):
            existing = json.loads(Path(TOKENS_CACHE_FILE).read_text(encoding="utf-8"))
            if not isinstance(existing, dict):
                existing = {}
        for sym in missing_requested:
            existing.pop(sym, None)
        existing.update({k: int(v) for k, v in mp.items()})
        Path(TOKENS_CACHE_FILE).write_text(json.dumps(existing, indent=2), encoding="utf-8")
    except Exception:
        pass

    if missing_requested:
        logger.warning(
            "Missing %d symbol(s) from current NSE instruments during token refresh: %s",
            len(missing_requested),
            ", ".join(missing_requested[:20]),
        )

    return {t: int(mp[t]) for t in syms_u if t in mp}


# ========= SAVE =========

def _stamp_opening_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    """Label the 09:15 session-open row in the live slim store.

    The live 5m store uses the hybrid convention: 09:15 is an opening snapshot,
    while 09:20..15:30 are completed end-stamped 5m candles. This additive
    column lets readers identify that row without changing OHLCV or indicators.
    """
    if df is None or df.empty or "date" not in df.columns:
        return df
    ts = pd.to_datetime(df["date"], errors="coerce")
    try:
        ts = ts.dt.tz_convert(IST_TZ)
    except (TypeError, AttributeError):
        try:
            ts = ts.dt.tz_localize(IST_TZ)
        except (TypeError, AttributeError):
            pass
    out = df.copy()
    out["opening_snapshot"] = (
        (ts.dt.hour == MARKET_OPEN_TIME.hour) & (ts.dt.minute == MARKET_OPEN_TIME.minute)
    ).fillna(False).astype(bool)
    return out


def _finalize_and_save(df: pd.DataFrame, out_path: str):
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    df = _stamp_opening_snapshot(df)
    ext = str(Path(out_path).suffix).lower()

    if ext == ".parquet":
        _ensure_parquet_engine()
        compression = None if DEFAULT_PARQUET_COMPRESSION in {"", "none", "off", "false", "0"} else DEFAULT_PARQUET_COMPRESSION
        # Keep the previous complete file visible until the replacement is
        # fully encoded. The scanner therefore sees either the old snapshot or
        # the new snapshot, never a partially-written Parquet file.
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=f".{Path(out_path).name}.",
                suffix=".tmp",
                dir=str(Path(out_path).parent),
                delete=False,
            ) as tmp:
                tmp_path = tmp.name
            df.to_parquet(tmp_path, engine="pyarrow", index=False, compression=compression)
            os.replace(tmp_path, out_path)
            tmp_path = None
        finally:
            if tmp_path is not None:
                try:
                    os.remove(tmp_path)
                except FileNotFoundError:
                    pass
        return

    df.to_csv(out_path, index=False)


# ========= INCREMENTAL LOAD + MERGE =========

def _load_existing_ohlc(out_path: str, intraday_ts: str, mode: str) -> pd.DataFrame:
    existing_path = _resolve_existing_store_path(out_path)
    if not os.path.exists(existing_path):
        return pd.DataFrame()

    try:
        keep_cols = [
            "date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "gap_filled",
            "opening_snapshot",
            "provisional_stale",
            "source_1m_count",
        ]
        ext = str(Path(existing_path).suffix).lower()

        if ext == ".parquet":
            _ensure_parquet_engine()
            try:
                df = pd.read_parquet(existing_path, columns=keep_cols, engine="pyarrow")
            except Exception:
                df = pd.read_parquet(existing_path, engine="pyarrow")
        else:
            df = pd.read_csv(existing_path)

        if df.empty or "date" not in df.columns:
            return pd.DataFrame()

        df["date"] = _to_ist(df["date"])

        if intraday_ts.lower() == "end":
            step = {"5min": 5, "15min": 15}[mode]
            df = _maybe_convert_existing_intraday_to_end(df, step)

        if mode.lower().strip() == "5min" and DEFAULT_5M_LIVE_SLIM_MODE:
            cutoff_date = (datetime.now(IST_TZ) - timedelta(days=DEFAULT_5M_LIVE_SLIM_CALENDAR_DAYS)).date()
            df = df[df["date"].dt.tz_convert(IST_TZ).dt.date >= cutoff_date].copy()

        if "gap_filled" not in df.columns:
            df["gap_filled"] = 0
        else:
            df["gap_filled"] = pd.to_numeric(df["gap_filled"], errors="coerce").fillna(0).astype(int)
        if "provisional_stale" not in df.columns:
            df["provisional_stale"] = 0
        else:
            df["provisional_stale"] = (
                pd.to_numeric(df["provisional_stale"], errors="coerce").fillna(0).astype(int)
            )
        if "source_1m_count" in df.columns:
            df["source_1m_count"] = pd.to_numeric(
                df["source_1m_count"], errors="coerce"
            )

        keep = [c for c in keep_cols if c in df.columns]
        return df[keep].drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    except Exception:
        return pd.DataFrame()

def _mode_step_minutes(mode: str) -> int | None:
    return {"5min": 5, "15min": 15}.get(mode)

def _coerce_ist_datetime(value) -> pd.Timestamp | None:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        return ts.tz_localize(IST_TZ)
    return ts.tz_convert(IST_TZ)

def _resolve_current_slot_window(
    mode: str,
    intraday_ts: str,
    expected_spec: dict,
    default_start: datetime,
    default_end: datetime,
) -> tuple[datetime, datetime, pd.Timestamp | None]:
    step_min = _mode_step_minutes(mode)
    if step_min is None or expected_spec.get("kind") != "ts":
        return default_start, default_end, None

    slot_target_ts = _coerce_ist_datetime(expected_spec.get("value"))
    if slot_target_ts is None:
        return default_start, default_end, None

    step_td = timedelta(minutes=step_min)
    intraday_kind = intraday_ts.lower()

    if intraday_kind == "end":
        fetch_start = max(default_start, (slot_target_ts - step_td).to_pydatetime())
        fetch_end = min(default_end, slot_target_ts.to_pydatetime())
    else:
        fetch_start = max(default_start, slot_target_ts.to_pydatetime())
        fetch_end = min(default_end, (slot_target_ts + step_td).to_pydatetime())

    return fetch_start, fetch_end, slot_target_ts


_OHLCV_COLUMNS = ("open", "high", "low", "close", "volume")


def _rows_have_same_ohlcv(left: pd.Series, right: pd.Series) -> bool:
    try:
        left_values = np.asarray([float(left[column]) for column in _OHLCV_COLUMNS])
        right_values = np.asarray([float(right[column]) for column in _OHLCV_COLUMNS])
    except (KeyError, TypeError, ValueError):
        return False
    return bool(
        np.isfinite(left_values).all()
        and np.isfinite(right_values).all()
        and np.allclose(left_values, right_values, rtol=0.0, atol=1e-9)
    )


def _replace_timestamp_row(
    frame: pd.DataFrame,
    replacement: pd.DataFrame,
    target_ts: pd.Timestamp,
) -> pd.DataFrame:
    out = frame.copy()
    out_dt = _to_ist(out["date"]).dt.floor("min")
    replacement = replacement.copy()
    replacement["date"] = _to_ist(replacement["date"])
    return (
        pd.concat([out.loc[out_dt.ne(target_ts.floor("min"))], replacement], ignore_index=True)
        .drop_duplicates(subset="date", keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )


def _revalidate_provisional_5min_target(
    ticker: str,
    kite: KiteConnect,
    token: int,
    fetched: pd.DataFrame,
    existing: pd.DataFrame,
    target_ts_ist: datetime | pd.Timestamp | None,
    logger: logging.Logger,
) -> pd.DataFrame:
    """Retry a just-published candle that is an exact copy of its predecessor."""
    if (
        not DEFAULT_5M_PROVISIONAL_DUPLICATE_RETRY
        or fetched is None
        or fetched.empty
        or existing is None
        or existing.empty
    ):
        return fetched

    target_ts = _coerce_ist_datetime(target_ts_ist)
    if target_ts is None:
        return fetched
    first_completed = pd.Timestamp(
        IST_TZ.localize(datetime(target_ts.year, target_ts.month, target_ts.day, 9, 20))
    )
    if target_ts <= first_completed:
        return fetched

    fetched_dt = _to_ist(fetched["date"]).dt.floor("min")
    target_rows = fetched.loc[fetched_dt.eq(target_ts.floor("min"))]
    if target_rows.empty:
        return fetched

    previous_ts = target_ts - pd.Timedelta(minutes=5)
    prior_pool = pd.concat([existing, fetched], ignore_index=True, sort=False)
    prior_dt = _to_ist(prior_pool["date"]).dt.floor("min")
    previous_rows = prior_pool.loc[prior_dt.eq(previous_ts.floor("min"))]
    if previous_rows.empty:
        return fetched

    target_row = target_rows.iloc[-1]
    previous_row = previous_rows.iloc[-1]
    if not _rows_have_same_ohlcv(target_row, previous_row):
        return fetched

    settle_at = target_ts + pd.Timedelta(seconds=DEFAULT_5M_PROVISIONAL_SETTLE_SEC)
    wait_seconds = max(0.0, (settle_at.to_pydatetime() - datetime.now(IST_TZ)).total_seconds())
    if wait_seconds > 0:
        _time.sleep(wait_seconds)

    refreshed_target = pd.DataFrame()
    for attempt in range(1, DEFAULT_5M_PROVISIONAL_RETRY_ATTEMPTS + 1):
        refreshed = fetch_historical_5min_df(
            kite,
            int(token),
            (target_ts - pd.Timedelta(minutes=5)).to_pydatetime(),
            target_ts.to_pydatetime(),
            logger,
            "end",
        )
        if not refreshed.empty:
            refreshed_dt = _to_ist(refreshed["date"]).dt.floor("min")
            refreshed_target = refreshed.loc[
                refreshed_dt.eq(target_ts.floor("min"))
            ].copy()
            if not refreshed_target.empty:
                refreshed_target["date"] = _to_ist(refreshed_target["date"])
                refreshed_target["gap_filled"] = 0
                refreshed_target["provisional_stale"] = 0
                if not _rows_have_same_ohlcv(refreshed_target.iloc[-1], previous_row):
                    logger.info(
                        "[5MIN] %s revalidated provisional %s candle on attempt %d",
                        ticker,
                        target_ts.strftime("%H:%M"),
                        attempt,
                    )
                    return _replace_timestamp_row(fetched, refreshed_target, target_ts)
        if attempt < DEFAULT_5M_PROVISIONAL_RETRY_ATTEMPTS:
            _time.sleep(DEFAULT_5M_PROVISIONAL_RETRY_INTERVAL_SEC)

    out = fetched.copy()
    out_dt = _to_ist(out["date"]).dt.floor("min")
    out.loc[out_dt.eq(target_ts.floor("min")), "provisional_stale"] = 1
    logger.warning(
        "[5MIN] %s %s candle remained an exact OHLCV copy after %d recheck(s); marked provisional_stale",
        ticker,
        target_ts.strftime("%H:%M"),
        DEFAULT_5M_PROVISIONAL_RETRY_ATTEMPTS,
    )
    return out

def _incremental_start_from_existing(
    mode: str,
    out_path: str,
    default_start: datetime,
    last_ts: pd.Timestamp | datetime | None = None,
) -> datetime:
    existing_path = _resolve_existing_store_path(out_path)
    if last_ts is None:
        if not os.path.exists(existing_path):
            return default_start
        last_ts = _read_last_ts_from_store(existing_path)
    if last_ts is None:
        return default_start

    if isinstance(last_ts, pd.Timestamp):
        if last_ts.tzinfo is None:
            last_ts = last_ts.tz_localize(IST_TZ)
        else:
            last_ts = last_ts.tz_convert(IST_TZ)
        last_ts = last_ts.to_pydatetime()
    elif last_ts.tzinfo is None:
        last_ts = IST_TZ.localize(last_ts)
    else:
        last_ts = last_ts.astimezone(IST_TZ)

    step_min = _mode_step_minutes(mode)
    if step_min is not None:
        # Fallback path only. Live intraday slot fetches are resolved in process_ticker.
        return max(default_start, last_ts + timedelta(minutes=step_min))

    warm = int(WARMUP_BARS.get(mode, 0))
    if warm <= 0:
        return default_start

    if mode == "5min":
        back = timedelta(minutes=5 * warm)
    else:
        back = timedelta(days=30)

    return max(default_start, last_ts - back)


def _expected_5min_session_stamps(expected_ts_ist: datetime | pd.Timestamp | None) -> list[pd.Timestamp]:
    if not DEFAULT_ENFORCE_5MIN_SESSION_COMPLETENESS:
        return []

    ts = _coerce_ist_datetime(expected_ts_ist)
    if ts is None:
        return []

    session_open = pd.Timestamp(
        IST_TZ.localize(datetime(ts.year, ts.month, ts.day, 9, 15, 0))
    )
    if ts < session_open:
        return []

    first_end = session_open + pd.Timedelta(minutes=5)
    expected: list[pd.Timestamp] = [session_open]
    if ts >= first_end:
        expected.extend(
            list(pd.date_range(start=first_end, end=ts.floor("min"), freq="5min", tz=IST_TZ))
        )
    return expected


def _read_store_dates(path: str) -> pd.Series:
    try:
        ext = str(Path(path).suffix).lower()
        if ext == ".parquet":
            _ensure_parquet_engine()
            df = pd.read_parquet(path, columns=["date"], engine="pyarrow")
        else:
            df = pd.read_csv(path, usecols=["date"])
    except Exception:
        return pd.Series([], dtype="datetime64[ns]")

    if df is None or df.empty or "date" not in df.columns:
        return pd.Series([], dtype="datetime64[ns]")

    dt = pd.to_datetime(df["date"], errors="coerce").dropna()
    if dt.empty:
        return pd.Series([], dtype="datetime64[ns]")
    if getattr(dt.dt, "tz", None) is None:
        return dt.dt.tz_localize(IST_TZ)
    return dt.dt.tz_convert(IST_TZ)


def _missing_5min_session_stamps_from_series(
    date_series: pd.Series,
    expected_ts_ist: datetime | pd.Timestamp | None,
) -> list[pd.Timestamp]:
    expected = _expected_5min_session_stamps(expected_ts_ist)
    if not expected:
        return []
    if date_series is None or len(date_series) == 0:
        return expected

    dt = pd.to_datetime(date_series, errors="coerce").dropna()
    if dt.empty:
        return expected
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize(IST_TZ)
    else:
        dt = dt.dt.tz_convert(IST_TZ)

    target_day = expected[0].date()
    actual = {
        pd.Timestamp(ts).floor("min")
        for ts in dt[dt.dt.date == target_day].tolist()
    }
    return [stamp for stamp in expected if stamp.floor("min") not in actual]


def _missing_5min_session_stamps_from_df(
    df: pd.DataFrame,
    expected_ts_ist: datetime | pd.Timestamp | None,
) -> list[pd.Timestamp]:
    if df is None or df.empty or "date" not in df.columns:
        return _expected_5min_session_stamps(expected_ts_ist)
    missing = _missing_5min_session_stamps_from_series(df["date"], expected_ts_ist)
    expected = set(_expected_5min_session_stamps(expected_ts_ist))
    unsafe: set[pd.Timestamp] = set()

    timestamps = _to_ist(df["date"]).dt.floor("min")
    quality_bad = pd.Series(False, index=df.index)
    for quality_flag in ("gap_filled", "provisional_stale"):
        if quality_flag in df.columns:
            quality_bad |= (
                pd.to_numeric(df[quality_flag], errors="coerce").fillna(0).ne(0)
                | df[quality_flag]
                .astype(str)
                .str.strip()
                .str.lower()
                .isin({"true", "yes", "on"})
            )
    if "source_1m_count" in df.columns:
        source_count = pd.to_numeric(df["source_1m_count"], errors="coerce")
        quality_bad |= source_count.notna() & source_count.ne(5)

    # The optional 09:15 row is an opening snapshot, not a completed strategy
    # candle. It remains valid for legacy store shape and is excluded here.
    quality_bad &= ~(
        timestamps.dt.hour.eq(9) & timestamps.dt.minute.eq(15)
    )
    unsafe.update(
        pd.Timestamp(stamp).floor("min")
        for stamp in timestamps.loc[quality_bad].dropna()
        if pd.Timestamp(stamp).floor("min") in expected
    )

    required_ohlcv = {"open", "high", "low", "close", "volume"}
    if required_ohlcv.issubset(df.columns):
        ordered = df.assign(_quality_ts=timestamps).sort_values("_quality_ts").reset_index(drop=True)
        previous = ordered.shift(1)
        adjacent = ordered["_quality_ts"].dt.date.eq(previous["_quality_ts"].dt.date)
        adjacent &= ordered["_quality_ts"].sub(previous["_quality_ts"]).eq(
            pd.Timedelta(minutes=5)
        )
        if "opening_snapshot" in ordered.columns:
            previous_opening = (
                pd.to_numeric(previous["opening_snapshot"], errors="coerce").fillna(0).ne(0)
                | previous["opening_snapshot"]
                .astype(str)
                .str.strip()
                .str.lower()
                .isin({"true", "yes", "on"})
            )
            adjacent &= ~previous_opening
        for column in sorted(required_ohlcv):
            adjacent &= pd.to_numeric(ordered[column], errors="coerce").eq(
                pd.to_numeric(previous[column], errors="coerce")
            )
        if "source_1m_count" in ordered.columns:
            source_count = pd.to_numeric(
                ordered["source_1m_count"], errors="coerce"
            )
            previous_source_count = source_count.shift(1)
            adjacent &= ~(source_count.eq(5) & previous_source_count.eq(5))
        unsafe.update(
            pd.Timestamp(stamp).floor("min")
            for stamp in ordered.loc[adjacent, "_quality_ts"].dropna()
            if pd.Timestamp(stamp).floor("min") in expected
        )

    return sorted(set(missing) | unsafe)


def _missing_5min_session_stamps_from_store(
    path: str,
    expected_ts_ist: datetime | pd.Timestamp | None,
) -> list[pd.Timestamp]:
    try:
        ext = str(Path(path).suffix).lower()
        if ext == ".parquet":
            _ensure_parquet_engine()
            try:
                frame = pd.read_parquet(
                    path,
                    columns=["date", "provisional_stale"],
                    engine="pyarrow",
                )
            except Exception:
                frame = pd.read_parquet(path, columns=["date"], engine="pyarrow")
        else:
            frame = pd.read_csv(path)
    except Exception:
        return _expected_5min_session_stamps(expected_ts_ist)
    return _missing_5min_session_stamps_from_df(frame, expected_ts_ist)


def _format_missing_stamp_sample(missing_stamps: list[pd.Timestamp]) -> str:
    if not missing_stamps:
        return ""
    return ", ".join(
        pd.Timestamp(ts).strftime("%H:%M") for ts in missing_stamps[:DEFAULT_SESSION_COMPLETENESS_LOG_LIMIT]
    )


def _trim_live_5min_history(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "date" not in df.columns:
        return df
    if not DEFAULT_5M_LIVE_SLIM_MODE:
        return df

    dt = pd.to_datetime(df["date"], errors="coerce")
    if dt.isna().all():
        return df
    if getattr(dt.dt, "tz", None) is None:
        dt = dt.dt.tz_localize(IST_TZ)
    else:
        dt = dt.dt.tz_convert(IST_TZ)

    cutoff_date = (datetime.now(IST_TZ) - timedelta(days=DEFAULT_5M_LIVE_SLIM_CALENDAR_DAYS)).date()
    trimmed = df.loc[dt.dt.date >= cutoff_date].copy()
    if trimmed.empty:
        trimmed = df.tail(400).copy()
    return trimmed.sort_values("date").reset_index(drop=True)


def _synthetic_fill_price_for_stamp(df: pd.DataFrame, stamp: pd.Timestamp) -> float | None:
    before = df.loc[df["date"] < stamp].sort_values("date")
    if not before.empty:
        try:
            px = float(before.iloc[-1]["close"])
            if np.isfinite(px):
                return px
        except Exception:
            pass

    after = df.loc[df["date"] > stamp].sort_values("date")
    if not after.empty:
        for col in ("open", "close"):
            try:
                px = float(after.iloc[0][col])
                if np.isfinite(px):
                    return px
            except Exception:
                continue

    return None


def _apply_synthetic_5min_gap_fill(
    df: pd.DataFrame,
    expected_ts_ist: datetime | pd.Timestamp | None,
    ticker: str,
    logger: logging.Logger,
) -> tuple[pd.DataFrame, list[pd.Timestamp]]:
    if not DEFAULT_5M_SYNTHETIC_GAP_FILL:
        return df, []
    if df is None or df.empty or "date" not in df.columns:
        return df, []

    out = df.copy()
    out["date"] = _to_ist(out["date"])
    if "gap_filled" not in out.columns:
        out["gap_filled"] = 0
    else:
        out["gap_filled"] = pd.to_numeric(out["gap_filled"], errors="coerce").fillna(0).astype(int)

    missing = _missing_5min_session_stamps_from_df(out, expected_ts_ist)
    if not missing:
        return out.sort_values("date").reset_index(drop=True), []

    synth_rows: list[dict[str, object]] = []
    for stamp in missing:
        fill_px = _synthetic_fill_price_for_stamp(out, pd.Timestamp(stamp))
        if fill_px is None:
            continue
        synth_rows.append(
            {
                "date": pd.Timestamp(stamp),
                "open": fill_px,
                "high": fill_px,
                "low": fill_px,
                "close": fill_px,
                "volume": 0.0,
                "gap_filled": 1,
            }
        )

    if not synth_rows:
        return out.sort_values("date").reset_index(drop=True), []

    synth_df = pd.DataFrame(synth_rows)
    out = (
        pd.concat([out, synth_df], ignore_index=True)
        .sort_values("date")
        .drop_duplicates(subset="date", keep="last")
        .reset_index(drop=True)
    )
    filled_now = [pd.Timestamp(row["date"]).floor("min") for row in synth_rows]
    logger.info(
        "[5MIN] %s synthetic gap fill applied | count=%d | sample=%s",
        ticker,
        len(filled_now),
        _format_missing_stamp_sample(filled_now),
    )
    return out, filled_now


def _contiguous_5min_stamp_ranges(
    stamps: list[pd.Timestamp],
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    normalized = sorted({pd.Timestamp(ts).floor("min") for ts in stamps})
    if not normalized:
        return []

    step = pd.Timedelta(minutes=5)
    ranges: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    range_start = normalized[0]
    range_end = normalized[0]
    for stamp in normalized[1:]:
        if stamp == range_end + step:
            range_end = stamp
            continue
        ranges.append((range_start, range_end))
        range_start = stamp
        range_end = stamp
    ranges.append((range_start, range_end))
    return ranges


def _fetch_missing_5min_session_rows(
    ticker: str,
    kite: KiteConnect,
    token: int,
    expected_ts_ist: datetime | pd.Timestamp | None,
    missing_stamps: list[pd.Timestamp],
    logger: logging.Logger,
) -> pd.DataFrame:
    expected_ts = _coerce_ist_datetime(expected_ts_ist)
    if expected_ts is None or not missing_stamps:
        return pd.DataFrame()

    session_open = pd.Timestamp(
        IST_TZ.localize(datetime(expected_ts.year, expected_ts.month, expected_ts.day, 9, 15, 0))
    )
    first_end = session_open + pd.Timedelta(minutes=5)
    normalized_missing = sorted(pd.Timestamp(ts).floor("min") for ts in missing_stamps)
    frames: list[pd.DataFrame] = []

    if session_open.floor("min") in normalized_missing:
        opening_df = fetch_historical_5min_df(
            kite,
            token,
            session_open.to_pydatetime(),
            first_end.to_pydatetime(),
            logger,
            "start",
        )
        if not opening_df.empty:
            opening_dt = pd.to_datetime(opening_df["date"], errors="coerce")
            if getattr(opening_dt.dt, "tz", None) is None:
                opening_dt = opening_dt.dt.tz_localize(IST_TZ)
            else:
                opening_dt = opening_dt.dt.tz_convert(IST_TZ)
            opening_df = opening_df.loc[opening_dt == session_open].copy()
            if not opening_df.empty:
                opening_df["date"] = opening_dt.loc[opening_df.index]
                frames.append(opening_df)

    end_missing = [ts for ts in normalized_missing if ts >= first_end]
    for range_start, range_end in _contiguous_5min_stamp_ranges(end_missing):
        # End-stamped storage shifts Kite's start-stamped bars by five minutes.
        # Fetch only the raw-bar span needed for this missing range instead of
        # redownloading the full session for every symbol on every live slot.
        fetch_start = max(session_open, range_start - pd.Timedelta(minutes=5))
        fetch_end = min(expected_ts, range_end)
        if fetch_start >= fetch_end:
            continue

        range_df = fetch_historical_5min_df(
            kite,
            token,
            fetch_start.to_pydatetime(),
            fetch_end.to_pydatetime(),
            logger,
            "end",
        )
        if not range_df.empty:
            range_dt = pd.to_datetime(range_df["date"], errors="coerce")
            if getattr(range_dt.dt, "tz", None) is None:
                range_dt = range_dt.dt.tz_localize(IST_TZ)
            else:
                range_dt = range_dt.dt.tz_convert(IST_TZ)
            wanted = {
                stamp
                for stamp in end_missing
                if range_start <= stamp <= range_end
            }
            mask = range_dt.isin(wanted)
            range_df = range_df.loc[mask].copy()
            if not range_df.empty:
                range_df["date"] = range_dt.loc[range_df.index]
                frames.append(range_df)

    if not frames:
        logger.warning(
            "[5MIN] %s session backfill returned no rows | missing=%s",
            ticker,
            _format_missing_stamp_sample(normalized_missing),
        )
        return pd.DataFrame()

    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset="date", keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )


# ========= PER-SYMBOL PIPELINE =========

def _compute_common_features(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    df = add_standard_indicators(df)

    if mode == "5min":
        df["AVWAP"] = calculate_anchored_vwap_5min(df)

    stoch_k, stoch_d = calculate_stochastic_slow(df, 14, 3, 3, "sma")
    df["Stoch_%K"], df["Stoch_%D"] = stoch_k, stoch_d
    df["ADX"] = calculate_adx(df)
    return df

def _downcast_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if col == "date":
            continue
        if pd.api.types.is_float_dtype(out[col]):
            out[col] = pd.to_numeric(out[col], downcast="float")
        elif pd.api.types.is_integer_dtype(out[col]):
            out[col] = pd.to_numeric(out[col], downcast="integer")
    return out

def _indicator_quality_snapshot(df: pd.DataFrame) -> dict[str, float]:
    checks: dict[str, float] = {}
    total = max(len(df), 1)

    for col in ("ATR", "RSI", "Stoch_%K", "Stoch_%D", "ADX", "EMA_20", "EMA_50"):
        if col in df.columns:
            checks[f"{col}_nan_pct"] = float(df[col].isna().sum() * 100.0 / total)

    if "RSI" in df.columns:
        r = pd.to_numeric(df["RSI"], errors="coerce")
        checks["RSI_out_of_range_pct"] = float(((r < 0) | (r > 100)).sum() * 100.0 / total)

    if "Stoch_%K" in df.columns:
        k = pd.to_numeric(df["Stoch_%K"], errors="coerce")
        checks["StochK_out_of_range_pct"] = float(((k < 0) | (k > 100)).sum() * 100.0 / total)

    return checks

def _log_indicator_quality(logger: logging.Logger, ticker: str, mode: str, df: pd.DataFrame) -> None:
    q = _indicator_quality_snapshot(df)
    if not q:
        return
    severe = [k for k, v in q.items() if v > 15.0]
    if severe:
        logger.warning("[%s] %s indicator quality warning: %s", mode.upper(), ticker, q)

def _safe_mkdir(p: str):
    os.makedirs(p, exist_ok=True)

def _fmt_expected(spec: dict) -> str:
    try:
        if spec.get("kind") == "ts":
            v = spec["value"]
            if isinstance(v, datetime):
                return f"ts>={v.strftime('%Y-%m-%d %H:%M:%S')}"
            return f"ts>={str(v)}"
    except Exception:
        pass
    return str(spec)

@dataclass
class UpdateReport:
    mode: str
    ticker: str
    status: str            # created|updated|noop|failed
    out_path: str
    existed_before: bool
    last_before: str | None
    expected: str | None
    new_rows_count: int
    new_first: str | None
    new_last: str | None
    new_rows_path: str | None
    load_existing_secs: float
    fetch_secs: float
    indicators_secs: float
    persist_secs: float
    total_secs: float
    allow_previous_slot_verify: bool = False
    last_after: str | None = None
    data_state: str = "unresolved"  # current|previous_slot|unresolved|failed


def _classify_symbol_timestamp(
    last_ts,
    expected_ts_ist: datetime,
    step_min: int,
) -> str:
    """Classify one symbol against the authoritative slot timestamp."""
    ts = _coerce_ist_datetime(last_ts)
    expected = _coerce_ist_datetime(expected_ts_ist)
    if ts is None or expected is None:
        return "unresolved"

    tol = timedelta(seconds=1)
    if ts >= (expected - tol):
        return "current"
    if step_min > 0 and ts >= (expected - timedelta(minutes=step_min) - tol):
        return "previous_slot"
    return "unresolved"


def _symbol_outcome_summary(
    universe_symbols: list[str],
    *,
    current_symbols: set[str] | list[str] = (),
    previous_slot_symbols: set[str] | list[str] = (),
    failed_symbols: set[str] | list[str] = (),
    unresolved_symbols: set[str] | list[str] = (),
    token_missing_symbols: set[str] | list[str] = (),
    written_symbols: set[str] | list[str] = (),
    noop_symbols: set[str] | list[str] = (),
) -> dict[str, object]:
    """
    Return scheduler-consumable, per-symbol completion accounting.

    Precedence is failed -> unresolved -> previous-slot -> current. Any
    universe symbol not explicitly classified is unresolved, preventing an
    assigned ticker from being accidentally counted as successfully written.
    """
    universe = {
        str(symbol).strip().upper()
        for symbol in universe_symbols
        if str(symbol).strip()
    }
    current = {str(s).strip().upper() for s in current_symbols if str(s).strip()} & universe
    previous = {str(s).strip().upper() for s in previous_slot_symbols if str(s).strip()} & universe
    failed = {str(s).strip().upper() for s in failed_symbols if str(s).strip()} & universe
    unresolved = {str(s).strip().upper() for s in unresolved_symbols if str(s).strip()} & universe
    token_missing = {str(s).strip().upper() for s in token_missing_symbols if str(s).strip()} & universe
    written = {str(s).strip().upper() for s in written_symbols if str(s).strip()} & universe
    noop = {str(s).strip().upper() for s in noop_symbols if str(s).strip()} & universe

    unresolved |= token_missing
    unresolved -= failed
    previous -= failed | unresolved
    current -= failed | unresolved | previous
    unresolved |= universe - current - previous - failed
    complete = current | previous

    def _ordered(values: set[str]) -> list[str]:
        return sorted(values)

    return {
        "universe_symbols": _ordered(universe),
        "current_symbols": _ordered(current),
        "previous_slot_symbols": _ordered(previous),
        "complete_symbols": _ordered(complete),
        "failed_symbols": _ordered(failed),
        "unresolved_symbols": _ordered(unresolved),
        "token_missing_symbols": _ordered(token_missing),
        "written_symbols": _ordered(written),
        "noop_symbols": _ordered(noop),
        "universe_count": int(len(universe)),
        "current_count": int(len(current)),
        "previous_slot_count": int(len(previous)),
        "complete_count": int(len(complete)),
        "failed_count": int(len(failed)),
        "unresolved_count": int(len(unresolved)),
        "token_missing_count": int(len(token_missing)),
        "written_count": int(len(written)),
        "noop_count": int(len(noop)),
        "outcome_counts": {
            "current": int(len(current)),
            "previous_slot": int(len(previous)),
            "failed": int(len(failed)),
            "unresolved": int(len(unresolved)),
        },
    }


def verify_mode_outputs(
    mode: str,
    symbols: list[str],
    expected_ts_ist: datetime,
    logger: logging.Logger,
    allow_previous_slot_tickers: set[str] | None = None,
) -> tuple[int, list[str]]:
    """
    Post-run verification:
    - output file exists
    - last timestamp is >= expected timestamp
    """
    failed: list[str] = []
    ok = 0
    tol = timedelta(seconds=1)

    if expected_ts_ist.tzinfo is None:
        expected_ts_ist = IST_TZ.localize(expected_ts_ist)
    allow_previous_slot_tickers = {
        str(t).strip().upper() for t in (allow_previous_slot_tickers or set()) if str(t).strip()
    }
    step_min = _mode_step_minutes(mode) or 0
    prev_slot_tol_ts = expected_ts_ist - timedelta(minutes=step_min) if step_min > 0 else None

    for t in symbols:
        t_u = t.upper()
        out_path = os.path.join(DIRS[mode]["out"], f"{t_u}_stocks_indicators_{mode}.parquet")
        existing_path = _resolve_existing_store_path(out_path)
        if not os.path.exists(existing_path):
            failed.append(f"{t_u}:file_missing")
            continue

        last_ts = _read_last_ts_from_store(existing_path)
        if last_ts is None:
            failed.append(f"{t_u}:last_ts_missing")
            continue
        if last_ts.tzinfo is None:
            last_ts = last_ts.tz_localize(IST_TZ)
        else:
            last_ts = last_ts.tz_convert(IST_TZ)

        if last_ts >= (expected_ts_ist - tol):
            if mode.lower().strip() == "5min" and DEFAULT_ENFORCE_5MIN_SESSION_COMPLETENESS:
                missing_session = _missing_5min_session_stamps_from_store(existing_path, expected_ts_ist)
                if missing_session:
                    failed.append(
                        f"{t_u}:missing_session_stamps={_format_missing_stamp_sample(missing_session)}"
                    )
                else:
                    ok += 1
            else:
                ok += 1
        elif (
            prev_slot_tol_ts is not None
            and last_ts >= (prev_slot_tol_ts - tol)
        ):
            # Fix A (2026-05-19): accept 1-slot lag for ALL symbols, not just the
            # per-ticker allowlist. Illiquid v2-universe symbols (e.g., ORICONENT)
            # have bars only when they trade; Kite returns 0 rows for empty bars.
            # Flagging them as failures wastes ~7s/slot on doomed recovery retries
            # and marks complete=false even though the data matches Kite's reality.
            ok += 1
        else:
            failed.append(f"{t_u}:stale_last_ts={last_ts.strftime('%Y-%m-%d %H:%M:%S%z')}")

    if failed:
        logger.warning("[%s][VERIFY] Failed=%d | sample=%s", mode.upper(), len(failed), ", ".join(failed[:20]))
    return ok, failed

def _sample_verify_symbols(symbols: list[str], sample_size: int) -> list[str]:
    if sample_size <= 0 or len(symbols) <= sample_size:
        return list(symbols)
    ordered = sorted({str(sym).strip().upper() for sym in symbols if str(sym).strip()})
    if len(ordered) <= sample_size:
        return ordered
    if sample_size == 1:
        return [ordered[-1]]
    picks: list[str] = []
    last_idx = len(ordered) - 1
    for i in range(sample_size):
        idx = round(i * last_idx / (sample_size - 1))
        sym = ordered[idx]
        if not picks or picks[-1] != sym:
            picks.append(sym)
    if len(picks) < sample_size:
        seen = set(picks)
        for sym in ordered:
            if sym in seen:
                continue
            picks.append(sym)
            if len(picks) >= sample_size:
                break
    return picks[:sample_size]


def _extract_failed_tickers(verify_failed: list[str], all_symbols: list[str]) -> list[str]:
    allowed = {s.upper() for s in all_symbols}
    out: list[str] = []
    seen: set[str] = set()

    for item in verify_failed:
        t = str(item).split(":", 1)[0].strip().upper()
        if not t or t not in allowed or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _recover_verify_failures(
    mode: str,
    verify_failed: list[str],
    all_symbols: list[str],
    expected_ts_ist: datetime,
    kite: KiteConnect,
    token_map: dict[str, int],
    refresh_tokens: bool,
    max_workers: int,
    start_dt_ist: datetime,
    end_dt_ist: datetime,
    logger: logging.Logger,
    holidays: set[date],
    intraday_ts: str,
    report_dir: str,
    print_missing_rows: bool,
    print_missing_rows_max: int,
    allow_previous_slot_tickers: set[str] | None = None,
) -> float:
    """
    Recovery pass for symbols that failed final verification.
    Re-fetch/recompute/re-save only failed tickers, then let caller run verify again.
    """
    failed_tickers = _extract_failed_tickers(verify_failed, all_symbols)
    if not failed_tickers:
        return 0.0

    t0 = _time.perf_counter()
    max_attempts = 2
    retry_sleep_sec = 4
    remaining = failed_tickers

    logger.warning(
        "[%s][VERIFY] Starting recovery for %d symbol(s): %s",
        mode.upper(),
        len(remaining),
        ", ".join(remaining[:30]),
    )

    for attempt in range(1, max_attempts + 1):
        if not remaining:
            break

        need_tokens = [t for t in remaining if t.upper() not in token_map]
        if need_tokens:
            fetched = load_or_fetch_tokens(kite, need_tokens, logger, refresh=(refresh_tokens and attempt == 1))
            token_map.update({k.upper(): int(v) for k, v in fetched.items()})

        work_items: list[tuple[str, int]] = []
        token_missing: list[str] = []
        for t in remaining:
            tok = token_map.get(t.upper())
            if tok:
                work_items.append((t.upper(), int(tok)))
            else:
                token_missing.append(t.upper())

        if token_missing:
            logger.warning(
                "[%s][VERIFY] Recovery attempt %d/%d: token missing for %d symbol(s): %s",
                mode.upper(),
                attempt,
                max_attempts,
                len(token_missing),
                ", ".join(token_missing[:20]),
            )

        if work_items:
            retry_workers = min(max(1, int(max_workers)), max(1, len(work_items)), 8)
            logger.warning(
                "[%s][VERIFY] Recovery attempt %d/%d: refetching %d symbol(s) with max_workers=%d",
                mode.upper(),
                attempt,
                max_attempts,
                len(work_items),
                retry_workers,
            )

            with ThreadPoolExecutor(max_workers=retry_workers) as executor:
                futures = {
                    executor.submit(
                        process_ticker,
                        mode,
                        tkr,
                        tok,
                        kite,
                        start_dt_ist,
                        end_dt_ist,
                        logger,
                        holidays,
                        False,  # force check/fetch for failed symbol
                        intraday_ts,
                        report_dir,
                        print_missing_rows,
                        print_missing_rows_max,
                    ): tkr
                    for (tkr, tok) in work_items
                }
                for fut in as_completed(futures):
                    tkr = futures[fut]
                    try:
                        rep: UpdateReport = fut.result()
                        if rep.status == "failed":
                            logger.warning("[%s][VERIFY] Recovery worker failed for %s", mode.upper(), tkr)
                    except Exception as e:
                        logger.exception("[%s][VERIFY] Recovery worker crashed for %s: %s", mode.upper(), tkr, e)

        check_symbols = token_missing + [t for (t, _) in work_items]
        _, verify_failed_subset = verify_mode_outputs(
            mode,
            check_symbols,
            expected_ts_ist,
            logger,
            allow_previous_slot_tickers=allow_previous_slot_tickers,
        )
        remaining = _extract_failed_tickers(verify_failed_subset, check_symbols)

        if remaining and attempt < max_attempts:
            logger.warning(
                "[%s][VERIFY] Recovery attempt %d incomplete. Remaining=%d; retrying after %ds.",
                mode.upper(),
                attempt,
                len(remaining),
                retry_sleep_sec,
            )
            _time.sleep(retry_sleep_sec)

    elapsed = _time.perf_counter() - t0
    if remaining:
        logger.warning(
            "[%s][VERIFY] Recovery ended with unresolved=%d | sample=%s | elapsed=%.2fs",
            mode.upper(),
            len(remaining),
            ", ".join(remaining[:20]),
            elapsed,
        )
    else:
        logger.info("[%s][VERIFY] Recovery succeeded for all failed symbols | elapsed=%.2fs", mode.upper(), elapsed)
    return elapsed

def process_ticker(
    mode: str,
    ticker: str,
    token: int,
    kite: KiteConnect,
    start_dt_ist: datetime,
    end_dt_ist: datetime,
    logger: logging.Logger,
    holidays: set[date],
    skip_if_fresh: bool,
    intraday_ts: str,
    report_dir: str,
    print_missing_rows: bool,
    print_missing_rows_max: int,
    known_last_ts=None,
    known_stale: bool = False,
) -> UpdateReport:
    t_total0 = _time.perf_counter()
    load_existing_secs = 0.0
    fetch_secs = 0.0
    indicators_secs = 0.0
    persist_secs = 0.0

    out_path = os.path.join(DIRS[mode]["out"], f"{ticker}_stocks_indicators_{mode}.parquet")
    now_ist = datetime.now(IST_TZ)

    existing_path = _resolve_existing_store_path(out_path)
    existed_before = os.path.exists(existing_path)
    if known_stale:
        # run_mode already performed the authoritative freshness scan. Reuse
        # its timestamp instead of reopening the same Parquet metadata here.
        last_before_ts = known_last_ts
    else:
        last_before_ts = _read_last_ts_from_store(existing_path) if existed_before else None
    if last_before_ts is not None:
        if last_before_ts.tzinfo is None:
            last_before_ts = last_before_ts.tz_localize(IST_TZ)
        else:
            last_before_ts = last_before_ts.tz_convert(IST_TZ)

    exp = expected_last_stamp(mode, now_ist, holidays, intraday_ts)
    exp_str = _fmt_expected(exp)

    if (
        skip_if_fresh
        and not known_stale
        and _ticker_is_fresh_from_last_ts(
            mode,
            existing_path,
            last_before_ts,
            now_ist,
            holidays,
            intraday_ts,
        )
    ):
        return UpdateReport(mode, ticker, "noop", out_path, existed_before,
                            last_before_ts.strftime("%Y-%m-%d %H:%M:%S") if last_before_ts is not None else None,
                            exp_str, 0, None, None, None,
                            load_existing_secs, fetch_secs, indicators_secs, persist_secs,
                            _time.perf_counter() - t_total0)

    t_load0 = _time.perf_counter()
    existing = _load_existing_ohlc(out_path, intraday_ts, mode)
    load_existing_secs = _time.perf_counter() - t_load0

    if not existing.empty:
        existing_last_ts = pd.to_datetime(existing["date"], errors="coerce").dropna().max()
        if pd.notna(existing_last_ts):
            if existing_last_ts.tzinfo is None:
                last_before_ts = existing_last_ts.tz_localize(IST_TZ)
            else:
                last_before_ts = existing_last_ts.tz_convert(IST_TZ)

    session_expected_ts = _coerce_ist_datetime(exp.get("value"))
    session_missing_stamps = (
        _missing_5min_session_stamps_from_df(existing, session_expected_ts)
        if mode == "5min" and DEFAULT_ENFORCE_5MIN_SESSION_COMPLETENESS
        else []
    )
    session_backfill_mode = bool(
        mode == "5min"
        and DEFAULT_ENFORCE_5MIN_SESSION_COMPLETENESS
        and not existing.empty
        and session_missing_stamps
    )
    if session_backfill_mode:
        logger.warning(
            "[%s] %s session completeness gap detected | missing=%d | sample=%s",
            mode.upper(),
            ticker,
            len(session_missing_stamps),
            _format_missing_stamp_sample(session_missing_stamps),
        )

    slot_fetch_start, slot_fetch_end, slot_target_ts = _resolve_current_slot_window(
        mode,
        intraday_ts,
        exp,
        start_dt_ist,
        end_dt_ist,
    )

    if (
        slot_target_ts is not None
        and last_before_ts is not None
        and last_before_ts >= slot_target_ts
        and not session_backfill_mode
    ):
        return UpdateReport(mode, ticker, "noop", out_path, existed_before,
                            last_before_ts.strftime("%Y-%m-%d %H:%M:%S") if last_before_ts is not None else None,
                            exp_str, 0, None, None, None,
                            load_existing_secs, fetch_secs, indicators_secs, persist_secs,
                            _time.perf_counter() - t_total0)

    inc_start = slot_fetch_start if slot_target_ts is not None else _incremental_start_from_existing(mode, out_path, start_dt_ist, last_before_ts)
    fetch_end_dt = slot_fetch_end if slot_target_ts is not None else end_dt_ist

    if inc_start >= fetch_end_dt:
        return UpdateReport(mode, ticker, "noop", out_path, existed_before,
                            last_before_ts.strftime("%Y-%m-%d %H:%M:%S") if last_before_ts is not None else None,
                            exp_str, 0, None, None, None,
                            load_existing_secs, fetch_secs, indicators_secs, persist_secs,
                            _time.perf_counter() - t_total0)

    def _fetch_for_token(active_token: int) -> pd.DataFrame:
        exact_fno_equity = bool(
            mode == "5min"
            and DEFAULT_FNO_5M_FROM_1M
            and ticker.upper() in _FNO_EQUITY_SYMBOLS
        )
        if exact_fno_equity:
            requested = (
                session_missing_stamps
                if session_backfill_mode
                else ([slot_target_ts] if slot_target_ts is not None else [])
            )
            if requested:
                return _fetch_exact_fno_5min_rows(
                    ticker,
                    kite,
                    active_token,
                    requested,
                    logger,
                )
        if session_backfill_mode:
            return _fetch_missing_5min_session_rows(
                ticker,
                kite,
                active_token,
                session_expected_ts,
                session_missing_stamps,
                logger,
            )
        if mode == "5min":
            return fetch_historical_5min_df(kite, active_token, inc_start, fetch_end_dt, logger, intraday_ts)
        if mode == "15min":
            return fetch_historical_15min_df(kite, active_token, inc_start, fetch_end_dt, logger, intraday_ts)
        raise ValueError(f"Unsupported mode for fetch: {mode}")

    active_token = int(token)
    try:
        t_fetch0 = _time.perf_counter()
        fetched = _fetch_for_token(active_token)
        fetch_secs = _time.perf_counter() - t_fetch0
    except InvalidInstrumentTokenError:
        refreshed = {}
        try:
            refreshed = load_or_fetch_tokens(kite, [ticker], logger, refresh=True)
        except Exception as refresh_exc:
            logger.warning("[%s] %s token refresh failed after invalid token: %s", mode.upper(), ticker, refresh_exc)

        refreshed_token = refreshed.get(ticker.upper())
        if refreshed_token and int(refreshed_token) != int(token):
            logger.warning(
                "[%s] %s invalid cached token %s refreshed to %s. Retrying once.",
                mode.upper(),
                ticker,
                token,
                refreshed_token,
            )
            try:
                active_token = int(refreshed_token)
                t_fetch0 = _time.perf_counter()
                fetched = _fetch_for_token(active_token)
                fetch_secs = _time.perf_counter() - t_fetch0
            except InvalidInstrumentTokenError:
                _quarantine_symbols([ticker], logger, "invalid_token_after_refresh")
                return UpdateReport(mode, ticker, "noop", out_path, existed_before,
                                    last_before_ts.strftime("%Y-%m-%d %H:%M:%S") if last_before_ts is not None else None,
                                    exp_str, 0, None, None, None,
                                    load_existing_secs, fetch_secs, indicators_secs, persist_secs,
                                    _time.perf_counter() - t_total0)
        else:
            _quarantine_symbols([ticker], logger, "missing_from_current_nse_instruments")
            logger.warning(
                "[%s] %s is absent from current NSE instruments after invalid token. Skipping future live fetch attempts.",
                mode.upper(),
                ticker,
            )
            return UpdateReport(mode, ticker, "noop", out_path, existed_before,
                                last_before_ts.strftime("%Y-%m-%d %H:%M:%S") if last_before_ts is not None else None,
                                exp_str, 0, None, None, None,
                                load_existing_secs, fetch_secs, indicators_secs, persist_secs,
                                _time.perf_counter() - t_total0)
    except Exception as e:
        logger.exception("[%s] %s fetch failed: %s", mode.upper(), ticker, e)
        return UpdateReport(mode, ticker, "failed", out_path, existed_before,
                            last_before_ts.strftime("%Y-%m-%d %H:%M:%S") if last_before_ts is not None else None,
                            exp_str, 0, None, None, None,
                            load_existing_secs, fetch_secs, indicators_secs, persist_secs,
                            _time.perf_counter() - t_total0)

    fetched_is_empty = fetched is None or fetched.empty
    synthetic_backfill_on_empty = bool(
        fetched_is_empty
        and mode == "5min"
        and session_backfill_mode
        and DEFAULT_5M_SYNTHETIC_GAP_FILL
        and not existing.empty
    )
    if fetched_is_empty and not synthetic_backfill_on_empty:
        return UpdateReport(mode, ticker, "noop", out_path, existed_before,
                            last_before_ts.strftime("%Y-%m-%d %H:%M:%S") if last_before_ts is not None else None,
                            exp_str, 0, None, None, None,
                            load_existing_secs, fetch_secs, indicators_secs, persist_secs,
                            _time.perf_counter() - t_total0,
                            allow_previous_slot_verify=bool(slot_target_ts is not None))
    if synthetic_backfill_on_empty:
        logger.info(
            "[5MIN] %s exchange returned no rows for missing session stamp(s); "
            "continuing with configured synthetic zero-volume gap fill.",
            ticker,
        )
        fetched = pd.DataFrame()

    fetched = fetched.copy()
    fetched["gap_filled"] = 0
    fetched["provisional_stale"] = 0

    if slot_target_ts is not None and not session_backfill_mode:
        fetched = fetched[fetched["date"] == slot_target_ts].reset_index(drop=True)
        if fetched.empty:
            return UpdateReport(mode, ticker, "noop", out_path, existed_before,
                                last_before_ts.strftime("%Y-%m-%d %H:%M:%S") if last_before_ts is not None else None,
                                exp_str, 0, None, None, None,
                                load_existing_secs, fetch_secs, indicators_secs, persist_secs,
                                _time.perf_counter() - t_total0,
                                allow_previous_slot_verify=True)

    fetched_from_exact_minutes = bool(
        mode == "5min"
        and DEFAULT_FNO_5M_FROM_1M
        and ticker.upper() in _FNO_EQUITY_SYMBOLS
        and "source_1m_count" in fetched.columns
        and pd.to_numeric(fetched["source_1m_count"], errors="coerce").eq(5).any()
    )
    if mode == "5min" and slot_target_ts is not None and not fetched_from_exact_minutes:
        t_revalidate0 = _time.perf_counter()
        fetched = _revalidate_provisional_5min_target(
            ticker,
            kite,
            active_token,
            fetched,
            existing,
            slot_target_ts,
            logger,
        )
        fetch_secs += _time.perf_counter() - t_revalidate0

    if mode in {"5min", "15min"} and last_before_ts is not None and not session_backfill_mode:
        replaceable_overlap = set()
        existing_dt = _to_ist(existing["date"]).dt.floor("min")
        replaceable = pd.Series(False, index=existing.index)
        for quality_flag in ("gap_filled", "provisional_stale"):
            if quality_flag in existing.columns:
                replaceable |= (
                    pd.to_numeric(existing[quality_flag], errors="coerce").fillna(0).astype(int) > 0
                )
        replaceable_overlap = {
            pd.Timestamp(ts) for ts in existing_dt.loc[replaceable].tolist()
        }

        overlap = fetched[fetched["date"] <= last_before_ts].copy()
        if not overlap.empty:
            overlap_first = pd.to_datetime(overlap["date"], errors="coerce").dropna().min()
            overlap_last = pd.to_datetime(overlap["date"], errors="coerce").dropna().max()
            logger.info(
                "[%s] %s ignoring %d overlapping historical row(s) <= %s | overlap_range=%s -> %s",
                mode.upper(),
                ticker,
                len(overlap),
                last_before_ts.strftime("%Y-%m-%d %H:%M:%S"),
                overlap_first.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(overlap_first) else "n/a",
                overlap_last.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(overlap_last) else "n/a",
            )
            if replaceable_overlap:
                fetched_dt = _to_ist(fetched["date"]).dt.floor("min")
                keep_overlap = fetched_dt.isin(replaceable_overlap)
                fetched = fetched[(fetched["date"] > last_before_ts) | keep_overlap].reset_index(drop=True)
            else:
                fetched = fetched[fetched["date"] > last_before_ts].reset_index(drop=True)

        if fetched.empty:
            return UpdateReport(mode, ticker, "noop", out_path, existed_before,
                                last_before_ts.strftime("%Y-%m-%d %H:%M:%S") if last_before_ts is not None else None,
                                exp_str, 0, None, None, None,
                                load_existing_secs, fetch_secs, indicators_secs, persist_secs,
                                _time.perf_counter() - t_total0,
                                allow_previous_slot_verify=bool(slot_target_ts is not None))

    merged = fetched
    if not existing.empty:
        if mode in {"5min", "15min"}:
            merged = (
                pd.concat([existing, fetched], ignore_index=True)
                  .drop_duplicates(subset="date", keep="last")
                  .sort_values("date")
                  .reset_index(drop=True)
            )
        else:
            merged = (
                pd.concat([existing, fetched], ignore_index=True)
                  .drop_duplicates(subset="date", keep="last")
                  .sort_values("date")
                  .reset_index(drop=True)
            )

    if mode == "5min":
        merged, _ = _apply_synthetic_5min_gap_fill(merged, session_expected_ts, ticker, logger)
        merged = _trim_live_5min_history(merged)

    try:
        t_ind0 = _time.perf_counter()
        merged = _compute_common_features(merged, mode)
        if DEFAULT_DOWNCAST_NUMERIC:
            merged = _downcast_numeric_columns(merged)
        if DEFAULT_LOG_INDICATOR_QUALITY:
            _log_indicator_quality(logger, ticker, mode, merged)
        indicators_secs = _time.perf_counter() - t_ind0

        t_persist0 = _time.perf_counter()
        _finalize_and_save(merged, out_path)

        # Optional: if we migrated from a legacy CSV, delete it after successful parquet write
        if DELETE_LEGACY_CSV and existed_before and str(existing_path).lower().endswith(".csv"):
            try:
                os.remove(existing_path)
            except Exception:
                pass

        if session_backfill_mode and not existing.empty:
            before_dates = {
                pd.Timestamp(ts).floor("min")
                for ts in _to_ist(existing["date"]).dropna().tolist()
            }
            merged_dates = _to_ist(merged["date"])
            new_rows = merged.loc[
                ~merged_dates.dt.floor("min").isin(before_dates)
            ].copy()
        elif existed_before and last_before_ts is not None:
            new_rows = merged[merged["date"] > last_before_ts].copy()
        else:
            new_rows = merged.copy()

        new_rows_count = int(len(new_rows))
        new_first = None
        new_last = None
        merged_last = None
        merged_last_ts = pd.to_datetime(merged["date"], errors="coerce").dropna().max()
        if pd.notna(merged_last_ts):
            merged_last = merged_last_ts.strftime("%Y-%m-%d %H:%M:%S")
        merged_state = _classify_symbol_timestamp(
            merged_last_ts,
            exp.get("value"),
            _mode_step_minutes(mode) or 0,
        )
        if (
            merged_state == "current"
            and mode == "5min"
            and DEFAULT_ENFORCE_5MIN_SESSION_COMPLETENESS
            and _missing_5min_session_stamps_from_df(merged, _coerce_ist_datetime(exp.get("value")))
        ):
            merged_state = "unresolved"
        if new_rows_count > 0:
            nf = pd.to_datetime(new_rows["date"], errors="coerce").dropna().min()
            nl = pd.to_datetime(new_rows["date"], errors="coerce").dropna().max()
            new_first = nf.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(nf) else None
            new_last = nl.strftime("%Y-%m-%d %H:%M:%S") if pd.notna(nl) else None

        new_rows_path = None
        if new_rows_count > 0 and DEFAULT_SAVE_NEW_ROWS_REPORTS:
            rep_dir = os.path.join(report_dir, "missing_rows", mode)
            _safe_mkdir(rep_dir)
            new_rows_path = os.path.join(rep_dir, f"{ticker}_missing_rows_{mode}.parquet")
            _finalize_and_save(new_rows, new_rows_path)

            if print_missing_rows:
                show = new_rows.tail(print_missing_rows_max)
                logger.info("[%s] %s NEW ROWS (last %d):\n%s",
                            mode.upper(), ticker, min(print_missing_rows_max, len(show)),
                            show.to_string(index=False))
        persist_secs = _time.perf_counter() - t_persist0

        status = "created" if not existed_before else ("updated" if new_rows_count > 0 else "noop")

        return UpdateReport(
            mode=mode,
            ticker=ticker,
            status=status,
            out_path=out_path,
            existed_before=existed_before,
            last_before=last_before_ts.strftime("%Y-%m-%d %H:%M:%S") if last_before_ts is not None else None,
            expected=exp_str,
            new_rows_count=new_rows_count,
            new_first=new_first,
            new_last=new_last,
            new_rows_path=new_rows_path,
            load_existing_secs=load_existing_secs,
            fetch_secs=fetch_secs,
            indicators_secs=indicators_secs,
            persist_secs=persist_secs,
            total_secs=_time.perf_counter() - t_total0,
            last_after=merged_last,
            data_state=merged_state,
        )

    except Exception as e:
        logger.exception("[%s] %s indicator/save failed: %s", mode.upper(), ticker, e)
        return UpdateReport(mode, ticker, "failed", out_path, existed_before,
                            last_before_ts.strftime("%Y-%m-%d %H:%M:%S") if last_before_ts is not None else None,
                            exp_str, 0, None, None, None,
                            load_existing_secs, fetch_secs, indicators_secs, persist_secs,
                            _time.perf_counter() - t_total0)


# ========= DRIVER =========

def run_mode(
    mode: str,
    max_workers: int,
    skip_if_fresh: bool,
    intraday_ts: str,
    holidays: set[date],
    refresh_tokens: bool,
    report_dir: str,
    print_missing_rows: bool,
    print_missing_rows_max: int
):
    logger = logging.getLogger("stocks_fetcher")
    t_mode0 = _time.perf_counter()

    mode = mode.lower().strip()
    if mode not in VALID_MODES:
        raise ValueError(f"Unknown mode '{mode}'. Expected: {', '.join(VALID_MODES)}")
    if mode == "5min":
        _refresh_fno_equity_symbols(logger)

    now_ist = datetime.now(IST_TZ)
    start_dt = get_start_date(mode, now_ist)

    step = 5 if mode == "5min" else 15
    end_dt = last_completed_intraday_end(now_ist, step, holidays)

    logger.info("=== MODE=%s | intraday_ts=%s | Window: %s -> %s (IST) ===",
                mode, intraday_ts, start_dt.strftime("%Y-%m-%d %H:%M"), end_dt.strftime("%Y-%m-%d %H:%M"))

    if end_dt <= start_dt:
        logger.info("End cutoff <= start. Nothing to fetch for %s.", mode)
        logger.info("[%s][TIMING] total=%.2fs (nothing_to_fetch)", mode.upper(), _time.perf_counter() - t_mode0)
        return {
            "verify_failed_count": 0,
            "verify_failed_sample": [],
            "total_elapsed_sec": float(_time.perf_counter() - t_mode0),
            **_symbol_outcome_summary([]),
        }

    syms, pre_token_map = load_stocks_universe(logger)

    missing_files: list[str] = []
    missing_rows: list[str] = []
    fresh: list[str] = []
    scan_last_ts: dict[str, object] = {}

    verify_expected_ts = end_dt if intraday_ts.lower() == "end" else (end_dt - timedelta(minutes=step))
    if verify_expected_ts.tzinfo is None:
        verify_expected_ts = IST_TZ.localize(verify_expected_ts)
    fresh_current: set[str] = set()
    fresh_previous: set[str] = set()

    t_scan0 = _time.perf_counter()
    if skip_if_fresh:
        for t in syms:
            t = t.upper()
            out_path = os.path.join(DIRS[mode]["out"], f"{t}_stocks_indicators_{mode}.parquet")
            ms = missing_spec(mode, out_path, now_ist, holidays, intraday_ts)
            scan_last_ts[t] = ms.get("last_ts")
            if ms["kind"] == "fresh":
                fresh.append(t)
                freshness_state = _classify_symbol_timestamp(
                    ms.get("last_ts"),
                    verify_expected_ts,
                    step,
                )
                if freshness_state == "current":
                    fresh_current.add(t)
                elif freshness_state == "previous_slot":
                    fresh_previous.add(t)
            elif ms["kind"] == "file_missing":
                missing_files.append(t)
                missing_rows.append(t)
            else:
                missing_rows.append(t)
    else:
        missing_rows = [t.upper() for t in syms]
    freshness_scan_secs = _time.perf_counter() - t_scan0

    if skip_if_fresh:
        logger.info("[%s] Missing files: %d", mode.upper(), len(missing_files))
        rep_dir = os.path.join(report_dir, "missing_files")
        _safe_mkdir(rep_dir)
        miss_file_path = os.path.join(rep_dir, f"missing_files_{mode}.txt")
        Path(miss_file_path).write_text("\n".join(missing_files), encoding="utf-8")
        logger.info("[%s] Missing files list saved: %s", mode.upper(), miss_file_path)
        if missing_files:
            logger.info("[%s] Missing files sample: %s", mode.upper(), ", ".join(missing_files[:50]))
        else:
            logger.info("[%s] Missing files list cleared (no missing files).", mode.upper())

        logger.info("[%s] Missing evaluation rows (stale symbols): %d", mode.upper(), len(missing_rows))
        logger.info("[%s] Fresh symbols: %d", mode.upper(), len(fresh))
    else:
        logger.info("[%s] no-skip enabled => processing all symbols: %d", mode.upper(), len(missing_rows))

    if not missing_rows:
        logger.info("[%s] Nothing missing - all symbols fresh.", mode.upper())
        t_verify0 = _time.perf_counter()
        ok_count, verify_failed = verify_mode_outputs(mode, syms, verify_expected_ts, logger)
        verify_secs = _time.perf_counter() - t_verify0
        logger.info("[%s][VERIFY] expected_last=%s | ok=%d/%d | failed=%d | elapsed=%.2fs",
                    mode.upper(),
                    verify_expected_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
                    ok_count, len(syms), len(verify_failed), verify_secs)
        logger.info("[%s][TIMING] scan=%.2fs | token_prep=0.00s | workers=0.00s | verify=%.2fs | total=%.2fs",
                    mode.upper(), freshness_scan_secs, verify_secs, _time.perf_counter() - t_mode0)
        verify_unresolved = set(_extract_failed_tickers(verify_failed, syms))
        return {
            "verify_failed_count": int(len(verify_failed)),
            "verify_failed_sample": list(verify_failed[:20]),
            "total_elapsed_sec": float(_time.perf_counter() - t_mode0),
            **_symbol_outcome_summary(
                syms,
                current_symbols=fresh_current - verify_unresolved,
                previous_slot_symbols=fresh_previous - verify_unresolved,
                unresolved_symbols=verify_unresolved,
                noop_symbols=syms,
            ),
        }

    t_token0 = _time.perf_counter()
    kite = setup_kite_session()

    token_map = {k.upper(): int(v) for k, v in dict(pre_token_map).items()}
    need_tokens = [t for t in missing_rows if t.upper() not in token_map]

    if need_tokens:
        fetched = load_or_fetch_tokens(kite, need_tokens, logger, refresh=refresh_tokens)
        token_map.update({k.upper(): int(v) for k, v in fetched.items()})
    token_prep_secs = _time.perf_counter() - t_token0

    work_items = []
    token_missing_symbols: set[str] = set()
    for t in missing_rows:
        tok = token_map.get(t.upper())
        if not tok:
            logger.warning("No token for %s, skipping.", t)
            token_missing_symbols.add(t.upper())
            continue
        work_items.append((t.upper(), int(tok)))

    if not work_items:
        logger.info("No valid symbols with tokens.")
        t_verify0 = _time.perf_counter()
        ok_count, verify_failed = verify_mode_outputs(mode, syms, verify_expected_ts, logger)
        verify_secs = _time.perf_counter() - t_verify0
        logger.info("[%s][VERIFY] expected_last=%s | ok=%d/%d | failed=%d | elapsed=%.2fs",
                    mode.upper(),
                    verify_expected_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
                    ok_count, len(syms), len(verify_failed), verify_secs)
        logger.info("[%s][TIMING] scan=%.2fs | token_prep=%.2fs | workers=0.00s | verify=%.2fs | total=%.2fs",
                    mode.upper(), freshness_scan_secs, token_prep_secs, verify_secs, _time.perf_counter() - t_mode0)
        verify_unresolved = set(_extract_failed_tickers(verify_failed, syms))
        return {
            "verify_failed_count": int(len(verify_failed)),
            "verify_failed_sample": list(verify_failed[:20]),
            "total_elapsed_sec": float(_time.perf_counter() - t_mode0),
            **_symbol_outcome_summary(
                syms,
                current_symbols=fresh_current - verify_unresolved,
                previous_slot_symbols=fresh_previous - verify_unresolved,
                unresolved_symbols=set(missing_rows) | verify_unresolved,
                token_missing_symbols=token_missing_symbols,
                noop_symbols=fresh,
            ),
        }

    logger.info("[%s] Processing ONLY missing symbols=%d with max_workers=%d ...", mode.upper(), len(work_items), max_workers)

    all_reports: list[UpdateReport] = []
    updated_reports: list[UpdateReport] = []
    allow_previous_slot_tickers: set[str] = set()
    current_symbols: set[str] = set(fresh_current)
    previous_slot_symbols: set[str] = set(fresh_previous)
    failed_symbols: set[str] = set()
    unresolved_symbols: set[str] = set()
    written_symbols: set[str] = set()
    noop_symbols: set[str] = set()
    failed = 0

    t_workers0 = _time.perf_counter()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_ticker,
                mode, tkr, tok, kite, start_dt, end_dt,
                logger, holidays,
                skip_if_fresh, intraday_ts,
                report_dir, print_missing_rows, print_missing_rows_max,
                scan_last_ts.get(tkr), bool(skip_if_fresh),
            ): tkr
            for (tkr, tok) in work_items
        }
        for fut in as_completed(futures):
            tkr = futures[fut]
            try:
                rep: UpdateReport = fut.result()
                rep.last_after = rep.last_after or rep.new_last or rep.last_before
                if rep.status == "failed":
                    rep.data_state = "failed"
                elif (
                    rep.data_state not in {"current", "previous_slot"}
                    and (rep.status in {"created", "updated"} or not skip_if_fresh)
                ):
                    rep.data_state = _classify_symbol_timestamp(
                        rep.last_after,
                        verify_expected_ts,
                        step,
                    )
                all_reports.append(rep)
                if rep.allow_previous_slot_verify:
                    allow_previous_slot_tickers.add(rep.ticker.upper())
                if rep.status == "failed":
                    failed += 1
                    failed_symbols.add(rep.ticker.upper())
                elif rep.data_state == "current":
                    current_symbols.add(rep.ticker.upper())
                elif rep.data_state == "previous_slot":
                    previous_slot_symbols.add(rep.ticker.upper())
                else:
                    unresolved_symbols.add(rep.ticker.upper())
                if rep.status in ("created", "updated"):
                    updated_reports.append(rep)
                    written_symbols.add(rep.ticker.upper())
                elif rep.status == "noop":
                    noop_symbols.add(rep.ticker.upper())
            except Exception as e:
                failed += 1
                failed_symbols.add(tkr.upper())
                logger.exception("Worker crashed for %s (%s): %s", tkr, mode, e)
    workers_secs = _time.perf_counter() - t_workers0

    if updated_reports:
        logger.info("[%s] Updated symbols: %d", mode.upper(), len(updated_reports))
        if DEFAULT_LOG_UPDATED_TICKERS:
            for r in sorted(updated_reports, key=lambda x: x.ticker):
                logger.info(
                    "[%s] %s %s | last_before=%s | expected=%s | new_rows=%d | new_range=%s -> %s | "
                    "timing_s(load=%.2f,fetch=%.2f,ind=%.2f,persist=%.2f,total=%.2f) | new_rows_store=%s",
                    mode.upper(),
                    r.ticker,
                    r.status,
                    r.last_before,
                    r.expected,
                    r.new_rows_count,
                    r.new_first,
                    r.new_last,
                    r.load_existing_secs,
                    r.fetch_secs,
                    r.indicators_secs,
                    r.persist_secs,
                    r.total_secs,
                    r.new_rows_path
                )
        else:
            slowest = sorted(updated_reports, key=lambda x: x.total_secs, reverse=True)[:DEFAULT_LOG_UPDATED_TICKERS_TOP_N]
            if slowest:
                logger.info(
                    "[%s] Slowest updated symbols sample: %s",
                    mode.upper(),
                    ", ".join(f"{r.ticker}:{r.total_secs:.2f}s" for r in slowest),
                )
    else:
        logger.info("[%s] No new rows were appended (everything ended up noop).", mode.upper())

    if failed:
        logger.warning("[%s] Failed symbols: %d (see stocks_fetcher_run.log)", mode.upper(), failed)

    if all_reports:
        n = len(all_reports)
        sum_load = sum(r.load_existing_secs for r in all_reports)
        sum_fetch = sum(r.fetch_secs for r in all_reports)
        sum_ind = sum(r.indicators_secs for r in all_reports)
        sum_persist = sum(r.persist_secs for r in all_reports)
        sum_total = sum(r.total_secs for r in all_reports)
        logger.info("[%s][TIMING] per_ticker_avg_s load=%.2f fetch=%.2f ind=%.2f persist=%.2f total=%.2f (n=%d)",
                    mode.upper(),
                    sum_load / n,
                    sum_fetch / n,
                    sum_ind / n,
                    sum_persist / n,
                    sum_total / n,
                    n)
        logger.info("[%s][TIMING] per_ticker_sum_s load=%.2f fetch=%.2f ind=%.2f persist=%.2f total=%.2f",
                    mode.upper(), sum_load, sum_fetch, sum_ind, sum_persist, sum_total)

    verify_universe = [s for s in syms if s.upper() not in _load_invalid_symbols()]
    skipped_invalid = len(syms) - len(verify_universe)
    if skipped_invalid > 0:
        logger.warning(
            "[%s][VERIFY] Skipping %d quarantined symbol(s) from verification.",
            mode.upper(),
            skipped_invalid,
        )
    verify_symbols = _sample_verify_symbols(verify_universe, DEFAULT_VERIFY_SAMPLE_SIZE)
    if len(verify_symbols) != len(verify_universe):
        logger.info(
            "[%s][VERIFY] Fast sample enabled: checking %d/%d symbols",
            mode.upper(),
            len(verify_symbols),
            len(verify_universe),
        )
    t_verify0 = _time.perf_counter()
    ok_count, verify_failed = verify_mode_outputs(
        mode,
        verify_symbols,
        verify_expected_ts,
        logger,
        allow_previous_slot_tickers=allow_previous_slot_tickers,
    )
    verify_secs = _time.perf_counter() - t_verify0
    logger.info("[%s][VERIFY] expected_last=%s | ok=%d/%d | failed=%d | elapsed=%.2fs",
                mode.upper(),
                verify_expected_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
                ok_count, len(verify_symbols), len(verify_failed), verify_secs)

    recovery_secs = 0.0
    verify_post_secs = 0.0
    recovery_candidates: set[str] = set()
    if verify_failed:
        recovery_candidates = set(_extract_failed_tickers(verify_failed, verify_symbols))
        recovery_secs = _recover_verify_failures(
            mode=mode,
            verify_failed=verify_failed,
            all_symbols=verify_symbols,
            expected_ts_ist=verify_expected_ts,
            kite=kite,
            token_map=token_map,
            refresh_tokens=refresh_tokens,
            max_workers=max_workers,
            start_dt_ist=start_dt,
            end_dt_ist=end_dt,
            logger=logger,
            holidays=holidays,
            intraday_ts=intraday_ts,
            report_dir=report_dir,
            print_missing_rows=print_missing_rows,
            print_missing_rows_max=print_missing_rows_max,
            allow_previous_slot_tickers=allow_previous_slot_tickers,
        )
        t_verify1 = _time.perf_counter()
        verify_universe = [s for s in syms if s.upper() not in _load_invalid_symbols()]
        ok_count, verify_failed = verify_mode_outputs(
            mode,
            verify_universe,
            verify_expected_ts,
            logger,
            allow_previous_slot_tickers=allow_previous_slot_tickers,
        )
        verify_post_secs = _time.perf_counter() - t_verify1
        logger.info("[%s][VERIFY][POST] expected_last=%s | ok=%d/%d | failed=%d | elapsed=%.2fs",
                    mode.upper(),
                    verify_expected_ts.strftime("%Y-%m-%d %H:%M:%S%z"),
                    ok_count, len(verify_universe), len(verify_failed), verify_post_secs)

        final_verify_failed = set(_extract_failed_tickers(verify_failed, verify_universe))
        for ticker in recovery_candidates:
            if ticker in final_verify_failed:
                if ticker not in failed_symbols:
                    unresolved_symbols.add(ticker)
                continue

            out_path = os.path.join(
                DIRS[mode]["out"],
                f"{ticker}_stocks_indicators_{mode}.parquet",
            )
            recovered_state = _classify_symbol_timestamp(
                _read_last_ts_from_store(_resolve_existing_store_path(out_path)),
                verify_expected_ts,
                step,
            )
            failed_symbols.discard(ticker)
            unresolved_symbols.discard(ticker)
            token_missing_symbols.discard(ticker)
            current_symbols.discard(ticker)
            previous_slot_symbols.discard(ticker)
            if recovered_state == "current":
                current_symbols.add(ticker)
            elif recovered_state == "previous_slot":
                previous_slot_symbols.add(ticker)
            else:
                unresolved_symbols.add(ticker)

    if recovery_secs > 0.0:
        logger.info("[%s][TIMING] scan=%.2fs | token_prep=%.2fs | workers=%.2fs | verify_pre=%.2fs | recover=%.2fs | verify_post=%.2fs | total=%.2fs",
                    mode.upper(),
                    freshness_scan_secs,
                    token_prep_secs,
                    workers_secs,
                    verify_secs,
                    recovery_secs,
                    verify_post_secs,
                    _time.perf_counter() - t_mode0)
    else:
        logger.info("[%s][TIMING] scan=%.2fs | token_prep=%.2fs | workers=%.2fs | verify=%.2fs | total=%.2fs",
                    mode.upper(),
                    freshness_scan_secs,
                    token_prep_secs,
                    workers_secs,
                    verify_secs,
                    _time.perf_counter() - t_mode0)
    final_verify_unresolved = set(_extract_failed_tickers(verify_failed, verify_universe))
    unresolved_symbols |= final_verify_unresolved - failed_symbols
    return {
        "verify_failed_count": int(len(verify_failed)),
        "verify_failed_sample": list(verify_failed[:20]),
        "total_elapsed_sec": float(_time.perf_counter() - t_mode0),
        **_symbol_outcome_summary(
            syms,
            current_symbols=current_symbols,
            previous_slot_symbols=previous_slot_symbols,
            failed_symbols=failed_symbols,
            unresolved_symbols=unresolved_symbols | token_missing_symbols,
            token_missing_symbols=token_missing_symbols,
            written_symbols=written_symbols,
            noop_symbols=noop_symbols,
        ),
    }

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("mode", nargs="?", default="all", help="5min|15min|all")
    p.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    p.add_argument("--no-skip", action="store_true",
                   help="Disable freshness skip (will refetch/recompute even if fresh)")
    p.add_argument("--intraday-ts", default=DEFAULT_INTRADAY_TIMESTAMP, choices=["start", "end"],
                   help="Store intraday timestamps as candle start or candle end (end recommended)")
    p.add_argument("--holidays-file", default=HOLIDAYS_FILE_DEFAULT,
                   help="Optional NSE holidays file (CSV with 'date' or one date per line)")
    p.add_argument("--refresh-tokens", action="store_true",
                   help="Force refresh token cache (kite.instruments NSE)")

    p.add_argument("--no-migrate-csv", action="store_true",
                   help="Do NOT read legacy CSV outputs (Parquet-only).")
    p.add_argument("--delete-legacy-csv", action="store_true",
                   help="After successful Parquet write, delete legacy CSV outputs (if they exist).")

    p.add_argument("--report-dir", default="reports/stocks_missing_reports",
                   help="Directory to write missing-files and missing-rows reports")
    p.add_argument("--print-missing-rows", action="store_true",
                   help="Print a small preview of newly appended rows per symbol")
    p.add_argument("--print-missing-rows-max", type=int, default=5,
                   help="Max rows to print per symbol when --print-missing-rows is enabled")

    return p.parse_args()


def main():
    logger = setup_logger()
    args = parse_args()

    global MIGRATE_LEGACY_CSV, DELETE_LEGACY_CSV
    MIGRATE_LEGACY_CSV = not args.no_migrate_csv
    DELETE_LEGACY_CSV = bool(args.delete_legacy_csv)

    if MIGRATE_LEGACY_CSV:
        logger.info("Legacy CSV migration ENABLED: will read *.csv if *.parquet is missing.")
    else:
        logger.info("Legacy CSV migration DISABLED: Parquet-only.")

    if DELETE_LEGACY_CSV:
        logger.warning("Legacy CSV deletion ENABLED: legacy *.csv files will be deleted after successful Parquet writes.")

    holidays = _read_holidays(args.holidays_file)
    if holidays:
        logger.info("Loaded %d holidays from %s", len(holidays), args.holidays_file)
    else:
        logger.info("No holidays loaded (weekend-only calendar).")

    mode = args.mode.lower().strip()
    skip_if_fresh = not args.no_skip

    if mode == "all":
        for m in VALID_MODES:
            run_mode(
                m,
                max_workers=args.max_workers,
                skip_if_fresh=skip_if_fresh,
                intraday_ts=args.intraday_ts,
                holidays=holidays,
                refresh_tokens=args.refresh_tokens,
                report_dir=args.report_dir,
                print_missing_rows=args.print_missing_rows,
                print_missing_rows_max=args.print_missing_rows_max
            )
    else:
        run_mode(
            mode,
            max_workers=args.max_workers,
            skip_if_fresh=skip_if_fresh,
            intraday_ts=args.intraday_ts,
            holidays=holidays,
            refresh_tokens=args.refresh_tokens,
            report_dir=args.report_dir,
            print_missing_rows=args.print_missing_rows,
            print_missing_rows_max=args.print_missing_rows_max
        )


if __name__ == "__main__":
    main()

