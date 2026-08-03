#!/usr/bin/env python3
"""
Shared helpers for Full-Pipeline Entry Research v2 and the shadow monitor.

Read-only. Provides:
  * CANON  - canonical feature -> {pool, truth} column aliases (features that
             exist, with the SAME meaning, in BOTH the unified pool and the live
             truth tables, so a mined condition can be applied to either source).
  * MinuteStore  - per-ticker 1-min OHLC arrays for a live-faithful walk.
  * resolve_one  - intraday/EOD (15:20 IST) first-touch resolver: enter at the
             first 1-min open after the signal, target/stop, else EOD close.
             Same-bar target+stop -> SL first. Net of the house v6 cost
             (16 bps flat + 3 bps on stops, 50k notional).

Nothing here modifies any live scanner/filter/setup/executor/backtesting config.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import runtime_dir
import avwap_5min_ID_v6_backtesting as v6  # house cost model (_net_pnl_rs, EFFECTIVE_NOTIONAL)


# ---- paths -----------------------------------------------------------------
POOL_CSV = runtime_dir("outputs_ID_v11_unified_pool") / "historical_all_available_pre_dedupe_live_candidates.csv"
TRUTH_DIR = runtime_dir("live_research_v7_research_layer", "truth_table")
DATA_1MIN_DIR = runtime_dir("stocks_indicators_1min_eq")
UNIVERSE_CSV = Path(__file__).resolve().parent / "configs" / "universe.csv"

NOTIONAL_RS = float(getattr(v6, "EFFECTIVE_NOTIONAL", 50_000.0))
DEFAULT_COST_BPS = float(getattr(v6, "DEFAULT_COST_BPS", 16.0))

# EOD square-off (matches v17D_exit_resolver / live), IST.
EOD_CUTOFF_SEC = 15 * 3600 + 20 * 60
_IST_OFF_NS = 19_800_000_000_000  # +05:30, India has no DST
_DAY_NS = 86_400_000_000_000


# ---- canonical feature map -------------------------------------------------
# Only features present with the SAME semantics in BOTH sources are mineable, so
# a discovery on the pool can be monitored forward on live truth tables.
CANON: dict[str, dict[str, str]] = {
    "quality_score":  {"pool": "quality_score",  "truth": "quality_score"},
    "rs_pct":         {"pool": "rs_pct",          "truth": "rs_pct"},
    "vol_ratio":      {"pool": "vol_ratio",       "truth": "vol_ratio"},
    "atr_pct":        {"pool": "atr_pct",         "truth": "atr_pct"},
    "body_pct":       {"pool": "body_pct",        "truth": "body_pct"},
    "close_loc":      {"pool": "close_loc",       "truth": "close_loc"},
    "vwap_dist_atr":  {"pool": "vwap_dist_atr",   "truth": "vwap_dist_atr"},
    "adx":            {"pool": "adx",             "truth": "sig5_adx_calc"},
    "ranker_score":   {"pool": "ranker_score",    "truth": "scanner_ranker_score"},
}
# market_ret is used for SHORT market-neutralization only, never as a mined band.
MARKET_RET = {"pool": "market_ret_pct", "truth": "market_ret_pct"}
MINEABLE_FEATURES = tuple(CANON.keys())


def canon_columns(source: str) -> dict[str, str]:
    """canonical name -> source column name (source in {'pool','truth'})."""
    return {canon: alias[source] for canon, alias in CANON.items()}


def rename_to_canon(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Return a copy with source columns renamed to canonical feature names."""
    mapping = {alias[source]: canon for canon, alias in CANON.items() if alias[source] in df.columns}
    mret = MARKET_RET[source]
    if mret in df.columns:
        mapping[mret] = "market_ret_pct"
    return df.rename(columns=mapping)


# ---- small utils -----------------------------------------------------------
def sf(value: Any, default: float = np.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def to_ns(ts: Any) -> int:
    t = pd.to_datetime(ts, errors="coerce")
    if pd.isna(t):
        return -1
    if getattr(t, "tzinfo", None) is None:
        t = t.tz_localize("Asia/Kolkata")
    return int(t.value)


def ns_to_ist_iso(ns: int) -> str:
    return pd.Timestamp(int(ns) + _IST_OFF_NS).isoformat() + "+05:30"


def ist_day_str(ns: int) -> str:
    return pd.Timestamp((int(ns) + _IST_OFF_NS)).strftime("%Y-%m-%d")


def _eod_ns_for(sig_ns: int) -> int:
    day_idx = (sig_ns + _IST_OFF_NS) // _DAY_NS
    day_start_utc = day_idx * _DAY_NS - _IST_OFF_NS
    return int(day_start_utc + EOD_CUTOFF_SEC * 1_000_000_000)


# ---- 1-min store -----------------------------------------------------------
class MinuteStore:
    """Loads one ticker's 1-min OHLC at a time (memory-safe for whole-pool sweeps)."""

    def __init__(self, data_dir: Path = DATA_1MIN_DIR):
        self.data_dir = data_dir
        self.missing: set[str] = set()

    def load(self, ticker: str) -> dict[str, Any] | None:
        key = str(ticker or "").upper().strip()
        if not key:
            return None
        path = self.data_dir / f"{key}_stocks_indicators_1min.parquet"
        if not path.exists():
            self.missing.add(key)
            return None
        try:
            df = pd.read_parquet(path, columns=["date", "open", "high", "low", "close"])
        except Exception:
            self.missing.add(key)
            return None
        bt = pd.to_datetime(df["date"], utc=True, errors="coerce")
        df = df.assign(_bt=bt).dropna(subset=["_bt", "open", "high", "low", "close"]).sort_values("_bt")
        if df.empty:
            self.missing.add(key)
            return None
        return {
            "ts": df["_bt"].to_numpy(dtype="datetime64[ns]").view("int64"),
            "o": df["open"].to_numpy(dtype=float, na_value=np.nan),
            "h": df["high"].to_numpy(dtype=float, na_value=np.nan),
            "l": df["low"].to_numpy(dtype=float, na_value=np.nan),
            "c": df["close"].to_numpy(dtype=float, na_value=np.nan),
        }


def resolve_one(arr: dict[str, Any] | None, signal_ns: int, side: str,
                sl_pct: float, tgt_pct: float, cost_bps: float) -> dict[str, Any] | None:
    """Intraday/EOD first-touch resolution for one signal. None if unresolvable."""
    if arr is None or signal_ns < 0:
        return None
    ts = arr["ts"]
    start = int(np.searchsorted(ts, signal_ns, side="right"))
    if start >= len(ts):
        return None
    eod_ns = _eod_ns_for(signal_ns)
    entry_ns = int(ts[start])
    # entry must be same IST day as the signal and at/before EOD cutoff
    if entry_ns > eod_ns or (entry_ns + _IST_OFF_NS) // _DAY_NS != (signal_ns + _IST_OFF_NS) // _DAY_NS:
        return None
    entry_price = float(arr["o"][start])
    if not math.isfinite(entry_price) or entry_price <= 0:
        return None
    end = int(np.searchsorted(ts, eod_ns, side="right"))
    hh = arr["h"][start:end]
    ll = arr["l"][start:end]
    cc = arr["c"][start:end]
    if hh.size == 0:
        return None

    is_long = str(side).upper() == "LONG"
    if is_long:
        tgt = entry_price * (1.0 + tgt_pct / 100.0)
        sl = entry_price * (1.0 - sl_pct / 100.0)
        hit_t = hh >= tgt
        hit_s = ll <= sl
    else:
        tgt = entry_price * (1.0 - tgt_pct / 100.0)
        sl = entry_price * (1.0 + sl_pct / 100.0)
        hit_t = ll <= tgt
        hit_s = hh >= sl

    have_t, have_s = bool(hit_t.any()), bool(hit_s.any())
    ti = int(np.argmax(hit_t)) if have_t else -1
    si = int(np.argmax(hit_s)) if have_s else -1
    if have_t and have_s:
        outcome, exit_i = ("SL", si) if si <= ti else ("TARGET", ti)      # tie -> SL first
    elif have_t:
        outcome, exit_i = "TARGET", ti
    elif have_s:
        outcome, exit_i = "SL", si
    else:
        outcome, exit_i = "EOD", len(cc) - 1

    if outcome == "TARGET":
        price_pnl_pct = tgt_pct
        exit_price = tgt
    elif outcome == "SL":
        price_pnl_pct = -sl_pct
        exit_price = sl
    else:
        exit_price = float(cc[-1])
        price_pnl_pct = (exit_price - entry_price) / entry_price * 100.0 * (1.0 if is_long else -1.0)

    net, gross, cost = v6._net_pnl_rs(price_pnl_pct, outcome, cost_bps)
    return {
        "outcome": outcome,
        "entry_time_ist": ns_to_ist_iso(entry_ns),
        "entry_price": entry_price,
        "exit_time_ist": ns_to_ist_iso(int(ts[start + exit_i])),
        "exit_price": float(exit_price),
        "bars_held": int(exit_i + 1),
        "pnl_pct_price": float(price_pnl_pct),
        "gross_pnl_rs": float(gross),
        "cost_rs": float(cost),
        "pnl_rs": float(net),
    }


# ---- ADV / liquidity -------------------------------------------------------
def load_adv_map(min_rs_cr: float = 50.0) -> tuple[dict[str, float], set[str]]:
    """Return (ticker->adv_rs_cr, set of tickers with adv >= min). Garbage/neg dropped."""
    if not UNIVERSE_CSV.exists():
        return {}, set()
    try:
        df = pd.read_csv(UNIVERSE_CSV)
    except Exception:
        return {}, set()
    if "ticker" not in df.columns or "adv_rs_cr" not in df.columns:
        return {}, set()
    df = df.copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["adv_rs_cr"] = pd.to_numeric(df["adv_rs_cr"], errors="coerce")
    df = df[df["adv_rs_cr"] > 0]  # drop garbage negatives / NaN
    adv = dict(zip(df["ticker"], df["adv_rs_cr"]))
    liquid = {t for t, v in adv.items() if v >= min_rs_cr}
    return adv, liquid


# ---- metrics ---------------------------------------------------------------
def profit_factor(net: pd.Series | np.ndarray) -> float:
    s = pd.to_numeric(pd.Series(net), errors="coerce").fillna(0.0)
    gains = float(s[s > 0].sum())
    losses = float(-s[s < 0].sum())
    if losses > 0:
        return gains / losses
    return float("inf") if gains > 0 else 0.0


def block_metrics(part: pd.DataFrame, day_col: str = "_day") -> dict[str, Any]:
    if part is None or part.empty:
        return {"trades": 0, "days": 0, "target_hits": 0, "sl_hits": 0,
                "win_rate": 0.0, "net_pnl_rs": 0.0, "profit_factor": 0.0, "top1_day_share": 0.0}
    net = pd.to_numeric(part["pnl_rs"], errors="coerce").fillna(0.0)
    tgt = int((part["outcome"] == "TARGET").sum())
    sl = int((part["outcome"] == "SL").sum())
    trades = int(len(part))
    pnl = float(net.sum())
    pos = float(net[net > 0].sum())
    day_pnl = part.assign(_n=net).groupby(day_col)["_n"].sum()
    top1_share = float(day_pnl.max() / pos) if (pos > 0 and len(day_pnl)) else 0.0
    return {
        "trades": trades,
        "days": int(part[day_col].nunique()),
        "target_hits": tgt,
        "sl_hits": sl,
        "win_rate": tgt / trades if trades else 0.0,
        "net_pnl_rs": pnl,
        "profit_factor": profit_factor(net),
        "top1_day_share": top1_share,
    }
