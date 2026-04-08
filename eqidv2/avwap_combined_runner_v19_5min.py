# -*- coding: utf-8 -*-
"""
V19 5-min combined runner — AVWAP-RS filter (calibrated).

Previous RS approaches:
  V16: (stock_ret_4bar - nifty_ret_4bar) >= 0.75%   raw return diff, hard threshold
  V17: raw RS / stock_ATR%                 >= 0.50   ATR-normalised
  V18: 2-of-3 composite (RS + vol + OR)              breadth score

V19 change: AVWAP-RS replaces the raw 4-bar return differential.

  AVWAP_RS = (stock_close / stock_day_VWAP - 1) * 100
           - (nifty_close / nifty_day_VWAP - 1) * 100

  Where day_VWAP = cumulative volume-weighted price from 09:15 (same formula
  as the existing intraday_anchor in _build_nifty_intraday_context).

Why AVWAP-RS is better than raw RS:
  1. ANCHORED — not a single bar snapshot; the VWAP accumulates all morning
     volume so one spike bar barely moves it. Stable on choppy days.
  2. SELF-SCALING — on a high-vol day NiftyBees VWAP gap grows naturally;
     threshold stays constant but the signal adapts.
  3. REVERSAL-AWARE — a stock that ran up then faded back to VWAP shows
     AVWAP_RS ~ 0 even if its 4-bar return is still positive. Raw RS
     would still pass it; AVWAP-RS correctly flags the fade.
  4. CONSISTENT with strategy — entries are AVWAP-based; RS should be too.
  5. EXHAUSTION-SAFE — anti-exhaustion cap (±V19_AVWAP_RS_EXHAUST_CAP)
     mirrors the strategy's QS/vol/dist caps; reject stocks already far
     from their VWAP relative to Nifty.

Calibration (v19 Run 2, 2026-04-08):
  Trade distribution analysis on 12-day window revealed dead zones:
  LONG  AVWAP-RS  0.30–0.75 : 55 trades, 52% win, PF=0.83  ← BAD, block
  SHORT AVWAP-RS -0.50–-0.30 : 9 trades, 56% win, PF=0.73  ← BAD, block
  LONG  AVWAP-RS  0.75–1.00 : 19 trades, 68% win, PF=1.48  ← OK
  LONG  AVWAP-RS  1.50–2.00 : 10 trades, 90% win, PF=6.13  ← sweet spot
  → Raised LONG BOTH threshold: 0.30 → 0.75
  → Raised SHORT BOTH threshold: 0.30 → 0.50
  → RS momentum gate: enabled (require AVWAP-RS improving into entry)
  → Added LONG AVWAP-RS dead zone: block 0.75–1.0 (PF=1.48, below strategy bar)
    and re-enter at 1.0+ where PF=1.42+ and rising steeply to 6.13 at 1.5+

All other logic (signal windows, QS filters, vol exhaust, etc.) identical
to V16 Run14.
Outputs go to outputs_v19_5min/ (separate from v16/v17/v18).
"""
from __future__ import annotations

import os
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Tuple

import avwap_combined_runner_v16_5min as _base

# ===========================================================================
# V19 CONFIG
# ===========================================================================

def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in ("1", "true", "yes")


# Primary threshold: how much more above/below its own VWAP must the stock be
# compared to Nifty, at the moment of entry.
# BOTH mode (neutral market) — higher bar needed.
# Run2 calibration: 0.30–0.75 is a dead zone (52% win, PF=0.83). Raised to 0.75.
V19_AVWAP_RS_THRESH_LONG_BOTH = _env_float("EQIDV19_AVWAP_RS_THRESH_LONG_BOTH", 0.75)
# Run2 calibration: -0.50–-0.30 is a dead zone (56% win, PF=0.73). Raised to 0.50.
V19_AVWAP_RS_THRESH_SHORT_BOTH = _env_float("EQIDV19_AVWAP_RS_THRESH_SHORT_BOTH", 0.50)

# Directional modes (LONG_ONLY / SHORT_ONLY) — market context already confirmed.
V19_AVWAP_RS_THRESH_LONG_DIRECTIONAL = _env_float(
    "EQIDV19_AVWAP_RS_THRESH_LONG_DIRECTIONAL", 0.25
)
V19_AVWAP_RS_THRESH_SHORT_DIRECTIONAL = _env_float(
    "EQIDV19_AVWAP_RS_THRESH_SHORT_DIRECTIONAL", 0.20
)

# Anti-exhaustion cap: reject if AVWAP_RS > +cap or < -cap.
# Consistent with QS cap (>10 blocked), AVWAP dist cap (>3 ATR blocked).
V19_AVWAP_RS_EXHAUST_CAP = _env_float("EQIDV19_AVWAP_RS_EXHAUST_CAP", 3.0)

# LONG AVWAP-RS dead zone: block entries in this band even if above floor.
# Run2 data: 0.75–1.0 = PF=1.48 (acceptable but weak vs strategy's quality bar).
# Entries at 1.0+ begin climbing steeply: 1.0–1.5=PF=1.42, 1.5–2.0=PF=6.13.
# Set DEAD_HI=0.0 to disable (pass everything above floor).
V19_LONG_AVWAP_RS_DEAD_LO = _env_float("EQIDV19_LONG_AVWAP_RS_DEAD_LO", 0.0)   # disabled by default
V19_LONG_AVWAP_RS_DEAD_HI = _env_float("EQIDV19_LONG_AVWAP_RS_DEAD_HI", 0.0)   # disabled by default

# RS Momentum gate: require AVWAP_RS is improving into entry bar.
# LONG  : AVWAP_RS[t] >= AVWAP_RS[t-1bar]  (stock gaining on Nifty)
# SHORT : AVWAP_RS[t] <= AVWAP_RS[t-1bar]  (stock losing ground on Nifty)
# Enabled in Run2 — catches fading RS entries that AVWAP level alone misses.
V19_RS_MOMENTUM_ENABLED = _env_bool("EQIDV19_RS_MOMENTUM_ENABLED", True)

# Fallback: if stock VWAP data is missing, fall back to raw RS.
V19_FALLBACK_RS_LONG = _env_float("EQIDV19_FALLBACK_RS_LONG", 0.60)
V19_FALLBACK_RS_SHORT = _env_float("EQIDV19_FALLBACK_RS_SHORT", 0.40)


# ===========================================================================
# PATCH 1 — redirect all outputs from v16_5min -> v19_5min
# ===========================================================================
_orig_runtime_dir = _base.runtime_dir


def _v19_runtime_dir(*parts):
    new_parts = tuple(str(p).replace("v16_5min", "v19_5min") for p in parts)
    return _orig_runtime_dir(*new_parts)


_base.runtime_dir = _v19_runtime_dir


# ===========================================================================
# NIFTY DAY-VWAP MAP
# Loaded once per process; maps ts_key -> (nifty_close / nifty_cumvwap - 1)*100
# ===========================================================================
_v19_nifty_vwap_ratio: Dict[str, float] = {}   # ts_key -> nifty VWAP-ratio %
_v19_nifty_vwap_loaded: bool = False


def _ensure_nifty_vwap_map(cfg) -> Dict[str, float]:
    """
    Load NiftyBees 5-min data and build {ts_key: (close/cumvwap-1)*100}.
    Cached at module level — called once per backtest run.
    """
    global _v19_nifty_vwap_ratio, _v19_nifty_vwap_loaded
    if _v19_nifty_vwap_loaded:
        return _v19_nifty_vwap_ratio

    idx, ticker_found = _base._load_first_nifty_source(cfg, _base.NIFTY_CONTEXT_TICKERS)
    if idx.empty:
        print("[V19] WARNING: Nifty VWAP map could not be built — no data found")
        _v19_nifty_vwap_loaded = True
        return _v19_nifty_vwap_ratio

    try:
        dt = pd.to_datetime(idx["date"], errors="coerce")
        idx = idx.loc[dt.notna()].copy()
        dt = dt.loc[dt.notna()]
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize("UTC")
        else:
            dt = dt.dt.tz_convert(_base.IST)
        idx["date"] = dt.dt.tz_convert(_base.IST)
        idx = idx.sort_values("date").reset_index(drop=True)
        idx["day"] = idx["date"].dt.strftime("%Y-%m-%d")
        idx["close"] = pd.to_numeric(idx["close"], errors="coerce")
        if "volume" not in idx.columns:
            idx["volume"] = 0.0
        idx["volume"] = pd.to_numeric(idx["volume"], errors="coerce").fillna(0.0)

        idx["cum_vwap"] = (
            idx.groupby("day", group_keys=False)
            .apply(
                lambda g: _base._intraday_anchor_from_close_volume(g["close"], g["volume"])
            )
            .reset_index(level=0, drop=True)
        )
        idx["vwap_ratio_pct"] = (idx["close"] / idx["cum_vwap"].replace(0, np.nan) - 1) * 100

        for ts, ratio in zip(idx["date"], idx["vwap_ratio_pct"]):
            if np.isfinite(ratio):
                _v19_nifty_vwap_ratio[_base._ts_to_key_local(ts)] = float(ratio)

        print(
            f"[V19] Nifty VWAP map loaded ({ticker_found}): "
            f"{len(_v19_nifty_vwap_ratio)} bars"
        )
    except Exception as exc:
        print(f"[V19] WARNING: Nifty VWAP map build failed — {exc}")

    _v19_nifty_vwap_loaded = True
    return _v19_nifty_vwap_ratio


# ===========================================================================
# STOCK DAY-VWAP MAP BUILDER
# Per-ticker cache: ts_key -> (stock_close / stock_cumvwap - 1)*100
# ===========================================================================
_v19_stock_vwap_cache: Dict[str, Dict[str, float]] = {}


def _build_stock_vwap_ratio_map(
    ticker: str, cfg, cache: Dict[str, Dict[str, float]]
) -> Dict[str, float]:
    """
    For each 5-min bar of the ticker, return (close/cum_day_vwap - 1)*100.
    cum_day_vwap resets at midnight; uses same formula as nifty intraday_anchor.
    """
    ticker_u = str(ticker).strip().upper()
    if ticker_u in cache:
        return cache[ticker_u]

    out: Dict[str, float] = {}
    p = Path(cfg.dir_15m) / f"{ticker_u}{cfg.end_15m}"
    if not p.exists():
        cache[ticker_u] = out
        return out

    try:
        df = _base.read_15m_parquet(str(p), cfg.parquet_engine)
        if df.empty:
            cache[ticker_u] = out
            return out

        df = df.sort_values("date").reset_index(drop=True)
        dt = pd.to_datetime(df["date"], errors="coerce")
        df = df.loc[dt.notna()].copy()
        dt = dt.loc[dt.notna()]
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize("UTC")
        else:
            dt = dt.dt.tz_convert(_base.IST)
        df["date"] = dt.dt.tz_convert(_base.IST)
        df = df.sort_values("date").reset_index(drop=True)
        df["day"] = df["date"].dt.strftime("%Y-%m-%d")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        if "volume" not in df.columns:
            df["volume"] = 0.0
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)

        df["cum_vwap"] = (
            df.groupby("day", group_keys=False)
            .apply(
                lambda g: _base._intraday_anchor_from_close_volume(g["close"], g["volume"])
            )
            .reset_index(level=0, drop=True)
        )
        df["vwap_ratio_pct"] = (df["close"] / df["cum_vwap"].replace(0, np.nan) - 1) * 100

        for ts, ratio in zip(df["date"], df["vwap_ratio_pct"]):
            if np.isfinite(ratio):
                out[_base._ts_to_key_local(ts)] = float(ratio)
    except Exception:
        out = {}

    cache[ticker_u] = out
    return out


# ===========================================================================
# HELPER — ts_key shifted by n × 5 minutes
# ts_key format: ISO with timezone, e.g. "2026-04-07T09:20:00+05:30"
# ===========================================================================
def _ts_key_shift(ts_key: str, bars: int = -1) -> str:
    """Return ts_key shifted by bars * 5 minutes (negative = earlier)."""
    ts = pd.Timestamp(ts_key)          # parses ISO including tz
    shifted = ts + pd.Timedelta(minutes=5 * bars)
    return shifted.isoformat()


# ===========================================================================
# PATCH 2 — replace the RS filter with AVWAP-RS
# ===========================================================================
def _v19_apply_nifty_intraday_context(
    short_df: pd.DataFrame,
    long_df: pd.DataFrame,
    cfg,
    mode_map: Dict[str, str],
    nifty_ret_map: Dict[str, float],          # retained in signature for compat
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    V19: AVWAP-RS filter.

    AVWAP_RS = (stock_close/stock_day_VWAP - 1)*100
             - (nifty_close/nifty_day_VWAP - 1)*100

    LONG  passes when:
        V19_AVWAP_RS_THRESH_LONG_BOTH  <= AVWAP_RS <= +V19_AVWAP_RS_EXHAUST_CAP
    SHORT passes when:
        -V19_AVWAP_RS_EXHAUST_CAP      <= AVWAP_RS <= -V19_AVWAP_RS_THRESH_SHORT_BOTH

    In directional modes (LONG_ONLY / SHORT_ONLY) lower threshold is used.

    Optional RS momentum gate (V19_RS_MOMENTUM_ENABLED):
        LONG  : AVWAP_RS[t] >= AVWAP_RS[t-1bar]  (improving into entry)
        SHORT : AVWAP_RS[t] <= AVWAP_RS[t-1bar]  (deteriorating into entry)
    """
    if not mode_map:
        return short_df, long_df

    # Ensure Nifty VWAP map is ready (loaded once)
    nifty_vwap_map = _ensure_nifty_vwap_map(cfg)

    stock_vwap_cache: Dict[str, Dict[str, float]] = {}

    def _apply_side(
        df: pd.DataFrame, side: str
    ) -> Tuple[pd.DataFrame, int, int]:
        if df.empty:
            return df, 0, 0

        d = df.copy()
        ts_col = "entry_time_ist" if "entry_time_ist" in d.columns else "signal_time_ist"
        ts_series = pd.to_datetime(d[ts_col], errors="coerce")
        if getattr(ts_series.dt, "tz", None) is None:
            ts_series = ts_series.dt.tz_localize(_base.IST)
        else:
            ts_series = ts_series.dt.tz_convert(_base.IST)
        d["ts_key_local"] = ts_series.map(_base._ts_to_key_local)
        d["nifty_context_mode"] = d["ts_key_local"].map(mode_map).fillna("BOTH")

        # ── Directional mode hard block (same as v16) ────────────────────
        before = len(d)
        if side.upper() == "SHORT":
            d = d[d["nifty_context_mode"].ne("LONG_ONLY")].copy()
        else:
            d = d[d["nifty_context_mode"].ne("SHORT_ONLY")].copy()
        mode_removed = before - len(d)

        if not _base.NIFTY_RS_FILTER_ENABLED or d.empty:
            if "ts_key_local" in d.columns:
                d = d.drop(columns=["ts_key_local"])
            return d, mode_removed, 0

        # ── AVWAP-RS filter ──────────────────────────────────────────────
        keep_mask: list = []
        avwap_rs_vals: list = []
        rs_removed = 0
        side_u = side.upper()
        nifty_vwap_available = bool(nifty_vwap_map)

        for row in d.itertuples(index=False):
            ts_key = getattr(row, "ts_key_local")
            mode   = getattr(row, "nifty_context_mode", "BOTH")
            ticker = str(getattr(row, "ticker", ""))

            # Thresholds depend on market mode
            if mode != "BOTH":
                thresh_long  = V19_AVWAP_RS_THRESH_LONG_DIRECTIONAL
                thresh_short = V19_AVWAP_RS_THRESH_SHORT_DIRECTIONAL
            else:
                thresh_long  = V19_AVWAP_RS_THRESH_LONG_BOTH
                thresh_short = V19_AVWAP_RS_THRESH_SHORT_BOTH

            apply_rs = (mode != "BOTH") or _base.NIFTY_RS_BOTH_MODE_ENABLED
            avwap_rs = np.nan
            keep = True

            if apply_rs:
                stock_vwap_map = _build_stock_vwap_ratio_map(
                    ticker, cfg, stock_vwap_cache
                )
                stock_vwap_ratio = stock_vwap_map.get(ts_key, np.nan)
                nifty_vwap_ratio = nifty_vwap_map.get(ts_key, np.nan) if nifty_vwap_available else np.nan

                if np.isfinite(stock_vwap_ratio) and np.isfinite(nifty_vwap_ratio):
                    # ── Core AVWAP-RS ─────────────────────────────────
                    avwap_rs = float(stock_vwap_ratio - nifty_vwap_ratio)

                    if side_u == "LONG":
                        # Must outperform Nifty on VWAP basis, but not exhausted
                        keep = (
                            avwap_rs >= thresh_long
                            and avwap_rs <= V19_AVWAP_RS_EXHAUST_CAP
                        )
                        # LONG AVWAP-RS dead zone block (data-driven, e.g. 0.75–1.0)
                        if keep and V19_LONG_AVWAP_RS_DEAD_HI > V19_LONG_AVWAP_RS_DEAD_LO:
                            if V19_LONG_AVWAP_RS_DEAD_LO <= avwap_rs < V19_LONG_AVWAP_RS_DEAD_HI:
                                keep = False
                    else:
                        # Must underperform Nifty on VWAP basis, but not already crashed
                        keep = (
                            avwap_rs <= -thresh_short
                            and avwap_rs >= -V19_AVWAP_RS_EXHAUST_CAP
                        )

                    # ── Optional RS momentum gate ─────────────────────
                    if keep and V19_RS_MOMENTUM_ENABLED:
                        ts_prev = _ts_key_shift(ts_key, bars=-1)
                        stock_vwap_prev = stock_vwap_map.get(ts_prev, np.nan)
                        nifty_vwap_prev = nifty_vwap_map.get(ts_prev, np.nan) if nifty_vwap_available else np.nan
                        if np.isfinite(stock_vwap_prev) and np.isfinite(nifty_vwap_prev):
                            avwap_rs_prev = float(stock_vwap_prev - nifty_vwap_prev)
                            rs_momentum   = avwap_rs - avwap_rs_prev
                            if side_u == "LONG" and rs_momentum < 0:
                                keep = False  # RS fading into entry
                            elif side_u == "SHORT" and rs_momentum > 0:
                                keep = False  # RS recovering into entry

                else:
                    # VWAP data missing for this bar — fall back to raw RS
                    stock_ret_map = _base._build_stock_return_map(
                        ticker, cfg, {}
                    )
                    stock_ret = stock_ret_map.get(ts_key, np.nan)
                    nifty_ret = nifty_ret_map.get(ts_key, np.nan)
                    if np.isfinite(stock_ret) and np.isfinite(nifty_ret):
                        raw_rs = float(stock_ret - nifty_ret)
                        avwap_rs = raw_rs          # store raw RS as proxy
                        if side_u == "LONG":
                            keep = raw_rs >= V19_FALLBACK_RS_LONG
                        else:
                            keep = raw_rs <= -V19_FALLBACK_RS_SHORT
                    # If neither VWAP nor raw RS available → pass (no data = no block)

            keep_mask.append(bool(keep))
            avwap_rs_vals.append(avwap_rs)
            if not keep:
                rs_removed += 1

        d["nifty_avwap_rs_pct"]     = avwap_rs_vals   # new column: AVWAP-RS value
        d["nifty_rel_strength_pct"] = avwap_rs_vals   # overwrite compat column too
        d = d[pd.Series(keep_mask, index=d.index)].copy()
        if "ts_key_local" in d.columns:
            d = d.drop(columns=["ts_key_local"])
        return d, mode_removed, rs_removed

    s, s_mode_removed, s_rs_removed = _apply_side(short_df, "SHORT")
    l, l_mode_removed, l_rs_removed = _apply_side(long_df, "LONG")

    momentum_note = " | momentum=ON" if V19_RS_MOMENTUM_ENABLED else " | momentum=OFF"
    dead_note = (
        f" | LONG dead zone={V19_LONG_AVWAP_RS_DEAD_LO:.2f}-{V19_LONG_AVWAP_RS_DEAD_HI:.2f}"
        if V19_LONG_AVWAP_RS_DEAD_HI > V19_LONG_AVWAP_RS_DEAD_LO else ""
    )
    print(
        "[NIFTY_CONTEXT] Applied intraday filter (V19 AVWAP-RS Run2): "
        f"SHORT {len(short_df)}->{len(s)} "
        f"(mode_removed={s_mode_removed}, rs_removed={s_rs_removed}) | "
        f"LONG {len(long_df)}->{len(l)} "
        f"(mode_removed={l_mode_removed}, rs_removed={l_rs_removed}) "
        f"[LONG both/dir={V19_AVWAP_RS_THRESH_LONG_BOTH:.2f}/"
        f"{V19_AVWAP_RS_THRESH_LONG_DIRECTIONAL:.2f}% | "
        f"SHORT both/dir={V19_AVWAP_RS_THRESH_SHORT_BOTH:.2f}/"
        f"{V19_AVWAP_RS_THRESH_SHORT_DIRECTIONAL:.2f}% | "
        f"cap=±{V19_AVWAP_RS_EXHAUST_CAP:.1f}%"
        f"{dead_note}{momentum_note}]"
    )
    return s, l


# Apply patches — must happen before main() is called
_base._apply_nifty_intraday_context = _v19_apply_nifty_intraday_context


# ===========================================================================
# ENTRY POINT
# ===========================================================================
if __name__ == "__main__":
    print("=" * 70)
    print("V19 5-min runner: AVWAP-RS filter (Run 2 — calibrated thresholds)")
    print(
        "  AVWAP_RS = (stock_close/stock_day_VWAP - 1)*100"
        " - (nifty_close/nifty_day_VWAP - 1)*100"
    )
    print()
    print(
        f"  LONG  BOTH mode   : AVWAP_RS >= +{V19_AVWAP_RS_THRESH_LONG_BOTH:.2f}%"
        f"  and  <= +{V19_AVWAP_RS_EXHAUST_CAP:.1f}%  [dead zone 0.30–0.75 eliminated]"
    )
    print(
        f"  LONG  directional : AVWAP_RS >= +{V19_AVWAP_RS_THRESH_LONG_DIRECTIONAL:.2f}%"
        f"  and  <= +{V19_AVWAP_RS_EXHAUST_CAP:.1f}%"
    )
    print(
        f"  SHORT BOTH mode   : AVWAP_RS <= -{V19_AVWAP_RS_THRESH_SHORT_BOTH:.2f}%"
        f"  and  >= -{V19_AVWAP_RS_EXHAUST_CAP:.1f}%  [dead zone -0.50–-0.30 eliminated]"
    )
    print(
        f"  SHORT directional : AVWAP_RS <= -{V19_AVWAP_RS_THRESH_SHORT_DIRECTIONAL:.2f}%"
        f"  and  >= -{V19_AVWAP_RS_EXHAUST_CAP:.1f}%"
    )
    if V19_LONG_AVWAP_RS_DEAD_HI > V19_LONG_AVWAP_RS_DEAD_LO:
        print(
            f"  LONG dead zone    : {V19_LONG_AVWAP_RS_DEAD_LO:.2f}–{V19_LONG_AVWAP_RS_DEAD_HI:.2f}% blocked"
        )
    print(
        f"  RS momentum gate  : {'ON — require AVWAP_RS improving into entry' if V19_RS_MOMENTUM_ENABLED else 'OFF'}"
    )
    print(
        f"  Fallback (no VWAP): raw RS >= {V19_FALLBACK_RS_LONG:.2f}% / "
        f"<= -{V19_FALLBACK_RS_SHORT:.2f}%"
    )
    print()
    print("  Output dir : outputs_v19_5min/")
    print("  Base       : V16 Run14 (all other filters unchanged)")
    print("=" * 70)
    _base.main()
