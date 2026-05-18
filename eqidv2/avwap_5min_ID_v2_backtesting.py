"""
AVWAP ID 5-min v2 backtester.

Built from the local AVWAP/backtest patterns plus the practical intraday
strategy framework:
  - long + short
  - momentum continuation and failed breakout/reclaim logic
  - market regime via NIFTY/NIFTYBEES context when available
  - relative strength vs market
  - liquidity, volume expansion, OR/previous-day levels, VWAP confirmation
  - candidate ranking, then only top 2-5 trades/day
  - next-candle entry realism
  - 1-minute intrabar exit resolution when available

Example:
    python avwap_5min_ID_v2_backtesting.py
    python avwap_5min_ID_v2_backtesting.py --limit 100 --workers 8
    python avwap_5min_ID_v2_backtesting.py --max_trades_per_day 24 --cost_bps 16
"""

from __future__ import annotations

import argparse
import math
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import time as dtime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

try:
    from filtered_stocks_MIS import selected_stocks
except Exception:
    selected_stocks = []


# -------------------- paths/config --------------------
DATA_ROOT_5M = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DATA_ROOT_1M = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
OUT_ROOT = Path("outputs_avwap_ID_5min_v2")
V17_CASCADE_OUT_ROOT = Path(r"C:\TradingData\eqidv2\outputs_ID_v2_5min")

MARKET_TICKERS = ("NIFTYBEES", "NIFTY", "NIFTY 50", "NIFTY50")

CAPITAL_PER_TRADE = 10_000.0
LEVERAGE = 5.0
EFFECTIVE_NOTIONAL = CAPITAL_PER_TRADE * LEVERAGE

SIGNAL_START = dtime(9, 30)
SIGNAL_END = dtime(14, 30)
EOD_EXIT = dtime(15, 15)

OR_MINUTES = 15
VWAP_LOOKBACK = 20
ATR_LOOKBACK = 20
RS_LOOKBACK_BARS = 6

MIN_PRICE = 80.0
MIN_5M_TRADED_VALUE_RS = 1_000_000.0
MIN_DAY_VALUE_BY_1000_RS = 20_000_000.0
MAX_CANDLE_RANGE_ATR = 3.5
MAX_VOL_RATIO = 8.0

VOL_RATIO_MIN = 1.5
STRONG_VOL_RATIO_MIN = 2.0
CLOSE_LOC_LONG_MIN = 0.60
CLOSE_LOC_SHORT_MAX = 0.40
BODY_PCT_MIN = 0.45

MOMENTUM_TARGET_PCT = 0.80
MOMENTUM_SL_PCT = 0.78
REVERSAL_TARGET_PCT = 0.80
REVERSAL_SL_PCT = 0.78
FADE_TARGET_PCT = 0.80
FADE_SL_PCT = 0.78

# v2 keeps the selected catalog setup logic inside the scanner. Research CSVs
# remain available as explicit presets only.
ENABLE_LONG_CONTINUATION = False
ENABLE_SHORT_CONTINUATION = False
ENABLE_OVEREXTENSION_FADE = False
ENABLE_LONG_FAILED_BREAKDOWN_REVERSAL = False
ENABLE_SHORT_FAILED_BREAKOUT_REVERSAL = False
FADE_MIN_VWAP_DIST_ATR = 0.50
FADE_MAX_VWAP_DIST_ATR = 96.0
FADE_MIN_RS_PCT = 0.50
FADE_MAX_RS_PCT = 999.0
FADE_MIN_ATR_PCT = 0.0026
FADE_MAX_QUALITY_SCORE = 703.37
FADE_MIN_MARKET_RET_PCT = -0.13
FADE_MIN_BODY_PCT = 0.6767
FADE_MIN_VOL_RATIO = 2.00

ENABLE_EXTRA_SLOTS = True
EXTRA_SLOT_MARKET_RET_MAX_PCT = 0.0667
EXTRA_SLOT_MIN_DAY_VALUE_RS = 314_000_000.0
ENABLE_NOISY_ADVANCED_SHORTS = False
ENABLE_NATIVE_V2_MINED_FILTER = True

DEFAULT_COST_BPS = 16.0
DEFAULT_WORKERS = 12
MAX_WORKERS = 16
DEFAULT_MAX_TRADES_PER_DAY = 24
DEFAULT_MAX_SAME_SIDE_PER_DAY = 16


def _init_worker(overrides: dict | None = None):
    if overrides:
        globals().update(overrides)


@dataclass
class Candidate:
    ticker: str
    date: str
    setup: str
    side: str
    signal_ts: pd.Timestamp
    signal_close: float
    entry_ts: pd.Timestamp
    entry_px: float
    target_px: float
    sl_px: float
    quality_score: float
    rs_pct: float
    market_ret_pct: float
    regime: str
    vol_ratio: float
    atr_pct: float
    close_loc: float
    body_pct: float
    vwap_dist_atr: float
    day_value_so_far_rs: float
    reason: str


@dataclass
class Trade:
    ticker: str
    date: str
    setup: str
    side: str
    signal_ts: pd.Timestamp
    entry_ts: pd.Timestamp
    exit_ts: pd.Timestamp
    exit_reason: str
    resolution: str
    entry_px: float
    target_px: float
    sl_px: float
    exit_px: float
    qty: int
    notional_rs: float
    quality_score: float
    gross_pnl_rs: float
    cost_rs: float
    net_pnl_rs: float
    pnl_pct: float
    pnl_pct_net: float
    ret_on_capital_pct: float
    bars_held: int
    rs_pct: float
    market_ret_pct: float
    regime: str
    vol_ratio: float
    atr_pct: float
    close_loc: float
    body_pct: float
    vwap_dist_atr: float
    day_value_so_far_rs: float
    reason: str


def _norm_ticker(ticker: str) -> str:
    return str(ticker).strip().upper().replace(".NS", "")


def _load_universe() -> list[str]:
    if selected_stocks:
        return sorted({_norm_ticker(x) for x in selected_stocks if str(x).strip()})

    cfg = Path("configs/universe.csv")
    if cfg.exists():
        df = pd.read_csv(cfg)
        col = "ticker" if "ticker" in df.columns else df.columns[0]
        return sorted({_norm_ticker(x) for x in df[col].dropna().astype(str)})

    return sorted({
        p.name.replace("_stocks_indicators_5min.parquet", "")
        for p in DATA_ROOT_5M.glob("*_stocks_indicators_5min.parquet")
    })


def _read_parquet(fp: Path, needed: Iterable[str] | None = None) -> pd.DataFrame:
    if needed is None:
        df = pd.read_parquet(fp)
    else:
        try:
            df = pd.read_parquet(fp, columns=list(needed))
        except Exception:
            df = pd.read_parquet(fp)

    if "date" not in df.columns:
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index().rename(columns={df.index.name or "index": "date"})
        else:
            raise ValueError(f"{fp} has no date column")

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    df["date_only"] = df["date"].dt.date
    return df


def _read_ohlcv(fp: Path) -> pd.DataFrame:
    cols = [
        "date", "open", "high", "low", "close", "volume",
        "VWAP", "AVWAP", "ATR", "EMA_20", "EMA_50", "EMA_200",
        "RSI", "ADX", "CCI", "MFI", "OBV", "MACD", "MACD_Signal", "MACD_Hist",
        "Upper_Band", "Lower_Band", "Stoch_%K", "Stoch_%D", "Prev_Day_Close",
        "Recent_High", "Recent_Low", "date_only",
    ]
    df = _read_parquet(fp, cols)
    for col in ("open", "high", "low", "close", "volume"):
        if col not in df.columns:
            raise ValueError(f"{fp} missing {col}")
    return df


def _calc_atr(df: pd.DataFrame, lookback: int = ATR_LOOKBACK) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(lookback, min_periods=max(5, lookback // 2)).mean()


def _calc_session_vwap(df: pd.DataFrame) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    pv = typical * df["volume"].clip(lower=0)
    vol_cum = df.groupby("date_only")["volume"].cumsum().replace(0, np.nan)
    pv_cum = pv.groupby(df["date_only"]).cumsum()
    return pv_cum / vol_cum


def _prepare_5m(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "ATR" not in out.columns or out["ATR"].isna().all():
        out["ATR"] = out.groupby("date_only", group_keys=False).apply(_calc_atr)
    if "VWAP" not in out.columns or out["VWAP"].isna().all():
        out["VWAP"] = _calc_session_vwap(out)
    out["Volume_SMA20"] = out.groupby("date_only")["volume"].transform(
        lambda s: s.shift(1).rolling(VWAP_LOOKBACK, min_periods=8).mean()
    )
    out["traded_value_rs"] = out["close"] * out["volume"]
    out["day_value_so_far_rs"] = out.groupby("date_only")["traded_value_rs"].cumsum()
    out["range"] = out["high"] - out["low"]
    out["body_pct"] = (out["close"] - out["open"]).abs() / out["range"].replace(0, np.nan)
    out["close_loc"] = (out["close"] - out["low"]) / out["range"].replace(0, np.nan)
    out["vol_ratio"] = out["volume"] / out["Volume_SMA20"].replace(0, np.nan)
    out["atr_pct"] = out["ATR"] / out["close"].replace(0, np.nan)
    out["vwap_dist_atr"] = (out["close"] - out["VWAP"]) / out["ATR"].replace(0, np.nan)
    out["ema20_dist_atr"] = (out["close"] - out.get("EMA_20", out["close"])) / out["ATR"].replace(0, np.nan)
    out["upper_wick_pct"] = (out["high"] - out[["open", "close"]].max(axis=1)) / out["range"].replace(0, np.nan)
    out["lower_wick_pct"] = (out[["open", "close"]].min(axis=1) - out["low"]) / out["range"].replace(0, np.nan)
    vol_mean = out["volume"].shift(1).rolling(20, min_periods=8).mean()
    vol_std = out["volume"].shift(1).rolling(20, min_periods=8).std(ddof=0)
    out["volume_z_20"] = (out["volume"] - vol_mean) / vol_std.replace(0, np.nan)
    out["buy_pressure_5"] = np.where(out["close"] > out["open"], out["volume"], 0.0)
    out["sell_pressure_5"] = np.where(out["close"] < out["open"], out["volume"], 0.0)
    out["buy_pressure_5"] = pd.Series(out["buy_pressure_5"], index=out.index).rolling(5, min_periods=2).sum()
    out["sell_pressure_5"] = pd.Series(out["sell_pressure_5"], index=out.index).rolling(5, min_periods=2).sum()
    out["pressure_ratio_5"] = out["buy_pressure_5"] / out["sell_pressure_5"].replace(0, np.nan)
    day_levels = (
        out.groupby("date_only", sort=True)
        .agg(day_high=("high", "max"), day_low=("low", "min"), day_close=("close", "last"))
        .reset_index()
    )
    day_levels["prev_day_high"] = day_levels["day_high"].shift(1)
    day_levels["prev_day_low"] = day_levels["day_low"].shift(1)
    day_levels["prev_day_close_calc"] = day_levels["day_close"].shift(1)
    out = out.merge(
        day_levels[["date_only", "prev_day_high", "prev_day_low", "prev_day_close_calc"]],
        on="date_only",
        how="left",
    )
    if "Prev_Day_Close" not in out.columns:
        out["Prev_Day_Close"] = out["prev_day_close_calc"]
    else:
        out["Prev_Day_Close"] = out["Prev_Day_Close"].fillna(out["prev_day_close_calc"])
    if "Upper_Band" in out.columns and "Lower_Band" in out.columns:
        out["bb_width_pct"] = (out["Upper_Band"] - out["Lower_Band"]) / out["close"].replace(0, np.nan)
        out["bb_width_mean100"] = out.groupby("date_only")["bb_width_pct"].transform(
            lambda s: s.shift(1).rolling(100, min_periods=20).mean()
        )
    else:
        out["bb_width_pct"] = np.nan
        out["bb_width_mean100"] = np.nan
    return out


def _opening_range(day: pd.DataFrame) -> tuple[float, float]:
    start = day["date"].iloc[0]
    cutoff = start + pd.Timedelta(minutes=OR_MINUTES)
    w = day[day["date"] < cutoff]
    if w.empty:
        return float("nan"), float("nan")
    return float(w["high"].max()), float(w["low"].min())


def _market_context_from_df(df: pd.DataFrame) -> dict[str, dict]:
    df = _prepare_5m(df)
    ctx: dict[str, dict] = {}
    for day, g in df.groupby("date_only", sort=True):
        g = g.reset_index(drop=True)
        if g.empty:
            continue
        day_open = float(g["open"].iloc[0])
        by_ts = {}
        for i, row in g.iterrows():
            if day_open <= 0:
                mret = 0.0
            else:
                mret = (float(row["close"]) / day_open - 1.0) * 100.0
            close = float(row["close"])
            vwap = float(row.get("VWAP", np.nan))
            if mret >= 0.15 and np.isfinite(vwap) and close >= vwap:
                regime = "BULL"
            elif mret <= -0.15 and np.isfinite(vwap) and close <= vwap:
                regime = "BEAR"
            elif abs(mret) >= 0.70:
                regime = "TREND"
            else:
                regime = "NEUTRAL"
            by_ts[pd.Timestamp(row["date"])] = {
                "market_ret_pct": float(mret),
                "regime": regime,
                "market_close": close,
            }
        ctx[str(day)] = by_ts
    return ctx


def _load_market_context() -> dict[str, dict]:
    for ticker in MARKET_TICKERS:
        fp = DATA_ROOT_5M / f"{ticker}_stocks_indicators_5min.parquet"
        if fp.exists():
            try:
                return _market_context_from_df(_read_ohlcv(fp))
            except Exception:
                pass
    return {}


def _bar_context(market_ctx: dict[str, dict], day: str, ts: pd.Timestamp) -> tuple[float, str]:
    by_ts = market_ctx.get(day, {})
    if not by_ts:
        return 0.0, "UNKNOWN"
    if ts in by_ts:
        d = by_ts[ts]
        return float(d["market_ret_pct"]), str(d["regime"])
    keys = [k for k in by_ts.keys() if k <= ts]
    if not keys:
        return 0.0, "UNKNOWN"
    d = by_ts[max(keys)]
    return float(d["market_ret_pct"]), str(d["regime"])


def _score(
    side: str,
    setup: str,
    rs_pct: float,
    vol_ratio: float,
    close_loc: float,
    vwap_dist_atr: float,
    atr_pct: float,
    regime: str,
) -> float:
    score = 0.0
    if "FADE" in setup:
        score += 20.0 * max(abs(rs_pct), 0.0)
        score += 14.0 * min(max(vol_ratio - 1.0, 0.0), 4.0)
        score += 12.0 * max(abs(vwap_dist_atr), 0.0)
        score += 10.0 * max(close_loc if side == "SHORT" else 1.0 - close_loc, 0.0)
        score += 8.0 if regime in {"BULL", "TREND", "NEUTRAL", "UNKNOWN"} else -8.0
    elif side == "LONG":
        score += 25.0 * max(rs_pct, 0.0)
        score += 12.0 * min(max(vol_ratio - 1.0, 0.0), 4.0)
        score += 18.0 * max(close_loc, 0.0)
        score += 8.0 if vwap_dist_atr > 0 else -10.0
        score += 10.0 if regime in {"BULL", "TREND", "UNKNOWN"} else -18.0
    else:
        score += 25.0 * max(-rs_pct, 0.0)
        score += 12.0 * min(max(vol_ratio - 1.0, 0.0), 4.0)
        score += 18.0 * max(1.0 - close_loc, 0.0)
        score += 8.0 if vwap_dist_atr < 0 else -10.0
        score += 10.0 if regime in {"BEAR", "TREND", "UNKNOWN"} else -18.0

    if setup.endswith("REVERSAL"):
        score += 8.0
    if "FADE" not in setup and np.isfinite(atr_pct) and atr_pct > 0.018:
        score -= 20.0
    if "FADE" not in setup and abs(vwap_dist_atr) > 3.0:
        score -= 12.0
    return float(score)


def _passes_common(row: pd.Series) -> bool:
    if float(row["close"]) < MIN_PRICE:
        return False
    if float(row.get("traded_value_rs", 0.0)) < MIN_5M_TRADED_VALUE_RS:
        return False
    if row["date"].time() >= dtime(10, 0) and float(row.get("day_value_so_far_rs", 0.0)) < MIN_DAY_VALUE_BY_1000_RS:
        return False
    atr = float(row.get("ATR", np.nan))
    rng = float(row.get("range", np.nan))
    if np.isfinite(atr) and atr > 0 and rng > MAX_CANDLE_RANGE_ATR * atr:
        return False
    vol_ratio = float(row.get("vol_ratio", np.nan))
    if not np.isfinite(vol_ratio) or vol_ratio < VOL_RATIO_MIN or vol_ratio > MAX_VOL_RATIO:
        return False
    if float(row.get("body_pct", 0.0)) < BODY_PCT_MIN:
        return False
    return True


def _make_candidate(
    ticker: str,
    day: str,
    setup: str,
    side: str,
    row: pd.Series,
    next_row: pd.Series,
    rs_pct: float,
    market_ret_pct: float,
    regime: str,
    reason: str,
) -> Candidate:
    entry_px = float(next_row["open"])
    if side == "LONG":
        if "FADE" in setup:
            sl_pct, tgt_pct = FADE_SL_PCT, FADE_TARGET_PCT
        elif setup.endswith("REVERSAL"):
            sl_pct, tgt_pct = REVERSAL_SL_PCT, REVERSAL_TARGET_PCT
        else:
            sl_pct, tgt_pct = MOMENTUM_SL_PCT, MOMENTUM_TARGET_PCT
        target_px = entry_px * (1.0 + tgt_pct / 100.0)
        sl_px = entry_px * (1.0 - sl_pct / 100.0)
    else:
        if "FADE" in setup:
            sl_pct, tgt_pct = FADE_SL_PCT, FADE_TARGET_PCT
        elif setup.endswith("REVERSAL"):
            sl_pct, tgt_pct = REVERSAL_SL_PCT, REVERSAL_TARGET_PCT
        else:
            sl_pct, tgt_pct = MOMENTUM_SL_PCT, MOMENTUM_TARGET_PCT
        target_px = entry_px * (1.0 - tgt_pct / 100.0)
        sl_px = entry_px * (1.0 + sl_pct / 100.0)

    score = _score(
        side=side,
        setup=setup,
        rs_pct=rs_pct,
        vol_ratio=float(row.get("vol_ratio", np.nan)),
        close_loc=float(row.get("close_loc", np.nan)),
        vwap_dist_atr=float(row.get("vwap_dist_atr", np.nan)),
        atr_pct=float(row.get("atr_pct", np.nan)),
        regime=regime,
    )
    return Candidate(
        ticker=ticker,
        date=day,
        setup=setup,
        side=side,
        signal_ts=pd.Timestamp(row["date"]),
        signal_close=float(row["close"]),
        entry_ts=pd.Timestamp(next_row["date"]),
        entry_px=float(entry_px),
        target_px=float(target_px),
        sl_px=float(sl_px),
        quality_score=score,
        rs_pct=float(rs_pct),
        market_ret_pct=float(market_ret_pct),
        regime=regime,
        vol_ratio=float(row.get("vol_ratio", np.nan)),
        atr_pct=float(row.get("atr_pct", np.nan)),
        close_loc=float(row.get("close_loc", np.nan)),
        body_pct=float(row.get("body_pct", np.nan)),
        vwap_dist_atr=float(row.get("vwap_dist_atr", np.nan)),
        day_value_so_far_rs=float(row.get("day_value_so_far_rs", 0.0)),
        reason=reason,
    )


def _scan_day(day_df: pd.DataFrame, ticker: str, market_ctx: dict[str, dict]) -> list[Candidate]:
    df = day_df.reset_index(drop=True)
    if len(df) < max(VWAP_LOOKBACK, RS_LOOKBACK_BARS) + 3:
        return []

    or_high, or_low = _opening_range(df)
    day_open = float(df["open"].iloc[0])
    prev_high = df["high"].shift(1).rolling(20, min_periods=8).max()
    prev_low = df["low"].shift(1).rolling(20, min_periods=8).min()
    out: list[Candidate] = []
    day = str(df["date_only"].iloc[0])

    for i in range(max(VWAP_LOOKBACK, RS_LOOKBACK_BARS), len(df) - 1):
        row = df.iloc[i]
        next_row = df.iloc[i + 1]
        ts = pd.Timestamp(row["date"])
        t = ts.time()
        signal_hour = ts.hour + ts.minute / 60.0
        if not (SIGNAL_START <= t <= SIGNAL_END):
            continue
        if not _passes_common(row):
            continue

        close = float(row["close"])
        open_ = float(row["open"])
        vwap = float(row.get("VWAP", np.nan))
        close_loc = float(row.get("close_loc", np.nan))
        vol_ratio = float(row.get("vol_ratio", np.nan))

        stock_ret = (close / day_open - 1.0) * 100.0 if day_open > 0 else 0.0
        market_ret, regime = _bar_context(market_ctx, day, ts)
        rs_pct = stock_ret - market_ret

        rh = float(prev_high.iloc[i]) if pd.notna(prev_high.iloc[i]) else float("nan")
        rl = float(prev_low.iloc[i]) if pd.notna(prev_low.iloc[i]) else float("nan")

        long_struct = close > open_ and close_loc >= CLOSE_LOC_LONG_MIN
        short_struct = close < open_ and close_loc <= CLOSE_LOC_SHORT_MAX
        above_vwap = np.isfinite(vwap) and close > vwap
        below_vwap = np.isfinite(vwap) and close < vwap

        long_momentum = (
            long_struct and above_vwap
            and rs_pct > 0.10
            and vol_ratio >= STRONG_VOL_RATIO_MIN
            and ((np.isfinite(or_high) and close > or_high) or (np.isfinite(rh) and close > rh))
            and regime not in {"BEAR"}
        )
        short_momentum = (
            short_struct and below_vwap
            and rs_pct < -0.10
            and vol_ratio >= STRONG_VOL_RATIO_MIN
            and ((np.isfinite(or_low) and close < or_low) or (np.isfinite(rl) and close < rl))
            and regime not in {"BULL"}
        )

        overextended_long_breakout = (
            long_momentum
            and ENABLE_OVEREXTENSION_FADE
            and float(row.get("vwap_dist_atr", 0.0)) >= FADE_MIN_VWAP_DIST_ATR
            and float(row.get("vwap_dist_atr", 0.0)) <= FADE_MAX_VWAP_DIST_ATR
            and rs_pct >= FADE_MIN_RS_PCT
            and rs_pct <= FADE_MAX_RS_PCT
            and float(row.get("atr_pct", 0.0)) >= FADE_MIN_ATR_PCT
            and float(row.get("body_pct", 0.0)) >= FADE_MIN_BODY_PCT
            and vol_ratio >= FADE_MIN_VOL_RATIO
            and market_ret >= FADE_MIN_MARKET_RET_PCT
        )

        if ENABLE_LONG_CONTINUATION and long_momentum:
            out.append(_make_candidate(
                ticker, day, "A_MOMENTUM_BREAKOUT", "LONG",
                row, next_row, rs_pct, market_ret, regime, "breakout_above_or_or_20bar_high",
            ))
        if overextended_long_breakout:
            fade = _make_candidate(
                ticker, day, "C_OVEREXTENDED_BREAKOUT_FADE", "SHORT",
                row, next_row, rs_pct, market_ret, regime, "fade_overextended_breakout",
            )
            if fade.quality_score <= FADE_MAX_QUALITY_SCORE:
                out.append(fade)
        if ENABLE_SHORT_CONTINUATION and short_momentum:
            out.append(_make_candidate(
                ticker, day, "A_MOMENTUM_BREAKDOWN", "SHORT",
                row, next_row, rs_pct, market_ret, regime, "breakdown_below_or_or_20bar_low",
            ))

        # Failed breakout/reversal: previous bar pierced level, current bar reclaims/loses VWAP/level.
        prev = df.iloc[i - 1]
        prev_low_break = np.isfinite(or_low) and float(prev["low"]) < or_low
        prev_high_break = np.isfinite(or_high) and float(prev["high"]) > or_high
        long_reversal = (
            long_struct and above_vwap and prev_low_break
            and close > or_low and rs_pct > -0.20 and regime not in {"BEAR"}
        )
        short_reversal = (
            short_struct and below_vwap and prev_high_break
            and close < or_high and rs_pct < 0.20 and regime not in {"BULL"}
        )
        if ENABLE_LONG_FAILED_BREAKDOWN_REVERSAL and long_reversal:
            out.append(_make_candidate(
                ticker, day, "B_FAILED_BREAKDOWN_REVERSAL", "LONG",
                row, next_row, rs_pct, market_ret, regime, "failed_or_low_break_reclaim",
            ))
        if ENABLE_SHORT_FAILED_BREAKOUT_REVERSAL and short_reversal:
            out.append(_make_candidate(
                ticker, day, "B_FAILED_BREAKOUT_REVERSAL", "SHORT",
                row, next_row, rs_pct, market_ret, regime, "failed_or_high_break_reject",
            ))

        atr = float(row.get("ATR", np.nan))
        ema20 = float(row.get("EMA_20", np.nan))
        ema50 = float(row.get("EMA_50", np.nan))
        ema200 = float(row.get("EMA_200", np.nan))
        upper_band = float(row.get("Upper_Band", np.nan))
        lower_band = float(row.get("Lower_Band", np.nan))
        rsi = float(row.get("RSI", np.nan))
        adx = float(row.get("ADX", np.nan))
        atr_pct = float(row.get("atr_pct", np.nan))
        rng = float(row.get("range", np.nan))
        bb_width = float(row.get("bb_width_pct", np.nan))
        bb_width_mean = float(row.get("bb_width_mean100", np.nan))
        prev_close = float(prev["close"])
        prev_open = float(prev["open"])
        prev_bar_high = float(prev["high"])
        prev_bar_low = float(prev["low"])
        prev_range = float(prev.get("range", np.nan))
        prev_atr = float(prev.get("ATR", np.nan))
        prev_vwap = float(prev.get("VWAP", np.nan))
        prev2 = df.iloc[i - 2] if i >= 2 else prev
        moderate_impulse = np.isfinite(atr) and atr > 0 and 0.60 * atr <= rng <= 2.20 * atr
        huge_prev = np.isfinite(prev_atr) and prev_atr > 0 and prev_range >= 1.80 * prev_atr
        near_ema20 = np.isfinite(ema20) and np.isfinite(atr) and atr > 0 and abs(close - ema20) <= 0.35 * atr
        squeeze = np.isfinite(bb_width) and np.isfinite(bb_width_mean) and bb_width_mean > 0 and bb_width <= 0.55 * bb_width_mean
        uptrend_stack = np.isfinite(ema20) and np.isfinite(ema50) and close > ema20 and ema20 >= ema50
        downtrend_stack = np.isfinite(ema20) and np.isfinite(ema50) and close < ema20 and ema20 <= ema50

        def add_catalog(setup: str, side: str, condition: bool, reason: str, min_qs: float | None = None) -> None:
            if not condition:
                return
            c = _make_candidate(ticker, day, setup, side, row, next_row, rs_pct, market_ret, regime, reason)
            if min_qs is not None and c.quality_score < min_qs:
                return
            out.append(c)

        add_catalog(
            "A_MOD_BREAK_C1_HIGH",
            "LONG",
            moderate_impulse and long_struct and above_vwap and close > prev_bar_high
            and rs_pct > 0.05 and vol_ratio >= 1.5 and regime != "BEAR",
            "moderate_impulse_break_prior_high",
            min_qs=7.0,
        )
        add_catalog(
            "A_MOD_BREAK_C1_LOW",
            "SHORT",
            moderate_impulse and short_struct and below_vwap and close < prev_bar_low
            and adx >= 19.12 and rsi >= 23.22 and atr_pct <= 0.0063
            and vol_ratio >= 1.5 and regime != "BULL",
            "moderate_impulse_break_prior_low",
        )
        add_catalog(
            "A_MOD_CLOSE_CONTINUATION_BREAK",
            "LONG",
            moderate_impulse and long_struct and above_vwap and close_loc >= 0.75
            and close > prev_bar_high and rs_pct > 0.00 and vol_ratio >= 1.4,
            "moderate_close_near_high_continuation",
            min_qs=6.8,
        )
        add_catalog(
            "A_PULLBACK_C2_THEN_BREAK_C2_LOW",
            "SHORT",
            short_struct and below_vwap and close < prev_bar_low and float(prev["close"]) > float(prev2["close"])
            and vol_ratio >= 1.4 and regime != "BULL",
            "bear_pullback_c2_break_low",
        )
        add_catalog(
            "B_AVWAP_RECLAIM_REVERSAL",
            "LONG",
            long_struct and np.isfinite(prev_vwap) and prev_close < prev_vwap and above_vwap
            and rs_pct > -0.10 and vol_ratio >= 1.4 and regime != "BEAR",
            "reclaim_session_vwap_from_below",
            min_qs=6.0,
        )
        add_catalog(
            "D_AVWAP_LOSE_REVERSAL",
            "SHORT",
            short_struct and np.isfinite(prev_vwap) and prev_close > prev_vwap and below_vwap
            and rs_pct < 0.15 and vol_ratio >= 1.4 and regime != "BULL",
            "lose_session_vwap_from_above",
        )
        add_catalog(
            "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
            "LONG",
            huge_prev and prev_close > prev_open and long_struct and close > prev_bar_high
            and above_vwap and vol_ratio >= 1.3 and regime != "BEAR",
            "huge_green_reclaim_then_break",
            min_qs=7.0,
        )
        add_catalog(
            "B_HUGE_RED_FAILED_BOUNCE",
            "SHORT",
            huge_prev and prev_close < prev_open and short_struct and close < prev_bar_low
            and below_vwap and vol_ratio >= 1.3 and regime != "BULL",
            "huge_red_failed_bounce_breakdown",
        )
        add_catalog(
            "C_OR_BREAKOUT",
            "LONG",
            np.isfinite(or_high) and long_struct and above_vwap and close > or_high
            and rs_pct > 0.00 and vol_ratio >= 1.5 and regime != "BEAR",
            "opening_range_breakout",
        )
        add_catalog(
            "C_OR_BREAKDOWN",
            "SHORT",
            np.isfinite(or_low) and short_struct and below_vwap and close < or_low
            and rs_pct < 0.10 and vol_ratio >= 1.5 and regime != "BULL",
            "opening_range_breakdown",
        )
        add_catalog(
            "D_EMA20_BOUNCE",
            "LONG",
            near_ema20 and long_struct and uptrend_stack and close > ema20
            and rs_pct > -0.05 and vol_ratio >= 1.3 and regime != "BEAR",
            "ema20_trend_bounce",
        )
        add_catalog(
            "D_EMA20_REJECTION",
            "SHORT",
            near_ema20 and short_struct and downtrend_stack and close < ema20
            and rs_pct < 0.10 and vol_ratio >= 1.3 and regime != "BULL",
            "ema20_trend_rejection",
        )
        add_catalog(
            "E_VWAP_BAND_FADE",
            "SHORT",
            short_struct and below_vwap and np.isfinite(upper_band) and float(row["high"]) >= upper_band
            and float(row.get("upper_wick_pct", 0.0)) >= 0.20 and vol_ratio >= 1.5,
            "upper_band_vwap_fade",
        )
        add_catalog(
            "G_HIGHER_HIGH_BREAK",
            "LONG",
            np.isfinite(rh) and long_struct and above_vwap and close > rh
            and rs_pct > 0.00 and vol_ratio >= 1.4 and regime != "BEAR",
            "twenty_bar_higher_high_break",
        )
        add_catalog(
            "G_LOWER_LOW_BREAK",
            "SHORT",
            np.isfinite(rl) and short_struct and below_vwap and close < rl
            and rs_pct < 0.10 and vol_ratio >= 1.4 and regime != "BULL",
            "twenty_bar_lower_low_break",
        )
        add_catalog(
            "L_BB_SQUEEZE_LONG",
            "LONG",
            squeeze and long_struct and np.isfinite(upper_band) and close > upper_band * 1.003
            and vol_ratio >= 2.0 and float(row.get("body_pct", 0.0)) >= 0.65,
            "bb_squeeze_upside_expansion",
        )
        add_catalog(
            "S_BB_SQUEEZE_SHORT",
            "SHORT",
            squeeze and short_struct and np.isfinite(lower_band) and close < lower_band * 0.997
            and vol_ratio >= 2.0 and float(row.get("body_pct", 0.0)) >= 0.65,
            "bb_squeeze_downside_expansion",
        )

        macd = float(row.get("MACD", np.nan))
        macd_sig = float(row.get("MACD_Signal", np.nan))
        macd_hist = float(row.get("MACD_Hist", np.nan))
        prev_macd = float(prev.get("MACD", np.nan))
        prev_macd_sig = float(prev.get("MACD_Signal", np.nan))
        cci = float(row.get("CCI", np.nan))
        prev_cci = float(prev.get("CCI", np.nan))
        mfi = float(row.get("MFI", np.nan))
        prev_mfi = float(prev.get("MFI", np.nan))
        obv = float(row.get("OBV", np.nan))
        obv_lag = float(df.iloc[max(i - 10, 0)].get("OBV", np.nan))
        prev_day_close = float(row.get("Prev_Day_Close", np.nan))
        prev_day_high = float(row.get("prev_day_high", np.nan))
        prev_day_low = float(row.get("prev_day_low", np.nan))
        intraday_low_8 = float(df["low"].iloc[max(0, i - 8):i].min())
        intraday_high_8 = float(df["high"].iloc[max(0, i - 8):i].max())
        mfi_3 = float(df.iloc[i - 3].get("MFI", np.nan)) if i >= 3 else float("nan")
        cci_max_3 = float(pd.to_numeric(df["CCI"].iloc[max(0, i - 3):i + 1], errors="coerce").max()) if "CCI" in df.columns else float("nan")
        cci_min_3 = float(pd.to_numeric(df["CCI"].iloc[max(0, i - 3):i + 1], errors="coerce").min()) if "CCI" in df.columns else float("nan")
        pressure_ratio = float(row.get("pressure_ratio_5", np.nan))
        volume_z = float(row.get("volume_z_20", np.nan))
        stock_ret_30m = (close / float(df.iloc[i - RS_LOOKBACK_BARS]["close"]) - 1.0) * 100.0 if i >= RS_LOOKBACK_BARS and float(df.iloc[i - RS_LOOKBACK_BARS]["close"]) > 0 else 0.0
        advanced_rs_30m = stock_ret_30m - market_ret

        add_catalog(
            "A_PULLBACK_C2_THEN_BREAK_C2_HIGH",
            "LONG",
            long_struct and above_vwap and close > prev_bar_high and float(prev["close"]) < float(prev2["close"])
            and vol_ratio >= 1.4 and regime != "BEAR",
            "bull_pullback_c2_break_high",
        )
        add_catalog(
            "B_HUGE_PULLBACK_HOLD_BREAK",
            "LONG",
            huge_prev and prev_close > prev_open and long_struct and close > prev_bar_high
            and float(row["low"]) >= min(prev_open, prev_close) and above_vwap and vol_ratio >= 1.2,
            "huge_impulse_pullback_hold_break",
        )
        add_catalog(
            "B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK",
            "LONG",
            huge_prev and prev_close > prev_open and long_struct and close > prev_bar_high
            and float(row["low"]) >= min(prev_open, prev_close) and above_vwap and close_loc >= 0.65,
            "huge_green_pullback_hold_then_break",
        )
        add_catalog(
            "C_SHORT_CONTINUATION_BREAK",
            "SHORT",
            short_struct and below_vwap and np.isfinite(rl) and close < rl
            and float(prev["close"]) < float(prev2["close"]) and vol_ratio >= 1.4,
            "short_continuation_pivot_low_break",
        )
        add_catalog(
            "B_HUGE_FAILED_BOUNCE",
            "SHORT",
            huge_prev and short_struct and close < prev_bar_low and below_vwap
            and float(prev["close"]) < float(prev["open"]) and vol_ratio >= 1.2,
            "huge_candle_failed_bounce",
        )
        add_catalog(
            "H_FAILED_BREAKOUT_TRAP",
            "SHORT",
            np.isfinite(or_high) and float(prev["high"]) > or_high and short_struct
            and close < or_high and below_vwap and vol_ratio >= 1.2,
            "failed_breakout_trap",
        )
        add_catalog(
            "L_MACD_BULL_VWAP",
            "LONG",
            np.isfinite(macd) and np.isfinite(macd_sig) and np.isfinite(prev_macd) and np.isfinite(prev_macd_sig)
            and prev_macd <= prev_macd_sig and macd > macd_sig and macd > 0
            and above_vwap and uptrend_stack and adx >= 25 and 55 <= rsi <= 68 and long_struct,
            "macd_bull_cross_vwap_trend",
        )
        add_catalog(
            "S_MACD_BEAR_VWAP",
            "SHORT",
            np.isfinite(macd) and np.isfinite(macd_sig) and np.isfinite(prev_macd) and np.isfinite(prev_macd_sig)
            and prev_macd >= prev_macd_sig and macd < macd_sig and macd < 0
            and below_vwap and downtrend_stack and adx >= 25 and 32 <= rsi <= 45 and short_struct,
            "macd_bear_cross_vwap_trend",
        )
        add_catalog(
            "L_TREND_PULLBACK",
            "LONG",
            np.isfinite(ema20) and np.isfinite(ema50) and np.isfinite(ema200)
            and ema20 > ema50 > ema200 and near_ema20 and long_struct and close > ema20,
            "stacked_uptrend_ema20_pullback",
        )
        add_catalog(
            "S_TREND_REJECT",
            "SHORT",
            np.isfinite(ema20) and np.isfinite(ema50) and np.isfinite(ema200)
            and ema20 < ema50 < ema200 and near_ema20 and short_struct and close < ema20,
            "stacked_downtrend_ema20_reject",
        )
        add_catalog(
            "L_MFI_OVERSOLD_RECLAIM",
            "LONG",
            np.isfinite(mfi) and np.isfinite(prev_mfi) and prev_mfi < 25 and mfi > 35
            and above_vwap and float(row.get("lower_wick_pct", 0.0)) >= 0.20 and adx >= 18 and vol_ratio >= 1.3,
            "mfi_oversold_reclaim",
        )
        add_catalog(
            "S_MFI_OVERBOUGHT_FAIL",
            "SHORT",
            np.isfinite(mfi) and np.isfinite(mfi_3) and mfi_3 > 75 and mfi < 65
            and below_vwap and short_struct and float(row.get("upper_wick_pct", 0.0)) >= 0.20
            and adx >= 18 and vol_ratio >= 1.3 and ENABLE_NOISY_ADVANCED_SHORTS,
            "mfi_overbought_fail",
        )
        add_catalog(
            "L_CCI_EXTREME_FLIP",
            "LONG",
            np.isfinite(cci) and np.isfinite(prev_cci) and prev_cci < -150 and cci > -80
            and close > ema20 and float(row.get("body_pct", 0.0)) >= 0.55 and adx >= 18 and rsi >= 40,
            "cci_negative_extreme_flip",
        )
        add_catalog(
            "S_CCI_EXTREME_FLIP",
            "SHORT",
            np.isfinite(cci) and np.isfinite(cci_max_3) and cci_max_3 > 150 and cci < 80
            and close < ema20 and short_struct and float(row.get("body_pct", 0.0)) >= 0.55 and rsi <= 60
            and np.isfinite(macd_hist) and macd_hist >= 2.24625,
            "cci_positive_extreme_flip",
        )
        add_catalog(
            "L_DOUBLE_BOTTOM_VWAP",
            "LONG",
            np.isfinite(atr) and atr > 0 and abs(float(row["low"]) - intraday_low_8) <= 0.4 * atr
            and above_vwap and vol_ratio >= 1.5 and long_struct,
            "double_bottom_vwap_reclaim",
        )
        add_catalog(
            "S_DOUBLE_TOP_VWAP",
            "SHORT",
            np.isfinite(atr) and atr > 0 and abs(float(row["high"]) - intraday_high_8) <= 0.4 * atr
            and below_vwap and vol_ratio >= 1.5 and short_struct and np.isfinite(cci) and cci <= -8.71136,
            "double_top_vwap_fail",
        )
        add_catalog(
            "L_PRESSURE_BURST_VWAP",
            "LONG",
            pressure_ratio >= 3.0 and above_vwap and close > ema20 and vol_ratio >= 1.5 and 50 <= rsi <= 75,
            "buy_pressure_burst_vwap",
        )
        add_catalog(
            "S_PRESSURE_DUMP_VWAP",
            "SHORT",
            np.isfinite(pressure_ratio) and pressure_ratio <= 0.33
            and below_vwap and close < ema20 and short_struct and volume_z >= 1.5 and 25 <= rsi <= 50 and adx >= 20
            and ENABLE_NOISY_ADVANCED_SHORTS,
            "sell_pressure_dump_vwap",
        )
        add_catalog(
            "L_PREV_DAY_LOW_SWEEP",
            "LONG",
            np.isfinite(prev_day_close) and float(row["low"]) < prev_day_close and close > prev_day_close
            and float(row.get("lower_wick_pct", 0.0)) >= 0.25 and vol_ratio >= 1.5,
            "previous_day_level_sweep_reclaim",
        )
        add_catalog(
            "S_PREV_DAY_HIGH_FAIL",
            "SHORT",
            np.isfinite(prev_day_high) and float(row["high"]) > prev_day_high and close < prev_day_high
            and short_struct and float(row.get("upper_wick_pct", 0.0)) >= 0.25 and vol_ratio >= 1.5
            and signal_hour <= 11.41667,
            "previous_day_high_sweep_fail",
        )
        add_catalog(
            "L_GAP_DOWN_REVERSAL",
            "LONG",
            np.isfinite(prev_day_close) and day_open < prev_day_close and close > prev_day_close and long_struct,
            "gap_down_reversal_above_prev_close",
        )
        add_catalog(
            "S_GAP_UP_REJECTION",
            "SHORT",
            np.isfinite(prev_day_close) and day_open > prev_day_close * 1.002
            and intraday_high_8 > day_open and close < prev_day_close and short_struct and rsi <= 55
            and ENABLE_NOISY_ADVANCED_SHORTS,
            "gap_up_rejection_below_prev_close",
        )
        add_catalog(
            "S_MOMENTUM_BREAKDOWN_OR",
            "SHORT",
            np.isfinite(or_low) and float(prev["close"]) >= or_low and close < or_low
            and advanced_rs_30m < 0 and volume_z >= 2.0 and short_struct and below_vwap
            and np.isfinite(atr) and atr > 0 and abs(close - vwap) / atr < 1.5
            and (close_loc <= 0.0 or rs_pct <= -2.39415),
            "advanced_or_breakdown_rs_volume_avwap",
        )
        add_catalog(
            "S_LIQUIDITY_SWEEP_REVERSAL",
            "SHORT",
            np.isfinite(prev_day_high) and float(prev["high"]) > prev_day_high
            and close < prev_day_high and short_struct and volume_z >= 1.5 and ENABLE_NOISY_ADVANCED_SHORTS,
            "advanced_prev_day_high_liquidity_sweep",
        )
        add_catalog(
            "S_MACD_HIST_FLIP",
            "SHORT",
            np.isfinite(macd_hist) and float(prev.get("MACD_Hist", np.nan)) >= 0 and macd_hist < 0
            and below_vwap and rsi <= 55 and ENABLE_NOISY_ADVANCED_SHORTS,
            "macd_hist_negative_flip",
        )
        add_catalog(
            "L_OBV_DIVERGENCE_VWAP",
            "LONG",
            np.isfinite(obv) and np.isfinite(obv_lag) and float(row["low"]) < intraday_low_8
            and obv > obv_lag and above_vwap and long_struct,
            "bullish_obv_divergence_vwap",
        )
        add_catalog(
            "S_OBV_DIVERGENCE_VWAP",
            "SHORT",
            np.isfinite(obv) and np.isfinite(obv_lag) and float(row["high"]) > intraday_high_8
            and obv < obv_lag and below_vwap and short_struct,
            "bearish_obv_divergence_vwap",
        )
    return out


def _resolve_exit_1m(c: Candidate, day1: pd.DataFrame) -> tuple[pd.Timestamp, float, str, int, str]:
    if day1.empty:
        return _resolve_exit_5m(c, pd.DataFrame())

    eod = c.entry_ts.replace(hour=EOD_EXIT.hour, minute=EOD_EXIT.minute, second=0, microsecond=0)
    walk = day1[day1["date"] >= c.entry_ts].reset_index(drop=True)
    if walk.empty:
        return c.entry_ts, c.entry_px, "NO_DATA", 0, "1min"

    for j, row in walk.iterrows():
        ts = pd.Timestamp(row["date"])
        if ts >= eod:
            return ts, float(row["close"]), "EOD", int(j), "1min"
        if c.side == "LONG":
            hit_sl = float(row["low"]) <= c.sl_px
            hit_tg = float(row["high"]) >= c.target_px
        else:
            hit_sl = float(row["high"]) >= c.sl_px
            hit_tg = float(row["low"]) <= c.target_px
        if hit_sl:
            return ts, c.sl_px, "SL", int(j), "1min"
        if hit_tg:
            return ts, c.target_px, "TARGET", int(j), "1min"

    last = walk.iloc[-1]
    return pd.Timestamp(last["date"]), float(last["close"]), "EOD", int(len(walk) - 1), "1min"


def _resolve_exit_5m(c: Candidate, day5_after: pd.DataFrame) -> tuple[pd.Timestamp, float, str, int, str]:
    if day5_after.empty:
        return c.entry_ts, c.entry_px, "NO_DATA", 0, "5min"
    eod = c.entry_ts.replace(hour=EOD_EXIT.hour, minute=EOD_EXIT.minute, second=0, microsecond=0)
    walk = day5_after[day5_after["date"] >= c.entry_ts].reset_index(drop=True)
    for j, row in walk.iterrows():
        ts = pd.Timestamp(row["date"])
        if ts >= eod:
            return ts, float(row["close"]), "EOD", int(j), "5min"
        if c.side == "LONG":
            hit_sl = float(row["low"]) <= c.sl_px
            hit_tg = float(row["high"]) >= c.target_px
        else:
            hit_sl = float(row["high"]) >= c.sl_px
            hit_tg = float(row["low"]) <= c.target_px
        if hit_sl:
            return ts, c.sl_px, "SL", int(j), "5min"
        if hit_tg:
            return ts, c.target_px, "TARGET", int(j), "5min"
    last = walk.iloc[-1]
    return pd.Timestamp(last["date"]), float(last["close"]), "EOD", int(len(walk) - 1), "5min"


def _candidate_to_trade(c: Candidate, day5: pd.DataFrame, day1: pd.DataFrame, cost_bps: float) -> Trade | None:
    if c.entry_px <= 0:
        return None
    qty = int(EFFECTIVE_NOTIONAL // c.entry_px)
    if qty <= 0:
        return None

    if not day1.empty:
        exit_ts, exit_px, exit_reason, bars_held, resolution = _resolve_exit_1m(c, day1)
        if exit_reason == "NO_DATA":
            exit_ts, exit_px, exit_reason, bars_held, resolution = _resolve_exit_5m(c, day5)
    else:
        exit_ts, exit_px, exit_reason, bars_held, resolution = _resolve_exit_5m(c, day5)

    if c.side == "LONG":
        gross_pnl_rs = qty * (exit_px - c.entry_px)
        pnl_pct = (exit_px / c.entry_px - 1.0) * 100.0
    else:
        gross_pnl_rs = qty * (c.entry_px - exit_px)
        pnl_pct = (c.entry_px / exit_px - 1.0) * 100.0 if exit_px > 0 else 0.0

    notional_rs = qty * c.entry_px
    extra_stop_bps = 3.0 if exit_reason == "SL" else 0.0
    cost_rs = notional_rs * ((cost_bps + extra_stop_bps) / 10_000.0)
    net_pnl_rs = gross_pnl_rs - cost_rs
    pnl_pct_net = pnl_pct - ((cost_bps + extra_stop_bps) / 100.0)

    return Trade(
        ticker=c.ticker,
        date=c.date,
        setup=c.setup,
        side=c.side,
        signal_ts=c.signal_ts,
        entry_ts=c.entry_ts,
        exit_ts=exit_ts,
        exit_reason=exit_reason,
        resolution=resolution,
        entry_px=float(c.entry_px),
        target_px=float(c.target_px),
        sl_px=float(c.sl_px),
        exit_px=float(exit_px),
        qty=int(qty),
        notional_rs=float(notional_rs),
        quality_score=float(c.quality_score),
        gross_pnl_rs=float(gross_pnl_rs),
        cost_rs=float(cost_rs),
        net_pnl_rs=float(net_pnl_rs),
        pnl_pct=float(pnl_pct),
        pnl_pct_net=float(pnl_pct_net),
        ret_on_capital_pct=float(net_pnl_rs / CAPITAL_PER_TRADE * 100.0),
        bars_held=int(bars_held),
        rs_pct=float(c.rs_pct),
        market_ret_pct=float(c.market_ret_pct),
        regime=c.regime,
        vol_ratio=float(c.vol_ratio),
        atr_pct=float(c.atr_pct),
        close_loc=float(c.close_loc),
        body_pct=float(c.body_pct),
        vwap_dist_atr=float(c.vwap_dist_atr),
        day_value_so_far_rs=float(c.day_value_so_far_rs),
        reason=c.reason,
    )


def run_ticker(ticker: str, market_ctx: dict[str, dict]) -> list[Candidate]:
    fp5 = DATA_ROOT_5M / f"{ticker}_stocks_indicators_5min.parquet"
    if not fp5.exists():
        return []

    df5 = _prepare_5m(_read_ohlcv(fp5))
    candidates: list[Candidate] = []
    for day, g in df5.groupby("date_only", sort=True):
        candidates.extend(_scan_day(g, ticker, market_ctx))
    return candidates


def _worker(payload: tuple[str, dict]) -> tuple[str, list[Candidate], str | None]:
    ticker, market_ctx = payload
    try:
        candidates = run_ticker(ticker, market_ctx)
        return ticker, candidates, None
    except Exception as exc:
        return ticker, [], f"{type(exc).__name__}: {exc}"


def _load_selected_day_maps(ticker: str, days: set[str]) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    day5_map: dict[str, pd.DataFrame] = {}
    day1_map: dict[str, pd.DataFrame] = {}

    fp5 = DATA_ROOT_5M / f"{ticker}_stocks_indicators_5min.parquet"
    if fp5.exists():
        df5 = _prepare_5m(_read_ohlcv(fp5))
        for day, g in df5.groupby("date_only", sort=True):
            day_s = str(day)
            if day_s in days:
                day5_map[day_s] = g.reset_index(drop=True)

    fp1 = DATA_ROOT_1M / f"{ticker}_stocks_indicators_1min.parquet"
    if fp1.exists():
        try:
            df1 = _read_ohlcv(fp1)
            for day, g in df1.groupby("date_only", sort=True):
                day_s = str(day)
                if day_s in days:
                    day1_map[day_s] = g.reset_index(drop=True)
        except Exception:
            day1_map = {}

    return day5_map, day1_map


def _extra_slot_allowed(row: dict) -> bool:
    return (
        ENABLE_EXTRA_SLOTS
        and float(row.get("market_ret_pct", 999.0)) <= EXTRA_SLOT_MARKET_RET_MAX_PCT
        and float(row.get("day_value_so_far_rs", 0.0)) >= EXTRA_SLOT_MIN_DAY_VALUE_RS
    )


def _apply_daily_selection(candidates: list[Candidate], max_trades: int, max_open: int) -> list[Candidate]:
    if not candidates:
        return []
    out: list[Candidate] = []
    cdf = pd.DataFrame([asdict(c) for c in candidates])
    for day, g in cdf.groupby("date", sort=True):
        g = g.sort_values(["quality_score", "signal_ts"], ascending=[False, True])
        selected: list[dict] = []
        used_tickers: set[str] = set()
        used_sides: dict[str, int] = {"LONG": 0, "SHORT": 0}
        for row in g.to_dict("records"):
            if len(selected) >= max_trades:
                break
            if row["ticker"] in used_tickers:
                continue
            if used_sides[row["side"]] >= max_open:
                continue
            if selected and not _extra_slot_allowed(row):
                continue
            selected.append(row)
            used_tickers.add(row["ticker"])
            used_sides[row["side"]] += 1
        for row in selected:
            out.append(Candidate(**row))
    return out


def _metrics(trades: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    if trades.empty:
        return {"total_trades": 0}, pd.DataFrame(), pd.DataFrame()

    wins = trades[trades["net_pnl_rs"] > 0]
    losses = trades[trades["net_pnl_rs"] <= 0]
    gross_win = float(wins["net_pnl_rs"].sum())
    gross_loss = float(-losses["net_pnl_rs"].sum())
    pf = gross_win / gross_loss if gross_loss > 0 else math.inf

    daily = trades.groupby("date", sort=True)["net_pnl_rs"].sum().reset_index()
    daily["cum_net_pnl_rs"] = daily["net_pnl_rs"].cumsum()
    daily["drawdown_rs"] = daily["cum_net_pnl_rs"] - daily["cum_net_pnl_rs"].cummax()

    by_setup = (
        trades.groupby(["side", "setup"], sort=True)
        .agg(
            trades=("ticker", "count"),
            win_rate_pct=("net_pnl_rs", lambda s: float((s > 0).mean() * 100.0)),
            pnl_rs=("net_pnl_rs", "sum"),
            avg_qs=("quality_score", "mean"),
        )
        .reset_index()
    )

    return {
        "total_trades": int(len(trades)),
        "trading_days": int(daily["date"].nunique()),
        "avg_trades_per_day": float(len(trades) / max(daily["date"].nunique(), 1)),
        "win_rate_pct": float((trades["net_pnl_rs"] > 0).mean() * 100.0),
        "target_rate_pct": float((trades["exit_reason"] == "TARGET").mean() * 100.0),
        "sl_rate_pct": float((trades["exit_reason"] == "SL").mean() * 100.0),
        "eod_rate_pct": float((trades["exit_reason"] == "EOD").mean() * 100.0),
        "profit_factor": float(pf),
        "gross_pnl_rs": float(trades["gross_pnl_rs"].sum()),
        "cost_rs": float(trades["cost_rs"].sum()),
        "net_pnl_rs": float(trades["net_pnl_rs"].sum()),
        "avg_net_pnl_rs": float(trades["net_pnl_rs"].mean()),
        "avg_pnl_pct_net": float(trades["pnl_pct_net"].mean()),
        "ret_on_capital_pct_sum": float(trades["net_pnl_rs"].sum() / CAPITAL_PER_TRADE * 100.0),
        "day_win_rate_pct": float((daily["net_pnl_rs"] > 0).mean() * 100.0),
        "max_drawdown_rs": float(daily["drawdown_rs"].min()),
        "pct_1min_resolved": float((trades["resolution"] == "1min").mean() * 100.0),
        "long_trades": int((trades["side"] == "LONG").sum()),
        "short_trades": int((trades["side"] == "SHORT").sum()),
        "long_pnl_rs": float(trades.loc[trades["side"] == "LONG", "net_pnl_rs"].sum()),
        "short_pnl_rs": float(trades.loc[trades["side"] == "SHORT", "net_pnl_rs"].sum()),
    }, daily, by_setup


def _summary_text(summary: dict, by_setup: pd.DataFrame, args) -> str:
    if summary.get("total_trades", 0) == 0:
        return "No trades."
    lines = [
        "=" * 78,
        "AVWAP ID 5-min v2 backtest",
        f"Top trades/day={args.max_trades_per_day}  max same-side/day={args.max_same_side_per_day}  cost={args.cost_bps:.1f} bps",
        f"Capital/trade=Rs {CAPITAL_PER_TRADE:,.0f}  leverage={LEVERAGE:.1f}x  notional=Rs {EFFECTIVE_NOTIONAL:,.0f}",
        "=" * 78,
        f"Trades              : {summary['total_trades']:,}",
        f"Trading days        : {summary['trading_days']:,}",
        f"Avg trades/day      : {summary['avg_trades_per_day']:.2f}",
        f"Win rate            : {summary['win_rate_pct']:.2f}%",
        f"Target / SL / EOD   : {summary['target_rate_pct']:.2f}% / {summary['sl_rate_pct']:.2f}% / {summary['eod_rate_pct']:.2f}%",
        f"Profit factor       : {summary['profit_factor']:.3f}",
        f"Net PnL             : Rs {summary['net_pnl_rs']:,.2f}",
        f"Return on capital   : {summary['ret_on_capital_pct_sum']:.2f}%",
        f"Day win rate        : {summary['day_win_rate_pct']:.2f}%",
        f"Max drawdown        : Rs {summary['max_drawdown_rs']:,.2f}",
        f"1-min resolved      : {summary['pct_1min_resolved']:.2f}%",
        f"LONG trades/PnL     : {summary['long_trades']:,} / Rs {summary['long_pnl_rs']:,.2f}",
        f"SHORT trades/PnL    : {summary['short_trades']:,} / Rs {summary['short_pnl_rs']:,.2f}",
        "",
        "By setup:",
    ]
    if by_setup.empty:
        lines.append("  none")
    else:
        view = by_setup.sort_values("pnl_rs", ascending=False)
        for _, r in view.iterrows():
            lines.append(
                f"  {r['side']:<5s} {r['setup']:<32s} "
                f"n={int(r['trades']):>5d} win={float(r['win_rate_pct']):>6.2f}% "
                f"pnl=Rs {float(r['pnl_rs']):>11,.2f} avg_qs={float(r['avg_qs']):>7.2f}"
            )
    lines.append("=" * 78)
    return "\n".join(lines)


def _apply_v2_mined_trade_filter(trades: pd.DataFrame) -> pd.DataFrame:
    """Keep only v2 internal rule pockets that survived the first full scan."""
    if trades.empty:
        return trades
    if not ENABLE_NATIVE_V2_MINED_FILTER:
        return trades.reset_index(drop=True)

    df = trades.copy()
    signal_ts = pd.to_datetime(df["signal_ts"], errors="coerce")
    df["_signal_hour"] = signal_ts.dt.hour + signal_ts.dt.minute / 60.0

    def n(col: str) -> pd.Series:
        return pd.to_numeric(df[col], errors="coerce")

    rules = [
        (
            (df["side"] == "LONG") & (df["setup"] == "A_MOD_CLOSE_CONTINUATION_BREAK")
            & (n("rs_pct") <= 1.55620) & (n("day_value_so_far_rs") <= 1_390_100_043.80297)
        ),
        (
            (df["side"] == "LONG") & (df["setup"] == "B_AVWAP_RECLAIM_REVERSAL")
            & (n("vol_ratio") >= 4.72487) & (n("body_pct") <= 0.84558)
        ),
        (
            (df["side"] == "LONG") & (df["setup"] == "B_HUGE_C1_CLOSE_RECLAIM_BREAK")
            & (df["_signal_hour"] >= 14.25) & (n("body_pct") >= 0.53061)
        ),
        (
            (df["side"] == "LONG") & (df["setup"] == "D_EMA20_BOUNCE")
            & (n("body_pct") >= 0.55554) & (df["_signal_hour"] <= 14.33333)
        ),
        (
            (df["side"] == "LONG") & (df["setup"] == "L_BB_SQUEEZE_LONG")
            & (n("vwap_dist_atr") <= -27.15195) & (n("quality_score") <= 169.38052)
        ),
        (
            (df["side"] == "SHORT") & (df["setup"] == "A_MOD_BREAK_C1_LOW")
            & (n("body_pct") >= 0.47619) & (n("body_pct") <= 0.50000)
        ),
        (
            (df["side"] == "SHORT") & (df["setup"] == "A_PULLBACK_C2_THEN_BREAK_C2_LOW")
            & (n("atr_pct") <= 0.00226) & (n("quality_score") >= 119.33943)
        ),
        (
            (df["side"] == "SHORT") & (df["setup"] == "B_HUGE_RED_FAILED_BOUNCE")
            & (df["_signal_hour"] >= 14.33333) & (n("day_value_so_far_rs") >= 651_584_174.20129)
        ),
        (
            (df["side"] == "SHORT") & (df["setup"] == "E_VWAP_BAND_FADE")
            & (n("vwap_dist_atr") >= -2.21203) & (n("rs_pct") >= -0.61414)
        ),
        (
            (df["side"] == "SHORT") & (df["setup"] == "S_BB_SQUEEZE_SHORT")
            & (n("body_pct") <= 0.78788) & (n("day_value_so_far_rs") >= 344_805_743.25125)
        ),
    ]
    tight_pf2_specs = [
        ("LONG", "L_BB_SQUEEZE_LONG", [("quality_score", "<=", 165.59560), ("body_pct", ">=", 0.85119)]),
        ("SHORT", "A_PULLBACK_C2_THEN_BREAK_C2_LOW", [("market_ret_pct", "<=", -0.99215), ("atr_pct", "<=", 0.00367)]),
        ("SHORT", "E_VWAP_BAND_FADE", [("body_pct", "<=", 0.45454), ("vol_ratio", "<=", 1.97277)]),
        ("SHORT", "E_VWAP_BAND_FADE", [("vwap_dist_atr", ">=", -3.25323), ("close_loc", ">=", 0.09677)]),
        ("LONG", "L_BB_SQUEEZE_LONG", [("quality_score", "<=", 104.00936), ("close_loc", ">=", 0.82682)]),
        ("SHORT", "A_PULLBACK_C2_THEN_BREAK_C2_LOW", [("atr_pct", "<=", 0.00181), ("body_pct", ">=", 0.73549)]),
        ("LONG", "D_EMA20_BOUNCE", [("body_pct", ">=", 0.51203), ("vol_ratio", ">=", 1.65942)]),
        ("SHORT", "S_BB_SQUEEZE_SHORT", [("rs_pct", ">=", -4.75087), ("day_value_so_far_rs", "<=", 2_774_799_985.72934)]),
        ("SHORT", "A_PULLBACK_C2_THEN_BREAK_C2_LOW", [("vwap_dist_atr", "<=", -95.59778), ("atr_pct", ">=", 0.00248)]),
        ("SHORT", "A_PULLBACK_C2_THEN_BREAK_C2_LOW", [("atr_pct", "<=", 0.00171), ("vol_ratio", "<=", 6.18796)]),
        ("LONG", "B_HUGE_C1_CLOSE_RECLAIM_BREAK", [("vwap_dist_atr", ">=", 42.06320), ("market_ret_pct", ">=", -0.33048)]),
        ("SHORT", "A_PULLBACK_C2_THEN_BREAK_C2_LOW", [("atr_pct", "<=", 0.00194), ("vwap_dist_atr", ">=", -47.79818)]),
        ("SHORT", "A_PULLBACK_C2_THEN_BREAK_C2_LOW", [("vol_ratio", ">=", 6.18796), ("_signal_hour", "<=", 14.33333)]),
        ("SHORT", "A_PULLBACK_C2_THEN_BREAK_C2_LOW", [("atr_pct", "<=", 0.00235), ("vwap_dist_atr", ">=", -25.22562)]),
        ("SHORT", "A_PULLBACK_C2_THEN_BREAK_C2_LOW", [("vol_ratio", ">=", 4.51085), ("_signal_hour", "<=", 13.33333)]),
        ("SHORT", "E_VWAP_BAND_FADE", [("body_pct", "<=", 0.45454), ("close_loc", ">=", 0.12820)]),
        ("SHORT", "A_PULLBACK_C2_THEN_BREAK_C2_LOW", [("atr_pct", "<=", 0.00235), ("body_pct", "<=", 0.66476)]),
        ("LONG", "L_BB_SQUEEZE_LONG", [("_signal_hour", "<=", 12.08333), ("vol_ratio", "<=", 3.89778)]),
        ("SHORT", "E_VWAP_BAND_FADE", [("body_pct", "<=", 0.45454), ("day_value_so_far_rs", "<=", 1_387_281_971.0)]),
        ("SHORT", "A_PULLBACK_C2_THEN_BREAK_C2_LOW", [("vol_ratio", ">=", 4.51085), ("vwap_dist_atr", ">=", -25.22562)]),
        ("LONG", "L_BB_SQUEEZE_LONG", [("body_pct", "<=", 0.69729), ("day_value_so_far_rs", ">=", 1_173_027_842.89355)]),
        ("LONG", "L_BB_SQUEEZE_LONG", [("market_ret_pct", ">=", -0.13851), ("quality_score", "<=", 344.17080)]),
        ("SHORT", "E_VWAP_BAND_FADE", [("vwap_dist_atr", ">=", -3.25323), ("atr_pct", ">=", 0.00277)]),
        ("LONG", "B_HUGE_C1_CLOSE_RECLAIM_BREAK", [("_signal_hour", ">=", 14.25), ("body_pct", ">=", 0.52632)]),
        ("SHORT", "S_BB_SQUEEZE_SHORT", [("body_pct", "<=", 0.77305), ("vol_ratio", "<=", 6.74825)]),
        ("SHORT", "A_MOD_BREAK_C1_LOW", [("body_pct", "<=", 0.51535), ("vwap_dist_atr", "<=", -23.92441)]),
        ("LONG", "C_OR_BREAKOUT", [("_signal_hour", ">=", 14.5), ("vol_ratio", ">=", 2.67516)]),
        ("LONG", "B_HUGE_C1_CLOSE_RECLAIM_BREAK", [("vwap_dist_atr", ">=", 39.04404), ("atr_pct", "<=", 0.00345)]),
        ("LONG", "L_BB_SQUEEZE_LONG", [("rs_pct", ">=", 10.77995), ("atr_pct", "<=", 0.00825)]),
        ("SHORT", "A_PULLBACK_C2_THEN_BREAK_C2_LOW", [("market_ret_pct", "<=", -0.97434), ("day_value_so_far_rs", "<=", 1_629_898_598.51339)]),
        ("SHORT", "A_PULLBACK_C2_THEN_BREAK_C2_LOW", [("close_loc", ">=", 0.34583), ("close_loc", "<=", 0.38455)]),
        ("SHORT", "A_MOD_BREAK_C1_LOW", [("body_pct", "<=", 0.51535), ("vwap_dist_atr", "<=", -7.53298)]),
        ("LONG", "B_HUGE_C1_CLOSE_RECLAIM_BREAK", [("vwap_dist_atr", ">=", 39.04404), ("vwap_dist_atr", "<=", 59.30909)]),
        ("LONG", "A_MOD_CLOSE_CONTINUATION_BREAK", [("_signal_hour", ">=", 14.5), ("quality_score", "<=", 184.49957)]),
        ("SHORT", "D_AVWAP_LOSE_REVERSAL", [("close_loc", "<=", 0.01538)]),
    ]

    rules = []
    for side, setup, conditions in tight_pf2_specs:
        rule = (df["side"] == side) & (df["setup"] == setup)
        for col, op, threshold in conditions:
            series = df["_signal_hour"] if col == "_signal_hour" else n(col)
            if op == ">=":
                rule &= series >= threshold
            elif op == "<=":
                rule &= series <= threshold
            else:
                raise ValueError(f"unsupported v2 mined op: {op}")
        rules.append(rule)

    keep = rules[0].copy()
    for rule in rules[1:]:
        keep |= rule
    return df.loc[keep].drop(columns=["_signal_hour"]).reset_index(drop=True)


def _latest_v17_trade_csv() -> Path | None:
    candidates: list[Path] = []
    runtime_out_dirs: list[Path] = []
    try:
        from eqidv2_runtime_paths import runtime_dir
        runtime_out_dirs.extend([
            runtime_dir("outputs_ID_v2_5min"),
            runtime_dir("outputs_v17r_5min"),
            runtime_dir("outputs_v17t_live_5min"),
            runtime_dir("outputs_v17p_5min"),
        ])
    except Exception:
        pass

    for out_dir in (
        Path("outputs_ID_v2_5min"),
        Path("outputs_v17r_5min"),
        Path("outputs_v17t_live_5min"),
        Path("outputs_v17p_5min"),
        *runtime_out_dirs,
    ):
        if out_dir.exists():
            candidates.extend(out_dir.glob("avwap_longshort_trades_v16_5min_ALL_DAYS_*.csv"))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _normalize_trade_report(df: pd.DataFrame, out_dir: Path, title: str) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    work = df.copy()

    if "trade_date" not in work.columns and "date" in work.columns:
        work["trade_date"] = work["date"]
    if "trade_date" not in work.columns:
        if "entry_time_ist" in work.columns:
            work["trade_date"] = pd.to_datetime(work["entry_time_ist"], errors="coerce").dt.date.astype(str)
        elif "entry_ts" in work.columns:
            work["trade_date"] = pd.to_datetime(work["entry_ts"], errors="coerce").dt.date.astype(str)
        else:
            raise SystemExit("v17 cascade output has no usable trade date column")

    work["capital_per_trade_rs"] = CAPITAL_PER_TRADE
    work["position_size_rs"] = CAPITAL_PER_TRADE
    work["leverage"] = LEVERAGE
    work["notional_exposure_rs"] = EFFECTIVE_NOTIONAL

    if "pnl_pct_price" in work.columns:
        pnl = pd.to_numeric(work["pnl_pct_price"], errors="coerce").fillna(0.0) / 100.0 * EFFECTIVE_NOTIONAL
    elif "pnl_pct" in work.columns:
        pnl = pd.to_numeric(work["pnl_pct"], errors="coerce").fillna(0.0) / 100.0 * CAPITAL_PER_TRADE
    elif "pnl_rs" in work.columns:
        pnl = pd.to_numeric(work["pnl_rs"], errors="coerce").fillna(0.0)
    else:
        raise SystemExit("v17 cascade output has no pnl_pct/pnl_rs column")

    work["pnl_rs"] = pnl
    work.to_csv(out_dir / "trades.csv", index=False)

    daily = work.groupby("trade_date", sort=True)["pnl_rs"].sum().reset_index()
    daily["cum_pnl_rs"] = daily["pnl_rs"].cumsum()
    daily["drawdown_rs"] = daily["cum_pnl_rs"] - daily["cum_pnl_rs"].cummax()
    daily.to_csv(out_dir / "daily.csv", index=False)

    by_setup = (
        work.groupby(["side", "setup"], sort=True)
        .agg(
            trades=("ticker", "count"),
            win_rate_pct=("pnl_rs", lambda s: float((pd.to_numeric(s, errors="coerce") > 0).mean() * 100.0)),
            pnl_rs=("pnl_rs", "sum"),
            avg_qs=("quality_score", "mean"),
        )
        .reset_index()
    )
    by_setup.to_csv(out_dir / "by_setup.csv", index=False)

    gains = pnl[pnl > 0].sum()
    losses = -pnl[pnl <= 0].sum()
    pf = float(gains / losses) if losses > 0 else math.inf
    side = work["side"].astype(str).str.upper()
    lines = [
        "=" * 78,
        title,
        "V17 cascade engine output normalized by v2: capital Rs 10,000, leverage 5x, notional Rs 50,000",
        "=" * 78,
        f"Trades              : {len(work):,}",
        f"Trading days        : {daily['trade_date'].nunique():,}",
        f"Avg trades/day      : {len(work) / max(daily['trade_date'].nunique(), 1):.2f}",
        f"Win rate            : {(pnl > 0).mean() * 100.0:.2f}%",
        f"Profit factor       : {pf:.3f}",
        f"Net PnL             : Rs {pnl.sum():,.2f}",
        f"Day win rate        : {(daily['pnl_rs'] > 0).mean() * 100.0:.2f}%",
        f"Max drawdown        : Rs {daily['drawdown_rs'].min():,.2f}",
        f"LONG trades/PnL     : {(side == 'LONG').sum():,} / Rs {work.loc[side == 'LONG', 'pnl_rs'].sum():,.2f}",
        f"SHORT trades/PnL    : {(side == 'SHORT').sum():,} / Rs {work.loc[side == 'SHORT', 'pnl_rs'].sum():,.2f}",
        "",
        "By setup:",
    ]
    for _, r in by_setup.sort_values("pnl_rs", ascending=False).iterrows():
        avg_qs = float(r["avg_qs"]) if pd.notna(r["avg_qs"]) else float("nan")
        lines.append(
            f"  {r['side']:<5s} {r['setup']:<32s} "
            f"n={int(r['trades']):>5d} win={float(r['win_rate_pct']):>6.2f}% "
            f"pnl=Rs {float(r['pnl_rs']):>11,.2f} avg_qs={avg_qs:>7.2f}"
        )
    lines.append("=" * 78)
    text = "\n".join(lines)
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")
    return 0


def _run_v17_cascade(out_dir: Path, timeout_sec: int = 0) -> int:
    before = _latest_v17_trade_csv()
    before_mtime = before.stat().st_mtime if before is not None else None
    env = os.environ.copy()
    env.setdefault("EQIDV16_5MIN_MAX_WORKERS", str(DEFAULT_WORKERS))
    env.setdefault("EQIDV16_5MIN_ENABLE_LEGACY_CHARTS", "0")
    env.setdefault("EQIDV16_5MIN_ENABLE_ENHANCED_CHARTS", "0")
    env.setdefault("EQIDV17R_PROFILE", "volume")
    cmd = [sys.executable, "-m", "ID_backtesting.v17r_5min"]
    print("[avwap_ID_5min_v2] launching ID_backtesting v17 cascade engine...")
    creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    try:
        proc = subprocess.Popen(cmd, cwd=Path.cwd(), env=env, creationflags=creationflags)
        wait_timeout = None if int(timeout_sec) <= 0 else int(timeout_sec)
        proc.wait(timeout=wait_timeout)
    except subprocess.TimeoutExpired as exc:
        try:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], check=False, capture_output=True)
        except Exception:
            pass
        raise SystemExit(f"v17 cascade timed out after {timeout_sec}s; no CSV replay was used") from exc
    if proc.returncode != 0:
        raise SystemExit(f"v17 cascade exited with code {proc.returncode}")

    after = _latest_v17_trade_csv()
    if after is None or (before_mtime is not None and after == before and after.stat().st_mtime <= before_mtime):
        raise SystemExit("v17 cascade finished but no new trade CSV was found")
    df = pd.read_csv(after, low_memory=False)
    return _normalize_trade_report(df, out_dir, f"AVWAP ID 5-min v2 backtest: v17 cascade ({after.name})")


def main():
    ap = argparse.ArgumentParser(description="AVWAP ID 5-min v2 backtester")
    ap.add_argument(
        "--preset",
        choices=["v17_cascade", "native"],
        default="v17_cascade",
        help="v17_cascade runs the old v17 cascade engine from parquet data; native runs the standalone v2 scanner.",
    )
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    ap.add_argument("--cost_bps", type=float, default=DEFAULT_COST_BPS)
    ap.add_argument("--max_trades_per_day", type=int, default=DEFAULT_MAX_TRADES_PER_DAY)
    ap.add_argument("--max_same_side_per_day", type=int, default=DEFAULT_MAX_SAME_SIDE_PER_DAY)
    ap.add_argument("--out", type=str, default=str(V17_CASCADE_OUT_ROOT))
    ap.add_argument("--v17_timeout_sec", type=int, default=0, help="0 means no timeout for the v17 cascade.")
    ap.add_argument("--min_price", type=float, default=None)
    ap.add_argument("--vol_ratio_min", type=float, default=None)
    ap.add_argument("--enable_long_continuation", action="store_true")
    ap.add_argument("--enable_short_continuation", action="store_true")
    ap.add_argument("--disable_fade", action="store_true")
    ap.add_argument("--fade_min_vwap_dist_atr", type=float, default=None)
    ap.add_argument("--fade_max_vwap_dist_atr", type=float, default=None)
    ap.add_argument("--fade_min_rs_pct", type=float, default=None)
    ap.add_argument("--fade_max_rs_pct", type=float, default=None)
    ap.add_argument("--fade_min_atr_pct", type=float, default=None)
    ap.add_argument("--fade_max_quality_score", type=float, default=None)
    ap.add_argument("--fade_min_market_ret_pct", type=float, default=None)
    ap.add_argument("--fade_min_body_pct", type=float, default=None)
    ap.add_argument("--fade_min_vol_ratio", type=float, default=None)
    ap.add_argument("--enable_long_reversal", action="store_true")
    ap.add_argument("--enable_short_reversal", action="store_true")
    ap.add_argument("--disable_short_reversal", action="store_true")
    ap.add_argument("--disable_extra_slots", action="store_true")
    ap.add_argument("--extra_slot_market_ret_max_pct", type=float, default=None)
    ap.add_argument("--extra_slot_min_day_value_rs", type=float, default=None)
    args = ap.parse_args()

    overrides = {}
    if args.min_price is not None:
        overrides["MIN_PRICE"] = float(args.min_price)
    if args.vol_ratio_min is not None:
        overrides["VOL_RATIO_MIN"] = float(args.vol_ratio_min)
    if args.enable_long_continuation:
        overrides["ENABLE_LONG_CONTINUATION"] = True
    if args.enable_short_continuation:
        overrides["ENABLE_SHORT_CONTINUATION"] = True
    if args.disable_fade:
        overrides["ENABLE_OVEREXTENSION_FADE"] = False
    if args.fade_min_vwap_dist_atr is not None:
        overrides["FADE_MIN_VWAP_DIST_ATR"] = float(args.fade_min_vwap_dist_atr)
    if args.fade_max_vwap_dist_atr is not None:
        overrides["FADE_MAX_VWAP_DIST_ATR"] = float(args.fade_max_vwap_dist_atr)
    if args.fade_min_rs_pct is not None:
        overrides["FADE_MIN_RS_PCT"] = float(args.fade_min_rs_pct)
    if args.fade_max_rs_pct is not None:
        overrides["FADE_MAX_RS_PCT"] = float(args.fade_max_rs_pct)
    if args.fade_min_atr_pct is not None:
        overrides["FADE_MIN_ATR_PCT"] = float(args.fade_min_atr_pct)
    if args.fade_max_quality_score is not None:
        overrides["FADE_MAX_QUALITY_SCORE"] = float(args.fade_max_quality_score)
    if args.fade_min_market_ret_pct is not None:
        overrides["FADE_MIN_MARKET_RET_PCT"] = float(args.fade_min_market_ret_pct)
    if args.fade_min_body_pct is not None:
        overrides["FADE_MIN_BODY_PCT"] = float(args.fade_min_body_pct)
    if args.fade_min_vol_ratio is not None:
        overrides["FADE_MIN_VOL_RATIO"] = float(args.fade_min_vol_ratio)
    if args.enable_long_reversal:
        overrides["ENABLE_LONG_FAILED_BREAKDOWN_REVERSAL"] = True
    if args.enable_short_reversal:
        overrides["ENABLE_SHORT_FAILED_BREAKOUT_REVERSAL"] = True
    if args.disable_short_reversal:
        overrides["ENABLE_SHORT_FAILED_BREAKOUT_REVERSAL"] = False
    if args.disable_extra_slots:
        overrides["ENABLE_EXTRA_SLOTS"] = False
    if args.extra_slot_market_ret_max_pct is not None:
        overrides["EXTRA_SLOT_MARKET_RET_MAX_PCT"] = float(args.extra_slot_market_ret_max_pct)
    if args.extra_slot_min_day_value_rs is not None:
        overrides["EXTRA_SLOT_MIN_DAY_VALUE_RS"] = float(args.extra_slot_min_day_value_rs)
    _init_worker(overrides)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.preset == "v17_cascade":
        return _run_v17_cascade(out_dir, timeout_sec=int(args.v17_timeout_sec))

    universe = _load_universe()
    if args.limit:
        universe = universe[: args.limit]

    print(f"[avwap_ID_5min_v2] loading market context...")
    market_ctx = _load_market_context()
    print(f"[avwap_ID_5min_v2] universe={len(universe)} workers={args.workers} cost_bps={args.cost_bps}")

    t0 = time.time()
    all_candidates: list[Candidate] = []
    errors: list[tuple[str, str]] = []

    workers = max(1, min(int(args.workers), MAX_WORKERS))
    payloads = [(tk, market_ctx) for tk in universe]
    if workers == 1:
        for k, payload in enumerate(payloads, 1):
            tk, candidates, err = _worker(payload)
            if err:
                errors.append((tk, err))
            else:
                all_candidates.extend(candidates)
            if k % 100 == 0 or k == len(payloads):
                print(f"  [{k:4d}/{len(payloads)}] candidates={len(all_candidates)} elapsed={time.time()-t0:.1f}s")
    else:
        with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker, initargs=(overrides,)) as ex:
            futures = {ex.submit(_worker, payload): payload[0] for payload in payloads}
            done = 0
            for fut in as_completed(futures):
                tk, candidates, err = fut.result()
                done += 1
                if err:
                    errors.append((tk, err))
                else:
                    all_candidates.extend(candidates)
                if done % 100 == 0 or done == len(payloads):
                    print(f"  [{done:4d}/{len(payloads)}] candidates={len(all_candidates)} elapsed={time.time()-t0:.1f}s")

    if errors:
        print(f"[WARN] errored tickers={len(errors)} first5={errors[:5]}")

    candidates_df = pd.DataFrame([asdict(c) for c in all_candidates])
    candidates_df.to_csv(out_dir / "candidates.csv", index=False)

    selected = _apply_daily_selection(
        all_candidates,
        max_trades=max(1, int(args.max_trades_per_day)),
        max_open=max(1, int(args.max_same_side_per_day)),
    )
    selected_df = pd.DataFrame([asdict(c) for c in selected])
    selected_df.to_csv(out_dir / "selected_candidates.csv", index=False)

    trades: list[Trade] = []
    selected_by_ticker: dict[str, list[Candidate]] = {}
    for c in selected:
        selected_by_ticker.setdefault(c.ticker, []).append(c)

    for ticker, ticker_candidates in selected_by_ticker.items():
        needed_days = {c.date for c in ticker_candidates}
        day5_map, day1_map = _load_selected_day_maps(ticker, needed_days)
        for c in ticker_candidates:
            day5 = day5_map.get(c.date, pd.DataFrame())
            day1 = day1_map.get(c.date, pd.DataFrame())
            tr = _candidate_to_trade(c, day5, day1, args.cost_bps)
            if tr is not None:
                trades.append(tr)

    trades_df = pd.DataFrame([asdict(t) for t in trades])
    raw_trades_df = trades_df.copy()
    trades_df = _apply_v2_mined_trade_filter(trades_df)
    raw_trades_df.to_csv(out_dir / "trades_raw_unfiltered.csv", index=False, float_format="%.6f")
    trades_df.to_csv(out_dir / "trades.csv", index=False, float_format="%.6f")

    summary, daily, by_setup = _metrics(trades_df)
    daily.to_csv(out_dir / "daily.csv", index=False, float_format="%.2f")
    by_setup.to_csv(out_dir / "by_setup.csv", index=False, float_format="%.4f")

    text = _summary_text(summary, by_setup, args)
    print()
    print(text)
    with open(out_dir / "summary.txt", "w", encoding="utf-8") as f:
        f.write(text + "\n")
        f.write("\nRaw metrics:\n")
        for k, v in summary.items():
            f.write(f"  {k}: {v}\n")

    print(f"\nwrote {out_dir / 'candidates.csv'}")
    print(f"wrote {out_dir / 'selected_candidates.csv'}")
    print(f"wrote {out_dir / 'trades.csv'}")
    print(f"wrote {out_dir / 'daily.csv'}")
    print(f"wrote {out_dir / 'by_setup.csv'}")
    print(f"wrote {out_dir / 'summary.txt'}")


if __name__ == "__main__":
    main()
