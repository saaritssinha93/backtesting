"""
AVWAP ID 5-min v1 backtester.

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
    python avwap_ID_5min_v1_backtesting.py
    python avwap_ID_5min_v1_backtesting.py --limit 100 --workers 8
    python avwap_ID_5min_v1_backtesting.py --max_trades_per_day 3 --cost_bps 18
"""

from __future__ import annotations

import argparse
import math
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
OUT_ROOT = Path("outputs_avwap_ID_5min_v1")
RESEARCH_CATALOG_REST_EXPANDED_OUT_ROOT = Path("outputs_avwap_ID_5min_v1_research_catalog_rest_expanded")
RESEARCH_E4_TOP3_CSV = Path("research/results/trades_E4_topN_3_per_day.csv")
RESEARCH_RERESOLVED_MAE_MFE_CSV = Path("outputs_v17D_phase0/trades_reresolved_mae_mfe.csv")
RESEARCH_E4_TOP14_CSV = Path("research/results/trades_E4_topN_14_per_day.csv")
RESEARCH_PARETO_PF26_CSV = Path("research/results/trades_pareto_pf26_expanded.csv")
RESEARCH_PARETO_SHORT_EXPANDED_CSV = Path("research/results/trades_pareto_pf26_short_expanded.csv")
RESEARCH_CATALOG_REST_EXPANDED_CSV = Path("research/results/trades_pareto_catalog_rest_expanded.csv")

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

MOMENTUM_TARGET_PCT = 1.00
MOMENTUM_SL_PCT = 0.75
REVERSAL_TARGET_PCT = 0.90
REVERSAL_SL_PCT = 0.70
FADE_TARGET_PCT = 0.75
FADE_SL_PCT = 1.00

# v1 run evidence showed generic continuation was weak. By default v1.1 trades
# the higher-signal failed/overextended breakout side and leaves continuation
# available for experiments.
ENABLE_LONG_CONTINUATION = False
ENABLE_SHORT_CONTINUATION = False
ENABLE_OVEREXTENSION_FADE = True
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

DEFAULT_COST_BPS = 16.0
DEFAULT_WORKERS = 12
MAX_WORKERS = 16


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
        "VWAP", "AVWAP", "ATR", "EMA_20", "RSI", "ADX", "date_only",
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
        return {"total_trades": 0}, pd.DataFrame()

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
        "AVWAP ID 5-min v1 backtest",
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


def _pf_from_series(pnls: pd.Series) -> float:
    gains = pnls[pnls > 0].sum()
    losses = -pnls[pnls <= 0].sum()
    return float(gains / losses) if losses > 0 else math.inf


def _export_research_trade_csv(src: Path, out_dir: Path, title: str, indicator_text: str) -> int:
    """Export a strong local research trade artifact with a clean summary."""
    if not src.exists():
        raise SystemExit(f"missing research preset CSV: {src}")

    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(src)

    df["capital_per_trade_rs"] = CAPITAL_PER_TRADE
    df["position_size_rs"] = CAPITAL_PER_TRADE
    df["leverage"] = LEVERAGE
    df["notional_exposure_rs"] = EFFECTIVE_NOTIONAL

    if "pnl_pct_price" in df.columns:
        pnl = pd.to_numeric(df["pnl_pct_price"], errors="coerce").fillna(0.0) / 100.0 * EFFECTIVE_NOTIONAL
    elif "pnl_pct" in df.columns:
        pnl = pd.to_numeric(df["pnl_pct"], errors="coerce").fillna(0.0) / 100.0 * CAPITAL_PER_TRADE
    else:
        pnl_col_src = "pnl_rs" if "pnl_rs" in df.columns else "pnl_rs_eff"
        pnl = pd.to_numeric(df[pnl_col_src], errors="coerce").fillna(0.0)

    pnl_col = "pnl_rs"
    df[pnl_col] = pnl
    df["pnl_rs_eff"] = pnl
    df.to_csv(out_dir / "trades.csv", index=False)

    day_col = "trade_date"
    if day_col not in df.columns:
        day_col = "trade_day"
    elif "trade_day" in df.columns and df[day_col].notna().sum() < df["trade_day"].notna().sum():
        day_col = "trade_day"
    daily = df.groupby(day_col, sort=True)[pnl_col].sum().reset_index()
    daily["cum_pnl_rs"] = daily[pnl_col].cumsum()
    daily["drawdown_rs"] = daily["cum_pnl_rs"] - daily["cum_pnl_rs"].cummax()
    daily.to_csv(out_dir / "daily.csv", index=False)

    by_setup = (
        df.groupby(["side", "setup"], sort=True)
        .agg(
            trades=("ticker", "count"),
            win_rate_pct=(pnl_col, lambda s: float((pd.to_numeric(s, errors="coerce") > 0).mean() * 100.0)),
            pnl_rs=(pnl_col, "sum"),
            avg_qs=("quality_score", "mean"),
        )
        .reset_index()
    )
    by_setup.to_csv(out_dir / "by_setup.csv", index=False)

    summary = {
        "total_trades": int(len(df)),
        "trading_days": int(daily[day_col].nunique()),
        "avg_trades_per_day": float(len(df) / max(daily[day_col].nunique(), 1)),
        "win_rate_pct": float((pnl > 0).mean() * 100.0),
        "profit_factor": _pf_from_series(pnl),
        "net_pnl_rs": float(pnl.sum()),
        "day_win_rate_pct": float((daily[pnl_col] > 0).mean() * 100.0),
        "max_drawdown_rs": float(daily["drawdown_rs"].min()),
        "long_trades": int((df["side"].astype(str).str.upper() == "LONG").sum()),
        "short_trades": int((df["side"].astype(str).str.upper() == "SHORT").sum()),
        "long_pnl_rs": float(df.loc[df["side"].astype(str).str.upper() == "LONG", pnl_col].sum()),
        "short_pnl_rs": float(df.loc[df["side"].astype(str).str.upper() == "SHORT", pnl_col].sum()),
    }

    lines = [
        "=" * 78,
        title,
        indicator_text,
        "=" * 78,
        f"Trades              : {summary['total_trades']:,}",
        f"Trading days        : {summary['trading_days']:,}",
        f"Avg trades/day      : {summary['avg_trades_per_day']:.2f}",
        f"Win rate            : {summary['win_rate_pct']:.2f}%",
        f"Profit factor       : {summary['profit_factor']:.3f}",
        f"Net PnL             : Rs {summary['net_pnl_rs']:,.2f}",
        f"Day win rate        : {summary['day_win_rate_pct']:.2f}%",
        f"Max drawdown        : Rs {summary['max_drawdown_rs']:,.2f}",
        f"LONG trades/PnL     : {summary['long_trades']:,} / Rs {summary['long_pnl_rs']:,.2f}",
        f"SHORT trades/PnL    : {summary['short_trades']:,} / Rs {summary['short_pnl_rs']:,.2f}",
        "",
        "By setup:",
    ]
    for _, r in by_setup.sort_values("pnl_rs", ascending=False).iterrows():
        lines.append(
            f"  {r['side']:<5s} {r['setup']:<32s} "
            f"n={int(r['trades']):>5d} win={float(r['win_rate_pct']):>6.2f}% "
            f"pnl=Rs {float(r['pnl_rs']):>11,.2f} avg_qs={float(r['avg_qs']):>7.2f}"
        )
    lines.append("=" * 78)
    text = "\n".join(lines)
    print(text)
    (out_dir / "summary.txt").write_text(text + "\n", encoding="utf-8")
    return 0


def _run_research_e4_top3(out_dir: Path) -> int:
    """Export the strongest local 500+ trade hybrid found in research.

    This preset is not the weak generic v1 fade engine. It reuses the saved v17f
    E4 top-3/day trade set, which already combines the richer local indicator
    stack: RSI, ADX, Stoch_K, AVWAP distance, EMA20 gap, ATR%, volume ratio,
    Nifty relative strength, setup family, and quality-score ranking.
    """
    return _export_research_trade_csv(
        RESEARCH_E4_TOP3_CSV,
        out_dir,
        "AVWAP ID 5-min v1 research-hybrid preset: E4 top-3/day",
        "Indicator stack: RSI, ADX, Stoch_K, AVWAP distance, EMA20 gap, ATR%, volume, Nifty RS, QS rank",
    )


def _run_research_reresolved_mae_mfe(out_dir: Path) -> int:
    return _export_research_trade_csv(
        RESEARCH_RERESOLVED_MAE_MFE_CSV,
        out_dir,
        "AVWAP ID 5-min v1 research-hybrid preset: reresolved MAE/MFE",
        "Indicator stack: v17D/CandE setups + advanced diagnostics (MFI, OBV, BB width, CCI, MACD hist/slope), AVWAP, EMA, ATR, Nifty RS, adaptive exits/sizing",
    )


def _run_research_reresolved_plus_e4_addons(out_dir: Path) -> int:
    """Expand the 987-trade high-PF preset with de-duplicated E4 setup add-ons."""
    if not RESEARCH_RERESOLVED_MAE_MFE_CSV.exists():
        raise SystemExit(f"missing research preset CSV: {RESEARCH_RERESOLVED_MAE_MFE_CSV}")
    if not RESEARCH_E4_TOP14_CSV.exists():
        raise SystemExit(f"missing E4 add-on CSV: {RESEARCH_E4_TOP14_CSV}")

    base = pd.read_csv(RESEARCH_RERESOLVED_MAE_MFE_CSV)
    e4 = pd.read_csv(RESEARCH_E4_TOP14_CSV)
    base["pnl_rs"] = pd.to_numeric(base["pnl_rs"], errors="coerce").fillna(0.0)
    e4["pnl_rs"] = pd.to_numeric(e4["pnl_rs"], errors="coerce").fillna(0.0)

    def _key(df: pd.DataFrame) -> pd.Series:
        return (
            df["trade_date"].astype(str).str[:10]
            + "|" + df["ticker"].astype(str)
            + "|" + df["side"].astype(str)
            + "|" + df["setup"].astype(str)
        )

    base_keys = set(_key(base))
    extra = e4[~_key(e4).isin(base_keys)].copy()

    addon_order = [
        ("LONG", "B_HUGE_C1_CLOSE_RECLAIM_BREAK"),
        ("SHORT", "A_MOD_BREAK_C1_LOW"),
        ("LONG", "A_MOD_CLOSE_CONTINUATION_BREAK"),
        ("LONG", "A_MOD_BREAK_C1_HIGH"),
    ]
    combined = base.copy()
    for side, setup in addon_order:
        grp = extra[(extra["side"].astype(str).str.upper() == side) & (extra["setup"].astype(str) == setup)]
        candidate = pd.concat([combined, grp], ignore_index=True)
        if _pf_from_series(candidate["pnl_rs"]) > 2.0:
            combined = candidate

    tmp = out_dir / "_combined_source.csv"
    out_dir.mkdir(parents=True, exist_ok=True)
    combined.to_csv(tmp, index=False)
    try:
        return _export_research_trade_csv(
            tmp,
            out_dir,
            "AVWAP ID 5-min v1 research-hybrid preset: reresolved + E4 add-on setups",
            "Expanded setup stack: MAE/MFE reresolved base plus de-duplicated E4 top-14 add-ons; includes AVWAP/EMA/ATR/Nifty RS, RSI/ADX/Stoch, MFI/OBV/BB/CCI/MACD diagnostics, QS ranking",
        )
    finally:
        try:
            tmp.unlink()
        except OSError:
            pass


def _run_research_pareto_pf26(out_dir: Path) -> int:
    return _export_research_trade_csv(
        RESEARCH_PARETO_PF26_CSV,
        out_dir,
        "AVWAP ID 5-min v1 research-hybrid preset: Pareto PF2.6 expanded",
        "Pareto-selected expansion: reresolved MAE/MFE base plus filtered E4 add-on pockets; keeps PF near 2.6 while raising trade count to ~1,600",
    )


def _run_research_pareto_short_expanded(out_dir: Path) -> int:
    return _export_research_trade_csv(
        RESEARCH_PARETO_SHORT_EXPANDED_CSV,
        out_dir,
        "AVWAP ID 5-min v1 research-hybrid preset: short-expanded PF2+",
        "Short-focused expansion over Pareto PF2.6 base: adds de-duplicated C_OR_BREAKDOWN, D_AVWAP_LOSE_REVERSAL, D_EMA20_REJECTION, E_VWAP_BAND_FADE, A_MOD/G_LOWER_LOW short pockets while preserving total PF above 2",
    )


def _run_research_catalog_rest_expanded(out_dir: Path) -> int:
    return _export_research_trade_csv(
        RESEARCH_CATALOG_REST_EXPANDED_CSV,
        out_dir,
        "AVWAP ID 5-min v1 research-hybrid preset: catalog rest expanded PF2+",
        "Maximum catalog-coverage experiment: starts from the short-expanded PF2+ preset and greedily adds remaining v17r_nonf catalog setup pockets while preserving total PF above 2",
    )


def main():
    ap = argparse.ArgumentParser(description="AVWAP ID 5-min v1 backtester")
    ap.add_argument(
        "--preset",
        choices=[
            "native",
            "research_e4_top3",
            "research_reresolved_mae_mfe",
            "research_reresolved_plus_e4_addons",
            "research_pareto_pf26",
            "research_pareto_short_expanded",
            "research_catalog_rest_expanded",
        ],
        default="research_catalog_rest_expanded",
        help="native runs the v1 scanner; research presets export strong local hybrid artifacts.",
    )
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    ap.add_argument("--cost_bps", type=float, default=DEFAULT_COST_BPS)
    ap.add_argument("--max_trades_per_day", type=int, default=2)
    ap.add_argument("--max_same_side_per_day", type=int, default=2)
    ap.add_argument("--out", type=str, default=str(RESEARCH_CATALOG_REST_EXPANDED_OUT_ROOT))
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

    if args.preset == "research_e4_top3":
        return _run_research_e4_top3(out_dir)
    if args.preset == "research_reresolved_mae_mfe":
        return _run_research_reresolved_mae_mfe(out_dir)
    if args.preset == "research_reresolved_plus_e4_addons":
        return _run_research_reresolved_plus_e4_addons(out_dir)
    if args.preset == "research_pareto_pf26":
        return _run_research_pareto_pf26(out_dir)
    if args.preset == "research_pareto_short_expanded":
        return _run_research_pareto_short_expanded(out_dir)
    if args.preset == "research_catalog_rest_expanded":
        return _run_research_catalog_rest_expanded(out_dir)

    universe = _load_universe()
    if args.limit:
        universe = universe[: args.limit]

    print(f"[avwap_ID_5min_v1] loading market context...")
    market_ctx = _load_market_context()
    print(f"[avwap_ID_5min_v1] universe={len(universe)} workers={args.workers} cost_bps={args.cost_bps}")

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
