# -*- coding: utf-8 -*-
"""
EQIDV4 ORB + VWAP + RVOL core utilities.

This module provides shared logic for:
- universe selection (20-session median traded value)
- 5m/15m data preparation with strict completed-candle usage
- opening range construction (ORB-15 / ORB-30)
- entry signal checks for long/short
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date as date_t
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import pytz

IST = pytz.timezone("Asia/Kolkata")

DIR_5M = "stocks_indicators_5min_eq"
DIR_15M = "stocks_indicators_15min_eq"
END_5M = "_stocks_indicators_5min.parquet"
END_15M = "_stocks_indicators_15min.parquet"

SESSION_START = dtime(9, 15)
SESSION_END = dtime(15, 30)


@dataclass
class StrategyConfig:
    # Universe
    universe_top_n: int = 500
    universe_lookback_sessions: int = 20
    universe_min_history_sessions: int = 10
    universe_min_median_traded_value: float = 5.0e7
    universe_min_price: float = 40.0

    # ORB + filters
    orb_minutes: int = 15  # 15 or 30
    or_buffer_pct: float = 0.0005
    or_buffer_atr15_mult: float = 0.10
    rvol_lookback_sessions: int = 20
    rvol_min: float = 1.5
    anti_chop_or_atr15_mult: float = 0.25
    require_two_close_confirm: bool = True

    # Time controls
    no_new_entries_after: dtime = dtime(14, 45)
    force_exit_time: dtime = dtime(15, 10)
    hard_exit_time: dtime = dtime(15, 15)

    # Risk
    risk_per_trade_pct: float = 0.001  # 0.10% capital
    max_open_positions: int = 10
    max_trades_per_symbol_per_day: int = 1
    daily_loss_limit_r: float = -1.5
    stop_atr5_mult: float = 0.8
    partial_at_r: float = 1.0
    time_stop_bars: int = 6
    time_stop_min_r: float = 0.5
    trail_method: str = "vwap15"  # "vwap15" or "ema20_5m"

    # Costs
    slippage_bps: float = 2.0
    tx_cost_bps_per_side: float = 1.0
    fixed_cost_per_order: float = 0.0

    # Index regime gate (optional; disabled if index data unavailable)
    use_index_regime_filter: bool = True
    index_ticker: str = "NIFTY50"

    def to_dict(self) -> Dict[str, object]:
        out = asdict(self)
        for k, v in list(out.items()):
            if isinstance(v, dtime):
                out[k] = v.strftime("%H:%M:%S")
        return out


def _parse_ts_ist(col: pd.Series) -> pd.Series:
    ts = pd.to_datetime(col, errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize(IST)
    else:
        ts = ts.dt.tz_convert(IST)
    return ts


def _ensure_numeric(df: pd.DataFrame, cols: Iterable[str]) -> None:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")


def _atr_wilder(df: pd.DataFrame, period: int = 14) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            (df["high"] - df["low"]).abs(),
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1.0 / float(period), adjust=False, min_periods=period).mean()
    return atr


def _session_vwap(df: pd.DataFrame) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    num = (tp * df["volume"]).groupby(df["session_date"]).cumsum()
    den = df["volume"].groupby(df["session_date"]).cumsum()
    return num / den.replace(0, np.nan)


def list_tickers(root: Path) -> List[str]:
    d = root / DIR_5M
    if not d.exists():
        return []
    out: List[str] = []
    for p in d.glob(f"*{END_5M}"):
        name = p.name
        if name.endswith(END_5M):
            out.append(name[: -len(END_5M)].upper())
    return sorted(set(out))


def _load_parquet(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
    except Exception:
        return None
    if df is None or df.empty or "date" not in df.columns:
        return None
    return df


def _prep_frame(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["date"] = _parse_ts_ist(x["date"])
    x = x.dropna(subset=["date"]).sort_values("date").drop_duplicates(subset=["date"])
    _ensure_numeric(x, ["open", "high", "low", "close", "volume", "ATR", "EMA_20", "EMA_50", "VWAP"])
    x["session_date"] = x["date"].dt.date
    x["clock"] = x["date"].dt.time
    x = x[(x["clock"] >= SESSION_START) & (x["clock"] <= SESSION_END)].copy()
    if "ATR" not in x.columns or x["ATR"].isna().all():
        x["ATR"] = _atr_wilder(x, 14)
    if "EMA_20" not in x.columns or x["EMA_20"].isna().all():
        x["EMA_20"] = x["close"].ewm(span=20, adjust=False).mean()
    if "EMA_50" not in x.columns or x["EMA_50"].isna().all():
        x["EMA_50"] = x["close"].ewm(span=50, adjust=False).mean()
    # Force session-reset VWAP to avoid leakage from incorrect precomputed fields.
    x["VWAP"] = _session_vwap(x)
    return x


def _add_rvol(df5: pd.DataFrame, lookback_sessions: int) -> pd.DataFrame:
    x = df5.copy()
    x["slot"] = x["date"].dt.strftime("%H:%M")
    x["rvol_ref"] = np.nan
    for slot, idx in x.groupby("slot").groups.items():
        sub = x.loc[idx, ["session_date", "volume"]].copy()
        sub = sub.sort_values("session_date")
        sub["rvol_ref"] = (
            sub["volume"]
            .rolling(lookback_sessions, min_periods=max(5, lookback_sessions // 2))
            .median()
            .shift(1)
        )
        x.loc[sub.index, "rvol_ref"] = sub["rvol_ref"].values
    x["rvol"] = x["volume"] / x["rvol_ref"]
    return x


def load_symbol_data(root: Path, ticker: str, cfg: StrategyConfig) -> Optional[Tuple[pd.DataFrame, pd.DataFrame]]:
    p5 = root / DIR_5M / f"{ticker.upper()}{END_5M}"
    p15 = root / DIR_15M / f"{ticker.upper()}{END_15M}"
    df5 = _load_parquet(p5)
    df15 = _load_parquet(p15)
    if df5 is None or df15 is None:
        return None

    df5 = _prep_frame(df5)
    df15 = _prep_frame(df15)
    if df5.empty or df15.empty:
        return None

    df5 = _add_rvol(df5, lookback_sessions=cfg.rvol_lookback_sessions)
    df15["EMA20_PREV"] = df15["EMA_20"].shift(1)
    return df5, df15


def build_turnover_cache(
    root: Path,
    tickers: Optional[List[str]] = None,
    refresh: bool = False,
) -> pd.DataFrame:
    out_dir = root / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    cache = out_dir / "universe_turnover_15m.csv"
    if cache.exists() and not refresh:
        try:
            c = pd.read_csv(cache)
            c["session_date"] = pd.to_datetime(c["session_date"]).dt.date
            return c
        except Exception:
            pass

    use_tickers = tickers or list_tickers(root)
    rows: List[Dict[str, object]] = []
    for i, t in enumerate(use_tickers, start=1):
        p = root / DIR_15M / f"{t}{END_15M}"
        df = _load_parquet(p)
        if df is None:
            continue
        df = _prep_frame(df)
        if df.empty:
            continue
        g = df.groupby("session_date", as_index=False).agg(
            traded_value=("close", lambda s: float(np.nansum(s.to_numpy() * df.loc[s.index, "volume"].to_numpy()))),
            close_last=("close", "last"),
        )
        g["ticker"] = t
        rows.extend(g.to_dict(orient="records"))
        if i % 200 == 0:
            print(f"[UNIVERSE] processed {i}/{len(use_tickers)} symbols", flush=True)

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out[["session_date", "ticker", "traded_value", "close_last"]].copy()
    out["session_date"] = pd.to_datetime(out["session_date"]).dt.date
    out.to_csv(cache, index=False)
    print(f"[UNIVERSE] turnover cache written: {cache} rows={len(out)}", flush=True)
    return out


def select_universe_for_date(
    turnover_df: pd.DataFrame,
    trade_date: date_t,
    cfg: StrategyConfig,
) -> List[str]:
    if turnover_df is None or turnover_df.empty:
        return []
    d = pd.to_datetime(trade_date).date()
    hist_dates = sorted(x for x in turnover_df["session_date"].unique() if x < d)
    if not hist_dates:
        return []
    use_dates = hist_dates[-cfg.universe_lookback_sessions :]
    hist = turnover_df[turnover_df["session_date"].isin(use_dates)].copy()
    if hist.empty:
        return []

    med = hist.groupby("ticker", as_index=False).agg(
        med_traded_value=("traded_value", "median"),
        med_close=("close_last", "median"),
        sessions=("session_date", "nunique"),
    )
    med = med[
        (med["sessions"] >= cfg.universe_min_history_sessions)
        & (med["med_traded_value"] >= cfg.universe_min_median_traded_value)
        & (med["med_close"] >= cfg.universe_min_price)
    ].copy()
    med = med.sort_values("med_traded_value", ascending=False).head(cfg.universe_top_n)
    return med["ticker"].astype(str).str.upper().tolist()


def prepare_day_merged_5m_15m(
    df5: pd.DataFrame,
    df15: pd.DataFrame,
    trade_date: date_t,
) -> pd.DataFrame:
    d = pd.to_datetime(trade_date).date()
    d5 = df5[df5["session_date"] == d].copy()
    d15 = df15[df15["session_date"] == d].copy()
    if d5.empty or d15.empty:
        return pd.DataFrame()

    ctx15 = d15[["date", "close", "VWAP", "EMA_20", "EMA_50", "EMA20_PREV", "ATR"]].copy()
    ctx15 = ctx15.rename(
        columns={
            "close": "close15",
            "VWAP": "vwap15",
            "EMA_20": "ema20_15",
            "EMA_50": "ema50_15",
            "EMA20_PREV": "ema20_prev_15",
            "ATR": "atr15",
        }
    ).sort_values("date")

    d5 = d5.sort_values("date")
    m = pd.merge_asof(d5, ctx15, on="date", direction="backward", allow_exact_matches=True)
    m["atr5"] = m["ATR"]
    m["ema20_5m"] = m["EMA_20"]
    return m


def opening_range(day5: pd.DataFrame, orb_minutes: int) -> Optional[Dict[str, object]]:
    if day5 is None or day5.empty:
        return None
    bars = max(1, int(round(orb_minutes / 5.0)))
    first = day5.sort_values("date").head(bars)
    if len(first) < bars:
        return None
    return {
        "or_high": float(first["high"].max()),
        "or_low": float(first["low"].min()),
        "or_range": float(first["high"].max() - first["low"].min()),
        "or_end_ts": pd.Timestamp(first.iloc[-1]["date"]),
    }


def infer_index_regime_flags(
    root: Path,
    trade_date: date_t,
    cfg: StrategyConfig,
) -> Optional[pd.DataFrame]:
    if not cfg.use_index_regime_filter:
        return None
    p = root / DIR_15M / f"{cfg.index_ticker.upper()}{END_15M}"
    df = _load_parquet(p)
    if df is None:
        return None
    df = _prep_frame(df)
    df = df[df["session_date"] == pd.to_datetime(trade_date).date()].copy()
    if df.empty:
        return None
    df["long_ok"] = (df["close"] > df["VWAP"]) & (df["EMA_20"] > df["EMA_50"])
    df["short_ok"] = (df["close"] < df["VWAP"]) & (df["EMA_20"] < df["EMA_50"])
    return df[["date", "long_ok", "short_ok"]].sort_values("date").copy()


def attach_index_flags(day_df: pd.DataFrame, idx_flags: Optional[pd.DataFrame]) -> pd.DataFrame:
    if day_df.empty:
        return day_df
    if idx_flags is None or idx_flags.empty:
        day_df = day_df.copy()
        day_df["index_long_ok"] = True
        day_df["index_short_ok"] = True
        return day_df
    out = pd.merge_asof(
        day_df.sort_values("date"),
        idx_flags.sort_values("date"),
        on="date",
        direction="backward",
        allow_exact_matches=True,
    )
    out["index_long_ok"] = out["long_ok"].fillna(False).astype(bool)
    out["index_short_ok"] = out["short_ok"].fillna(False).astype(bool)
    out = out.drop(columns=["long_ok", "short_ok"], errors="ignore")
    return out


def _buffer_value(price: float, atr15: float, cfg: StrategyConfig) -> float:
    atr_term = 0.0 if not np.isfinite(atr15) else cfg.or_buffer_atr15_mult * float(atr15)
    return max(cfg.or_buffer_pct * float(price), atr_term)


def check_entry_signal(
    row: pd.Series,
    prev_row: Optional[pd.Series],
    orb: Dict[str, object],
    side: str,
    cfg: StrategyConfig,
) -> Tuple[bool, str, Optional[float]]:
    s = side.upper()
    if s not in ("LONG", "SHORT"):
        return False, "bad_side", None
    if not np.isfinite(row.get("atr5", np.nan)) or float(row["atr5"]) <= 0:
        return False, "bad_atr5", None
    if not np.isfinite(row.get("atr15", np.nan)) or float(row["atr15"]) <= 0:
        return False, "bad_atr15", None
    if not np.isfinite(row.get("rvol", np.nan)):
        return False, "no_rvol_ref", None
    if float(row["rvol"]) < float(cfg.rvol_min):
        return False, "low_rvol", None

    or_range = float(orb["or_range"])
    if or_range < float(cfg.anti_chop_or_atr15_mult) * float(row["atr15"]):
        return False, "anti_chop", None

    buffer_now = _buffer_value(float(row["close"]), float(row["atr15"]), cfg)
    or_high = float(orb["or_high"])
    or_low = float(orb["or_low"])

    if s == "LONG":
        if not bool(row.get("index_long_ok", True)):
            return False, "index_regime", None
        if not (
            float(row["close15"]) > float(row["vwap15"])
            and float(row["ema20_15"]) > float(row["ema50_15"])
            and float(row["ema20_15"]) > float(row["ema20_prev_15"])
        ):
            return False, "context_filter", None
        cond_now = float(row["close"]) > (or_high + buffer_now)
        if cfg.require_two_close_confirm:
            if prev_row is None:
                return False, "need_prev_confirm", None
            prev_buf = _buffer_value(float(prev_row["close"]), float(prev_row["atr15"]), cfg)
            cond_prev = float(prev_row["close"]) > (or_high + prev_buf)
            if not (cond_now and cond_prev):
                return False, "breakout_confirm_fail", None
        elif not cond_now:
            return False, "breakout_fail", None
        return True, "ok", buffer_now

    # SHORT
    if not bool(row.get("index_short_ok", True)):
        return False, "index_regime", None
    if not (
        float(row["close15"]) < float(row["vwap15"])
        and float(row["ema20_15"]) < float(row["ema50_15"])
        and float(row["ema20_15"]) < float(row["ema20_prev_15"])
    ):
        return False, "context_filter", None
    cond_now = float(row["close"]) < (or_low - buffer_now)
    if cfg.require_two_close_confirm:
        if prev_row is None:
            return False, "need_prev_confirm", None
        prev_buf = _buffer_value(float(prev_row["close"]), float(prev_row["atr15"]), cfg)
        cond_prev = float(prev_row["close"]) < (or_low - prev_buf)
        if not (cond_now and cond_prev):
            return False, "breakdown_confirm_fail", None
    elif not cond_now:
        return False, "breakdown_fail", None
    return True, "ok", buffer_now


def apply_slippage(price: float, side: str, is_entry: bool, slippage_bps: float) -> float:
    b = float(slippage_bps) / 10000.0
    s = side.upper()
    if s == "LONG":
        return float(price) * (1.0 + b) if is_entry else float(price) * (1.0 - b)
    return float(price) * (1.0 - b) if is_entry else float(price) * (1.0 + b)


def transaction_cost(notional: float, cfg: StrategyConfig) -> float:
    return float(notional) * (float(cfg.tx_cost_bps_per_side) / 10000.0) + float(cfg.fixed_cost_per_order)
