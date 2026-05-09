"""v17D Step 2.2 — sector relative strength feature computation.

Replaces v17C's synthetic NIFTY context (`_v17C_no_nifty_context`) with
per-stock vs sector-ETF relative strength. Three features:

    sector_rs_5d           5-day stock return - 5-day sector return
    sector_intraday_rs     today's stock return so far - today's sector return so far
    sector_above_vwap      boolean: is the sector ETF above its session VWAP

Used as filter inputs in the v17D Pareto search and as live-runner gates.

Public API:
    load_sector_map(path=None) -> dict (sector_name -> stocks list, ETF symbol)
    sector_for(ticker, sector_map=None) -> str | None
    compute_features_for_day(stock_5min, sector_5min, today_date) -> dict
    attach_sector_rs(stock_df, sector_df_loader, ticker, sector_map=None)
        -> stock_df with three new columns

Notes:
    - Sector ETF data must be in the same 5-min indicator parquet schema.
    - If a stock isn't in the sector map, sector_for() returns None and
      attach_sector_rs() leaves the new columns as NaN (downstream filters
      will treat the trade as missing-context).
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

DEFAULT_MAP_PATH = Path(__file__).parent / "configs" / "sector_etf_map.json"


def load_sector_map(path: str | Path | None = None) -> dict:
    p = Path(path) if path else DEFAULT_MAP_PATH
    if not p.exists():
        raise SystemExit(f"sector map not found: {p}")
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["sectors"]


_TICKER_TO_SECTOR_CACHE: dict | None = None


def sector_for(ticker: str, sector_map: dict | None = None) -> str | None:
    global _TICKER_TO_SECTOR_CACHE
    if _TICKER_TO_SECTOR_CACHE is None or sector_map is not None:
        m = sector_map or load_sector_map()
        _TICKER_TO_SECTOR_CACHE = {}
        for sec_name, sec_block in m.items():
            for stock in sec_block.get("stocks", []):
                _TICKER_TO_SECTOR_CACHE[stock] = sec_name
    return _TICKER_TO_SECTOR_CACHE.get(ticker)


def sector_etf_symbol(ticker: str, sector_map: dict | None = None) -> str | None:
    sec = sector_for(ticker, sector_map)
    if sec is None:
        return None
    m = sector_map or load_sector_map()
    return m.get(sec, {}).get("index_symbol")


def _daily_return(df: pd.DataFrame, end_ts: pd.Timestamp, lookback_days: int) -> float:
    """Return the close-to-close % return ending at end_ts over lookback_days."""
    if df.empty:
        return float("nan")
    end_ts = pd.Timestamp(end_ts)
    if end_ts.tz is None:
        end_ts = end_ts.tz_localize("Asia/Kolkata")
    end_close_row = df[df.index <= end_ts]
    if end_close_row.empty:
        return float("nan")
    end_close = float(end_close_row["close"].iloc[-1])
    cutoff = end_ts.normalize() - pd.Timedelta(days=lookback_days)
    start_row = df[df.index <= cutoff]
    if start_row.empty:
        return float("nan")
    start_close = float(start_row["close"].iloc[-1])
    if start_close <= 0:
        return float("nan")
    return (end_close - start_close) / start_close * 100.0


def _intraday_return(df: pd.DataFrame, ts: pd.Timestamp) -> float:
    """% return from today's first bar open to ts close."""
    if df.empty:
        return float("nan")
    ts = pd.Timestamp(ts)
    if ts.tz is None:
        ts = ts.tz_localize("Asia/Kolkata")
    today_bars = df[df.index.normalize() == ts.normalize()]
    today_bars = today_bars[today_bars.index <= ts]
    if today_bars.empty:
        return float("nan")
    open_price = float(today_bars["open"].iloc[0])
    close_price = float(today_bars["close"].iloc[-1])
    if open_price <= 0:
        return float("nan")
    return (close_price - open_price) / open_price * 100.0


def _above_session_vwap(df: pd.DataFrame, ts: pd.Timestamp) -> bool | None:
    """Is sector close > sector VWAP at ts? Requires VWAP column."""
    if df.empty or "VWAP" not in df.columns:
        return None
    ts = pd.Timestamp(ts)
    if ts.tz is None:
        ts = ts.tz_localize("Asia/Kolkata")
    sub = df[df.index <= ts]
    if sub.empty:
        return None
    last = sub.iloc[-1]
    if pd.isna(last.get("VWAP")):
        return None
    return bool(last["close"] > last["VWAP"])


def compute_features_at(
    stock_df: pd.DataFrame,
    sector_df: pd.DataFrame,
    ts: pd.Timestamp,
) -> dict:
    """Compute the three sector-RS features at a specific timestamp."""
    s_5d = _daily_return(stock_df, ts, 5)
    sec_5d = _daily_return(sector_df, ts, 5)
    s_intra = _intraday_return(stock_df, ts)
    sec_intra = _intraday_return(sector_df, ts)
    above = _above_session_vwap(sector_df, ts)
    return {
        "sector_rs_5d": (s_5d - sec_5d) if pd.notna(s_5d) and pd.notna(sec_5d) else float("nan"),
        "sector_intraday_rs_pct": (s_intra - sec_intra) if pd.notna(s_intra) and pd.notna(sec_intra) else float("nan"),
        "sector_above_vwap": above,
    }


def attach_sector_rs(
    signal_rows: list,
    stock_df: pd.DataFrame,
    sector_df: pd.DataFrame,
) -> list:
    """Attach sector_rs_* columns to a list of signal-row dicts.

    Each row must have 'signal_time_ist' parseable as Timestamp.
    Returns a new list (does not mutate input).
    """
    out = []
    for row in signal_rows:
        feats = compute_features_at(stock_df, sector_df, row["signal_time_ist"])
        new = dict(row)
        new.update(feats)
        out.append(new)
    return out


if __name__ == "__main__":
    import numpy as np

    # Smoke test: synthetic stock + sector data
    idx = pd.DatetimeIndex(
        [pd.Timestamp("2026-05-08 09:15", tz="Asia/Kolkata")
         + pd.Timedelta(minutes=5 * i) for i in range(75)]
    )
    stock = pd.DataFrame({
        "open": np.linspace(100, 105, 75),
        "high": np.linspace(101, 106, 75),
        "low":  np.linspace(99, 104, 75),
        "close": np.linspace(100, 105, 75),
        "VWAP": np.linspace(100, 104, 75),
    }, index=idx)
    sector = pd.DataFrame({
        "open": np.full(75, 200.0),
        "high": np.full(75, 200.5),
        "low":  np.full(75, 199.5),
        "close": np.linspace(200, 201, 75),
        "VWAP": np.full(75, 200.0),
    }, index=idx)
    feats = compute_features_at(stock, sector, idx[60])
    print(f"Sector RS features at {idx[60]}: {feats}")
    assert feats["sector_intraday_rs_pct"] > 0, "stock outperforming sector intraday"
    print("Sector RS smoke test passed.")
    sec = sector_for("RELIANCE")
    print(f"sector_for('RELIANCE') = {sec}")
