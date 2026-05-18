"""
Advanced 5-minute intraday indicator calculator.

Reads the configured 5-minute backtesting parquet files and writes enriched
feature files plus ranked setup candidates.

Default input:
    eqidv2_runtime_paths.DATA_5M_DIR
    currently: C:\\TradingData\\eqidv2\\stocks_indicators_5min_eq_live2

Examples:
    python advanced_indicators_calculator.py --limit 20
    python advanced_indicators_calculator.py --workers 8
    python advanced_indicators_calculator.py --data-dir C:\\TradingData\\eqidv2\\stocks_indicators_5min_eq_live2
"""

from __future__ import annotations

import argparse
import json
import math
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

try:
    from eqidv2_runtime_paths import DATA_5M_DIR
except Exception:
    DATA_5M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")


MARKET_TICKERS = ("NIFTYBEES", "NIFTY", "NIFTY50", "NIFTY_50", "NIFTY 50")
DEFAULT_OUT_ROOT = Path("outputs_advanced_indicators_5min")
FILE_SUFFIX = "_stocks_indicators_5min.parquet"

ATR_LOOKBACK = 20
VOL_LOOKBACK = 20
TOD_VOL_SESSIONS = 20
RS_LOOKBACK_BARS = 6          # 30 minutes on 5m bars
OPENING_RANGE_MINUTES = 30
COMPRESSION_LOOKBACK = 20
BB_LOOKBACK = 20
BB_PERCENTILE_LOOKBACK = 100


@dataclass
class SymbolResult:
    ticker: str
    status: str
    rows: int = 0
    first_ts: str = ""
    last_ts: str = ""
    features_path: str = ""
    signals: int = 0
    error: str = ""


def _ticker_from_path(path: Path) -> str:
    return path.name.replace(FILE_SUFFIX, "")


def _safe_div(num: pd.Series, den: pd.Series | float) -> pd.Series:
    return num / pd.Series(den).replace(0, np.nan)


def _read_ohlcv(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if "date" not in df.columns:
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index().rename(columns={df.index.name or "index": "date"})
        else:
            raise ValueError("missing date column")

    required = ["date", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"missing required columns: {missing}")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").drop_duplicates("date").reset_index(drop=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["date", "open", "high", "low", "close"])
    df["volume"] = df["volume"].fillna(0.0)
    df["date_only"] = df["date"].dt.date
    df["bar_time"] = df["date"].dt.strftime("%H:%M")
    return df.reset_index(drop=True)


def _calc_atr(df: pd.DataFrame, lookback: int = ATR_LOOKBACK) -> pd.Series:
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(lookback, min_periods=max(5, lookback // 2)).mean()


def _session_vwap(df: pd.DataFrame) -> pd.Series:
    typical = (df["high"] + df["low"] + df["close"]) / 3.0
    vol = df["volume"].clip(lower=0)
    pv = typical * vol
    cum_vol = vol.groupby(df["date_only"]).cumsum().replace(0, np.nan)
    cum_pv = pv.groupby(df["date_only"]).cumsum()
    return cum_pv / cum_vol


def _rolling_time_of_day_z(df: pd.DataFrame) -> pd.Series:
    out = pd.Series(np.nan, index=df.index, dtype="float64")
    for _, idx in df.groupby("bar_time").groups.items():
        s = df.loc[idx, "volume"].astype(float)
        mean = s.shift(1).rolling(TOD_VOL_SESSIONS, min_periods=5).mean()
        std = s.shift(1).rolling(TOD_VOL_SESSIONS, min_periods=5).std(ddof=0)
        out.loc[idx] = (s - mean) / std.replace(0, np.nan)
    return out


def _add_previous_day_levels(df: pd.DataFrame) -> pd.DataFrame:
    day = (
        df.groupby("date_only", sort=True)
        .agg(day_open=("open", "first"), day_high=("high", "max"),
             day_low=("low", "min"), day_close=("close", "last"),
             day_volume=("volume", "sum"))
        .reset_index()
    )
    day["prev_day_high"] = day["day_high"].shift(1)
    day["prev_day_low"] = day["day_low"].shift(1)
    day["prev_day_close"] = day["day_close"].shift(1)
    day["gap_pct"] = (day["day_open"] / day["prev_day_close"] - 1.0) * 100.0
    return df.merge(
        day[["date_only", "day_open", "day_high", "day_low", "prev_day_high",
             "prev_day_low", "prev_day_close", "gap_pct"]],
        on="date_only",
        how="left",
    )


def _opening_range_levels(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["minutes_from_open"] = out.groupby("date_only").cumcount() * 5
    in_or = out["minutes_from_open"] < OPENING_RANGE_MINUTES
    levels = (
        out[in_or].groupby("date_only")
        .agg(opening_range_high=("high", "max"), opening_range_low=("low", "min"))
        .reset_index()
    )
    out = out.merge(levels, on="date_only", how="left")
    return out


def _add_market_rs(df: pd.DataFrame, market: pd.DataFrame | None) -> pd.DataFrame:
    if market is None or market.empty:
        df["market_ret_30m_pct"] = np.nan
        df["rs_vs_nifty_30m_pct"] = np.nan
        return df

    m = market[["date", "close"]].rename(columns={"close": "market_close"}).copy()
    m["market_ret_30m_pct"] = (m["market_close"] / m["market_close"].shift(RS_LOOKBACK_BARS) - 1.0) * 100.0
    out = df.merge(m[["date", "market_close", "market_ret_30m_pct"]], on="date", how="left")
    out["stock_ret_30m_pct"] = (out["close"] / out["close"].shift(RS_LOOKBACK_BARS) - 1.0) * 100.0
    out["rs_vs_nifty_30m_pct"] = out["stock_ret_30m_pct"] - out["market_ret_30m_pct"]
    return out


def _add_sector_rs(df: pd.DataFrame, sector_df: pd.DataFrame | None) -> pd.DataFrame:
    if sector_df is None or sector_df.empty:
        df["sector_ret_30m_pct"] = np.nan
        df["rs_vs_sector_30m_pct"] = np.nan
        return df
    s = sector_df[["date", "close"]].rename(columns={"close": "sector_close"}).copy()
    s["sector_ret_30m_pct"] = (s["sector_close"] / s["sector_close"].shift(RS_LOOKBACK_BARS) - 1.0) * 100.0
    out = df.merge(s[["date", "sector_close", "sector_ret_30m_pct"]], on="date", how="left")
    if "stock_ret_30m_pct" not in out.columns:
        out["stock_ret_30m_pct"] = (out["close"] / out["close"].shift(RS_LOOKBACK_BARS) - 1.0) * 100.0
    out["rs_vs_sector_30m_pct"] = out["stock_ret_30m_pct"] - out["sector_ret_30m_pct"]
    return out


def _trend_score(df: pd.DataFrame) -> pd.Series:
    higher_high = (df["high"] > df["high"].shift(1)).rolling(5, min_periods=3).sum()
    higher_low = (df["low"] > df["low"].shift(1)).rolling(5, min_periods=3).sum()
    lower_high = (df["high"] < df["high"].shift(1)).rolling(5, min_periods=3).sum()
    lower_low = (df["low"] < df["low"].shift(1)).rolling(5, min_periods=3).sum()

    ema20 = df["EMA_20"] if "EMA_20" in df.columns else df["close"].ewm(span=20, adjust=False).mean()
    up_vol = df["volume"].where(df["close"] > df["open"]).rolling(5, min_periods=3).mean()
    down_vol = df["volume"].where(df["close"] < df["open"]).rolling(5, min_periods=3).mean()

    score = pd.Series(0.0, index=df.index)
    score += np.where(df["close"] > ema20, 15.0, -15.0)
    score += np.where(df["close"] > df["session_vwap"], 15.0, -15.0)
    score += (higher_high.fillna(0) + higher_low.fillna(0)) * 4.0
    score -= (lower_high.fillna(0) + lower_low.fillna(0)) * 4.0
    score += np.where(up_vol > down_vol, 10.0, -5.0)
    score += np.where(df["rs_vs_nifty_30m_pct"] > 0, 15.0, -10.0)
    return score.clip(-100, 100)


def _add_core_features(df: pd.DataFrame, market: pd.DataFrame | None, sector_df: pd.DataFrame | None) -> pd.DataFrame:
    out = _add_previous_day_levels(df)
    out = _opening_range_levels(out)
    out["intraday_high_so_far"] = out.groupby("date_only")["high"].cummax()
    out["intraday_low_so_far"] = out.groupby("date_only")["low"].cummin()
    out["intraday_high_prior"] = out.groupby("date_only")["intraday_high_so_far"].shift(1)
    out["intraday_low_prior"] = out.groupby("date_only")["intraday_low_so_far"].shift(1)

    if "ATR" not in out.columns or out["ATR"].isna().all():
        out["ATR"] = _calc_atr(out)
    else:
        out["ATR"] = pd.to_numeric(out["ATR"], errors="coerce")

    out["session_vwap"] = _session_vwap(out)
    out["avwap_day_open"] = out["session_vwap"]
    out["range"] = out["high"] - out["low"]
    out["body_efficiency"] = (out["close"] - out["open"]).abs() / out["range"].replace(0, np.nan)
    out["close_location"] = (out["close"] - out["low"]) / out["range"].replace(0, np.nan)
    out["upper_wick_pct"] = (out["high"] - out[["open", "close"]].max(axis=1)) / out["range"].replace(0, np.nan)
    out["lower_wick_pct"] = (out[["open", "close"]].min(axis=1) - out["low"]) / out["range"].replace(0, np.nan)
    out["avwap_dist_atr"] = (out["close"] - out["avwap_day_open"]) / out["ATR"].replace(0, np.nan)
    out["avwap_abs_dist_atr"] = out["avwap_dist_atr"].abs()

    prev_vol_mean = out["volume"].shift(1).rolling(VOL_LOOKBACK, min_periods=8).mean()
    prev_vol_std = out["volume"].shift(1).rolling(VOL_LOOKBACK, min_periods=8).std(ddof=0)
    out["volume_ratio_20"] = out["volume"] / prev_vol_mean.replace(0, np.nan)
    out["volume_z_20"] = (out["volume"] - prev_vol_mean) / prev_vol_std.replace(0, np.nan)
    out["volume_z_tod_20"] = _rolling_time_of_day_z(out)

    prev_range_mean = out["range"].shift(1).rolling(20, min_periods=8).mean()
    out["range_expansion_ratio"] = out["range"] / prev_range_mean.replace(0, np.nan)
    out["atr_pct"] = out["ATR"] / out["close"].replace(0, np.nan) * 100.0

    out = _add_market_rs(out, market)
    out = _add_sector_rs(out, sector_df)

    out["buy_volume_proxy"] = np.where(out["close"] > out["open"], out["volume"], 0.0)
    out["sell_volume_proxy"] = np.where(out["close"] < out["open"], out["volume"], 0.0)
    out["buy_pressure_5"] = out["buy_volume_proxy"].rolling(5, min_periods=2).sum()
    out["sell_pressure_5"] = out["sell_volume_proxy"].rolling(5, min_periods=2).sum()
    out["pressure_ratio_5"] = out["buy_pressure_5"] / out["sell_pressure_5"].replace(0, np.nan)

    ma = out["close"].rolling(BB_LOOKBACK, min_periods=10).mean()
    sd = out["close"].rolling(BB_LOOKBACK, min_periods=10).std(ddof=0)
    upper = ma + 2.0 * sd
    lower = ma - 2.0 * sd
    out["bb_width_pct"] = (upper - lower) / ma.replace(0, np.nan) * 100.0
    out["bb_width_percentile_100"] = out["bb_width_pct"].rolling(
        BB_PERCENTILE_LOOKBACK, min_periods=30
    ).rank(pct=True)
    out["inside_candle"] = (out["high"] <= out["high"].shift(1)) & (out["low"] >= out["low"].shift(1))
    out["inside_candle_count_10"] = out["inside_candle"].rolling(10, min_periods=1).sum()
    out["range_contraction"] = out["range_expansion_ratio"] < 0.75
    out["volume_contraction"] = out["volume_ratio_20"] < 0.75
    out["compression_score"] = (
        out["range_contraction"].astype(int) * 30
        + out["volume_contraction"].astype(int) * 25
        + (out["bb_width_percentile_100"] < 0.25).astype(int) * 25
        + (out["inside_candle_count_10"] >= 3).astype(int) * 20
    )

    out["sweep_prev_day_low_long"] = (
        (out["low"] < out["prev_day_low"])
        & (out["close"] > out["prev_day_low"])
        & (out["volume_z_20"] > 1.5)
    )
    out["failed_prev_day_high_short"] = (
        (out["high"] > out["prev_day_high"])
        & (out["close"] < out["prev_day_high"])
        & (out["volume_z_20"] > 1.5)
    )
    after_opening_range = out["minutes_from_open"] >= OPENING_RANGE_MINUTES
    out["break_opening_high"] = after_opening_range & (out["close"] > out["opening_range_high"]) & (
        out["close"].shift(1) <= out["opening_range_high"]
    )
    out["break_opening_low"] = after_opening_range & (out["close"] < out["opening_range_low"]) & (
        out["close"].shift(1) >= out["opening_range_low"]
    )

    out["trend_quality_score"] = _trend_score(out)

    bullish_body = (out["close"] > out["open"]) & (out["body_efficiency"] > 0.65) & (out["close_location"] > 0.70)
    bearish_body = (out["close"] < out["open"]) & (out["body_efficiency"] > 0.65) & (out["close_location"] < 0.30)
    out["momentum_breakout_long"] = (
        out["break_opening_high"]
        & (out["rs_vs_nifty_30m_pct"] > 0)
        & (out["volume_z_tod_20"].fillna(out["volume_z_20"]) > 2.0)
        & bullish_body
        & (out["close"] > out["avwap_day_open"])
        & (out["avwap_abs_dist_atr"] < 1.5)
    )
    out["momentum_breakdown_short"] = (
        out["break_opening_low"]
        & (out["rs_vs_nifty_30m_pct"] < 0)
        & (out["volume_z_tod_20"].fillna(out["volume_z_20"]) > 2.0)
        & bearish_body
        & (out["close"] < out["avwap_day_open"])
        & (out["avwap_abs_dist_atr"] < 1.5)
    )
    out["compression_breakout_long"] = (
        (out["compression_score"].shift(1).rolling(COMPRESSION_LOOKBACK, min_periods=5).max() >= 70)
        & (out["close"] >= out["intraday_high_prior"].rolling(10, min_periods=3).max())
        & (out["rs_vs_nifty_30m_pct"] > 0)
        & (out["volume_z_tod_20"].fillna(out["volume_z_20"]) > 2.0)
        & bullish_body
    )
    out["liquidity_sweep_reversal_long"] = out["sweep_prev_day_low_long"] & (
        out["close"].shift(-1) > out["prev_day_low"]
    )
    out["failed_breakout_short"] = out["failed_prev_day_high_short"] & (
        out["close"].shift(-1) < out["prev_day_high"]
    )

    vol_z = out["volume_z_tod_20"].fillna(out["volume_z_20"]).clip(-3, 6)
    rs = out["rs_vs_nifty_30m_pct"].clip(-5, 5)
    body = out["body_efficiency"].clip(0, 1)
    range_exp = out["range_expansion_ratio"].clip(0, 4)
    avwap_align_long = np.where(out["close"] > out["avwap_day_open"], 1.0, -1.0)
    avwap_align_short = np.where(out["close"] < out["avwap_day_open"], 1.0, -1.0)
    not_extended = np.where(out["avwap_abs_dist_atr"] <= 1.5, 1.0, -1.0)

    out["long_signal_score"] = (
        25 * rs.fillna(0) / 5
        + 20 * vol_z.fillna(0) / 6
        + 15 * body.fillna(0)
        + 15 * avwap_align_long
        + 10 * range_exp.fillna(0) / 4
        + 10 * np.where(out["trend_quality_score"] > 0, 1.0, -1.0)
        + 5 * not_extended
    )
    out["short_signal_score"] = (
        25 * (-rs.fillna(0)) / 5
        + 20 * vol_z.fillna(0) / 6
        + 15 * body.fillna(0)
        + 15 * avwap_align_short
        + 10 * range_exp.fillna(0) / 4
        + 10 * np.where(out["trend_quality_score"] < 0, 1.0, -1.0)
        + 5 * not_extended
    )
    return out


def _signal_rows(ticker: str, df: pd.DataFrame) -> pd.DataFrame:
    setups = [
        ("momentum_breakout_long", "LONG", "long_signal_score"),
        ("compression_breakout_long", "LONG", "long_signal_score"),
        ("liquidity_sweep_reversal_long", "LONG", "long_signal_score"),
        ("momentum_breakdown_short", "SHORT", "short_signal_score"),
        ("failed_breakout_short", "SHORT", "short_signal_score"),
    ]
    rows = []
    base_cols = [
        "date", "date_only", "close", "volume", "ATR", "avwap_day_open",
        "avwap_dist_atr", "volume_z_20", "volume_z_tod_20",
        "rs_vs_nifty_30m_pct", "rs_vs_sector_30m_pct", "body_efficiency",
        "close_location", "range_expansion_ratio", "trend_quality_score",
        "opening_range_high", "opening_range_low", "prev_day_high", "prev_day_low",
    ]
    for setup, side, score_col in setups:
        if setup not in df.columns:
            continue
        sub = df[df[setup]].copy()
        if sub.empty:
            continue
        keep = [c for c in base_cols if c in sub.columns]
        sub = sub[keep].copy()
        sub.insert(0, "ticker", ticker)
        sub.insert(2, "setup", setup)
        sub.insert(3, "side", side)
        sub["score"] = df.loc[sub.index, score_col].values
        rows.append(sub)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    return out.sort_values(["date", "score"], ascending=[True, False])


def _process_one(args: tuple[str, str, str, str | None]) -> tuple[SymbolResult, pd.DataFrame]:
    path_s, out_features_s, market_s, sector_s = args
    path = Path(path_s)
    out_features = Path(out_features_s)
    ticker = _ticker_from_path(path)
    try:
        df = _read_ohlcv(path)
        market = _read_ohlcv(Path(market_s)) if market_s else None
        sector = _read_ohlcv(Path(sector_s)) if sector_s else None
        features = _add_core_features(df, market, sector)

        feature_path = out_features / f"{ticker}_advanced_indicators_5min.parquet"
        features.to_parquet(feature_path, index=False)
        signals = _signal_rows(ticker, features)
        result = SymbolResult(
            ticker=ticker,
            status="ok",
            rows=len(features),
            first_ts=str(features["date"].min()),
            last_ts=str(features["date"].max()),
            features_path=str(feature_path),
            signals=len(signals),
        )
        return result, signals
    except Exception as exc:
        return SymbolResult(ticker=ticker, status="error", error=repr(exc)), pd.DataFrame()


def _load_sector_map(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    if not path.exists():
        return {}, {}
    data = json.loads(path.read_text())
    stock_to_sector: dict[str, str] = {}
    sector_to_symbol: dict[str, str] = {}
    for sector, payload in data.get("sectors", {}).items():
        sector_to_symbol[sector] = payload.get("index_symbol") or payload.get("etf_symbol") or ""
        for stock in payload.get("stocks", []):
            stock_to_sector[str(stock).upper()] = sector
    return stock_to_sector, sector_to_symbol


def _find_market_file(data_dir: Path) -> Path | None:
    for ticker in MARKET_TICKERS:
        p = data_dir / f"{ticker}{FILE_SUFFIX}"
        if p.exists():
            return p
    return None


def _build_manifest(
    data_dir: Path,
    out_dir: Path,
    files: list[Path],
    results: list[SymbolResult],
    market_file: Path | None,
    sector_files_found: dict[str, str],
    started_at: str,
    elapsed_sec: float,
) -> dict:
    ok = [r for r in results if r.status == "ok"]
    latest_dates = [r.last_ts[:10] for r in ok if r.last_ts]
    latest_counts = dict(pd.Series(latest_dates).value_counts().head(20)) if latest_dates else {}
    missing_or_limited = [
        "True bid/ask spread and order-book liquidity cannot be calculated from OHLCV parquet files.",
        "True volume delta/order flow cannot be calculated; buy/sell pressure is only a candle-direction proxy.",
        "News candle anchors require an external/manual event feed; this script uses day/opening/level-derived anchors only.",
        "Sector relative strength is calculated only when a matching sector index/ETF parquet exists in the 5m data folder.",
        "Backtest quality depends on data freshness; stale input folders will produce stale feature outputs.",
    ]
    return {
        "started_at": started_at,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "elapsed_sec": round(elapsed_sec, 2),
        "data_dir": str(data_dir),
        "out_dir": str(out_dir),
        "input_files": len(files),
        "processed_ok": len(ok),
        "processed_error": len(results) - len(ok),
        "market_file": str(market_file) if market_file else None,
        "sector_files_found": sector_files_found,
        "latest_date_distribution": latest_counts,
        "missing_or_limited": missing_or_limited,
        "results": [asdict(r) for r in results],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Calculate advanced indicators for 5-minute EQIDV2 backtesting data.")
    ap.add_argument("--data-dir", type=Path, default=Path(DATA_5M_DIR), help="5-minute parquet input directory.")
    ap.add_argument("--out-dir", type=Path, default=None, help="Output directory. Defaults to timestamped folder.")
    ap.add_argument("--sector-map", type=Path, default=Path("configs/sector_etf_map.json"))
    ap.add_argument("--tickers", nargs="*", default=None, help="Optional ticker list.")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of symbols for smoke runs.")
    ap.add_argument("--workers", type=int, default=1, help="Parallel workers. Use 1 for easiest debugging.")
    ap.add_argument("--include-market-files", action="store_true", help="Also compute features for NIFTY/NIFTYBEES files.")
    args = ap.parse_args()

    started = datetime.now().isoformat(timespec="seconds")
    t0 = time.time()
    data_dir = args.data_dir
    if not data_dir.exists():
        raise SystemExit(f"Data directory not found: {data_dir}")

    out_dir = args.out_dir or DEFAULT_OUT_ROOT / datetime.now().strftime("%Y%m%d_%H%M%S")
    features_dir = out_dir / "features"
    out_dir.mkdir(parents=True, exist_ok=True)
    features_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(data_dir.glob(f"*{FILE_SUFFIX}"))
    if args.tickers:
        wanted = {x.upper() for x in args.tickers}
        files = [p for p in files if _ticker_from_path(p).upper() in wanted]
    if not args.include_market_files:
        market_names = {x.upper() for x in MARKET_TICKERS}
        files = [p for p in files if _ticker_from_path(p).upper() not in market_names]
    if args.limit:
        files = files[: args.limit]

    market_file = _find_market_file(data_dir)
    stock_to_sector, sector_to_symbol = _load_sector_map(args.sector_map)
    sector_files_found: dict[str, str] = {}

    tasks = []
    for p in files:
        ticker = _ticker_from_path(p).upper()
        sector_file = None
        sector = stock_to_sector.get(ticker)
        sector_symbol = sector_to_symbol.get(sector or "")
        if sector_symbol:
            candidate = data_dir / f"{sector_symbol}{FILE_SUFFIX}"
            if candidate.exists():
                sector_file = str(candidate)
                sector_files_found[sector_symbol] = str(candidate)
        tasks.append((str(p), str(features_dir), str(market_file) if market_file else None, sector_file))

    print(f"[advanced_indicators] data_dir={data_dir}")
    print(f"[advanced_indicators] out_dir={out_dir}")
    print(f"[advanced_indicators] symbols={len(tasks)} workers={args.workers}")
    print(f"[advanced_indicators] market_file={market_file}")

    results: list[SymbolResult] = []
    signal_frames: list[pd.DataFrame] = []
    if args.workers and args.workers > 1:
        workers = max(1, min(args.workers, 16))
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_process_one, task) for task in tasks]
            for i, fut in enumerate(as_completed(futs), 1):
                result, signals = fut.result()
                results.append(result)
                if not signals.empty:
                    signal_frames.append(signals)
                if i % 50 == 0 or i == len(futs):
                    print(f"[advanced_indicators] processed {i}/{len(futs)}")
    else:
        for i, task in enumerate(tasks, 1):
            result, signals = _process_one(task)
            results.append(result)
            if not signals.empty:
                signal_frames.append(signals)
            if i % 50 == 0 or i == len(tasks):
                print(f"[advanced_indicators] processed {i}/{len(tasks)}")

    signals_all = pd.concat(signal_frames, ignore_index=True) if signal_frames else pd.DataFrame()
    if not signals_all.empty:
        signals_all = signals_all.sort_values(["date", "score"], ascending=[True, False])
        signals_all.to_csv(out_dir / "advanced_indicator_signals_all.csv", index=False)
        top_daily = (
            signals_all.sort_values(["date_only", "side", "score"], ascending=[True, True, False])
            .groupby(["date_only", "side"], group_keys=False)
            .head(5)
        )
        top_daily.to_csv(out_dir / "advanced_indicator_top5_by_day_side.csv", index=False)
    else:
        (out_dir / "advanced_indicator_signals_all.csv").write_text("")
        (out_dir / "advanced_indicator_top5_by_day_side.csv").write_text("")

    manifest = _build_manifest(
        data_dir=data_dir,
        out_dir=out_dir,
        files=files,
        results=sorted(results, key=lambda r: r.ticker),
        market_file=market_file,
        sector_files_found=sector_files_found,
        started_at=started,
        elapsed_sec=time.time() - t0,
    )
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str))

    limitations_md = ["# Advanced Indicator Calculator Notes", ""]
    limitations_md.append(f"- Data dir: `{data_dir}`")
    limitations_md.append(f"- Output dir: `{out_dir}`")
    limitations_md.append(f"- Processed OK: `{manifest['processed_ok']}`")
    limitations_md.append(f"- Processed errors: `{manifest['processed_error']}`")
    limitations_md.append(f"- Market file: `{manifest['market_file']}`")
    limitations_md.append("")
    limitations_md.append("## Missing Or Limited")
    for item in manifest["missing_or_limited"]:
        limitations_md.append(f"- {item}")
    limitations_md.append("")
    limitations_md.append("## Main Outputs")
    limitations_md.append("- `features/<TICKER>_advanced_indicators_5min.parquet`")
    limitations_md.append("- `advanced_indicator_signals_all.csv`")
    limitations_md.append("- `advanced_indicator_top5_by_day_side.csv`")
    limitations_md.append("- `manifest.json`")
    (out_dir / "README.md").write_text("\n".join(limitations_md) + "\n")

    print(f"[advanced_indicators] done signals={len(signals_all)} out={out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
