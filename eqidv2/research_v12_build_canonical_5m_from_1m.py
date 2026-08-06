"""Build an immutable, end-labelled 5-minute research store from V12 1-minute bars.

The existing historical 5-minute directory contains mixed ticker/day timestamp
regimes.  This research utility avoids mutating that source: it aggregates the
V12 one-minute execution files into canonical IST completion buckets, fills
empty five-minute buckets causally, and recomputes the lightweight indicators
needed by the hourly prefilter and two-bar replay.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


IST = "Asia/Kolkata"
DEFAULT_START = "2026-05-26"
DEFAULT_END = "2026-08-04"
DEFAULT_1M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
DEFAULT_UNIVERSE = Path(
    r"C:\TradingData\eqidv2\runtime_status\feed_universe_kiteticker_5m.json"
)
DEFAULT_OUT = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\canonical_5m_from_1m_20260526_20260804"
)
ONE_MINUTE_COLUMNS = ("date", "open", "high", "low", "close", "volume")


def _normalise_ist(values: pd.Series) -> pd.Series:
    out = pd.to_datetime(values, errors="coerce")
    if out.dt.tz is None:
        return out.dt.tz_localize(IST)
    return out.dt.tz_convert(IST)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_window(path: Path, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    available = set(pq.ParquetFile(path).schema_arrow.names)
    missing = set(ONE_MINUTE_COLUMNS) - available
    if missing:
        raise RuntimeError(f"missing 1m columns {sorted(missing)}")
    try:
        frame = pd.read_parquet(
            path,
            columns=list(ONE_MINUTE_COLUMNS),
            filters=[("date", ">=", start), ("date", "<", end)],
        )
    except Exception:
        frame = pd.read_parquet(path, columns=list(ONE_MINUTE_COLUMNS))
    frame["date"] = _normalise_ist(frame["date"])
    return frame.loc[frame["date"].between(start, end, inclusive="left")].copy()


def _market_days(one_minute_dir: Path, start_date: str, end_date: str) -> list[str]:
    path = one_minute_dir / "RELIANCE_stocks_indicators_1min.parquet"
    start = pd.Timestamp(start_date, tz=IST)
    end = pd.Timestamp(end_date, tz=IST) + pd.Timedelta(days=1)
    frame = _read_window(path, start, end)
    clock = frame["date"].dt.hour * 60 + frame["date"].dt.minute
    days = sorted(frame.loc[clock.between(9 * 60 + 16, 15 * 60 + 30), "date"].dt.strftime("%Y-%m-%d").unique())
    if not days:
        raise RuntimeError("RELIANCE 1m file has no market sessions in requested window")
    return days


def _expected_grid(market_days: list[str]) -> pd.DatetimeIndex:
    values: list[pd.Timestamp] = []
    for day in market_days:
        values.extend(
            pd.date_range(
                pd.Timestamp(f"{day} 09:20", tz=IST),
                pd.Timestamp(f"{day} 15:30", tz=IST),
                freq="5min",
            ).tolist()
        )
    return pd.DatetimeIndex(values, name="date")


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    change = close.diff()
    gain = change.clip(lower=0.0)
    loss = (-change).clip(lower=0.0)
    average_gain = gain.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    average_loss = loss.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean()
    relative = average_gain / average_loss.replace(0.0, np.nan)
    return 100.0 - 100.0 / (1.0 + relative)


def _dmi_adx(frame: pd.DataFrame, period: int = 14) -> tuple[pd.Series, pd.Series, pd.Series]:
    high = frame["high"]
    low = frame["low"]
    close = frame["close"]
    up = high.diff()
    down = -low.diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0.0), up, 0.0), index=frame.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0.0), down, 0.0), index=frame.index)
    previous_close = close.shift(1)
    true_range = pd.concat(
        [(high - low).abs(), (high - previous_close).abs(), (low - previous_close).abs()],
        axis=1,
    ).max(axis=1)
    alpha = 1.0 / period
    smoothed_range = true_range.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    plus = 100.0 * plus_dm.ewm(alpha=alpha, adjust=False, min_periods=period).mean() / smoothed_range.replace(0.0, np.nan)
    minus = 100.0 * minus_dm.ewm(alpha=alpha, adjust=False, min_periods=period).mean() / smoothed_range.replace(0.0, np.nan)
    dx = 100.0 * (plus - minus).abs() / (plus + minus).replace(0.0, np.nan)
    adx = dx.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
    return plus, minus, adx


def _add_indicators(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy()
    close = work["close"]
    previous_close = close.shift(1)
    true_range = pd.concat(
        [
            (work["high"] - work["low"]).abs(),
            (work["high"] - previous_close).abs(),
            (work["low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    work["ATR"] = true_range.rolling(14, min_periods=14).mean()
    work["EMA_20"] = close.ewm(span=20, adjust=False).mean()
    work["EMA_50"] = close.ewm(span=50, adjust=False).mean()
    work["EMA_200"] = close.ewm(span=200, adjust=False).mean()
    work["RSI"] = _rsi(close)
    plus, minus, adx = _dmi_adx(work)
    work["Plus_DI"] = plus
    work["Minus_DI"] = minus
    work["ADX"] = adx
    low_14 = work["low"].rolling(14, min_periods=14).min()
    high_14 = work["high"].rolling(14, min_periods=14).max()
    span = (high_14 - low_14).replace(0.0, np.nan)
    work["Stoch_%K"] = 100.0 * (close - low_14) / span
    work["Stoch_%D"] = work["Stoch_%K"].rolling(3, min_periods=3).mean()
    return work


def _session_open_avwap(frame: pd.DataFrame) -> pd.Series:
    """Return causal session-open AVWAP for already eligible real 5m bars."""
    timestamps = _normalise_ist(frame["date"])
    high = pd.to_numeric(frame["high"], errors="coerce")
    low = pd.to_numeric(frame["low"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    volume = pd.to_numeric(frame["volume"], errors="coerce").clip(lower=0.0)
    typical_price = (high + low + close) / 3.0
    valid = (
        timestamps.notna()
        & typical_price.notna()
        & volume.notna()
        & np.isfinite(typical_price)
        & np.isfinite(volume)
    )
    session = timestamps.dt.normalize()
    weighted_price = (typical_price * volume).where(valid, 0.0)
    eligible_volume = volume.where(valid, 0.0)
    numerator = weighted_price.groupby(session, sort=False).cumsum()
    denominator = eligible_volume.groupby(session, sort=False).cumsum()
    avwap = numerator.div(denominator.replace(0.0, np.nan))
    return avwap.where(valid)


def _canonical_ticker(
    ticker: str,
    one_minute_dir: Path,
    output_dir: Path,
    start: pd.Timestamp,
    end: pd.Timestamp,
    grid: pd.DatetimeIndex,
    market_days: list[str],
) -> dict[str, Any]:
    source = one_minute_dir / f"{ticker}_stocks_indicators_1min.parquet"
    if not source.exists():
        raise RuntimeError(f"{ticker}:missing 1m file")
    minute = _read_window(source, start, end)
    if minute.empty:
        raise RuntimeError(f"{ticker}:empty 1m window")
    for column in ONE_MINUTE_COLUMNS[1:]:
        minute[column] = pd.to_numeric(minute[column], errors="coerce")
    minute = minute.dropna(subset=["date", "open", "high", "low", "close", "volume"])
    minute = minute.sort_values("date", kind="mergesort").drop_duplicates("date", keep="last")
    clock = minute["date"].dt.hour * 60 + minute["date"].dt.minute
    minute = minute.loc[clock.between(9 * 60 + 16, 15 * 60 + 30)].copy()
    minute["date"] = minute["date"].dt.ceil("5min")
    bars = minute.groupby("date", sort=True).agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        source_1m_count=("close", "size"),
    )
    bars = bars.reindex(grid)
    observed = bars["close"].notna()
    # Never back-fill a pre-listing period from a future trade.  Leading rows
    # remain NaN/gap-filled and are therefore ineligible to the prefilter.
    fill_price = bars["close"].ffill()
    for column in ("open", "high", "low", "close"):
        bars[column] = bars[column].where(observed, fill_price)
    bars["volume"] = bars["volume"].where(observed, 0.0).fillna(0.0)
    bars["source_1m_count"] = bars["source_1m_count"].fillna(0).astype(int)
    complete_source_bucket = observed & bars["source_1m_count"].eq(5)
    bars["gap_filled"] = (~complete_source_bucket).astype(int)
    bars["opening_snapshot"] = False
    bars = bars.reset_index()
    complete = bars.loc[bars["gap_filled"].eq(0)].copy()
    complete = _add_indicators(complete)
    complete["AVWAP"] = _session_open_avwap(complete)
    indicator_columns = (
        "ATR",
        "EMA_20",
        "EMA_50",
        "EMA_200",
        "RSI",
        "Plus_DI",
        "Minus_DI",
        "ADX",
        "Stoch_%K",
        "Stoch_%D",
        "AVWAP",
    )
    complete = complete.set_index("date")
    for column in indicator_columns:
        bars[column] = bars["date"].map(complete[column])

    first = bars.loc[
        (bars["date"].dt.hour == 9) & (bars["date"].dt.minute == 20)
    ].copy()
    first["date"] = first["date"] - pd.Timedelta(minutes=5)
    first["opening_snapshot"] = True
    # The 09:15 row is a backward copy of the completed 09:20 bucket.  It is
    # retained for legacy shape compatibility, but must never leak that future
    # bucket into an anchored indicator.
    first["AVWAP"] = np.nan
    canonical = (
        pd.concat([first, bars], ignore_index=True)
        .sort_values("date", kind="mergesort")
        .reset_index(drop=True)
    )
    expected_rows = len(market_days) * 76
    if len(canonical) != expected_rows or canonical["date"].duplicated().any():
        raise RuntimeError(
            f"{ticker}:canonical completeness mismatch rows={len(canonical)} expected={expected_rows}"
        )
    target = output_dir / f"{ticker}_stocks_indicators_5min.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{target.name}.", suffix=".tmp", dir=target.parent, delete=False
        ) as handle:
            temporary = handle.name
        canonical.to_parquet(temporary, index=False, compression="snappy")
        os.replace(temporary, target)
        temporary = None
    finally:
        if temporary is not None:
            try:
                os.remove(temporary)
            except FileNotFoundError:
                pass
    return {
        "ticker": ticker,
        "rows": len(canonical),
        "observed_5m_bars": int(observed.sum()),
        "complete_source_5m_bars": int(complete_source_bucket.sum()),
        "gap_filled_5m_bars": int((~complete_source_bucket).sum()),
        "partial_source_buckets": int(bars["source_1m_count"].between(1, 4).sum()),
        "output_bytes": target.stat().st_size,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build canonical research 5m store from V12 1m")
    parser.add_argument("--start-date", default=DEFAULT_START)
    parser.add_argument("--end-date", default=DEFAULT_END)
    parser.add_argument("--one-minute-dir", type=Path, default=DEFAULT_1M_DIR)
    parser.add_argument("--universe-manifest", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--workers", type=int, default=8)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = time.time()
    universe_payload = json.loads(args.universe_manifest.read_text(encoding="utf-8"))
    tickers = sorted({str(value).upper().strip() for value in universe_payload["symbols"]})
    if len(tickers) != int(universe_payload["universe_count"]):
        raise RuntimeError("universe manifest count mismatch")
    market_days = _market_days(args.one_minute_dir, args.start_date, args.end_date)
    grid = _expected_grid(market_days)
    start = pd.Timestamp(args.start_date, tz=IST)
    end = pd.Timestamp(args.end_date, tz=IST) + pd.Timedelta(days=1)
    args.out.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
        futures = {
            executor.submit(
                _canonical_ticker,
                ticker,
                args.one_minute_dir,
                args.out,
                start,
                end,
                grid,
                market_days,
            ): ticker
            for ticker in tickers
        }
        for done, future in enumerate(as_completed(futures), 1):
            ticker = futures[future]
            try:
                rows.append(future.result())
            except Exception as exc:
                errors.append(f"{ticker}:{type(exc).__name__}:{exc}")
            if done % 100 == 0 or done == len(futures):
                print(
                    f"[canonical 5m] {done:,}/{len(futures):,} "
                    f"ok={len(rows):,} errors={len(errors):,} elapsed={time.time()-started:.1f}s",
                    flush=True,
                )
    if errors:
        raise RuntimeError(f"canonical build incomplete: {errors[:30]}")
    audit = pd.DataFrame(rows).sort_values("ticker", kind="mergesort")
    audit.to_csv(args.out / "canonical_build_audit.csv", index=False)
    summary = {
        "research_only": True,
        "source_one_minute_dir": str(args.one_minute_dir.resolve()),
        "source_universe_manifest": str(args.universe_manifest.resolve()),
        "source_universe_sha256": _sha256(args.universe_manifest),
        "universe_sha256": universe_payload["universe_sha256"],
        "start_date": args.start_date,
        "end_date": args.end_date,
        "market_days": market_days,
        "sessions": len(market_days),
        "tickers": len(tickers),
        "expected_rows_per_ticker": len(market_days) * 76,
        "observed_5m_bars": int(audit["observed_5m_bars"].sum()),
        "complete_source_5m_bars": int(audit["complete_source_5m_bars"].sum()),
        "gap_filled_5m_bars": int(audit["gap_filled_5m_bars"].sum()),
        "partial_source_buckets": int(audit["partial_source_buckets"].sum()),
        "runtime_seconds": time.time() - started,
        "contract": {
            "timestamps": "IST end-labelled 5m completion times",
            "aggregation": "1m rows ending within each 5m completion bucket",
            "missing_bucket": "previous canonical close, zero volume, gap_filled=1",
            "opening_snapshot": "09:15 duplicate of first completed 09:20 bar",
            "AVWAP": (
                "causal session-open typical-price x volume / volume; real complete "
                "5m bars only; 09:15 snapshots and gap/partial buckets are NaN"
            ),
            "source_mutated": False,
        },
    }
    (args.out / "canonical_build_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
