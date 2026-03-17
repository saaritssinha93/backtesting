# -*- coding: utf-8 -*-
"""
Dedicated 15-minute NIFTY/NIFTYBEES fetcher for the backtest parquet folder.

This is a thin wrapper around the stock-only 15m fetcher so that:
- timestamps are identical
- incremental updates behave the same way
- indicators/parquet layout match the stock dataset
- outputs land in the runtime 15m parquet folder for v7/v15 runners

Examples:
    python trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_15minonly.py
    python trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_15minonly.py --symbol NIFTYBEES --aliases NIFTYBEES
    python trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_15minonly.py --symbol NIFTYBEES --aliases NIFTYBEES,NIFTY50,NIFTY_50,NIFTY
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_15minonly import (
    DEFAULT_INTRADAY_TIMESTAMP,
    HOLIDAYS_FILE_DEFAULT,
    IST_TZ,
    MARKET_CLOSE_TIME,
    MARKET_OPEN_TIME,
    OUT_DIR,
    STEP_MIN,
    _is_trading_day,
    _compute_features_15m,
    _downcast_numeric_columns,
    _finalize_and_save,
    _load_existing_ohlc,
    _read_holidays,
    _read_last_ts_from_store,
    _resolve_existing_store_path,
    _round_down_session_anchored,
    expected_last_stamp,
    fetch_historical_15min_df,
    get_start_date,
    setup_kite_session,
    setup_logger,
    ticker_is_fresh,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=f"Fetch/update 15m NIFTY parquet(s) in {OUT_DIR}."
    )
    p.add_argument(
        "--symbol",
        type=str,
        default="NIFTYBEES",
        help="Preferred NSE symbol to fetch. Default=NIFTYBEES",
    )
    p.add_argument(
        "--aliases",
        type=str,
        default="NIFTYBEES",
        help="Comma-separated output aliases. Example: NIFTYBEES,NIFTY50,NIFTY_50,NIFTY",
    )
    p.add_argument(
        "--from-date",
        type=str,
        default=None,
        help="Historical start date YYYY-MM-DD. Default uses fetcher anchor.",
    )
    p.add_argument(
        "--to-date",
        type=str,
        default=None,
        help="Historical end date YYYY-MM-DD IST. Default uses last completed 15m bar.",
    )
    p.add_argument(
        "--intraday-ts",
        choices=["end", "start"],
        default=DEFAULT_INTRADAY_TIMESTAMP,
        help="Store candle timestamp as interval end or start.",
    )
    p.add_argument(
        "--holidays-file",
        type=str,
        default=HOLIDAYS_FILE_DEFAULT,
        help="Holiday CSV/TXT file used by the stock 15m fetcher.",
    )
    p.add_argument(
        "--token-override",
        type=int,
        default=None,
        help="Manual instrument token override.",
    )
    p.add_argument(
        "--no-skip",
        action="store_true",
        help="Fetch even when the target parquet already looks fresh.",
    )
    return p.parse_args()


def _normalize_aliases(raw: str) -> list[str]:
    out: list[str] = []
    for part in str(raw).split(","):
        s = part.strip().upper()
        if s:
            out.append(s)
    return sorted(set(out))


def _resolve_symbol_token(kite, symbol: str, token_override: Optional[int]) -> int:
    if token_override is not None and int(token_override) > 0:
        return int(token_override)

    wanted = str(symbol).strip().upper()
    instruments = pd.DataFrame(kite.instruments("NSE"))
    if instruments.empty:
        raise RuntimeError("kite.instruments('NSE') returned no rows.")

    candidates = instruments[instruments["tradingsymbol"].astype(str).str.upper() == wanted]
    if candidates.empty:
        name_match = instruments[instruments["name"].astype(str).str.upper() == wanted]
        candidates = name_match
    if candidates.empty:
        contains = instruments[instruments["tradingsymbol"].astype(str).str.upper().str.contains(wanted, regex=False)]
        candidates = contains
    if candidates.empty:
        raise RuntimeError(f"Could not resolve NSE token for symbol={wanted}")

    row = candidates.iloc[0]
    return int(row["instrument_token"])


def _compute_start_dt(now_ist: datetime, from_date_raw: Optional[str]):
    if from_date_raw:
        d = pd.to_datetime(from_date_raw, errors="raise").date()
        return IST_TZ.localize(datetime(d.year, d.month, d.day, 0, 0, 0))
    return get_start_date(now_ist)


def _compute_end_dt(now_ist: datetime, holidays: set, to_date_raw: Optional[str], intraday_ts: str):
    if to_date_raw:
        d = pd.to_datetime(to_date_raw, errors="raise").date()
        if intraday_ts.lower() == "end":
            return IST_TZ.localize(datetime(d.year, d.month, d.day, 15, 30, 0))
        return IST_TZ.localize(datetime(d.year, d.month, d.day, 15, 15, 0))
    return expected_last_stamp(now_ist, holidays, intraday_ts)


def _incremental_start(out_path: str, base_start_dt):
    existing_path = _resolve_existing_store_path(out_path)
    if not Path(existing_path).exists():
        return base_start_dt

    last_ts = _read_last_ts_from_store(existing_path)
    if last_ts is None:
        return base_start_dt
    if last_ts.tzinfo is None:
        last_ts = IST_TZ.localize(last_ts)
    else:
        last_ts = last_ts.tz_convert(IST_TZ)

    # Re-fetch a short warmup tail to stabilize indicators near the merge edge.
    warmup = pd.Timedelta(minutes=15 * 400)
    inc = max(base_start_dt, last_ts - warmup)
    return inc


def _normalize_fetch_start(dt_obj, holidays: set):
    dt_obj = pd.Timestamp(dt_obj)
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.tz_localize(IST_TZ)
    else:
        dt_obj = dt_obj.tz_convert(IST_TZ)

    d = dt_obj.date()
    while not _is_trading_day(d, holidays):
        d = d + timedelta(days=1)

    session_start = IST_TZ.localize(datetime(d.year, d.month, d.day, MARKET_OPEN_TIME.hour, MARKET_OPEN_TIME.minute))
    session_end = IST_TZ.localize(datetime(d.year, d.month, d.day, MARKET_CLOSE_TIME.hour, MARKET_CLOSE_TIME.minute))

    if dt_obj < session_start or dt_obj.time() < MARKET_OPEN_TIME:
        return session_start
    if dt_obj >= session_end or dt_obj.time() > MARKET_CLOSE_TIME:
        d = d + timedelta(days=1)
        while not _is_trading_day(d, holidays):
            d = d + timedelta(days=1)
        return IST_TZ.localize(datetime(d.year, d.month, d.day, MARKET_OPEN_TIME.hour, MARKET_OPEN_TIME.minute))
    return _round_down_session_anchored(dt_obj.to_pydatetime(), STEP_MIN)


def _merge_fetch(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return fetched.copy()
    return (
        pd.concat([existing, fetched], ignore_index=True)
        .drop_duplicates(subset="date", keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )


def main() -> int:
    args = parse_args()
    logger = setup_logger()
    now_ist = datetime.now(IST_TZ)
    holidays = _read_holidays(args.holidays_file)
    aliases = _normalize_aliases(args.aliases)
    if not aliases:
        raise RuntimeError("At least one alias is required.")

    logger.info("Output directory: %s", OUT_DIR)
    logger.info("Preferred symbol: %s", args.symbol)
    logger.info("Aliases: %s", ", ".join(aliases))

    kite = setup_kite_session()
    token = _resolve_symbol_token(kite, args.symbol, args.token_override)
    logger.info("Resolved token %s for symbol %s", token, args.symbol.upper())

    start_dt = _compute_start_dt(now_ist, args.from_date)
    end_dt = _compute_end_dt(now_ist, holidays, args.to_date, args.intraday_ts)
    if end_dt <= start_dt:
        logger.info("End cutoff <= start. Nothing to fetch.")
        return 0

    primary_alias = aliases[0]
    primary_out = str(Path(OUT_DIR) / f"{primary_alias}_stocks_indicators_15min.parquet")

    if (not args.no_skip) and ticker_is_fresh(primary_out, now_ist, holidays, args.intraday_ts):
        logger.info("%s already fresh. Skipping fetch.", primary_alias)
        return 0

    inc_start = _normalize_fetch_start(_incremental_start(primary_out, start_dt), holidays)
    logger.info("Fetch window: %s -> %s", inc_start, end_dt)
    existing = _load_existing_ohlc(primary_out, args.intraday_ts)
    fetched = fetch_historical_15min_df(kite, token, inc_start, end_dt, logger, args.intraday_ts)
    if fetched.empty:
        logger.info("No new rows fetched.")
        return 0

    merged = _merge_fetch(existing, fetched)
    merged = _compute_features_15m(merged)
    merged = _downcast_numeric_columns(merged)

    for alias in aliases:
        out_path = str(Path(OUT_DIR) / f"{alias}_stocks_indicators_15min.parquet")
        _finalize_and_save(merged, out_path)
        logger.info("[SAVE] %s | rows=%d | last=%s", out_path, len(merged), merged["date"].iloc[-1])

    logger.info("Done. Stored NIFTY 15m parquet(s) in %s", OUT_DIR)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
