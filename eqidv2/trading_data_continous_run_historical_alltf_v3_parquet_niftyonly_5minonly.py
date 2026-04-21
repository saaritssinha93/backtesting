# -*- coding: utf-8 -*-
"""
Dedicated 5-minute NIFTY/NIFTYBEES fetcher for the backtest parquet folder.

This is a thin wrapper around the stock-only 5m fetcher so that:
- timestamps are identical
- incremental updates behave the same way
- indicators/parquet layout match the stock dataset
- outputs land in the runtime 5m parquet folder for v7/v15 runners

Examples:
    python trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_5minonly.py
    python trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_5minonly.py --symbol NIFTYBEES --aliases NIFTYBEES
    python trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_5minonly.py --symbol NIFTYBEES --aliases NIFTYBEES,NIFTY50,NIFTY_50,NIFTY
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from trading_data_continous_run_historical_alltf_v3_parquet_stocksonly_5minonly import (
    DEFAULT_INTRADAY_TIMESTAMP,
    HOLIDAYS_FILE_DEFAULT,
    IST_TZ,
    MARKET_CLOSE_TIME,
    MARKET_OPEN_TIME,
    OUT_DIR,
    STEP_MIN,
    _is_trading_day,
    _compute_features_5m,
    _downcast_numeric_columns,
    _finalize_and_save,
    _load_existing_ohlc,
    _read_holidays,
    _read_last_ts_from_store,
    _resolve_existing_store_path,
    _round_down_session_anchored,
    expected_last_stamp,
    fetch_historical_5min_df,
    get_start_date,
    setup_kite_session,
    setup_logger,
    ticker_is_fresh,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=f"Fetch/update 5m NIFTY parquet(s) in {OUT_DIR}."
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
        help="Historical end date YYYY-MM-DD IST. Default uses last completed 5m bar.",
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
        help="Holiday CSV/TXT file used by the stock 5m fetcher.",
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
        return IST_TZ.localize(datetime(d.year, d.month, d.day, 15, 25, 0))
    return expected_last_stamp(now_ist, holidays, intraday_ts)


def _incremental_start(out_path: str, base_start_dt):
    existing_path = _resolve_existing_store_path(out_path)
    if not Path(existing_path).exists():
        return base_start_dt

    try:
        existing = _load_existing_ohlc_raw(out_path)
        if not existing.empty and "date" in existing.columns:
            first_ts = pd.to_datetime(existing["date"], errors="coerce").dropna().min()
            if pd.notna(first_ts):
                first_ts = pd.Timestamp(first_ts)
                if first_ts.tzinfo is None:
                    first_ts = first_ts.tz_localize(IST_TZ)
                else:
                    first_ts = first_ts.tz_convert(IST_TZ)
                base_ts = pd.Timestamp(base_start_dt)
                if base_ts.tzinfo is None:
                    base_ts = base_ts.tz_localize(IST_TZ)
                else:
                    base_ts = base_ts.tz_convert(IST_TZ)
                if first_ts > (base_ts + pd.Timedelta(seconds=1)):
                    return base_ts
    except Exception:
        pass

    last_ts = _read_last_ts_from_store(existing_path)
    if last_ts is None:
        return base_start_dt
    if last_ts.tzinfo is None:
        last_ts = IST_TZ.localize(last_ts)
    else:
        last_ts = last_ts.tz_convert(IST_TZ)

    # Keep the NIFTY guard append-only so it fills only the next missing slot.
    return max(base_start_dt, last_ts + pd.Timedelta(minutes=STEP_MIN))


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


def _nifty_is_exactly_fresh(out_path: str, now_ist: datetime, holidays: set, intraday_ts: str) -> bool:
    existing_path = _resolve_existing_store_path(out_path)
    if not Path(existing_path).exists():
        return False

    last_ts = _read_last_ts_from_store(existing_path)
    if last_ts is None:
        return False

    last_ts = pd.Timestamp(last_ts)
    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize(IST_TZ)
    else:
        last_ts = last_ts.tz_convert(IST_TZ)

    exp_ts = pd.Timestamp(expected_last_stamp(now_ist, holidays, intraday_ts))
    if exp_ts.tzinfo is None:
        exp_ts = exp_ts.tz_localize(IST_TZ)
    else:
        exp_ts = exp_ts.tz_convert(IST_TZ)

    return last_ts >= (exp_ts - pd.Timedelta(seconds=1))


def _nifty_needs_prefix_backfill(out_path: str, base_start_dt) -> bool:
    try:
        existing = _load_existing_ohlc_raw(out_path)
        if existing.empty or "date" not in existing.columns:
            return False
        first_ts = pd.to_datetime(existing["date"], errors="coerce").dropna().min()
        if pd.isna(first_ts):
            return False
        first_ts = pd.Timestamp(first_ts)
        if first_ts.tzinfo is None:
            first_ts = first_ts.tz_localize(IST_TZ)
        else:
            first_ts = first_ts.tz_convert(IST_TZ)
        base_ts = pd.Timestamp(base_start_dt)
        if base_ts.tzinfo is None:
            base_ts = base_ts.tz_localize(IST_TZ)
        else:
            base_ts = base_ts.tz_convert(IST_TZ)
        return bool(first_ts > (base_ts + pd.Timedelta(seconds=1)))
    except Exception:
        return False


def _merge_fetch(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        return fetched.copy()
    return (
        pd.concat([existing, fetched], ignore_index=True)
        .drop_duplicates(subset="date", keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )


def _load_existing_ohlc_raw(out_path: str) -> pd.DataFrame:
    existing_path = _resolve_existing_store_path(out_path)
    if not Path(existing_path).exists():
        return pd.DataFrame()

    keep_cols = ["date", "open", "high", "low", "close", "volume"]
    try:
        if str(existing_path).lower().endswith(".parquet"):
            df = pd.read_parquet(existing_path, columns=keep_cols)
        else:
            df = pd.read_csv(existing_path)
        if df.empty or "date" not in df.columns:
            return pd.DataFrame()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if getattr(df["date"].dt, "tz", None) is None:
            df["date"] = df["date"].dt.tz_localize(IST_TZ)
        else:
            df["date"] = df["date"].dt.tz_convert(IST_TZ)
        keep = [c for c in keep_cols if c in df.columns]
        return df[keep].drop_duplicates(subset="date", keep="last").sort_values("date").reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def _session_open_ts(target_ts: datetime | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(target_ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize(IST_TZ)
    else:
        ts = ts.tz_convert(IST_TZ)
    return pd.Timestamp(
        IST_TZ.localize(datetime(ts.year, ts.month, ts.day, MARKET_OPEN_TIME.hour, MARKET_OPEN_TIME.minute, 0))
    )


def _missing_opening_snapshot(existing: pd.DataFrame, target_ts: datetime | pd.Timestamp) -> bool:
    if existing is None or existing.empty or "date" not in existing.columns:
        return True
    target = pd.Timestamp(target_ts)
    if target.tzinfo is None:
        target = target.tz_localize(IST_TZ)
    else:
        target = target.tz_convert(IST_TZ)
    session_open = _session_open_ts(target)
    today_df = existing[existing["date"].dt.date == target.date()].copy()
    if today_df.empty:
        return True
    return bool((today_df["date"].dt.floor("min") == session_open.floor("min")).sum() == 0)


def _nifty_slot_ready_dir() -> Path:
    """strategy_v2 C1 — resolve the NF slot-ready marker directory."""
    runtime_root = os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2")
    return Path(
        os.getenv("EQIDV2_NIFTY_SLOT_READY_DIR", str(Path(runtime_root) / "nifty_slot_ready_5m"))
    )


def _slot_key_from_bar_end(bar_end_ts, intraday_ts: str) -> str:
    """Slot key for the marker, always keyed to bar-END (= DE slot OPEN)."""
    ts = pd.Timestamp(bar_end_ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize(IST_TZ)
    else:
        ts = ts.tz_convert(IST_TZ)
    if intraday_ts.lower() == "start":
        ts = ts + pd.Timedelta(minutes=STEP_MIN)
    return ts.strftime("%Y%m%d_%H%M")


def _write_nifty_slot_ready_marker(
    slot_end_ts,
    last_bar_ts,
    primary_alias: str,
    out_path: str,
    intraday_ts: str,
    logger,
) -> None:
    """
    strategy_v2 C1 — write an atomic slot-ready marker after a confirmed
    NIFTY fetch. DE's `_wait_for_nifty_slot_ready` blocks on this file and
    aborts the slot with [ABORT] NF_STALE if it never appears. The marker
    key uses bar-END time (= DE slot OPEN convention).
    """
    try:
        marker_dir = _nifty_slot_ready_dir()
        marker_dir.mkdir(parents=True, exist_ok=True)
        slot_key = _slot_key_from_bar_end(slot_end_ts, intraday_ts)
        last_ts = pd.Timestamp(last_bar_ts)
        if last_ts.tzinfo is None:
            last_ts = last_ts.tz_localize(IST_TZ)
        else:
            last_ts = last_ts.tz_convert(IST_TZ)
        slot_end = pd.Timestamp(slot_end_ts)
        if slot_end.tzinfo is None:
            slot_end = slot_end.tz_localize(IST_TZ)
        else:
            slot_end = slot_end.tz_convert(IST_TZ)
        now_ist = datetime.now(IST_TZ)
        payload = {
            "slot_key":         slot_key,
            "slot_bar_end_ist": slot_end.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "last_bar_ist":     last_ts.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "written_at_ist":   now_ist.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "symbol":           primary_alias,
            "parquet_path":     str(out_path),
            "intraday_ts":      intraday_ts,
            "source":           "trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_5minonly.py",
        }
        marker_path = marker_dir / f"nifty_ready_{slot_key}.json"
        tmp_path = marker_path.with_suffix(".json.tmp")
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, sort_keys=True)
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except Exception:
                pass
        os.replace(tmp_path, marker_path)
        logger.info("[SLOT_READY] NF marker written: %s", marker_path)
    except Exception as exc:
        logger.warning("NF slot-ready marker write failed: %s", exc)


def _maybe_write_nf_marker_from_existing(
    existing: pd.DataFrame,
    end_dt,
    primary_alias: str,
    out_path: str,
    intraday_ts: str,
    logger,
) -> None:
    """Write the NF marker using the already-fresh on-disk parquet (skip path)."""
    if existing is None or existing.empty or "date" not in existing.columns:
        return
    try:
        last_bar = pd.to_datetime(existing["date"], errors="coerce").dropna().max()
        if pd.isna(last_bar):
            return
        last_ts = pd.Timestamp(last_bar)
        if last_ts.tzinfo is None:
            last_ts = last_ts.tz_localize(IST_TZ)
        else:
            last_ts = last_ts.tz_convert(IST_TZ)
        exp = pd.Timestamp(end_dt)
        if exp.tzinfo is None:
            exp = exp.tz_localize(IST_TZ)
        else:
            exp = exp.tz_convert(IST_TZ)
        if last_ts < exp - pd.Timedelta(seconds=1):
            return
        _write_nifty_slot_ready_marker(
            slot_end_ts=end_dt,
            last_bar_ts=last_ts,
            primary_alias=primary_alias,
            out_path=out_path,
            intraday_ts=intraday_ts,
            logger=logger,
        )
    except Exception as exc:
        logger.warning("NF slot-ready skip-path marker failed: %s", exc)


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
    primary_out = str(Path(OUT_DIR) / f"{primary_alias}_stocks_indicators_5min.parquet")
    existing = _load_existing_ohlc_raw(primary_out)
    opening_snapshot_missing = _missing_opening_snapshot(existing, end_dt)

    if (
        (not args.no_skip)
        and _nifty_is_exactly_fresh(primary_out, now_ist, holidays, args.intraday_ts)
        and not _nifty_needs_prefix_backfill(primary_out, start_dt)
        and not opening_snapshot_missing
    ):
        logger.info("%s already fresh. Skipping fetch.", primary_alias)
        _maybe_write_nf_marker_from_existing(
            existing=existing,
            end_dt=end_dt,
            primary_alias=primary_alias,
            out_path=primary_out,
            intraday_ts=args.intraday_ts,
            logger=logger,
        )
        return 0

    merged = existing.copy()
    if opening_snapshot_missing:
        session_open = _session_open_ts(end_dt)
        repair_end = min(pd.Timestamp(end_dt), session_open + pd.Timedelta(minutes=STEP_MIN))
        session_open_dt = session_open.to_pydatetime()
        repair_end_dt = repair_end.to_pydatetime()
        logger.warning(
            "Opening snapshot missing for %s on %s. Repairing via intraday_ts=start from %s to %s.",
            primary_alias,
            session_open.date(),
            session_open_dt,
            repair_end_dt,
        )
        repair = fetch_historical_5min_df(kite, token, session_open_dt, repair_end_dt, logger, "start")
        if repair.empty:
            logger.warning("Opening snapshot repair fetch returned no rows.")
        else:
            merged = _merge_fetch(merged, repair)
            logger.info(
                "Opening snapshot repair added %d row(s); first=%s last=%s",
                len(repair),
                repair["date"].min(),
                repair["date"].max(),
            )

    inc_start = _normalize_fetch_start(_incremental_start(primary_out, start_dt), holidays)
    if inc_start >= end_dt:
        inc_start = end_dt - timedelta(minutes=STEP_MIN)
    logger.info("Fetch window: %s -> %s", inc_start, end_dt)
    fetched = fetch_historical_5min_df(kite, token, inc_start, end_dt, logger, args.intraday_ts)
    if fetched.empty and merged.empty:
        logger.info("No new rows fetched.")
        return 0

    if not fetched.empty:
        merged = _merge_fetch(merged, fetched)
    merged = _compute_features_5m(merged)
    merged = _downcast_numeric_columns(merged)

    for alias in aliases:
        out_path = str(Path(OUT_DIR) / f"{alias}_stocks_indicators_5min.parquet")
        _finalize_and_save(merged, out_path)
        logger.info("[SAVE] %s | rows=%d | last=%s", out_path, len(merged), merged["date"].iloc[-1])

    logger.info("Done. Stored NIFTY 5m parquet(s) in %s", OUT_DIR)

    # strategy_v2 C1 — only publish the NF slot-ready marker when the confirmed
    # last bar actually reaches end_dt. A partial fetch must NOT advertise the
    # slot to DE; better for DE to ABORT with NF_STALE than run RS off stale data.
    try:
        if not merged.empty:
            last_bar = merged["date"].iloc[-1]
            last_ts = pd.Timestamp(last_bar)
            if last_ts.tzinfo is None:
                last_ts = last_ts.tz_localize(IST_TZ)
            else:
                last_ts = last_ts.tz_convert(IST_TZ)
            exp_ts = pd.Timestamp(end_dt)
            if exp_ts.tzinfo is None:
                exp_ts = exp_ts.tz_localize(IST_TZ)
            else:
                exp_ts = exp_ts.tz_convert(IST_TZ)
            if last_ts >= exp_ts - pd.Timedelta(seconds=1):
                _write_nifty_slot_ready_marker(
                    slot_end_ts=end_dt,
                    last_bar_ts=last_ts,
                    primary_alias=primary_alias,
                    out_path=primary_out,
                    intraday_ts=args.intraday_ts,
                    logger=logger,
                )
            else:
                logger.warning(
                    "NF slot-ready marker suppressed: last_bar=%s < expected=%s",
                    last_ts, exp_ts,
                )
    except Exception as exc:
        logger.warning("NF slot-ready success-path marker failed: %s", exc)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

