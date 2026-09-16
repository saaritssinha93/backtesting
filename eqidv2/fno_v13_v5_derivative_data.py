"""Fetch and audit derivative market data needed by V13-v5.

This module is deliberately a data-acquisition companion to
``fno_v13_corrected_v5_backtest.py``.  It never places orders and it does not
change the cash-equity signal rules.  It freezes the selected V13-v5 trade
file, archives the current NFO instrument master, maps filled signals to the
same-expiry ATM CE/PE contract, downloads a configurable ATM strike ladder,
downloads active futures, and writes one-lot reference/capital coverage.

Expired options are never silently replaced with a live expiry.  When the
required contract is no longer present in the broker instrument master, the
trade remains in the audit with an explicit unavailable state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

import fno_oi_common as common


DATASET_VERSION = "FNO_V13_V5_DERIVATIVE_MARKET_DATA_V1"
SESSION = "fno_v13_v5_derivative_data"

V13_V5_ROOT = common.FNO_ROOT / "strategy_research" / "v13_corrected_v5"
DEFAULT_TRADES_PATH = (
    V13_V5_ROOT
    / "higher_frequency"
    / "fno_v13_corrected_v5_higher_frequency_trades.csv"
)
DEFAULT_OUTPUT_ROOT = V13_V5_ROOT / "derivative_market_data"
LOCAL_FUTURES_1M_ROOT = common.FNO_ROOT / "raw_contracts_1m_hist"
LOCAL_FUTURES_5M_ROOT = common.FNO_ROOT / "raw_contracts_5m"

MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)

REQUIRED_TRADE_COLUMNS = {
    "sid",
    "day",
    "tradingsymbol",
    "side",
    "filled",
    "entry_price",
    "entry_ts",
    "exit_ts",
    "futures_tradingsymbol",
    "futures_instrument_token",
    "contract_month",
    "expiry_date",
    "profile",
    "strategy_version",
}

MASTER_COLUMNS = (
    "instrument_token",
    "exchange_token",
    "tradingsymbol",
    "name",
    "last_price",
    "expiry",
    "strike",
    "tick_size",
    "lot_size",
    "instrument_type",
    "segment",
    "exchange",
)


@dataclass
class ClientRuntime:
    app_name: str
    client: Any
    last_call_at: float = 0.0


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _as_ist(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        return pd.NaT
    if stamp.tzinfo is None:
        return stamp.tz_localize(common.IST)
    return stamp.tz_convert(common.IST)


def _filled_mask(values: pd.Series) -> pd.Series:
    return values.astype(str).str.strip().str.lower().isin({"true", "1", "yes"})


def load_filled_trades(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"V13-v5 trade file is missing: {path}")
    frame = pd.read_csv(path)
    missing = sorted(REQUIRED_TRADE_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"V13-v5 trade file is missing columns: {', '.join(missing)}")
    frame = frame.loc[_filled_mask(frame["filled"])].copy()
    if frame.empty:
        raise ValueError("V13-v5 trade file contains no filled trades.")

    frame["_trade_day"] = pd.to_datetime(frame["day"], errors="coerce").dt.date
    frame["_entry_ts"] = [_as_ist(value) for value in frame["entry_ts"]]
    frame["_exit_ts"] = [_as_ist(value) for value in frame["exit_ts"]]
    frame["_required_expiry"] = pd.to_datetime(
        frame["expiry_date"], errors="coerce"
    ).dt.normalize()
    frame["_equity_entry_price"] = pd.to_numeric(
        frame["entry_price"], errors="coerce"
    )
    frame["_trade_id"] = [
        f"{day}|{sid}|{symbol}|{pd.Timestamp(stamp).isoformat()}"
        for day, sid, symbol, stamp in zip(
            frame["day"], frame["sid"], frame["tradingsymbol"], frame["_entry_ts"]
        )
    ]
    invalid = frame[
        frame["_trade_day"].isna()
        | frame["_entry_ts"].isna()
        | frame["_exit_ts"].isna()
        | frame["_required_expiry"].isna()
        | frame["_equity_entry_price"].isna()
    ]
    if not invalid.empty:
        raise ValueError(
            "Filled V13-v5 rows contain invalid date/time/price fields: "
            f"{invalid['sid'].head(10).tolist()}"
        )
    if frame["_trade_id"].duplicated().any():
        raise ValueError("V13-v5 filled trade identifiers are not unique.")
    return frame.reset_index(drop=True)


def normalize_nfo_master(
    records: Iterable[Mapping[str, Any]], *, master_date: date
) -> pd.DataFrame:
    frame = pd.DataFrame(list(records))
    if frame.empty:
        raise ValueError("Kite returned an empty NFO instrument master.")
    for column in MASTER_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA
    frame = frame.loc[:, list(MASTER_COLUMNS)].copy()
    for column in ("tradingsymbol", "name", "instrument_type", "segment", "exchange"):
        frame[column] = (
            frame[column].astype("string").fillna("").str.upper().str.strip()
        )
    frame["expiry"] = pd.to_datetime(frame["expiry"], errors="coerce").dt.normalize()
    for column in (
        "instrument_token",
        "exchange_token",
        "last_price",
        "strike",
        "tick_size",
        "lot_size",
    ):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.loc[
        frame["exchange"].eq("NFO")
        & frame["instrument_type"].isin({"FUT", "CE", "PE"})
        & frame["expiry"].notna()
        & frame["instrument_token"].notna()
        & frame["tradingsymbol"].ne("")
    ].copy()
    if frame.empty:
        raise ValueError("No usable NFO FUT/CE/PE rows remained after validation.")
    frame["instrument_token"] = frame["instrument_token"].astype("int64")
    frame["exchange_token"] = frame["exchange_token"].astype("Int64")
    frame["lot_size"] = frame["lot_size"].astype("Int64")
    frame["underlying"] = frame["name"].astype(str).str.upper().str.strip()
    frame["master_date"] = pd.Timestamp(master_date)
    frame["unique_key"] = "NFO:" + frame["tradingsymbol"].astype(str)
    return frame.sort_values(
        ["instrument_type", "underlying", "expiry", "strike", "tradingsymbol"],
        kind="stable",
    ).reset_index(drop=True)


def build_option_contract_map(
    trades: pd.DataFrame, nfo_master: pd.DataFrame, *, master_date: date
) -> pd.DataFrame:
    options = nfo_master.loc[
        nfo_master["instrument_type"].isin({"CE", "PE"})
        & nfo_master["segment"].eq("NFO-OPT")
    ].copy()
    records: list[dict[str, Any]] = []
    for row in trades.to_dict("records"):
        side = str(row["side"]).upper()
        option_type = "CE" if side == "LONG" else "PE"
        symbol = str(row["tradingsymbol"]).upper()
        required_expiry = pd.Timestamp(row["_required_expiry"]).normalize()
        equity_entry = float(row["_equity_entry_price"])
        candidates = options.loc[
            options["underlying"].eq(symbol)
            & options["expiry"].eq(required_expiry)
            & options["instrument_type"].eq(option_type)
        ].copy()
        base: dict[str, Any] = {
            "trade_id": row["_trade_id"],
            "sid": row["sid"],
            "day": str(row["day"]),
            "profile": str(row["profile"]),
            "strategy_version": str(row["strategy_version"]),
            "equity_symbol": symbol,
            "equity_side": side,
            "equity_entry_price": equity_entry,
            "equity_entry_ts": pd.Timestamp(row["_entry_ts"]),
            "equity_exit_ts": pd.Timestamp(row["_exit_ts"]),
            "contract_month": str(row["contract_month"]),
            "required_option_expiry": required_expiry,
            "required_option_type": option_type,
            "position_side": "BUY",
            "lots": 1,
        }
        if candidates.empty:
            state = (
                "EXPIRED_OPTION_NOT_IN_CURRENT_MASTER"
                if required_expiry.date() < master_date
                else "NO_MATCHING_LIVE_OPTION_CONTRACT"
            )
            records.append(
                {
                    **base,
                    "mapping_status": state,
                    "option_tradingsymbol": "",
                    "option_instrument_token": pd.NA,
                    "option_strike": np.nan,
                    "atm_distance": np.nan,
                    "option_tick_size": np.nan,
                    "lot_size": pd.NA,
                    "quantity": pd.NA,
                }
            )
            continue

        candidates["atm_distance"] = (
            pd.to_numeric(candidates["strike"], errors="coerce") - equity_entry
        ).abs()
        # Deterministic tie-break: lower strike, then exchange tradingsymbol.
        selected = candidates.sort_values(
            ["atm_distance", "strike", "tradingsymbol"], kind="stable"
        ).iloc[0]
        lot_size = int(selected["lot_size"])
        records.append(
            {
                **base,
                "mapping_status": "MAPPED_ATM",
                "option_tradingsymbol": str(selected["tradingsymbol"]),
                "option_instrument_token": int(selected["instrument_token"]),
                "option_strike": float(selected["strike"]),
                "atm_distance": float(selected["atm_distance"]),
                "option_tick_size": float(selected["tick_size"]),
                "lot_size": lot_size,
                "quantity": lot_size,
            }
        )
    return pd.DataFrame(records)


def build_option_fetch_plan(
    contract_map: pd.DataFrame,
    nfo_master: pd.DataFrame,
    *,
    strike_window: int,
) -> pd.DataFrame:
    options = nfo_master.loc[
        nfo_master["instrument_type"].isin({"CE", "PE"})
        & nfo_master["segment"].eq("NFO-OPT")
    ].copy()
    plan: dict[int, dict[str, Any]] = {}
    for mapping in contract_map.loc[
        contract_map["mapping_status"].eq("MAPPED_ATM")
    ].itertuples(index=False):
        candidates = options.loc[
            options["underlying"].eq(str(mapping.equity_symbol))
            & options["expiry"].eq(pd.Timestamp(mapping.required_option_expiry))
            & options["instrument_type"].eq(str(mapping.required_option_type))
        ].sort_values(["strike", "tradingsymbol"], kind="stable")
        strikes = sorted(pd.to_numeric(candidates["strike"], errors="coerce").dropna().unique())
        primary_strike = float(mapping.option_strike)
        primary_index = strikes.index(primary_strike)
        low = max(0, primary_index - max(0, int(strike_window)))
        high = min(len(strikes), primary_index + max(0, int(strike_window)) + 1)
        selected_strikes = set(strikes[low:high])
        selected = candidates.loc[candidates["strike"].isin(selected_strikes)]
        for candidate in selected.itertuples(index=False):
            token = int(candidate.instrument_token)
            item = plan.setdefault(
                token,
                {
                    "instrument_kind": "OPTION",
                    "underlying": str(candidate.underlying),
                    "tradingsymbol": str(candidate.tradingsymbol),
                    "instrument_token": token,
                    "expiry": pd.Timestamp(candidate.expiry),
                    "strike": float(candidate.strike),
                    "instrument_type": str(candidate.instrument_type),
                    "lot_size": int(candidate.lot_size),
                    "tick_size": float(candidate.tick_size),
                    "required_days": set(),
                    "trade_ids": set(),
                    "primary_trade_ids": set(),
                },
            )
            item["required_days"].add(str(mapping.day))
            item["trade_ids"].add(str(mapping.trade_id))
            if math.isclose(float(candidate.strike), primary_strike):
                item["primary_trade_ids"].add(str(mapping.trade_id))

    rows: list[dict[str, Any]] = []
    for item in plan.values():
        days = sorted(item.pop("required_days"))
        trade_ids = sorted(item.pop("trade_ids"))
        primary_ids = sorted(item.pop("primary_trade_ids"))
        rows.append(
            {
                **item,
                "first_required_day": days[0],
                "last_required_day": days[-1],
                "required_session_count": len(days),
                "required_sessions": "|".join(days),
                "linked_trade_count": len(trade_ids),
                "primary_trade_count": len(primary_ids),
            }
        )
    columns = [
        "instrument_kind",
        "underlying",
        "tradingsymbol",
        "instrument_token",
        "expiry",
        "strike",
        "instrument_type",
        "lot_size",
        "tick_size",
        "first_required_day",
        "last_required_day",
        "required_session_count",
        "required_sessions",
        "linked_trade_count",
        "primary_trade_count",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows).sort_values(
        ["underlying", "instrument_type", "expiry", "strike"], kind="stable"
    ).reset_index(drop=True)


def build_active_futures_fetch_plan(
    trades: pd.DataFrame, nfo_master: pd.DataFrame
) -> pd.DataFrame:
    futures = nfo_master.loc[
        nfo_master["instrument_type"].eq("FUT")
        & nfo_master["segment"].eq("NFO-FUT")
    ].drop_duplicates("tradingsymbol", keep="last")
    indexed = futures.set_index("tradingsymbol", drop=False)
    rows: list[dict[str, Any]] = []
    for contract, group in trades.groupby("futures_tradingsymbol", sort=True):
        symbol = str(contract).upper()
        if symbol not in indexed.index:
            continue
        candidate = indexed.loc[symbol]
        if isinstance(candidate, pd.DataFrame):
            candidate = candidate.iloc[0]
        days = sorted(str(value) for value in group["day"].unique())
        rows.append(
            {
                "instrument_kind": "FUTURE",
                "underlying": str(candidate["underlying"]),
                "tradingsymbol": symbol,
                "instrument_token": int(candidate["instrument_token"]),
                "expiry": pd.Timestamp(candidate["expiry"]),
                "strike": 0.0,
                "instrument_type": "FUT",
                "lot_size": int(candidate["lot_size"]),
                "tick_size": float(candidate["tick_size"]),
                "first_required_day": days[0],
                "last_required_day": days[-1],
                "required_session_count": len(days),
                "required_sessions": "|".join(days),
                "linked_trade_count": int(len(group)),
                "primary_trade_count": int(len(group)),
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=[
                "instrument_kind",
                "underlying",
                "tradingsymbol",
                "instrument_token",
                "expiry",
                "strike",
                "instrument_type",
                "lot_size",
                "tick_size",
                "first_required_day",
                "last_required_day",
                "required_session_count",
                "required_sessions",
                "linked_trade_count",
                "primary_trade_count",
            ]
        )
    return pd.DataFrame(rows).sort_values("tradingsymbol").reset_index(drop=True)


def connect_and_download_master(
    *, max_apps: int, timeout_sec: float
) -> tuple[pd.DataFrame, list[ClientRuntime], str]:
    credentials = common.discover_kite_credentials(max_apps=max_apps)
    runtimes = [
        ClientRuntime(
            app_name=credential.app_name,
            client=common.make_kite_client(credential, timeout_sec=timeout_sec),
        )
        for credential in credentials
    ]
    failures: list[str] = []
    for runtime in runtimes:
        try:
            records = runtime.client.instruments("NFO")
            if records:
                return (
                    normalize_nfo_master(records, master_date=common.now_ist().date()),
                    runtimes,
                    runtime.app_name,
                )
            failures.append(f"{runtime.app_name}:empty")
        except Exception as exc:
            failures.append(f"{runtime.app_name}:{type(exc).__name__}:{str(exc)[:120]}")
    raise RuntimeError("Unable to download current NFO master: " + " | ".join(failures))


def normalize_minute_candles(
    records: Iterable[Mapping[str, Any]], contract: Mapping[str, Any], *, fetched_at: datetime
) -> pd.DataFrame:
    frame = pd.DataFrame(list(records))
    columns = [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "oi",
        "tradingsymbol",
        "instrument_token",
        "underlying",
        "expiry",
        "strike",
        "instrument_type",
        "lot_size",
        "tick_size",
        "fetch_timestamp_ist",
        "dataset_version",
    ]
    if frame.empty or "date" not in frame.columns:
        return pd.DataFrame(columns=columns)
    frame["timestamp"] = common._to_ist(frame["date"])
    for column in ("open", "high", "low", "close", "volume", "oi"):
        if column not in frame.columns:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.loc[
        frame["timestamp"].notna()
        & frame["timestamp"].dt.time.between(MARKET_OPEN, MARKET_CLOSE, inclusive="both")
        & frame[["open", "high", "low", "close"]].notna().all(axis=1)
    ].copy()
    frame["tradingsymbol"] = str(contract["tradingsymbol"])
    frame["instrument_token"] = int(contract["instrument_token"])
    frame["underlying"] = str(contract["underlying"])
    frame["expiry"] = pd.Timestamp(contract["expiry"])
    frame["strike"] = float(contract.get("strike", 0.0))
    frame["instrument_type"] = str(contract["instrument_type"])
    frame["lot_size"] = int(contract["lot_size"])
    frame["tick_size"] = float(contract["tick_size"])
    frame["fetch_timestamp_ist"] = fetched_at
    frame["dataset_version"] = DATASET_VERSION
    return frame.loc[:, columns].sort_values("timestamp").drop_duplicates(
        "timestamp", keep="last"
    ).reset_index(drop=True)


def _paced_history(
    runtimes: list[ClientRuntime],
    *,
    preferred_index: int,
    token: int,
    start: datetime,
    end: datetime,
    request_interval_sec: float,
    max_retries: int,
) -> tuple[list[dict[str, Any]], str]:
    failures: list[str] = []
    attempts = max(1, int(max_retries))
    for attempt in range(attempts):
        runtime = runtimes[(preferred_index + attempt) % len(runtimes)]
        wait = max(0.0, float(request_interval_sec)) - (
            time.monotonic() - runtime.last_call_at
        )
        if wait > 0:
            time.sleep(wait)
        try:
            runtime.last_call_at = time.monotonic()
            records = runtime.client.historical_data(
                int(token), start, end, "minute", continuous=False, oi=True
            )
            return list(records), runtime.app_name
        except Exception as exc:
            failures.append(f"{runtime.app_name}:{type(exc).__name__}:{str(exc)[:100]}")
            message = str(exc).lower()
            if "429" in message or "rate limit" in message or "too many" in message:
                time.sleep(min(8.0, 2.0 ** (attempt + 1)))
            elif attempt + 1 < attempts:
                time.sleep(min(3.0, 0.5 * (attempt + 1)))
    raise RuntimeError(" | ".join(failures))


def _contract_path(root: Path, symbol: str) -> Path:
    return root / f"{common.safe_contract_stem(symbol)}_1minute.parquet"


def _merge_candles(path: Path, incoming: pd.DataFrame) -> pd.DataFrame:
    frames = [incoming]
    if path.exists():
        try:
            existing = pd.read_parquet(path)
            if not existing.empty:
                frames.insert(0, existing)
        except Exception:
            pass
    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined["timestamp"] = common._to_ist(combined["timestamp"])
    combined = combined.loc[combined["timestamp"].notna()].copy()
    combined = combined.sort_values("timestamp", kind="stable").drop_duplicates(
        "timestamp", keep="last"
    ).reset_index(drop=True)
    common.atomic_write_parquet(combined, path)
    return combined


def fetch_plan(
    plan: pd.DataFrame,
    runtimes: list[ClientRuntime],
    *,
    output_dir: Path,
    request_interval_sec: float,
    max_retries: int,
) -> pd.DataFrame:
    outcomes: list[dict[str, Any]] = []
    total = int(len(plan))
    for position, contract in enumerate(plan.to_dict("records"), start=1):
        symbol = str(contract["tradingsymbol"])
        path = _contract_path(output_dir, symbol)
        before_rows = 0
        if path.exists():
            try:
                before_rows = len(pd.read_parquet(path, columns=["timestamp"]))
            except Exception:
                before_rows = 0
        start = datetime.combine(
            pd.Timestamp(contract["first_required_day"]).date(), MARKET_OPEN, common.IST
        )
        end = datetime.combine(
            pd.Timestamp(contract["last_required_day"]).date(), MARKET_CLOSE, common.IST
        )
        started = time.monotonic()
        try:
            records, app_name = _paced_history(
                runtimes,
                preferred_index=position - 1,
                token=int(contract["instrument_token"]),
                start=start,
                end=end,
                request_interval_sec=request_interval_sec,
                max_retries=max_retries,
            )
            incoming = normalize_minute_candles(
                records, contract, fetched_at=common.now_ist()
            )
            if incoming.empty:
                state = "NO_CANDLE"
                combined = incoming
            else:
                combined = _merge_candles(path, incoming)
                state = "WRITTEN"
            outcome = {
                **contract,
                "fetch_state": state,
                "source_app": app_name,
                "rows_returned": int(len(incoming)),
                "rows_added": max(0, int(len(combined)) - before_rows),
                "total_rows": int(len(combined)),
                "first_bar": str(combined["timestamp"].min()) if not combined.empty else "",
                "last_bar": str(combined["timestamp"].max()) if not combined.empty else "",
                "file_path": str(path.resolve()),
                "elapsed_sec": round(time.monotonic() - started, 3),
                "error": "",
            }
        except Exception as exc:
            outcome = {
                **contract,
                "fetch_state": "FAILED",
                "source_app": "",
                "rows_returned": 0,
                "rows_added": 0,
                "total_rows": before_rows,
                "first_bar": "",
                "last_bar": "",
                "file_path": str(path.resolve()),
                "elapsed_sec": round(time.monotonic() - started, 3),
                "error": f"{type(exc).__name__}: {exc}",
            }
        outcomes.append(outcome)
        if position % 10 == 0 or position == total:
            print(f"[FETCH] {position}/{total} {symbol} -> {outcome['fetch_state']}", flush=True)
            common.publish_heartbeat(
                SESSION,
                "RUNNING",
                progress=f"{position}/{total}",
                contract=symbol,
                fetch_state=outcome["fetch_state"],
            )
    return pd.DataFrame(outcomes)


def _read_minute_file(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(path)
    if frame.empty:
        return frame
    if "timestamp" in frame.columns:
        source = "timestamp"
    elif "date" in frame.columns:
        source = "date"
    elif "ts" in frame.columns:
        source = "ts"
    else:
        return pd.DataFrame()
    frame = frame.copy()
    frame["_timestamp"] = common._to_ist(frame[source])
    for column in ("open", "high", "low", "close", "volume", "oi"):
        if column not in frame.columns:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame.loc[frame["_timestamp"].notna()].sort_values("_timestamp").drop_duplicates(
        "_timestamp", keep="last"
    ).reset_index(drop=True)


def _first_bar_at_or_after(
    frame: pd.DataFrame, timestamp: pd.Timestamp
) -> tuple[pd.Series | None, float | None]:
    if frame.empty:
        return None, None
    stamp = _as_ist(timestamp)
    candidates = frame.loc[
        frame["_timestamp"].dt.date.eq(stamp.date())
        & frame["_timestamp"].ge(stamp)
    ]
    if candidates.empty:
        return None, None
    row = candidates.iloc[0]
    delay = (pd.Timestamp(row["_timestamp"]) - stamp).total_seconds() / 60.0
    return row, float(delay)


def _first_traded_bar_at_or_after(
    frame: pd.DataFrame, timestamp: pd.Timestamp
) -> tuple[pd.Series | None, float | None]:
    if frame.empty:
        return None, None
    traded = frame.loc[pd.to_numeric(frame["volume"], errors="coerce").fillna(0).gt(0)]
    return _first_bar_at_or_after(traded, timestamp)


def enrich_option_trades(
    contract_map: pd.DataFrame,
    *,
    option_dir: Path,
    capital_rupees: float,
    max_reference_delay_minutes: float,
) -> pd.DataFrame:
    cache: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    for mapping in contract_map.to_dict("records"):
        result = dict(mapping)
        result.update(
            {
                "coverage_state": mapping["mapping_status"],
                "reference_entry_rule": (
                    "FIRST_NONZERO_VOLUME_OPTION_1M_OPEN_AT_OR_AFTER_"
                    "EQUITY_ENTRY_PLUS_1M"
                ),
                "reference_exit_rule": (
                    "FIRST_NONZERO_VOLUME_OPTION_1M_OPEN_AT_OR_AFTER_"
                    "EQUITY_EXIT_PLUS_1M"
                ),
                "maximum_reference_delay_minutes": max_reference_delay_minutes,
                "reference_entry_bar": pd.NaT,
                "reference_entry_delay_min": np.nan,
                "reference_entry_premium": np.nan,
                "entry_bar_volume": np.nan,
                "entry_bar_oi": np.nan,
                "one_lot_premium_outlay_rupees": np.nan,
                "capital_input_rupees": capital_rupees if capital_rupees > 0 else np.nan,
                "capital_eligible_one_lot": pd.NA,
                "reference_exit_bar": pd.NaT,
                "reference_exit_delay_min": np.nan,
                "reference_exit_premium": np.nan,
                "reference_gross_pnl_rupees": np.nan,
                "reference_gross_return_pct": np.nan,
            }
        )
        if mapping["mapping_status"] != "MAPPED_ATM":
            rows.append(result)
            continue
        symbol = str(mapping["option_tradingsymbol"])
        if symbol not in cache:
            cache[symbol] = _read_minute_file(_contract_path(option_dir, symbol))
        candles = cache[symbol]
        if candles.empty:
            result["coverage_state"] = "OPTION_CANDLE_FILE_MISSING_OR_EMPTY"
            rows.append(result)
            continue
        entry_at = _as_ist(mapping["equity_entry_ts"]) + pd.Timedelta(minutes=1)
        exit_at = _as_ist(mapping["equity_exit_ts"]) + pd.Timedelta(minutes=1)
        entry_bar, entry_delay = _first_traded_bar_at_or_after(candles, entry_at)
        exit_bar, exit_delay = _first_traded_bar_at_or_after(candles, exit_at)
        if entry_bar is None:
            result["coverage_state"] = "NO_TRADED_REFERENCE_ENTRY_BAR"
            rows.append(result)
            continue
        premium = float(entry_bar["open"])
        lot_size = int(mapping["lot_size"])
        outlay = premium * lot_size
        result.update(
            {
                "coverage_state": "PENDING_LIQUIDITY_VALIDATION",
                "reference_entry_bar": entry_bar["_timestamp"],
                "reference_entry_delay_min": entry_delay,
                "reference_entry_premium": premium,
                "entry_bar_volume": float(entry_bar["volume"]),
                "entry_bar_oi": float(entry_bar["oi"]),
                "one_lot_premium_outlay_rupees": outlay,
                "capital_eligible_one_lot": (
                    bool(outlay <= capital_rupees) if capital_rupees > 0 else pd.NA
                ),
            }
        )
        if entry_delay is not None and entry_delay > max_reference_delay_minutes:
            result["coverage_state"] = "ENTRY_LIQUIDITY_DELAY_EXCEEDS_LIMIT"
        elif exit_bar is None:
            result["coverage_state"] = "NO_TRADED_REFERENCE_EXIT_BAR"
        elif exit_delay is not None and exit_delay > max_reference_delay_minutes:
            result["coverage_state"] = "EXIT_LIQUIDITY_DELAY_EXCEEDS_LIMIT"
        else:
            result["coverage_state"] = "READY"
        if exit_bar is not None:
            exit_premium = float(exit_bar["open"])
            result.update(
                {
                    "reference_exit_bar": exit_bar["_timestamp"],
                    "reference_exit_delay_min": exit_delay,
                    "reference_exit_premium": exit_premium,
                    "reference_gross_pnl_rupees": (exit_premium - premium) * lot_size,
                    "reference_gross_return_pct": (
                        (exit_premium / premium - 1.0) * 100.0 if premium > 0 else np.nan
                    ),
                }
            )
        rows.append(result)
    return pd.DataFrame(rows)


def _futures_registry() -> pd.DataFrame:
    if not common.CONTRACT_REGISTRY_PATH.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(common.CONTRACT_REGISTRY_PATH)
    if "tradingsymbol" not in frame.columns:
        return pd.DataFrame()
    frame = frame.copy()
    frame["tradingsymbol"] = frame["tradingsymbol"].astype(str).str.upper().str.strip()
    return frame.drop_duplicates("tradingsymbol", keep="last")


def _combined_future_candles(package_path: Path, local_path: Path) -> pd.DataFrame:
    frames = []
    for path in (local_path, package_path):
        try:
            frame = _read_minute_file(path)
        except Exception:
            frame = pd.DataFrame()
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True, sort=False)
    return combined.sort_values("_timestamp", kind="stable").drop_duplicates(
        "_timestamp", keep="last"
    ).reset_index(drop=True)


def audit_futures_trades(
    trades: pd.DataFrame,
    nfo_master: pd.DataFrame,
    *,
    futures_dir: Path,
    capital_rupees: float,
    futures_margin_rate: float,
) -> pd.DataFrame:
    current = nfo_master.loc[nfo_master["instrument_type"].eq("FUT")].drop_duplicates(
        "tradingsymbol", keep="last"
    )
    current_by_symbol = current.set_index("tradingsymbol", drop=False)
    registry = _futures_registry()
    registry_by_symbol = registry.set_index("tradingsymbol", drop=False) if not registry.empty else None
    cache: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, Any]] = []
    for trade in trades.to_dict("records"):
        symbol = str(trade["futures_tradingsymbol"]).upper()
        metadata = None
        metadata_source = ""
        if symbol in current_by_symbol.index:
            metadata = current_by_symbol.loc[symbol]
            metadata_source = "CURRENT_NFO_MASTER"
        elif registry_by_symbol is not None and symbol in registry_by_symbol.index:
            metadata = registry_by_symbol.loc[symbol]
            metadata_source = "RETAINED_FUTURES_REGISTRY"
        if isinstance(metadata, pd.DataFrame):
            metadata = metadata.iloc[-1]
        lot_size = int(metadata["lot_size"]) if metadata is not None and pd.notna(metadata.get("lot_size")) else None
        package_path = _contract_path(futures_dir, symbol)
        local_path = LOCAL_FUTURES_1M_ROOT / f"{common.safe_contract_stem(symbol)}_1minute.parquet"
        if symbol not in cache:
            cache[symbol] = _combined_future_candles(package_path, local_path)
        candles = cache[symbol]
        result: dict[str, Any] = {
            "trade_id": trade["_trade_id"],
            "sid": trade["sid"],
            "day": str(trade["day"]),
            "profile": str(trade["profile"]),
            "strategy_version": str(trade["strategy_version"]),
            "equity_symbol": str(trade["tradingsymbol"]),
            "equity_side": str(trade["side"]),
            "equity_entry_ts": trade["_entry_ts"],
            "equity_exit_ts": trade["_exit_ts"],
            "futures_tradingsymbol": symbol,
            "futures_instrument_token": int(trade["futures_instrument_token"]),
            "metadata_source": metadata_source or "MISSING",
            "expiry": pd.Timestamp(metadata["expiry"]) if metadata is not None and "expiry" in metadata else trade["_required_expiry"],
            "lot_size": lot_size if lot_size is not None else pd.NA,
            "quantity": lot_size if lot_size is not None else pd.NA,
            "lots": 1,
            "coverage_state": "MINUTE_CANDLE_FILE_MISSING_OR_EMPTY",
            "minute_package_path": str(package_path.resolve()),
            "retained_local_1m_path": str(local_path.resolve()),
            "retained_local_5m_path": str(
                (LOCAL_FUTURES_5M_ROOT / f"{common.safe_contract_stem(symbol)}_5minute.parquet").resolve()
            ),
            "reference_entry_rule": "FIRST_FUTURE_1M_OPEN_AT_OR_AFTER_EQUITY_ENTRY_PLUS_1M",
            "reference_exit_rule": "FIRST_FUTURE_1M_OPEN_AT_OR_AFTER_EQUITY_EXIT_PLUS_1M",
            "reference_entry_bar": pd.NaT,
            "reference_entry_delay_min": np.nan,
            "reference_entry_price": np.nan,
            "one_lot_notional_rupees": np.nan,
            "futures_margin_rate_input": futures_margin_rate if futures_margin_rate > 0 else np.nan,
            "estimated_margin_rupees": np.nan,
            "capital_input_rupees": capital_rupees if capital_rupees > 0 else np.nan,
            "capital_eligible_one_lot": pd.NA,
            "reference_exit_bar": pd.NaT,
            "reference_exit_delay_min": np.nan,
            "reference_exit_price": np.nan,
            "reference_gross_pnl_rupees": np.nan,
            "reference_gross_return_on_notional_pct": np.nan,
        }
        if candles.empty or lot_size is None:
            if lot_size is None:
                result["coverage_state"] = "FUTURES_LOT_SIZE_MISSING"
            rows.append(result)
            continue
        entry_at = _as_ist(trade["_entry_ts"]) + pd.Timedelta(minutes=1)
        exit_at = _as_ist(trade["_exit_ts"]) + pd.Timedelta(minutes=1)
        entry_bar, entry_delay = _first_bar_at_or_after(candles, entry_at)
        exit_bar, exit_delay = _first_bar_at_or_after(candles, exit_at)
        if entry_bar is None:
            result["coverage_state"] = "REFERENCE_ENTRY_CANDLE_MISSING"
            rows.append(result)
            continue
        entry_price = float(entry_bar["open"])
        notional = entry_price * lot_size
        margin = notional * futures_margin_rate if futures_margin_rate > 0 else np.nan
        result.update(
            {
                "coverage_state": "READY" if exit_bar is not None else "REFERENCE_EXIT_CANDLE_MISSING",
                "reference_entry_bar": entry_bar["_timestamp"],
                "reference_entry_delay_min": entry_delay,
                "reference_entry_price": entry_price,
                "one_lot_notional_rupees": notional,
                "estimated_margin_rupees": margin,
                "capital_eligible_one_lot": (
                    bool(margin <= capital_rupees)
                    if capital_rupees > 0 and futures_margin_rate > 0
                    else pd.NA
                ),
            }
        )
        if exit_bar is not None:
            exit_price = float(exit_bar["open"])
            direction = 1.0 if str(trade["side"]).upper() == "LONG" else -1.0
            pnl = direction * (exit_price - entry_price) * lot_size
            result.update(
                {
                    "reference_exit_bar": exit_bar["_timestamp"],
                    "reference_exit_delay_min": exit_delay,
                    "reference_exit_price": exit_price,
                    "reference_gross_pnl_rupees": pnl,
                    "reference_gross_return_on_notional_pct": (
                        pnl / notional * 100.0 if notional > 0 else np.nan
                    ),
                }
            )
        rows.append(result)
    return pd.DataFrame(rows)


def _state_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if frame.empty or column not in frame.columns:
        return {}
    return {str(key): int(value) for key, value in frame[column].value_counts(dropna=False).items()}


def peak_option_capital(option_coverage: pd.DataFrame) -> tuple[float, int, str]:
    if option_coverage.empty:
        return 0.0, 0, ""
    ready = option_coverage.loc[option_coverage["coverage_state"].eq("READY")].copy()
    if ready.empty:
        return 0.0, 0, ""
    events: list[tuple[pd.Timestamp, int, float, int]] = []
    for row in ready.to_dict("records"):
        entry = pd.Timestamp(row["reference_entry_bar"])
        exit_stamp = pd.Timestamp(row["reference_exit_bar"])
        outlay = float(row["one_lot_premium_outlay_rupees"])
        # Conservative same-timestamp ordering: allocate a new entry before
        # releasing an exiting trade's cash.
        events.append((entry, 0, outlay, 1))
        events.append((exit_stamp, 1, -outlay, -1))
    cash = 0.0
    positions = 0
    peak_cash = 0.0
    peak_positions = 0
    peak_stamp: pd.Timestamp | None = None
    for stamp, _, cash_delta, position_delta in sorted(events):
        cash += cash_delta
        positions += position_delta
        if cash > peak_cash:
            peak_cash = cash
            peak_stamp = stamp
        peak_positions = max(peak_positions, positions)
    return peak_cash, peak_positions, str(peak_stamp) if peak_stamp is not None else ""


def build_data_file_inventory(
    *, option_dir: Path, futures_dir: Path
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for instrument_kind, root in (("OPTION", option_dir), ("FUTURE", futures_dir)):
        for path in sorted(root.glob("*_1minute.parquet")):
            try:
                frame = pd.read_parquet(path)
                if "timestamp" not in frame.columns:
                    raise ValueError("timestamp column missing")
                timestamps = common._to_ist(frame["timestamp"])
                for column in ("open", "high", "low", "close", "volume", "oi"):
                    if column not in frame.columns:
                        frame[column] = np.nan
                    frame[column] = pd.to_numeric(frame[column], errors="coerce")
                duplicate_timestamps = int(timestamps.duplicated().sum())
                invalid_ohlc = int(
                    (
                        frame[["open", "high", "low", "close"]].isna().any(axis=1)
                        | frame["high"].lt(frame[["open", "close"]].max(axis=1))
                        | frame["low"].gt(frame[["open", "close"]].min(axis=1))
                        | frame["high"].lt(frame["low"])
                        | frame[["open", "high", "low", "close"]].le(0).any(axis=1)
                    ).sum()
                )
                negative_volume = int(frame["volume"].lt(0).sum())
                negative_oi = int(frame["oi"].lt(0).sum())
                symbol = (
                    str(frame["tradingsymbol"].dropna().iloc[0])
                    if "tradingsymbol" in frame.columns and frame["tradingsymbol"].notna().any()
                    else path.stem.removesuffix("_1minute")
                )
                qa_state = (
                    "PASS"
                    if not any(
                        (duplicate_timestamps, invalid_ohlc, negative_volume, negative_oi)
                    )
                    else "FAIL"
                )
                rows.append(
                    {
                        "instrument_kind": instrument_kind,
                        "tradingsymbol": symbol,
                        "file_path": str(path.resolve()),
                        "file_size_bytes": path.stat().st_size,
                        "sha256": sha256_file(path),
                        "rows": int(len(frame)),
                        "first_bar": str(timestamps.min()) if not frame.empty else "",
                        "last_bar": str(timestamps.max()) if not frame.empty else "",
                        "duplicate_timestamps": duplicate_timestamps,
                        "invalid_ohlc_rows": invalid_ohlc,
                        "negative_volume_rows": negative_volume,
                        "negative_oi_rows": negative_oi,
                        "qa_state": qa_state,
                        "error": "",
                    }
                )
            except Exception as exc:
                rows.append(
                    {
                        "instrument_kind": instrument_kind,
                        "tradingsymbol": path.stem.removesuffix("_1minute"),
                        "file_path": str(path.resolve()),
                        "file_size_bytes": path.stat().st_size,
                        "sha256": sha256_file(path),
                        "rows": 0,
                        "first_bar": "",
                        "last_bar": "",
                        "duplicate_timestamps": 0,
                        "invalid_ohlc_rows": 0,
                        "negative_volume_rows": 0,
                        "negative_oi_rows": 0,
                        "qa_state": "FAIL",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
    return pd.DataFrame(rows)


def render_report(
    *,
    trades: pd.DataFrame,
    option_map: pd.DataFrame,
    option_plan: pd.DataFrame,
    option_fetch: pd.DataFrame,
    option_coverage: pd.DataFrame,
    futures_plan: pd.DataFrame,
    futures_fetch: pd.DataFrame,
    futures_coverage: pd.DataFrame,
    file_inventory: pd.DataFrame,
    trades_sha256: str,
    master_source_app: str,
    output_root: Path,
    capital_rupees: float,
    futures_margin_rate: float,
    max_reference_delay_minutes: float,
) -> str:
    contract_months = (
        trades.groupby("contract_month").size().sort_index().to_dict()
    )
    option_outlays = pd.to_numeric(
        option_coverage.get("one_lot_premium_outlay_rupees", pd.Series(dtype=float)),
        errors="coerce",
    ).dropna()
    peak_cash, peak_positions, peak_stamp = peak_option_capital(option_coverage)
    lines = [
        "# V13-V5 Derivative Market-Data Fetch Report",
        "",
        f"- Generated: {common.now_ist().isoformat(timespec='seconds')}",
        f"- Dataset version: `{DATASET_VERSION}`",
        f"- Frozen V13-v5 filled rows: {len(trades)}",
        f"- Frozen trade SHA256: `{trades_sha256}`",
        f"- Contract-month fills: `{json.dumps(contract_months, sort_keys=True)}`",
        f"- NFO instrument source: `{master_source_app}`",
        f"- Output root: `{output_root.resolve()}`",
        "",
        "## Option acquisition",
        "",
        f"- ATM mapping states: `{json.dumps(_state_counts(option_map, 'mapping_status'), sort_keys=True)}`",
        f"- Unique option contracts fetched (ATM ladder included): {len(option_plan)}",
        f"- Option fetch states: `{json.dumps(_state_counts(option_fetch, 'fetch_state'), sort_keys=True)}`",
            f"- One-lot reference coverage: `{json.dumps(_state_counts(option_coverage, 'coverage_state'), sort_keys=True)}`",
            f"- Option execution-liquidity proxy: nonzero-volume bar within {max_reference_delay_minutes:g} minutes",
    ]
    if not option_outlays.empty:
        lines.extend(
            [
                f"- One-lot premium outlay range: INR {option_outlays.min():,.2f} to INR {option_outlays.max():,.2f}",
                f"- Median one-lot premium outlay: INR {option_outlays.median():,.2f}",
                f"- Observed peak concurrent option cash for READY references: INR {peak_cash:,.2f}",
                f"- Maximum concurrent READY option positions: {peak_positions} (peak-cash timestamp {peak_stamp})",
            ]
        )
    lines.extend(
        [
            "",
            "## Futures acquisition",
            "",
            f"- Active futures contracts fetched: {len(futures_plan)}",
            f"- Futures fetch states: `{json.dumps(_state_counts(futures_fetch, 'fetch_state'), sort_keys=True)}`",
            f"- One-lot futures reference coverage: `{json.dumps(_state_counts(futures_coverage, 'coverage_state'), sort_keys=True)}`",
            "",
            "## Raw-file integrity",
            "",
            f"- Inventoried parquet files: {len(file_inventory)}",
            f"- QA states: `{json.dumps(_state_counts(file_inventory, 'qa_state'), sort_keys=True)}`",
            "",
            "## Capital inputs",
            "",
            f"- Portfolio capital input: {'NOT_APPLIED' if capital_rupees <= 0 else f'INR {capital_rupees:,.2f}'}",
            f"- Futures margin-rate input: {'NOT_APPLIED' if futures_margin_rate <= 0 else f'{futures_margin_rate:.4f}'}",
            "- Long-option cash required is premium x exchange lot size, before charges.",
            "- Futures notional is price x lot size. Historical margin is not claimed unless a margin-rate input is supplied.",
            "",
            "## Non-negotiable limitations",
            "",
            "- Expired option contracts are not substituted with September contracts. Missing August CE/PE minute data remains explicit.",
            "- Historical minute candles contain OHLCV and OI, not bid/ask spread or market-depth history.",
            "- Reference entry/exit columns are causal next-minute-open marks for data QA; they are not an option-native stop/target backtest.",
            "- One derivative lot cannot be split into V13-v5's 10%/90% scale-out. A later one-lot replay must test a whole-lot exit rule.",
            "- All rupee P&L fields are gross of brokerage, taxes, exchange charges, impact and slippage.",
            "",
            "## Repeatable commands",
            "",
            "```powershell",
            "python fno_v13_v5_derivative_data.py",
            "python fno_v13_v5_derivative_data.py --capital-rupees 500000 --futures-margin-rate 0.20",
            "python -m pytest tests\\test_fno_v13_v5_derivative_data.py -q",
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def market_is_open(moment: datetime) -> bool:
    current = moment.astimezone(common.IST)
    return (
        common.is_trading_day(current.date(), common.load_holidays())
        and MARKET_OPEN <= current.time() <= MARKET_CLOSE
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trades", type=Path, default=DEFAULT_TRADES_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--strike-window",
        type=int,
        default=2,
        help="Fetch this many strikes below and above each ATM strike (default 2).",
    )
    parser.add_argument(
        "--capital-rupees",
        type=float,
        default=0.0,
        help="Optional capital ceiling used only for one-lot eligibility columns.",
    )
    parser.add_argument(
        "--futures-margin-rate",
        type=float,
        default=0.0,
        help="Optional decimal margin estimate, e.g. 0.20. Zero leaves margin unknown.",
    )
    parser.add_argument(
        "--max-reference-delay-minutes",
        type=float,
        default=5.0,
        help=(
            "Maximum wait for a nonzero-volume option bar after the causal "
            "next-minute reference time (default 5)."
        ),
    )
    parser.add_argument("--max-apps", type=int, default=8)
    parser.add_argument("--timeout-sec", type=float, default=15.0)
    parser.add_argument("--request-interval-sec", type=float, default=0.36)
    parser.add_argument("--max-retries", type=int, default=8)
    parser.add_argument("--allow-market-hours", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.strike_window < 0:
        raise ValueError("--strike-window must be non-negative.")
    if args.capital_rupees < 0:
        raise ValueError("--capital-rupees cannot be negative.")
    if not 0.0 <= args.futures_margin_rate <= 1.0:
        raise ValueError("--futures-margin-rate must be between 0 and 1.")
    if args.max_reference_delay_minutes < 0:
        raise ValueError("--max-reference-delay-minutes cannot be negative.")
    if market_is_open(common.now_ist()) and not args.allow_market_hours and not args.dry_run:
        print(
            "[GUARD] Market is open; derivative history fetch is blocked to avoid "
            "competing with live Kite sessions. Re-run after 15:30 IST or pass "
            "--allow-market-hours.",
            flush=True,
        )
        return 0

    started = time.monotonic()
    trades_path = args.trades.resolve()
    output_root = args.output_root.resolve()
    master_dir = output_root / "instrument_master"
    frozen_dir = output_root / "frozen_input"
    option_dir = output_root / "raw_options_1m"
    futures_dir = output_root / "raw_futures_1m"
    audit_dir = output_root / "audit"
    for directory in (master_dir, frozen_dir, option_dir, futures_dir, audit_dir):
        directory.mkdir(parents=True, exist_ok=True)

    trades = load_filled_trades(trades_path)
    trades_sha = sha256_file(trades_path)
    frozen_path = frozen_dir / trades_path.name
    if not args.dry_run:
        shutil.copy2(trades_path, frozen_path)

    common.publish_status(
        SESSION,
        "RUNNING",
        phase="MASTER",
        filled_trades=len(trades),
        source_sha256=trades_sha,
    )
    nfo_master, runtimes, master_source_app = connect_and_download_master(
        max_apps=args.max_apps, timeout_sec=args.timeout_sec
    )
    master_day = common.now_ist().date()
    dated_master_path = master_dir / f"nfo_master_{master_day.isoformat()}.parquet"
    latest_master_path = master_dir / "latest_nfo_master.parquet"
    if not args.dry_run:
        common.atomic_write_parquet(nfo_master, dated_master_path)
        common.atomic_write_parquet(nfo_master, latest_master_path)

    option_map = build_option_contract_map(trades, nfo_master, master_date=master_day)
    option_plan = build_option_fetch_plan(
        option_map, nfo_master, strike_window=args.strike_window
    )
    futures_plan = build_active_futures_fetch_plan(trades, nfo_master)
    print(
        f"[PLAN] filled={len(trades)} option_contracts={len(option_plan)} "
        f"active_futures={len(futures_plan)}",
        flush=True,
    )
    if args.dry_run:
        print(option_map["mapping_status"].value_counts().to_string(), flush=True)
        return 0

    common.atomic_write_csv(option_map, audit_dir / "option_contract_map.csv")
    common.atomic_write_csv(option_plan, audit_dir / "option_fetch_plan.csv")
    common.atomic_write_csv(futures_plan, audit_dir / "active_futures_fetch_plan.csv")

    option_fetch = fetch_plan(
        option_plan,
        runtimes,
        output_dir=option_dir,
        request_interval_sec=args.request_interval_sec,
        max_retries=args.max_retries,
    )
    futures_fetch = fetch_plan(
        futures_plan,
        runtimes,
        output_dir=futures_dir,
        request_interval_sec=args.request_interval_sec,
        max_retries=args.max_retries,
    )
    option_coverage = enrich_option_trades(
        option_map,
        option_dir=option_dir,
        capital_rupees=args.capital_rupees,
        max_reference_delay_minutes=args.max_reference_delay_minutes,
    )
    futures_coverage = audit_futures_trades(
        trades,
        nfo_master,
        futures_dir=futures_dir,
        capital_rupees=args.capital_rupees,
        futures_margin_rate=args.futures_margin_rate,
    )

    option_fetch_path = audit_dir / "option_fetch_outcomes.csv"
    futures_fetch_path = audit_dir / "futures_fetch_outcomes.csv"
    option_coverage_path = audit_dir / "option_trade_coverage_and_capital.csv"
    futures_coverage_path = audit_dir / "futures_trade_coverage_and_capital.csv"
    common.atomic_write_csv(option_fetch, option_fetch_path)
    common.atomic_write_csv(futures_fetch, futures_fetch_path)
    common.atomic_write_csv(option_coverage, option_coverage_path)
    common.atomic_write_csv(futures_coverage, futures_coverage_path)
    file_inventory = build_data_file_inventory(
        option_dir=option_dir, futures_dir=futures_dir
    )
    file_inventory_path = audit_dir / "data_file_inventory.csv"
    common.atomic_write_csv(file_inventory, file_inventory_path)

    report = render_report(
        trades=trades,
        option_map=option_map,
        option_plan=option_plan,
        option_fetch=option_fetch,
        option_coverage=option_coverage,
        futures_plan=futures_plan,
        futures_fetch=futures_fetch,
        futures_coverage=futures_coverage,
        file_inventory=file_inventory,
        trades_sha256=trades_sha,
        master_source_app=master_source_app,
        output_root=output_root,
        capital_rupees=args.capital_rupees,
        futures_margin_rate=args.futures_margin_rate,
        max_reference_delay_minutes=args.max_reference_delay_minutes,
    )
    report_path = output_root / "V13_V5_DERIVATIVE_DATA_FETCH_REPORT.md"
    common.atomic_write_text(report_path, report)

    manifest = {
        "dataset_version": DATASET_VERSION,
        "generated_at_ist": common.now_ist().isoformat(),
        "elapsed_sec": round(time.monotonic() - started, 3),
        "source_trades": str(trades_path),
        "frozen_trades": str(frozen_path.resolve()),
        "source_trades_sha256": trades_sha,
        "filled_trades": int(len(trades)),
        "source_profiles": sorted(trades["profile"].astype(str).unique().tolist()),
        "strategy_versions": sorted(trades["strategy_version"].astype(str).unique().tolist()),
        "nfo_master_source_app": master_source_app,
        "nfo_master_rows": int(len(nfo_master)),
        "nfo_master_sha256": sha256_file(dated_master_path),
        "strike_window": int(args.strike_window),
        "capital_rupees": float(args.capital_rupees),
        "futures_margin_rate": float(args.futures_margin_rate),
        "max_reference_delay_minutes": float(args.max_reference_delay_minutes),
        "option_mapping_states": _state_counts(option_map, "mapping_status"),
        "option_fetch_states": _state_counts(option_fetch, "fetch_state"),
        "option_coverage_states": _state_counts(option_coverage, "coverage_state"),
        "futures_fetch_states": _state_counts(futures_fetch, "fetch_state"),
        "futures_coverage_states": _state_counts(futures_coverage, "coverage_state"),
        "data_file_qa_states": _state_counts(file_inventory, "qa_state"),
        "paths": {
            "report": str(report_path.resolve()),
            "option_map": str((audit_dir / "option_contract_map.csv").resolve()),
            "option_plan": str((audit_dir / "option_fetch_plan.csv").resolve()),
            "option_fetch": str(option_fetch_path.resolve()),
            "option_coverage": str(option_coverage_path.resolve()),
            "futures_fetch": str(futures_fetch_path.resolve()),
            "futures_coverage": str(futures_coverage_path.resolve()),
            "data_file_inventory": str(file_inventory_path.resolve()),
            "nfo_master": str(dated_master_path.resolve()),
        },
        "one_lot_exit_constraint": (
            "ONE_LOT_CANNOT_EXECUTE_10_90_SCALEOUT; TEST WHOLE_LOT EXIT VARIANTS"
        ),
    }
    common.atomic_write_json(output_root / "manifest.json", manifest)
    failed = int((option_fetch.get("fetch_state", pd.Series(dtype=str)) == "FAILED").sum())
    failed += int((futures_fetch.get("fetch_state", pd.Series(dtype=str)) == "FAILED").sum())
    failed += int((file_inventory.get("qa_state", pd.Series(dtype=str)) == "FAIL").sum())
    common.publish_status(
        SESSION,
        "SUCCESS" if failed == 0 else "PARTIAL",
        phase="DONE",
        filled_trades=len(trades),
        option_contracts=len(option_plan),
        active_futures=len(futures_plan),
        failed_fetches=failed,
        report=report_path,
    )
    print(f"[DONE] report={report_path}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
