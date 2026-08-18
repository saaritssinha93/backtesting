"""Cash-price/futures-OI data contract shared by FNO V5 backtest and live."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import fno_oi_common as common
from eqidv2_runtime_paths import DATA_1MIN_DIR, DATA_5M_DIR, runtime_dir


DATA_CONTRACT_VERSION = "fno_v5_equity_real_5m_futures_oi_v4"
BACKTEST_EQUITY_5M_CONSTRUCTION = "NSE_EQUITY_1M_CAUSAL_5X_AGGREGATION"
EQUITY_SYMBOL_ALIASES = {
    # LTI Mindtree changed its cash symbol to LTIM while the NFO root remained LTM.
    "LTM": "LTIM",
}
DEFAULT_BACKTEST_EQUITY_5M_DIR = Path(
    os.getenv(
        "EQIDV2_FNO_V5_BACKTEST_EQUITY_5M_DIR",
        str(runtime_dir("stocks_indicators_5min_eq_live2")),
    )
)
DEFAULT_BACKTEST_EQUITY_1M_DIR = Path(
    os.getenv(
        "EQIDV2_FNO_V5_BACKTEST_EQUITY_1M_DIR",
        str(runtime_dir("stocks_indicators_1min_eq")),
    )
)
LIVE_EQUITY_5M_DIR = Path(DATA_5M_DIR)
LIVE_EQUITY_1M_DIR = Path(DATA_1MIN_DIR)
TOKEN_CACHE_PATH = Path(__file__).resolve().with_name("stocks_tokens_cache.json")
EMA_SPANS = (9, 20, 50)


def equity_symbol_for_underlying(underlying: Any) -> str | None:
    symbol = str(underlying or "").strip().upper()
    if not symbol or symbol in common.INDEX_UNDERLYINGS:
        return None
    return EQUITY_SYMBOL_ALIASES.get(symbol, symbol)


def load_equity_token_map(path: Path = TOKEN_CACHE_PATH) -> dict[str, int]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Equity token cache is not an object: {path}")
    result: dict[str, int] = {}
    for raw_symbol, raw_token in payload.items():
        try:
            token = int(raw_token)
        except (TypeError, ValueError):
            continue
        symbol = str(raw_symbol).strip().upper()
        if symbol and token > 0:
            result[symbol] = token
    return result


def attach_equity_instruments(
    universe: pd.DataFrame,
    nse_instruments: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Attach one exact NSE-EQ instrument to every stock future."""
    required = {"underlying", "tradingsymbol", "instrument_token"}
    missing = required - set(universe.columns)
    if missing:
        raise ValueError(f"Futures universe missing columns: {sorted(missing)}")
    nse_required = {"tradingsymbol", "instrument_token"}
    missing = nse_required - set(nse_instruments.columns)
    if missing:
        raise ValueError(f"NSE instrument master missing columns: {sorted(missing)}")

    cash = nse_instruments.copy()
    if "instrument_type" in cash.columns:
        cash = cash.loc[cash["instrument_type"].astype(str).str.upper().eq("EQ")]
    if "exchange" in cash.columns:
        cash = cash.loc[cash["exchange"].astype(str).str.upper().eq("NSE")]
    cash["tradingsymbol"] = cash["tradingsymbol"].astype(str).str.upper().str.strip()
    cash = cash.drop_duplicates("tradingsymbol", keep="last").set_index("tradingsymbol")

    mapped_rows: list[dict[str, Any]] = []
    excluded_rows: list[dict[str, Any]] = []
    for contract in universe.to_dict("records"):
        underlying = str(contract.get("underlying", "")).upper().strip()
        if underlying in common.INDEX_UNDERLYINGS:
            excluded_rows.append(
                {
                    "underlying": underlying,
                    "futures_tradingsymbol": contract.get("tradingsymbol", ""),
                    "reason": "INDEX_FUTURE_HAS_NO_CASH_EQUITY",
                }
            )
            continue
        candidates = [underlying]
        alias = EQUITY_SYMBOL_ALIASES.get(underlying)
        if alias and alias not in candidates:
            candidates.append(alias)
        equity_symbol = next((symbol for symbol in candidates if symbol in cash.index), "")
        if not equity_symbol:
            excluded_rows.append(
                {
                    "underlying": underlying,
                    "futures_tradingsymbol": contract.get("tradingsymbol", ""),
                    "equity_symbol": equity_symbol,
                    "reason": "EQUITY_INSTRUMENT_NOT_FOUND",
                }
            )
            continue
        equity = cash.loc[equity_symbol]
        row = dict(contract)
        row.update(
            {
                "futures_tradingsymbol": str(contract["tradingsymbol"]),
                "futures_instrument_token": int(contract["instrument_token"]),
                "futures_lot_size": int(contract.get("lot_size") or 1),
                "futures_tick_size": float(contract.get("tick_size") or 0.05),
                "equity_symbol": equity_symbol,
                "equity_instrument_token": int(equity["instrument_token"]),
                "equity_tick_size": float(equity.get("tick_size", 0.05) or 0.05),
                "equity_exchange": "NSE",
                "data_contract": DATA_CONTRACT_VERSION,
            }
        )
        mapped_rows.append(row)
    mapped = pd.DataFrame(mapped_rows)
    excluded = pd.DataFrame(excluded_rows)
    return mapped, excluded


def ensure_equity_mapping(universe: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize persisted mappings, with the token cache as a legacy fallback."""
    token_map = load_equity_token_map()
    mapped_rows: list[dict[str, Any]] = []
    excluded_rows: list[dict[str, Any]] = []
    for contract in universe.to_dict("records"):
        underlying = str(contract.get("underlying", "")).upper().strip()
        equity_symbol = str(contract.get("equity_symbol", "") or "").upper().strip()
        equity_symbol = equity_symbol or equity_symbol_for_underlying(underlying)
        if not equity_symbol:
            excluded_rows.append(
                {
                    "underlying": underlying,
                    "futures_tradingsymbol": contract.get("tradingsymbol", ""),
                    "reason": "INDEX_FUTURE_HAS_NO_CASH_EQUITY",
                }
            )
            continue
        token = contract.get("equity_instrument_token")
        try:
            equity_token = int(token)
        except (TypeError, ValueError):
            equity_token = int(token_map.get(equity_symbol, 0))
        if equity_token <= 0:
            excluded_rows.append(
                {
                    "underlying": underlying,
                    "futures_tradingsymbol": contract.get("tradingsymbol", ""),
                    "equity_symbol": equity_symbol,
                    "reason": "EQUITY_TOKEN_NOT_FOUND",
                }
            )
            continue
        row = dict(contract)
        row.update(
            {
                "futures_tradingsymbol": str(
                    contract.get("futures_tradingsymbol") or contract["tradingsymbol"]
                ),
                "futures_instrument_token": int(
                    contract.get("futures_instrument_token") or contract["instrument_token"]
                ),
                "futures_lot_size": int(
                    contract.get("futures_lot_size") or contract.get("lot_size") or 1
                ),
                "futures_tick_size": float(
                    contract.get("futures_tick_size") or contract.get("tick_size") or 0.05
                ),
                "equity_symbol": equity_symbol,
                "equity_instrument_token": equity_token,
                "equity_tick_size": float(contract.get("equity_tick_size") or 0.05),
                "equity_exchange": "NSE",
                "data_contract": DATA_CONTRACT_VERSION,
            }
        )
        mapped_rows.append(row)
    return pd.DataFrame(mapped_rows), pd.DataFrame(excluded_rows)


def equity_five_minute_path(symbol: str, root: Path) -> Path:
    return Path(root) / f"{str(symbol).upper()}_stocks_indicators_5min.parquet"


def equity_one_minute_path(symbol: str, root: Path) -> Path:
    return Path(root) / f"{str(symbol).upper()}_stocks_indicators_1min.parquet"


def resolve_backtest_equity_symbol(symbol: str) -> str:
    """Resolve historical cash filenames without changing the live NSE mapping."""
    requested = str(symbol).upper().strip()
    candidates = [requested]
    alias = EQUITY_SYMBOL_ALIASES.get(requested)
    if alias and alias not in candidates:
        candidates.append(alias)
    for candidate in candidates:
        if equity_one_minute_path(candidate, DEFAULT_BACKTEST_EQUITY_1M_DIR).exists():
            return candidate
    return requested


def _to_ist(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce")
    if parsed.dt.tz is None:
        return parsed.dt.tz_localize(common.IST)
    return parsed.dt.tz_convert(common.IST)


def _load_equity_bars(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    available = set(pq.ParquetFile(path).schema.names)
    required = ["date", "open", "high", "low", "close", "volume"]
    missing = set(required) - available
    if missing:
        raise ValueError(f"Equity bars missing columns {sorted(missing)}: {path}")
    optional = [
        column
        for column in (
            "gap_filled",
            "opening_snapshot",
            "provisional_stale",
            "source_1m_count",
            "EMA_20",
            "EMA_50",
            "EMA_200",
            "RSI",
            "ATR",
        )
        if column in available
    ]
    frame = pd.read_parquet(path, columns=required + optional)
    frame["ts"] = _to_ist(frame["date"])
    numeric = ["open", "high", "low", "close", "volume"]
    frame[numeric] = frame[numeric].apply(pd.to_numeric, errors="coerce")
    return (
        frame.dropna(subset=["ts"])
        .drop_duplicates("ts", keep="last")
        .sort_values("ts")
        .reset_index(drop=True)
    )


def completed_real_equity_five_minute_bars(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep only completed exchange candles suitable for V5 decisions."""
    if frame.empty:
        return frame.copy()

    out = frame.copy()
    if "ts" not in out.columns:
        if "date" not in out.columns:
            raise ValueError("Equity 5-minute bars require a date or ts column.")
        out["ts"] = _to_ist(out["date"])

    ts = _to_ist(out["ts"])
    eligible = ts.notna()

    # End-labelled 5-minute data starts at 09:20. The optional 09:15 row in
    # the live store is an opening snapshot, never a completed strategy bar.
    eligible &= ~((ts.dt.hour == 9) & (ts.dt.minute == 15))
    for column in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if column in out.columns:
            flagged = (
                pd.to_numeric(out[column], errors="coerce").fillna(0).ne(0)
                | out[column].astype(str).str.strip().str.lower().isin({"true", "yes", "on"})
            )
            eligible &= ~flagged

    if "source_1m_count" in out.columns:
        source_count = pd.to_numeric(out["source_1m_count"], errors="coerce")
        eligible &= source_count.isna() | source_count.eq(5)

    out = out.loc[eligible].copy()
    out["ts"] = ts.loc[out.index]
    return out.sort_values("ts").reset_index(drop=True)


def reject_exact_adjacent_ohlcv_copies(frame: pd.DataFrame) -> pd.DataFrame:
    """Reject unverified provisional copies while trusting exact 1m lineage."""
    if frame.empty:
        return frame.copy()
    out = frame.sort_values("ts").reset_index(drop=True).copy()
    previous = out.shift(1)
    same = out["ts"].dt.date.eq(previous["ts"].dt.date)
    same &= out["ts"].sub(previous["ts"]).eq(pd.Timedelta(minutes=5))
    for column in ("open", "high", "low", "close", "volume"):
        same &= pd.to_numeric(out[column], errors="coerce").eq(
            pd.to_numeric(previous[column], errors="coerce")
        )
    if "source_1m_count" in out.columns:
        source_count = pd.to_numeric(out["source_1m_count"], errors="coerce")
        previous_source_count = source_count.shift(1)
        same &= ~(source_count.eq(5) & previous_source_count.eq(5))
    return out.loc[~same].reset_index(drop=True)


def aggregate_equity_one_minute_to_five_minute(frame: pd.DataFrame) -> pd.DataFrame:
    """Build causal end-labelled 5-minute equity candles from five real 1m rows."""
    if frame.empty:
        return pd.DataFrame()

    minute = frame.sort_values("ts").drop_duplicates("ts", keep="last").copy()
    ts = _to_ist(minute["ts"])
    session_open = ts.dt.normalize() + pd.Timedelta(hours=9, minutes=15)
    minute_offset = (ts - session_open).dt.total_seconds().div(60)
    eligible = minute_offset.between(1, 375)
    for column in ("gap_filled", "opening_snapshot", "provisional_stale"):
        if column in minute.columns:
            flagged = (
                pd.to_numeric(minute[column], errors="coerce").fillna(0).ne(0)
                | minute[column].astype(str).str.strip().str.lower().isin({"true", "yes", "on"})
            )
            eligible &= ~flagged
    for column in ("open", "high", "low", "close", "volume"):
        values = pd.to_numeric(minute[column], errors="coerce")
        eligible &= values.notna() & np.isfinite(values)
        minute[column] = values

    minute = minute.loc[eligible].copy()
    if minute.empty:
        return pd.DataFrame()
    ts = ts.loc[minute.index]
    session_open = session_open.loc[minute.index]
    minute_offset = minute_offset.loc[minute.index]
    slot_number = ((minute_offset - 1) // 5 + 1).astype(int)
    minute["slot_end"] = session_open + pd.to_timedelta(slot_number * 5, unit="m")

    grouped = (
        minute.groupby("slot_end", sort=True, as_index=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            source_1m_count=("ts", "size"),
            source_1m_first=("ts", "first"),
            source_1m_last=("ts", "last"),
        )
        .sort_values("slot_end")
        .reset_index(drop=True)
    )
    grouped["source_1m_first"] = _to_ist(grouped["source_1m_first"])
    grouped["source_1m_last"] = _to_ist(grouped["source_1m_last"])
    exact = (
        grouped["source_1m_count"].eq(5)
        & grouped["source_1m_first"].eq(grouped["slot_end"] - pd.Timedelta(minutes=4))
        & grouped["source_1m_last"].eq(grouped["slot_end"])
    )
    grouped = grouped.loc[exact].copy()
    grouped["date"] = grouped["slot_end"]
    grouped["ts"] = grouped["slot_end"]
    grouped["gap_filled"] = 0
    grouped["opening_snapshot"] = False
    grouped["provisional_stale"] = 0
    return completed_real_equity_five_minute_bars(
        grouped.drop(columns=["slot_end", "source_1m_first", "source_1m_last"])
    )


def load_equity_five_minute(
    symbol: str,
    root: Path = DEFAULT_BACKTEST_EQUITY_5M_DIR,
) -> pd.DataFrame:
    return reject_exact_adjacent_ohlcv_copies(
        completed_real_equity_five_minute_bars(
            _load_equity_bars(equity_five_minute_path(symbol, root))
        )
    )


def load_equity_one_minute(
    symbol: str,
    root: Path = DEFAULT_BACKTEST_EQUITY_1M_DIR,
) -> pd.DataFrame:
    return _load_equity_bars(equity_one_minute_path(symbol, root))


def add_equity_five_minute_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values("ts").reset_index(drop=True).copy()
    for span in EMA_SPANS:
        out[f"ema{span}"] = out["close"].ewm(span=span, adjust=False).mean()
    out["prev_close"] = out["close"].shift(1)
    out["price_change_pct"] = (out["close"] / out["prev_close"] - 1.0) * 100.0
    prior_volume = out["volume"].shift(1).rolling(20, min_periods=5).mean()
    out["volume_ratio"] = out["volume"].div(prior_volume.where(prior_volume.gt(0)))
    out["traded_value"] = out["close"] * out["volume"]
    return out


def join_equity_price_with_futures_oi(
    equity_frame: pd.DataFrame,
    futures_frame: pd.DataFrame,
) -> pd.DataFrame:
    """Return cash OHLCV/features with only OI fields joined from the future."""
    if equity_frame.empty or futures_frame.empty:
        return pd.DataFrame()
    equity = add_equity_five_minute_features(
        completed_real_equity_five_minute_bars(equity_frame)
    )
    futures = futures_frame.sort_values("ts").reset_index(drop=True).copy()
    futures["oi"] = pd.to_numeric(futures["oi"], errors="coerce")
    futures["prev_oi"] = futures["oi"].shift(1)
    valid_oi_pair = (
        futures["oi"].gt(0)
        & futures["prev_oi"].gt(0)
        & np.isfinite(futures["oi"])
        & np.isfinite(futures["prev_oi"])
    )
    futures["oi_change_pct"] = np.where(
        valid_oi_pair,
        (futures["oi"] / futures["prev_oi"] - 1.0) * 100.0,
        np.nan,
    )
    oi_only = futures[["ts", "oi", "prev_oi", "oi_change_pct"]].drop_duplicates(
        "ts", keep="last"
    )
    merged = equity.merge(oi_only, on="ts", how="inner", validate="one_to_one")
    merged["data_contract"] = DATA_CONTRACT_VERSION
    merged["price_source"] = "NSE_EQUITY"
    merged["oi_source"] = "NFO_FUTURE"
    return merged.sort_values("ts").reset_index(drop=True)


def cache_manifest_payload() -> dict[str, Any]:
    return {
        "data_contract": DATA_CONTRACT_VERSION,
        "backtest_equity_5m_construction": BACKTEST_EQUITY_5M_CONSTRUCTION,
        "backtest_equity_1m_dir": str(DEFAULT_BACKTEST_EQUITY_1M_DIR.resolve()),
        "five_minute_timestamp_convention": "candle_end",
        "one_minute_timestamp_convention": "candle_end",
        "price_volume_indicator_source": "NSE_EQUITY",
        "oi_source": "NFO_FUTURE",
        "futures_fields_admitted_to_v5": ["oi", "prev_oi", "oi_change_pct"],
        "oi_validity": "positive_current_and_previous_finite_change",
        "confirmation_instrument": "NSE_EQUITY",
        "entry_instrument": "NSE_EQUITY",
        "exit_path_instrument": "NSE_EQUITY",
    }


def backtest_historical_coverage() -> dict[str, Any]:
    universe_path = common.UNIVERSE_DIR / "latest_near_month.parquet"
    if not universe_path.exists():
        return {"mapped_stock_futures": 0, "complete_equity_histories": 0, "missing": []}
    universe = pd.read_parquet(universe_path)
    mapped, _ = ensure_equity_mapping(universe)
    missing: list[str] = []
    complete = 0
    required_start = pd.Timestamp("2026-05-27").date()
    required_end = pd.Timestamp("2026-08-10").date()
    for row in mapped.to_dict("records"):
        symbol = resolve_backtest_equity_symbol(str(row["equity_symbol"]))
        minute_path = equity_one_minute_path(symbol, DEFAULT_BACKTEST_EQUITY_1M_DIR)
        covers_period = False
        if minute_path.exists():
            try:
                dates = _to_ist(pd.read_parquet(minute_path, columns=["date"])["date"])
                covers_period = (
                    not dates.empty
                    and dates.min().date() <= required_start
                    and dates.max().date() >= required_end
                )
            except Exception:
                covers_period = False
        if covers_period:
            complete += 1
        else:
            missing.append(str(row.get("underlying") or symbol))
    return {
        "mapped_stock_futures": int(len(mapped)),
        "complete_equity_histories": int(complete),
        "missing": sorted(missing),
    }
