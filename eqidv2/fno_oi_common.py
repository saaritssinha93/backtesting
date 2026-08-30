from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import tempfile
import threading
import time as time_module
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from eqidv2_runtime_paths import RUNTIME_STATUS_DIR, runtime_dir


IST = ZoneInfo("Asia/Kolkata")
SCRIPT_DIR = Path(__file__).resolve().parent
FNO_ROOT = runtime_dir("fno_oi")
MASTER_DIR = FNO_ROOT / "instrument_master"
UNIVERSE_DIR = FNO_ROOT / "universe"
CONTRACT_REGISTRY_PATH = UNIVERSE_DIR / "contract_registry.parquet"
RAW_CONTRACT_DIR = FNO_ROOT / "raw_contracts_5m"
FETCH_SLOT_DIR = FNO_ROOT / "slot_ready"
EQUITY_1M_RAW_DIR = FNO_ROOT / "raw_equity_1m"
EQUITY_1M_SLOT_DIR = FNO_ROOT / "equity_1m_slot_ready"
FEATURE_CONTRACT_DIR = FNO_ROOT / "contract_features"
FEATURE_SNAPSHOT_DIR = FNO_ROOT / "feature_snapshots"
RANKING_DIR = FNO_ROOT / "rankings"
RANKING_SLOT_DIR = FNO_ROOT / "ranking_ready"
LATEST_DIR = FNO_ROOT / "latest"
QC_DIR = FNO_ROOT / "eod_qc"
CASH_SLOT_DIR = runtime_dir("slot_ready_5m")

RAW_DATA_VERSION = "fno_oi_raw_v1"
FEATURE_DATA_VERSION = "fno_oi_features_v1"
FNO_FETCH_SLOT_SCHEMA_VERSION = "fno_oi_fetch_slot_v2"
VERIFIED_NO_CANDLE_POLICY_VERSION = "verified_stock_no_candle_skip_v1"
EQUITY_1M_SLOT_SCHEMA_VERSION = "fno_equity_1m_slot_v1"
MIN_STOCK_FUTURES_COVERAGE = 0.99
MAX_VERIFIED_NO_CANDLE_STOCKS = 2
MIN_NO_CANDLE_FETCH_ATTEMPTS = 3
ATOMIC_REPLACE_ATTEMPTS = 8
ATOMIC_REPLACE_BACKOFF_SEC = 0.05
_PATH_LOCKS: dict[str, threading.RLock] = {}
_PATH_LOCKS_GUARD = threading.Lock()
INDEX_UNDERLYINGS = {
    "BANKNIFTY",
    "FINNIFTY",
    "MIDCPNIFTY",
    "NIFTY",
    "NIFTYFPI",
    "NIFTYNXT50",
}

INSTRUMENT_COLUMNS = (
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

RAW_COLUMNS = (
    "timestamp",
    "candle_start",
    "underlying",
    "tradingsymbol",
    "instrument_token",
    "exchange_token",
    "expiry",
    "contract_month",
    "days_to_expiry",
    "lot_size",
    "tick_size",
    "is_index_future",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "oi",
    "quality_state",
    "fetch_timestamp",
    "source",
    "data_version",
)


for _directory in (
    FNO_ROOT,
    MASTER_DIR,
    UNIVERSE_DIR,
    RAW_CONTRACT_DIR,
    FETCH_SLOT_DIR,
    EQUITY_1M_RAW_DIR,
    EQUITY_1M_SLOT_DIR,
    FEATURE_CONTRACT_DIR,
    FEATURE_SNAPSHOT_DIR,
    RANKING_DIR,
    RANKING_SLOT_DIR,
    LATEST_DIR,
    QC_DIR,
    RUNTIME_STATUS_DIR,
):
    _directory.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class KiteCredential:
    app_name: str
    api_key: str
    access_token: str


def now_ist() -> datetime:
    return datetime.now(IST)


def symbol_set_sha256(values: Iterable[Any]) -> str:
    """Return a stable fingerprint for a case-normalized set of symbols."""
    symbols = sorted(
        {
            str(value).strip().upper()
            for value in values
            if str(value).strip()
        }
    )
    return hashlib.sha256("\n".join(symbols).encode("utf-8")).hexdigest()


def canonical_json_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _path_lock(path: Path) -> threading.RLock:
    key = str(path.resolve()).lower()
    with _PATH_LOCKS_GUARD:
        return _PATH_LOCKS.setdefault(key, threading.RLock())


def _atomic_replace_bytes(path: Path, writer: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _path_lock(path):
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=f".{path.name}.",
                suffix=".tmp",
                dir=str(path.parent),
                delete=False,
            ) as handle:
                temp_path = Path(handle.name)
            writer(temp_path)
            last_error: OSError | None = None
            for attempt in range(ATOMIC_REPLACE_ATTEMPTS):
                try:
                    os.replace(temp_path, path)
                    temp_path = None
                    return
                except OSError as exc:
                    last_error = exc
                    if attempt + 1 < ATOMIC_REPLACE_ATTEMPTS:
                        time_module.sleep(ATOMIC_REPLACE_BACKOFF_SEC * (attempt + 1))
            assert last_error is not None
            raise last_error
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink()
                except FileNotFoundError:
                    pass


def atomic_write_text(path: Path, text: str) -> None:
    encoded = text.encode("utf-8")

    def _write(temp_path: Path) -> None:
        temp_path.write_bytes(encoded)

    _atomic_replace_bytes(path, _write)


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    atomic_write_text(path, json.dumps(dict(payload), indent=2, ensure_ascii=True, default=str) + "\n")


def atomic_write_parquet(frame: pd.DataFrame, path: Path) -> None:
    def _write(temp_path: Path) -> None:
        frame.to_parquet(temp_path, index=False, engine="pyarrow")

    _atomic_replace_bytes(path, _write)


def atomic_write_csv(frame: pd.DataFrame, path: Path) -> None:
    def _write(temp_path: Path) -> None:
        frame.to_csv(temp_path, index=False)

    _atomic_replace_bytes(path, _write)


def _status_value(value: Any) -> str:
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, ensure_ascii=True, default=str, separators=(",", ":"))
    return " ".join(str(value).replace("=", ":").split())


def atomic_write_kv(path: Path, payload: Mapping[str, Any]) -> bool:
    lines = [f"{key}={_status_value(value)}" for key, value in payload.items()]
    text = "\n".join(lines) + "\n"
    try:
        atomic_write_text(path, text)
        return True
    except OSError as replace_error:
        # Runtime status is telemetry. A dashboard reader can temporarily deny
        # FILE_SHARE_DELETE on Windows; a direct write is preferable to taking
        # down the market-data session.
        try:
            with _path_lock(path):
                path.write_text(text, encoding="utf-8")
            return True
        except OSError as direct_error:
            print(
                f"[STATUS][WARN] could not update {path.name}: "
                f"replace={replace_error} direct={direct_error}",
                file=sys.stderr,
                flush=True,
            )
            return False


def session_status_path(session: str) -> Path:
    return RUNTIME_STATUS_DIR / f"{session}.status"


def session_heartbeat_path(session: str) -> Path:
    return RUNTIME_STATUS_DIR / f"{session}.heartbeat"


def publish_status(
    session: str,
    status: str,
    *,
    heartbeat_state: str | None = None,
    **extra: Any,
) -> None:
    stamp = now_ist().isoformat(timespec="seconds")
    payload: dict[str, Any] = {
        "status": status,
        "session": session,
        "ts": stamp,
        **extra,
    }
    atomic_write_kv(session_status_path(session), payload)
    heartbeat: dict[str, Any] = {
        "state": heartbeat_state or status,
        "session": session,
        "ts": stamp,
        **extra,
    }
    atomic_write_kv(session_heartbeat_path(session), heartbeat)


def publish_heartbeat(session: str, state: str, **extra: Any) -> None:
    atomic_write_kv(
        session_heartbeat_path(session),
        {
            "state": state,
            "session": session,
            "ts": now_ist().isoformat(timespec="seconds"),
            **extra,
        },
    )


def _read_first_token(path: Path) -> str:
    value = path.read_text(encoding="utf-8", errors="ignore").strip()
    if not value:
        raise ValueError(f"Credential file is empty: {path.name}")
    return value.split()[0]


def discover_kite_credentials(
    base_dir: Path = SCRIPT_DIR,
    *,
    max_apps: int = 8,
) -> list[KiteCredential]:
    credentials: list[KiteCredential] = []
    for app_index in range(1, max(1, int(max_apps)) + 1):
        suffix = "" if app_index == 1 else str(app_index)
        api_path = base_dir / f"api_key{suffix}.txt"
        access_path = base_dir / f"access_token{suffix}.txt"
        if not api_path.exists() or not access_path.exists():
            continue
        try:
            credentials.append(
                KiteCredential(
                    app_name=f"app{app_index}",
                    api_key=_read_first_token(api_path),
                    access_token=_read_first_token(access_path),
                )
            )
        except (OSError, ValueError):
            continue
    if not credentials:
        raise RuntimeError("No usable api_key/access_token credential pairs were found.")
    return credentials


def make_kite_client(credential: KiteCredential, *, timeout_sec: float = 8.0):
    from kiteconnect import KiteConnect

    client = KiteConnect(api_key=credential.api_key, timeout=float(timeout_sec))
    client.set_access_token(credential.access_token)
    return client


def _derived_underlying(tradingsymbol: str, expiry: pd.Timestamp | None) -> str:
    symbol = str(tradingsymbol).strip().upper()
    if expiry is not None and not pd.isna(expiry):
        suffix = f"{expiry.strftime('%y')}{expiry.strftime('%b').upper()}FUT"
        if symbol.endswith(suffix) and len(symbol) > len(suffix):
            return symbol[: -len(suffix)]
    match = re.match(
        r"^(?P<underlying>.+?)\d{2}(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)FUT$",
        symbol,
    )
    return match.group("underlying") if match else symbol.removesuffix("FUT")


def normalize_futures_master(
    records: Iterable[Mapping[str, Any]],
    *,
    session_date: date,
) -> pd.DataFrame:
    frame = pd.DataFrame(list(records))
    if frame.empty:
        raise ValueError("Kite returned an empty NFO instrument master.")
    for column in INSTRUMENT_COLUMNS:
        if column not in frame.columns:
            frame[column] = pd.NA

    frame = frame.loc[:, list(INSTRUMENT_COLUMNS)].copy()
    for column in ("tradingsymbol", "name", "instrument_type", "segment", "exchange"):
        frame[column] = frame[column].astype("string").fillna("").str.upper().str.strip()
    frame["expiry"] = pd.to_datetime(frame["expiry"], errors="coerce").dt.normalize()
    frame["instrument_token"] = pd.to_numeric(frame["instrument_token"], errors="coerce")
    frame["exchange_token"] = pd.to_numeric(frame["exchange_token"], errors="coerce")
    for column in ("last_price", "strike", "tick_size", "lot_size"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.loc[
        frame["segment"].eq("NFO-FUT")
        & frame["instrument_type"].eq("FUT")
        & frame["expiry"].notna()
        & frame["instrument_token"].notna()
        & frame["tradingsymbol"].ne("")
        & frame["expiry"].ge(pd.Timestamp(session_date))
    ].copy()
    if frame.empty:
        raise ValueError("No active NFO-FUT contracts remained after validation.")

    frame["instrument_token"] = frame["instrument_token"].astype("int64")
    frame["exchange_token"] = frame["exchange_token"].astype("Int64")
    frame["lot_size"] = frame["lot_size"].astype("Int64")
    frame["underlying"] = [
        _derived_underlying(symbol, expiry)
        for symbol, expiry in zip(frame["tradingsymbol"], frame["expiry"])
    ]
    fallback_name = frame["name"].where(frame["name"].ne(""))
    frame["underlying"] = frame["underlying"].replace("", pd.NA).fillna(fallback_name)
    frame["underlying"] = frame["underlying"].astype("string").str.upper().str.strip()
    frame["is_index_future"] = frame["underlying"].isin(INDEX_UNDERLYINGS)
    frame["contract_month"] = frame["expiry"].dt.strftime("%Y-%m")
    frame["master_date"] = pd.Timestamp(session_date)
    frame["unique_key"] = frame["exchange"] + ":" + frame["tradingsymbol"]

    if frame["unique_key"].duplicated().any():
        duplicates = frame.loc[frame["unique_key"].duplicated(False), "unique_key"].tolist()
        raise ValueError(f"Duplicate exchange/tradingsymbol keys in instrument master: {duplicates[:8]}")

    return frame.sort_values(
        ["underlying", "expiry", "tradingsymbol"], kind="stable"
    ).reset_index(drop=True)


def select_near_month(master: pd.DataFrame) -> pd.DataFrame:
    required = {"underlying", "expiry", "tradingsymbol", "instrument_token"}
    missing = required - set(master.columns)
    if missing:
        raise ValueError(f"Instrument master missing columns: {sorted(missing)}")
    ordered = master.sort_values(
        ["underlying", "expiry", "tradingsymbol"], kind="stable"
    )
    selected = ordered.groupby("underlying", sort=True, as_index=False).head(1).copy()
    selected["contract_rank"] = 1
    return selected.reset_index(drop=True)


def universe_sha256(universe: pd.DataFrame) -> str:
    values = []
    for row in universe.sort_values(["exchange", "tradingsymbol"]).itertuples(index=False):
        expiry = pd.Timestamp(getattr(row, "expiry")).strftime("%Y-%m-%d")
        values.append(
            f"{getattr(row, 'exchange')}:{getattr(row, 'tradingsymbol')}:"
            f"{int(getattr(row, 'instrument_token'))}:{expiry}"
        )
    return hashlib.sha256("\n".join(values).encode("utf-8")).hexdigest()


def master_paths(session_date: date) -> tuple[Path, Path]:
    dated = MASTER_DIR / f"instrument_master_{session_date.isoformat()}.parquet"
    return dated, MASTER_DIR / "latest_instrument_master.parquet"


def universe_paths(session_date: date) -> tuple[Path, Path]:
    dated = UNIVERSE_DIR / f"near_month_{session_date.isoformat()}.parquet"
    return dated, UNIVERSE_DIR / "latest_near_month.parquet"


def persist_universe(
    master: pd.DataFrame,
    universe: pd.DataFrame,
    *,
    session_date: date,
) -> dict[str, Any]:
    dated_master, latest_master = master_paths(session_date)
    dated_universe, latest_universe = universe_paths(session_date)
    atomic_write_parquet(master, dated_master)
    atomic_write_parquet(master, latest_master)
    atomic_write_parquet(universe, dated_universe)
    atomic_write_parquet(universe, latest_universe)
    registry = update_contract_registry(master, universe, session_date=session_date)
    summary = {
        "session_date": session_date.isoformat(),
        "published_at_ist": now_ist().isoformat(timespec="seconds"),
        "active_futures": int(len(master)),
        "near_month_contracts": int(len(universe)),
        "stock_futures": int((~universe["is_index_future"]).sum()),
        "index_futures": int(universe["is_index_future"].sum()),
        "universe_sha256": universe_sha256(universe),
        "master_path": str(dated_master),
        "universe_path": str(dated_universe),
        "contract_registry_records": int(len(registry)),
        "contract_registry_path": str(CONTRACT_REGISTRY_PATH),
    }
    atomic_write_json(UNIVERSE_DIR / "latest_universe_summary.json", summary)
    return summary


def update_contract_registry(
    master: pd.DataFrame,
    universe: pd.DataFrame,
    *,
    session_date: date,
) -> pd.DataFrame:
    registry_columns = [
        "unique_key",
        "underlying",
        "tradingsymbol",
        "instrument_token",
        "exchange_token",
        "expiry",
        "contract_month",
        "lot_size",
        "tick_size",
        "is_index_future",
        "is_near_month",
        "first_seen",
        "last_seen",
        "status",
    ]
    current = master.copy()
    current["contract_month"] = current["expiry"].dt.strftime("%Y-%m")
    current["is_near_month"] = current["unique_key"].isin(set(universe["unique_key"]))
    current["first_seen"] = pd.Timestamp(session_date)
    current["last_seen"] = pd.Timestamp(session_date)
    current["status"] = "ACTIVE"
    current = current.loc[:, registry_columns]

    if CONTRACT_REGISTRY_PATH.exists():
        try:
            existing = pd.read_parquet(CONTRACT_REGISTRY_PATH)
        except Exception as exc:
            raise RuntimeError(f"Unable to read contract registry: {exc}") from exc
        for column in registry_columns:
            if column not in existing.columns:
                existing[column] = pd.NA
        existing = existing.loc[:, registry_columns].copy()
        existing["expiry"] = pd.to_datetime(existing["expiry"], errors="coerce").dt.normalize()
        existing["first_seen"] = pd.to_datetime(existing["first_seen"], errors="coerce").dt.normalize()
        existing["last_seen"] = pd.to_datetime(existing["last_seen"], errors="coerce").dt.normalize()
        existing["is_near_month"] = False
        existing["status"] = np.where(
            existing["expiry"].lt(pd.Timestamp(session_date)),
            "EXPIRED",
            "NOT_IN_LATEST_MASTER",
        )
    else:
        existing = pd.DataFrame(columns=registry_columns)

    combined = (
        current.copy()
        if existing.empty
        else pd.concat([existing, current], ignore_index=True, sort=False)
    )
    combined["first_seen"] = pd.to_datetime(combined["first_seen"], errors="coerce").dt.normalize()
    combined["last_seen"] = pd.to_datetime(combined["last_seen"], errors="coerce").dt.normalize()
    first_seen = combined.groupby("unique_key", sort=False)["first_seen"].min()
    last_seen = combined.groupby("unique_key", sort=False)["last_seen"].max()
    registry = combined.drop_duplicates("unique_key", keep="last").copy()
    registry["first_seen"] = registry["unique_key"].map(first_seen)
    registry["last_seen"] = registry["unique_key"].map(last_seen)
    registry["instrument_token"] = pd.to_numeric(
        registry["instrument_token"], errors="coerce"
    ).astype("Int64")
    registry["exchange_token"] = pd.to_numeric(
        registry["exchange_token"], errors="coerce"
    ).astype("Int64")
    registry["lot_size"] = pd.to_numeric(registry["lot_size"], errors="coerce").astype("Int64")
    registry = registry.sort_values(
        ["underlying", "expiry", "tradingsymbol"], kind="stable"
    ).reset_index(drop=True)
    atomic_write_parquet(registry, CONTRACT_REGISTRY_PATH)
    return registry


def load_near_month_universe(*, expected_date: date | None = None) -> pd.DataFrame:
    path = UNIVERSE_DIR / "latest_near_month.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Near-month universe does not exist: {path}")
    frame = pd.read_parquet(path)
    if frame.empty:
        raise ValueError(f"Near-month universe is empty: {path}")
    frame["expiry"] = pd.to_datetime(frame["expiry"], errors="coerce").dt.normalize()
    frame["master_date"] = pd.to_datetime(frame["master_date"], errors="coerce").dt.normalize()
    if expected_date is not None:
        observed = set(frame["master_date"].dropna().dt.date.tolist())
        if observed != {expected_date}:
            raise ValueError(
                f"Near-month universe is not for {expected_date.isoformat()}: {sorted(map(str, observed))}"
            )
    return frame.sort_values("underlying").reset_index(drop=True)


def safe_contract_stem(tradingsymbol: str) -> str:
    symbol = str(tradingsymbol).strip().upper()
    stem = re.sub(r"[^A-Z0-9._-]+", "_", symbol).strip("._") or "CONTRACT"
    if stem != symbol:
        digest = hashlib.sha1(symbol.encode("utf-8")).hexdigest()[:8]
        stem = f"{stem}_{digest}"
    return stem


def raw_contract_path(tradingsymbol: str) -> Path:
    return RAW_CONTRACT_DIR / f"{safe_contract_stem(tradingsymbol)}_5minute.parquet"


def equity_1m_path(session_date: date, tradingsymbol: str) -> Path:
    return (
        EQUITY_1M_RAW_DIR
        / session_date.isoformat()
        / f"{safe_contract_stem(tradingsymbol)}_1minute.parquet"
    )


def equity_1m_slot_path(
    slot_end: datetime | pd.Timestamp,
    *,
    generation: str,
    scanner_sha256: str,
) -> Path:
    stamp = pd.Timestamp(slot_end)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(IST)
    else:
        stamp = stamp.tz_convert(IST)
    safe_generation = re.sub(r"[^a-z0-9_-]+", "_", str(generation).lower())
    digest = str(scanner_sha256).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("scanner_sha256 must be a full SHA-256 digest")
    return (
        EQUITY_1M_SLOT_DIR
        / safe_generation
        / stamp.date().isoformat()
        / f"slot_{stamp.strftime('%H%M')}_{digest[:16]}.json"
    )


def equity_1m_slot_data_path(
    slot_end: datetime | pd.Timestamp,
    *,
    generation: str,
    scanner_sha256: str,
) -> Path:
    return equity_1m_slot_path(
        slot_end,
        generation=generation,
        scanner_sha256=scanner_sha256,
    ).with_suffix(".parquet")


def feature_contract_path(tradingsymbol: str) -> Path:
    return FEATURE_CONTRACT_DIR / f"{safe_contract_stem(tradingsymbol)}_features.parquet"


def fetch_slot_path(slot_end: datetime | pd.Timestamp) -> Path:
    stamp = pd.Timestamp(slot_end)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(IST)
    else:
        stamp = stamp.tz_convert(IST)
    return FETCH_SLOT_DIR / f"slot_{stamp.strftime('%Y%m%d_%H%M')}.json"


def cash_slot_path(slot_end: datetime | pd.Timestamp) -> Path:
    stamp = pd.Timestamp(slot_end)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(IST)
    else:
        stamp = stamp.tz_convert(IST)
    return CASH_SLOT_DIR / f"slot_{stamp.strftime('%Y%m%d_%H%M')}.json"


def ranking_slot_path(slot_end: datetime | pd.Timestamp) -> Path:
    stamp = pd.Timestamp(slot_end)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize(IST)
    else:
        stamp = stamp.tz_convert(IST)
    return RANKING_SLOT_DIR / f"slot_{stamp.strftime('%Y%m%d_%H%M')}.json"


def _to_ist(values: Sequence[Any] | pd.Series) -> pd.Series:
    source = values if isinstance(values, pd.Series) else pd.Series(values)

    # Parquet preserves these columns as datetime64 (usually already in IST).
    # Keep that common path vectorised: the previous per-value pd.Timestamp
    # conversion dominated every live append as contract histories grew.
    if isinstance(source.dtype, pd.DatetimeTZDtype):
        converted = source.dt.tz_convert(IST)
        return pd.Series(
            converted.array,
            index=source.index,
            dtype="datetime64[ns, Asia/Kolkata]",
        )
    if pd.api.types.is_datetime64_any_dtype(source.dtype):
        converted = source.dt.tz_localize(IST)
        return pd.Series(
            converted.array,
            index=source.index,
            dtype="datetime64[ns, Asia/Kolkata]",
        )

    def _convert(value: Any) -> pd.Timestamp:
        try:
            stamp = pd.Timestamp(value)
        except Exception:
            return pd.NaT
        if pd.isna(stamp):
            return pd.NaT
        if stamp.tzinfo is None:
            return stamp.tz_localize(IST)
        return stamp.tz_convert(IST)

    return pd.Series(
        [_convert(value) for value in source],
        index=source.index,
        dtype="datetime64[ns, Asia/Kolkata]",
    )


def normalize_historical_candles(
    records: Iterable[Mapping[str, Any]],
    contract: Mapping[str, Any] | pd.Series,
    *,
    fetch_timestamp: datetime | None = None,
    slot_end: datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    frame = pd.DataFrame(list(records))
    if frame.empty or "date" not in frame.columns:
        return pd.DataFrame(columns=list(RAW_COLUMNS))
    for column in ("open", "high", "low", "close", "volume", "oi"):
        if column not in frame.columns:
            frame[column] = np.nan
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["candle_start"] = _to_ist(frame["date"])
    frame["timestamp"] = frame["candle_start"] + pd.Timedelta(minutes=5)
    frame = frame.loc[
        frame["timestamp"].dt.time.between(time(9, 20), time(15, 30), inclusive="both")
    ].copy()
    if slot_end is not None:
        target = pd.Timestamp(slot_end)
        if target.tzinfo is None:
            target = target.tz_localize(IST)
        else:
            target = target.tz_convert(IST)
        frame = frame.loc[frame["timestamp"].eq(target)].copy()
    if frame.empty:
        return pd.DataFrame(columns=list(RAW_COLUMNS))

    expiry = pd.to_datetime(contract.get("expiry"), errors="coerce")
    underlying = str(contract.get("underlying", "")).strip().upper()
    tradingsymbol = str(contract.get("tradingsymbol", "")).strip().upper()
    frame["underlying"] = underlying
    frame["tradingsymbol"] = tradingsymbol
    frame["instrument_token"] = int(contract.get("instrument_token"))
    frame["exchange_token"] = pd.to_numeric(contract.get("exchange_token"), errors="coerce")
    frame["expiry"] = expiry
    frame["contract_month"] = "" if pd.isna(expiry) else expiry.strftime("%Y-%m")
    frame["days_to_expiry"] = (
        expiry.normalize() - frame["timestamp"].dt.tz_localize(None).dt.normalize()
    ).dt.days
    frame["lot_size"] = pd.to_numeric(contract.get("lot_size"), errors="coerce")
    frame["tick_size"] = pd.to_numeric(contract.get("tick_size"), errors="coerce")
    frame["is_index_future"] = bool(contract.get("is_index_future", False))

    valid_ohlc = (
        frame[["open", "high", "low", "close"]].notna().all(axis=1)
        & frame["high"].ge(frame[["open", "close"]].max(axis=1))
        & frame["low"].le(frame[["open", "close"]].min(axis=1))
        & frame["high"].ge(frame["low"])
        & frame["close"].gt(0)
    )
    valid_volume = frame["volume"].notna() & frame["volume"].ge(0)
    valid_oi = frame["oi"].notna() & frame["oi"].ge(0)
    frame["quality_state"] = np.select(
        [~valid_ohlc, ~valid_volume, frame["oi"].isna(), frame["oi"].lt(0)],
        ["INVALID_OHLC", "INVALID_VOLUME", "MISSING_OI", "NEGATIVE_OI"],
        default="VALID",
    )
    frame.loc[~valid_oi & frame["oi"].notna(), "quality_state"] = "NEGATIVE_OI"
    fetched_at = fetch_timestamp or now_ist()
    frame["fetch_timestamp"] = pd.Timestamp(fetched_at)
    frame["source"] = "kite_historical"
    frame["data_version"] = RAW_DATA_VERSION
    frame = frame.drop_duplicates(subset=["instrument_token", "timestamp"], keep="last")
    return frame.loc[:, list(RAW_COLUMNS)].sort_values("timestamp").reset_index(drop=True)


def merge_contract_rows(
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
) -> pd.DataFrame:
    """Apply the canonical append/dedupe/order contract to in-memory frames."""
    if incoming is None or incoming.empty:
        return pd.DataFrame() if existing is None else existing.copy()
    current = (
        pd.DataFrame(columns=incoming.columns)
        if existing is None
        else existing
    )
    combined = (
        incoming.copy()
        if current.empty
        else pd.concat([current, incoming], ignore_index=True, sort=False)
    )
    combined["timestamp"] = _to_ist(combined["timestamp"])
    combined["candle_start"] = _to_ist(combined["candle_start"])
    combined["fetch_timestamp"] = _to_ist(combined["fetch_timestamp"])
    combined = (
        combined.drop_duplicates(subset=["instrument_token", "timestamp"], keep="last")
        .sort_values("timestamp", kind="stable")
        .reset_index(drop=True)
    )
    ordered = [column for column in RAW_COLUMNS if column in combined.columns]
    extra = [column for column in combined.columns if column not in ordered]
    return combined.loc[:, ordered + extra]


def append_contract_rows(path: Path, incoming: pd.DataFrame) -> pd.DataFrame:
    if incoming is None or incoming.empty:
        return pd.DataFrame() if not path.exists() else pd.read_parquet(path)
    existing = pd.read_parquet(path) if path.exists() else None
    combined = merge_contract_rows(existing, incoming)
    atomic_write_parquet(combined, path)
    return combined


def expected_slot_ends(session_date: date) -> pd.DatetimeIndex:
    return pd.date_range(
        start=datetime.combine(session_date, time(9, 20), tzinfo=IST),
        end=datetime.combine(session_date, time(15, 30), tzinfo=IST),
        freq="5min",
    )


def load_holidays(path: Path | None = None) -> set[date]:
    holiday_path = path or (SCRIPT_DIR / "nse_holidays.csv")
    if not holiday_path.exists():
        return set()
    try:
        frame = pd.read_csv(holiday_path)
        candidates = frame["date"] if "date" in frame.columns else frame.iloc[:, 0]
    except Exception:
        candidates = pd.Series(
            holiday_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        )
    return set(pd.to_datetime(candidates, errors="coerce").dropna().dt.date.tolist())


def is_trading_day(day: date, holidays: set[date] | None = None) -> bool:
    return day.weekday() < 5 and day not in (holidays or set())


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object: {path}")
    return payload
