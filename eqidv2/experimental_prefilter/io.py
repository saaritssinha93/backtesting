from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pyarrow.parquet as pq


BAR_COLUMNS = (
    "date",
    "datetime",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "ATR",
    "EMA_20",
    "EMA_50",
    "EMA_200",
    "RSI",
    "ADX",
    "gap_filled",
    "opening_snapshot",
)

FORBIDDEN_OUTPUT_ROOTS = (
    Path(r"C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID"),
    Path(r"C:\TradingData\eqidv2\backtesting_result_v11"),
    Path(r"C:\TradingData\eqidv2\live_signals"),
    Path(r"C:\TradingData\eqidv2\entry_engine_1min_v5_ID"),
)


@dataclass(frozen=True)
class UniverseManifest:
    path: str
    schema_version: str
    slot_ist: str
    published_at_ist: str
    symbols: tuple[str, ...]
    universe_sha256: str


@dataclass(frozen=True)
class SlotMarker:
    path: str
    sha256: str
    slot_ist: str
    published_at_ist: str
    source: str
    complete: bool
    tickers_expected: int
    tickers_written: int
    universe_sha256: str
    unresolved_symbol_count: int
    failed_symbol_count: int
    token_missing_symbol_count: int
    verification_failed_count: int
    partition_failures: tuple[Any, ...]
    duration_ms: float | None


@dataclass(frozen=True)
class BarLoadStats:
    requested_symbols: int
    loaded_symbols: int
    missing_files: int
    read_errors: int
    rows: int
    elapsed_seconds: float
    errors: tuple[str, ...]


def normalise_symbols(symbols: Iterable[str]) -> list[str]:
    return sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})


def universe_sha256(symbols: Iterable[str]) -> str:
    return hashlib.sha256("\n".join(normalise_symbols(symbols)).encode("utf-8")).hexdigest()


def load_universe_manifest(path: str | Path) -> UniverseManifest:
    source = Path(path)
    with source.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    symbols = normalise_symbols(payload.get("symbols", []))
    declared_count = int(payload.get("universe_count", -1))
    declared_hash = str(payload.get("universe_sha256", "")).strip().lower()
    actual_hash = universe_sha256(symbols)
    if declared_count != len(symbols):
        raise ValueError(f"universe count mismatch: declared={declared_count} actual={len(symbols)}")
    if declared_hash != actual_hash:
        raise ValueError("universe hash mismatch")
    return UniverseManifest(
        path=str(source.resolve()),
        schema_version=str(payload.get("schema_version", "")),
        slot_ist=str(payload.get("slot_ist", "")),
        published_at_ist=str(payload.get("published_at_ist", "")),
        symbols=tuple(symbols),
        universe_sha256=actual_hash,
    )


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_slot_marker(path: str | Path) -> SlotMarker:
    source_path = Path(path)
    raw = source_path.read_bytes()
    payload = json.loads(raw.decode("utf-8"))
    marker = SlotMarker(
        path=str(source_path.resolve()),
        sha256=hashlib.sha256(raw).hexdigest(),
        slot_ist=str(payload.get("slot_ist", "")),
        published_at_ist=str(payload.get("published_at_ist", "")),
        source=str(payload.get("source", "")),
        complete=bool(payload.get("complete", False)),
        tickers_expected=int(payload.get("tickers_expected", -1)),
        tickers_written=int(payload.get("tickers_written", -1)),
        universe_sha256=str(payload.get("universe_sha256", "")).strip().lower(),
        unresolved_symbol_count=int(payload.get("unresolved_symbol_count", -1)),
        failed_symbol_count=int(payload.get("failed_symbol_count", -1)),
        token_missing_symbol_count=int(payload.get("token_missing_symbol_count", -1)),
        verification_failed_count=int(payload.get("verification_failed_count", -1)),
        partition_failures=tuple(payload.get("partition_failures", []) or []),
        duration_ms=(float(payload["duration_ms"]) if payload.get("duration_ms") is not None else None),
    )
    validate_slot_marker(marker)
    return marker


def validate_slot_marker(marker: SlotMarker) -> None:
    if marker.source.lower() != "final":
        raise ValueError(f"slot marker is not final: source={marker.source!r}")
    if not marker.complete:
        raise ValueError("slot marker is incomplete")
    if marker.tickers_expected < 0 or marker.tickers_written != marker.tickers_expected:
        raise ValueError(
            "slot marker ticker accounting mismatch: "
            f"expected={marker.tickers_expected} written={marker.tickers_written}"
        )
    error_counts = (
        marker.unresolved_symbol_count,
        marker.failed_symbol_count,
        marker.token_missing_symbol_count,
        marker.verification_failed_count,
    )
    if any(value != 0 for value in error_counts):
        raise ValueError(f"slot marker has non-zero failure accounting: {error_counts}")
    if marker.partition_failures:
        raise ValueError("slot marker has partition failures")
    if not marker.universe_sha256:
        raise ValueError("slot marker has no universe hash")


def validate_slot_contract(marker: SlotMarker, manifest: UniverseManifest) -> None:
    marker_slot = pd.Timestamp(marker.slot_ist)
    manifest_slot = pd.Timestamp(manifest.slot_ist)
    if marker_slot != manifest_slot:
        raise ValueError(
            f"slot mismatch: marker={marker.slot_ist} manifest={manifest.slot_ist}"
        )
    if marker.tickers_expected != len(manifest.symbols):
        raise ValueError(
            "universe count differs from final marker: "
            f"marker={marker.tickers_expected} manifest={len(manifest.symbols)}"
        )
    if marker.universe_sha256 != manifest.universe_sha256:
        raise ValueError("universe hash differs between final marker and manifest")


def final_marker_path(marker_dir: str | Path, slot_ist: str) -> Path:
    slot = pd.Timestamp(slot_ist)
    return Path(marker_dir) / f"slot_{slot.strftime('%Y%m%d_%H%M')}.json"


def _read_symbol_bars(data_dir: Path, ticker: str, lookback_bars: int) -> tuple[str, pd.DataFrame | None, str | None]:
    path = data_dir / f"{ticker}_stocks_indicators_5min.parquet"
    if not path.exists():
        return ticker, None, "missing_file"
    try:
        parquet = pq.ParquetFile(path)
        available = set(parquet.schema.names)
        columns = [column for column in BAR_COLUMNS if column in available]
        required = {"open", "high", "low", "close", "volume"}
        if not required.issubset(columns) or not ({"date", "datetime"} & set(columns)):
            return ticker, None, "missing_required_columns"
        frame = parquet.read(columns=columns, use_threads=False).to_pandas()
        if lookback_bars > 0:
            frame = frame.tail(lookback_bars).copy()
        frame["ticker"] = ticker
        return ticker, frame, None
    except Exception as exc:  # pragma: no cover - exact parquet failures are environment-specific
        return ticker, None, f"{type(exc).__name__}:{exc}"


def _read_symbol_bars_through_slot(
    data_dir: Path,
    ticker: str,
    slot_ist: object,
    history_bars: int,
) -> tuple[str, pd.DataFrame | None, str | None]:
    """Read a causal historical tail ending no later than ``slot_ist``."""

    path = data_dir / f"{ticker}_stocks_indicators_5min.parquet"
    if not path.exists():
        return ticker, None, "missing_file"
    try:
        parquet = pq.ParquetFile(path)
        available = set(parquet.schema.names)
        columns = [column for column in BAR_COLUMNS if column in available]
        required = {"open", "high", "low", "close", "volume"}
        time_column = "date" if "date" in columns else "datetime" if "datetime" in columns else None
        if not required.issubset(columns) or time_column is None:
            return ticker, None, "missing_required_columns"
        frame = parquet.read(columns=columns, use_threads=False).to_pandas()
        cutoff = pd.Timestamp(slot_ist)
        if cutoff.tzinfo is None:
            cutoff = cutoff.tz_localize("Asia/Kolkata")
        else:
            cutoff = cutoff.tz_convert("Asia/Kolkata")
        timestamps = frame[time_column].map(
            lambda value: (
                pd.Timestamp(value).tz_localize("Asia/Kolkata")
                if pd.Timestamp(value).tzinfo is None
                else pd.Timestamp(value).tz_convert("Asia/Kolkata")
            )
        )
        frame = frame.loc[timestamps.le(cutoff)].copy()
        if history_bars > 0:
            frame = frame.tail(history_bars).copy()
        frame[time_column] = timestamps.loc[frame.index]
        frame["ticker"] = ticker
        return ticker, frame, None
    except Exception as exc:  # pragma: no cover - exact parquet failures are environment-specific
        return ticker, None, f"{type(exc).__name__}:{exc}"


def load_bar_directory(
    data_dir: str | Path,
    symbols: Iterable[str],
    *,
    lookback_bars: int = 48,
    max_workers: int = 8,
) -> tuple[pd.DataFrame, BarLoadStats]:
    """Read a bounded bar tail per symbol without importing production code."""

    root = Path(data_dir)
    requested = normalise_symbols(symbols)
    started = time.perf_counter()
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    missing = 0
    read_errors = 0
    workers = max(1, min(int(max_workers), max(1, len(requested))))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="prefilter-read") as executor:
        futures = {
            executor.submit(_read_symbol_bars, root, ticker, lookback_bars): ticker
            for ticker in requested
        }
        for future in as_completed(futures):
            ticker, frame, error = future.result()
            if frame is not None:
                frames.append(frame)
                continue
            if error == "missing_file":
                missing += 1
            else:
                read_errors += 1
            if len(errors) < 100:
                errors.append(f"{ticker}:{error}")
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    stats = BarLoadStats(
        requested_symbols=len(requested),
        loaded_symbols=len(frames),
        missing_files=missing,
        read_errors=read_errors,
        rows=int(len(combined)),
        elapsed_seconds=time.perf_counter() - started,
        errors=tuple(errors),
    )
    return combined, stats


def load_bar_directory_through_slot(
    data_dir: str | Path,
    symbols: Iterable[str],
    slot_ist: object,
    *,
    history_bars: int,
    max_workers: int = 8,
) -> tuple[pd.DataFrame, BarLoadStats]:
    """Read bounded historical bars without retaining any row after a cutoff."""

    root = Path(data_dir)
    requested = normalise_symbols(symbols)
    started = time.perf_counter()
    frames: list[pd.DataFrame] = []
    errors: list[str] = []
    missing = 0
    read_errors = 0
    workers = max(1, min(int(max_workers), max(1, len(requested))))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="prefilter-replay-read") as executor:
        futures = {
            executor.submit(
                _read_symbol_bars_through_slot,
                root,
                ticker,
                slot_ist,
                history_bars,
            ): ticker
            for ticker in requested
        }
        for future in as_completed(futures):
            ticker, frame, error = future.result()
            if frame is not None:
                frames.append(frame)
                continue
            if error == "missing_file":
                missing += 1
            else:
                read_errors += 1
            if len(errors) < 100:
                errors.append(f"{ticker}:{error}")
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    stats = BarLoadStats(
        requested_symbols=len(requested),
        loaded_symbols=len(frames),
        missing_files=missing,
        read_errors=read_errors,
        rows=int(len(combined)),
        elapsed_seconds=time.perf_counter() - started,
        errors=tuple(errors),
    )
    return combined, stats


def validate_bar_snapshot(
    bars: pd.DataFrame,
    symbols: Iterable[str],
    slot_ist: str,
) -> None:
    expected = set(normalise_symbols(symbols))
    if bars is None or bars.empty:
        raise ValueError("bar snapshot is empty")
    time_column = "date" if "date" in bars.columns else "datetime" if "datetime" in bars.columns else None
    if time_column is None or "ticker" not in bars.columns:
        raise ValueError("bar snapshot lacks ticker/date columns")
    work = bars[["ticker", time_column]].copy()
    work["ticker"] = work["ticker"].astype(str).str.upper().str.strip()
    work["_ts"] = work[time_column].map(
        lambda value: (
            pd.Timestamp(value).tz_localize("Asia/Kolkata")
            if pd.Timestamp(value).tzinfo is None
            else pd.Timestamp(value).tz_convert("Asia/Kolkata")
        )
    )
    actual_symbols = set(work["ticker"])
    missing = sorted(expected - actual_symbols)
    if missing:
        raise ValueError(f"bar snapshot is missing {len(missing)} symbols; sample={missing[:10]}")
    expected_slot = pd.Timestamp(slot_ist)
    if expected_slot.tzinfo is None:
        expected_slot = expected_slot.tz_localize("Asia/Kolkata")
    else:
        expected_slot = expected_slot.tz_convert("Asia/Kolkata")
    last_by_ticker = work.groupby("ticker", sort=False)["_ts"].max()
    wrong = last_by_ticker.loc[~last_by_ticker.eq(expected_slot)]
    if not wrong.empty:
        sample = {ticker: value.isoformat() for ticker, value in wrong.head(10).items()}
        raise ValueError(
            f"bar snapshot has {len(wrong)} symbols not at final slot {expected_slot.isoformat()}; sample={sample}"
        )


def ensure_experimental_output_dir(path: str | Path) -> Path:
    target = Path(path).resolve()
    target_text = str(target).lower()
    if "experiment" not in target_text and "research" not in target_text:
        raise ValueError("output path must contain 'experiment' or 'research'")
    for forbidden in FORBIDDEN_OUTPUT_ROOTS:
        forbidden_resolved = forbidden.resolve()
        try:
            target.relative_to(forbidden_resolved)
        except ValueError:
            continue
        raise ValueError(f"refusing production output path: {target}")
    target.mkdir(parents=True, exist_ok=True)
    return target


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="") as temp:
            temp.write(content)
            temp.flush()
            os.fsync(temp.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def write_research_text(path: str | Path, content: str) -> str:
    target = Path(path).resolve()
    ensure_experimental_output_dir(target.parent)
    _atomic_write_text(target, content)
    return str(target)


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def write_shadow_outputs(
    output_dir: str | Path,
    manifest: dict[str, Any],
    full_ranking: pd.DataFrame,
) -> dict[str, str]:
    root = ensure_experimental_output_dir(output_dir)
    slot = pd.Timestamp(manifest["slot_ist"])
    slot_key = slot.strftime("%Y%m%d_%H%M")
    manifest_path = root / f"prefilter_candidates_{slot_key}.json"
    ranking_path = root / f"prefilter_ranking_{slot_key}.csv"
    latest_manifest = root / "latest_prefilter_candidates.json"
    latest_ranking = root / "latest_prefilter_ranking.csv"
    ranking_text = full_ranking.to_csv(index=False)
    published_manifest = dict(manifest)
    published_manifest["publication"] = {
        "ranking_path": str(ranking_path),
        "ranking_sha256": hashlib.sha256(ranking_text.encode("utf-8")).hexdigest(),
        "latest_ranking_path": str(latest_ranking),
    }
    manifest_text = json.dumps(published_manifest, indent=2, sort_keys=True, default=_json_default) + "\n"
    # Immutable slot data first, its completion manifest second.  Convenience
    # latest files follow the same ordering, with latest manifest written last.
    _atomic_write_text(ranking_path, ranking_text)
    _atomic_write_text(manifest_path, manifest_text)
    _atomic_write_text(latest_ranking, ranking_text)
    _atomic_write_text(latest_manifest, manifest_text)
    return {
        "manifest": str(manifest_path),
        "ranking": str(ranking_path),
        "latest_manifest": str(latest_manifest),
        "latest_ranking": str(latest_ranking),
    }


def dataclass_dict(value: Any) -> dict[str, Any]:
    return asdict(value)
