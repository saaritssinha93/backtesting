"""Immutable input provenance for the FNO hybrid backtests.

The historical files are monolithic Parquets that are updated in place.  A
directory name is therefore not an input identity.  This module resolves the
daily universe pointer to a dated file, fingerprints every mapped source file
once, and records an order-independent digest that can be used as a cache key.

File hashes are reused only while ``size`` and ``mtime_ns`` are unchanged.  A
changed file is streamed once; hashes are never calculated once per row.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import pyarrow.parquet as pq

import fno_oi_common as common
import fno_oi_hybrid_data as hybrid


SOURCE_INVENTORY_SCHEMA_VERSION = "fno_backtest_source_inventory_v1"
CACHE_MANIFEST_SCHEMA_VERSION = "fno_signal_cache_manifest_v2"
RUN_PROVENANCE_SCHEMA_VERSION = "fno_backtest_run_provenance_v1"
CURRENT_SOURCE_PROVENANCE_CLAIM = (
    "RECREATED_CURRENT_SOURCE_REPLAY_NOT_ORIGINAL_SELECTION_PROVENANCE"
)


def sha256_file(path: Path | str, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    """Stream one file into SHA-256 without loading it into memory."""

    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normal_path(path: Path | str) -> str:
    return str(Path(path).resolve())


def _single_master_date(frame: pd.DataFrame, path: Path) -> date:
    if "master_date" not in frame.columns:
        raise ValueError(f"Dated FNO universe has no master_date: {path}")
    values = pd.to_datetime(frame["master_date"], errors="coerce").dropna().dt.date
    observed = sorted(set(values.tolist()))
    if len(observed) != 1 or len(values) != len(frame):
        raise ValueError(
            f"Dated FNO universe must contain one complete master_date: "
            f"path={path}, observed={observed}"
        )
    return observed[0]


def resolve_dated_universe(
    *,
    universe_path: Path | str | None = None,
    universe_date: date | str | None = None,
    expected_file_sha256: str = "",
    expected_universe_sha256: str = "",
) -> tuple[Path, pd.DataFrame, dict[str, Any]]:
    """Resolve an immutable dated universe and verify its declared identity.

    With no explicit input, ``latest_near_month.parquet`` is read only to find
    its master date.  The returned/read backtest input is always the matching
    ``near_month_YYYY-MM-DD.parquet`` file, never the mutable latest pointer.
    """

    wanted_date = (
        pd.Timestamp(universe_date).date() if universe_date not in (None, "") else None
    )
    if universe_path is not None:
        path = Path(universe_path)
        if path.name.lower() == "latest_near_month.parquet":
            raise ValueError(
                "Backtests may not consume mutable latest_near_month.parquet; "
                "pass a dated near_month_YYYY-MM-DD.parquet file."
            )
    elif wanted_date is not None:
        path = common.universe_paths(wanted_date)[0]
    else:
        latest_path = common.UNIVERSE_DIR / "latest_near_month.parquet"
        if not latest_path.exists():
            raise FileNotFoundError(f"FNO universe pointer is missing: {latest_path}")
        latest = pd.read_parquet(latest_path)
        latest_date = _single_master_date(latest, latest_path)
        path = common.universe_paths(latest_date)[0]

    path = path.resolve()
    if not path.exists():
        raise FileNotFoundError(f"Dated FNO universe is missing: {path}")
    frame = pd.read_parquet(path)
    if frame.empty:
        raise ValueError(f"Dated FNO universe is empty: {path}")
    observed_date = _single_master_date(frame, path)
    if wanted_date is not None and observed_date != wanted_date:
        raise ValueError(
            f"FNO universe master_date mismatch: expected {wanted_date}, "
            f"observed {observed_date} in {path}"
        )
    expected_name = f"near_month_{observed_date.isoformat()}.parquet"
    if path.name.lower() != expected_name.lower():
        raise ValueError(
            f"FNO universe is not a canonical dated file: expected name "
            f"{expected_name}, observed {path.name}"
        )

    file_hash = sha256_file(path)
    semantic_hash = common.universe_sha256(frame)
    if expected_file_sha256 and file_hash != expected_file_sha256:
        raise AssertionError(
            "Dated FNO universe file hash changed: "
            f"expected {expected_file_sha256}, observed {file_hash}"
        )
    if expected_universe_sha256 and semantic_hash != expected_universe_sha256:
        raise AssertionError(
            "Dated FNO universe semantic hash changed: "
            f"expected {expected_universe_sha256}, observed {semantic_hash}"
        )
    record = {
        "path": str(path),
        "master_date": observed_date.isoformat(),
        "rows": int(len(frame)),
        "file_sha256": file_hash,
        "universe_sha256": semantic_hash,
    }
    return path, frame, record


def load_backtest_universe(
    *,
    universe_path: Path | str | None = None,
    universe_date: date | str | None = None,
    contract_month_contains: str = "26AUG",
    require_persisted_mapping: bool = False,
    expected_file_sha256: str = "",
    expected_universe_sha256: str = "",
    expected_mapped_universe_sha256: str = "",
    expected_mapped_symbol_set_sha256: str = "",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load the exact mapped stock-futures universe used by a backtest."""

    path, full, universe_record = resolve_dated_universe(
        universe_path=universe_path,
        universe_date=universe_date,
        expected_file_sha256=expected_file_sha256,
        expected_universe_sha256=expected_universe_sha256,
    )
    selected = full.copy()
    if contract_month_contains:
        selected = selected.loc[
            selected["tradingsymbol"].astype(str).str.contains(
                str(contract_month_contains), case=False, na=False
            )
        ].copy()
    if selected.empty:
        raise ValueError(
            f"Dated FNO universe has no contracts matching {contract_month_contains!r}: {path}"
        )

    index_mask = selected["is_index_future"].fillna(False).astype(bool)
    stocks = selected.loc[~index_mask].copy()
    if require_persisted_mapping:
        required = {
            "equity_symbol",
            "equity_instrument_token",
            "futures_tradingsymbol",
            "futures_instrument_token",
        }
        missing_columns = required - set(stocks.columns)
        if missing_columns:
            raise ValueError(
                "Promoted V6 requires persisted futures/equity mappings; missing "
                f"columns: {sorted(missing_columns)}"
            )
        missing_rows: list[str] = []
        for row in stocks.to_dict("records"):
            raw_equity_symbol = row.get("equity_symbol")
            raw_futures_symbol = row.get("futures_tradingsymbol")
            equity_symbol = (
                "" if pd.isna(raw_equity_symbol) else str(raw_equity_symbol).strip()
            )
            futures_symbol = (
                "" if pd.isna(raw_futures_symbol) else str(raw_futures_symbol).strip()
            )
            try:
                equity_token = int(row.get("equity_instrument_token"))
                futures_token = int(row.get("futures_instrument_token"))
            except (TypeError, ValueError):
                equity_token = futures_token = 0
            if not equity_symbol or not futures_symbol or equity_token <= 0 or futures_token <= 0:
                missing_rows.append(str(row.get("tradingsymbol") or row.get("underlying")))
        if missing_rows:
            raise ValueError(
                "Promoted V6 refuses legacy token-cache mapping fallback: "
                f"{sorted(missing_rows)[:20]}"
            )

    mapped, excluded = hybrid.ensure_equity_mapping(selected)
    unexpected = (
        excluded.loc[excluded["reason"].ne("INDEX_FUTURE_HAS_NO_CASH_EQUITY")]
        if not excluded.empty
        else excluded
    )
    if not unexpected.empty:
        raise ValueError(
            "Backtest stock-future equity mapping is incomplete: "
            f"{unexpected.head(20).to_dict('records')}"
        )
    if len(mapped) != len(stocks):
        raise ValueError(
            "Backtest mapped-stock count mismatch: "
            f"expected {len(stocks)}, observed {len(mapped)}"
        )

    mapped_semantic_hash = common.universe_sha256(mapped)
    mapped_symbol_hash = common.symbol_set_sha256(mapped["futures_tradingsymbol"])
    if (
        expected_mapped_universe_sha256
        and mapped_semantic_hash != expected_mapped_universe_sha256
    ):
        raise AssertionError(
            "Mapped stock-futures universe hash changed: "
            f"expected {expected_mapped_universe_sha256}, observed {mapped_semantic_hash}"
        )
    if (
        expected_mapped_symbol_set_sha256
        and mapped_symbol_hash != expected_mapped_symbol_set_sha256
    ):
        raise AssertionError(
            "Mapped stock-futures symbol set changed: "
            f"expected {expected_mapped_symbol_set_sha256}, observed {mapped_symbol_hash}"
        )
    universe_record.update(
        {
            "contract_month_filter": str(contract_month_contains),
            "selected_contracts": int(len(selected)),
            "mapped_stock_futures": int(len(mapped)),
            "excluded_index_futures": int(
                excluded["reason"].eq("INDEX_FUTURE_HAS_NO_CASH_EQUITY").sum()
            )
            if not excluded.empty
            else 0,
            "mapping_source": (
                "PERSISTED_DATED_UNIVERSE_ONLY"
                if require_persisted_mapping
                else "PERSISTED_WITH_LEGACY_FALLBACK_ALLOWED"
            ),
            "mapped_universe_sha256": mapped_semantic_hash,
            "mapped_symbol_set_sha256": mapped_symbol_hash,
        }
    )
    return mapped.reset_index(drop=True), universe_record


def _entry_identity(entry: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(entry.get("role", "")),
        str(entry.get("logical_symbol", "")),
        str(entry.get("resolved_path", "")),
    )


def _source_entry(
    *,
    role: str,
    logical_symbol: str,
    path: Path,
    previous: Mapping[str, Any] | None,
) -> dict[str, Any]:
    resolved = path.resolve()
    base = {
        "role": str(role),
        "logical_symbol": str(logical_symbol).upper().strip(),
        "resolved_path": str(resolved),
    }
    if not resolved.exists():
        return {
            **base,
            "exists": False,
            "size": None,
            "mtime_ns": None,
            "sha256": "",
        }

    before = resolved.stat()
    if (
        previous
        and bool(previous.get("exists"))
        and int(previous.get("size", -1)) == int(before.st_size)
        and int(previous.get("mtime_ns", -1)) == int(before.st_mtime_ns)
        and str(previous.get("sha256", ""))
    ):
        file_hash = str(previous["sha256"])
    else:
        file_hash = sha256_file(resolved)
    after = resolved.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise RuntimeError(f"Backtest source changed while fingerprinting: {resolved}")
    return {
        **base,
        "exists": True,
        "size": int(after.st_size),
        "mtime_ns": int(after.st_mtime_ns),
        "sha256": file_hash,
    }


def build_source_inventory(
    mapped_universe: pd.DataFrame,
    universe_record: Mapping[str, Any],
    *,
    previous_inventory: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Fingerprint the exact mapped futures/equity source-file inventory."""

    previous_entries = {
        _entry_identity(item): item
        for item in (previous_inventory or {}).get("entries", [])
        if isinstance(item, Mapping)
    }
    specifications: list[tuple[str, str, Path]] = []
    for row in mapped_universe.to_dict("records"):
        futures_symbol = str(row["futures_tradingsymbol"]).upper().strip()
        equity_symbol = hybrid.resolve_backtest_equity_symbol(
            str(row["equity_symbol"])
        ).upper().strip()
        specifications.append(
            ("NFO_FUTURES_5M", futures_symbol, common.raw_contract_path(futures_symbol))
        )
        specifications.append(
            (
                "NSE_EQUITY_1M",
                equity_symbol,
                hybrid.equity_one_minute_path(
                    equity_symbol, hybrid.DEFAULT_BACKTEST_EQUITY_1M_DIR
                ),
            )
        )
    specifications.sort(key=lambda item: (item[0], item[1], _normal_path(item[2])))

    entries: list[dict[str, Any]] = []
    for role, symbol, path in specifications:
        identity = (role, symbol, _normal_path(path))
        entries.append(
            _source_entry(
                role=role,
                logical_symbol=symbol,
                path=path,
                previous=previous_entries.get(identity),
            )
        )

    digest_entries = [
        {
            "role": entry["role"],
            "logical_symbol": entry["logical_symbol"],
            "resolved_path": entry["resolved_path"],
            "exists": entry["exists"],
            "size": entry["size"],
            "sha256": entry["sha256"],
        }
        for entry in entries
    ]
    inventory_hash = common.canonical_json_sha256(digest_entries)
    source_fingerprint = common.canonical_json_sha256(
        {
            "universe_file_sha256": universe_record.get("file_sha256", ""),
            "universe_sha256": universe_record.get("universe_sha256", ""),
            "mapped_universe_sha256": universe_record.get(
                "mapped_universe_sha256", ""
            ),
            "mapped_symbol_set_sha256": universe_record.get(
                "mapped_symbol_set_sha256", ""
            ),
            "inventory_sha256": inventory_hash,
        }
    )
    return {
        "schema_version": SOURCE_INVENTORY_SCHEMA_VERSION,
        "inventory_scope": (
            "WHOLE_FILE_BYTES_FOR_EACH_MAPPED_SOURCE; NOT DATE SLICED; "
            "MAY INCLUDE ROWS AFTER A BACKTEST --through-day CUTOFF"
        ),
        "date_sliced": False,
        "entry_count": int(len(entries)),
        "existing_count": int(sum(bool(entry["exists"]) for entry in entries)),
        "missing_count": int(sum(not bool(entry["exists"]) for entry in entries)),
        "total_bytes": int(
            sum(int(entry["size"] or 0) for entry in entries if entry["exists"])
        ),
        "inventory_sha256": inventory_hash,
        "source_fingerprint": source_fingerprint,
        "entries": entries,
    }


def validate_source_inventory_readable(inventory: Mapping[str, Any]) -> None:
    """Fail closed unless every declared promoted source is a usable Parquet."""

    required_columns = {
        "NFO_FUTURES_5M": {
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "oi",
        },
        "NSE_EQUITY_1M": {"date", "open", "high", "low", "close", "volume"},
    }
    failures: list[str] = []
    for entry in inventory.get("entries", []):
        if not bool(entry.get("exists")):
            failures.append(
                f"{entry.get('role')}:{entry.get('logical_symbol')}:MISSING"
            )
            continue
        path = Path(str(entry.get("resolved_path", "")))
        try:
            parquet = pq.ParquetFile(path)
            available = set(parquet.schema.names)
        except Exception as exc:
            failures.append(
                f"{entry.get('role')}:{entry.get('logical_symbol')}:"
                f"{type(exc).__name__}"
            )
            continue
        if parquet.metadata.num_rows <= 0:
            failures.append(
                f"{entry.get('role')}:{entry.get('logical_symbol')}:ZERO_ROWS"
            )
            continue
        missing = required_columns.get(str(entry.get("role")), set()) - available
        if missing:
            failures.append(
                f"{entry.get('role')}:{entry.get('logical_symbol')}:"
                f"MISSING_COLUMNS={','.join(sorted(missing))}"
            )
    if failures:
        raise RuntimeError(
            "Promoted V6 source inventory contains missing, unreadable, or invalid "
            f"Parquets: {failures[:20]}"
        )


def artifact_record(path: Path | str) -> dict[str, Any]:
    resolved = Path(path).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Backtest cache artifact is missing: {resolved}")
    stat = resolved.stat()
    return {
        "path": str(resolved),
        "size": int(stat.st_size),
        "sha256": sha256_file(resolved),
    }


def artifact_matches(path: Path | str, record: Mapping[str, Any]) -> bool:
    resolved = Path(path).resolve()
    if not resolved.exists():
        return False
    stat = resolved.stat()
    if int(record.get("size", -1)) != int(stat.st_size):
        return False
    return str(record.get("sha256", "")) == sha256_file(resolved)


def write_immutable_json(path: Path | str, payload: Mapping[str, Any]) -> Path:
    """Create an immutable JSON artifact; never replace different contents."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True, default=str)
        + "\n"
    ).encode("utf-8")
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{target.name}.",
            suffix=".tmp",
            dir=str(target.parent),
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            # A hard-link publish is atomic and refuses to replace an existing
            # immutable artifact, including under concurrent writers.
            os.link(temp_path, target)
        except FileExistsError:
            if target.read_bytes() != encoded:
                raise FileExistsError(
                    "Immutable backtest provenance already exists with different "
                    f"contents: {target}"
                )
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
    return target


def publish_immutable_copy(
    source: Path | str,
    target: Path | str,
    *,
    expected_sha256: str = "",
    chunk_size: int = 8 * 1024 * 1024,
) -> Path:
    """Atomically publish a byte-for-byte immutable copy without replacement."""

    source_path = Path(source)
    target_path = Path(target)
    if not source_path.exists():
        raise FileNotFoundError(f"Immutable publication source is missing: {source_path}")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Path | None = None
    try:
        digest = hashlib.sha256()
        with source_path.open("rb") as source_handle, tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{target_path.name}.",
            suffix=".tmp",
            dir=str(target_path.parent),
            delete=False,
        ) as target_handle:
            temp_path = Path(target_handle.name)
            for chunk in iter(lambda: source_handle.read(chunk_size), b""):
                digest.update(chunk)
                target_handle.write(chunk)
            target_handle.flush()
            os.fsync(target_handle.fileno())
        copied_hash = digest.hexdigest()
        if expected_sha256 and copied_hash != expected_sha256:
            raise AssertionError(
                "Immutable publication source hash changed: "
                f"expected {expected_sha256}, observed {copied_hash}"
            )
        try:
            os.link(temp_path, target_path)
        except FileExistsError:
            existing_hash = sha256_file(target_path)
            if existing_hash != copied_hash:
                raise FileExistsError(
                    "Immutable artifact already exists with different contents: "
                    f"{target_path}"
                )
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink()
            except FileNotFoundError:
                pass
    return target_path


def backtest_input_fingerprint(
    cache_manifest: Mapping[str, Any],
    *,
    strategy_payload: Mapping[str, Any],
    parameters: Mapping[str, Any],
) -> str:
    return common.canonical_json_sha256(
        {
            "cache_input_fingerprint": cache_manifest.get("input_fingerprint", ""),
            "strategy": dict(strategy_payload),
            "parameters": dict(parameters),
        }
    )


def build_run_provenance(
    *,
    generated_at: datetime,
    strategy_version: str,
    objective: str,
    strategy_payload: Mapping[str, Any],
    parameters: Mapping[str, Any],
    backtest_window: Mapping[str, Any],
    cache_manifest_path: Path | str,
    cache_manifest: Mapping[str, Any],
    output_paths: Mapping[str, Path | str],
    results: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the immutable provenance payload after all outputs exist."""

    fingerprint = backtest_input_fingerprint(
        cache_manifest,
        strategy_payload=strategy_payload,
        parameters=parameters,
    )
    outputs = {
        str(name): artifact_record(path) for name, path in sorted(output_paths.items())
    }
    return {
        "schema_version": RUN_PROVENANCE_SCHEMA_VERSION,
        "generated_at_ist": generated_at.isoformat(timespec="microseconds"),
        "provenance_claim": CURRENT_SOURCE_PROVENANCE_CLAIM,
        "original_selection_source_provenance_available": False,
        "source_inventory_scope": (
            "WHOLE_FILE_BYTES; NOT DATE SLICED; MAY INCLUDE ROWS AFTER THE "
            "DECLARED BACKTEST WINDOW"
        ),
        "strategy_version": str(strategy_version),
        "objective": str(objective),
        "strategy_payload": dict(strategy_payload),
        "backtest_input_fingerprint": fingerprint,
        "parameters": dict(parameters),
        "backtest_window": dict(backtest_window),
        "cache_manifest_path": _normal_path(cache_manifest_path),
        "cache_input_fingerprint": cache_manifest.get("input_fingerprint", ""),
        "universe": dict(cache_manifest.get("universe", {})),
        "source_inventory": dict(cache_manifest.get("source_inventory", {})),
        "cache_artifacts": dict(cache_manifest.get("artifacts", {})),
        "results": dict(results or {}),
        "outputs": outputs,
    }
