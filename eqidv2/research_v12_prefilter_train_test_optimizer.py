"""Standalone V12 hourly-prefilter train/test optimizer.

This module is intentionally outside the V7 live and V11/V12 production paths.
It consumes already hourly-prefiltered candidate CSVs, builds an *ungated*
executable-entry pool with the exact V12 entry helpers, searches only the
chronological TRAIN window, freezes a proposed setup book, and evaluates the
TEST window once.

Safety / honesty invariants
--------------------------
* ``final_setup_conf_v12.py`` is read-only.  The proposed book is a new module.
* TRAIN is 2026-06-04..2026-07-06; TEST is 2026-07-07..2026-08-03.
* The authoritative session calendar is used, including zero-trade sessions.
* Test rows never participate in this run's thresholds, trial scoring, or
  portfolio search.  Because the source V12 book was assembled after the test
  dates and may reflect earlier inspection of 2026-08-03, this is explicitly a
  chronological validation window, not a virgin out-of-sample holdout.
* Every original mask/pre-momentum term and operator is retained.
* Every original entry-guard key is retained.  Numeric/time guards may only be
  relaxed in their permissive direction.
* Masks, pre-momentum gates, and per-slot Top-N run before the exact V12
  ticker/day de-duplication.
* Stops are a coarse, deterministic grid.  Risk-normalised pre-momentum
  features are recomputed for every stop width instead of being reused under a
  different risk denominator.
* Setups with too little TRAIN evidence keep the current V12 configuration.

The expensive run is deliberately opt-in; importing this file has no side
effects.  See ``--help`` for inputs and output artifacts.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import itertools
import json
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


TRAIN_START = "2026-06-04"
TRAIN_END = "2026-07-06"
TEST_START = "2026-07-07"
TEST_END = "2026-08-03"

DEFAULT_ROOT = Path(
    r"C:\TradingData\eqidv2_experiments\v12_prefilter_2mo_20260604_20260803_k300"
)
DEFAULT_CANDIDATE_CSVS = (
    DEFAULT_ROOT
    / "chunk_20260604_20260706"
    / "historical_all_available_pre_dedupe_live_candidates.csv",
    DEFAULT_ROOT
    / "chunk_20260707_20260803"
    / "historical_all_available_pre_dedupe_live_candidates.csv",
)
DEFAULT_SESSION_CALENDAR = DEFAULT_ROOT / "combined" / "calendar_daily.csv"
DEFAULT_OUT = Path(
    r"C:\TradingData\eqidv2_experiments"
    r"\v12_prefilter_train_test_optimizer_20260604_20260803"
)

TARGET_MIN_TRADES_PER_SESSION = 10.0
TARGET_MAX_TRADES_PER_SESSION = 15.0
DEFAULT_MIN_SEARCHABLE_ROWS = 16
DEFAULT_MIN_SEARCHABLE_DAYS = 5


def install_windowed_1m_loader(
    v12: Any,
    *,
    start_date: str = TRAIN_START,
    end_date: str = TEST_END,
) -> Any:
    """Install a process-local, date-filtered historical 1-minute cache.

    The canonical V12 helper caches each symbol's entire multi-month file and
    merges current live-raw bars.  This standalone replay needs only the fixed
    train/test window; filtering at Parquet read time cuts memory sharply and
    prevents current live rows from entering the historical experiment.
    """
    start = pd.Timestamp(start_date, tz="Asia/Kolkata")
    end = pd.Timestamp(end_date, tz="Asia/Kolkata") + pd.Timedelta(days=1)
    columns = ["date", "open", "high", "low", "close", "volume", "ADX", "RSI"]

    @lru_cache(maxsize=None)
    def load(ticker: str) -> pd.DataFrame | None:
        path = v12.v6.DATA_1M_DIR / f"{str(ticker).upper()}_stocks_indicators_1min.parquet"
        if not path.exists():
            return None
        filters = [("date", ">=", start), ("date", "<", end)]
        try:
            frame = pd.read_parquet(path, columns=columns, filters=filters)
        except Exception:
            try:
                frame = pd.read_parquet(path, filters=filters)
            except Exception:
                try:
                    frame = pd.read_parquet(path, columns=columns)
                except Exception:
                    try:
                        frame = pd.read_parquet(path)
                    except Exception:
                        return None
        if frame is None or frame.empty:
            return None
        normalised = v12._normalise_bars_date_index(frame, naive_tz="UTC")
        if normalised is None or normalised.empty:
            return None
        keep = (normalised.index >= start) & (normalised.index < end)
        out = normalised.loc[keep].copy()
        return out if not out.empty else None

    old_loader = getattr(v12, "_load_1m_with_open", None)
    if hasattr(old_loader, "cache_clear"):
        old_loader.cache_clear()
    v12._load_1m_with_open = load
    return load


def prewarm_windowed_1m_loader(
    loader: Any,
    tickers: Iterable[str],
    *,
    workers: int = 8,
) -> dict[str, int]:
    """Load each required two-month symbol once, concurrently and read-only."""
    symbols = sorted(
        {str(ticker).upper().strip() for ticker in tickers if str(ticker).strip()}
    )
    loaded = 0
    missing = 0
    failed = 0

    def one(symbol: str) -> tuple[str, bool, str]:
        try:
            frame = loader(symbol)
            return symbol, frame is not None and not frame.empty, ""
        except Exception as exc:  # pragma: no cover - defensive worker boundary
            return symbol, False, f"{type(exc).__name__}: {exc}"

    max_workers = max(1, int(workers))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(one, symbol): symbol for symbol in symbols}
        for done, future in enumerate(as_completed(futures), 1):
            _, ok, error = future.result()
            if ok:
                loaded += 1
            elif error:
                failed += 1
            else:
                missing += 1
            if done % 100 == 0 or done == len(symbols):
                print(
                    f"[optimizer prewarm] {done:,}/{len(symbols):,} "
                    f"loaded={loaded:,} missing={missing:,} failed={failed:,}",
                    flush=True,
                )
    if failed:
        raise ContractError(f"windowed 1-minute prewarm failed for {failed} symbols")
    return {
        "requested": len(symbols),
        "loaded": loaded,
        "missing": missing,
        "failed": failed,
        "workers": max_workers,
    }


def install_day_1m_adapter(v12: Any, full_loader: Any) -> Any:
    """Install exact cached session slices for momentum and exit resolution."""
    old_entry_bars = v12._entry_bars_for_signal

    @lru_cache(maxsize=None)
    def load_day(ticker: str, trade_day: str) -> pd.DataFrame | None:
        frame = full_loader(str(ticker).upper().strip())
        if frame is None or frame.empty:
            return None
        start = pd.Timestamp(str(trade_day), tz="Asia/Kolkata")
        end = start + pd.Timedelta(days=1)
        if frame.index.is_monotonic_increasing:
            left = int(frame.index.searchsorted(start, side="left"))
            right = int(frame.index.searchsorted(end, side="left"))
            sliced = frame.iloc[left:right]
        else:  # defensive parity fallback for non-canonical inputs
            sliced = frame.loc[(frame.index >= start) & (frame.index < end)]
        return sliced if not sliced.empty else None

    def entry_bars_for_signal(
        ticker: str, signal_ts: pd.Timestamp
    ) -> tuple[pd.DataFrame | None, str]:
        if bool(getattr(v12, "_V11_EXACT_LIVE_PARITY", False)):
            return old_entry_bars(ticker, signal_ts)
        signal = v12._normalise_ts(signal_ts)
        if pd.isna(signal):
            return None, "invalid_signal_time"
        return load_day(ticker, str(signal.date())), "historical_1min_day_slice"

    v12._entry_bars_for_signal = entry_bars_for_signal
    v12._optimizer_load_1m_day = load_day
    return load_day


class ContractError(ValueError):
    """Raised when an honesty or structure invariant is violated."""


@dataclass(frozen=True)
class SplitContract:
    train_start: str = TRAIN_START
    train_end: str = TRAIN_END
    test_start: str = TEST_START
    test_end: str = TEST_END

    def validate(self) -> None:
        ts = pd.Timestamp(self.train_start)
        te = pd.Timestamp(self.train_end)
        hs = pd.Timestamp(self.test_start)
        he = pd.Timestamp(self.test_end)
        if not (ts <= te < hs <= he):
            raise ContractError(
                "split must be chronological and disjoint: "
                f"train={self.train_start}..{self.train_end}, "
                f"test={self.test_start}..{self.test_end}"
            )


@dataclass
class TrialChoice:
    setup: str
    trial_id: str
    config: dict[str, Any]
    metrics: dict[str, Any]
    filtered: pd.DataFrame | None = None
    filtered_row_ids: tuple[int, ...] = ()
    filter_signature: str = ""
    status: str = "searched"


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
        return value if math.isfinite(value) else str(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if pd.isna(value) if not isinstance(value, (str, bytes)) else False:
        return None
    return value


def _stable_json(value: Any) -> str:
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _trial_id(config: Mapping[str, Any]) -> str:
    return hashlib.sha256(_stable_json(config).encode("utf-8")).hexdigest()[:16]


def _filter_signature(config: Mapping[str, Any]) -> str:
    """Identify identical eligibility work; target affects outcomes, not entry."""
    payload = {
        "sl_pct": float(config["exit"]["sl_pct"]),
        "mask_terms": config.get("mask_terms", []),
        "pre_momentum_terms": config.get("pre_momentum_terms", []),
        "entry_guards": config.get("entry_guards", {}),
    }
    return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()[:20]


def _slot_minute(value: object) -> int:
    hh, mm = str(value).split(":")
    return int(hh) * 60 + int(mm)


def _minute_slot(value: int | float) -> str:
    minute = int(round(float(value) / 5.0) * 5)
    minute = max(0, min(23 * 60 + 59, minute))
    return f"{minute // 60:02d}:{minute % 60:02d}"


def _numeric(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def original_constraints(cfg: Mapping[str, Any]) -> dict[str, Any]:
    """Return the immutable constraint anchor carried by the V12 book."""
    source = cfg.get("v12_original_constraints")
    if not isinstance(source, Mapping):
        source = {
            "mask_terms": cfg.get("mask_terms", []),
            "pre_momentum_terms": cfg.get("pre_momentum_terms", []),
            "entry_guards": cfg.get("entry_guards", {}),
        }
    return {
        "mask_terms": copy.deepcopy(list(source.get("mask_terms", []))),
        "pre_momentum_terms": copy.deepcopy(
            list(source.get("pre_momentum_terms", []))
        ),
        "entry_guards": copy.deepcopy(dict(source.get("entry_guards", {}))),
    }


def _validate_term_list(
    setup: str,
    label: str,
    original: Sequence[Sequence[Any]],
    proposed: Sequence[Sequence[Any]],
) -> None:
    if len(proposed) != len(original):
        raise ContractError(
            f"{setup} {label}: term count changed {len(original)} -> {len(proposed)}"
        )
    for index, (old, new) in enumerate(zip(original, proposed)):
        if len(old) < 3 or len(new) < 3:
            raise ContractError(f"{setup} {label}[{index}]: malformed term")
        old_feature, old_op, old_value = old[:3]
        new_feature, new_op, new_value = new[:3]
        if str(new_feature) != str(old_feature) or str(new_op) != str(old_op):
            raise ContractError(
                f"{setup} {label}[{index}]: feature/operator changed "
                f"{old_feature} {old_op} -> {new_feature} {new_op}"
            )
        old_num = _numeric(old_value)
        new_num = _numeric(new_value)
        if old_num is None or new_num is None:
            if new_value != old_value:
                raise ContractError(
                    f"{setup} {label}[{index}]: categorical value changed"
                )
            continue
        if old_op == ">=" and new_num > old_num + 1e-12:
            raise ContractError(
                f"{setup} {label}[{index}]: >= threshold tightened "
                f"{old_num} -> {new_num}"
            )
        if old_op == "<=" and new_num < old_num - 1e-12:
            raise ContractError(
                f"{setup} {label}[{index}]: <= threshold tightened "
                f"{old_num} -> {new_num}"
            )
        if old_op not in {">=", "<="} and new_num != old_num:
            raise ContractError(
                f"{setup} {label}[{index}]: non-relaxable threshold changed"
            )


def validate_constraint_relaxation(
    setup: str,
    original: Mapping[str, Any],
    proposed: Mapping[str, Any],
) -> None:
    """Fail if a proposed setup removes or tightens an original condition."""
    _validate_term_list(
        setup,
        "mask_terms",
        list(original.get("mask_terms", [])),
        list(proposed.get("mask_terms", [])),
    )
    _validate_term_list(
        setup,
        "pre_momentum_terms",
        list(original.get("pre_momentum_terms", [])),
        list(proposed.get("pre_momentum_terms", [])),
    )
    old_guards = dict(original.get("entry_guards", {}))
    new_guards = dict(proposed.get("entry_guards", {}))
    if set(new_guards) != set(old_guards):
        raise ContractError(
            f"{setup} entry_guards keys changed: "
            f"{sorted(old_guards)} -> {sorted(new_guards)}"
        )
    for key, old_value in old_guards.items():
        new_value = new_guards[key]
        if key == "min_slot":
            if _slot_minute(new_value) > _slot_minute(old_value):
                raise ContractError(f"{setup} min_slot tightened")
        elif key == "max_slot":
            if _slot_minute(new_value) < _slot_minute(old_value):
                raise ContractError(f"{setup} max_slot tightened")
        elif key == "top_n":
            if int(new_value) < int(old_value):
                raise ContractError(f"{setup} top_n tightened")
        elif new_value != old_value:
            # Exclusion windows and unknown guards remain intact.  Their exact
            # monotonic relaxation is ambiguous, so the optimizer never mines it.
            raise ContractError(f"{setup} non-relaxable guard {key!r} changed")


def split_frame(
    frame: pd.DataFrame,
    *,
    date_column: str,
    contract: SplitContract = SplitContract(),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Perform and verify the immutable chronological split."""
    contract.validate()
    if date_column not in frame.columns:
        raise ContractError(f"missing split date column {date_column!r}")
    dates = pd.to_datetime(frame[date_column], errors="coerce").dt.strftime("%Y-%m-%d")
    train_mask = dates.between(contract.train_start, contract.train_end)
    test_mask = dates.between(contract.test_start, contract.test_end)
    train = frame.loc[train_mask].copy().reset_index(drop=True)
    test = frame.loc[test_mask].copy().reset_index(drop=True)
    train_dates = set(dates.loc[train_mask].dropna())
    test_dates = set(dates.loc[test_mask].dropna())
    overlap = train_dates & test_dates
    if overlap:
        raise ContractError(f"train/test overlap: {sorted(overlap)}")
    if train_dates and max(train_dates) > contract.train_end:
        raise ContractError("training rows escaped training boundary")
    if test_dates and min(test_dates) < contract.test_start:
        raise ContractError("test rows escaped holdout boundary")
    return train, test


def _read_sessions(path: Path, contract: SplitContract) -> tuple[list[str], list[str]]:
    if not path.exists():
        raise FileNotFoundError(
            f"authoritative session calendar not found: {path}; "
            "zero-trade sessions must not be inferred away"
        )
    calendar = pd.read_csv(path, low_memory=False)
    date_col = next(
        (name for name in ("date", "trade_date", "session_date") if name in calendar),
        None,
    )
    if date_col is None:
        raise ContractError(f"session calendar {path} has no date column")
    dates = sorted(
        set(pd.to_datetime(calendar[date_col], errors="coerce").dropna().dt.strftime("%Y-%m-%d"))
    )
    train = [d for d in dates if contract.train_start <= d <= contract.train_end]
    test = [d for d in dates if contract.test_start <= d <= contract.test_end]
    if not train or not test:
        raise ContractError(
            f"calendar must cover both windows; got train={len(train)}, test={len(test)}"
        )
    if set(train) & set(test):
        raise ContractError("authoritative calendar has overlapping split sessions")
    return train, test


def _candidate_day(frame: pd.DataFrame) -> pd.Series:
    for column in (
        "signal_time_ist",
        "signal_datetime",
        "bar_time_ist",
        "scan_slot_ist",
        "v11_source_day",
    ):
        if column in frame.columns:
            values = pd.to_datetime(frame[column], errors="coerce")
            if values.notna().any():
                return values.dt.strftime("%Y-%m-%d")
    raise ContractError("candidate input has no usable signal date column")


def load_candidates(
    paths: Sequence[Path],
    supplemental_paths: Sequence[Path],
    active_setups: Iterable[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    records: list[pd.DataFrame] = []
    audit: list[dict[str, Any]] = []
    active = set(active_setups)
    for source_kind, items in (
        ("prefilter_pre_dedupe", paths),
        ("supplemental_missing_setup", supplemental_paths),
    ):
        for path in items:
            if not path.exists():
                raise FileNotFoundError(path)
            frame = pd.read_csv(path, low_memory=False)
            required = {"ticker", "side", "setup"}
            missing = sorted(required - set(frame.columns))
            if missing:
                raise ContractError(f"{path}: missing columns {missing}")
            before = len(frame)
            frame = frame.loc[frame["setup"].astype(str).isin(active)].copy()
            frame["_optimizer_source_csv"] = str(path)
            frame["_optimizer_source_kind"] = source_kind
            frame["_optimizer_source_order"] = np.arange(len(frame), dtype=int)
            records.append(frame)
            audit.append(
                {
                    "source_kind": source_kind,
                    "path": str(path),
                    "sha256": _sha256_file(path),
                    "rows_total": before,
                    "rows_active_setup": len(frame),
                }
            )
    if not records:
        raise ContractError("no candidate CSVs supplied")
    combined = pd.concat(records, ignore_index=True, sort=False)
    combined["_optimizer_signal_day"] = _candidate_day(combined)
    dedupe = [
        col
        for col in ("candidate_id", "ticker", "side", "setup", "signal_time_ist")
        if col in combined.columns
    ]
    if "candidate_id" in dedupe:
        dedupe = ["candidate_id"]
    elif len(dedupe) < 4:
        raise ContractError("candidate input lacks a safe de-duplication key")
    before_dedupe = len(combined)
    combined = combined.drop_duplicates(subset=dedupe, keep="first").reset_index(drop=True)
    audit.append(
        {
            "source_kind": "combined",
            "path": "",
            "sha256": "",
            "rows_total": before_dedupe,
            "rows_active_setup": len(combined),
            "dedupe_key": ",".join(dedupe),
        }
    )
    return combined, pd.DataFrame(audit)


def apply_prefilter_rank_band(
    candidates: pd.DataFrame,
    *,
    min_rank: int | None,
    max_rank: int | None,
) -> tuple[pd.DataFrame, dict[str, Any] | None]:
    """Apply a frozen hourly-prefilter rank band before any V12 computation.

    Missing/non-numeric ranks fail closed whenever a bound is requested.  This
    remains a prefilter-universe operation; it does not alter or remove any V12
    setup mask, pre-momentum condition, or entry guard.
    """
    if min_rank is None and max_rank is None:
        return candidates.copy(), None
    if min_rank is not None and min_rank < 1:
        raise ContractError("prefilter rank minimum must be at least 1")
    if max_rank is not None and max_rank < 1:
        raise ContractError("prefilter rank maximum must be at least 1")
    if min_rank is not None and max_rank is not None and min_rank > max_rank:
        raise ContractError("prefilter rank minimum cannot exceed maximum")
    if "prefilter_selection_rank" not in candidates.columns:
        raise ContractError(
            "prefilter rank band requested but candidates have no "
            "prefilter_selection_rank column"
        )
    ranks = pd.to_numeric(candidates["prefilter_selection_rank"], errors="coerce")
    mask = ranks.notna()
    if min_rank is not None:
        mask &= ranks >= int(min_rank)
    if max_rank is not None:
        mask &= ranks <= int(max_rank)
    selected = candidates.loc[mask].copy().reset_index(drop=True)
    audit = {
        "source_kind": "prefilter_rank_band",
        "path": "",
        "sha256": "",
        "rows_total": int(len(candidates)),
        "rows_active_setup": int(len(selected)),
        "dedupe_key": "",
        "min_rank": min_rank,
        "max_rank": max_rank,
        "rows_missing_or_non_numeric_rank": int(ranks.isna().sum()),
    }
    return selected, audit


def build_ungated_executable_pool(
    candidates: pd.DataFrame,
    v12: Any,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Use exact V12 entry helpers while bypassing only pre-momentum gates."""
    conf = v12._activate_final_setup_conf()
    active = set(conf)
    scoped = candidates.loc[candidates["setup"].astype(str).isin(active)].copy()
    saved_gates = v12.PRE_ENTRY_MOMENTUM_SETUP_GATES
    try:
        v12.PRE_ENTRY_MOMENTUM_SETUP_GATES = {}
        raw, rejects = v12._v7_entry_engine_raw_rows(scoped)
    finally:
        v12.PRE_ENTRY_MOMENTUM_SETUP_GATES = saved_gates
    if raw.empty:
        return raw, rejects
    raw = v12._selected_strategy_features(raw)
    raw["_optimizer_row_id"] = np.arange(len(raw), dtype=int)
    raw["_optimizer_signal_day"] = raw["signal_day"].astype(str)
    return raw.reset_index(drop=True), rejects.reset_index(drop=True)


def _exit_grid(exit_cfg: Mapping[str, Any]) -> list[tuple[float, float]]:
    """Small deterministic grid around the existing exit; never data-derived."""
    base_sl = float(exit_cfg["sl_pct"])
    base_tgt = float(exit_cfg["tgt_pct"])

    def levels(value: float, floor: float, ceiling: float) -> list[float]:
        vals = {
            round(value, 4),
            round(max(floor, value * 0.85), 4),
            round(min(ceiling, value * 1.15), 4),
        }
        return sorted(vals)

    sls = levels(base_sl, 0.50, 1.50)
    tgts = levels(base_tgt, 0.60, 3.50)
    return [(sl, tgt) for sl in sls for tgt in tgts]


def _sl_key(sl_pct: float) -> str:
    return f"{float(sl_pct):.4f}".replace("-", "m").replace(".", "p")


def _premom_col(sl_pct: float, feature: str) -> str:
    return f"premom__sl_{_sl_key(sl_pct)}__{feature}"


def _premom_missing_col(sl_pct: float) -> str:
    return f"premom__sl_{_sl_key(sl_pct)}__missing_reason"


def _signal_stop_price(
    signal_entry_price: float,
    side: str,
    sl_pct: float,
) -> float:
    """Mirror the V12 entry engine's signal-price stop rounding."""
    entry = float(signal_entry_price)
    if str(side).upper() == "LONG":
        return round(entry * (1.0 - float(sl_pct) / 100.0), 2)
    return round(entry * (1.0 + float(sl_pct) / 100.0), 2)


def enrich_pre_momentum_features(
    pool: pd.DataFrame,
    setup_book: Mapping[str, Mapping[str, Any]],
    sl_values: Mapping[str, Sequence[float]],
    v12: Any,
    *,
    progress_label: str,
) -> pd.DataFrame:
    """Persist every required pre-momentum feature under the matching SL risk."""
    work = pool.copy()
    if work.empty:
        return work
    total = sum(
        len(work.loc[work["setup"].astype(str).eq(setup)]) * len(sl_values.get(setup, []))
        for setup, cfg in setup_book.items()
        if original_constraints(cfg)["pre_momentum_terms"]
    )
    done = 0
    for setup, cfg in setup_book.items():
        terms = original_constraints(cfg)["pre_momentum_terms"]
        features_needed = sorted({str(term[0]) for term in terms})
        if not features_needed:
            continue
        positions = np.flatnonzero(work["setup"].astype(str).eq(setup).to_numpy())
        for sl_pct in sorted(set(float(v) for v in sl_values.get(setup, []))):
            for feature in features_needed:
                if _premom_col(sl_pct, feature) not in work.columns:
                    work[_premom_col(sl_pct, feature)] = np.nan
            missing_col = _premom_missing_col(sl_pct)
            if missing_col not in work.columns:
                work[missing_col] = ""
            for pos in positions:
                row = work.iloc[int(pos)]
                entry_price = float(row["v7_signal_entry_price"])
                side = str(row["side"]).upper()
                stop_price = _signal_stop_price(entry_price, side, sl_pct)
                signal_ts = v12._normalise_ts(row.get("signal_time_ist"))
                values, missing = v12._pre_entry_momentum_features_v11(
                    str(row["ticker"]),
                    side,
                    entry_price,
                    stop_price,
                    (signal_ts + pd.Timedelta(minutes=1)).floor("min"),
                    signal_ts,
                    candidate=row,
                )
                row_index = work.index[int(pos)]
                for feature in features_needed:
                    work.at[row_index, _premom_col(sl_pct, feature)] = values.get(
                        feature, np.nan
                    )
                work.at[row_index, missing_col] = str(missing or "")
                done += 1
                if done % 500 == 0 or done == total:
                    print(
                        f"[optimizer {progress_label}] pre-momentum {done:,}/{total:,}",
                        flush=True,
                    )
    return work


def _resolve_exit_row(
    row: pd.Series,
    *,
    sl_pct: float,
    tgt_pct: float,
    v12: Any,
) -> dict[str, Any] | None:
    """Resolve one exit with V12 paper slippage, sizing, policy, and costs."""
    ticker = str(row.get("ticker", "")).upper().strip()
    side = str(row.get("side", "")).upper().strip()
    setup = str(row.get("setup", ""))
    entry_ts = v12._normalise_ts(row.get("v7_signal_entry_time_ist"))
    signal_entry = float(row.get("v7_signal_entry_price", 0.0))
    if pd.isna(entry_ts) or side not in {"LONG", "SHORT"} or signal_entry <= 0:
        return None
    day_loader = getattr(v12, "_optimizer_load_1m_day", None)
    bars = (
        day_loader(ticker, str(entry_ts.date()))
        if callable(day_loader)
        else v12._load_1m_with_open(ticker)
    )
    if bars is None or bars.empty:
        return None
    signal_stop = _signal_stop_price(signal_entry, side, sl_pct)
    quantity = v12._risk_based_qty(signal_entry, signal_stop)
    if side == "LONG":
        entry_price = round(signal_entry * (1.0 + v12.V7_PAPER_SLIPPAGE_PCT), 2)
    else:
        entry_price = round(signal_entry * (1.0 - v12.V7_PAPER_SLIPPAGE_PCT), 2)
    short_mult = 1.0
    if side == "SHORT":
        short_mult = v12._historical_nifty_short_mult(str(entry_ts.date()))
        if short_mult < 1.0:
            quantity = max(1, int(quantity * short_mult))
    result = v12.er.resolve(
        bars=bars,
        side=side,
        entry_price=entry_price,
        entry_time_ist=entry_ts,
        sl_pct=float(sl_pct),
        tgt_pct=float(tgt_pct),
        exit_policy=v12._FINAL_CONF_EXIT_POLICIES.get(setup),
    )
    if result is None:
        return None
    import nse_intraday_costs as nse

    costs = nse.intraday_equity_costs(
        entry_price,
        float(result.exit_price),
        int(quantity),
        side,
    )
    gross = v12._price_pnl_rs(side, entry_price, float(result.exit_price), quantity)
    return {
        "_optimizer_row_id": int(row["_optimizer_row_id"]),
        "ticker": ticker,
        "side": side,
        "setup": setup,
        "trade_date": str(entry_ts.date()),
        "signal_time_ist": str(row.get("signal_time_ist", "")),
        "entry_time_ist": entry_ts,
        "entry_price": float(entry_price),
        "quantity": int(quantity),
        "sl_pct": float(sl_pct),
        "tgt_pct": float(tgt_pct),
        "outcome": str(result.outcome),
        "exit_time_ist": result.exit_time_ist,
        "exit_price": float(result.exit_price),
        "bars_held": int(result.bars_held),
        "gross_pnl_rs": float(gross),
        "cost_rs": float(costs.total_cost),
        "net_pnl_rs": float(costs.net_pnl),
        "nifty_short_size_mult": float(short_mult),
        "cost_rates_as_of": str(nse.CostConfig().rates_as_of),
    }


def resolve_exit_grid(
    pool: pd.DataFrame,
    exits: Mapping[str, Sequence[tuple[float, float]]],
    v12: Any,
    *,
    progress_label: str,
) -> pd.DataFrame:
    if pool.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    expected = sum(
        len(pool.loc[pool["setup"].astype(str).eq(setup)]) * len(pairs)
        for setup, pairs in exits.items()
    )
    done = 0
    for setup, pairs in exits.items():
        subset = pool.loc[pool["setup"].astype(str).eq(setup)]
        for sl_pct, tgt_pct in pairs:
            for _, row in subset.iterrows():
                record = _resolve_exit_row(
                    row, sl_pct=float(sl_pct), tgt_pct=float(tgt_pct), v12=v12
                )
                if record is not None:
                    rows.append(record)
                done += 1
                if done % 1000 == 0 or done == expected:
                    print(
                        f"[optimizer {progress_label}] exits {done:,}/{expected:,} "
                        f"resolved={len(rows):,}",
                        flush=True,
                    )
    return pd.DataFrame(rows)


def _series(frame: pd.DataFrame, feature: str) -> pd.Series:
    if feature in frame.columns:
        return frame[feature]
    return pd.Series(np.nan, index=frame.index, dtype="float64")


def _apply_terms(
    frame: pd.DataFrame,
    terms: Sequence[Sequence[Any]],
    *,
    sl_pct: float | None = None,
    pre_momentum: bool = False,
) -> pd.Series:
    mask = pd.Series(True, index=frame.index, dtype=bool)
    for feature, op, threshold, *_ in terms:
        column = _premom_col(float(sl_pct), str(feature)) if pre_momentum else str(feature)
        values = _series(frame, column)
        if isinstance(threshold, str):
            text = values.astype(str).str.upper()
            value = threshold.upper()
            current = text.ne(value) if op == "!=" else text.eq(value)
        else:
            numeric = pd.to_numeric(values, errors="coerce")
            if op == ">=":
                current = numeric >= float(threshold)
            elif op == "<=":
                current = numeric <= float(threshold)
            elif op == "!=":
                current = numeric != float(threshold)
            else:
                current = numeric == float(threshold)
        mask &= current.fillna(False)
    return mask


def apply_setup_config(frame: pd.DataFrame, config: Mapping[str, Any]) -> pd.DataFrame:
    """Apply masks + momentum + guards + Top-N; do not ticker/day dedupe yet."""
    if frame.empty:
        return frame.copy()
    work = frame.copy()
    sl_pct = float(config["exit"]["sl_pct"])
    mask = _apply_terms(work, config.get("mask_terms", []))
    mask &= _apply_terms(
        work,
        config.get("pre_momentum_terms", []),
        sl_pct=sl_pct,
        pre_momentum=True,
    )
    minutes = pd.to_numeric(work.get("signal_minute"), errors="coerce")
    guards = dict(config.get("entry_guards", {}))
    if guards.get("min_slot"):
        mask &= minutes >= _slot_minute(guards["min_slot"])
    if guards.get("max_slot"):
        mask &= minutes <= _slot_minute(guards["max_slot"])
    for start, end in guards.get("exclude_windows", []):
        mask &= ~minutes.between(
            _slot_minute(start), _slot_minute(end), inclusive="both"
        )
    accepted = work.loc[mask.fillna(False)].copy()
    top_n = int(guards.get("top_n") or 0)
    if top_n > 0 and not accepted.empty:
        accepted["_optimizer_topn_vwap"] = pd.to_numeric(
            accepted.get("vwap_dist_atr"), errors="coerce"
        )
        accepted["_optimizer_topn_order"] = np.arange(len(accepted), dtype=int)
        accepted = accepted.sort_values(
            [
                "signal_day",
                "signal_minute",
                "_optimizer_topn_vwap",
                "_optimizer_topn_order",
            ],
            ascending=[True, True, False, True],
            kind="mergesort",
        )
        accepted = accepted.groupby(
            ["signal_day", "signal_minute"], sort=False, dropna=False
        ).head(top_n)
        accepted = accepted.drop(
            columns=["_optimizer_topn_vwap", "_optimizer_topn_order"],
            errors="ignore",
        )
    return accepted.reset_index(drop=True)


def exact_dedupe(frame: pd.DataFrame, v12: Any) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    # The V12 selector reads only these fields.  Avoid copying/parsing the
    # hundreds of feature columns on every trial; row IDs recover identity.
    selector_columns = [
        name
        for name in (
            "_optimizer_row_id",
            "ticker",
            "side",
            "setup",
            "bar_time_ist",
            "score",
            "quality_score",
        )
        if name in frame.columns
    ]
    return v12._select_v7_entry_engine_signals(frame[selector_columns].copy())


def attach_outcomes(
    selected: pd.DataFrame,
    config_book: Mapping[str, Mapping[str, Any]],
    outcomes: pd.DataFrame,
) -> pd.DataFrame:
    if selected.empty:
        return pd.DataFrame()
    if outcomes.empty:
        raise ContractError(
            f"outcome coverage failure: 0/{len(selected)} selected rows resolved"
        )
    selected_keys = selected[["_optimizer_row_id", "setup"]].copy()
    selected_keys["sl_pct"] = selected_keys["setup"].map(
        lambda name: float(config_book[str(name)]["exit"]["sl_pct"])
    )
    selected_keys["tgt_pct"] = selected_keys["setup"].map(
        lambda name: float(config_book[str(name)]["exit"]["tgt_pct"])
    )
    merged = selected_keys.merge(
        outcomes,
        on=["_optimizer_row_id", "setup", "sl_pct", "tgt_pct"],
        how="left",
        validate="one_to_one",
        indicator=True,
    )
    missing = merged["_merge"].ne("both")
    if missing.any():
        raise ContractError(
            "outcome coverage failure: "
            f"{int((~missing).sum())}/{len(merged)} selected rows resolved"
        )
    merged = merged.drop(columns=["_merge"])
    return merged.sort_values(
        ["trade_date", "entry_time_ist", "ticker"], kind="mergesort"
    ).reset_index(drop=True)


def _profit_factor(pnl: pd.Series) -> float:
    values = pd.to_numeric(pnl, errors="coerce").dropna()
    gross_profit = float(values.loc[values > 0].sum())
    gross_loss = float(-values.loc[values < 0].sum())
    if gross_loss <= 0:
        return float("inf") if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def performance_metrics(trades: pd.DataFrame, sessions: Sequence[str]) -> dict[str, Any]:
    pnl = (
        pd.to_numeric(trades.get("net_pnl_rs"), errors="coerce").fillna(0.0)
        if not trades.empty
        else pd.Series(dtype="float64")
    )
    daily = pd.Series(0.0, index=list(sessions), dtype="float64")
    if not trades.empty:
        grouped = trades.assign(_pnl=pnl).groupby("trade_date")["_pnl"].sum()
        for day, value in grouped.items():
            if str(day) in daily.index:
                daily.loc[str(day)] = float(value)
    daily_counts = pd.Series(0, index=list(sessions), dtype="int64")
    if not trades.empty:
        grouped_counts = trades.groupby("trade_date").size()
        for day, value in grouped_counts.items():
            if str(day) in daily_counts.index:
                daily_counts.loc[str(day)] = int(value)
    midpoint = max(1, len(daily) // 2)
    half1 = daily.iloc[:midpoint]
    half2 = daily.iloc[midpoint:]
    cumulative = daily.cumsum()
    drawdown = cumulative - cumulative.cummax().clip(lower=0.0)
    positive_total = float(daily.loc[daily > 0].sum())
    top_day_share = (
        float(daily.max() / positive_total)
        if positive_total > 0 and len(daily)
        else 0.0
    )
    net = float(pnl.sum())
    pf = _profit_factor(pnl)
    rate = float(len(trades) / len(sessions)) if sessions else 0.0
    if len(daily_counts):
        count_values = daily_counts.astype(float)
        daily_distance = np.where(
            count_values < TARGET_MIN_TRADES_PER_SESSION,
            TARGET_MIN_TRADES_PER_SESSION - count_values,
            np.where(
                count_values > TARGET_MAX_TRADES_PER_SESSION,
                count_values - TARGET_MAX_TRADES_PER_SESSION,
                0.0,
            ),
        )
        median_rate = float(count_values.median())
        p10_rate = float(count_values.quantile(0.10))
        p90_rate = float(count_values.quantile(0.90))
        in_band_sessions = int(
            count_values.between(
                TARGET_MIN_TRADES_PER_SESSION,
                TARGET_MAX_TRADES_PER_SESSION,
                inclusive="both",
            ).sum()
        )
        zero_sessions = int(count_values.eq(0.0).sum())
        in_band_pct = float(in_band_sessions / len(count_values) * 100.0)
        zero_pct = float(zero_sessions / len(count_values) * 100.0)
        mean_daily_distance = float(np.mean(daily_distance))
    else:
        median_rate = p10_rate = p90_rate = 0.0
        in_band_sessions = zero_sessions = 0
        in_band_pct = zero_pct = mean_daily_distance = 0.0
    average_in_band = TARGET_MIN_TRADES_PER_SESSION <= rate <= TARGET_MAX_TRADES_PER_SESSION
    median_in_band = (
        TARGET_MIN_TRADES_PER_SESSION
        <= median_rate
        <= TARGET_MAX_TRADES_PER_SESSION
    )
    distribution_met = in_band_pct >= 50.0 and zero_pct <= 20.0
    return {
        "trades": int(len(trades)),
        "sessions": int(len(sessions)),
        "trades_per_session": rate,
        "median_trades_per_session": median_rate,
        "p10_trades_per_session": p10_rate,
        "p90_trades_per_session": p90_rate,
        "sessions_in_target_band": in_band_sessions,
        "sessions_in_target_band_pct": in_band_pct,
        "zero_trade_sessions": zero_sessions,
        "zero_trade_sessions_pct": zero_pct,
        "mean_daily_frequency_distance": mean_daily_distance,
        "active_trade_days": int((daily.index.to_series().isin(
            set(trades.get("trade_date", pd.Series(dtype=str)).astype(str))
        )).sum()) if len(daily) else 0,
        "net_pnl_rs": net,
        "profit_factor": pf,
        "win_rate_pct": float((pnl > 0).mean() * 100.0) if len(pnl) else 0.0,
        "positive_sessions": int((daily > 0).sum()),
        "half1_net_pnl_rs": float(half1.sum()),
        "half2_net_pnl_rs": float(half2.sum()),
        "worst_half_net_pnl_rs": float(min(half1.sum(), half2.sum())),
        "max_drawdown_rs": float(drawdown.min()) if len(drawdown) else 0.0,
        "top_positive_day_share": top_day_share,
        "target_average_frequency_met": bool(average_in_band),
        "target_daily_distribution_met": bool(distribution_met),
        "target_frequency_met": bool(
            average_in_band and median_in_band and distribution_met
        ),
    }


def robust_score(metrics: Mapping[str, Any]) -> float:
    """TRAIN-only stability score; net profit remains the dominant component."""
    net = float(metrics.get("net_pnl_rs", 0.0))
    worst_half = float(metrics.get("worst_half_net_pnl_rs", 0.0))
    drawdown = abs(min(0.0, float(metrics.get("max_drawdown_rs", 0.0))))
    pf = float(metrics.get("profit_factor", 0.0))
    if not math.isfinite(pf):
        pf = 4.0
    concentration = max(0.0, float(metrics.get("top_positive_day_share", 0.0)) - 0.50)
    return (
        net
        + 0.35 * min(0.0, worst_half)
        - 0.15 * drawdown
        + 500.0 * min(max(pf - 1.0, -1.0), 2.0)
        - abs(net) * concentration
    )


def frequency_distance(rate: float) -> float:
    if rate < TARGET_MIN_TRADES_PER_SESSION:
        return TARGET_MIN_TRADES_PER_SESSION - rate
    if rate > TARGET_MAX_TRADES_PER_SESSION:
        return rate - TARGET_MAX_TRADES_PER_SESSION
    return 0.0


def portfolio_rank(metrics: Mapping[str, Any]) -> tuple[Any, ...]:
    """Profit-aware constrained ordering used only on TRAIN portfolios."""
    rate = float(metrics.get("trades_per_session", 0.0))
    net = float(metrics.get("net_pnl_rs", 0.0))
    pf = float(metrics.get("profit_factor", 0.0))
    profitable = net > 0.0 and (math.isinf(pf) or pf >= 1.0)
    average_distance = frequency_distance(rate)
    daily_distance = float(
        metrics.get("mean_daily_frequency_distance", average_distance)
    )
    in_band_pct = float(metrics.get("sessions_in_target_band_pct", 0.0))
    zero_pct = float(metrics.get("zero_trade_sessions_pct", 100.0))
    qualified = profitable and bool(
        metrics.get("target_frequency_met", average_distance == 0.0)
    )
    # Never manufacture the frequency target with a losing portfolio.  If no
    # profitable in-band solution exists, prefer profitable near-band evidence
    # and report the miss explicitly.
    tier = 2 if qualified else (1 if profitable else 0)
    return (
        tier,
        -daily_distance,
        -average_distance,
        in_band_pct,
        -zero_pct,
        robust_score(metrics),
        net,
        -abs(rate - 12.5),
    )


def _threshold_levels(
    values: pd.Series,
    *,
    op: str,
    original_value: float,
    current_value: float,
) -> list[float]:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    levels = {float(original_value), float(current_value)}
    if len(numeric):
        quantiles = (0.25, 0.10, 0.02) if op == ">=" else (0.75, 0.90, 0.98)
        for quantile in quantiles:
            candidate = float(numeric.quantile(quantile))
            if op == ">=":
                candidate = min(candidate, float(original_value))
            else:
                candidate = max(candidate, float(original_value))
            levels.add(candidate)
    if op == ">=":
        levels = {value for value in levels if value <= original_value + 1e-12}
        return sorted(levels, reverse=True)
    levels = {value for value in levels if value >= original_value - 1e-12}
    return sorted(levels)


def _term_variants(
    original: Sequence[Sequence[Any]],
    current: Sequence[Sequence[Any]],
    train: pd.DataFrame,
    *,
    pre_momentum: bool,
    reference_sl: float,
) -> list[list[list[Any]]]:
    if len(original) != len(current):
        raise ContractError("current setup has already removed original terms")
    per_term: list[list[list[Any]]] = []
    for old, now in zip(original, current):
        feature, op, old_value = old[:3]
        if str(feature) != str(now[0]) or str(op) != str(now[1]):
            raise ContractError("current setup changed original term identity")
        old_num = _numeric(old_value)
        now_num = _numeric(now[2])
        if old_num is None or now_num is None or op not in {">=", "<="}:
            per_term.append([[feature, op, copy.deepcopy(old_value)]])
            continue
        column = _premom_col(reference_sl, str(feature)) if pre_momentum else str(feature)
        levels = _threshold_levels(
            _series(train, column),
            op=str(op),
            original_value=old_num,
            current_value=now_num,
        )
        per_term.append([[feature, op, float(value)] for value in levels])
    if not per_term:
        return [[]]
    return [[copy.deepcopy(term) for term in combo] for combo in itertools.product(*per_term)]


def _guard_variants(
    original: Mapping[str, Any],
    current: Mapping[str, Any],
    train: pd.DataFrame,
) -> list[dict[str, Any]]:
    if set(original) != set(current):
        raise ContractError("current setup has already removed original guard keys")
    minutes = pd.to_numeric(train.get("signal_minute"), errors="coerce").dropna()
    per_key: list[tuple[str, list[Any]]] = []
    for key, old in original.items():
        now = current[key]
        if key == "min_slot":
            observed = _minute_slot(minutes.min()) if len(minutes) else str(now)
            vals = sorted(
                {str(old), str(now), observed}, key=_slot_minute, reverse=True
            )
            vals = [value for value in vals if _slot_minute(value) <= _slot_minute(old)]
        elif key == "max_slot":
            observed = _minute_slot(minutes.max()) if len(minutes) else str(now)
            vals = sorted({str(old), str(now), observed}, key=_slot_minute)
            vals = [value for value in vals if _slot_minute(value) >= _slot_minute(old)]
        elif key == "top_n":
            expanded = max(int(now), int(old), 6)
            vals = sorted({int(old), int(now), expanded})
            vals = [value for value in vals if value >= int(old)]
        else:
            vals = [copy.deepcopy(old)]
        per_key.append((key, vals))
    if not per_key:
        return [{}]
    variants = []
    for values in itertools.product(*(items for _, items in per_key)):
        variants.append(
            {key: copy.deepcopy(value) for (key, _), value in zip(per_key, values)}
        )
    return variants


def generate_setup_trials(
    setup: str,
    cfg: Mapping[str, Any],
    train: pd.DataFrame,
    exits: Sequence[tuple[float, float]],
) -> list[dict[str, Any]]:
    """Generate variants using TRAIN feature distributions only."""
    anchor = original_constraints(cfg)
    current = {
        "mask_terms": copy.deepcopy(cfg.get("mask_terms", [])),
        "pre_momentum_terms": copy.deepcopy(cfg.get("pre_momentum_terms", [])),
        "entry_guards": copy.deepcopy(cfg.get("entry_guards", {})),
    }
    validate_constraint_relaxation(setup, anchor, current)
    variants: dict[str, dict[str, Any]] = {}
    for sl_pct, tgt_pct in exits:
        mask_variants = _term_variants(
            anchor["mask_terms"],
            current["mask_terms"],
            train,
            pre_momentum=False,
            reference_sl=sl_pct,
        )
        premom_variants = _term_variants(
            anchor["pre_momentum_terms"],
            current["pre_momentum_terms"],
            train,
            pre_momentum=True,
            reference_sl=sl_pct,
        )
        guards = _guard_variants(
            anchor["entry_guards"], current["entry_guards"], train
        )
        for mask_terms, premom_terms, entry_guards in itertools.product(
            mask_variants, premom_variants, guards
        ):
            proposal = {
                "side": str(cfg.get("side", "")),
                "exit": {"sl_pct": float(sl_pct), "tgt_pct": float(tgt_pct)},
                "mask_terms": mask_terms,
                "pre_momentum_terms": premom_terms,
                "entry_guards": entry_guards,
            }
            validate_constraint_relaxation(setup, anchor, proposal)
            variants[_trial_id(proposal)] = proposal
    return list(variants.values())


def _outcome_subset(
    outcomes: pd.DataFrame, setup: str, sl_pct: float, tgt_pct: float
) -> pd.DataFrame:
    if outcomes.empty:
        return outcomes.copy()
    mask = outcomes["setup"].astype(str).eq(setup)
    mask &= np.isclose(pd.to_numeric(outcomes["sl_pct"]), sl_pct)
    mask &= np.isclose(pd.to_numeric(outcomes["tgt_pct"]), tgt_pct)
    return outcomes.loc[mask].copy()


def score_setup_trials(
    setup: str,
    train: pd.DataFrame,
    trials: Sequence[dict[str, Any]],
    outcomes: pd.DataFrame,
    train_sessions: Sequence[str],
    v12: Any,
) -> tuple[list[TrialChoice], pd.DataFrame]:
    choices: list[TrialChoice] = []
    records: list[dict[str, Any]] = []
    setup_frame = train.loc[train["setup"].astype(str).eq(setup)].copy()
    filter_cache: dict[str, tuple[tuple[int, ...], tuple[int, ...]]] = {}
    selection_cache: dict[tuple[int, ...], tuple[int, ...]] = {}
    outcome_cache: dict[tuple[float, float], pd.DataFrame] = {}
    for index, config in enumerate(trials, 1):
        sl_pct = float(config["exit"]["sl_pct"])
        tgt_pct = float(config["exit"]["tgt_pct"])
        filter_signature = _filter_signature(config)
        cached = filter_cache.get(filter_signature)
        if cached is None:
            filtered = apply_setup_config(setup_frame, config)
            filtered_ids = tuple(
                int(value) for value in filtered["_optimizer_row_id"].tolist()
            )
            selected_ids = selection_cache.get(filtered_ids)
            if selected_ids is None:
                selected = exact_dedupe(filtered, v12)
                selected_ids = tuple(
                    int(value) for value in selected["_optimizer_row_id"].tolist()
                )
                selection_cache[filtered_ids] = selected_ids
            filter_cache[filter_signature] = (filtered_ids, selected_ids)
        else:
            filtered_ids, selected_ids = cached
        selected_stub = pd.DataFrame(
            {
                "_optimizer_row_id": list(selected_ids),
                "setup": [setup] * len(selected_ids),
            }
        )
        outcome_key = (sl_pct, tgt_pct)
        if outcome_key not in outcome_cache:
            outcome_cache[outcome_key] = _outcome_subset(
                outcomes, setup, sl_pct, tgt_pct
            )
        one_book = {setup: config}
        resolved = attach_outcomes(
            selected_stub,
            one_book,
            outcome_cache[outcome_key],
        )
        metrics = performance_metrics(resolved, train_sessions)
        metrics["robust_score"] = robust_score(metrics)
        trial_id = _trial_id(config)
        choice = TrialChoice(
            setup=setup,
            trial_id=trial_id,
            config=config,
            metrics=metrics,
            filtered=None,
            filtered_row_ids=filtered_ids,
            filter_signature=filter_signature,
        )
        choices.append(choice)
        records.append(
            {
                "setup": setup,
                "trial_id": trial_id,
                "config_json": _stable_json(config),
                **metrics,
            }
        )
        if index % 500 == 0:
            print(
                f"[optimizer train] {setup}: scored {index:,}/{len(trials):,}",
                flush=True,
            )
    return choices, pd.DataFrame(records)


def _current_choice(
    setup: str,
    cfg: Mapping[str, Any],
    train: pd.DataFrame,
    outcomes: pd.DataFrame,
    train_sessions: Sequence[str],
    v12: Any,
    *,
    status: str,
) -> TrialChoice:
    config = {
        "side": str(cfg.get("side", "")),
        "exit": copy.deepcopy(cfg["exit"]),
        "mask_terms": copy.deepcopy(cfg.get("mask_terms", [])),
        "pre_momentum_terms": copy.deepcopy(cfg.get("pre_momentum_terms", [])),
        "entry_guards": copy.deepcopy(cfg.get("entry_guards", {})),
    }
    filtered = apply_setup_config(
        train.loc[train["setup"].astype(str).eq(setup)].copy(), config
    )
    selected = exact_dedupe(filtered, v12)
    resolved = attach_outcomes(selected, {setup: config}, outcomes)
    metrics = performance_metrics(resolved, train_sessions)
    metrics["robust_score"] = robust_score(metrics)
    return TrialChoice(
        setup=setup,
        trial_id=_trial_id(config),
        config=config,
        metrics=metrics,
        filtered=filtered,
        filtered_row_ids=tuple(
            int(value) for value in filtered["_optimizer_row_id"].tolist()
        ),
        filter_signature=_filter_signature(config),
        status=status,
    )


def materialize_choice_frames(
    choices: Iterable[TrialChoice],
    pool: pd.DataFrame,
) -> None:
    """Attach full eligible rows only to shortlisted portfolio choices."""
    values = list(choices)
    pending = [choice for choice in values if choice.filtered is None]
    if not pending:
        return
    if "_optimizer_row_id" not in pool:
        raise ContractError("cannot materialize choices without optimizer row IDs")
    indexed = pool.set_index("_optimizer_row_id", drop=False)
    cache: dict[tuple[int, ...], pd.DataFrame] = {}
    for choice in pending:
        ids = tuple(choice.filtered_row_ids)
        frame = cache.get(ids)
        if frame is None:
            if ids:
                frame = indexed.loc[list(ids)].reset_index(drop=True)
            else:
                frame = pool.iloc[0:0].copy()
            cache[ids] = frame
        choice.filtered = frame


def shortlist_choices(
    choices: Sequence[TrialChoice],
    current: TrialChoice,
    *,
    limit: int = 16,
) -> list[TrialChoice]:
    """Keep profit, robustness, and frequency frontier points for portfolio search."""
    by_id = {choice.trial_id: choice for choice in choices}
    by_id[current.trial_id] = current
    ranked = list(by_id.values())
    selected: dict[str, TrialChoice] = {current.trial_id: current}
    orders = (
        sorted(ranked, key=lambda item: robust_score(item.metrics), reverse=True),
        sorted(ranked, key=lambda item: float(item.metrics["net_pnl_rs"]), reverse=True),
        sorted(ranked, key=lambda item: int(item.metrics["trades"]), reverse=True),
        sorted(
            ranked,
            key=lambda item: (
                int(item.metrics["trades"]) // 5,
                robust_score(item.metrics),
            ),
            reverse=True,
        ),
    )
    cursor = [0] * len(orders)
    while len(selected) < limit:
        grew = False
        for order_index, order in enumerate(orders):
            while cursor[order_index] < len(order):
                choice = order[cursor[order_index]]
                cursor[order_index] += 1
                if choice.trial_id not in selected:
                    selected[choice.trial_id] = choice
                    grew = True
                    break
            if len(selected) >= limit:
                break
        if not grew:
            break
    return list(selected.values())


def evaluate_portfolio(
    choices: Mapping[str, TrialChoice],
    outcomes: pd.DataFrame,
    sessions: Sequence[str],
    v12: Any,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    missing = [choice.setup for choice in choices.values() if choice.filtered is None]
    if missing:
        raise ContractError(f"unmaterialized portfolio choices: {sorted(missing)}")
    frames = [
        choice.filtered
        for choice in choices.values()
        if choice.filtered is not None and not choice.filtered.empty
    ]
    pre_dedupe = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    selected = exact_dedupe(pre_dedupe, v12)
    book = {setup: choice.config for setup, choice in choices.items()}
    trades = attach_outcomes(selected, book, outcomes)
    return trades, performance_metrics(trades, sessions)


def coordinate_portfolio_search(
    frontiers: Mapping[str, Sequence[TrialChoice]],
    current: Mapping[str, TrialChoice],
    outcomes: pd.DataFrame,
    train_sessions: Sequence[str],
    v12: Any,
) -> tuple[dict[str, TrialChoice], pd.DataFrame, dict[str, Any], pd.DataFrame]:
    setups = sorted(frontiers)
    starts: list[dict[str, TrialChoice]] = [dict(current)]
    starts.append(
        {
            setup: max(
                frontiers[setup], key=lambda item: robust_score(item.metrics)
            )
            for setup in setups
        }
    )
    starts.append(
        {
            setup: max(frontiers[setup], key=lambda item: int(item.metrics["trades"]))
            for setup in setups
        }
    )
    audit: list[dict[str, Any]] = []
    best_book: dict[str, TrialChoice] | None = None
    best_trades = pd.DataFrame()
    best_metrics: dict[str, Any] | None = None
    for start_index, initial in enumerate(starts):
        chosen = dict(initial)
        trades, metrics = evaluate_portfolio(chosen, outcomes, train_sessions, v12)
        for iteration in range(1, 7):
            changed = False
            for setup in setups:
                local_choice = chosen[setup]
                local_trades = trades
                local_metrics = metrics
                for candidate in frontiers[setup]:
                    proposal = dict(chosen)
                    proposal[setup] = candidate
                    candidate_trades, candidate_metrics = evaluate_portfolio(
                        proposal, outcomes, train_sessions, v12
                    )
                    if portfolio_rank(candidate_metrics) > portfolio_rank(local_metrics):
                        local_choice = candidate
                        local_trades = candidate_trades
                        local_metrics = candidate_metrics
                if local_choice.trial_id != chosen[setup].trial_id:
                    chosen[setup] = local_choice
                    trades = local_trades
                    metrics = local_metrics
                    changed = True
                audit.append(
                    {
                        "start": start_index,
                        "iteration": iteration,
                        "setup": setup,
                        "chosen_trial_id": chosen[setup].trial_id,
                        **metrics,
                    }
                )
            if not changed:
                break
        if best_metrics is None or portfolio_rank(metrics) > portfolio_rank(best_metrics):
            best_book = chosen
            best_trades = trades
            best_metrics = metrics
    if best_book is None or best_metrics is None:
        raise ContractError("portfolio search produced no result")
    return best_book, best_trades, best_metrics, pd.DataFrame(audit)


def _setup_summary(
    trades: pd.DataFrame,
    sessions: Sequence[str],
    setup_names: Iterable[str] | None = None,
) -> pd.DataFrame:
    names = sorted(
        set(str(name) for name in (setup_names or ()))
        | (
            set(trades["setup"].astype(str))
            if not trades.empty and "setup" in trades
            else set()
        )
    )
    rows = []
    for setup in names:
        group = (
            trades.loc[trades["setup"].astype(str).eq(setup)].copy()
            if not trades.empty and "setup" in trades
            else pd.DataFrame()
        )
        rows.append({"setup": setup, **performance_metrics(group, sessions)})
    return pd.DataFrame(rows)


def _constraint_fingerprint(book: Mapping[str, Mapping[str, Any]]) -> str:
    # Fingerprint the complete active setup payload, including entry/exit
    # policies.  A proposal must fail closed if any behavior-bearing source
    # field changes after training.
    payload = {name: copy.deepcopy(dict(cfg)) for name, cfg in sorted(book.items())}
    return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()


def render_proposed_module(
    source_book: Mapping[str, Mapping[str, Any]],
    proposed: Mapping[str, Mapping[str, Any]],
    *,
    generated_at: str,
    train_summary: Mapping[str, Any],
) -> str:
    """Render a thin, source-fingerprinted proposal module (never auto-enabled)."""
    source_fingerprint = _constraint_fingerprint(source_book)
    overlay = {
        name: {
            "exit": copy.deepcopy(cfg["exit"]),
            "mask_terms": copy.deepcopy(cfg.get("mask_terms", [])),
            "pre_momentum_terms": copy.deepcopy(cfg.get("pre_momentum_terms", [])),
            "entry_guards": copy.deepcopy(cfg.get("entry_guards", {})),
        }
        for name, cfg in sorted(proposed.items())
    }
    return f'''"""TRAIN-only V12 proposal; experimental and not production-approved.

Generated by research_v12_prefilter_train_test_optimizer.py at {generated_at}.
TRAIN: {TRAIN_START}..{TRAIN_END}.  TEST was not used to select this book.
"""
from copy import deepcopy as _deepcopy
import hashlib as _hashlib
import json as _json
from final_setup_conf_v12 import FINAL_SETUP_CONF as _SOURCE_FINAL_SETUP_CONF
from final_setup_conf_v12 import RESEARCH_WATCH_CONF

PRODUCTION_APPROVED = False
TRAIN_ONLY_SELECTED = True
SOURCE_CONSTRAINT_FINGERPRINT = {source_fingerprint!r}
TRAIN_SUMMARY = {_jsonable(dict(train_summary))!r}
PROPOSED_OVERLAY = {overlay!r}

def _stable(value):
    return _json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)

def _fingerprint(book):
    payload = {{name: cfg for name, cfg in sorted(book.items())}}
    return _hashlib.sha256(_stable(payload).encode("utf-8")).hexdigest()

if _fingerprint(_SOURCE_FINAL_SETUP_CONF) != SOURCE_CONSTRAINT_FINGERPRINT:
    raise RuntimeError("final_setup_conf_v12 changed after this proposal was trained")

FINAL_SETUP_CONF = _deepcopy(_SOURCE_FINAL_SETUP_CONF)
if set(PROPOSED_OVERLAY) != set(FINAL_SETUP_CONF):
    raise RuntimeError("proposal must cover every active V12 setup exactly")
for _name, _overlay in PROPOSED_OVERLAY.items():
    FINAL_SETUP_CONF[_name].update(_deepcopy(_overlay))
    FINAL_SETUP_CONF[_name]["v12_train_test_proposal"] = {{
        "train_window": [{TRAIN_START!r}, {TRAIN_END!r}],
        "test_window": [{TEST_START!r}, {TEST_END!r}],
        "production_approved": False,
    }}
'''


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(_jsonable(payload), indent=2, sort_keys=True), encoding="utf-8")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--candidate-csv",
        action="append",
        default=None,
        help="Hourly-prefiltered pre_dedupe CSV; repeat for chunks",
    )
    parser.add_argument(
        "--supplemental-csv",
        action="append",
        default=[],
        help="Optional candidate-compatible CSV for active setups missing from the main pool",
    )
    parser.add_argument(
        "--session-calendar", default=str(DEFAULT_SESSION_CALENDAR)
    )
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument(
        "--proposed-config-name",
        default="final_setup_conf_v12_train_optimized_proposed.py",
    )
    parser.add_argument(
        "--min-searchable-rows", type=int, default=DEFAULT_MIN_SEARCHABLE_ROWS
    )
    parser.add_argument(
        "--min-searchable-days", type=int, default=DEFAULT_MIN_SEARCHABLE_DAYS
    )
    parser.add_argument(
        "--frontier-size", type=int, default=16, help="Per-setup portfolio frontier"
    )
    parser.add_argument(
        "--prefilter-rank-min",
        type=int,
        default=None,
        help="Inclusive hourly prefilter-selection rank floor applied before V12",
    )
    parser.add_argument(
        "--prefilter-rank-max",
        type=int,
        default=None,
        help="Inclusive hourly prefilter-selection rank ceiling applied before V12",
    )
    parser.add_argument(
        "--io-workers",
        type=int,
        default=8,
        help="Concurrent readers used only to prewarm the fixed-window 1-minute cache",
    )
    parser.add_argument(
        "--smoke-rows",
        type=int,
        default=0,
        help="Development-only row cap; nonzero output is marked SMOKE and not a proposal",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    contract = SplitContract()
    contract.validate()
    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    candidate_paths = [Path(value) for value in (args.candidate_csv or DEFAULT_CANDIDATE_CSVS)]
    supplemental_paths = [Path(value) for value in args.supplemental_csv]

    # Importing V12 mutates no process outside this standalone Python run.  The
    # module itself hard-pins final_setup_conf_v12, preventing V11/live loading.
    os.environ["EQIDV2_V12_FINAL_SETUP_CONF_MODULE"] = "final_setup_conf_v12"
    import avwap_5min_ID_v12_backtesting as v12

    windowed_1m_loader = install_windowed_1m_loader(
        v12,
        start_date=contract.train_start,
        end_date=contract.test_end,
    )
    install_day_1m_adapter(v12, windowed_1m_loader)
    source_book = v12._activate_final_setup_conf()
    candidates, input_audit = load_candidates(
        candidate_paths, supplemental_paths, source_book
    )
    train_sessions, test_sessions = _read_sessions(
        Path(args.session_calendar), contract
    )
    valid_sessions = set(train_sessions) | set(test_sessions)
    candidates = candidates.loc[
        candidates["_optimizer_signal_day"].astype(str).isin(valid_sessions)
    ].reset_index(drop=True)
    candidates, rank_band_audit = apply_prefilter_rank_band(
        candidates,
        min_rank=args.prefilter_rank_min,
        max_rank=args.prefilter_rank_max,
    )
    if rank_band_audit is not None:
        input_audit = pd.concat(
            [input_audit, pd.DataFrame([rank_band_audit])],
            ignore_index=True,
            sort=False,
        )
    if candidates.empty:
        raise SystemExit("no candidates remain after the prefilter rank band")
    if args.smoke_rows:
        candidates = candidates.head(int(args.smoke_rows)).copy()

    train_candidates, test_candidates = split_frame(
        candidates, date_column="_optimizer_signal_day", contract=contract
    )
    prewarm_audit = prewarm_windowed_1m_loader(
        windowed_1m_loader,
        candidates["ticker"].astype(str),
        workers=int(args.io_workers),
    )
    train_pool, train_entry_rejects = build_ungated_executable_pool(
        train_candidates, v12
    )
    if train_pool.empty:
        raise SystemExit("no ungated executable V12 entries")

    exit_grids = {name: _exit_grid(cfg["exit"]) for name, cfg in source_book.items()}
    sl_values = {
        name: sorted({sl for sl, _ in pairs}) for name, pairs in exit_grids.items()
    }
    train_pool = enrich_pre_momentum_features(
        train_pool, source_book, sl_values, v12, progress_label="TRAIN"
    )
    train_outcomes = resolve_exit_grid(
        train_pool, exit_grids, v12, progress_label="TRAIN"
    )

    all_trials: list[pd.DataFrame] = []
    frontiers: dict[str, list[TrialChoice]] = {}
    current_choices: dict[str, TrialChoice] = {}
    setup_selection_audit: list[dict[str, Any]] = []
    for setup, cfg in source_book.items():
        subset = train_pool.loc[train_pool["setup"].astype(str).eq(setup)].copy()
        days = subset["_optimizer_signal_day"].astype(str).nunique()
        current = _current_choice(
            setup,
            cfg,
            train_pool,
            train_outcomes,
            train_sessions,
            v12,
            status="current_baseline",
        )
        current_choices[setup] = current
        insufficient = (
            len(subset) < int(args.min_searchable_rows)
            or days < int(args.min_searchable_days)
        )
        if insufficient:
            current.status = "insufficient_sample_kept_unchanged"
            frontiers[setup] = [current]
            setup_selection_audit.append(
                {
                    "setup": setup,
                    "ungated_train_rows": len(subset),
                    "ungated_train_days": days,
                    "search_status": current.status,
                    "trial_count": 1,
                }
            )
            all_trials.append(
                pd.DataFrame(
                    [
                        {
                            "setup": setup,
                            "trial_id": current.trial_id,
                            "config_json": _stable_json(current.config),
                            **current.metrics,
                        }
                    ]
                )
            )
            continue
        trials = generate_setup_trials(setup, cfg, subset, exit_grids[setup])
        choices, trial_report = score_setup_trials(
            setup, train_pool, trials, train_outcomes, train_sessions, v12
        )
        all_trials.append(trial_report)
        frontiers[setup] = shortlist_choices(
            choices, current, limit=int(args.frontier_size)
        )
        materialize_choice_frames(frontiers[setup], train_pool)
        setup_selection_audit.append(
            {
                "setup": setup,
                "ungated_train_rows": len(subset),
                "ungated_train_days": days,
                "search_status": "searched_train_only",
                "trial_count": len(trials),
                "frontier_count": len(frontiers[setup]),
            }
        )

    selected_choices, train_trades, train_metrics, portfolio_audit = (
        coordinate_portfolio_search(
            frontiers,
            current_choices,
            train_outcomes,
            train_sessions,
            v12,
        )
    )
    proposed = {name: choice.config for name, choice in selected_choices.items()}
    for setup, cfg in source_book.items():
        validate_constraint_relaxation(setup, original_constraints(cfg), proposed[setup])

    # This run's chronological validation rows are touched only after the
    # complete proposal has been frozen.  The source-book reuse caveat is
    # disclosed in the output manifest; no claim of a virgin holdout is made.
    test_pool, test_entry_rejects = build_ungated_executable_pool(
        test_candidates, v12
    )
    selected_sl_values = {
        setup: [float(config["exit"]["sl_pct"])]
        for setup, config in proposed.items()
    }
    selected_exits = {
        setup: [
            (
                float(config["exit"]["sl_pct"]),
                float(config["exit"]["tgt_pct"]),
            )
        ]
        for setup, config in proposed.items()
    }
    test_pool = enrich_pre_momentum_features(
        test_pool, source_book, selected_sl_values, v12, progress_label="TEST_ONCE"
    )
    test_outcomes = resolve_exit_grid(
        test_pool, selected_exits, v12, progress_label="TEST_ONCE"
    )
    test_filtered = []
    if not test_pool.empty:
        for setup, config in proposed.items():
            subset = test_pool.loc[test_pool["setup"].astype(str).eq(setup)]
            test_filtered.append(apply_setup_config(subset, config))
    test_pre_dedupe = (
        pd.concat(test_filtered, ignore_index=True, sort=False)
        if test_filtered
        else pd.DataFrame()
    )
    test_selected = exact_dedupe(test_pre_dedupe, v12)
    test_trades = attach_outcomes(test_selected, proposed, test_outcomes)
    test_metrics = performance_metrics(test_trades, test_sessions)

    smoke = bool(args.smoke_rows)
    generated_at = pd.Timestamp.now(tz="Asia/Kolkata").isoformat()
    proposal_name = str(args.proposed_config_name)
    if Path(proposal_name).name != proposal_name or not proposal_name.endswith(".py"):
        raise ContractError("--proposed-config-name must be a plain .py filename")
    proposal_path = out_dir / proposal_name
    if not smoke:
        proposal_path.write_text(
            render_proposed_module(
                source_book,
                proposed,
                generated_at=generated_at,
                train_summary=train_metrics,
            ),
            encoding="utf-8",
        )

    train_pool.to_csv(out_dir / "train_ungated_executable_pool.csv", index=False)
    test_pool.to_csv(out_dir / "test_ungated_executable_pool.csv", index=False)
    train_entry_rejects = train_entry_rejects.copy()
    train_entry_rejects["_optimizer_split"] = "TRAIN"
    test_entry_rejects = test_entry_rejects.copy()
    test_entry_rejects["_optimizer_split"] = "TEST"
    pd.concat(
        [train_entry_rejects, test_entry_rejects], ignore_index=True, sort=False
    ).to_csv(out_dir / "entry_engine_rejects.csv", index=False)
    train_outcomes.to_csv(out_dir / "train_exit_outcomes.csv", index=False)
    test_outcomes.to_csv(out_dir / "test_selected_exit_outcomes.csv", index=False)
    pd.concat(all_trials, ignore_index=True, sort=False).to_csv(
        out_dir / "train_config_trials.csv", index=False
    )
    pd.DataFrame(setup_selection_audit).to_csv(
        out_dir / "train_setup_search_audit.csv", index=False
    )
    portfolio_audit.to_csv(out_dir / "train_portfolio_search_audit.csv", index=False)
    train_trades.to_csv(out_dir / "train_trades.csv", index=False)
    test_trades.to_csv(out_dir / "test_trades.csv", index=False)
    _setup_summary(train_trades, train_sessions, source_book).to_csv(
        out_dir / "train_setup_summary.csv", index=False
    )
    _setup_summary(test_trades, test_sessions, source_book).to_csv(
        out_dir / "test_setup_summary.csv", index=False
    )
    input_audit.to_csv(out_dir / "input_audit.csv", index=False)
    proposal_payload = {
        name: {
            "trial_id": selected_choices[name].trial_id,
            "search_status": selected_choices[name].status,
            **config,
        }
        for name, config in proposed.items()
    }
    _write_json(out_dir / "proposed_setup_book.json", proposal_payload)
    summary = {
        "status": "SMOKE_NOT_A_PROPOSAL" if smoke else "RESEARCH_PROPOSAL_NOT_APPROVED",
        "generated_at_ist": generated_at,
        "train_window": [contract.train_start, contract.train_end],
        "test_window": [contract.test_start, contract.test_end],
        "train_sessions": len(train_sessions),
        "test_sessions": len(test_sessions),
        "target_trades_per_session": [
            TARGET_MIN_TRADES_PER_SESSION,
            TARGET_MAX_TRADES_PER_SESSION,
        ],
        "prefilter_rank_band": {
            "min_rank": args.prefilter_rank_min,
            "max_rank": args.prefilter_rank_max,
        },
        "train": train_metrics,
        "test_chronological_validation_after_freeze": test_metrics,
        "test_evaluation_count": 1,
        "virgin_out_of_sample_test": False,
        "validation_reuse_disclosure": (
            "The source V12 setup book was created after this validation window "
            "and may reflect prior inspection of 2026-08-03. TEST rows were not "
            "used by this optimizer after launch, but this window is not a virgin "
            "out-of-sample holdout."
        ),
        "proposed_config": str(proposal_path) if not smoke else None,
        "production_files_modified": False,
    }
    _write_json(out_dir / "summary.json", summary)
    manifest = {
        **summary,
        "candidate_inputs": [str(path.resolve()) for path in candidate_paths],
        "supplemental_inputs": [str(path.resolve()) for path in supplemental_paths],
        "session_calendar": str(Path(args.session_calendar).resolve()),
        "source_config_module": str(Path(v12._load_final_setup_conf_module().__file__).resolve()),
        "source_constraint_fingerprint": _constraint_fingerprint(source_book),
        "one_minute_prewarm": prewarm_audit,
        "methodology": {
            "candidate_pool": "hourly-prefiltered V12 pre-dedupe candidates",
            "prefilter_rank_band": (
                "inclusive prefilter_selection_rank bounds applied before "
                "entry-feature computation; missing ranks fail closed"
            ),
            "entry_pool": "exact V12 executable entry helper with PRE_ENTRY_MOMENTUM_SETUP_GATES temporarily empty",
            "one_minute_loader": "historical Parquet filtered to fixed TRAIN+TEST dates; no current live-raw merge",
            "one_minute_session_slicing": (
                "cached monotonic-index day slices for pre-momentum and exits; "
                "binary search for next-minute entry"
            ),
            "filter_order": "mask -> pre-momentum -> entry guards/top-N -> exact ticker/day dedupe",
            "search_data": "TRAIN only",
            "test_policy": (
                "one chronological-validation evaluation after proposal freeze; "
                "no retuning; not claimed as virgin OOS because the source V12 "
                "book post-dates the window"
            ),
            "costs": "NSE statutory intraday costs",
            "sizing": (
                "V12 risk sizing plus NIFTY short multiplier computed only from "
                "sessions completed before each trade day"
            ),
            "exit_grid": "deterministic base and +/-15%, clipped to stable bounds",
            "insufficient_sample_policy": "keep current V12 setup unchanged",
        },
    }
    _write_json(out_dir / "run_manifest.json", manifest)
    print(json.dumps(_jsonable(summary), indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
