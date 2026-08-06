"""Efficient multi-day V12 prefilter replay, with a missing-setup supplement mode.

This is a research-only candidate replay.  It deliberately does not change the
V7/V11/V12 production files or ``final_setup_conf_v12``.  The expensive work is
sharded by ticker (not ticker/day): each worker loads and prepares a ticker once,
then scans every requested trading date before the parent applies the causal
hourly pre-filter and the unchanged V12 live-candidate pipeline.

The default ``missing`` scope is restricted to:

* S9_MIDDAY_LOSE
* DOC5D_AVWAP_RECLAIM_LONG
* L_LATE_BB10_COMPRESSION_BREAKOUT

``--setup-scope all_active`` instead scans the exact active V12 setup book with
the same one-task-per-ticker execution.  Both scopes write the canonical
raw/pre-dedupe stage names used by the multi-day V12 backtest, plus per-day files
and a detailed state/data audit.  They stop at the candidate pipeline;
entry/exit resolution belongs to the later combined optimization run.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from research_v12_hourly_prefilter_backtest import (
    HourlyPool,
    _ist,
    filter_candidates_for_hourly_pools,
    load_hourly_pools,
    ticker_union_by_date,
)


IST = "Asia/Kolkata"
TARGET_SETUPS = (
    "S9_MIDDAY_LOSE",
    "DOC5D_AVWAP_RECLAIM_LONG",
    "L_LATE_BB10_COMPRESSION_BREAKOUT",
)
DEFAULT_PREFILTER = Path(
    r"C:\TradingData\eqidv2_experiments\prefilter_six_month_replay_20260204_20260803_k300"
    r"\hourly_candidates_20260204_20260803_k300.csv"
)
DEFAULT_5M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DEFAULT_OUTPUT = Path(
    r"C:\TradingData\eqidv2_experiments\v12_prefilter_missing_setup_supplement"
)


_WORKER_V12: Any = None
_WORKER_SCAN_STATE: dict[str, Any] | None = None
_WORKER_SCAN_STATE_DIGEST = ""
_WORKER_TARGET_SETUPS: frozenset[str] = frozenset(TARGET_SETUPS)
_WORKER_START_MINUTE = 9 * 60 + 20
_WORKER_END_MINUTE = 15 * 60


def _sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalise_name_set(values: Iterable[Any]) -> tuple[str, ...]:
    return tuple(sorted({str(value).strip() for value in values if str(value).strip()}))


def canonical_scan_state(state: dict[str, Any]) -> dict[str, Any]:
    """Return a stable, JSON-safe worker state used for parent/child parity."""
    return {
        "schema_version": "eqidv2_v12_missing_setups_worker_state_v1",
        "setup_conf_module": str(state.get("setup_conf_module", "")).strip(),
        "target_setups": _normalise_name_set(state.get("target_setups", ())),
        "allowed_setups": _normalise_name_set(state.get("allowed_setups", ())),
        "excluded_setups": _normalise_name_set(state.get("excluded_setups", ())),
        "filter_to_v8_exit_setups": bool(state.get("filter_to_v8_exit_setups", False)),
        "enable_s9_midday_lose": bool(state.get("enable_s9_midday_lose", False)),
        "enable_doc5d_avwap_reclaim": bool(state.get("enable_doc5d_avwap_reclaim", False)),
        "early_mode_enable": bool(state.get("early_mode_enable", False)),
        "early_tight_filters_enable": bool(state.get("early_tight_filters_enable", False)),
        "enable_noisy_advanced_shorts": bool(state.get("enable_noisy_advanced_shorts", False)),
        "enable_native_v2_mined_filter": bool(state.get("enable_native_v2_mined_filter", False)),
        "selection_mode": str(state.get("selection_mode", "")).strip(),
    }


def scan_state_digest(state: dict[str, Any]) -> str:
    payload = json.dumps(canonical_scan_state(state), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def ticker_dates_from_daily_unions(
    daily_unions: dict[str, frozenset[str]],
) -> list[tuple[str, tuple[str, ...]]]:
    """Invert date -> tickers into deterministic one-task-per-ticker payloads."""
    grouped: dict[str, list[str]] = {}
    for day in sorted(daily_unions):
        for ticker in sorted(daily_unions[day]):
            name = str(ticker).upper().strip()
            if name:
                grouped.setdefault(name, []).append(str(day)[:10])
    return [(ticker, tuple(days)) for ticker, days in sorted(grouped.items())]


def setups_for_scope(scope: str, active_setups: Iterable[str]) -> tuple[str, ...]:
    """Resolve the CLI scope without mutating the imported setup book."""
    value = str(scope).strip().lower()
    active = _normalise_name_set(active_setups)
    if value == "missing":
        absent = sorted(set(TARGET_SETUPS) - set(active))
        if absent:
            raise ValueError(f"active setup book is missing supplemental setups: {absent}")
        return tuple(TARGET_SETUPS)
    if value == "all_active":
        if not active:
            raise ValueError("active setup book is empty")
        return active
    raise ValueError(f"unknown setup scope: {scope!r}")


def aggregate_breadth_contributions(
    contributions: Iterable[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    """Aggregate exact per-ticker above-AVWAP breadth observations by slot."""
    totals: dict[str, list[int]] = {}
    for item in contributions:
        slot = _ist(item.get("slot_ist"))
        if pd.isna(slot):
            continue
        key = slot.floor("min").isoformat()
        values = totals.setdefault(key, [0, 0])
        values[0] += int(item.get("above", 0))
        values[1] += int(item.get("total", 0))
    return {
        key: {
            "above": int(above),
            "total": int(total),
            "market_breadth": (float(above) / float(total) if total else float("nan")),
        }
        for key, (above, total) in sorted(totals.items())
    }


def _import_v12(setup_conf_module: str) -> Any:
    global _WORKER_V12
    os.environ["EQIDV2_V12_FINAL_SETUP_CONF_MODULE"] = str(setup_conf_module)
    os.environ["EQIDV2_FINAL_SETUP_CONF_MODULE"] = str(setup_conf_module)
    os.environ.setdefault("EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS", "0")
    if _WORKER_V12 is None:
        _WORKER_V12 = importlib.import_module("avwap_5min_ID_v12_backtesting")
    return _WORKER_V12


def _actual_worker_scan_state(v12: Any, setup_conf_module: str) -> dict[str, Any]:
    scan = v12.candidate_scan
    v2 = scan.v2
    return canonical_scan_state(
        {
            "setup_conf_module": setup_conf_module,
            "target_setups": _WORKER_TARGET_SETUPS,
            "allowed_setups": scan.ALLOWED_SETUPS,
            "excluded_setups": scan.EXCLUDED_SETUPS,
            "filter_to_v8_exit_setups": scan.FILTER_TO_V8_EXIT_SETUPS,
            "enable_s9_midday_lose": v2.ENABLE_S9_MIDDAY_LOSE,
            "enable_doc5d_avwap_reclaim": v2.ENABLE_DOC5D_AVWAP_RECLAIM,
            "early_mode_enable": scan.EARLY_MODE_ENABLE,
            "early_tight_filters_enable": scan.EARLY_TIGHT_FILTERS_ENABLE,
            "enable_noisy_advanced_shorts": v2.ENABLE_NOISY_ADVANCED_SHORTS,
            "enable_native_v2_mined_filter": v2.ENABLE_NATIVE_V2_MINED_FILTER,
            "selection_mode": scan.SELECTION_MODE,
        }
    )


def _worker_init(
    data_root: str,
    setup_conf_module: str,
    expected_scan_state: dict[str, Any],
    start_minute: int,
    end_minute: int,
) -> None:
    """Initialize a spawned process and fail immediately on scanner-state drift."""
    global _WORKER_SCAN_STATE, _WORKER_SCAN_STATE_DIGEST, _WORKER_TARGET_SETUPS
    global _WORKER_START_MINUTE, _WORKER_END_MINUTE

    v12 = _import_v12(setup_conf_module)
    expected = canonical_scan_state(expected_scan_state)
    _WORKER_TARGET_SETUPS = frozenset(expected["target_setups"])
    base_state = {
        "allowed_setups": expected["allowed_setups"],
        "filter_to_v8_exit_setups": expected["filter_to_v8_exit_setups"],
        "enable_s9_midday_lose": expected["enable_s9_midday_lose"],
        "enable_doc5d_avwap_reclaim": expected["enable_doc5d_avwap_reclaim"],
    }
    v12._v11_worker_init(str(data_root), base_state)

    scan = v12.candidate_scan
    scan.ALLOWED_SETUPS = frozenset(expected["allowed_setups"])
    scan.EXCLUDED_SETUPS = set(expected["excluded_setups"])
    scan.FILTER_TO_V8_EXIT_SETUPS = expected["filter_to_v8_exit_setups"]
    scan.EARLY_MODE_ENABLE = expected["early_mode_enable"]
    scan.EARLY_TIGHT_FILTERS_ENABLE = expected["early_tight_filters_enable"]
    scan.SELECTION_MODE = expected["selection_mode"]
    scan.v2.ENABLE_S9_MIDDAY_LOSE = expected["enable_s9_midday_lose"]
    scan.v2.ENABLE_DOC5D_AVWAP_RECLAIM = expected["enable_doc5d_avwap_reclaim"]
    scan.v2.ENABLE_NOISY_ADVANCED_SHORTS = expected["enable_noisy_advanced_shorts"]
    scan.v2.ENABLE_NATIVE_V2_MINED_FILTER = expected["enable_native_v2_mined_filter"]

    actual = _actual_worker_scan_state(v12, setup_conf_module)
    if actual != expected:
        raise RuntimeError(
            "spawned V12 scanner state differs from parent: "
            f"expected={expected!r} actual={actual!r}"
        )
    _WORKER_SCAN_STATE = actual
    _WORKER_SCAN_STATE_DIGEST = scan_state_digest(actual)
    _WORKER_START_MINUTE = int(start_minute)
    _WORKER_END_MINUTE = int(end_minute)


def _slot_times(day_df: pd.DataFrame, start_minute: int, end_minute: int) -> list[pd.Timestamp]:
    minutes = day_df["date"].dt.hour * 60 + day_df["date"].dt.minute
    selected = day_df.loc[minutes.between(int(start_minute), int(end_minute), inclusive="both"), "date"]
    return sorted({pd.Timestamp(value).floor("min") for value in selected})


def _late_signal_from_features(
    features: pd.DataFrame,
    slot_ist: Any,
) -> dict[str, Any] | None:
    """Exact ``late_bb10.signal_for_slot`` logic using an already-built frame."""
    if features is None or features.empty:
        return None
    v12 = _WORKER_V12
    late = v12.candidate_scan.late_bb10
    slot = _ist(slot_ist).floor("min")
    minute = slot.hour * 60 + slot.minute
    if not late.SIGNAL_START_MINUTE <= minute <= late.SIGNAL_END_MINUTE:
        return None
    same_day = features["session"].eq(slot.normalize())
    mask, score = late._stock_mask(features)
    eligible = features.loc[same_day & mask].sort_values("date")
    if eligible.empty or pd.Timestamp(eligible.iloc[0]["date"]).floor("min") != slot:
        return None
    idx = eligible.index[0]
    row = features.loc[idx]
    breakout = float(row["prev_high10"])
    trigger = math.ceil((float(row["high"]) + late.TICK_SIZE) / late.TICK_SIZE - 1e-9) * late.TICK_SIZE
    cancel = max(float(row["low"]), breakout)
    adx_two_bars_ago = features["adx"].shift(2).loc[idx]
    rsi_two_bars_ago = features["rsi"].shift(2).loc[idx]
    rank_score = (
        float(score.loc[idx])
        + min(float(row["rel_volume"]), 2.0) * 0.75
        + max(0.0, min(float(row["adx"] - adx_two_bars_ago), 10.0)) * 0.05
        + max(0.0, min(float(row["rsi"] - rsi_two_bars_ago), 15.0)) * 0.035
        - max(0.0, float(row["avwap_ext"])) * 1.50
        - max(0.0, float(row["range_atr"])) * 0.20
    )
    return {
        **row.to_dict(),
        "confirmation_score": int(score.loc[idx]),
        "quality_score": float(rank_score),
        "breakout_level": breakout,
        "entry_trigger_price": round(trigger, 2),
        "entry_cancel_price": round(cancel, 2),
        "entry_valid_minutes": late.ENTRY_VALID_MINUTES,
        "entry_max_gap_pct": late.ENTRY_MAX_GAP_PCT,
    }


def _late_candidate_tuple(ticker: str, custom: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
    v12 = _WORKER_V12
    scan = v12.candidate_scan
    late = scan.late_bb10
    slot = _ist(custom["date"]).floor("min")
    close = float(custom["close"])
    candle_range = float(custom["high"]) - float(custom["low"])
    body_pct = abs(float(custom["close"]) - float(custom["open"])) / candle_range if candle_range > 0 else 0.0
    atr = float(custom["atr"])
    vwap_dist_atr = (close - float(custom["avwap"])) / atr if atr > 0 else np.nan
    candidate = scan.v2.Candidate(
        ticker=str(ticker).upper(),
        date=str(slot.date()),
        setup=late.SETUP,
        side="LONG",
        signal_ts=slot,
        signal_close=close,
        entry_ts=slot + pd.Timedelta(minutes=1),
        entry_px=float(custom["entry_trigger_price"]),
        target_px=0.0,
        sl_px=0.0,
        quality_score=float(custom["quality_score"]),
        rs_pct=0.0,
        market_ret_pct=0.0,
        regime="NEUTRAL",
        vol_ratio=float(custom["rel_volume"]),
        atr_pct=float(custom["atr_pct"]),
        close_loc=float(custom["close_loc"]),
        body_pct=float(body_pct),
        vwap_dist_atr=float(vwap_dist_atr),
        day_value_so_far_rs=float(custom["traded_value"]),
        reason="late_bb10_causal_compression_breakout",
    )
    return candidate, custom


def _breadth_contributions(
    ticker: str,
    late_features: pd.DataFrame,
    dates: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Reproduce the production breadth denominator/numerator without reloading."""
    if late_features is None or late_features.empty or "NIFTY" in str(ticker).upper():
        return []
    d = late_features.copy()
    typical = (d["high"] + d["low"] + d["close"]) / 3.0
    pv = typical * d["volume"].fillna(0)
    cumulative_pv = pv.groupby(d["session"]).cumsum()
    cumulative_volume = d["volume"].fillna(0).groupby(d["session"]).cumsum()
    d["_breadth_avwap"] = cumulative_pv / cumulative_volume.replace(0, np.nan)
    by_time = d.set_index(d["date"].dt.floor("min"))
    rows: list[dict[str, Any]] = []
    late = _WORKER_V12.candidate_scan.late_bb10
    for day in dates:
        for minute in range(late.SIGNAL_START_MINUTE, late.SIGNAL_END_MINUTE + 1, 5):
            slot = pd.Timestamp(f"{day} {minute // 60:02d}:{minute % 60:02d}", tz=IST)
            if slot not in by_time.index:
                continue
            row = by_time.loc[slot]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[-1]
            avwap = float(row.get("_breadth_avwap", np.nan))
            if bool(row.get("valid", False)) and np.isfinite(avwap):
                rows.append(
                    {
                        "slot_ist": slot.isoformat(),
                        "above": int(float(row["close"]) >= avwap),
                        "total": 1,
                    }
                )
    return rows


def _scan_ticker_dates(payload: tuple[str, tuple[str, ...]]) -> dict[str, Any]:
    """Load/prepare one ticker once and scan all of its requested dates."""
    ticker, dates = payload
    started = time.perf_counter()
    if _WORKER_V12 is None or _WORKER_SCAN_STATE is None:
        raise RuntimeError("V12 missing-setup worker was not initialized")
    v12 = _WORKER_V12
    scan = v12.candidate_scan
    audit: dict[str, Any] = {
        "ticker": ticker,
        "requested_dates": len(dates),
        "file_loads": 0,
        "v2_preparations": 0,
        "late_feature_preparations": 0,
        "dates_with_data": 0,
        "native_scan_errors": 0,
        "early_scan_errors": 0,
        "candidate_rows": 0,
        "status": "ok",
        "error": "",
        "worker_state_digest": _WORKER_SCAN_STATE_DIGEST,
    }
    try:
        # One filtered Parquet read for the complete requested multi-day window
        # plus the immediately preceding session.  This avoids both 41 daily
        # reads and loading unrelated months into every worker.
        raw = v12._load_historical_5m_window(ticker, dates)
        audit["file_loads"] = 1
        if raw is None or raw.empty:
            audit["status"] = "no_data"
            audit["elapsed_sec"] = round(time.perf_counter() - started, 6)
            return {"rows": [], "breadth": [], "audit": audit}

        prepared = scan.v2._prepare_5m(raw)
        audit["v2_preparations"] = 1
        prepared = prepared.copy()
        prepared["date"] = v12._normalise_date_series(prepared["date"])
        prepared = prepared.dropna(subset=["date"]).sort_values("date").drop_duplicates("date", keep="last")
        prepared["date_only"] = prepared["date"].dt.date

        late_features = pd.DataFrame()
        if scan.late_bb10.SETUP in scan.ALLOWED_SETUPS:
            late_features = scan.late_bb10.add_features(raw)
            audit["late_feature_preparations"] = 1

        frames: list[pd.DataFrame] = []
        for day in dates:
            day_value = pd.Timestamp(day).date()
            day_df = prepared.loc[prepared["date_only"].eq(day_value)].copy().reset_index(drop=True)
            if day_df.empty:
                continue
            slots = _slot_times(day_df, _WORKER_START_MINUTE, _WORKER_END_MINUTE)
            if not slots:
                continue
            audit["dates_with_data"] += 1
            slot_set = set(slots)
            signal_rows = {
                pd.Timestamp(row["date"]).floor("min"): row.to_dict()
                for _, row in day_df.iterrows()
                if pd.Timestamp(row["date"]).floor("min") in slot_set
            }
            by_slot: dict[pd.Timestamp, list[tuple[Any, dict[str, Any]]]] = {}

            scan_df = scan._append_synthetic_successor(day_df, slots[-1])
            try:
                native = scan.v2._scan_day(scan_df, ticker, v12._V11_WORKER_MARKET_CTX or {}) or []
            except Exception:
                native = []
                audit["native_scan_errors"] += 1
            for candidate in native:
                candidate_slot = _ist(candidate.signal_ts).floor("min")
                setup = str(candidate.setup)
                if candidate_slot not in slot_set or setup not in _WORKER_TARGET_SETUPS:
                    continue
                if setup in scan.EXCLUDED_SETUPS:
                    continue
                signal_row = signal_rows.get(candidate_slot)
                if signal_row is not None:
                    by_slot.setdefault(candidate_slot, []).append((candidate, signal_row))

            if scan.EARLY_MODE_ENABLE:
                for slot in slots:
                    try:
                        early = scan._scan_early_slot_candidates(
                            scan_df, ticker, slot, v12._V11_WORKER_MARKET_CTX or {}
                        ) or []
                    except Exception:
                        early = []
                        audit["early_scan_errors"] += 1
                    for candidate in early:
                        setup = str(candidate.setup)
                        if setup not in _WORKER_TARGET_SETUPS or setup in scan.EXCLUDED_SETUPS:
                            continue
                        signal_row = signal_rows.get(slot)
                        if signal_row is not None:
                            by_slot.setdefault(slot, []).append((candidate, signal_row))

            if not late_features.empty:
                day_late = late_features.loc[late_features["session"].eq(pd.Timestamp(day, tz=IST).normalize())]
                if not day_late.empty:
                    for slot in slots:
                        minute = slot.hour * 60 + slot.minute
                        if not scan.late_bb10.SIGNAL_START_MINUTE <= minute <= scan.late_bb10.SIGNAL_END_MINUTE:
                            continue
                        custom = _late_signal_from_features(late_features, slot)
                        if custom is not None:
                            by_slot.setdefault(slot, []).append(_late_candidate_tuple(ticker, custom))

            for slot, rows in sorted(by_slot.items()):
                frame = scan.candidates_to_dataframe(rows, slot)
                if not frame.empty:
                    frames.append(frame)

        candidates = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if not candidates.empty:
            candidates = scan._dedupe_candidate_frame(candidates)
        audit["candidate_rows"] = int(len(candidates))
        breadth = _breadth_contributions(ticker, late_features, dates)
        audit["breadth_observations"] = int(len(breadth))
        audit["elapsed_sec"] = round(time.perf_counter() - started, 6)
        return {
            "rows": candidates.to_dict("records") if not candidates.empty else [],
            "breadth": breadth,
            "audit": audit,
        }
    except Exception as exc:
        audit["status"] = "error"
        audit["error"] = f"{type(exc).__name__}: {exc}"
        audit["elapsed_sec"] = round(time.perf_counter() - started, 6)
        return {"rows": [], "breadth": [], "audit": audit}


def _parse_hhmm(value: str) -> int:
    try:
        hour, minute = str(value).strip().split(":", 1)
        result = int(hour) * 60 + int(minute)
    except Exception as exc:
        raise argparse.ArgumentTypeError(f"invalid HH:MM value: {value!r}") from exc
    if result < 0 or result >= 24 * 60:
        raise argparse.ArgumentTypeError(f"invalid HH:MM value: {value!r}")
    return result


def _candidate_day(frame: pd.DataFrame) -> pd.Series:
    if frame is None or frame.empty:
        return pd.Series(index=frame.index if frame is not None else pd.RangeIndex(0), dtype="object")
    for column in ("signal_time_ist", "scan_slot_ist", "bar_time_ist", "slot_ist", "day"):
        if column not in frame.columns:
            continue
        if column == "day":
            return frame[column].astype(str).str[:10]
        return frame[column].map(_ist).map(
            lambda value: value.strftime("%Y-%m-%d") if pd.notna(value) else ""
        )
    return pd.Series("", index=frame.index, dtype="object")


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}")
    frame.to_csv(temporary, index=False)
    os.replace(temporary, path)


def _atomic_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _setup_counts(frame: pd.DataFrame) -> dict[str, int]:
    if frame is None or frame.empty or "setup" not in frame.columns:
        return {}
    return {
        str(key): int(value)
        for key, value in frame["setup"].astype(str).value_counts().sort_index().items()
    }


def _tag_supplement(
    frame: pd.DataFrame,
    stage: str,
    target_setups: Iterable[str],
    setup_scope: str,
) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    out = frame.copy()
    if not out.empty:
        out["v12_supplement_stage"] = stage
        out["v12_supplement_scope"] = "|".join(sorted(target_setups))
        out["v12_setup_scope"] = str(setup_scope)
        out["v12_supplement_research_only"] = True
    return out


def _full_slot_audit(
    dates: list[str],
    start_minute: int,
    end_minute: int,
    raw_before: pd.DataFrame,
    raw_after: pd.DataFrame,
    pipeline_slots: pd.DataFrame,
) -> pd.DataFrame:
    rows = [
        {"day": day, "slot_ist": pd.Timestamp(f"{day} {minute // 60:02d}:{minute % 60:02d}", tz=IST).isoformat()}
        for day in dates
        for minute in range(start_minute, end_minute + 1, 5)
    ]
    out = pd.DataFrame(rows)
    for name, frame in (("scanner_raw_before_prefilter", raw_before), ("scanner_raw_after_prefilter", raw_after)):
        if frame is None or frame.empty:
            counts = pd.DataFrame(columns=["slot_ist", name])
        else:
            column = next(
                (value for value in ("scan_slot_ist", "signal_time_ist") if value in frame.columns),
                None,
            )
            if column is None:
                counts = pd.DataFrame(columns=["slot_ist", name])
            else:
                keys = frame[column].map(lambda value: _ist(value).floor("min").isoformat())
                counts = keys.value_counts().rename_axis("slot_ist").rename(name).reset_index()
        out = out.merge(counts, on="slot_ist", how="left")
    if pipeline_slots is not None and not pipeline_slots.empty:
        pipeline = pipeline_slots.copy()
        pipeline["slot_ist"] = pipeline["slot_ist"].map(lambda value: _ist(value).floor("min").isoformat())
        pipeline = pipeline.drop(columns=["day"], errors="ignore")
        out = out.merge(pipeline, on="slot_ist", how="left")
    numeric = out.select_dtypes(include=["number"]).columns
    out[numeric] = out[numeric].fillna(0)
    return out


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Research-only efficient V12 multi-day prefilter replay"
    )
    parser.add_argument("--start-date", default="2026-06-04")
    parser.add_argument("--end-date", default="2026-08-03")
    parser.add_argument("--prefilter-candidates", type=Path, default=DEFAULT_PREFILTER)
    parser.add_argument("--budget", type=int, default=300)
    parser.add_argument("--effective-budget", type=int)
    parser.add_argument("--candidate-5m-dir", type=Path, default=DEFAULT_5M_DIR)
    parser.add_argument("--setup-conf-module", default="final_setup_conf_v12")
    parser.add_argument(
        "--setup-scope",
        choices=("missing", "all_active"),
        default="missing",
        help=(
            "missing scans only S9/DOC5D/LATE_BB10; all_active scans the exact "
            "active setup book while still loading/preparing each ticker once"
        ),
    )
    parser.add_argument("--out", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--chunksize", type=int, default=2)
    parser.add_argument("--start-time", default="09:20")
    parser.add_argument("--end-time", default="15:00")
    parser.add_argument("--ab-gate-min-quality", type=float, default=200.0)
    parser.add_argument("--ab-gate-max-per-side", type=int, default=2)
    parser.add_argument("--ab-gate-max-per-slot", type=int, default=4)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    start_day = pd.Timestamp(args.start_date)
    end_day = pd.Timestamp(args.end_date)
    if end_day < start_day:
        raise SystemExit("end date precedes start date")
    start_minute = _parse_hhmm(args.start_time)
    end_minute = _parse_hhmm(args.end_time)
    if end_minute < start_minute:
        raise SystemExit("end time precedes start time")
    if args.workers < 1 or args.chunksize < 1:
        raise SystemExit("workers and chunksize must be positive")
    if args.out.exists() and any(args.out.iterdir()):
        raise SystemExit(f"refusing non-empty output directory: {args.out}")

    pools = load_hourly_pools(
        args.prefilter_candidates,
        args.start_date,
        end_date_text=args.end_date,
        expected_budget=args.budget,
        effective_budget=args.effective_budget,
    )
    pool_dates = sorted({pool.slot_ist.strftime("%Y-%m-%d") for pool in pools})
    daily_unions = ticker_union_by_date(pools)
    payloads = ticker_dates_from_daily_unions(daily_unions)
    if not payloads:
        raise SystemExit("prefilter produced an empty ticker/date work list")

    v12 = _import_v12(args.setup_conf_module)
    conf = v12._activate_final_setup_conf()
    try:
        target_setups = setups_for_scope(args.setup_scope, conf)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    scan = v12.candidate_scan
    scan.ALLOWED_SETUPS = frozenset(target_setups)
    scan.FILTER_TO_V8_EXIT_SETUPS = True
    scan.v2.ENABLE_S9_MIDDAY_LOSE = True
    scan.v2.ENABLE_DOC5D_AVWAP_RECLAIM = True

    expected_state = canonical_scan_state(
        {
            "setup_conf_module": args.setup_conf_module,
            "target_setups": target_setups,
            "allowed_setups": target_setups,
            "excluded_setups": scan.EXCLUDED_SETUPS,
            "filter_to_v8_exit_setups": True,
            "enable_s9_midday_lose": True,
            "enable_doc5d_avwap_reclaim": True,
            "early_mode_enable": scan.EARLY_MODE_ENABLE,
            "early_tight_filters_enable": scan.EARLY_TIGHT_FILTERS_ENABLE,
            "enable_noisy_advanced_shorts": True,
            "enable_native_v2_mined_filter": False,
            "selection_mode": scan.SELECTION_MODE,
        }
    )
    expected_digest = scan_state_digest(expected_state)
    print(
        f"[v12 efficient replay] scope={args.setup_scope} dates={len(pool_dates)} "
        f"tickers={len(payloads)} workers={args.workers} state={expected_digest[:12]}",
        flush=True,
    )

    started = time.perf_counter()
    all_rows: list[dict[str, Any]] = []
    all_breadth: list[dict[str, Any]] = []
    worker_audit: list[dict[str, Any]] = []
    initargs = (
        str(args.candidate_5m_dir),
        str(args.setup_conf_module),
        expected_state,
        start_minute,
        end_minute,
    )
    if args.workers == 1:
        _worker_init(*initargs)
        iterator = map(_scan_ticker_dates, payloads)
        executor = None
    else:
        executor = ProcessPoolExecutor(
            max_workers=args.workers,
            initializer=_worker_init,
            initargs=initargs,
        )
        iterator = executor.map(_scan_ticker_dates, payloads, chunksize=args.chunksize)
    try:
        for index, result in enumerate(iterator, 1):
            all_rows.extend(result.get("rows", []))
            all_breadth.extend(result.get("breadth", []))
            worker_audit.append(dict(result.get("audit", {})))
            if index % 100 == 0 or index == len(payloads):
                print(
                    f"  [v12 efficient replay] tickers={index}/{len(payloads)} "
                    f"raw={len(all_rows)} elapsed={time.perf_counter() - started:.1f}s",
                    flush=True,
                )
    finally:
        if executor is not None:
            executor.shutdown(wait=True, cancel_futures=False)

    worker_audit_frame = pd.DataFrame(worker_audit)
    bad_state = worker_audit_frame.loc[
        ~worker_audit_frame.get("worker_state_digest", pd.Series("", index=worker_audit_frame.index)).eq(expected_digest)
    ]
    if not bad_state.empty:
        raise SystemExit(f"worker state digest mismatch for {len(bad_state)} ticker tasks")

    raw_before = pd.DataFrame(all_rows)
    if not raw_before.empty:
        raw_before = scan._dedupe_candidate_frame(raw_before)
    breadth = aggregate_breadth_contributions(all_breadth)
    # Membership is causal and precedes the expensive market-alignment lookup.
    # Only surviving Late-BB slots need Nifty alignment; breadth observations
    # were accumulated from the same once-per-ticker filtered reads above.
    raw_after, prefilter_stats = filter_candidates_for_hourly_pools(raw_before, pools)
    late_mask = (
        raw_after.get("setup", pd.Series("", index=raw_after.index))
        .astype(str)
        .eq("L_LATE_BB10_COMPRESSION_BREAKOUT")
    )
    late_slots = sorted(
        {
            _ist(value).floor("min")
            for value in raw_after.loc[late_mask, "scan_slot_ist"].dropna().tolist()
        }
    ) if not raw_after.empty and "scan_slot_ist" in raw_after.columns else []
    nifty_alignment: dict[str, dict[str, float]] = {}
    if late_slots:
        v12._set_candidate_5m_dir(args.candidate_5m_dir)
        nifty_alignment = scan.late_bb10.market_alignment_for_slots(
            [],
            late_slots,
            lambda ticker: v12._load_historical_5m_window(ticker, tuple(pool_dates)),
        )
        for index in raw_after.index[late_mask]:
            key = _ist(raw_after.at[index, "scan_slot_ist"]).floor("min").isoformat()
            raw_after.at[index, "market_breadth"] = breadth.get(key, {}).get("market_breadth", np.nan)
            raw_after.at[index, "nifty_ema_up"] = nifty_alignment.get(key, {}).get("nifty_ema_up", 0.0)
    pipeline = v12._apply_v7_live_strategy(
        raw_after,
        "",
        ab_gate_profile="quality_top_slot",
        ab_gate_min_quality=args.ab_gate_min_quality,
        ab_gate_max_per_side=args.ab_gate_max_per_side,
        ab_gate_max_per_slot=args.ab_gate_max_per_slot,
        selected_strategy_profile="final_setup_conf",
    )

    stages = {
        "scanner_raw_before_prefilter": raw_before,
        "raw_candidates": raw_after,
        "ranked_raw_candidates": pipeline["ranked_raw_candidates"],
        "v8_gated_candidates": pipeline["v8_gated_candidates"],
        "research_rejected_candidates": pipeline["research_rejected_candidates"],
        "pre_dedupe_live_candidates": pipeline["pre_dedupe_live_candidates"],
        "live_like_candidates": pipeline["live_like_candidates"],
    }
    stages = {
        name: _tag_supplement(frame, name, target_setups, args.setup_scope)
        for name, frame in stages.items()
    }
    raw_before = stages["scanner_raw_before_prefilter"]
    raw_after = stages["raw_candidates"]

    args.out.mkdir(parents=True, exist_ok=True)
    canonical_paths = {
        "scanner_raw_before_prefilter": args.out / "scanner_raw_candidates_before_prefilter.csv",
        "raw_candidates": args.out / "historical_all_available_raw_candidates.csv",
        "ranked_raw_candidates": args.out / "historical_all_available_ranked_raw_candidates.csv",
        "v8_gated_candidates": args.out / "historical_all_available_v8_gated_candidates.csv",
        "research_rejected_candidates": args.out / "historical_all_available_research_rejected_candidates.csv",
        "pre_dedupe_live_candidates": args.out / "historical_all_available_pre_dedupe_live_candidates.csv",
        "live_like_candidates": args.out / "historical_all_available_live_like_candidates.csv",
    }
    for name, path in canonical_paths.items():
        _atomic_csv(stages[name], path)
    _atomic_csv(stages["v8_gated_candidates"], args.out / "historical_all_available_gated_candidates.csv")

    pipeline_slot_audit = pipeline["slot_audit"].copy()
    _atomic_csv(
        pipeline_slot_audit,
        args.out / "historical_all_available_live_pipeline_slot_audit.csv",
    )
    complete_slot_audit = _full_slot_audit(
        pool_dates,
        start_minute,
        end_minute,
        raw_before,
        raw_after,
        pipeline_slot_audit,
    )
    _atomic_csv(complete_slot_audit, args.out / "complete_slot_audit.csv")
    _atomic_csv(worker_audit_frame, args.out / "worker_audit.csv")

    day_rows: list[dict[str, Any]] = []
    for day in pool_dates:
        row: dict[str, Any] = {
            "day": day,
            "prefilter_daily_union_tickers": len(daily_unions.get(day, ())),
        }
        daily_dir = args.out / "daily" / day
        for name, frame in stages.items():
            days = _candidate_day(frame)
            daily = frame.loc[days.eq(day)].copy() if not frame.empty else frame.copy()
            _atomic_csv(daily, daily_dir / f"{name}.csv")
            row[name] = int(len(daily))
            row[f"{name}_setup_counts_json"] = json.dumps(_setup_counts(daily), sort_keys=True)
        day_rows.append(row)
    day_audit = pd.DataFrame(day_rows)
    _atomic_csv(day_audit, args.out / "historical_all_available_pipeline_stats_by_day.csv")

    output_hashes = {
        path.name: _sha256(path)
        for path in [
            *canonical_paths.values(),
            args.out / "historical_all_available_gated_candidates.csv",
            args.out / "historical_all_available_live_pipeline_slot_audit.csv",
            args.out / "complete_slot_audit.csv",
            args.out / "worker_audit.csv",
            args.out / "historical_all_available_pipeline_stats_by_day.csv",
        ]
    }
    config_module = v12._load_final_setup_conf_module()
    config_path = Path(config_module.__file__).resolve()
    worker_errors = int(worker_audit_frame.get("status", pd.Series(dtype=str)).eq("error").sum())
    manifest = {
        "schema_version": "eqidv2_v12_efficient_prefilter_replay_v2",
        "mode": "RESEARCH_ONLY_CANDIDATE_REPLAY",
        "production_consumption_allowed": False,
        "created_at_ist": datetime.now(ZoneInfo(IST)).isoformat(),
        "setup_scope": args.setup_scope,
        "target_setups": list(target_setups),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "trading_dates": pool_dates,
        "trading_day_count": len(pool_dates),
        "start_time": args.start_time,
        "end_time": args.end_time,
        "ticker_task_count": len(payloads),
        "worker_count": args.workers,
        "worker_chunksize": args.chunksize,
        "worker_partitioning": "one_task_per_ticker_looping_all_requested_pool_dates",
        "worker_load_contract": "one_5m_file_load_and_one_v2_prepare_per_ticker_task",
        "expected_scan_state": expected_state,
        "expected_scan_state_digest": expected_digest,
        "observed_worker_state_digests": sorted(
            set(worker_audit_frame.get("worker_state_digest", pd.Series(dtype=str)).dropna().astype(str))
        ),
        "worker_error_count": worker_errors,
        "worker_no_data_count": int(worker_audit_frame.get("status", pd.Series(dtype=str)).eq("no_data").sum()),
        "prefilter_candidates_path": str(args.prefilter_candidates.resolve()),
        "prefilter_candidates_sha256": _sha256(args.prefilter_candidates),
        "prefilter_source_budget": args.budget,
        "prefilter_effective_budget": args.effective_budget or args.budget,
        "prefilter_activation_policy": "slot_HH20_activates_at_HH25_until_next_activation_same_date",
        "prefilter_stats": prefilter_stats,
        "candidate_5m_dir": str(args.candidate_5m_dir.resolve()),
        "setup_conf_module": args.setup_conf_module,
        "setup_conf_path": str(config_path),
        "setup_conf_sha256": _sha256(config_path),
        "v12_source_path": str(Path(v12.__file__).resolve()),
        "v12_source_sha256": _sha256(v12.__file__),
        "adapter_path": str(Path(__file__).resolve()),
        "adapter_sha256": _sha256(__file__),
        "pipeline_parameters": {
            "selected_strategy_profile": "final_setup_conf",
            "ab_gate_profile": "quality_top_slot",
            "ab_gate_min_quality": args.ab_gate_min_quality,
            "ab_gate_max_per_side": args.ab_gate_max_per_side,
            "ab_gate_max_per_slot": args.ab_gate_max_per_slot,
        },
        "pipeline_stats": pipeline["stats"],
        "stage_counts": {name: int(len(frame)) for name, frame in stages.items()},
        "stage_setup_counts": {name: _setup_counts(frame) for name, frame in stages.items()},
        "breadth_policy": "exact daily-union above-causal-session-avwap aggregation",
        "breadth_slot_count": len(breadth),
        "nifty_alignment_slot_count": len(nifty_alignment),
        "output_sha256": output_hashes,
        "elapsed_sec": round(time.perf_counter() - started, 3),
        "main_v7_modified": False,
        "main_v11_modified": False,
        "main_v12_modified": False,
        "final_setup_conf_v12_modified": False,
    }
    _atomic_json(manifest, args.out / "supplement_audit.json")
    print(
        f"[v12 efficient replay] complete scope={args.setup_scope} raw={len(raw_after)} "
        f"pre_dedupe={len(stages['pre_dedupe_live_candidates'])} "
        f"worker_errors={worker_errors} out={args.out}",
        flush=True,
    )
    return 2 if worker_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
