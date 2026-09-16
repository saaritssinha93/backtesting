#!/usr/bin/env python3
"""Deterministically regenerate the V13-v5 timing/entry research tables.

This is a read-only replay of the frozen V13-v3 signal caches.  It never calls
the cache builders and never writes into a strategy result directory.  By
default, generated files go into ``regenerated/`` beside this script so the
original research artifacts remain immutable.

The experiment contract is loaded from ``reproduction_spec.json``:

* exact V13-v3 setup book, 1.00% global OI cap, and 09:25 SHORT NIFTY gate;
* every inactive 5-minute time/side cell from 09:25 through 15:00, evaluated
  with one frozen modal setup;
* one-at-a-time active-slot removals and max-entry +1 relaxations;
* trigger-buffer, entry-window, confirmation-displacement, body, and wick
  one-factor entry experiments;
* causal stop-entry and pessimistic same-bar stop/target handling.

Examples
--------
    python run_timing_entry_experiments.py --help
    python run_timing_entry_experiments.py --verify-reference
    python run_timing_entry_experiments.py --output-dir C:/tmp/v13_v5_regen
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


HERE = Path(__file__).resolve().parent
DEFAULT_SPEC = HERE / "reproduction_spec.json"
DEFAULT_OUTPUT = HERE / "regenerated"
SUPPORTED_BY = "STRICT_CACHE_AND_FORWARD_HLC_PATHS"
MISSING_INDEX = np.iinfo(np.int32).max

SPLIT_ORDER = ("TRAIN", "VALIDATION", "PSEUDO_TEST", "ALL")
DETAIL_METRICS = (
    "sessions",
    "orders",
    "fills",
    "wins",
    "losses",
    "win_rate",
    "profit_factor",
    "net_pct",
    "expectancy_pct",
    "max_drawdown_pct",
    "target_hits",
    "stop_hits",
    "eod_exits",
    "median_entry_path_index",
    "median_holding_bars",
    "median_mfe_pct",
    "median_mae_pct",
)
SIMPLE_METRICS = DETAIL_METRICS[:9]
HEADLINE_METRICS = ("fills", "profit_factor", "win_rate", "net_pct")


@dataclass(frozen=True)
class Setup:
    signal_end: str
    confirmation_end: str
    side: str
    mode: str
    max_entries: int
    picker: str
    price_change_pct: float
    oi_change_pct: float
    volume_ratio: float
    body_ratio: float
    max_wick_ratio: float
    min_traded_value: float
    stop_pct: float
    target_pct: float
    source_version: str

    @property
    def setup_id(self) -> str:
        return f"{self.confirmation_end.replace(':', '')}_{self.side}"


@dataclass(frozen=True)
class EntryVariant:
    experiment_id: str
    family: str
    description: str
    trigger_buffer_pct: float | None = None
    expiry_bars: int | None = None
    activation_delay_bars: int | None = None
    confirmation_displacement_min_pct: float | None = None
    body_ratio_delta: float | None = None
    wick_cap_delta: float | None = None


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def json_clean(value: Any) -> Any:
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (float, np.floating)):
        if np.isnan(value):
            return None
        if np.isposinf(value):
            return "INF"
        if np.isneginf(value):
            return "-INF"
        return float(value)
    if isinstance(value, (date, datetime, pd.Timestamp, Path)):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): json_clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_clean(item) for item in value]
    return value


def canonical_setup_json(setup: Setup) -> str:
    return json.dumps(asdict(setup), sort_keys=True)


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, lineterminator="\n")


def write_json(payload: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(json_clean(payload), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _require_hash(path: Path, expected: str | None, allow_drift: bool) -> str:
    if not path.is_file():
        raise FileNotFoundError(path)
    observed = sha256(path)
    if expected and observed != expected and not allow_drift:
        raise RuntimeError(
            f"Input hash mismatch for {path}: expected {expected}, observed {observed}. "
            "Use --allow-input-drift only for an explicitly reviewed regeneration."
        )
    return observed


def _load_paths(npz_path: Path) -> dict[int, dict[str, np.ndarray]]:
    suffix = {"h": "high", "l": "low", "c": "close"}
    paths: dict[int, dict[str, np.ndarray]] = {}
    # A context manager closes the read-only ZipFile before any outputs begin.
    with np.load(npz_path, allow_pickle=False) as blob:
        for key in blob.files:
            sid_text, code = key.rsplit("_", 1)
            if code not in suffix:
                raise RuntimeError(f"Unexpected NPZ member: {key}")
            paths.setdefault(int(sid_text), {})[suffix[code]] = np.asarray(
                blob[key], dtype=float
            )
    for sid, path in paths.items():
        if set(path) != {"high", "low", "close"}:
            raise RuntimeError(f"Incomplete HLC path for sid={sid}: {sorted(path)}")
        lengths = {len(values) for values in path.values()}
        if len(lengths) != 1:
            raise RuntimeError(f"Unequal HLC path lengths for sid={sid}: {lengths}")
    return paths


def load_frozen_inputs(
    spec: Mapping[str, Any], allow_drift: bool
) -> tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]], list[dict[str, Any]]]:
    parts: list[tuple[pd.DataFrame, dict[int, dict[str, np.ndarray]]]] = []
    checks: list[dict[str, Any]] = []
    for record in spec["inputs"]:
        if "parquet" not in record:
            continue
        parquet = Path(record["parquet"])
        npz_path = Path(record["npz"])
        parquet_hash = _require_hash(
            parquet, record.get("parquet_sha256"), allow_drift
        )
        npz_hash = _require_hash(npz_path, record.get("npz_sha256"), allow_drift)
        signals = pd.read_parquet(parquet).copy()
        signals["day"] = pd.to_datetime(signals["day"]).dt.date
        paths = _load_paths(npz_path)
        signal_sids = set(signals["sid"].astype(int))
        if signal_sids != set(paths):
            raise RuntimeError(
                f"Signal/path sid mismatch in {parquet.name}: "
                f"signals={len(signal_sids)}, paths={len(paths)}"
            )
        if len(signals) != int(record.get("rows", len(signals))):
            raise RuntimeError(f"Unexpected row count in {parquet}: {len(signals)}")
        parts.append((signals, paths))
        checks.append(
            {
                "parquet": str(parquet.resolve()),
                "parquet_sha256": parquet_hash,
                "npz": str(npz_path.resolve()),
                "npz_sha256": npz_hash,
                "rows": len(signals),
                "paths": len(paths),
            }
        )

    frames: list[pd.DataFrame] = []
    merged_paths: dict[int, dict[str, np.ndarray]] = {}
    offset = 0
    for signals, paths in parts:
        block = signals.copy()
        block["sid"] = block["sid"].astype(int) + offset
        frames.append(block)
        for sid, path in paths.items():
            merged_paths[int(sid) + offset] = path
        offset += max(paths) + 1 if paths else 0
    if not frames:
        raise RuntimeError("No cache inputs were declared in the reproduction spec")
    signals = (
        pd.concat(frames, ignore_index=True, sort=False)
        .sort_values(["day", "sid"], kind="stable")
        .reset_index(drop=True)
    )
    return signals, merged_paths, checks


def assert_inputs_unchanged(checks: Sequence[Mapping[str, Any]]) -> None:
    for record in checks:
        for kind in ("parquet", "npz"):
            path = Path(str(record[kind]))
            observed = sha256(path)
            expected = str(record[f"{kind}_sha256"])
            if observed != expected:
                raise RuntimeError(
                    f"Frozen cache changed during regeneration: {path}; "
                    f"before={expected}, after={observed}"
                )


def _as_ist(stamps: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(stamps, errors="coerce")
    if parsed.dt.tz is None:
        return parsed.dt.tz_localize("Asia/Kolkata")
    return parsed.dt.tz_convert("Asia/Kolkata")


def annotate_nifty_gate(
    signals: pd.DataFrame, cache_checks: Sequence[Mapping[str, Any]]
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    # .../strategy_research/v13_corrected_v3/_cache/file -> .../fno_oi
    cache_dir = Path(str(cache_checks[0]["parquet"])).parent
    fno_root = cache_dir.parents[2]
    nifty_root = fno_root / "raw_contracts_5m"
    contexts: list[pd.DataFrame] = []
    source_records: list[dict[str, Any]] = []
    for month in sorted(signals["contract_month"].astype(str).unique()):
        path = nifty_root / f"NIFTY{month}FUT_5minute.parquet"
        if not path.is_file():
            continue
        source_records.append(
            {"path": str(path.resolve()), "sha256": sha256(path), "month": month}
        )
        frame = pd.read_parquet(path, columns=["timestamp", "open", "close"])
        stamps = _as_ist(frame["timestamp"])
        frame["day"] = stamps.dt.date
        frame["hhmm_int"] = stamps.dt.strftime("%H%M").astype(int)
        frame["open"] = pd.to_numeric(frame["open"], errors="coerce")
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame = frame.loc[frame["hhmm_int"].eq(920)].copy()
        frame["nifty_first_bar_return_pct"] = (
            frame["close"] / frame["open"] - 1.0
        ) * 100.0
        frame["contract_month"] = month
        contexts.append(
            frame[["contract_month", "day", "nifty_first_bar_return_pct"]]
        )
    context = (
        pd.concat(contexts, ignore_index=True)
        .drop_duplicates(["contract_month", "day"], keep="last")
        if contexts
        else pd.DataFrame(
            columns=["contract_month", "day", "nifty_first_bar_return_pct"]
        )
    )
    annotated = signals.merge(
        context,
        on=["contract_month", "day"],
        how="left",
        validate="many_to_one",
    )
    applies = annotated["hhmm_int"].eq(925) & annotated["side"].eq("SHORT")
    annotated["nifty_first_bar_gate_applies"] = applies
    annotated["nifty_first_bar_gate_pass"] = (~applies) | (
        annotated["nifty_first_bar_return_pct"].notna()
        & annotated["nifty_first_bar_return_pct"].le(-0.05)
    )
    return annotated, source_records


def picker_column(setup: Setup) -> str:
    return {
        "max_oi": "oi_change_pct",
        "max_volume": "volume_ratio",
        "max_move": "abs_price_change_pct",
        "max_body": "body_ratio",
        "max_liquidity": "traded_value",
    }[setup.picker]


def eligible_rows(signals: pd.DataFrame, setup: Setup) -> pd.DataFrame:
    rows = signals.loc[
        signals["hhmm_int"].eq(int(setup.signal_end.replace(":", "")))
        & signals["side"].eq(setup.side)
    ].copy()
    if rows.empty or setup.mode == "FORCE_DAILY":
        return rows
    price_ok = (
        rows["price_change_pct"].ge(setup.price_change_pct)
        if setup.side == "LONG"
        else rows["price_change_pct"].le(-setup.price_change_pct)
    )
    return rows.loc[
        price_ok
        & rows["oi_change_pct"].ge(setup.oi_change_pct)
        & rows["volume_ratio"].ge(setup.volume_ratio)
        & rows["body_ratio"].ge(setup.body_ratio)
        & rows["wick_ratio"].le(setup.max_wick_ratio)
        & rows["traded_value"].ge(setup.min_traded_value)
    ].copy()


def select_setup_rows(signals: pd.DataFrame, setup: Setup) -> pd.DataFrame:
    rows = eligible_rows(signals, setup)
    if rows.empty:
        return rows
    column = picker_column(setup)
    if column == "abs_price_change_pct":
        rows[column] = rows["price_change_pct"].abs()
    rows = rows.sort_values(
        ["day", column, "traded_value", "tradingsymbol"],
        ascending=[True, False, False, True],
        kind="stable",
    )
    return rows.groupby("day", sort=False, as_index=False).head(setup.max_entries)


def directional_confirmation_displacement(frame: pd.DataFrame) -> pd.Series:
    raw = (frame["confirmation_close"] / frame["signal_close"] - 1.0) * 100.0
    return raw.where(frame["side"].eq("LONG"), -raw)


def simulate_selected(
    selected: pd.DataFrame,
    paths: Mapping[int, Mapping[str, np.ndarray]],
    setup: Setup,
    *,
    cost_bps: float,
    trigger_buffer_pct: float = 0.0,
    expiry_bars: int | None = None,
    activation_delay_bars: int = 0,
) -> pd.DataFrame:
    out = selected.copy().reset_index(drop=True)
    columns: dict[str, list[Any]] = {
        "effective_trigger": [],
        "net_return_pct": [],
        "filled": [],
        "entry_path_index": [],
        "holding_bars": [],
        "mfe_pct": [],
        "mae_pct": [],
        "exit_reason": [],
    }
    cost = float(cost_bps) / 10000.0
    for row in out.itertuples(index=False):
        path = paths.get(int(row.sid))
        long_side = row.side == "LONG"
        trigger = float(row.trigger) * (
            1.0 + trigger_buffer_pct / 100.0
            if long_side
            else 1.0 - trigger_buffer_pct / 100.0
        )
        columns["effective_trigger"].append(trigger)
        if path is None or len(path["high"]) == 0:
            for name in (
                "net_return_pct",
                "entry_path_index",
                "holding_bars",
                "mfe_pct",
                "mae_pct",
            ):
                columns[name].append(np.nan)
            columns["filled"].append(False)
            columns["exit_reason"].append("")
            continue
        high = np.asarray(path["high"], dtype=float)
        low = np.asarray(path["low"], dtype=float)
        close = np.asarray(path["close"], dtype=float)
        start = max(0, int(activation_delay_bars))
        touches = (
            np.flatnonzero(high[start:] >= trigger)
            if long_side
            else np.flatnonzero(low[start:] <= trigger)
        )
        entry = start + int(touches[0]) if touches.size else None
        if entry is None or (expiry_bars is not None and entry >= int(expiry_bars)):
            for name in (
                "net_return_pct",
                "entry_path_index",
                "holding_bars",
                "mfe_pct",
                "mae_pct",
            ):
                columns[name].append(np.nan)
            columns["filled"].append(False)
            columns["exit_reason"].append("")
            continue

        stop = trigger * (
            1.0 - setup.stop_pct / 100.0
            if long_side
            else 1.0 + setup.stop_pct / 100.0
        )
        target = trigger * (
            1.0 + setup.target_pct / 100.0
            if long_side
            else 1.0 - setup.target_pct / 100.0
        )
        stop_hits = (
            np.flatnonzero(low[entry:] <= stop)
            if long_side
            else np.flatnonzero(high[entry:] >= stop)
        )
        target_hits = (
            np.flatnonzero(high[entry:] >= target)
            if long_side
            else np.flatnonzero(low[entry:] <= target)
        )
        stop_index = int(stop_hits[0]) if stop_hits.size else int(MISSING_INDEX)
        target_index = (
            int(target_hits[0]) if target_hits.size else int(MISSING_INDEX)
        )
        if stop_index == target_index == MISSING_INDEX:
            exit_index = len(close) - 1
            exit_price = float(close[-1])
            reason = "EOD"
        elif stop_index <= target_index:  # pessimistic same-bar tie handling
            exit_index = entry + stop_index
            exit_price = stop
            reason = "STOP"
        else:
            exit_index = entry + target_index
            exit_price = target
            reason = "TARGET"
        gross = (
            exit_price / trigger - 1.0
            if long_side
            else 1.0 - exit_price / trigger
        )
        observed = slice(entry, exit_index + 1)
        if long_side:
            mfe = (float(np.max(high[observed])) / trigger - 1.0) * 100.0
            mae = (float(np.min(low[observed])) / trigger - 1.0) * 100.0
        else:
            mfe = (1.0 - float(np.min(low[observed])) / trigger) * 100.0
            mae = (1.0 - float(np.max(high[observed])) / trigger) * 100.0
        columns["net_return_pct"].append((gross - cost) * 100.0)
        columns["filled"].append(True)
        columns["entry_path_index"].append(entry)
        columns["holding_bars"].append(exit_index - entry)
        columns["mfe_pct"].append(mfe)
        columns["mae_pct"].append(mae)
        columns["exit_reason"].append(reason)

    for name, values in columns.items():
        out[name] = values
    out["setup_id"] = setup.setup_id
    out["confirmation_end"] = setup.confirmation_end
    out["setup_mode"] = setup.mode
    out["picker"] = setup.picker
    out["max_entries"] = setup.max_entries
    out["stop_pct"] = setup.stop_pct
    out["target_pct"] = setup.target_pct
    return out


def replay_book(
    policy_signals: pd.DataFrame,
    paths: Mapping[int, Mapping[str, np.ndarray]],
    setups: Iterable[Setup],
    *,
    cost_bps: float,
    variant: EntryVariant | None = None,
) -> pd.DataFrame:
    variant = variant or EntryVariant("BASELINE_V13_V3", "BASELINE", "baseline")
    source = policy_signals
    if variant.confirmation_displacement_min_pct is not None:
        displacement = directional_confirmation_displacement(source)
        source = source.loc[
            displacement.ge(variant.confirmation_displacement_min_pct)
        ].copy()
    parts: list[pd.DataFrame] = []
    for original in setups:
        setup = original
        if variant.body_ratio_delta is not None:
            setup = replace(
                setup, body_ratio=max(0.0, setup.body_ratio + variant.body_ratio_delta)
            )
        if variant.wick_cap_delta is not None:
            setup = replace(
                setup,
                max_wick_ratio=min(
                    1.0, max(0.0, setup.max_wick_ratio + variant.wick_cap_delta)
                ),
            )
        selected = select_setup_rows(source, setup)
        if selected.empty:
            continue
        parts.append(
            simulate_selected(
                selected,
                paths,
                setup,
                cost_bps=cost_bps,
                trigger_buffer_pct=variant.trigger_buffer_pct or 0.0,
                expiry_bars=variant.expiry_bars,
                activation_delay_bars=variant.activation_delay_bars or 0,
            )
        )
    if not parts:
        return empty_audit()
    return (
        pd.concat(parts, ignore_index=True, sort=False)
        .sort_values(
            ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def empty_audit() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "sid",
            "day",
            "hhmm_int",
            "side",
            "tradingsymbol",
            "setup_id",
            "filled",
            "net_return_pct",
            "entry_path_index",
            "holding_bars",
            "mfe_pct",
            "mae_pct",
            "exit_reason",
        ]
    )


def _pf(values: np.ndarray) -> float:
    profit = float(values[values > 0].sum()) if values.size else 0.0
    loss = float(-values[values < 0].sum()) if values.size else 0.0
    if loss > 0:
        return profit / loss
    if profit > 0:
        return float("inf")
    return float("nan")


def audit_metrics(
    audit: pd.DataFrame, days: Sequence[date], *, detailed: bool = True
) -> dict[str, Any]:
    day_set = set(days)
    subset = audit.loc[audit["day"].isin(day_set)].copy() if not audit.empty else audit
    filled = subset.loc[subset["filled"].astype(bool)].copy() if not subset.empty else subset
    returns = pd.to_numeric(filled.get("net_return_pct"), errors="coerce").dropna().to_numpy(float)
    daily = (
        filled.groupby("day", sort=True)["net_return_pct"].sum().reindex(days, fill_value=0.0)
        if not filled.empty
        else pd.Series(np.zeros(len(days)), index=list(days), dtype=float)
    )
    curve = np.r_[0.0, np.cumsum(daily.to_numpy(float))]
    drawdown = curve - np.maximum.accumulate(curve)
    result: dict[str, Any] = {
        "sessions": len(days),
        "orders": len(subset),
        "fills": len(filled),
        "wins": int((returns > 0).sum()),
        "losses": int((returns < 0).sum()),
        "win_rate": float((returns > 0).mean()) if returns.size else np.nan,
        "profit_factor": _pf(returns),
        "net_pct": float(returns.sum()) if returns.size else 0.0,
        "expectancy_pct": float(returns.mean()) if returns.size else np.nan,
        "max_drawdown_pct": float(drawdown.min()) if drawdown.size else 0.0,
    }
    if detailed:
        reasons = filled.get("exit_reason", pd.Series(dtype=str))
        result.update(
            {
                "target_hits": int(reasons.eq("TARGET").sum()),
                "stop_hits": int(reasons.eq("STOP").sum()),
                "eod_exits": int(reasons.eq("EOD").sum()),
                "median_entry_path_index": pd.to_numeric(
                    filled.get("entry_path_index"), errors="coerce"
                ).median(),
                "median_holding_bars": pd.to_numeric(
                    filled.get("holding_bars"), errors="coerce"
                ).median(),
                "median_mfe_pct": pd.to_numeric(
                    filled.get("mfe_pct"), errors="coerce"
                ).median(),
                "median_mae_pct": pd.to_numeric(
                    filled.get("mae_pct"), errors="coerce"
                ).median(),
            }
        )
    return result


def split_metrics(
    audit: pd.DataFrame,
    split_days: Mapping[str, Sequence[date]],
    *,
    detailed: bool = True,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    fields = DETAIL_METRICS if detailed else SIMPLE_METRICS
    for split in SPLIT_ORDER:
        values = audit_metrics(audit, split_days[split], detailed=detailed)
        prefix = split.lower()
        for field in fields:
            result[f"{prefix}_{field}"] = values[field]
    return result


def add_deltas(row: dict[str, Any], baseline: Mapping[str, Any]) -> None:
    for field in HEADLINE_METRICS:
        row[f"delta_all_{field}"] = row[f"all_{field}"] - baseline[f"all_{field}"]


def parse_split_days(spec: Mapping[str, Any]) -> dict[str, list[date]]:
    return {
        split: [pd.Timestamp(value).date() for value in spec["split"][split]]
        for split in SPLIT_ORDER
    }


def minute_slots(start: str = "09:25", end: str = "15:00") -> list[str]:
    current = datetime.strptime(start, "%H:%M")
    finish = datetime.strptime(end, "%H:%M")
    slots: list[str] = []
    while current <= finish:
        slots.append(current.strftime("%H:%M"))
        current += timedelta(minutes=5)
    return slots


def confirmation_end(signal_end: str) -> str:
    return (datetime.strptime(signal_end, "%H:%M") + timedelta(minutes=1)).strftime(
        "%H:%M"
    )


def modal_setup(spec: Mapping[str, Any], signal_end: str, side: str) -> Setup:
    payload = dict(spec["time_addition_one_factor"]["template"])
    payload.update(
        {
            "signal_end": signal_end,
            "confirmation_end": confirmation_end(signal_end),
            "side": side,
        }
    )
    return Setup(**payload)


def verify_baseline_parity(
    baseline: pd.DataFrame, spec: Mapping[str, Any], allow_drift: bool
) -> dict[str, Any]:
    published_record = next(item for item in spec["inputs"] if "path" in item)
    published_path = Path(published_record["path"])
    published_hash = _require_hash(
        published_path, published_record.get("sha256"), allow_drift
    )
    published = pd.read_csv(published_path)
    published["day"] = pd.to_datetime(published["day"]).dt.date
    if published["filled"].dtype != bool:
        published["filled"] = (
            published["filled"].astype(str).str.strip().str.lower().eq("true")
        )
    keys = ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"]
    left = baseline.sort_values(keys, kind="stable").reset_index(drop=True)
    right = published.sort_values(keys, kind="stable").reset_index(drop=True)
    key_mismatches = (
        len(left) + len(right)
        if len(left) != len(right)
        else int((left[keys].astype(str) != right[keys].astype(str)).any(axis=1).sum())
    )
    fill_mismatches = (
        abs(len(left) - len(right))
        if len(left) != len(right)
        else int((left["filled"].to_numpy(bool) != right["filled"].to_numpy(bool)).sum())
    )
    max_abs_net_diff = (
        float("inf")
        if len(left) != len(right)
        else float(
            np.nanmax(
                np.abs(
                    pd.to_numeric(left["net_return_pct"], errors="coerce").to_numpy(float)
                    - pd.to_numeric(right["net_return_pct"], errors="coerce").to_numpy(float)
                )
            )
        )
    )
    result = {
        "published_path": str(published_path.resolve()),
        "published_sha256": published_hash,
        "published_orders": len(right),
        "replay_orders": len(left),
        "key_mismatches": key_mismatches,
        "fill_mismatches": fill_mismatches,
        "max_abs_net_diff": max_abs_net_diff,
    }
    expected = spec.get("expected_parity", {})
    if (
        key_mismatches != int(expected.get("key_mismatches", 0))
        or fill_mismatches != int(expected.get("fill_mismatches", 0))
        or max_abs_net_diff > 1e-12
    ):
        raise RuntimeError(f"Frozen V13-v3 baseline parity failed: {result}")
    return result


def build_entry_variants(spec: Mapping[str, Any]) -> list[EntryVariant]:
    config = spec["entry_variants"]
    variants = [
        EntryVariant("BASELINE_V13_V3", "BASELINE", "Exact frozen V13-v3 replay")
    ]
    for value in config["trigger_buffer_pct"]:
        variants.append(
            EntryVariant(
                f"TRIGGER_BUFFER_{value:.2f}PCT",
                "TRIGGER_BUFFER",
                f"Move stop-entry trigger {value:.2f}% beyond confirmation extreme for every setup",
                trigger_buffer_pct=float(value),
            )
        )
    for value in config["entry_expiry_forward_bars"]:
        variants.append(
            EntryVariant(
                f"ENTRY_EXPIRY_{int(value)}BAR",
                "ENTRY_WINDOW",
                f"Order must first touch trigger in first {int(value)} forward 1m bar(s); exits retain full path",
                expiry_bars=int(value),
            )
        )
    for value in config["activation_delay_forward_bars"]:
        variants.append(
            EntryVariant(
                f"ACTIVATION_DELAY_{int(value)}BAR",
                "ENTRY_WINDOW",
                f"Activate stop-entry only after skipping first {int(value)} forward 1m bar(s)",
                activation_delay_bars=int(value),
            )
        )
    for value in config["confirmation_directional_displacement_min_pct"]:
        variants.append(
            EntryVariant(
                f"CONFIRM_DISPLACEMENT_MIN_{value:.2f}PCT",
                "CONFIRMATION_FILTER",
                f"Require directional confirmation-close displacement >= {value:.2f}% from signal close",
                confirmation_displacement_min_pct=float(value),
            )
        )
    for value in config["body_ratio_min_delta"]:
        variants.append(
            EntryVariant(
                f"BODY_THRESHOLD_DELTA_{value:+.2f}",
                "CONFIRMATION_FILTER",
                f"Change every setup body-ratio minimum by {value:+.2f}",
                body_ratio_delta=float(value),
            )
        )
    for value in config["adverse_wick_cap_delta"]:
        variants.append(
            EntryVariant(
                f"WICK_CAP_DELTA_{value:+.2f}",
                "CONFIRMATION_FILTER",
                f"Change every setup adverse-wick cap by {value:+.2f}",
                wick_cap_delta=float(value),
            )
        )
    return variants


def build_entry_experiments(
    policy_signals: pd.DataFrame,
    paths: Mapping[int, Mapping[str, np.ndarray]],
    setups: Sequence[Setup],
    split_days: Mapping[str, Sequence[date]],
    baseline_metrics: Mapping[str, Any],
    spec: Mapping[str, Any],
    cost_bps: float,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    rows: list[dict[str, Any]] = []
    audits: dict[str, pd.DataFrame] = {}
    for variant in build_entry_variants(spec):
        audit = replay_book(
            policy_signals,
            paths,
            setups,
            cost_bps=cost_bps,
            variant=variant,
        )
        audits[variant.experiment_id] = audit
        row: dict[str, Any] = {
            "experiment_id": variant.experiment_id,
            "family": variant.family,
            "description": variant.description,
            "run_status": "COMPLETED",
            "causal": True,
            "supported_by": SUPPORTED_BY,
            **split_metrics(audit, split_days, detailed=True),
        }
        add_deltas(row, baseline_metrics)
        row.update(
            {
                "trigger_buffer_pct": variant.trigger_buffer_pct,
                "expiry_bars": variant.expiry_bars,
                "activation_delay_bars": variant.activation_delay_bars,
                "confirmation_displacement_min_pct": variant.confirmation_displacement_min_pct,
                "body_ratio_delta": variant.body_ratio_delta,
                "wick_cap_delta": variant.wick_cap_delta,
            }
        )
        rows.append(row)
    return pd.DataFrame(rows), audits


def build_timing_inventory(
    all_signals: pd.DataFrame,
    gated_signals: pd.DataFrame,
    policy_signals: pd.DataFrame,
    paths: Mapping[int, Mapping[str, np.ndarray]],
    setups: Sequence[Setup],
    split_days: Mapping[str, Sequence[date]],
    spec: Mapping[str, Any],
    cost_bps: float,
) -> tuple[pd.DataFrame, dict[tuple[str, str], pd.DataFrame], dict[tuple[str, str], Setup]]:
    active = {(setup.signal_end, setup.side): setup for setup in setups}
    audits: dict[tuple[str, str], pd.DataFrame] = {}
    resolved: dict[tuple[str, str], Setup] = {}
    rows: list[dict[str, Any]] = []
    for signal_end in minute_slots():
        hhmm = int(signal_end.replace(":", ""))
        for side in ("LONG", "SHORT"):
            key = (signal_end, side)
            setup = active.get(key) or modal_setup(spec, signal_end, side)
            audit = replay_book(
                policy_signals, paths, [setup], cost_bps=cost_bps
            )
            audits[key] = audit
            resolved[key] = setup
            strict = all_signals.loc[
                all_signals["hhmm_int"].eq(hhmm) & all_signals["side"].eq(side)
            ]
            after_gate = gated_signals.loc[
                gated_signals["hhmm_int"].eq(hhmm)
                & gated_signals["side"].eq(side)
            ]
            after_cap = policy_signals.loc[
                policy_signals["hhmm_int"].eq(hhmm)
                & policy_signals["side"].eq(side)
            ]
            rows.append(
                {
                    "hhmm_int": hhmm,
                    "signal_end": signal_end,
                    "confirmation_end": setup.confirmation_end,
                    "side": side,
                    "is_active_v13_v3": key in active,
                    "evaluation_template": (
                        "EXACT_ACTIVE_SETUP"
                        if key in active
                        else "FROZEN_MODAL_TIME_ONLY_TEMPLATE"
                    ),
                    "strict_confirmed_candidates": len(strict),
                    "after_nifty_gate": len(after_gate),
                    "after_global_oi_cap": len(after_cap),
                    "setup_id": setup.setup_id,
                    "template_price_change_pct": setup.price_change_pct,
                    "template_oi_change_pct": setup.oi_change_pct,
                    "template_volume_ratio": setup.volume_ratio,
                    "template_body_ratio": setup.body_ratio,
                    "template_max_wick_ratio": setup.max_wick_ratio,
                    "template_picker": setup.picker,
                    "template_max_entries": setup.max_entries,
                    "template_stop_pct": setup.stop_pct,
                    "template_target_pct": setup.target_pct,
                    **split_metrics(audit, split_days, detailed=True),
                }
            )
    return pd.DataFrame(rows), audits, resolved


def _combined_audit(*frames: pd.DataFrame) -> pd.DataFrame:
    nonempty = [frame for frame in frames if not frame.empty]
    if not nonempty:
        return empty_audit()
    return (
        pd.concat(nonempty, ignore_index=True, sort=False)
        .sort_values(
            ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def _leg_summary_fields(
    audit: pd.DataFrame, split_days: Mapping[str, Sequence[date]]
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for split in SPLIT_ORDER:
        metrics = audit_metrics(audit, split_days[split], detailed=False)
        prefix = split.lower()
        for field in (
            "orders",
            "fills",
            "wins",
            "losses",
            "win_rate",
            "profit_factor",
            "net_pct",
            "expectancy_pct",
        ):
            result[f"leg_{prefix}_{field}"] = metrics[field]
    dev_days = list(split_days["TRAIN"]) + list(split_days["VALIDATION"])
    development = audit_metrics(audit, dev_days, detailed=False)
    for field in ("fills", "wins", "win_rate", "net_pct", "profit_factor"):
        result[f"leg_development_{field}"] = development[field]
    return result


def _is_leg_admissible(row: Mapping[str, Any]) -> tuple[bool, str]:
    reasons: list[str] = []
    for prefix in ("train", "validation"):
        if int(row[f"leg_{prefix}_fills"]) < 2:
            reasons.append(f"LEG_{prefix.upper()}_FILLS_LT_2")
        if not float(row[f"leg_{prefix}_net_pct"]) > 0:
            reasons.append(f"LEG_{prefix.upper()}_NET_NONPOSITIVE")
        if not float(row[f"leg_{prefix}_profit_factor"]) > 1:
            reasons.append(f"LEG_{prefix.upper()}_PF_NOT_GT_1")
    return not reasons, "|".join(reasons)


def nondominated_indices(
    frame: pd.DataFrame, columns: Sequence[str]
) -> list[int]:
    indices: list[int] = []
    values = frame.loc[:, columns].astype(float).to_numpy()
    for i, candidate in enumerate(values):
        dominated = False
        for j, challenger in enumerate(values):
            if i == j:
                continue
            if np.all(challenger >= candidate) and np.any(challenger > candidate):
                dominated = True
                break
        if not dominated:
            indices.append(int(frame.index[i]))
    return indices


def build_timing_additions(
    inventory: pd.DataFrame,
    leg_audits: Mapping[tuple[str, str], pd.DataFrame],
    setups_by_cell: Mapping[tuple[str, str], Setup],
    baseline_audit: pd.DataFrame,
    baseline_metrics: Mapping[str, Any],
    split_days: Mapping[str, Sequence[date]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    metric_columns = {
        f"{split.lower()}_{field}"
        for split in SPLIT_ORDER
        for field in DETAIL_METRICS
    }
    for inventory_row in inventory.loc[~inventory["is_active_v13_v3"]].itertuples(
        index=False
    ):
        signal_end = str(inventory_row.signal_end)
        side = str(inventory_row.side)
        key = (signal_end, side)
        leg = leg_audits[key]
        combined = _combined_audit(baseline_audit, leg)
        setup = setups_by_cell[key]
        leg_all = audit_metrics(leg, split_days["ALL"], detailed=False)
        row: dict[str, Any] = {
            "experiment_id": f"ADD_{signal_end.replace(':', '')}_{side}",
            "family": "TIMING_ADDITION",
            "description": f"Add {signal_end} {side} only with frozen modal template; baseline unchanged",
            "run_status": "COMPLETED",
            "causal": True,
            "supported_by": SUPPORTED_BY,
            "changed_signal_end": signal_end,
            "changed_side": side,
            "leg_orders": leg_all["orders"],
            "leg_fills": leg_all["fills"],
            "leg_net_pct": leg_all["net_pct"],
            "leg_profit_factor": leg_all["profit_factor"],
            "template_setup_json": canonical_setup_json(setup),
            **split_metrics(combined, split_days, detailed=True),
        }
        add_deltas(row, baseline_metrics)
        inv = inventory_row._asdict()
        for name, value in inv.items():
            row[f"{name}_leg" if name in metric_columns else name] = value
        row.update(_leg_summary_fields(leg, split_days))
        admissible, reason = _is_leg_admissible(row)
        row["marginal_development_admissible"] = admissible
        row["marginal_development_selection_status"] = (
            "ELIGIBLE_FOR_DEVELOPMENT_PARETO"
            if admissible
            else "REJECTED_BEFORE_PSEUDO_TEST"
        )
        row["marginal_development_rejection_reason"] = reason
        rows.append(row)
    out = pd.DataFrame(rows)
    eligible = out.loc[out["marginal_development_admissible"]].copy()
    if not eligible.empty:
        frontier = nondominated_indices(
            eligible,
            [
                "leg_development_fills",
                "leg_development_profit_factor",
                "leg_development_win_rate",
                "leg_development_net_pct",
            ],
        )
        selected_ids = set(out.loc[frontier, "experiment_id"])
        selected = out["experiment_id"].isin(selected_ids)
        out.loc[selected, "marginal_development_selection_status"] = (
            "SELECTED_DEVELOPMENT_PARETO"
        )
        out.loc[
            out["marginal_development_admissible"] & ~selected,
            "marginal_development_selection_status",
        ] = "DOMINATED_ON_DEVELOPMENT"
        out.loc[
            out["marginal_development_admissible"] & ~selected,
            "marginal_development_rejection_reason",
        ] = "DOMINATED_ON_LEG_DEVELOPMENT_FILLS_PF_WR_NET"
    return out


def build_timing_removals(
    baseline_audit: pd.DataFrame,
    setups: Sequence[Setup],
    baseline_metrics: Mapping[str, Any],
    split_days: Mapping[str, Sequence[date]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for setup in setups:
        audit = baseline_audit.loc[
            ~baseline_audit["setup_id"].eq(setup.setup_id)
        ].copy()
        row: dict[str, Any] = {
            "experiment_id": f"REMOVE_{setup.signal_end.replace(':', '')}_{setup.side}",
            "family": "TIMING_REMOVAL",
            "description": f"Remove exact active {setup.signal_end} {setup.side} setup only",
            "run_status": "COMPLETED",
            "causal": True,
            "supported_by": SUPPORTED_BY,
            "changed_signal_end": setup.signal_end,
            "changed_side": setup.side,
            "removed_setup_id": setup.setup_id,
            "removed_setup_json": canonical_setup_json(setup),
            **split_metrics(audit, split_days, detailed=True),
        }
        add_deltas(row, baseline_metrics)
        rows.append(row)
    return pd.DataFrame(rows)


def build_max_entries_experiments(
    policy_signals: pd.DataFrame,
    paths: Mapping[int, Mapping[str, np.ndarray]],
    setups: Sequence[Setup],
    baseline_metrics_simple: Mapping[str, Any],
    split_days: Mapping[str, Sequence[date]],
    cost_bps: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for index, setup in enumerate(setups):
        changed = replace(setup, max_entries=setup.max_entries + 1)
        candidate = list(setups)
        candidate[index] = changed
        audit = replay_book(policy_signals, paths, candidate, cost_bps=cost_bps)
        row: dict[str, Any] = {
            "experiment_id": f"MAX_ENTRIES_PLUS1_{setup.signal_end.replace(':', '')}_{setup.side}",
            "family": "MAX_ENTRIES_RELAXATION",
            "description": (
                f"Increase only {setup.signal_end} {setup.side} max_entries "
                f"from {setup.max_entries} to {changed.max_entries}"
            ),
            "run_status": "COMPLETED",
            "causal": True,
            "supported_by": SUPPORTED_BY,
            "changed_signal_end": setup.signal_end,
            "changed_side": setup.side,
            "baseline_max_entries": setup.max_entries,
            "candidate_max_entries": changed.max_entries,
            "setup_id_before": setup.setup_id,
            "setup_id_after": changed.setup_id,
            **split_metrics(audit, split_days, detailed=False),
        }
        add_deltas(row, baseline_metrics_simple)
        rows.append(row)
    return pd.DataFrame(rows)


def build_funnel(
    all_signals: pd.DataFrame,
    paths: Mapping[int, Mapping[str, np.ndarray]],
    setups: Sequence[Setup],
    baseline_audit: pd.DataFrame,
    cost_bps: float,
    oi_cap: float,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for setup in setups:
        bucket = all_signals.loc[
            all_signals["hhmm_int"].eq(int(setup.signal_end.replace(":", "")))
            & all_signals["side"].eq(setup.side)
        ].copy()
        strict_count = len(bucket)
        selected_count = int(baseline_audit["setup_id"].eq(setup.setup_id).sum())
        filled_count = int(
            (
                baseline_audit["setup_id"].eq(setup.setup_id)
                & baseline_audit["filled"].astype(bool)
            ).sum()
        )
        stages: list[tuple[str, Any]] = [
            ("NIFTY_GATE", bucket["nifty_first_bar_gate_pass"]),
            ("GLOBAL_OI_CAP", bucket["oi_change_pct"].le(oi_cap)),
            (
                "PRICE_THRESHOLD",
                bucket["price_change_pct"].ge(setup.price_change_pct)
                if setup.side == "LONG"
                else bucket["price_change_pct"].le(-setup.price_change_pct),
            ),
            ("OI_MIN_THRESHOLD", bucket["oi_change_pct"].ge(setup.oi_change_pct)),
            ("VOLUME_THRESHOLD", bucket["volume_ratio"].ge(setup.volume_ratio)),
            ("BODY_THRESHOLD", bucket["body_ratio"].ge(setup.body_ratio)),
            ("WICK_CAP", bucket["wick_ratio"].le(setup.max_wick_ratio)),
            (
                "MIN_TRADED_VALUE",
                bucket["traded_value"].ge(setup.min_traded_value),
            ),
        ]
        current = bucket
        for stage, mask_on_bucket in stages:
            previous = len(current)
            current = current.loc[mask_on_bucket.reindex(current.index).fillna(False)].copy()
            rows.append(
                {
                    "signal_end": setup.signal_end,
                    "confirmation_end": setup.confirmation_end,
                    "side": setup.side,
                    "setup_id": setup.setup_id,
                    "stage": stage,
                    "stage_rejections": previous - len(current),
                    "remaining": len(current),
                    "strict_confirmed_cache_scope": strict_count,
                    "selected_orders": selected_count,
                    "filled_orders": filled_count,
                }
            )
        previous = len(current)
        selected = select_setup_rows(current, setup)
        rows.append(
            {
                "signal_end": setup.signal_end,
                "confirmation_end": setup.confirmation_end,
                "side": setup.side,
                "setup_id": setup.setup_id,
                "stage": "PICKER_MAX_ENTRIES",
                "stage_rejections": previous - len(selected),
                "remaining": len(selected),
                "strict_confirmed_cache_scope": strict_count,
                "selected_orders": selected_count,
                "filled_orders": filled_count,
            }
        )
        selected_audit = simulate_selected(
            selected, paths, setup, cost_bps=cost_bps
        ) if not selected.empty else empty_audit()
        rows.append(
            {
                "signal_end": setup.signal_end,
                "confirmation_end": setup.confirmation_end,
                "side": setup.side,
                "setup_id": setup.setup_id,
                "stage": "STOP_ENTRY_FILL",
                "stage_rejections": len(selected) - int(selected_audit["filled"].sum()),
                "remaining": int(selected_audit["filled"].sum()),
                "strict_confirmed_cache_scope": strict_count,
                "selected_orders": selected_count,
                "filled_orders": filled_count,
            }
        )
    return pd.DataFrame(rows)


def build_baseline_per_time_side(
    baseline: pd.DataFrame, split_days: Mapping[str, Sequence[date]]
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if baseline.empty:
        return pd.DataFrame()
    for (hhmm, signal_end, side), audit in baseline.groupby(
        ["hhmm_int", "hhmm", "side"], sort=True
    ):
        rows.append(
            {
                "hhmm_int": int(hhmm),
                "signal_end": f"{str(signal_end).zfill(4)[:2]}:{str(signal_end).zfill(4)[2:]}",
                "side": side,
                "active_setup_count": int(audit["setup_id"].nunique()),
                **split_metrics(audit, split_days, detailed=True),
            }
        )
    return pd.DataFrame(rows)


def enrich_completed_experiments(
    timing_additions: pd.DataFrame,
    timing_removals: pd.DataFrame,
    entry: pd.DataFrame,
    max_entries: pd.DataFrame,
    baseline_metrics: Mapping[str, Any],
    baseline_audit: pd.DataFrame,
    split_days: Mapping[str, Sequence[date]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # The baseline row is a control, not a candidate experiment.
    entry_candidates = entry.loc[~entry["family"].eq("BASELINE")]
    all_completed = pd.concat(
        [timing_additions, timing_removals, entry_candidates, max_entries],
        ignore_index=True,
        sort=False,
    )
    dev_days = list(split_days["TRAIN"]) + list(split_days["VALIDATION"])
    baseline_dev = audit_metrics(baseline_audit, dev_days, detailed=False)
    all_completed["development_orders"] = (
        all_completed["train_orders"] + all_completed["validation_orders"]
    )
    all_completed["development_fills"] = (
        all_completed["train_fills"] + all_completed["validation_fills"]
    )
    all_completed["development_wins"] = (
        all_completed["train_wins"] + all_completed["validation_wins"]
    )
    all_completed["development_losses"] = (
        all_completed["train_losses"] + all_completed["validation_losses"]
    )
    all_completed["development_win_rate"] = (
        all_completed["development_wins"] / all_completed["development_fills"]
    )
    # PF and net pool TRAIN + VALIDATION. Recover gross wins/losses from PF/net.
    # Direct trade audits are not retained for every experiment, so use the
    # identity net = profit-loss and PF = profit/loss for the two splits.
    profits = np.zeros(len(all_completed), dtype=float)
    losses = np.zeros(len(all_completed), dtype=float)
    for split in ("train", "validation"):
        pf = pd.to_numeric(all_completed[f"{split}_profit_factor"], errors="coerce")
        net = pd.to_numeric(all_completed[f"{split}_net_pct"], errors="coerce")
        finite = np.isfinite(pf) & pf.ne(1.0)
        split_loss = pd.Series(0.0, index=all_completed.index)
        split_profit = pd.Series(0.0, index=all_completed.index)
        split_loss.loc[finite] = net.loc[finite] / (pf.loc[finite] - 1.0)
        split_profit.loc[finite] = pf.loc[finite] * split_loss.loc[finite]
        only_profit = np.isposinf(pf)
        split_profit.loc[only_profit] = net.loc[only_profit]
        only_loss = pf.eq(0.0)
        split_loss.loc[only_loss] = -net.loc[only_loss]
        profits += split_profit.to_numpy(float)
        losses += split_loss.to_numpy(float)
    all_completed["development_profit_factor"] = np.divide(
        profits,
        losses,
        out=np.full(len(all_completed), np.inf),
        where=losses > 0,
    )
    all_completed.loc[(losses == 0) & (profits == 0), "development_profit_factor"] = np.nan
    all_completed["development_net_pct"] = (
        all_completed["train_net_pct"] + all_completed["validation_net_pct"]
    )
    all_completed["development_split_admissible"] = (
        all_completed["train_net_pct"].gt(0)
        & all_completed["train_profit_factor"].gt(1)
        & all_completed["validation_net_pct"].gt(0)
        & all_completed["validation_profit_factor"].gt(1)
    )
    nonworse = np.ones(len(all_completed), dtype=bool)
    for field in HEADLINE_METRICS:
        nonworse &= all_completed[f"development_{field}"].ge(
            baseline_dev[field]
        ).to_numpy(bool)
    all_completed["development_simultaneous_nonworse_fills_pf_wr_net"] = nonworse

    admissible = all_completed.loc[all_completed["development_split_admissible"]].copy()
    frontier_indices = nondominated_indices(
        admissible,
        [
            "development_fills",
            "development_profit_factor",
            "development_win_rate",
            "development_net_pct",
        ],
    ) if not admissible.empty else []
    selected_ids = set(all_completed.loc[frontier_indices, "experiment_id"])
    selected_mask = all_completed["experiment_id"].isin(selected_ids)
    all_completed["development_selection_status"] = np.where(
        selected_mask,
        "SELECTED_DEVELOPMENT_PARETO",
        np.where(
            all_completed["development_split_admissible"],
            "DOMINATED_ON_DEVELOPMENT",
            "REJECTED_SPLIT_ADMISSIBILITY",
        ),
    )
    all_completed["development_rejection_reason"] = np.where(
        selected_mask,
        "",
        np.where(
            all_completed["development_split_admissible"],
            "DOMINATED_ON_DEVELOPMENT_FILLS_PF_WR_NET",
            "TRAIN_OR_VALIDATION_PF_NET_NOT_POSITIVE",
        ),
    )
    pareto = all_completed.loc[selected_mask].copy()
    pareto["selection_basis"] = "TRAIN_PLUS_VALIDATION_FILLS_PF_WIN_RATE_NET_PARETO"
    pareto["pseudo_test_used_for_selection"] = False
    pareto["pseudo_test_reveal_status"] = "REVEALED_AFTER_SELECTION"
    rejected = all_completed.loc[~selected_mask].copy()
    strict = all_completed.loc[
        all_completed["development_split_admissible"]
        & all_completed["development_simultaneous_nonworse_fills_pf_wr_net"]
    ].copy()
    return all_completed, pareto, rejected, strict


def reorder_like_reference(
    frame: pd.DataFrame, name: str, reference_dir: Path
) -> pd.DataFrame:
    path = reference_dir / name
    if not path.is_file():
        return frame
    expected = list(pd.read_csv(path, nrows=0).columns)
    missing = [column for column in expected if column not in frame.columns]
    for column in missing:
        frame[column] = np.nan
    extras = [column for column in frame.columns if column not in expected]
    return frame[expected + extras]


def verify_reference_outputs(
    generated: Mapping[str, pd.DataFrame], reference_dir: Path
) -> list[dict[str, Any]]:
    key_columns = {
        "available_timing_inventory.csv": ["signal_end", "side"],
        "timing_addition_experiments.csv": ["experiment_id"],
        "timing_removal_experiments.csv": ["experiment_id"],
        "max_entries_experiments.csv": ["experiment_id"],
        "entry_experiments.csv": ["experiment_id"],
    }
    reports: list[dict[str, Any]] = []
    failures: list[str] = []
    for name, keys in key_columns.items():
        reference_path = reference_dir / name
        if not reference_path.is_file():
            failures.append(f"missing reference {reference_path}")
            continue
        expected = pd.read_csv(reference_path)
        actual = generated[name].copy()
        expected = expected.sort_values(keys, kind="stable").reset_index(drop=True)
        actual = actual.sort_values(keys, kind="stable").reset_index(drop=True)
        key_match = len(expected) == len(actual) and expected[keys].astype(str).equals(
            actual[keys].astype(str)
        )
        numeric_columns = [
            column
            for column in expected.columns.intersection(actual.columns)
            if pd.api.types.is_numeric_dtype(expected[column])
            and (
                any(token in column for token in HEADLINE_METRICS)
                or column.endswith("_orders")
                or column.endswith("_sessions")
                or column.startswith("delta_all_")
            )
        ]
        max_diff = 0.0
        bad_columns: list[str] = []
        if len(expected) == len(actual):
            for column in numeric_columns:
                left = pd.to_numeric(expected[column], errors="coerce").to_numpy(float)
                right = pd.to_numeric(actual[column], errors="coerce").to_numpy(float)
                finite = np.isfinite(left) & np.isfinite(right)
                difference = float(np.max(np.abs(left[finite] - right[finite]))) if finite.any() else 0.0
                special_match = np.array_equal(np.isnan(left), np.isnan(right)) and np.array_equal(
                    np.isposinf(left), np.isposinf(right)
                ) and np.array_equal(np.isneginf(left), np.isneginf(right))
                if difference > 1e-10 or not special_match:
                    bad_columns.append(column)
                max_diff = max(max_diff, difference)
        else:
            bad_columns.append("ROW_COUNT")
        report = {
            "file": name,
            "expected_rows": len(expected),
            "actual_rows": len(actual),
            "keys_match": key_match,
            "numeric_columns_checked": len(numeric_columns),
            "max_abs_numeric_diff": max_diff,
            "bad_columns": "|".join(bad_columns),
        }
        reports.append(report)
        if not key_match or bad_columns:
            failures.append(f"{name}: {report}")
    if failures:
        raise RuntimeError("Reference verification failed:\n" + "\n".join(failures))
    return reports


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Regenerate the frozen-cache V13-v5 timing additions/removals, "
            "max-entry relaxations, and entry experiments without touching V3 caches."
        )
    )
    parser.add_argument(
        "--spec",
        type=Path,
        default=DEFAULT_SPEC,
        help="reproduction spec JSON (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="new output directory; the source artifact directory is refused (default: %(default)s)",
    )
    parser.add_argument(
        "--reference-dir",
        type=Path,
        default=HERE,
        help="existing artifact directory used only for column ordering/optional verification",
    )
    parser.add_argument(
        "--verify-reference",
        action="store_true",
        help="fail unless primary headline/count metrics match the existing CSVs within 1e-10",
    )
    parser.add_argument(
        "--allow-input-drift",
        action="store_true",
        help="permit source input hashes to differ from the frozen reproduction spec",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    spec_path = args.spec.resolve()
    output_dir = args.output_dir.resolve()
    reference_dir = args.reference_dir.resolve()
    if output_dir == HERE.resolve():
        raise ValueError(
            "Refusing to overwrite the original timing-entry artifact directory; "
            "choose a separate --output-dir."
        )
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    if spec.get("analysis_id") != "V13_V5_CONTROLLED_TIMING_ENTRY_CACHE_REPLAY_V1":
        raise RuntimeError(f"Unexpected analysis_id in {spec_path}")
    split_days = parse_split_days(spec)
    cost_bps = float(spec["fixed_runtime"]["cost_bps"])
    oi_cap = float(spec["fixed_runtime"]["global_oi_cap_pct"])
    setups = tuple(Setup(**payload) for payload in spec["baseline_setups"])

    signals, paths, cache_checks = load_frozen_inputs(spec, args.allow_input_drift)
    annotated, nifty_sources = annotate_nifty_gate(signals, cache_checks)
    gated = annotated.loc[annotated["nifty_first_bar_gate_pass"]].copy()
    policy_signals = gated.loc[gated["oi_change_pct"].le(oi_cap)].copy()

    baseline = replay_book(policy_signals, paths, setups, cost_bps=cost_bps)
    baseline_metrics = split_metrics(baseline, split_days, detailed=True)
    baseline_metrics_simple = split_metrics(baseline, split_days, detailed=False)
    parity = verify_baseline_parity(baseline, spec, args.allow_input_drift)

    inventory, timing_leg_audits, setups_by_cell = build_timing_inventory(
        annotated,
        gated,
        policy_signals,
        paths,
        setups,
        split_days,
        spec,
        cost_bps,
    )
    additions = build_timing_additions(
        inventory,
        timing_leg_audits,
        setups_by_cell,
        baseline,
        baseline_metrics,
        split_days,
    )
    removals = build_timing_removals(
        baseline, setups, baseline_metrics, split_days
    )
    max_entries = build_max_entries_experiments(
        policy_signals,
        paths,
        setups,
        baseline_metrics_simple,
        split_days,
        cost_bps,
    )
    entry, _entry_audits = build_entry_experiments(
        policy_signals,
        paths,
        setups,
        split_days,
        baseline_metrics,
        spec,
        cost_bps,
    )
    funnel = build_funnel(
        annotated, paths, setups, baseline, cost_bps, oi_cap
    )
    baseline_by_cell = build_baseline_per_time_side(baseline, split_days)
    all_completed, pareto, rejected, strict = enrich_completed_experiments(
        additions,
        removals,
        entry,
        max_entries,
        baseline_metrics,
        baseline,
        split_days,
    )

    marginal_selected = additions.loc[
        additions["marginal_development_selection_status"].eq(
            "SELECTED_DEVELOPMENT_PARETO"
        )
    ].copy()
    marginal_rejected = additions.loc[
        ~additions["marginal_development_selection_status"].eq(
            "SELECTED_DEVELOPMENT_PARETO"
        )
    ].copy()
    pseudo_columns = [
        "experiment_id",
        "family",
        "changed_signal_end",
        "changed_side",
        "development_fills",
        "development_profit_factor",
        "development_win_rate",
        "development_net_pct",
        *[f"pseudo_test_{field}" for field in SIMPLE_METRICS],
    ]
    pseudo_reveal = pareto[[column for column in pseudo_columns if column in pareto]].copy()
    marginal_pseudo_columns = [
        "experiment_id",
        "changed_signal_end",
        "changed_side",
        "leg_development_fills",
        "leg_development_profit_factor",
        "leg_development_win_rate",
        "leg_development_net_pct",
        *[f"leg_pseudo_test_{field}" for field in SIMPLE_METRICS],
    ]
    marginal_pseudo = marginal_selected[
        [column for column in marginal_pseudo_columns if column in marginal_selected]
    ].copy()

    outputs: dict[str, pd.DataFrame] = {
        "active_setup_funnel.csv": funnel,
        "available_timing_inventory.csv": inventory,
        "baseline_per_time_side_metrics.csv": baseline_by_cell,
        "timing_addition_experiments.csv": additions,
        "timing_removal_experiments.csv": removals,
        "max_entries_experiments.csv": max_entries,
        "entry_experiments.csv": entry,
        "all_completed_experiments.csv": all_completed,
        "development_pareto_selected.csv": pareto,
        "pareto_candidates.csv": pareto.copy(),
        "rejected_experiments.csv": rejected,
        "development_strict_improvements.csv": strict,
        "pseudo_test_reveal.csv": pseudo_reveal,
        "timing_marginal_development_selected.csv": marginal_selected,
        "timing_marginal_rejected.csv": marginal_rejected,
        "timing_marginal_pseudo_reveal.csv": marginal_pseudo,
    }
    outputs = {
        name: reorder_like_reference(frame, name, reference_dir)
        for name, frame in outputs.items()
    }

    verification: list[dict[str, Any]] = []
    if args.verify_reference:
        verification = verify_reference_outputs(outputs, reference_dir)

    assert_inputs_unchanged(cache_checks)
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, frame in outputs.items():
        write_csv(frame, output_dir / name)
    metadata = {
        "analysis_id": spec["analysis_id"],
        "generator": str(Path(__file__).resolve()),
        "spec": str(spec_path),
        "output_dir": str(output_dir),
        "cache_mode": "READ_ONLY_DIRECT_LOAD_NO_BUILDERS",
        "cache_checks": cache_checks,
        "nifty_sources": nifty_sources,
        "baseline_parity": parity,
        "cost_bps": cost_bps,
        "global_oi_cap_pct": oi_cap,
        "same_bar_semantics": "STOP_WINS_TIES; ENTRY_BAR_PARTICIPATES",
        "warning_no_untouched_test": (
            "No truly untouched test remains; PSEUDO_TEST is a chronological "
            "reveal label only."
        ),
        "rows": {name: len(frame) for name, frame in outputs.items()},
        "reference_verification": verification,
    }
    write_json(metadata, output_dir / "regeneration_metadata.json")
    manifest_rows = []
    for path in sorted(output_dir.iterdir(), key=lambda item: item.name):
        if path.is_file():
            manifest_rows.append(
                {
                    "name": path.name,
                    "bytes": path.stat().st_size,
                    "sha256": sha256(path),
                }
            )
    write_csv(pd.DataFrame(manifest_rows), output_dir / "artifact_manifest.csv")
    assert_inputs_unchanged(cache_checks)

    print(
        json.dumps(
            json_clean(
                {
                    "status": "SUCCESS",
                    "output_dir": str(output_dir),
                    "baseline_parity": parity,
                    "primary_rows": {
                        name: len(outputs[name])
                        for name in (
                            "available_timing_inventory.csv",
                            "timing_addition_experiments.csv",
                            "timing_removal_experiments.csv",
                            "max_entries_experiments.csv",
                            "entry_experiments.csv",
                        )
                    },
                    "reference_verified": bool(args.verify_reference),
                }
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
