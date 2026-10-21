"""Small, predeclared exit catalogue for replacement-aware G4 research.

This module never reads a latest pointer or writes a strategy configuration.
It accepts the entire selected-order ledger (including newly selected candidate
orders and unfilled orders), and full one-minute paths keyed by unique SID.
Call ``replay_raw`` before applying the portfolio allocator to the entire book;
old allocation/P&L columns in raw input are not valid after an exit changes.

The execution engine uses candle-end timestamps, conservative stop-first ties,
observed-open stop gaps, and close-armed protection effective next bar. Paths
are not truncated at the old exit, so larger targets can continue to 15:15.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
import math

import numpy as np
import pandas as pd

from research.g3_sl_target_simulator import simulate_exit


VERSION = "G4-SETUP-EXIT-ENGINE-V2"
MODEL_COST_PCT = .05


@dataclass(frozen=True)
class ExitVariant:
    name: str
    target_pct: float
    stop_pct: float = 1.2
    tighten_after_minutes: float | None = 120
    tightened_stop_pct: float | None = 1.0
    protect_after_close_pct: float | None = None
    protected_profit_pct: float = 0.0

    def configuration(self) -> dict:
        return asdict(self)


def _positive(value: float, name: str) -> float:
    if isinstance(value, (bool, np.bool_)):
        raise ValueError(f"{name} must be a finite positive number")
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{name} must be a finite positive number")
    return number


def variants_for_setup(
    target_pct: float,
    stop_pct: float = 1.2,
    tighten_after_minutes: float | None = 120,
    tightened_stop_pct: float | None = 1.0,
) -> tuple[ExitVariant, ...]:
    """Return baseline and at most ten alternatives, without looking at returns.

    Only target alternatives are clipped to G4's .60--2.00% bounds. Baseline
    settings must already be valid within those bounds. Duplicate settings
    caused by clipping are removed, always keeping baseline first.
    """
    target = _positive(target_pct, "target_pct")
    stop = _positive(stop_pct, "stop_pct")
    if not .6 <= target <= 2. or stop > 1.2:
        raise ValueError("Baseline must respect current G4 target/stop bounds")
    if (tighten_after_minutes is None) != (tightened_stop_pct is None):
        raise ValueError("Scheduled tightening needs both delay and stop")
    if tighten_after_minutes is not None:
        _positive(tighten_after_minutes, "tighten_after_minutes")
        if _positive(tightened_stop_pct, "tightened_stop_pct") > stop:
            raise ValueError("Scheduled tightening cannot loosen the initial stop")
    base = dict(target_pct=target, stop_pct=stop,
                tighten_after_minutes=tighten_after_minutes,
                tightened_stop_pct=tightened_stop_pct)
    proposed = [ExitVariant("baseline", **base)]
    for multiplier in (.8, 1.2):
        proposed.append(ExitVariant(f"target_x{int(multiplier*100):03d}",
            **dict(base, target_pct=float(np.clip(target*multiplier, .6, 2.)))))
    for initial in (.8, 1.0):
        # The fixed grid never introduces a wider stop for an already tighter
        # setup, and its timed stop cannot loosen the new initial stop.
        if initial <= stop:
            proposed.append(ExitVariant(f"stop_{int(initial*100):03d}",
                **dict(base, stop_pct=initial, tightened_stop_pct=(
                    min(initial, tightened_stop_pct) if tightened_stop_pct is not None else None))))
    if tightened_stop_pct is not None:
        for delay in (60, 90):
            proposed.append(ExitVariant(f"tighten_{delay:03d}",
                **dict(base, tighten_after_minutes=delay)))
    for activation in (.4, .6):
        for floor in (.1, .2):
            proposed.append(ExitVariant(
                f"protect_{int(activation*100):03d}_floor_{int(floor*100):03d}",
                **base, protect_after_close_pct=activation, protected_profit_pct=floor))
    unique = {}
    for variant in proposed:
        key = tuple(value for key, value in asdict(variant).items() if key != "name")
        unique.setdefault(key, variant)
    return tuple(unique.values())


def _filled(value) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return value.lower() == "true"
    raise ValueError("filled must contain explicit boolean values")


def _stamp(value, name) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if pd.isna(result) or result.tzinfo is None:
        raise ValueError(f"{name} must be a timezone-aware timestamp")
    return result


def _check_clocks(row: Mapping, path: Mapping) -> None:
    """Reject mismatched entry paths and features dated after their decision."""
    raw_index = row["entry_path_index"]
    index = int(raw_index)
    if isinstance(raw_index, (bool, np.bool_)) or index != raw_index:
        raise ValueError("entry_path_index must be integral")
    stamps = np.asarray(path["timestamp_ns"])
    if not 0 <= index < len(stamps):
        raise ValueError("entry_path_index is outside the path")
    entry = _stamp(row["entry_ts"], "entry_ts")
    if entry.value != int(stamps[index]):
        raise ValueError(f"Entry clock differs from path for SID {row['sid']}")
    confirmation = row.get("confirmation_ts")
    if confirmation is not None and not pd.isna(confirmation):
        confirmation = _stamp(confirmation, "confirmation_ts")
        if confirmation > entry:
            raise ValueError("Confirmation clock is after entry")
        signal = row.get("signal_ts")
        if signal is not None and not pd.isna(signal) and _stamp(signal, "signal_ts") > confirmation:
            raise ValueError("Signal clock is after confirmation")
        feature = row.get("v9_1m_feature_ts")
        if feature is not None and not pd.isna(feature) and _stamp(feature, "v9_1m_feature_ts") > confirmation:
            raise ValueError("One-minute feature clock is after confirmation")


def _check_baseline(row: Mapping, outcome: Mapping, variant: ExitVariant) -> None:
    for key in ("exit_price", "gross_return_pct"):
        if key not in row or not np.isclose(float(row[key]), outcome[key], atol=1e-8, rtol=0):
            raise ValueError(f"Baseline {key} differs for SID {row['sid']}")
    if (_stamp(row["exit_ts"], "exit_ts") != outcome["exit_ts"]
            or row["exit_reason"] != outcome["exit_reason"]):
        raise ValueError(f"Baseline exit differs for SID {row['sid']}")
    for field, expected in (("native_target_pct", variant.target_pct),
                            ("native_stop_pct", variant.stop_pct)):
        if field in row and not pd.isna(row[field]) and not np.isclose(float(row[field]), expected, atol=1e-8, rtol=0):
            raise ValueError(f"Baseline {field} differs for SID {row['sid']}")
    if "net_return_pct" in row and not np.isclose(
            float(row["net_return_pct"]), outcome["gross_return_pct"] - MODEL_COST_PCT,
            atol=1e-8, rtol=0):
        raise ValueError(f"Baseline modeled cost differs for SID {row['sid']}")


def replay_raw(
    native: pd.DataFrame,
    paths: Mapping[int, Mapping],
    setup_id: str,
    variant: ExitVariant,
    *,
    verify_baseline: bool = True,
) -> pd.DataFrame:
    """Replay every filled order of one setup, preserving entry/selection data.

    A baseline is independently replayed and checked, then returned as an exact
    DataFrame copy. Nonbaseline output includes full original rows and changed
    raw outcomes; call ``allocate`` (or the same G4 allocator) before using
    portfolio metrics. Unfilled orders need no path and retain their outcomes.
    The union can contain orders absent from the original selection, provided
    their entries and complete paths have first been simulated by the native
    entry engine. No old exit timestamp limits the alternative replay.
    """
    required = {"sid", "setup_id", "filled", "side"}
    if not required.issubset(native.columns):
        raise ValueError(f"Missing ledger fields: {sorted(required-set(native.columns))}")
    if not native.index.is_unique or native.sid.duplicated().any():
        raise ValueError("Ledger index and SID must be unique")
    if not isinstance(variant, ExitVariant):
        raise TypeError("variant must be ExitVariant")
    raw = native.copy(deep=True)
    outcome_rows = []
    for index, row in native.loc[native.setup_id.eq(setup_id)].iterrows():
        if row.side not in ("LONG", "SHORT"):
            raise ValueError("Unknown trade side")
        if not _filled(row.filled):
            continue
        sid = int(row.sid)
        if sid not in paths:
            raise ValueError(f"Missing full minute path for filled SID {sid}")
        _check_clocks(row, paths[sid])
        result = simulate_exit(paths[sid], int(row.entry_path_index),
            float(row.entry_price), row.side == "LONG", variant.stop_pct,
            variant.target_pct, variant.protect_after_close_pct,
            variant.protected_profit_pct,
            tighten_after_minutes=variant.tighten_after_minutes,
            tightened_stop_pct=variant.tightened_stop_pct)
        if variant.name == "baseline":
            if verify_baseline:
                _check_baseline(row, result, variant)
            continue
        result.update(net_return_pct=result["gross_return_pct"] - MODEL_COST_PCT,
            cost_pct=MODEL_COST_PCT, target_pct=variant.target_pct,
            native_target_pct=variant.target_pct, stop_pct=variant.stop_pct,
            native_stop_pct=variant.stop_pct, initial_stop_pct=variant.stop_pct,
            tightened_stop_pct=(variant.tightened_stop_pct if variant.tightened_stop_pct is not None else np.nan),
            tighten_after_minutes=(variant.tighten_after_minutes if variant.tighten_after_minutes is not None else np.nan))
        outcome_rows.append((index, result))
    for index, result in outcome_rows:
        for key, value in result.items():
            if key not in raw.columns:
                raw[key] = pd.Series(index=raw.index, dtype=object)
            # Timestamp strings read from CSV and typed native timestamps both
            # remain acceptable inputs for the native portfolio allocator.
            if value is not None and not pd.isna(value):
                is_native_timestamp = (isinstance(value, pd.Timestamp)
                    and isinstance(raw[key].dtype, pd.DatetimeTZDtype))
                if (isinstance(value, (str, pd.Timestamp)) and not is_native_timestamp
                        and not pd.api.types.is_object_dtype(raw[key].dtype)):
                    raw[key] = raw[key].astype(object)
            raw.at[index, key] = value
    if variant.name != "baseline":
        raw.attrs["requires_portfolio_reallocation"] = True
        raw.attrs["research_exit_variant"] = variant.configuration()
    return raw


def allocate(raw: pd.DataFrame, policy: str) -> pd.DataFrame:
    """Reapply G4's unchanged sizing, cost, and shared-capital constraints."""
    from research.g4_trade_monitor_backtest import allocate as g4_allocate
    result = g4_allocate(raw, policy)
    result.attrs.pop("requires_portfolio_reallocation", None)
    result.attrs["research_exit_engine"] = VERSION
    return result
