"""Fixed retrospective follow-up gates; no selection, fills or profile writes.

Every input is known at the exact next-minute confirmation. Capacity uses the
planned order notional, never subsequent fills, realized exposure or P&L. The
six configurations and thresholds were declared after inspecting the original
study; their historical results are diagnostics, not an untouched holdout.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd

from .selection import UNLIMITED_SELECTION

VERSION = "V14_FIXED_IMPROVEMENT_DIAGNOSTIC_V1"
PLANNED_NOTIONAL_RUPEES = 500_000.0
SIGNAL_CAPACITY_FRACTION = .05
CONFIRMATION_CAPACITY_FRACTION = .10
MAX_ABS_MOVE_PCT = 1.25
MAX_SIGNAL_RANGE_PCT = 2.5
MAX_EMA9_EXTENSION_PCT = 2.5
MIN_INTERVAL_RVOL = 1.5
MIN_SIDE_CMF = 0.0


@dataclass(frozen=True)
class ImprovementVariant:
    name: str
    capacity: bool = False
    anti_chase: bool = False
    interval_rvol: bool = False
    side_cmf: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def variants() -> list[ImprovementVariant]:
    """Declared order is also the final simplicity tie-break order."""
    return [
        ImprovementVariant("no_oi_control"),
        ImprovementVariant("capacity_guard", capacity=True),
        ImprovementVariant("anti_chase_guard", anti_chase=True),
        ImprovementVariant("capacity_anti_chase", capacity=True, anti_chase=True),
        ImprovementVariant("capacity_anti_chase_rvol1p5", capacity=True, anti_chase=True, interval_rvol=True),
        ImprovementVariant("capacity_anti_chase_rvol1p5_cmf0", capacity=True, anti_chase=True,
                           interval_rvol=True, side_cmf=True),
    ]


def _number(rows: pd.DataFrame, column: str) -> pd.Series:
    if column not in rows:
        return pd.Series(np.nan, index=rows.index, dtype=float)
    return pd.to_numeric(rows[column], errors="coerce").astype(float)


def _positive_finite(*values: pd.Series) -> pd.Series:
    passed = pd.Series(True, index=values[0].index)
    for value in values:
        passed &= np.isfinite(value) & value.gt(0)
    return passed


def diagnostic_features(rows: pd.DataFrame, *,
                        planned_notional_rupees: float = PLANNED_NOTIONAL_RUPEES) -> pd.DataFrame:
    """Return causal measurements and inclusive fixed gates without mutation.

    Absent/invalid required row values produce NaN measurements or False gates.
    The no-OI control does not require any of these optional gates to pass.
    Signal price and volume validation remains the baseline's responsibility;
    these guards also reject impossible range geometry when they consume it.
    """
    notional = float(planned_notional_rupees)
    if not np.isfinite(notional) or notional <= 0:
        raise ValueError("Planned order notional must be finite and positive")
    turnover = _number(rows, "traded_value")
    confirmation_close = _number(rows, "confirmation_close")
    confirmation_volume = _number(rows, "confirmation_volume")
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        confirmation_turnover = confirmation_close * confirmation_volume
    capacity_valid = _positive_finite(turnover, confirmation_close, confirmation_volume, confirmation_turnover)
    out = pd.DataFrame(index=rows.index)
    out["signal_capacity_fraction"] = (notional / turnover.where(turnover.gt(0))).where(capacity_valid)
    out["confirmation_capacity_fraction"] = (notional / confirmation_turnover.where(confirmation_turnover.gt(0))).where(capacity_valid)
    out["capacity_inputs_valid"] = capacity_valid
    out["capacity_pass"] = (capacity_valid
                            & out.signal_capacity_fraction.le(SIGNAL_CAPACITY_FRACTION)
                            & out.confirmation_capacity_fraction.le(CONFIRMATION_CAPACITY_FRACTION))

    close, high, low, ema9 = (_number(rows, name) for name in ("close", "high", "low", "ema9"))
    move = _number(rows, "price_change_pct")
    price_valid = (_positive_finite(close, high, low, ema9) & np.isfinite(move)
                   & high.ge(close) & low.le(close) & high.ge(low))
    out["abs_move_pct"] = move.abs().where(price_valid)
    # Difference arithmetic preserves inclusive symmetric boundary comparisons.
    out["signal_range_pct"] = (100 * (high - low) / close.where(close.gt(0))).where(price_valid)
    out["ema9_extension_pct"] = (100 * (close - ema9).abs() / ema9.where(ema9.gt(0))).where(price_valid)
    out["anti_chase_inputs_valid"] = price_valid
    out["anti_chase_pass"] = (price_valid & out.abs_move_pct.le(MAX_ABS_MOVE_PCT)
                              & out.signal_range_pct.le(MAX_SIGNAL_RANGE_PCT)
                              & out.ema9_extension_pct.le(MAX_EMA9_EXTENSION_PCT))
    rvol = _number(rows, "interval_rvol")
    side = rows["side"] if "side" in rows else pd.Series(None, index=rows.index, dtype=object)
    cmf = _number(rows, "cmf10")
    side_sign = side.map({"LONG": 1., "SHORT": -1.})
    out["interval_rvol_pass"] = np.isfinite(rvol) & rvol.ge(MIN_INTERVAL_RVOL)
    out["side_cmf"] = (cmf * side_sign).where(np.isfinite(cmf) & cmf.between(-1., 1.) & side_sign.notna())
    out["side_cmf_pass"] = out.side_cmf.ge(MIN_SIDE_CMF) & np.isfinite(out.side_cmf)
    return out


def participation_mask(rows: pd.DataFrame, variant: ImprovementVariant, *,
                       planned_notional_rupees: float = PLANNED_NOTIONAL_RUPEES) -> pd.Series:
    """Eligibility callback for unchanged native ranks and execution rules."""
    measurements = diagnostic_features(rows, planned_notional_rupees=planned_notional_rupees)
    passed = pd.Series(True, index=rows.index, dtype=bool)
    for enabled, column in ((variant.capacity, "capacity_pass"),
                            (variant.anti_chase, "anti_chase_pass"),
                            (variant.interval_rvol, "interval_rvol_pass"),
                            (variant.side_cmf, "side_cmf_pass")):
        if enabled:
            passed &= measurements[column]
    return passed.fillna(False).astype(bool)


def specification() -> dict[str, Any]:
    return dict(version=VERSION, variants=[variant.to_dict() for variant in variants()],
        planned_notional_rupees=PLANNED_NOTIONAL_RUPEES,
        thresholds=dict(signal_capacity_fraction_max=SIGNAL_CAPACITY_FRACTION,
            confirmation_capacity_fraction_max=CONFIRMATION_CAPACITY_FRACTION,
            abs_price_change_pct_max=MAX_ABS_MOVE_PCT, signal_range_pct_max=MAX_SIGNAL_RANGE_PCT,
            ema9_extension_pct_max=MAX_EMA9_EXTENSION_PCT,
            interval_rvol_min=MIN_INTERVAL_RVOL, side_cmf_min=MIN_SIDE_CMF),
        threshold_boundaries="Inclusive; no fitted thresholds or threshold sweep",
        availability="Completed signal five-minute bar and exact next-minute confirmation only",
        capacity_basis="Planned inherited capital times leverage; observed turnover proxy, not measured fill capacity",
        anti_chase_basis="Existing 1.25% initial stop: one stop for absolute move, two stops for range and EMA9 extension",
        missing_values="Required missing, nonfinite or impossible source values fail the enabled gate closed",
        slot_selection_policy=UNLIMITED_SELECTION,
        ranking="Filter before unchanged native setup ranking; select all eligible stocks per slot, retaining execution and portfolio constraints",
        evaluation="Historical follow-up is retrospective; original TRAIN/VALIDATION selection only; TEST never selects",
        future_evidence="Freeze a candidate before future unseen sessions; no profitability claim from retrospective pruning",
        execution_authority=False)
