"""Immutable PAPER profiles for the frozen V10/V11/V12 strategy family.

This module is deliberately data-only.  It does not import a backtest runner,
discover credentials, create files, or patch process-global functions.  Both
the incremental paper reducer and parity tests consume these exact objects.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from types import MappingProxyType
from typing import Any, Mapping


PROFILE_SCHEMA_VERSION = "fno_multi_paper_profile_v1"
MODE = "PAPER"
PAPER_ONLY = True
ENTRY_INHERIT = "INHERIT"


@dataclass(frozen=True)
class SetupDefinition:
    signal_end: str
    side: str
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
    entry_conf_minute: int | None = None
    entry_buffer_bps: float | None = None
    entry_midpoint: bool | None = None
    entry_clv: float | str | None = ENTRY_INHERIT

    @property
    def setup_id(self) -> str:
        return f"{self.signal_end}_{self.side}"


@dataclass(frozen=True)
class EntryPolicyDefinition:
    buffer_bps: float = 0.0
    max_confirmation_minute: int = 1
    entry_expiry_minute: int = 5
    close_location_min: float | None = None
    cost_bps: float = 15.0
    slippage_bps: float = 0.0
    midpoint_invalidation: bool = False
    post_confirmation_cancel: bool = True
    allow_cap_reassignment: bool = True
    same_bar_policy: str = "STOP_FIRST"
    square_off: str = "15:30"
    eod_policy: str = "EXACT_SQUARE_OFF"


@dataclass(frozen=True)
class PortfolioDefinition:
    capital_rs: float = 120_000.0
    margin_per_entry_rs: float = 10_000.0
    target_exposure_per_entry_rs: float = 50_000.0
    max_concurrent_positions: int = 12
    pending_reserves_margin: bool = True
    one_position_per_symbol: bool = True


@dataclass(frozen=True)
class SelectionConstraint:
    setup_id: str
    max_directional_move_pct: float | None = None


@dataclass(frozen=True)
class ExecutionDefinition:
    max_adverse_gap_bps: float | None
    entry_not_before: tuple[tuple[str, int], ...] = ()
    same_side_symbol_limit: int = 1
    prohibit_opposite_side: bool = True

    def entry_not_before_map(self) -> Mapping[str, int]:
        return MappingProxyType(dict(self.entry_not_before))


@dataclass(frozen=True)
class StrategyProfile:
    key: str
    profile_id: str
    display_name: str
    lineage: str
    setups: tuple[SetupDefinition, ...]
    entry_policies: tuple[tuple[str, EntryPolicyDefinition], ...]
    portfolio: PortfolioDefinition
    selection_constraints: tuple[SelectionConstraint, ...]
    execution: ExecutionDefinition

    @property
    def setup_by_id(self) -> Mapping[str, SetupDefinition]:
        return MappingProxyType({item.setup_id: item for item in self.setups})

    @property
    def entry_policy_by_id(self) -> Mapping[str, EntryPolicyDefinition]:
        return MappingProxyType(dict(self.entry_policies))

    @property
    def selection_constraint_by_id(self) -> Mapping[str, SelectionConstraint]:
        return MappingProxyType({item.setup_id: item for item in self.selection_constraints})

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": PROFILE_SCHEMA_VERSION,
            "mode": MODE,
            "paper_only": PAPER_ONLY,
            "key": self.key,
            "profile_id": self.profile_id,
            "display_name": self.display_name,
            "lineage": self.lineage,
            "setups": [asdict(item) for item in self.setups],
            "entry_policies": {
                setup_id: asdict(policy) for setup_id, policy in self.entry_policies
            },
            "portfolio": asdict(self.portfolio),
            "selection_constraints": [
                asdict(item) for item in self.selection_constraints
            ],
            "execution": asdict(self.execution),
            "data_contract": {
                "execution_instrument": "NSE_CASH_EQUITY",
                "oi_instrument": "NFO_NEAR_MONTH_STOCK_FUTURE",
                "completed_real_one_minute_bars_only": True,
                "same_confirmation_bar_fill": False,
            },
        }

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.payload())

    @property
    def setup_book_sha256(self) -> str:
        return canonical_sha256([asdict(item) for item in self.setups])

    def validate(self) -> None:
        if self.key not in {"v10", "v11", "v12"}:
            raise ValueError(f"unsupported profile key: {self.key!r}")
        if len(self.setups) != 10 or len(self.setup_by_id) != 10:
            raise ValueError(f"{self.profile_id} requires ten unique setup legs")
        if set(self.entry_policy_by_id) != set(self.setup_by_id):
            raise ValueError("every setup requires exactly one entry policy")
        if set(self.selection_constraint_by_id) - set(self.setup_by_id):
            raise ValueError("selection constraint targets an unknown setup")
        expected = {
            f"{slot}_{side}"
            for slot in ("09:25", "09:30", "09:35", "09:40", "09:45")
            for side in ("LONG", "SHORT")
        }
        if set(self.setup_by_id) != expected:
            raise ValueError("setup clocks/sides differ from the frozen ten-leg book")
        if self.execution.max_adverse_gap_bps is not None and (
            not math.isfinite(float(self.execution.max_adverse_gap_bps))
            or float(self.execution.max_adverse_gap_bps) < 0
        ):
            raise ValueError("gap guard must be finite and non-negative")
        if self.execution.same_side_symbol_limit not in {1, 2}:
            raise ValueError("same-side symbol limit must be one or two")
        for setup_id, minute in self.execution.entry_not_before:
            if setup_id not in self.setup_by_id or not 2 <= int(minute) <= 5:
                raise ValueError("invalid delayed-entry rule")


def canonical_sha256(value: Any) -> str:
    raw = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


S = SetupDefinition
_V10_SETUPS: tuple[SetupDefinition, ...] = (
    S("09:25", "LONG", 4, "max_move", 0.30, 0.10, 3.0, 0.00, 0.50, 0.0, 0.40, 1.0, 3, 0.0, False, None),
    S("09:25", "SHORT", 4, "max_move", 0.20, 0.10, 1.5, 0.60, 0.60, 25_000_000.0, 0.50, 3.0, 3, 2.0, False, None),
    S("09:30", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.50, 0.50, 0.0, 1.00, 2.5),
    S("09:30", "SHORT", 4, "max_volume", 0.20, 1.00, 1.0, 0.45, 0.30, 25_000_000.0, 1.00, 4.0, 3, 0.0, True, 0.50),
    S("09:35", "LONG", 1, "max_liquidity", 0.20, 0.10, 1.0, 0.60, 0.50, 0.0, 1.00, 2.5),
    S("09:35", "SHORT", 2, "max_liquidity", 0.50, 1.00, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    # Frozen Stage 7: this is 0.40, not the V8 base value 0.20.
    S("09:40", "LONG", 1, "max_liquidity", 0.40, 0.10, 2.0, 0.50, 0.50, 0.0, 0.50, 2.5),
    S("09:40", "SHORT", 1, "max_move", 0.20, 0.10, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    S("09:45", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    S("09:45", "SHORT", 1, "max_volume", 0.20, 0.75, 1.0, 0.40, 0.30, 0.0, 1.00, 2.0),
)


def _resolved_entry_policies(
    setups: tuple[SetupDefinition, ...],
) -> tuple[tuple[str, EntryPolicyDefinition], ...]:
    values: list[tuple[str, EntryPolicyDefinition]] = []
    for setup in setups:
        values.append(
            (
                setup.setup_id,
                EntryPolicyDefinition(
                    buffer_bps=float(setup.entry_buffer_bps or 0.0),
                    max_confirmation_minute=int(setup.entry_conf_minute or 1),
                    close_location_min=(
                        None
                        if setup.entry_clv in {None, ENTRY_INHERIT}
                        else float(setup.entry_clv)
                    ),
                    midpoint_invalidation=bool(setup.entry_midpoint or False),
                ),
            )
        )
    return tuple(values)


_PORTFOLIO = PortfolioDefinition()
_MAX050 = (SelectionConstraint("09:35_LONG", max_directional_move_pct=0.50),)
_V10_ENTRY_POLICIES = _resolved_entry_policies(_V10_SETUPS)

V10_PROFILE = StrategyProfile(
    key="v10",
    profile_id="V10_STAGE7_0935_LONG_MAX_050_GAP2",
    display_name="V10 .50 + Gap2",
    lineage="FROZEN_V10_STAGE7_PLUS_MAX050_PLUS_GAP2",
    setups=_V10_SETUPS,
    entry_policies=_V10_ENTRY_POLICIES,
    portfolio=_PORTFOLIO,
    selection_constraints=_MAX050,
    execution=ExecutionDefinition(max_adverse_gap_bps=2.0),
)

V11_PROFILE = StrategyProfile(
    key="v11",
    profile_id="V11_S10_POST_HOC_TOP2_1436C7D363",
    display_name="V11 Stage 10",
    lineage="V10_MAX050_GAP2_PLUS_0930_SHORT_S3_PLUS_SAME_SIDE_2",
    setups=_V10_SETUPS,
    entry_policies=_V10_ENTRY_POLICIES,
    portfolio=_PORTFOLIO,
    selection_constraints=_MAX050,
    execution=ExecutionDefinition(
        max_adverse_gap_bps=2.0,
        entry_not_before=(("09:30_SHORT", 3),),
        same_side_symbol_limit=2,
    ),
)

_V12_SETUPS = tuple(
    SetupDefinition(
        **{
            **asdict(setup),
            "volume_ratio": (
                1.50
                if setup.setup_id in {"09:40_SHORT", "09:45_SHORT"}
                else setup.volume_ratio
            ),
        }
    )
    for setup in _V10_SETUPS
)

V12_PROFILE = StrategyProfile(
    key="v12",
    profile_id="V12_S06_LATE_SHORT_VOLUME_MIN_150",
    display_name="V12 Selected",
    lineage="V11_STAGE10_PLUS_LATE_SHORT_VOLUME_MIN_150",
    setups=_V12_SETUPS,
    entry_policies=_resolved_entry_policies(_V12_SETUPS),
    portfolio=_PORTFOLIO,
    selection_constraints=_MAX050,
    execution=V11_PROFILE.execution,
)

PROFILES: tuple[StrategyProfile, ...] = (V10_PROFILE, V11_PROFILE, V12_PROFILE)
PROFILE_BY_KEY: Mapping[str, StrategyProfile] = MappingProxyType(
    {profile.key: profile for profile in PROFILES}
)
PROFILE_BY_FINGERPRINT: Mapping[str, StrategyProfile] = MappingProxyType(
    {profile.fingerprint: profile for profile in PROFILES}
)


def profile_for(value: str) -> StrategyProfile:
    key = str(value).strip().lower()
    if key in PROFILE_BY_KEY:
        return PROFILE_BY_KEY[key]
    matches = [profile for profile in PROFILES if profile.profile_id.lower() == key]
    if len(matches) == 1:
        return matches[0]
    raise KeyError(f"unknown PAPER profile: {value!r}")


for _profile in PROFILES:
    _profile.validate()


__all__ = [
    "EntryPolicyDefinition",
    "ExecutionDefinition",
    "MODE",
    "PAPER_ONLY",
    "PROFILE_BY_FINGERPRINT",
    "PROFILE_BY_KEY",
    "PROFILE_SCHEMA_VERSION",
    "PROFILES",
    "PortfolioDefinition",
    "SelectionConstraint",
    "SetupDefinition",
    "StrategyProfile",
    "V10_PROFILE",
    "V11_PROFILE",
    "V12_PROFILE",
    "canonical_sha256",
    "profile_for",
]
