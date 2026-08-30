"""Frozen configuration for the independent FNO V8-Combined PAPER session.

This module deliberately does not import a backtest launcher, an optimizer, or
any V6/V7 live module.  The ten setup legs are repeated literally and protected
by the same canonical setup-book hash as the researched V8-Combined book.

The module is configuration only: importing it creates no files, discovers no
credentials, and can never enable a scheduled task.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, replace
from datetime import time
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from eqidv2_runtime_paths import runtime_dir


IST = ZoneInfo("Asia/Kolkata")

CONFIG_SCHEMA_VERSION = "fno_v8_combined_paper_config_v1"
STRATEGY_FAMILY = "FNO_V8_COMBINED_BEST_PER_LEG_20260820"
STRATEGY_VERSION = "FNO_V8_COMBINED_PAPER_20260821_V1"
MODE = "PAPER"
PAPER_ONLY = True

# This is the canonical hash of the ten literal V8-Combined setup dataclasses.
COMBINED_SETUP_BOOK_SHA256 = (
    "ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675"
)
SETUP_BOOK_SHA256 = COMBINED_SETUP_BOOK_SHA256
ENTRY_INHERIT = "INHERIT"

# Forward PAPER economics.  These are intentionally more conservative than the
# old V6 live defaults and match the native 15 bps V8 comparison contract.
COST_BPS = 15.0
SLIPPAGE_BPS = 0.0
TARGET_EXPOSURE_PER_ENTRY_RS = 50_000.0
PORTFOLIO_CAPITAL_RS = 120_000.0
MARGIN_PER_ENTRY_RS = 10_000.0
MAX_CONCURRENT_POSITIONS = 12
REQUIRED_KITE_APPS = 8

SQUARE_OFF = "15:30"
CONTROL_EXPIRY = "15:35"
ENTRY_EXPIRY_MINUTE = 5

EXECUTION_INSTRUMENT = "NSE_CASH_EQUITY"
OI_INSTRUMENT = "NFO_NEAR_MONTH_STOCK_FUTURE"
BAR_SOURCE_POLICY = "EXACT_COMPLETED_REAL_ONE_MINUTE_OHLCV_ONLY"
PORTFOLIO_ALLOCATION_POLICY = (
    "GLOBAL_EVENT_TIME_THEN_SIGNAL_TIME_SETUP_RANK_SYMBOL"
)

# All authoritative runtime state is off OneDrive and disjoint from V6 live and
# V8 research/cache namespaces.
ROOT = runtime_dir("fno_oi", "v8_combined_paper_v1")
CONTROL_ROOT = ROOT / "control"
PERMIT_ARCHIVE_ROOT = CONTROL_ROOT / "permits"
ACTIVATION_PATH = CONTROL_ROOT / "activation.json"
KILL_SWITCH_PATH = CONTROL_ROOT / "kill_switch.json"
SESSION_ROOT = ROOT / "sessions"
EVIDENCE_ROOT = ROOT / "evidence"
CHECKPOINT_ROOT = ROOT / "checkpoints"
LOCK_PATH = ROOT / "fno_v8_combined_paper.lock"
LATEST_REPORT_PATH = runtime_dir(
    "fno_oi", "latest", "latest_fno_v8_combined_paper.md"
)


@dataclass(frozen=True)
class PaperSetup:
    """One frozen five-minute setup and its optional one-minute overrides."""

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

    @property
    def overrides_entry_policy(self) -> bool:
        return bool(
            self.entry_conf_minute is not None
            or self.entry_buffer_bps is not None
            or self.entry_midpoint is not None
            or self.entry_clv != ENTRY_INHERIT
        )


S = PaperSetup
ACTIVE_SETUPS: tuple[PaperSetup, ...] = (
    S(
        "09:25", "LONG", 4, "max_move", 0.30, 0.10, 3.0, 0.00, 0.50,
        0.0, 0.40, 1.0, entry_conf_minute=3, entry_buffer_bps=0.0,
        entry_midpoint=False, entry_clv=None,
    ),
    S(
        "09:25", "SHORT", 4, "max_move", 0.20, 0.10, 1.5, 0.60, 0.60,
        25_000_000.0, 0.50, 3.0, entry_conf_minute=3,
        entry_buffer_bps=2.0, entry_midpoint=False, entry_clv=None,
    ),
    S(
        "09:30", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.50, 0.50,
        0.0, 1.00, 2.5,
    ),
    S(
        "09:30", "SHORT", 4, "max_volume", 0.20, 1.00, 1.0, 0.45, 0.30,
        25_000_000.0, 1.00, 4.0, entry_conf_minute=3,
        entry_buffer_bps=0.0, entry_midpoint=True, entry_clv=0.50,
    ),
    S(
        "09:35", "LONG", 1, "max_liquidity", 0.20, 0.10, 1.0, 0.60,
        0.50, 0.0, 1.00, 2.5,
    ),
    S(
        "09:35", "SHORT", 2, "max_liquidity", 0.50, 1.00, 1.0, 0.40,
        0.50, 0.0, 1.00, 3.0,
    ),
    S(
        "09:40", "LONG", 1, "max_liquidity", 0.20, 0.10, 2.0, 0.50,
        0.50, 0.0, 0.50, 2.5,
    ),
    S(
        "09:40", "SHORT", 1, "max_move", 0.20, 0.10, 1.0, 0.40, 0.50,
        0.0, 1.00, 3.0,
    ),
    S(
        "09:45", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.40, 0.50,
        0.0, 1.00, 3.0,
    ),
    S(
        "09:45", "SHORT", 1, "max_volume", 0.20, 0.75, 1.0, 0.40, 0.30,
        0.0, 1.00, 2.0,
    ),
)
# A clearer public alias for callers that do not share the backtest naming.
SETUPS = ACTIVE_SETUPS


@dataclass(frozen=True)
class EntryPolicy:
    """Resolved PAPER execution policy for one setup leg."""

    buffer_bps: float = 0.0
    max_confirmation_minute: int = 1
    entry_expiry_minute: int = ENTRY_EXPIRY_MINUTE
    close_location_min: float | None = None
    cost_bps: float = COST_BPS
    slippage_bps: float = SLIPPAGE_BPS
    midpoint_invalidation: bool = False
    post_confirmation_cancel: bool = True
    allow_cap_reassignment: bool = True
    same_bar_policy: str = "STOP_FIRST"
    square_off: str = SQUARE_OFF
    eod_policy: str = "EXACT_SQUARE_OFF"

    def validate(self) -> None:
        if isinstance(self.max_confirmation_minute, bool) or not isinstance(
            self.max_confirmation_minute, int
        ):
            raise ValueError("max_confirmation_minute must be an integer")
        if isinstance(self.entry_expiry_minute, bool) or not isinstance(
            self.entry_expiry_minute, int
        ):
            raise ValueError("entry_expiry_minute must be an integer")
        if self.max_confirmation_minute < 1:
            raise ValueError("max_confirmation_minute must be positive")
        if self.entry_expiry_minute <= self.max_confirmation_minute:
            raise ValueError("entry expiry must follow the confirmation window")
        for name, value in (
            ("buffer_bps", self.buffer_bps),
            ("cost_bps", self.cost_bps),
            ("slippage_bps", self.slippage_bps),
        ):
            if not math.isfinite(float(value)) or float(value) < 0:
                raise ValueError(f"{name} must be finite and non-negative")
        if self.close_location_min is not None and not (
            math.isfinite(float(self.close_location_min))
            and 0.0 <= float(self.close_location_min) <= 1.0
        ):
            raise ValueError("close_location_min must be in [0, 1]")
        if self.same_bar_policy != "STOP_FIRST":
            raise ValueError("only STOP_FIRST is supported")
        if self.eod_policy != "EXACT_SQUARE_OFF":
            raise ValueError("forward PAPER requires an exact square-off bar")


BASE_ENTRY_POLICY = EntryPolicy()


@dataclass(frozen=True)
class PortfolioPolicy:
    capital_rs: float = PORTFOLIO_CAPITAL_RS
    margin_per_entry_rs: float = MARGIN_PER_ENTRY_RS
    target_exposure_per_entry_rs: float = TARGET_EXPOSURE_PER_ENTRY_RS
    max_concurrent_positions: int = MAX_CONCURRENT_POSITIONS
    pending_reserves_margin: bool = True
    one_position_per_symbol: bool = True

    def validate(self) -> None:
        for name, value in (
            ("capital_rs", self.capital_rs),
            ("margin_per_entry_rs", self.margin_per_entry_rs),
            ("target_exposure_per_entry_rs", self.target_exposure_per_entry_rs),
        ):
            if not math.isfinite(float(value)) or float(value) <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if isinstance(self.max_concurrent_positions, bool) or not isinstance(
            self.max_concurrent_positions, int
        ):
            raise ValueError("max_concurrent_positions must be an integer")
        if self.max_concurrent_positions <= 0:
            raise ValueError("max_concurrent_positions must be positive")
        if not self.pending_reserves_margin or not self.one_position_per_symbol:
            raise ValueError("V8 PAPER requires pending margin and symbol dedupe")


PORTFOLIO_POLICY = PortfolioPolicy()


def canonical_json_bytes(value: Any) -> bytes:
    """Return the stable JSON representation used by all control hashes."""

    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def canonical_json_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def setup_payload() -> list[dict[str, Any]]:
    return [asdict(setup) for setup in ACTIVE_SETUPS]


def setup_for(signal_end: str, side: str) -> PaperSetup | None:
    wanted_side = str(side).strip().upper()
    wanted_end = str(signal_end).strip()
    return next(
        (
            setup
            for setup in ACTIVE_SETUPS
            if setup.signal_end == wanted_end and setup.side == wanted_side
        ),
        None,
    )


def entry_policy_for_setup(
    setup_or_signal_end: PaperSetup | str,
    side: str | None = None,
) -> EntryPolicy:
    """Resolve the global VC entry seam plus the setup's literal overrides."""

    if isinstance(setup_or_signal_end, PaperSetup):
        setup = setup_or_signal_end
    else:
        if side is None:
            raise ValueError("side is required when resolving by signal_end")
        setup = setup_for(str(setup_or_signal_end), side)
        if setup is None:
            raise KeyError(f"Unknown V8-Combined setup: {setup_or_signal_end}_{side}")

    changes: dict[str, Any] = {}
    if setup.entry_conf_minute is not None:
        changes["max_confirmation_minute"] = int(setup.entry_conf_minute)
    if setup.entry_buffer_bps is not None:
        changes["buffer_bps"] = float(setup.entry_buffer_bps)
    if setup.entry_midpoint is not None:
        changes["midpoint_invalidation"] = bool(setup.entry_midpoint)
    if setup.entry_clv != ENTRY_INHERIT:
        changes["close_location_min"] = (
            None if setup.entry_clv is None else float(setup.entry_clv)
        )
    policy = replace(BASE_ENTRY_POLICY, **changes)
    policy.validate()
    return policy


def resolved_entry_policies_payload() -> dict[str, dict[str, Any]]:
    return {
        setup.setup_id: asdict(entry_policy_for_setup(setup))
        for setup in ACTIVE_SETUPS
    }


def strategy_payload() -> dict[str, Any]:
    """Return every semantic input that an activation permit must bind."""

    return {
        "schema_version": CONFIG_SCHEMA_VERSION,
        "strategy_family": STRATEGY_FAMILY,
        "strategy_version": STRATEGY_VERSION,
        "mode": MODE,
        "paper_only": PAPER_ONLY,
        "setup_book_sha256": COMBINED_SETUP_BOOK_SHA256,
        "setups": setup_payload(),
        "entry_policies": resolved_entry_policies_payload(),
        "portfolio_policy": asdict(PORTFOLIO_POLICY),
        "required_kite_apps": REQUIRED_KITE_APPS,
        "data_contract": {
            "execution_instrument": EXECUTION_INSTRUMENT,
            "oi_instrument": OI_INSTRUMENT,
            "bar_source_policy": BAR_SOURCE_POLICY,
            "completed_bars_only": True,
            "partial_bars": False,
            "synthetic_or_gap_filled_bars": False,
            "ltp_fill_fallback": False,
            "same_session_only": True,
        },
        "execution_contract": {
            "confirmation": "FIRST_STRICT_PASS_THROUGH_PER_LEG_MAX",
            "same_confirmation_bar_fill": False,
            "entry_expiry_minute": ENTRY_EXPIRY_MINUTE,
            "gap_fill": "ADVERSE_OPEN",
            "trigger_rounding": "DIRECTIONAL_AWAY_TO_EQUITY_TICK",
            "brackets": "SETUP_PERCENT_FROM_ACTUAL_MODELED_FILL",
            "same_bar_policy": "STOP_FIRST",
            "post_confirmation_cancel": "CLOSE_REVERSES_THROUGH_5M_CLOSE",
            "square_off": SQUARE_OFF,
            "eod_policy": "EXACT_SQUARE_OFF",
        },
        "portfolio_allocation": PORTFOLIO_ALLOCATION_POLICY,
    }


def strategy_fingerprint() -> str:
    return canonical_json_sha256(strategy_payload())


def source_paths() -> Mapping[str, Path]:
    """Expose isolated paths without creating them."""

    return {
        "root": ROOT,
        "control_root": CONTROL_ROOT,
        "permit_archive_root": PERMIT_ARCHIVE_ROOT,
        "activation": ACTIVATION_PATH,
        "kill_switch": KILL_SWITCH_PATH,
        "session_root": SESSION_ROOT,
        "evidence_root": EVIDENCE_ROOT,
        "checkpoint_root": CHECKPOINT_ROOT,
        "lock": LOCK_PATH,
        "latest_report": LATEST_REPORT_PATH,
    }


def validate_configuration() -> None:
    if MODE != "PAPER" or PAPER_ONLY is not True:
        raise AssertionError("V8-Combined forward session must remain PAPER only")
    observed_hash = canonical_json_sha256(setup_payload())
    if observed_hash != COMBINED_SETUP_BOOK_SHA256:
        raise AssertionError(
            "V8-Combined PAPER setup book changed: "
            f"expected {COMBINED_SETUP_BOOK_SHA256}, observed {observed_hash}"
        )
    if len(ACTIVE_SETUPS) != 10 or len({s.setup_id for s in ACTIVE_SETUPS}) != 10:
        raise AssertionError("V8-Combined PAPER requires ten unique legs")
    expected_pairs = {
        (slot, side)
        for slot in ("09:25", "09:30", "09:35", "09:40", "09:45")
        for side in ("LONG", "SHORT")
    }
    if {(s.signal_end, s.side) for s in ACTIVE_SETUPS} != expected_pairs:
        raise AssertionError("V8-Combined PAPER setup clocks or sides changed")
    if REQUIRED_KITE_APPS != 8:
        raise AssertionError("V8-Combined PAPER requires exactly eight Kite apps")
    if time.fromisoformat(CONTROL_EXPIRY) <= time.fromisoformat(SQUARE_OFF):
        raise AssertionError("control expiry must follow square-off")
    PORTFOLIO_POLICY.validate()
    for setup in ACTIVE_SETUPS:
        entry_policy_for_setup(setup).validate()
    if ROOT.name != "v8_combined_paper_v1":
        raise AssertionError("V8-Combined PAPER runtime root changed")
    lowered_root = str(ROOT).replace("\\", "/").lower()
    if "/v6_live" in lowered_root or "/strategy_research/" in lowered_root:
        raise AssertionError("V8-Combined PAPER paths overlap another namespace")


validate_configuration()
