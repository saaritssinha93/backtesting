"""Independent FNO V8 windowed strict-confirmation backtester.

V8 keeps a literal copy of the V6 five-minute setup book but owns its data
construction, one-minute entry state machine, execution simulation, cache,
outputs and provenance.  It intentionally does not import the V6/V7 strategy
modules, their caches, the legacy sweep builder or the legacy replay engine.

Prototype data contract
-----------------------
* NSE cash-equity OHLCV supplies prices and execution paths.
* The statically mapped August stock future supplies five-minute OI only.
* The dated 2026-08-11 universe is used for prototype parity research.
* Every path is restricted to the signal date; no next-session fallback.

This prototype is not promotion-quality point-in-time futures research.  The
historical point-in-time universes, rolling near-month futures data and exact
15:30 coverage required for that claim are not yet available for the full
research window.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import inspect
import json
import math
import sys
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common


STRATEGY_VERSION = "FNO_V8_WINDOWED_STRICT_CONFIRM_BREAKOUT_20260818"
OBJECTIVE = "WINDOWED_STRICT_ENTRY_RESEARCH"
CONFIG_SOURCE = "LITERAL_V6_5M_BOOK_WITH_INDEPENDENT_V8_ENTRY_ENGINE"

CACHE_SCHEMA_VERSION = "fno_v8_windowed_1m_cache_manifest_v2"
PATH_POLICY_VERSION = "fno_v8_same_session_exact_grid_ohlcvt_v2"
STATE_EVENT_SCHEMA_VERSION = "fno_v8_windowed_1m_state_event_v1"
TRADE_SCHEMA_VERSION = "fno_v8_windowed_1m_trade_v3"
DIAGNOSTIC_BREAKDOWN_SCHEMA_VERSION = "fno_v8_diagnostic_breakdown_v1"
EXCURSION_POLICY_VERSION = "fno_v8_post_fill_ohlc_bounds_v2"
RUN_SCHEMA_VERSION = "fno_v8_windowed_1m_run_v4"

BACKTEST_UNIVERSE_DATE = date(2026, 8, 11)
BACKTEST_UNIVERSE_PATH = common.UNIVERSE_DIR / "near_month_2026-08-11.parquet"
BACKTEST_UNIVERSE_HASHES = {
    "file_sha256": "24170f39c7cf99021553396e40e0d88a435f857364b2423dcfbe9312539dbf09",
    "universe_sha256": "18c496bbf9e09b6914d073cba21c4c6c56305da1ed5759f4f91cc8cb66c19ad5",
    "mapped_universe_sha256": "2cc160189f87bff4eb987a15a4684d95619ee9c810db3cd37276b114ad5824bf",
    "mapped_symbol_set_sha256": "d42f87a9c5fc8ab1710b09b6c4c9832c9d19ecc440ef92b84cad6981499a05a3",
}

# Frozen regular-session calendar for this research generation.  The holiday
# list is copied from NSE F&O circular NSE/FAOP/71777 (December 12, 2025).
# The special Muhurat session on Sunday 2026-11-08 is deliberately excluded:
# V8 models only the standard 09:15-15:30 session.  Runs outside calendar year
# 2026 fail closed instead of inferring sessions from the bars that happen to
# exist in mutable or incomplete source files.
NSE_FO_CALENDAR_SCHEMA_VERSION = "nse_fo_regular_session_calendar_2026_v1"
NSE_FO_CALENDAR_SOURCE = (
    "https://nsearchives.nseindia.com/content/circulars/FAOP71777.pdf"
)
NSE_FO_CALENDAR_SOURCE_SHA256 = (
    "5a2079cd78b2e6b536ef0d28300e63b645721bed22cc82a91facf5945f3296ea"
)
NSE_FO_CALENDAR_CIRCULAR = "NSE/FAOP/71777"
NSE_FO_CALENDAR_AMENDMENTS = (
    {
        "circular": "NSE/FAOP/72262",
        "source": "https://nsearchives.nseindia.com/content/circulars/FAOP72262.pdf",
        "source_sha256": (
            "f98ae37cf6a6f8c18b1064b0551599d084c4b4a27804842c487f0da43fea8f0f"
        ),
        "change": "ADDED_TRADING_HOLIDAY_2026-01-15",
    },
)
NSE_CASH_CALENDAR_SOURCES = (
    {
        "circular": "NSE/CMTR/71775",
        "source": "https://nsearchives.nseindia.com/content/circulars/CMTR71775.pdf",
        "source_sha256": (
            "aa97e0afc0ce394097f2fc62631c68e3c2e4c7c23541ed35f21d9fc06b0dcacb"
        ),
        "change": "BASE_2026_TRADING_HOLIDAYS",
    },
    {
        "circular": "NSE/CMTR/72260",
        "source": "https://nsearchives.nseindia.com/content/circulars/CMTR72260.pdf",
        "source_sha256": (
            "c5d32a838b46d5044717b830a4793479a51a40f8e4e19d3896c65682d15b6ebc"
        ),
        "change": "ADDED_TRADING_HOLIDAY_2026-01-15",
    },
)
NSE_REGULAR_SPECIAL_SESSION_SOURCES = (
    {
        "segment": "FUTURES_AND_OPTIONS",
        "circular": "NSE/FAOP/72352",
        "source": "https://nsearchives.nseindia.com/content/circulars/FAOP72352.pdf",
        "source_sha256": (
            "7b282150e8cf7757da6944c682fe810189d2ed86fe6b171094bb4cd4d7f1facb"
        ),
        "change": "ADDED_FULL_REGULAR_SESSION_2026-02-01",
    },
    {
        "segment": "CAPITAL_MARKET",
        "circular": "NSE/CMTR/72349",
        "source": "https://nsearchives.nseindia.com/content/circulars/CMTR72349.pdf",
        "source_sha256": (
            "70458f7b8126f47584d4bc402a78dde357e42dbebaa5d53cfce0c70c82dabfc3"
        ),
        "change": "ADDED_FULL_REGULAR_SESSION_2026-02-01",
    },
)
NSE_FO_TRADING_HOLIDAYS_2026 = (
    "2026-01-15",
    "2026-01-26",
    "2026-03-03",
    "2026-03-26",
    "2026-03-31",
    "2026-04-03",
    "2026-04-14",
    "2026-05-01",
    "2026-05-28",
    "2026-06-26",
    "2026-09-14",
    "2026-10-02",
    "2026-10-20",
    "2026-11-10",
    "2026-11-24",
    "2026-12-25",
)
NSE_FO_NONSTANDARD_SESSIONS_EXCLUDED = ("2026-11-08",)
NSE_REGULAR_SPECIAL_SESSIONS_INCLUDED = ("2026-02-01",)
NSE_FO_CALENDAR_SHA256 = (
    "bbbd6306c532bc2cdfd2c8dab6880bd7df2eed81e5f8a5c3cf51df40b4e55bd4"
)

SOURCE_V6_SETUP_BOOK_SHA256 = (
    "3c3e59187768afbc015024b5735d1c1b62d91128e8d6888ccfaa6f1c6c15694a"
)
# Book hash after the 2026-08-19 retune of four legs.  The pre-retune
# V6-lineage book hashed to
# c50bc5d17fdbde3cad824a4103a6a4b4c9ebc91235dab39b6a533d601b6e24d9.
V8_SETUP_BOOK_SHA256 = (
    "ed32937129246ca3500bd421a77bebca71c83014a4e2a4eb5cbc318e74016fb6"
)

# Sentinel for "this leg does not override the global entry seam".  It is a
# plain string so it survives asdict()/canonical JSON hashing of the book.
ENTRY_INHERIT = "INHERIT"

BASE_PRICE_CHANGE_PCT = 0.10
BASE_OI_CHANGE_PCT = 0.05
BASE_VOLUME_RATIO = 0.80

V8_ROOT = common.FNO_ROOT / "strategy_research" / "v8_windowed_strict_v1"
CACHE_DIR = V8_ROOT / "cache"
SNAPSHOT_ROOT = V8_ROOT / "snapshots"
RUN_ROOT = V8_ROOT / "runs"
PROVENANCE_ROOT = V8_ROOT / "provenance"
REPORT_PATH = common.LATEST_DIR / "latest_fno_v8_windowed_1m_research.md"
CACHE_MANIFEST_PATH = CACHE_DIR / "manifest.json"
CANDIDATE_CACHE_PATH = CACHE_DIR / "five_minute_candidates.parquet"
PATH_CACHE_PATH = CACHE_DIR / "same_session_minute_paths.parquet"

# Independence is enforced by requiring the source snapshot explicitly on the
# CLI.  A V8 run never silently consumes a V6/V7 cache or snapshot namespace.
DEFAULT_SOURCE_SNAPSHOT: Path | None = None

FORBIDDEN_IMPORT_PREFIXES = (
    "fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6",
    "fno_oi_ema_confirm_0925_0930_0935_0940_0945_v7",
    "fno_oi_ema_confirm_sweep",
    "fno_v5_hybrid_backtest",
    "fno_oi_ema_confirm_optimize",
    "fno_oi_ema_confirm_v7_signal_cache",
    "fno_v5_live_config",
    "fno_v6_live_config",
)

MODULE_IMPORT_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


@dataclass(frozen=True)
class V8Setup:
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
    # Optional per-setup overrides of the one-minute entry seam.  ``None``
    # means "inherit the run's global EntryPolicy", which keeps every leg that
    # does not override behaving exactly as it did under any variant.
    # ``entry_clv`` needs its own sentinel because ``None`` is a meaningful
    # value there (no close-location floor at all).
    entry_conf_minute: int | None = None
    entry_buffer_bps: float | None = None
    entry_midpoint: bool | None = None
    entry_clv: float | str | None = ENTRY_INHERIT

    @property
    def setup_id(self) -> str:
        return f"{self.signal_end}_{self.side}"

    @property
    def overrides_entry_policy(self) -> bool:
        return (
            self.entry_conf_minute is not None
            or self.entry_buffer_bps is not None
            or self.entry_midpoint is not None
            or self.entry_clv != ENTRY_INHERIT
        )


@dataclass(frozen=True)
class EntryPolicy:
    buffer_bps: float = 2.0
    max_confirmation_minute: int = 4
    entry_expiry_minute: int = 5
    close_location_min: float | None = None
    cost_bps: float = 5.0
    slippage_bps: float = 0.0
    midpoint_invalidation: bool = True
    post_confirmation_cancel: bool = True
    allow_cap_reassignment: bool = True
    same_bar_policy: str = "STOP_FIRST"
    square_off: str | None = None
    eod_policy: str = "LAST_REAL_BAR_SENSITIVITY"

    def validate(self) -> None:
        for name, value in (
            ("max_confirmation_minute", self.max_confirmation_minute),
            ("entry_expiry_minute", self.entry_expiry_minute),
        ):
            if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
                raise ValueError(f"{name} must be an integer")
        if self.max_confirmation_minute < 1:
            raise ValueError("max_confirmation_minute must be positive")
        if self.entry_expiry_minute <= self.max_confirmation_minute:
            raise ValueError(
                "entry_expiry_minute must be later than max confirmation minute"
            )
        if not all(
            math.isfinite(float(value))
            for value in (self.buffer_bps, self.cost_bps, self.slippage_bps)
        ):
            raise ValueError("buffer/cost/slippage must be finite")
        if self.buffer_bps < 0 or self.cost_bps < 0 or self.slippage_bps < 0:
            raise ValueError("buffer/cost/slippage cannot be negative")
        if self.buffer_bps >= 10_000 or self.slippage_bps >= 10_000:
            raise ValueError("buffer/slippage must be below 10,000 bps")
        if self.close_location_min is not None:
            if not math.isfinite(float(self.close_location_min)) or not (
                0.0 <= self.close_location_min <= 1.0
            ):
                raise ValueError("close_location_min must be finite and in [0, 1]")
        if self.same_bar_policy != "STOP_FIRST":
            raise ValueError("V8 prototype supports only STOP_FIRST")
        if self.eod_policy not in {
            "EXACT_SQUARE_OFF",
            "LAST_REAL_BAR_SENSITIVITY",
        }:
            raise ValueError(f"Unsupported eod policy: {self.eod_policy}")
        if self.eod_policy == "EXACT_SQUARE_OFF" and not self.square_off:
            raise ValueError("EXACT_SQUARE_OFF requires square_off")
        if self.square_off:
            try:
                parsed_square_off = datetime.strptime(self.square_off, "%H:%M").time()
            except ValueError as exc:
                raise ValueError("square_off must be HH:MM") from exc
            if parsed_square_off > datetime.strptime("15:30", "%H:%M").time():
                raise ValueError("square_off cannot be later than the 15:30 session close")


@dataclass(frozen=True)
class CandidateInput:
    symbol: str
    signal_time: pd.Timestamp
    five_min_open: float
    five_min_high: float
    five_min_low: float
    five_min_close: float
    price_change_pct: float
    oi_change_pct: float
    volume_ratio: float
    traded_value: float
    tick_size: float = 0.05
    futures_symbol: str = ""
    futures_instrument_token: int = 0
    equity_instrument_token: int = 0
    lot_size: int = 1
    five_min_volume: float = math.nan
    ema9: float = math.nan
    ema20: float = math.nan
    ema50: float = math.nan
    oi: float = math.nan
    prev_oi: float = math.nan

    @property
    def signal_ts(self) -> pd.Timestamp:
        return _to_ist_timestamp(self.signal_time)

    @property
    def session_date(self) -> date:
        return self.signal_ts.date()


@dataclass(frozen=True)
class MinuteBar:
    timestamp: pd.Timestamp
    open: float
    high: float
    low: float
    close: float
    volume: float
    gap_filled: bool = False
    opening_snapshot: bool = False
    provisional_stale: bool = False

    @property
    def ts(self) -> pd.Timestamp:
        return _to_ist_timestamp(self.timestamp)


@dataclass(frozen=True)
class PortfolioPolicy:
    capital_rs: float = 120_000.0
    margin_per_entry_rs: float = 10_000.0
    target_exposure_per_entry_rs: float = 50_000.0
    max_concurrent_positions: int = 12
    pending_reserves_margin: bool = True
    one_position_per_symbol: bool = True

    def validate(self) -> None:
        for name, value in (
            ("capital_rs", self.capital_rs),
            ("margin_per_entry_rs", self.margin_per_entry_rs),
            ("target_exposure_per_entry_rs", self.target_exposure_per_entry_rs),
        ):
            if not math.isfinite(float(value)):
                raise ValueError(f"{name} must be finite")
        if self.capital_rs <= 0 or self.margin_per_entry_rs <= 0:
            raise ValueError("portfolio capital and margin must be positive")
        if self.target_exposure_per_entry_rs <= 0:
            raise ValueError("target exposure must be positive")
        if (
            isinstance(self.max_concurrent_positions, bool)
            or not isinstance(self.max_concurrent_positions, (int, np.integer))
            or self.max_concurrent_positions <= 0
        ):
            raise ValueError("max_concurrent_positions must be positive")


class SignalState(str, Enum):
    MONITORING = "MONITORING_CONFIRMATION"
    PRECONF_INVALIDATED = "PRECONF_INVALIDATED"
    CONFIRMED_WAITING_CAP = "CONFIRMED_WAITING_CAP"
    PENDING_STOP = "PENDING_STOP"
    POSTCONF_CANCELLED = "POSTCONF_CANCELLED"
    WINDOW_EXPIRED = "WINDOW_EXPIRED"
    NO_CONFIRMATION = "NO_CONFIRMATION"
    DATA_INCOMPLETE = "DATA_INCOMPLETE"
    FILLED_OPEN = "FILLED_OPEN"
    STOPPED = "STOPPED"
    TARGETED = "TARGETED"
    SQUARE_OFF = "SQUARE_OFF"
    PORTFOLIO_REJECTED = "PORTFOLIO_REJECTED"
    DUPLICATE_REJECTED = "DUPLICATE_REJECTED"


_ALLOWED_TRANSITIONS: dict[SignalState, frozenset[SignalState]] = {
    SignalState.MONITORING: frozenset(
        {
            SignalState.PRECONF_INVALIDATED,
            SignalState.CONFIRMED_WAITING_CAP,
            SignalState.NO_CONFIRMATION,
            SignalState.DATA_INCOMPLETE,
            SignalState.PORTFOLIO_REJECTED,
            SignalState.DUPLICATE_REJECTED,
        }
    ),
    SignalState.CONFIRMED_WAITING_CAP: frozenset(
        {
            SignalState.PENDING_STOP,
            SignalState.POSTCONF_CANCELLED,
            SignalState.WINDOW_EXPIRED,
            SignalState.DATA_INCOMPLETE,
            SignalState.PORTFOLIO_REJECTED,
            SignalState.DUPLICATE_REJECTED,
        }
    ),
    SignalState.PENDING_STOP: frozenset(
        {
            SignalState.FILLED_OPEN,
            SignalState.POSTCONF_CANCELLED,
            SignalState.WINDOW_EXPIRED,
            SignalState.DATA_INCOMPLETE,
            SignalState.PORTFOLIO_REJECTED,
            SignalState.DUPLICATE_REJECTED,
        }
    ),
    SignalState.FILLED_OPEN: frozenset(
        {
            SignalState.STOPPED,
            SignalState.TARGETED,
            SignalState.SQUARE_OFF,
            SignalState.DATA_INCOMPLETE,
        }
    ),
}


@dataclass
class _CandidateRuntime:
    candidate: CandidateInput
    state: SignalState = SignalState.MONITORING
    reason: str = ""
    confirmation_minute: int | None = None
    confirmation_bar: MinuteBar | None = None
    trigger: float | None = None
    order_placed_at: pd.Timestamp | None = None
    entry_minute: int | None = None
    entry_time: pd.Timestamp | None = None
    entry_price: float | None = None
    stop_price: float | None = None
    target_price: float | None = None
    exit_time: pd.Timestamp | None = None
    exit_price: float | None = None
    exit_reason: str = ""
    exit_at_bar_open: bool = False
    gross_return_pct: float | None = None
    net_return_pct: float | None = None
    gap_fill: bool = False
    intrabar_trigger_fill: bool = False
    ambiguous_entry_bar: bool = False
    confirmation_checks: list[dict[str, Any]] = field(default_factory=list)
    events: list[dict[str, Any]] = field(default_factory=list)

    def transition(
        self,
        new_state: SignalState,
        *,
        event_ts: pd.Timestamp,
        reason: str,
    ) -> None:
        old_state = self.state
        if new_state not in _ALLOWED_TRANSITIONS.get(old_state, frozenset()):
            raise AssertionError(
                f"Invalid V8 state transition: {old_state.value} -> "
                f"{new_state.value} ({reason})"
            )
        self.state = new_state
        self.reason = reason
        self.events.append(
            {
                "symbol": self.candidate.symbol,
                "event_ts": _to_ist_timestamp(event_ts),
                "state_before": old_state.value,
                "state_after": new_state.value,
                "reason": reason,
            }
        )


# Four legs were retuned on 2026-08-19 from the setup-parameter sweep over
# 2026-05-27..2026-08-17 (57 sessions, conditional-stream coverage).  Each
# carries the entry seam its own sweep selected; the six untouched legs keep
# the original V6-lineage values and inherit the run's global entry policy.
#
# Provenance for the four retuned legs (setup_param_sweeps run directories):
#   09:25_LONG  sweep_0925_LONG_20260819T174128830527+0530_befe1256f673
#   09:25_SHORT sweep_0925_SHORT_20260819T182024291731+0530_d8c66340bc60
#   09:30_SHORT sweep_0930_SHORT_20260819T190613582965+0530_cadb8948a596
#   09:40_SHORT sweep_0940_SHORT_20260819T194335202048+0530_afbcb4356b45
#
# These were fit on the whole 57-session window with no holdout left over, and
# were never simulated jointly through one portfolio ledger.  Treat the book
# below as a research configuration, not a validated one.
ACTIVE_SETUPS: tuple[V8Setup, ...] = (
    V8Setup("09:25", "LONG", 4, "max_move", 0.30, 0.10, 3.0, 0.00, 0.50, 0.0, 0.40, 1.0,
            entry_conf_minute=3, entry_buffer_bps=0.0, entry_midpoint=False, entry_clv=None),
    V8Setup("09:25", "SHORT", 4, "max_move", 0.20, 0.10, 1.5, 0.60, 0.60, 25_000_000.0, 0.50, 3.0,
            entry_conf_minute=3, entry_buffer_bps=2.0, entry_midpoint=False, entry_clv=None),
    V8Setup("09:30", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.50, 0.50, 0.0, 1.00, 2.5),
    V8Setup("09:30", "SHORT", 4, "max_volume", 0.20, 1.00, 1.0, 0.45, 0.30, 25_000_000.0, 1.00, 4.0,
            entry_conf_minute=3, entry_buffer_bps=0.0, entry_midpoint=True, entry_clv=0.50),
    V8Setup("09:35", "LONG", 1, "max_liquidity", 0.20, 0.10, 1.0, 0.60, 0.50, 0.0, 1.00, 2.5),
    V8Setup("09:35", "SHORT", 2, "max_liquidity", 0.50, 1.00, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    V8Setup("09:40", "LONG", 1, "max_liquidity", 0.20, 0.10, 2.0, 0.50, 0.50, 0.0, 0.50, 2.5),
    V8Setup("09:40", "SHORT", 4, "max_volume", 0.20, 0.75, 1.0, 0.00, 0.20, 0.0, 1.00, 4.0,
            entry_conf_minute=4, entry_buffer_bps=0.0, entry_midpoint=False, entry_clv=0.50),
    V8Setup("09:45", "LONG", 1, "max_move", 0.65, 0.10, 1.0, 0.40, 0.50, 0.0, 1.00, 3.0),
    V8Setup("09:45", "SHORT", 1, "max_volume", 0.20, 0.75, 1.0, 0.40, 0.30, 0.0, 1.00, 2.0),
)


def _to_ist_timestamp(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize(common.IST)
    return ts.tz_convert(common.IST)


def _decimal(value: float | str | Decimal) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def round_up_to_tick(value: float, tick_size: float) -> float:
    tick = _decimal(tick_size)
    if tick <= 0:
        raise ValueError("tick_size must be positive")
    units = (_decimal(value) / tick).to_integral_value(rounding=ROUND_CEILING)
    return float(units * tick)


def round_down_to_tick(value: float, tick_size: float) -> float:
    tick = _decimal(tick_size)
    if tick <= 0:
        raise ValueError("tick_size must be positive")
    units = (_decimal(value) / tick).to_integral_value(rounding=ROUND_FLOOR)
    return float(units * tick)


def _valid_bar(bar: MinuteBar) -> bool:
    prices = np.asarray([bar.open, bar.high, bar.low, bar.close], dtype=float)
    return bool(
        np.isfinite(prices).all()
        and (prices > 0).all()
        and float(bar.high) >= max(float(bar.open), float(bar.close))
        and float(bar.low) <= min(float(bar.open), float(bar.close))
        and float(bar.high) >= float(bar.low)
        and math.isfinite(float(bar.volume))
        and float(bar.volume) >= 0
        and not bar.gap_filled
        and not bar.opening_snapshot
        and not bar.provisional_stale
    )


def _confirmation_check(
    setup: V8Setup,
    candidate: CandidateInput,
    bar: MinuteBar,
    policy: EntryPolicy | None = None,
) -> dict[str, Any]:
    """Return the strict-gate decision and its lossless audit diagnostics.

    Rejection codes are emitted in rule-evaluation order.  The calculation is
    observational only: :func:`strict_confirmation_passes` remains the public
    Boolean strategy seam and consumes the same decision.
    """

    policy = policy or EntryPolicy()
    record: dict[str, Any] = {
        "timestamp": bar.ts,
        "open": float(bar.open),
        "high": float(bar.high),
        "low": float(bar.low),
        "close": float(bar.close),
        "volume": float(bar.volume),
        "candle_range": None,
        "body_ratio": None,
        "adverse_wick_ratio": None,
        "close_location": None,
        "passed": False,
        "rejection_codes": [],
    }
    if not _valid_bar(bar):
        record["rejection_codes"] = ["INVALID_BAR"]
        return record
    candle_range = float(bar.high) - float(bar.low)
    record["candle_range"] = candle_range
    if candle_range <= 0:
        record["rejection_codes"] = ["NONPOSITIVE_RANGE"]
        return record
    body_ratio = abs(float(bar.close) - float(bar.open)) / candle_range
    if setup.side == "LONG":
        candle_direction_ok = float(bar.close) > float(bar.open)
        beyond_five_minute_close = float(bar.close) > float(
            candidate.five_min_close
        )
        adverse_wick = (
            float(bar.high) - max(float(bar.open), float(bar.close))
        ) / candle_range
        close_location = (float(bar.close) - float(bar.low)) / candle_range
    else:
        candle_direction_ok = float(bar.close) < float(bar.open)
        beyond_five_minute_close = float(bar.close) < float(
            candidate.five_min_close
        )
        adverse_wick = (
            min(float(bar.open), float(bar.close)) - float(bar.low)
        ) / candle_range
        close_location = (float(bar.high) - float(bar.close)) / candle_range
    record.update(
        {
            "body_ratio": body_ratio,
            "adverse_wick_ratio": adverse_wick,
            "close_location": close_location,
        }
    )
    rejection_codes: list[str] = []
    if not candle_direction_ok:
        rejection_codes.append("WRONG_CANDLE_DIRECTION")
    if not beyond_five_minute_close:
        rejection_codes.append("CLOSE_NOT_BEYOND_FIVE_MINUTE_CLOSE")
    if body_ratio + 1e-12 < float(setup.body_ratio):
        rejection_codes.append("BODY_RATIO_BELOW_MINIMUM")
    if adverse_wick - 1e-12 > float(setup.max_wick_ratio):
        rejection_codes.append("ADVERSE_WICK_RATIO_ABOVE_MAXIMUM")
    if (
        policy.close_location_min is not None
        and close_location + 1e-12 < policy.close_location_min
    ):
        rejection_codes.append("CLOSE_LOCATION_BELOW_MINIMUM")
    record["rejection_codes"] = rejection_codes
    record["passed"] = not rejection_codes
    return record


def strict_confirmation_passes(
    setup: V8Setup,
    candidate: CandidateInput,
    bar: MinuteBar,
    policy: EntryPolicy | None = None,
) -> bool:
    return bool(_confirmation_check(setup, candidate, bar, policy)["passed"])


def build_trigger(
    setup: V8Setup,
    bar: MinuteBar,
    policy: EntryPolicy,
    *,
    tick_size: float = 0.05,
) -> float:
    policy.validate()
    scale = policy.buffer_bps / 10_000.0
    if setup.side == "LONG":
        return round_up_to_tick(float(bar.high) * (1.0 + scale), tick_size)
    return round_down_to_tick(float(bar.low) * (1.0 - scale), tick_size)


def _picker_value(setup: V8Setup, candidate: CandidateInput) -> float:
    values = {
        "max_oi": float(candidate.oi_change_pct),
        "max_volume": float(candidate.volume_ratio),
        "max_move": abs(float(candidate.price_change_pct)),
        "max_liquidity": float(candidate.traded_value),
    }
    if setup.picker not in values:
        raise ValueError(f"Unsupported picker: {setup.picker}")
    return values[setup.picker]


def _rank_candidates(
    setup: V8Setup, runtimes: Iterable[_CandidateRuntime]
) -> list[_CandidateRuntime]:
    return sorted(
        runtimes,
        key=lambda runtime: (
            -_picker_value(setup, runtime.candidate),
            -float(runtime.candidate.traded_value),
            str(runtime.candidate.symbol),
        ),
    )


def _bar_map(
    candidate: CandidateInput, bars: Sequence[MinuteBar]
) -> dict[pd.Timestamp, MinuteBar]:
    mapped: dict[pd.Timestamp, MinuteBar] = {}
    for bar in bars:
        ts = bar.ts
        if ts.date() != candidate.session_date:
            continue
        if ts in mapped:
            raise ValueError(
                f"Duplicate one-minute timestamp for {candidate.symbol}: {ts}"
            )
        mapped[ts] = bar
    return mapped


def _relative_minute(candidate: CandidateInput, ts: pd.Timestamp) -> int:
    delta = (_to_ist_timestamp(ts) - candidate.signal_ts).total_seconds() / 60.0
    if not float(delta).is_integer():
        raise ValueError("V8 entry bars must be exact whole minutes after signal")
    return int(delta)


def _preconfirmation_invalidated(
    setup: V8Setup, candidate: CandidateInput, bar: MinuteBar
) -> bool:
    midpoint = (float(candidate.five_min_high) + float(candidate.five_min_low)) / 2.0
    return (
        float(bar.close) < midpoint
        if setup.side == "LONG"
        else float(bar.close) > midpoint
    )


def _postconfirmation_invalidated(
    setup: V8Setup, candidate: CandidateInput, bar: MinuteBar
) -> bool:
    return (
        float(bar.close) < float(candidate.five_min_close)
        if setup.side == "LONG"
        else float(bar.close) > float(candidate.five_min_close)
    )


def _entry_fill(
    setup: V8Setup,
    runtime: _CandidateRuntime,
    bar: MinuteBar,
    policy: EntryPolicy,
) -> tuple[float, bool] | None:
    assert runtime.trigger is not None
    trigger = float(runtime.trigger)
    slippage = policy.slippage_bps / 10_000.0
    tick = runtime.candidate.tick_size
    if setup.side == "LONG":
        if float(bar.open) >= trigger:
            raw = float(bar.open) * (1.0 + slippage)
            return round_up_to_tick(raw, tick), True
        if float(bar.high) >= trigger:
            raw = trigger * (1.0 + slippage)
            return round_up_to_tick(raw, tick), False
    else:
        if float(bar.open) <= trigger:
            raw = float(bar.open) * (1.0 - slippage)
            return round_down_to_tick(raw, tick), True
        if float(bar.low) <= trigger:
            raw = trigger * (1.0 - slippage)
            return round_down_to_tick(raw, tick), False
    return None


def _brackets(
    setup: V8Setup, candidate: CandidateInput, entry_price: float
) -> tuple[float, float]:
    tick = candidate.tick_size
    if setup.side == "LONG":
        stop = round_down_to_tick(
            entry_price * (1.0 - setup.stop_pct / 100.0), tick
        )
        target = round_down_to_tick(
            entry_price * (1.0 + setup.target_pct / 100.0), tick
        )
    else:
        stop = round_up_to_tick(
            entry_price * (1.0 + setup.stop_pct / 100.0), tick
        )
        target = round_up_to_tick(
            entry_price * (1.0 - setup.target_pct / 100.0), tick
        )
    return stop, target


def _exit_on_bar(
    setup: V8Setup,
    runtime: _CandidateRuntime,
    bar: MinuteBar,
    *,
    position_open_at_bar_start: bool = True,
) -> tuple[str, float] | None:
    assert runtime.stop_price is not None and runtime.target_price is not None
    stop = float(runtime.stop_price)
    target = float(runtime.target_price)
    if setup.side == "LONG":
        if position_open_at_bar_start and float(bar.open) <= stop:
            return "STOP_GAP", round_down_to_tick(
                float(bar.open), runtime.candidate.tick_size
            )
        if position_open_at_bar_start and float(bar.open) >= target:
            return "TARGET", target
        stop_hit = float(bar.low) <= stop
        target_hit = float(bar.high) >= target
    else:
        if position_open_at_bar_start and float(bar.open) >= stop:
            return "STOP_GAP", round_up_to_tick(
                float(bar.open), runtime.candidate.tick_size
            )
        if position_open_at_bar_start and float(bar.open) <= target:
            return "TARGET", target
        stop_hit = float(bar.high) >= stop
        target_hit = float(bar.low) <= target
    if stop_hit:
        return "STOP", stop
    if target_hit:
        return "TARGET", target
    return None


def _exit_occurs_at_bar_open(
    setup: V8Setup,
    runtime: _CandidateRuntime,
    bar: MinuteBar,
) -> bool:
    """Whether an already-open position deterministically exits at this open."""

    assert runtime.stop_price is not None and runtime.target_price is not None
    opening = float(bar.open)
    stop = float(runtime.stop_price)
    target = float(runtime.target_price)
    if setup.side == "LONG":
        return opening <= stop or opening >= target
    return opening >= stop or opening <= target


def _close_runtime(
    setup: V8Setup,
    runtime: _CandidateRuntime,
    *,
    exit_time: pd.Timestamp,
    exit_price: float,
    exit_reason: str,
    policy: EntryPolicy,
) -> None:
    assert runtime.entry_price is not None
    entry = float(runtime.entry_price)
    gross = (
        exit_price / entry - 1.0
        if setup.side == "LONG"
        else 1.0 - exit_price / entry
    ) * 100.0
    runtime.exit_time = _to_ist_timestamp(exit_time)
    runtime.exit_price = float(exit_price)
    runtime.exit_reason = exit_reason
    runtime.gross_return_pct = gross
    runtime.net_return_pct = gross - policy.cost_bps / 100.0
    terminal = (
        SignalState.STOPPED
        if exit_reason.startswith("STOP")
        else SignalState.TARGETED
        if exit_reason == "TARGET"
        else SignalState.SQUARE_OFF
    )
    runtime.transition(
        terminal,
        event_ts=runtime.exit_time,
        reason=exit_reason,
    )


def _audit_record(
    setup: V8Setup,
    runtime: _CandidateRuntime,
) -> dict[str, Any]:
    candidate = runtime.candidate
    passed_check = next(
        (
            check
            for check in runtime.confirmation_checks
            if bool(check.get("passed", False))
        ),
        None,
    )
    last_check = runtime.confirmation_checks[-1] if runtime.confirmation_checks else None
    rejection_codes = (
        []
        if passed_check is not None or last_check is None
        else list(last_check.get("rejection_codes", []))
    )
    trigger_distance_c5_bps: float | None = None
    if runtime.trigger is not None and float(candidate.five_min_close) > 0:
        if setup.side == "LONG":
            trigger_distance_c5_bps = (
                float(runtime.trigger) / float(candidate.five_min_close) - 1.0
            ) * 10_000.0
        else:
            trigger_distance_c5_bps = (
                1.0 - float(runtime.trigger) / float(candidate.five_min_close)
            ) * 10_000.0
    entry_delay_minutes = (
        int(runtime.entry_minute) - int(runtime.confirmation_minute)
        if runtime.entry_minute is not None
        and runtime.confirmation_minute is not None
        else None
    )
    ema_values = np.asarray(
        [candidate.ema9, candidate.ema20, candidate.ema50], dtype=float
    )
    ema_structure = None
    if np.isfinite(ema_values).all():
        ema_structure = (
            "BULLISH"
            if candidate.ema9 > candidate.ema20 > candidate.ema50
            else "BEARISH"
            if candidate.ema9 < candidate.ema20 < candidate.ema50
            else "MIXED"
        )
    return {
        "candidate_id": (
            f"{candidate.session_date.isoformat()}|{setup.setup_id}|{candidate.symbol}"
        ),
        "session_date": candidate.session_date,
        "signal_time": candidate.signal_ts,
        "signal_end": setup.signal_end,
        "setup_id": setup.setup_id,
        "setup_cap": setup.max_entries,
        "side": setup.side,
        "symbol": candidate.symbol,
        "futures_symbol": candidate.futures_symbol,
        "status": runtime.state.value,
        "reason": runtime.reason,
        "confirmation_minute": runtime.confirmation_minute,
        "confirmation_time": (
            runtime.confirmation_bar.ts if runtime.confirmation_bar else pd.NaT
        ),
        "confirmation_open": (
            passed_check.get("open") if passed_check is not None else None
        ),
        "confirmation_high": (
            passed_check.get("high") if passed_check is not None else None
        ),
        "confirmation_low": (
            passed_check.get("low") if passed_check is not None else None
        ),
        "confirmation_close": (
            passed_check.get("close") if passed_check is not None else None
        ),
        "confirmation_volume": (
            passed_check.get("volume") if passed_check is not None else None
        ),
        "confirmation_range": (
            passed_check.get("candle_range") if passed_check is not None else None
        ),
        "confirmation_body_ratio": (
            passed_check.get("body_ratio") if passed_check is not None else None
        ),
        "confirmation_adverse_wick_ratio": (
            passed_check.get("adverse_wick_ratio")
            if passed_check is not None
            else None
        ),
        "confirmation_close_location": (
            passed_check.get("close_location") if passed_check is not None else None
        ),
        "confirmation_rejection_codes": rejection_codes,
        "confirmation_rejection_reason": "|".join(rejection_codes),
        "confirmation_checks": runtime.confirmation_checks,
        "entry_minute": runtime.entry_minute,
        "entry_delay_minutes": entry_delay_minutes,
        "entry_time": runtime.entry_time or pd.NaT,
        "trigger": runtime.trigger,
        "trigger_distance_c5_bps": trigger_distance_c5_bps,
        "entry_price": runtime.entry_price,
        "gap_fill": runtime.gap_fill,
        "intrabar_trigger_fill": runtime.intrabar_trigger_fill,
        "ambiguous_entry_bar": runtime.ambiguous_entry_bar,
        "stop_price": runtime.stop_price,
        "target_price": runtime.target_price,
        "exit_time": runtime.exit_time or pd.NaT,
        "exit_price": runtime.exit_price,
        "exit_reason": runtime.exit_reason,
        "exit_at_bar_open": runtime.exit_at_bar_open,
        "gross_return_pct": runtime.gross_return_pct,
        "net_return_pct": runtime.net_return_pct,
        "five_min_open": candidate.five_min_open,
        "five_min_high": candidate.five_min_high,
        "five_min_low": candidate.five_min_low,
        "five_min_close": candidate.five_min_close,
        "five_min_volume": candidate.five_min_volume,
        "five_min_range_pct": (
            (candidate.five_min_high - candidate.five_min_low)
            / candidate.five_min_close
            * 100.0
        ),
        "ema9": candidate.ema9,
        "ema20": candidate.ema20,
        "ema50": candidate.ema50,
        "ema_structure": ema_structure,
        "price_change_pct": candidate.price_change_pct,
        "oi": candidate.oi,
        "prev_oi": candidate.prev_oi,
        "oi_change_pct": candidate.oi_change_pct,
        "volume_ratio": candidate.volume_ratio,
        "traded_value": candidate.traded_value,
        "tick_size": candidate.tick_size,
        "event_count": len(runtime.events),
        "events": runtime.events,
        "schema_version": TRADE_SCHEMA_VERSION,
    }


def five_minute_candidate_passes(
    setup: V8Setup,
    candidate: CandidateInput,
) -> bool:
    """Validate the frozen setup-specific five-minute authority fields.

    EMA-side eligibility is computed by the independent candidate builder;
    this boundary validates every value carried into the state-machine seam.
    """

    prices = np.asarray(
        [
            candidate.five_min_open,
            candidate.five_min_high,
            candidate.five_min_low,
            candidate.five_min_close,
        ],
        dtype=float,
    )
    metrics = np.asarray(
        [
            candidate.price_change_pct,
            candidate.oi_change_pct,
            candidate.volume_ratio,
            candidate.traded_value,
            candidate.tick_size,
        ],
        dtype=float,
    )
    if not np.isfinite(prices).all() or not (prices > 0).all():
        return False
    if not np.isfinite(metrics).all():
        return False
    if float(candidate.five_min_high) < max(
        float(candidate.five_min_open), float(candidate.five_min_close)
    ):
        return False
    if float(candidate.five_min_low) > min(
        float(candidate.five_min_open), float(candidate.five_min_close)
    ):
        return False
    if float(candidate.five_min_high) < float(candidate.five_min_low):
        return False
    if (
        float(candidate.tick_size) <= 0
        or float(candidate.oi_change_pct) + 1e-12 < float(setup.oi_change_pct)
        or float(candidate.volume_ratio) + 1e-12 < float(setup.volume_ratio)
        or float(candidate.traded_value) + 1e-12 < float(setup.min_traded_value)
    ):
        return False
    if setup.side == "LONG":
        return float(candidate.price_change_pct) + 1e-12 >= float(
            setup.price_change_pct
        )
    return float(candidate.price_change_pct) - 1e-12 <= -float(
        setup.price_change_pct
    )


def simulate_setup_window(
    setup: V8Setup,
    candidates: Sequence[CandidateInput],
    bars_by_symbol: Mapping[str, Sequence[MinuteBar]],
    policy: EntryPolicy,
) -> pd.DataFrame:
    """Simulate one day/slot/side with deterministic asynchronous cap use.

    The function is pure with respect to disk and returns one terminal audit
    record per candidate.  It is the public state-machine seam used by tests
    and by the historical runner.
    """

    policy.validate()
    if not candidates:
        return pd.DataFrame()
    signal_times = {candidate.signal_ts for candidate in candidates}
    if len(signal_times) != 1:
        raise ValueError("simulate_setup_window requires one signal timestamp")
    if any(
        candidate.signal_ts.strftime("%H:%M") != setup.signal_end
        for candidate in candidates
    ):
        raise ValueError("candidate signal_time does not match setup.signal_end")
    if len({candidate.symbol for candidate in candidates}) != len(candidates):
        raise ValueError("duplicate candidate symbol in setup occurrence")
    rejected_authority = [
        candidate.symbol
        for candidate in candidates
        if not five_minute_candidate_passes(setup, candidate)
    ]
    if rejected_authority:
        raise ValueError(
            "simulate_setup_window received candidates that fail the frozen "
            f"five-minute setup authority: {sorted(rejected_authority)}"
        )

    runtimes = [_CandidateRuntime(candidate=candidate) for candidate in candidates]
    runtime_by_symbol = {runtime.candidate.symbol: runtime for runtime in runtimes}
    maps: dict[str, dict[pd.Timestamp, MinuteBar]] = {}
    signal_ts = next(iter(signal_times))

    required_times = [
        signal_ts + pd.Timedelta(minutes=index)
        for index in range(1, policy.entry_expiry_minute + 1)
    ]
    if policy.square_off:
        configured_cutoff = pd.Timestamp(
            f"{next(iter(signal_times)).date().isoformat()} {policy.square_off}",
            tz=common.IST,
        )
        if configured_cutoff <= required_times[-1]:
            raise ValueError(
                "square_off must be later than this setup's S+5 entry-window bar"
            )
    for runtime in runtimes:
        symbol = runtime.candidate.symbol
        maps[symbol] = _bar_map(runtime.candidate, bars_by_symbol.get(symbol, ()))

    filled_cap = 0
    allocated_once = 0
    pending_symbols: set[str] = set()

    for minute_index, ts in enumerate(required_times, start=1):
        # Validate only the bar that has now completed.  A missing future bar
        # cannot erase a trade that already reached a terminal state.
        for runtime in _rank_candidates(setup, runtimes):
            if runtime.state not in {
                SignalState.MONITORING,
                SignalState.CONFIRMED_WAITING_CAP,
                SignalState.PENDING_STOP,
                SignalState.FILLED_OPEN,
            }:
                continue
            bar = maps[runtime.candidate.symbol].get(ts)
            if bar is not None and _valid_bar(bar):
                continue
            pending_symbols.discard(runtime.candidate.symbol)
            runtime.transition(
                SignalState.DATA_INCOMPLETE,
                event_ts=ts,
                reason=(
                    "MISSING_ENTRY_WINDOW_BAR"
                    if bar is None
                    else "INVALID_ENTRY_WINDOW_BAR"
                ),
            )

        # 1) Existing pending orders may fill on this bar.  Orders placed after
        # this same completed candle are not eligible until the next minute.
        for runtime in _rank_candidates(setup, runtimes):
            if runtime.state != SignalState.PENDING_STOP:
                continue
            if runtime.order_placed_at is None or ts <= runtime.order_placed_at:
                continue
            bar = maps[runtime.candidate.symbol][ts]
            fill = _entry_fill(setup, runtime, bar, policy)
            if fill is None:
                continue
            entry_price, gap_fill = fill
            runtime.entry_minute = minute_index
            runtime.entry_time = ts
            runtime.entry_price = entry_price
            runtime.gap_fill = gap_fill
            runtime.intrabar_trigger_fill = not gap_fill
            runtime.stop_price, runtime.target_price = _brackets(
                setup, runtime.candidate, entry_price
            )
            pending_symbols.discard(runtime.candidate.symbol)
            filled_cap += 1
            runtime.transition(
                SignalState.FILLED_OPEN,
                event_ts=ts,
                reason="GAP_FILL" if gap_fill else "TRIGGER_TOUCH_FILL",
            )
            immediate_exit = _exit_on_bar(
                setup,
                runtime,
                bar,
                position_open_at_bar_start=False,
            )
            if immediate_exit is not None:
                runtime.ambiguous_entry_bar = True
                reason, price = immediate_exit
                _close_runtime(
                    setup,
                    runtime,
                    exit_time=ts,
                    exit_price=price,
                    exit_reason=reason,
                    policy=policy,
                )

        # 2) Positions already open before this bar resolve their brackets.
        for runtime in _rank_candidates(setup, runtimes):
            if runtime.state != SignalState.FILLED_OPEN:
                continue
            if runtime.entry_time is not None and runtime.entry_time == ts:
                continue
            bar = maps[runtime.candidate.symbol].get(ts)
            if bar is None:
                continue
            exit_event = _exit_on_bar(setup, runtime, bar)
            if exit_event is None:
                continue
            runtime.exit_at_bar_open = _exit_occurs_at_bar_open(
                setup, runtime, bar
            )
            reason, price = exit_event
            _close_runtime(
                setup,
                runtime,
                exit_time=ts,
                exit_price=price,
                exit_reason=reason,
                policy=policy,
            )

        # 3) A close-based cancellation is known only after fill processing.
        for runtime in _rank_candidates(setup, runtimes):
            if runtime.state not in {
                SignalState.PENDING_STOP,
                SignalState.CONFIRMED_WAITING_CAP,
            }:
                continue
            bar = maps[runtime.candidate.symbol][ts]
            if policy.post_confirmation_cancel and _postconfirmation_invalidated(
                setup, runtime.candidate, bar
            ):
                pending_symbols.discard(runtime.candidate.symbol)
                runtime.transition(
                    SignalState.POSTCONF_CANCELLED,
                    event_ts=ts,
                    reason="CLOSE_REVERSED_THROUGH_SIGNAL_CLOSE",
                )

        # 4) Monitoring candidates invalidate first, then latch the first
        # strict confirmation.  Confirmation is allowed only through S+4.
        for runtime in _rank_candidates(setup, runtimes):
            if runtime.state != SignalState.MONITORING:
                continue
            bar = maps[runtime.candidate.symbol][ts]
            confirmation_check: dict[str, Any] | None = None
            if minute_index <= policy.max_confirmation_minute:
                confirmation_check = _confirmation_check(
                    setup, runtime.candidate, bar, policy
                )
                confirmation_check["minute_index"] = minute_index
                confirmation_check["gate_evaluated"] = True
            if policy.midpoint_invalidation and _preconfirmation_invalidated(
                setup, runtime.candidate, bar
            ):
                if confirmation_check is not None:
                    confirmation_check["gate_evaluated"] = False
                    confirmation_check["passed"] = False
                    confirmation_check["rejection_codes"] = [
                        "PRECONF_MIDPOINT_INVALIDATED"
                    ]
                    runtime.confirmation_checks.append(confirmation_check)
                runtime.transition(
                    SignalState.PRECONF_INVALIDATED,
                    event_ts=ts,
                    reason="CLOSE_CROSSED_FIVE_MINUTE_MIDPOINT",
                )
                continue
            if confirmation_check is not None:
                runtime.confirmation_checks.append(confirmation_check)
            if confirmation_check is not None and bool(
                confirmation_check["passed"]
            ):
                runtime.confirmation_minute = minute_index
                runtime.confirmation_bar = bar
                runtime.trigger = build_trigger(
                    setup,
                    bar,
                    policy,
                    tick_size=runtime.candidate.tick_size,
                )
                runtime.transition(
                    SignalState.CONFIRMED_WAITING_CAP,
                    event_ts=ts,
                    reason="FIRST_STRICT_CONFIRMATION",
                )

        if minute_index == policy.max_confirmation_minute:
            for runtime in _rank_candidates(setup, runtimes):
                if runtime.state == SignalState.MONITORING:
                    runtime.transition(
                        SignalState.NO_CONFIRMATION,
                        event_ts=ts,
                        reason="CONFIRMATION_WINDOW_EXPIRED",
                    )

        # 5) Recompute capacity after cancellation.  Do not allocate after the
        # expiry candle because no later eligible entry bar remains.
        if minute_index < policy.entry_expiry_minute:
            reserved = len(pending_symbols)
            capacity_used = (
                filled_cap + reserved
                if policy.allow_cap_reassignment
                else allocated_once
            )
            available = max(0, setup.max_entries - capacity_used)
            waiting = _rank_candidates(
                setup,
                (
                    runtime
                    for runtime in runtimes
                    if runtime.state == SignalState.CONFIRMED_WAITING_CAP
                ),
            )
            for runtime in waiting[:available]:
                runtime.order_placed_at = ts
                pending_symbols.add(runtime.candidate.symbol)
                allocated_once += 1
                runtime.transition(
                    SignalState.PENDING_STOP,
                    event_ts=ts,
                    reason="CAP_RESERVED_BY_FROZEN_RANK_AMONG_CONFIRMED",
                )

        # 6) At S+5 existing triggers were processed; now expire all remaining
        # non-filled states.  There is deliberately no new allocation here.
        if minute_index == policy.entry_expiry_minute:
            for runtime in _rank_candidates(setup, runtimes):
                if runtime.state in {
                    SignalState.PENDING_STOP,
                    SignalState.CONFIRMED_WAITING_CAP,
                    SignalState.MONITORING,
                }:
                    pending_symbols.discard(runtime.candidate.symbol)
                    reason = (
                        "NO_STRICT_CONFIRMATION"
                        if runtime.state == SignalState.MONITORING
                        else "ENTRY_WINDOW_EXPIRED"
                    )
                    state = (
                        SignalState.NO_CONFIRMATION
                        if runtime.state == SignalState.MONITORING
                        else SignalState.WINDOW_EXPIRED
                    )
                    runtime.transition(state, event_ts=ts, reason=reason)

    # Resolve still-open positions through an exact, consecutive same-session
    # path after S+5.  Never inspect or trade beyond the configured cutoff.
    for runtime in _rank_candidates(setup, runtimes):
        if runtime.state != SignalState.FILLED_OPEN:
            continue
        mapped = maps[runtime.candidate.symbol]
        regular_close = pd.Timestamp(
            f"{runtime.candidate.session_date.isoformat()} 15:30",
            tz=common.IST,
        )
        requested_cutoff = (
            pd.Timestamp(
                f"{runtime.candidate.session_date.isoformat()} {policy.square_off}",
                tz=common.IST,
            )
            if policy.square_off
            else regular_close
        )
        cutoff = min(requested_cutoff, regular_close)
        start = required_times[-1] + pd.Timedelta(minutes=1)
        if policy.eod_policy == "LAST_REAL_BAR_SENSITIVITY":
            available = [
                ts for ts in mapped if start <= ts <= cutoff and _valid_bar(mapped[ts])
            ]
            if not available:
                runtime.transition(
                    SignalState.DATA_INCOMPLETE,
                    event_ts=start,
                    reason="NO_POST_WINDOW_REAL_BAR",
                )
                continue
            cutoff = max(available)

        terminal_bar: MinuteBar | None = None
        for expected_ts in pd.date_range(start=start, end=cutoff, freq="1min"):
            bar = mapped.get(expected_ts)
            if bar is None or not _valid_bar(bar):
                runtime.transition(
                    SignalState.DATA_INCOMPLETE,
                    event_ts=expected_ts,
                    reason=(
                        "MISSING_POST_ENTRY_PATH_BAR"
                        if bar is None
                        else "INVALID_POST_ENTRY_PATH_BAR"
                    ),
                )
                break
            terminal_bar = bar
            exit_event = _exit_on_bar(setup, runtime, bar)
            if exit_event is None:
                continue
            runtime.exit_at_bar_open = _exit_occurs_at_bar_open(
                setup, runtime, bar
            )
            reason, price = exit_event
            _close_runtime(
                setup,
                runtime,
                exit_time=bar.ts,
                exit_price=price,
                exit_reason=reason,
                policy=policy,
            )
            break
        if runtime.state != SignalState.FILLED_OPEN:
            continue
        if terminal_bar is None or terminal_bar.ts != cutoff:
            runtime.transition(
                SignalState.DATA_INCOMPLETE,
                event_ts=cutoff,
                reason="MISSING_TERMINAL_SQUARE_OFF_BAR",
            )
            continue
        _close_runtime(
            setup,
            runtime,
            exit_time=terminal_bar.ts,
            exit_price=float(terminal_bar.close),
            exit_reason=(
                "SQUARE_OFF"
                if policy.eod_policy == "EXACT_SQUARE_OFF"
                else "LAST_REAL_BAR_SENSITIVITY"
            ),
            policy=policy,
        )

    return pd.DataFrame([_audit_record(setup, runtime) for runtime in runtimes])


def _setup_payload() -> list[dict[str, Any]]:
    return [asdict(setup) for setup in ACTIVE_SETUPS]


def _module_source_sha256() -> str:
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def validate_configuration() -> None:
    if _module_source_sha256() != MODULE_IMPORT_SOURCE_SHA256:
        raise RuntimeError("V8 source file changed after this process imported it")
    if len(ACTIVE_SETUPS) != 10:
        raise AssertionError("V8 must contain ten literal setup legs")
    if len({setup.setup_id for setup in ACTIVE_SETUPS}) != 10:
        raise AssertionError("V8 setup IDs must be unique")
    expected = {
        "09:25": "09:25",
        "09:30": "09:30",
        "09:35": "09:35",
        "09:40": "09:40",
        "09:45": "09:45",
    }
    for setup in ACTIVE_SETUPS:
        if setup.signal_end not in expected:
            raise AssertionError(f"Unexpected V8 signal time: {setup.signal_end}")
        if setup.side not in {"LONG", "SHORT"}:
            raise AssertionError(f"Unexpected V8 side: {setup.side}")
        if setup.max_entries <= 0:
            raise AssertionError("V8 setup cap must be positive")
    observed_hash = common.canonical_json_sha256(_setup_payload())
    if observed_hash != V8_SETUP_BOOK_SHA256:
        raise AssertionError(
            f"V8 setup book hash changed: expected {V8_SETUP_BOOK_SHA256}, "
            f"observed {observed_hash}"
        )
    if SOURCE_V6_SETUP_BOOK_SHA256 != (
        "3c3e59187768afbc015024b5735d1c1b62d91128e8d6888ccfaa6f1c6c15694a"
    ):
        raise AssertionError("V8 copied-setup lineage hash changed")
    observed_calendar_hash = common.canonical_json_sha256(nse_fo_calendar_payload())
    if observed_calendar_hash != NSE_FO_CALENDAR_SHA256:
        raise AssertionError(
            f"V8 NSE F&O calendar hash changed: expected {NSE_FO_CALENDAR_SHA256}, "
            f"observed {observed_calendar_hash}"
        )
    source = Path(__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    observed_imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            observed_imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            observed_imports.add(node.module)
    forbidden = sorted(
        name
        for name in observed_imports
        if name.startswith(FORBIDDEN_IMPORT_PREFIXES)
    )
    if forbidden:
        raise AssertionError(f"V8 imports forbidden strategy modules: {forbidden}")


EXECUTION_INSTRUMENT = "NSE_CASH_EQUITY"
OI_INSTRUMENT = "STATIC_26AUG_NFO_FUTURE_RESEARCH_ONLY"
FIVE_MINUTE_CONSTRUCTION = "FIVE_EXACT_REAL_END_LABELLED_NSE_1M_BARS"
TIMESTAMP_CONVENTION = "CANDLE_END_ASIA_KOLKATA"
PORTFOLIO_MODE = (
    "GLOBAL_PENDING_MARGIN_AND_DUPLICATE_RESERVATION_"
    "CONSERVATIVE_NO_BACKFILL_V1"
)


def _parse_day(value: date | str | pd.Timestamp) -> date:
    return pd.Timestamp(value).date()


def nse_fo_calendar_payload() -> dict[str, Any]:
    """Return the literal exchange-calendar contract used by V8."""

    return {
        "schema_version": NSE_FO_CALENDAR_SCHEMA_VERSION,
        "source": NSE_FO_CALENDAR_SOURCE,
        "source_sha256": NSE_FO_CALENDAR_SOURCE_SHA256,
        "circular": NSE_FO_CALENDAR_CIRCULAR,
        "amendments": [dict(value) for value in NSE_FO_CALENDAR_AMENDMENTS],
        "cash_execution_segment_sources": [
            dict(value) for value in NSE_CASH_CALENDAR_SOURCES
        ],
        "regular_special_session_sources": [
            dict(value) for value in NSE_REGULAR_SPECIAL_SESSION_SOURCES
        ],
        "segment_alignment": "NSE_CASH_AND_FO_REGULAR_TRADING_HOLIDAYS_MATCH",
        "calendar_year": 2026,
        "regular_session_open": "09:15",
        "regular_session_first_end_label": "09:16",
        "regular_session_close": "15:30",
        "timezone": "Asia/Kolkata",
        "trading_holidays": list(NSE_FO_TRADING_HOLIDAYS_2026),
        "nonstandard_sessions_excluded": list(
            NSE_FO_NONSTANDARD_SESSIONS_EXCLUDED
        ),
        "regular_special_sessions_included": list(
            NSE_REGULAR_SPECIAL_SESSIONS_INCLUDED
        ),
        "session_rule": (
            "MONDAY_TO_FRIDAY_EXCLUDING_TRADING_HOLIDAYS_PLUS_"
            "EXPLICIT_FULL_REGULAR_SPECIAL_SESSIONS"
        ),
    }


def expected_regular_session_dates(
    from_day: date | str | pd.Timestamp,
    through_day: date | str | pd.Timestamp,
) -> list[date]:
    """Build expected regular sessions from the frozen NSE F&O calendar.

    Expected dates are independent of source-bar availability.  This prevents
    a whole-market data blackout from being mistaken for a holiday or flat day.
    """

    start_day = _parse_day(from_day)
    end_day = _parse_day(through_day)
    if end_day < start_day:
        raise ValueError("through_day cannot precede from_day")
    if start_day.year != 2026 or end_day.year != 2026:
        raise ValueError(
            "V8's frozen NSE F&O session calendar covers calendar year 2026 only"
        )
    holidays = {date.fromisoformat(value) for value in NSE_FO_TRADING_HOLIDAYS_2026}
    excluded_nonstandard = {
        date.fromisoformat(value) for value in NSE_FO_NONSTANDARD_SESSIONS_EXCLUDED
    }
    included_special = {
        date.fromisoformat(value) for value in NSE_REGULAR_SPECIAL_SESSIONS_INCLUDED
    }
    sessions = [
        stamp.date()
        for stamp in pd.date_range(start_day, end_day, freq="D")
        if (
            stamp.date() in included_special
            or (stamp.weekday() < 5 and stamp.date() not in holidays)
        )
        and stamp.date() not in excluded_nonstandard
    ]
    if not sessions:
        raise ValueError(
            "Requested V8 window contains no expected regular NSE F&O session"
        )
    return sessions


def _series_to_ist(values: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(values, errors="coerce")
    try:
        timezone = parsed.dt.tz
    except AttributeError as exc:
        raise ValueError("Timestamp column has mixed or invalid timezone values") from exc
    if timezone is None:
        return parsed.dt.tz_localize(common.IST)
    return parsed.dt.tz_convert(common.IST)


def _flag_series(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(False, index=frame.index, dtype=bool)
    values = frame[column]
    return (
        pd.to_numeric(values, errors="coerce").fillna(0).ne(0)
        | values.astype(str)
        .str.strip()
        .str.lower()
        .isin({"true", "yes", "on"})
    )


def _read_available_columns(path: Path, requested: Sequence[str]) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"V8 source file is missing: {path}")
    available = set(pq.read_schema(path).names)
    columns = [column for column in requested if column in available]
    if not columns:
        raise ValueError(f"V8 source has none of the requested columns: {path}")
    return pd.read_parquet(path, columns=columns, engine="pyarrow")


def load_validated_source_contract(
    source_snapshot_path: Path | str,
    *,
    symbols: Iterable[str] | None = None,
) -> tuple[
    pd.DataFrame,
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[tuple[str, str], Path],
]:
    """Load a physically frozen source set and bind it to the literal universe."""

    if source_snapshot_path is None:
        raise ValueError("V8 requires an explicit --source-snapshot manifest")
    snapshot = provenance.load_source_snapshot(source_snapshot_path)
    mapped, universe_record = provenance.load_backtest_universe(
        universe_path=snapshot["universe_path"],
        universe_date=BACKTEST_UNIVERSE_DATE,
        contract_month_contains="26AUG",
        require_persisted_mapping=True,
        expected_file_sha256=BACKTEST_UNIVERSE_HASHES["file_sha256"],
        expected_universe_sha256=BACKTEST_UNIVERSE_HASHES["universe_sha256"],
        expected_mapped_universe_sha256=BACKTEST_UNIVERSE_HASHES[
            "mapped_universe_sha256"
        ],
        expected_mapped_symbol_set_sha256=BACKTEST_UNIVERSE_HASHES[
            "mapped_symbol_set_sha256"
        ],
    )
    snapshot, inventory = provenance.validate_source_snapshot(
        snapshot,
        mapped,
        universe_record,
        require_complete_sources=True,
    )
    # Snapshot validation in the shared helper may reuse hashes on unchanged
    # size/mtime.  V8's trust boundary re-hashes every frozen artifact so
    # same-length tampering with restored metadata cannot pass.
    for entry in inventory.get("entries", []):
        if not bool(entry.get("exists")):
            raise FileNotFoundError(
                f"V8 frozen source is missing: {entry.get('resolved_path', '')}"
            )
        source_path = Path(str(entry.get("resolved_path", ""))).resolve()
        before = source_path.stat()
        observed_sha256 = provenance.sha256_file(source_path)
        after = source_path.stat()
        if (before.st_size, before.st_mtime_ns) != (
            after.st_size,
            after.st_mtime_ns,
        ):
            raise RuntimeError(f"V8 frozen source changed while hashing: {source_path}")
        if observed_sha256 != str(entry.get("sha256", "")):
            raise AssertionError(f"V8 frozen source hash changed: {source_path}")
    lookup: dict[tuple[str, str], Path] = {}
    for entry in inventory.get("entries", []):
        if not bool(entry.get("exists")):
            continue
        identity = (
            str(entry.get("role", "")).upper().strip(),
            str(entry.get("logical_symbol", "")).upper().strip(),
        )
        resolved = Path(str(entry.get("resolved_path", ""))).resolve()
        if identity in lookup:
            raise ValueError(f"Duplicate source inventory identity: {identity}")
        lookup[identity] = resolved

    requested = (
        {str(symbol).upper().strip() for symbol in symbols if str(symbol).strip()}
        if symbols is not None
        else None
    )
    if requested is not None:
        mapped = mapped.loc[
            mapped["equity_symbol"].astype(str).str.upper().isin(requested)
        ].copy()
        observed = set(mapped["equity_symbol"].astype(str).str.upper())
        missing = sorted(requested - observed)
        if missing:
            raise ValueError(f"Requested symbols are not in the frozen universe: {missing}")
    if mapped.empty:
        raise ValueError("V8 mapped universe selection is empty")
    return mapped.reset_index(drop=True), universe_record, snapshot, inventory, lookup


def load_equity_minute_history(path: Path, *, symbol: str) -> pd.DataFrame:
    requested = [
        "date",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "gap_filled",
        "opening_snapshot",
        "provisional_stale",
    ]
    frame = _read_available_columns(path, requested)
    timestamp_column = "date" if "date" in frame.columns else "timestamp"
    required = {timestamp_column, "open", "high", "low", "close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Equity 1m source is missing columns {sorted(missing)}: {path}")
    out = pd.DataFrame(
        {
            "ts": _series_to_ist(frame[timestamp_column]),
            "open": pd.to_numeric(frame["open"], errors="coerce"),
            "high": pd.to_numeric(frame["high"], errors="coerce"),
            "low": pd.to_numeric(frame["low"], errors="coerce"),
            "close": pd.to_numeric(frame["close"], errors="coerce"),
            "volume": pd.to_numeric(frame["volume"], errors="coerce"),
            "gap_filled": _flag_series(frame, "gap_filled"),
            "opening_snapshot": _flag_series(frame, "opening_snapshot"),
            "provisional_stale": _flag_series(frame, "provisional_stale"),
        }
    )
    out["symbol"] = str(symbol).upper().strip()
    out["legacy_lineage_flags_absent"] = not any(
        column in frame.columns
        for column in ("gap_filled", "opening_snapshot", "provisional_stale")
    )
    if out["ts"].isna().any():
        raise ValueError(f"Equity 1m source has invalid timestamps: {path}")
    out = out.sort_values("ts", kind="stable").reset_index(drop=True)
    if out["ts"].duplicated().any():
        duplicate = out.loc[out["ts"].duplicated(), "ts"].iloc[0]
        raise ValueError(f"Equity 1m source has duplicate timestamp {duplicate}: {path}")
    return out


def load_futures_five_minute_history(
    path: Path,
    *,
    symbol: str,
    expected_instrument_token: int | None = None,
    expected_expiry: date | str | pd.Timestamp | None = None,
    expected_contract_month: str | None = None,
) -> pd.DataFrame:
    frame = _read_available_columns(
        path,
        [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "oi",
            "quality_state",
            "tradingsymbol",
            "instrument_token",
            "expiry",
            "contract_month",
        ],
    )
    required = {
        "timestamp",
        "oi",
        "quality_state",
        "tradingsymbol",
        "instrument_token",
        "expiry",
        "contract_month",
    }
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Futures 5m source is missing columns {sorted(missing)}: {path}")
    requested_symbol = str(symbol).upper().strip()
    observed_symbols = set(frame["tradingsymbol"].astype(str).str.upper().str.strip())
    if observed_symbols != {requested_symbol}:
        raise ValueError(
            f"Futures source identity mismatch for {requested_symbol}: "
            f"tradingsymbols={sorted(observed_symbols)}"
        )
    token_values = pd.to_numeric(frame["instrument_token"], errors="coerce")
    observed_tokens = set(token_values.dropna().astype(int))
    if token_values.isna().any() or not np.equal(
        token_values.to_numpy(dtype=float),
        np.floor(token_values.to_numpy(dtype=float)),
    ).all() or len(observed_tokens) != 1 or (
        expected_instrument_token is not None
        and observed_tokens != {int(expected_instrument_token)}
    ):
        raise ValueError(
            f"Futures source token mismatch for {requested_symbol}: "
            f"tokens={sorted(observed_tokens)}"
        )
    expiry_values = pd.to_datetime(frame["expiry"], errors="coerce")
    observed_expiries = set(expiry_values.dropna().dt.date)
    if expiry_values.isna().any() or len(observed_expiries) != 1 or (
        expected_expiry is not None
        and observed_expiries != {_parse_day(expected_expiry)}
    ):
        raise ValueError(
            f"Futures source expiry mismatch for {requested_symbol}: "
            f"expiries={sorted(str(value) for value in observed_expiries)}"
        )
    month_values = frame["contract_month"].astype(str).str.strip()
    observed_months = set(month_values)
    if month_values.eq("").any() or len(observed_months) != 1 or (
        expected_contract_month is not None
        and observed_months != {str(expected_contract_month).strip()}
    ):
        raise ValueError(
            f"Futures source contract-month mismatch for {requested_symbol}: "
            f"months={sorted(observed_months)}"
        )
    out = pd.DataFrame(
        {
            "ts": _series_to_ist(frame["timestamp"]),
            "oi": pd.to_numeric(frame["oi"], errors="coerce"),
            "quality_state": frame["quality_state"].astype(str).str.upper().str.strip(),
        }
    )
    out["futures_symbol"] = requested_symbol
    if out["ts"].isna().any():
        raise ValueError(f"Futures 5m source has invalid timestamps: {path}")
    out = out.sort_values("ts", kind="stable").reset_index(drop=True)
    if out["ts"].duplicated().any():
        duplicate = out.loc[out["ts"].duplicated(), "ts"].iloc[0]
        raise ValueError(f"Futures 5m source has duplicate timestamp {duplicate}: {path}")
    out["oi_valid"] = (
        out["quality_state"].eq("VALID")
        & out["oi"].gt(0)
        & np.isfinite(out["oi"])
    )
    # OI change is defined against the exact preceding five-minute timestamp,
    # never the previous physical row.  This prevents an off-grid/duplicate
    # source record from contaminating a signal slot.
    previous = out[["ts", "oi", "oi_valid"]].copy()
    previous["ts"] = previous["ts"] + pd.Timedelta(minutes=5)
    previous = previous.rename(
        columns={"oi": "prev_oi", "oi_valid": "prev_oi_valid"}
    )
    out = out.merge(previous, on="ts", how="left", validate="one_to_one")
    previous_valid = out["prev_oi_valid"].eq(True)
    valid_pair = (
        out["oi_valid"]
        & previous_valid
        & out["prev_oi"].gt(0)
        & np.isfinite(out["prev_oi"])
    )
    out["oi_change_pct"] = np.where(
        valid_pair,
        (out["oi"] / out["prev_oi"] - 1.0) * 100.0,
        np.nan,
    )
    return out.drop(columns=["prev_oi_valid"])


def _valid_minute_rows(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(False, index=frame.index, dtype=bool)
    prices = frame[["open", "high", "low", "close"]].to_numpy(dtype=float)
    return pd.Series(
        np.isfinite(prices).all(axis=1)
        & (prices > 0).all(axis=1)
        & frame["high"].ge(frame[["open", "close"]].max(axis=1)).to_numpy(bool)
        & frame["low"].le(frame[["open", "close"]].min(axis=1)).to_numpy(bool)
        & frame["high"].ge(frame["low"]).to_numpy(bool)
        & np.isfinite(frame["volume"])
        & frame["volume"].ge(0).to_numpy(bool)
        & ~frame["gap_filled"].to_numpy(bool)
        & ~frame["opening_snapshot"].to_numpy(bool)
        & ~frame["provisional_stale"].to_numpy(bool),
        index=frame.index,
        dtype=bool,
    )


def _exact_minute_end_labels(values: pd.Series) -> pd.Series:
    return (
        values.notna()
        & values.dt.second.eq(0)
        & values.dt.microsecond.eq(0)
        & values.dt.nanosecond.eq(0)
    )


def aggregate_equity_one_minute_to_five_minute(frame: pd.DataFrame) -> pd.DataFrame:
    """Aggregate only exact groups of five valid, real end-labelled rows."""

    if frame.empty:
        return pd.DataFrame()
    minute = frame.sort_values("ts", kind="stable").copy()
    ts = minute["ts"]
    session_open = ts.dt.normalize() + pd.Timedelta(hours=9, minutes=15)
    offset = (ts - session_open).dt.total_seconds().div(60.0)
    valid = (
        offset.between(1, 375)
        & _exact_minute_end_labels(ts)
        & _valid_minute_rows(minute)
    )
    minute = minute.loc[valid].copy()
    if minute.empty:
        return pd.DataFrame()
    ts = minute["ts"]
    session_open = ts.dt.normalize() + pd.Timedelta(hours=9, minutes=15)
    offset = (ts - session_open).dt.total_seconds().div(60.0)
    slot_number = ((offset - 1) // 5 + 1).astype(int)
    minute["slot_end"] = session_open + pd.to_timedelta(slot_number * 5, unit="m")
    grouped = (
        minute.groupby("slot_end", sort=True, as_index=False)
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            source_1m_count=("ts", "size"),
            source_1m_first=("ts", "first"),
            source_1m_last=("ts", "last"),
        )
        .sort_values("slot_end", kind="stable")
        .reset_index(drop=True)
    )
    exact = (
        grouped["source_1m_count"].eq(5)
        & grouped["source_1m_first"].eq(
            grouped["slot_end"] - pd.Timedelta(minutes=4)
        )
        & grouped["source_1m_last"].eq(grouped["slot_end"])
    )
    grouped = grouped.loc[exact].copy()
    grouped["ts"] = grouped.pop("slot_end")
    return grouped.drop(columns=["source_1m_first", "source_1m_last"])


def add_five_minute_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values("ts", kind="stable").reset_index(drop=True).copy()
    for span in (9, 20, 50):
        out[f"ema{span}"] = out["close"].ewm(span=span, adjust=False).mean()
    out["prev_close"] = out["close"].shift(1)
    out["price_change_pct"] = (out["close"] / out["prev_close"] - 1.0) * 100.0
    prior_volume = out["volume"].shift(1).rolling(20, min_periods=5).mean()
    out["volume_ratio"] = out["volume"].div(prior_volume.where(prior_volume.gt(0)))
    out["traded_value"] = out["close"] * out["volume"]
    return out


def join_cash_features_with_futures_oi(
    equity_five: pd.DataFrame,
    futures_five: pd.DataFrame,
) -> pd.DataFrame:
    if equity_five.empty or futures_five.empty:
        return pd.DataFrame()
    equity = add_five_minute_features(equity_five)
    oi = futures_five.loc[
        futures_five["oi_valid"],
        ["ts", "oi", "prev_oi", "oi_change_pct", "quality_state"],
    ].copy()
    merged = equity.merge(oi, on="ts", how="inner", validate="one_to_one")
    merged["data_contract"] = (
        "NSE_CASH_1M_TO_5M_PRICE_VOLUME_PLUS_STATIC_NFO_5M_OI_V1"
    )
    return merged.sort_values("ts", kind="stable").reset_index(drop=True)


def _candidate_identifier(session_day: date, setup_id: str, symbol: str) -> str:
    return f"{session_day.isoformat()}|{setup_id}|{str(symbol).upper().strip()}"


def _setup_eligible_rows(joined: pd.DataFrame, setup: V8Setup) -> pd.DataFrame:
    if joined.empty:
        return joined.copy()
    rows = joined.loc[joined["ts"].dt.strftime("%H:%M").eq(setup.signal_end)].copy()
    if rows.empty:
        return rows
    bull = rows["ema9"].gt(rows["ema20"]) & rows["ema20"].gt(rows["ema50"])
    bear = rows["ema9"].lt(rows["ema20"]) & rows["ema20"].lt(rows["ema50"])
    oi_base = rows["oi"].gt(rows["prev_oi"]) & rows["oi_change_pct"].ge(
        BASE_OI_CHANGE_PCT
    )
    volume_base = rows["volume_ratio"].ge(BASE_VOLUME_RATIO)
    if setup.side == "LONG":
        broad = (
            bull
            & oi_base
            & volume_base
            & rows["price_change_pct"].ge(BASE_PRICE_CHANGE_PCT)
        )
        price_ok = rows["price_change_pct"].ge(setup.price_change_pct)
    else:
        broad = (
            bear
            & oi_base
            & volume_base
            & rows["price_change_pct"].le(-BASE_PRICE_CHANGE_PCT)
        )
        price_ok = rows["price_change_pct"].le(-setup.price_change_pct)
    eligible = (
        broad
        & price_ok
        & rows["oi_change_pct"].ge(setup.oi_change_pct)
        & rows["volume_ratio"].ge(setup.volume_ratio)
        & rows["traded_value"].ge(setup.min_traded_value)
    )
    return rows.loc[eligible].copy()


def _empty_candidate_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "candidate_id",
            "session_date",
            "signal_time",
            "signal_end",
            "setup_id",
            "side",
            "symbol",
            "futures_symbol",
            "equity_instrument_token",
            "futures_instrument_token",
            "tick_size",
            "lot_size",
            "five_min_open",
            "five_min_high",
            "five_min_low",
            "five_min_close",
            "five_min_volume",
            "ema9",
            "ema20",
            "ema50",
            "price_change_pct",
            "oi",
            "prev_oi",
            "oi_change_pct",
            "volume_ratio",
            "traded_value",
            "picker",
            "picker_value",
            "frozen_rank",
            "schema_version",
        ]
    )


def _empty_path_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "candidate_id",
            "session_date",
            "signal_time",
            "setup_id",
            "side",
            "symbol",
            "bar_ts",
            "minute_index",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "gap_filled",
            "opening_snapshot",
            "provisional_stale",
            "legacy_lineage_flags_absent",
            "path_policy_version",
        ]
    )


def build_v8_candidate_tables(
    mapped_universe: pd.DataFrame,
    source_lookup: Mapping[tuple[str, str], Path],
    *,
    from_day: date | str,
    through_day: date | str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build timestamped candidate/path tables without legacy strategy code."""

    start_day = _parse_day(from_day)
    end_day = _parse_day(through_day)
    if end_day < start_day:
        raise ValueError("through_day cannot precede from_day")
    expected_session_days = expected_regular_session_dates(start_day, end_day)
    expected_session_day_set = set(expected_session_days)
    full_calendar_session_day_set = set(
        expected_regular_session_dates("2026-01-01", "2026-12-31")
    )
    excluded_nonstandard_day_set = {
        date.fromisoformat(value)
        for value in NSE_FO_NONSTANDARD_SESSIONS_EXCLUDED
        if start_day <= date.fromisoformat(value) <= end_day
    }
    expected_session_dates = [value.isoformat() for value in expected_session_days]
    candidate_records: list[dict[str, Any]] = []
    path_parts: list[pd.DataFrame] = []
    coverage_records: list[dict[str, Any]] = []

    for contract in mapped_universe.to_dict("records"):
        symbol = str(contract["equity_symbol"]).upper().strip()
        futures_symbol = str(contract["futures_tradingsymbol"]).upper().strip()
        equity_key = ("NSE_EQUITY_1M", symbol)
        futures_key = ("NFO_FUTURES_5M", futures_symbol)
        if equity_key not in source_lookup or futures_key not in source_lookup:
            raise FileNotFoundError(
                f"Frozen source lookup is incomplete for {symbol}/{futures_symbol}"
            )
        minute = load_equity_minute_history(source_lookup[equity_key], symbol=symbol)
        futures = load_futures_five_minute_history(
            source_lookup[futures_key],
            symbol=futures_symbol,
            expected_instrument_token=int(contract["futures_instrument_token"]),
            expected_expiry=contract["expiry"],
            expected_contract_month=str(contract["contract_month"]),
        )
        equity_five = aggregate_equity_one_minute_to_five_minute(minute)
        # Feature state must not consume weekend/holiday/vendor-artifact rows.
        # The explicit 2026-02-01 full Union Budget session remains included.
        equity_feature_input = (
            equity_five.loc[
                equity_five["ts"].dt.date.isin(full_calendar_session_day_set)
            ].copy()
            if not equity_five.empty
            else equity_five.copy()
        )
        futures_feature_input = futures.loc[
            futures["ts"].dt.date.isin(full_calendar_session_day_set)
        ].copy()
        joined = join_cash_features_with_futures_oi(
            equity_feature_input, futures_feature_input
        )

        minute_day = minute["ts"].dt.date
        equity_five_day = equity_five["ts"].dt.date if not equity_five.empty else pd.Series(dtype=object)
        futures_day = futures["ts"].dt.date
        joined_day = joined["ts"].dt.date if not joined.empty else pd.Series(dtype=object)
        window_mask_minute = minute_day.between(start_day, end_day)
        window_mask_futures = futures_day.between(start_day, end_day)
        date_mask_minute = window_mask_minute & minute_day.isin(
            expected_session_day_set
        )
        date_mask_equity5 = (
            equity_five_day.between(start_day, end_day)
            & equity_five_day.isin(expected_session_day_set)
            if not equity_five.empty
            else pd.Series(False, index=equity_five.index)
        )
        date_mask_futures = window_mask_futures & futures_day.isin(
            expected_session_day_set
        )
        date_mask_joined = (
            joined_day.between(start_day, end_day)
            & joined_day.isin(expected_session_day_set)
            if not joined.empty
            else pd.Series(False, index=joined.index)
        )
        exact_squareoff_days = set(
            minute.loc[
                date_mask_minute
                & minute["ts"].dt.strftime("%H:%M").eq("15:30")
                & _exact_minute_end_labels(minute["ts"])
                & _valid_minute_rows(minute),
                "ts",
            ].dt.date
        )
        observed_session_days = sorted(
            set(minute.loc[window_mask_minute, "ts"].dt.date)
        )
        all_observed_days = set(observed_session_days) | set(
            futures.loc[window_mask_futures, "ts"].dt.date
        )
        ignored_nonstandard_days = sorted(
            all_observed_days & excluded_nonstandard_day_set
        )
        unexpected_session_days = sorted(
            all_observed_days
            - expected_session_day_set
            - excluded_nonstandard_day_set
        )
        futures_by_ts = {
            _to_ist_timestamp(row["ts"]): row
            for row in futures.loc[date_mask_futures].to_dict("records")
        }
        source_complete_days: list[date] = []
        for session_day in expected_session_days:
            expected_minutes = pd.date_range(
                start=pd.Timestamp(
                    f"{session_day.isoformat()} 09:16", tz=common.IST
                ),
                end=pd.Timestamp(
                    f"{session_day.isoformat()} 15:30", tz=common.IST
                ),
                freq="1min",
            )
            minute_start = expected_minutes[0]
            minute_end = expected_minutes[-1]
            day_minute_rows = minute.loc[
                date_mask_minute
                & minute["ts"].dt.date.eq(session_day)
                & minute["ts"].between(minute_start, minute_end)
            ]
            actual_minute_timestamps = set(day_minute_rows["ts"])
            minute_complete = (
                len(day_minute_rows) == len(expected_minutes)
                and actual_minute_timestamps == set(expected_minutes)
                and bool(_valid_minute_rows(day_minute_rows).all())
            )
            required_futures_times = [
                pd.Timestamp(
                    f"{session_day.isoformat()} {clock}", tz=common.IST
                )
                for clock in ("09:20", "09:25", "09:30", "09:35", "09:40", "09:45")
            ]
            futures_window_rows = futures.loc[
                date_mask_futures
                & futures["ts"].dt.date.eq(session_day)
                & futures["ts"].between(
                    required_futures_times[0], required_futures_times[-1]
                )
            ]
            futures_complete = (
                len(futures_window_rows) == len(required_futures_times)
                and set(futures_window_rows["ts"]) == set(required_futures_times)
            )
            for index, timestamp in enumerate(required_futures_times):
                row = futures_by_ts.get(timestamp)
                if row is None or not bool(row["oi_valid"]):
                    futures_complete = False
                    break
                if index > 0 and not math.isfinite(float(row["oi_change_pct"])):
                    futures_complete = False
                    break
            if minute_complete and futures_complete:
                source_complete_days.append(session_day)
        coverage_records.append(
            {
                "symbol": symbol,
                "futures_symbol": futures_symbol,
                "equity_1m_rows": int(date_mask_minute.sum()),
                "equity_5m_rows": int(date_mask_equity5.sum()),
                "futures_5m_rows": int(date_mask_futures.sum()),
                "joined_5m_rows": int(date_mask_joined.sum()),
                "exact_1530_session_count": int(len(exact_squareoff_days)),
                "session_dates_json": json.dumps(
                    expected_session_dates,
                    separators=(",", ":"),
                ),
                "observed_session_dates_json": json.dumps(
                    [value.isoformat() for value in observed_session_days],
                    separators=(",", ":"),
                ),
                "unexpected_session_dates_json": json.dumps(
                    [value.isoformat() for value in unexpected_session_days],
                    separators=(",", ":"),
                ),
                "ignored_nonstandard_session_dates_json": json.dumps(
                    [value.isoformat() for value in ignored_nonstandard_days],
                    separators=(",", ":"),
                ),
                "source_complete_session_dates_json": json.dumps(
                    [value.isoformat() for value in source_complete_days],
                    separators=(",", ":"),
                ),
                "legacy_lineage_flags_absent": bool(
                    minute["legacy_lineage_flags_absent"].all()
                ),
            }
        )
        if joined.empty:
            continue
        joined_window = joined.loc[date_mask_joined].copy()
        symbol_candidate_records: list[dict[str, Any]] = []
        for setup in ACTIVE_SETUPS:
            rows = _setup_eligible_rows(joined_window, setup)
            for row in rows.to_dict("records"):
                signal_ts = _to_ist_timestamp(row["ts"])
                session_day = signal_ts.date()
                candidate_id = _candidate_identifier(session_day, setup.setup_id, symbol)
                candidate = CandidateInput(
                    symbol=symbol,
                    signal_time=signal_ts,
                    five_min_open=float(row["open"]),
                    five_min_high=float(row["high"]),
                    five_min_low=float(row["low"]),
                    five_min_close=float(row["close"]),
                    price_change_pct=float(row["price_change_pct"]),
                    oi_change_pct=float(row["oi_change_pct"]),
                    volume_ratio=float(row["volume_ratio"]),
                    traded_value=float(row["traded_value"]),
                    tick_size=float(contract["equity_tick_size"]),
                    futures_symbol=futures_symbol,
                    futures_instrument_token=int(contract["futures_instrument_token"]),
                    equity_instrument_token=int(contract["equity_instrument_token"]),
                    lot_size=1,
                )
                if not five_minute_candidate_passes(setup, candidate):
                    raise AssertionError(
                        f"Builder admitted a candidate outside setup authority: {candidate_id}"
                    )
                record = {
                    "candidate_id": candidate_id,
                    "session_date": session_day,
                    "signal_time": signal_ts,
                    "signal_end": setup.signal_end,
                    "setup_id": setup.setup_id,
                    "side": setup.side,
                    "symbol": symbol,
                    "futures_symbol": futures_symbol,
                    "equity_instrument_token": candidate.equity_instrument_token,
                    "futures_instrument_token": candidate.futures_instrument_token,
                    "tick_size": candidate.tick_size,
                    "lot_size": candidate.lot_size,
                    "five_min_open": candidate.five_min_open,
                    "five_min_high": candidate.five_min_high,
                    "five_min_low": candidate.five_min_low,
                    "five_min_close": candidate.five_min_close,
                    "five_min_volume": float(row["volume"]),
                    "ema9": float(row["ema9"]),
                    "ema20": float(row["ema20"]),
                    "ema50": float(row["ema50"]),
                    "price_change_pct": candidate.price_change_pct,
                    "oi": float(row["oi"]),
                    "prev_oi": float(row["prev_oi"]),
                    "oi_change_pct": candidate.oi_change_pct,
                    "volume_ratio": candidate.volume_ratio,
                    "traded_value": candidate.traded_value,
                    "picker": setup.picker,
                    "picker_value": _picker_value(setup, candidate),
                    "schema_version": CACHE_SCHEMA_VERSION,
                }
                candidate_records.append(record)
                symbol_candidate_records.append(record)

        for record in symbol_candidate_records:
            signal_ts = _to_ist_timestamp(record["signal_time"])
            path = minute.loc[
                minute["ts"].dt.date.eq(signal_ts.date())
                & minute["ts"].gt(signal_ts)
                & _exact_minute_end_labels(minute["ts"])
                & minute["ts"].le(
                    pd.Timestamp(
                        f"{signal_ts.date().isoformat()} 15:30", tz=common.IST
                    )
                )
            ].copy()
            if path.empty:
                continue
            path.insert(0, "candidate_id", record["candidate_id"])
            path.insert(1, "session_date", signal_ts.date())
            path.insert(2, "signal_time", signal_ts)
            path.insert(3, "setup_id", record["setup_id"])
            path.insert(4, "side", record["side"])
            path["minute_index"] = (
                (path["ts"] - signal_ts).dt.total_seconds().div(60).astype(int)
            )
            path = path.rename(columns={"ts": "bar_ts"})
            path["path_policy_version"] = PATH_POLICY_VERSION
            path_parts.append(path[_empty_path_frame().columns])

    candidates = (
        pd.DataFrame(candidate_records)
        if candidate_records
        else _empty_candidate_frame()
    )
    if not candidates.empty:
        candidates = candidates.sort_values(
            [
                "session_date",
                "setup_id",
                "picker_value",
                "traded_value",
                "symbol",
            ],
            ascending=[True, True, False, False, True],
            kind="stable",
        ).reset_index(drop=True)
        candidates["frozen_rank"] = (
            candidates.groupby(["session_date", "setup_id"], sort=False)
            .cumcount()
            .add(1)
        )
        candidates = candidates[_empty_candidate_frame().columns]
    paths = (
        pd.concat(path_parts, ignore_index=True)
        if path_parts
        else _empty_path_frame()
    )
    if not paths.empty:
        paths = paths.sort_values(
            ["candidate_id", "bar_ts"], kind="stable"
        ).reset_index(drop=True)
        if paths.duplicated(["candidate_id", "bar_ts"]).any():
            raise AssertionError("V8 candidate path contains duplicate timestamps")
        if not paths["bar_ts"].dt.date.eq(paths["session_date"]).all():
            raise AssertionError("V8 candidate path crossed a session date")
        if paths["bar_ts"].dt.strftime("%H:%M").gt("15:30").any():
            raise AssertionError("V8 candidate path extends beyond 15:30")
    coverage = pd.DataFrame(coverage_records)
    all_session_dates = expected_session_dates
    if not coverage.empty:
        incomplete_json: list[str] = []
        complete_counts: list[int] = []
        incomplete_counts: list[int] = []
        for encoded in coverage["source_complete_session_dates_json"]:
            complete = set(json.loads(str(encoded)))
            incomplete = sorted(set(all_session_dates) - complete)
            complete_counts.append(len(complete))
            incomplete_counts.append(len(incomplete))
            incomplete_json.append(json.dumps(incomplete, separators=(",", ":")))
        coverage["source_complete_session_count"] = complete_counts
        coverage["source_incomplete_session_count"] = incomplete_counts
        coverage["source_incomplete_session_dates_json"] = incomplete_json
        coverage["unexpected_session_count"] = coverage[
            "unexpected_session_dates_json"
        ].map(lambda value: len(json.loads(str(value))))
    return candidates, paths, coverage


def derive_coverage_completeness(
    coverage: pd.DataFrame,
    *,
    selected_symbols: Sequence[str],
    expected_session_dates: Sequence[date | str | pd.Timestamp],
) -> dict[str, Any]:
    """Recompute headline source readiness from the coverage artifact itself."""

    symbols = sorted({str(value).upper().strip() for value in selected_symbols})
    expected = sorted({_parse_day(value).isoformat() for value in expected_session_dates})
    if not symbols:
        raise ValueError("V8 coverage validation requires selected symbols")
    if not expected:
        raise ValueError("V8 coverage validation requires expected exchange sessions")
    required_columns = {
        "symbol",
        "session_dates_json",
        "source_complete_session_dates_json",
        "source_incomplete_session_dates_json",
        "source_complete_session_count",
        "source_incomplete_session_count",
        "unexpected_session_dates_json",
        "unexpected_session_count",
    }
    missing_columns = sorted(required_columns - set(coverage.columns))
    if missing_columns:
        raise ValueError(f"V8 coverage artifact is missing columns: {missing_columns}")
    if len(coverage) != len(symbols) or coverage["symbol"].duplicated().any():
        raise ValueError("V8 coverage must contain exactly one row per selected symbol")
    observed_symbols = sorted(coverage["symbol"].astype(str).str.upper().str.strip())
    if observed_symbols != symbols:
        raise ValueError("V8 coverage symbol set does not match the cache contract")

    expected_set = set(expected)
    complete_symbol_sessions = 0
    incomplete_symbol_sessions = 0
    unexpected_source_symbol_sessions = 0
    for row in coverage.to_dict("records"):
        row_sessions = list(json.loads(str(row["session_dates_json"])))
        complete = list(json.loads(str(row["source_complete_session_dates_json"])))
        incomplete = list(
            json.loads(str(row["source_incomplete_session_dates_json"]))
        )
        unexpected = list(json.loads(str(row["unexpected_session_dates_json"])))
        if row_sessions != expected:
            raise ValueError("V8 coverage row has the wrong expected-session list")
        if len(set(complete)) != len(complete) or not set(complete).issubset(
            expected_set
        ):
            raise ValueError("V8 coverage row has invalid complete-session dates")
        derived_incomplete = sorted(expected_set - set(complete))
        if sorted(incomplete) != derived_incomplete:
            raise ValueError("V8 coverage incomplete-session partition is invalid")
        if int(row["source_complete_session_count"]) != len(complete):
            raise ValueError("V8 coverage complete-session count is invalid")
        if int(row["source_incomplete_session_count"]) != len(incomplete):
            raise ValueError("V8 coverage incomplete-session count is invalid")
        if len(set(unexpected)) != len(unexpected):
            raise ValueError("V8 coverage unexpected-session list has duplicates")
        if int(row["unexpected_session_count"]) != len(unexpected):
            raise ValueError("V8 coverage unexpected-session count is invalid")
        complete_symbol_sessions += len(complete)
        incomplete_symbol_sessions += len(incomplete)
        unexpected_source_symbol_sessions += len(unexpected)

    expected_symbol_sessions = len(symbols) * len(expected)
    headline_source_complete = (
        complete_symbol_sessions == expected_symbol_sessions
        and incomplete_symbol_sessions == 0
        and unexpected_source_symbol_sessions == 0
    )
    return {
        "coverage_symbol_count": len(symbols),
        "expected_symbol_sessions": expected_symbol_sessions,
        "complete_symbol_sessions": complete_symbol_sessions,
        "source_incomplete_symbol_sessions": incomplete_symbol_sessions,
        "unexpected_source_symbol_sessions": unexpected_source_symbol_sessions,
        "headline_source_complete": headline_source_complete,
    }


def _manifest_completeness_matches(
    manifest: Mapping[str, Any], derived: Mapping[str, Any]
) -> bool:
    fields = (
        "coverage_symbol_count",
        "expected_symbol_sessions",
        "complete_symbol_sessions",
        "source_incomplete_symbol_sessions",
        "unexpected_source_symbol_sessions",
    )
    return all(int(manifest.get(field, -1)) == int(derived[field]) for field in fields) and (
        bool(manifest.get("headline_source_complete"))
        == bool(derived["headline_source_complete"])
    )


def _cache_contract_payload(
    *,
    snapshot: Mapping[str, Any],
    inventory: Mapping[str, Any],
    universe_record: Mapping[str, Any],
    symbols: Sequence[str],
    from_day: date,
    through_day: date,
) -> dict[str, Any]:
    expected_sessions = expected_regular_session_dates(from_day, through_day)
    return {
        "schema_version": CACHE_SCHEMA_VERSION,
        "path_policy_version": PATH_POLICY_VERSION,
        "strategy_version": STRATEGY_VERSION,
        "strategy_code_sha256": _module_source_sha256(),
        "setup_book_sha256": V8_SETUP_BOOK_SHA256,
        "source_v6_setup_book_sha256": SOURCE_V6_SETUP_BOOK_SHA256,
        "execution_instrument": EXECUTION_INSTRUMENT,
        "oi_instrument": OI_INSTRUMENT,
        "five_minute_construction": FIVE_MINUTE_CONSTRUCTION,
        "timestamp_convention": TIMESTAMP_CONVENTION,
        "universe": dict(universe_record),
        "snapshot_fingerprint": snapshot.get("snapshot_fingerprint", ""),
        "source_inventory_sha256": inventory.get("inventory_sha256", ""),
        "source_fingerprint": inventory.get("source_fingerprint", ""),
        "from_day": from_day.isoformat(),
        "through_day": through_day.isoformat(),
        "session_calendar": {
            "calendar_sha256": NSE_FO_CALENDAR_SHA256,
            "calendar": nse_fo_calendar_payload(),
            "expected_session_dates": [
                value.isoformat() for value in expected_sessions
            ],
            "expected_session_count": len(expected_sessions),
        },
        "symbols": list(symbols),
        "source_limitations": [
            "STATIC_2026_08_11_UNIVERSE_SURVIVORSHIP_RESEARCH",
            "STATIC_26AUG_FUTURES_OI_NOT_POINT_IN_TIME_ROLLING",
            "LEGACY_EQUITY_1M_HAS_NO_ROW_LINEAGE_FLAGS",
            "SOURCE_SNAPSHOT_IS_PER_FILE_STABLE_NOT_GLOBAL_TRANSACTION",
        ],
    }


def _cache_paths(input_fingerprint: str) -> dict[str, Path]:
    root = CACHE_DIR / input_fingerprint[:16]
    return {
        "root": root,
        "manifest": root / "manifest.json",
        "candidates": root / "five_minute_candidates.parquet",
        "paths": root / "same_session_minute_paths.parquet",
        "coverage": root / "coverage.parquet",
    }


def load_or_build_v8_cache(
    *,
    source_snapshot_path: Path | str,
    from_day: date | str,
    through_day: date | str,
    symbols: Iterable[str] | None = None,
    rebuild: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any], Path]:
    validate_configuration()
    start_day = _parse_day(from_day)
    end_day = _parse_day(through_day)
    mapped, universe_record, snapshot, inventory, source_lookup = (
        load_validated_source_contract(source_snapshot_path, symbols=symbols)
    )
    selected_symbols = sorted(mapped["equity_symbol"].astype(str).str.upper().tolist())
    contract = _cache_contract_payload(
        snapshot=snapshot,
        inventory=inventory,
        universe_record=universe_record,
        symbols=selected_symbols,
        from_day=start_day,
        through_day=end_day,
    )
    input_fingerprint = common.canonical_json_sha256(contract)
    paths = _cache_paths(input_fingerprint)
    manifest_path = paths["manifest"]
    if not rebuild and manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError) as exc:
            raise ValueError(f"V8 cache manifest is unreadable: {manifest_path}") from exc
        artifacts = manifest.get("artifacts", {})
        valid = (
            manifest.get("schema_version") == CACHE_SCHEMA_VERSION
            and bool(manifest.get("complete"))
            and manifest.get("input_fingerprint") == input_fingerprint
            and common.canonical_json_sha256(manifest.get("input_contract", {}))
            == common.canonical_json_sha256(contract)
            and common.canonical_json_sha256(manifest.get("universe", {}))
            == common.canonical_json_sha256(universe_record)
            and common.canonical_json_sha256(manifest.get("session_calendar", {}))
            == common.canonical_json_sha256(contract.get("session_calendar", {}))
            and list(manifest.get("session_dates", []))
            == list(
                dict(contract.get("session_calendar", {})).get(
                    "expected_session_dates", []
                )
            )
            and str(
                dict(manifest.get("source_inventory", {})).get(
                    "inventory_sha256", ""
                )
            )
            == str(inventory.get("inventory_sha256", ""))
            and str(
                dict(manifest.get("source_inventory", {})).get(
                    "source_fingerprint", ""
                )
            )
            == str(inventory.get("source_fingerprint", ""))
            and all(
                provenance.artifact_matches(paths[name], artifacts.get(name, {}))
                for name in ("candidates", "paths", "coverage")
            )
        )
        if valid:
            cached_candidates = pd.read_parquet(paths["candidates"])
            cached_paths = pd.read_parquet(paths["paths"])
            cached_coverage = pd.read_parquet(paths["coverage"])
            try:
                derived = derive_coverage_completeness(
                    cached_coverage,
                    selected_symbols=selected_symbols,
                    expected_session_dates=dict(
                        contract.get("session_calendar", {})
                    ).get("expected_session_dates", []),
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                valid = False
            else:
                valid = (
                    _manifest_completeness_matches(manifest, derived)
                    and int(manifest.get("candidate_count", -1))
                    == len(cached_candidates)
                    and int(manifest.get("path_row_count", -1))
                    == len(cached_paths)
                )
            if valid:
                return (
                    cached_candidates,
                    cached_paths,
                    cached_coverage,
                    manifest,
                    manifest_path,
                )

    candidates, minute_paths, coverage = build_v8_candidate_tables(
        mapped,
        source_lookup,
        from_day=start_day,
        through_day=end_day,
    )
    if len(coverage) != len(selected_symbols):
        raise AssertionError(
            "V8 coverage does not contain exactly one row per selected symbol"
        )
    if _module_source_sha256() != str(contract["strategy_code_sha256"]):
        raise RuntimeError("V8 source changed during cache construction")
    paths["root"].mkdir(parents=True, exist_ok=True)
    common.atomic_write_parquet(candidates, paths["candidates"])
    common.atomic_write_parquet(minute_paths, paths["paths"])
    common.atomic_write_parquet(coverage, paths["coverage"])
    artifacts = {
        name: provenance.artifact_record(paths[name])
        for name in ("candidates", "paths", "coverage")
    }
    expected_session_dates = list(
        dict(contract.get("session_calendar", {})).get(
            "expected_session_dates", []
        )
    )
    if not expected_session_dates:
        raise AssertionError("V8 cache contract has no expected exchange sessions")
    derived = derive_coverage_completeness(
        coverage,
        selected_symbols=selected_symbols,
        expected_session_dates=expected_session_dates,
    )
    manifest = {
        "schema_version": CACHE_SCHEMA_VERSION,
        "complete": True,
        "created_at_ist": common.now_ist().isoformat(timespec="microseconds"),
        "input_fingerprint": input_fingerprint,
        "input_contract": contract,
        "universe": dict(universe_record),
        "source_snapshot": {
            "manifest_path": snapshot.get("manifest_path", ""),
            "snapshot_fingerprint": snapshot.get("snapshot_fingerprint", ""),
            "capture_scope": snapshot.get("capture_scope", ""),
            "physical_copy": bool(snapshot.get("physical_copy")),
        },
        "source_inventory": dict(inventory),
        "candidate_count": int(len(candidates)),
        "path_row_count": int(len(minute_paths)),
        "session_dates": expected_session_dates,
        "session_calendar": dict(contract["session_calendar"]),
        **derived,
        "artifacts": artifacts,
    }
    if _module_source_sha256() != str(contract["strategy_code_sha256"]):
        raise RuntimeError("V8 source changed before cache manifest publication")
    common.atomic_write_json(manifest_path, manifest)
    return candidates, minute_paths, coverage, manifest, manifest_path


VARIANT_REGISTRY: dict[str, dict[str, Any]] = {
    "B0": {
        "description": "S+1 strict confirmation, raw break, S+5 expiry",
        "max_confirmation_minute": 1,
        "buffer_bps": 0.0,
        "midpoint_invalidation": False,
        "close_location_min": None,
    },
    "B1": {
        "description": "First strict confirmation S+1..S+4, raw break",
        "max_confirmation_minute": 4,
        "buffer_bps": 0.0,
        "midpoint_invalidation": False,
        "close_location_min": None,
    },
    "B2": {
        "description": "B1 plus two-basis-point trigger buffer",
        "max_confirmation_minute": 4,
        "buffer_bps": 2.0,
        "midpoint_invalidation": False,
        "close_location_min": None,
    },
    "B3": {
        "description": "B1 plus five-basis-point trigger buffer",
        "max_confirmation_minute": 4,
        "buffer_bps": 5.0,
        "midpoint_invalidation": False,
        "close_location_min": None,
    },
    "B4": {
        "description": "B2 plus five-minute midpoint pre-confirmation invalidation",
        "max_confirmation_minute": 4,
        "buffer_bps": 2.0,
        "midpoint_invalidation": True,
        "close_location_min": None,
    },
    "B5": {
        "description": "B4 plus 0.75 directional close-location",
        "max_confirmation_minute": 4,
        "buffer_bps": 2.0,
        "midpoint_invalidation": True,
        "close_location_min": 0.75,
    },
}


def entry_policy_for_variant(
    variant: str,
    *,
    cost_bps: float,
    slippage_bps: float,
    square_off: str,
    eod_policy: str,
) -> EntryPolicy:
    key = str(variant).upper().strip()
    if key not in VARIANT_REGISTRY:
        raise ValueError(f"Unknown V8 variant {variant!r}")
    config = VARIANT_REGISTRY[key]
    policy = EntryPolicy(
        buffer_bps=float(config["buffer_bps"]),
        max_confirmation_minute=int(config["max_confirmation_minute"]),
        entry_expiry_minute=5,
        close_location_min=config["close_location_min"],
        cost_bps=float(cost_bps),
        slippage_bps=float(slippage_bps),
        midpoint_invalidation=bool(config["midpoint_invalidation"]),
        post_confirmation_cancel=True,
        allow_cap_reassignment=True,
        same_bar_policy="STOP_FIRST",
        square_off=str(square_off),
        eod_policy=str(eod_policy),
    )
    validate_backtest_policy(policy)
    return policy


def policy_for_setup(setup: V8Setup, base_policy: EntryPolicy) -> EntryPolicy:
    """Apply a leg's optional entry-seam overrides to the run's global policy.

    Legs that override nothing return ``base_policy`` unchanged, so a run that
    uses the frozen book behaves exactly as it did before per-setup overrides
    existed.  Cost, slippage, square-off and EOD policy are never overridable:
    they are run economics, not strategy.
    """

    if not setup.overrides_entry_policy:
        return base_policy
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
    policy = replace(base_policy, **changes)
    validate_backtest_policy(policy)
    return policy


def validate_backtest_policy(policy: EntryPolicy) -> None:
    """Validate policy independently of whether the run has any candidates."""

    policy.validate()
    if not policy.square_off:
        return
    square_off_time = datetime.strptime(policy.square_off, "%H:%M").time()
    latest_window_end = max(
        (
            datetime.combine(date(2000, 1, 1), datetime.strptime(setup.signal_end, "%H:%M").time())
            + pd.Timedelta(minutes=5)
        ).time()
        for setup in ACTIVE_SETUPS
    )
    if square_off_time <= latest_window_end:
        raise ValueError(
            "square_off must be later than the latest V8 S+5 entry-window bar "
            f"({latest_window_end.strftime('%H:%M')})"
        )


def _candidate_from_cache_row(row: Mapping[str, Any]) -> CandidateInput:
    return CandidateInput(
        symbol=str(row["symbol"]),
        signal_time=_to_ist_timestamp(row["signal_time"]),
        five_min_open=float(row["five_min_open"]),
        five_min_high=float(row["five_min_high"]),
        five_min_low=float(row["five_min_low"]),
        five_min_close=float(row["five_min_close"]),
        price_change_pct=float(row["price_change_pct"]),
        oi_change_pct=float(row["oi_change_pct"]),
        volume_ratio=float(row["volume_ratio"]),
        traded_value=float(row["traded_value"]),
        tick_size=float(row["tick_size"]),
        futures_symbol=str(row["futures_symbol"]),
        futures_instrument_token=int(row["futures_instrument_token"]),
        equity_instrument_token=int(row["equity_instrument_token"]),
        lot_size=int(row["lot_size"]),
        five_min_volume=float(row.get("five_min_volume", math.nan)),
        ema9=float(row.get("ema9", math.nan)),
        ema20=float(row.get("ema20", math.nan)),
        ema50=float(row.get("ema50", math.nan)),
        oi=float(row.get("oi", math.nan)),
        prev_oi=float(row.get("prev_oi", math.nan)),
    )


def _minute_bars_from_cache(frame: pd.DataFrame) -> list[MinuteBar]:
    return [
        MinuteBar(
            timestamp=_to_ist_timestamp(row["bar_ts"]),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
            gap_filled=bool(row["gap_filled"]),
            opening_snapshot=bool(row["opening_snapshot"]),
            provisional_stale=bool(row["provisional_stale"]),
        )
        for row in frame.sort_values("bar_ts", kind="stable").to_dict("records")
    ]


_EXCURSION_VALUE_COLUMNS = (
    "mfe_pct_ohlc_lower_bound",
    "mfe_pct_ohlc_upper_bound",
    "mae_pct_ohlc_lower_bound",
    "mae_pct_ohlc_upper_bound",
)
_EXCURSION_AMBIGUITY_COLUMNS = (
    "excursion_entry_bar_ambiguous",
    "excursion_exit_bar_ambiguous",
    "excursion_boundary_ambiguous",
)

_AUDIT_EXPORT_REQUIRED_COLUMNS = (
    "candidate_id",
    "session_date",
    "signal_time",
    "signal_end",
    "setup_id",
    "setup_cap",
    "side",
    "symbol",
    "futures_symbol",
    "status",
    "reason",
    "confirmation_minute",
    "confirmation_time",
    "confirmation_open",
    "confirmation_high",
    "confirmation_low",
    "confirmation_close",
    "confirmation_volume",
    "confirmation_range",
    "confirmation_body_ratio",
    "confirmation_adverse_wick_ratio",
    "confirmation_close_location",
    "confirmation_rejection_codes",
    "confirmation_rejection_reason",
    "confirmation_checks",
    "entry_minute",
    "entry_delay_minutes",
    "entry_time",
    "trigger",
    "trigger_distance_c5_bps",
    "entry_price",
    "gap_fill",
    "intrabar_trigger_fill",
    "ambiguous_entry_bar",
    "stop_price",
    "target_price",
    "exit_time",
    "exit_price",
    "exit_reason",
    "exit_at_bar_open",
    "gross_return_pct",
    "net_return_pct",
    "five_min_open",
    "five_min_high",
    "five_min_low",
    "five_min_close",
    "five_min_volume",
    "five_min_range_pct",
    "ema9",
    "ema20",
    "ema50",
    "ema_structure",
    "price_change_pct",
    "oi",
    "prev_oi",
    "oi_change_pct",
    "volume_ratio",
    "traded_value",
    "tick_size",
    "event_count",
    "events",
    "schema_version",
    "frozen_rank",
    "picker",
    "picker_value",
    "variant",
    "buffer_bps",
    "cost_bps",
    "slippage_bps",
    "eod_policy",
    "filled",
    "quantity",
    "gross_pnl_rs",
    "estimated_cost_rs",
    "net_pnl_rs",
    "position_notional_rs",
    *_EXCURSION_VALUE_COLUMNS,
    *_EXCURSION_AMBIGUITY_COLUMNS,
    "excursion_observed_bar_count",
    "excursion_complete_bar_count",
    "excursion_policy_version",
    "portfolio_mode",
    "portfolio_decision",
    "portfolio_reject_reason",
    "portfolio_active_at_reservation",
    "portfolio_reserved_margin_rs",
)


def _excursion_percentages(
    side: str,
    entry_price: float,
    *,
    observed_high: float,
    observed_low: float,
) -> tuple[float, float]:
    if side == "LONG":
        favorable = observed_high / entry_price - 1.0
        adverse = 1.0 - observed_low / entry_price
    else:
        favorable = 1.0 - observed_low / entry_price
        adverse = observed_high / entry_price - 1.0
    return max(0.0, favorable * 100.0), max(0.0, adverse * 100.0)


def attach_excursion_diagnostics(
    audit: pd.DataFrame,
    minute_paths: pd.DataFrame,
) -> pd.DataFrame:
    """Attach conservative post-fill MFE/MAE OHLC bounds.

    Entry and exit prices are certain observations.  OHLC extremes from bars
    strictly between them are also certain because the position spans each
    whole bar.  Entry/exit-bar extremes are included in the upper bound but
    enter the lower bound only when the position is known to span the whole
    boundary bar (gap-at-open entry or close-based square-off respectively).
    This avoids inventing an intrabar ordering that one-minute OHLC cannot
    prove.
    """

    out = audit.copy()
    for column in _EXCURSION_VALUE_COLUMNS:
        out[column] = np.nan
    for column in _EXCURSION_AMBIGUITY_COLUMNS:
        out[column] = pd.Series(pd.NA, index=out.index, dtype="boolean")
    out["excursion_observed_bar_count"] = np.nan
    out["excursion_complete_bar_count"] = np.nan
    out["excursion_policy_version"] = EXCURSION_POLICY_VERSION
    required_path_columns = {
        "candidate_id",
        "bar_ts",
        "high",
        "low",
    }
    if out.empty or minute_paths.empty or not required_path_columns.issubset(
        minute_paths.columns
    ):
        return out

    path_by_candidate = {
        str(candidate_id): group.copy()
        for candidate_id, group in minute_paths.groupby("candidate_id", sort=False)
    }
    close_based_exit_reasons = {"SQUARE_OFF", "LAST_REAL_BAR_SENSITIVITY"}
    for index, row in out.iterrows():
        if pd.isna(row.get("entry_time")) or pd.isna(row.get("exit_time")):
            continue
        entry_price = float(row.get("entry_price", math.nan))
        exit_price = float(row.get("exit_price", math.nan))
        if not math.isfinite(entry_price) or entry_price <= 0 or not math.isfinite(
            exit_price
        ):
            continue
        entry_ts = _to_ist_timestamp(row["entry_time"])
        exit_ts = _to_ist_timestamp(row["exit_time"])
        if exit_ts < entry_ts:
            raise AssertionError("V8 exit precedes entry while computing excursions")
        path = path_by_candidate.get(str(row["candidate_id"]))
        if path is None or path.empty:
            continue
        path = path.copy()
        path["_bar_ts"] = path["bar_ts"].map(_to_ist_timestamp)
        path["_high"] = pd.to_numeric(path["high"], errors="coerce")
        path["_low"] = pd.to_numeric(path["low"], errors="coerce")
        observed = path.loc[
            path["_bar_ts"].between(entry_ts, exit_ts, inclusive="both")
            & np.isfinite(path["_high"])
            & np.isfinite(path["_low"])
        ].sort_values("_bar_ts", kind="stable")
        if observed.empty:
            continue

        gap_fill_value = row.get("gap_fill", False)
        gap_fill = bool(gap_fill_value) if not pd.isna(gap_fill_value) else False
        close_based_exit = str(row.get("exit_reason", "")) in close_based_exit_reasons
        exit_at_open_value = row.get("exit_at_bar_open", False)
        exit_at_open = (
            bool(exit_at_open_value)
            if not pd.isna(exit_at_open_value)
            else False
        )
        fully_held = observed["_bar_ts"].gt(entry_ts) & observed["_bar_ts"].lt(
            exit_ts
        )
        if entry_ts == exit_ts:
            if gap_fill and close_based_exit:
                fully_held |= observed["_bar_ts"].eq(entry_ts)
        else:
            if gap_fill:
                fully_held |= observed["_bar_ts"].eq(entry_ts)
            if close_based_exit:
                fully_held |= observed["_bar_ts"].eq(exit_ts)

        certain_highs = [entry_price, exit_price]
        certain_lows = [entry_price, exit_price]
        if bool(fully_held.any()):
            certain_highs.extend(observed.loc[fully_held, "_high"].astype(float))
            certain_lows.extend(observed.loc[fully_held, "_low"].astype(float))
        lower_mfe, lower_mae = _excursion_percentages(
            str(row["side"]),
            entry_price,
            observed_high=max(certain_highs),
            observed_low=min(certain_lows),
        )
        # A stop-gap or target-at-open exit occurs before every later extreme
        # in that bar.  Exclude that bar's H/L from even the upper bound.
        upper_observed = observed.loc[
            ~(
                observed["_bar_ts"].eq(exit_ts)
                if exit_at_open
                else pd.Series(False, index=observed.index, dtype=bool)
            )
        ]
        upper_highs = [entry_price, exit_price]
        upper_lows = [entry_price, exit_price]
        if not upper_observed.empty:
            upper_highs.append(float(upper_observed["_high"].max()))
            upper_lows.append(float(upper_observed["_low"].min()))
        upper_mfe, upper_mae = _excursion_percentages(
            str(row["side"]),
            entry_price,
            observed_high=max(upper_highs),
            observed_low=min(upper_lows),
        )
        out.at[index, "mfe_pct_ohlc_lower_bound"] = lower_mfe
        out.at[index, "mfe_pct_ohlc_upper_bound"] = upper_mfe
        out.at[index, "mae_pct_ohlc_lower_bound"] = lower_mae
        out.at[index, "mae_pct_ohlc_upper_bound"] = upper_mae
        entry_ambiguous = not gap_fill
        exit_ambiguous = not close_based_exit and not exit_at_open
        out.at[index, "excursion_entry_bar_ambiguous"] = entry_ambiguous
        out.at[index, "excursion_exit_bar_ambiguous"] = exit_ambiguous
        out.at[index, "excursion_boundary_ambiguous"] = (
            entry_ambiguous or exit_ambiguous
        )
        out.at[index, "excursion_observed_bar_count"] = len(observed)
        out.at[index, "excursion_complete_bar_count"] = int(fully_held.sum())
    return out


def apply_global_portfolio_constraints(
    audit: pd.DataFrame,
    portfolio_policy: PortfolioPolicy,
) -> pd.DataFrame:
    """Apply a chronological pending-margin and duplicate-symbol ledger.

    A global rejection deliberately does not backfill another setup candidate;
    this is conservative, deterministic, and explicitly fingerprinted.  The
    unconstrained leg result remains in diagnostic columns.
    """

    portfolio_policy.validate()
    if not portfolio_policy.pending_reserves_margin:
        raise ValueError("V8 supports only pending_reserves_margin=True")
    if not portfolio_policy.one_position_per_symbol:
        raise ValueError("V8 supports only one_position_per_symbol=True")
    if audit.empty:
        return audit.copy()
    out = audit.copy().reset_index(drop=True)
    out["portfolio_decision"] = "NOT_APPLICABLE"
    out["portfolio_reject_reason"] = ""
    out["portfolio_active_at_reservation"] = np.nan
    out["portfolio_reserved_margin_rs"] = np.nan
    for column in _EXCURSION_AMBIGUITY_COLUMNS:
        if column in out.columns:
            out[column] = out[column].astype("boolean")
    out["unconstrained_status"] = out["status"]
    out["unconstrained_net_return_pct"] = out["net_return_pct"]
    out["unconstrained_net_pnl_rs"] = out.get("net_pnl_rs", np.nan)
    for column in (
        "events",
        "confirmation_minute",
        "confirmation_time",
        "entry_minute",
        "entry_delay_minutes",
        "entry_time",
        "trigger",
        "entry_price",
        "gap_fill",
        "intrabar_trigger_fill",
        "ambiguous_entry_bar",
        "stop_price",
        "target_price",
        "exit_time",
        "exit_price",
        "exit_reason",
        "exit_at_bar_open",
        "gross_return_pct",
        "quantity",
        "position_notional_rs",
        "gross_pnl_rs",
        "estimated_cost_rs",
        *_EXCURSION_VALUE_COLUMNS,
        *_EXCURSION_AMBIGUITY_COLUMNS,
        "excursion_observed_bar_count",
        "excursion_complete_bar_count",
    ):
        if column in out.columns:
            out[f"unconstrained_{column}"] = out[column]

    actions: list[dict[str, Any]] = []
    terminal_states = {
        SignalState.POSTCONF_CANCELLED.value,
        SignalState.WINDOW_EXPIRED.value,
        SignalState.STOPPED.value,
        SignalState.TARGETED.value,
        SignalState.SQUARE_OFF.value,
        SignalState.DATA_INCOMPLETE.value,
    }
    for row in out.to_dict("records"):
        candidate_id = str(row["candidate_id"])
        for sequence, event in enumerate(row.get("events", []) or []):
            before = str(event.get("state_before", ""))
            after = str(event.get("state_after", ""))
            action = None
            phase = 1
            if after == SignalState.PENDING_STOP.value:
                action = "RESERVE"
                phase = 2
            elif (
                before == SignalState.PENDING_STOP.value
                and after in terminal_states
            ) or (
                before == SignalState.FILLED_OPEN.value
                and after in terminal_states
            ):
                action = "RELEASE"
                phase = 0
            if action is None:
                continue
            actions.append(
                {
                    "candidate_id": candidate_id,
                    "event_ts": _to_ist_timestamp(event["event_ts"]),
                    "phase": phase,
                    "sequence": sequence,
                    "action": action,
                    "signal_time": _to_ist_timestamp(row["signal_time"]),
                    "setup_id": str(row["setup_id"]),
                    "frozen_rank": int(row.get("frozen_rank") or 0),
                    "symbol": str(row["symbol"]),
                }
            )
    actions.sort(
        key=lambda item: (
            item["event_ts"],
            item["phase"],
            item["signal_time"],
            item["setup_id"],
            item["frozen_rank"],
            item["symbol"],
            item["candidate_id"],
            item["sequence"],
        )
    )
    accepted: set[str] = set()
    rejected: dict[str, str] = {}
    active_by_symbol: dict[str, str] = {}
    max_by_margin = int(
        math.floor(portfolio_policy.capital_rs / portfolio_policy.margin_per_entry_rs)
    )
    capacity = min(portfolio_policy.max_concurrent_positions, max_by_margin)
    reservation_stats: dict[str, tuple[int, float]] = {}

    for action in actions:
        candidate_id = action["candidate_id"]
        symbol = action["symbol"]
        if action["action"] == "RELEASE":
            if candidate_id in accepted and active_by_symbol.get(symbol) == candidate_id:
                active_by_symbol.pop(symbol, None)
            continue
        if candidate_id in accepted or candidate_id in rejected:
            continue
        if portfolio_policy.one_position_per_symbol and symbol in active_by_symbol:
            rejected[candidate_id] = "DUPLICATE_SYMBOL_PENDING_OR_OPEN"
            continue
        if len(active_by_symbol) >= capacity:
            rejected[candidate_id] = "CAPITAL_MARGIN_OR_CONCURRENCY_LIMIT"
            continue
        accepted.add(candidate_id)
        active_by_symbol[symbol] = candidate_id
        reservation_stats[candidate_id] = (
            len(active_by_symbol),
            len(active_by_symbol) * portfolio_policy.margin_per_entry_rs,
        )

    for index, row in out.iterrows():
        candidate_id = str(row["candidate_id"])
        if candidate_id in accepted:
            active_count, margin = reservation_stats[candidate_id]
            out.at[index, "portfolio_decision"] = "ACCEPTED"
            out.at[index, "portfolio_active_at_reservation"] = active_count
            out.at[index, "portfolio_reserved_margin_rs"] = margin
            continue
        if candidate_id not in rejected:
            continue
        reason = rejected[candidate_id]
        out.at[index, "portfolio_decision"] = "REJECTED"
        out.at[index, "portfolio_reject_reason"] = reason
        out.at[index, "status"] = (
            SignalState.DUPLICATE_REJECTED.value
            if reason.startswith("DUPLICATE")
            else SignalState.PORTFOLIO_REJECTED.value
        )
        terminal_state = str(out.at[index, "status"])
        terminal_reason = f"{reason}:CONSERVATIVE_NO_BACKFILL"
        out.at[index, "reason"] = terminal_reason
        original_events = list(out.at[index, "events"] or [])
        constrained_events: list[dict[str, Any]] = []
        rejection_ts = _to_ist_timestamp(row["signal_time"])
        for event in original_events:
            if str(event.get("state_after", "")) == SignalState.PENDING_STOP.value:
                rejection_ts = _to_ist_timestamp(event["event_ts"])
                constrained_events.append(
                    {
                        "symbol": str(row["symbol"]),
                        "event_ts": rejection_ts,
                        "state_before": SignalState.CONFIRMED_WAITING_CAP.value,
                        "state_after": terminal_state,
                        "reason": terminal_reason,
                    }
                )
                break
            constrained_events.append(event)
        out.at[index, "events"] = constrained_events
        out.at[index, "event_count"] = len(constrained_events)
        out.at[index, "filled"] = False
        for column in (
            "entry_minute",
            "entry_delay_minutes",
            "entry_time",
            "entry_price",
            "gap_fill",
            "intrabar_trigger_fill",
            "ambiguous_entry_bar",
            "stop_price",
            "target_price",
            "exit_time",
            "exit_price",
            "exit_reason",
            "exit_at_bar_open",
            "quantity",
            "position_notional_rs",
            "gross_pnl_rs",
            "estimated_cost_rs",
            "net_pnl_rs",
            "gross_return_pct",
            "net_return_pct",
            *_EXCURSION_VALUE_COLUMNS,
            *_EXCURSION_AMBIGUITY_COLUMNS,
            "excursion_observed_bar_count",
            "excursion_complete_bar_count",
        ):
            if column in out.columns:
                if column in {
                    "gap_fill",
                    "intrabar_trigger_fill",
                    "ambiguous_entry_bar",
                }:
                    out.at[index, column] = False
                elif column == "quantity":
                    out.at[index, column] = 0
                elif column == "exit_reason":
                    out.at[index, column] = ""
                elif column == "exit_at_bar_open":
                    out.at[index, column] = False
                elif column in _EXCURSION_AMBIGUITY_COLUMNS:
                    out.at[index, column] = pd.NA
                else:
                    out.at[index, column] = pd.NaT if column.endswith("time") else np.nan
    return out


def run_v8_backtest(
    candidates: pd.DataFrame,
    minute_paths: pd.DataFrame,
    *,
    variant: str,
    policy: EntryPolicy,
    target_exposure_per_entry_rs: float | None = None,
    portfolio_policy: PortfolioPolicy | None = None,
) -> pd.DataFrame:
    """Replay cached candidates with the independent windowed state machine."""

    validate_backtest_policy(policy)
    if target_exposure_per_entry_rs is not None and (
        not math.isfinite(float(target_exposure_per_entry_rs))
        or target_exposure_per_entry_rs <= 0
    ):
        raise ValueError("target exposure must be finite and positive")
    if portfolio_policy is None:
        effective_portfolio_policy = PortfolioPolicy(
            target_exposure_per_entry_rs=(
                50_000.0
                if target_exposure_per_entry_rs is None
                else float(target_exposure_per_entry_rs)
            )
        )
    else:
        effective_portfolio_policy = portfolio_policy
        if target_exposure_per_entry_rs is not None and not math.isclose(
            float(target_exposure_per_entry_rs),
            float(portfolio_policy.target_exposure_per_entry_rs),
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise ValueError(
                "target exposure argument does not match portfolio policy"
            )
    effective_portfolio_policy.validate()
    effective_target_exposure = float(
        effective_portfolio_policy.target_exposure_per_entry_rs
    )
    if candidates.empty:
        return pd.DataFrame()
    setup_by_id = {setup.setup_id: setup for setup in ACTIVE_SETUPS}
    path_by_candidate = {
        str(candidate_id): group.copy()
        for candidate_id, group in minute_paths.groupby("candidate_id", sort=False)
    }
    audit_parts: list[pd.DataFrame] = []
    grouped = candidates.groupby(["session_date", "setup_id"], sort=True)
    for (_, setup_id), group in grouped:
        setup_key = str(setup_id)
        if setup_key not in setup_by_id:
            raise ValueError(f"Cache contains unknown setup ID: {setup_key}")
        setup = setup_by_id[setup_key]
        candidate_inputs = [
            _candidate_from_cache_row(row) for row in group.to_dict("records")
        ]
        bars_by_symbol: dict[str, list[MinuteBar]] = {}
        for row in group.to_dict("records"):
            candidate_id = str(row["candidate_id"])
            path = path_by_candidate.get(candidate_id, _empty_path_frame())
            bars_by_symbol[str(row["symbol"])] = _minute_bars_from_cache(path)
        leg_policy = policy_for_setup(setup, policy)
        audit = simulate_setup_window(
            setup, candidate_inputs, bars_by_symbol, leg_policy
        )
        if audit.empty:
            continue
        ranks = group[["candidate_id", "frozen_rank", "picker", "picker_value"]]
        audit = audit.merge(ranks, on="candidate_id", how="left", validate="one_to_one")
        audit["variant"] = str(variant).upper()
        audit["buffer_bps"] = leg_policy.buffer_bps
        audit["cost_bps"] = leg_policy.cost_bps
        audit["slippage_bps"] = leg_policy.slippage_bps
        audit["entry_policy_overridden"] = setup.overrides_entry_policy
        audit["max_confirmation_minute"] = leg_policy.max_confirmation_minute
        audit["midpoint_invalidation"] = leg_policy.midpoint_invalidation
        audit["close_location_min"] = leg_policy.close_location_min
        audit["eod_policy"] = policy.eod_policy
        audit_parts.append(audit)
    if not audit_parts:
        return pd.DataFrame()
    audit = pd.concat(audit_parts, ignore_index=True)
    filled = audit["entry_price"].notna()
    audit["filled"] = filled
    audit["quantity"] = 0
    audit.loc[filled, "quantity"] = np.floor(
        effective_target_exposure
        / pd.to_numeric(audit.loc[filled, "entry_price"], errors="coerce")
    ).astype(int)
    audit["gross_pnl_rs"] = np.nan
    audit["estimated_cost_rs"] = np.nan
    audit["net_pnl_rs"] = np.nan
    audit["position_notional_rs"] = np.nan
    audit.loc[filled, "position_notional_rs"] = (
        pd.to_numeric(audit.loc[filled, "entry_price"], errors="coerce")
        * audit.loc[filled, "quantity"].astype(float)
    )
    closed = filled & audit["exit_price"].notna() & audit["net_return_pct"].notna()
    direction = np.where(audit.loc[closed, "side"].eq("LONG"), 1.0, -1.0)
    quantity = audit.loc[closed, "quantity"].astype(float)
    entry = audit.loc[closed, "entry_price"].astype(float)
    exit_price = audit.loc[closed, "exit_price"].astype(float)
    gross_rs = direction * (exit_price - entry) * quantity
    cost_rs = entry * quantity * policy.cost_bps / 10_000.0
    audit.loc[closed, "gross_pnl_rs"] = gross_rs
    audit.loc[closed, "estimated_cost_rs"] = cost_rs
    audit.loc[closed, "net_pnl_rs"] = gross_rs - cost_rs
    audit = attach_excursion_diagnostics(audit, minute_paths)
    audit["portfolio_mode"] = PORTFOLIO_MODE
    constrained = apply_global_portfolio_constraints(
        audit,
        effective_portfolio_policy,
    )
    return constrained.sort_values(
        ["session_date", "signal_time", "side", "frozen_rank", "symbol"],
        kind="stable",
    ).reset_index(drop=True)


def summarize_v8_results(
    audit: pd.DataFrame,
    *,
    session_dates: Iterable[date | str | pd.Timestamp] | None = None,
    split_day: date | str | pd.Timestamp | None = None,
    eod_policy: str = "EXACT_SQUARE_OFF",
    source_complete: bool = True,
    source_incomplete_symbol_sessions: int = 0,
    unexpected_source_symbol_sessions: int = 0,
) -> tuple[dict[str, Any], pd.DataFrame]:
    calendar = sorted(
        {
            _parse_day(value)
            for value in (
                session_dates
                if session_dates is not None
                else ([] if audit.empty else audit["session_date"].tolist())
            )
        }
    )
    accepted_filled = (
        audit["filled"].fillna(False).astype(bool)
        if not audit.empty and "filled" in audit.columns
        else pd.Series(False, index=audit.index, dtype=bool)
    )
    finite_terminal = (
        np.isfinite(pd.to_numeric(audit["net_return_pct"], errors="coerce"))
        & np.isfinite(pd.to_numeric(audit["net_pnl_rs"], errors="coerce"))
        if not audit.empty
        and {"net_return_pct", "net_pnl_rs"}.issubset(audit.columns)
        else pd.Series(False, index=audit.index, dtype=bool)
    )
    completed = (
        audit.loc[accepted_filled & finite_terminal].copy()
        if not audit.empty
        else pd.DataFrame(
            columns=["session_date", "net_return_pct", "net_pnl_rs", "candidate_id"]
        )
    )
    unresolved_filled_trades = int((accepted_filled & ~finite_terminal).sum())
    returns = pd.to_numeric(completed["net_return_pct"], errors="coerce").dropna()
    profits = float(returns.loc[returns.gt(0)].sum())
    losses = float(-returns.loc[returns.lt(0)].sum())
    profit_factor = profits / losses if losses > 0 else (math.inf if profits > 0 else None)
    trade_daily = (
        completed.groupby("session_date", as_index=False)
        .agg(
            net_return_pct=("net_return_pct", "sum"),
            net_pnl_rs=("net_pnl_rs", "sum"),
            fills=("candidate_id", "size"),
        )
    )
    candidate_daily = (
        audit.groupby("session_date", as_index=False)
        .agg(candidates=("candidate_id", "size"))
        if not audit.empty
        else pd.DataFrame(columns=["session_date", "candidates"])
    )
    daily = pd.DataFrame(
        {"session_date": pd.Series(calendar, dtype="object")}
    )
    daily = daily.merge(candidate_daily, on="session_date", how="left")
    daily = daily.merge(trade_daily, on="session_date", how="left")
    for column in ("candidates", "fills"):
        daily[column] = pd.to_numeric(daily[column], errors="coerce").fillna(0).astype(int)
    for column in ("net_return_pct", "net_pnl_rs"):
        daily[column] = (
            pd.to_numeric(daily[column], errors="coerce").fillna(0.0).astype(float)
        )
    daily = daily.sort_values("session_date", kind="stable").reset_index(drop=True)
    split = _parse_day(split_day) if split_day is not None else None
    daily["period"] = (
        np.where(daily["session_date"].lt(split), "TRAIN", "TEST")
        if split is not None
        else "FULL"
    )
    cumulative = np.concatenate(
        ([0.0], daily["net_return_pct"].cumsum().to_numpy(dtype=float))
    )
    drawdown = cumulative - np.maximum.accumulate(cumulative)
    status_counts = {
        str(key): int(value)
        for key, value in (
            audit["status"].value_counts(dropna=False).items()
            if not audit.empty
            else []
        )
    }
    period_metrics: dict[str, dict[str, Any]] = {}
    for period, period_daily in daily.groupby("period", sort=False):
        period_days = set(period_daily["session_date"])
        period_trades = completed.loc[completed["session_date"].isin(period_days)]
        period_returns = pd.to_numeric(
            period_trades["net_return_pct"], errors="coerce"
        ).dropna()
        period_profit = float(period_returns.loc[period_returns.gt(0)].sum())
        period_loss = float(-period_returns.loc[period_returns.lt(0)].sum())
        period_metrics[str(period)] = {
            "sessions": int(len(period_daily)),
            "fills": int(len(period_trades)),
            "net_return_percentage_points": float(period_returns.sum()),
            "profit_factor": (
                period_profit / period_loss
                if period_loss > 0
                else math.inf
                if period_profit > 0
                else None
            ),
            "positive_days": int(period_daily["net_return_pct"].gt(0).sum()),
        }
    incomplete_candidates = (
        int(audit["status"].eq(SignalState.DATA_INCOMPLETE.value).sum())
        if not audit.empty
        else 0
    )
    headline_valid = (
        str(eod_policy) == "EXACT_SQUARE_OFF"
        and incomplete_candidates == 0
        and unresolved_filled_trades == 0
        and bool(source_complete)
        and bool(calendar)
    )
    diagnostic_metrics = {
        "profit_factor": profit_factor,
        "net_return_percentage_points": float(returns.sum()),
        "net_pnl_rs": float(
            pd.to_numeric(completed["net_pnl_rs"], errors="coerce").sum()
        ),
        "max_daily_drawdown_percentage_points": (
            max(0.0, float(-drawdown.min())) if drawdown.size else 0.0
        ),
    }
    promotion_blockers = [
        "STATIC_LATER_DATED_UNIVERSE",
        "STATIC_AUGUST_FUTURES_OI_NOT_ROLLING_POINT_IN_TIME",
        "LEGACY_EQUITY_ROW_LINEAGE_UNPROVEN",
        "GLOBAL_PORTFOLIO_LEDGER_USES_CONSERVATIVE_NO_BACKFILL_OVERLAY",
        "PROSPECTIVE_20_SESSIONS_AND_100_FILLS_NOT_COMPLETED",
    ]
    if incomplete_candidates:
        promotion_blockers.append("DATA_INCOMPLETE_CANDIDATE_PATHS")
    if unresolved_filled_trades:
        promotion_blockers.append("FILLED_TRADES_WITHOUT_FINITE_TERMINAL_ECONOMICS")
    if not source_complete:
        promotion_blockers.append("UPSTREAM_SOURCE_SLOT_COVERAGE_INCOMPLETE")
    if unexpected_source_symbol_sessions:
        promotion_blockers.append("UNEXPECTED_NON_CALENDAR_SOURCE_SESSIONS")
    if not calendar:
        promotion_blockers.append("NO_EXPECTED_EXCHANGE_SESSIONS")
    if str(eod_policy) != "EXACT_SQUARE_OFF":
        promotion_blockers.append("LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE")
    summary = {
        "candidates": int(len(audit)),
        "fills": int(audit["filled"].sum()) if not audit.empty else 0,
        "closed_fills": int(len(completed)),
        "profit_factor": profit_factor if headline_valid else None,
        "net_return_percentage_points": (
            diagnostic_metrics["net_return_percentage_points"]
            if headline_valid
            else None
        ),
        "net_pnl_rs": diagnostic_metrics["net_pnl_rs"] if headline_valid else None,
        "positive_days": int(daily["net_return_pct"].gt(0).sum()),
        "negative_days": int(daily["net_return_pct"].lt(0).sum()),
        "flat_days": int(daily["net_return_pct"].eq(0).sum()),
        "sessions": int(len(daily)),
        "max_daily_drawdown_percentage_points": (
            diagnostic_metrics["max_daily_drawdown_percentage_points"]
            if headline_valid
            else None
        ),
        "headline_valid": headline_valid,
        "data_incomplete_candidates": incomplete_candidates,
        "unresolved_filled_trades": unresolved_filled_trades,
        "source_complete": bool(source_complete),
        "source_incomplete_symbol_sessions": int(source_incomplete_symbol_sessions),
        "unexpected_source_symbol_sessions": int(
            unexpected_source_symbol_sessions
        ),
        "diagnostic_closed_trade_metrics": diagnostic_metrics,
        "status_counts": status_counts,
        "split_day": split.isoformat() if split else None,
        "period_metrics": period_metrics,
        "promotion_eligible": False,
        "promotion_blockers": promotion_blockers,
    }
    return summary, daily


DIAGNOSTIC_BREAKDOWN_DIMENSIONS = (
    "side",
    "setup_id",
    "signal_slot",
    "confirmation_minute",
    "entry_minute",
    "buffer_bps",
    "symbol",
    "five_session_block",
    "gap_fill",
)

_DIAGNOSTIC_BREAKDOWN_COLUMNS = (
    "schema_version",
    "dimension",
    "bucket",
    "bucket_order",
    "bucket_start_date",
    "bucket_end_date",
    "candidates",
    "confirmed",
    "fills",
    "closed_fills",
    "wins",
    "losses",
    "flat_trades",
    "net_return_percentage_points",
    "gross_pnl_rs",
    "estimated_cost_rs",
    "net_pnl_rs",
    "profit_factor",
)


def _diagnostic_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    def numeric(name: str) -> pd.Series:
        if name not in frame.columns:
            return pd.Series(np.nan, index=frame.index, dtype=float)
        return pd.to_numeric(frame[name], errors="coerce")

    filled = (
        frame["filled"].fillna(False).astype(bool)
        if "filled" in frame.columns
        else pd.Series(False, index=frame.index, dtype=bool)
    )
    confirmed = (
        frame["confirmation_minute"].notna()
        if "confirmation_minute" in frame.columns
        else pd.Series(False, index=frame.index, dtype=bool)
    )
    returns = numeric("net_return_pct")
    net_pnl = numeric("net_pnl_rs")
    closed = filled & np.isfinite(returns) & np.isfinite(net_pnl)
    closed_returns = returns.loc[closed]
    profits = float(closed_returns.loc[closed_returns.gt(0)].sum())
    losses = float(-closed_returns.loc[closed_returns.lt(0)].sum())
    return {
        "candidates": int(len(frame)),
        "confirmed": int(confirmed.sum()),
        "fills": int(filled.sum()),
        "closed_fills": int(closed.sum()),
        "wins": int(closed_returns.gt(0).sum()),
        "losses": int(closed_returns.lt(0).sum()),
        "flat_trades": int(closed_returns.eq(0).sum()),
        "net_return_percentage_points": float(closed_returns.sum()),
        "gross_pnl_rs": float(numeric("gross_pnl_rs").loc[closed].sum()),
        "estimated_cost_rs": float(
            numeric("estimated_cost_rs").loc[closed].sum()
        ),
        "net_pnl_rs": float(net_pnl.loc[closed].sum()),
        "profit_factor": (
            profits / losses
            if losses > 0
            else math.inf
            if profits > 0
            else None
        ),
    }


def build_v8_diagnostic_breakdowns(
    audit: pd.DataFrame,
    *,
    session_dates: Iterable[date | str | pd.Timestamp] | None = None,
) -> pd.DataFrame:
    """Build deterministic, constrained-result B0-B5 diagnostic tables.

    The table intentionally excludes the unfrozen B6 previous-ten-bar ratios,
    B7 context, sectors and post-hoc quantile buckets.  Five-session blocks
    use the supplied official session calendar and therefore retain empty
    sessions rather than grouping only dates that happened to trade.
    """

    work = audit.copy()
    for column in (
        "side",
        "setup_id",
        "signal_end",
        "confirmation_minute",
        "entry_minute",
        "buffer_bps",
        "symbol",
        "session_date",
        "filled",
        "gap_fill",
    ):
        if column not in work.columns:
            work[column] = np.nan

    def text_bucket(series: pd.Series) -> pd.Series:
        return series.map(
            lambda value: "MISSING" if pd.isna(value) else str(value)
        )

    def minute_bucket(series: pd.Series, missing: str) -> pd.Series:
        return series.map(
            lambda value: missing
            if pd.isna(value)
            else str(int(float(value)))
        )

    def basis_point_bucket(series: pd.Series) -> pd.Series:
        return series.map(
            lambda value: "MISSING"
            if pd.isna(value)
            else f"{float(value):g}"
        )

    filled = work["filled"].fillna(False).astype(bool)
    gap_fill = work["gap_fill"].fillna(False).astype(bool)
    bucket_series: dict[str, pd.Series] = {
        "side": text_bucket(work["side"]),
        "setup_id": text_bucket(work["setup_id"]),
        "signal_slot": text_bucket(work["signal_end"]),
        "confirmation_minute": minute_bucket(
            work["confirmation_minute"], "NO_CONFIRMATION"
        ),
        "entry_minute": minute_bucket(work["entry_minute"], "NO_ENTRY"),
        "buffer_bps": basis_point_bucket(work["buffer_bps"]),
        "symbol": text_bucket(work["symbol"]),
        "gap_fill": pd.Series(
            np.where(
                ~filled,
                "NO_ENTRY",
                np.where(gap_fill, "GAP_FILL", "NON_GAP_FILL"),
            ),
            index=work.index,
        ),
    }
    records: list[dict[str, Any]] = []
    for dimension_order, dimension in enumerate(
        DIAGNOSTIC_BREAKDOWN_DIMENSIONS
    ):
        if dimension == "five_session_block":
            continue
        labels = bucket_series[dimension]
        buckets = sorted(set(labels.astype(str)))
        for bucket_order, bucket in enumerate(buckets, start=1):
            subset = work.loc[labels.eq(bucket)]
            records.append(
                {
                    "schema_version": DIAGNOSTIC_BREAKDOWN_SCHEMA_VERSION,
                    "dimension": dimension,
                    "dimension_order": dimension_order,
                    "bucket": bucket,
                    "bucket_order": bucket_order,
                    "bucket_start_date": None,
                    "bucket_end_date": None,
                    **_diagnostic_metrics(subset),
                }
            )

    calendar = sorted(
        {
            _parse_day(value)
            for value in (
                session_dates
                if session_dates is not None
                else work["session_date"].dropna().tolist()
            )
        }
    )
    parsed_session_dates = work["session_date"].map(
        lambda value: _parse_day(value) if not pd.isna(value) else None
    )
    mapped_dates: set[date] = set()
    block_dimension_order = DIAGNOSTIC_BREAKDOWN_DIMENSIONS.index(
        "five_session_block"
    )
    for block_number, offset in enumerate(range(0, len(calendar), 5), start=1):
        block_days = calendar[offset : offset + 5]
        mapped_dates.update(block_days)
        bucket = (
            f"B{block_number:03d}:"
            f"{block_days[0].isoformat()}..{block_days[-1].isoformat()}"
        )
        subset = work.loc[parsed_session_dates.isin(block_days)]
        records.append(
            {
                "schema_version": DIAGNOSTIC_BREAKDOWN_SCHEMA_VERSION,
                "dimension": "five_session_block",
                "dimension_order": block_dimension_order,
                "bucket": bucket,
                "bucket_order": block_number,
                "bucket_start_date": block_days[0],
                "bucket_end_date": block_days[-1],
                **_diagnostic_metrics(subset),
            }
        )
    unmapped = work.loc[
        parsed_session_dates.notna() & ~parsed_session_dates.isin(mapped_dates)
    ]
    if not unmapped.empty:
        records.append(
            {
                "schema_version": DIAGNOSTIC_BREAKDOWN_SCHEMA_VERSION,
                "dimension": "five_session_block",
                "dimension_order": block_dimension_order,
                "bucket": "UNMAPPED_SESSION",
                "bucket_order": len(calendar) // 5 + 2,
                "bucket_start_date": None,
                "bucket_end_date": None,
                **_diagnostic_metrics(unmapped),
            }
        )
    if not records:
        return pd.DataFrame(columns=_DIAGNOSTIC_BREAKDOWN_COLUMNS)
    result = pd.DataFrame(records).sort_values(
        ["dimension_order", "bucket_order", "bucket"], kind="stable"
    )
    return result.loc[:, _DIAGNOSTIC_BREAKDOWN_COLUMNS].reset_index(drop=True)


def _normalized_diagnostic_breakdowns(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize a breakdown table for semantic provenance comparison."""

    missing = sorted(set(_DIAGNOSTIC_BREAKDOWN_COLUMNS) - set(frame.columns))
    if missing:
        raise ValueError(f"V8 diagnostic breakdown is missing columns: {missing}")
    out = frame.loc[:, _DIAGNOSTIC_BREAKDOWN_COLUMNS].copy()
    for column in ("schema_version", "dimension", "bucket"):
        out[column] = out[column].map(
            lambda value: "" if pd.isna(value) else str(value)
        )
    for column in ("bucket_start_date", "bucket_end_date"):
        out[column] = out[column].map(
            lambda value: "" if pd.isna(value) else _parse_day(value).isoformat()
        )
    for column in (
        "bucket_order",
        "candidates",
        "confirmed",
        "fills",
        "closed_fills",
        "wins",
        "losses",
        "flat_trades",
    ):
        out[column] = pd.to_numeric(out[column], errors="raise").astype(int)
    for column in (
        "net_return_percentage_points",
        "gross_pnl_rs",
        "estimated_cost_rs",
        "net_pnl_rs",
        "profit_factor",
    ):
        out[column] = pd.to_numeric(out[column], errors="coerce").astype(float)
    return out.sort_values(
        ["dimension", "bucket_order", "bucket"], kind="stable"
    ).reset_index(drop=True)


def strategy_payload() -> dict[str, Any]:
    return {
        "strategy_version": STRATEGY_VERSION,
        "objective": OBJECTIVE,
        "configuration_source": CONFIG_SOURCE,
        "setup_book_sha256": V8_SETUP_BOOK_SHA256,
        "source_v6_setup_book_sha256": SOURCE_V6_SETUP_BOOK_SHA256,
        "setups": _setup_payload(),
        "variant_registry": VARIANT_REGISTRY,
        "entry_state_machine": {
            "confirmation_bars": "S+1_THROUGH_CONFIGURED_MAX_AT_MOST_S+4",
            "entry_bars": "BAR_AFTER_CONFIRMATION_THROUGH_S+5",
            "same_confirmation_bar_fill": False,
            "gap_fill": "ADVERSE_OPEN",
            "trigger_rounding": "DIRECTIONAL_AWAY_TO_EQUITY_TICK",
            "brackets": "ORIGINAL_SETUP_PERCENT_FROM_ACTUAL_FILL",
            "same_bar_policy": "STOP_FIRST",
            "post_confirmation_cancel": "CLOSE_REVERSES_THROUGH_5M_CLOSE",
            "cap_allocation": "FROZEN_5M_RANK_AS_CONFIRMATIONS_ARRIVE",
        },
        "diagnostics": {
            "trade_schema_version": TRADE_SCHEMA_VERSION,
            "breakdown_schema_version": DIAGNOSTIC_BREAKDOWN_SCHEMA_VERSION,
            "confirmation_rejection_code_order": [
                "INVALID_BAR",
                "NONPOSITIVE_RANGE",
                "WRONG_CANDLE_DIRECTION",
                "CLOSE_NOT_BEYOND_FIVE_MINUTE_CLOSE",
                "BODY_RATIO_BELOW_MINIMUM",
                "ADVERSE_WICK_RATIO_ABOVE_MAXIMUM",
                "CLOSE_LOCATION_BELOW_MINIMUM",
            ],
            "preconfirmation_precedence_code": "PRECONF_MIDPOINT_INVALIDATED",
            "excursion_policy_version": EXCURSION_POLICY_VERSION,
            "excursion_policy": (
                "SIDE_NORMALIZED_POST_FILL_OHLC_LOWER_AND_UPPER_BOUNDS;"
                "ENTRY_AND_EXIT_PRICES_CERTAIN;STRICT_INTERIOR_BARS_CERTAIN;"
                "BOUNDARY_EXTREMES_UPPER_ONLY_UNLESS_WHOLE_BAR_HELD"
                ";EXIT_AT_OPEN_EXCLUDES_ALL_LATER_EXIT_BAR_EXTREMES"
            ),
            "breakdown_dimensions": list(DIAGNOSTIC_BREAKDOWN_DIMENSIONS),
            "chronological_block_policy": (
                "NONOVERLAPPING_FIVE_OFFICIAL_SESSION_BLOCKS_IN_CALENDAR_ORDER"
            ),
            "deferred_fields": [
                "B6_PREVIOUS_10_ONE_MINUTE_VOLUME_AND_RANGE_RATIOS",
                "B7_MARKET_AND_SECTOR_CONTEXT",
                "UNFROZEN_LIQUIDITY_OI_VOLUME_VOLATILITY_BUCKETS",
            ],
        },
        "data_contract": {
            "execution_instrument": EXECUTION_INSTRUMENT,
            "oi_instrument": OI_INSTRUMENT,
            "five_minute_construction": FIVE_MINUTE_CONSTRUCTION,
            "timestamp_convention": TIMESTAMP_CONVENTION,
            "path_policy_version": PATH_POLICY_VERSION,
            "session_calendar_sha256": NSE_FO_CALENDAR_SHA256,
            "session_calendar": nse_fo_calendar_payload(),
            "source_completeness": (
                "EVERY_SELECTED_SYMBOL_X_EXPECTED_SESSION_EXACT_CASH_1M_GRID_"
                "AND_REQUIRED_FUTURES_SIGNAL_SLOTS"
            ),
        },
        "portfolio": {
            "mode": PORTFOLIO_MODE,
            "default_policy": asdict(PortfolioPolicy()),
            "allocation": "GLOBAL_EVENT_TIME_THEN_SIGNAL_SETUP_RANK_SYMBOL",
            "release_before_reserve_same_timestamp": True,
            "portfolio_rejection_backfill": False,
        },
    }


def _format_number(value: Any, digits: int = 3) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "N/A"
    if isinstance(value, float) and math.isinf(value):
        return "inf"
    return f"{float(value):,.{digits}f}"


def _diagnostic_report_lines(breakdowns: pd.DataFrame) -> list[str]:
    lines = [
        "## B0-B5 diagnostic breakdowns",
        "",
        "Metrics use constrained, finite closed trades. The complete long-form "
        "table, including every symbol, is in `diagnostic_breakdowns.csv`.",
        "",
    ]
    if breakdowns.empty:
        return [*lines, "No diagnostic rows were available.", ""]
    compact = breakdowns.loc[~breakdowns["dimension"].eq("symbol")].copy()
    symbols = breakdowns.loc[breakdowns["dimension"].eq("symbol")].copy()
    if not symbols.empty:
        symbols = symbols.sort_values(
            ["fills", "net_pnl_rs", "bucket"],
            ascending=[False, False, True],
            kind="stable",
        ).head(10)
        compact = pd.concat([compact, symbols], ignore_index=True)
    lines.extend(
        [
            "| Dimension | Bucket | Candidates | Confirmed | Fills | Closed | "
            "Net points | Net P&L Rs | PF |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in compact.to_dict("records"):
        dimension = str(row["dimension"]).replace("|", "\\|")
        bucket = str(row["bucket"]).replace("|", "\\|")
        lines.append(
            f"| {dimension} | {bucket} | {int(row['candidates'])} | "
            f"{int(row['confirmed'])} | {int(row['fills'])} | "
            f"{int(row['closed_fills'])} | "
            f"{_format_number(row['net_return_percentage_points'])} | "
            f"{_format_number(row['net_pnl_rs'], 2)} | "
            f"{_format_number(row['profit_factor'])} |"
        )
    lines.extend(
        [
            "",
            "Previous-10 one-minute ratios (B6), market/sector context (B7), "
            "sector tables and unfrozen quantitative buckets are deliberately "
            "excluded.",
            "",
        ]
    )
    return lines


def render_v8_report(
    *,
    summary: Mapping[str, Any],
    variant: str,
    policy: EntryPolicy,
    cache_manifest: Mapping[str, Any],
    coverage: pd.DataFrame,
    from_day: date,
    through_day: date,
    run_id: str,
    diagnostic_breakdowns: pd.DataFrame | None = None,
) -> str:
    status_counts = summary.get("status_counts", {})
    status_text = ", ".join(
        f"{key}={value}" for key, value in sorted(dict(status_counts).items())
    ) or "none"
    exact_sessions = int(coverage["exact_1530_session_count"].sum()) if not coverage.empty else 0
    lines = [
        "# FNO V8 Windowed 1-Minute Entry Backtest",
        "",
        f"- Run: `{run_id}`",
        f"- Strategy: `{STRATEGY_VERSION}`",
        f"- Variant: `{variant}` - {VARIANT_REGISTRY[variant]['description']}",
        f"- Window: {from_day.isoformat()} through {through_day.isoformat()}",
        f"- Cost / slippage: {policy.cost_bps:g} / {policy.slippage_bps:g} bps",
        f"- EOD policy: `{policy.eod_policy}` at {policy.square_off}",
        f"- Cache input fingerprint: `{cache_manifest.get('input_fingerprint', '')}`",
        "",
        "## Result",
        "",
        f"- Headline valid: {bool(summary.get('headline_valid', False))}",
        f"- Data-incomplete candidates: {summary.get('data_incomplete_candidates', 0)}",
        f"- Filled trades without finite terminal economics: "
        f"{summary.get('unresolved_filled_trades', 0)}",
        f"- Upstream incomplete symbol-sessions: "
        f"{summary.get('source_incomplete_symbol_sessions', 0)}",
        f"- Unexpected non-calendar source symbol-sessions: "
        f"{summary.get('unexpected_source_symbol_sessions', 0)}",
        f"- Candidates: {summary.get('candidates', 0)}",
        f"- Filled / closed: {summary.get('fills', 0)} / {summary.get('closed_fills', 0)}",
        f"- Profit factor: {_format_number(summary.get('profit_factor'))}",
        "- Additive net return: "
        f"{_format_number(summary.get('net_return_percentage_points'))} percentage points",
        f"- Cash-equity sizing proxy net P&L: Rs {_format_number(summary.get('net_pnl_rs'), 2)}",
        "- Peak-to-trough cumulative daily drawdown: "
        f"{_format_number(summary.get('max_daily_drawdown_percentage_points'))} points",
        f"- Terminal states: {status_text}",
        "",
        "## Chronological periods",
        "",
        "Period rows are diagnostic when the headline is invalid.",
        "",
    ]
    for period, metrics in dict(summary.get("period_metrics", {})).items():
        lines.append(
            f"- {period}: sessions={metrics.get('sessions', 0)}, "
            f"fills={metrics.get('fills', 0)}, "
            f"PF={_format_number(metrics.get('profit_factor'))}, "
            f"net={_format_number(metrics.get('net_return_percentage_points'))} points"
        )
    lines.append("")
    lines.extend(
        _diagnostic_report_lines(
            diagnostic_breakdowns
            if diagnostic_breakdowns is not None
            else pd.DataFrame(columns=_DIAGNOSTIC_BREAKDOWN_COLUMNS)
        )
    )
    lines.extend(
        [
        "",
        "## Data and execution contract",
        "",
        f"- Execution prices: `{EXECUTION_INSTRUMENT}`; OI: `{OI_INSTRUMENT}`.",
        f"- Candidate paths are timestamped and restricted to the same date through {policy.square_off}.",
        "- Confirmation cannot fill its own candle; gaps fill at the adverse open; "
        "stops and targets are recomputed from actual fill; same-bar ambiguity is stop-first.",
        f"- Exact 15:30 coverage count across symbol-days: {exact_sessions}.",
        "- Net return is an additive independent-trade diagnostic, not an account return.",
        "",
        "## Promotion status",
        "",
        "**NOT PROMOTION ELIGIBLE.** This is the independent Phase-0/Phase-1 research engine.",
        "",
        ]
    )
    for blocker in summary.get("promotion_blockers", []):
        lines.append(f"- `{blocker}`")
    lines.extend(
        [
            "",
            "The full historical period still lacks daily point-in-time F&O universes, "
            "rolling near-month OI lineage, certified one-minute row lineage, and full "
            "cross-setup backfill semantics in the portfolio scheduler. Results must remain "
            "research-only.",
            "",
        ]
    )
    return "\n".join(lines)


def _state_events_frame(audit: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    if audit.empty:
        return pd.DataFrame(
            columns=[
                "candidate_id",
                "session_date",
                "setup_id",
                "side",
                "symbol",
                "event_ts",
                "state_before",
                "state_after",
                "reason",
                "schema_version",
            ]
        )
    for row in audit.to_dict("records"):
        for event in row.get("events", []) or []:
            records.append(
                {
                    "candidate_id": row["candidate_id"],
                    "session_date": row["session_date"],
                    "setup_id": row["setup_id"],
                    "side": row["side"],
                    "symbol": row["symbol"],
                    "event_ts": event.get("event_ts"),
                    "state_before": event.get("state_before"),
                    "state_after": event.get("state_after"),
                    "reason": event.get("reason"),
                    "schema_version": STATE_EVENT_SCHEMA_VERSION,
                }
            )
    return pd.DataFrame(records)


def write_v8_run_artifacts(
    *,
    audit: pd.DataFrame,
    daily: pd.DataFrame,
    coverage: pd.DataFrame,
    summary: Mapping[str, Any],
    variant: str,
    policy: EntryPolicy,
    cache_manifest: Mapping[str, Any],
    cache_manifest_path: Path,
    from_day: date,
    through_day: date,
    split_day: date | None,
    diagnostic_breakdowns: pd.DataFrame | None = None,
) -> tuple[Path, Path, dict[str, Any]]:
    generated_at = common.now_ist()
    expected_code_sha256 = str(
        dict(cache_manifest.get("input_contract", {})).get(
            "strategy_code_sha256", ""
        )
    )
    if expected_code_sha256 != _module_source_sha256():
        raise RuntimeError("V8 run code does not match the cache construction code")
    cache_manifest_sha256 = provenance.sha256_file(cache_manifest_path)
    parameters = {
        "variant": variant,
        "entry_policy": asdict(policy),
        "from_day": from_day.isoformat(),
        "through_day": through_day.isoformat(),
        "split_day": split_day.isoformat() if split_day else None,
        "target_exposure_per_entry_rs": 50_000.0,
        "portfolio_mode": PORTFOLIO_MODE,
        "cache_manifest_sha256": cache_manifest_sha256,
    }
    input_fingerprint = provenance.backtest_input_fingerprint(
        cache_manifest,
        strategy_payload=strategy_payload(),
        parameters=parameters,
    )
    run_id = (
        f"fno_v8_{variant.lower()}_"
        f"{generated_at.strftime('%Y%m%dT%H%M%S%f%z')}_{input_fingerprint[:12]}"
    )
    run_dir = RUN_ROOT / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    audit_path = run_dir / "candidate_order_audit.csv"
    events_path = run_dir / "state_events.parquet"
    daily_path = run_dir / "daily.csv"
    diagnostic_breakdowns_path = run_dir / "diagnostic_breakdowns.csv"
    coverage_path = run_dir / "coverage.csv"
    setups_path = run_dir / "setups.csv"
    report_path = run_dir / "report.md"
    source_archive_path = run_dir / "fno_v8_windowed_1m_entry_backtest.py"
    cache_manifest_archive_path = run_dir / "cache_manifest.json"

    source_sha256 = _module_source_sha256()
    provenance.publish_immutable_copy(
        Path(__file__),
        source_archive_path,
        expected_sha256=source_sha256,
    )
    provenance.publish_immutable_copy(
        cache_manifest_path,
        cache_manifest_archive_path,
        expected_sha256=cache_manifest_sha256,
    )

    audit_export = audit.copy()
    missing_audit_columns = sorted(
        set(_AUDIT_EXPORT_REQUIRED_COLUMNS) - set(audit_export.columns)
    )
    if missing_audit_columns and not audit_export.empty:
        raise AssertionError(
            "V8 candidate/order audit is missing required columns: "
            f"{missing_audit_columns}"
        )
    if audit_export.empty:
        audit_export = pd.DataFrame(columns=_AUDIT_EXPORT_REQUIRED_COLUMNS)
    for event_column in (
        "events",
        "unconstrained_events",
        "confirmation_checks",
        "confirmation_rejection_codes",
    ):
        if event_column in audit_export.columns:
            audit_export[event_column] = audit_export[event_column].map(
                lambda value: json.dumps(value, ensure_ascii=True, default=str)
            )
    breakdown_export = (
        diagnostic_breakdowns.copy()
        if diagnostic_breakdowns is not None
        else build_v8_diagnostic_breakdowns(
            audit,
            session_dates=cache_manifest.get("session_dates", []),
        )
    )
    common.atomic_write_csv(audit_export, audit_path)
    common.atomic_write_parquet(_state_events_frame(audit), events_path)
    common.atomic_write_csv(daily, daily_path)
    common.atomic_write_csv(breakdown_export, diagnostic_breakdowns_path)
    common.atomic_write_csv(coverage, coverage_path)
    common.atomic_write_csv(pd.DataFrame(_setup_payload()), setups_path)
    report = render_v8_report(
        summary=summary,
        variant=variant,
        policy=policy,
        cache_manifest=cache_manifest,
        coverage=coverage,
        from_day=from_day,
        through_day=through_day,
        run_id=run_id,
        diagnostic_breakdowns=breakdown_export,
    )
    common.atomic_write_text(report_path, report)

    output_paths = {
        "candidate_order_audit": audit_path,
        "state_events": events_path,
        "daily": daily_path,
        "diagnostic_breakdowns": diagnostic_breakdowns_path,
        "coverage": coverage_path,
        "setups": setups_path,
        "report": report_path,
        "strategy_source_archive": source_archive_path,
        "cache_manifest_archive": cache_manifest_archive_path,
    }
    provenance_payload = provenance.build_run_provenance(
        generated_at=generated_at,
        strategy_version=STRATEGY_VERSION,
        objective=OBJECTIVE,
        strategy_payload=strategy_payload(),
        parameters=parameters,
        backtest_window={
            "from_day": from_day.isoformat(),
            "through_day": through_day.isoformat(),
            "split_day": split_day.isoformat() if split_day else None,
        },
        cache_manifest_path=cache_manifest_path,
        cache_manifest=cache_manifest,
        output_paths=output_paths,
        results=dict(summary),
    )
    provenance_payload["v8_run_schema_version"] = RUN_SCHEMA_VERSION
    provenance_payload["promotion_eligible"] = False
    provenance_payload["strategy_source_sha256"] = source_sha256
    provenance_payload["cache_manifest_sha256"] = cache_manifest_sha256
    run_provenance_path = run_dir / "provenance.json"
    central_provenance_path = PROVENANCE_ROOT / f"{run_id}.json"
    provenance.write_immutable_json(run_provenance_path, provenance_payload)
    provenance.write_immutable_json(central_provenance_path, provenance_payload)
    common.atomic_write_text(REPORT_PATH, report)
    return run_dir, run_provenance_path, provenance_payload


def execute_v8_run(
    *,
    source_snapshot_path: Path | str,
    from_day: date | str,
    through_day: date | str,
    symbols: Iterable[str] | None,
    variant: str,
    cost_bps: float,
    slippage_bps: float,
    square_off: str,
    eod_policy: str,
    split_day: date | str | None = None,
    rebuild_cache: bool = False,
    write_outputs: bool = True,
) -> dict[str, Any]:
    start_day = _parse_day(from_day)
    end_day = _parse_day(through_day)
    split = _parse_day(split_day) if split_day is not None else None
    if split is not None and not (start_day < split <= end_day):
        raise ValueError("split_day must be after from_day and no later than through_day")
    variant_key = str(variant).upper().strip()
    policy = entry_policy_for_variant(
        variant_key,
        cost_bps=cost_bps,
        slippage_bps=slippage_bps,
        square_off=square_off,
        eod_policy=eod_policy,
    )
    candidates, minute_paths, coverage, manifest, manifest_path = (
        load_or_build_v8_cache(
            source_snapshot_path=source_snapshot_path,
            from_day=start_day,
            through_day=end_day,
            symbols=symbols,
            rebuild=rebuild_cache,
        )
    )
    cache_contract = dict(manifest.get("input_contract", {}))
    derived_completeness = derive_coverage_completeness(
        coverage,
        selected_symbols=cache_contract.get("symbols", []),
        expected_session_dates=dict(
            cache_contract.get("session_calendar", {})
        ).get("expected_session_dates", []),
    )
    if not _manifest_completeness_matches(manifest, derived_completeness):
        raise AssertionError("V8 cache completeness metadata is not bound to coverage")
    audit = run_v8_backtest(
        candidates,
        minute_paths,
        variant=variant_key,
        policy=policy,
    )
    summary, daily = summarize_v8_results(
        audit,
        session_dates=manifest.get("session_dates", []),
        split_day=split,
        eod_policy=policy.eod_policy,
        source_complete=bool(derived_completeness["headline_source_complete"]),
        source_incomplete_symbol_sessions=int(
            derived_completeness["source_incomplete_symbol_sessions"]
        ),
        unexpected_source_symbol_sessions=int(
            derived_completeness["unexpected_source_symbol_sessions"]
        ),
    )
    diagnostic_breakdowns = build_v8_diagnostic_breakdowns(
        audit,
        session_dates=manifest.get("session_dates", []),
    )
    result: dict[str, Any] = {
        "summary": summary,
        "audit": audit,
        "daily": daily,
        "diagnostic_breakdowns": diagnostic_breakdowns,
        "coverage": coverage,
        "cache_manifest": manifest,
        "cache_manifest_path": manifest_path,
        "policy": policy,
    }
    if write_outputs:
        run_dir, provenance_path, provenance_payload = write_v8_run_artifacts(
            audit=audit,
            daily=daily,
            coverage=coverage,
            summary=summary,
            variant=variant_key,
            policy=policy,
            cache_manifest=manifest,
            cache_manifest_path=manifest_path,
            from_day=start_day,
            through_day=end_day,
            split_day=split,
            diagnostic_breakdowns=diagnostic_breakdowns,
        )
        result.update(
            {
                "run_dir": run_dir,
                "provenance_path": provenance_path,
                "provenance": provenance_payload,
            }
        )
    return result


def validate_v8_run_provenance(path: Path | str) -> dict[str, Any]:
    supplied = Path(path).resolve()
    payload = json.loads(supplied.read_text(encoding="utf-8"))
    if payload.get("v8_run_schema_version") != RUN_SCHEMA_VERSION:
        raise ValueError("Not a supported V8 run provenance artifact")
    if payload.get("strategy_version") != STRATEGY_VERSION:
        raise ValueError("V8 run provenance strategy version mismatch")
    outputs = dict(payload.get("outputs", {}))
    required_outputs = {
        "candidate_order_audit",
        "state_events",
        "daily",
        "diagnostic_breakdowns",
        "coverage",
        "setups",
        "report",
        "strategy_source_archive",
        "cache_manifest_archive",
    }
    missing_outputs = sorted(required_outputs - set(outputs))
    if missing_outputs:
        raise ValueError(f"V8 run provenance is missing outputs: {missing_outputs}")
    for name, record in outputs.items():
        if not provenance.artifact_matches(record.get("path", ""), record):
            raise AssertionError(f"V8 output artifact changed: {name}")
    audit_record = dict(outputs.get("candidate_order_audit", {}))
    audit_path = Path(str(audit_record.get("path", "")))
    audit_frame = pd.read_csv(audit_path)
    missing_audit_columns = sorted(
        set(_AUDIT_EXPORT_REQUIRED_COLUMNS) - set(audit_frame.columns)
    )
    if missing_audit_columns:
        raise ValueError(
            "V8 candidate/order audit is missing required columns: "
            f"{missing_audit_columns}"
        )
    if not audit_frame.empty:
        audit_schemas = audit_frame["schema_version"]
        if audit_schemas.isna().any() or not audit_schemas.astype(str).eq(
            TRADE_SCHEMA_VERSION
        ).all():
            raise ValueError("V8 candidate/order audit schema is not supported")
    diagnostic_record = dict(outputs.get("diagnostic_breakdowns", {}))
    diagnostic_path = Path(str(diagnostic_record.get("path", "")))
    diagnostic_frame = pd.read_csv(diagnostic_path)
    missing_diagnostic_columns = sorted(
        set(_DIAGNOSTIC_BREAKDOWN_COLUMNS) - set(diagnostic_frame.columns)
    )
    if missing_diagnostic_columns:
        raise ValueError(
            "V8 diagnostic breakdown artifact is missing columns: "
            f"{missing_diagnostic_columns}"
        )
    diagnostic_schemas = diagnostic_frame["schema_version"]
    if diagnostic_frame.empty or diagnostic_schemas.isna().any() or not (
        diagnostic_schemas.astype(str).eq(DIAGNOSTIC_BREAKDOWN_SCHEMA_VERSION).all()
    ):
        raise ValueError("V8 diagnostic breakdown schema is not supported")
    source_record = outputs.get("strategy_source_archive", {})
    cache_record = outputs.get("cache_manifest_archive", {})
    source_path = Path(str(source_record.get("path", "")))
    archived_cache_path = Path(str(cache_record.get("path", "")))
    if provenance.sha256_file(source_path) != str(
        payload.get("strategy_source_sha256", "")
    ):
        raise AssertionError("Archived V8 strategy source hash is invalid")
    if provenance.sha256_file(archived_cache_path) != str(
        payload.get("cache_manifest_sha256", "")
    ):
        raise AssertionError("Archived V8 cache manifest hash is invalid")
    cache_manifest = json.loads(archived_cache_path.read_text(encoding="utf-8"))
    if cache_manifest.get("schema_version") != CACHE_SCHEMA_VERSION:
        raise ValueError("Archived V8 cache schema is not supported")
    input_contract = dict(cache_manifest.get("input_contract", {}))
    if cache_manifest.get("input_fingerprint") != common.canonical_json_sha256(
        input_contract
    ):
        raise AssertionError("Archived V8 cache input contract fingerprint is invalid")
    if cache_manifest.get("input_fingerprint") != payload.get(
        "cache_input_fingerprint"
    ):
        raise AssertionError("Archived V8 cache input fingerprint does not match run")
    if str(input_contract.get("strategy_code_sha256", "")) != str(
        payload.get("strategy_source_sha256", "")
    ):
        raise AssertionError("V8 run source does not match cache construction source")
    if str(dict(payload.get("parameters", {})).get("cache_manifest_sha256", "")) != str(
        payload.get("cache_manifest_sha256", "")
    ):
        raise AssertionError("V8 run parameters do not bind the cache manifest bytes")
    calendar_contract = dict(input_contract.get("session_calendar", {}))
    if calendar_contract.get("calendar_sha256") != NSE_FO_CALENDAR_SHA256:
        raise AssertionError("Archived V8 cache calendar hash is invalid")
    if common.canonical_json_sha256(calendar_contract.get("calendar", {})) != (
        NSE_FO_CALENDAR_SHA256
    ):
        raise AssertionError("Archived V8 cache calendar payload is invalid")
    expected_calendar_dates = [
        value.isoformat()
        for value in expected_regular_session_dates(
            input_contract.get("from_day", ""),
            input_contract.get("through_day", ""),
        )
    ]
    if list(calendar_contract.get("expected_session_dates", [])) != (
        expected_calendar_dates
    ):
        raise AssertionError("Archived V8 expected-session list is invalid")
    if int(calendar_contract.get("expected_session_count", -1)) != len(
        expected_calendar_dates
    ):
        raise AssertionError("Archived V8 expected-session count is invalid")
    if list(cache_manifest.get("session_dates", [])) != expected_calendar_dates:
        raise AssertionError("Archived V8 manifest session dates are invalid")
    if common.canonical_json_sha256(cache_manifest.get("session_calendar", {})) != (
        common.canonical_json_sha256(calendar_contract)
    ):
        raise AssertionError("Archived V8 manifest calendar contract is invalid")
    expected_diagnostics = build_v8_diagnostic_breakdowns(
        audit_frame,
        session_dates=expected_calendar_dates,
    )
    try:
        pd.testing.assert_frame_equal(
            _normalized_diagnostic_breakdowns(diagnostic_frame),
            _normalized_diagnostic_breakdowns(expected_diagnostics),
            check_dtype=False,
            check_exact=False,
            rtol=1e-12,
            atol=1e-12,
        )
    except AssertionError as exc:
        raise AssertionError(
            "V8 diagnostic breakdown does not reconcile to the candidate audit"
        ) from exc
    for name, record in dict(cache_manifest.get("artifacts", {})).items():
        if not provenance.artifact_matches(record.get("path", ""), record):
            raise AssertionError(f"V8 cache artifact changed: {name}")
    cache_artifacts = dict(cache_manifest.get("artifacts", {}))
    coverage_record = dict(cache_artifacts.get("coverage", {}))
    candidate_record = dict(cache_artifacts.get("candidates", {}))
    paths_record = dict(cache_artifacts.get("paths", {}))
    coverage_frame = pd.read_parquet(Path(str(coverage_record.get("path", ""))))
    derived_completeness = derive_coverage_completeness(
        coverage_frame,
        selected_symbols=input_contract.get("symbols", []),
        expected_session_dates=calendar_contract.get("expected_session_dates", []),
    )
    if not _manifest_completeness_matches(cache_manifest, derived_completeness):
        raise AssertionError("Archived V8 cache completeness metadata is invalid")
    if int(cache_manifest.get("candidate_count", -1)) != pq.read_metadata(
        Path(str(candidate_record.get("path", "")))
    ).num_rows:
        raise AssertionError("Archived V8 cache candidate count is invalid")
    if int(cache_manifest.get("path_row_count", -1)) != pq.read_metadata(
        Path(str(paths_record.get("path", "")))
    ).num_rows:
        raise AssertionError("Archived V8 cache path-row count is invalid")
    recomputed_run_fingerprint = provenance.backtest_input_fingerprint(
        cache_manifest,
        strategy_payload=dict(payload.get("strategy_payload", {})),
        parameters=dict(payload.get("parameters", {})),
    )
    if recomputed_run_fingerprint != payload.get("backtest_input_fingerprint"):
        raise AssertionError("V8 run input fingerprint is invalid")
    snapshot_record = dict(cache_manifest.get("source_snapshot", {}))
    snapshot_path = snapshot_record.get("manifest_path")
    if not snapshot_path:
        raise ValueError("Archived V8 cache has no source snapshot manifest")
    contract = dict(cache_manifest.get("input_contract", {}))
    (
        _,
        validated_universe_record,
        validated_snapshot,
        validated_inventory,
        _,
    ) = load_validated_source_contract(
        snapshot_path,
        symbols=contract.get("symbols") or None,
    )
    snapshot_fingerprint = str(validated_snapshot.get("snapshot_fingerprint", ""))
    inventory_sha256 = str(validated_inventory.get("inventory_sha256", ""))
    source_fingerprint = str(validated_inventory.get("source_fingerprint", ""))
    archived_inventory = dict(cache_manifest.get("source_inventory", {}))
    if snapshot_fingerprint != str(contract.get("snapshot_fingerprint", "")) or (
        snapshot_fingerprint != str(snapshot_record.get("snapshot_fingerprint", ""))
    ):
        raise AssertionError("V8 source snapshot fingerprint no longer matches the run")
    if inventory_sha256 != str(contract.get("source_inventory_sha256", "")) or (
        inventory_sha256 != str(archived_inventory.get("inventory_sha256", ""))
    ):
        raise AssertionError("V8 source inventory hash no longer matches the run")
    if source_fingerprint != str(contract.get("source_fingerprint", "")) or (
        source_fingerprint != str(archived_inventory.get("source_fingerprint", ""))
    ):
        raise AssertionError("V8 source fingerprint no longer matches the run")
    if common.canonical_json_sha256(validated_universe_record) != (
        common.canonical_json_sha256(cache_manifest.get("universe", {}))
    ) or common.canonical_json_sha256(validated_universe_record) != (
        common.canonical_json_sha256(contract.get("universe", {}))
    ):
        raise AssertionError("V8 source universe no longer matches the run")
    payload["current_strategy_source_matches_archive"] = (
        _module_source_sha256() == str(payload.get("strategy_source_sha256", ""))
    )
    return payload


def _add_source_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-snapshot", type=Path, required=True)
    parser.add_argument("--symbols", default="")
    parser.add_argument("--rebuild-cache", action="store_true")


def _add_replay_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--variant", choices=sorted(VARIANT_REGISTRY), default="B4")
    parser.add_argument("--cost-bps", type=float, default=15.0)
    parser.add_argument("--slippage-bps", type=float, default=0.0)
    parser.add_argument("--square-off", default="15:30")
    parser.add_argument(
        "--eod-policy",
        choices=["EXACT_SQUARE_OFF", "LAST_REAL_BAR_SENSITIVITY"],
        default="EXACT_SQUARE_OFF",
    )
    parser.add_argument("--split-day")
    parser.add_argument("--no-write", action="store_true")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Independent FNO V8 windowed one-minute entry backtester"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    cache_parser = subparsers.add_parser("build-cache")
    _add_source_arguments(cache_parser)
    cache_parser.add_argument("--from-day", required=True)
    cache_parser.add_argument("--through-day", required=True)

    run_parser = subparsers.add_parser("run")
    _add_source_arguments(run_parser)
    _add_replay_arguments(run_parser)
    run_parser.add_argument("--from-day", required=True)
    run_parser.add_argument("--through-day", required=True)

    smoke_parser = subparsers.add_parser("smoke")
    _add_source_arguments(smoke_parser)
    _add_replay_arguments(smoke_parser)

    snapshot_parser = subparsers.add_parser("snapshot")
    snapshot_parser.add_argument("--snapshot-root", type=Path, default=SNAPSHOT_ROOT)

    validate_parser = subparsers.add_parser("validate")
    validate_parser.add_argument("--provenance", type=Path, required=True)
    return parser.parse_args(argv)


def _parse_symbols(value: str) -> list[str] | None:
    symbols = [item.strip().upper() for item in str(value).split(",") if item.strip()]
    return symbols or None


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.command == "snapshot":
            mapped, universe_record = provenance.load_backtest_universe(
                universe_path=BACKTEST_UNIVERSE_PATH,
                universe_date=BACKTEST_UNIVERSE_DATE,
                contract_month_contains="26AUG",
                require_persisted_mapping=True,
                expected_file_sha256=BACKTEST_UNIVERSE_HASHES["file_sha256"],
                expected_universe_sha256=BACKTEST_UNIVERSE_HASHES["universe_sha256"],
                expected_mapped_universe_sha256=BACKTEST_UNIVERSE_HASHES[
                    "mapped_universe_sha256"
                ],
                expected_mapped_symbol_set_sha256=BACKTEST_UNIVERSE_HASHES[
                    "mapped_symbol_set_sha256"
                ],
            )
            snapshot = provenance.create_source_snapshot(
                mapped,
                universe_record,
                universe_path=BACKTEST_UNIVERSE_PATH,
                snapshot_root=args.snapshot_root,
                require_complete_sources=True,
            )
            print(snapshot["manifest_path"])
            return 0
        if args.command == "validate":
            payload = validate_v8_run_provenance(args.provenance)
            print(payload["backtest_input_fingerprint"])
            return 0

        symbols = _parse_symbols(args.symbols)
        if args.command == "build-cache":
            candidates, paths, coverage, manifest, manifest_path = load_or_build_v8_cache(
                source_snapshot_path=args.source_snapshot,
                from_day=args.from_day,
                through_day=args.through_day,
                symbols=symbols,
                rebuild=args.rebuild_cache,
            )
            print(
                f"[V8][CACHE] candidates={len(candidates)} paths={len(paths)} "
                f"symbols={len(coverage)} manifest={manifest_path} "
                f"fingerprint={manifest['input_fingerprint']}"
            )
            return 0

        if args.command == "smoke":
            smoke_symbols = symbols or ["ITC", "TCS", "INFY"]
            result = execute_v8_run(
                source_snapshot_path=args.source_snapshot,
                from_day="2026-07-28",
                through_day="2026-07-29",
                symbols=smoke_symbols,
                variant=args.variant,
                cost_bps=args.cost_bps,
                slippage_bps=args.slippage_bps,
                square_off=args.square_off,
                eod_policy=args.eod_policy,
                split_day=args.split_day,
                rebuild_cache=args.rebuild_cache,
                write_outputs=not args.no_write,
            )
            coverage = result["coverage"]
            if set(smoke_symbols) == {"ITC", "TCS", "INFY"}:
                expected = {
                    "equity_1m_rows": 750,
                    "equity_5m_rows": 150,
                    "futures_5m_rows": 150,
                    "joined_5m_rows": 150,
                    "exact_1530_session_count": 2,
                }
                for column, value in expected.items():
                    if not coverage[column].eq(value).all():
                        raise AssertionError(
                            f"V8 smoke coverage mismatch for {column}: "
                            f"{coverage[['symbol', column]].to_dict('records')}"
                        )
            print(json.dumps(result["summary"], indent=2, default=str))
            if "run_dir" in result:
                print(f"[V8][RUN] {result['run_dir']}")
            return 0

        result = execute_v8_run(
            source_snapshot_path=args.source_snapshot,
            from_day=args.from_day,
            through_day=args.through_day,
            symbols=symbols,
            variant=args.variant,
            cost_bps=args.cost_bps,
            slippage_bps=args.slippage_bps,
            square_off=args.square_off,
            eod_policy=args.eod_policy,
            split_day=args.split_day,
            rebuild_cache=args.rebuild_cache,
            write_outputs=not args.no_write,
        )
        print(json.dumps(result["summary"], indent=2, default=str))
        if "run_dir" in result:
            print(f"[V8][RUN] {result['run_dir']}")
        return 0
    except Exception as exc:
        print(f"[V8][ERROR] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
