"""Incremental PAPER reducer for the frozen V8-Combined entry policy.

The historical V8 engine is a batch simulator.  This module implements the
same completed-candle decisions as an in-memory, single-writer reducer that can
be checkpointed by a session orchestrator.  It deliberately contains no
broker, quote/LTP, credential, scheduler, or filesystem calls.

The reducer's atomic input is one *completed* minute and the symbol bars that
belong to that exact end label.  Callers must submit minutes chronologically.
Submitting the same minute with the same payload is idempotent; changing a
previously submitted payload is rejected.

Five-minute construction and proof that OI came from exact current/S-5 rows
remain the scanner's responsibility.  At registration this reducer
independently revalidates the scalar V8 authority (trend, move, positive/rising
OI metrics, volume and setup thresholds), freezes the picker rank, and then
owns every one-minute and portfolio transition.  Scalar fields cannot prove
their own source timestamps, so callers must bind that lineage in evidence.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable
from zoneinfo import ZoneInfo

import fno_v8_combined_paper_config as paper_config


IST = ZoneInfo("Asia/Kolkata")
SCHEMA_VERSION = "fno_v8_combined_incremental_paper_engine_v1"
BASE_PRICE_CHANGE_PCT = 0.10
BASE_OI_CHANGE_PCT = 0.05
BASE_VOLUME_RATIO = 0.80
ENTRY_INHERIT = "INHERIT"


class PaperEngineError(RuntimeError):
    """Base exception for deterministic reducer contract violations."""


class ReplayConflictError(PaperEngineError):
    """A previously accepted input was replayed with different bytes."""


class OutOfOrderMinuteError(PaperEngineError):
    """Completed-minute input moved backwards in event time."""


class CandidateState(str, Enum):
    MONITORING = "MONITORING_CONFIRMATION"
    PRECONF_INVALIDATED = "PRECONF_INVALIDATED"
    CONFIRMED_WAITING_CAP = "CONFIRMED_WAITING_CAP"
    PENDING_STOP = "PENDING_STOP"
    FILLED_OPEN = "FILLED_OPEN"
    POSTCONF_CANCELLED = "POSTCONF_CANCELLED"
    NO_CONFIRMATION = "NO_CONFIRMATION"
    WINDOW_EXPIRED = "WINDOW_EXPIRED"
    STOPPED = "STOPPED"
    TARGETED = "TARGETED"
    SQUARE_OFF = "SQUARE_OFF"
    INTERVENTION_CANCELLED = "INTERVENTION_CANCELLED"
    INTERVENTION_CLOSED = "INTERVENTION_CLOSED"
    DATA_INCOMPLETE = "DATA_INCOMPLETE"


NO_CONFIRMATION_VALUE = CandidateState.NO_CONFIRMATION.value


ACTIVE_STATES = {
    CandidateState.MONITORING.value,
    CandidateState.CONFIRMED_WAITING_CAP.value,
    CandidateState.PENDING_STOP.value,
    CandidateState.FILLED_OPEN.value,
}
ENTRY_WINDOW_ACTIVE_STATES = {
    CandidateState.MONITORING.value,
    CandidateState.CONFIRMED_WAITING_CAP.value,
    CandidateState.PENDING_STOP.value,
    CandidateState.FILLED_OPEN.value,
}
TERMINAL_STATES = {
    CandidateState.PRECONF_INVALIDATED.value,
    CandidateState.POSTCONF_CANCELLED.value,
    NO_CONFIRMATION_VALUE,
    CandidateState.WINDOW_EXPIRED.value,
    CandidateState.STOPPED.value,
    CandidateState.TARGETED.value,
    CandidateState.SQUARE_OFF.value,
    CandidateState.INTERVENTION_CANCELLED.value,
    CandidateState.INTERVENTION_CLOSED.value,
    CandidateState.DATA_INCOMPLETE.value,
}


@runtime_checkable
class SetupLike(Protocol):
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
    entry_conf_minute: int | None
    entry_buffer_bps: float | None
    entry_midpoint: bool | None
    entry_clv: float | str | None


@dataclass(frozen=True)
class SetupPolicy:
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

    @classmethod
    def from_object(cls, value: SetupLike | Mapping[str, Any]) -> "SetupPolicy":
        def get(name: str, default: Any = None) -> Any:
            if isinstance(value, Mapping):
                return value.get(name, default)
            return getattr(value, name, default)

        setup = cls(
            signal_end=str(get("signal_end")).strip(),
            side=str(get("side")).strip().upper(),
            max_entries=int(get("max_entries")),
            picker=str(get("picker")).strip(),
            price_change_pct=float(get("price_change_pct")),
            oi_change_pct=float(get("oi_change_pct")),
            volume_ratio=float(get("volume_ratio")),
            body_ratio=float(get("body_ratio")),
            max_wick_ratio=float(get("max_wick_ratio")),
            min_traded_value=float(get("min_traded_value")),
            stop_pct=float(get("stop_pct")),
            target_pct=float(get("target_pct")),
            entry_conf_minute=(
                None if get("entry_conf_minute") is None else int(get("entry_conf_minute"))
            ),
            entry_buffer_bps=(
                None if get("entry_buffer_bps") is None else float(get("entry_buffer_bps"))
            ),
            entry_midpoint=(
                None if get("entry_midpoint") is None else bool(get("entry_midpoint"))
            ),
            entry_clv=get("entry_clv", ENTRY_INHERIT),
        )
        setup.validate()
        return setup

    def validate(self) -> None:
        try:
            parsed = time.fromisoformat(self.signal_end)
        except ValueError as exc:
            raise ValueError(f"invalid signal_end {self.signal_end!r}") from exc
        if self.signal_end != parsed.strftime("%H:%M") or parsed.minute % 5:
            raise ValueError("signal_end must be an HH:MM five-minute end label")
        if self.side not in {"LONG", "SHORT"}:
            raise ValueError("side must be LONG or SHORT")
        if self.max_entries <= 0:
            raise ValueError("max_entries must be positive")
        if self.picker not in {"max_oi", "max_volume", "max_move", "max_liquidity"}:
            raise ValueError(f"unsupported picker {self.picker!r}")
        numeric = (
            self.price_change_pct,
            self.oi_change_pct,
            self.volume_ratio,
            self.body_ratio,
            self.max_wick_ratio,
            self.min_traded_value,
            self.stop_pct,
            self.target_pct,
        )
        if not all(math.isfinite(float(item)) for item in numeric):
            raise ValueError("setup thresholds must be finite")
        if self.entry_conf_minute is not None and not 1 <= self.entry_conf_minute < 5:
            raise ValueError("entry_conf_minute must be in [1, 4]")


@dataclass(frozen=True)
class ResolvedEntryPolicy:
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

    @classmethod
    def from_object(cls, value: Any) -> "ResolvedEntryPolicy":
        fields = cls.__dataclass_fields__
        payload = {
            name: (value.get(name) if isinstance(value, Mapping) else getattr(value, name))
            for name in fields
        }
        policy = cls(**payload)
        policy.validate()
        return policy

    def validate(self) -> None:
        if not 1 <= int(self.max_confirmation_minute) < int(self.entry_expiry_minute):
            raise ValueError("confirmation window must end before entry expiry")
        if int(self.entry_expiry_minute) != 5:
            raise ValueError("V8-Combined entry expiry must remain S+5")
        if self.same_bar_policy != "STOP_FIRST":
            raise ValueError("only STOP_FIRST is supported")
        if self.eod_policy != "EXACT_SQUARE_OFF":
            raise ValueError("paper engine requires EXACT_SQUARE_OFF")
        if time.fromisoformat(self.square_off) != time(15, 30):
            raise ValueError("V8-Combined square-off must remain 15:30")
        for item in (self.buffer_bps, self.cost_bps, self.slippage_bps):
            if not math.isfinite(float(item)) or float(item) < 0:
                raise ValueError("buffer, costs and slippage must be finite/non-negative")
        if self.close_location_min is not None and not 0 <= float(self.close_location_min) <= 1:
            raise ValueError("close_location_min must be in [0, 1]")


@dataclass(frozen=True)
class PaperPortfolioPolicy:
    capital_rs: float = 120_000.0
    margin_per_entry_rs: float = 10_000.0
    target_exposure_per_entry_rs: float = 50_000.0
    max_concurrent_positions: int = 12
    pending_reserves_margin: bool = True
    one_position_per_symbol: bool = True

    @classmethod
    def from_object(cls, value: Any) -> "PaperPortfolioPolicy":
        fields = cls.__dataclass_fields__
        payload = {
            name: (value.get(name) if isinstance(value, Mapping) else getattr(value, name))
            for name in fields
        }
        policy = cls(**payload)
        policy.validate()
        return policy

    @property
    def capacity(self) -> int:
        return min(
            int(self.max_concurrent_positions),
            int(math.floor(float(self.capital_rs) / float(self.margin_per_entry_rs))),
        )

    def validate(self) -> None:
        if not self.pending_reserves_margin or not self.one_position_per_symbol:
            raise ValueError("pending reservation and one-symbol policy are mandatory")
        if self.capacity <= 0 or float(self.target_exposure_per_entry_rs) <= 0:
            raise ValueError("portfolio capacity and exposure must be positive")


@dataclass(frozen=True)
class PaperEngineConfig:
    setups: tuple[SetupPolicy, ...]
    entry_policies: Mapping[str, ResolvedEntryPolicy]
    portfolio_policy: PaperPortfolioPolicy
    setup_book_sha256: str
    strategy_fingerprint: str

    @classmethod
    def from_module(cls, module: Any = paper_config) -> "PaperEngineConfig":
        if hasattr(module, "validate_configuration"):
            module.validate_configuration()
        setups = tuple(SetupPolicy.from_object(item) for item in module.ACTIVE_SETUPS)
        policies = {
            setup.setup_id: ResolvedEntryPolicy.from_object(
                module.entry_policy_for_setup(
                    next(item for item in module.ACTIVE_SETUPS if item.setup_id == setup.setup_id)
                )
            )
            for setup in setups
        }
        portfolio = PaperPortfolioPolicy.from_object(module.PORTFOLIO_POLICY)
        fingerprint = (
            str(module.strategy_fingerprint())
            if callable(getattr(module, "strategy_fingerprint", None))
            else str(getattr(module, "STRATEGY_FINGERPRINT"))
        )
        result = cls(
            setups=setups,
            entry_policies=policies,
            portfolio_policy=portfolio,
            setup_book_sha256=str(
                getattr(module, "SETUP_BOOK_SHA256", module.COMBINED_SETUP_BOOK_SHA256)
            ),
            strategy_fingerprint=fingerprint,
        )
        result.validate()
        return result

    @property
    def setup_by_id(self) -> dict[str, SetupPolicy]:
        return {setup.setup_id: setup for setup in self.setups}

    def validate(self) -> None:
        if not self.setups or len(self.setup_by_id) != len(self.setups):
            raise ValueError("setup IDs must be nonempty and unique")
        if set(self.entry_policies) != set(self.setup_by_id):
            raise ValueError("each setup requires one resolved entry policy")
        for setup in self.setups:
            setup.validate()
            self.entry_policies[setup.setup_id].validate()
        self.portfolio_policy.validate()
        if len(self.setup_book_sha256) != 64 or len(self.strategy_fingerprint) != 64:
            raise ValueError("configuration identities must be SHA-256 values")


def _as_ist(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif hasattr(value, "to_pydatetime"):
        parsed = value.to_pydatetime()
    elif isinstance(value, date):
        parsed = datetime.combine(value, time.min)
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=IST)
    return parsed.astimezone(IST)


def _exact_minute(value: Any) -> datetime:
    parsed = _as_ist(value)
    nanosecond = int(getattr(value, "nanosecond", getattr(parsed, "nanosecond", 0)) or 0)
    if parsed.second or parsed.microsecond or nanosecond:
        raise ValueError(f"timestamp is not an exact minute end label: {parsed.isoformat()}")
    return parsed


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _fingerprint_safe(value: Any) -> Any:
    """Return a deterministic JSON value even for malformed numeric input.

    Non-finite prices/volume are invalid market data and are rejected by the
    reducer.  They still have to be hashable first so replay protection can
    fail closed through the normal DATA_INCOMPLETE transition instead of
    raising from ``json.dumps(..., allow_nan=False)``.
    """

    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            label = "NaN"
        elif value > 0:
            label = "+Infinity"
        else:
            label = "-Infinity"
        return {"__nonfinite_float__": label}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Mapping):
        return {str(key): _fingerprint_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_fingerprint_safe(item) for item in value]
    return value


def _fingerprint(value: Any) -> str:
    encoded = json.dumps(
        _fingerprint_safe(value),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _decimal(value: float) -> Decimal:
    return Decimal(str(float(value)))


def round_up_to_tick(value: float, tick_size: float) -> float:
    tick = _decimal(tick_size)
    if tick <= 0:
        raise ValueError("tick_size must be positive")
    return float((_decimal(value) / tick).to_integral_value(rounding=ROUND_CEILING) * tick)


def round_down_to_tick(value: float, tick_size: float) -> float:
    tick = _decimal(tick_size)
    if tick <= 0:
        raise ValueError("tick_size must be positive")
    return float((_decimal(value) / tick).to_integral_value(rounding=ROUND_FLOOR) * tick)


@dataclass(frozen=True)
class PaperCandidate:
    symbol: str
    signal_time: datetime
    five_min_open: float
    five_min_high: float
    five_min_low: float
    five_min_close: float
    price_change_pct: float
    oi_change_pct: float
    volume_ratio: float
    traded_value: float
    ema9: float
    ema20: float
    ema50: float
    oi: float
    prev_oi: float
    tick_size: float = 0.05
    equity_instrument_token: int = 0
    futures_instrument_token: int = 0
    futures_symbol: str = ""

    @classmethod
    def from_object(cls, value: "PaperCandidate" | Mapping[str, Any] | Any) -> "PaperCandidate":
        if isinstance(value, cls):
            return value

        def get(name: str, default: Any = None) -> Any:
            if isinstance(value, Mapping):
                return value.get(name, default)
            return getattr(value, name, default)

        signal = get("signal_time", get("signal_ts"))
        return cls(
            symbol=str(get("symbol", get("tradingsymbol"))).strip().upper(),
            signal_time=_exact_minute(signal),
            five_min_open=float(get("five_min_open", get("signal_open"))),
            five_min_high=float(get("five_min_high", get("signal_high"))),
            five_min_low=float(get("five_min_low", get("signal_low"))),
            five_min_close=float(get("five_min_close", get("signal_close"))),
            price_change_pct=float(get("price_change_pct")),
            oi_change_pct=float(get("oi_change_pct")),
            volume_ratio=float(get("volume_ratio")),
            traded_value=float(get("traded_value")),
            ema9=float(get("ema9")),
            ema20=float(get("ema20")),
            ema50=float(get("ema50")),
            oi=float(get("oi")),
            prev_oi=float(get("prev_oi")),
            tick_size=float(get("tick_size", 0.05)),
            equity_instrument_token=int(get("equity_instrument_token", get("instrument_token", 0))),
            futures_instrument_token=int(get("futures_instrument_token", 0)),
            futures_symbol=str(get("futures_symbol", get("futures_tradingsymbol", ""))),
        )

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass(frozen=True)
class CompletedMinuteBar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    gap_filled: bool = False
    opening_snapshot: bool = False
    provisional_stale: bool = False

    @classmethod
    def from_object(
        cls, value: "CompletedMinuteBar" | Mapping[str, Any] | Any
    ) -> "CompletedMinuteBar":
        if isinstance(value, cls):
            return value

        def get(name: str, default: Any = None) -> Any:
            if isinstance(value, Mapping):
                return value.get(name, default)
            return getattr(value, name, default)

        return cls(
            timestamp=_exact_minute(get("timestamp", get("bar_ts", get("ts")))),
            open=float(get("open")),
            high=float(get("high")),
            low=float(get("low")),
            close=float(get("close")),
            volume=float(get("volume", 0.0)),
            gap_filled=bool(get("gap_filled", False)),
            opening_snapshot=bool(get("opening_snapshot", False)),
            provisional_stale=bool(get("provisional_stale", False)),
        )

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


def valid_completed_bar(bar: CompletedMinuteBar) -> bool:
    prices = (bar.open, bar.high, bar.low, bar.close)
    return bool(
        all(math.isfinite(float(item)) and float(item) > 0 for item in prices)
        and float(bar.high) >= max(float(bar.open), float(bar.close))
        and float(bar.low) <= min(float(bar.open), float(bar.close))
        and float(bar.high) >= float(bar.low)
        and math.isfinite(float(bar.volume))
        and float(bar.volume) >= 0
        and not bar.gap_filled
        and not bar.opening_snapshot
        and not bar.provisional_stale
    )


@dataclass(frozen=True)
class PaperEvent:
    sequence: int
    event_time: datetime
    candidate_id: str
    setup_id: str
    symbol: str
    scope: str
    state_before: str
    state_after: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass
class _Runtime:
    candidate: PaperCandidate
    candidate_id: str
    frozen_rank: int
    picker_value: float
    state: str = CandidateState.MONITORING.value
    reason: str = ""
    confirmation_minute: int | None = None
    confirmation_time: datetime | None = None
    confirmation_bar: CompletedMinuteBar | None = None
    trigger: float | None = None
    order_placed_at: datetime | None = None
    entry_minute: int | None = None
    entry_time: datetime | None = None
    entry_price: float | None = None
    gap_fill: bool = False
    intrabar_trigger_fill: bool = False
    ambiguous_entry_bar: bool = False
    stop_price: float | None = None
    target_price: float | None = None
    exit_time: datetime | None = None
    exit_price: float | None = None
    exit_reason: str = ""
    exit_at_bar_open: bool = False
    gross_return_pct: float | None = None
    net_return_pct: float | None = None
    portfolio_decision: str = "NOT_APPLICABLE"
    portfolio_reject_reason: str = ""
    portfolio_active_at_reservation: int | None = None
    portfolio_reserved_margin_rs: float | None = None
    confirmation_checks: list[dict[str, Any]] = field(default_factory=list)
    transition_events: list[dict[str, Any]] = field(default_factory=list)

    @property
    def constrained_state(self) -> str:
        if self.portfolio_decision == "REJECTED":
            return (
                "DUPLICATE_REJECTED"
                if self.portfolio_reject_reason.startswith("DUPLICATE")
                else "PORTFOLIO_REJECTED"
            )
        return self.state


@dataclass
class _Occurrence:
    setup: SetupPolicy
    policy: ResolvedEntryPolicy
    signal_time: datetime
    runtimes: list[_Runtime]
    filled_cap: int = 0
    allocated_once: int = 0
    last_processed_time: datetime | None = None

    @property
    def key(self) -> str:
        return f"{self.signal_time.date().isoformat()}|{self.setup.setup_id}"

    def ranked(self) -> list[_Runtime]:
        return sorted(self.runtimes, key=lambda item: item.frozen_rank)

    def has_active_shadow(self) -> bool:
        return any(runtime.state in ACTIVE_STATES for runtime in self.runtimes)


@dataclass(frozen=True)
class _ReserveProposal:
    event_time: datetime
    signal_time: datetime
    setup_id: str
    frozen_rank: int
    symbol: str
    candidate_id: str
    runtime: _Runtime


@dataclass(frozen=True)
class _ReleaseAction:
    event_time: datetime
    signal_time: datetime
    setup_id: str
    frozen_rank: int
    symbol: str
    candidate_id: str
    runtime: _Runtime


def _candidate_id(candidate: PaperCandidate, setup_id: str) -> str:
    return f"{candidate.signal_time.date().isoformat()}|{setup_id}|{candidate.symbol}"


def _picker_value(setup: SetupPolicy, candidate: PaperCandidate) -> float:
    return {
        "max_oi": float(candidate.oi_change_pct),
        "max_volume": float(candidate.volume_ratio),
        "max_move": abs(float(candidate.price_change_pct)),
        "max_liquidity": float(candidate.traded_value),
    }[setup.picker]


def candidate_passes(setup: SetupPolicy, candidate: PaperCandidate) -> bool:
    prices = (
        candidate.five_min_open,
        candidate.five_min_high,
        candidate.five_min_low,
        candidate.five_min_close,
    )
    metrics = (
        candidate.price_change_pct,
        candidate.oi_change_pct,
        candidate.volume_ratio,
        candidate.traded_value,
        candidate.ema9,
        candidate.ema20,
        candidate.ema50,
        candidate.oi,
        candidate.prev_oi,
        candidate.tick_size,
    )
    if not all(math.isfinite(float(item)) for item in prices + metrics):
        return False
    if not all(float(item) > 0 for item in prices):
        return False
    if candidate.five_min_high < max(candidate.five_min_open, candidate.five_min_close):
        return False
    if candidate.five_min_low > min(candidate.five_min_open, candidate.five_min_close):
        return False
    if candidate.five_min_high < candidate.five_min_low or candidate.tick_size <= 0:
        return False
    if (
        candidate.oi <= 0
        or candidate.prev_oi <= 0
        or candidate.oi <= candidate.prev_oi
        or candidate.oi_change_pct < BASE_OI_CHANGE_PCT
    ):
        return False
    if candidate.volume_ratio < BASE_VOLUME_RATIO:
        return False
    if setup.side == "LONG":
        broad = (
            candidate.ema9 > candidate.ema20 > candidate.ema50
            and candidate.price_change_pct >= BASE_PRICE_CHANGE_PCT
        )
        price_ok = candidate.price_change_pct >= setup.price_change_pct
    else:
        broad = (
            candidate.ema9 < candidate.ema20 < candidate.ema50
            and candidate.price_change_pct <= -BASE_PRICE_CHANGE_PCT
        )
        price_ok = candidate.price_change_pct <= -setup.price_change_pct
    return bool(
        broad
        and price_ok
        and candidate.oi_change_pct >= setup.oi_change_pct
        and candidate.volume_ratio >= setup.volume_ratio
        and candidate.traded_value >= setup.min_traded_value
    )


def confirmation_check(
    setup: SetupPolicy,
    candidate: PaperCandidate,
    bar: CompletedMinuteBar,
    policy: ResolvedEntryPolicy,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "timestamp": bar.timestamp.isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "candle_range": None,
        "body_ratio": None,
        "adverse_wick_ratio": None,
        "close_location": None,
        "passed": False,
        "rejection_codes": [],
    }
    if not valid_completed_bar(bar):
        record["rejection_codes"] = ["INVALID_BAR"]
        return record
    candle_range = bar.high - bar.low
    record["candle_range"] = candle_range
    if candle_range <= 0:
        record["rejection_codes"] = ["NONPOSITIVE_RANGE"]
        return record
    body_ratio = abs(bar.close - bar.open) / candle_range
    if setup.side == "LONG":
        direction_ok = bar.close > bar.open
        beyond = bar.close > candidate.five_min_close
        adverse_wick = (bar.high - max(bar.open, bar.close)) / candle_range
        close_location = (bar.close - bar.low) / candle_range
    else:
        direction_ok = bar.close < bar.open
        beyond = bar.close < candidate.five_min_close
        adverse_wick = (min(bar.open, bar.close) - bar.low) / candle_range
        close_location = (bar.high - bar.close) / candle_range
    record.update(
        body_ratio=body_ratio,
        adverse_wick_ratio=adverse_wick,
        close_location=close_location,
    )
    rejected: list[str] = []
    if not direction_ok:
        rejected.append("WRONG_CANDLE_DIRECTION")
    if not beyond:
        rejected.append("CLOSE_NOT_BEYOND_FIVE_MINUTE_CLOSE")
    if body_ratio + 1e-12 < setup.body_ratio:
        rejected.append("BODY_RATIO_BELOW_MINIMUM")
    if adverse_wick - 1e-12 > setup.max_wick_ratio:
        rejected.append("ADVERSE_WICK_RATIO_ABOVE_MAXIMUM")
    if policy.close_location_min is not None and (
        close_location + 1e-12 < policy.close_location_min
    ):
        rejected.append("CLOSE_LOCATION_BELOW_MINIMUM")
    record["rejection_codes"] = rejected
    record["passed"] = not rejected
    return record


def build_trigger(
    setup: SetupPolicy,
    bar: CompletedMinuteBar,
    policy: ResolvedEntryPolicy,
    tick_size: float,
) -> float:
    scale = policy.buffer_bps / 10_000.0
    if setup.side == "LONG":
        return round_up_to_tick(bar.high * (1.0 + scale), tick_size)
    return round_down_to_tick(bar.low * (1.0 - scale), tick_size)


class V8CombinedPaperEngine:
    """Single-writer incremental V8-Combined PAPER state machine."""

    def __init__(self, config: PaperEngineConfig | None = None) -> None:
        supplied = config or PaperEngineConfig.from_module()
        supplied.validate()
        # ``PaperEngineConfig`` is frozen, but a caller could still mutate a
        # plain dict stored in its Mapping field.  Snapshot that mapping at the
        # reducer boundary so policy cannot change mid-session.
        self.config = PaperEngineConfig(
            setups=tuple(supplied.setups),
            entry_policies=MappingProxyType(dict(supplied.entry_policies)),
            portfolio_policy=supplied.portfolio_policy,
            setup_book_sha256=supplied.setup_book_sha256,
            strategy_fingerprint=supplied.strategy_fingerprint,
        )
        self._occurrences: dict[str, _Occurrence] = {}
        self._runtime_by_id: dict[str, _Runtime] = {}
        self._registration_fingerprints: dict[str, str] = {}
        self._processed_minute_fingerprints: dict[str, str] = {}
        self._intervention_fingerprints: dict[str, str] = {}
        self._last_submitted_time: datetime | None = None
        self._active_by_symbol: dict[str, str] = {}
        self._events: list[PaperEvent] = []
        self._sequence = 0

    def register_candidates(
        self,
        setup_id: str,
        signal_time: Any,
        candidates: Sequence[PaperCandidate | Mapping[str, Any] | Any],
    ) -> list[PaperEvent]:
        setup_key = str(setup_id).strip().upper()
        setup = self.config.setup_by_id.get(setup_key)
        if setup is None:
            raise KeyError(f"unknown V8-Combined setup {setup_id!r}")
        signal = _exact_minute(signal_time)
        if signal.strftime("%H:%M") != setup.signal_end:
            raise ValueError("signal_time does not match setup.signal_end")
        if self._last_submitted_time is not None and self._last_submitted_time >= signal + timedelta(minutes=1):
            raise OutOfOrderMinuteError("cannot register a setup after its S+1 decision bar")
        normalized = [PaperCandidate.from_object(item) for item in candidates]
        if any(item.signal_time != signal for item in normalized):
            raise ValueError("every candidate must carry the occurrence signal_time")
        symbols = [item.symbol for item in normalized]
        if len(symbols) != len(set(symbols)):
            raise ValueError("duplicate candidate symbol in setup occurrence")
        rejected = [item.symbol for item in normalized if not candidate_passes(setup, item)]
        if rejected:
            raise ValueError(f"candidates fail frozen five-minute authority: {sorted(rejected)}")
        normalized.sort(
            key=lambda item: (-_picker_value(setup, item), -item.traded_value, item.symbol)
        )
        occurrence_key = f"{signal.date().isoformat()}|{setup.setup_id}"
        payload = {
            "setup_id": setup.setup_id,
            "signal_time": signal,
            "candidates": [item.to_dict() for item in normalized],
        }
        fingerprint = _fingerprint(payload)
        prior = self._registration_fingerprints.get(occurrence_key)
        if prior is not None:
            if prior != fingerprint:
                raise ReplayConflictError(f"candidate registration changed: {occurrence_key}")
            return []
        runtimes = [
            _Runtime(
                candidate=item,
                candidate_id=_candidate_id(item, setup.setup_id),
                frozen_rank=rank,
                picker_value=_picker_value(setup, item),
            )
            for rank, item in enumerate(normalized, start=1)
        ]
        occurrence = _Occurrence(
            setup=setup,
            policy=self.config.entry_policies[setup.setup_id],
            signal_time=signal,
            runtimes=runtimes,
            last_processed_time=signal,
        )
        self._occurrences[occurrence_key] = occurrence
        self._registration_fingerprints[occurrence_key] = fingerprint
        for runtime in runtimes:
            self._runtime_by_id[runtime.candidate_id] = runtime
        return []

    def process_completed_minute(
        self,
        timestamp: Any,
        bars_by_symbol: Mapping[str, CompletedMinuteBar | Mapping[str, Any] | Any],
    ) -> list[PaperEvent]:
        event_time = _exact_minute(timestamp)
        bars = {
            str(symbol).strip().upper(): CompletedMinuteBar.from_object(bar)
            for symbol, bar in bars_by_symbol.items()
        }
        mismatched = [symbol for symbol, bar in bars.items() if bar.timestamp != event_time]
        if mismatched:
            raise ValueError(f"bars do not match completed minute: {sorted(mismatched)}")
        payload = {
            "timestamp": event_time,
            "bars": {symbol: bar.to_dict() for symbol, bar in sorted(bars.items())},
        }
        fingerprint = _fingerprint(payload)
        minute_key = event_time.isoformat()
        prior = self._processed_minute_fingerprints.get(minute_key)
        if prior is not None:
            if prior != fingerprint:
                raise ReplayConflictError(f"completed minute changed: {minute_key}")
            return []
        if self._last_submitted_time is not None and event_time <= self._last_submitted_time:
            raise OutOfOrderMinuteError(
                f"completed minute {minute_key} follows {self._last_submitted_time.isoformat()}"
            )
        before = len(self._events)
        releases: list[_ReleaseAction] = []
        reserves: list[_ReserveProposal] = []
        for occurrence in self._ordered_occurrences():
            if event_time.date() != occurrence.signal_time.date():
                continue
            if event_time <= occurrence.signal_time or not occurrence.has_active_shadow():
                continue
            expected = (occurrence.last_processed_time or occurrence.signal_time) + timedelta(minutes=1)
            if event_time > expected:
                self._process_occurrence_minute(occurrence, expected, {}, releases, reserves)
                occurrence.last_processed_time = expected
                continue
            if event_time < expected:
                continue
            self._process_occurrence_minute(occurrence, event_time, bars, releases, reserves)
            occurrence.last_processed_time = event_time

        self._apply_releases(releases)
        self._apply_reserves(reserves)
        self._processed_minute_fingerprints[minute_key] = fingerprint
        self._last_submitted_time = event_time
        return list(self._events[before:])

    def terminate_for_intervention(
        self,
        timestamp: Any,
        bars_by_symbol: Mapping[str, CompletedMinuteBar | Mapping[str, Any] | Any],
        reason: str,
    ) -> list[PaperEvent]:
        """Fail closed after a kill/revoke using only completed-bar economics.

        Monitoring, waiting and pending states are cancelled.  A modeled open
        position is closed only when the caller supplies a valid completed bar
        for that symbol at ``timestamp``; the exact bar close is used.  The
        method validates every required close before mutating any state, so a
        partial intervention cannot invent or strand paper economics.
        """

        event_time = _exact_minute(timestamp)
        why = str(reason).strip()
        if not why:
            raise ValueError("intervention reason must be nonempty")
        bars = {
            str(symbol).strip().upper(): CompletedMinuteBar.from_object(value)
            for symbol, value in bars_by_symbol.items()
        }
        mismatched = [symbol for symbol, value in bars.items() if value.timestamp != event_time]
        if mismatched:
            raise ValueError(f"intervention bars do not match timestamp: {sorted(mismatched)}")
        if self._last_submitted_time is not None and event_time < self._last_submitted_time:
            raise OutOfOrderMinuteError("intervention predates the last completed minute")
        payload = {
            "timestamp": event_time,
            "reason": why,
            "bars": {symbol: value.to_dict() for symbol, value in sorted(bars.items())},
        }
        fingerprint = _fingerprint(payload)
        key = event_time.isoformat()
        prior = self._intervention_fingerprints.get(key)
        if prior is not None:
            if prior != fingerprint:
                raise ReplayConflictError(f"intervention changed: {key}")
            return []

        open_runtimes = [
            runtime
            for occurrence in self._occurrences.values()
            for runtime in occurrence.runtimes
            if runtime.state == CandidateState.FILLED_OPEN.value
            and runtime.portfolio_decision == "ACCEPTED"
        ]
        invalid_open = [
            runtime.candidate.symbol
            for runtime in open_runtimes
            if runtime.candidate.symbol not in bars
            or not valid_completed_bar(bars[runtime.candidate.symbol])
        ]
        if invalid_open:
            raise PaperEngineError(
                "intervention requires a valid completed bar for every modeled open: "
                f"{sorted(set(invalid_open))}"
            )

        before = len(self._events)
        releases: list[_ReleaseAction] = []
        for occurrence in self._ordered_occurrences():
            for runtime in occurrence.ranked():
                prior_state = runtime.state
                if (
                    prior_state == CandidateState.FILLED_OPEN.value
                    and runtime.portfolio_decision == "ACCEPTED"
                ):
                    self._close_for_intervention(
                        occurrence,
                        runtime,
                        event_time,
                        bars[runtime.candidate.symbol].close,
                        why,
                    )
                    releases.append(self._release_action(occurrence, runtime, event_time))
                elif prior_state == CandidateState.FILLED_OPEN.value:
                    # A globally rejected unconstrained shadow is not a PAPER
                    # portfolio position.  End it without inventing a close.
                    self._transition(
                        occurrence,
                        runtime,
                        CandidateState.INTERVENTION_CANCELLED.value,
                        event_time,
                        f"INTERVENTION:{why}:REJECTED_SHADOW_ONLY",
                    )
                elif prior_state in {
                    CandidateState.MONITORING.value,
                    CandidateState.CONFIRMED_WAITING_CAP.value,
                    CandidateState.PENDING_STOP.value,
                }:
                    self._transition(
                        occurrence,
                        runtime,
                        CandidateState.INTERVENTION_CANCELLED.value,
                        event_time,
                        f"INTERVENTION:{why}",
                    )
                    if prior_state == CandidateState.PENDING_STOP.value:
                        releases.append(self._release_action(occurrence, runtime, event_time))
                occurrence.last_processed_time = max(
                    occurrence.last_processed_time or occurrence.signal_time,
                    event_time,
                )
        self._apply_releases(releases)
        self._intervention_fingerprints[key] = fingerprint
        if self._last_submitted_time is None or event_time > self._last_submitted_time:
            self._last_submitted_time = event_time
        return list(self._events[before:])

    def _ordered_occurrences(self) -> list[_Occurrence]:
        return sorted(
            self._occurrences.values(),
            key=lambda item: (item.signal_time, item.setup.setup_id),
        )

    def _process_occurrence_minute(
        self,
        occurrence: _Occurrence,
        event_time: datetime,
        bars: Mapping[str, CompletedMinuteBar],
        releases: list[_ReleaseAction],
        reserves: list[_ReserveProposal],
    ) -> None:
        minute_index = int((event_time - occurrence.signal_time).total_seconds() // 60)
        if minute_index <= 0:
            return
        setup = occurrence.setup
        policy = occurrence.policy

        # Validate only candidates whose unconstrained state still needs this
        # completed bar.  Later missing data cannot erase an earlier terminal.
        for runtime in occurrence.ranked():
            if runtime.state not in ACTIVE_STATES:
                continue
            if minute_index > policy.entry_expiry_minute and runtime.state != CandidateState.FILLED_OPEN.value:
                continue
            bar = bars.get(runtime.candidate.symbol)
            if bar is not None and valid_completed_bar(bar):
                continue
            prior = runtime.state
            self._transition(
                occurrence,
                runtime,
                CandidateState.DATA_INCOMPLETE.value,
                event_time,
                "MISSING_REQUIRED_MINUTE_BAR" if bar is None else "INVALID_REQUIRED_MINUTE_BAR",
            )
            if prior in {CandidateState.PENDING_STOP.value, CandidateState.FILLED_OPEN.value}:
                releases.append(self._release_action(occurrence, runtime, event_time))

        if minute_index <= policy.entry_expiry_minute:
            # 1) Only orders placed after an earlier completed bar may fill.
            for runtime in occurrence.ranked():
                if runtime.state != CandidateState.PENDING_STOP.value:
                    continue
                if runtime.order_placed_at is None or event_time <= runtime.order_placed_at:
                    continue
                bar = bars[runtime.candidate.symbol]
                fill = self._entry_fill(setup, policy, runtime, bar)
                if fill is None:
                    continue
                entry_price, gap_fill = fill
                runtime.entry_minute = minute_index
                runtime.entry_time = event_time
                runtime.entry_price = entry_price
                runtime.gap_fill = gap_fill
                runtime.intrabar_trigger_fill = not gap_fill
                runtime.stop_price, runtime.target_price = self._brackets(
                    setup, runtime.candidate, entry_price
                )
                occurrence.filled_cap += 1
                self._transition(
                    occurrence,
                    runtime,
                    CandidateState.FILLED_OPEN.value,
                    event_time,
                    "GAP_FILL" if gap_fill else "TRIGGER_TOUCH_FILL",
                )
                immediate = self._exit_on_bar(setup, runtime, bar, open_at_start=False)
                if immediate is not None:
                    runtime.ambiguous_entry_bar = True
                    reason, price = immediate
                    self._close_runtime(occurrence, runtime, event_time, price, reason)
                    releases.append(self._release_action(occurrence, runtime, event_time))

            # 2) Resolve positions that were open before this bar.
            for runtime in occurrence.ranked():
                if runtime.state != CandidateState.FILLED_OPEN.value:
                    continue
                if runtime.entry_time == event_time:
                    continue
                event = self._exit_on_bar(
                    setup, runtime, bars[runtime.candidate.symbol], open_at_start=True
                )
                if event is None:
                    continue
                runtime.exit_at_bar_open = self._exit_occurs_at_bar_open(
                    setup, runtime, bars[runtime.candidate.symbol]
                )
                reason, price = event
                self._close_runtime(occurrence, runtime, event_time, price, reason)
                releases.append(self._release_action(occurrence, runtime, event_time))

            # 3) A close cancellation is known only after fill processing.
            for runtime in occurrence.ranked():
                if runtime.state not in {
                    CandidateState.PENDING_STOP.value,
                    CandidateState.CONFIRMED_WAITING_CAP.value,
                }:
                    continue
                bar = bars[runtime.candidate.symbol]
                if policy.post_confirmation_cancel and self._postconfirm_invalidated(
                    setup, runtime.candidate, bar
                ):
                    was_pending = runtime.state == CandidateState.PENDING_STOP.value
                    self._transition(
                        occurrence,
                        runtime,
                        CandidateState.POSTCONF_CANCELLED.value,
                        event_time,
                        "CLOSE_REVERSED_THROUGH_SIGNAL_CLOSE",
                    )
                    if was_pending:
                        releases.append(self._release_action(occurrence, runtime, event_time))

            # 4) Midpoint invalidation precedes first strict confirmation.
            for runtime in occurrence.ranked():
                if runtime.state != CandidateState.MONITORING.value:
                    continue
                bar = bars[runtime.candidate.symbol]
                check: dict[str, Any] | None = None
                if minute_index <= policy.max_confirmation_minute:
                    check = confirmation_check(setup, runtime.candidate, bar, policy)
                    check["minute_index"] = minute_index
                    check["gate_evaluated"] = True
                if policy.midpoint_invalidation and self._preconfirm_invalidated(
                    setup, runtime.candidate, bar
                ):
                    if check is not None:
                        check["gate_evaluated"] = False
                        check["passed"] = False
                        check["rejection_codes"] = ["PRECONF_MIDPOINT_INVALIDATED"]
                        runtime.confirmation_checks.append(check)
                    self._transition(
                        occurrence,
                        runtime,
                        CandidateState.PRECONF_INVALIDATED.value,
                        event_time,
                        "CLOSE_CROSSED_FIVE_MINUTE_MIDPOINT",
                    )
                    continue
                if check is not None:
                    runtime.confirmation_checks.append(check)
                if check is not None and bool(check["passed"]):
                    runtime.confirmation_minute = minute_index
                    runtime.confirmation_time = event_time
                    runtime.confirmation_bar = bar
                    runtime.trigger = build_trigger(
                        setup, bar, policy, runtime.candidate.tick_size
                    )
                    self._transition(
                        occurrence,
                        runtime,
                        CandidateState.CONFIRMED_WAITING_CAP.value,
                        event_time,
                        "FIRST_STRICT_CONFIRMATION",
                    )

            if minute_index == policy.max_confirmation_minute:
                for runtime in occurrence.ranked():
                    if runtime.state == CandidateState.MONITORING.value:
                        self._transition(
                            occurrence,
                            runtime,
                            NO_CONFIRMATION_VALUE,
                            event_time,
                            "CONFIRMATION_WINDOW_EXPIRED",
                        )

            # 5) Recompute local capacity.  A globally rejected selection stays
            # in the unconstrained shadow, reproducing conservative no-backfill.
            if minute_index < policy.entry_expiry_minute:
                pending_count = sum(
                    runtime.state == CandidateState.PENDING_STOP.value
                    for runtime in occurrence.runtimes
                )
                capacity_used = (
                    occurrence.filled_cap + pending_count
                    if policy.allow_cap_reassignment
                    else occurrence.allocated_once
                )
                available = max(0, setup.max_entries - capacity_used)
                waiting = [
                    runtime
                    for runtime in occurrence.ranked()
                    if runtime.state == CandidateState.CONFIRMED_WAITING_CAP.value
                ]
                for runtime in waiting[:available]:
                    runtime.order_placed_at = event_time
                    occurrence.allocated_once += 1
                    self._transition(
                        occurrence,
                        runtime,
                        CandidateState.PENDING_STOP.value,
                        event_time,
                        "CAP_RESERVED_BY_FROZEN_RANK_AMONG_CONFIRMED",
                    )
                    reserves.append(self._reserve_proposal(occurrence, runtime, event_time))

            # 6) Existing S+5 triggers were handled; now expire remainder.
            if minute_index == policy.entry_expiry_minute:
                for runtime in occurrence.ranked():
                    if runtime.state not in {
                        CandidateState.PENDING_STOP.value,
                        CandidateState.CONFIRMED_WAITING_CAP.value,
                        CandidateState.MONITORING.value,
                    }:
                        continue
                    was_pending = runtime.state == CandidateState.PENDING_STOP.value
                    target = (
                        NO_CONFIRMATION_VALUE
                        if runtime.state == CandidateState.MONITORING.value
                        else CandidateState.WINDOW_EXPIRED.value
                    )
                    reason = (
                        "NO_STRICT_CONFIRMATION"
                        if runtime.state == CandidateState.MONITORING.value
                        else "ENTRY_WINDOW_EXPIRED"
                    )
                    self._transition(occurrence, runtime, target, event_time, reason)
                    if was_pending:
                        releases.append(self._release_action(occurrence, runtime, event_time))

        else:
            # Consecutive post-window path for still-open positions.
            for runtime in occurrence.ranked():
                if runtime.state != CandidateState.FILLED_OPEN.value:
                    continue
                bar = bars[runtime.candidate.symbol]
                event = self._exit_on_bar(setup, runtime, bar, open_at_start=True)
                if event is not None:
                    runtime.exit_at_bar_open = self._exit_occurs_at_bar_open(
                        setup, runtime, bar
                    )
                    reason, price = event
                    self._close_runtime(occurrence, runtime, event_time, price, reason)
                    releases.append(self._release_action(occurrence, runtime, event_time))

        cutoff = datetime.combine(event_time.date(), time.fromisoformat(policy.square_off), IST)
        if event_time == cutoff:
            for runtime in occurrence.ranked():
                if runtime.state != CandidateState.FILLED_OPEN.value:
                    continue
                bar = bars[runtime.candidate.symbol]
                self._close_runtime(
                    occurrence, runtime, event_time, float(bar.close), "SQUARE_OFF"
                )
                releases.append(self._release_action(occurrence, runtime, event_time))

    def _transition(
        self,
        occurrence: _Occurrence,
        runtime: _Runtime,
        new_state: str,
        event_time: datetime,
        reason: str,
    ) -> None:
        before = runtime.state
        runtime.state = new_state
        runtime.reason = reason
        payload = {
            "event_ts": event_time.isoformat(),
            "state_before": before,
            "state_after": new_state,
            "reason": reason,
        }
        runtime.transition_events.append(payload)
        self._emit(occurrence, runtime, event_time, "LOCAL_SHADOW", before, new_state, reason)

    def _emit(
        self,
        occurrence: _Occurrence,
        runtime: _Runtime,
        event_time: datetime,
        scope: str,
        before: str,
        after: str,
        reason: str,
    ) -> None:
        self._sequence += 1
        self._events.append(
            PaperEvent(
                sequence=self._sequence,
                event_time=event_time,
                candidate_id=runtime.candidate_id,
                setup_id=occurrence.setup.setup_id,
                symbol=runtime.candidate.symbol,
                scope=scope,
                state_before=before,
                state_after=after,
                reason=reason,
            )
        )

    def _reserve_proposal(
        self, occurrence: _Occurrence, runtime: _Runtime, event_time: datetime
    ) -> _ReserveProposal:
        return _ReserveProposal(
            event_time,
            occurrence.signal_time,
            occurrence.setup.setup_id,
            runtime.frozen_rank,
            runtime.candidate.symbol,
            runtime.candidate_id,
            runtime,
        )

    def _release_action(
        self, occurrence: _Occurrence, runtime: _Runtime, event_time: datetime
    ) -> _ReleaseAction:
        return _ReleaseAction(
            event_time,
            occurrence.signal_time,
            occurrence.setup.setup_id,
            runtime.frozen_rank,
            runtime.candidate.symbol,
            runtime.candidate_id,
            runtime,
        )

    @staticmethod
    def _action_key(action: _ReserveProposal | _ReleaseAction) -> tuple[Any, ...]:
        return (
            action.event_time,
            action.signal_time,
            action.setup_id,
            action.frozen_rank,
            action.symbol,
            action.candidate_id,
        )

    def _apply_releases(self, actions: Sequence[_ReleaseAction]) -> None:
        for action in sorted(actions, key=self._action_key):
            runtime = action.runtime
            if runtime.portfolio_decision != "ACCEPTED":
                continue
            if self._active_by_symbol.get(action.symbol) != action.candidate_id:
                continue
            self._active_by_symbol.pop(action.symbol, None)
            self._emit(
                self._occurrences[
                    f"{action.signal_time.date().isoformat()}|{action.setup_id}"
                ],
                runtime,
                action.event_time,
                "PORTFOLIO",
                "RESERVED",
                "RELEASED",
                "TERMINAL_LOCAL_STATE",
            )

    def _apply_reserves(self, actions: Sequence[_ReserveProposal]) -> None:
        portfolio = self.config.portfolio_policy
        for action in sorted(actions, key=self._action_key):
            runtime = action.runtime
            occurrence = self._occurrences[
                f"{action.signal_time.date().isoformat()}|{action.setup_id}"
            ]
            if runtime.portfolio_decision != "NOT_APPLICABLE":
                continue
            if action.symbol in self._active_by_symbol:
                reason = "DUPLICATE_SYMBOL_PENDING_OR_OPEN"
            elif len(self._active_by_symbol) >= portfolio.capacity:
                reason = "CAPITAL_MARGIN_OR_CONCURRENCY_LIMIT"
            else:
                runtime.portfolio_decision = "ACCEPTED"
                self._active_by_symbol[action.symbol] = action.candidate_id
                runtime.portfolio_active_at_reservation = len(self._active_by_symbol)
                runtime.portfolio_reserved_margin_rs = (
                    len(self._active_by_symbol) * portfolio.margin_per_entry_rs
                )
                self._emit(
                    occurrence,
                    runtime,
                    action.event_time,
                    "PORTFOLIO",
                    "UNRESERVED",
                    "RESERVED",
                    "PORTFOLIO_ACCEPTED",
                )
                continue
            runtime.portfolio_decision = "REJECTED"
            runtime.portfolio_reject_reason = reason
            self._emit(
                occurrence,
                runtime,
                action.event_time,
                "PORTFOLIO",
                CandidateState.CONFIRMED_WAITING_CAP.value,
                runtime.constrained_state,
                f"{reason}:CONSERVATIVE_NO_BACKFILL",
            )

    @staticmethod
    def _preconfirm_invalidated(
        setup: SetupPolicy, candidate: PaperCandidate, bar: CompletedMinuteBar
    ) -> bool:
        midpoint = (candidate.five_min_high + candidate.five_min_low) / 2.0
        return bar.close < midpoint if setup.side == "LONG" else bar.close > midpoint

    @staticmethod
    def _postconfirm_invalidated(
        setup: SetupPolicy, candidate: PaperCandidate, bar: CompletedMinuteBar
    ) -> bool:
        return (
            bar.close < candidate.five_min_close
            if setup.side == "LONG"
            else bar.close > candidate.five_min_close
        )

    @staticmethod
    def _entry_fill(
        setup: SetupPolicy,
        policy: ResolvedEntryPolicy,
        runtime: _Runtime,
        bar: CompletedMinuteBar,
    ) -> tuple[float, bool] | None:
        assert runtime.trigger is not None
        trigger = runtime.trigger
        slip = policy.slippage_bps / 10_000.0
        tick = runtime.candidate.tick_size
        if setup.side == "LONG":
            if bar.open >= trigger:
                return round_up_to_tick(bar.open * (1.0 + slip), tick), True
            if bar.high >= trigger:
                return round_up_to_tick(trigger * (1.0 + slip), tick), False
        else:
            if bar.open <= trigger:
                return round_down_to_tick(bar.open * (1.0 - slip), tick), True
            if bar.low <= trigger:
                return round_down_to_tick(trigger * (1.0 - slip), tick), False
        return None

    @staticmethod
    def _brackets(
        setup: SetupPolicy, candidate: PaperCandidate, entry_price: float
    ) -> tuple[float, float]:
        if setup.side == "LONG":
            return (
                round_down_to_tick(entry_price * (1 - setup.stop_pct / 100), candidate.tick_size),
                round_down_to_tick(entry_price * (1 + setup.target_pct / 100), candidate.tick_size),
            )
        return (
            round_up_to_tick(entry_price * (1 + setup.stop_pct / 100), candidate.tick_size),
            round_up_to_tick(entry_price * (1 - setup.target_pct / 100), candidate.tick_size),
        )

    @staticmethod
    def _exit_on_bar(
        setup: SetupPolicy,
        runtime: _Runtime,
        bar: CompletedMinuteBar,
        *,
        open_at_start: bool,
    ) -> tuple[str, float] | None:
        assert runtime.stop_price is not None and runtime.target_price is not None
        stop, target = runtime.stop_price, runtime.target_price
        tick = runtime.candidate.tick_size
        if setup.side == "LONG":
            if open_at_start and bar.open <= stop:
                return "STOP_GAP", round_down_to_tick(bar.open, tick)
            if open_at_start and bar.open >= target:
                return "TARGET", target
            stop_hit, target_hit = bar.low <= stop, bar.high >= target
        else:
            if open_at_start and bar.open >= stop:
                return "STOP_GAP", round_up_to_tick(bar.open, tick)
            if open_at_start and bar.open <= target:
                return "TARGET", target
            stop_hit, target_hit = bar.high >= stop, bar.low <= target
        if stop_hit:
            return "STOP", stop
        if target_hit:
            return "TARGET", target
        return None

    @staticmethod
    def _exit_occurs_at_bar_open(
        setup: SetupPolicy, runtime: _Runtime, bar: CompletedMinuteBar
    ) -> bool:
        assert runtime.stop_price is not None and runtime.target_price is not None
        if setup.side == "LONG":
            return bar.open <= runtime.stop_price or bar.open >= runtime.target_price
        return bar.open >= runtime.stop_price or bar.open <= runtime.target_price

    def _close_runtime(
        self,
        occurrence: _Occurrence,
        runtime: _Runtime,
        event_time: datetime,
        exit_price: float,
        reason: str,
    ) -> None:
        assert runtime.entry_price is not None
        gross = (
            exit_price / runtime.entry_price - 1.0
            if occurrence.setup.side == "LONG"
            else 1.0 - exit_price / runtime.entry_price
        ) * 100.0
        runtime.exit_time = event_time
        runtime.exit_price = float(exit_price)
        runtime.exit_reason = reason
        runtime.gross_return_pct = gross
        runtime.net_return_pct = gross - occurrence.policy.cost_bps / 100.0
        target_state = (
            CandidateState.STOPPED.value
            if reason.startswith("STOP")
            else CandidateState.TARGETED.value
            if reason == "TARGET"
            else CandidateState.SQUARE_OFF.value
        )
        self._transition(occurrence, runtime, target_state, event_time, reason)

    def _close_for_intervention(
        self,
        occurrence: _Occurrence,
        runtime: _Runtime,
        event_time: datetime,
        exit_price: float,
        reason: str,
    ) -> None:
        assert runtime.entry_price is not None
        gross = (
            exit_price / runtime.entry_price - 1.0
            if occurrence.setup.side == "LONG"
            else 1.0 - exit_price / runtime.entry_price
        ) * 100.0
        runtime.exit_time = event_time
        runtime.exit_price = float(exit_price)
        runtime.exit_reason = f"INTERVENTION:{reason}"
        runtime.gross_return_pct = gross
        runtime.net_return_pct = gross - occurrence.policy.cost_bps / 100.0
        self._transition(
            occurrence,
            runtime,
            CandidateState.INTERVENTION_CLOSED.value,
            event_time,
            runtime.exit_reason,
        )

    def records(self) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        target = self.config.portfolio_policy.target_exposure_per_entry_rs
        for occurrence in self._ordered_occurrences():
            for runtime in occurrence.ranked():
                rejected = runtime.portfolio_decision == "REJECTED"
                entry = None if rejected else runtime.entry_price
                exit_price = None if rejected else runtime.exit_price
                quantity = int(math.floor(target / entry)) if entry else 0
                gross_pnl = None
                estimated_cost = None
                net_pnl = None
                if entry is not None and exit_price is not None:
                    direction = 1.0 if occurrence.setup.side == "LONG" else -1.0
                    gross_pnl = direction * (exit_price - entry) * quantity
                    estimated_cost = entry * quantity * occurrence.policy.cost_bps / 10_000.0
                    net_pnl = gross_pnl - estimated_cost
                unconstrained_events = _json_safe(runtime.transition_events)
                constrained_events = (
                    self._constrained_transition_events(runtime)
                    if rejected
                    else unconstrained_events
                )
                output.append(
                    {
                        "candidate_id": runtime.candidate_id,
                        "session_date": occurrence.signal_time.date().isoformat(),
                        "signal_time": occurrence.signal_time.isoformat(),
                        "setup_id": occurrence.setup.setup_id,
                        "side": occurrence.setup.side,
                        "symbol": runtime.candidate.symbol,
                        "frozen_rank": runtime.frozen_rank,
                        "picker": occurrence.setup.picker,
                        "picker_value": runtime.picker_value,
                        "status": runtime.constrained_state,
                        "reason": (
                            f"{runtime.portfolio_reject_reason}:CONSERVATIVE_NO_BACKFILL"
                            if rejected
                            else runtime.reason
                        ),
                        "unconstrained_status": runtime.state,
                        "unconstrained_reason": runtime.reason,
                        "portfolio_decision": runtime.portfolio_decision,
                        "portfolio_reject_reason": runtime.portfolio_reject_reason,
                        "portfolio_active_at_reservation": runtime.portfolio_active_at_reservation,
                        "portfolio_reserved_margin_rs": runtime.portfolio_reserved_margin_rs,
                        "confirmation_minute": runtime.confirmation_minute,
                        "confirmation_time": (
                            runtime.confirmation_time.isoformat()
                            if runtime.confirmation_time is not None
                            else None
                        ),
                        "trigger": runtime.trigger,
                        "entry_minute": None if rejected else runtime.entry_minute,
                        "entry_time": (
                            None if rejected or runtime.entry_time is None else runtime.entry_time.isoformat()
                        ),
                        "entry_price": entry,
                        "gap_fill": False if rejected else runtime.gap_fill,
                        "intrabar_trigger_fill": (
                            False if rejected else runtime.intrabar_trigger_fill
                        ),
                        "ambiguous_entry_bar": False if rejected else runtime.ambiguous_entry_bar,
                        "stop_price": None if rejected else runtime.stop_price,
                        "target_price": None if rejected else runtime.target_price,
                        "exit_time": (
                            None if rejected or runtime.exit_time is None else runtime.exit_time.isoformat()
                        ),
                        "exit_price": exit_price,
                        "exit_reason": "" if rejected else runtime.exit_reason,
                        "exit_at_bar_open": False if rejected else runtime.exit_at_bar_open,
                        "gross_return_pct": None if rejected else runtime.gross_return_pct,
                        "net_return_pct": None if rejected else runtime.net_return_pct,
                        "quantity": quantity,
                        "gross_pnl_rs": gross_pnl,
                        "estimated_cost_rs": estimated_cost,
                        "net_pnl_rs": net_pnl,
                        "confirmation_checks": _json_safe(runtime.confirmation_checks),
                        "event_count": len(constrained_events),
                        "events": constrained_events,
                        "unconstrained_events": unconstrained_events,
                    }
                )
        return output

    @staticmethod
    def _constrained_transition_events(runtime: _Runtime) -> list[dict[str, Any]]:
        reason = f"{runtime.portfolio_reject_reason}:CONSERVATIVE_NO_BACKFILL"
        terminal = runtime.constrained_state
        events: list[dict[str, Any]] = []
        for event in runtime.transition_events:
            if event.get("state_after") == CandidateState.PENDING_STOP.value:
                events.append(
                    {
                        "event_ts": event["event_ts"],
                        "state_before": CandidateState.CONFIRMED_WAITING_CAP.value,
                        "state_after": terminal,
                        "reason": reason,
                    }
                )
                break
            events.append(dict(event))
        return events

    def events(self) -> list[PaperEvent]:
        return list(self._events)

    @property
    def last_processed_minute(self) -> datetime | None:
        """Latest atomic completed-minute input accepted by the reducer."""

        return self._last_submitted_time

    def required_symbols(self) -> list[str]:
        """Symbols whose unconstrained state still requires minute bars.

        Globally rejected candidates intentionally remain in the local shadow
        state because V8's conservative no-backfill overlay depends on their
        hypothetical pending/fill lifecycle.
        """

        return sorted(
            {
                runtime.candidate.symbol
                for occurrence in self._occurrences.values()
                for runtime in occurrence.runtimes
                if runtime.state in ACTIVE_STATES
            }
        )

    def state_summary(self) -> dict[str, Any]:
        return {
            "last_processed_minute": (
                self._last_submitted_time.isoformat()
                if self._last_submitted_time is not None
                else None
            ),
            "required_symbols": self.required_symbols(),
            "active_portfolio_symbols": sorted(self._active_by_symbol),
            "registered_occurrences": len(self._occurrences),
            "candidate_count": len(self._runtime_by_id),
            "event_count": len(self._events),
        }

    def checkpoint(self) -> dict[str, Any]:
        occurrences: list[dict[str, Any]] = []
        for occurrence in self._ordered_occurrences():
            occurrences.append(
                {
                    "setup_id": occurrence.setup.setup_id,
                    "signal_time": occurrence.signal_time.isoformat(),
                    "filled_cap": occurrence.filled_cap,
                    "allocated_once": occurrence.allocated_once,
                    "last_processed_time": (
                        occurrence.last_processed_time.isoformat()
                        if occurrence.last_processed_time is not None
                        else None
                    ),
                    "runtimes": [self._runtime_checkpoint(item) for item in occurrence.ranked()],
                }
            )
        return {
            "schema_version": SCHEMA_VERSION,
            "setup_book_sha256": self.config.setup_book_sha256,
            "strategy_fingerprint": self.config.strategy_fingerprint,
            "last_submitted_time": (
                self._last_submitted_time.isoformat()
                if self._last_submitted_time is not None
                else None
            ),
            "state_summary": self.state_summary(),
            "sequence": self._sequence,
            "registration_fingerprints": dict(self._registration_fingerprints),
            "processed_minute_fingerprints": dict(self._processed_minute_fingerprints),
            "intervention_fingerprints": dict(self._intervention_fingerprints),
            "active_by_symbol": dict(self._active_by_symbol),
            "occurrences": occurrences,
            "events": [event.to_dict() for event in self._events],
        }

    def checkpoint_json(self) -> str:
        return json.dumps(
            self.checkpoint(), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )

    @classmethod
    def from_checkpoint(
        cls, payload: Mapping[str, Any], config: PaperEngineConfig | None = None
    ) -> "V8CombinedPaperEngine":
        engine = cls(config)
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise ValueError("unsupported paper-engine checkpoint schema")
        if payload.get("setup_book_sha256") != engine.config.setup_book_sha256 or payload.get(
            "strategy_fingerprint"
        ) != engine.config.strategy_fingerprint:
            raise ReplayConflictError("checkpoint configuration identity changed")
        engine._last_submitted_time = (
            _exact_minute(payload["last_submitted_time"])
            if payload.get("last_submitted_time")
            else None
        )
        engine._sequence = int(payload.get("sequence", 0))
        engine._registration_fingerprints = dict(payload.get("registration_fingerprints", {}))
        engine._processed_minute_fingerprints = dict(
            payload.get("processed_minute_fingerprints", {})
        )
        engine._intervention_fingerprints = dict(payload.get("intervention_fingerprints", {}))
        engine._active_by_symbol = {
            str(key): str(value) for key, value in dict(payload.get("active_by_symbol", {})).items()
        }
        for occurrence_payload in payload.get("occurrences", []):
            setup_id = str(occurrence_payload["setup_id"])
            setup = engine.config.setup_by_id[setup_id]
            signal = _exact_minute(occurrence_payload["signal_time"])
            runtimes = [engine._runtime_from_checkpoint(item) for item in occurrence_payload["runtimes"]]
            occurrence = _Occurrence(
                setup=setup,
                policy=engine.config.entry_policies[setup_id],
                signal_time=signal,
                runtimes=runtimes,
                filled_cap=int(occurrence_payload.get("filled_cap", 0)),
                allocated_once=int(occurrence_payload.get("allocated_once", 0)),
                last_processed_time=(
                    _exact_minute(occurrence_payload["last_processed_time"])
                    if occurrence_payload.get("last_processed_time")
                    else None
                ),
            )
            engine._occurrences[occurrence.key] = occurrence
            for runtime in runtimes:
                engine._runtime_by_id[runtime.candidate_id] = runtime
        engine._events = [
            PaperEvent(
                sequence=int(item["sequence"]),
                event_time=_exact_minute(item["event_time"]),
                candidate_id=str(item["candidate_id"]),
                setup_id=str(item["setup_id"]),
                symbol=str(item["symbol"]),
                scope=str(item["scope"]),
                state_before=str(item["state_before"]),
                state_after=str(item["state_after"]),
                reason=str(item["reason"]),
            )
            for item in payload.get("events", [])
        ]
        if engine._events and max(item.sequence for item in engine._events) != engine._sequence:
            raise ValueError("checkpoint event sequence is inconsistent")
        return engine

    @classmethod
    def from_checkpoint_json(
        cls, text: str, config: PaperEngineConfig | None = None
    ) -> "V8CombinedPaperEngine":
        return cls.from_checkpoint(json.loads(text), config)

    @staticmethod
    def _runtime_checkpoint(runtime: _Runtime) -> dict[str, Any]:
        payload = asdict(runtime)
        payload["candidate"] = runtime.candidate.to_dict()
        return _json_safe(payload)

    @staticmethod
    def _runtime_from_checkpoint(payload: Mapping[str, Any]) -> _Runtime:
        candidate = PaperCandidate.from_object(payload["candidate"])
        timestamp_fields = {
            "confirmation_time",
            "order_placed_at",
            "entry_time",
            "exit_time",
        }
        bar_payload = payload.get("confirmation_bar")
        fields: dict[str, Any] = {}
        for name in _Runtime.__dataclass_fields__:
            if name == "candidate":
                continue
            value = payload.get(name)
            if name in timestamp_fields and value is not None:
                value = _exact_minute(value)
            elif name == "confirmation_bar" and bar_payload is not None:
                value = CompletedMinuteBar.from_object(bar_payload)
            fields[name] = value
        return _Runtime(candidate=candidate, **fields)


# Concise aliases for session/orchestrator code.
PaperEngine = V8CombinedPaperEngine


__all__ = [
    "CompletedMinuteBar",
    "PaperCandidate",
    "PaperEngine",
    "PaperEngineConfig",
    "PaperEngineError",
    "PaperEvent",
    "ReplayConflictError",
    "OutOfOrderMinuteError",
    "V8CombinedPaperEngine",
    "build_trigger",
    "candidate_passes",
    "confirmation_check",
    "round_down_to_tick",
    "round_up_to_tick",
    "valid_completed_bar",
]
