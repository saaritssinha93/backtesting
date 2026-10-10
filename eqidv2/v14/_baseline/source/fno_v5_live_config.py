"""Current live configuration for the hybrid FNO OI / NSE-equity V5 strategy.

The active setup book is the conservative train-only optimizer selection made
on 2026-08-11. It keeps the existing signal filters/rankers, tunes brackets and
leg inclusion on train, and is replayed on NSE equity prices with mapped
futures OI. Inactive scan slots remain observable but cannot create entries.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

import fno_oi_common as common
import fno_oi_hybrid_data as hybrid


STRATEGY_VERSION = "FNO_V5_EQUITY_5M_FUTURES_OI_V4_OPTIMIZED_20260811"
SELECTED_OBJECTIVE = "TRAIN_ONLY_ROBUST_V5_OPTIMIZER"
EXPECTED_BACKTEST: dict[str, float | int] = {
    "sessions": 52,
    "orders": 71,
    "fills": 69,
    "trade_pf": 1.976662,
    "day_pf": 2.232425,
    "net_pct": 25.004116,
}
PROTECTED_BACKTEST_START = date(2026, 5, 27)
PROTECTED_BACKTEST_END = date(2026, 8, 10)
PROTECTED_SELECTED_DAILY_SHA256 = (
    "fca8f66bdc2e5a1f656df140098a3a8f9613999ade4fc93e581d377ee9088eb6"
)

# The cache used by the selected backtests was built from this loose signal
# superset.  FORCE_DAILY removes additional setup filters, not these base gates.
BASE_PRICE_CHANGE_PCT = 0.10
BASE_OI_CHANGE_PCT = 0.05
BASE_VOLUME_RATIO = 0.80
FNO_FETCH_SLOT_SCHEMA_VERSION = common.FNO_FETCH_SLOT_SCHEMA_VERSION
FNO_READINESS_POLICY = common.VERIFIED_NO_CANDLE_POLICY_VERSION
MIN_STOCK_FUTURES_COVERAGE = common.MIN_STOCK_FUTURES_COVERAGE
MAX_VERIFIED_NO_CANDLE_STOCKS = common.MAX_VERIFIED_NO_CANDLE_STOCKS
MIN_NO_CANDLE_FETCH_ATTEMPTS = common.MIN_NO_CANDLE_FETCH_ATTEMPTS
CONFIRMATION_FEED_SCHEMA_VERSION = common.EQUITY_1M_SLOT_SCHEMA_VERSION
CONFIRMATION_FEED_POLICY = "candidate_exact_completed_1m_verified_no_candle_v1"
CONFIRMATION_NO_CANDLE_OBSERVATIONS = 3
CONFIRMATION_NO_CANDLE_MIN_AGE_SEC = 15
CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC = 2.0

SIGNAL_TO_CONFIRMATION = {
    "09:25": "09:26",
    "09:30": "09:31",
    "09:35": "09:36",
    "09:40": "09:41",
    "09:45": "09:46",
}
SQUARE_OFF = "15:30"
ROUND_TRIP_COST_BPS = 5.0
ENTRY_ACTIVATION_GRACE_SEC = 90
CAPITAL_PER_ENTRY_RS = 10_000.0
LEVERAGE = 5.0
TARGET_EXPOSURE_RS = CAPITAL_PER_ENTRY_RS * LEVERAGE
SELECTED_DAILY_PATH = (
    common.FNO_ROOT
    / "strategy_research"
    / "ema_confirm_0925_0930_0935_0940_0945_v5_selected_daily.csv"
)


@dataclass(frozen=True)
class SetupSpec:
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


# Exact selected legs from the conservative train-only optimizer output.
ACTIVE_SETUPS: tuple[SetupSpec, ...] = (
    SetupSpec(
        signal_end="09:25",
        confirmation_end="09:26",
        side="LONG",
        mode="FORCE_DAILY",
        max_entries=1,
        picker="max_liquidity",
        price_change_pct=0.0,
        oi_change_pct=0.0,
        volume_ratio=0.0,
        body_ratio=0.0,
        max_wick_ratio=999.0,
        min_traded_value=0.0,
        stop_pct=1.0,
        target_pct=2.0,
        source_version="V5_HYBRID_TRAIN_ONLY_OPTIMIZED",
    ),
    SetupSpec(
        signal_end="09:25",
        confirmation_end="09:26",
        side="SHORT",
        mode="FILTERED",
        max_entries=2,
        picker="max_volume",
        price_change_pct=0.3,
        oi_change_pct=0.5,
        volume_ratio=3.0,
        body_ratio=0.4,
        max_wick_ratio=0.5,
        min_traded_value=0.0,
        stop_pct=0.4,
        target_pct=3.0,
        source_version="V5_HYBRID_TRAIN_ONLY_OPTIMIZED",
    ),
    SetupSpec(
        signal_end="09:40",
        confirmation_end="09:41",
        side="SHORT",
        mode="FILTERED",
        max_entries=2,
        picker="max_oi",
        price_change_pct=0.4,
        oi_change_pct=1.0,
        volume_ratio=1.5,
        body_ratio=0.4,
        max_wick_ratio=0.5,
        min_traded_value=0.0,
        stop_pct=0.3,
        target_pct=1.5,
        source_version="V5_BEST_TRADE_PF",
    ),
)


@dataclass(frozen=True)
class PositionSize:
    capital_rs: float
    leverage: float
    target_exposure_rs: float
    theoretical_units: int
    quantity: int
    lot_size: int
    estimated_exposure_rs: float
    state: str


def setup_for(signal_end: str, side: str) -> SetupSpec | None:
    wanted_side = side.upper()
    return next(
        (
            setup
            for setup in ACTIVE_SETUPS
            if setup.signal_end == signal_end and setup.side == wanted_side
        ),
        None,
    )


def slot_datetime(session_date: date, hhmm: str) -> datetime:
    hour, minute = (int(part) for part in hhmm.split(":"))
    return datetime(
        session_date.year,
        session_date.month,
        session_date.day,
        hour,
        minute,
        tzinfo=common.IST,
    )


def activation_deadline(session_date: date, confirmation_end: str) -> datetime:
    return slot_datetime(session_date, confirmation_end) + timedelta(
        seconds=ENTRY_ACTIVATION_GRACE_SEC
    )


def strategy_payload() -> dict[str, Any]:
    return {
        "strategy_version": STRATEGY_VERSION,
        "selected_objective": SELECTED_OBJECTIVE,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "price_volume_indicator_source": "NSE_EQUITY",
        "equity_five_minute_quality": "COMPLETED_REAL_END_LABELLED_ONLY",
        "backtest_equity_five_minute_construction": hybrid.BACKTEST_EQUITY_5M_CONSTRUCTION,
        "oi_source": "NFO_FUTURE",
        "futures_readiness": {
            "fetch_marker_schema": FNO_FETCH_SLOT_SCHEMA_VERSION,
            "policy": FNO_READINESS_POLICY,
            "universe": "MAPPED_STOCK_FUTURES_EXCLUDING_INDEX_FUTURES",
            "minimum_stock_coverage": MIN_STOCK_FUTURES_COVERAGE,
            "maximum_verified_no_candle_stocks": MAX_VERIFIED_NO_CANDLE_STOCKS,
            "minimum_no_candle_fetch_attempts": MIN_NO_CANDLE_FETCH_ATTEMPTS,
            "verified_no_candle_action": "SKIPPED_NO_CANDLE",
            "synthetic_or_forward_filled_futures_bars": False,
        },
        "confirmation_feed": {
            "schema": CONFIRMATION_FEED_SCHEMA_VERSION,
            "policy": CONFIRMATION_FEED_POLICY,
            "source": "DURABLE_COMPLETED_NSE_EQUITY_1M_FEED",
            "candidate_set_and_scanner_snapshot_hashed": True,
            "immutable_slot_bar_snapshot": True,
            "confirmation_is_read_only_consumer": True,
            "activation_deadline_sec": ENTRY_ACTIVATION_GRACE_SEC,
            "candidate_resolution_policy": "ALL_WRITTEN_OR_VERIFIED_NO_CANDLE",
            "minimum_no_candle_observations": CONFIRMATION_NO_CANDLE_OBSERVATIONS,
            "minimum_no_candle_verification_age_sec": CONFIRMATION_NO_CANDLE_MIN_AGE_SEC,
            "no_candle_observation_spacing_sec": CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC,
            "verified_no_candle_action": "INELIGIBLE_NO_CANDLE",
            "verified_no_candle_cap": None,
            "written_bar_minimum_ratio": None,
        },
        "confirmation_and_entry_instrument": "NSE_EQUITY",
        "expected_backtest": EXPECTED_BACKTEST,
        "base_signal_gates": {
            "price_change_pct": BASE_PRICE_CHANGE_PCT,
            "oi_change_pct": BASE_OI_CHANGE_PCT,
            "volume_ratio": BASE_VOLUME_RATIO,
        },
        "signal_to_confirmation": SIGNAL_TO_CONFIRMATION,
        "square_off": SQUARE_OFF,
        "round_trip_cost_bps": ROUND_TRIP_COST_BPS,
        "entry_activation_grace_sec": ENTRY_ACTIVATION_GRACE_SEC,
        "capital_per_entry_rs": CAPITAL_PER_ENTRY_RS,
        "leverage": LEVERAGE,
        "target_exposure_rs": TARGET_EXPOSURE_RS,
        "active_setups": [asdict(setup) for setup in ACTIVE_SETUPS],
        "inactive_selected_legs": [
            {"signal_end": "09:30", "confirmation_end": "09:31", "side": "LONG"},
            {"signal_end": "09:30", "confirmation_end": "09:31", "side": "SHORT"},
            {"signal_end": "09:35", "confirmation_end": "09:36", "side": "LONG"},
            {"signal_end": "09:35", "confirmation_end": "09:36", "side": "SHORT"},
            {"signal_end": "09:40", "confirmation_end": "09:41", "side": "LONG"},
            {"signal_end": "09:45", "confirmation_end": "09:46", "side": "LONG"},
            {"signal_end": "09:45", "confirmation_end": "09:46", "side": "SHORT"},
        ],
    }


def strategy_fingerprint() -> str:
    encoded = json.dumps(
        strategy_payload(), sort_keys=True, separators=(",", ":")
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def passes_selected_filters(candidate: dict[str, Any], setup: SetupSpec) -> bool:
    if str(candidate.get("side", "")).upper() != setup.side:
        return False
    if setup.mode == "FORCE_DAILY":
        return True
    price = float(candidate.get("price_change_pct", math.nan))
    oi_change = float(candidate.get("oi_change_pct", math.nan))
    volume = float(candidate.get("volume_ratio", math.nan))
    body = float(candidate.get("body_ratio", math.nan))
    wick = float(candidate.get("wick_ratio", math.nan))
    traded_value = float(candidate.get("traded_value", math.nan))
    price_ok = (
        price >= setup.price_change_pct
        if setup.side == "LONG"
        else price <= -setup.price_change_pct
    )
    return bool(
        price_ok
        and oi_change >= setup.oi_change_pct
        and volume >= setup.volume_ratio
        and body >= setup.body_ratio
        and wick <= setup.max_wick_ratio
        and traded_value >= setup.min_traded_value
    )


def picker_value(candidate: dict[str, Any], picker: str) -> float:
    if picker == "max_oi":
        return float(candidate["oi_change_pct"])
    if picker == "max_volume":
        return float(candidate["volume_ratio"])
    if picker == "max_move":
        return abs(float(candidate["price_change_pct"]))
    if picker == "max_body":
        return float(candidate["body_ratio"])
    if picker == "max_liquidity":
        return float(candidate["traded_value"])
    raise ValueError(f"Unknown picker: {picker}")


def rank_candidates(
    candidates: list[dict[str, Any]], setup: SetupSpec
) -> list[dict[str, Any]]:
    eligible = [
        candidate
        for candidate in candidates
        if passes_selected_filters(candidate, setup)
    ]
    return sorted(
        eligible,
        key=lambda candidate: (
            -picker_value(candidate, setup.picker),
            -float(candidate["traded_value"]),
            str(candidate["tradingsymbol"]),
        ),
    )[: setup.max_entries]


def round_to_tick(value: float, tick_size: float) -> float:
    tick = float(tick_size)
    if not math.isfinite(tick) or tick <= 0:
        tick = 0.05
    return round(round(float(value) / tick) * tick, 8)


def bracket_levels(
    entry_price: float,
    side: str,
    stop_pct: float,
    target_pct: float,
    tick_size: float,
) -> tuple[float, float]:
    long_side = side.upper() == "LONG"
    stop = entry_price * (
        1.0 - stop_pct / 100.0 if long_side else 1.0 + stop_pct / 100.0
    )
    target = entry_price * (
        1.0 + target_pct / 100.0 if long_side else 1.0 - target_pct / 100.0
    )
    return round_to_tick(stop, tick_size), round_to_tick(target, tick_size)


def size_position(
    entry_price: float,
    lot_size: int,
    *,
    live: bool,
    capital_rs: float = CAPITAL_PER_ENTRY_RS,
    leverage: float = LEVERAGE,
) -> PositionSize:
    entry = float(entry_price)
    lot = max(1, int(lot_size))
    exposure = float(capital_rs) * float(leverage)
    theoretical = int(math.floor(exposure / entry)) if entry > 0 else 0
    if live:
        quantity = (theoretical // lot) * lot
        state = "LIVE_LOT_SIZED" if quantity > 0 else "BLOCKED_LOT_EXCEEDS_BUDGET"
    else:
        quantity = theoretical
        state = "PAPER_EXPOSURE_SIZED" if quantity > 0 else "BLOCKED_PRICE_EXCEEDS_BUDGET"
    return PositionSize(
        capital_rs=float(capital_rs),
        leverage=float(leverage),
        target_exposure_rs=exposure,
        theoretical_units=theoretical,
        quantity=quantity,
        lot_size=lot,
        estimated_exposure_rs=float(quantity * entry),
        state=state,
    )


def validate_strategy() -> None:
    if SELECTED_OBJECTIVE != "TRAIN_ONLY_ROBUST_V5_OPTIMIZER":
        raise AssertionError("V5 live objective is not the selected optimizer objective.")
    if tuple(SIGNAL_TO_CONFIRMATION) != (
        "09:25",
        "09:30",
        "09:35",
        "09:40",
        "09:45",
    ):
        raise AssertionError("V5 live signal windows changed.")
    observed_readiness = (
        FNO_FETCH_SLOT_SCHEMA_VERSION,
        FNO_READINESS_POLICY,
        MIN_STOCK_FUTURES_COVERAGE,
        MAX_VERIFIED_NO_CANDLE_STOCKS,
        MIN_NO_CANDLE_FETCH_ATTEMPTS,
    )
    expected_readiness = (
        "fno_oi_fetch_slot_v2",
        "verified_stock_no_candle_skip_v1",
        0.99,
        2,
        3,
    )
    if observed_readiness != expected_readiness:
        raise AssertionError(
            f"V5 futures-readiness policy changed: {observed_readiness}"
        )
    observed_confirmation_feed = (
        CONFIRMATION_FEED_SCHEMA_VERSION,
        CONFIRMATION_FEED_POLICY,
        ENTRY_ACTIVATION_GRACE_SEC,
        CONFIRMATION_NO_CANDLE_OBSERVATIONS,
        CONFIRMATION_NO_CANDLE_MIN_AGE_SEC,
        CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC,
    )
    if observed_confirmation_feed != (
        "fno_equity_1m_slot_v1",
        "candidate_exact_completed_1m_verified_no_candle_v1",
        90,
        3,
        15,
        2.0,
    ):
        raise AssertionError(
            f"V5 confirmation-feed policy changed: {observed_confirmation_feed}"
        )
    seen: set[tuple[str, str]] = set()
    for setup in ACTIVE_SETUPS:
        key = (setup.signal_end, setup.side)
        if key in seen:
            raise AssertionError(f"Duplicate selected setup: {key}")
        seen.add(key)
        if SIGNAL_TO_CONFIRMATION[setup.signal_end] != setup.confirmation_end:
            raise AssertionError(f"Confirmation mismatch for {setup.setup_id}")
        cap = 1 if setup.side == "LONG" else 2
        if setup.max_entries > cap:
            raise AssertionError(f"Selected setup exceeds V5 cap: {setup.setup_id}")
        if setup.stop_pct <= 0 or setup.target_pct <= 0:
            raise AssertionError(f"Invalid bracket for {setup.setup_id}")


def attest_selected_backtest(
    path: Path | str = SELECTED_DAILY_PATH,
) -> dict[str, Any]:
    selected_path = Path(path)
    if not selected_path.exists():
        raise FileNotFoundError(f"Selected V5 daily curve is missing: {selected_path}")
    if selected_path.resolve() == SELECTED_DAILY_PATH.resolve():
        observed_sha256 = hashlib.sha256(selected_path.read_bytes()).hexdigest()
        if observed_sha256 != PROTECTED_SELECTED_DAILY_SHA256:
            raise AssertionError(
                "Selected V5 protected curve hash changed: "
                f"expected {PROTECTED_SELECTED_DAILY_SHA256}, observed {observed_sha256}"
            )
    frame = pd.read_csv(selected_path)
    required = {
        "objective",
        "selections",
        "fills",
        "cumulative_net_pct",
        "cumulative_day_pf",
        "cumulative_trade_pf",
        "data_contract",
    }
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(f"Selected V5 curve is missing columns: {sorted(missing)}")
    objectives = set(frame["objective"].dropna().astype(str))
    if objectives != {SELECTED_OBJECTIVE}:
        raise AssertionError(f"Selected V5 objective changed: {sorted(objectives)}")
    contracts = set(frame["data_contract"].dropna().astype(str))
    if contracts != {hybrid.DATA_CONTRACT_VERSION}:
        raise AssertionError(f"Selected V5 data contract changed: {sorted(contracts)}")
    expected_sessions = int(EXPECTED_BACKTEST["sessions"])
    if len(frame) != expected_sessions:
        raise AssertionError(
            f"Selected V5 session count changed: expected {expected_sessions}, "
            f"observed {len(frame)}"
        )
    days = pd.to_datetime(frame["day"], errors="coerce").dt.date
    if days.isna().any() or days.duplicated().any():
        raise AssertionError("Selected V5 curve has invalid or duplicate session dates.")
    if days.iloc[0] != PROTECTED_BACKTEST_START or days.iloc[-1] != PROTECTED_BACKTEST_END:
        raise AssertionError(
            "Selected V5 protected date range changed: "
            f"expected {PROTECTED_BACKTEST_START}..{PROTECTED_BACKTEST_END}, "
            f"observed {days.iloc[0]}..{days.iloc[-1]}"
        )
    last = frame.iloc[-1]
    observed = {
        "sessions": int(len(frame)),
        "orders": int(pd.to_numeric(frame["selections"]).sum()),
        "fills": int(pd.to_numeric(frame["fills"]).sum()),
        "trade_pf": float(last["cumulative_trade_pf"]),
        "day_pf": float(last["cumulative_day_pf"]),
        "net_pct": float(last["cumulative_net_pct"]),
    }
    for metric, expected in EXPECTED_BACKTEST.items():
        tolerance = 0 if metric in {"sessions", "orders", "fills"} else 0.0005
        if abs(float(observed[metric]) - float(expected)) > tolerance:
            raise AssertionError(
                f"Selected V5 {metric} changed: expected {expected}, "
                f"observed {observed[metric]}"
            )
    return observed


validate_strategy()
