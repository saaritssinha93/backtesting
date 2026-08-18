"""Live configuration promoted from the V6 BEST_NET cash-equity backtest."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

import fno_oi_common as common
import fno_oi_backtest_provenance as backtest_provenance
import fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6 as backtest_v6
import fno_oi_hybrid_data as hybrid


LIVE_GENERATION = "v6"
STRATEGY_VERSION = backtest_v6.STRATEGY_VERSION
SELECTED_OBJECTIVE = backtest_v6.OBJECTIVE
SetupSpec = backtest_v6.SetupSpec
PositionSize = __import__("fno_v5_live_config").PositionSize
ACTIVE_SETUPS: tuple[SetupSpec, ...] = backtest_v6.ACTIVE_SETUPS
EXPECTED_BACKTEST = dict(backtest_v6.CURRENT_SOURCE_PROMOTED_HISTORY)
LEGACY_EXPECTED_BACKTEST = dict(backtest_v6.EXPECTED_SELECTED_HISTORY)
PROTECTED_BACKTEST_START = date(2026, 5, 27)
PROTECTED_BACKTEST_END = backtest_v6.SELECTED_HISTORY_END
PROTECTED_SELECTED_DAILY_SHA256 = (
    "7ba3426c16497f4d0aa1f18c3aa3d3cd42c5d8ee8090d154c4b261ed69ed85b7"
)
PROTECTED_SELECTED_PROVENANCE_PATH = (
    backtest_v6.CURRENT_SOURCE_SELECTED_PROVENANCE_PATH
)
PROTECTED_SELECTED_PROVENANCE_SHA256 = (
    "de394f5d2e831d5ca32a362476e66df8ad2ef343a1be8d901a1037aff81cd296"
)
PROTECTED_SELECTED_INPUT_FINGERPRINT = (
    "199effd6d7aa430444a33f43fff4530925b131c15e47da226b953cc27687178d"
)
LEGACY_SELECTED_DAILY_PATH = backtest_v6.SELECTED_DAILY_PROTECTED_PATH
LEGACY_SELECTED_DAILY_SHA256 = (
    "677470bb890f53c73a5eb20d6aebe55ac830e160dbfd07c20c32c42baec97a6b"
)
LEGACY_SELECTED_MISMATCH_AUDIT_SHA256 = (
    "b147f63de9a0aa7da9618cc2814b7b905af0cdd00b3509f0b18a9118bfddaa9e"
)

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
CONFIRMATION_COMPLETED_BOUNDARY_BUFFER_SEC = 3.0
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
SELECTED_DAILY_PATH = backtest_v6.CURRENT_SOURCE_SELECTED_DAILY_PATH
ROLLING_DAILY_PATH = backtest_v6.DAILY_OUTPUT_PATH
LIVE_ACK_ENV = "FNO_V6_LIVE_ACK"
LIVE_ACK = "I_UNDERSTAND_REAL_FNO_V6_EQUITY_ORDERS"
ORDER_TAG_PREFIX = "FV6"


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
        "live_generation": LIVE_GENERATION,
        "strategy_version": STRATEGY_VERSION,
        "selected_objective": SELECTED_OBJECTIVE,
        "data_contract": hybrid.DATA_CONTRACT_VERSION,
        "price_volume_indicator_source": "NSE_EQUITY",
        "equity_five_minute_quality": "COMPLETED_REAL_END_LABELLED_ONLY",
        "backtest_equity_five_minute_construction": hybrid.BACKTEST_EQUITY_5M_CONSTRUCTION,
        "oi_source": "NFO_FUTURE",
        "futures_fields_admitted": ["oi", "prev_oi", "oi_change_pct"],
        "backtest_source_identity": {
            "provenance_schema": backtest_provenance.RUN_PROVENANCE_SCHEMA_VERSION,
            "provenance_claim": backtest_provenance.CURRENT_SOURCE_PROVENANCE_CLAIM,
            "original_selection_source_provenance_available": False,
            "current_source_replay_provenance_pinned": True,
            "replay_revision": backtest_v6.CURRENT_SOURCE_REPLAY_REVISION,
            "inventory_scope": "WHOLE_SOURCE_FILES_NOT_DATE_SLICED",
            "universe_date": backtest_v6.BACKTEST_UNIVERSE_DATE.isoformat(),
            "universe_path_name": backtest_v6.BACKTEST_UNIVERSE_PATH.name,
            **backtest_v6.BACKTEST_UNIVERSE_HASHES,
            "mapping_source": "PERSISTED_DATED_UNIVERSE_ONLY",
            "complete_mapped_sources_required": True,
            "selected_daily_sha256": PROTECTED_SELECTED_DAILY_SHA256,
            "promoted_backtest_input_fingerprint": (
                PROTECTED_SELECTED_INPUT_FINGERPRINT
            ),
            "legacy_selected_daily_sha256": LEGACY_SELECTED_DAILY_SHA256,
            "legacy_mismatch_audit_sha256": (
                LEGACY_SELECTED_MISMATCH_AUDIT_SHA256
            ),
        },
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
            "completed_candle_boundary_buffer_sec": CONFIRMATION_COMPLETED_BOUNDARY_BUFFER_SEC,
            "candidate_resolution_policy": "ALL_WRITTEN_OR_VERIFIED_NO_CANDLE",
            "minimum_no_candle_observations": CONFIRMATION_NO_CANDLE_OBSERVATIONS,
            "minimum_no_candle_verification_age_sec": CONFIRMATION_NO_CANDLE_MIN_AGE_SEC,
            "no_candle_observation_spacing_sec": CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC,
            "verified_no_candle_action": "INELIGIBLE_NO_CANDLE",
            "verified_no_candle_cap": None,
            "written_bar_minimum_ratio": None,
        },
        "confirmation_entry_exit_instrument": "NSE_EQUITY",
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
        "inactive_selected_legs": [],
    }


def strategy_fingerprint() -> str:
    encoded = json.dumps(
        strategy_payload(), sort_keys=True, separators=(",", ":")
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


def passes_selected_filters(candidate: dict[str, Any], setup: SetupSpec) -> bool:
    if str(candidate.get("side", "")).upper() != setup.side:
        return False
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
    if SELECTED_OBJECTIVE != "BEST_NET":
        raise AssertionError("V6 live objective must be BEST_NET.")
    if tuple(SIGNAL_TO_CONFIRMATION) != (
        "09:25",
        "09:30",
        "09:35",
        "09:40",
        "09:45",
    ):
        raise AssertionError("V6 live signal windows changed.")
    if len(ACTIVE_SETUPS) != 10:
        raise AssertionError("V6 live must contain all ten BEST_NET legs.")
    observed_backtest_source = strategy_payload()["backtest_source_identity"]
    expected_backtest_source = {
        "provenance_schema": "fno_backtest_run_provenance_v1",
        "provenance_claim": (
            "RECREATED_CURRENT_SOURCE_REPLAY_NOT_ORIGINAL_SELECTION_PROVENANCE"
        ),
        "original_selection_source_provenance_available": False,
        "current_source_replay_provenance_pinned": True,
        "replay_revision": "20260818_V1",
        "inventory_scope": "WHOLE_SOURCE_FILES_NOT_DATE_SLICED",
        "universe_date": "2026-08-11",
        "universe_path_name": "near_month_2026-08-11.parquet",
        "file_sha256": "24170f39c7cf99021553396e40e0d88a435f857364b2423dcfbe9312539dbf09",
        "universe_sha256": "18c496bbf9e09b6914d073cba21c4c6c56305da1ed5759f4f91cc8cb66c19ad5",
        "mapped_universe_sha256": "2cc160189f87bff4eb987a15a4684d95619ee9c810db3cd37276b114ad5824bf",
        "mapped_symbol_set_sha256": "d42f87a9c5fc8ab1710b09b6c4c9832c9d19ecc440ef92b84cad6981499a05a3",
        "mapping_source": "PERSISTED_DATED_UNIVERSE_ONLY",
        "complete_mapped_sources_required": True,
        "selected_daily_sha256": "7ba3426c16497f4d0aa1f18c3aa3d3cd42c5d8ee8090d154c4b261ed69ed85b7",
        "promoted_backtest_input_fingerprint": (
            "199effd6d7aa430444a33f43fff4530925b131c15e47da226b953cc27687178d"
        ),
        "legacy_selected_daily_sha256": (
            "677470bb890f53c73a5eb20d6aebe55ac830e160dbfd07c20c32c42baec97a6b"
        ),
        "legacy_mismatch_audit_sha256": (
            "b147f63de9a0aa7da9618cc2814b7b905af0cdd00b3509f0b18a9118bfddaa9e"
        ),
    }
    if observed_backtest_source != expected_backtest_source:
        raise AssertionError("V6 frozen backtest source identity changed.")
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
            f"V6 futures-readiness policy changed: {observed_readiness}"
        )
    observed_confirmation_feed = (
        CONFIRMATION_FEED_SCHEMA_VERSION,
        CONFIRMATION_FEED_POLICY,
        ENTRY_ACTIVATION_GRACE_SEC,
        CONFIRMATION_COMPLETED_BOUNDARY_BUFFER_SEC,
        CONFIRMATION_NO_CANDLE_OBSERVATIONS,
        CONFIRMATION_NO_CANDLE_MIN_AGE_SEC,
        CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC,
    )
    if observed_confirmation_feed != (
        "fno_equity_1m_slot_v1",
        "candidate_exact_completed_1m_verified_no_candle_v1",
        90,
        3.0,
        3,
        15,
        2.0,
    ):
        raise AssertionError(
            f"V6 confirmation-feed policy changed: {observed_confirmation_feed}"
        )
    seen: set[tuple[str, str]] = set()
    for setup in ACTIVE_SETUPS:
        key = (setup.signal_end, setup.side)
        if key in seen:
            raise AssertionError(f"Duplicate V6 setup: {key}")
        seen.add(key)
        if SIGNAL_TO_CONFIRMATION[setup.signal_end] != setup.confirmation_end:
            raise AssertionError(f"Confirmation mismatch: {setup.setup_id}")
        cap = 1 if setup.side == "LONG" else 2
        if setup.max_entries > cap:
            raise AssertionError(f"V6 setup exceeds cap: {setup.setup_id}")


def attest_selected_backtest_provenance(
    path: Path | str = PROTECTED_SELECTED_PROVENANCE_PATH,
) -> dict[str, Any]:
    provenance_path = Path(path)
    if not provenance_path.exists():
        raise FileNotFoundError(
            f"V6 protected selected provenance is missing: {provenance_path}"
        )
    observed_hash = backtest_provenance.sha256_file(provenance_path)
    if observed_hash != PROTECTED_SELECTED_PROVENANCE_SHA256:
        raise AssertionError(
            "V6 protected selected provenance hash changed: "
            f"expected {PROTECTED_SELECTED_PROVENANCE_SHA256}, observed {observed_hash}"
        )
    payload = json.loads(provenance_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != backtest_provenance.RUN_PROVENANCE_SCHEMA_VERSION:
        raise AssertionError("V6 protected provenance schema changed.")
    if payload.get("provenance_claim") != (
        backtest_provenance.CURRENT_SOURCE_PROVENANCE_CLAIM
    ) or bool(payload.get("original_selection_source_provenance_available")):
        raise AssertionError("V6 protected provenance overstates its historical claim.")
    if payload.get("strategy_version") != STRATEGY_VERSION or payload.get(
        "objective"
    ) != SELECTED_OBJECTIVE:
        raise AssertionError("V6 protected provenance strategy identity changed.")
    if payload.get("backtest_input_fingerprint") != (
        PROTECTED_SELECTED_INPUT_FINGERPRINT
    ):
        raise AssertionError("V6 protected provenance input fingerprint changed.")
    recomputed = backtest_provenance.backtest_input_fingerprint(
        {"input_fingerprint": payload.get("cache_input_fingerprint", "")},
        strategy_payload=payload.get("strategy_payload", {}),
        parameters=payload.get("parameters", {}),
    )
    if recomputed != payload.get("backtest_input_fingerprint"):
        raise AssertionError("V6 protected provenance input fingerprint is invalid.")

    universe = payload.get("universe", {})
    for key, expected in backtest_v6.BACKTEST_UNIVERSE_HASHES.items():
        if universe.get(key) != expected:
            raise AssertionError(
                f"V6 protected provenance universe {key} changed: "
                f"expected {expected}, observed {universe.get(key)}"
            )
    if universe.get("master_date") != PROTECTED_BACKTEST_END.isoformat() or universe.get(
        "mapping_source"
    ) != "PERSISTED_DATED_UNIVERSE_ONLY":
        raise AssertionError("V6 protected provenance dated mapping changed.")

    inventory = payload.get("source_inventory", {})
    if (
        inventory.get("schema_version")
        != backtest_provenance.SOURCE_INVENTORY_SCHEMA_VERSION
        or bool(inventory.get("date_sliced", True))
        or int(inventory.get("missing_count", -1)) != 0
        or int(inventory.get("entry_count", 0)) <= 0
    ):
        raise AssertionError("V6 protected provenance source inventory is incomplete.")
    selected_output = payload.get("outputs", {}).get("protected_selected_daily", {})
    if selected_output.get("sha256") != PROTECTED_SELECTED_DAILY_SHA256:
        raise AssertionError(
            "V6 protected provenance is not bound to the protected selected CSV."
        )
    if Path(str(selected_output.get("path", ""))).resolve() != (
        SELECTED_DAILY_PATH.resolve()
    ):
        raise AssertionError("V6 protected provenance selected path changed.")
    mismatch_output = payload.get("outputs", {}).get("legacy_mismatch_audit", {})
    if mismatch_output.get("sha256") != LEGACY_SELECTED_MISMATCH_AUDIT_SHA256:
        raise AssertionError("V6 legacy mismatch audit binding changed.")
    mismatch_path = Path(str(mismatch_output.get("path", "")))
    if (
        not mismatch_path.exists()
        or backtest_provenance.sha256_file(mismatch_path)
        != LEGACY_SELECTED_MISMATCH_AUDIT_SHA256
    ):
        raise AssertionError("V6 legacy mismatch audit is missing or tampered.")
    if (
        not LEGACY_SELECTED_DAILY_PATH.exists()
        or backtest_provenance.sha256_file(LEGACY_SELECTED_DAILY_PATH)
        != LEGACY_SELECTED_DAILY_SHA256
    ):
        raise AssertionError("V6 legacy selected artifact changed.")
    window = payload.get("backtest_window", {})
    if (
        window.get("first_session") != PROTECTED_BACKTEST_START.isoformat()
        or window.get("last_session") != PROTECTED_BACKTEST_END.isoformat()
        or int(window.get("sessions", -1)) != int(EXPECTED_BACKTEST["sessions"])
    ):
        raise AssertionError("V6 protected provenance backtest window changed.")
    return payload


def attest_selected_backtest(
    path: Path | str = SELECTED_DAILY_PATH,
    *,
    require_provenance: bool = True,
) -> dict[str, Any]:
    selected_path = Path(path)
    if not selected_path.exists():
        raise FileNotFoundError(f"V6 selected daily curve is missing: {selected_path}")
    if (
        require_provenance
        and selected_path.resolve() == SELECTED_DAILY_PATH.resolve()
    ):
        observed_sha256 = hashlib.sha256(selected_path.read_bytes()).hexdigest()
        if observed_sha256 != PROTECTED_SELECTED_DAILY_SHA256:
            raise AssertionError(
                "V6 protected curve hash changed: "
                f"expected {PROTECTED_SELECTED_DAILY_SHA256}, observed {observed_sha256}"
            )
    frame = pd.read_csv(selected_path)
    required = {
        "day",
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
        raise RuntimeError(f"V6 selected curve is missing columns: {sorted(missing)}")
    if set(frame["objective"].dropna().astype(str)) != {SELECTED_OBJECTIVE}:
        raise AssertionError("V6 selected objective changed.")
    if set(frame["data_contract"].dropna().astype(str)) != {
        hybrid.DATA_CONTRACT_VERSION
    }:
        raise AssertionError("V6 selected data contract changed.")
    expected_sessions = int(EXPECTED_BACKTEST["sessions"])
    if len(frame) != expected_sessions:
        raise AssertionError(
            f"V6 selected session count changed: expected {expected_sessions}, "
            f"observed {len(frame)}"
        )
    days = pd.to_datetime(frame["day"], errors="coerce").dt.date
    if days.isna().any() or days.duplicated().any() or not days.is_monotonic_increasing:
        raise AssertionError("V6 selected curve has invalid, duplicate, or unordered dates.")
    if days.iloc[0] != PROTECTED_BACKTEST_START or days.iloc[-1] != PROTECTED_BACKTEST_END:
        raise AssertionError(
            "V6 protected date range changed: "
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
                f"V6 {metric} changed: expected {expected}, observed {observed[metric]}"
            )
    if selected_path.resolve() == SELECTED_DAILY_PATH.resolve():
        provenance_payload = attest_selected_backtest_provenance()
        for metric, expected in EXPECTED_BACKTEST.items():
            provenance_value = provenance_payload.get("results", {}).get(metric)
            tolerance = 0 if metric in {"sessions", "orders", "fills"} else 1e-9
            if provenance_value is None or abs(
                float(provenance_value) - float(expected)
            ) > tolerance:
                raise AssertionError(
                    f"V6 protected provenance metric changed for {metric}: "
                    f"expected {expected}, observed {provenance_value}"
                )
        observed["backtest_input_fingerprint"] = provenance_payload[
            "backtest_input_fingerprint"
        ]
    return observed


validate_strategy()
