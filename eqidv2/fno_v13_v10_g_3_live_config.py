"""Import-safe G3 runtime profile using the canonical accepted decision contract.

No frozen file, runtime directory or broker is accessed during import. Explicit
validation reads the repository's production spec; attestation only verifies
historical evidence and never grants order authority.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

import fno_v13_v10_g_3_cutoff_production as contract
import fno_v13_v10_g_3_policy as policy
import fno_v13_v10_g_3_live_execution_policy as live_execution_policy
from g3_one_minute_extension import last_features
from fno_v13_v10_g_3_cutoff_production import *
from fno_v13_v10_g_3_contract import _number, _stamp, _true

LIVE_GENERATION = "v6"
LIVE_LABEL = "V13-V10-G-3"
DISPLAY_LABEL = LIVE_LABEL
SESSION_PREFIX = "fno_v13_v10_g_3"
LIVE_ROOT_NAME = "v13_v10_g_3_live"
REPORT_PREFIX = "fno_v13_v10_g_3"
CONTROL_ROOT_NAME = "v13_v10_g_3_live"
LEGACY_EMERGENCY_CONTROL_ROOT_NAME = "v6_live"
SESSION_END = "15:32"
# Latest exact confirmation plus its 90-second publication grace, rounded up.
PIPELINE_DEADLINE = (datetime.strptime(max(SIGNAL_TO_CONFIRMATION.values()), "%H:%M")
    + timedelta(seconds=ENTRY_ACTIVATION_GRACE_SEC + 59)).strftime("%H:%M")
SELECTED_OBJECTIVE = "G3_W1_LONG110_SHORT120_CORE_FIRST_0941_LONG080_0950_LONG060_TARGET075_0920_LONG180_TARGET300_LATE_SIX_SELECTED_TEN_MIDDAY_TWELVE_AFTERNOON_FIFTEEN_0920_SHORT_1010_LONG_TARGET150_0926_LONG_5M_VOLUME120_1011_SHORT_MOVE011_TARGET_GT075_RESTORED3"
LIVE_ACK_ENV = "FNO_V13_V10_G_3_LIVE_ACK"
LIVE_ACK = "I_UNDERSTAND_REAL_FNO_V13_V10_G_3_EQUITY_ORDERS"
ORDER_TAG_PREFIX = "FVG3"
CONFIG_PATH = Path(__file__).resolve().parent / "configs/strategies/v13_v10_g_3_production_v13.json"
CONFIG_SHA256 = hashlib.sha256((json.dumps(contract.strategy_payload(), indent=2, sort_keys=True) + "\n").encode()).hexdigest()
FROZEN_ROOT = (Path(os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2")) / "fno_oi/strategy_research/v13_corrected_v10_g_3/frozen_20261008_long110_nextminute_v1")
FROZEN_CONFIG_PATH = FROZEN_ROOT / "frozen_config.json"
SELECTED_LEDGER_PATH = FROZEN_ROOT / "trades.csv"
SELECTED_LEDGER_SHA256 = FROZEN_LEDGER_SHA256
SELECTED_DAILY_PATH = FROZEN_ROOT / "daily_results.csv"
EXPECTED_BACKTEST = dict(sessions=46, orders=102, fills=93, net_profit_rupees=232886.13774822396)
CAPABILITIES = dict(exact_minute_confirmation=True, confirmation_volume_warmup=True,
    paper_portfolio_lock=True, staged_stops=True, strict_0925_long=True,
    profile_scoped_identity=True, isolated_controls=True, live_quantity_one=False,
    separate_live_execution_policy=True, live_exposure_sizing=True,
    setup_specific_confirmation_volume=True, long_0950_target075=True, long_0920_target300=True, opening_oi_asof_required=True, six_late_setups=True, selected_ten_setups=True, midday_twelve_setups=True, afternoon_fifteen_setups=True, short_0920_target150=True, long_1010_target150=True)
FNO_FETCH_SLOT_SCHEMA_VERSION = "fno_oi_fetch_slot_v2"
FNO_READINESS_POLICY = "verified_stock_no_candle_skip_v1"
MIN_STOCK_FUTURES_COVERAGE = .99
MAX_VERIFIED_NO_CANDLE_STOCKS = 2
MIN_NO_CANDLE_FETCH_ATTEMPTS = 3
CONFIRMATION_FEED_SCHEMA_VERSION = "fno_equity_1m_slot_v1"
CONFIRMATION_FEED_POLICY = "candidate_exact_completed_1m_verified_no_candle_v1"
CONFIRMATION_NO_CANDLE_OBSERVATIONS = 3
CONFIRMATION_NO_CANDLE_MIN_AGE_SEC = 15
CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC = 2.
CONFIRMATION_COMPLETED_BOUNDARY_BUFFER_SEC = 3.
# Side defaults only; setup-aware callers must include the signal slot.
MIN_CONFIRMATION_VOLUME_RATIO = dict(MIN_CONFIRMATION_VOLUME_BY_SIDE)


def strategy_payload() -> dict[str, Any]:
    return contract.strategy_payload()


def strategy_fingerprint() -> str:
    return contract.strategy_fingerprint()


config_fingerprint = strategy_fingerprint
policy_for_day = policy.policy_for_day


def load_frozen_config(path: Path | str = FROZEN_CONFIG_PATH) -> dict[str, Any]:
    return contract.verify_frozen_config(path)


def validate_strategy() -> None:
    contract.validate_contract()
    contract.load_contract(CONFIG_PATH)
    if hashlib.sha256(CONFIG_PATH.read_bytes()).hexdigest() != CONFIG_SHA256:
        raise ValueError("G3 production configuration bytes changed")
    if (STRATEGY_PROFILE, STRATEGY_VERSION, LIVE_ROOT_NAME, CONTROL_ROOT_NAME, LIVE_QUANTITY_CAP,
            ARMED, EXECUTION_AUTHORITY) != ("V13_V10_G_3", "V13-V10-G-3-PRODUCTION-V13",
            "v13_v10_g_3_live", "v13_v10_g_3_live", 1, False, False):
        raise ValueError("G3 runtime identity, controls or accepted decision contract changed")
    live_execution_policy.validate_policy()


def attest_selected_backtest(path: Path | str = SELECTED_LEDGER_PATH, *, require_provenance: bool = True) -> dict[str, Any]:
    """Historical evidence only; this does not attest production execution parity."""
    validate_strategy()
    load_frozen_config()
    selected = Path(path)
    if selected.resolve() != SELECTED_LEDGER_PATH.resolve():
        raise ValueError("Only the accepted frozen G3 ledger can attest historical evidence")
    if hashlib.sha256(selected.read_bytes()).hexdigest() != SELECTED_LEDGER_SHA256:
        raise ValueError("Accepted frozen G3 selected ledger hash mismatch")
    return {**EXPECTED_BACKTEST, "frozen_config_sha256": FROZEN_CONFIG_SHA256,
            "selected_ledger_sha256": SELECTED_LEDGER_SHA256,
            "production_contract_sha256": strategy_fingerprint(),
            "attestation_scope": "ACCEPTED_FROZEN_G3_RECORDED_LEDGER_ONLY",
            "production_parity_verified": False, "execution_authority": False,
            "evidence": "ARCHIVED_46_SESSION_REUSED_HISTORY_REPLAY"}


def slot_datetime(session_date: date, hhmm: str) -> datetime:
    hour, minute = map(int, hhmm.split(":"))
    return datetime.combine(session_date, datetime.min.time()).replace(hour=hour, minute=minute, tzinfo=IST)


def activation_deadline(session_date: date, confirmation_end: str) -> datetime:
    """Last permissible trigger/fill time; publication has its own deadline."""
    return slot_datetime(session_date, confirmation_end) + timedelta(seconds=ENTRY_TRIGGER_EXPIRY_SEC)


def confirmation_deadline(session_date: date, confirmation_end: str) -> datetime:
    return slot_datetime(session_date, confirmation_end) + timedelta(seconds=ENTRY_ACTIVATION_GRACE_SEC)


def entry_expiry(session_date: date, confirmation_end: str) -> datetime:
    return slot_datetime(session_date, confirmation_end) + timedelta(minutes=ENTRY_EXPIRY_MINUTES)


def nifty_context_from_bars(frame: pd.DataFrame, session_date: date) -> float:
    """Caller must supply the dated near-month NIFTY futures, not cash index bars."""
    column = "ts" if "ts" in frame else "timestamp"
    if frame.empty or column not in frame:
        return math.nan
    stamps = pd.to_datetime(frame[column], errors="coerce")
    stamps = stamps.dt.tz_localize(IST) if stamps.dt.tz is None else stamps.dt.tz_convert(IST)
    bars = frame.loc[stamps.eq(pd.Timestamp(slot_datetime(session_date, NIFTY_FIRST_BAR_END)))]
    if len(bars) != 1:
        return math.nan
    o, c = _number(bars.iloc[0].get("open")), _number(bars.iloc[0].get("close"))
    return (c / o - 1.) * 100. if math.isfinite(o) and math.isfinite(c) and min(o, c) > 0 else math.nan


def annotate_confirmation_volume(bar: dict[str, Any], minute_history: pd.DataFrame) -> dict[str, Any]:
    """Snapshot the causal denominator with the exact confirmation candle.

    History may span sessions and contain later rows: only the last 20 observed
    rows strictly before this candle enter the rolling mean, matching research.
    """
    result = dict(bar)
    cutoff = _stamp(bar.get("timestamp", bar.get("ts")))
    frame = minute_history.copy()
    column = "ts" if "ts" in frame else "timestamp" if "timestamp" in frame else "date"
    if frame.empty or column not in frame or "volume" not in frame:
        prior = pd.DataFrame(columns=["ts", "volume"])
    else:
        frame["ts"] = frame[column].map(_stamp)
        prior = (frame.loc[frame.ts.lt(cutoff), ["ts", "volume"]]
                 .sort_values("ts", kind="stable").drop_duplicates("ts", keep="last")
                 .tail(CONFIRMATION_VOLUME_LOOKBACK))
    volumes = pd.to_numeric(prior.volume, errors="coerce")
    valid = volumes.notna() & volumes.ge(0) & volumes.map(math.isfinite)
    count = int(valid.sum())
    mean = float(volumes.loc[valid].mean()) if count >= CONFIRMATION_VOLUME_MIN_PERIODS else math.nan
    current = _number(bar.get("volume"))
    ratio = current / mean if math.isfinite(current) and current >= 0 and math.isfinite(mean) and mean > 0 else math.nan
    evidence = [{"ts": row.ts.isoformat(), "volume": _number(row.volume)} for row in prior.itertuples()]
    result.update(v9_1m_feature_ts=cutoff.isoformat(),
                  v9_1m_volume_ratio=ratio if math.isfinite(ratio) else None,
                  confirmation_volume_policy=CONFIRMATION_VOLUME_POLICY,
                  confirmation_prior_volume_count=count,
                  confirmation_prior_volume_mean=mean if math.isfinite(mean) else None,
                  confirmation_prior_volume_last_ts=prior.ts.iloc[-1].isoformat() if len(prior) else "",
                  confirmation_prior_volume_sha256=hashlib.sha256(json.dumps(evidence, sort_keys=True).encode()).hexdigest())
    if cutoff.strftime("%H:%M") == "10:36" and not frame.empty and "ts" in frame:
        price_history = (frame.loc[frame.ts.lt(cutoff)]
                         .sort_values("ts", kind="stable").drop_duplicates("ts", keep="last"))
        ema20, atr14 = last_features(price_history, bar)
        result["v9_1m_ema20"] = ema20 if math.isfinite(ema20) else None
        result["v9_1m_atr14"] = atr14 if math.isfinite(atr14) else None
    return result


def round_to_tick(value: float, tick_size: float) -> float:
    tick = _number(tick_size)
    if not math.isfinite(tick) or tick <= 0:
        tick = .05
    return round(round(float(value) / tick) * tick, 8)


def bracket_levels(entry_price: float, side: str, stop_pct: float, target_pct: float,
                   tick_size: float) -> tuple[float, float]:
    direction = 1 if side.upper() == "LONG" else -1
    return (round_to_tick(entry_price * (1 - direction * stop_pct / 100.), tick_size),
            round_to_tick(entry_price * (1 + direction * target_pct / 100.), tick_size))


def confirmation_metrics(candidate: dict[str, Any], bar: dict[str, Any],
                         minute_history: pd.DataFrame | None = None) -> dict[str, Any]:
    if minute_history is not None:
        bar = annotate_confirmation_volume(bar, minute_history)
    result = dict(candidate)
    o, h, low, c = (_number(bar.get(k)) for k in ("open", "high", "low", "close"))
    result.update(confirm_open=o, confirm_high=h, confirm_low=low, confirm_close=c,
                  confirm_volume=_number(bar.get("volume")),
                  confirmation_timestamp=str(bar.get("timestamp", bar.get("ts", ""))),
                  confirmed=False, confirmation_reason="invalid_ohlc")
    for name, value in bar.items():
        if name.startswith(("v9_1m_", "confirmation_prior_volume_")) or name == "confirmation_volume_policy":
            result[name] = value
    for flag in ("gap_filled", "opening_snapshot", "provisional_stale", "confirmation_source_flagged"):
        result[flag] = _true(bar.get(flag, False))
        if result[flag]:
            result["confirmation_reason"] = "flagged_confirmation_source"
            return result
    if not all(math.isfinite(v) and v > 0 for v in (o, h, low, c)) or h <= low or h < max(o, c) or low > min(o, c):
        return result
    side = str(candidate.get("side", "")).upper()
    if side not in ("LONG", "SHORT"):
        result["confirmation_reason"] = "invalid_side"
        return result
    displacement = c - _number(candidate.get("signal_close"))
    directional = (c > o and displacement > 0) if side == "LONG" else (c < o and displacement < 0)
    result.update(body_ratio=abs(c-o)/(h-low),
                  wick_ratio=((h-max(o,c)) if side == "LONG" else (min(o,c)-low))/(h-low),
                  trigger=h if side == "LONG" else low,
                  confirmed=bool(directional), confirmation_reason="ok" if directional else "direction_rejected")
    try:
        signal = _stamp(candidate.get("signal_timestamp", candidate.get("signal_ts")))
        expected = signal + timedelta(minutes=1)
        if signal != signal.floor("min") or _stamp(result["confirmation_timestamp"]) != expected or _stamp(result.get("v9_1m_feature_ts")) != expected:
            raise ValueError("not exact next minute")
    except (TypeError, ValueError):
        result.update(confirmed=False, confirmation_reason="confirmation_clock_mismatch")
        return result
    ratio = _number(result.get("v9_1m_volume_ratio"))
    minimum = confirmation_volume_minimum(side, signal_end=signal.strftime("%H:%M"))
    result["minimum_confirmation_volume_ratio"] = minimum
    if not math.isfinite(ratio) or ratio < minimum:
        result.update(confirmed=False, confirmation_reason=f"confirmation_volume_below_{minimum:.2f}_or_missing")
    return result


def size_position(entry_price: float, lot_size: int, *, live: bool,
                  capital_rs: float = CAPITAL_PER_ENTRY_RS, leverage: float = LEVERAGE) -> PositionSize:
    values = tuple(map(_number, (entry_price, capital_rs, leverage)))
    if not all(math.isfinite(v) and v > 0 for v in values):
        raise ValueError("Entry, capital and exposure multiplier must be finite and positive")
    if live:
        # The decision contract's capital arguments remain PAPER-only. LIVE has
        # its own reviewed budget and cannot inherit a larger research budget.
        return PositionSize(**live_execution_policy.size_position(entry_price, lot_size))
    entry, capital, multiplier = values
    theoretical = int(math.floor(capital * multiplier / entry))
    lot = max(1, int(lot_size))
    quantity = theoretical
    return PositionSize(capital_rs=capital, leverage=multiplier, target_exposure_rs=capital * multiplier,
        theoretical_units=theoretical, quantity=quantity, lot_size=lot, estimated_exposure_rs=quantity * entry,
        state="PAPER_EXPOSURE_SIZED" if quantity else "BLOCKED_QUANTITY_OR_BUDGET")
