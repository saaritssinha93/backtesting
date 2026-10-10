"""Pinned active V13-v10-G selection contract for the V6 transport sessions.

The frozen baseline is the 14-slot F-core-first strategy. From session
2026-10-06 the dated policy adds the 09:25 LONG relaxation and staged stops.
Rejected extra-morning and two-bar experiments remain disabled. Market-data
helpers cannot fetch prices or submit orders.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

import fno_oi_common as common
import fno_oi_hybrid_data as hybrid
import fno_v13_v10_g_policy as policy
from fno_v5_live_config import PositionSize, SetupSpec


LIVE_GENERATION = "v6"  # Existing session IDs, feed routing and control namespace.
LIVE_LABEL = "V13-V10-G"
DISPLAY_LABEL = LIVE_LABEL
SESSION_PREFIX = "fno_v13_v10_g"
LIVE_ROOT_NAME = "v13_v10_g_live"
REPORT_PREFIX = "fno_v13_v10_g"
STRATEGY_VERSION = "FNO_V13_V10_G_RETAINED_20260914"
STRATEGY_PROFILE = "V13_V10_G"
CONTROL_ROOT_NAME = "v6_live"
SESSION_END = "15:32"
PIPELINE_DEADLINE = "11:23"
SELECTED_OBJECTIVE = "V13_V10_G_RETAINED_CORE_FIRST"
CONFIG_PATH = (common.FNO_ROOT / "strategy_research/v13_corrected_v10_g"
               / "run_20260914_opportunity_expansion/frozen_config.json")
CONFIG_SHA256 = "d8bcae37d7725279f8ac6e5c4c96a44e8a43f41b5e42d413b167fd806b52a127"
SELECTED_LEDGER_PATH = CONFIG_PATH.parent / "final/V13_V10_G/portfolio_trades.csv"
SELECTED_LEDGER_SHA256 = "d39217bfaaa7bf2a200f67fda4d53d4b418fddf849f3e7b5017cf18ca2bb1951"
SELECTED_DAILY_PATH = CONFIG_PATH.parent / "daily_detailed.csv"
EXPECTED_BACKTEST = dict(sessions=31, orders=73, fills=66, wins=43, losses=23,
                        trade_pf=3.4303108495913297, net_profit_rupees=178875.185650253)
BASE_PRICE_CHANGE_PCT = .10
BASE_OI_CHANGE_PCT = .05
BASE_VOLUME_RATIO = .80
MAX_OI_CHANGE_PCT = 1.
MIN_CONFIRMATION_VOLUME_RATIO = 1.20
CONFIRMATION_VOLUME_LOOKBACK = 20
CONFIRMATION_VOLUME_MIN_PERIODS = 5
CONFIRMATION_VOLUME_POLICY = "previous_20_observed_completed_1m_min5_excluding_current_v1"
LIVE_CONFIRMATION_VOLUME_REQUIRED_PRIOR = 20
LIVE_CONFIRMATION_HISTORY_DAYS = 7
LIVE_CONFIRMATION_HISTORY_FALLBACK_DAYS = 35
LIVE_CONFIRMATION_FIRST_MINUTE_END = "09:16"
LIVE_CONFIRMATION_LAST_MINUTE_END = "15:30"
NIFTY_FIRST_BAR_END = "09:20"
NIFTY_FIRST_BAR_MAX_RETURN_PCT = -.05
SQUARE_OFF = "15:15"
ROUND_TRIP_COST_BPS = 5.
ENTRY_EXPIRY_MINUTES = 10
ENTRY_TRIGGER_EXPIRY_SEC = 600
ENTRY_ACTIVATION_GRACE_SEC = 90
CONFIRMATION_MAX_WAIT_SEC = 90
CAPITAL_PER_ENTRY_RS = 100_000.
LEVERAGE = 5.
TARGET_EXPOSURE_RS = CAPITAL_PER_ENTRY_RS * LEVERAGE
PORTFOLIO_CAPITAL_RS = 1_000_000.
MAX_POSITIONS = None
LIVE_ACK_ENV = "FNO_V6_LIVE_ACK"
LIVE_ACK = "I_UNDERSTAND_REAL_FNO_V6_EQUITY_ORDERS"
ORDER_TAG_PREFIX = "FVG"
FNO_FETCH_SLOT_SCHEMA_VERSION = common.FNO_FETCH_SLOT_SCHEMA_VERSION
FNO_READINESS_POLICY = common.VERIFIED_NO_CANDLE_POLICY_VERSION
MIN_STOCK_FUTURES_COVERAGE = common.MIN_STOCK_FUTURES_COVERAGE
MAX_VERIFIED_NO_CANDLE_STOCKS = common.MAX_VERIFIED_NO_CANDLE_STOCKS
MIN_NO_CANDLE_FETCH_ATTEMPTS = common.MIN_NO_CANDLE_FETCH_ATTEMPTS
CONFIRMATION_FEED_SCHEMA_VERSION = common.EQUITY_1M_SLOT_SCHEMA_VERSION
CONFIRMATION_FEED_POLICY = "candidate_exact_completed_1m_verified_no_candle_v1"
CONFIRMATION_NO_CANDLE_OBSERVATIONS = 3
CONFIRMATION_NO_CANDLE_MIN_AGE_SEC = 15
CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC = 2.
CONFIRMATION_COMPLETED_BOUNDARY_BUFFER_SEC = 3.


def load_frozen_config(path: Path | str = CONFIG_PATH) -> dict[str, Any]:
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != CONFIG_SHA256:
        raise ValueError("Active G configuration is missing or has changed; promotion must be explicit.")
    settings = json.loads(raw)
    if settings.get("morning_slots", False) or settings.get("two_bar_continuation", False):
        raise ValueError("Rejected G expansions must remain disabled.")
    return settings


_SETTINGS = load_frozen_config()
# Exact F table: SHORT OI minimums already halved, with the raw 0.05% floor.
# Columns: signal, side, quota, picker, price, OI, 5m volume, 1m body, 1m wick.
_F_BOOK = (
    ("09:25", "LONG", 1, "max_liquidity", .30, .10, 3., .60, .60),
    ("09:25", "SHORT", 2, "max_volume", .20, .05, 1.5, .40, .60),
    ("09:30", "LONG", 1, "max_move", .65, .10, 1., .50, .60),
    ("09:30", "SHORT", 1, "max_move", .20, .125, 1., .40, .60),
    ("09:35", "LONG", 1, "max_liquidity", .20, .15, 1., .60, .60),
    ("09:35", "SHORT", 2, "max_liquidity", .50, .50, 1., .40, .60),
    ("09:40", "LONG", 1, "max_liquidity", .20, .075, 2., .50, .60),
    ("09:40", "SHORT", 1, "max_move", .20, .05, 1., .40, .60),
    ("09:45", "LONG", 1, "max_move", .65, .10, 1., .40, .60),
    ("09:45", "SHORT", 1, "max_volume", .20, .375, 1., .40, .40),
    ("09:55", "LONG", 1, "max_liquidity", .20, .10, 1., .40, .60),
    ("10:00", "LONG", 1, "max_liquidity", .40, .05, 1., .40, .60),
    ("09:50", "SHORT", 1, "max_liquidity", .20, .05, 1., .40, .60),
    ("11:20", "SHORT", 1, "max_liquidity", .20, .05, 1., .40, .60),
)


def _make_setup(row: tuple) -> SetupSpec:
    signal, side, quota, picker, price, oi, volume, body, wick = row
    confirmation = (datetime.strptime(signal, "%H:%M") + timedelta(minutes=1)).strftime("%H:%M")
    pair = _SETTINGS["exit"]["setups"][f"{confirmation.replace(':', '')}_{side}"]
    return SetupSpec(signal, confirmation, side, "FILTERED", quota, picker,
                     price, oi, volume, body, wick, 0., pair["stop_pct"],
                     pair["target_pct"], STRATEGY_VERSION)


CORE_SETUPS = tuple(_make_setup(row) for row in _F_BOOK)
ACTIVE_SETUPS = tuple(replace(s, price_change_pct=max(BASE_PRICE_CHANGE_PCT,
                     s.price_change_pct * (.65 if s.side == "SHORT" else 1.)))
                     for s in CORE_SETUPS)
SIGNAL_TO_CONFIRMATION = dict(sorted({s.signal_end: s.confirmation_end for s in ACTIVE_SETUPS}.items()))
_CORE_BY_ID = {s.setup_id: s for s in CORE_SETUPS}
_ACTIVE_BY_ID = {s.setup_id: s for s in ACTIVE_SETUPS}


def setup_for(signal_end: str, side: str, *, session_date: date | None = None) -> SetupSpec | None:
    setup = next((s for s in ACTIVE_SETUPS if s.signal_end == signal_end and s.side == side.upper()), None)
    if setup is None or not policy.enabled_for_session(session_date):
        return setup
    setup = replace(setup, stop_pct=policy.INITIAL_STOP_PCT)
    if setup.setup_id == "0926_LONG":
        setup = replace(setup, volume_ratio=policy.RELAXED_0925_LONG["minimum_volume_ratio"],
                        body_ratio=policy.RELAXED_0925_LONG["minimum_body_ratio"])
    return setup


def _candidate_day(row: Any, session_date: date | None = None) -> date | None:
    for key in ("signal_timestamp", "signal_ts", "timestamp", "ts"):
        value = row.get(key)
        if value is not None:
            try:
                observed = _stamp(value).date()
            except (TypeError, ValueError):
                return None
            if session_date is not None and observed != session_date:
                return None
            return observed
    return session_date


def slot_datetime(session_date: date, hhmm: str) -> datetime:
    hour, minute = map(int, hhmm.split(":"))
    return datetime.combine(session_date, datetime.min.time()).replace(hour=hour, minute=minute, tzinfo=common.IST)


def activation_deadline(session_date: date, confirmation_end: str) -> datetime:
    """Last permissible trigger/fill time; publication has its own deadline."""
    return slot_datetime(session_date, confirmation_end) + timedelta(seconds=ENTRY_TRIGGER_EXPIRY_SEC)


def confirmation_deadline(session_date: date, confirmation_end: str) -> datetime:
    return slot_datetime(session_date, confirmation_end) + timedelta(seconds=ENTRY_ACTIVATION_GRACE_SEC)


def entry_expiry(session_date: date, confirmation_end: str) -> datetime:
    return slot_datetime(session_date, confirmation_end) + timedelta(minutes=ENTRY_EXPIRY_MINUTES)


def _number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def _stamp(value: Any) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("Missing feature timestamp")
    return stamp.tz_localize(common.IST) if stamp.tzinfo is None else stamp.tz_convert(common.IST)


def base_signal_side(row: Any, signal_end: str = "", nifty_first_bar_return_pct: float | None = None,
                     *, session_date: date | None = None, original_only: bool = False) -> str | None:
    """Dated raw gates; the promotion only widens the 09:25 LONG candidate pool."""
    vals = {k: _number(row.get(k)) for k in ("ema9", "ema20", "ema50", "price_change_pct",
             "oi_change_pct", "volume_ratio", "oi", "prev_oi")}
    day = _candidate_day(row, session_date)
    if session_date is not None and day != session_date:
        return None
    if not original_only and signal_end == "09:25" and policy.enabled_for_session(day):
        required = ("price_change_pct", "oi_change_pct", "volume_ratio", "oi", "prev_oi")
        if (all(math.isfinite(vals[k]) for k in required)
                and vals["prev_oi"] > 0 and vals["oi"] > vals["prev_oi"]
                and .10 <= vals["oi_change_pct"] <= policy.RELAXED_0925_LONG["oi_max_pct"]
                and vals["price_change_pct"] >= .30
                and vals["volume_ratio"] >= policy.RELAXED_0925_LONG["minimum_volume_ratio"]):
            return "LONG"
    if not all(math.isfinite(v) for v in vals.values()):
        return None
    if (vals["prev_oi"] <= 0 or vals["oi"] <= vals["prev_oi"] or
            not BASE_OI_CHANGE_PCT <= vals["oi_change_pct"] <= MAX_OI_CHANGE_PCT or
            vals["volume_ratio"] < BASE_VOLUME_RATIO):
        return None
    if vals["ema9"] > vals["ema20"] > vals["ema50"] and vals["price_change_pct"] >= BASE_PRICE_CHANGE_PCT:
        return "LONG"
    if vals["ema9"] < vals["ema20"] < vals["ema50"] and vals["price_change_pct"] <= -BASE_PRICE_CHANGE_PCT:
        if signal_end == "09:25":
            context = _number(nifty_first_bar_return_pct if nifty_first_bar_return_pct is not None
                              else row.get("nifty_first_bar_return_pct"))
            if not math.isfinite(context) or context > NIFTY_FIRST_BAR_MAX_RETURN_PCT:
                return None
        return "SHORT"
    return None


def nifty_context_from_bars(frame: pd.DataFrame, session_date: date) -> float:
    """Caller must supply the dated near-month NIFTY futures, not cash index bars."""
    column = "ts" if "ts" in frame else "timestamp"
    if frame.empty or column not in frame:
        return math.nan
    stamps = pd.to_datetime(frame[column], errors="coerce")
    stamps = stamps.dt.tz_localize(common.IST) if stamps.dt.tz is None else stamps.dt.tz_convert(common.IST)
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
    return result


def confirmation_metrics(candidate: dict[str, Any], bar: dict[str, Any], minute_history: pd.DataFrame | None = None) -> dict[str, Any]:
    if minute_history is not None:
        bar = annotate_confirmation_volume(bar, minute_history)
    result = dict(candidate)
    o, h, low, c = (_number(bar.get(k)) for k in ("open", "high", "low", "close"))
    result.update(confirm_open=o, confirm_high=h, confirm_low=low, confirm_close=c,
                  confirm_volume=_number(bar.get("volume")),
                  confirmation_timestamp=str(bar.get("timestamp", bar.get("ts", ""))))
    for name, value in bar.items():
        if name.startswith(("v9_1m_", "confirmation_prior_volume_")) or name == "confirmation_volume_policy":
            result[name] = value
    result.update(confirmed=False, confirmation_reason="invalid_ohlc")
    if not all(math.isfinite(v) and v > 0 for v in (o, h, low, c)) or h <= low or h < max(o, c) or low > min(o, c):
        return result
    side = str(candidate.get("side", "")).upper()
    if side not in ("LONG", "SHORT"):
        result["confirmation_reason"] = "invalid_side"
        return result
    long_side = side == "LONG"
    displacement = c - _number(candidate.get("signal_close"))
    directional = (c > o and displacement > 0) if long_side else (c < o and displacement < 0)
    result.update(body_ratio=abs(c - o) / (h - low),
                  wick_ratio=((h - max(o, c)) if long_side else (min(o, c) - low)) / (h - low),
                  trigger=h if long_side else low,
                  confirmed=bool(directional), confirmation_reason="ok" if directional else "direction_rejected")
    try:
        signal_ts = _stamp(candidate.get("signal_timestamp", candidate.get("signal_ts")))
        expected = signal_ts + timedelta(minutes=1)
        if (_stamp(result["confirmation_timestamp"]) != expected or
                _stamp(result.get("v9_1m_feature_ts")) != expected):
            raise ValueError("not exact next minute")
    except (TypeError, ValueError):
        result.update(confirmed=False, confirmation_reason="confirmation_clock_mismatch")
        return result
    ratio = _number(result.get("v9_1m_volume_ratio"))
    if not math.isfinite(ratio) or ratio < MIN_CONFIRMATION_VOLUME_RATIO:
        result.update(confirmed=False, confirmation_reason="confirmation_volume_below_1_20_or_missing")
    return result


def passes_selected_filters(candidate: dict[str, Any], setup: SetupSpec, *,
                            session_date: date | None = None, original_only: bool = False) -> bool:
    if str(candidate.get("side", "")).upper() != setup.side:
        return False
    try:
        signal_ts = _stamp(candidate.get("signal_timestamp", candidate.get("signal_ts")))
        conf_ts = _stamp(candidate.get("confirmation_timestamp", candidate.get("confirmation_ts")))
        feature_ts = _stamp(candidate.get("v9_1m_feature_ts"))
        if session_date is not None and signal_ts.date() != session_date:
            return False
        if (signal_ts.strftime("%H:%M") != setup.signal_end or conf_ts.strftime("%H:%M") != setup.confirmation_end
                or signal_ts != signal_ts.floor("min")
                or conf_ts != signal_ts + timedelta(minutes=1) or feature_ts != conf_ts):
            return False
    except (TypeError, ValueError):
        return False
    if candidate.get("signal_end", setup.signal_end) != setup.signal_end:
        return False
    if candidate.get("confirmed", True) is False:
        return False
    if base_signal_side(candidate, setup.signal_end, session_date=session_date,
                        original_only=original_only) != setup.side:
        return False
    if not original_only:
        setup = setup_for(setup.signal_end, setup.side, session_date=signal_ts.date())
    vals = {k: _number(candidate.get(k)) for k in ("price_change_pct", "oi_change_pct", "volume_ratio",
            "body_ratio", "wick_ratio", "traded_value", "v9_1m_volume_ratio")}
    if not all(math.isfinite(v) for v in vals.values()):
        return False
    price = vals["price_change_pct"] * (1 if setup.side == "LONG" else -1)
    return (price >= setup.price_change_pct and vals["oi_change_pct"] >= setup.oi_change_pct
            and vals["volume_ratio"] >= setup.volume_ratio
            and setup.body_ratio <= vals["body_ratio"] <= 1.
            and 0 <= vals["wick_ratio"] <= setup.max_wick_ratio
            and vals["traded_value"] >= setup.min_traded_value
            and vals["v9_1m_volume_ratio"] >= MIN_CONFIRMATION_VOLUME_RATIO)


def picker_value(candidate: dict[str, Any], picker: str) -> float:
    name = {"max_oi": "oi_change_pct", "max_volume": "volume_ratio", "max_move": "price_change_pct",
            "max_body": "body_ratio", "max_liquidity": "traded_value"}[picker]
    value = float(candidate[name])
    return abs(value) if picker == "max_move" else value


def rank_candidates(candidates: list[dict[str, Any]], setup: SetupSpec, *,
                    session_date: date | None = None) -> list[dict[str, Any]]:
    eligible = [row for row in candidates if passes_selected_filters(row, setup, session_date=session_date)]
    ranked = sorted(eligible, key=lambda row: (-picker_value(row, setup.picker),
                    -float(row["traded_value"]), str(row["tradingsymbol"])))
    core_setup = _CORE_BY_ID[setup.setup_id]
    core = [row for row in ranked if passes_selected_filters(row, core_setup, session_date=session_date,
                                                            original_only=True)][:core_setup.max_entries]
    core_symbols = {str(row["tradingsymbol"]) for row in core}
    extras = [row for row in ranked if str(row["tradingsymbol"]) not in core_symbols][:setup.max_entries - len(core)]
    return [{**row, "v10_g_f_core": str(row["tradingsymbol"]) in core_symbols,
             "relaxed_0925_added": setup.setup_id == "0926_LONG"
                 and policy.enabled_for_session(_candidate_day(row, session_date))
                 and not passes_selected_filters(row, _ACTIVE_BY_ID[setup.setup_id],
                                                 session_date=session_date, original_only=True),
             "strategy_policy": policy.policy_for_day(_candidate_day(row, session_date))}
            for row in core + extras]


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


def size_position(entry_price: float, lot_size: int, *, live: bool,
                  capital_rs: float = CAPITAL_PER_ENTRY_RS, leverage: float = LEVERAGE) -> PositionSize:
    values = tuple(map(_number, (entry_price, capital_rs, leverage)))
    if not all(math.isfinite(v) and v > 0 for v in values):
        raise ValueError("Entry, capital and exposure multiplier must be finite and positive")
    entry, capital, multiplier = values
    exposure, lot = capital * multiplier, max(1, int(lot_size))
    theoretical = int(math.floor(exposure / entry))
    quantity = theoretical // lot * lot if live else theoretical
    return PositionSize(capital_rs=capital, leverage=multiplier, target_exposure_rs=exposure,
                        theoretical_units=theoretical, quantity=quantity, lot_size=lot,
                        estimated_exposure_rs=quantity * entry,
                        state=("LIVE_LOT_SIZED" if live else "PAPER_EXPOSURE_SIZED") if quantity
                        else ("BLOCKED_LOT_EXCEEDS_BUDGET" if live else "BLOCKED_PRICE_EXCEEDS_BUDGET"))


def strategy_payload() -> dict[str, Any]:
    return dict(live_generation=LIVE_GENERATION, strategy_version=STRATEGY_VERSION,
                selected_objective=SELECTED_OBJECTIVE, data_contract=hybrid.DATA_CONTRACT_VERSION,
                frozen_config_sha256=CONFIG_SHA256, frozen_config=copy.deepcopy(_SETTINGS),
                price_volume_indicator_source="NSE_EQUITY", oi_source="NFO_FUTURE",
                confirmation_entry_exit_instrument="NSE_EQUITY", signal_to_confirmation=SIGNAL_TO_CONFIRMATION,
                active_setups=[asdict(s) for s in ACTIVE_SETUPS], core_setups=[asdict(s) for s in CORE_SETUPS],
                maximum_oi_change_pct=MAX_OI_CHANGE_PCT, confirmation_volume_policy=CONFIRMATION_VOLUME_POLICY,
                confirmation_volume_minimum=MIN_CONFIRMATION_VOLUME_RATIO,
                live_confirmation_volume_warmup=dict(
                    source="BROKER_COMPLETED_REGULAR_SESSION_ONE_MINUTE",
                    required_prior_observations=LIVE_CONFIRMATION_VOLUME_REQUIRED_PRIOR,
                    initial_history_days=LIVE_CONFIRMATION_HISTORY_DAYS,
                    fallback_history_days=LIVE_CONFIRMATION_HISTORY_FALLBACK_DAYS,
                    first_minute_end=LIVE_CONFIRMATION_FIRST_MINUTE_END,
                    last_minute_end=LIVE_CONFIRMATION_LAST_MINUTE_END,
                    insufficient_history_action="FAIL_CLOSED",
                    research_min_periods=CONFIRMATION_VOLUME_MIN_PERIODS,
                    operational_difference="Live requires 20 prior candles to avoid a truncated rolling denominator; research accepts min5."),
                nifty_gate=dict(signal="09:25", side="SHORT", first_bar_end=NIFTY_FIRST_BAR_END,
                                instrument="DATED_NEAR_MONTH_NIFTY_FUTURE", maximum_return_pct=NIFTY_FIRST_BAR_MAX_RETURN_PCT),
                square_off=SQUARE_OFF, entry_activation_grace_sec=ENTRY_ACTIVATION_GRACE_SEC,
                entry_expiry_minutes=ENTRY_EXPIRY_MINUTES, capital_per_entry_rs=CAPITAL_PER_ENTRY_RS,
                leverage=LEVERAGE, target_exposure_rs=TARGET_EXPOSURE_RS,
                portfolio_capital_rs=PORTFOLIO_CAPITAL_RS, max_positions=MAX_POSITIONS,
                partial_exits=False, breakeven_stop=False, round_trip_cost_bps=ROUND_TRIP_COST_BPS,
                futures_readiness=dict(schema=FNO_FETCH_SLOT_SCHEMA_VERSION,
                                       policy=FNO_READINESS_POLICY,
                                       minimum_stock_coverage=MIN_STOCK_FUTURES_COVERAGE,
                                       maximum_verified_no_candle_stocks=MAX_VERIFIED_NO_CANDLE_STOCKS,
                                       minimum_fetch_attempts=MIN_NO_CANDLE_FETCH_ATTEMPTS),
                confirmation_feed_schema=CONFIRMATION_FEED_SCHEMA_VERSION,
                confirmation_feed_policy=CONFIRMATION_FEED_POLICY,
                confirmation_completed_boundary_buffer_sec=CONFIRMATION_COMPLETED_BOUNDARY_BUFFER_SEC,
                confirmation_no_candle_observations=CONFIRMATION_NO_CANDLE_OBSERVATIONS,
                confirmation_no_candle_min_age_sec=CONFIRMATION_NO_CANDLE_MIN_AGE_SEC,
                confirmation_no_candle_spacing_sec=CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC,
                scheduled_promotion=dict(
                    **policy.policy_for_day(policy.EFFECTIVE_DATE),
                    relaxed_0925_thresholds=dict(policy.RELAXED_0925_LONG),
                    selection_priority="ORIGINAL_G_CHOICES_FIRST_WITHIN_EXISTING_QUOTA",
                    target_policy="UNCHANGED_FROM_RETAINED_G",
                    timing_reference="ACTUAL_ENTRY; BAR_REPLAY_USES_ENTRY_BAR_END",
                    active_setups=[asdict(setup_for(s.signal_end, s.side, session_date=policy.EFFECTIVE_DATE))
                                   for s in ACTIVE_SETUPS]),
                expected_backtest=EXPECTED_BACKTEST)


def strategy_fingerprint() -> str:
    return hashlib.sha256(json.dumps(strategy_payload(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def validate_strategy() -> None:
    if load_frozen_config() != _SETTINGS:
        raise AssertionError("Frozen G settings changed after import")
    if len(ACTIVE_SETUPS) != 14 or len({s.setup_id for s in ACTIVE_SETUPS}) != 14:
        raise AssertionError("Retained G must have exactly the original 14 setups")
    if (CAPITAL_PER_ENTRY_RS, LEVERAGE, PORTFOLIO_CAPITAL_RS, MAX_POSITIONS,
            ENTRY_EXPIRY_MINUTES, MIN_CONFIRMATION_VOLUME_RATIO) != (100_000., 5., 1_000_000., None, 10, 1.2):
        raise AssertionError("Pinned G portfolio or timing settings changed")
    if any(s.stop_pct < .6 or s.stop_pct > 1.5 or s.target_pct > 3. or
           s.target_pct / s.stop_pct < 1.5 - 1e-12 for s in ACTIVE_SETUPS):
        raise AssertionError("Pinned B exit constraints violated")
    for original in ACTIVE_SETUPS:
        promoted = setup_for(original.signal_end, original.side, session_date=policy.EFFECTIVE_DATE)
        if (promoted.stop_pct != policy.INITIAL_STOP_PCT or promoted.target_pct != original.target_pct
                or promoted.max_entries != original.max_entries):
            raise AssertionError("Promoted G stop, target or setup quota drift")


def attest_selected_backtest(path: Path | str = SELECTED_LEDGER_PATH, *, require_provenance: bool = True) -> dict[str, Any]:
    """Attest baseline provenance; this is not validation of the promoted rules."""
    validate_strategy()
    selected = Path(path)
    if selected.resolve() != SELECTED_LEDGER_PATH.resolve():
        raise ValueError("Only the pinned retained G ledger can attest this live strategy")
    if hashlib.sha256(selected.read_bytes()).hexdigest() != SELECTED_LEDGER_SHA256:
        raise AssertionError("Retained G selected ledger changed")
    return {**EXPECTED_BACKTEST, "selected_ledger_sha256": SELECTED_LEDGER_SHA256,
            "frozen_config_sha256": CONFIG_SHA256,
            "attestation_scope": "FROZEN_RETAINED_G_BASELINE_ONLY",
            "scheduled_promotion": policy.policy_for_day(policy.EFFECTIVE_DATE),
            "promoted_rules_independently_validated": False,
            "evidence": "EXPLORATORY_REUSED_HISTORY_NO_UNTOUCHED_TEST"}


validate_strategy()
