"""Pure accepted G3 decision contract, shared by replay and runtime.

Importing this module performs no file, broker, network, clock or runtime-root
access. Frozen evidence is inspected only by an explicit verification call.
Outcome columns never participate in a decision. All percentages use percent
units, e.g. 1.25 is a 1.25 percent stop.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    import pandas as pd

from fno_v13_v10_g_identity import canonical_signal_id
import fno_v13_v10_g_3_late_setups as late_setups
import fno_v13_v10_g_3_selected_setups as selected_setups
import fno_v13_v10_g_3_midday_setups as midday_setups
import fno_v13_v10_g_3_afternoon_setups as afternoon_setups
from g3_one_minute_extension import extension_atr

IST = ZoneInfo("Asia/Kolkata")
STRATEGY_PROFILE = "V13_V10_G_3"
STRATEGY_VERSION = "V13-V10-G-3-PRODUCTION-V11"
PREVIOUS_STRATEGY_VERSION = "V13-V10-G-3-PRODUCTION-V10"
PREVIOUS_STRATEGY_FINGERPRINT = "04697846e334466cf902d7977cefe186c04cfafdb66b4a6582d250405f7b9e7d"
LEGACY_STRATEGY_VERSION = "V13-V10-G-3-PRODUCTION-V1"
LEGACY_STRATEGY_FINGERPRINT = "9761be9583ef63f4627bad43d3b67f6c5b12480e09a5d853325fe2b994e82275"
SIGNAL_ID_SCHEMA_VERSION = "eqidv2.v13_v10_g_3.signal_id.v1"
ACCEPTED_VARIANT = afternoon_setups.ACCEPTED_VARIANT + "_0941_LONG_VOLUME080_1035_1M_EXTENSION_ATR3_0920_SHORT_1010_LONG_TARGET150_0926_LONG_5M_VOLUME120"
ADDED_SETUP_IDS = frozenset({"0921_SHORT", "1011_LONG"})
FROZEN_ACCEPTED_VARIANT = "G3_W1_V1p1"
FROZEN_VERSION = "V13-V10-G-3-FROZEN-LONG110-NEXTMINUTE-V1"
FROZEN_CONFIG_SHA256 = "788fbb3dbda6cf86d3f0f26a9db777e7462ba64c7906368cca077e983f72552a"
FROZEN_LEDGER_SHA256 = "f7287c21ebeba5cd1d2346fb241e6daafc270d33822e979d681f3f508690aaf3"
BASE_PRICE_CHANGE_PCT = .10
BASE_OI_CHANGE_PCT = .05
BASE_VOLUME_RATIO = .80
MAX_OI_CHANGE_PCT = 1.
MIN_CONFIRMATION_VOLUME_BY_SIDE = {"LONG": 1.10, "SHORT": 1.20}
MIN_CONFIRMATION_VOLUME_BY_SETUP = {"0921_LONG": 1.80, "0921_SHORT": 1.20, "0941_LONG": .80, "0951_LONG": .60, "1011_LONG": .60,
    **late_setups.CONFIRMATION_VOLUME, **selected_setups.CONFIRMATION_VOLUME,
    **midday_setups.CONFIRMATION_VOLUME, **afternoon_setups.CONFIRMATION_VOLUME}
CONFIRMATION_VOLUME_LOOKBACK = 20
CONFIRMATION_VOLUME_MIN_PERIODS = 5
CONFIRMATION_VOLUME_POLICY = "previous_20_observed_completed_1m_min5_excluding_current_v1"
MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP = {"1036_LONG": 3.0, "1036_SHORT": 3.0}
CONFIRMATION_EXTENSION_POLICY = "completed_1m_close_vs_ema20_over_sma_true_range14_v1"
LIVE_CONFIRMATION_VOLUME_REQUIRED_PRIOR = 20
LIVE_CONFIRMATION_HISTORY_DAYS = 7
LIVE_CONFIRMATION_HISTORY_FALLBACK_DAYS = 35
LIVE_CONFIRMATION_FIRST_MINUTE_END = "09:16"
LIVE_CONFIRMATION_LAST_MINUTE_END = "15:30"
NIFTY_FIRST_BAR_END = "09:20"
NIFTY_FIRST_BAR_MAX_RETURN_PCT = -.05
NIFTY_SHORT_SIGNAL_ENDS = ("09:20", "09:25")
INITIAL_STOP_PCT = 1.25
TIGHTENED_STOP_PCT = 1.
TIGHTEN_AFTER_MINUTES = 120
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
LIVE_QUANTITY_CAP = 1
ARMED = False
EXECUTION_AUTHORITY = False


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


# Selection fields are the frozen setup_rules.core table. Final two columns
# are effective exit.setups targets, NOT donor setup_rules targets/stops.
_SETUP_ROWS = (
    ("09:20", "LONG", 1, "max_liquidity", .30, .10, 3., .75, .35, 3.),
    # User-enabled slots: donor entry filters retained; targets set to 1.50%.
    ("09:20", "SHORT", 2, "max_volume", .20, .05, 1.5, .40, .60, 1.50),
    ("09:25", "LONG", 1, "max_liquidity", .30, .10, 1.2, .60, .60, .97),
    ("09:25", "SHORT", 2, "max_volume", .20, .05, 1.5, .40, .60, 2.67),
    ("09:30", "LONG", 1, "max_move", .65, .10, 1., .50, .60, 2.),
    ("09:30", "SHORT", 1, "max_move", .20, .125, 1., .40, .60, 2.),
    ("09:35", "LONG", 1, "max_liquidity", .20, .15, 1., .60, .60, 1.73),
    ("09:35", "SHORT", 2, "max_liquidity", .50, .50, 1., .40, .60, 1.23),
    ("09:40", "LONG", 1, "max_liquidity", .20, .075, 2., .50, .60, 3.),
    ("09:40", "SHORT", 1, "max_move", .20, .05, 1., .40, .60, 2.43),
    ("09:45", "LONG", 1, "max_move", .65, .10, 1., .40, .60, 1.23),
    ("09:45", "SHORT", 1, "max_volume", .20, .375, 1., .40, .40, 1.23),
    ("09:50", "LONG", 1, "max_liquidity", .10, .05, 1.3, .50, .50, .75),
    ("09:50", "SHORT", 1, "max_liquidity", .20, .05, 1., .40, .60, .93),
    ("09:55", "LONG", 1, "max_liquidity", .20, .10, 1., .40, .60, .90),
    ("10:00", "LONG", 1, "max_liquidity", .40, .05, 1., .40, .60, 3.),
    ("10:10", "LONG", 1, "max_liquidity", .10, .05, 1.3, .50, .50, 1.50),
    ("11:20", "SHORT", 1, "max_liquidity", .20, .05, 1., .40, .60, 2.),
    *late_setups.ROWS,
    *selected_setups.ROWS,
    *midday_setups.ROWS,
    *afternoon_setups.ROWS,
)


def _make_setup(row: tuple) -> SetupSpec:
    signal, side, quota, picker, price, oi, volume, body, wick, target = row
    confirmation = (datetime.strptime(signal, "%H:%M") + timedelta(minutes=1)).strftime("%H:%M")
    return SetupSpec(signal, confirmation, side, "FILTERED", quota, picker,
                     price, oi, volume, body, wick, 0., INITIAL_STOP_PCT, target, STRATEGY_VERSION)


CORE_SETUPS = tuple(_make_setup(row) for row in _SETUP_ROWS)
# Late G4 thresholds are already effective, with identical core/expanded pools.
ACTIVE_SETUPS = tuple(replace(s, price_change_pct=max(BASE_PRICE_CHANGE_PCT,
                     s.price_change_pct * (.65 if s.side == "SHORT" and s.setup_id not in
                         (late_setups.SETUP_IDS | selected_setups.SETUP_IDS | midday_setups.SETUP_IDS | afternoon_setups.SETUP_IDS) else 1.))) for s in CORE_SETUPS)
SIGNAL_TO_CONFIRMATION = dict(sorted({s.signal_end: s.confirmation_end for s in ACTIVE_SETUPS}.items()))
_CORE_BY_ID = {s.setup_id: s for s in CORE_SETUPS}
_ACTIVE_BY_ID = {s.setup_id: s for s in ACTIVE_SETUPS}


def setup_for(signal_end: str, side: str, *, session_date: date | None = None) -> SetupSpec | None:
    """G3 is a separate profile: no date enables G's relaxed 09:25 fallback."""
    return next((s for s in ACTIVE_SETUPS if s.signal_end == signal_end and s.side == side.upper()), None)


def confirmation_volume_minimum(side: str, *, signal_end: str | None = None,
                                setup_id: str | None = None) -> float:
    """Apply an exception only to its exact setup; retain other side defaults."""
    try:
        side = str(side).upper()
        minimum = MIN_CONFIRMATION_VOLUME_BY_SIDE[side]
    except KeyError as exc:
        raise ValueError("Confirmation side must be LONG or SHORT") from exc
    if signal_end is not None:
        setup = setup_for(signal_end, side)
        expected_id = setup.setup_id if setup is not None else None
        if setup_id is not None and setup_id != expected_id:
            raise ValueError("Confirmation setup clock mismatch")
        setup_id = expected_id
    if setup_id is not None and not setup_id.endswith("_" + side):
        raise ValueError("Confirmation setup side mismatch")
    return MIN_CONFIRMATION_VOLUME_BY_SETUP.get(setup_id, minimum)


def _number(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def _stamp(value: Any) -> pd.Timestamp:
    import pandas as pd
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("Missing feature timestamp")
    return stamp.tz_localize(IST) if stamp.tzinfo is None else stamp.tz_convert(IST)


def _true(value: Any) -> bool:
    return str(value).strip().lower() in ("true", "1", "1.0", "yes", "on")


def _candidate(row: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for canonical, aliases in {
        "signal_timestamp": ("signal_ts",), "confirmation_timestamp": ("confirmation_ts",),
        "ema9": ("v9_5m_ema9",), "ema20": ("v9_5m_ema20",), "ema50": ("v9_5m_ema50",),
        "confirm_open": ("confirmation_open",), "confirm_high": ("confirmation_high",),
        "confirm_low": ("confirmation_low",), "confirm_close": ("confirmation_close",),
    }.items():
        if canonical not in out:
            for alias in aliases:
                if alias in out:
                    out[canonical] = out[alias]
                    break
    return out


def base_signal_side(row: Mapping[str, Any], signal_end: str = "", nifty_first_bar_return_pct: float | None = None,
                     *, session_date: date | None = None, original_only: bool = False) -> str | None:
    row = _candidate(row)
    if signal_end == "09:20":
        # Never accept the usual previous-session price/OI differences here.
        if (row.get("opening_0920_status") != "VALID"
                or row.get("price_change_basis") != "09:15_CASH_OPEN_TO_09:20_CASH_CLOSE"
                or row.get("oi_change_source") not in {
                    "FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M", "PROVISIONAL_FULL_QUOTE"}):
            return None
    if session_date is not None:
        try:
            if _stamp(row.get("signal_timestamp")).date() != session_date:
                return None
        except (TypeError, ValueError):
            return None
    vals = {k: _number(row.get(k)) for k in ("ema9", "ema20", "ema50", "price_change_pct",
             "oi_change_pct", "volume_ratio", "oi", "prev_oi")}
    if not all(math.isfinite(v) for v in vals.values()):
        return None
    if (vals["prev_oi"] <= 0 or vals["oi"] <= vals["prev_oi"] or
            not BASE_OI_CHANGE_PCT <= vals["oi_change_pct"] <= MAX_OI_CHANGE_PCT or
            vals["volume_ratio"] < BASE_VOLUME_RATIO):
        return None
    if vals["ema9"] > vals["ema20"] > vals["ema50"] and vals["price_change_pct"] >= BASE_PRICE_CHANGE_PCT:
        return "LONG"
    if vals["ema9"] < vals["ema20"] < vals["ema50"] and vals["price_change_pct"] <= -BASE_PRICE_CHANGE_PCT:
        if signal_end in NIFTY_SHORT_SIGNAL_ENDS:
            context = _number(nifty_first_bar_return_pct if nifty_first_bar_return_pct is not None
                              else row.get("nifty_first_bar_return_pct"))
            if not math.isfinite(context) or context > NIFTY_FIRST_BAR_MAX_RETURN_PCT:
                return None
        return "SHORT"
    return None


def _confirmation_valid(row: Mapping[str, Any], side: str) -> bool:
    for flag in ("gap_filled", "opening_snapshot", "provisional_stale", "confirmation_source_flagged"):
        if _true(row.get(flag, False)):
            return False
    if "v9_exact_confirmation_present" in row and not _true(row["v9_exact_confirmation_present"]):
        return False
    if "source_1m_count" in row and _number(row["source_1m_count"]) != 5:
        return False
    if "confirmed" in row and not _true(row["confirmed"]):
        return False
    fields = ("confirm_open", "confirm_high", "confirm_low", "confirm_close")
    if any(k in row for k in fields):
        o, h, low, c = (_number(row.get(k)) for k in fields)
        signal_close = _number(row.get("signal_close"))
        if not all(math.isfinite(v) and v > 0 for v in (o, h, low, c, signal_close)):
            return False
        if h <= low or h < max(o, c) or low > min(o, c):
            return False
        return (c > o and c > signal_close) if side == "LONG" else (c < o and c < signal_close)
    # Live confirmation_metrics can supply an explicit validated decision.
    # Raw feature rows without either OHLC evidence or that decision fail closed.
    return _true(row.get("confirmed", False))


def passes_selected_filters(candidate: Mapping[str, Any], setup: SetupSpec, *,
                            session_date: date | None = None, original_only: bool = False) -> bool:
    row = _candidate(candidate)
    if str(row.get("side", "")).upper() != setup.side:
        return False
    try:
        signal = _stamp(row.get("signal_timestamp"))
        confirmation = _stamp(row.get("confirmation_timestamp"))
        feature = _stamp(row.get("v9_1m_feature_ts"))
        if (session_date is not None and signal.date() != session_date or
                signal.strftime("%H:%M") != setup.signal_end or signal != signal.floor("min") or
                confirmation != signal + timedelta(minutes=1) or feature != confirmation):
            return False
        if "v9_5m_feature_ts" in row and _stamp(row["v9_5m_feature_ts"]) > signal:
            return False
        if "v9_feature_available_ts" in row and _stamp(row["v9_feature_available_ts"]) > confirmation:
            return False
    except (TypeError, ValueError):
        return False
    if row.get("signal_end", setup.signal_end) != setup.signal_end:
        return False
    if not _confirmation_valid(row, setup.side) or base_signal_side(row, setup.signal_end, session_date=session_date) != setup.side:
        return False
    vals = {k: _number(row.get(k)) for k in ("price_change_pct", "oi_change_pct", "volume_ratio",
            "body_ratio", "wick_ratio", "traded_value", "v9_1m_volume_ratio")}
    if not all(math.isfinite(v) for v in vals.values()):
        return False
    price = vals["price_change_pct"] * (1 if setup.side == "LONG" else -1)
    if setup.setup_id in MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP:
        extension = extension_atr(row.get("confirm_close"), row.get("v9_1m_ema20"),
                                  row.get("v9_1m_atr14"), setup.side)
        if not math.isfinite(extension) or extension > MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP[setup.setup_id]:
            return False
    return (price >= setup.price_change_pct and vals["oi_change_pct"] >= setup.oi_change_pct
            and vals["volume_ratio"] >= setup.volume_ratio
            and setup.body_ratio <= vals["body_ratio"] <= 1.
            and 0 <= vals["wick_ratio"] <= setup.max_wick_ratio
            and vals["traded_value"] >= setup.min_traded_value
            and vals["v9_1m_volume_ratio"] >= confirmation_volume_minimum(setup.side, setup_id=setup.setup_id))


def picker_value(candidate: Mapping[str, Any], picker: str) -> float:
    name = {"max_oi": "oi_change_pct", "max_volume": "volume_ratio", "max_move": "price_change_pct",
            "max_body": "body_ratio", "max_liquidity": "traded_value"}[picker]
    value = float(candidate[name])
    return abs(value) if picker == "max_move" else value


def _rank_key(row: Mapping[str, Any], setup: SetupSpec) -> tuple:
    return (-picker_value(row, setup.picker), -float(row["traded_value"]), str(row["tradingsymbol"]))


def rank_candidates(candidates: list[dict[str, Any]], setup: SetupSpec, *,
                    session_date: date | None = None) -> list[dict[str, Any]]:
    eligible = [row for row in candidates if passes_selected_filters(row, setup, session_date=session_date)]
    days = {_stamp(_candidate(row)["signal_timestamp"]).date() for row in eligible}
    if len(days) > 1:
        raise ValueError("rank_candidates expects one session; use select_orders for multiple dates")
    symbols = [str(row["tradingsymbol"]) for row in eligible]
    if len(set(symbols)) != len(symbols):
        raise ValueError("Duplicate candidate for one setup/session/symbol")
    ranked = sorted(eligible, key=lambda row: _rank_key(row, setup))
    core_setup = _CORE_BY_ID[setup.setup_id]
    core = [row for row in ranked if passes_selected_filters(row, core_setup, session_date=session_date)][:core_setup.max_entries]
    core_symbols = {str(row["tradingsymbol"]) for row in core}
    extras = [row for row in ranked if str(row["tradingsymbol"]) not in core_symbols][:setup.max_entries - len(core)]
    from fno_v13_v10_g_3_policy import policy_for_day
    return [{**row, "v10_g_f_core": str(row["tradingsymbol"]) in core_symbols,
             "relaxed_0925_added": False,
             "strategy_policy": policy_for_day(_stamp(_candidate(row)["signal_timestamp"]).date())}
            for row in core + extras]


def selection_audit(signals: pd.DataFrame) -> pd.DataFrame:
    """Audit all matched setup rows; select from complete session/setup pools.

    Raw OHLC/EMA/OI data are rechecked. No threshold rescaling or outcome field
    is used. Native columns are preserved for the immutable execution simulator.
    """
    import pandas as pd
    metadata = ["setup_id", "native_stop_pct", "native_target_pct", "configured_confirmation_end",
                "picker", "max_entries", "v10_g_f_core", "v10_g_morning_slot", "v9_selected",
                "v9_filter_pass", "v9_rank_in_setup_day", "v9_decision", "v9_configuration",
                "strategy_version", "strategy_fingerprint", "signal_id", "day", "hhmm_int", "sid"]
    if signals.empty:
        return pd.DataFrame(columns=list(dict.fromkeys([*signals.columns, *metadata])))
    if "sid" in signals and signals.sid.duplicated().any():
        raise ValueError("Duplicate signal IDs")
    pools: dict[tuple, list[dict]] = {}
    for index, record in enumerate(signals.to_dict("records")):
        row = _candidate(record)
        try:
            stamp = _stamp(row.get("signal_timestamp"))
        except (TypeError, ValueError):
            continue
        signal_end = stamp.strftime("%H:%M")
        side = str(row.get("side", "")).upper()
        if side not in ("LONG", "SHORT"):
            side = base_signal_side(row, signal_end) or ""
        setup = setup_for(signal_end, side)
        if setup is None:
            continue
        day = stamp.date()
        if "day" in row and _stamp(row["day"]).date() != day:
            raise ValueError("Candidate day conflicts with signal timestamp")
        try:
            confirmation_stamp = _stamp(row.get("confirmation_timestamp"))
        except (TypeError, ValueError):
            confirmation_stamp = pd.NaT
        if "wick_ratio" not in row:
            row["wick_ratio"] = row.get("v9_1m_upper_wick_ratio" if side == "LONG" else "v9_1m_lower_wick_ratio")
        if "trigger" not in row:
            row["trigger"] = row.get("confirm_high" if side == "LONG" else "confirm_low")
        row.update(day=day, side=side, signal_ts=stamp,
                   confirmation_ts=confirmation_stamp,
                   hhmm=stamp.strftime("%H%M"), hhmm_int=int(stamp.strftime("%H%M")),
                   sid=row.get("sid", index))
        if setup.setup_id in MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP:
            row["v9_1m_extension_atr"] = extension_atr(row.get("confirm_close"),
                row.get("v9_1m_ema20"), row.get("v9_1m_atr14"), side)
        pools.setdefault((day, setup.setup_id), []).append(row)
    parts = []
    fingerprint = strategy_fingerprint()
    for (day, setup_id), rows in sorted(pools.items()):
        setup = _ACTIVE_BY_ID[setup_id]
        if len({str(row["tradingsymbol"]) for row in rows}) != len(rows):
            raise ValueError("Duplicate candidate for one setup/session/symbol")
        chosen = rank_candidates(rows, setup, session_date=day)
        selected = {str(row["tradingsymbol"]): row for row in chosen}
        eligible = sorted([row for row in rows if passes_selected_filters(row, setup, session_date=day)],
                          key=lambda row: _rank_key(row, setup))
        ranks = {str(row["tradingsymbol"]): n for n, row in enumerate(eligible, 1)}
        for row in rows:
            symbol = str(row["tradingsymbol"])
            picked = selected.get(symbol)
            passed = symbol in ranks
            core = bool(picked and picked["v10_g_f_core"])
            row.update(setup_id=setup_id, native_stop_pct=setup.stop_pct, native_target_pct=setup.target_pct,
                       stop_pct=setup.stop_pct, target_pct=setup.target_pct, initial_stop_pct=INITIAL_STOP_PCT,
                       tightened_stop_pct=TIGHTENED_STOP_PCT, tighten_after_minutes=TIGHTEN_AFTER_MINUTES,
                       configured_confirmation_end=setup.confirmation_end, picker=setup.picker,
                       max_entries=setup.max_entries, v10_g_f_core=core, v10_g_morning_slot=False,
                       v9_selected=picked is not None, v9_filter_pass=passed,
                       v9_rank_in_setup_day=ranks.get(symbol),
                       v9_decision="SELECTED" if picked else "RANKED_OUT" if passed else "FILTER_REJECTED",
                       v9_configuration=STRATEGY_PROFILE, strategy_version=STRATEGY_VERSION,
                       strategy_fingerprint=fingerprint, relaxed_0925_added=False,
                       g3_confirmation_offset_minutes=1,
                       signal_id=canonical_signal_id(STRATEGY_VERSION, day, setup.signal_end,
                                                     setup.confirmation_end, setup.side, symbol))
            parts.append(row)
    if not parts:
        return pd.DataFrame(columns=list(dict.fromkeys([*signals.columns, *metadata])))
    return pd.DataFrame(parts).sort_values(["day", "hhmm_int", "side", "setup_id", "tradingsymbol"],
                                          kind="stable").reset_index(drop=True)


def select_orders(signals: pd.DataFrame) -> pd.DataFrame:
    audit = selection_audit(signals)
    return audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True)


def strategy_payload() -> dict[str, Any]:
    """Portable normalized contract; contains no runtime paths or arming state."""
    return dict(schema_version="fno_g3_production_contract_v1", strategy_profile=STRATEGY_PROFILE,
        strategy_version=STRATEGY_VERSION, accepted_variant=ACCEPTED_VARIANT,
        frozen_baseline=dict(version=FROZEN_VERSION, config_sha256=FROZEN_CONFIG_SHA256,
                             execution_authority=False),
        core_first=True, strict_0925_long=True, morning_slots=False, two_bar_continuation=False,
        opening_0920=dict(sides=["LONG", "SHORT"], short_enabled=True, price_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE",
            historical_oi_baseline="VERIFIED_DATED_FUTURES_0915_0916_MINUTE",
            live_oi_baseline="VERIFIED_MINUTE_OR_TIMELY_PROVISIONAL_QUOTE",
            baseline_received_before="09:20", quote_window_seconds=15, maximum_quote_receipt_lag_seconds=15,
            missing_or_late_evidence="EXCLUDE_STOCK_FROM_0920_AND_DISCLOSE",
            all_opening_inputs_missing="BLOCK_0920", oi_comparison_interval="ACTUAL_0916_TO_0920_NOT_FIVE_MINUTES"),
        base_gates=dict(minimum_price_change_pct=BASE_PRICE_CHANGE_PCT, minimum_oi_change_pct=BASE_OI_CHANGE_PCT,
                        maximum_oi_change_pct=MAX_OI_CHANGE_PCT, minimum_volume_ratio=BASE_VOLUME_RATIO,
                        positive_increasing_oi=True, strict_ema_order=[9, 20, 50]),
        core_setups=[asdict(s) for s in CORE_SETUPS], active_setups=[asdict(s) for s in ACTIVE_SETUPS],
        signal_to_confirmation=dict(SIGNAL_TO_CONFIRMATION), confirmation_window_minutes=1,
        minimum_confirmation_1m_volume_ratio=dict(MIN_CONFIRMATION_VOLUME_BY_SIDE),
        minimum_confirmation_1m_volume_ratio_by_setup=dict(MIN_CONFIRMATION_VOLUME_BY_SETUP),
        confirmation_volume_policy=CONFIRMATION_VOLUME_POLICY,
        confirmation_extension_policy=CONFIRMATION_EXTENSION_POLICY,
        maximum_confirmation_extension_atr_by_setup=dict(MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP),
        live_confirmation_volume_warmup=dict(required_prior_observations=LIVE_CONFIRMATION_VOLUME_REQUIRED_PRIOR,
            initial_history_days=LIVE_CONFIRMATION_HISTORY_DAYS, fallback_history_days=LIVE_CONFIRMATION_HISTORY_FALLBACK_DAYS,
            first_minute_end=LIVE_CONFIRMATION_FIRST_MINUTE_END, last_minute_end=LIVE_CONFIRMATION_LAST_MINUTE_END,
            research_min_periods=CONFIRMATION_VOLUME_MIN_PERIODS, insufficient_history_action="FAIL_CLOSED"),
        nifty_gate=dict(signals=list(NIFTY_SHORT_SIGNAL_ENDS), side="SHORT", first_bar_end=NIFTY_FIRST_BAR_END,
                        instrument="DATED_NEAR_MONTH_NIFTY_FUTURE", maximum_return_pct=NIFTY_FIRST_BAR_MAX_RETURN_PCT),
        exit=dict(setups={s.setup_id: dict(stop_pct=s.stop_pct, target_pct=s.target_pct) for s in ACTIVE_SETUPS},
                  initial_stop_pct=INITIAL_STOP_PCT, tightened_stop_pct=TIGHTENED_STOP_PCT,
                  tighten_after_minutes=TIGHTEN_AFTER_MINUTES, square_off=SQUARE_OFF,
                  historical_clock="FIRST_BAR_OPEN_AT_OR_AFTER_ENTRY_BAR_END_PLUS_DELAY",
                  runtime_clock="ACTUAL_FILL_TIMESTAMP_PLUS_DELAY", partial_exits=False, breakeven_stop=False),
        entry_expiry_minutes=ENTRY_EXPIRY_MINUTES, entry_activation_grace_sec=ENTRY_ACTIVATION_GRACE_SEC,
        capital_per_entry_rupees=CAPITAL_PER_ENTRY_RS, leverage_factor=LEVERAGE,
        portfolio_capital_rupees=PORTFOLIO_CAPITAL_RS, max_positions=MAX_POSITIONS, cost_bps=ROUND_TRIP_COST_BPS,
        live_quantity_cap=LIVE_QUANTITY_CAP, default_armed=ARMED, execution_authority=EXECUTION_AUTHORITY,
        prediction_observers_can_trade=False)


def strategy_fingerprint() -> str:
    return hashlib.sha256(json.dumps(strategy_payload(), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


config_fingerprint = strategy_fingerprint


def supported_identity(version: str, fingerprint: str, *, allow_legacy: bool = False) -> bool:
    """Archived readers may opt into prior identities; execution requires current."""
    return ((version, fingerprint) == (STRATEGY_VERSION, strategy_fingerprint()) or
            allow_legacy and (version, fingerprint) in {
                (LEGACY_STRATEGY_VERSION, LEGACY_STRATEGY_FINGERPRINT),
                ("V13-V10-G-3-PRODUCTION-V2", "8d4d62d7f3e4aff5f39905e4f1cd2297a50b74af7c8fa83ee7b69aa855cc2aa6"),
                ("V13-V10-G-3-PRODUCTION-V3", "0119d67996b0f801dcfe55057d76e82482d10bec40898dab017262b09506a811"),
                ("V13-V10-G-3-PRODUCTION-V4", "181c37b6a43291311525b0b6970ea0019d5237bce172735913418cba623fb6b9"),
                ("V13-V10-G-3-BACKTEST-SELECTED-TEN-V5", "494ca2cc653ca5c58f297040b43e33355a3c38351d9a9ac00b592b1ac6e44eb0"),
                ("V13-V10-G-3-PRODUCTION-V5", "fcb7c981b518b8c9e427155cdfb06e3cc34c6ed16525e78514e6ecf3afab2aea"),
                ("V13-V10-G-3-PRODUCTION-V6", "7d01e80f60cc874e7c7b94a894785d134b70df71b27be98fdf3c302a96f44b4a"),
                ("V13-V10-G-3-PRODUCTION-V7", "68e842bb99002c950304cc40a41aa40e93a6394b131199ade872ec5e852a281f"),
                ("V13-V10-G-3-PRODUCTION-V8", "5a2f8949d6cea0f13d18c8b7cd2cf9aecf856ddf350b97553be40ac137b6dd61"),
                ("V13-V10-G-3-PRODUCTION-V9", "d65237ffc1e5caf932258fc91b15c487951d1b6b75d32614e4fd0cbe88fa61b5"),
                (PREVIOUS_STRATEGY_VERSION, PREVIOUS_STRATEGY_FINGERPRINT)})


def load_contract(path: Path | str | None = None) -> dict[str, Any]:
    value = strategy_payload() if path is None else json.loads(Path(path).read_text(encoding="utf-8"))
    validate_contract(value)
    return copy.deepcopy(value)


def validate_contract(value: dict[str, Any] | None = None) -> None:
    if value is not None and value != strategy_payload():
        raise ValueError("G3 production configuration differs from the accepted normalized contract")
    if len(ACTIVE_SETUPS) != 61 or len(_ACTIVE_BY_ID) != 61:
        raise ValueError("G3 V11 requires exactly 61 effective setups")
    if MIN_CONFIRMATION_VOLUME_BY_SIDE != {"LONG": 1.1, "SHORT": 1.2}:
        raise ValueError("G3 side confirmation thresholds changed")
    if MIN_CONFIRMATION_VOLUME_BY_SETUP != {"0921_LONG": 1.8, "0921_SHORT": 1.2, "0941_LONG": .8, "0951_LONG": .6, "1011_LONG": .6,
            **late_setups.CONFIRMATION_VOLUME, **selected_setups.CONFIRMATION_VOLUME,
            **midday_setups.CONFIRMATION_VOLUME, **afternoon_setups.CONFIRMATION_VOLUME}:
        raise ValueError("G3 setup confirmation thresholds changed")
    if MAX_CONFIRMATION_EXTENSION_ATR_BY_SETUP != {"1036_LONG": 3.0, "1036_SHORT": 3.0}:
        raise ValueError("G3 10:35 one-minute extension guard changed")
    for row in (*late_setups.ROWS, *selected_setups.ROWS, *midday_setups.ROWS, *afternoon_setups.ROWS):
        expected = _make_setup(row)
        if _ACTIVE_BY_ID.get(expected.setup_id) != expected or _CORE_BY_ID.get(expected.setup_id) != expected:
            raise ValueError("G3 approved late setup differs from historical rules")
    if setup_for("09:20", "LONG") != _make_setup(("09:20", "LONG", 1, "max_liquidity", .30, .10, 3., .75, .35, 3.)):
        raise ValueError("G3 09:20 LONG production setup changed")
    if setup_for("09:25", "LONG") != _make_setup(("09:25", "LONG", 1, "max_liquidity", .30, .10, 1.2, .60, .60, .97)):
        raise ValueError("G3 09:26 LONG five-minute volume must be 1.2x")
    for new_id, donor_id in (("0921_SHORT", "0926_SHORT"), ("1011_LONG", "0951_LONG")):
        for family in (_CORE_BY_ID, _ACTIVE_BY_ID):
            new, donor = family[new_id], family[donor_id]
            if new != replace(donor, signal_end=new.signal_end, confirmation_end=new.confirmation_end, target_pct=1.5):
                raise ValueError("G3 new slots must retain donor entry filters and 1.5% targets")
    if NIFTY_SHORT_SIGNAL_ENDS != ("09:20", "09:25"):
        raise ValueError("G3 opening SHORT must retain the donor NIFTY gate")
    new = setup_for("09:50", "LONG")
    if new != _make_setup(("09:50", "LONG", 1, "max_liquidity", .10, .05, 1.3, .50, .50, .75)):
        raise ValueError("G3 09:50 LONG production setup changed")
    if (INITIAL_STOP_PCT, TIGHTENED_STOP_PCT, TIGHTEN_AFTER_MINUTES) != (1.25, 1., 120):
        raise ValueError("G3 scheduled stops changed")
    if any(s.stop_pct != 1.25 or not 0 < s.target_pct <= 7.15 for s in ACTIVE_SETUPS):
        raise ValueError("G3 effective exits changed")
    if (CAPITAL_PER_ENTRY_RS, LEVERAGE, PORTFOLIO_CAPITAL_RS, MAX_POSITIONS, ENTRY_EXPIRY_MINUTES,
            LIVE_QUANTITY_CAP, ARMED, EXECUTION_AUTHORITY) != (100_000., 5., 1_000_000., None, 10, 1, False, False):
        raise ValueError("G3 portfolio, timing or default authority changed")


def verify_frozen_config(path: Path | str) -> dict[str, Any]:
    """Explicit read-only evidence check; donor exits deliberately ignored."""
    raw = Path(path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != FROZEN_CONFIG_SHA256:
        raise ValueError("Accepted frozen G3 configuration hash mismatch")
    frozen = json.loads(raw)
    if (frozen["version"] != FROZEN_VERSION or frozen["accepted_variant"] != FROZEN_ACCEPTED_VARIANT
            or frozen["confirmation_window_minutes"] != 1 or frozen["execution_authority"] is not False
            or frozen["minimum_confirmation_1m_volume_ratio"] != MIN_CONFIRMATION_VOLUME_BY_SIDE):
        raise ValueError("Accepted frozen G3 baseline metadata mismatch")
    for family, setups in (("core", CORE_SETUPS), ("expanded", ACTIVE_SETUPS)):
        # The archived oracle covers the original fourteen setups only.
        setups = tuple(s for s in setups if s.setup_id not in
            {"0921_LONG", "0951_LONG", *ADDED_SETUP_IDS, *late_setups.SETUP_IDS, *selected_setups.SETUP_IDS, *midday_setups.SETUP_IDS, *afternoon_setups.SETUP_IDS})
        actual = {f"{row[family]['confirmation_end'].replace(':', '')}_{row[family]['side']}": row[family]
                  for row in frozen["setup_rules"]}
        if set(actual) != {s.setup_id for s in setups}:
            raise ValueError("Frozen G3 setup identity mismatch")
        for setup in setups:
            expected = asdict(setup)
            for key in ("stop_pct", "target_pct", "source_version"):
                expected.pop(key)
            if setup.setup_id == "0926_LONG":
                expected["volume_ratio"] = 3.0  # Frozen oracle predates the approved 1.2x change.
            if any(actual[setup.setup_id][key] != value for key, value in expected.items()):
                raise ValueError(f"Frozen G3 selection mismatch: {setup.setup_id}")
            if frozen["exit"]["setups"][setup.setup_id] != dict(stop_pct=setup.stop_pct, target_pct=setup.target_pct):
                raise ValueError(f"Frozen G3 effective exit mismatch: {setup.setup_id}")
    return frozen
