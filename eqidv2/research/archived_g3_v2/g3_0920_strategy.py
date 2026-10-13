"""Backtest-only 09:20 G-3 LONG/SHORT selection from verified opening features.

The first-bar price comparison is 09:15 cash open to 09:20 cash close. The OI
comparison is the same dated futures contract's 09:16 minute to 09:20 completed
bar (four minutes). Historical feature availability is retrospective; nothing
in this module arms, schedules, or changes the production G-3 strategy.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta
import math

import pandas as pd

import fno_v13_v10_g_3_contract as contract
from fno_v13_v10_g_identity import canonical_signal_id


RESEARCH_VERSION = "V13-V10-G-3-BT-0920-LONG-SHORT-V1"
OI_SOURCE = "FIRST_MINUTE_0916_TO_COMPLETED_0920_FUTURES_4M"
OI_AVAILABILITY = "RETROSPECTIVE_RESEARCH"
REQUIRED_COLUMNS = frozenset({
    "session_date", "symbol", "contract", "research_feature_status",
    "cash_0920_status", "confirmation_0921_status", "futures_0920_status",
    "opening_0916_status", "price_first_5m_pct", "oi_change_pct",
    "oi_change_source", "oi_change_interval_minutes", "oi_change_availability",
    "futures_0920_oi", "opening_0916_oi", "cash_close", "ema9", "ema20",
    "ema50", "volume_ratio", "traded_value", "confirmation_open",
    "confirmation_high", "confirmation_low", "confirmation_close",
    "confirmation_body_ratio", "confirmation_upper_wick_ratio",
    "confirmation_lower_wick_ratio", "v9_1m_volume_ratio",
    "nifty_context_status", "nifty_first_bar_return_pct",
})


@dataclass(frozen=True)
class FiveMinuteSelection:
    """09:15–09:20 signal-bar thresholds, in percentage points where applicable."""

    active_price_pct: float
    core_price_pct: float
    oi_change_pct: float
    volume_ratio: float


@dataclass(frozen=True)
class OneMinuteSelection:
    """Completed 09:21 confirmation-bar thresholds."""

    body_ratio: float
    max_wick_ratio: float
    confirmation_volume_ratio: float


@dataclass(frozen=True)
class SideSelectionProfile:
    five_minute: FiveMinuteSelection
    one_minute: OneMinuteSelection


@dataclass(frozen=True)
class SelectionProfile:
    long: SideSelectionProfile
    short: SideSelectionProfile

    def for_side(self, side: str) -> SideSelectionProfile:
        if side == "LONG":
            return self.long
        if side == "SHORT":
            return self.short
        raise ValueError("Selection side must be LONG or SHORT")


def reference_profile() -> SelectionProfile:
    """The current 09:25-derived 09:20 rules, without optimization overrides."""
    specs = setup_specs()
    sides = {}
    for side, (core, active) in specs.items():
        sides[side] = SideSelectionProfile(
            five_minute=FiveMinuteSelection(
                active_price_pct=active.price_change_pct,
                core_price_pct=core.price_change_pct,
                oi_change_pct=active.oi_change_pct,
                volume_ratio=active.volume_ratio,
            ),
            one_minute=OneMinuteSelection(
                body_ratio=active.body_ratio,
                max_wick_ratio=active.max_wick_ratio,
                confirmation_volume_ratio=contract.confirmation_volume_minimum(
                    side, setup_id=active.setup_id),
            ),
        )
    return SelectionProfile(long=sides["LONG"], short=sides["SHORT"])


def _validate_profile(profile: SelectionProfile) -> None:
    if not isinstance(profile, SelectionProfile):
        raise TypeError("profile must be a SelectionProfile")
    for side in ("LONG", "SHORT"):
        rule = profile.for_side(side)
        if not isinstance(rule, SideSelectionProfile) or not isinstance(rule.five_minute, FiveMinuteSelection) \
                or not isinstance(rule.one_minute, OneMinuteSelection):
            raise TypeError(f"{side} profile must contain five-minute and one-minute thresholds")
        first, confirm = rule.five_minute, rule.one_minute
        values = (first.active_price_pct, first.core_price_pct, first.oi_change_pct,
                  first.volume_ratio, confirm.body_ratio, confirm.max_wick_ratio,
                  confirm.confirmation_volume_ratio)
        if any(_finite(value) is None for value in values):
            raise ValueError(f"{side} thresholds must be finite")
        if not (contract.BASE_PRICE_CHANGE_PCT <= first.active_price_pct
                <= first.core_price_pct):
            raise ValueError(f"{side} core price threshold must be at least the active threshold and contractual base")
        if not (contract.BASE_OI_CHANGE_PCT <= first.oi_change_pct <= contract.MAX_OI_CHANGE_PCT):
            raise ValueError(f"{side} OI threshold must stay within contractual base and maximum")
        if first.volume_ratio < contract.BASE_VOLUME_RATIO:
            raise ValueError(f"{side} signal volume threshold must respect contractual base")
        if not 0 <= confirm.body_ratio <= 1 or not 0 <= confirm.max_wick_ratio <= 1:
            raise ValueError(f"{side} confirmation body and wick ratios must be between zero and one")
        native_floor = contract.confirmation_volume_minimum(side, setup_id=f"0921_{side}")
        if confirm.confirmation_volume_ratio < native_floor:
            raise ValueError(f"{side} confirmation volume must respect the native side floor")


def setup_specs(profile: SelectionProfile | None = None) -> dict[str, tuple[contract.SetupSpec, contract.SetupSpec]]:
    """Copy the 09:25 active/core rules with an optional 09:20 research profile."""
    if profile is not None:
        _validate_profile(profile)
    result = {}
    for side in ("LONG", "SHORT"):
        core = next(s for s in contract.CORE_SETUPS if s.signal_end == "09:25" and s.side == side)
        active = contract.setup_for("09:25", side)
        assert active is not None
        core = replace(core, signal_end="09:20", confirmation_end="09:21",
                       source_version=RESEARCH_VERSION)
        active = replace(active, signal_end="09:20", confirmation_end="09:21",
                         source_version=RESEARCH_VERSION)
        if profile is not None:
            first, confirm = profile.for_side(side).five_minute, profile.for_side(side).one_minute
            core = replace(core, price_change_pct=first.core_price_pct,
                           oi_change_pct=first.oi_change_pct, volume_ratio=first.volume_ratio,
                           body_ratio=confirm.body_ratio, max_wick_ratio=confirm.max_wick_ratio)
            active = replace(active, price_change_pct=first.active_price_pct,
                             oi_change_pct=first.oi_change_pct, volume_ratio=first.volume_ratio,
                             body_ratio=confirm.body_ratio, max_wick_ratio=confirm.max_wick_ratio)
        result[side] = core, active
    return result


def _finite(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _source_reasons(raw: dict, side: str) -> list[str]:
    reasons = []
    for key, expected in (
        ("research_feature_status", "COMPLETE"),
        ("cash_0920_status", "VALID_COMPLETED_FIRST_BAR"),
        ("confirmation_0921_status", "VALID_REAL_MINUTE"),
        ("futures_0920_status", "VALID_COMPLETED_FUTURES_0920"),
        ("opening_0916_status", "VALID_FIRST_MINUTE_EVIDENCE"),
        ("oi_change_source", OI_SOURCE),
        ("oi_change_availability", OI_AVAILABILITY),
        ("nifty_context_status", "VALID_COMPLETED_NIFTY_FIRST_BAR"),
    ):
        if raw.get(key) != expected:
            reasons.append(f"INVALID_{key.upper()}")
    if _finite(raw.get("oi_change_interval_minutes")) != 4:
        reasons.append("OI_INTERVAL_NOT_0916_TO_0920_4M")
    opening, completed, change = (_finite(raw.get(key)) for key in
                                  ("opening_0916_oi", "futures_0920_oi", "oi_change_pct"))
    if (opening is None or opening <= 0 or completed is None or completed <= 0
            or change is None or not math.isclose(change, (completed / opening - 1) * 100,
                                                   rel_tol=1e-9, abs_tol=1e-9)):
        reasons.append("INVALID_SAME_CONTRACT_OI_ARITHMETIC")
    if side == "SHORT":
        nifty = _finite(raw.get("nifty_first_bar_return_pct"))
        if nifty is None or nifty > contract.NIFTY_FIRST_BAR_MAX_RETURN_PCT:
            reasons.append("NIFTY_FIRST_BAR_SHORT_GATE")
    return reasons


def _candidate(raw: dict, day: date, side: str, sid: int) -> dict:
    signal = pd.Timestamp(datetime.combine(day, time(9, 20), contract.IST))
    confirmation = signal + timedelta(minutes=1)
    reasons = _source_reasons(raw, side)
    high = _finite(raw.get("confirmation_high"))
    low = _finite(raw.get("confirmation_low"))
    trigger = high if side == "LONG" else low
    row = dict(raw)
    row.update(
        sid=sid, signal_id=canonical_signal_id(RESEARCH_VERSION, day, "09:20", "09:21",
                                                side, str(raw["symbol"])),
        strategy=RESEARCH_VERSION, strategy_version=RESEARCH_VERSION,
        scope="BACKTEST_ONLY", execution_authority=False,
        backtest_evidence=OI_AVAILABILITY, day=day,
        tradingsymbol=str(raw["symbol"]), futures_tradingsymbol=str(raw["contract"]),
        side=side, setup_id=f"0921_{side}", signal_end="09:20",
        signal_ts=signal, confirmation_ts=confirmation,
        signal_timestamp=signal, confirmation_timestamp=confirmation,
        v9_5m_feature_ts=signal, v9_1m_feature_ts=confirmation,
        hhmm="0920", hhmm_int=920,
        signal_close=raw.get("cash_close"),
        price_change_pct=raw.get("price_first_5m_pct"),
        price_change_basis="09:15_CASH_OPEN_TO_09:20_CASH_CLOSE",
        oi=raw.get("futures_0920_oi"), prev_oi=raw.get("opening_0916_oi"),
        oi_change_pct=raw.get("oi_change_pct"),
        oi_change_basis=OI_SOURCE, oi_change_interval_minutes=4,
        confirm_open=raw.get("confirmation_open"),
        confirm_high=raw.get("confirmation_high"),
        confirm_low=raw.get("confirmation_low"),
        confirm_close=raw.get("confirmation_close"),
        body_ratio=raw.get("confirmation_body_ratio"),
        wick_ratio=raw.get("confirmation_upper_wick_ratio" if side == "LONG"
                           else "confirmation_lower_wick_ratio"),
        v9_exact_confirmation_present=raw.get("confirmation_0921_status") == "VALID_REAL_MINUTE",
        source_1m_count=5 if raw.get("cash_0920_status") == "VALID_COMPLETED_FIRST_BAR" else 0,
        trigger=trigger,
        source_reasons=";".join(reasons),
    )
    return row


def _rank_key(row: dict, setup: contract.SetupSpec) -> tuple:
    return (-contract.picker_value(row, setup.picker), -float(row["traded_value"]),
            str(row["tradingsymbol"]))


def selection_audit_0920(features: pd.DataFrame, *, sid_start: int = 10_000_000,
                         profile: SelectionProfile | None = None) -> pd.DataFrame:
    """Audit every dated stock and side; select core-first within each day/side.

    ``sid_start`` must be chosen not to overlap the caller's baseline order IDs.
    This never mutates the production G-3 contract or publishes a pointer.
    """
    if not isinstance(features, pd.DataFrame):
        raise TypeError("features must be a DataFrame")
    if not isinstance(sid_start, int) or sid_start < 0:
        raise ValueError("sid_start must be a nonnegative integer")
    missing = sorted(REQUIRED_COLUMNS - set(features.columns))
    if missing:
        raise ValueError(f"09:20 research features lack required fields: {missing}")
    if features.empty:
        return pd.DataFrame(columns=["sid", "signal_id", "day", "tradingsymbol", "side",
                                     "setup_id", "v9_filter_pass", "v9_selected", "v9_decision"])
    if features[["session_date", "symbol", "contract"]].isna().any().any():
        raise ValueError("Dated 09:20 universe has missing stock or contract identity")
    work = features.copy()
    work["session_date"] = work.session_date.astype(str)
    if work.duplicated(["session_date", "symbol"]).any() or work.duplicated(["session_date", "contract"]).any():
        raise ValueError("Dated 09:20 universe contains duplicate stock or contract")
    specs = setup_specs(profile)
    rows = []
    for raw in work.sort_values(["session_date", "symbol"], kind="stable").to_dict("records"):
        day = date.fromisoformat(raw["session_date"])
        for side in ("LONG", "SHORT"):
            rows.append(_candidate(raw, day, side, sid_start + len(rows)))
    for day in sorted({row["day"] for row in rows}):
        for side, (core, active) in specs.items():
            pool = [row for row in rows if row["day"] == day and row["side"] == side]
            eligible = sorted((row for row in pool if not row["source_reasons"] and
                               contract.passes_selected_filters(row, active, session_date=day) and
                               (profile is None or _finite(row.get("v9_1m_volume_ratio")) >=
                                profile.for_side(side).one_minute.confirmation_volume_ratio)),
                              key=lambda row: _rank_key(row, active))
            core_eligible = sorted((row for row in eligible if
                                    contract.passes_selected_filters(row, core, session_date=day)),
                                   key=lambda row: _rank_key(row, core))
            core_chosen = core_eligible[:core.max_entries]
            core_ids = {row["signal_id"] for row in core_chosen}
            chosen = core_chosen + [row for row in eligible if row["signal_id"] not in core_ids][
                :active.max_entries - len(core_chosen)]
            selected_ids = {row["signal_id"] for row in chosen}
            ranks = {row["signal_id"]: index for index, row in enumerate(eligible, 1)}
            for row in pool:
                picked = row["signal_id"] in selected_ids
                passed = row["signal_id"] in ranks
                row.update(
                    picker=active.picker, max_entries=active.max_entries,
                    native_stop_pct=active.stop_pct, native_target_pct=active.target_pct,
                    stop_pct=active.stop_pct, target_pct=active.target_pct,
                    initial_stop_pct=contract.INITIAL_STOP_PCT,
                    tightened_stop_pct=contract.TIGHTENED_STOP_PCT,
                    tighten_after_minutes=contract.TIGHTEN_AFTER_MINUTES,
                    configured_confirmation_end=active.confirmation_end,
                    v10_g_f_core=row["signal_id"] in core_ids,
                    v9_filter_pass=passed, v9_selected=picked,
                    v9_rank_in_setup_day=ranks.get(row["signal_id"]),
                    v9_decision="SELECTED" if picked else "RANKED_OUT" if passed
                                else "SOURCE_REJECTED" if row["source_reasons"] else "FILTER_REJECTED",
                )
    result = pd.DataFrame(rows)
    if result.sid.duplicated().any() or result.signal_id.duplicated().any():
        raise ValueError("09:20 audit generated duplicate order identities")
    return result.sort_values(["day", "hhmm_int", "side", "tradingsymbol"],
                              kind="stable").reset_index(drop=True)


def select_orders_0920(features: pd.DataFrame, *, sid_start: int = 10_000_000,
                       profile: SelectionProfile | None = None) -> pd.DataFrame:
    audit = selection_audit_0920(features, sid_start=sid_start, profile=profile)
    return audit.loc[audit.v9_selected.eq(True)].reset_index(drop=True)
