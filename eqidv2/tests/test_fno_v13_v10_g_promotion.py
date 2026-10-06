"""Production activation, raw-gate boundaries and retained selection priority."""
from datetime import date

import pytest

import fno_v13_v10_g_live_config as config
import fno_v13_v10_g_policy as policy

DAY = date(2026, 10, 6)


def candidate(symbol="RELAXED", day=DAY, **updates):
    row = dict(tradingsymbol=symbol, side="LONG", signal_end="09:25",
               signal_timestamp=f"{day}T09:25:00+05:30",
               confirmation_timestamp=f"{day}T09:26:00+05:30",
               v9_1m_feature_ts=f"{day}T09:26:00+05:30",
               signal_close=100., price_change_pct=.30, oi_change_pct=1.20,
               oi=101200., prev_oi=100000., ema9=99., ema20=100., ema50=101.,
               volume_ratio=1.75, body_ratio=.54, wick_ratio=.20,
               traded_value=1e6, v9_1m_volume_ratio=1.20, confirmed=True)
    row.update(updates)
    return row


def selected(rows, day=DAY):
    setup = config.setup_for("09:25", "LONG", session_date=day)
    return config.rank_candidates(rows, setup, session_date=day)


def test_exact_effective_session_and_legacy_artifact_contract():
    old = date(2026, 10, 5)
    assert not selected([candidate(day=old)], old)
    rows = selected([candidate()])
    assert len(rows) == 1 and rows[0]["relaxed_0925_added"] is True
    assert policy.policy_for_day(old)["staged_stop"] is False
    assert policy.policy_for_day(DAY)["staged_stop"] is True
    for original in config.ACTIVE_SETUPS:
        assert config.setup_for(original.signal_end, original.side, session_date=old) == original
        current = config.setup_for(original.signal_end, original.side, session_date=DAY)
        assert current.stop_pct == 1.25
        assert current.target_pct == original.target_pct
        assert current.max_entries == original.max_entries
    assert config.load_frozen_config()["exit"]["setups"]["0926_LONG"]["stop_pct"] != 1.25


@pytest.mark.parametrize("field,value", [
    ("oi_change_pct", 1.200001), ("oi_change_pct", .09999),
    ("volume_ratio", 1.74999), ("body_ratio", .53999),
    ("price_change_pct", .29999), ("wick_ratio", .60001),
    ("v9_1m_volume_ratio", 1.19999), ("prev_oi", 101201.),
    ("confirmation_timestamp", "2026-10-06T09:27:00+05:30"),
    ("v9_1m_feature_ts", "2026-10-06T09:27:00+05:30"),
    ("signal_timestamp", "2026-10-05T09:25:00+05:30"),
    ("confirmed", False),
])
def test_relaxation_retains_other_boundaries(field, value):
    assert selected([candidate(**{field: value})]) == []


def test_original_choice_precedes_more_liquid_new_candidate():
    original = candidate("ORIGINAL", oi_change_pct=.5, volume_ratio=3., body_ratio=.60,
                         ema9=103., ema20=102., ema50=101., traded_value=10.)
    result = selected([candidate(), original])
    assert [row["tradingsymbol"] for row in result] == ["ORIGINAL"]
    assert result[0]["relaxed_0925_added"] is False


def test_relaxed_liquidity_ranking_tie_break_and_one_order_quota():
    result = selected([candidate("Z"), candidate("A"), candidate("LOW", traded_value=1.)])
    assert [row["tradingsymbol"] for row in result] == ["A"]


def test_ema_bypass_scoped_to_0925_long_only():
    row = candidate(ema9=float("nan"), ema20=float("nan"), ema50=float("nan"))
    assert selected([row])
    assert config.base_signal_side(row, "09:30", session_date=DAY) is None
    short = candidate(side="SHORT", price_change_pct=-.30, volume_ratio=2.,
                      nifty_first_bar_return_pct=-.10)
    assert config.base_signal_side(short, "09:25", session_date=DAY) is None
    short["oi_change_pct"] = .5
    assert config.base_signal_side(short, "09:25", session_date=DAY) == "SHORT"


def test_promotion_is_in_fingerprint_and_attestation_is_only_baseline():
    current = config.strategy_payload()["scheduled_promotion"]
    assert current["effective_from"] == "2026-10-06"
    assert current["relaxed_0925_thresholds"] == policy.RELAXED_0925_LONG
    proof = config.attest_selected_backtest()
    assert proof["attestation_scope"] == "FROZEN_RETAINED_G_BASELINE_ONLY"
    assert proof["promoted_rules_independently_validated"] is False
