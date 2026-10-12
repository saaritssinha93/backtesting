"""Selection parity, outcome screening, and 1% target isolation for joint research."""
from copy import deepcopy

import pandas as pd
import pytest

from research.g3_0920_joint_70_search import (
    PreparedSelection, choose, five_tiers, one_tiers, qualifies, search_grid,
    selection_key, with_research_targets,
)
from research.g3_0920_strategy import OI_SOURCE, reference_profile


def feature(symbol, *, side="LONG", price=None, volume_ratio=4., oi_change=.3,
            nifty=-.1, day="2026-10-12", **changes):
    long = side == "LONG"
    row = dict(session_date=day, symbol=symbol, contract=f"{symbol}26OCTFUT",
               research_feature_status="COMPLETE", cash_0920_status="VALID_COMPLETED_FIRST_BAR",
               confirmation_0921_status="VALID_REAL_MINUTE",
               futures_0920_status="VALID_COMPLETED_FUTURES_0920",
               opening_0916_status="VALID_FIRST_MINUTE_EVIDENCE",
               price_first_5m_pct=(.5 if long else -.5) if price is None else price,
               oi_change_pct=oi_change, oi_change_source=OI_SOURCE, oi_change_interval_minutes=4,
               oi_change_availability="RETROSPECTIVE_RESEARCH", opening_0916_oi=100_000.,
               futures_0920_oi=100_000. * (1 + oi_change / 100), cash_close=100.5 if long else 99.5,
               ema9=101. if long else 99., ema20=100., ema50=99. if long else 101.,
               volume_ratio=volume_ratio, traded_value=1_000_000.,
               confirmation_open=100.5 if long else 99.5,
               confirmation_high=101. if long else 99.6,
               confirmation_low=100.4 if long else 99., confirmation_close=100.8 if long else 99.1,
               confirmation_body_ratio=.6, confirmation_upper_wick_ratio=.2,
               confirmation_lower_wick_ratio=.2, v9_1m_volume_ratio=1.2,
               nifty_context_status="VALID_COMPLETED_NIFTY_FIRST_BAR",
               nifty_first_bar_return_pct=nifty)
    row.update(changes)
    return row


def sample_features():
    return pd.DataFrame([
        feature("LONG_LIQUID", traded_value=3_000_000., confirmation_body_ratio=.60),
        feature("LONG_STRONG", price=.8, volume_ratio=6., oi_change=.4,
                confirmation_body_ratio=.8, confirmation_upper_wick_ratio=.1,
                v9_1m_volume_ratio=2.2),
        feature("CORE_A", side="SHORT", price=-.7, volume_ratio=3.5, oi_change=.3,
                confirmation_body_ratio=.8, confirmation_lower_wick_ratio=.1,
                v9_1m_volume_ratio=2.2),
        feature("CORE_B", side="SHORT", price=-.4, volume_ratio=2.),
        feature("ACTIVE", side="SHORT", price=-.15, volume_ratio=10.),
        feature("SOURCE_BAD", confirmation_0921_status="MISSING", traded_value=9_000_000.),
        feature("NIFTY_BAD", side="SHORT", nifty=.5, volume_ratio=10.),
        feature("NEXT_DAY", day="2026-10-09", confirmation_body_ratio=.76, v9_1m_volume_ratio=1.8),
    ])


def test_joint_grid_is_complete_and_starts_with_5m_relaxation():
    grid = search_grid()
    assert len(grid) == len(five_tiers()) * len(one_tiers()) == 702
    assert len({profile for _, _, profile in grid}) == len(grid)
    assert grid[0][:2] == ("strict", "strict")
    assert all(one == "strict" for _, one, _ in grid[:len(five_tiers())])
    assert any(profile == reference_profile() for _, _, profile in grid)


@pytest.mark.parametrize("five,one", [
    ("strict", "strict"), ("balanced", "strict"), ("reference_5m", "reference_1m"),
    ("oi_base", "coarse_relaxed"), ("coarse_relaxed", "coarse_balanced"),
    ("probe_price_020", "volume_180"), ("price_base", "probe_body_065_050"),
])
def test_prepared_selection_matches_full_audit_with_ranking_and_source_rejections(five, one):
    features = sample_features()
    selector = PreparedSelection(features)
    profile = next(p for f, o, p in search_grid() if (f, o) == (five, one))
    selector.verify(features, profile)
    orders, _ = selector.select(profile)
    assert not set(orders.tradingsymbol) & {"SOURCE_BAD", "NIFTY_BAD"}


def test_preparation_retains_ranked_out_candidates_for_stricter_selection():
    features = sample_features()
    selector = PreparedSelection(features)
    broad = selector.audit.loc[selector.audit.v9_selected & selector.audit.side.eq("LONG")]
    assert "LONG_LIQUID" in set(broad.tradingsymbol)
    strict = search_grid()[0][2]
    orders, _ = selector.select(strict)
    assert "LONG_STRONG" in set(orders.tradingsymbol)
    assert "LONG_LIQUID" not in set(orders.tradingsymbol)


def test_one_percent_target_applies_to_both_sides_without_mutating_donor():
    selector = PreparedSelection(sample_features())
    orders, _ = selector.select(reference_profile())
    before = orders.copy(deep=True)
    changed = with_research_targets(orders)
    assert set(changed.side) == {"LONG", "SHORT"}
    assert changed.native_target_pct.eq(1.).all() and changed.target_pct.eq(1.).all()
    assert changed.native_stop_pct.eq(1.25).all()
    pd.testing.assert_frame_equal(before, orders)
    assert set(orders.native_target_pct) == {.97, 2.67}


def _period():
    return dict(added_0920=dict(executed_trades=10, win_rate_pct=70.),
                combined=dict(win_rate_pct=71.), incremental_pnl_rs=100.,
                added_long=dict(executed_trades=5, win_rate_pct=80.),
                added_short=dict(executed_trades=5, win_rate_pct=60.))


def test_seventy_screen_does_not_round_up_or_hide_weak_short_side():
    period = _period()
    assert qualifies(period)
    assert not qualifies(period, sides=True)
    period["added_short"]["win_rate_pct"] = 80.
    assert qualifies(period, sides=True)
    period["added_0920"]["win_rate_pct"] = 69.99999
    assert not qualifies(period)
    period["added_0920"].update(win_rate_pct=100., executed_trades=9)
    assert not qualifies(period)


def test_discovery_ranking_never_uses_recent_outcomes():
    a = dict(step=1, periods=dict(discovery=_period(), holdout=_period()))
    b = deepcopy(a)
    b["step"] = 2
    b["periods"]["discovery"]["added_0920"]["executed_trades"] = 11
    b["periods"]["holdout"]["added_0920"]["win_rate_pct"] = 0.
    assert choose([a, b], "discovery") is b
    b["periods"]["discovery"]["added_0920"]["win_rate_pct"] = 69.
    assert choose([a, b], "discovery") is a


def test_portfolio_cache_key_keeps_core_and_rank_changes_distinct():
    orders, _ = PreparedSelection(sample_features()).select(reference_profile())
    changed = orders.copy()
    changed.loc[changed.index[0], "v10_g_f_core"] = not bool(changed.iloc[0].v10_g_f_core)
    assert selection_key(orders) != selection_key(changed)
    changed = orders.copy()
    changed.loc[changed.index[0], "v9_rank_in_setup_day"] += 1
    assert selection_key(orders) != selection_key(changed)
