"""Targeted tests for the explicitly scoped hindsight research exception."""
import numpy as np
import pandas as pd
import pytest

import fno_v13_v10_g_2_relaxed_0925_backtest as study


def row(**changes):
    value = dict(tradingsymbol="KALYANKJIL", signal_end="09:25",
                 signal_ts="2026-10-05T09:25:00+05:30", confirmation_ts="2026-10-05T09:26:00+05:30",
                 oi=25002000., prev_oi=24721200., oi_change_pct=1.1358671909,
                 price_change_pct=.6793260574, volume_ratio=1.793475012,
                 signal_close=540.9500122, confirmation_open=541., confirmation_high=544.3499756,
                 confirmation_low=539.2999878, confirmation_close=543.75,
                 body_ratio=.5445557718, v9_1m_upper_wick_ratio=.1188073339,
                 v9_1m_volume_ratio=1.749504176, traded_value=129023069.31,
                 ema9=531.369842, ema20=529.179161, ema50=532.248349)
    value.update(changes)
    return value


def test_known_record_passes_relaxed_but_not_original_core():
    a = study.eligibility(pd.DataFrame([row()]))
    assert bool(a.iloc[0].relaxed_pass)
    assert not bool(a.iloc[0].original_core_pass)
    assert a.iloc[0].relaxed_failed_rules == ""


@pytest.mark.parametrize("field,value", [
    ("oi_change_pct", 1.201), ("oi_change_pct", .099), ("volume_ratio", 1.749),
    ("body_ratio", .539), ("v9_1m_volume_ratio", 1.199), ("v9_1m_upper_wick_ratio", .601),
    ("price_change_pct", .299), ("confirmation_close", 540.), ("prev_oi", 25003000.),
    ("confirmation_high", 539.), ("traded_value", np.nan),
    ("confirmation_ts", "2026-10-05T09:27:00+05:30"),
])
def test_other_safety_gates_remain_active(field, value):
    a = study.eligibility(pd.DataFrame([row(**{field: value})]))
    assert not bool(a.iloc[0].relaxed_pass)


def test_threshold_boundaries_are_inclusive():
    a = study.eligibility(pd.DataFrame([row(oi_change_pct=1.2, volume_ratio=1.75, body_ratio=.54)]))
    assert bool(a.iloc[0].relaxed_pass)


def test_only_0925_rows_are_reconsidered():
    a = study.eligibility(pd.DataFrame([row(), row(signal_end="09:30")]))
    assert len(a) == 1


def test_missing_ema_does_not_block_explicit_ema_disabled_rule():
    a = study.eligibility(pd.DataFrame([row(ema9=np.nan, ema20=np.nan, ema50=np.nan)]))
    assert bool(a.iloc[0].relaxed_pass)
    assert not bool(a.iloc[0].original_core_pass)


def test_exact_candle_end_required():
    a = study.eligibility(pd.DataFrame([row(signal_ts="2026-10-05T09:25:30+05:30",
                                          confirmation_ts="2026-10-05T09:26:30+05:30")]))
    assert not bool(a.iloc[0].relaxed_pass)


def test_core_first_and_one_order_quota_remain():
    a = study.eligibility(pd.DataFrame([row(), row(tradingsymbol="CORE", traded_value=100.,
        oi_change_pct=.2, volume_ratio=3., body_ratio=.6, ema9=530., ema20=529., ema50=528.)]))
    orders, audit = study.rank_orders(a, 100)
    assert orders.tradingsymbol.tolist() == ["CORE"]
    assert orders.sid.tolist() == [100]
    assert orders.side.tolist() == ["LONG"]
    assert orders.setup_id.tolist() == ["0926_LONG"]
    assert len(audit.loc[audit.relaxed_selected]) == 1


def test_without_core_liquidity_ranks_without_stock_name_preference():
    a = study.eligibility(pd.DataFrame([row(), row(tradingsymbol="OTHER", traded_value=200000000.)]))
    orders, _ = study.rank_orders(a, 200)
    assert orders.tradingsymbol.tolist() == ["OTHER"]


def test_deterministic_symbol_tie_and_no_execution_authority():
    a = study.eligibility(pd.DataFrame([row(tradingsymbol="ZZZ"), row(tradingsymbol="AAA")]))
    orders, _ = study.rank_orders(a, 300)
    assert orders.tradingsymbol.tolist() == ["AAA"]
    assert study.PARAMETERS["execution_authority"] is False
    assert study.config.setup_for("09:25", "LONG").volume_ratio == 3.
    assert study.config.setup_for("09:25", "LONG").body_ratio == .6
    assert study.config.MAX_OI_CHANGE_PCT == 1.
