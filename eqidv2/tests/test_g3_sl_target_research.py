"""Guard the bounded study design and its ranking/cost accounting contracts."""
from types import SimpleNamespace

import pandas as pd
import pytest

from research import g3_sl_target_research as research


def score(candidate_id, **changes):
    row = dict(candidate_id=candidate_id, native_fills=100, profitable_days=20,
               win_rate_pct=70., net_pnl_rs=10000., stress_net_pnl_rs=5000.,
               daily_close_drawdown_rs=1000.)
    return {**row, **changes}


def test_registered_exits_obey_user_bounds_even_for_outside_range_current_targets():
    registry = research.candidates()
    assert len({row['candidate_id'] for row in registry}) == len(registry)
    assert not any(row['family'] == 'structure' for row in registry)
    # The control must handle both the restored 0.56% target and large historical targets.
    for current_target in (.56, .6, .97, 1.5, 2., 7.15):
        source = SimpleNamespace(native_target_pct=current_target)
        for candidate in registry:
            stop, target, skip = research.parameters(source, candidate)
            assert not skip
            assert 0 < stop <= 1.2
            assert .6 <= target <= 2.
            if 'tightened_stop_pct' in candidate:
                assert 0 < candidate['tightened_stop_pct'] <= stop
            if 'protect_after_close_pct' in candidate:
                assert 0 <= candidate['protected_profit_pct'] < candidate['protect_after_close_pct'] < target


@pytest.mark.parametrize('current_target,expected', [(.56, .6), (.97, .97), (7.15, 2.)])
def test_bounded_control_clips_targets_without_replacing_valid_current_target(current_target, expected):
    candidate = next(row for row in research.candidates() if row['family'] == 'bounded_current')
    assert research.parameters(SimpleNamespace(native_target_pct=current_target), candidate) == (1.2, expected, False)


@pytest.mark.parametrize('clock,expected', [
    ('0921', 'opening'), ('1001', 'opening'),
    ('1006', 'midmorning'), ('1126', 'midmorning'),
    ('1136', 'midday'), ('1331', 'midday'),
])
@pytest.mark.parametrize('side', ['LONG', 'SHORT'])
def test_clock_groups_use_signal_time_not_one_minute_confirmation(clock, expected, side):
    assert research.group_for(f'{clock}_{side}') == f'{expected}_{side}'


@pytest.mark.parametrize('preferred,other', [
    # One extra profitable day outranks higher win rate/profit: the declared objective is lexicographic.
    (score('chosen', profitable_days=21), score('other', win_rate_pct=99., net_pnl_rs=100000.)),
    (score('chosen', win_rate_pct=71.), score('other', net_pnl_rs=100000.)),
    (score('chosen', net_pnl_rs=10001.), score('other', daily_close_drawdown_rs=0.)),
    (score('chosen', daily_close_drawdown_rs=999.), score('other')),
    (score('a'), score('b')),
])
def test_selection_precedence_and_ties_are_independent_of_candidate_iteration(preferred, other):
    for options in ([preferred, other], [other, preferred]):
        assert research.choose(options, 100)[0] == preferred['candidate_id']


@pytest.mark.parametrize('invalid', [
    score('invalid', native_fills=89, profitable_days=35),
    score('invalid', net_pnl_rs=0., profitable_days=35),
    score('invalid', net_pnl_rs=-1., profitable_days=35),
    score('invalid', stress_net_pnl_rs=0., profitable_days=35),
    score('invalid', stress_net_pnl_rs=-1., profitable_days=35),
])
def test_better_headline_score_cannot_bypass_retention_or_cost_stress_gates(invalid):
    assert research.choose([invalid, score('valid', native_fills=90)], 100)[0] == 'valid'


def test_retention_gate_includes_exact_ninety_percent_and_fallback_is_explicit():
    assert research.choose([score('edge', native_fills=9)], 10)[0] == 'edge'
    for options, reference in [([], 0), ([score('none', native_fills=8)], 10),
                               ([score('none', native_fills=0, net_pnl_rs=0., stress_net_pnl_rs=0.)], 0)]:
        selected, explanation = research.choose(options, reference)
        assert selected == 'bounded_current'
        assert 'No candidate passed' in explanation


def test_empty_live_projection_has_stable_schema_and_no_false_wins():
    ledger = pd.DataFrame({'portfolio_executed': pd.Series(dtype=bool)})
    projection, summaries = research.live_projection(ledger, ['2026-10-14', '2026-10-15'])
    assert projection.empty
    assert {'quantity', 'fees_rs', 'net_after_fees_rs', 'net_after_fees_and_5bps_slippage_rs'} <= set(projection.columns)
    assert len(summaries) == 2
    for result in summaries:
        assert result['trades'] == result['wins'] == result['losses'] == 0
        assert result['profitable_days'] == result['losing_days'] == 0
        assert result['net_pnl_rs'] == result['daily_close_drawdown_rs'] == 0.


@pytest.mark.parametrize('buy,sell,expected', [
    (0., 0., 0.),
    # Rs50k each side: brokerage30,STT12.5,exchange3.07,SEBI.1,IPFT.0001,stamp1.5,GST5.970618.
    (50000., 50000., 53.140718),
    # Brokerage is independently capped at Rs20 per leg.
    (100000., 100000., 82.681436),
    (100000., 50000., 62.411077),
])
def test_indicative_fee_invoice_matches_documented_levy_bases(buy, sell, expected):
    assert research.fees(buy, sell) == pytest.approx(expected, abs=1e-9)


def test_sell_tax_and_buy_stamp_are_applied_to_correct_sides():
    # Swapping these notionals changes STT by +.25 and stamp duty by -.03.
    assert research.fees(100000., 101000.)-research.fees(101000., 100000.) == pytest.approx(.22)


@pytest.mark.parametrize('side,exit_price', [('LONG', 101.2), ('SHORT', 99.2)])
def test_live_projection_sizes_at_trigger_then_costs_actual_side_aware_fills(side, exit_price):
    ledger = pd.DataFrame([dict(portfolio_executed=True, trigger=100., entry_price=100.2,
        exit_price=exit_price, side=side, day='2026-10-15', sid=1,
        setup_id='1036_'+side, tradingsymbol='EXAMPLE')])
    projection, summaries = research.live_projection(ledger, ['2026-10-14', '2026-10-15'])
    trade = projection.iloc[0]
    assert trade.quantity == 500  # The entry gap does not retrospectively change the submitted quantity.
    assert trade.entry_notional_rs == pytest.approx(50100.)
    assert trade.gross_pnl_rs == pytest.approx(500.)
    assert trade.net_after_fees_rs == pytest.approx(500.-trade.fees_rs)
    assert trade.net_after_fees_rs-trade.net_after_fees_and_5bps_slippage_rs == pytest.approx(25.05)
    assert all(row['trades'] == row['wins'] == row['profitable_days'] == 1 for row in summaries)


def test_stats_include_no_trade_calendar_days_and_apply_stress_to_exposure():
    ledger = pd.DataFrame([dict(day='2026-10-15', portfolio_executed=True,
        portfolio_net_profit_rupees=250., portfolio_trade_exposure_rupees=500000.,
        exit_reason='TARGET', filled=True)])
    days = ['2026-10-14', '2026-10-15']
    normal = research.stats(ledger, days)
    stress = research.stats(ledger, days, extra_bps=10)
    assert normal['profitable_days'] == 1 and normal['zero_days'] == 1
    assert normal['profitable_day_rate_pct'] == 50.
    assert stress['net_pnl_rs'] == -250.
    assert stress['losses'] == stress['losing_days'] == 1
    assert stress['daily_close_drawdown_rs'] == 250.
