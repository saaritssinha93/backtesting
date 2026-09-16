"""Registered quality gates must control which expansion can become active."""
import copy

import pandas as pd
import pytest

import fno_v13_v10_g_expansion_research as study


def baseline():
    return dict(trades=66, win_rate_pct=65.15, profit_factor=3.43,
                daily_close_drawdown_rupees=7950., net_profit_rupees=178875.)


def candidate():
    return dict(trades=80, win_rate_pct=64., profit_factor=3.4,
                daily_close_drawdown_rupees=8000., net_profit_rupees=190000.)


def test_more_profit_and_trades_cannot_compensate_for_failed_pf():
    expanded = candidate()
    expanded.update(trades=100, net_profit_rupees=250000., profit_factor=3.29)
    assert study.passed_quality(expanded, baseline(), {'net_profit_rupees':71125.}, 66) == ['PF_BELOW_3.30']


def test_displacing_previous_g_fills_is_rejected_even_with_higher_metrics():
    expanded = candidate()
    expanded.update(win_rate_pct=70., profit_factor=5.)
    assert study.passed_quality(expanded, baseline(), {'net_profit_rupees':20000.}, 65) == ['EXISTING_G_EXECUTIONS_DISPLACED']


@pytest.mark.parametrize('field,value,reason', [
    ('win_rate_pct',62.999,'WIN_RATE_BELOW_63'),
    ('daily_close_drawdown_rupees',9540.01,'DRAWDOWN_OVER_1.20X'),
    ('net_profit_rupees',178874.99,'NET_BELOW_G'),
])
def test_each_quality_requirement_remains_binding(field, value, reason):
    expanded = candidate()
    expanded[field] = value
    assert study.passed_quality(expanded, baseline(), {'net_profit_rupees':11125.}, 66) == [reason]


@pytest.mark.parametrize('net',[0.,-1.])
def test_added_trades_must_contribute_positive_net(net):
    assert study.passed_quality(candidate(), baseline(), {'net_profit_rupees':net}, 66) == ['ADDITIONAL_TRADES_NOT_PROFITABLE']


def test_exact_quality_boundaries_pass():
    expanded = candidate()
    expanded.update(win_rate_pct=63.,profit_factor=3.30,daily_close_drawdown_rupees=9540.)
    assert study.passed_quality(expanded, baseline(), {'net_profit_rupees':11125.}, 66) == []


def table():
    return pd.DataFrame([
        dict(case='G_CONTROL',trades=66,changes=0,profit_factor=3.43,quality_pass=True,frequency_pass=False),
        dict(case='MORNING_ONLY',trades=77,changes=1,profit_factor=3.35,quality_pass=True,frequency_pass=True),
        dict(case='TWO_BAR_ONLY',trades=79,changes=1,profit_factor=3.40,quality_pass=True,frequency_pass=True),
        dict(case='BOTH',trades=93,changes=2,profit_factor=5.,quality_pass=True,frequency_pass=True),
    ])


def test_full_pass_prefers_fewer_changes_then_pf_before_most_trades():
    chosen, status = study.choose(table())
    assert chosen == 'TWO_BAR_ONLY'
    assert status == 'HISTORICAL_FREQUENCY_AND_QUALITY_OBJECTIVE_MET'
    rows = table()
    rows.loc[rows.case.eq('MORNING_ONLY'), ['trades','profit_factor']] = [80,3.40]
    assert study.choose(rows)[0] == 'MORNING_ONLY'


def test_quality_failure_cannot_win_and_partial_frequency_status_is_explicit():
    rows = table()
    rows['frequency_pass'] = False
    rows.loc[rows.case.eq('MORNING_ONLY'),'trades'] = 73
    rows.loc[rows.case.eq('TWO_BAR_ONLY'),'trades'] = 74
    rows.loc[rows.case.eq('BOTH'),['quality_pass','frequency_pass']] = [False,True]
    assert study.choose(rows) == ('TWO_BAR_ONLY','QUALITY_PRESERVED_FREQUENCY_TARGET_NOT_MET')
    rows.loc[~rows.case.eq('G_CONTROL'),'quality_pass'] = False
    assert study.choose(rows) == ('G_CONTROL','EXTENSIONS_REJECTED_EXISTING_G_RETAINED')


def test_registered_plan_cannot_be_changed_on_rerun(tmp_path, monkeypatch):
    study.register(tmp_path)
    original = study.protocol
    def changed():
        value = copy.deepcopy(original())
        value['quality']['minimum_pf'] = 2.
        return value
    monkeypatch.setattr(study,'protocol',changed)
    with pytest.raises(ValueError,match='registered expansion protocol'):
        study.register(tmp_path)
