import pandas as pd
import pytest

from .daywise import comparison, markdown


def frame():
    return pd.DataFrame([dict(day='2026-11-03', trades=2, wins=1, losses=1,
                             win_rate_pct=50., net_pnl_rs=100., gross_loss_rs=50.),
                         dict(day='2026-11-04', trades=1, wins=1, losses=0,
                             win_rate_pct=100., net_pnl_rs=50., gross_loss_rs=0.)])


def test_counts_must_match_per_day_even_when_totals_match():
    changed = frame()
    changed['trades'] = [1, 2]
    with pytest.raises(ValueError, match='at least one day'):
        comparison(frame(), changed)


def test_same_days_required():
    with pytest.raises(ValueError, match='same sessions'):
        comparison(frame(), frame().iloc[:1])


def test_report_counts_profit_and_weighted_win_rate():
    joined = comparison(frame(), frame())
    report = markdown(joined)
    assert '**66.67%**' in report and '**150.00**' in report
    assert joined.pnl_change_rs.eq(0).all()
