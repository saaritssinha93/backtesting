import json

import numpy as np
import pandas as pd
import pytest

from . import reporting as r


def fixture():
    days = ['2026-11-05', '2026-11-06', '2026-11-09']
    identities = [dict(sid=sid, day=days[1 if sid < 4 else 2],
                       setup_id='0926_'+('LONG' if sid % 2 else 'SHORT'),
                       tradingsymbol='S'+str(sid), side='LONG' if sid % 2 else 'SHORT')
                  for sid in range(1, 7)]
    inventory = pd.DataFrame(identities)

    def trade(sid, pnl, retimed=False):
        row = identities[sid-1].copy()
        entry = 101. if retimed else 100.
        minute = 28 if retimed else 27
        sign = 1 if row['side'] == 'LONG' else -1
        return dict(row, entry_ts=f"{row['day']} 09:{minute}:00+05:30",
            exit_ts=f"{row['day']} 10:15:00+05:30", entry_price=entry,
            exit_price=entry*(1+sign*(pnl+250)/500000), exit_reason='TEST',
            portfolio_net_profit_rupees=float(pnl))

    baseline = pd.DataFrame([trade(1,100), trade(2,200), trade(3,-300), trade(4,-100), trade(6,0)])
    candidate = pd.DataFrame([trade(1,100), trade(4,50,True), trade(5,75,True), trade(6,-25,True)])
    books = {r.BASELINE:baseline, r.PRIMARY:candidate,
             'rsi_reset_recovery_keep_trigger':candidate.copy()}
    audits = {name:inventory.assign(filled=inventory.sid.isin(book.sid)) for name,book in books.items()}
    return days, books, audits


def test_retimed_common_sid_is_allowed_and_attribution_reconciles():
    days, books, audits = fixture()
    books, _ = r.validate(books, audits, days)
    detailed, summary = r.attribution(books)
    row = summary.loc[summary.policy.eq(r.PRIMARY)].iloc[0]
    assert row.unchanged_fills == 1
    assert row.retimed_fills == 2
    assert row.missed_winners == 1 and row.missed_losers == 1
    assert row.new_fills == 1
    assert row.missed_winning_pnl_rs == 200
    assert row.avoided_losing_pnl_rs == 300
    assert row.retimed_delta_net_pnl_rs == 125
    assert row.new_fill_net_pnl_rs == 75
    assert row.delta_net_pnl_rs == 300
    assert detailed.loc[detailed.policy.eq(r.PRIMARY), 'contribution_to_delta_rs'].sum() == 300


def test_same_entry_cannot_change_frozen_exit():
    days, books, audits = fixture()
    books[r.PRIMARY].loc[books[r.PRIMARY].sid.eq(1), 'exit_price'] += 1
    with pytest.raises(ValueError, match='Same entry changed'):
        r.validate(books, audits, days)


def test_changed_inventory_or_fill_flags_rejected():
    days, books, audits = fixture()
    audits[r.PRIMARY] = audits[r.PRIMARY].loc[audits[r.PRIMARY].sid.ne(2)]
    with pytest.raises(ValueError, match='changed admitted inventory'):
        r.validate(books, audits, days)
    days, books, audits = fixture()
    audits[r.PRIMARY].loc[audits[r.PRIMARY].sid.eq(2), 'filled'] = True
    with pytest.raises(ValueError, match='fill flags disagree'):
        r.validate(books, audits, days)


def test_publish_retains_zero_days_sides_and_does_not_promote(tmp_path):
    days, books, audits = fixture()
    summary = r.publish(tmp_path/'results', books, audits, days, {}, [])
    daily = pd.read_csv(tmp_path/'results/daily.csv')
    sides = pd.read_csv(tmp_path/'results/by_side.csv')
    overall = pd.read_csv(tmp_path/'results/overall.csv')
    assert len(daily) == 9 and len(sides) == 6
    assert daily.loc[daily.day.eq(days[0]), 'trades'].eq(0).all()
    base = overall.loc[overall.policy.eq(r.BASELINE)&overall.period.eq('full_3')].iloc[0]
    assert base.trades == 5 and base.breakeven_trades == 1
    assert base.win_rate_pct == 40
    assert base.net_pnl_rs == -100
    assert base.daily_close_drawdown_rs == 100
    assert summary['automatic_promotion'] is False
    assert summary['admitted_inventory_parity'] == 'PASS'
    assert summary['attribution_reconciliation'] == 'PASS'
    json.dumps(summary, allow_nan=False)
    assert 'retrospective' in (tmp_path/'results/REPORT.md').read_text()
    assert '| **Total** |' in (tmp_path/'results/DAY_WISE_RESULTS.md').read_text()
    with pytest.raises(ValueError, match='already exist'):
        r.publish(tmp_path/'results', books, audits, days, {}, [])


def test_nonfinite_pnl_and_naive_times_rejected():
    days, books, audits = fixture()
    books[r.PRIMARY].loc[0, r.PNL] = np.inf
    with pytest.raises(ValueError, match='Nonfinite'):
        r.validate(books, audits, days)
    days, books, audits = fixture()
    books[r.PRIMARY].loc[0, 'entry_ts'] = '2026-11-06 09:27:00'
    with pytest.raises(ValueError, match='chronology'):
        r.validate(books, audits, days)


def test_empty_candidate_books_are_reported_without_fabricating_fills(tmp_path):
    days, books, audits = fixture()
    for name in books:
        if name != r.BASELINE:
            books[name] = books[name].iloc[:0]
            audits[name]['filled'] = False
    summary = r.publish(tmp_path, books, audits, days, {}, [])
    row = next(item for item in summary['comparisons'] if item['policy'] == r.PRIMARY)
    # Avoiding all trades raises net from -100 to0, but does not establish a usable improvement.
    assert row['delta_net_pnl_rs'] == 100
    assert row['no_trade_count_reduction'] is False
    assert row['full_history_objective_met'] is False
    assert next(x for x in summary['attribution'] if x['policy']==r.PRIMARY)['missed_baseline_fills'] == 5
