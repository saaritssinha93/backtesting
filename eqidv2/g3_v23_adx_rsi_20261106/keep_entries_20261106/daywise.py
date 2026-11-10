"""Day-wise comparison helpers; keep-all-entries reports must reconcile exactly."""
from __future__ import annotations

import pandas as pd


def comparison(baseline, revised):
    fields = ['day', 'trades', 'wins', 'losses', 'win_rate_pct', 'net_pnl_rs', 'gross_loss_rs']
    for name, frame in [('baseline', baseline), ('V23', revised)]:
        if frame.day.duplicated().any() or frame[fields].isna().any().any():
            raise ValueError('Invalid day-wise inventory: ' + name)
    if set(baseline.day) != set(revised.day):
        raise ValueError('Baseline and V23 must cover exactly the same sessions')
    joined = baseline[fields].merge(revised[fields], on='day', suffixes=('_baseline', '_v23'),
                                    validate='one_to_one').sort_values('day').reset_index(drop=True)
    if not joined.trades_baseline.eq(joined.trades_v23).all():
        raise ValueError('V23 reduced or changed the number of trades on at least one day')
    joined['pnl_change_rs'] = joined.net_pnl_rs_v23 - joined.net_pnl_rs_baseline
    joined['gross_loss_change_rs'] = joined.gross_loss_rs_v23 - joined.gross_loss_rs_baseline
    return joined


def markdown(rows, title='V23 day-wise results — all baseline entries retained'):
    lines = [f'# {title}', '',
             'History: 65 sealed sessions, 29 July–4 November 2026. November 5–6 are not included. '
             'All 73 setup/direction pairs through 14:00 remain configured. '
             'Amounts are after the modelled costs unless labelled gross loss; gross loss means the '
             'absolute sum of negative after-cost trade P&L, before offsetting winning trades.', '',
             'Sizing: Rs 100,000 capital per trade at 5x exposure; unlimited portfolio capital. '
             'This is research sizing, not LIVE sizing. Previously observed history; not independent forward validation.', '',
             '| Day | Trades (both) | V21 + G7 net (Rs) | V23 net (Rs) | Change (Rs) | V23 wins | V23 losses | V23 win rate | V23 loss amount (Rs) |',
             '|:--|--:|--:|--:|--:|--:|--:|--:|--:|']
    for r in rows.itertuples():
        lines.append(f'| {r.day} | {int(r.trades_v23)} | {r.net_pnl_rs_baseline:,.2f} | '
                     f'{r.net_pnl_rs_v23:,.2f} | {r.pnl_change_rs:+,.2f} | {int(r.wins_v23)} | '
                     f'{int(r.losses_v23)} | {r.win_rate_pct_v23:.2f}% | {r.gross_loss_rs_v23:,.2f} |')
    total = rows.select_dtypes(include='number').sum()
    win_rate = 100 * total.wins_v23 / total.trades_v23 if total.trades_v23 else 0
    lines += [f'| **Total** | **{int(total.trades_v23)}** | **{total.net_pnl_rs_baseline:,.2f}** | '
              f'**{total.net_pnl_rs_v23:,.2f}** | **{total.pnl_change_rs:+,.2f}** | '
              f'**{int(total.wins_v23)}** | **{int(total.losses_v23)}** | **{win_rate:.2f}%** | '
              f'**{total.gross_loss_rs_v23:,.2f}** |', '',
              'The trade-count equality is checked on every day, not only in the overall total.', '']
    return '\n'.join(lines)
