"""Reconciled, dynamically dated reports for a fixed-policy V23 extension."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

BASELINE = 'baseline_v21_g7'
PROTECTION = 'protect_mfe0p6_lock0p1'
ARCHIVED = 'archived_entry_filter'
ADDITIVE = ('trades', 'wins', 'losses', 'net_pnl_rs', 'gross_loss_rs')
FIELDS = ('day', 'trades', 'wins', 'losses', 'win_rate_pct', 'net_pnl_rs', 'gross_loss_rs')


def _summary(frame):
    result = {key: float(frame[key].sum()) for key in ADDITIVE}
    if 'selected_orders' in frame:
        result['selected_orders'] = int(frame.selected_orders.sum())
    result['sessions'] = len(frame)
    result['win_rate_pct'] = 100 * result['wins'] / result['trades'] if result['trades'] else 0.
    equity = np.r_[0., frame.sort_values('day').net_pnl_rs.cumsum().to_numpy()]
    result['daily_close_drawdown_rs'] = float((np.maximum.accumulate(equity) - equity).max())
    result['negative_days'] = int(frame.net_pnl_rs.lt(0).sum())
    return result


def compare_days(baseline, revised):
    for name, frame in [('baseline', baseline), ('revised', revised)]:
        if frame.empty or frame.day.duplicated().any() or frame[list(FIELDS)].isna().any().any():
            raise ValueError('Invalid daily inventory: ' + name)
        values = frame[list(FIELDS)[1:]].to_numpy(dtype=float)
        if not np.isfinite(values).all():
            raise ValueError('Non-finite daily metrics: ' + name)
        if (frame.trades < frame.wins + frame.losses).any():
            raise ValueError('Win/loss counts exceed trades: ' + name)
    if set(baseline.day) != set(revised.day):
        raise ValueError('Candidate daily sessions differ from baseline')
    merged = baseline[list(FIELDS)].merge(revised[list(FIELDS)], on='day',
        suffixes=('_baseline', '_v23'), validate='one_to_one').sort_values('day').reset_index(drop=True)
    if not merged.trades_baseline.eq(merged.trades_v23).all():
        raise ValueError('Candidate changed at least one daily trade count')
    merged['pnl_change_rs'] = merged.net_pnl_rs_v23 - merged.net_pnl_rs_baseline
    merged['gross_loss_change_rs'] = merged.gross_loss_rs_v23 - merged.gross_loss_rs_baseline
    return merged


def _day_table(rows, title, note):
    days = sorted(rows.day.unique())
    lines = [f'# {title}', '', note, '',
        f'History: **{len(days)} sessions, {days[0]} through {days[-1]}**. '
        'The final listed session includes the complete strategy session, with entries through '
        'the 14:00 signal / 14:01 confirmation and execution through 15:15.', '',
        'All 73 configured setup/direction pairs remain enabled. Original pairs retain cap 3; '
        'the 39 new pairs retain cap 1 plus the volume gate. Every baseline entry is retained '
        'in this comparison, checked separately on every day.', '',
        'Amounts are after modelled costs. Loss amount is the absolute sum of negative after-cost '
        'trade P&L before offsetting winners. Exposure is Rs 500,000 per trade '
        '(Rs 100,000 capital at 5x), with Rs 250 round-trip costs and unlimited concurrent capital. '
        'This is retrospective research; policies were not retuned on the added session.', '',
        '| Day | Trades (both) | Baseline net (Rs) | V23 net (Rs) | Change (Rs) | V23 wins | V23 losses | V23 win rate | V23 loss amount (Rs) |',
        '|:--|--:|--:|--:|--:|--:|--:|--:|--:|']
    for row in rows.itertuples(index=False):
        lines.append(f'| {row.day} | {int(row.trades_v23)} | {row.net_pnl_rs_baseline:,.2f} | '
            f'{row.net_pnl_rs_v23:,.2f} | {row.pnl_change_rs:+,.2f} | {int(row.wins_v23)} | '
            f'{int(row.losses_v23)} | {row.win_rate_pct_v23:.2f}% | {row.gross_loss_rs_v23:,.2f} |')
    totals = rows.select_dtypes(include='number').sum()
    rate = 100 * totals.wins_v23 / totals.trades_v23 if totals.trades_v23 else 0.
    lines += [f'| **Total** | **{int(totals.trades_v23)}** | **{totals.net_pnl_rs_baseline:,.2f}** | '
        f'**{totals.net_pnl_rs_v23:,.2f}** | **{totals.pnl_change_rs:+,.2f}** | '
        f'**{int(totals.wins_v23)}** | **{int(totals.losses_v23)}** | **{rate:.2f}%** | '
        f'**{totals.gross_loss_rs_v23:,.2f}** |', '',
        'Zero-trade sessions remain in the inventory. Total win rate uses all trades, '
        'not the average of daily win rates.', '']
    return '\n'.join(lines)


def _metrics_table(rows):
    lines = ['| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |',
             '|:--|--:|--:|--:|--:|--:|--:|--:|']
    for label, row in rows:
        lines.append(f'| {label} | {int(row["trades"])} | {int(row["wins"])} | '
            f'{int(row["losses"])} | {row["win_rate_pct"]:.2f}% | {row["net_pnl_rs"]:,.2f} | '
            f'{row["gross_loss_rs"]:,.2f} | {row["daily_close_drawdown_rs"]:,.2f} |')
    return lines


def write_reports(output: Path, overall: pd.DataFrame, daily: pd.DataFrame, config: dict) -> None:
    """Write reports after checking each policy's dates and full-history sums.

    Required config: selected_exit_policy. Optional coverage_notes is a list of
    verified source descriptions, and history_cutoff must equal the final day.
    """
    output = Path(output)
    selected = config['selected_exit_policy']
    required = {BASELINE, selected, PROTECTION, ARCHIVED}
    if not required.issubset(set(daily.policy)):
        raise ValueError('Required comparison policies are missing')
    if daily.duplicated(['policy', 'day']).any() or overall.duplicated(['policy', 'period']).any():
        raise ValueError('Duplicate policy/day or policy/period')
    baseline = daily.loc[daily.policy.eq(BASELINE)].sort_values('day')
    days = sorted(baseline.day.unique())
    if not days or config.get('history_cutoff', days[-1]) != days[-1]:
        raise ValueError('History cutoff does not match the complete daily inventory')
    full_period = f'full_{len(days)}'
    full = overall.loc[overall.period.eq(full_period)].set_index('policy')
    for name, frame in daily.groupby('policy'):
        if set(frame.day) != set(days) or name not in full.index:
            raise ValueError('Policy does not cover every session: ' + name)
        summary = _summary(frame)
        for field in (*ADDITIVE, 'win_rate_pct', 'daily_close_drawdown_rs'):
            if not np.isclose(summary[field], full.loc[name, field], atol=1e-5, rtol=0):
                raise ValueError('Daily/full reconciliation failed: ' + name + ':' + field)
        if name != ARCHIVED:
            compare_days(baseline, frame)
    current = compare_days(baseline, daily.loc[daily.policy.eq(selected)])
    alternative = compare_days(baseline, daily.loc[daily.policy.eq(PROTECTION)])
    output.mkdir(parents=True, exist_ok=True)
    current.to_csv(output / 'day_wise_results.csv', index=False)
    alternative.to_csv(output / 'profit_protection_day_wise.csv', index=False)
    current_note = f'Current selected exit policy: **{selected}**; ADX/RSI entry vetoes are disabled.'
    (output / 'DAY_WISE_RESULTS.md').write_text(_day_table(current,
        'V23 day-wise results — every baseline entry retained', current_note), encoding='utf-8')
    alternative_note = ('**Unselected profit-protection experiment.** A completed post-entry bar reaching '
        '+0.6% favorable movement activates a +0.1% entry-relative stop from the next bar. '
        'Adverse opening gaps can fill worse. This is not the current V23 configuration.')
    (output / 'PROFIT_PROTECTION_DAY_WISE.md').write_text(_day_table(alternative,
        'V23 profit-protection day-wise experiment — not selected', alternative_note), encoding='utf-8')
    current_daily = daily.loc[daily.policy.eq(selected)]
    previous = current_daily.loc[current_daily.day.lt(days[-1])]
    newest = current_daily.loc[current_daily.day.eq(days[-1])]
    labels = [(f'Previous {len(previous)} sessions through {previous.day.max()}', _summary(previous)),
              (f'Added full session: {days[-1]}', _summary(newest)),
              (f'Updated full history: {len(days)} sessions', _summary(current_daily))]
    compare = [('V21 + G7 / current V23 (all entries retained)', full.loc[BASELINE]),
               ('Archived entry-filter experiment (not current)', full.loc[ARCHIVED]),
               ('Profit-protection experiment (not selected)', full.loc[PROTECTION])]
    if selected != BASELINE:
        compare.insert(1, ('Current V23: ' + selected, full.loc[selected]))
    lines = [f'# V23 results through {days[-1]}', '',
        f'**{days[-1]} is included as a complete strategy session.** The updated history contains '
        f'**{len(days)} sessions**, from {days[0]} through {days[-1]}, and '
        f'**{int(full.loc[selected, "trades"])} current V23 filled trades**.', '',
        '## Added session and updated total', ''] + _metrics_table(labels)
    lines += ['', 'Drawdown in each row is recalculated within that period; drawdowns are not additive.', '',
        '## Fixed-policy comparison over the full history', ''] + _metrics_table(compare)
    lines += ['', 'ADX/RSI entry vetoes remain disabled in the current mode. All baseline entry identities, '
        'prices and times are retained. The earlier entry-filter experiment remains a comparison only: '
        'it removes winners as well as losers.', '',
        'The selected exit and sizing policies, five exit alternatives and five sizing alternatives '
        'were frozen before appending this session. Selection still uses the earlier 59 sessions ending '
        f'{config.get("training_cutoff", "2026-10-27")}; the added day does not select thresholds. '
        'Earlier attempts did not simultaneously retain baseline net profit, reduce loss amounts and '
        'reduce daily-close drawdown. Historical results do not establish that only losing trades '
        'can be reduced in future.', '',
        '## Day-wise results', '',
        '[Current V23 — every session](DAY_WISE_RESULTS.md) · [Current day-wise CSV](day_wise_results.csv) · '
        '[Profit-protection experiment](PROFIT_PROTECTION_DAY_WISE.md) · '
        '[Alternative day-wise CSV](profit_protection_day_wise.csv)', '',
        '## Coverage and assumptions', '',
        f'- All {len(days)} sessions have a row, including sessions without trades. October 1 remains '
        'excluded by the original sealed history because its source is incomplete.',
        '- All 73 setup/direction pairs remain enabled through the 14:00 signal, with final confirmation '
        'at 14:01. Execution paths run through 15:15. Original pairs retain cap 3; new pairs cap 1 plus '
        'the existing volume gate.',
        '- Rs 100,000 capital at 5x exposure per entry, Rs 500,000 fixed notional, Rs 250 modelled '
        'round-trip costs, and unlimited concurrent capital. Loss amount is the absolute sum of negative '
        'after-cost trade P&L before offsetting winners.',
        '- The full strategy session does not mean absent source data were filled synthetically. '
        'Source-specific exclusions are retained and disclosed below.',
    ]
    for note in config.get('coverage_notes', []):
        lines.append('- ' + str(note))
    lines += ['- One-minute OHLC cannot reveal the intrabar path. Exit experiments retain stop-first '
        'tie handling and adverse-open stop fills. New protection starts after the observed bar.',
        '- All periods are retrospective. This is not independent forward validation or LIVE sizing. '
        'LIVE execution, approvals and scheduling are unchanged.', '',
        '[All exit policy totals](overall.csv) · [All exit policy daily rows](daily.csv) · '
        '[Sizing totals](sizing_overall.csv) · [Sizing daily results](sizing_daily.csv) · '
        '[Configuration](config.json)', '']
    (output / 'REPORT.md').write_text('\n'.join(lines), encoding='utf-8')
