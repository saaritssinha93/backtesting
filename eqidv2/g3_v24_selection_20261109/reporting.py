"""Offline V24 comparisons; selection diagnostics never grant trading authority."""
from __future__ import annotations

from pathlib import Path
from datetime import date, datetime
import json
import math

import numpy as np
import pandas as pd


BASELINE = 'baseline_v23'
POLICIES = (BASELINE, 'stretch_rank', 'structure_rank', 'combined_rank', 'combined_volume_rank')
LABELS = {
    BASELINE: 'V23 unchanged baseline',
    'stretch_rank': 'Stretch ranking',
    'structure_rank': 'Breakout-position ranking',
    'combined_rank': 'Stretch + position ranking',
    'combined_volume_rank': 'Stretch + position + volume ranking',
}
PNL = 'portfolio_net_profit_rupees'
FLAT_TOLERANCE = 1e-7
IDENTITY = ('day', 'setup_id', 'tradingsymbol', 'side')
ADDITIVE = ('selected_orders', 'trades', 'wins', 'losses', 'breakeven_trades',
            'net_pnl_rs', 'gross_profit_rs', 'gross_loss_rs', 'costs_rs')


def periods_for(days):
    inherited = [d for d in days if d <= '2026-11-05']
    early = [d for d in days if d <= '2026-10-27']
    later = [d for d in days if d > '2026-10-27']
    new = [d for d in days if d > '2026-11-05']
    return {f'full_{len(days)}': days, f'inherited_{len(inherited)}': inherited,
            f'early_{len(early)}': early, f'later_{len(later)}': later,
            f'new_{len(new)}': new, 'fit_44': days[:44], 'validation_15': days[44:59]}


def metric(book, selected, days):
    """Use after-cost trade PnL; zero-trade sessions remain in daily metrics."""
    part = book.loc[book.day.isin(days)]
    pnl = pd.to_numeric(part[PNL], errors='raise').astype(float).copy()
    if not np.isfinite(pnl).all():
        raise ValueError('Trade PnL must be finite')
    pnl.loc[pnl.abs().lt(FLAT_TOLERANCE)] = 0.
    daily = part.assign(_pnl=pnl).groupby('day')._pnl.sum().reindex(days, fill_value=0.)
    equity = np.r_[0., daily.cumsum().to_numpy(float)]
    loss = float(-pnl.clip(upper=0).sum())
    profit = float(pnl.clip(lower=0).sum())
    return dict(sessions=len(days), selected_orders=int(selected), trades=len(part),
        wins=int(pnl.gt(0).sum()), losses=int(pnl.lt(0).sum()),
        breakeven_trades=int(pnl.eq(0).sum()),
        win_rate_pct=float(pnl.gt(0).mean()*100) if len(pnl) else 0.,
        net_pnl_rs=float(pnl.sum()), gross_profit_rs=profit, gross_loss_rs=loss,
        profit_factor=profit/loss if loss else None,
        average_trade_rs=float(pnl.mean()) if len(pnl) else 0.,
        median_trade_rs=float(pnl.median()) if len(pnl) else 0.,
        average_daily_pnl_rs=float(daily.mean()) if len(daily) else 0.,
        median_daily_pnl_rs=float(daily.median()) if len(daily) else 0.,
        daily_close_drawdown_rs=float((np.maximum.accumulate(equity)-equity).max()),
        worst_day_rs=float(daily.min()) if len(daily) else 0.,
        negative_days=int(daily.lt(0).sum()), flat_days=int(daily.eq(0).sum()),
        costs_rs=250.*len(part))


def _validate(books, orders, days):
    if days != sorted(set(days)) or not days:
        raise ValueError('History dates must be nonempty, unique and ordered')
    if set(books) != set(POLICIES) or set(orders) != set(POLICIES):
        raise ValueError('All five declared V24 arms are required')
    clean_books, clean_orders = {}, {}
    for name in POLICIES:
        book, selected = books[name].copy(), orders[name].copy()
        required = {'sid', *IDENTITY, PNL, 'entry_ts', 'exit_ts', 'entry_price', 'exit_price', 'exit_reason'}
        if not required.issubset(book) or not {'sid', *IDENTITY}.issubset(selected):
            raise ValueError('Missing trade/order identity fields: '+name)
        for frame in (book, selected):
            if frame.sid.isna().any() or frame.sid.duplicated().any():
                raise ValueError('Duplicate or missing SID: '+name)
            frame['day'] = frame.day.astype(str).str[:10]
            if not set(frame.day).issubset(days):
                raise ValueError('Trade/order is outside declared history: '+name)
        if not set(book.sid).issubset(selected.sid):
            raise ValueError('Filled trade was not an admitted order: '+name)
        joined = book[['sid', *IDENTITY]].merge(selected[['sid', *IDENTITY]], on='sid',
            how='left', suffixes=('_trade', '_order'), validate='one_to_one')
        for key in IDENTITY:
            if not joined[key+'_trade'].astype(str).equals(joined[key+'_order'].astype(str)):
                raise ValueError('Filled/admitted identity differs: '+name+'/'+key)
        book[PNL] = pd.to_numeric(book[PNL], errors='raise')
        if not np.isfinite(book[PNL]).all():
            raise ValueError('Trade PnL must be finite')
        book.loc[book[PNL].abs().lt(FLAT_TOLERANCE), PNL] = 0.
        clean_books[name], clean_orders[name] = book, selected
    baseline = clean_books[BASELINE].set_index('sid')
    for name in POLICIES[1:]:
        candidate = clean_books[name].set_index('sid')
        common = baseline.index.intersection(candidate.index)
        left, right = baseline.loc[common], candidate.loc[common]
        for key in (*IDENTITY, 'exit_reason'):
            if not left[key].astype(str).equals(right[key].astype(str)):
                raise ValueError('Common trade identity/exit changed: '+name+'/'+key)
        for key in ('entry_ts', 'exit_ts'):
            if not pd.to_datetime(left[key], utc=True).equals(pd.to_datetime(right[key], utc=True)):
                raise ValueError('Common trade timestamp changed: '+name+'/'+key)
        for key in ('entry_price', 'exit_price', PNL):
            if not np.allclose(left[key].astype(float), right[key].astype(float), atol=1e-6, rtol=0):
                raise ValueError('Common trade economics changed: '+name+'/'+key)
    return clean_books, clean_orders


def choose_diagnostic(overall):
    """Freeze the candidate using only the first44 sessions, never later results."""
    fit = overall.loc[overall.period.eq('fit_44')]
    base = fit.loc[fit.policy.eq(BASELINE)].iloc[0]
    eligible = fit.loc[fit.policy.ne(BASELINE)
        & fit.net_pnl_rs.gt(base.net_pnl_rs+1e-7)
        & fit.gross_loss_rs.le(base.gross_loss_rs+1e-7)
        & fit.daily_close_drawdown_rs.le(base.daily_close_drawdown_rs+1e-7)]
    if eligible.empty:
        return BASELINE
    return str(eligible.sort_values(['net_pnl_rs', 'daily_close_drawdown_rs', 'gross_loss_rs', 'policy'],
        ascending=[False, True, True, True], kind='stable').iloc[0].policy)


def trade_changes(books):
    base = books[BASELINE].set_index('sid')
    changes, summaries = [], []
    for name in POLICIES[1:]:
        candidate = books[name].set_index('sid')
        removed = base.loc[~base.index.isin(candidate.index)]
        added = candidate.loc[~candidate.index.isin(base.index)]
        for action, part, direction in [('REMOVED', removed, -1), ('ADDED', added, 1)]:
            for sid, row in part.iterrows():
                value = float(row[PNL])
                changes.append(dict(policy=name, action=action, sid=sid,
                    **{key:row[key] for key in IDENTITY},
                    outcome='WIN' if value > 0 else 'LOSS' if value < 0 else 'FLAT',
                    trade_net_pnl_rs=value, contribution_to_delta_rs=direction*value))
        result = dict(policy=name, retained_trades=int(len(base.index.intersection(candidate.index))),
            removed_trades=len(removed), added_trades=len(added),
            removed_winners=int(removed[PNL].gt(0).sum()), removed_losers=int(removed[PNL].lt(0).sum()),
            removed_flats=int(removed[PNL].eq(0).sum()), added_winners=int(added[PNL].gt(0).sum()),
            added_losers=int(added[PNL].lt(0).sum()), added_flats=int(added[PNL].eq(0).sum()),
            winning_pnl_removed_rs=float(removed[PNL].clip(lower=0).sum()),
            losing_pnl_avoided_rs=float(-removed[PNL].clip(upper=0).sum()),
            winning_pnl_added_rs=float(added[PNL].clip(lower=0).sum()),
            losing_pnl_added_rs=float(-added[PNL].clip(upper=0).sum()),
            delta_net_pnl_rs=float(candidate[PNL].sum()-base[PNL].sum()))
        reconstructed = (result['losing_pnl_avoided_rs']-result['winning_pnl_removed_rs']
                         +result['winning_pnl_added_rs']-result['losing_pnl_added_rs'])
        if not np.isclose(reconstructed, result['delta_net_pnl_rs'], atol=1e-5, rtol=0):
            raise ValueError('Trade-change attribution does not reconcile: '+name)
        summaries.append(result)
    columns = ['policy','action','sid',*IDENTITY,'outcome','trade_net_pnl_rs','contribution_to_delta_rs']
    return pd.DataFrame(changes, columns=columns), pd.DataFrame(summaries)


def _table(frame):
    lines = ['| Policy | Orders | Trades | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/trade Rs | Median/trade Rs | Daily-close DD Rs |',
             '|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|']
    for r in frame.itertuples(index=False):
        lines.append(f'| {LABELS[r.policy]} | {r.selected_orders} | {r.trades} | {r.wins} | {r.losses} | '
            f'{r.breakeven_trades} | {r.win_rate_pct:.2f}% | {r.net_pnl_rs:,.2f} | {r.gross_loss_rs:,.2f} | '
            f'{r.average_trade_rs:,.2f} | {r.median_trade_rs:,.2f} | {r.daily_close_drawdown_rs:,.2f} |')
    return lines


def _json_ready(value):
    if isinstance(value, dict):
        return {str(k):_json_ready(v) for k,v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, (np.integer, np.bool_)):
        return value.item()
    if isinstance(value, (float, np.floating)):
        return float(value) if math.isfinite(value) else None
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def publish(output, books, orders, days, config, coverage):
    """Write new research reports. Returns JSON-ready summary; no runtime changes."""
    if config.get('execution_authority', False) or config.get('live_changed', False):
        raise ValueError('V24 reporting requires backtest-only scope')
    books, orders = _validate(books, orders, list(days))
    days = list(days)
    periods = periods_for(days)
    overall = pd.DataFrame([dict(policy=name, period=label,
        **metric(books[name], int(orders[name].day.isin(cohort).sum()), cohort))
        for label,cohort in periods.items() for name in POLICIES])
    daily = pd.DataFrame([dict(policy=name, day=day,
        **metric(books[name], int(orders[name].day.eq(day).sum()), [day]))
        for day in days for name in POLICIES])
    full_key = f'full_{len(days)}'
    for name in POLICIES:
        full = overall.loc[overall.policy.eq(name) & overall.period.eq(full_key)].iloc[0]
        part = daily.loc[daily.policy.eq(name)]
        for field in ADDITIVE:
            if not np.isclose(full[field], part[field].sum(), atol=1e-5, rtol=0):
                raise ValueError('Daily reconciliation failed: '+name+'/'+field)
    candidate = choose_diagnostic(overall)
    changes, change_summary = trade_changes(books)
    full = overall.loc[overall.period.eq(full_key)]
    baserow = full.loc[full.policy.eq(BASELINE)].iloc[0]
    candidate_row = full.loc[full.policy.eq(candidate)].iloc[0]
    summary = _json_ready(dict(version='G3-V24-SELECTION-RESEARCH-20261109',
        execution_authority=False, live_changed=False, diagnostic_candidate=candidate,
        selection_basis='FIRST_44_SESSIONS_ONLY_STRICT_NET_IMPROVEMENT_LOSS_AND_DRAWDOWN_NOT_WORSE',
        selected_for_live=False, automatic_promotion=False, retrospective_only=True,
        sessions=len(days), first_day=days[0], last_day=days[-1], periods=periods,
        overall=overall.to_dict('records'), change_summary=change_summary.to_dict('records'),
        source_coverage=coverage, daily_reconciliation='PASS', common_trade_exit_parity='PASS',
        comparison_delta_net_pnl_rs=float(candidate_row.net_pnl_rs-baserow.net_pnl_rs)))
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    names = ['overall.csv','daily.csv','trade_changes.csv','change_summary.csv','REPORT.md','DAY_WISE_RESULTS.md']
    names += [name+suffix for name in POLICIES for suffix in ('_trades.csv','_orders.csv')]
    if any((output/name).exists() for name in names):
        raise ValueError('Research reports already exist; use a fresh output folder')
    overall.to_csv(output/'overall.csv', index=False)
    daily.to_csv(output/'daily.csv', index=False)
    changes.to_csv(output/'trade_changes.csv', index=False)
    change_summary.to_csv(output/'change_summary.csv', index=False)
    for name in POLICIES:
        books[name].to_csv(output/(name+'_trades.csv'), index=False)
        orders[name].to_csv(output/(name+'_orders.csv'), index=False)

    report = ['# G3 V24 selection-ranking backtest', '',
        f'{len(days)} available sessions from {days[0]} through {days[-1]}. '
        'The comparison changes selection ranking while retaining the adopted V23 exits and entry caps.', '',
        'The unchanged baseline uses a 1.50% initial stop and each setup\'s existing target. '
        'A completed post-entry one-minute close at 80% of the target activates a stop at +0.05% signed return '
        'from entry on the next bar. The entry bar cannot arm protection. Stops only tighten. '
        'Adverse gap-open fills, stop-first ambiguous bars and the 15:15 time exit are retained.', '',
        'All arms use Rs 500,000 modeled exposure and Rs 250 round-trip costs per filled trade with unlimited '
        'concurrent capital. The 34 original pairs retain cap three; the 39 added pairs retain cap one. '
        'The 73 pairs run through 14:00. Ranking keeps these caps; it does not guarantee identical admission or fill counts.', '',
        'The existing new-pair volume veto and G7 confirmation rules retain their post-ranking timing. '
        'A vetoed selected candidate does not promote the next stock. V23 already includes G7 filters, '
        'so this research baseline differs from the production paper strategy examined on 9 November.', '',
        '## Full available history', '', *_table(full), '',
        'Loss amount is the absolute sum of negative after-cost trade PnL. Flat trades are not wins. '
        'Daily-close drawdown includes zero-trade dates and starts from zero equity; it is not intraday drawdown.', '',
        '## Chronological comparisons', '']
    for label in periods:
        if label == full_key:
            continue
        cohort = periods[label]
        dates = f'{cohort[0]} through {cohort[-1]}' if cohort else 'no available sessions'
        report += [f'### {label}: {dates}', '', *_table(overall.loc[overall.period.eq(label)]), '']
    report += ['## Displaced and replacement trades', '',
        '| Ranking arm | Removed winners | Removed losers | Added winners | Added losers | Winning PnL removed Rs | Losing PnL avoided Rs | Winning PnL added Rs | Losing PnL added Rs | Net change Rs |',
        '|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|']
    for r in change_summary.itertuples(index=False):
        report.append(f'| {LABELS[r.policy]} | {r.removed_winners} | {r.removed_losers} | {r.added_winners} | '
            f'{r.added_losers} | {r.winning_pnl_removed_rs:,.2f} | {r.losing_pnl_avoided_rs:,.2f} | '
            f'{r.winning_pnl_added_rs:,.2f} | {r.losing_pnl_added_rs:,.2f} | {r.delta_net_pnl_rs:+,.2f} |')
    report += ['', '## Interpretation', '']
    if candidate == BASELINE:
        report.append('No ranking arm qualified on the first 44 sessions: qualification requires higher net profit '
            'with no increase in loss amount or daily-close drawdown. The unchanged baseline remains the diagnostic choice.')
    else:
        report.append(f'The first-44-session diagnostic choice is **{LABELS[candidate]}**. '
            'It was selected without reading later-period results. Every arm remains visible above, including later failures. '
            'This diagnostic choice is not an activation or promotion.')
    report += ['', 'These are retrospective comparisons. Earlier strategy choices used this history, and the 9 November '
        'trades informed the selection hypothesis. The chronological slices are sensitivity checks, not untouched '
        'out-of-sample proof. No new live policy is promoted from this report.', '',
        'The inherited source excludes 1 October for incomplete sealed data. The 8 November special session uses a '
        'different schedule and is not part of this normal-session backtest. Other source exclusions and unavailable '
        'features are recorded in the configuration and coverage artifacts.', '',
        'The inherited 28 October expanded-slot source covers 211 of 215 stocks, with 116 unavailable appended '
        'stock/slot rows. These rows remain absent in every arm; healthy-stock observations are retained. '
        'Some earlier archived universes are not strictly point-in-time, so survivorship/cohort bias remains a limitation.', '',
        '[Day-wise results](DAY_WISE_RESULTS.md) | [All daily metrics](daily.csv) | '
        '[Displaced/replacement trades](trade_changes.csv) | [Overall metrics](overall.csv)', '',
        'Previous versions, production schedules, live trading and paper runtime settings were not changed.', '']
    (output/'REPORT.md').write_text('\n'.join(report), encoding='utf-8')

    day_lines = ['# G3 V24 day-wise results', '',
        'Net PnL includes Rs 250 per filled trade at Rs 500,000 modeled exposure. '
        'All available days, including any zero-trade day, are retained. Trade counts, win rates and losses for each '
        'arm are available in [daily.csv](daily.csv).', '',
        '| Day | Baseline trades | Baseline net Rs | Stretch net Rs | Position net Rs | Combined net Rs | Combined + volume net Rs |',
        '|:--|--:|--:|--:|--:|--:|--:|']
    for day in days:
        values = daily.loc[daily.day.eq(day)].set_index('policy')
        day_lines.append(f'| {day} | {int(values.loc[BASELINE,"trades"])} | '+
            ' | '.join(f'{values.loc[name,"net_pnl_rs"]:,.2f}' for name in POLICIES)+' |')
    full_indexed = full.set_index('policy')
    day_lines += [f'| **Total** | **{int(full_indexed.loc[BASELINE,"trades"])}** | '+
        ' | '.join(f'**{full_indexed.loc[name,"net_pnl_rs"]:,.2f}**' for name in POLICIES)+' |', '',
        'Daily sums reconcile to each full-history total. Win rates are calculated from individual trades, '
        'not averaged across days. All comparisons are retrospective.', '',
        '[Full report](REPORT.md) | [Displaced/replacement trades](trade_changes.csv)', '']
    (output/'DAY_WISE_RESULTS.md').write_text('\n'.join(day_lines), encoding='utf-8')
    return summary
