"""Matched-inventory reporting for the isolated RSI entry-timing experiment."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from ..reporting import metric, _json_ready

BASELINE = 'baseline_v23'
PRIMARY = 'rsi_reset_recovery'
LABELS = {
    BASELINE: 'V23 unchanged entry timing',
    PRIMARY: 'RSI reset and recovery',
    'rsi_reset_recovery_keep_trigger': 'RSI recovery with original trigger retained',
}
IDENTITY = ('day', 'setup_id', 'tradingsymbol', 'side')
PNL = 'portfolio_net_profit_rupees'
ADDITIVE = ('selected_orders', 'trades', 'wins', 'losses', 'breakeven_trades',
            'net_pnl_rs', 'gross_profit_rs', 'gross_loss_rs', 'costs_rs')
TOLERANCE = 1e-7


def periods_for(days):
    early = [day for day in days if day <= '2026-10-27']
    later = [day for day in days if day > '2026-10-27']
    inherited = [day for day in days if day <= '2026-11-05']
    new = [day for day in days if day > '2026-11-05']
    return {f'full_{len(days)}': days, f'earlier_{len(early)}': early,
            f'later_{len(later)}': later, f'inherited_{len(inherited)}': inherited,
            f'new_{len(new)}': new,
            '2026-11-06': [day for day in days if day == '2026-11-06'],
            '2026-11-09': [day for day in days if day == '2026-11-09']}


def _same_float(left, right):
    return np.isclose(float(left), float(right), atol=1e-6, rtol=0)


def _same_clock(left, right):
    return pd.Timestamp(left).tz_convert('UTC') == pd.Timestamp(right).tz_convert('UTC')


def _same_execution(left, right):
    return (all(_same_clock(left[key], right[key]) for key in ('entry_ts', 'exit_ts'))
            and all(_same_float(left[key], right[key]) for key in ('entry_price', 'exit_price', PNL))
            and str(left.exit_reason) == str(right.exit_reason))


def validate(books, order_audits, days):
    if days != sorted(set(days)) or not days:
        raise ValueError('Dates must be nonempty, ordered and unique')
    if set(books) != set(LABELS) or set(books) != set(order_audits):
        raise ValueError('All three fixed timing arms with matching order audits are required')
    clean_books, clean_audits = {}, {}
    for policy in books:
        book, audit = books[policy].copy(), order_audits[policy].copy()
        if (not {'sid', *IDENTITY, PNL, 'entry_ts', 'exit_ts', 'entry_price', 'exit_price', 'exit_reason'}.issubset(book)
                or not {'sid', *IDENTITY}.issubset(audit)):
            raise ValueError('Missing trade/order columns: '+policy)
        for frame in (book, audit):
            if frame.sid.isna().any() or frame.sid.duplicated().any():
                raise ValueError('Missing or duplicate SID: '+policy)
            frame['day'] = frame.day.astype(str).str[:10]
            if not set(frame.day).issubset(days) or not frame.side.isin(('LONG', 'SHORT')).all():
                raise ValueError('Unexpected session/side: '+policy)
        if not set(book.sid).issubset(audit.sid):
            raise ValueError('Fill outside admitted inventory: '+policy)
        book[PNL] = pd.to_numeric(book[PNL], errors='raise')
        if not np.isfinite(book[PNL]).all():
            raise ValueError('Nonfinite trade PnL: '+policy)
        book.loc[book[PNL].abs().lt(TOLERANCE), PNL] = 0.
        indexed = audit.set_index('sid')
        for row in book.itertuples(index=False):
            original = indexed.loc[row.sid]
            if any(str(getattr(row, key)) != str(original[key]) for key in IDENTITY):
                raise ValueError('Filled/admitted identity mismatch: '+policy)
            if not all(np.isfinite(getattr(row, key)) and getattr(row, key) > 0 for key in ('entry_price', 'exit_price')):
                raise ValueError('Invalid entry/exit price: '+policy)
            entry, exit = pd.Timestamp(row.entry_ts), pd.Timestamp(row.exit_ts)
            if entry.tzinfo is None or exit.tzinfo is None or exit < entry:
                raise ValueError('Invalid entry/exit chronology: '+policy)
        if 'filled' in audit:
            if not audit.filled.isin((True, False)).all() or set(audit.loc[audit.filled.eq(True), 'sid']) != set(book.sid):
                raise ValueError('Order fill flags disagree: '+policy)
        else:
            audit['filled'] = audit.sid.isin(book.sid)
        clean_books[policy], clean_audits[policy] = book, audit
    baseline_audit = clean_audits[BASELINE].set_index('sid').sort_index()
    baseline_book = clean_books[BASELINE].set_index('sid')
    for policy, audit in clean_audits.items():
        current = audit.set_index('sid').sort_index()
        if not current.index.equals(baseline_audit.index):
            raise ValueError('Timing test changed admitted inventory: '+policy)
        for key in IDENTITY:
            if not current[key].astype(str).equals(baseline_audit[key].astype(str)):
                raise ValueError('Timing test changed admitted identity: '+policy+'/'+key)
        if policy == BASELINE:
            continue
        book = clean_books[policy].set_index('sid')
        for sid in baseline_book.index.intersection(book.index):
            left, right = baseline_book.loc[sid], book.loc[sid]
            same_entry = _same_clock(left.entry_ts, right.entry_ts) and _same_float(left.entry_price, right.entry_price)
            if same_entry and not _same_execution(left, right):
                raise ValueError('Same entry changed its frozen exit: '+policy)
    return clean_books, clean_audits


def attribution(books):
    """Decompose total PnL change without treating retimed fills as new stocks."""
    baseline = books[BASELINE].set_index('sid')
    details, summaries = [], []
    for policy in books:
        if policy == BASELINE:
            continue
        candidate = books[policy].set_index('sid')
        local = []
        for sid in sorted(set(baseline.index) | set(candidate.index)):
            left = baseline.loc[sid] if sid in baseline.index else None
            right = candidate.loc[sid] if sid in candidate.index else None
            identity = left if left is not None else right
            old_pnl = float(left[PNL]) if left is not None else 0.
            new_pnl = float(right[PNL]) if right is not None else 0.
            action = ('NEW_FILL' if left is None else 'MISSED_BASELINE_FILL' if right is None
                      else 'UNCHANGED_FILL' if _same_execution(left, right) else 'RETIMED_FILL')
            row = dict(policy=policy, sid=int(sid), action=action,
                **{key:identity[key] for key in IDENTITY},
                baseline_outcome=None if left is None else 'WIN' if old_pnl > 0 else 'LOSS' if old_pnl < 0 else 'FLAT',
                candidate_outcome=None if right is None else 'WIN' if new_pnl > 0 else 'LOSS' if new_pnl < 0 else 'FLAT',
                baseline_net_pnl_rs=old_pnl, candidate_net_pnl_rs=new_pnl,
                contribution_to_delta_rs=new_pnl-old_pnl)
            for prefix, source in (('baseline', left), ('candidate', right)):
                for key in ('entry_ts', 'entry_price', 'exit_ts', 'exit_price', 'exit_reason'):
                    row[prefix+'_'+key] = None if source is None else source[key]
            local.append(row)
        part = pd.DataFrame(local)
        # Both books can be empty on deliberately tiny test cohorts.
        if part.empty:
            part = pd.DataFrame(columns=['action', 'baseline_outcome', 'candidate_outcome',
                'baseline_net_pnl_rs', 'candidate_net_pnl_rs', 'contribution_to_delta_rs'])
        missing = part.loc[part.action.eq('MISSED_BASELINE_FILL')]
        new = part.loc[part.action.eq('NEW_FILL')]
        retimed = part.loc[part.action.eq('RETIMED_FILL')]
        unchanged = part.loc[part.action.eq('UNCHANGED_FILL')]
        result = dict(policy=policy, unchanged_fills=len(unchanged), retimed_fills=len(retimed),
            missed_baseline_fills=len(missing), new_fills=len(new),
            missed_winners=int(missing.baseline_outcome.eq('WIN').sum()),
            missed_losers=int(missing.baseline_outcome.eq('LOSS').sum()),
            missed_flats=int(missing.baseline_outcome.eq('FLAT').sum()),
            missed_winning_pnl_rs=float(missing.baseline_net_pnl_rs.clip(lower=0).sum()),
            avoided_losing_pnl_rs=float(-missing.baseline_net_pnl_rs.clip(upper=0).sum()),
            new_winners=int(new.candidate_outcome.eq('WIN').sum()),
            new_losers=int(new.candidate_outcome.eq('LOSS').sum()),
            new_flats=int(new.candidate_outcome.eq('FLAT').sum()),
            new_fill_net_pnl_rs=float(new.candidate_net_pnl_rs.sum()),
            retimed_delta_net_pnl_rs=float(retimed.contribution_to_delta_rs.sum()),
            retimed_winner_to_loser=int((retimed.baseline_outcome.eq('WIN') & retimed.candidate_outcome.eq('LOSS')).sum()),
            retimed_loser_to_winner=int((retimed.baseline_outcome.eq('LOSS') & retimed.candidate_outcome.eq('WIN')).sum()),
            retimed_better=int(retimed.contribution_to_delta_rs.gt(TOLERANCE).sum()),
            retimed_worse=int(retimed.contribution_to_delta_rs.lt(-TOLERANCE).sum()),
            delta_net_pnl_rs=float(candidate[PNL].sum()-baseline[PNL].sum()))
        reconstructed = (result['avoided_losing_pnl_rs']-result['missed_winning_pnl_rs']
                         +result['new_fill_net_pnl_rs']+result['retimed_delta_net_pnl_rs'])
        if (not np.isclose(reconstructed, result['delta_net_pnl_rs'], atol=1e-5, rtol=0)
                or not np.isclose(part.contribution_to_delta_rs.sum(), result['delta_net_pnl_rs'], atol=1e-5, rtol=0)):
            raise ValueError('Timing attribution does not reconcile: '+policy)
        details.extend(local)
        summaries.append(result)
    return pd.DataFrame(details), pd.DataFrame(summaries)


def _table(frame):
    lines = ['| Policy | Orders | Fills | Wins | Losses | Flat | Win rate | Net PnL Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |',
             '|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|']
    for row in frame.itertuples(index=False):
        lines.append(f'| {LABELS[row.policy]} | {row.selected_orders} | {row.trades} | {row.wins} | {row.losses} | '
            f'{row.breakeven_trades} | {row.win_rate_pct:.2f}% | {row.net_pnl_rs:,.2f} | {row.gross_loss_rs:,.2f} | '
            f'{row.average_trade_rs:,.2f} | {row.median_trade_rs:,.2f} | {row.average_daily_pnl_rs:,.2f} | '
            f'{row.median_daily_pnl_rs:,.2f} | {row.daily_close_drawdown_rs:,.2f} |')
    return lines


def publish(output, books, order_audits, days, config, coverage):
    """Write immutable new artifacts and return a JSON-ready summary."""
    if config.get('execution_authority', False) or config.get('live_changed', False):
        raise ValueError('This report requires backtest-only scope')
    if config.get('per_trade_exposure_rs', 500000) != 500000 or config.get('cost_per_trade_rs', 250) != 250:
        raise ValueError('Reporting assumptions differ from the fixed baseline')
    days = list(days)
    books, audits = validate(books, order_audits, days)
    policies = list(books)
    periods = periods_for(days)
    overall = pd.DataFrame([dict(policy=policy, period=period,
        **metric(books[policy], int(audits[policy].day.isin(cohort).sum()), cohort))
        for period, cohort in periods.items() for policy in policies])
    daily = pd.DataFrame([dict(policy=policy, day=day,
        **metric(books[policy], int(audits[policy].day.eq(day).sum()), [day]))
        for day in days for policy in policies])
    by_side = pd.DataFrame([dict(policy=policy, side=side,
        **metric(books[policy].loc[books[policy].side.eq(side)], int(audits[policy].side.eq(side).sum()), days))
        for side in ('LONG', 'SHORT') for policy in policies])
    full_key = f'full_{len(days)}'
    full = overall.loc[overall.period.eq(full_key)]
    for policy in policies:
        reference = full.loc[full.policy.eq(policy)].iloc[0]
        for part in (daily.loc[daily.policy.eq(policy)], by_side.loc[by_side.policy.eq(policy)]):
            for field in ADDITIVE:
                if not np.isclose(reference[field], part[field].sum(), atol=1e-5, rtol=0):
                    raise ValueError('Metric reconciliation failed: '+policy+'/'+field)
    changed, change_summary = attribution(books)
    reason_rows = []
    for policy, audit in audits.items():
        field = next((name for name in ('entry_status', 'status', 'reason') if name in audit), None)
        statuses = audit[field].fillna('UNSPECIFIED').astype(str) if field else audit.filled.map({True:'FILLED', False:'UNFILLED'})
        for status, count in statuses.value_counts().sort_index().items():
            reason_rows.append(dict(policy=policy, status=status, orders=int(count)))
    reasons = pd.DataFrame(reason_rows)
    base = full.loc[full.policy.eq(BASELINE)].iloc[0]
    assessments = []
    for policy in policies:
        if policy == BASELINE:
            continue
        row = full.loc[full.policy.eq(policy)].iloc[0]
        assessments.append(dict(policy=policy, delta_net_pnl_rs=float(row.net_pnl_rs-base.net_pnl_rs),
            delta_loss_amount_rs=float(row.gross_loss_rs-base.gross_loss_rs),
            delta_trades=int(row.trades-base.trades),
            delta_win_rate_points=float(row.win_rate_pct-base.win_rate_pct),
            delta_daily_close_drawdown_rs=float(row.daily_close_drawdown_rs-base.daily_close_drawdown_rs),
            full_history_profit_and_loss_improvement=bool(row.net_pnl_rs > base.net_pnl_rs+TOLERANCE
                and row.gross_loss_rs < base.gross_loss_rs-TOLERANCE),
            full_history_objective_met=bool(row.net_pnl_rs > base.net_pnl_rs+TOLERANCE
                and row.gross_loss_rs <= base.gross_loss_rs+TOLERANCE
                and row.win_rate_pct >= base.win_rate_pct-TOLERANCE
                and row.daily_close_drawdown_rs <= base.daily_close_drawdown_rs+TOLERANCE),
            no_trade_count_reduction=bool(row.trades >= base.trades)))
    summary = _json_ready(dict(version='G3-V24-RSI-ENTRY-TIMING-20261110',
        execution_authority=False, live_changed=False, selected_for_live=False, automatic_promotion=False,
        retrospective_only=True, thresholds_optimized=False, sessions=len(days), first_day=days[0], last_day=days[-1],
        policies=policies, periods=periods, overall=overall.to_dict('records'),
        side_metrics=by_side.to_dict('records'), comparisons=assessments,
        attribution=change_summary.to_dict('records'), order_status_counts=reasons.to_dict('records'),
        source_coverage=coverage, admitted_inventory_parity='PASS',
        same_entry_exit_parity='PASS', daily_and_side_reconciliation='PASS', attribution_reconciliation='PASS'))
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    files = ['overall.csv','daily.csv','by_side.csv','trade_attribution.csv','attribution_summary.csv',
             'order_status_counts.csv','REPORT.md','DAY_WISE_RESULTS.md']
    files += [policy+suffix for policy in policies for suffix in ('_trades.csv','_orders.csv')]
    if any((output/name).exists() for name in files):
        raise ValueError('Result files already exist; use a fresh result directory')
    for name, frame in [('overall',overall),('daily',daily),('by_side',by_side),
                        ('trade_attribution',changed),('attribution_summary',change_summary),('order_status_counts',reasons)]:
        frame.to_csv(output/(name+'.csv'), index=False)
    for policy in policies:
        books[policy].to_csv(output/(policy+'_trades.csv'), index=False)
        audits[policy].to_csv(output/(policy+'_orders.csv'), index=False)
    report = ['# G3 V24 RSI entry-timing experiment', '',
        f'{len(days)} available completed sessions from {days[0]} through {days[-1]}. '
        'All variants start with the exact same G3/G7-admitted stock and slot inventory. Entry timing alone varies.', '',
        'The RSI hypothesis waits for a price pause with RSI in 45–55, then a fresh completed-candle cross '
        'above 55 for LONG or below 45 for SHORT. A later candle must break the cross candle\'s directional extreme '
        'within the original entry expiry. Exact state/invalidation and any control-arm rules are recorded in config.json. '
        'These starting thresholds were fixed before the outcome comparison; they are not optimized.', '',
        'The baseline retains its existing targets, 1.50% initial stop and 80%-of-target completed-close breakeven rule. '
        'The same exit policy is rebased to each variant\'s actual entry price. Modeled exposure is Rs 500,000 and '
        'round-trip cost Rs 250 per fill with unlimited concurrent capital. Original and added slot caps remain fixed. '
        'Stops use the inherited conservative ambiguous-bar and adverse-gap rules; square-off remains 15:15.', '',
        '## Full matched history', '', *_table(full), '',
        'Loss amount is the absolute sum of negative after-cost trade PnL; flat trades do not count as wins. '
        'Daily-close drawdown starts from zero cumulative PnL and includes available zero-trade sessions. '
        'It does not measure intraday drawdown.', '', '## Direction', '']
    for side in ('LONG', 'SHORT'):
        report += [f'### {side}', '', *_table(by_side.loc[by_side.side.eq(side)]), '']
    report += ['## Chronological comparisons', '']
    for period, cohort in periods.items():
        if period == full_key:
            continue
        span = f'{cohort[0]} through {cohort[-1]}' if cohort else 'no available sessions'
        report += [f'### {period}: {span}', '', *_table(overall.loc[overall.period.eq(period)]), '']
    report += ['## What changed', '',
        '| Policy | Unchanged fills | Retimed fills | Missed winners | Missed losers | New fills | Profit missed Rs | Loss avoided Rs | Retiming PnL change Rs | New-fill net Rs | Total PnL change Rs |',
        '|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|']
    for row in change_summary.itertuples(index=False):
        report.append(f'| {LABELS[row.policy]} | {row.unchanged_fills} | {row.retimed_fills} | '
            f'{row.missed_winners} | {row.missed_losers} | {row.new_fills} | {row.missed_winning_pnl_rs:,.2f} | '
            f'{row.avoided_losing_pnl_rs:,.2f} | {row.retimed_delta_net_pnl_rs:+,.2f} | '
            f'{row.new_fill_net_pnl_rs:+,.2f} | {row.delta_net_pnl_rs:+,.2f} |')
    report += ['', 'A missed baseline fill contributes the negative of its original PnL to the comparison. '
        'A new fill contributes its entire net PnL. A retimed fill keeps the same candidate SID and contributes '
        'candidate PnL minus baseline PnL. These three effects reconcile exactly to the total change. '
        'Fewer losing fills alone is not evidence that the entry timing improved: profitable fills can also be missed.', '',
        '## Interpretation', '']
    for assessment in assessments:
        report.append(f"- **{LABELS[assessment['policy']]}**: net PnL change Rs {assessment['delta_net_pnl_rs']:+,.2f}; "
            f"loss-amount change Rs {assessment['delta_loss_amount_rs']:+,.2f}; "
            f"fill-count change {assessment['delta_trades']:+d}; "
            f"win-rate change {assessment['delta_win_rate_points']:+.2f} percentage points; "
            f"daily-close drawdown change Rs {assessment['delta_daily_close_drawdown_rs']:+,.2f}.")
    report += ['', 'All observations are retrospective. The earlier/later slices are sensitivity checks, not '
        'untouched out-of-sample evidence. This study does not promote a live policy. Delaying entries can miss '
        'uninterrupted breakouts; a higher win rate or smaller loss total can occur simply because fewer trades fill.', '',
        'Coverage remains inherited from the V24 reference: October 1 is excluded for incomplete source data and '
        'the November 8 special session uses a different schedule. October 28 retains its documented partial '
        'expanded-slot stock coverage. Older archived universe definitions are not all strictly point-in-time. '
        'Feature availability and skipped-entry reasons are included in the audit artifacts.', '',
        '[Day-wise results](DAY_WISE_RESULTS.md) | [All daily metrics](daily.csv) | '
        '[Trade attribution](trade_attribution.csv) | [Order status counts](order_status_counts.csv)', '',
        'Existing strategies, prior result artifacts, production schedules and live/paper settings remain unchanged.', '']
    (output/'REPORT.md').write_text('\n'.join(report), encoding='utf-8')
    daily_lines = ['# G3 V24 RSI timing: day-wise results', '',
        'Matched admitted candidates; filled trades may differ. Net PnL includes Rs 250 modeled cost per fill '
        'on Rs 500,000 exposure. All available dates are retained.', '',
        '| Day | '+' | '.join(f'{LABELS[policy]} fills | Net Rs' for policy in policies)+' |',
        '|:--|'+'--:|--:|'*len(policies)]
    for day in days:
        row = daily.loc[daily.day.eq(day)].set_index('policy')
        daily_lines.append('| '+day+' | '+' | '.join(f'{int(row.loc[policy,"trades"])} | {row.loc[policy,"net_pnl_rs"]:,.2f}' for policy in policies)+' |')
    totals = full.set_index('policy')
    daily_lines += ['| **Total** | '+' | '.join(f'**{int(totals.loc[policy,"trades"])}** | **{totals.loc[policy,"net_pnl_rs"]:,.2f}**' for policy in policies)+' |', '',
        'Daily totals reconcile to the full-history report. Detailed win rates, loss amounts and mean/median '
        'PnL are in [daily.csv](daily.csv).', '', '[Full report](REPORT.md)', '']
    (output/'DAY_WISE_RESULTS.md').write_text('\n'.join(daily_lines), encoding='utf-8')
    return summary
