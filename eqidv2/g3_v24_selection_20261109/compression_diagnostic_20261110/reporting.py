"""Outcome diagnostics for fixed entry-context labels; no trading decisions."""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from ..reporting import metric, _json_ready

PNL = 'portfolio_net_profit_rupees'
IDENTITY = ('day', 'setup_id', 'tradingsymbol', 'side')
BASELINE = 'ALL_BASELINE'
ADDITIVE = ('selected_orders', 'trades', 'wins', 'losses', 'breakeven_trades',
            'net_pnl_rs', 'gross_profit_rs', 'gross_loss_rs', 'costs_rs')
TOLERANCE = 1e-7
LABEL_NAMES = {
    'COMPRESSION_BREAKOUT': 'Fresh compression breakout',
    'EARLIER_BREAKOUT_CONTINUATION': 'Earlier breakout continuation',
    'BREAKOUT_WITHOUT_COMPRESSION': 'Breakout without compression',
    'OTHER_READY': 'Other ready context',
    'EARLY_SESSION_NOT_APPLICABLE': 'Early session: not applicable',
    'UNAVAILABLE': 'Unavailable evidence',
}


def periods_for(days):
    earlier = [day for day in days if day <= '2026-10-27']
    later = [day for day in days if day > '2026-10-27']
    return {f'full_{len(days)}': days, f'earlier_{len(earlier)}': earlier,
            f'later_{len(later)}': later}


def validate(orders, trades, features, days, config):
    if days != sorted(set(days)) or not days:
        raise ValueError('Sessions must be nonempty, ordered and unique')
    label_column = config.get('label_column', 'compression_label')
    primary = config.get('primary_label', 'COMPRESSION_BREAKOUT')
    unavailable = config.get('unavailable_label', 'UNAVAILABLE')
    orders, trades, features = orders.copy(), trades.copy(), features.copy()
    for name, frame in [('orders', orders), ('trades', trades), ('features', features)]:
        if not {'sid', *IDENTITY}.issubset(frame):
            raise ValueError('Missing identity fields: '+name)
        if frame.sid.isna().any() or frame.sid.duplicated().any():
            raise ValueError('Missing or duplicate SID: '+name)
        frame['day'] = frame.day.astype(str).str[:10]
        if not set(frame.day).issubset(days) or not frame.side.isin(['LONG', 'SHORT']).all():
            raise ValueError('Unexpected day or side: '+name)
    if set(features.sid) != set(orders.sid) or not set(trades.sid).issubset(orders.sid):
        raise ValueError('The complete frozen admitted inventory must be retained')
    if label_column not in features or features[label_column].isna().any():
        raise ValueError('Every admitted order needs one explicit diagnostic label')
    for name, frame in [('features', features), ('trades', trades)]:
        frozen = orders.set_index('sid').loc[frame.sid]
        for field in IDENTITY:
            if frozen[field].astype(str).tolist() != frame[field].astype(str).tolist():
                raise ValueError('Diagnostic identity mismatch: '+name+'/'+field)
    if PNL not in trades or not np.isfinite(pd.to_numeric(trades[PNL], errors='raise')).all():
        raise ValueError('Trade PnL must be finite')
    trades[PNL] = pd.to_numeric(trades[PNL], errors='raise')
    mapping = features.set_index('sid')[label_column].astype(str)
    labels = list(config.get('label_order', config.get('labels', sorted(set(mapping) | {primary, unavailable}))))
    if (len(labels) != len(set(labels)) or BASELINE in labels
            or primary not in labels or unavailable not in labels or set(mapping)-set(labels)):
        raise ValueError('Diagnostic label inventory differs from configuration')
    ready_labels = list(config.get('ready_labels', [label for label in labels if label not in (unavailable, 'EARLY_SESSION_NOT_APPLICABLE')]))
    if primary not in ready_labels or not set(ready_labels).issubset(labels) or unavailable in ready_labels:
        raise ValueError('Invalid ready-label inventory')
    if 'compression_ready' in features:
        if (not features.compression_ready.isin([True,False]).all()
                or not features.compression_ready.eq(features[label_column].isin(ready_labels)).all()):
            raise ValueError('Readiness differs from diagnostic label')
    if 'is_primary' in features and not features.is_primary.eq(features[label_column].eq(primary)).all():
        raise ValueError('Primary flag differs from diagnostic label')
    for frame in (orders, trades):
        frame['diagnostic_label'] = frame.sid.map(mapping)
    orders['filled'] = orders.sid.isin(trades.sid)
    return orders, trades, features, labels, primary, unavailable, ready_labels


def group_metric(trades, orders, dates):
    fills = trades.loc[trades.day.isin(dates)]
    selected = orders.loc[orders.day.isin(dates)]
    result = metric(fills, len(selected), dates)
    result['fill_rate_pct'] = 100*len(fills)/len(selected) if len(selected) else 0.
    result['small_sample'] = len(fills) < 30
    return result


def comparisons(trades, days, primary, unavailable, ready_labels):
    """Associations on common support, with outcome-independent weights."""
    eligible = trades.loc[trades.diagnostic_label.isin(ready_labels)].copy()
    eligible['primary_group'] = eligible.diagnostic_label.eq(primary)
    paired, strata = [], []
    for day in days:
        rows = eligible.loc[eligible.day.eq(day)]
        fresh, other = rows.loc[rows.primary_group], rows.loc[~rows.primary_group]
        if fresh.empty or other.empty:
            continue
        fp, op = fresh[PNL].to_numpy(float), other[PNL].to_numpy(float)
        paired.append(dict(day=day, primary_fills=len(fresh), comparison_fills=len(other),
            primary_mean_rs=float(fp.mean()), comparison_mean_rs=float(op.mean()),
            difference_mean_rs=float(fp.mean()-op.mean()),
            primary_win_rate_pct=float((fp>TOLERANCE).mean()*100),
            comparison_win_rate_pct=float((op>TOLERANCE).mean()*100),
            difference_win_rate_points=float(((fp>TOLERANCE).mean()-(op>TOLERANCE).mean())*100)))
    for (setup, side), rows in eligible.groupby(['setup_id', 'side'], sort=True):
        fresh, other = rows.loc[rows.primary_group], rows.loc[~rows.primary_group]
        if fresh.empty or other.empty:
            continue
        fp, op = fresh[PNL].to_numpy(float), other[PNL].to_numpy(float)
        strata.append(dict(setup_id=setup, side=side, primary_fills=len(fresh), comparison_fills=len(other),
            pooled_fills=len(rows), primary_mean_rs=float(fp.mean()), comparison_mean_rs=float(op.mean()),
            difference_mean_rs=float(fp.mean()-op.mean()),
            primary_win_rate_pct=float((fp>TOLERANCE).mean()*100),
            comparison_win_rate_pct=float((op>TOLERANCE).mean()*100),
            difference_win_rate_points=float(((fp>TOLERANCE).mean()-(op>TOLERANCE).mean())*100)))
    pair_columns = ['day','primary_fills','comparison_fills','primary_mean_rs','comparison_mean_rs',
                    'difference_mean_rs','primary_win_rate_pct','comparison_win_rate_pct','difference_win_rate_points']
    stratum_columns = ['setup_id','side','primary_fills','comparison_fills','pooled_fills',
                    'primary_mean_rs','comparison_mean_rs','difference_mean_rs',
                    'primary_win_rate_pct','comparison_win_rate_pct','difference_win_rate_points']
    paired = pd.DataFrame(paired, columns=pair_columns)
    strata = pd.DataFrame(strata, columns=stratum_columns)
    if len(strata):
        strata['weight'] = strata.pooled_fills/strata.pooled_fills.sum()
        weighted = lambda field: float((strata.weight*strata[field]).sum())
    else:
        strata['weight'] = pd.Series(dtype=float)
        weighted = lambda field: None
    fresh = eligible.loc[eligible.primary_group]
    other = eligible.loc[~eligible.primary_group]
    raw_mean = lambda frame: float(frame[PNL].mean()) if len(frame) else None
    raw_win_rate = lambda frame: float(frame[PNL].gt(TOLERANCE).mean()*100) if len(frame) else None
    summary = dict(primary_label=primary, comparison='All other ready diagnostic labels',
        ready_labels=ready_labels,
        excluded_unavailable_fills=int(trades.diagnostic_label.eq(unavailable).sum()),
        excluded_early_session_fills=int(trades.diagnostic_label.eq('EARLY_SESSION_NOT_APPLICABLE').sum()),
        excluded_nonready_fills=int((~trades.diagnostic_label.isin(ready_labels)).sum()),
        primary_fills=len(fresh), comparison_fills=len(other),
        raw_primary_mean_rs=raw_mean(fresh), raw_comparison_mean_rs=raw_mean(other),
        raw_primary_win_rate_pct=raw_win_rate(fresh), raw_comparison_win_rate_pct=raw_win_rate(other),
        raw_primary_median_rs=float(fresh[PNL].median()) if len(fresh) else None,
        raw_comparison_median_rs=float(other[PNL].median()) if len(other) else None,
        paired_days=len(paired),
        paired_primary_fills=int(paired.primary_fills.sum()), paired_comparison_fills=int(paired.comparison_fills.sum()),
        paired_equal_day_mean_difference_rs=float(paired.difference_mean_rs.mean()) if len(paired) else None,
        paired_median_day_mean_difference_rs=float(paired.difference_mean_rs.median()) if len(paired) else None,
        paired_equal_day_win_rate_difference_points=float(paired.difference_win_rate_points.mean()) if len(paired) else None,
        paired_days_primary_mean_better=int(paired.difference_mean_rs.gt(TOLERANCE).sum()),
        paired_days_primary_mean_worse=int(paired.difference_mean_rs.lt(-TOLERANCE).sum()),
        common_support_setup_side_strata=len(strata),
        common_support_primary_fills=int(strata.primary_fills.sum()),
        common_support_comparison_fills=int(strata.comparison_fills.sum()),
        standardized_primary_mean_rs=weighted('primary_mean_rs'),
        standardized_comparison_mean_rs=weighted('comparison_mean_rs'),
        standardized_mean_difference_rs=weighted('difference_mean_rs'),
        standardized_win_rate_difference_points=weighted('difference_win_rate_points'),
        standardization_weights='Pooled fill count per setup+side stratum, identical for both groups; only strata containing both groups',
        small_sample=len(fresh)<30 or len(paired)<10,
        interpretation='Descriptive associations, not causal effects; day and setup/side checks are separate, not a joint adjustment')
    return paired, strata, summary


def _table(rows, display):
    lines = ['| Context | Orders | Fills | Fill rate | Win rate | Net Rs | Loss amount Rs | Mean/fill Rs | Median/fill Rs | Mean/day Rs | Median/day Rs | Daily-close DD Rs |',
             '|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|']
    for r in rows.itertuples(index=False):
        label = display.get(r.cohort, r.cohort)
        if r.small_sample:
            label += ' (small sample)'
        lines.append(f'| {label} | {r.selected_orders} | {r.trades} | {r.fill_rate_pct:.2f}% | {r.win_rate_pct:.2f}% | '
            f'{r.net_pnl_rs:,.2f} | {r.gross_loss_rs:,.2f} | {r.average_trade_rs:,.2f} | {r.median_trade_rs:,.2f} | '
            f'{r.average_daily_pnl_rs:,.2f} | {r.median_daily_pnl_rs:,.2f} | {r.daily_close_drawdown_rs:,.2f} |')
    return lines


def _number(value, signed=False):
    return 'not estimable (no common support)' if value is None else format(value, '+,.2f' if signed else ',.2f')


def publish(output, orders, baseline_trades, features, days, config):
    orders, trades, features, labels, primary, unavailable, ready_labels = validate(orders, baseline_trades, features, days, config)
    cohorts = [BASELINE, *labels]
    display = LABEL_NAMES | dict(config.get('label_names', {}))
    display[BASELINE] = 'Unchanged baseline: all trades'
    periods = periods_for(days)
    overall_rows, side_rows, daily_rows = [], [], []
    for period, dates in periods.items():
        for label in cohorts:
            selected = orders if label==BASELINE else orders.loc[orders.diagnostic_label.eq(label)]
            filled = trades if label==BASELINE else trades.loc[trades.diagnostic_label.eq(label)]
            overall_rows.append(dict(period=period, cohort=label, **group_metric(filled, selected, dates)))
    for side in ('LONG','SHORT'):
        for label in cohorts:
            selected, filled = orders.loc[orders.side.eq(side)], trades.loc[trades.side.eq(side)]
            if label != BASELINE:
                selected, filled = selected.loc[selected.diagnostic_label.eq(label)], filled.loc[filled.diagnostic_label.eq(label)]
            side_rows.append(dict(side=side, cohort=label, **group_metric(filled, selected, days)))
    for day in days:
        for label in cohorts:
            selected, filled = orders.loc[orders.day.eq(day)], trades.loc[trades.day.eq(day)]
            if label != BASELINE:
                selected, filled = selected.loc[selected.diagnostic_label.eq(label)], filled.loc[filled.diagnostic_label.eq(label)]
            daily_rows.append(dict(day=day, cohort=label, **group_metric(filled, selected, [day])))
    overall, by_side, daily = map(pd.DataFrame, (overall_rows, side_rows, daily_rows))
    for period in periods:
        frame = overall.loc[overall.period.eq(period)].set_index('cohort')
        for field in ADDITIVE:
            if not np.isclose(frame.loc[BASELINE, field], frame.loc[labels, field].sum(), atol=1e-5, rtol=0):
                raise ValueError('Context partition does not reconcile: '+period+'/'+field)
    for day in days:
        frame = daily.loc[daily.day.eq(day)].set_index('cohort')
        for field in ADDITIVE:
            if not np.isclose(frame.loc[BASELINE, field], frame.loc[labels, field].sum(), atol=1e-5, rtol=0):
                raise ValueError('Daily context partition does not reconcile: '+day+'/'+field)
    full_key = f'full_{len(days)}'
    full = overall.loc[overall.period.eq(full_key)]
    for label in cohorts:
        row = full.loc[full.cohort.eq(label)].iloc[0]
        for field in ADDITIVE:
            if not np.isclose(row[field], daily.loc[daily.cohort.eq(label), field].sum(), atol=1e-5, rtol=0):
                raise ValueError('Daily totals do not reconcile: '+label+'/'+field)
    paired_parts, stratum_parts, comparison_rows = [], [], []
    for period, dates in periods.items():
        paired, strata, summary = comparisons(trades.loc[trades.day.isin(dates)], dates, primary, unavailable, ready_labels)
        paired['period'] = period
        strata['period'] = period
        paired_parts.append(paired); stratum_parts.append(strata)
        comparison_rows.append(dict(period=period, **summary))
    paired = pd.concat(paired_parts, ignore_index=True)
    strata = pd.concat(stratum_parts, ignore_index=True)
    comparison_frame = pd.DataFrame(comparison_rows)
    primary_trades = trades.loc[trades.diagnostic_label.eq(primary)]
    excluded = trades.loc[~trades.diagnostic_label.eq(primary)]
    primary_only = dict(applied=False, primary_label=primary,
        retained_fills=len(primary_trades), discarded_fills=len(excluded),
        retained_fill_share_pct=100*len(primary_trades)/len(trades) if len(trades) else 0.,
        retained_net_pnl_rs=float(primary_trades[PNL].sum()),
        foregone_winners=int(excluded[PNL].gt(TOLERANCE).sum()),
        foregone_flats=int(excluded[PNL].abs().le(TOLERANCE).sum()),
        avoided_losers=int(excluded[PNL].lt(-TOLERANCE).sum()),
        foregone_winning_pnl_rs=float(excluded[PNL].clip(lower=0).sum()),
        avoided_losing_pnl_rs=float(-excluded[PNL].clip(upper=0).sum()),
        discarded_net_pnl_rs=float(excluded[PNL].sum()),
        interpretation='Descriptive subset only: no actual filter, new fills, or ranking changes were applied')
    if not np.isclose(primary_only['retained_net_pnl_rs']+primary_only['discarded_net_pnl_rs'], trades[PNL].sum(), atol=1e-5, rtol=0):
        raise ValueError('Primary-only illustration does not reconcile')
    baseline_row = full.loc[full.cohort.eq(BASELINE)].iloc[0]
    primary_row = full.loc[full.cohort.eq(primary)].iloc[0]
    summary = _json_ready(dict(version='G3-V24-COMPRESSION-DIAGNOSTIC-20261110',
        execution_authority=False, live_changed=False, baseline_entries_changed=False,
        automatic_promotion=False, diagnostic_only=True, retrospective_only=True,
        sessions=len(days), first_day=days[0], last_day=days[-1],
        admitted_orders=len(orders), filled_trades=len(trades), primary_label=primary,
        labels=labels, ready_labels=ready_labels, periods=periods, baseline_metrics=baseline_row.to_dict(),
        primary_metrics=primary_row.to_dict(), overall=overall.to_dict('records'),
        comparisons=comparison_rows, primary_only_illustration=primary_only,
        daily_reconciliation='PASS', cohort_partition_reconciliation='PASS', inventory_parity='PASS'))
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    frames = dict(overall=overall, by_side=by_side, daily=daily,
        paired_day_comparisons=paired, setup_side_standardization=strata,
        comparison_summary=comparison_frame, labeled_orders=orders, labeled_trades=trades)
    names = [name+'.csv' for name in frames] + ['REPORT.md','DAY_WISE_RESULTS.md']
    if any((output/name).exists() for name in names):
        raise ValueError('Diagnostic results already exist; use a fresh directory')
    for name, frame in frames.items():
        frame.to_csv(output/(name+'.csv'), index=False)
    report = ['# G3 V24 compression and breakout diagnostic', '',
        f'{len(days)} completed available sessions, {days[0]} through {days[-1]}. '
        f'All {len(orders)} admitted opportunities and {len(trades)} filled baseline trades are retained. '
        '**This study labels existing trades; it does not change selection, entries, exits, capital or PnL.**', '',
        'Labels use observations available by the original G3 confirmation. One-minute Bollinger BandWidth uses '
        '20 consecutive same-session closes and two population standard deviations. Compression compares the '
        'preceding bar with the twentieth percentile of its preceding 100 valid BandWidth observations; that '
        'reference excludes the observation being classified and may contain prior sessions. The primary label '
        'requires preceding-bar compression, a completed current close beyond the preceding five-bar high/low '
        'in G3 direction, and increasing BandWidth. Exact earlier-breakout and range-anchor rules are in config.json. '
        'A full context needs 25 same-session bars. Earlier slots are explicitly not applicable, rather than '
        'being treated as failed or late breakouts. Unavailable evidence is separately labeled. '
        'Neither condition deletes a baseline trade. No thresholds were optimized.', '',
        'Existing G3 volume confirmation is already satisfied by the frozen admitted inventory. It is held '
        'constant here; this study does not isolate an incremental benefit from volume or add another volume gate.', '',
        'Modeled exposure remains Rs 500,000 per fill with Rs 250 round-trip cost and unlimited concurrent capital. '
        'The V23 targets, 1.50% initial stop, 80%-of-target completed-close breakeven protection, '
        'slot caps, G7 confirmations and 15:15 exit remain unchanged.', '',
        '## Full history by context', '', *_table(full, display), '',
        'Context rows partition the baseline; their order, trade, win/loss and net-PnL totals reconcile exactly. '
        'Loss amount sums negative after-cost trade PnL. Flat fills do not count as wins. '
        'Daily-close drawdown uses zero initial cumulative PnL and every available date; smaller context-group '
        'drawdown may merely reflect fewer trades and is not a strategy improvement. '
        'A small-sample label means fewer than 30 fills.', '', '## Direction', '']
    for side in ('LONG','SHORT'):
        report += [f'### {side}', '', *_table(by_side.loc[by_side.side.eq(side)], display), '']
    report += ['## Chronological checks', '']
    for period, dates in periods.items():
        if period == full_key:
            continue
        span = f'{dates[0]} through {dates[-1]}' if dates else 'no sessions'
        report += [f'### {period}: {span}', '', *_table(overall.loc[overall.period.eq(period)], display), '']
    report += ['## Comparisons on shared context', '',
        f'The primary group is **{display.get(primary, primary)}**. Its comparison group contains all other '
        'ready labels. Early-session not-applicable and unavailable evidence are both excluded from these '
        'comparisons only; their trades remain in baseline totals.', '']
    for row in comparison_rows:
        report += [f"### {row['period']}", '',
            f"Primary fills: {row['primary_fills']}; other available fills: {row['comparison_fills']}. "
            f"Days containing both: {row['paired_days']}.", '',
            f"Unadjusted mean PnL per fill: primary Rs {_number(row['raw_primary_mean_rs'])}, "
            f"other ready Rs {_number(row['raw_comparison_mean_rs'])}. Win rates: "
            f"{_number(row['raw_primary_win_rate_pct'])}% and {_number(row['raw_comparison_win_rate_pct'])}% respectively.", '',
            f"Equal-day average difference in mean PnL per fill: **Rs {_number(row['paired_equal_day_mean_difference_rs'], True)}**. "
            f"Equal-day win-rate difference: {_number(row['paired_equal_day_win_rate_difference_points'], True)} percentage points.", '',
            f"Common setup/side strata: {row['common_support_setup_side_strata']}, covering "
            f"{row['common_support_primary_fills']} primary and {row['common_support_comparison_fills']} comparison fills. "
            f"Standardized mean-PnL difference: **Rs {_number(row['standardized_mean_difference_rs'], True)}**; "
            f"win-rate difference: {_number(row['standardized_win_rate_difference_points'], True)} percentage points.", '',
            ('**Small sample / limited overlap:** fewer than 30 primary fills or fewer than 10 paired days; treat the differences as descriptive.'
             if row['small_sample'] else 'These are descriptive differences; sample size alone does not establish statistical reliability.'), '']
    report += ['Both groups receive identical pooled-fill weights within each common setup/side stratum. '
        'Weights depend on counts, not outcomes. Day pairing and setup/side standardization are separate checks; '
        'they do not jointly remove all day, stock, side, slot or market-regime confounding. No causal benefit or '
        'statistical significance is claimed.', '', '## If the primary label alone were required', '',
        '**This was not applied.** The descriptive subset would retain '
        f"{primary_only['retained_fills']} fills ({primary_only['retained_fill_share_pct']:.2f}%) and Rs "
        f"{primary_only['retained_net_pnl_rs']:,.2f} net PnL. It would omit {primary_only['foregone_winners']} "
        f"winning and {primary_only['avoided_losers']} losing baseline fills, forgoing Rs "
        f"{primary_only['foregone_winning_pnl_rs']:,.2f} winning PnL while avoiding Rs "
        f"{primary_only['avoided_losing_pnl_rs']:,.2f} losing PnL. This illustrates the cost of a mandatory filter; "
        'it is not a new strategy result or a recommendation.', '',
        '## Interpretation and scope', '',
        f"The unchanged baseline remains {len(trades)} fills and Rs {baseline_row.net_pnl_rs:,.2f} net PnL. "
        'Any difference between label groups is an association within that fixed book. Check direction, periods, '
        'common-support comparisons and sample sizes before treating the label as useful entry information.', '',
        'All observations are retrospective and the source history has been used in earlier research. '
        'The earlier/later slices are sensitivity checks, not untouched out-of-sample evidence. '
        'No label is promoted to a live filter or ranking change.', '',
        'Source limitations remain inherited: October 1 is excluded, the November 8 special session uses a different '
        'schedule, October 28 has partial expanded-slot stock coverage, and older archived universes are not all '
        'strictly point-in-time. Existing versions, source artifacts and live/paper settings remain unchanged.', '',
        '[Day-wise results](DAY_WISE_RESULTS.md) | [Every labeled trade](labeled_trades.csv) | '
        '[All admitted orders](labeled_orders.csv) | [Matched-context comparisons](comparison_summary.csv)', '']
    (output/'REPORT.md').write_text('\n'.join(report), encoding='utf-8')
    daily_lines = ['# G3 V24 compression diagnostic: day-wise results', '',
        'Every baseline trade remains. Context columns are contributions to the same baseline PnL, not alternative strategies. '
        'Net PnL includes Rs 250 cost per fill on Rs 500,000 modeled exposure.', '',
        '| Day | Baseline fills | Baseline net Rs | '+' | '.join(display.get(label,label)+' net Rs' for label in labels)+' |',
        '|:--|--:|--:|'+'--:|'*len(labels)]
    for day in days:
        rows = daily.loc[daily.day.eq(day)].set_index('cohort')
        daily_lines.append(f'| {day} | {int(rows.loc[BASELINE,"trades"])} | {rows.loc[BASELINE,"net_pnl_rs"]:,.2f} | '+
            ' | '.join(f'{rows.loc[label,"net_pnl_rs"]:,.2f}' for label in labels)+' |')
    totals = full.set_index('cohort')
    daily_lines += [f'| **Total** | **{int(totals.loc[BASELINE,"trades"])}** | **{totals.loc[BASELINE,"net_pnl_rs"]:,.2f}** | '+
        ' | '.join(f'**{totals.loc[label,"net_pnl_rs"]:,.2f}**' for label in labels)+' |', '',
        'All label contributions reconcile to baseline every day. Full label counts, win rates and loss amounts '
        'are in [daily.csv](daily.csv). No entry filter was applied.', '', '[Full report](REPORT.md)', '']
    (output/'DAY_WISE_RESULTS.md').write_text('\n'.join(daily_lines), encoding='utf-8')
    return summary
