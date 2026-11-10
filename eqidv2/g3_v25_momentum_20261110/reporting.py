"""Retrospective V25 trade/mover attribution; never supplies strategy inputs."""
from __future__ import annotations

from pathlib import Path
import json
import re

import numpy as np
import pandas as pd

from g3_v24_selection_20261109.reporting import metric, _json_ready, ADDITIVE

BASELINE = 'baseline_v24'
PNL = 'portfolio_net_profit_rupees'
KEY = ['day', 'tradingsymbol', 'side']
TIMEZONE = 'Asia/Kolkata'
TOLERANCE = 1e-7


def periods_for(days):
    return {'full_history': list(days), 'fit_44': list(days[:44]),
            'validation_15': list(days[44:59]), 'later_9': list(days[59:])}


def _booleans(values, name):
    normalized = values.astype(str).str.lower().str.strip()
    if not normalized.isin(['true', 'false', '1', '0', '1.0', '0.0']).all():
        raise ValueError('Invalid boolean values: '+name)
    return normalized.isin(['true', '1', '1.0'])


def _timestamps(values, name, allow_missing=False):
    for value in values.dropna():
        if pd.Timestamp(value).tzinfo is None:
            raise ValueError('Naive timestamp: '+name)
    result = pd.to_datetime(values, utc=True)
    if not allow_missing and result.isna().any():
        raise ValueError('Missing timestamp: '+name)
    return result


def validate(books, orders, movers, days):
    if not days or days != sorted(set(days)):
        raise ValueError('Dates must be nonempty, unique and ordered')
    if BASELINE not in books or set(books) != set(orders):
        raise ValueError('Baseline and matching trade/order arms are required')
    if any(not re.fullmatch(r'[A-Za-z0-9_-]+', name) for name in books):
        raise ValueError('Unsafe arm name')
    mover_fields = {*KEY, 'top2', 'top5', 'move_pct', 'first3pct_ts',
                    'extreme_ts', 'extreme_price', 'first_open'}
    if not mover_fields.issubset(movers):
        raise ValueError('Missing mover fields: '+str(sorted(mover_fields-set(movers))))
    movers = movers.copy()
    movers['day'] = movers.day.astype(str).str[:10]
    if movers.duplicated(KEY).any() or not set(movers.day).issubset(days):
        raise ValueError('Duplicate or out-of-history mover identity')
    if not movers.side.isin(['LONG', 'SHORT']).all():
        raise ValueError('Invalid mover direction')
    for name in ['top2', 'top5']:
        movers[name] = _booleans(movers[name], name)
    if (movers.top2 & ~movers.top5).any():
        raise ValueError('Top-two cases must be top-five cases')
    for name in ['move_pct', 'extreme_price', 'first_open']:
        movers[name] = pd.to_numeric(movers[name], errors='raise')
        if not np.isfinite(movers[name]).all():
            raise ValueError('Nonfinite mover value: '+name)
    if movers[['extreme_price', 'first_open']].le(0).any().any():
        raise ValueError('Mover prices must be positive')
    if (movers.top5 & movers.move_pct.abs().lt(3-TOLERANCE)).any():
        raise ValueError('Top-five case does not meet the 3% threshold')
    movers['extreme_ts'] = _timestamps(movers.extreme_ts, 'extreme_ts')
    movers['first3pct_ts'] = _timestamps(movers.first3pct_ts, 'first3pct_ts', True)
    if movers.loc[movers.top5, 'first3pct_ts'].isna().any():
        raise ValueError('Qualifying mover has no first 3% crossing')
    if not movers.extreme_ts.dt.tz_convert(TIMEZONE).dt.strftime('%Y-%m-%d').eq(movers.day).all():
        raise ValueError('Mover extreme does not belong to its session')
    if 'rank' not in movers:
        ranked = movers.assign(_absolute=movers.move_pct.abs()).sort_values(
            ['day', 'side', '_absolute', 'tradingsymbol'], ascending=[True, True, False, True], kind='stable')
        movers['rank'] = ranked.groupby(['day', 'side']).cumcount().add(1).reindex(movers.index)
    clean_books, clean_orders = {}, {}
    for name in books:
        book, selected = books[name].copy(), orders[name].copy()
        required = {'sid', *KEY, 'entry_ts', 'entry_price', PNL}
        if not required.issubset(book) or not {'sid', *KEY}.issubset(selected):
            raise ValueError('Missing trade/order fields: '+name)
        for frame in (book, selected):
            frame['day'] = frame.day.astype(str).str[:10]
            if frame.sid.isna().any() or frame.sid.duplicated().any():
                raise ValueError('Duplicate or missing SID: '+name)
            if not set(frame.day).issubset(days) or not frame.side.isin(['LONG', 'SHORT']).all():
                raise ValueError('Invalid session/direction: '+name)
        if not set(book.sid).issubset(selected.sid):
            raise ValueError('Filled trade was not admitted: '+name)
        joined = book[['sid', *KEY]].merge(selected[['sid', *KEY]], on='sid',
                                          suffixes=('_trade', '_order'), validate='one_to_one')
        for field in KEY:
            if not joined[field+'_trade'].astype(str).equals(joined[field+'_order'].astype(str)):
                raise ValueError('Trade/order identity mismatch: '+name+'/'+field)
        book['entry_ts'] = _timestamps(book.entry_ts, name+'/entry_ts')
        if not book.entry_ts.dt.tz_convert(TIMEZONE).dt.strftime('%Y-%m-%d').eq(book.day).all():
            raise ValueError('Entry does not belong to its session: '+name)
        for field in [PNL, 'entry_price']:
            book[field] = pd.to_numeric(book[field], errors='raise')
            if not np.isfinite(book[field]).all():
                raise ValueError('Nonfinite trade field: '+name+'/'+field)
        if book.entry_price.le(0).any():
            raise ValueError('Entry price must be positive')
        book.loc[book[PNL].abs().lt(TOLERANCE), PNL] = 0.
        clean_books[name], clean_orders[name] = book, selected
    return clean_books, clean_orders, movers


def annotate_trades(book, movers):
    """Join full-day outcomes only after strategy decisions and fills are fixed."""
    fields = [*KEY, 'top2', 'top5', 'move_pct', 'first3pct_ts', 'extreme_ts',
              'extreme_price', 'first_open', 'rank']
    collisions = (set(fields)-set(KEY)) & set(book)
    if collisions:
        raise ValueError('Trade book already contains hindsight mover fields: '+str(sorted(collisions)))
    result = book.merge(movers[fields], on=KEY, how='left', validate='many_to_one', indicator=True)
    if not result._merge.eq('both').all():
        raise ValueError('Filled trade is missing full-universe mover evidence')
    result = result.drop(columns='_merge')
    sign = np.where(result.side.eq('LONG'), 1., -1.)
    result['entry_before_3pct'] = result.first3pct_ts.notna() & result.entry_ts.lt(result.first3pct_ts)
    result['entry_before_final_extreme'] = result.entry_ts.lt(result.extreme_ts)
    result['entry_before_3pct_conservative'] = result.entry_before_3pct
    result['entry_before_final_extreme_conservative'] = result.entry_before_final_extreme
    result['entry_timing_ts'] = result.entry_ts
    result['entry_clock_basis'] = 'BASELINE_ENTRY_MINUTE_END_CONSERVATIVE'
    if 'entry_execution_ts' in result:
        known = result.entry_execution_ts.notna()
        if known.any():
            actual = _timestamps(result.loc[known, 'entry_execution_ts'], 'entry_execution_ts')
            ends = (_timestamps(result.loc[known, 'entry_bar_end_ts'], 'entry_bar_end_ts')
                    if 'entry_bar_end_ts' in result else result.loc[known, 'entry_ts'])
            if not actual.add(pd.Timedelta(minutes=1)).eq(ends).all():
                raise ValueError('Known next-open time differs from its entry bar')
            if 'raw_entry_open' not in result:
                raise ValueError('Known next-open chronology needs raw entry open')
            opening = pd.to_numeric(result.loc[known, 'raw_entry_open'], errors='raise')
            if not np.isfinite(opening).all() or opening.le(0).any():
                raise ValueError('Known next-open chronology needs a valid raw opening price')
            side_sign = pd.Series(sign, index=result.index).loc[known]
            first3 = result.loc[known, 'first3pct_ts']
            extreme = result.loc[known, 'extreme_ts']
            threshold = result.loc[known, 'first_open']*(1+side_sign*.03)
            # A crossing/extreme in the same end-labeled minute follows the
            # known open only if price was still strictly short of that level.
            # A gap already at/beyond3% is not an entry before the3% crossing.
            before3 = first3.notna() & (ends.lt(first3)
                | (ends.eq(first3) & (side_sign*(threshold-opening)).gt(1e-10)))
            before_extreme = ends.lt(extreme) | (ends.eq(extreme)
                & (side_sign*(result.loc[known, 'extreme_price']-opening)).gt(1e-10))
            result.loc[known, 'entry_before_3pct'] = before3
            result.loc[known, 'entry_before_final_extreme'] = before_extreme
            result.loc[known, 'entry_timing_ts'] = actual
            result.loc[known, 'entry_clock_basis'] = 'KNOWN_NEXT_OPEN'
    # Distance is arithmetic distance to the day's extreme, not an executable
    # payoff. It represents remaining room only when entry precedes the extreme.
    result['distance_to_day_extreme_pct'] = sign*(result.extreme_price/result.entry_price-1)*100
    result['remaining_to_final_extreme_pct'] = result.distance_to_day_extreme_pct.where(
        result.entry_before_final_extreme, np.nan)
    result['retrospective_near_extreme'] = (~result.entry_before_final_extreme
        | result.distance_to_day_extreme_pct.lt(.30-TOLERANCE))
    result['retrospective_near_extreme_conservative'] = (~result.entry_before_final_extreme_conservative
        | result.distance_to_day_extreme_pct.lt(.30-TOLERANCE))
    result['useful_capture'] = (result.entry_before_final_extreme
        & result.distance_to_day_extreme_pct.ge(.80-TOLERANCE) & result[PNL].gt(0))
    result['useful_capture_conservative'] = (result.entry_before_final_extreme_conservative
        & result.distance_to_day_extreme_pct.ge(.80-TOLERANCE) & result[PNL].gt(0))
    result['mover_context'] = np.where(result.top5, 'TOP5', 'NON_TOP5')
    result['outcome'] = np.where(result[PNL].gt(0), 'WIN', np.where(result[PNL].lt(0), 'LOSS', 'FLAT'))
    return result


def _ratio(numerator, denominator):
    return 100.*numerator/denominator if denominator else None


def mover_metrics(annotated, movers, days):
    part = annotated.loc[annotated.day.isin(days)]
    universe = movers.loc[movers.day.isin(days)]
    unique = part.groupby(KEY, sort=False).agg(top2=('top2', 'max'), top5=('top5', 'max'),
        before3=('entry_before_3pct', 'max'), before_extreme=('entry_before_final_extreme', 'max'),
        useful=('useful_capture', 'max'), before3_conservative=('entry_before_3pct_conservative', 'max'),
        before_extreme_conservative=('entry_before_final_extreme_conservative', 'max'),
        useful_conservative=('useful_capture_conservative', 'max')).reset_index()
    values = dict(traded_stock_day_side_cases=len(unique),
        top5_traded_cases=int(unique.top5.sum()),
        top5_precision_pct=_ratio(int(unique.top5.sum()), len(unique)),
        retrospective_near_extreme_trades=int(part.retrospective_near_extreme.sum()),
        retrospective_near_extreme_pct=_ratio(int(part.retrospective_near_extreme.sum()), len(part)),
        retrospective_near_extreme_conservative_trades=int(part.retrospective_near_extreme_conservative.sum()),
        retrospective_near_extreme_conservative_pct=_ratio(int(part.retrospective_near_extreme_conservative.sum()), len(part)),
        non_top5_trades=int((~part.top5).sum()),
        non_top5_winners=int((~part.top5 & part[PNL].gt(0)).sum()),
        non_top5_losers=int((~part.top5 & part[PNL].lt(0)).sum()),
        non_top5_net_pnl_rs=float(part.loc[~part.top5, PNL].sum()),
        non_top5_winning_pnl_rs=float(part.loc[~part.top5, PNL].clip(lower=0).sum()),
        non_top5_losing_pnl_rs=float(-part.loc[~part.top5, PNL].clip(upper=0).sum()))
    for label in ['top2', 'top5']:
        total = int(universe[label].sum())
        group = unique.loc[unique[label]]
        values.update({label+'_cases': total, label+'_captured_cases': len(group),
            label+'_capture_pct': _ratio(len(group), total),
            label+'_before3_cases': int(group.before3.sum()),
            label+'_before3_pct': _ratio(int(group.before3.sum()), total),
            label+'_before_extreme_cases': int(group.before_extreme.sum()),
            label+'_before_extreme_pct': _ratio(int(group.before_extreme.sum()), total),
            label+'_useful_cases': int(group.useful.sum()),
            label+'_useful_capture_pct': _ratio(int(group.useful.sum()), total),
            label+'_before3_conservative_cases': int(group.before3_conservative.sum()),
            label+'_before3_conservative_pct': _ratio(int(group.before3_conservative.sum()), total),
            label+'_before_extreme_conservative_cases': int(group.before_extreme_conservative.sum()),
            label+'_before_extreme_conservative_pct': _ratio(int(group.before_extreme_conservative.sum()), total),
            label+'_useful_conservative_cases': int(group.useful_conservative.sum()),
            label+'_useful_capture_conservative_pct': _ratio(int(group.useful_conservative.sum()), total)})
    return values


def _case_summary(book, prefix):
    if book.empty:
        return pd.DataFrame(columns=[*KEY, prefix+'_first_entry_ts', prefix+'_fills', prefix+'_net_pnl_rs'])
    clock = 'entry_timing_ts' if 'entry_timing_ts' in book else 'entry_ts'
    return book.groupby(KEY, sort=False).agg(**{
        prefix+'_first_entry_ts': (clock, 'min'), prefix+'_fills': ('sid', 'size'),
        prefix+'_net_pnl_rs': (PNL, 'sum')}).reset_index()


def _clock(value):
    return '-' if pd.isna(value) else pd.Timestamp(value).tz_convert(TIMEZONE).strftime('%H:%M')


def _number(value, precision=2):
    return 'n/a' if value is None or pd.isna(value) else f'{value:,.{precision}f}'


def _safe(value):
    return str(value).replace('|', '/').replace('\n', ' ')


def _performance_table(frame):
    lines = ['| Arm | Orders | Fills | Win rate | Net Rs | Losing PnL Rs | Mean/fill Rs | Median/fill Rs | Profit factor | Daily-close DD Rs |',
             '|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|']
    for row in frame.itertuples(index=False):
        lines.append(f'| {_safe(row.arm)} | {row.selected_orders} | {row.trades} | {row.win_rate_pct:.2f}% | '
            f'{row.net_pnl_rs:,.2f} | {row.gross_loss_rs:,.2f} | {row.average_trade_rs:,.2f} | '
            f'{row.median_trade_rs:,.2f} | {_number(row.profit_factor)} | {row.daily_close_drawdown_rs:,.2f} |')
    return lines


def _mover_table(frame):
    lines = ['| Arm | Top5 cases filled | Top5 precision | Before first 3% (common clock) | Useful top5 capture (common clock) | Top2 filled | Near-extreme entries (common clock) | Non-top5 net Rs |',
             '|:--|--:|--:|--:|--:|--:|--:|--:|']
    for row in frame.itertuples(index=False):
        lines.append(f'| {_safe(row.arm)} | {row.top5_captured_cases}/{row.top5_cases} | '
            f'{_number(row.top5_precision_pct)}% | {row.top5_before3_conservative_cases}/{row.top5_cases} | '
            f'{row.top5_useful_conservative_cases}/{row.top5_cases} | {row.top2_captured_cases}/{row.top2_cases} | '
            f'{row.retrospective_near_extreme_conservative_trades} | {row.non_top5_net_pnl_rs:,.2f} |')
    return lines


def _chosen_name(selection, books):
    # This function only displays the caller's already frozen decision. It
    # never selects an arm using full-history or later-period PnL.
    validated = selection.get('validation_chosen')
    exploratory = selection.get('exploratory_best')
    for value in [validated, exploratory]:
        if value is not None and value not in books:
            raise ValueError('Selection references unknown arm: '+str(value))
    return validated or exploratory or BASELINE


def publish(output, books, orders, decisions, movers, days, config, selection):
    """Publish fixed-arm research without changing any entry or selecting a model."""
    if config.get('execution_authority', False) or config.get('live_changed', False):
        raise ValueError('Research reporting cannot grant trading authority')
    days = list(days)
    books, orders, movers = validate(books, orders, movers, days)
    if set(decisions)-set(books):
        raise ValueError('Decisions contain an unknown arm')
    chosen = _chosen_name(selection, books)
    periods = periods_for(days)
    annotated = {name: annotate_trades(book, movers) for name, book in books.items()}
    overall = pd.DataFrame([dict(arm=name, period=period,
        **metric(book, int(orders[name].day.isin(cohort).sum()), cohort))
        for period, cohort in periods.items() for name, book in books.items()])
    daily = pd.DataFrame([dict(arm=name, day=day,
        **metric(book, int(orders[name].day.eq(day).sum()), [day]))
        for day in days for name, book in books.items()])
    by_side = pd.DataFrame([dict(arm=name, period=period, side=side,
        **metric(book.loc[book.side.eq(side)], int((orders[name].day.isin(cohort)
          & orders[name].side.eq(side)).sum()), cohort))
        for period, cohort in periods.items() for name, book in books.items() for side in ['LONG', 'SHORT']])
    mover_summary = pd.DataFrame([dict(arm=name, period=period,
        **mover_metrics(book, movers, cohort))
        for period, cohort in periods.items() for name, book in annotated.items()])
    mover_side = pd.DataFrame([dict(arm=name, period=period, side=side,
        **mover_metrics(book.loc[book.side.eq(side)], movers.loc[movers.side.eq(side)], cohort))
        for period, cohort in periods.items() for name, book in annotated.items() for side in ['LONG', 'SHORT']])
    group_rows = []
    for period, cohort in periods.items():
        for name, book in annotated.items():
            for field, labels in [('mover_context', ['TOP5', 'NON_TOP5']),
                                  ('retrospective_near_extreme', [True, False])]:
                for label in labels:
                    part = book.loc[book[field].eq(label)]
                    group_rows.append(dict(arm=name, period=period, grouping=field, group=str(label),
                        **metric(part, int(part.day.isin(cohort).sum()), cohort)))
    contexts = pd.DataFrame(group_rows)
    for name in books:
        full = overall.loc[overall.arm.eq(name) & overall.period.eq('full_history')].iloc[0]
        part = daily.loc[daily.arm.eq(name)]
        for field in ADDITIVE:
            if not np.isclose(full[field], part[field].sum(), atol=1e-5, rtol=0):
                raise ValueError('Daily reconciliation failed: '+name+'/'+field)
        for grouping in contexts.grouping.unique():
            group = contexts.loc[contexts.arm.eq(name) & contexts.period.eq('full_history')
                & contexts.grouping.eq(grouping)]
            for field in [key for key in ADDITIVE if key != 'selected_orders']:
                if not np.isclose(full[field], group[field].sum(), atol=1e-5, rtol=0):
                    raise ValueError('Context partition failed: '+name+'/'+field)
    catalogue = movers.loc[movers.top5].copy()
    catalogue = catalogue.merge(_case_summary(annotated[BASELINE], 'baseline'), on=KEY, how='left', validate='one_to_one')
    catalogue = catalogue.merge(_case_summary(annotated[chosen], 'comparison'), on=KEY, how='left', validate='one_to_one')
    for prefix in ['baseline', 'comparison']:
        catalogue[prefix+'_fills'] = catalogue[prefix+'_fills'].fillna(0).astype(int)
        catalogue[prefix+'_net_pnl_rs'] = catalogue[prefix+'_net_pnl_rs'].fillna(0.)
    admitted = set(map(tuple, orders[chosen][KEY].to_numpy()))
    catalogue['comparison_status'] = ['FILLED' if count else 'ADMITTED_BUT_UNFILLED' if tuple(key) in admitted
        else 'NOT_ADMITTED_SEE_MISSED_CASE_AUDIT'
        for key, count in zip(catalogue[KEY].to_numpy(), catalogue.comparison_fills)]
    catalogue['comparison_arm'] = chosen
    catalogue = catalogue.sort_values(['day', 'side', 'rank', 'tradingsymbol'], kind='stable')
    summary = _json_ready(dict(version='G3-V25-MOMENTUM-20261110', execution_authority=False,
        automatic_promotion=False, selected_for_live=False, retrospective_only=True, live_changed=False,
        sessions=len(days), first_day=days[0], last_day=days[-1], periods=periods, selection=selection,
        displayed_comparison_arm=chosen, baseline_metrics=overall.loc[overall.arm.eq(BASELINE)
            & overall.period.eq('full_history')].iloc[0].to_dict(),
        overall=overall.to_dict('records'), mover_comparisons=mover_summary.to_dict('records'),
        daily_reconciliation='PASS', context_partition_reconciliation='PASS',
        hindsight_mover_labels_used_in_selection=False,
        useful_capture_definition='Unique top-mover stock/day/side with an entry before the final extreme, at least 0.8% distance remaining to that extreme and positive realized after-cost PnL',
        near_extreme_definition='Retrospective only: entry not strictly before the final extreme, or directional distance to it below 0.3%',
        top5_precision_denominator='Unique traded stock/day/side cases, not repeated fills',
        common_clock_comparison='*_conservative_* metrics use entry-minute end labels for every arm; no same-bar timing credit even for a known next-open fill',
        comparison_caution='Previously examined historical dates; later9 is a fixed chronological check, not untouched prospective evidence'))
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    names = ['overall.csv', 'daily.csv', 'by_side.csv', 'mover_summary.csv', 'mover_by_side.csv',
             'trade_contexts.csv', 'movers_all_stock_days.csv', 'top5_daily_cases.csv',
             'REPORT.md', 'DAY_WISE_RESULTS.md', 'FULL_HISTORY_DAILY_TOP5_MOVERS.md']
    names += [name+suffix for name in books for suffix in ['_trades.csv', '_orders.csv', '_trade_mover_audit.csv']]
    names += [name+'_decisions.csv' for name in decisions]
    if any((output/name).exists() for name in names):
        raise ValueError('Reports already exist; use a fresh output directory')
    for filename, frame in [('overall.csv', overall), ('daily.csv', daily), ('by_side.csv', by_side),
        ('mover_summary.csv', mover_summary), ('mover_by_side.csv', mover_side), ('trade_contexts.csv', contexts),
        ('movers_all_stock_days.csv', movers), ('top5_daily_cases.csv', catalogue)]:
        frame.to_csv(output/filename, index=False)
    for name in books:
        books[name].to_csv(output/(name+'_trades.csv'), index=False)
        orders[name].to_csv(output/(name+'_orders.csv'), index=False)
        annotated[name].to_csv(output/(name+'_trade_mover_audit.csv'), index=False)
    for name, frame in decisions.items():
        frame.to_csv(output/(name+'_decisions.csv'), index=False)
    report = ['# G3 V25: full-universe momentum entry research', '',
        f'{len(days)} completed available sessions, {days[0]} through {days[-1]}. '
        'New scanner arms create opportunities across the available historical stock universe; '
        'the published V24 baseline is retained for comparison.', '',
        '**No automatic promotion.** The report describes historical research. Final-day mover rankings, '
        'extremes and first 3% crossing times are joined only after entry decisions and filled trades are fixed.', '',
        'All reported net PnL includes Rs 250 modeled round-trip costs per fill on Rs 500,000 exposure, '
        'with unlimited concurrent capital. New arms use the stated fixed target and 1.50% initial stop; '
        'completed-close 80%-of-target breakeven protection and 15:15 square-off are retained. '
        'Matched-exit controls, when present, retain the original baseline entries with the corresponding fixed target.', '',
        '## Frozen selection decision', '',
        'Validation-chosen arm: **'+_safe(selection.get('validation_chosen') or 'NONE')+'**. '
        'Exploratory comparison: **'+_safe(selection.get('exploratory_best') or 'NONE')+'**.', '',
        'The catalogue displays **'+_safe(chosen)+'**. A displayed exploratory arm is not a recommended replacement. '
        'Selection details and acceptance criteria are recorded in config/summary; reporting does not select an arm.', '',
        'Qualification is a quality/risk shortlist: sufficient fills and active dates, positive validation net PnL, '
        'mean trade profit and profit factor no worse than baseline, and no larger daily-close drawdown. '
        'It does not require total net profit to exceed baseline. A qualifying lower-trade arm can therefore '
        'still sacrifice total profit; the tables must be checked before treating it as an improvement.', '',
        '## Full-history results', '']
    report += _performance_table(overall.loc[overall.period.eq('full_history')])
    report += ['', '## Chronological checks', '',
        'The first 44 dates are the development slice, the following 15 are validation, and the final nine '
        'are the frozen later check. These histories have been examined previously, including in earlier '
        'G3 research; none is claimed to be untouched prospective evidence.', '']
    for period in ['fit_44', 'validation_15', 'later_9']:
        cohort = periods[period]
        report += ['### '+period, '',
                   (f'{len(cohort)} sessions: {cohort[0]} through {cohort[-1]}.' if cohort else 'No dates in this slice.'), '']
        report += _performance_table(overall.loc[overall.period.eq(period)])+['']
    report += ['## Long and short results', '']
    for side in ['LONG', 'SHORT']:
        report += ['### '+side, '']+_performance_table(by_side.loc[by_side.period.eq('full_history')
            & by_side.side.eq(side)])+['']
    report += ['## Did entries capture the daily movers?', '',
        'Mover labels cover up to five stocks per direction/day with at least a 3% move from the first '
        'available session open to the observed daily high/low. They are hindsight evaluation labels, not '
        'a list available to a scanner in advance. Coverage is the available sealed G3 universe, not every NSE security.', '',
        '**Useful capture** requires an actual entry strictly before the final extreme, at least 0.8% '
        'directional distance from entry to that extreme, and positive realized net PnL on that trade. '
        'A case counts once even if the baseline enters it repeatedly. Top-five precision divides unique '
        'top-five stock/day/direction cases traded by all unique stock/day/direction cases traded.', '',
        'For fair timing comparisons, the table uses the same conservative entry-minute end label for both '
        'baseline and new arms, excluding all same-minute crossings/extremes. CSV columns containing '
        '`conservative` reproduce this common-clock comparison; other timing columns additionally use '
        'known next-open timing where available. Filled-case coverage and PnL do not depend on this distinction.', '']
    for period in ['full_history', 'validation_15', 'later_9']:
        report += ['### '+period, '']+_mover_table(mover_summary.loc[mover_summary.period.eq(period)])+['']
    report += ['The near-extreme label is retrospective: entry at/after the final extreme, or less than '
        '0.3% directional distance to it. It measures historical timing and must never become an entry-time input. '
        'Baseline intrabar fills have only an entry-minute end label, so equality with a crossing/extreme minute '
        'is conservatively excluded. New next-open entries have known opening times: a crossing or extreme '
        'in that entry minute counts as later only when the raw opening price was strictly short of that level. '
        'An opening gap already at/beyond 3% never counts as entry before the 3% crossing. '
        'OHLC data does not otherwise resolve within-minute event ordering.', '',
        'Stocks outside the top-five list can still produce profitable trades. The non-top-five PnL column '
        'and trade-context tables quantify the profit sacrificed by an oracle-like desire to avoid all other stocks. '
        'A higher capture percentage alone does not establish a better strategy.', '',
        '## Verification and limits', '',
        'Every filled trade belongs to an admitted order with matching identity. Daily counts, wins/losses, '
        'costs and net PnL reconcile to overall totals. Top-five/non-top-five and near-extreme/other partitions '
        'also reconcile. Parent manifests and code/source provenance are verified by the runner.', '',
        'Inherited source coverage, excluded sessions, missing opening minutes and archived universe limitations '
        'remain listed in config and source provenance. Smaller drawdown in a low-trade arm is not by itself an improvement. '
        'The modeled costs are not a guarantee of live fills or market impact. Existing versions and live/paper settings remain unchanged.', '',
        '[Day-wise results](DAY_WISE_RESULTS.md) | [Every daily top-five mover](FULL_HISTORY_DAILY_TOP5_MOVERS.md) | '
        '[Mover coverage](mover_summary.csv) | [Direction coverage](mover_by_side.csv) | '
        '[Trade context contributions](trade_contexts.csv) | [Missed-case reasons](missed_cases.csv)', '']
    (output/'REPORT.md').write_text('\n'.join(report), encoding='utf-8')
    day_report = ['# G3 V25: day-wise results', '',
        'Net PnL includes modeled costs. All completed available dates are retained, including zero-trade dates. '
        'The full set of daily metrics is in [daily.csv](daily.csv).', '']
    for day in days:
        day_report += ['## '+day, '']+_performance_table(daily.loc[daily.day.eq(day)])+['']
    (output/'DAY_WISE_RESULTS.md').write_text('\n'.join(day_report), encoding='utf-8')
    top_report = ['# G3 V25: daily top-five long and short movers and actual entries', '',
        f'Available completed history: {days[0]} through {days[-1]}. '
        'These daily rankings are evaluated after trades are fixed. A stock can appear in both directions.', '',
        f'Comparison arm: **{chosen}**. Baseline and comparison PnL sum all filled trades for the stock/day/direction. '
        'Entry time is the first known next-open fill for new trades; the baseline uses its conservative '
        'entry-minute end label because exact intrabar fill time is unavailable. Missing entries are marked explicitly. '
        'Detailed gate reasons are in [missed_cases.csv](missed_cases.csv).', '']
    for day in days:
        top_report += ['## '+day, '']
        for side in ['LONG', 'SHORT']:
            part = catalogue.loc[catalogue.day.eq(day) & catalogue.side.eq(side)]
            top_report += ['### '+side, '']
            if part.empty:
                top_report += ['No stock in the available cohort reached 3% in this direction.', '']
                continue
            top_report += ['| Rank | Stock | Move | First 3% | Final extreme | Baseline first fill | Baseline net Rs | Comparison first fill | Comparison net Rs | Status |',
                           '|--:|:--|--:|:--:|:--:|:--:|--:|:--:|--:|:--|']
            for row in part.itertuples(index=False):
                signed = abs(row.move_pct)*(1 if side == 'LONG' else -1)
                top_report.append(f'| {int(row.rank)} | {_safe(row.tradingsymbol)} | {signed:+.2f}% | '
                    f'{_clock(row.first3pct_ts)} | {_clock(row.extreme_ts)} | {_clock(row.baseline_first_entry_ts)} | '
                    f'{row.baseline_net_pnl_rs:,.2f} | {_clock(row.comparison_first_entry_ts)} | '
                    f'{row.comparison_net_pnl_rs:,.2f} | {row.comparison_status} |')
            top_report.append('')
    (output/'FULL_HISTORY_DAILY_TOP5_MOVERS.md').write_text('\n'.join(top_report), encoding='utf-8')
    return summary
