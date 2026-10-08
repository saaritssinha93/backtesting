"""Isolated, approval-only research of unused G2 setup/confirmation windows.

Existing source, selections, exits and run artifacts are read-only. New rules
are fixed donor copies, evaluated chronologically. Nothing is promoted.
"""
from __future__ import annotations

import argparse
import html
import json
from dataclasses import asdict, replace
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_g_2_backtest as g2
import fno_v13_v10_g_2_extend_results as ext

BASELINE = g2.DEFAULT_OUTPUT.parent / 'run_20261005_230448_relaxed0925_comparison'
EXTENSION_CACHE = g2.DEFAULT_OUTPUT.parent / 'unused_windows_extension_cache_20261006'
IST = 'Asia/Kolkata'
BANDS = {'1005_1055': (1005, 1055), '1100_1155': (1100, 1155),
         '1200_1255': (1200, 1255), '1300_1400': (1300, 1400),
         '1005_1400': (1005, 1400)}


def clocks():
    return [t.strftime('%H:%M') for t in pd.date_range('2000-01-01 10:05', '2000-01-01 14:00', freq='5min')]


def normalize(frame):
    out = frame.copy()
    out['day'] = pd.to_datetime(out.day).dt.date
    for col in ('signal_ts', 'confirmation_ts', 'entry_ts', 'exit_ts'):
        if col in out:
            out[col] = pd.to_datetime(out[col], utc=True, errors='coerce').dt.tz_convert(IST)
    for col in ('filled', 'portfolio_executed'):
        if col in out:
            out[col] = out[col].eq(True) | out[col].astype(str).str.lower().eq('true')
    return out


def specs(source):
    change = g2.g.SelectionChange(**source['selection_change'])
    return [g2.g.setup_pair(s, change)[1] for s in
            g2.g.v9.v5.profile_setups(g2.g.v9.v5.PROFILES['higher_frequency'])]


def donor_setups(source):
    originals = {s.setup_id: s for s in specs(source)}
    occupied = {(s.signal_end, s.side) for s in originals.values()}
    result = []
    for clock in clocks():
        for side, donor_id in [('LONG', '1001_LONG'), ('SHORT', '1121_SHORT')]:
            if (clock, side) in occupied:
                continue
            confirm = (pd.Timestamp('2000-01-01 ' + clock) + pd.Timedelta(minutes=1)).strftime('%H:%M')
            result.append(replace(originals[donor_id], signal_end=clock, confirmation_end=confirm,
                max_entries=1, stop_pct=g2.INITIAL_STOP_PCT,
                target_pct=source['exit']['setups'][donor_id]['target_pct']))
    assert len(result) == 95
    return result


def protocol(source):
    return dict(version='G2_UNUSED_WINDOWS_RESEARCH_V1', live_enabled=False, promoted=False,
        baseline=str(BASELINE), first_signal='10:05', last_signal='14:00',
        confirmation='exact next completed minute; entry only after confirmation',
        donor_long='1001_LONG', donor_short='1121_SHORT',
        donor_priority='Current G core-first priority retained: SHORT price move>=0.20% candidates precede expanded0.13% candidates; native liquidity ranking inside each tier.',
        setups=[asdict(s) | {'setup_id': s.setup_id} for s in donor_setups(source)],
        extra_policies='First qualifying top-liquidity selection per side/day in each of five fixed bands; plus whole-window BOTH capped at two orders/day',
        bands=BANDS, total_policies=106,
        coverage='No additions on any day with missing requested-slot raw coverage. Baseline remains intact.',
        train_end='2026-08-31', validation_end='2026-09-18', audit_start='2026-09-21',
        screen=dict(train_fills_min=5, validation_fills_min=3, train_and_validation_net_positive=True,
                    train_and_validation_pf_min=1.2, train_and_validation_net_positive_at_10bps=True),
        proposal='Among screen passes, highest train net, then validation net, then case ID. Audit outcomes never select or retune.',
        final_review='Proposal must have >=3 audit fills, audit PF>=1.20, positive audit incremental net after10bps and full combined daily DD <=1.20x baseline; still needs user approval.',
        preservation='Reserve all pending/open original orders and every not-yet-confirmed original setup quota, using only information known at addon confirmation.',
        exits=g2.config(source)['stop_change'], squareoff='15:15', entry_expiry_minutes=10,
        cost_bps=source['cost_bps'], capital_per_entry_rupees=source['capital_per_entry_rupees'],
        leverage_factor=source['leverage_factor'], capital_rupees=source['portfolio_capital_rupees'],
        limitation='All history has been previously inspected. Chronological retrospective audit, not untouched out-of-sample evidence. Multiple comparisons across106 cases.')


def occupied_orders(orders, now):
    """Count pending/open reservations using information observable by now.

    Future fills are never used to discard an outstanding order. An unfilled
    order remains reserved through the exact ten-minute expiry boundary.
    """
    if orders.empty:
        return 0
    now = pd.Timestamp(now)
    confirmation = pd.to_datetime(orders.confirmation_ts, utc=True)
    entry = pd.to_datetime(orders.entry_ts, utc=True, errors='coerce')
    exit_time = pd.to_datetime(orders.exit_ts, utc=True, errors='coerce')
    known = confirmation.le(now)
    entered = entry.notna() & entry.le(now)
    active = entered & (exit_time.isna() | exit_time.ge(now))
    pending = ~entered & confirmation.add(pd.Timedelta(minutes=10)).ge(now)
    return int((known & (active | pending)).sum())


def reserved_baseline_slots(baseline, now, setup_specs):
    now = pd.Timestamp(now)
    day = now.tz_convert(IST).date()
    future = sum(s.max_entries for s in setup_specs
                 if pd.Timestamp(f'{day} {s.confirmation_end}', tz=IST) > now)
    rows = baseline.loc[pd.to_datetime(baseline.day).dt.date.eq(day)]
    return occupied_orders(rows, now) + future


def admit_additions(additions, baseline, setup_specs, capacity):
    kept, rejected = [], []
    for _, row in additions.sort_values(['confirmation_ts', 'side', 'tradingsymbol'], kind='stable').iterrows():
        now = row.confirmation_ts
        active_additions = pd.DataFrame(kept) if kept else additions.iloc[:0]
        used = reserved_baseline_slots(baseline, now, setup_specs) + occupied_orders(active_additions, now)
        if used + 1 > capacity:
            rejected.append(dict(case_order_key=row.research_key, reason='BASELINE_CAPACITY_RESERVED', reserved_slots=used))
        else:
            kept.append(row)
    return (pd.DataFrame(kept).reset_index(drop=True) if kept else additions.iloc[:0].copy(), rejected)


def select_addons(signals, source):
    parts = []
    volume = pd.to_numeric(signals.v9_1m_volume_ratio, errors='coerce')
    good = signals.loc[np.isfinite(volume) & volume.ge(1.2)].copy()
    originals={s.setup_id:s for s in g2.g.v9.v5.profile_setups(g2.g.v9.v5.PROFILES['higher_frequency'])}
    for spec in donor_setups(source):
        donor_id='1001_LONG' if spec.side=='LONG' else '1121_SHORT'
        core,_=g2.g.setup_pair(originals[donor_id],g2.g.SelectionChange(**source['selection_change']))
        core=replace(core,signal_end=spec.signal_end,confirmation_end=spec.confirmation_end,max_entries=1)
        primary=g2.g.v9.v5.replay.select_setup_rows(good,core)
        expanded=g2.g.v9.v5.replay.select_setup_rows(good,spec)
        rows=pd.concat([primary,expanded.loc[~expanded.day.isin(primary.day)]],ignore_index=False)
        if rows.empty:
            continue
        confirm = pd.to_datetime(rows.confirmation_ts, utc=True)
        signal = pd.to_datetime(rows.signal_ts, utc=True)
        feature = pd.to_datetime(rows.v9_1m_feature_ts, utc=True)
        if not (confirm.sub(signal).eq(pd.Timedelta(minutes=1)) & feature.eq(confirm)).all():
            raise ValueError('Exact confirmation chronology violation')
        rows = g2.g.v9._setup_metadata(rows, spec)
        rows['research_case'] = spec.signal_end.replace(':', '') + '_' + spec.side
        rows['research_key'] = rows.day.astype(str) + ':' + rows.research_case + ':' + rows.tradingsymbol
        rows['book'] = 'ADDON'
        parts.append(rows)
    return normalize(pd.concat(parts, ignore_index=True))


def base_coverage(raw):
    raw = raw.sort_values(['tradingsymbol','futures_tradingsymbol','signal_ts'],kind='stable').copy()
    grouped = raw.groupby(['tradingsymbol','futures_tradingsymbol'],sort=False)
    previous_stamp = grouped.signal_ts.shift(1)
    previous_oi = grouped.oi.shift(1)
    raw['exact_oi_pair'] = (pd.to_datetime(raw.signal_ts,utc=True).sub(pd.to_datetime(previous_stamp,utc=True)).eq(pd.Timedelta(minutes=5))
                           & np.isclose(raw.prev_oi,previous_oi,atol=0,rtol=0,equal_nan=False))
    raw = raw.loc[raw.hhmm_int.between(1005, 1400)].copy()
    raw['day'] = pd.to_datetime(raw.day).dt.date
    valid = np.isfinite(raw.oi) & raw.oi.gt(0) & np.isfinite(raw.prev_oi) & raw.prev_oi.gt(0)
    valid &= raw.v9_exact_confirmation_present.eq(True) & ~raw.confirmation_source_flagged.fillna(False)
    valid &= pd.to_datetime(raw.v9_1m_feature_ts, utc=True).eq(pd.to_datetime(raw.confirmation_ts, utc=True))
    valid &= raw.source_1m_count.eq(5) & raw.exact_oi_pair
    raw['invalid_rows'] = (~valid).astype(int)
    out = raw.groupby(['day', 'tradingsymbol']).agg(observed_slots=('hhmm_int','nunique'),
        invalid_rows=('invalid_rows','sum')).reset_index().rename(columns={'tradingsymbol':'symbol'})
    out['expected_slots'] = 48
    out['complete'] = out.observed_slots.eq(48) & out.invalid_rows.eq(0)
    return out


def metric(ledger, days):
    rows = ledger.loc[ledger.portfolio_executed.eq(True)]
    pnl = rows.portfolio_net_profit_rupees.astype(float)
    daily = pnl.groupby(rows.day).sum().reindex(days, fill_value=0.)
    curve = daily.cumsum()
    dd = curve.cummax().clip(lower=0) - curve
    loss = -pnl[pnl.lt(0)].sum()
    return dict(trades=len(rows), wins=int(pnl.gt(0).sum()), losses=int(pnl.lt(0).sum()),
        win_rate_pct=100*float(pnl.gt(0).mean()) if len(rows) else 0.,
        net=float(pnl.sum()), gross=float(rows.portfolio_gross_profit_rupees.sum()),
        costs=float(rows.portfolio_cost_rupees.sum()), pf=float(pnl[pnl.gt(0)].sum()/loss) if loss else (float('inf') if len(rows) else 0.),
        daily_dd=float(dd.max()) if len(dd) else 0., positive_days=int(daily.gt(0).sum()),
        negative_days=int(daily.lt(0).sum()), worst_trade=float(pnl.min()) if len(pnl) else 0.,
        net_10bps=float(pnl.sum()-250*len(rows)), net_15bps=float(pnl.sum()-500*len(rows)),
        time_exits=int(rows.exit_reason.eq('TIME_EXIT_1515').sum()),
        targets=int(rows.exit_reason.eq('TARGET').sum()),
        stops=int(rows.exit_reason.isin(['STOP','TIGHTENED_STOP']).sum()),
        worst_day=float(daily.min()) if len(daily) else 0.)


def daily_table(ledger, days):
    """Small empty-safe day table, including sessions without fills."""
    done = ledger.loc[ledger.portfolio_executed.eq(True)].copy()
    result = pd.DataFrame(index=pd.Index(days,name='day'))
    result['selected_orders'] = ledger.groupby('day').size().reindex(days,fill_value=0)
    result['trades'] = done.groupby('day').size().reindex(days,fill_value=0)
    result['wins'] = done.loc[done.portfolio_net_profit_rupees.gt(0)].groupby('day').size().reindex(days,fill_value=0)
    result['losses'] = done.loc[done.portfolio_net_profit_rupees.lt(0)].groupby('day').size().reindex(days,fill_value=0)
    for short,long in [('gross','gross_profit'),('cost','cost'),('net','net_profit')]:
        result[short+'_pnl_rupees'] = done.groupby('day')['portfolio_'+long+'_rupees'].sum().reindex(days,fill_value=0.)
    result['cumulative_net_pnl_rupees'] = result.net_pnl_rupees.cumsum()
    result['drawdown_rupees'] = result.cumulative_net_pnl_rupees.cummax().clip(lower=0)-result.cumulative_net_pnl_rupees
    return result.reset_index()


def ensure_baseline_preserved(combined, baseline):
    observed = combined.loc[combined.book.eq('BASELINE')].set_index('research_key').sort_index()
    original = baseline.set_index('research_key').sort_index()
    if not observed.index.equals(original.index):
        raise AssertionError('Original selected entries changed')
    for col in ('portfolio_executed', 'filled', 'entry_ts', 'exit_ts', 'exit_reason'):
        if not observed[col].astype(str).equals(original[col].astype(str)):
            raise AssertionError(f'Original {col} changed')
    for col in ('entry_price','exit_price','portfolio_net_profit_rupees','portfolio_gross_profit_rupees','portfolio_cost_rupees'):
        if not np.allclose(observed[col], original[col], atol=1e-6, rtol=0, equal_nan=True):
            raise AssertionError(f'Original {col} changed')


def policies(simulated, source):
    result = {}
    for spec in donor_setups(source):
        key = spec.signal_end.replace(':','') + '_' + spec.side
        result[key] = simulated.loc[simulated.research_case.eq(key)].copy()
    for band, (start,end) in BANDS.items():
        for side in ('LONG','SHORT'):
            pool = simulated.loc[simulated.hhmm_int.between(start,end) & simulated.side.eq(side)]
            result['FIRST_' + band + '_' + side] = pool.sort_values(['confirmation_ts','tradingsymbol'],kind='stable').groupby('day',sort=False).head(1)
    result['FIRST_1005_1400_BOTH'] = pd.concat([result['FIRST_1005_1400_LONG'],result['FIRST_1005_1400_SHORT']],ignore_index=True)
    assert len(result) == 106
    return result


def write_review(output, outcome, table, daily):
    """Human-readable research report; never an activation or config artifact."""
    def show(frame):
        return frame.to_html(index=False,border=0,float_format=lambda v:f'{v:,.2f}',escape=True)
    top=table.sort_values('addon_net',ascending=False).head(12)
    columns=['case','addon_trades','addon_win_rate_pct','addon_net','addon_costs','addon_pf',
             'train_net','validation_net','audit_net','screen_pass']
    proposal=outcome['proposal_selected_without_audit']
    proposal_text=(f'Preselected research proposal: {proposal}. Later-period review passed: '
                   f'{outcome["proposal_passed_final_review"]}.' if proposal else
                   'No policy passed the predeclared training and validation screen. No new window is proposed for adoption.')
    folds=[]
    if proposal:
        row=table.set_index('case').loc[proposal]
        for fold in ('train','validation','audit'):
            folds.append(dict(period=fold,trades=row[fold+'_trades'],win_rate_pct=row[fold+'_win_rate_pct'],
                              net_rupees=row[fold+'_net'],net_at_10bps=row[fold+'_net_10bps'],
                              profit_factor=row[fold+'_pf']))
    baseline=outcome['baseline']
    comparison=[dict(case='CURRENT_G2',trades=baseline['trades'],win_rate_pct=baseline['win_rate_pct'],
        net_rupees=baseline['net'],cost_rupees=baseline['costs'],profit_factor=baseline['pf'],daily_drawdown_rupees=baseline['daily_dd'])]
    for case in outcome['review_cases']:
        row=table.set_index('case').loc[case]
        comparison.append(dict(case=case,trades=row.combined_trades,win_rate_pct=row.combined_win_rate_pct,
            net_rupees=row.combined_net,cost_rupees=row.combined_costs,profit_factor=row.combined_pf,daily_drawdown_rupees=row.combined_daily_dd))
    day_columns=['baseline_net_pnl_rupees']+[c+'_addon_net' for c in outcome['review_cases']]
    day_view=daily[day_columns+['addon_coverage_complete']].reset_index()
    notes=[
        '106 fixed policies: 95 unused side/time cells, ten first-entry band/side policies, and one first-entry both-side policy.',
        'Five-minute signals 10:05 through 14:00; confirmation at the next completed minute 10:06 through 14:01. Existing 11:20 SHORT remains reserved.',
        'LONG donor 10:00: price change >=0.40%, OI increase 0.05% to 1.00%, target 3.00%. SHORT donor 11:20: price change <=-0.13%, OI increase 0.05% to 1.00%, target 2.00%.',
        'Both sides retain directional five-minute EMA9/20/50, five-minute volume >=1x, next-minute volume >=1.2x, directional confirmation, body >=40%, adverse wick <=60%. Current core-first priority is retained: SHORT moves >=0.20% precede the relaxed 0.13% tier, ranked by traded value within each tier; top 1 per setup/day.',
        'Stops 1.25% from actual entry, tightened to 1.00% after 120 minutes. Ten-minute entry expiry, stop-first ambiguous minute bars, 15:15 squareoff.',
        'Capital 1,000,000 rupees, 100,000 per entry, 5x exposure. Flat 5 bps round-trip cost =250 rupees per fill. 10/15 bps stress is reported. No separate market-impact or integer-share fill model.',
        'All 100 original selected orders and 91 executed trades retained identically in every case; existing source files verified unchanged.',
        'Baseline 44 recorded sessions through 2026-10-05. Addons evaluated only on '+str(len(outcome['addon_eligible_sessions']))+' fully covered sessions. Missing addon coverage dates: '+', '.join(outcome['excluded_addon_sessions'])+'. Existing trades on these dates retained.',
        'Combined totals retain all 44 baseline sessions and assume zero additional contribution on the three unavailable research dates. This is incomplete addon coverage, not proof that an addon strategy would have taken no trades on those dates. Same-session baseline metrics are also recorded in summary.json.',
        'Train through Aug 31, validation Sep 1–18, later audit Sep 21–Oct 5. The proposal uses only train and validation results. No replacement is chosen after viewing audit failure.',
        'This history was previously examined. The chronological audit is retrospective, not untouched out-of-sample evidence. Best full-history rankings are diagnostics affected by 106 comparisons.',
        'Training and validation require at least 5 and 3 fills, respectively, PF >=1.20 and positive net after 10 bps. Later review requires at least 3 fills, PF >=1.20, positive net after 10 bps and combined daily drawdown <=120% of baseline.',
        'No changes are approved, promoted, scheduled or connected to broker orders. User approval is required before integration.'
    ]
    body=f'''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>G-2 unused-window research</title><style>body{{font:15px system-ui,sans-serif;margin:32px;color:#17212b;background:#f6f8fa}}main{{max-width:1500px;margin:auto}}h1{{font-size:26px}}h2{{font-size:20px;margin-top:30px}}p,li{{line-height:1.55}}table{{border-collapse:collapse;background:white;font-size:13px;width:100%}}th,td{{padding:9px 12px;text-align:right;border-bottom:1px solid #dce3e8;white-space:nowrap}}th:first-child,td:first-child{{text-align:left}}th{{background:#e5edf4}}section{{overflow:auto}}a{{color:#174d78}}.status{{background:#fff3cd;padding:16px;border-left:4px solid #a77916}}</style><main>
<h1>V13-v10-G-2 unused-window research</h1><p class="status">Research only. Awaiting user approval. {html.escape(proposal_text)}</p>
<p>Current G-2 net profit: {baseline['net']:,.2f} rupees from {baseline['trades']} trades. Every original entry and exit is preserved.</p>
<h2>Combined comparison</h2><section>{show(pd.DataFrame(comparison))}</section>
<h2>Chronological results for the preselected proposal</h2><section>{show(pd.DataFrame(folds)) if folds else '<p>No proposal passed the screen.</p>'}</section>
<h2>Highest full-history incremental profits — diagnostic ranking</h2><p>These rows are not approved strategies. Compare their earlier and later results before interpreting the totals.</p><section>{show(top[columns])}</section>
<h2>Daywise comparison</h2><p>Addon columns show only the additional net contribution. False coverage means research was unavailable for that day, not an observed no-trade outcome.</p><section>{show(day_view)}</section>
<h2>Rules and evidence</h2><ul>{''.join('<li>'+html.escape(n)+'</li>' for n in notes)}</ul>
<p><a href="all_106_policy_results.csv">All 106 policy results</a> · <a href="all_106_daywise_results.csv">All 106 daywise results</a> · <a href="daywise_comparison.csv">Shortlist daywise comparison</a> · <a href="summary.json">Full research evidence</a></p></main></html>'''
    (output/'research_report.html').write_text(body,encoding='utf-8')


def run(output, baseline_root=BASELINE):
    from fno_v13_v10_g_2_unused_slots_data import build_extension_data
    output = Path(output)
    if output.exists():
        raise FileExistsError('Use a new isolated research output directory')
    output.mkdir(parents=True)
    existing = [p for p in Path('.').glob('fno*.py') if 'unused_slots' not in p.name]
    existing += list(Path(baseline_root).glob('*.*'))
    before = {str(p.resolve()):g2.sha256(p) for p in existing if p.is_file()}
    dataset = g2.load_bundle(g2.DEFAULT_SOURCE_BUNDLE,g2.DEFAULT_G_CONFIG)
    source = dataset['source_g']
    plan = protocol(source)
    g2.dump_json(output/'experiment_plan.json',plan)
    summary = g2.read_json(Path(baseline_root)/'summary.json')
    baseline = normalize(pd.read_csv(Path(baseline_root)/'relaxed_portfolio_trades.csv'))
    baseline['book'] = 'BASELINE'
    baseline['research_key'] = baseline.comparison_key
    days = [date.fromisoformat(d) for d in summary['sessions']]
    control = metric(baseline,days)
    if control['trades'] != 91 or not np.isclose(control['net'],239257.28188589335,atol=1e-6,rtol=0):
        raise AssertionError('G2 baseline differs from published latest run')
    coverage = base_coverage(pd.read_parquet(dataset['source']/'dataset/all_5m_features.parquet'))
    print('Protocol registered. Loading unused-slot observations; baseline91 fills verified.',flush=True)
    extension_signals, extension_paths, extension_coverage, evidence = build_extension_data(summary,EXTENSION_CACHE)
    coverage = pd.concat([coverage,extension_coverage],ignore_index=True)
    coverage['day'] = pd.to_datetime(coverage.day).dt.date
    coverage['complete'] = coverage.observed_slots.eq(48) & coverage.invalid_rows.eq(0)
    excluded = sorted(coverage.loc[~coverage.complete,'day'].unique())
    eligible_days = [d for d in days if d not in excluded]
    coverage.to_csv(output/'data_coverage.csv',index=False)
    signals = pd.concat([dataset['signals'],extension_signals],ignore_index=True,sort=False)
    signals['day'] = pd.to_datetime(signals.day).dt.date
    signals = signals.loc[signals.day.isin(eligible_days)]
    orders = select_addons(signals,source)
    if orders.sid.duplicated().any():
        raise AssertionError('Research selected signal IDs overlap')
    paths = dict(extension_paths)
    needed = set(orders.sid.astype(int))
    with np.load(dataset['source']/'dataset/paths.npz',allow_pickle=False) as archive:
        for name in archive.files:
            sid,field = name.split('_',1)
            if int(sid) in needed:
                paths.setdefault(int(sid),{})[field] = archive[name]
    g2.g.v9.validate_paths(orders,paths)
    simulated = g2.simulate_staged(orders,paths,cost_bps=source['cost_bps'],max_entry_delay_minutes=10)
    simulated = normalize(g2.g.v9.v5.apply_fixed_capital_model(simulated,source['capital_per_entry_rupees'],source['leverage_factor']))
    simulated.to_csv(output/'all_selected_addon_orders.csv',index=False)
    folds = {'train':[d for d in days if d <= date(2026,8,31)],
             'validation':[d for d in days if date(2026,8,31)<d<=date(2026,9,18)],
             'audit':[d for d in days if d>=date(2026,9,21)]}
    rows, ledgers, addon_ledgers, rejected_rows, all_daily = [], {}, {}, [], []
    baseline_daily = daily_table(baseline,days).set_index('day').net_pnl_rupees
    capacity = int(source['portfolio_capital_rupees']/source['capital_per_entry_rupees'])
    for case, candidate in policies(simulated,source).items():
        admitted,rejected = admit_additions(candidate,baseline,specs(source),capacity)
        for item in rejected:
            rejected_rows.append({'case':case,**item})
        merged = pd.concat([baseline,admitted],ignore_index=True,sort=False)
        ledger,pstats = g2.g.v9.v6.apply_portfolio_constraints(merged,dataset['v9_config'].portfolio_config())
        ledger = normalize(ledger)
        ensure_baseline_preserved(ledger,baseline)
        extra = ledger.loc[ledger.book.eq('ADDON')]
        if len(extra) and (extra.filled & ~extra.portfolio_executed).any():
            raise AssertionError('Causal reservation failed to guarantee addon capacity')
        row = {'case':case,'selected_addons':len(candidate),'admitted_addons':len(admitted),
               'baseline_fills_retained':int(ledger.loc[ledger.book.eq('BASELINE'),'portfolio_executed'].sum()),
               'peak_positions':pstats['peak_concurrent_positions']}
        row.update({'addon_'+k:v for k,v in metric(extra,days).items()})
        row.update({'combined_'+k:v for k,v in metric(ledger,days).items()})
        for fold,fold_days in folds.items():
            row.update({fold+'_'+k:v for k,v in metric(extra.loc[extra.day.isin(fold_days)],fold_days).items()})
        row['screen_pass'] = (row['train_trades']>=5 and row['validation_trades']>=3
            and row['train_net']>0 and row['validation_net']>0
            and row['train_pf']>=1.2 and row['validation_pf']>=1.2
            and row['train_net_10bps']>0 and row['validation_net_10bps']>0)
        reasons=[]
        for fold,min_count in [('train',5),('validation',3)]:
            if row[fold+'_trades']<min_count: reasons.append(fold+'_TOO_FEW_TRADES')
            if row[fold+'_pf']<1.2: reasons.append(fold+'_PF_BELOW_1.2')
            if row[fold+'_net_10bps']<=0: reasons.append(fold+'_NONPOSITIVE_NET_AT_10BPS')
        row['screen_failures'] = ';'.join(reasons)
        perday=daily_table(extra,days)
        perday.insert(0,'case',case)
        perday['baseline_net_rupees']=perday.day.map(baseline_daily)
        perday['combined_net_rupees']=perday.net_pnl_rupees+perday.baseline_net_rupees
        perday['addon_coverage_complete']=perday.day.isin(eligible_days)
        all_daily.append(perday)
        rows.append(row); ledgers[case]=ledger; addon_ledgers[case]=extra
    table = pd.DataFrame(rows)
    passing = table.loc[table.screen_pass].sort_values(['train_net','validation_net','case'],ascending=[False,False,True],kind='stable')
    proposal = None if passing.empty else str(passing.iloc[0]['case'])
    final_pass = bool(proposal and table.set_index('case').loc[proposal,'audit_trades']>=3
                      and table.set_index('case').loc[proposal,'audit_pf']>=1.2
                      and table.set_index('case').loc[proposal,'audit_net_10bps']>0
                      and table.set_index('case').loc[proposal,'combined_daily_dd']<=1.2*control['daily_dd'])
    table.sort_values(['addon_net','case'],ascending=[False,True]).to_csv(output/'all_106_policy_results.csv',index=False)
    pd.concat(all_daily,ignore_index=True).to_csv(output/'all_106_daywise_results.csv',index=False)
    pd.concat([frame.assign(policy_case=case) for case,frame in addon_ledgers.items()],ignore_index=True,sort=False).to_csv(output/'all_106_addon_trades.csv',index=False)
    pd.DataFrame(rejected_rows,columns=['case','case_order_key','reason','reserved_slots']).to_csv(output/'capacity_rejections.csv',index=False)
    best_full = str(table.sort_values(['addon_net','case'],ascending=[False,True]).iloc[0]['case'])
    review_cases = list(dict.fromkeys([c for c in [proposal,best_full,'FIRST_1005_1400_LONG','FIRST_1005_1400_SHORT','FIRST_1005_1400_BOTH'] if c]))
    daily = daily_table(baseline,days).set_index('day').add_prefix('baseline_')
    for case in review_cases:
        folder = output/case; folder.mkdir()
        ledgers[case].to_csv(folder/'portfolio_trades.csv',index=False)
        addon_ledgers[case].to_csv(folder/'addon_trades.csv',index=False)
        daily_table(ledgers[case],days).to_csv(folder/'daily_results.csv',index=False)
        add_daily = daily_table(addon_ledgers[case],days)
        add_daily.to_csv(folder/'addon_daily_results.csv',index=False)
        daily[case+'_addon_net'] = add_daily.set_index('day').net_pnl_rupees
        daily[case+'_combined_net'] = daily.baseline_net_pnl_rupees+daily[case+'_addon_net']
    daily['addon_coverage_complete'] = [d in eligible_days for d in daily.index]
    daily.to_csv(output/'daywise_comparison.csv')
    after = {path:g2.sha256(Path(path)) for path in before}
    if before != after:
        raise AssertionError('Pre-existing file changed during research')
    outcome = dict(protocol=plan,baseline=control,
        common_session_baseline=metric(baseline.loc[baseline.day.isin(eligible_days)],eligible_days),
        sessions=[str(d) for d in days],
        addon_eligible_sessions=[str(d) for d in eligible_days],excluded_addon_sessions=[str(d) for d in excluded],
        folds={k:[str(d) for d in ds] for k,ds in folds.items()},
        policies_tested=len(table),screen_passes=int(table.screen_pass.sum()),
        proposal_selected_without_audit=proposal,proposal_passed_final_review=final_pass,
        full_history_best_diagnostic_only=best_full,review_cases=review_cases,
        proposal_metrics=table.loc[table.case.eq(proposal)].to_dict('records'),
        top_full_history_diagnostics=table.sort_values('addon_net',ascending=False).head(10).to_dict('records'),
        baseline_preserved_in_all_cases=True,existing_files_unchanged=True,execution_authority=False,
        approval_status='AWAITING_USER_APPROVAL_NO_PROMOTION',data_evidence=evidence,
        protected_sha256=before,code_sha256={p.name:g2.sha256(p) for p in Path('.').glob('fno_v13_v10_g_2_unused_slots*.py')})
    g2.dump_json(output/'summary.json',ext._finite_json(outcome))
    write_review(output,outcome,table,daily)
    g2.dump_json(output/'research_manifest.json',dict(complete=True,
        artifacts={str(p.relative_to(output)):g2.sha256(p) for p in output.rglob('*') if p.is_file() and p.name!='research_manifest.json'},
        code_sha256=outcome['code_sha256'],protected_files_unchanged=True,execution_authority=False))
    print(json.dumps(ext._finite_json({k:outcome[k] for k in ('baseline','policies_tested','screen_passes','proposal_selected_without_audit','proposal_passed_final_review','full_history_best_diagnostic_only','excluded_addon_sessions','proposal_metrics')}),indent=2),flush=True)
    print('RESULTS: '+str(output),flush=True)
    return outcome


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=g2.DEFAULT_OUTPUT.parent/f'run_{datetime.now():%Y%m%d_%H%M%S}_unused_windows_research')
    args=parser.parse_args()
    run(args.output)
