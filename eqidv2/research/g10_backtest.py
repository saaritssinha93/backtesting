"""Fixed G10 experiments against the entire current G7 V4 lock history.

This is a conditional comparison on the inherited historical universe, not
an independent validation or a repaired causal-universe backtest.
"""
from __future__ import annotations
import argparse
from datetime import datetime
import hashlib
import html
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from research import slot_win70_backtest as reference
from research import g7_v4_backtest as cache_module
from research import g7_v4_filtered_backtest as current
from research.g7_v4_lock_backtest import same_execution
from research.g8_exit_engine import _flags, metrics as native_metrics
from research.g10_entry_risk import cancel_overextended_orders
from research.g10_portfolio import execute
import strategy_slot_selection as slot_policy

CONFIG = ROOT/'configs/strategies/v13_v10_g_10_backtest.json'
RESULT_ROOT = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_10')
RECOVERY = ROOT/'configs/research/g10_path_recovery.json'


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def clean(value):
    if isinstance(value, dict): return {str(k): clean(v) for k,v in value.items()}
    if isinstance(value, (list, tuple)): return [clean(v) for v in value]
    if isinstance(value, np.generic): return clean(value.item())
    if isinstance(value, float) and not np.isfinite(value): return None
    if isinstance(value, (Path, pd.Timestamp)): return str(value)
    return value


def dump(path, value):
    Path(path).write_text(json.dumps(clean(value), indent=2, allow_nan=False), encoding='utf-8')


def protected_files():
    files = list((ROOT/'configs/strategies').glob('*.json'))
    for name in ('backtesting_result_v13_v10_g_3', 'backtesting_result_v13_v10_g_7'):
        base = RESULT_ROOT.parent/name
        files += list((base/'latest').rglob('*.json'))
        files += list(base.glob('**/latest.json'))
    return {str(p): sha(p) for p in files}


def load_paths(source):
    _, _, _, original_paths, proof = cache_module.load_cache(reference.G7_CACHE)
    paths = {int(sid): original_paths[int(sid)] for sid in source.sid if int(sid) in original_paths}
    missing = source.loc[~source.sid.isin(paths)]
    recovery_evidence = []
    if len(missing):
        recovery = read(RECOVERY)
        for row in missing.itertuples():
            item = next(x for x in recovery['paths'] if int(x['sid']) == int(row.sid))
            path = Path(item['path'])
            if sha(path) != item['sha256']:
                raise ValueError('Recovered source changed: '+str(path))
            if item['sha256'] != item['original_expected_sha256'] or sha(item['manifest_path']) != item['manifest_sha256']:
                raise ValueError('Recovered source does not preserve pinned provenance')
            captured=[x for x in read(item['manifest_path'])['sources']
                      if x.get('captured') and x.get('sha256') == item['sha256']
                      and (Path(item['manifest_path']).parent / x['snapshot_relative_path']).resolve() == path.resolve()]
            if len(captured)!=1:
                raise ValueError('Recovered source is not a captured manifest member')
            frame = pd.read_parquet(path)
            frame['ts'] = pd.to_datetime(frame.date, utc=True).dt.tz_convert('Asia/Kolkata')
            confirmation = pd.Timestamp(row.confirmation_ts)
            if confirmation.tzinfo is None: confirmation = confirmation.tz_localize('Asia/Kolkata')
            else: confirmation = confirmation.tz_convert('Asia/Kolkata')
            end = pd.Timestamp(str(row.day)[:10]+' 15:15', tz='Asia/Kolkata')
            frame = frame.loc[frame.ts.gt(confirmation) & frame.ts.le(end)].sort_values('ts')
            wanted = pd.date_range(confirmation+pd.Timedelta(minutes=1), end, freq='min')
            if not pd.DatetimeIndex(frame.ts).equals(wanted):
                raise ValueError('Recovered path incomplete')
            for field in ('gap_filled', 'opening_snapshot', 'provisional_stale', 'is_synthetic'):
                if field in frame:
                    flags = pd.to_numeric(frame[field], errors='coerce').fillna(0).ne(0) | frame[field].astype(str).str.lower().isin(['true','yes','on'])
                    if flags.any(): raise ValueError('Flagged recovered path')
            paths[int(row.sid)] = {'timestamp_ns': frame.ts.astype('int64').to_numpy(),
                                   **{k: frame[k].to_numpy(float) for k in ('open','high','low','close')}}
            recovery_evidence.append(item)
    if set(paths) != set(source.sid): raise ValueError('Missing execution path')
    return paths, proof, recovery_evidence


def stats(book, days):
    # Keep both drawdown aliases explicitly anchored to zero starting P&L.
    result = native_metrics(book, days)
    x = book.loc[book.day.astype(str).str[:10].isin(days) & _flags(book.portfolio_executed, 'executed')]
    pnl = pd.to_numeric(x.portfolio_net_profit_rupees)
    daily = x.assign(_day=x.day.astype(str).str[:10]).groupby('_day').portfolio_net_profit_rupees.sum().reindex(days, fill_value=0.)
    equity = np.r_[0., daily.cumsum().to_numpy(float)]
    result['daily_close_drawdown_rs'] = float((np.maximum.accumulate(equity)-equity).max())
    result['max_drawdown_rs'] = result['daily_close_drawdown_rs']
    wins, losses = int(pnl.gt(1e-8).sum()), int(pnl.lt(-1e-8).sum())
    result.update(winning_trade_sum_rs=float(pnl[pnl>0].sum()),
                  average_win_rs=float(pnl[pnl>0].mean()) if wins else 0.,
                  average_loss_rs=float(-pnl[pnl<0].mean()) if losses else 0.,
                  expectancy_rs=float(pnl.mean()) if len(pnl) else 0.,
                  total_initial_risk_rs=float(x.portfolio_trade_initial_risk_rupees.sum()),
                  total_exposure_rs=float(x.portfolio_trade_exposure_rupees.sum()),
                  worst_day_rs=float(daily.min()), losing_days=int(daily.lt(-1e-8).sum()))
    result['net_per_initial_risk'] = result['net_pnl_rs']/result['total_initial_risk_rs'] if result['total_initial_risk_rs'] else None
    result['profit_factor'] = float(pnl[pnl>0].sum()/-pnl[pnl<0].sum()) if losses else None
    return clean(result)


def acceptance(actual, control, changed_days):
    failures=[]
    if actual['net_pnl_rs'] < control['net_pnl_rs']-1e-6: failures.append('net profit decreased')
    if actual['negative_trade_sum_abs_rs'] > .9*control['negative_trade_sum_abs_rs']+1e-6: failures.append('loss amount reduction below 10%')
    if actual['daily_close_drawdown_rs'] > control['daily_close_drawdown_rs']+1e-6: failures.append('daily-close drawdown increased')
    if actual['trades'] < .8*control['trades']: failures.append('trade retention below 80%')
    if actual['wins'] < .9*control['wins']: failures.append('winner retention below 90%')
    if changed_days < 3: failures.append('fewer than three changed days')
    return failures


def compare_rows(book, control, days):
    a = book.set_index('sid').sort_index(); b = control.set_index('sid').loc[a.index]
    in_period = a.day.astype(str).str[:10].isin(days)
    ax = _flags(a.portfolio_executed, 'executed'); bx = _flags(b.portfolio_executed, 'control')
    changed = (ax.ne(bx) | (a.portfolio_net_profit_rupees-b.portfolio_net_profit_rupees).abs().gt(1e-6)) & in_period
    removed = bx & ~ax & in_period
    return dict(changed_days=int(a.loc[changed,'day'].nunique()), changed_trades=int(changed.sum()),
        avoided_baseline_losers=int((removed & b.portfolio_net_profit_rupees.lt(0)).sum()),
        missed_baseline_winners=int((removed & b.portfolio_net_profit_rupees.gt(0)).sum()),
        new_allocations=int((ax & ~bx & in_period).sum()))


def write_report(out, summary, overall, daywise, acceptance_rows):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    wide = daywise.pivot(index='day', columns='variant', values='net_pnl_rs').fillna(0)
    primary = summary['primary_candidate']
    labels = {'g7_v4_lock':'G7 V4 lock', 'g10_entry_rr80':'G10 entry check',
              'g10_risk6000':'G10 risk sizing', 'g10_risk6000_cap12000':'G10 sizing + cap',
              'g10_combined':'G10 combined (primary)'}
    fig, axes = plt.subplots(2, 1, figsize=(11,7), sharex=True)
    for name,color in [('g7_v4_lock','#365c9c'),(primary,'#c96d28')]:
        values = np.r_[0,wide[name].cumsum().to_numpy()]
        axes[0].plot(range(len(values)),values,label=labels[name],color=color)
        axes[1].plot(range(len(values)),np.maximum.accumulate(values)-values,label=labels[name],color=color)
    axes[0].set_ylabel('Cumulative net P&L (Rs)'); axes[1].set_ylabel('Daily-close drawdown (Rs)')
    axes[1].set_xlabel('Session number; full 59-session comparison'); axes[0].legend()
    for ax in axes: ax.grid(alpha=.2)
    fig.suptitle('G10 versus G7 V4 lock — retrospective comparison, 5 bps costs')
    fig.tight_layout(); fig.savefig(out/'comparison.png',dpi=140); plt.close(fig)
    columns=['variant','trades','wins','losses','win_rate_pct','net_pnl_rs','negative_trade_sum_abs_rs','daily_close_drawdown_rs','missed_baseline_winners','new_allocations']
    headers={'variant':'Variant','trades':'Trades','wins':'Wins','losses':'Losses',
             'win_rate_pct':'Win %','net_pnl_rs':'Net Rs','negative_trade_sum_abs_rs':'Losing-trade amount Rs',
             'daily_close_drawdown_rs':'Daily-close drawdown Rs','missed_baseline_winners':'Missed G7 winners',
             'new_allocations':'New allocations'}
    def show(frame):
        frame=frame.copy()
        if 'variant' in frame: frame['variant']=frame['variant'].map(labels).fillna(frame['variant'])
        return frame.rename(columns=headers).to_html(index=False,float_format=lambda x:f'{x:,.2f}',border=0)
    all_rows=overall.loc[overall.period.eq('all') & overall.cost_bps.eq(5.)]
    oct_rows=overall.loc[overall.period.eq('october') & overall.cost_bps.eq(5.)]
    daily=wide.rename(columns={name:label+' net Rs' for name,label in labels.items()})
    daily['Primary minus G7 Rs']=wide[primary]-wide.g7_v4_lock
    conclusion='No G10 variant passes the whole-period acceptance screen.' if not acceptance_rows.passes_numeric_screen.any() else 'See the preset whole-period acceptance checks below.'
    text=f'''<!doctype html><html><head><meta charset="utf-8"><title>G10 backtest comparison</title>
    <style>body{{font:15px system-ui;max-width:1250px;margin:32px auto;color:#172334}}table{{border-collapse:collapse;width:100%;font-size:13px}}th,td{{padding:7px;border-bottom:1px solid #ddd;text-align:right}}th:first-child,td:first-child{{text-align:left}}.scroll{{overflow:auto}}.notice{{padding:18px;background:#fff1d6}}img{{max-width:100%}}li{{margin:7px}}</style></head><body>
    <h1>G10 compared with G7 V4 lock</h1><p>{summary['first_date']} to {summary['last_date']}; {summary['sessions']} sessions. Primary G10 was fixed before results: {html.escape(primary)}.</p>
    <div class="notice"><b>Research result, no promotion.</b> Both strategies inherit future-dependent historical stock-day OI eligibility. These dates were previously inspected. This is a comparison on the same inherited universe, not independent validation or an estimate of live returns.</div>
    <p><b>{html.escape(conclusion)}</b> The primary combined variant reduced losing-trade amount but sacrificed too many winners and increased daily-close drawdown. No tested variant increased whole-period net profit.</p>
    <h2>Whole period at 5 bps roundtrip costs</h2><div class="scroll">{show(all_rows[columns])}</div>
    <p>Initial capital Rs1,000,000; maximum inherited allocation Rs100,000 per entry at 5x modeled leverage. G10 risk budget Rs6,000 includes costs; open-risk cap Rs12,000. Smaller losses from smaller exposure alone are not evidence of a better signal. See risk-normalized metrics in overall_comparison.csv.</p>
    <img src="comparison.png" alt="Net P and L and daily close drawdown"><h2>October at 5 bps</h2><div class="scroll">{show(oct_rows[columns])}</div>
    <h2>Fixed acceptance checks</h2><div class="scroll">{show(acceptance_rows)}</div>
    <h2>All sessions daywise</h2><div class="scroll">{show(daily.reset_index())}</div>
    <h2>Method and limitations</h2><ul>'''
    text+=''.join('<li>'+html.escape(x)+'</li>' for x in summary['limitations'])
    text+='</ul><p>All experiments, chronological diagnostics, cost stress, rejected opportunities, bootstrap diagnostics and source hashes are supplied alongside this report. No strategy settings, live/paper controls or G7 dashboard pointers were updated.</p></body></html>'
    (out/'report.html').write_text(text,encoding='utf-8')


def run(output_dir=None):
    protocol=read(CONFIG); config_hash=sha(CONFIG); recovery_hash=sha(RECOVERY)
    if protocol['scope']!='BACKTEST_ONLY' or protocol['execution_authority'] is not False:
        raise ValueError('G10 must remain backtest-only')
    now=datetime.now(ZoneInfo('Asia/Kolkata'))
    out=Path(output_dir) if output_dir else RESULT_ROOT/'runs'/now.date().isoformat()/(now.strftime('%Y%m%dT%H%M%S%f')+'_fixed_entry_risk_v1')
    out=out.resolve()
    if out.exists(): raise FileExistsError(out)
    out.mkdir(parents=True)
    dump(out/'protocol.json',protocol)
    dump(out/'run_started.json',dict(started_at=now.isoformat(),protocol_sha256=config_hash,
        historical_data_previously_inspected=True,parameters_fixed_before_first_g10_result=True,
        existing_g10_results_present=(RESULT_ROOT/'latest.json').exists()))
    protected=protected_files()
    print('Loading verified G7 control and minute paths',flush=True)
    books,days,base_config=reference.source_inputs(); source=books['G7_V4_LOCK2']
    paths,proof,recovery=load_paths(source)
    settings=read(current.CONFIG)
    # Published `filled` includes direction permissions. Restore the sealed
    # native touch inventory before replay; permissions are applied separately.
    native_source=source.copy(deep=True)
    native_source['filled']=_flags(source.g8_baseline_filled,'native touch inventory')
    raw=current.previous.replay(native_source,paths,base_config,current.protection_settings(settings))
    parity=same_execution(raw,native_source,'Native minute-exit reproduction')
    if not parity['passed']: raise ValueError(parity)
    # Validate every pending native event even for the no-filter baseline.
    validation=cancel_overextended_orders(raw,paths,retention=.8,cost_bps=5.)
    enabled=slot_policy.enabled_setup_ids('G7_V4_LOCK2')
    allowed=pd.Series((_flags(source.base_entry_allowed,'base') & source.setup_id.isin(enabled)).to_numpy(bool),index=source.sid)
    unchanged_control=reference.apply_slot_policy(source,'G7_V4_LOCK2',enabled,base_config,5.)
    baseline=execute(dict(raw=raw,allowed=allowed,integer=True,clock=True,limit=2),base_config,'g7_v4_lock',cost=5.)
    parity2=reference.verify_control(baseline,unchanged_control,'G7 current 42-slot parity')
    old_path=ROOT/'outputs/full_backtest_20261029/shared_replay/g7_v4_lock2_portfolio_trades.csv'
    saved_parity=reference.verify_control(baseline,pd.read_csv(old_path),'Saved October29 current G7 replay')
    print('Baseline parity PASS',stats(baseline,days),flush=True)
    periods=dict(proof['periods']); periods['october']=[d for d in days if d.startswith('2026-10')]
    periods['later_diagnostic']=[d for d in days if d not in set(periods['development'])]
    all_rows=[]; daily_rows=[]; slot_rows=[]; acceptance_rows=[]; changed=[]; final_books={}; controls={}
    for cost in protocol['costs_bps']:
        for variant in protocol['variants']:
            name=variant['name']
            working=cancel_overextended_orders(raw,paths,protocol['entry_risk']['minimum_reference_reward_risk_retention'],cost) if variant['entry_check'] else raw.copy(deep=True)
            book=execute(dict(raw=working,allowed=allowed,integer=True,clock=True,limit=2),base_config,name,cost=cost,
                risk_per_trade_rs=protocol['risk_budget_rs'] if variant['risk_sizing'] else None,
                max_open_risk_rs=protocol['max_open_risk_rs'] if variant['open_risk_cap'] else None)
            book['g10_variant']=name; book['scope']='BACKTEST_ONLY'; book['execution_authority']=False
            if name=='g7_v4_lock': controls[cost]=book
            control=controls[cost]
            for period,calendar in periods.items():
                m=stats(book,calendar); differences=compare_rows(book,control,calendar)
                all_rows.append(dict(variant=name,cost_bps=cost,period=period,**m,**differences))
                if name!='g7_v4_lock' and period in ('all','development','validation','assessment','later_diagnostic','october'):
                    failures=acceptance(m,stats(control,calendar),differences['changed_days'])
                    acceptance_rows.append(dict(variant=name,cost_bps=cost,period=period,passes_numeric_screen=not failures,
                        failures='; '.join(failures),independent_validation=False,promotion_allowed=False))
            if cost==5.:
                final_books[name]=book
                book.to_csv(out/(name+'_portfolio_trades.csv'),index=False)
                for day in days: daily_rows.append(dict(variant=name,day=day,**stats(book,[day])))
                for setup,part in book.groupby('setup_id'):
                    slot_rows.append(dict(variant=name,setup_id=setup,slot=setup[:2]+':'+setup[2:4],side=setup.split('_')[1],**stats(part,days)))
                a=book.set_index('sid'); b=control.set_index('sid').loc[a.index]
                for sid in a.index:
                    changed.append(dict(variant=name,sid=int(sid),day=a.loc[sid,'day'],setup_id=a.loc[sid,'setup_id'],symbol=a.loc[sid,'tradingsymbol'],
                        baseline_executed=bool(b.loc[sid,'portfolio_executed']),variant_executed=bool(a.loc[sid,'portfolio_executed']),
                        baseline_pnl_rs=float(b.loc[sid,'portfolio_net_profit_rupees']),variant_pnl_rs=float(a.loc[sid,'portfolio_net_profit_rupees']),
                        net_change_rs=float(a.loc[sid,'portfolio_net_profit_rupees']-b.loc[sid,'portfolio_net_profit_rupees']),
                        status=a.loc[sid,'portfolio_status'],entry_check=a.loc[sid].get('g10_entry_check','NOT_APPLIED')))
            print(name,'cost',cost,stats(book,days)['net_pnl_rs'],flush=True)
    overall=pd.DataFrame(all_rows); daily=pd.DataFrame(daily_rows); checks=pd.DataFrame(acceptance_rows)
    overall.to_csv(out/'overall_comparison.csv',index=False); daily.to_csv(out/'daywise_comparison.csv',index=False)
    daily.loc[daily.day.str.startswith('2026-10')].to_csv(out/'october_daywise_comparison.csv',index=False)
    wide=daily.pivot(index='day',columns='variant',values='net_pnl_rs').reset_index()
    wide['g10_primary_minus_g7_rs']=wide[protocol['primary_candidate']]-wide.g7_v4_lock
    wide.to_csv(out/'daywise_net_comparison.csv',index=False)
    wide.loc[wide.day.str.startswith('2026-10')].to_csv(out/'october_daywise_net_comparison.csv',index=False)
    pd.DataFrame(slot_rows).to_csv(out/'slot_long_short_comparison.csv',index=False)
    pd.DataFrame(changed).to_csv(out/'opportunity_changes.csv',index=False); checks.to_csv(out/'acceptance_checks.csv',index=False)
    rng=np.random.default_rng(1931); bootstrap=[]
    for name in final_books:
        if name=='g7_v4_lock': continue
        delta=wide[name].to_numpy()-wide.g7_v4_lock.to_numpy()
        draws=rng.integers(0,len(days),size=(10000,len(days)))
        totals=delta[draws].sum(axis=1)
        bootstrap.append(dict(variant=name,observed_net_difference_rs=float(delta.sum()),
            conditional_bootstrap_total_delta_p025=float(np.quantile(totals,.025)),conditional_bootstrap_total_delta_p975=float(np.quantile(totals,.975)),
            seed=1931,replays=10000,unit='whole_session',independent_validation=False))
    dump(out/'paired_session_bootstrap.json',bootstrap)
    if sha(CONFIG)!=config_hash or sha(RECOVERY)!=recovery_hash or protected_files()!=protected:
        raise ValueError('Protected files or recovery evidence changed during backtest')
    summary=dict(variant_id=protocol['variant_id'],scope='BACKTEST_ONLY',execution_authority=False,
        primary_candidate=protocol['primary_candidate'],first_date=days[0],last_date=days[-1],sessions=len(days),
        opportunities=len(source),enabled_slots=len(enabled),independent_holdout=False,causal_universe_repaired=False,
        comparison_basis='SAME_INHERITED_UNIVERSE_POST_RANKING_EXECUTION_CHECK_NO_REPLACEMENT',
        protocol_sha256=config_hash,baseline_reproduction=[parity,parity2,saved_parity],
        full_results=overall.loc[overall.period.eq('all') & overall.cost_bps.eq(5.)].to_dict('records'),
        october_results=overall.loc[overall.period.eq('october') & overall.cost_bps.eq(5.)].to_dict('records'),
        protected_files_unchanged=True,limitations=protocol['limitations']+[
            'Missing a later-slot futures OI bar excludes the whole historical stock-day upstream. First38 sessions lack sealed all-stock raw snapshots to recover omitted candidates.',
            'The 595-row opportunity inventory includes unfilled and previously portfolio-rejected orders. Every variant freshly recomputes admission, fees, position limits and two-loss locks.',
            'Execution-risk vetoes occur after G7 ranking; no replacement is claimed. All original G7 selected opportunities remain in audit output.',
            'Reference-RR levels are used only for the entry quality check; accepted trades keep actual-fill-anchored G7 exit rules.',
            'Drawdown includes zero starting P&L and measures daily closes, not intraday or intrabar peak-to-trough risk.',
            'Whole-session bootstrap describes uncertainty conditional on this reused sample and inherited selection; it is not prospective validation.',
            'Later chronological blocks, including October, are reused-history diagnostics. No full-window winner is automatically selected or promoted.'])
    dump(out/'summary.json',summary)
    files=[CONFIG,reference.SOURCE/'artifact_manifest.json',reference.SOURCE/'portfolio_trades.csv',reference.G7_CACHE/'manifest.json',reference.G7_CACHE/'minute_paths.npz',old_path,RECOVERY]
    files += [ROOT/'research'/n for n in ('g10_backtest.py','g10_entry_risk.py','g10_portfolio.py','g7_v4_conservative_allocation.py','g7_v4_selected_backtest.py','g3_sl_target_simulator.py','g8_exit_engine.py')]
    files += [ROOT/'fno_v13_v10_g_10_backtest.py', *sorted((ROOT/'tests').glob('test_g10_*.py'))]
    dump(out/'source_provenance.json',dict(files={str(p):sha(p) for p in files if p.exists()},recovered_paths=recovery,protected_files=protected,
        repository_inspiration=protocol['inspiration']))
    write_report(out,summary,overall,daily,checks.loc[checks.cost_bps.eq(5.) & checks.period.eq('all')])
    dump(out/'artifact_manifest.json',dict(files={p.name:sha(p) for p in sorted(out.iterdir()) if p.is_file() and p.name!='artifact_manifest.json'}))
    dump(RESULT_ROOT/'latest.json',dict(output=str(out),summary=str(out/'summary.json'),report=str(out/'report.html'),execution_authority=False))
    print('G10_OUTPUT',out,flush=True)
    return out


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path)
    args=parser.parse_args(argv)
    run(args.output_dir)


if __name__=='__main__': main()
