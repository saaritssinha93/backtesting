"""Finite registered target/SL grid; freeze on development before later replay."""
from __future__ import annotations

import argparse
import json
import shutil
import time
from dataclasses import asdict, replace
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_backtest as engine

EPS = 1e-7


def dump(path: Path, value) -> None:
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,indent=2,default=str),encoding='utf-8')


def grid() -> list[dict]:
    result = []
    for stop_int in range(25,151,5):
        for target_int in range(25,201,5):
            stop,target = stop_int / 100,target_int / 100
            for policy in ('QUARTER','HALF','THREE_QUARTERS','LEGACY_T1'):
                t1 = 1.075 if policy == 'LEGACY_T1' else target * {'QUARTER':.25,'HALF':.5,'THREE_QUARTERS':.75}[policy]
                t1 = round(t1,6)
                if .1 <= t1 < target:
                    result.append(dict(candidate_id=len(result),policy=policy,partial_pct=.1,runner_stop='BREAKEVEN',
                        initial_stop_pct=stop,first_target_pct=t1,runner_target_pct=target))
    # Broad exit-family study on a predeclared coarse grid, plus fine single exits.
    for stop_int in range(25,151,5):
        for target_int in range(25,201,5):
            result.append(dict(candidate_id=len(result),policy='SINGLE',partial_pct=1.,runner_stop='INITIAL',
                initial_stop_pct=stop_int/100,first_target_pct=target_int/100,runner_target_pct=target_int/100))
    seen={(r['initial_stop_pct'],r['first_target_pct'],r['runner_target_pct'],r['partial_pct'],r['runner_stop']) for r in result}
    for stop_int in range(25,151,25):
        for target_int in range(25,201,25):
            for policy,ratio in [('QUARTER',.25),('HALF',.5),('THREE_QUARTERS',.75),('LEGACY_T1',None)]:
                t1=1.075 if ratio is None else round(target_int/100*ratio,6)
                if not .1<=t1<target_int/100:
                    continue
                for fraction in [.1,.25,.5,.75]:
                    for stop_rule in ['BREAKEVEN','INITIAL']:
                        key=(stop_int/100,t1,target_int/100,fraction,stop_rule)
                        if key in seen:
                            continue
                        seen.add(key)
                        result.append(dict(candidate_id=len(result),policy=policy,partial_pct=fraction,runner_stop=stop_rule,
                            initial_stop_pct=stop_int/100,first_target_pct=t1,runner_target_pct=target_int/100))
    return result


def config(row: dict) -> engine.V10Config:
    return engine.V10Config(**{key:float(row[key]) for key in ['initial_stop_pct','first_target_pct','runner_target_pct','partial_pct']},runner_stop=row['runner_stop'])


def periods(days: list) -> dict:
    return dict(TRAIN=[d for d in days if str(d)<='2026-08-13'],
        VALIDATION=[d for d in days if '2026-08-13'<str(d)<='2026-08-26'],
        LATER_PREVIOUSLY_SEEN=[d for d in days if str(d)>'2026-08-26'],
        SEPTEMBER=[d for d in days if '2026-09-01'<=str(d)<='2026-09-11'],FULL=days)


def metric(ledger: pd.DataFrame, days: list, *, cost_bps: float = 5.) -> dict:
    dates = pd.to_datetime(ledger.day).dt.strftime('%Y-%m-%d')
    selected = ledger.loc[dates.isin([str(d) for d in days])]
    executed = selected.loc[selected.portfolio_executed.eq(True)].copy()
    # Costs cannot change allocation: fixed capital, no cost/risk budget gate.
    pnl = executed.gross_return_pct.to_numpy(float) / 100 * executed.exposure_per_entry_rupees.to_numpy(float)
    pnl -= cost_bps / 10000 * executed.exposure_per_entry_rupees.to_numpy(float)
    dates = pd.to_datetime(executed.day).dt.strftime('%Y-%m-%d')
    daily = pd.Series(pnl,index=dates).groupby(level=0).sum().reindex([str(d) for d in days],fill_value=0.)
    curve = np.r_[0.,daily.cumsum().to_numpy()]
    gains,losses = pnl[pnl>EPS].sum(), -pnl[pnl < -EPS].sum()
    return dict(selected_orders=len(selected),trades=len(pnl),wins=int((pnl>EPS).sum()),
        losses=int((pnl < -EPS).sum()),breakeven=int((np.abs(pnl)<=EPS).sum()),
        win_rate_pct=float((pnl>EPS).mean()*100) if len(pnl) else 0.,
        profit_factor=float(gains/losses) if losses>EPS else (float('inf') if gains>EPS else 0.),
        net_profit_rupees=float(pnl.sum()),daily_close_drawdown_rupees=float(np.max(np.maximum.accumulate(curve)-curve)),
        median_trade_rupees=float(np.median(pnl)) if len(pnl) else 0.,cost_bps=cost_bps)


def assessed(row: dict, stats: dict, control: dict) -> dict:
    out = dict(row)
    reasons = []
    for period,minimum in [('TRAIN',15),('VALIDATION',10)]:
        for cost in [5,9]:
            m = stats[(period,cost)]
            prefix = f'{period}_{cost}bps'
            out.update({f'{prefix}_{field}':value for field,value in m.items()})
            if m['trades'] < minimum:
                reasons.append(f'{prefix}_SAMPLE')
            if m['net_profit_rupees']<=0 or not np.isfinite(m['net_profit_rupees']):
                reasons.append(f'{prefix}_NET')
            if m['win_rate_pct']<=control[(period,cost)]['win_rate_pct']+EPS:
                reasons.append(f'{prefix}_WIN_RATE_NOT_HIGHER')
            if m['profit_factor'] < (2. if cost==5 else 1.5):
                reasons.append(f'{prefix}_PF')
    out['passes_objective'] = not reasons
    out['reasons'] = '|'.join(reasons)
    out['minimum_split_win_pct'] = min(stats[(p,5)]['win_rate_pct'] for p in ['TRAIN','VALIDATION'])
    out['minimum_split_pf'] = min(stats[(p,5)]['profit_factor'] for p in ['TRAIN','VALIDATION'])
    out['development_net'] = sum(stats[(p,5)]['net_profit_rupees'] for p in ['TRAIN','VALIDATION'])
    return out


def choose(results: pd.DataFrame) -> tuple[dict,str,pd.DataFrame]:
    """Require at least two adjacent passing cells when such a stable group exists."""
    frame = results.copy()
    passed = frame.loc[frame.passes_objective]
    keys = {(r.policy,r.partial_pct,r.runner_stop,round(r.initial_stop_pct*100),round(r.runner_target_pct*100)) for r in passed.itertuples()}
    neighbors = []
    for r in frame.itertuples():
        s,t = round(r.initial_stop_pct*100),round(r.runner_target_pct*100)
        step=5 if r.policy=='SINGLE' or (r.partial_pct==.1 and r.runner_stop=='BREAKEVEN') else 25
        neighbors.append(sum((r.policy,r.partial_pct,r.runner_stop,s+ds,t+dt) in keys for ds,dt in [(-step,0),(step,0),(0,-step),(0,step)]))
    frame['adjacent_passing_cells'] = neighbors
    stable = frame.loc[frame.passes_objective & frame.adjacent_passing_cells.ge(2)]
    if len(stable):
        eligible,status = stable,'DEVELOPMENT_OBJECTIVE_PASSED_WITH_NEIGHBOR_SUPPORT'
    elif len(passed):
        eligible,status = frame.loc[frame.passes_objective],'DEVELOPMENT_OBJECTIVE_PASSED_ISOLATED_SETTINGS'
    else:
        eligible = frame.loc[(frame.TRAIN_5bps_net_profit_rupees>0) & (frame.VALIDATION_5bps_net_profit_rupees>0)
            & frame.minimum_split_pf.ge(1.5)]
        status = 'EXPERIMENTAL_BEST_AVAILABLE_OBJECTIVE_NOT_MET'
        if eligible.empty:
            eligible = frame
        # Fallback prioritizes the weaker split PF instead of inflating win rate.
        eligible = eligible.sort_values(['minimum_split_pf','minimum_split_win_pct','development_net','candidate_id'],
            ascending=[False,False,False,True],kind='stable')
        return eligible.iloc[0].to_dict(),status,frame
    eligible = eligible.sort_values(['minimum_split_win_pct','minimum_split_pf','development_net','candidate_id'],
        ascending=[False,False,False,True],kind='stable')
    return eligible.iloc[0].to_dict(),status,frame


def save(folder: Path, trades,ledger,summary) -> None:
    folder.mkdir(parents=True,exist_ok=True)
    trades.to_csv(folder/'selected_trades.csv',index=False)
    ledger.to_csv(folder/'portfolio_trades.csv',index=False)
    dump(folder/'summary.json',summary)


def run(output: Path = engine.DEFAULT_OUTPUT, source: Path = engine.DEFAULT_SOURCE) -> dict:
    output.mkdir(parents=True,exist_ok=True)
    candidates = grid()
    source_files = [Path(__file__).resolve(),Path(engine.__file__).resolve()]
    code_hashes = {str(path):engine.sha(path) for path in source_files}
    protocol = dict(grid_count=len(candidates),grid='SL 0.25..1.50 by .05; runner .25..2.00 by .05; T1=25%,50%,75% of runner or legacy1.075 when >=.10 and <runner',
        added_family_grid='Single target exits on same .05 grid; partial fractions .1,.25,.5,.75 and BE/INITIAL runner stops on .25 stop/runner grid; no duplicate parameter tuples',
        registered_before_results=True,change_scope='Targets, stops, partial fraction and runner stop as authorized; V9 entries/scale/capital/slots/cost/entry expiry unchanged',
        objective='Win rate after costs strictly higher than V9 in TRAIN and VALIDATION; PF>=2 at5bps and >=1.5 at9bps; higher win rate at9bps too; positive net, >=15/10 fills',
        choice='Prefer >=2 adjacent passing grid cells; then maximum weaker-split win%, minimum split PF, development PnL, candidate id',
        fallback='Best weaker-split PF among profitable splits/PF>=1.5, explicitly experimental if objective unmet',
        splits={'train_end':'2026-08-13','validation_end':'2026-08-26','later_end':'2026-09-11'},
        evidence='All history previously seen. Finite grid search amplifies selection bias; no untouched holdout. Later outcomes never choose settings.',
        money_comparison_tolerance_rupees=EPS,source_hashes=code_hashes)
    dump(output/'research_protocol.json',protocol)
    pd.DataFrame(candidates).to_csv(output/'registered_grid.csv',index=False)
    print(f'[V10] Registered {len(candidates)} bounded target/stop combinations.',flush=True)
    dataset = engine.load_source(source)
    orders,paths,base = dataset['orders'],dataset['paths'],dataset['v9_config']
    groups = periods(dataset['days'])
    # Validate baseline against the actual published V9 ledger before any tuning.
    baseline_trades = engine.v9.replay_candidates(orders,paths,base)
    baseline_ledger,baseline_summary = engine.v9.v6.apply_portfolio_constraints(baseline_trades,base.portfolio_config())
    prior = pd.read_csv(source/'final/V13_V9_FROZEN/portfolio_trades.csv',float_precision='round_trip')
    dump(output/'baseline_parity.json',engine.v9.assert_control_parity(prior,baseline_ledger))
    devdays = groups['TRAIN']+groups['VALIDATION']
    devorders = orders.loc[pd.to_datetime(orders.day).dt.date.isin(devdays)].reset_index(drop=True)
    control = {(p,c):metric(baseline_ledger,groups[p],cost_bps=c) for p in ['TRAIN','VALIDATION'] for c in [5,9]}
    results = []
    began = time.monotonic()
    for i,row in enumerate(candidates,1):
        _,ledger,_ = engine.evaluate_orders(devorders,paths,config(row),base,validate_paths=False)
        stats = {(p,c):metric(ledger,groups[p],cost_bps=c) for p in ['TRAIN','VALIDATION'] for c in [5,9]}
        results.append(assessed(row,stats,control))
        if i%100==0 or i==len(candidates):
            pd.DataFrame(results).to_csv(output/'development_grid_progress.csv',index=False)
            print(f'[V10 SEARCH] {i}/{len(candidates)}; passing={sum(r["passes_objective"] for r in results)}; elapsed={time.monotonic()-began:.1f}s',flush=True)
    chosen,status,results_frame = choose(pd.DataFrame(results))
    results_frame.to_csv(output/'development_grid_results.csv',index=False)
    cfg = config(chosen)
    dump(output/'frozen_config.json',asdict(cfg))
    dump(output/'selection_freeze.json',dict(config=asdict(cfg),candidate_id=int(chosen['candidate_id']),policy=chosen['policy'],
        status=status,passing_settings=int(results_frame.passes_objective.sum()),adjacent_passing_cells=int(chosen['adjacent_passing_cells']),
        later_results_used=False,protocol_sha256=engine.sha(output/'research_protocol.json'),
        grid_sha256=engine.sha(output/'development_grid_results.csv')))
    print(f'[V10 FROZEN] {asdict(cfg)}; {status}',flush=True)
    final = {'V9_CONTROL':(baseline_trades,baseline_ledger,baseline_summary),
        'V9_TARGET_CAPPED_2':engine.evaluate_orders(orders,paths,engine.V10Config(),base),
        'V13_V10':engine.evaluate_orders(orders,paths,cfg,base)}
    metrics,stress,daily = [],[],[]
    for name,(trades,ledger,summary) in final.items():
        save(output/'final'/name,trades,ledger,summary)
        for period,days in groups.items():
            metrics.append(dict(name=name,period=period,**metric(ledger,days)))
            stress.append(dict(name=name,period=period,**metric(ledger,days,cost_bps=9)))
        actual = ledger.loc[ledger.portfolio_executed.eq(True)]
        byday=actual.groupby('day').net_profit_rupees.sum()
        for day in dataset['days']:
            daily.append(dict(name=name,day=day,net_profit_rupees=float(byday.get(day,0))))
    metrics_frame = pd.DataFrame(metrics)
    metrics_frame.to_csv(output/'final_metrics.csv',index=False)
    pd.DataFrame(stress).to_csv(output/'cost_stress_metrics.csv',index=False)
    pd.DataFrame(daily).to_csv(output/'final_daily.csv',index=False)
    # Attribute changed allocation, which follows from different exit times.
    keys = ['day','tradingsymbol','side','setup_id']
    frames = [x[1].loc[x[1].portfolio_executed,keys+['net_profit_rupees','entry_ts','exit_ts','exit_reason']] for x in [final['V9_CONTROL'],final['V13_V10']]]
    bridge = frames[0].merge(frames[1],on=keys,how='outer',suffixes=('_v9','_v10'),indicator=True)
    bridge['change']=bridge['_merge'].astype(str).map({'both':'RETAINED','left_only':'REMOVED','right_only':'ADDED'})
    bridge['pnl_delta_rupees']=bridge.net_profit_rupees_v10.fillna(0)-bridge.net_profit_rupees_v9.fillna(0)
    bridge.to_csv(output/'execution_attribution.csv',index=False)
    for name,(trades,ledger,_) in final.items():
        if not trades.sid.tolist()==baseline_trades.sid.tolist():
            raise AssertionError('Selection changed during exit-only study')
        for column in ['filled','entry_ts','entry_price']:
            pd.testing.assert_series_equal(trades[column].reset_index(drop=True),baseline_trades[column].reset_index(drop=True),check_names=False)
    report(output,cfg,status,metrics_frame,pd.DataFrame(stress),chosen,len(candidates))
    for path in source_files:
        if engine.sha(path)!=code_hashes[str(path)]:
            raise AssertionError('Source changed during research replay')
    snapshot=output/'source_snapshot'
    snapshot.mkdir(exist_ok=True)
    for path in source_files+list(Path(__file__).parent.glob('tests/test_fno_v13_v10*.py')):
        shutil.copy2(path,snapshot/path.name)
    dump(output/'research_manifest.json',dict(status=status,iterations=len(candidates),code_sha256=code_hashes,
        v9_manifest_sha256=engine.sha(source/'dataset/dataset_manifest.json'),
        artifacts={str(path.relative_to(output)):engine.sha(path) for path in sorted(output.rglob('*')) if path.is_file() and path.name!='research_manifest.json'}))
    print(metrics_frame.loc[metrics_frame.period.isin(['FULL','SEPTEMBER'])].to_string(index=False),flush=True)
    return dict(output=str(output),config=asdict(cfg),status=status,iterations=len(candidates))


def report(output,cfg,status,metrics,stress,chosen,iterations):
    cols=['name','period','trades','wins','losses','win_rate_pct','profit_factor','net_profit_rupees','daily_close_drawdown_rupees']
    table=lambda frame:frame.to_markdown(index=False,floatfmt='.2f')
    text=[ '# V13-v10 target and stop research','',f'Frozen settings: initial SL **{cfg.initial_stop_pct:.3f}%**, first target **{cfg.first_target_pct:.3f}%**, runner target **{cfg.runner_target_pct:.3f}%**.','',
        f'Status: {status}. Searched {iterations:,} registered combinations using TRAIN/VALIDATION only. Adjacent passing cells: {chosen["adjacent_passing_cells"]}.','',
        f'V9 entry selection is unchanged. Book {cfg.partial_pct*100:.0f}% at the first target. Runner stop policy: {cfg.runner_stop}; 15:15 square-off. If 100% is booked there is no remaining runner. Every candidate reruns the same three-position portfolio because exit times change capital availability.','',
        table(metrics[cols]),'', '## Nine-basis-point cost sensitivity','',table(stress.loc[stress.period.isin(['FULL','SEPTEMBER']),cols]),'',
        '## Interpretation','',
        '- A win means positive modeled net P&L after costs; values within Rs 0.0000001 of zero are break-even. First-target hits alone are not counted as wins.',
        '- Every V10 initial stop is at most 1.5%, and both targets are at most 2%. Adverse price gaps can still produce a realized loss beyond the configured stop distance.',
        '- The 2.6% runner is retained only for the original V9 comparison. V9_TARGET_CAPPED_2 changes that runner to 2% without the grid optimization.',
        '- Five-basis-point headline and nine-basis-point stressed P&L use the inherited cash-price execution, futures-OI signals, fractional notional, Rs 100,000 capital per trade, Rs 300,000 portfolio and 5x exposure. No verified exchange-lot or options-premium P&L is claimed.',
        '- All 31 source-eligible sessions and later September history have been seen before. Thousands of iterations increase selection bias. This is historical research with no untouched test; later results do not choose the default.',
        '- Minute OHLC uses the inherited conservative stop-first ambiguity rule. Drawdown is daily close, not maximum intraday drawdown.',
        '- Higher win rate can mean smaller average wins and less net profit. Inspect both P&L and cost stress before treating it as an improvement.', '',
        '## Reproduce','', '```powershell','python -B fno_v13_v10_research.py','python -B fno_v13_v10_backtest.py','```','',
        'The registered grid, every development result, frozen decision, final trade ledgers and execution attribution accompany this report.']
    (output/'V13_V10_DETAILED_RESULTS.md').write_text('\n'.join(text),encoding='utf-8')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir',type=Path,default=engine.DEFAULT_OUTPUT)
    parser.add_argument('--source-dir',type=Path,default=engine.DEFAULT_SOURCE)
    args=parser.parse_args()
    print(json.dumps(run(args.output_dir,args.source_dir),indent=2))
