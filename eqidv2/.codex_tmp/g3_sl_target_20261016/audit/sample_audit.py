"""Read-only sample inventory; no parameter fitting or production edits."""
from pathlib import Path
import hashlib
import json
import math

import pandas as pd

SOURCE = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3/backtests/runs/2026-10-15/20261015T171145325060_264a0ea2_current_full_history')
OUT = Path(__file__).resolve().parent


def wilson(wins, count):
    if not count:
        return None, None
    z = 1.959963984540054
    p = wins/count
    denom = 1+z*z/count
    center = (p+z*z/(2*count))/denom
    half = z*math.sqrt(p*(1-p)/count+z*z/(4*count*count))/denom
    return center-half, center+half


def counts(frame, period_days):
    selected = frame.loc[frame.day.isin(period_days)]
    native = selected.loc[selected.filled]
    executed = selected.loc[selected.portfolio_executed]
    pnl = executed.portfolio_net_profit_rupees
    daily = selected.groupby('day').portfolio_net_profit_rupees.sum().reindex(period_days, fill_value=0)
    wins = int((pnl>0).sum())
    low, high = wilson(wins, len(executed))
    return dict(selected=len(selected), native=len(native), executed=len(executed),
                native_days=native.day.nunique(), executed_days=executed.day.nunique(),
                native_symbols=native.tradingsymbol.nunique(), wins=wins,
                losses=int((pnl<0).sum()), net_pnl_rupees=float(pnl.sum()),
                positive_days=int((daily>0).sum()), losing_days=int((daily<0).sum()),
                zero_pnl_days=int((daily==0).sum()), positive_day_rate_pct=float((daily>0).mean()*100),
                observed_win_rate_pct=100*wins/len(executed) if len(executed) else None,
                iid_wilson95_low_pct=100*low if low is not None else None,
                iid_wilson95_high_pct=100*high if high is not None else None)


def main():
    summary = json.loads((SOURCE/'summary.json').read_text())
    config = json.loads((SOURCE/'config.json').read_text())
    ledger = pd.read_csv(SOURCE/'portfolio_trades.csv')
    assert ledger.filled.dtype == bool and ledger.portfolio_executed.dtype == bool
    days = summary['days']
    periods = {'development35': days[:35], 'later17': days[35:], 'all52': days}
    rows = []
    for spec in config['active_setups']:
        sid = spec['confirmation_end'].replace(':', '')+'_'+spec['side']
        row = dict(setup_id=sid, signal_time=spec['signal_end'], confirmation_time=spec['confirmation_end'],
                   side=spec['side'], current_target_pct=spec['target_pct'], current_stop_pct=spec['stop_pct'])
        group = ledger.loc[ledger.setup_id.eq(sid)]
        for name, calendar in periods.items():
            row.update({name+'_'+k:v for k,v in counts(group, calendar).items()})
        row['meets_15_development_and_5_later_native'] = row['development35_native']>=15 and row['later17_native']>=5
        row['meets_10_development_and_5_later_native'] = row['development35_native']>=10 and row['later17_native']>=5
        rows.append(row)
    inventory = pd.DataFrame(rows).sort_values('setup_id')
    inventory.to_csv(OUT/'per_setup_samples.csv', index=False)
    # Fixed clock groups specified without consulting PnL or fitted exits.
    spec_map = {row['setup_id']:row['signal_time'] for row in rows}
    ledger['signal_clock'] = ledger.setup_id.map(spec_map)
    ledger['clock_group'] = ledger.signal_clock.map(lambda x: 'opening_0920_1000' if x<='10:00' else
                                                    'midmorning_1005_1125' if x<='11:25' else 'midday_1135_1330')
    pooled=[]
    for (group, side), frame in ledger.groupby(['clock_group','side']):
        row={'clock_group':group, 'side':side}
        for name, calendar in periods.items():
            row.update({name+'_'+k:v for k,v in counts(frame, calendar).items()})
        pooled.append(row)
    pd.DataFrame(pooled).to_csv(OUT/'fixed_clock_group_samples.csv',index=False)
    report = dict(source_run=str(SOURCE), source_portfolio_sha256=hashlib.sha256((SOURCE/'portfolio_trades.csv').read_bytes()).hexdigest(),
                  scope='READ_ONLY_SAMPLE_AUDIT_NO_PARAMETER_FIT', active_setups=len(inventory),
                  periods={name:dict(first=cal[0],last=cal[-1],sessions=len(cal),**counts(ledger,cal)) for name,cal in periods.items()},
                  median_native_fills_per_setup=float(inventory.all52_native.median()),
                  max_native_fills_per_setup=int(inventory.all52_native.max()),
                  native_fills_under10=int((inventory.all52_native<10).sum()),
                  native_fills_under20=int((inventory.all52_native<20).sum()),
                  development_native_fills_under10=int((inventory.development35_native<10).sum()),
                  zero_later_fill_setups=inventory.loc[inventory.later17_native.eq(0),'setup_id'].tolist(),
                  exploratory_10plus5_setups=inventory.loc[inventory.meets_10_development_and_5_later_native,'setup_id'].tolist(),
                  minimum_15plus5_setups=inventory.loc[inventory.meets_15_development_and_5_later_native,'setup_id'].tolist(),
                  naive_full_wilson_lower_at_least70_setups=inventory.loc[inventory.all52_iid_wilson95_low_pct.ge(70),'setup_id'].tolist(),
                  qualifications=[
                      'Sample thresholds are conservative design choices, not guarantees of statistical power.',
                      'Wilson intervals assume independent Bernoulli trades; correlated stock/day outcomes and historical selection invalidate nominal coverage.',
                      '51 separate grids would multiply selection opportunities and exploit very small samples.',
                      'The strategy and targets already used this historical cohort; later17 is a diagnostic, not an untouched holdout.',
                      'Choose parameters only on development35, freeze, then evaluate later17 once; no reranking on later17.',
                      'All52 refits can be presented as exploratory current fit only and require future frozen forward assessment.',
                      'Use chronological day blocks for uncertainty; do not random-split trades or shuffle within-day market regimes.',
                      'Track whipsaw using causal path simulations and retained common-trade counterfactuals; final MAE alone does not give the time order of stop/target hits.',
                  ])
    (OUT/'sample_audit.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report,indent=2))
    print(pd.DataFrame(pooled)[['clock_group','side','development35_native','development35_executed','later17_native','later17_executed']].to_string(index=False))


if __name__=='__main__':
    main()
