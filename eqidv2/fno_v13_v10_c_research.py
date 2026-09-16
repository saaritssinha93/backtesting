"""Build and report deterministic V13-v10-C against V10-A and V10-B."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd

import fno_v13_v10_c_backtest as c

b, a, v10, r = c.b, c.b.a, c.b.a.v10, c.b.a.metrics


def run():
    output = c.DEFAULT_OUTPUT
    output.mkdir(parents=True, exist_ok=True)
    source_b = json.loads(c.DEFAULT_SOURCE_CONFIG.read_text(encoding='utf-8'))
    settings = c.transform(source_b)
    r.dump(output / 'frozen_config.json', settings)
    audit = []
    for name, old in [('DEFAULT', source_b['default']), *source_b['setups'].items()]:
        new = settings['default'] if name == 'DEFAULT' else settings['setups'][name]
        audit.append(dict(setup_id=name, old_stop_pct=old['stop_pct'], old_target_pct=old['target_pct'],
                          new_stop_pct=new['stop_pct'], new_target_pct=new['target_pct'], reward_risk=2.,
                          stop_unchanged=old['stop_pct'] == new['stop_pct']))
    pd.DataFrame(audit).to_csv(output / 'transformation_audit.csv', index=False)
    dataset = v10.load_source()
    result_c = c.evaluate(dataset, settings)
    r.save(output / 'final/V13_V10_C', *result_c)
    result_b = b.evaluate(dataset, source_b)
    r.save(output / 'final/V13_V10_B_CONTROL', *result_b)
    source_a = json.loads(b.DEFAULT_SOURCE_CONFIG.read_text(encoding='utf-8'))
    result_a = a.evaluate(dataset, source_a)
    r.save(output / 'final/V13_V10_A_CONTROL', *result_a)
    groups = r.periods(dataset['days'])
    groups.update({name: [d for d in dataset['days'] if str(d).startswith(prefix)]
                   for name, prefix in [('JULY','2026-07'), ('AUGUST','2026-08')]})
    rows, daily = [], []
    for name, result in [('V10-A',result_a), ('V10-B',result_b), ('V10-C',result_c)]:
        ledger = result[1]
        for period, days in groups.items():
            for cost in [5,9]:
                rows.append(dict(version=name, period=period, **r.metric(ledger,days,cost_bps=cost)))
        for day in dataset['days']:
            daily.append(dict(version=name, day=day, **r.metric(ledger,[day])))
    metrics = pd.DataFrame(rows)
    metrics.to_csv(output/'comparison_metrics.csv',index=False)
    pd.DataFrame(daily).to_csv(output/'daily_results.csv',index=False)
    ledger = result_c[1]
    slots=[]
    for setup,pair in settings['setups'].items():
        subset=ledger.loc[ledger.setup_id.eq(setup)]
        slots.append(dict(setup_id=setup,
            signal_time=(pd.Timestamp('2026-01-01 '+setup[:2]+':'+setup[2:4])-pd.Timedelta(minutes=1)).strftime('%H:%M'),
            side=setup.split('_')[1],**pair,reward_risk=2.,
            **{'FULL_'+key:value for key,value in r.metric(subset,groups['FULL']).items()}))
    pd.DataFrame(slots).to_csv(output/'slot_settings_and_results.csv',index=False)
    exits=ledger.loc[ledger.portfolio_executed].groupby('exit_reason').agg(
        trades=('sid','size'),net_profit_rupees=('portfolio_net_profit_rupees','sum'))
    exits.to_csv(output/'exit_reason_summary.csv')
    shown=metrics.loc[metrics.cost_bps.eq(5)&metrics.period.isin(['FULL','JULY','AUGUST','SEPTEMBER']),
        ['version','period','trades','wins','losses','win_rate_pct','profit_factor','net_profit_rupees','daily_close_drawdown_rupees']]
    parameter=pd.DataFrame(audit).loc[lambda x:x.setup_id.ne('DEFAULT'),
        ['setup_id','new_stop_pct','new_target_pct','reward_risk']]
    report='\n\n'.join(['# V13-v10-C detailed results',
        'V10-C keeps every V10-B SL unchanged and replaces every target with exactly 2x its SL. Full-position exits only; no partial exit or break-even move.',
        shown.to_markdown(index=False,floatfmt='.2f'),'## V10-C settings',parameter.to_markdown(index=False,floatfmt='.2f'),
        'The strategy retains all 115 selected orders, exact one-minute confirmation, next-minute entry, S+10 expiry, 15:15 square-off, stop-first same-bar ordering and adverse-gap stop fills. Exit changes receive a fresh chronological three-position portfolio replay.',
        'Accounting: Rs 100,000 capital per entry, 5x exposure, Rs 300,000 portfolio and flat 5bps round-trip cost. Results use modeled cash-stock execution with futures OI selection. A and B are derived from settings fitted on this same history, so C is an in-sample sensitivity test rather than independent performance evidence.'])
    (output/'V13_V10_C_DETAILED_RESULTS.md').write_text(report+'\n',encoding='utf-8')
    sources=[Path(__file__),Path(c.__file__),Path(b.__file__),Path(a.__file__),Path(v10.__file__)]
    snap=output/'source_snapshot';snap.mkdir(exist_ok=True)
    for path in sources+[Path('tests/test_fno_v13_v10_c.py')]:
        if path.is_file():shutil.copy2(path,snap/path.name)
    r.dump(output/'research_manifest.json',dict(complete=True,source_config=str(c.DEFAULT_SOURCE_CONFIG),
        source_config_sha256=v10.sha(c.DEFAULT_SOURCE_CONFIG),code_sha256={str(p.resolve()):v10.sha(p) for p in sources},
        artifacts={str(p.relative_to(output)):v10.sha(p) for p in sorted(output.rglob('*')) if p.is_file() and p.name!='research_manifest.json'}))
    print(shown.to_string(index=False))


if __name__=='__main__':
    run()
