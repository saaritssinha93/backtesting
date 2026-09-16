"""Build, replay, compare and document deterministic V13-v10-B."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd

import fno_v13_v10_b_backtest as b

a, v10, r = b.a, b.a.v10, b.a.metrics


def run():
    output = b.DEFAULT_OUTPUT
    output.mkdir(parents=True, exist_ok=True)
    source = json.loads(b.DEFAULT_SOURCE_CONFIG.read_text(encoding='utf-8'))
    settings = b.transform(source)
    r.dump(output / 'frozen_config.json', settings)
    audit = []
    for name, old in [('DEFAULT', source['default']), *source['setups'].items()]:
        new = settings['default'] if name == 'DEFAULT' else settings['setups'][name]
        audit.append(dict(setup_id=name, old_stop_pct=old['stop_pct'], old_target_pct=old['target_pct'],
            old_reward_risk=old['target_pct']/old['stop_pct'], new_stop_pct=new['stop_pct'],
            new_target_pct=new['target_pct'], new_reward_risk=new['target_pct']/new['stop_pct'],
            changed=old != new, target_capped_at_3=bool(old['stop_pct'] < .5 and old['target_pct']/old['stop_pct']*.6 > 3)))
    pd.DataFrame(audit).to_csv(output / 'transformation_audit.csv', index=False)
    dataset = v10.load_source()
    result_b = b.evaluate(dataset, settings)
    r.save(output / 'final/V13_V10_B', *result_b)
    result_a = a.evaluate(dataset, source)
    r.save(output / 'final/V13_V10_A_CONTROL', *result_a)
    v10_cfg = v10.V10Config(**json.loads((v10.DEFAULT_OUTPUT / 'balanced/frozen_config.json').read_text()))
    result_v10 = v10.evaluate_orders(dataset['orders'], dataset['paths'], v10_cfg, dataset['v9_config'])
    r.save(output / 'final/V13_V10_CONTROL', *result_v10)
    groups = r.periods(dataset['days'])
    groups.update({name: [d for d in dataset['days'] if str(d).startswith(prefix)]
                   for name, prefix in [('JULY','2026-07'), ('AUGUST','2026-08')]})
    rows, daily = [], []
    for name, result in [('V10', result_v10), ('V10-A', result_a), ('V10-B', result_b)]:
        ledger = result[1]
        for period, days in groups.items():
            for cost in [5, 9]:
                rows.append(dict(version=name, period=period, **r.metric(ledger, days, cost_bps=cost)))
        for day in dataset['days']:
            daily.append(dict(version=name, day=day, **r.metric(ledger, [day])))
    metrics = pd.DataFrame(rows)
    metrics.to_csv(output / 'comparison_metrics.csv', index=False)
    pd.DataFrame(daily).to_csv(output / 'daily_results.csv', index=False)
    ledger = result_b[1]
    slot_rows = []
    for setup, pair in settings['setups'].items():
        subset = ledger.loc[ledger.setup_id.eq(setup)]
        slot_rows.append(dict(setup_id=setup, signal_time=(pd.Timestamp('2026-01-01 '+setup[:2]+':'+setup[2:4])-pd.Timedelta(minutes=1)).strftime('%H:%M'),
            side=setup.split('_')[1], **pair, reward_risk=pair['target_pct']/pair['stop_pct'],
            **{('FULL_'+key): val for key,val in r.metric(subset, groups['FULL']).items()}))
    pd.DataFrame(slot_rows).to_csv(output / 'slot_settings_and_results.csv', index=False)
    exit_summary = ledger.loc[ledger.portfolio_executed].groupby('exit_reason').agg(
        trades=('sid','size'), net_profit_rupees=('portfolio_net_profit_rupees','sum'))
    exit_summary.to_csv(output / 'exit_reason_summary.csv')
    report_metrics = metrics.loc[metrics.cost_bps.eq(5) & metrics.version.isin(['V10-A','V10-B']) &
                                 metrics.period.isin(['FULL','JULY','AUGUST','SEPTEMBER']),
        ['version','period','trades','wins','losses','win_rate_pct','profit_factor','net_profit_rupees','daily_close_drawdown_rupees']]
    setting_table = pd.DataFrame(audit).loc[lambda x:x.setup_id.ne('DEFAULT'),
        ['setup_id','old_stop_pct','old_target_pct','new_stop_pct','new_target_pct','new_reward_risk','target_capped_at_3']]
    report = '\n\n'.join(['# V13-v10-B detailed results',
        'Deterministic V10-A adjustment: where SL was below 0.50%, V10-B sets SL to 0.60% and scales the target by the original target:SL ratio. Adjusted targets are rounded to 0.01% and capped at 3.00%. All other pairs are unchanged. Full exits only; no partial or break-even rule.',
        report_metrics.to_markdown(index=False, floatfmt='.2f'),
        '## Parameter transformation', setting_table.to_markdown(index=False, floatfmt='.2f'),
        'V10-B keeps all 115 V10-A selections, next-minute entries, S+10 expiry, 15:15 square-off, three-position chronological allocation, stop-first same-bar policy and adverse-gap fills. Changed exits can alter the number of portfolio executions.',
        'Accounting: Rs 100,000 capital and 5x exposure per trade, Rs 300,000 portfolio, flat 5bps round-trip costs. Results are modeled cash-price execution using futures OI selections. V10-A was fitted on this same history; V10-B is a requested deterministic sensitivity test and not an untouched performance estimate.',
        'Artifacts include the complete selected-trade and portfolio ledgers, monthly/daily metrics, 9bps stress, exit summary and transformation audit.'])
    (output / 'V13_V10_B_DETAILED_RESULTS.md').write_text(report+'\n', encoding='utf-8')
    sources = [Path(__file__), Path(b.__file__), Path(a.__file__), Path(v10.__file__)]
    snapshot = output / 'source_snapshot'; snapshot.mkdir(exist_ok=True)
    for path in sources + [Path('tests/test_fno_v13_v10_b.py')]:
        if path.is_file(): shutil.copy2(path, snapshot/path.name)
    r.dump(output / 'research_manifest.json', dict(complete=True, source_config=str(b.DEFAULT_SOURCE_CONFIG),
        source_config_sha256=v10.sha(b.DEFAULT_SOURCE_CONFIG), code_sha256={str(p.resolve()):v10.sha(p) for p in sources},
        artifacts={str(p.relative_to(output)):v10.sha(p) for p in sorted(output.rglob('*')) if p.is_file() and p.name!='research_manifest.json'}))
    print(report_metrics.to_string(index=False))


if __name__ == '__main__':
    run()
