"""Reproduce V10-F's fixed-volume selection study and final results."""
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_f_backtest as f
import fno_v13_v10_d_research as dr

e, b, d, v10, v9, r = f.e, f.b, f.d, f.v10, f.v9, f.r
C = f.SelectionChange
CANDIDATES = {
    'E_CONTROL': C(),
    'EXTRA_SETUP_ORDER': C(extra_setup_entries=1),
    'OI_075': C(oi_multiplier=.75),
    'OI_050': C(oi_multiplier=.5),
    'PRICE_075': C(price_multiplier=.75),
    'PRICE_050': C(price_multiplier=.5),
    'BODY_MINUS_010': C(body_reduction=.1),
    'WICK_PLUS_010': C(wick_increase=.1),
    'ENTRY_EXPIRY_15': C(entry_expiry_minutes=15),
    'OI_075_EXTRA_ORDER': C(oi_multiplier=.75, extra_setup_entries=1),
    'PRICE_075_EXTRA_ORDER': C(price_multiplier=.75, extra_setup_entries=1),
    'OI_PRICE_075': C(oi_multiplier=.75, price_multiplier=.75),
    'OI_075_PRESERVE': C(oi_multiplier=.75, preserve_primary=True),
    'OI_050_PRESERVE': C(oi_multiplier=.5, preserve_primary=True),
    'PRICE_075_PRESERVE': C(price_multiplier=.75, preserve_primary=True),
    'PRICE_050_PRESERVE': C(price_multiplier=.5, preserve_primary=True),
    'OI_PRICE_075_PRESERVE': C(oi_multiplier=.75, price_multiplier=.75, preserve_primary=True),
    'OI_050_PRESERVE_BODY060': C(oi_multiplier=.5, preserve_primary=True, additional_min_body=.6),
    'PRICE_050_PRESERVE_BODY060': C(price_multiplier=.5, preserve_primary=True, additional_min_body=.6),
    'OI_075_SHORT_ONLY': C(oi_multiplier=.75, expansion_side='SHORT'),
    'OI_050_SHORT_ONLY': C(oi_multiplier=.5, expansion_side='SHORT'),
    'OI_075_LONG_ONLY': C(oi_multiplier=.75, expansion_side='LONG'),
    'OI_050_LONG_ONLY': C(oi_multiplier=.5, expansion_side='LONG'),
}
FINAL_CANDIDATE = 'OI_050_SHORT_ONLY'


def finalize_manifest(output):
    sources = [Path(__file__), Path(f.__file__), Path(e.__file__), Path(d.__file__),
               Path(b.__file__), Path(dr.__file__), Path('tests/test_fno_v13_v10_f.py')]
    snap = output / 'source_snapshot'
    snap.mkdir(exist_ok=True)
    for path in sources:
        shutil.copy2(path, snap / path.name)
    r.dump(output / 'research_manifest.json', dict(
        complete=True, source_exit_config=str(f.DEFAULT_SOURCE_CONFIG),
        source_exit_config_sha256=v10.sha(f.DEFAULT_SOURCE_CONFIG),
        source_dataset_manifest_sha256=v10.sha(f.DEFAULT_SOURCE / 'dataset/dataset_manifest.json'),
        code_sha256={str(path.resolve()): v10.sha(path) for path in sources},
        artifacts={str(path.relative_to(output)): v10.sha(path)
                   for path in sorted(output.rglob('*'))
                   if path.is_file() and path.name != 'research_manifest.json'},
    ))


def run(probe_only=False):
    output = f.DEFAULT_OUTPUT
    output.mkdir(parents=True, exist_ok=True)
    r.dump(output / 'experiment_plan.json', dict(
        volume_ratio_fixed=1.2, exits='EXACT_V10_B', capital_and_portfolio='UNCHANGED',
        candidates={name: asdict(change) for name, change in CANDIDATES.items()},
        objective='Increase selected and executed orders; assess >=20% more executions, full win>=65%, PF>=3; show September separately.',
        evidence='ALL_PERIODS_PREVIOUSLY_SEEN; gates are research preferences, not statistical validation',
        adaptive_stages=[
            '12 initial control/general threshold and entry candidates',
            '7 primary-preservation and added-candle-quality follow-ups',
            '4 long/short OI follow-ups after reviewing the direction breakdown',
        ],
        decision='OI_050_SHORT_ONLY increases executions 17%, short of the preferred 20%; full win>=65% and PF>=3, with higher profit. No additional search to force the target.',
    ))
    source_exit = json.loads(f.DEFAULT_SOURCE_CONFIG.read_text(encoding='utf-8'))
    base = v10.load_source()
    signals = pd.read_parquet(f.DEFAULT_SOURCE / 'dataset/signals.parquet')
    days = base['days']
    groups = {'FULL': days, **{name: [day for day in days if str(day).startswith(prefix)]
              for name, prefix in [('JULY','2026-07'), ('AUGUST','2026-08'), ('SEPTEMBER','2026-09')]}}
    results, datasets, rows = {}, {}, []
    with np.load(f.DEFAULT_SOURCE / 'dataset/paths.npz', allow_pickle=False) as archive:
        for name, change in CANDIDATES.items():
            orders = f.select_orders(signals, base['v9_config'], change)
            dataset = f.orders_dataset(base, signals, archive, orders)
            result = f.evaluate(dataset, f.config(source_exit, change))
            results[name], datasets[name] = result, dataset
            for period, period_days in groups.items():
                rows.append(dict(candidate=name, period=period, **r.metric(result[1], period_days)))
    sweep = pd.DataFrame(rows)
    sweep.to_csv(output / 'selection_parameter_sweep.csv', index=False)
    cols = ['candidate','period','selected_orders','trades','wins','losses',
            'win_rate_pct','profit_factor','net_profit_rupees','daily_close_drawdown_rupees']
    print(sweep.loc[sweep.period.isin(['FULL','SEPTEMBER']), cols].to_string(index=False))
    if probe_only:
        return

    assert CANDIDATES[FINAL_CANDIDATE] == f.DEFAULT_CHANGE
    chosen, dataset = results[FINAL_CANDIDATE], datasets[FINAL_CANDIDATE]
    settings = f.config(source_exit)
    r.dump(output / 'frozen_config.json', settings)
    control = results['E_CONTROL']
    prior_e = pd.read_csv(e.DEFAULT_OUTPUT / 'final/V13_V10_E/portfolio_trades.csv',
                          float_precision='round_trip')
    r.dump(output / 'v10_e_control_parity.json', v9.assert_control_parity(prior_e, control[1]))
    original_b = b.evaluate(base, source_exit)
    prior_b = pd.read_csv(b.DEFAULT_OUTPUT / 'final/V13_V10_B/portfolio_trades.csv',
                          float_precision='round_trip')
    r.dump(output / 'v10_b_control_parity.json', v9.assert_control_parity(prior_b, original_b[1]))
    metrics, daily_frames = [], []
    for version, result in [('V10-B',original_b), ('V10-E',control), ('V10-F',chosen)]:
        r.save(output / ('final/V13_' + version.replace('-', '_')
                         + ('' if version == 'V10-F' else '_CONTROL')), *result)
        daily = dr.detailed_daily(result[1], days)
        daily.insert(0, 'version', version)
        daily_frames.append(daily)
        for period, period_days in groups.items():
            for cost in [5,9]:
                metrics.append(dict(version=version,period=period,**r.metric(result[1],period_days,cost_bps=cost)))
    metrics = pd.DataFrame(metrics)
    metrics.to_csv(output / 'comparison_metrics.csv', index=False)
    pd.concat(daily_frames, ignore_index=True).to_csv(output / 'daily_comparison.csv',index=False)
    daily = dr.detailed_daily(chosen[1], days)
    daily.to_csv(output / 'daily_detailed.csv',index=False)
    f.selection_audit(signals,base['v9_config']).to_csv(output/'selection_audit.csv', index=False)
    orders_e, orders_f = datasets['E_CONTROL']['orders'], dataset['orders']
    keys = lambda frame: set(zip(frame.sid.astype(int),frame.setup_id.astype(str)))
    ekeys, fkeys = keys(orders_e), keys(orders_f)
    attribution = dict(
        e_selected=len(orders_e),f_selected=len(orders_f),
        retained=len(ekeys & fkeys),added=len(fkeys-ekeys),removed=len(ekeys-fkeys),
        all_f_volume_pass=bool(orders_f.v9_1m_volume_ratio.ge(1.2).all()),
        minimum_selected_volume_ratio=float(orders_f.v9_1m_volume_ratio.min()),
    )
    r.dump(output/'selection_attribution.json',attribution)
    oi_changes=pd.DataFrame([
        dict(signal_time=setup.signal_end,side=setup.side,
             e_minimum_oi_change_pct=setup.oi_change_pct,
             f_minimum_oi_change_pct=setup.oi_change_pct * .5)
        for setup in v9.v5.profile_setups(v9.v5.PROFILES['higher_frequency'])
        if setup.side=='SHORT'
    ])
    oi_changes.to_csv(output/'short_oi_threshold_changes.csv',index=False)
    chosen[1].loc[chosen[1].portfolio_executed].groupby('exit_reason').agg(
        trades=('sid','size'),net_profit_rupees=('portfolio_net_profit_rupees','sum')
    ).to_csv(output/'exit_reason_summary.csv')
    show=metrics.loc[metrics.cost_bps.eq(5),
        ['version','period','selected_orders','trades','wins','losses','win_rate_pct',
         'profit_factor','net_profit_rupees','daily_close_drawdown_rupees']]
    report='\n\n'.join([
        '# Corrected V13-v10-F: volume >=1.20',
        'This supersedes the earlier V10-F with a 0.90 volume fallback. Every selected candidate must now pass E\'s completed 1m confirmation volume ratio >=1.20. B\'s exact SL/target table is preserved.',
        'Final selection change: '+FINAL_CANDIDATE+' '+json.dumps(asdict(f.DEFAULT_CHANGE))+'.',
        'Only short-setup minimum OI-change thresholds are halved. Long thresholds, all volume gates, price/body/wick gates, setup quotas, entry expiry, ranking and B exits are unchanged. Trade count rises 17%, below the preferred 20% objective; full-period win rate and PF decline from E.',
        oi_changes.to_markdown(index=False,floatfmt='.3f'),
        show.to_markdown(index=False,floatfmt='.2f'),
        '## Daywise V10-F',daily.to_markdown(index=False,floatfmt='.2f'),
        '## Selection-parameter sensitivity',sweep[cols].to_markdown(index=False,floatfmt='.2f'),
        'Selection attribution: '+json.dumps(attribution)+'.',
        'Selections are recomputed before ranking, and all trades undergo a fresh chronological portfolio replay. Extra selections may occupy capital and displace other executions.',
        'The study used 23 configurations including control in three adaptive stages: 12 initial candidates, 7 primary-preservation follow-ups and 4 side-specific OI follow-ups. No candidate jointly reached 20% extra executions and full win>=65%. The selected variant adds 17% executions; August profit and win rate decline, while September profit and PF improve.',
        'Modeled cash-equity prices with futures OI signals; Rs 100,000 capital per entry, 5x exposure, Rs 300,000 portfolio, at most three simultaneous positions, 5bps round-trip cost, stop-first ambiguity, adverse-gap fills and 15:15 time exit.',
        'All periods have been used in previous research; B exits were fitted on them. This selection study is descriptive, with no untouched test and no claim of forward improvement.',
    ])
    (output/'V13_V10_F_DETAILED_RESULTS.md').write_text(report+'\n',encoding='utf-8')
    # Independent entry point reloads and reselects using the frozen configuration.
    replay=f.evaluate(f.load_source(settings=settings),settings)
    r.save(output/'cli_replay',*replay)
    r.dump(output/'cli_parity.json',v9.assert_control_parity(chosen[1],replay[1]))
    finalize_manifest(output)
    print(show.to_string(index=False))
    print(daily.to_string(index=False))


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--probe-only',action='store_true')
    run(parser.parse_args().probe_only)
