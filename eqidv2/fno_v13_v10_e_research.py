"""Freeze, replay, compare, and document V10-E: B exits plus volume filter."""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd

import fno_v13_v10_e_backtest as e
import fno_v13_v10_d_research as dr

b, d, v10, v9, r = e.b, e.d, e.d.v10, e.v9, e.r


def run():
    output = e.DEFAULT_OUTPUT
    output.mkdir(parents=True, exist_ok=True)
    source_exit = json.loads(e.DEFAULT_SOURCE_CONFIG.read_text(encoding='utf-8'))
    settings = e.config(source_exit, 1.2)
    r.dump(output / 'frozen_config.json', settings)

    dataset_e = e.load_source(settings=settings)
    result_e = e.evaluate(dataset_e, settings)
    r.save(output / 'final/V13_V10_E', *result_e)

    dataset_b = v10.load_source()
    result_b = b.evaluate(dataset_b, source_exit)
    r.save(output / 'final/V13_V10_B_CONTROL', *result_b)
    prior_b = pd.read_csv(
        b.DEFAULT_OUTPUT / 'final/V13_V10_B/portfolio_trades.csv',
        float_precision='round_trip',
    )
    r.dump(output / 'v10_b_control_parity.json', v9.assert_control_parity(prior_b, result_b[1]))

    groups = r.periods(dataset_e['days'])
    groups.update({
        name: [day for day in dataset_e['days'] if str(day).startswith(prefix)]
        for name, prefix in [('JULY', '2026-07'), ('AUGUST', '2026-08')]
    })
    rows = []
    for name, result in [('V10-B', result_b), ('V10-E', result_e)]:
        for period, days in groups.items():
            for cost in [5, 9]:
                rows.append(dict(version=name, period=period, **r.metric(result[1], days, cost_bps=cost)))
    metrics = pd.DataFrame(rows)
    metrics.to_csv(output / 'comparison_metrics.csv', index=False)

    daily_e = dr.detailed_daily(result_e[1], dataset_e['days'])
    daily_e.to_csv(output / 'daily_detailed.csv', index=False)
    daily_b = dr.detailed_daily(result_b[1], dataset_e['days'])
    daily_b.insert(0, 'version', 'V10-B')
    daily_e_copy = daily_e.copy()
    daily_e_copy.insert(0, 'version', 'V10-E')
    pd.concat([daily_b, daily_e_copy], ignore_index=True).to_csv(
        output / 'daily_comparison.csv', index=False
    )

    exits = result_e[1].loc[result_e[1].portfolio_executed].groupby('exit_reason').agg(
        trades=('sid', 'size'), net_profit_rupees=('portfolio_net_profit_rupees', 'sum')
    )
    exits.to_csv(output / 'exit_reason_summary.csv')

    b_orders = set(dataset_b['orders'].sid.astype(int))
    e_orders = set(dataset_e['orders'].sid.astype(int))
    selection = dict(
        b_selected=len(b_orders), e_selected=len(e_orders), retained=len(b_orders & e_orders),
        removed=len(b_orders - e_orders), added_after_reranking=len(e_orders - b_orders),
        all_e_confirmation_volume_pass=bool(dataset_e['orders'].v9_1m_volume_ratio.ge(1.2).all()),
    )
    r.dump(output / 'selection_attribution.json', selection)

    show = metrics.loc[
        metrics.cost_bps.eq(5) & metrics.period.isin(['FULL', 'JULY', 'AUGUST', 'SEPTEMBER']),
        ['version', 'period', 'selected_orders', 'trades', 'wins', 'losses', 'win_rate_pct',
         'profit_factor', 'net_profit_rupees', 'daily_close_drawdown_rupees'],
    ]
    day_show = daily_e[
        ['day', 'selected', 'triggered', 'executed', 'wins', 'losses', 'win_rate_pct',
         'profit_factor', 'net_profit_rupees', 'cumulative_net_profit_rupees',
         'drawdown_rupees', 'targets', 'stops', 'time_exits']
    ]
    report = '\n\n'.join([
        '# V13-v10-E detailed results',
        ('V10-E keeps V10-B setup-specific SL and target percentages exactly and requires '
         'the completed one-minute confirmation candle volume ratio to be at least 1.20 '
         'before native per-setup ranking. Entry remains next-minute trigger with the inherited '
         'ten-minute expiry. There are no partial exits or break-even stop moves.'),
        show.to_markdown(index=False, floatfmt='.2f'),
        '## Daywise V10-E',
        day_show.to_markdown(index=False, floatfmt='.2f'),
        'Selection attribution: ' + json.dumps(selection) + '.',
        ('Accounting remains Rs 100,000 capital per entry, 5x exposure, Rs 300,000 portfolio, '
         'three positions and 5bps round-trip costs. Same stop-first ambiguity, adverse-gap fills '
         'and 15:15 exit. V10-B exits were fitted on this history and the volume rule was selected '
         'after reviewing September; these results are descriptive and need forward validation.'),
    ])
    (output / 'V13_V10_E_DETAILED_RESULTS.md').write_text(report + '\n', encoding='utf-8')

    sources = [
        Path(__file__), Path(e.__file__), Path(d.__file__), Path(b.__file__), Path(dr.__file__)
    ]
    snapshot = output / 'source_snapshot'
    snapshot.mkdir(exist_ok=True)
    for path in sources + [Path('tests/test_fno_v13_v10_e.py')]:
        if path.is_file():
            shutil.copy2(path, snapshot / path.name)
    r.dump(output / 'research_manifest.json', dict(
        complete=True,
        source_exit_config=str(e.DEFAULT_SOURCE_CONFIG),
        source_exit_config_sha256=v10.sha(e.DEFAULT_SOURCE_CONFIG),
        code_sha256={str(path.resolve()): v10.sha(path) for path in sources},
        artifacts={
            str(path.relative_to(output)): v10.sha(path)
            for path in sorted(output.rglob('*'))
            if path.is_file() and path.name != 'research_manifest.json'
        },
    ))
    print(show.to_string(index=False))
    print(day_show.to_string(index=False))


if __name__ == '__main__':
    run()
