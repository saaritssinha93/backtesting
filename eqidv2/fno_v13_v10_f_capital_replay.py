"""Correct F portfolio capacity without changing its frozen selections or fills.

Uses Rs10 lakh as a conservative lower bound for the user's >Rs10 lakh
portfolio. Retains Rs1 lakh per trade. Reports both inherited 5x exposure
and 1x position-value interpretation explicitly, pending clarification.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_v10_f_backtest as f
from fno_v13_v10_d_research import detailed_daily

OUTPUT = f.DEFAULT_OUTPUT.parent / 'run_20260914_portfolio10l'


def replay(source, portfolio_capital, leverage):
    if not np.isfinite(portfolio_capital) or portfolio_capital < 100_000:
        raise ValueError('Portfolio must fund at least one Rs1 lakh trade')
    if not np.isfinite(leverage) or leverage <= 0:
        raise ValueError('Leverage must be finite and positive')
    cfg = f.v9.V9Config(capital_per_entry_rupees=100_000.,
        leverage_factor=leverage, portfolio_capital_rupees=portfolio_capital,
        max_positions=None)
    trades = f.v9.v5.apply_fixed_capital_model(source.copy(), 100_000., leverage)
    ledger, summary = f.v9.v6.apply_portfolio_constraints(trades, cfg.portfolio_config())
    return trades, ledger, summary


def run(output=OUTPUT, portfolio_capital=1_000_000.):
    source_dir = f.DEFAULT_OUTPUT
    proof = json.loads((source_dir/'research_manifest.json').read_text())
    proof['artifacts'] = {key.replace('\\','/'):value for key,value in proof['artifacts'].items()}
    names = ['final/V13_V10_F/portfolio_trades.csv','daily_detailed.csv','frozen_config.json']
    for name in names:
        if f.v10.sha(source_dir/name) != proof['artifacts'][name]:
            raise ValueError(f'Frozen F artifact drift: {name}')
    source = pd.read_csv(source_dir/names[0],float_precision='round_trip')
    days = pd.read_csv(source_dir/'daily_detailed.csv').day.tolist()
    assert source.loc[source.filled, 'capital_per_entry_rupees'].eq(100_000).all()
    assert source.leverage_factor.eq(5).all()
    prior_cfg = f.v9.V9Config()
    prior, _ = f.v9.v6.apply_portfolio_constraints(source, prior_cfg.portfolio_config())
    np.testing.assert_allclose(prior.portfolio_net_profit_rupees, source.portfolio_net_profit_rupees,atol=1e-7,rtol=0)
    assert prior.portfolio_executed.equals(source.portfolio_executed)
    output.mkdir(parents=True,exist_ok=True)
    rows = []
    groups = {'FULL':days, **{name:[day for day in days if day.startswith(prefix)] for name,prefix in [('JULY','2026-07'),('AUGUST','2026-08'),('SEPTEMBER','2026-09')]}}
    for period, period_days in groups.items():
        rows.append(dict(model='OLD_3L_3_POSITIONS_5X',period=period,**f.r.metric(prior,period_days)))
    results = {}
    for leverage in (5.,1.):
        name = f'CAPACITY_CORRECTED_{int(leverage)}X'
        trades, ledger, summary = replay(source,portfolio_capital,leverage)
        results[name] = (trades,ledger,summary)
        f.r.save(output/name,trades,ledger,summary)
        daily = detailed_daily(ledger,days)
        daily.to_csv(output/name/'daily_detailed.csv',index=False)
        for period, period_days in groups.items():
            rows.append(dict(model=name,period=period,**f.r.metric(ledger,period_days)))
        assert ledger.sid.equals(source.sid) and ledger.setup_id.equals(source.setup_id)
        assert ledger.filled.equals(source.filled)
        assert ledger.native_stop_pct.equals(source.native_stop_pct)
        assert ledger.native_target_pct.equals(source.native_target_pct)
    five = results['CAPACITY_CORRECTED_5X'][1]
    one = results['CAPACITY_CORRECTED_1X'][1]
    assert five.portfolio_executed.equals(one.portfolio_executed)
    np.testing.assert_allclose(five.portfolio_net_profit_rupees,5*one.portfolio_net_profit_rupees,atol=1e-7,rtol=0)
    larger = replay(source,portfolio_capital*2,5.)[1]
    assert five.portfolio_executed.equals(larger.portfolio_executed)
    recovered = five.loc[five.portfolio_executed & ~prior.portfolio_executed]
    recovered.to_csv(output/'recovered_entries.csv',index=False)
    comparison = pd.DataFrame(rows)
    comparison.to_csv(output/'comparison_metrics.csv',index=False)
    config = dict(version='V13-v10-F', user_portfolio='Greater than Rs10 lakh; exact amount unspecified',
        portfolio_capital_lower_bound_rupees=portfolio_capital,capital_per_entry_rupees=100_000,
        max_positions=None,selected_orders=len(source),original_5x_exposure_per_trade_rupees=500_000,
        leverage_interpretation='Report inherited 5x and alternative 1x explicitly; user clarification pending',
        selection_and_exits='Unchanged frozen F',validation='Original portfolio parity; frozen artifact hashes; 1x/5x scaling; larger-capital invariance passed',
        observed_peak_positions=results['CAPACITY_CORRECTED_5X'][2]['peak_concurrent_positions'],
        observed_peak_allocated_capital=results['CAPACITY_CORRECTED_5X'][2]['peak_reserved_capital_rupees'])
    f.r.dump(output/'configuration.json',config)
    daily = detailed_daily(five,days)
    text = '\n\n'.join([
        '# V10-F corrected portfolio capacity',
        'The previous Rs3 lakh / three-position constraint did not match the user. This replay uses Rs10 lakh as a conservative lower bound, Rs1 lakh capital per trade and no separate three-position cap. No selection or exit parameters were optimized.',
        '**Sizing distinction:** the old F model used 5x leverage, meaning Rs5 lakh position exposure per Rs1 lakh allocated capital. The 1x alternative models Rs1 lakh total position value. Both are reported explicitly pending clarification.',
        comparison.to_markdown(index=False,floatfmt='.2f'),
        f"Peak concurrent positions: {config['observed_peak_positions']}; peak allocated capital Rs{config['observed_peak_allocated_capital']:,.0f}. Doubling the portfolio capacity causes no further execution changes on this history.",
        f'{len(recovered)} previously capital-rejected trades are now executed. Six original selections remain unfilled because their price triggers were not reached within the entry window.',
        '## Daily results using inherited 5x exposure', daily.to_markdown(index=False,floatfmt='.2f'),
        '## Recovered entries',recovered[['day','tradingsymbol','side','portfolio_net_profit_rupees','exit_reason']].to_markdown(index=False,floatfmt='.2f'),
        'Source hashes and original F per-row accounting reproduce exactly. No historical F artifacts were rewritten. This is a capital-allocation correction to previously reviewed history, not out-of-sample evidence.',
        'Costs retain the original flat 5 bps model. Drawdown is daily-close realized drawdown, not intraday mark-to-market. Percent targets and stops are unchanged.',
        '`python -B fno_v13_v10_f_capital_replay.py`',
    ])
    (output/'V10_F_CORRECTED_PORTFOLIO_RESULTS.md').write_text(text,encoding='utf-8')
    f.r.dump(output/'manifest.json',dict(complete=True,source_files={str(source_dir/name):proof['artifacts'][name] for name in names},
        code_sha256=f.v10.sha(Path(__file__)),artifacts={str(path.relative_to(output)):f.v10.sha(path) for path in output.rglob('*') if path.is_file() and path.name!='manifest.json'}))
    print(comparison.to_string(index=False))
    print(json.dumps(config,indent=2))


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--portfolio-capital',type=float,default=1_000_000.)
    parser.add_argument('--output-dir',type=Path,default=OUTPUT)
    args=parser.parse_args()
    run(args.output_dir,args.portfolio_capital)
