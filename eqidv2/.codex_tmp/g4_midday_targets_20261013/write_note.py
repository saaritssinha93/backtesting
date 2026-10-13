"""Build the repository note from the completed target and portfolio studies."""
import json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
BASE = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_4/target_sweeps/2026-10-13')
RUN = BASE / '20261013_midday_1135_1230_targets_10pct_v2'


def table(frame):
    def cell(x):
        if pd.isna(x):
            return 'None'
        if isinstance(x, float):
            return f'{x:,.2f}'
        return str(x)
    headers = [str(c) for c in frame.columns]
    return '\n'.join(['| ' + ' | '.join(headers) + ' |', '| ' + ' | '.join(['---'] * len(headers)) + ' |'] +
                     ['| ' + ' | '.join(cell(x) for x in row) + ' |' for row in frame.itertuples(index=False, name=None)])


def main():
    summary = json.loads((RUN / 'summary.json').read_text())
    best = pd.read_csv(RUN / 'best_targets.csv', float_precision='round_trip')
    scenarios = pd.read_csv(RUN / 'portfolio_results.csv', float_precision='round_trip')
    setups = pd.read_csv(RUN / 'scenario_setup_results.csv', float_precision='round_trip')
    actual = setups.loc[setups.scenario.eq('historical_profitable70_only') & setups.period.eq('all')]
    rows = best[['setup', 'setup_id', 'historical_target_pct', 'historical_pass']].merge(
        actual[['setup_id', 'executed_trades', 'wins', 'win_rate_pct', 'net_pnl_rs']], on='setup_id', how='left')
    rows = rows.drop(columns='setup_id').rename(columns={'historical_target_pct':'Target %',
        'historical_pass':'Profitable >=70%', 'executed_trades':'Actual fills', 'wins':'Wins',
        'win_rate_pct':'Actual win %', 'net_pnl_rs':'Actual net P&L (Rs)'})
    comparison = scenarios.loc[scenarios.period.eq('all'),
        ['scenario', 'scope', 'executed_trades', 'wins', 'win_rate_pct', 'net_pnl_rs', 'max_drawdown_rs']]
    holdout = scenarios.loc[scenarios.period.eq('holdout'),
        ['scenario', 'scope', 'executed_trades', 'win_rate_pct', 'net_pnl_rs']]
    checks = best.loc[best.chronological_check_pass,
        ['setup', 'training_target_pct', 'training_train_fills', 'training_train_win_rate_pct',
         'training_holdout_fills', 'training_holdout_win_rate_pct']]
    actual_later = setups.loc[setups.scenario.eq('chronological_check_subset') & setups.period.eq('holdout'),
        ['setup_id', 'target_pct', 'executed_trades', 'wins', 'win_rate_pct', 'net_pnl_rs', 'observed_70_positive']]
    text = f'''# G4 midday target optimization - 2026-10-13

The target-only study covers all 24 LONG/SHORT setups at 11:35 through 12:30,
every five minutes. It tests **{summary['target_levels']} targets, 0.06%-10.00%
in 0.01% increments**, for **{summary['target_trials']:,} setup/target trials**.
The 345 selected orders and 287 native potential fills remain unchanged at
every target. Entry filters are the fixed selections from the prior midday
study, including the diagnostic configurations that failed its 1% screen.

**22 of 24 setups** have positive net P&L and at least 70% historical wins;
all 22 also meet those conditions after capital allocation. The exceptions
are **11:55 SHORT and 12:00 LONG**: they can reach 70% wins with small targets,
but no target meeting that win rate has positive net P&L.

Run: `{RUN.as_posix()}`

## Fixed rules and selection objective

The period is July 29-October 12, 2026, covering 49 complete sessions. October 1
is excluded for incomplete immutable input. The initial stop stays at 1.25%,
tightening to 1.00% after 120 minutes. Confirmation is exactly the next minute;
the entry trigger expires after ten minutes; square-off is 15:15. The source
study's single malformed negative-liquidity input remains excluded.

Capital is Rs 100,000 per entry with 5x exposure and Rs 1,000,000 portfolio
capital. Net P&L deducts five basis points round trip. A win is a positive-net
trade, including profitable time exits, and is not necessarily a target hit.
Broker-specific fees, additional slippage and market impact are not modeled.
The 0.06% target leaves only 0.01% after modeled costs, so its cost margin is
especially small.

For each fixed setup, choose maximum positive net P&L among tested targets
with at least five historical fills and at least 70% net wins. Replay the
resulting target map with the existing 37-setup G4 baseline and recheck actual
executions. Common-target comparisons require 70% and positive P&L for both
the additions and the combined book; a strong baseline cannot mask weak
new trades. The baseline contributes Rs 607,998.28 on 289 executions.

Targets were initially tested through 5%. KAYNES at 11:35 LONG on July 29
reached beyond that boundary, so the sweep was extended to 10%. No native
trade hits the 10% target; higher targets therefore cannot change outcomes
under these fixed stops and minute paths. The optimum remains scoped to the
0.01% grid, rather than every possible continuous target.

## All 24 target choices

These are actual executions in the combined 22-setup qualifying portfolio,
not standalone fills that might be rejected by the capital limit.

{table(rows)}

## Portfolio comparisons

Full-period results; additions are the new trades actually executed alongside
the baseline. The qualifying-only comparison also excludes the two failed
setups, so improvement over original all-24 at 1% is not target-only with
identical membership.

{table(comparison)}

Later 11 sessions:

{table(holdout)}

## Training choices and evidence limits

Historical best targets use all 49 outcomes. They are an in-sample fit,
including when their later-period win rate is above 70%. The training-only
alternative chooses targets on the first 38 sessions, through September 23;
the later 11 sessions were already used in previous research and are not an
untouched out-of-sample test. Small samples and many tested targets limit
what these results establish about future performance.

Five training-selected targets pass the standalone chronological screen:

{table(checks)}

After portfolio allocation, the same later-period checks are:

{table(actual_later)}

12:20 SHORT and 12:25 LONG have only two actual later fills each, below the
three-fill minimum. Therefore five standalone passes must not be represented
as five portfolio-supported passes. The chronological subset membership
itself uses later outcomes.

## Artifacts and reproduction

`report.html` contains the comparison. `best_targets.csv` covers every setup;
`target_sweep.csv` contains all {summary['target_trials']:,} trials;
`scenario_setup_results.csv` reports actual executions;
`scenario_configs.json` contains every tested final target map and its entry
filters. Daily/monthly results, complete trade ledgers and the cached minute
replays are retained alongside `summary.json` and `manifest.json`.

```powershell
python -m research.g4_extended_target_sweep --config configs/strategies/v13_v10_g_4_target_sweep_1135_1230_10pct_v2.json --workers 3
```

Loading frozen CSVs uses round-trip float precision. A regression test verifies
that a SHORT trigger exactly equal to a minute low remains filled. The sweep
verified the original 1% result, entry invariance at every target, unchanged
stop policy, and independently replayed final target maps. The main focused
test suite passed 92 tests. All work is marked BACKTEST_ONLY.
'''
    destination = ROOT / 'docs/G4_1135_1230_TARGET_SWEEP_2026-10-13.md'
    destination.write_text(text, encoding='utf-8')
    print(destination)


if __name__ == '__main__':
    main()
