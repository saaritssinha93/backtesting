"""Summarize the immutable afternoon experiments without altering their artifacts."""
from pathlib import Path
import hashlib
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
BASE = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_4')
ENTRY = BASE / 'runs/2026-10-13/20261013_afternoon_1235_1330_v6'
TARGET = BASE / 'target_sweeps/2026-10-13/20261013_afternoon_1235_1330_targets_v3'
DELETE = BASE / 'target_sweeps/2026-10-13/20261013_afternoon_deletion_refined_v2'
ACTUAL = BASE / 'target_sweeps/2026-10-13/20261013_afternoon_portfolio_targets_v4'
FINAL = BASE / 'target_sweeps/2026-10-13/20261013_afternoon_dual70_targets_v5'

def read(path):
    return pd.read_csv(path, float_precision='round_trip')

def js(path):
    return json.loads(path.read_text(encoding='utf-8'))

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def table(headers, rows):
    return '\n'.join(['| ' + ' | '.join(headers) + ' |',
                      '| ' + ' | '.join(['---'] * len(headers)) + ' |'] +
                     ['| ' + ' | '.join(str(x) for x in row) + ' |' for row in rows])

def stats(frame):
    done = frame.loc[frame.portfolio_executed.eq(True)]
    pnl = done.net_return_pct * 5000
    return len(done), int(pnl.gt(0).sum()), 100*pnl.gt(0).mean(), float(pnl.sum())

def main():
    summary = js(FINAL / 'summary.json')
    assert summary['local_coordinate_convergence']
    assert summary['baseline_trades_changed'] == 0
    result = read(FINAL / 'refined_qualified_trades.csv')
    raw = read(FINAL / 'refined_added_raw_trades.csv')
    baseline = read(FINAL / 'baseline_g4_trades.csv')
    config = js(FINAL / 'refined_config.json')
    final_map = {row['setup_id']: row['target_pct'] for row in config['setups']}
    added = result.loc[result.setup_id.isin(final_map)]
    entry_config = js(ENTRY / 'selected_config.json')
    best = read(TARGET / 'best_targets.csv').set_index('setup_id')
    first = read(ENTRY / 'all_24_diagnostic_trades.csv')
    original = read(ENTRY / 'selected_orders.csv')
    first_added = first.loc[first.sid.isin(original.sid)]
    scopes = [('Existing 49 G4 setups', baseline), ('All 24 additions at 1% (additions only)', first_added),
              ('All 24 additions at 1% plus baseline', first),
              ('Best tested native + allocated 70% additions', added),
              ('Best tested native + allocated 70% combined', result)]
    scope_rows = []
    for name, frame in scopes:
        n,w,r,p = stats(frame)
        scope_rows.append([name,n,w,f'{r:.2f}%',f'{p:,.2f}'])
    setups = []
    for row in entry_config['setups']:
        setup = row['setup_id']
        label = row['signal_end'] + ' ' + row['side']
        if setup in final_map:
            native = raw.loc[raw.setup_id.eq(setup) & raw.filled.eq(True)]
            native_pnl = native.net_return_pct*5000
            n,w,rate,pnl = stats(added.loc[added.setup_id.eq(setup)])
            native_rate = 100*native_pnl.gt(0).mean()
            assert n>=5 and rate>=70 and pnl>0
            assert len(native)>=5 and native_rate>=70 and native_pnl.sum()>0
            setups.append([label,row['stage'],f'{final_map[setup]:.2f}%',len(native),f'{native_rate:.2f}%',
                           n,f'{rate:.2f}%',f'{pnl:,.2f}','Included'])
        else:
            reason = 'No native target qualifies' if not best.loc[setup,'historical_pass'] else 'Excluded by portfolio search'
            setups.append([label,row['stage'],'—','—','—','—','—','—',reason])
    periods = read(FINAL / 'portfolio_results.csv')
    periods = periods.loc[periods.scenario.eq('refined_qualified')]
    period_rows = [[r.period,r.scope,r.executed_trades,r.wins,f'{r.win_rate_pct:.2f}%',f'{r.net_pnl_rs:,.2f}']
                   for r in periods.itertuples()]
    stage = read(ENTRY / 'portfolio_results.csv')
    stage = stage.loc[stage.scenario.isin(['strict_training_eligible','relax_1m_training_eligible','relax_5m_training_eligible'])]
    stage_rows = [[r.scenario,r.period,r.executed_trades,f'{r.win_rate_pct:.2f}%',f'{r.net_pnl_rs:,.2f}']
                  for r in stage.itertuples()]
    actual_summary = js(ACTUAL / 'summary.json')
    final_stats = periods.loc[periods.period.eq('all') & periods.scope.eq('combined')].iloc[0]
    text = f'''# G4 12:35–13:30 research: strict selection and target optimization

All 24 requested LONG/SHORT pairs are implemented in isolated G4 research.
The existing 49-setup G4 V5 baseline is preserved. The final stricter research
configuration retains **{len(final_map)} additions** and therefore contains
**{49+len(final_map)} total setups**. Every retained addition has at least five
native fills and five actual executions, at least 70% net wins, and positive
net P&L in **both** views. This is the best tested configuration under those
constraints, not a global optimum or a prospective win-rate guarantee.

## Results

Period: **July 29–October 12, 2026, 49 complete sessions**. October 1 is
excluded for incomplete immutable input. All rupee results use Rs100,000
capital per entry, 5x exposure and Rs1,000,000 portfolio capital. Five basis
points round-trip costs are deducted (Rs250 per executed trade).

{table(['Scope','Trades','Wins','Win rate','Net P&L (Rs)'],scope_rows)}

Final combined daily-realized maximum drawdown: **Rs{final_stats.max_drawdown_rs:,.2f}**.
Capital allocation changes no existing baseline execution or P&L.

## Every requested setup

The target and P&L columns below describe the final stricter portfolio.
Excluded setups remain present in the original 1% and all-target diagnostics;
exclusion is not a claim that no other configuration could ever work.

{table(['Signal / side','Selection stage','Target','Native fills','Native win rate','Actual trades','Actual win rate','Actual net P&L (Rs)','Final status'],setups)}

Detailed thresholds are in `refined_config.json`. The initial entry filters
are frozen after strict → relaxed 1m → relaxed 5m selection at a 1% target.
The target search does not jointly optimize all possible filters and targets.

## Strict, 1m and 5m results at the original 1% target

Each stage independently selects its training-qualified configurations;
membership differs between stages. The following includes the existing G4
baseline. No setup passes the original complete training/later/capital screen.

{table(['Stage portfolio','Period','Trades','Win rate','Net P&L (Rs)'],stage_rows)}

Selection uses the first 38 sessions through September 23. The later 11
sessions begin September 24. The complete initial screen requires five
training fills, three later fills, 70% wins and positive P&L in both periods,
and checks standalone and allocated outcomes. The final optimized results
below use all 49 outcomes and therefore are historical fits.

## Final training and later-period breakdown

{table(['Period','Scope','Trades','Wins','Win rate','Net P&L (Rs)'],period_rows)}

Later outcomes participated in target and portfolio selection. These later
figures are descriptive, not an untouched out-of-sample validation.

## What was tested

- 6,168 entry-filter configurations across strict, relaxed 1m and relaxed 5m.
- 995 target levels per setup, 0.06%–10.00% in 0.01% increments: 23,880 trials.
- The 333 chosen orders and 296 native potential fills stay invariant across
  targets. Highest liquidity is chosen before observing whether the trigger fills.
- Native historical target screening yields 18 qualifying setups. Rechecking
  actual executions initially leaves 14. Whole-setup deletion alone gives no
  improvement. The stricter target/add/change/delete search tests
  {summary['tested_configurations']:,} configurations and accepts
  {summary['accepted_changes']} changes before reaching a local optimum.
- Every new target map is allocated jointly with all existing G4 orders. No
  individual trade is removed from a setup's selected orders.
- The fast allocator is checked against all 995 native common-target portfolios;
  final exits are independently replayed through the native minute simulator.
- No native trade hits the 10% ceiling; all setup target outcomes plateau by
  4.97% or lower. The upper target bound is not binding under these fixed paths.

The separately retained actual-execution-only optimization returns
Rs{actual_summary['final']['combined_net_pnl_rs']:,.2f} combined and
Rs{actual_summary['final']['added_net_pnl_rs']:,.2f} from
{actual_summary['final_setups']} additions. Three of those final configurations
(12:35 LONG, 12:40 LONG, 12:55 SHORT) fail the standalone 70% requirement;
12:40 LONG also loses money standalone. Capital filtering hides those weak
native outcomes. It is a diagnostic and is not the stricter result above.

## Execution and evidence rules

Stops match 09:50: 1.25% initially, tightening to 1.00% after 120 minutes from
actual entry. Confirmation remains exactly the next minute; relaxation changes
filters, not confirmation timing. Entry expiry is ten minutes; square-off is
15:15. Signals at 13:15 or later cannot reach the 120-minute tightening before
square-off. Ambiguous bars use the retained conservative stop-first treatment.

Wins mean positive net P&L, including profitable square-off exits; they are
not target-hit counts. The source retains directional EMA order, directional
price change of at least 0.10%, increasing OI of 0.05%–1.00%, five-minute volume
of at least 0.80x, and a directional confirmation beyond the signal close.
Three malformed negative-liquidity rows were rejected before outcome selection:
FORCEMOT SHORT on October 7 at 13:10 and October 8 at 13:10 and 13:15.
1,033 of 1,036 source candidates enter the study.

The evidence reuses previously researched dates, contains small setup samples
and many parameter trials. Fees are the fixed 5 bps model; additional slippage,
market impact and broker-specific charges are not included. Small targets have
limited cost margin. No live or paper execution authority is granted by any
configuration in this study. The current G3 production profile remains V6/44.

## Artifacts and reproduction

Entry study: `{ENTRY.as_posix()}`

Target sweep: `{TARGET.as_posix()}`

Final stricter result: `{FINAL.as_posix()}`

The final directory contains `report.html`, `refined_config.json`,
`refined_qualified_trades.csv`, `refined_added_raw_trades.csv`,
`scenario_setup_results.csv`, `portfolio_results.csv`, `daily_results.csv`,
`monthly_results.csv`, `refinement_trials.csv`, `accepted_maps.json`,
`fast_allocator_validation.csv`, `summary.json` and `manifest.json`.

```powershell
python fno_v13_v10_g_4_backtest.py --config configs/strategies/v13_v10_g_4_backtest_1235_1330_v6.json
python -m research.g4_extended_target_sweep --config configs/strategies/v13_v10_g_4_target_sweep_1235_1330_v3.json --workers 3
python -m research.g4_afternoon_portfolio_target_refinement --require-native70 --source-manifest-sha256 {sha(TARGET/'manifest.json')} --initial-config {DELETE.as_posix()}/refined_config.json --max-passes 10 --output C:/TradingData/eqidv2/backtesting_result_v13_v10_g_4/target_sweeps/2026-10-13/NEW_REPRODUCTION_DIRECTORY
```

The target-sweep config pins the named immutable entry run. A newly generated
entry run must be explicitly pinned by its path and manifest hash before it
can replace that reference. Commands refuse to overwrite completed output.

| Evidence | Manifest SHA-256 |
| --- | --- |
| Entry study | `{sha(ENTRY/'manifest.json')}` |
| Target sweep | `{sha(TARGET/'manifest.json')}` |
| Final stricter result | `{sha(FINAL/'manifest.json')}` |

Independent validation receipts and report screenshots are under
`.codex_tmp/g4_1235_1330_20261013/`.
'''
    path = ROOT / 'docs/G4_1235_1330_TARGET_RESEARCH_2026-10-13.md'
    path.write_text(text, encoding='utf-8')
    print(path)
    print(table(['Scope','Trades','Wins','Win rate','Net P&L (Rs)'],scope_rows))
    print(table(['Signal / side','Selection stage','Target','Native fills','Native win rate','Actual trades','Actual win rate','Actual net P&L (Rs)','Final status'],setups))

if __name__=='__main__':
    main()
