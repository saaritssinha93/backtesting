# FNO V13-v5 research audit and experiment index

## Scope and verdict

This archive contains every completed timing/entry, exit, NIFTY and controlled profile row generated during V13-v5 research, including rejected and not-run experiments. No profile is production-promoted because all 25 sessions were previously inspected and the added timing legs are sparse.

`higher_frequency` is now the explicitly configured default at the user's direction. This changes configuration selection, not the evidence grade: it remains an experimental forward-shadow profile.

The controlled profile archive was frozen with adverse trigger-gap handling but before the final later-bar stop-gap correction. That correction affects one native V3 trade by about -0.03745 percentage point and no final scale-out profile trade, so profile selection and profile metrics are unchanged; the main V5 comparison is the authoritative corrected-baseline row.

## Final profile leaderboard (5 bps)

| label                         | configured_default   |   executed_trades |   average_trades_per_day |   win_rate_pct |   target_hit_rate_pct |   runner_target_hit_rate_pct |   stop_hit_rate_pct |   pre_cost_return_pct |   total_cost_pct |   net_profit_pct |   profit_factor |   expectancy_pct |   average_winning_trade_pct |   average_losing_trade_pct |   payoff_ratio |   maximum_drawdown_pct |
|:------------------------------|:---------------------|------------------:|-------------------------:|---------------:|----------------------:|-----------------------------:|--------------------:|----------------------:|-----------------:|-----------------:|----------------:|-----------------:|----------------------------:|---------------------------:|---------------:|-----------------------:|
| V13_V3_OFFICIAL_PUBLISHED     | False                |                78 |                    3.120 |         56.410 |                21.795 |                       21.795 |              38.462 |                49.934 |            3.900 |           46.034 |           2.770 |            0.590 |                       1.637 |                     -0.765 |          2.141 |                 -3.041 |
| V13_V3_CORRECTED_UNIFORM_1515 | False                |                78 |                    3.120 |         56.410 |                21.795 |                       21.795 |              38.462 |                50.555 |            3.900 |           46.655 |           2.803 |            0.598 |                       1.648 |                     -0.761 |          2.166 |                 -3.041 |
| V13_V5_BALANCED               | False                |                83 |                    3.320 |         72.289 |                55.422 |                       24.096 |              10.843 |                52.900 |            4.150 |           48.750 |           3.197 |            0.587 |                       1.182 |                     -0.965 |          1.226 |                 -2.913 |
| V13_V5_CONSERVATIVE           | False                |                68 |                    2.720 |         66.176 |                51.471 |                       19.118 |               4.412 |                40.075 |            3.400 |           36.675 |           3.541 |            0.539 |                       1.136 |                     -0.627 |          1.810 |                 -1.945 |
| V13_V5_HIGHER_FREQUENCY       | True                 |                89 |                    3.560 |         73.034 |                56.180 |                       22.472 |              10.112 |                56.748 |            4.450 |           52.298 |           3.312 |            0.588 |                       1.153 |                     -0.943 |          1.223 |                 -2.913 |

V5 target hit is a first-stage touch with only 10% or 20% booked; the runner hit column is the economically stricter +2.60% outcome. Returns are summed cash-equity percentages, not F&O premium or capital-sized INR P&L.

## Unified ledger coverage

| source_suite             | decision                    |   rows |
|:-------------------------|:----------------------------|-------:|
| EXIT_ROBUSTNESS          | GRID_ONLY_NOT_FROZEN        |    586 |
| EXIT_ROBUSTNESS          | INVESTIGATE_FORWARD         |      1 |
| EXIT_ROBUSTNESS          | REFERENCE                   |      2 |
| PROFILE_GRID             | BALANCED_SHADOW             |      4 |
| PROFILE_GRID             | HIGHER_FREQUENCY_SHADOW     |      4 |
| PROFILE_GRID             | NUMERIC_PARETO_SHADOW       |      4 |
| PROFILE_GRID             | REFERENCE_GRID              |     96 |
| PROFILE_GRID             | REJECT_OR_NEIGHBOR          |    660 |
| RAW_CONFIRMATION_REBUILD | REJECT                      |      2 |
| TIMING_ENTRY             | NOT_RUN                     |      5 |
| TIMING_ENTRY             | REJECTED_BEFORE_PSEUDO_TEST |    159 |
| TIMING_ENTRY             | SELECTED_BEFORE_PSEUDO_TEST |      6 |

The normalized ledger contains **1,529 rows**. Native source tables are also preserved because their protocols and extra diagnostics cannot be losslessly compressed into one schema.

## One-minute confirmation funnel

Stage | Count
--- | ---:
Loose-gate exact positive-range S+1 candles | 9,935
Strict directional S+1 survivors | 4,025
Rejected by strict direction versus V7-valid pool | 5,910
Strict survivors after NIFTY/OI policy | 3,837
Strict survivors inside 12 V3 active cells | 921
V3 selected orders / fills | 79 / 78
V7 selected orders / fills | 121 / 114

Corrected V7 full-book validation: 25 fills, PF 0.875, net -1.361%, full drawdown -6.849%. It is rejected.

## Native artifact directories

- `validation_audit/`: end-to-end V3 code audit, rule registry and support matrix.
- `timing_entry/`: all 136 time/side inventory cells, 124 additions, removals, max-entry and entry-filter tests, pseudo reveal and rejected rows.
- `exit_robustness/`: 589 exit configurations, MAE/MFE, NIFTY ablations, cost, fill-delay, top-trade removal, bootstrap and Monte Carlo diagnostics.
- `profile_eval/`: 2³ entry-component ablation, 24 local exit configurations per profile at 5/10/20/30 bps, frozen development decisions and pseudo reveal.

## Reproduce

```powershell
python fno_v13_corrected_v3_backtest.py --through-day 2026-09-03 --cost-bps 5
python .codex_tmp/v13_v5_build_v7.py
python .codex_tmp/v13_v5_timing_entry/run_timing_entry_experiments.py
python .codex_tmp/v13_v5_exit_robustness/run_exit_robustness.py
python .codex_tmp/v13_v5_exit_robustness/run_nifty_firstbar_ablation.py
python .codex_tmp/v13_v5_profile_eval/run_profile_eval.py
# Configured default run
python fno_v13_corrected_v5_backtest.py --profile higher_frequency --through-day 2026-09-03 --cost-bps 5
# Explicit full-profile research comparison
python fno_v13_corrected_v5_backtest.py --profile all --through-day 2026-09-03 --cost-bps 5
python fno_v13_v5_research.py
```

V13-v3 SHA-256 before/after consolidation: `85c2ff1c37a342e8e0bc4b73eb115de8ebc5aa7e990db261e46036b68aeafbab` / `85c2ff1c37a342e8e0bc4b73eb115de8ebc5aa7e990db261e46036b68aeafbab`.

Unified ledger: `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v5\research\fno_v13_v5_experiment_ledger.csv`
V7 trade audit: `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v5\research\confirmation_v7_trade_audit.csv`
Artifact manifest: `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v5\research\fno_v13_v5_research_manifest.csv`
Created-file index: `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v5\research\fno_v13_v5_created_files.csv`
