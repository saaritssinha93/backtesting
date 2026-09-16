# V13 v9 research status — 2026-09-13

V9 is implemented on the original V13-v5/V6 lineage. Eight registered selection changes were tested; none improved both development periods. The default remains the exact V6 control, with 77 executions and Rs 188,829.63 modeled net profit through September 11. No performance upgrade is claimed.

The same-capital V7 180-minute comparison returns Rs 159,629.26. V5's unconstrained reference returns Rs 233,379.98 and requires a different aggregate capital comparison.

## Files

- `fno_v13_v9_backtest.py`: inherited V13 entries/exits, configurable selection before ranking, fresh V6 portfolio allocation.
- `fno_v13_v9_data.py`: source-hashed 5m/1m features, full rejection audit, exact native parity and isolated recoverable caches.
- `fno_v13_v9_diagnostics.py`: matched V13 payoff on selections/rejections, raw-minute quality checks, V8 causal coverage.
- `fno_v13_v9_research.py`: registered eight-arm study, frozen choice, baseline parity, same-calendar comparisons and report.

Results: `C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v9/run_20260913/V13_V9_DETAILED_RESULTS.md`

Dataset: 31 sessions, 211 stocks, 464,976 observed 5m bars, 90,412 setup opportunities. There are 152 fully eligible native candidates: 115 selected and 37 ranked out. All candidate observations, rejection reasons and corresponding 1m outcomes are preserved in the result directory. Counterfactual observations overlap and are not additive portfolio profits.

## Validation

98 tests pass across the four V9 test modules and inherited V5/V6 execution tests. Published V5/V6 orders, fills, entry/exit timestamps and P&L reproduce within Rs 1e-7. The independent broad and strict native replay agree exactly on all 152 eligible candidates. Of 90,412 broad paths, 90,092 are complete and 320 have zero-range confirmation candles.

All history was previously seen. Native fractional exposure, flat costs and minute-bar assumptions remain explicit. V6's September profit is only Rs 981.61 at 5bps and becomes a loss at 9bps; the report includes these sensitivities.

## Run

```powershell
python -B fno_v13_v9_research.py --output-dir 'C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v9\run_20260913'
python -B fno_v13_v9_backtest.py --dataset-dir 'C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v9\run_20260913\dataset' --config-json 'C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v9\run_20260913\frozen_config.json' --output-dir 'C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v9\run_20260913\replay'
```

## V14 cleanup

The workspace V14 F&O engines, tests, temporary audit scripts and compiled caches were deleted. Original V13 files and unrelated AVWAP V14 code remain intact.

Automatic approval review rejected recursive deletion with “blocked by policy,” so two output folders remain:

- `C:/TradingData/eqidv2/fno_oi/strategy_research/v14_master_20260913_122535`
- `C:/TradingData/eqidv2/fno_oi/strategy_research/v14_regression_20260913`

The exact cleanup state is recorded in `run_20260913/v14_deletion_manifest.json`. No V9 engine imports or reads V14 outputs.
