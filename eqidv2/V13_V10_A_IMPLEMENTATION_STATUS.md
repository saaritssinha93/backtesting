# V13-v10-A

Created from V13-v10 with unchanged selection and entry logic. Each existing five-minute signal time and direction receives a fixed full-exit target and initial stop. No partial exits, break-even moves or new selection filters. Unclosed trades retain the 15:15 square-off.

Constraints: target <=2%, initial stop <=1.5%, target:SL >=1.5 before costs. With a 2% target cap, the ratio restricts feasible stops to <=1.3333%; the registered grid stops at 1.33%. Adverse gaps can exceed the configured stop loss.

## Active configuration: full-history fit

The CLI defaults to `C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_a/run_20260913/historical_fit/frozen_config.json`.

This version is explicitly optimized on all 31 sessions through September 11, including September results. It meets the historical overall win-rate objective; it is not independent evidence of future improvement. The earlier development-frozen experiment is preserved in the parent directory and failed the overall 65% objective.

| Period | Version | Trades | Win rate | Net PF | Modeled net profit |
| --- | --- | ---: | ---: | ---: | ---: |
| All 31 sessions | V10 | 87 | 75.86% | 2.1344 | Rs 122,476.00 |
| All 31 sessions | V10-A full-history fit | 93 | 67.74% | 5.1638 | Rs 238,298.38 |
| July 29-31 | V10-A full-history fit | 10 | 90.00% | 72.9486 | Rs 50,274.76 |
| August, 19 eligible sessions | V10-A full-history fit | 63 | 66.67% | 5.1831 | Rs 165,518.06 |
| September 1-11 | V10-A full-history fit | 20 | 60.00% | 2.3267 | Rs 22,505.56 |
| All 31 sessions | A fitted only through August 26 | 87 | 60.92% | 3.5849 | Rs 229,013.72 |
| September 1-11 | A fitted only through August 26 | 20 | 40.00% | 1.1068 | Rs 4,346.99 |

The 65% objective applies to the overall portfolio, not each month or individual slot. September is below it. All 115 original selected orders are retained; changed exit times allow 93 executions instead of 87.

Search: 11,594 distinct brackets, SL/target step 0.01%, SL minimum 0.10%, target minimum 0.15%. The active fit evaluated 4,092,682 portfolio configurations including repeated coordinate scans, using eight diverse starts and up to eight passes. Eleven slots have at least five triggered historical examples and receive fitted pairs; three sparse slots use a shared fallback. This is the highest PF found in that finite search, not a proven global optimum. Small slot samples and use of all dates create substantial fitting bias.

## Signal-time settings

| Signal time | Direction | SL | Full target |
| --- | --- | ---: | ---: |
| 09:25 | LONG | 0.21% | 0.34% |
| 09:25 | SHORT | 0.45% | 2.00% |
| 09:30 | LONG | 0.85% | 2.00% |
| 09:30 | SHORT | 0.89% | 2.00% |
| 09:35 | LONG | 0.60% | 1.73% |
| 09:35 | SHORT | 0.82% | 1.23% |
| 09:40 | LONG | 0.11% | 1.24% |
| 09:40 | SHORT | 0.39% | 1.58% |
| 09:45 | LONG | 0.82% | 1.23% |
| 09:45 | SHORT | 0.82% | 1.23% |
| 09:50 | SHORT | 0.27% | 0.42% |
| 09:55 | LONG | 0.16% | 0.24% |
| 10:00 | LONG | 0.29% | 2.00% |
| 11:20 | SHORT | 0.62% | 2.00% |

These are five-minute signal closing times. Internal setup IDs use confirmation time one minute later. Settings remain fixed across stocks and days; live entry decisions do not consult forward returns.

Accounting follows V10: cash-price execution with futures OI selections, modeled Rs 100,000 capital per trade, 5x exposure, Rs 300,000 portfolio, three positions and 5bps round-trip cost. At 9bps, active full-period win rate is 67.74%, PF 4.4745 and net Rs 219,698.38. These are modeled returns, not integer-lot futures/option-premium execution results. Daily-close drawdown is Rs 6,979.26.

Verification: 91 relevant tests passed, native full-bracket parity across selected wide-grid pairs, original V10 baseline parity and independent CLI replay. Daily and monthly totals reconcile. Thirty-six of 37 valid adjacent one-coordinate settings retain >=65% fitted win rate; their PF ranges from 4.6471 to 5.1638. This local check does not establish independent generalization.

## Files and reproduction

- Engine: `fno_v13_v10_a_backtest.py`
- Research: `fno_v13_v10_a_research.py`
- Verification: `fno_v13_v10_a_review.py`
- Tests: `tests/test_fno_v13_v10_a.py`
- Active detailed report: `C:/TradingData/eqidv2/fno_oi/strategy_research/v13_corrected_v10_a/run_20260913/historical_fit/V13_V10_A_DETAILED_RESULTS.md`

```powershell
python -B fno_v13_v10_a_research.py --fit-scope development
python -B fno_v13_v10_a_research.py --fit-scope full
python -B fno_v13_v10_a_backtest.py
python -B fno_v13_v10_a_review.py
```

To replay the earlier development-frozen experiment, pass the parent `run_20260913/frozen_config.json` explicitly via `--config-json` and use a separate `--output-dir`.
