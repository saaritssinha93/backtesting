# V21 ADX14 and RSI14 entry check research through 4 November 2026

This isolated backtest applies the saved V21 cap/volume policy and all four G7 entry checks first, then tests a small set of fixed ADX/RSI conditions. It reuses V21's native entry, exit, cost and per-order size with unlimited portfolio capital. A rejected order never promotes another stock.

ADX14 here is newly calculated from each stock's hash-verified, completed one-minute session prefix using Wilder's directional movement smoothing. It is **session-reset**, so it is a different definition from the continuous EWM ADX column in some source snapshots. RSI14 and its direction-normalized three-minute change are the existing G7 session-reset features. ADX is trend strength; the +DI/-DI comparison supplies direction.

When ADX or RSI has not warmed up, that new condition passes the existing G7 decision rather than substituting a future observation. The ADX threshold 25, RSI midpoint 50 and nonnegative three-minute change are fixed research hypotheses, not established G3 thresholds.

## Full 65-session comparison

| Policy | Selected | Trades | Losses | Win rate | Mean/trade (Rs) | Median/trade (Rs) | Net P&L (Rs) | Median/day (Rs) | Daily-close DD (Rs) |
|:---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| all_g7 | 871 | 771 | 247 | 67.96% | 1,490.95 | 2,550.00 | 1,149,524.05 | 15,430.74 | 35,384.36 |
| adx14_ge25 | 708 | 626 | 200 | 68.05% | 1,542.87 | 2,675.58 | 965,837.55 | 11,929.51 | 37,572.94 |
| di_direction | 855 | 757 | 242 | 68.03% | 1,505.66 | 2,550.00 | 1,139,787.50 | 15,430.74 | 36,069.87 |
| adx14_ge25_and_di_direction | 697 | 616 | 196 | 68.18% | 1,559.71 | 2,748.55 | 960,781.99 | 10,690.11 | 32,322.94 |
| rsi14_directional_50 | 862 | 763 | 244 | 68.02% | 1,498.15 | 2,550.00 | 1,143,091.32 | 15,430.74 | 36,069.87 |
| adx14_ge25_di_direction_rsi14_directional_50 | 697 | 616 | 196 | 68.18% | 1,559.71 | 2,748.55 | 960,781.99 | 10,690.11 | 32,322.94 |
| adx14_rising3_di_direction_rsi14_rising3 | 619 | 539 | 168 | 68.83% | 1,585.61 | 2,550.00 | 854,641.52 | 11,028.97 | 23,143.39 |

## Later six observed sessions

The first 59 sessions end 27 October and the later six span 28 October to 4 November. All have already been observed; this split is a retrospective stability check.

| Policy | Trades | Losses | Win rate | Mean/trade (Rs) | Median/trade (Rs) | Net P&L (Rs) | Daily-close DD (Rs) |
|:---|---:|---:|---:|---:|---:|---:|---:|
| all_g7 | 84 | 36 | 57.14% | 494.41 | 1,503.47 | 41,530.08 | 24,615.85 |
| adx14_ge25 | 64 | 28 | 56.25% | 369.65 | 2,344.71 | 23,657.39 | 27,794.22 |
| di_direction | 83 | 35 | 57.83% | 510.66 | 1,797.38 | 42,385.16 | 23,760.77 |
| adx14_ge25_and_di_direction | 63 | 27 | 57.14% | 389.09 | 2,391.73 | 24,512.47 | 26,939.14 |
| rsi14_directional_50 | 83 | 35 | 57.83% | 510.66 | 1,797.38 | 42,385.16 | 23,760.77 |
| adx14_ge25_di_direction_rsi14_directional_50 | 63 | 27 | 57.14% | 389.09 | 2,391.73 | 24,512.47 | 26,939.14 |
| adx14_rising3_di_direction_rsi14_rising3 | 56 | 24 | 57.14% | 757.49 | 936.48 | 42,419.29 | 9,389.33 |

## Coverage and interpretation

- All 73 configured V21 pairs are present in the source; 1,092 orders pass the strict V21 selection and 871 pass the saved all-G7 entry checks.
- Of the all-G7 selected orders, 781 have ready ADX14 and 781 have ready RSI14. Unavailable early-session indicators are explicit in the audit.
- Mean and median trade P&L are after-cost values from executed trades. The median per day includes zero-trade days. Daily-close drawdown uses realized P&L and does not measure intraday lows.
- Entry gates use only confirmation-time prices and indicator decisions. All seven arms share the same already observed history. Testing several rules can overfit; no arm is activated for live trading or independently validated by these results.

Source sessions: 2026-07-29 through 2026-11-04. Saved all-G7 baseline metrics and daily P&L were reproduced. Source result manifests, stock snapshots and output file/code hashes were checked.

[Overall results](overall.csv) | [Day-wise results](daily.csv) | [Per-order decisions](admission_audit.csv) | [ADX source proofs](adx_source_proofs.json) | [Configuration](config.json) | [Provenance](provenance.json)
