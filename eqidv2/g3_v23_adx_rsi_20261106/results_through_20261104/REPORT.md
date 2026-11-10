# G3 V23 ADX/RSI backtesting

Selected research configuration: **adx20_di_rsi**. ADX14 >= 20 AND directional DI AND non-weakening 3-minute RSI.

Baseline: V21 + G7 confirmations; 73 configured pairs through 14:00, last confirmation 14:01. Original 34 pairs retain cap 3; 39 newer pairs retain cap 1 plus the volume gate. New pairs retain target 0.8% and initial stop 1.25%. Native exits are unchanged.

History: **65 sealed sessions, 29 July–4 November 2026**. November 5–6 are not included. October 1 remains excluded for incomplete immutable source data. Unlimited portfolio capital; Rs 100,000 per entry with 5x exposure and Rs 250 round-trip cost per fill. This differs from LIVE sizing.

## Full-history baseline comparison

| Policy | Trades | Losses | Win rate | Net P&L (Rs) | Gross losses (Rs) | Mean/trade (Rs) | Median/trade (Rs) | Daily-close DD (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 771 | 247 | 67.96% | 1,149,524.05 | 840,329.22 | 1,490.95 | 2,550.00 | 35,384.36 |
| adx20_di_rsi | 561 | 175 | 68.81% | 878,716.64 | 615,245.12 | 1,566.34 | 2,550.00 | 28,457.07 |

Losing trades removed: **72**; winning trades also removed: **138**.

## Earlier 59 sessions used for selection

| Policy | Trades | Losses | Win rate | Net P&L (Rs) | Gross losses (Rs) | Mean/trade (Rs) | Median/trade (Rs) | Daily-close DD (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 687 | 211 | 69.29% | 1,107,993.97 | 696,752.00 | 1,612.80 | 2,550.00 | 35,384.36 |
| adx20_di_rsi | 501 | 148 | 70.46% | 851,372.33 | 509,544.20 | 1,699.35 | 2,775.19 | 28,258.48 |

## Later six sessions: retrospective check

| Policy | Trades | Losses | Win rate | Net P&L (Rs) | Gross losses (Rs) | Mean/trade (Rs) | Median/trade (Rs) | Daily-close DD (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 84 | 36 | 57.14% | 41,530.08 | 143,577.23 | 494.41 | 1,503.47 | 24,615.85 |
| adx20_di_rsi | 60 | 27 | 55.00% | 27,344.31 | 105,700.91 | 455.74 | 471.70 | 11,641.19 |

All these sessions have been observed in earlier V21/G7 research. The later six are excluded from this selector but are NOT independent or prospective validation. Report both periods; do not change the chosen rule based on the later results. Daily-close drawdown is not intraday drawdown.

## All fixed hypotheses

| Policy | Trades | Losses | Win rate | Net P&L (Rs) | Gross losses (Rs) | Mean/trade (Rs) | Median/trade (Rs) | Daily-close DD (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline_v21_g7 | 771 | 247 | 67.96% | 1,149,524.05 | 840,329.22 | 1,490.95 | 2,550.00 | 35,384.36 |
| adx25_di_rsi50 | 616 | 196 | 68.18% | 960,781.99 | 689,061.46 | 1,559.71 | 2,748.55 | 32,322.94 |
| rising_adx_di_rsi | 539 | 168 | 68.83% | 854,641.52 | 582,557.04 | 1,585.61 | 2,550.00 | 23,143.39 |
| early_or_established | 706 | 227 | 67.85% | 1,080,000.05 | 780,331.39 | 1,529.75 | 2,615.63 | 42,247.09 |
| balanced_or | 717 | 230 | 67.92% | 1,074,780.47 | 788,599.98 | 1,499.00 | 2,550.00 | 43,297.09 |
| balanced_or_rsi_guard | 441 | 148 | 66.44% | 609,836.77 | 531,497.17 | 1,382.85 | 2,658.83 | 23,529.62 |
| di_rsi_weakness_veto | 744 | 238 | 68.01% | 1,100,430.14 | 816,983.55 | 1,479.07 | 2,550.00 | 38,187.98 |
| rsi_momentum | 615 | 195 | 68.29% | 920,258.07 | 678,372.71 | 1,496.35 | 2,550.00 | 24,920.50 |
| adx20_di_rsi | 561 | 175 | 68.81% | 878,716.64 | 615,245.12 | 1,566.34 | 2,550.00 | 28,457.07 |

## Selection and data controls

Eight ADX/RSI hypotheses plus baseline were fixed before this run. Selection uses only the first 59 sessions: retain at least 50% of trades and 75% of baseline net profit, improve win rate by at least 0.5 percentage points, lower gross loss, and do not worsen daily-close drawdown. Rank eligible policies by win rate, then lower drawdown, then net profit. If none qualifies, retain baseline.

Indicators use the completed confirmation-minute prefix. ADX14 and RSI14 reset each session; early warm-up unavailability preserves the baseline decision. Invalid ready values or future-dated features abort the run. Rejected orders do not promote another stock. Removing independent native fills is exact under the inherited unlimited-capital/no-refill convention; it is not a replay of a finite-capital account. Baseline aggregate and all 65 daily results matched the source.

## Results

[Day-wise comparison](daily_results.csv) · [All 73 pairs](slot_results.csv) · [V23 trades](v23_trades.csv) · [Every admission decision](admission_audit.csv) · [Removed winners and losers](trade_comparison.csv) · [Configuration](config.json) · [Provenance](provenance.json)

No LIVE configuration, scheduler, approval, broker connection, or V21 result was changed.
