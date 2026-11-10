# V23 keep-all-entries risk-sizing study

Every variant preserves all **771 fills**, their symbols, sides, setup IDs, entry/exit times and prices. No entries or exits change. Positive size multipliers preserve all **524 winning / 247 losing trades** and **67.96% win rate**. Only rupee amounts change.

History: 65 sessions, 29 July–4 November 2026. Baseline is V21 + G7, Rs 100,000 capital at 5x exposure per trade, unlimited portfolio capital.

**Selection:** NO_ELIGIBLE_SIZING_CHANGE; selected policy: `baseline_fixed_size`. Selection uses the earlier 59 sessions only, requiring no lower net profit, lower gross loss and lower daily-close drawdown. Candidates are ranked by net profit, then drawdown.

## All 65 sessions

| Policy | Trades | Net P&L (Rs) | Gross loss (Rs) | Daily-close DD (Rs) | Exposure / baseline | Losing days |
|:--|--:|--:|--:|--:|--:|--:|
| baseline_fixed_size | 771 | 1,149,524.05 | 840,329.22 | 35,384.36 | 1.0000x | 16 |
| weak_75pct | 771 | 1,081,822.20 | 784,058.20 | 33,602.89 | 0.9319x | 16 |
| weak_50pct | 771 | 1,014,120.34 | 727,787.17 | 31,821.42 | 0.8638x | 15 |
| strong_110_weak_75 | 771 | 1,154,922.40 | 833,347.53 | 36,646.90 | 0.9942x | 16 |
| strong_125_weak_50 | 771 | 1,196,870.86 | 851,010.49 | 39,431.45 | 1.0195x | 14 |
| train_normalized_weak_50 | 771 | 1,169,965.12 | 839,629.74 | 36,711.57 | 0.9966x | 15 |

## Earlier 59 sessions

| Policy | Trades | Net P&L (Rs) | Gross loss (Rs) | Daily-close DD (Rs) | Exposure / baseline | Losing days |
|:--|--:|--:|--:|--:|--:|--:|
| baseline_fixed_size | 687 | 1,107,993.97 | 696,752.00 | 35,384.36 | 1.0000x | 13 |
| weak_75pct | 687 | 1,043,838.56 | 649,950.05 | 33,602.89 | 0.9323x | 13 |
| weak_50pct | 687 | 979,683.15 | 603,148.10 | 31,821.42 | 0.8646x | 12 |
| strong_110_weak_75 | 687 | 1,113,436.59 | 691,044.29 | 36,646.90 | 0.9945x | 13 |
| strong_125_weak_50 | 687 | 1,153,678.22 | 705,883.69 | 39,431.45 | 1.0200x | 10 |
| train_normalized_weak_50 | 687 | 1,130,235.80 | 695,836.78 | 36,711.57 | 0.9975x | 12 |

## Later six retrospective sessions

| Policy | Trades | Net P&L (Rs) | Gross loss (Rs) | Daily-close DD (Rs) | Exposure / baseline | Losing days |
|:--|--:|--:|--:|--:|--:|--:|
| baseline_fixed_size | 84 | 41,530.08 | 143,577.23 | 24,615.85 | 1.0000x | 3 |
| weak_75pct | 84 | 37,983.63 | 134,108.15 | 21,372.19 | 0.9286x | 3 |
| weak_50pct | 84 | 34,437.19 | 124,639.07 | 18,128.52 | 0.8571x | 3 |
| strong_110_weak_75 | 84 | 41,485.81 | 142,303.24 | 22,536.31 | 0.9917x | 3 |
| strong_125_weak_50 | 84 | 43,192.64 | 145,126.80 | 21,038.82 | 1.0149x | 4 |
| train_normalized_weak_50 | 84 | 39,729.32 | 143,792.96 | 20,914.42 | 0.9889x | 3 |

## Limits and fairness

A lower rupee loss from smaller positions is partly less risk deployed, not fewer losing trades. Increasing strong-position size can increase profit by increasing exposure; compare the exposure column. The normalized arm uses a single normalization constant computed from earlier admitted orders, not which orders eventually filled or any later results. It targets equal earlier-order notional, not exact equal filled exposure in every period.

Source native fees and P&L are retained alongside adjusted values. The sensitivity assumes linear 5bps costs, unchanged fills/slippage and fractional notional. Real integer share counts, fixed fee components and size-dependent market impact can differ. No sizing change is proposed for LIVE. The later six sessions were previously observed, so are not independent forward validation.

[Day-wise results](daily_results.csv) · [Every trade and factor](trade_comparison.csv) · [Configuration](config.json)
