# V21 noon-CHOP with ADX/RSI overlay through 4 November 2026

Backtest-only replay of the seven saved ADX/RSI admission masks on the earlier **post-hoc** all-G7 + CHOP14<=50 from noon candidate. The same native V21 fills and after-cost P&L are used, with unlimited capital and no promotion of the next ranked stock. Both the noon-CHOP clock condition and these ADX/RSI combinations were examined on this history; this is **not a forward validation** or a live setting.

## All 65 sealed sessions

| Extra ADX/RSI mask | Selected | Fills | Losses | Win rate | Mean/trade (Rs) | Median/trade (Rs) | Net P&L (Rs) | Median/day (Rs) | Daily-close DD (Rs) |
|:---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| all_g7 | 820 | 727 | 227 | 68.78% | 1,585.94 | 2,658.83 | 1,152,981.80 | 15,130.19 | 24,615.85 |
| adx14_ge25 | 679 | 599 | 185 | 69.12% | 1,635.83 | 2,792.18 | 979,864.34 | 12,902.72 | 27,794.22 |
| di_direction | 808 | 716 | 224 | 68.72% | 1,593.33 | 2,707.11 | 1,140,823.58 | 15,130.19 | 24,137.98 |
| adx14_ge25_and_di_direction | 670 | 591 | 182 | 69.20% | 1,650.59 | 2,867.27 | 975,497.60 | 11,590.80 | 26,939.14 |
| rsi14_directional_50 | 812 | 720 | 224 | 68.89% | 1,597.64 | 2,685.91 | 1,150,299.07 | 15,130.19 | 24,137.98 |
| adx14_ge25_di_direction_rsi14_directional_50 | 670 | 591 | 182 | 69.20% | 1,650.59 | 2,867.27 | 975,497.60 | 11,590.80 | 26,939.14 |
| adx14_rising3_di_direction_rsi14_rising3 | 594 | 517 | 155 | 70.02% | 1,702.12 | 2,692.33 | 879,996.65 | 11,028.97 | 22,814.66 |

## Six later observed sessions

28 October through 4 November is a retrospective sensitivity check. Only two filled trades were vetoed by the noon-CHOP condition in this period, so it offers very little evidence for that rule.

| Extra ADX/RSI mask | Fills | Losses | Win rate | Mean/trade (Rs) | Median/trade (Rs) | Net P&L (Rs) | Daily-close DD (Rs) |
|:---|---:|---:|---:|---:|---:|---:|---:|
| all_g7 | 82 | 35 | 57.32% | 515.76 | 2,047.53 | 42,291.96 | 24,615.85 |
| adx14_ge25 | 62 | 27 | 56.45% | 393.86 | 2,410.43 | 24,419.27 | 27,794.22 |
| di_direction | 81 | 34 | 58.02% | 532.68 | 2,297.68 | 43,147.04 | 23,760.77 |
| adx14_ge25_and_di_direction | 61 | 26 | 57.38% | 414.33 | 2,429.14 | 25,274.35 | 26,939.14 |
| rsi14_directional_50 | 81 | 34 | 58.02% | 532.68 | 2,297.68 | 43,147.04 | 23,760.77 |
| adx14_ge25_di_direction_rsi14_directional_50 | 61 | 26 | 57.38% | 414.33 | 2,429.14 | 25,274.35 | 26,939.14 |
| adx14_rising3_di_direction_rsi14_rising3 | 54 | 23 | 57.41% | 799.65 | 1,753.62 | 43,181.18 | 9,389.33 |

## Interpretation

- The `all_g7` row is the exact noon-CHOP baseline, even though its name reflects the ADX source arm. Its earlier result matches every available metric and each daily row within Rs 0.01.
- Baseline: 727 fills, 227 losses, Rs 1,152,981.80 net. ADX/RSI masks can raise win rate and per-trade profit while skipping profitable trades; the table shows the total-P&L and drawdown tradeoffs.
- ADX14 is a session-reset Wilder feature from exact, hash-verified stock confirmation prefixes. RSI14 is the saved G7 session-reset feature. Missing early indicators pass through. Every mask is causal at confirmation, but thresholds were tested against the already observed 65 sessions.
- Daily-close drawdown uses realized daily P&L, not intraday lows. All after-cost trade amounts use the saved V21 unlimited-capital allocation at unchanged per-entry size and exit rules.

[Overall](overall.csv) | [Day-wise](daily.csv) | [Admission decisions](admission_audit.csv) | [Trade decisions](trade_decisions.csv) | [Configuration](config.json) | [Provenance](provenance.json)
