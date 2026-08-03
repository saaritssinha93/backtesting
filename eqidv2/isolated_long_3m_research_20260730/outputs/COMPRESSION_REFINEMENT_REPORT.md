# Compression breakout refinement

## Classification: PROFITABLE_BUT_INSUFFICIENT_SAMPLE

This rule was selected using TRAIN and VALIDATION only. TEST was evaluated after the configuration, late-session window, ranking limit, target, stop, and time exit were frozen.

## Exact rule

- Base configuration: `compression_129` (strict / bb10 / not_bearish)
- Signal window: 14:00-14:29
- Simultaneous rank limit: unlimited
- Target: 0.75%
- Stop: 0.70%
- Time exit: 60 minutes, capped by 15:15

| Split | Trades | PF | Expectancy | Net P&L | Active days | Top day share | Top ticker share |
|---|---:|---:|---:|---:|---:|---:|---:|
| TRAIN | 38 | 1.262 | Rs 67.48 | Rs 2,564.10 | 9 | 41.75% | 5.40% |
| VALIDATION | 12 | 1.263 | Rs 44.35 | Rs 532.19 | 3 | 51.48% | 26.06% |
| TEST | 8 | 3.163 | Rs 226.58 | Rs 1,812.63 | 3 | 37.70% | 25.14% |

At 150% of normal exit slippage, TEST PF is 2.833, expectancy is Rs 214.35, and net P&L is Rs 1,714.80.

## Honest interpretation

Profitable neighboring target/stop cases: TRAIN 9/9, VALIDATION 9/9, TEST 9/9.

A positive TEST result with fewer than 100 trades is not sufficient for production promotion. High day/ticker concentration also fails the framework's robustness gates. Treat this as a profitable research lead for forward paper collection, not a proven live strategy.