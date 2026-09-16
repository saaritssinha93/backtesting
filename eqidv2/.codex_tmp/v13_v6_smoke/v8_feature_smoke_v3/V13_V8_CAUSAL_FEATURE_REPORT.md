# V13-v8 Causal Futures Feature Shadow

Status: **RESEARCH ONLY — NOT PROMOTED**

Features use completed futures minutes before equity entry. Time-of-day baselines use only earlier sessions of the same contract. Outcome P&L is attached afterward for descriptive analysis.

- Input trades: 101
- Feature status counts: `{"INSUFFICIENT_CAUSAL_HISTORY": 30, "MISSING_READY_FUTURES_COVERAGE": 41, "READY": 30}`

| period   | v8_shadow_pass   |   trades |   wins |   losses |   win_rate_pct |   net_profit_rupees |   profit_factor |   average_pnl_rupees |
|:---------|:-----------------|---------:|-------:|---------:|---------------:|--------------------:|----------------:|---------------------:|
| 2026-07  | False            |        1 |      1 |        0 |       100      |            11987.5  |       inf       |             11987.5  |
| 2026-07  | True             |       10 |      9 |        1 |        90      |            55275.6  |        80.1053  |              5527.56 |
| 2026-08  | False            |        2 |      1 |        1 |        50      |             3157.43 |         1.65172 |              1578.71 |
| 2026-08  | True             |       17 |     14 |        3 |        82.3529 |            82079.5  |         6.04809 |              4828.21 |

The shadow pass thresholds are engineering guardrails, not an optimized strategy. They require walk-forward validation before any live or backtest selection change.
