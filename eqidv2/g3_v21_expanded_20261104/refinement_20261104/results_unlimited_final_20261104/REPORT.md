# Expanded G3 V21 under unlimited portfolio capital

All native filled orders execute. Each order retains its saved ₹100,000 capital, 5× exposure, target, stop, cost and execution path. The accepted portfolio constraint engine was reapplied at a ₹1 trillion capital ceiling; executed flags equal native filled flags and portfolio P&L equals direct native fill P&L for every order.

These structural policies were selected after examining the 64-session history. The five later sessions are a small retrospective check, not a fresh holdout.

## Full 64 sessions

| Policy | Orders | Trades | Losing trades | Win rate | Net P&L (₹) | Negative days | Worst day (₹) | Daily-close drawdown (₹) |
|:---|---:|---:|---:|---:|---:|---:|---:|---:|
| frozen_v21 | 635 | 561 | 158 | 71.84% | 1,122,397.79 | 13 | -24,501.07 | 28,166.32 |
| expanded_all | 1320 | 1158 | 423 | 63.47% | 1,212,221.43 | 14 | -62,286.27 | 108,801.51 |
| existing34_only | 577 | 508 | 143 | 71.85% | 1,018,257.68 | 13 | -21,355.34 | 24,948.81 |
| existing34_plus_new_late12 | 707 | 622 | 189 | 69.61% | 1,054,416.02 | 11 | -19,395.37 | 23,060.62 |
| existing34_plus_donor_new_after_11_and_late | 1212 | 1062 | 378 | 64.41% | 1,248,804.95 | 15 | -66,036.27 | 101,074.45 |
| existing34_plus_donor_new_after_12_and_late | 1129 | 991 | 348 | 64.88% | 1,216,219.80 | 16 | -59,497.28 | 100,378.24 |
| new_donor_cap1 | 1145 | 1003 | 341 | 66.00% | 1,262,630.80 | 17 | -34,474.78 | 47,754.89 |
| all73_cap2 | 1233 | 1083 | 388 | 64.17% | 1,225,059.47 | 16 | -58,624.78 | 89,460.73 |
| all73_cap1 | 986 | 872 | 282 | 67.66% | 1,220,734.59 | 16 | -37,283.28 | 51,463.81 |

## Five sessions after 27 October

| Policy | Trades | Losing trades | Win rate | Net P&L (₹) | Negative days | Daily-close drawdown (₹) |
|:---|---:|---:|---:|---:|---:|---:|
| frozen_v21 | 53 | 23 | 56.60% | 13,675.18 | 2 | 28,166.32 |
| expanded_all | 108 | 48 | 55.56% | 28,215.27 | 2 | 42,167.37 |
| existing34_only | 51 | 21 | 58.82% | 26,675.18 | 2 | 21,666.32 |
| existing34_plus_new_late12 | 58 | 25 | 56.90% | 25,792.39 | 2 | 23,060.62 |
| existing34_plus_donor_new_after_11_and_late | 97 | 43 | 55.67% | 30,522.94 | 2 | 39,417.37 |
| existing34_plus_donor_new_after_12_and_late | 88 | 37 | 57.95% | 45,062.94 | 2 | 31,004.89 |
| new_donor_cap1 | 94 | 42 | 55.32% | 27,233.14 | 2 | 47,754.89 |
| all73_cap2 | 102 | 45 | 55.88% | 29,124.27 | 2 | 42,178.44 |
| all73_cap1 | 82 | 38 | 53.66% | 19,574.22 | 2 | 51,463.81 |

## Interpretation

For a risk-first reduction, `existing34_plus_new_late12` substantially cuts trades, losing trades, losing days, and daily-close drawdown compared with `expanded_all`, while giving up some net P&L. The higher-P&L `new_donor_cap1` reduces trades and drawdown but still has more negative days than `expanded_all`. These are retrospective comparisons, not a validated trading recommendation.

[Overall CSV](overall.csv) | [Day-wise CSV](daily.csv) | [Policy definitions](config.json) | [Provenance](provenance.json)
