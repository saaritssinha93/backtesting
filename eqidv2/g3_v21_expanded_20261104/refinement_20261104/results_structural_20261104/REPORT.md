# Expanded G3 V21: structural trade-reduction ablations

All arms reuse the same saved native fills, targets, stops, costs and minute paths. A filtered arm reapplies the original causal portfolio constraints so freed capital can admit remaining fills. Exact expanded baseline reapplication passed on all 1,320 orders.

These policies were devised after inspecting the 64-session history; the five sessions after 27 October are a small later cohort already used for research, not an independent validation set.

## Full 64 sessions

| Policy | Orders | Executed | Win rate | Net P&L (₹) | Negative days | Worst day (₹) | Daily-close drawdown (₹) |
|:---|---:|---:|---:|---:|---:|---:|---:|
| frozen_v21 | 635 | 518 | 71.62% | 1,013,391.20 | 13 | -24,501.07 | 28,166.32 |
| expanded_all | 1320 | 940 | 64.68% | 1,127,051.92 | 16 | -32,363.80 | 58,821.11 |
| existing34_only | 577 | 503 | 71.97% | 1,012,049.55 | 13 | -21,355.34 | 24,948.81 |
| existing34_plus_new_late12 | 707 | 612 | 69.77% | 1,045,576.28 | 11 | -19,395.37 | 23,060.62 |
| existing34_plus_donor_new_after_11_and_late | 1212 | 874 | 66.02% | 1,181,807.12 | 16 | -30,207.30 | 49,155.58 |
| existing34_plus_donor_new_after_12_and_late | 1129 | 845 | 66.51% | 1,169,193.64 | 15 | -36,668.31 | 62,524.39 |
| new_donor_cap1 | 1145 | 886 | 67.04% | 1,186,435.50 | 16 | -29,639.78 | 42,464.44 |
| all73_cap2 | 1233 | 926 | 65.44% | 1,156,881.67 | 19 | -32,497.90 | 55,643.71 |
| all73_cap1 | 986 | 826 | 67.92% | 1,176,923.69 | 16 | -37,283.28 | 52,673.37 |
| expanded_max_positions8 | 1320 | 824 | 65.29% | 1,063,225.53 | 16 | -30,157.97 | 50,462.07 |
| expanded_max_positions9 | 1320 | 889 | 65.02% | 1,098,612.19 | 15 | -31,552.27 | 55,869.29 |

## Five sessions after 27 October

| Policy | Orders | Executed | Win rate | Net P&L (₹) | Negative days | Daily-close drawdown (₹) |
|:---|---:|---:|---:|---:|---:|---:|
| frozen_v21 | 57 | 50 | 60.00% | 27,717.05 | 2 | 28,166.32 |
| expanded_all | 123 | 86 | 56.98% | 46,095.88 | 2 | 37,301.82 |
| existing34_only | 55 | 49 | 61.22% | 34,217.05 | 2 | 21,666.32 |
| existing34_plus_new_late12 | 63 | 55 | 58.18% | 29,584.26 | 2 | 23,060.62 |
| existing34_plus_donor_new_after_11_and_late | 109 | 82 | 57.32% | 42,545.29 | 2 | 34,551.82 |
| existing34_plus_donor_new_after_12_and_late | 98 | 77 | 59.74% | 51,679.26 | 2 | 32,639.34 |
| new_donor_cap1 | 108 | 83 | 55.42% | 27,827.25 | 2 | 42,464.44 |
| all73_cap2 | 115 | 85 | 56.47% | 36,821.52 | 2 | 47,137.99 |
| all73_cap1 | 95 | 78 | 52.56% | 12,953.29 | 2 | 52,673.37 |
| expanded_max_positions8 | 123 | 73 | 54.79% | 28,547.41 | 2 | 30,503.68 |
| expanded_max_positions9 | 123 | 80 | 55.00% | 29,003.11 | 2 | 34,647.98 |

[Overall CSV](overall.csv) | [Day-wise CSV](daily.csv) | [Policy config](config.json) | [Provenance](provenance.json)
