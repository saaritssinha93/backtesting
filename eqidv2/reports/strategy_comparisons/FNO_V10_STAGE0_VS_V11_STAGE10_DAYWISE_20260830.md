# Frozen V10 Stage 0 versus V11 Stage 10 — daywise comparison

Comparison scope: `FULL / REFERENCE_15_0`. Frozen V10 Stage 0 is reference-only, so no honest V10 stress-scenario daywise comparison exists.

Source run: `C:\TradingData\eqidv2\fno_oi\strategy_research\v11_fno_staged_research_v2\run_20260830T204713657310+0530`

The complete 65-session ledger, including all unchanged dates and cumulative deltas, is stored in `fno_v10_stage0_vs_v11_stage10_daywise_reference_20260830.csv` beside this report.

## Reconciliation

| Metric | Frozen V10 Stage 0 | V11 Stage 10 | Difference |
|---|---:|---:|---:|
| Sessions | 65 | 65 | 0 |
| Fills | 232 | 237 | +5 |
| Wins | 116 | 123 | +7 |
| Losses | 116 | 114 | -2 |
| Net points | 73.054423 | 94.630860 | +21.576437 |
| Modeled net P&L | Rs 36,312.05 | Rs 46,783.23 | Rs +10,471.18 |

Stage 10 improved 15 sessions, worsened 6, and was economically identical on 44. Total favorable daily deltas were +25.963676 points, while adverse daily deltas were -4.387239 points.

Trade-level reconciliation is exact: 218 fills are common and economically unchanged. Stage 9 adds 19 fills worth +16.481129 points; Stage 4 removes 14 V10 fills whose combined result was -5.095308 points. Together these explain the entire +21.576437-point Stage 10 lift.

## Changed sessions

| Date | V10 pts | Stage 10 pts | Delta pts | Delta P&L | Fills | Cumulative delta |
|---|---:|---:|---:|---:|---:|---:|
| 2026-06-02 | -0.6157 | 0.5451 | +1.1608 | Rs +579.48 | 4 -> 3 | +1.1608 |
| 2026-06-03 | 5.6647 | 8.2162 | +2.5515 | Rs +1,271.98 | 5 -> 7 | +3.7123 |
| 2026-06-16 | -1.4119 | -0.6280 | +0.7839 | Rs +370.25 | 2 -> 2 | +4.4962 |
| 2026-06-24 | 0.5380 | -0.6198 | -1.1578 | Rs -473.32 | 3 -> 4 | +3.3384 |
| 2026-06-25 | 3.3271 | 3.2024 | -0.1247 | Rs -62.10 | 5 -> 4 | +3.2137 |
| 2026-06-29 | 4.6595 | 5.8985 | +1.2390 | Rs +600.56 | 7 -> 7 | +4.4527 |
| 2026-06-30 | -1.1540 | -2.3040 | -1.1500 | Rs -570.86 | 1 -> 2 | +3.3027 |
| 2026-07-07 | 9.6513 | 11.4999 | +1.8486 | Rs +887.95 | 7 -> 8 | +5.1513 |
| 2026-07-10 | 1.1639 | 4.0132 | +2.8492 | Rs +1,388.41 | 6 -> 7 | +8.0005 |
| 2026-07-14 | -0.8854 | -1.5368 | -0.6514 | Rs -322.71 | 4 -> 5 | +7.3491 |
| 2026-07-16 | -1.7224 | -1.2090 | +0.5134 | Rs +257.25 | 4 -> 4 | +7.8625 |
| 2026-07-20 | -1.6316 | -0.4791 | +1.1526 | Rs +574.29 | 6 -> 5 | +9.0151 |
| 2026-07-22 | 5.9358 | 6.9763 | +1.0404 | Rs +485.00 | 10 -> 8 | +10.0555 |
| 2026-07-23 | 6.1584 | 11.8417 | +5.6833 | Rs +2,581.37 | 10 -> 11 | +15.7388 |
| 2026-07-24 | 4.0688 | 4.2355 | +0.1667 | Rs +97.70 | 9 -> 8 | +15.9055 |
| 2026-07-28 | 8.7274 | 8.0743 | -0.6531 | Rs -319.48 | 9 -> 10 | +15.2524 |
| 2026-08-03 | 1.9752 | 4.3181 | +2.3429 | Rs +1,148.00 | 4 -> 5 | +17.5953 |
| 2026-08-06 | 1.8673 | 1.2171 | -0.6502 | Rs -318.60 | 8 -> 9 | +16.9451 |
| 2026-08-12 | -2.7677 | -1.6103 | +1.1574 | Rs +576.98 | 5 -> 4 | +18.1024 |
| 2026-08-24 | 1.1506 | 1.3971 | +0.2464 | Rs +107.32 | 3 -> 2 | +18.3489 |
| 2026-08-25 | 1.7384 | 4.9659 | +3.2275 | Rs +1,611.72 | 6 -> 8 | +21.5764 |

## Monthly difference

| Month | Improved | Worse | Unchanged | Delta points |
|---|---:|---:|---:|---:|
| May 2026 | 0 | 0 | 2 | 0.000000 |
| June 2026 | 4 | 3 | 14 | +3.302705 |
| July 2026 | 7 | 2 | 14 | +11.949737 |
| August 2026 | 4 | 1 | 14 | +6.323996 |

The best delta was 23-Jul at +5.683320 points. The worst points delta was 24-Jun at -1.157828; the largest modeled rupee reduction was 30-Jun at Rs -570.86.

One session flipped from loss to profit (2-Jun), and one flipped from profit to loss (24-Jun). The total number of positive, negative, and flat strategy days remained 37, 25, and 3 respectively.

The improvement is concentrated: the top three dates—23-Jul, 25-Aug, and 10-Jul—supply 54.50% of the net Stage 10 lift. This is evidence for prospective validation, not a claim of future performance.
