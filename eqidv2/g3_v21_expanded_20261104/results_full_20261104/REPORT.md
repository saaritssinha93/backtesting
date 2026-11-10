# G3 V21 expanded backtest through 14:00

Research-only V21 variant. The frozen V21 code and saved results were not edited.
The replay covers the same 64 sealed sessions with expanded setup selection and new-pair exits.
Every setup/direction pair can select at most three stocks per day.

## Rules

- All 61 configured G3 donor setup/direction pairs through 13:30 are enabled. The 34 already enabled V21 pairs retain their targets and V21 confirmation rules.
- The 27 newly enabled donor pairs use 0.8% target and 1.25% initial stop.
- Six new five-minute signal clocks, 13:35 through 14:00, add LONG and SHORT pairs (12 total). All have 0.8% target and 1.25% initial stop. A 14:00 signal is confirmed at 14:01.
- New LONG pairs clone the untuned 13:30 LONG entry gates; new SHORT pairs clone the 13:25 SHORT gates. Exact next-minute confirmation, EMA stack, price, OI and volume gates remain required. Orders are ranked by liquidity with core gates first.
- The inherited G3 stop starts at 1.25% and tightens to 1.0% after 120 minutes. The shared portfolio simulator retains ₹100,000 capital per entry, 5× exposure, ₹1,000,000 capital, 10-minute entry expiry, 5 bps round-trip costs and 15:15 square-off.

| New 13:35–14:00 side | Directional price | OI rise | 5m volume ratio | 1m body | Directional wick max | 1m volume ratio |
|:---|---:|---:|---:|---:|---:|---:|
| LONG | ≥0.15% | ≥0.07% | ≥1.40 | ≥0.55 | ≤0.45 | ≥0.80 |
| SHORT | ≥0.13% | ≥0.05% | ≥1.00 | ≥0.40 | ≤0.60 | ≥1.20 |

## Full-history comparison

| Arm | Sessions | Orders | Executed | Win rate | Net P&L (₹) | Daily-close drawdown (₹) | Worst day (₹) |
|:---|---:|---:|---:|---:|---:|---:|---:|
| Frozen V21 | 64 | 635 | 518 | 71.62% | 1,013,391.20 | 28,166.32 | -24,501.07 |
| V21 expanded cap 3 | 64 | 1320 | 940 | 64.68% | 1,127,051.92 | 58,821.11 | -32,363.80 |

**P&L change:** ₹113,660.72. **Executed-trade change:** +422. **Frozen V21 reproduction:** PASS_64_OF_64_DAYS.

## Source coverage

The replay appended 112,326 completed-clock stock rows from dated archives or hash-checked daily snapshots. Expected rows still missing: 116 on 1 sessions. Original V21 feature rows and execution minutes were preserved. A feature row may still lack futures OI; approximately 2.62% of the six late-clock stock rows in the first 38 archive sessions had unusable OI and failed the strict gate. October 1 is excluded from both arms because its immutable source is incomplete.

## Five sessions after the V21 allowlist cutoff

| Arm | Orders | Executed | Win rate | Net P&L (₹) | Worst day (₹) |
|:---|---:|---:|---:|---:|---:|
| Frozen V21 | 57 | 50 | 60.00% | 27,717.05 | -24,501.07 |
| V21 expanded cap 3 | 123 | 86 | 56.98% | 46,095.88 | -31,552.27 |

## Selection changes

- Newly selected versus frozen V21: 743 orders.
- Removed by the three-entry cap/ranking: 58 frozen V21 orders.
- New 13:35–14:00 pairs: 130 orders; 70 portfolio executions.

## Day-wise results

| Day | V21 orders | Expanded orders | V21 trades | Expanded trades | V21 P&L (₹) | Expanded P&L (₹) | Change (₹) |
|:---|---:|---:|---:|---:|---:|---:|---:|
| 2026-07-29 | 14 | 19 | 12 | 15 | 70,956.79 | 79,097.53 | 8,140.75 |
| 2026-07-30 | 4 | 15 | 4 | 15 | 11,988.07 | 10,157.21 | -1,830.86 |
| 2026-07-31 | 10 | 25 | 10 | 17 | 43,637.59 | 53,047.63 | 9,410.04 |
| 2026-08-03 | 14 | 26 | 12 | 16 | 38,667.82 | 41,822.99 | 3,155.16 |
| 2026-08-04 | 16 | 33 | 13 | 15 | 28,258.58 | 27,509.00 | -749.58 |
| 2026-08-05 | 20 | 20 | 14 | 14 | 27,395.83 | 45,270.46 | 17,874.63 |
| 2026-08-06 | 13 | 34 | 11 | 23 | 11,551.86 | 32,005.05 | 20,453.19 |
| 2026-08-07 | 7 | 19 | 7 | 13 | -1,625.10 | 2,776.42 | 4,401.52 |
| 2026-08-10 | 11 | 26 | 10 | 22 | 46,627.95 | 65,904.32 | 19,276.38 |
| 2026-08-11 | 12 | 26 | 10 | 16 | 21,866.38 | 11,231.93 | -10,634.45 |
| 2026-08-12 | 11 | 22 | 8 | 17 | 9,709.96 | 1,505.79 | -8,204.17 |
| 2026-08-13 | 9 | 13 | 6 | 10 | 5,027.38 | 3,169.45 | -1,857.93 |
| 2026-08-14 | 8 | 17 | 8 | 13 | 10,029.18 | 15,957.79 | 5,928.61 |
| 2026-08-17 | 3 | 9 | 2 | 8 | -10,500.00 | -11,059.40 | -559.40 |
| 2026-08-18 | 10 | 23 | 8 | 16 | -2,289.56 | -718.79 | 1,570.76 |
| 2026-08-19 | 7 | 7 | 7 | 7 | 5,255.14 | 5,255.14 | 0.00 |
| 2026-08-20 | 6 | 15 | 6 | 15 | 4,373.98 | 2,923.90 | -1,450.08 |
| 2026-08-21 | 7 | 9 | 7 | 8 | -5,928.33 | -5,003.49 | 924.84 |
| 2026-08-26 | 14 | 26 | 13 | 18 | 45,535.13 | 58,800.37 | 13,265.24 |
| 2026-08-27 | 10 | 29 | 9 | 19 | 29,098.88 | 22,348.39 | -6,750.49 |
| 2026-08-28 | 12 | 24 | 11 | 18 | 47,084.95 | 47,688.28 | 603.33 |
| 2026-08-31 | 8 | 26 | 7 | 19 | 6,712.77 | 3,542.77 | -3,170.00 |
| 2026-09-01 | 6 | 25 | 5 | 19 | 17,975.37 | 22,709.97 | 4,734.59 |
| 2026-09-02 | 6 | 13 | 5 | 10 | 10,406.56 | 22,401.00 | 11,994.44 |
| 2026-09-03 | 8 | 14 | 6 | 11 | 12,566.01 | 21,955.56 | 9,389.55 |
| 2026-09-04 | 15 | 25 | 14 | 20 | 36,297.15 | 31,300.53 | -4,996.62 |
| 2026-09-07 | 11 | 23 | 8 | 12 | 17,485.43 | 7,944.26 | -9,541.16 |
| 2026-09-08 | 6 | 15 | 3 | 12 | 7,552.15 | 10,532.12 | 2,979.97 |
| 2026-09-09 | 15 | 27 | 10 | 14 | 11,777.61 | 20,775.54 | 8,997.93 |
| 2026-09-10 | 12 | 16 | 10 | 10 | -15,829.60 | -13,706.77 | 2,122.83 |
| 2026-09-11 | 6 | 15 | 6 | 12 | 31,051.92 | 49,995.68 | 18,943.76 |
| 2026-09-15 | 13 | 19 | 12 | 14 | 63,970.77 | 66,565.20 | 2,594.42 |
| 2026-09-16 | 3 | 9 | 3 | 8 | -5,161.13 | -9,208.74 | -4,047.61 |
| 2026-09-17 | 4 | 19 | 4 | 14 | -392.30 | -12,021.10 | -11,628.80 |
| 2026-09-18 | 5 | 11 | 5 | 11 | 10,081.13 | 7,893.92 | -2,187.21 |
| 2026-09-21 | 6 | 11 | 6 | 11 | 15,498.20 | 33,121.76 | 17,623.56 |
| 2026-09-22 | 11 | 23 | 10 | 17 | 31,659.70 | 46,182.55 | 14,522.85 |
| 2026-09-23 | 4 | 15 | 4 | 13 | 18,982.86 | 23,030.64 | 4,047.78 |
| 2026-09-24 | 3 | 23 | 1 | 14 | 4,480.33 | 21,315.49 | 16,835.16 |
| 2026-09-25 | 7 | 19 | 6 | 13 | 27,396.34 | -9,381.68 | -36,778.02 |
| 2026-09-28 | 12 | 26 | 9 | 11 | 20,370.40 | 22,184.95 | 1,814.55 |
| 2026-09-29 | 8 | 19 | 6 | 15 | 13,397.04 | 41,887.81 | 28,490.78 |
| 2026-09-30 | 7 | 19 | 7 | 17 | 13,858.04 | 9,179.11 | -4,678.93 |
| 2026-10-05 | 3 | 9 | 3 | 6 | 24,149.06 | 26,714.40 | 2,565.35 |
| 2026-10-06 | 9 | 24 | 9 | 17 | 13,254.98 | -902.73 | -14,157.71 |
| 2026-10-07 | 19 | 33 | 12 | 17 | 12,127.88 | 15,962.43 | 3,834.55 |
| 2026-10-08 | 45 | 35 | 15 | 19 | 54,005.78 | 52,713.51 | -1,292.27 |
| 2026-10-09 | 4 | 10 | 3 | 8 | 4,519.67 | 15,779.46 | 11,259.79 |
| 2026-10-12 | 8 | 16 | 7 | 13 | 23,844.71 | 23,855.92 | 11.22 |
| 2026-10-13 | 13 | 25 | 12 | 15 | 656.67 | -15,919.14 | -16,575.81 |
| 2026-10-14 | 9 | 18 | 7 | 16 | 13,326.72 | 27,311.37 | 13,984.65 |
| 2026-10-15 | 8 | 18 | 7 | 15 | -3,593.47 | -21,409.42 | -17,815.95 |
| 2026-10-16 | 7 | 20 | 6 | 19 | -21,355.34 | -4,764.04 | 16,591.30 |
| 2026-10-19 | 3 | 14 | 3 | 11 | 11,402.28 | 22,275.28 | 10,873.00 |
| 2026-10-21 | 23 | 29 | 18 | 19 | 70,442.80 | 66,062.99 | -4,379.81 |
| 2026-10-22 | 6 | 24 | 6 | 20 | -7,450.09 | -32,363.80 | -24,913.71 |
| 2026-10-23 | 12 | 28 | 11 | 16 | -11,983.89 | -26,457.30 | -14,473.41 |
| 2026-10-26 | 11 | 29 | 10 | 18 | 2,617.18 | 17,731.90 | 15,114.73 |
| 2026-10-27 | 4 | 16 | 4 | 13 | 12,850.00 | -13,525.35 | -26,375.35 |
| 2026-10-28 | 3 | 20 | 3 | 17 | 141.44 | 9,205.48 | 9,064.04 |
| 2026-10-29 | 18 | 36 | 15 | 19 | 44,268.18 | 51,284.73 | 7,016.54 |
| 2026-10-30 | 11 | 18 | 10 | 15 | -24,501.07 | -31,552.27 | -7,051.19 |
| 2026-11-02 | 12 | 25 | 10 | 18 | -3,665.25 | -5,749.56 | -2,084.31 |
| 2026-11-03 | 13 | 24 | 12 | 17 | 11,473.75 | 22,907.50 | 11,433.75 |

## Newly enabled setup activity

| Setup | Class | Selected | Executed | Portfolio net P&L (₹) |
|:---|:---|---:|---:|---:|
| 1336_LONG | new_1335_to_1400 | 7 | 4 | 231.04 |
| 1336_SHORT | new_1335_to_1400 | 7 | 3 | 8,200.67 |
| 1341_LONG | new_1335_to_1400 | 10 | 4 | -1,772.97 |
| 1341_SHORT | new_1335_to_1400 | 21 | 17 | 17,257.34 |
| 1346_LONG | new_1335_to_1400 | 10 | 4 | 1,516.98 |
| 1346_SHORT | new_1335_to_1400 | 12 | 5 | 4,709.90 |
| 1351_LONG | new_1335_to_1400 | 10 | 3 | -3,003.06 |
| 1351_SHORT | new_1335_to_1400 | 18 | 12 | -9,679.51 |
| 1356_LONG | new_1335_to_1400 | 5 | 4 | 7,747.33 |
| 1356_SHORT | new_1335_to_1400 | 12 | 6 | 5,451.27 |
| 1401_LONG | new_1335_to_1400 | 7 | 3 | 6,271.42 |
| 1401_SHORT | new_1335_to_1400 | 11 | 5 | 2,051.45 |
| 0921_SHORT | newly_enabled_donor | 15 | 12 | -21,590.77 |
| 0931_LONG | newly_enabled_donor | 7 | 6 | 6,224.28 |
| 0936_LONG | newly_enabled_donor | 10 | 9 | 11,045.06 |
| 0941_LONG | newly_enabled_donor | 19 | 17 | -18,249.72 |
| 0941_SHORT | newly_enabled_donor | 22 | 21 | -2,210.12 |
| 0946_LONG | newly_enabled_donor | 7 | 6 | 4,500.00 |
| 0946_SHORT | newly_enabled_donor | 4 | 4 | -8,725.33 |
| 1001_LONG | newly_enabled_donor | 11 | 9 | -3,500.00 |
| 1011_LONG | newly_enabled_donor | 13 | 12 | -4,076.91 |
| 1111_SHORT | newly_enabled_donor | 19 | 16 | 10,058.31 |
| 1141_SHORT | newly_enabled_donor | 24 | 18 | 15,841.73 |
| 1146_SHORT | newly_enabled_donor | 40 | 31 | 6,776.23 |
| 1216_SHORT | newly_enabled_donor | 47 | 34 | 40,011.89 |
| 1226_SHORT | newly_enabled_donor | 24 | 15 | 12,297.22 |
| 1231_SHORT | newly_enabled_donor | 22 | 10 | 11,747.39 |
| 1236_LONG | newly_enabled_donor | 29 | 22 | 23,151.58 |
| 1246_LONG | newly_enabled_donor | 35 | 26 | 11,697.90 |
| 1246_SHORT | newly_enabled_donor | 26 | 17 | -13,413.11 |
| 1251_LONG | newly_enabled_donor | 24 | 14 | 8,126.88 |
| 1251_SHORT | newly_enabled_donor | 23 | 9 | -3,663.48 |
| 1256_SHORT | newly_enabled_donor | 29 | 11 | 7,117.11 |
| 1306_LONG | newly_enabled_donor | 28 | 20 | 10,728.99 |
| 1316_LONG | newly_enabled_donor | 32 | 19 | 19,590.77 |
| 1316_SHORT | newly_enabled_donor | 15 | 7 | 3,623.90 |
| 1321_SHORT | newly_enabled_donor | 47 | 18 | -2,396.70 |
| 1326_LONG | newly_enabled_donor | 31 | 17 | 6,315.51 |
| 1326_SHORT | newly_enabled_donor | 10 | 5 | -7,567.74 |

**Interpretation:** The new rules and slots were evaluated on historical sessions; their P&L is in-sample evidence, not a forward expectation. The frozen V21 allowlist used outcomes through 27 October, leaving five later sealed sessions. Per-slot P&L is the expanded portfolio's realized contribution and cannot be summed to infer the strategy change because orders compete for capital.

[Daily CSV](daily_comparison.csv) · [Slot CSV](slot_results.csv) · [Expanded trades](expanded_trades.csv) · [Selection audit](selection_audit.csv) · [Source coverage](source_coverage.csv)
