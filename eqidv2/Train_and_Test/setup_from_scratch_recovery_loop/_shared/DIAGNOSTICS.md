# ROUND-4 EXECUTION DIAGNOSTICS — B family

_Generated 2026-07-03. Research-only. Broad detection books (family dedupe, no masks) on the recreated enriched pools; TRAIN 2026-03-01..2026-05-30._

## B_AVWAP_RECLAIM_REVERSAL (LONG) — baseline exit 0.7/1.5

### D1 cost anatomy (broad TRAIN book)

| pass | n | PF | sum Rs | target-fill % |
|---|---|---|---|---|
| gross_0bps | 4606 | 0.883 | -201,678 | 15.9 |
| net_5bps | 4606 | 0.544 | -1,018,883 | 14.8 |
| net_15bps | 4606 | 0.324 | -1,899,619 | 12.6 |

### D2 MFE/MAE from entry (medians, % of entry px)

| horizon min | MFE med | MFE p75 | MAE med | MAE p25 | %MFE>=0.5 | %MAE<=-0.7 |
|---|---|---|---|---|---|---|
| 15 | 0.022 | 0.235 | -0.363 | -0.52 | 11.7 | 12.3 |
| 30 | 0.114 | 0.427 | -0.443 | -0.673 | 21.1 | 23.0 |
| 60 | 0.203 | 0.63 | -0.551 | -0.888 | 32.0 | 36.3 |
| 120 | 0.336 | 0.905 | -0.704 | -1.176 | 41.0 | 50.1 |

### D3 retest/limit fill feasibility (pullback depth after signal, ATR units)

| window min | fill@0.3 ATR | fill@0.6 ATR | fill@1.0 ATR | median depth |
|---|---|---|---|---|
| 15 | 79.1% | 65.6% | 43.4% | 0.905 |
| 30 | 84.7% | 75.0% | 58.4% | 1.187 |

### D4 fade (side flipped, statutory @15bps)

| exit | TRAIN n/PF/net | TEST n/PF/net |
|---|---|---|
| 0.7/1.0 | 2244/0.415/Rs-696,503 | 913/0.434/Rs-261,782 |
| 0.9/1.25 | 1853/0.475/Rs-566,340 | 752/0.533/Rs-193,522 |
| 1.2/1.5 | 1541/0.53/Rs-450,346 | 640/0.538/Rs-180,134 |

## B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — baseline exit 1.0/1.5

### D1 cost anatomy (broad TRAIN book)

| pass | n | PF | sum Rs | target-fill % |
|---|---|---|---|---|
| gross_0bps | 1828 | 1.073 | 56,527 | 23.9 |
| net_5bps | 1828 | 0.72 | -265,880 | 22.9 |
| net_15bps | 1828 | 0.472 | -616,217 | 20.8 |

### D2 MFE/MAE from entry (medians, % of entry px)

| horizon min | MFE med | MFE p75 | MAE med | MAE p25 | %MFE>=0.5 | %MAE<=-0.7 |
|---|---|---|---|---|---|---|
| 15 | 0.143 | 0.501 | -0.454 | -0.701 | 25.1 | 25.1 |
| 30 | 0.255 | 0.685 | -0.538 | -0.882 | 34.1 | 36.6 |
| 60 | 0.372 | 0.965 | -0.678 | -1.108 | 43.0 | 48.6 |
| 120 | 0.535 | 1.25 | -0.838 | -1.362 | 52.0 | 56.9 |

### D3 retest/limit fill feasibility (pullback depth after signal, ATR units)

| window min | fill@0.3 ATR | fill@0.6 ATR | fill@1.0 ATR | median depth |
|---|---|---|---|---|
| 15 | 80.7% | 69.1% | 53.4% | 1.073 |
| 30 | 84.6% | 75.4% | 63.7% | 1.427 |

### D4 fade (side flipped, statutory @15bps)

| exit | TRAIN n/PF/net | TEST n/PF/net |
|---|---|---|
| 0.7/1.0 | 1566/0.383/Rs-530,784 | 610/0.326/Rs-231,899 |
| 0.9/1.25 | 1478/0.454/Rs-482,971 | 571/0.398/Rs-208,416 |
| 1.2/1.5 | 1378/0.465/Rs-490,199 | 503/0.445/Rs-183,222 |

## B_HUGE_RED_FAILED_BOUNCE (SHORT) — baseline exit 0.9/1.25

### D1 cost anatomy (broad TRAIN book)

| pass | n | PF | sum Rs | target-fill % |
|---|---|---|---|---|
| gross_0bps | 1300 | 1.137 | 66,902 | 27.5 |
| net_5bps | 1300 | 0.712 | -177,710 | 25.2 |
| net_15bps | 1300 | 0.431 | -441,595 | 21.4 |

### D2 MFE/MAE from entry (medians, % of entry px)

| horizon min | MFE med | MFE p75 | MAE med | MAE p25 | %MFE>=0.5 | %MAE<=-0.7 |
|---|---|---|---|---|---|---|
| 15 | 0.073 | 0.328 | -0.404 | -0.624 | 18.3 | 20.1 |
| 30 | 0.185 | 0.511 | -0.509 | -0.815 | 25.6 | 32.7 |
| 60 | 0.331 | 0.836 | -0.641 | -1.105 | 38.9 | 45.4 |
| 120 | 0.517 | 1.141 | -0.773 | -1.368 | 50.7 | 55.4 |

### D3 retest/limit fill feasibility (pullback depth after signal, ATR units)

| window min | fill@0.3 ATR | fill@0.6 ATR | fill@1.0 ATR | median depth |
|---|---|---|---|---|
| 15 | 81.4% | 66.4% | 48.4% | 0.956 |
| 30 | 87.3% | 75.9% | 60.7% | 1.329 |

### D4 fade (side flipped, statutory @15bps)

| exit | TRAIN n/PF/net | TEST n/PF/net |
|---|---|---|
| 0.7/1.0 | 1183/0.338/Rs-445,999 | 506/0.332/Rs-192,065 |
| 0.9/1.25 | 1140/0.388/Rs-437,137 | 471/0.416/Rs-164,866 |
| 1.2/1.5 | 1063/0.419/Rs-419,020 | 437/0.446/Rs-155,326 |

## B_HUGE_FAILED_BOUNCE (SHORT) — baseline exit 1.2/1.5

### D1 cost anatomy (broad TRAIN book)

| pass | n | PF | sum Rs | target-fill % |
|---|---|---|---|---|
| gross_0bps | 2000 | 1.192 | 148,734 | 20.2 |
| net_5bps | 2000 | 0.762 | -230,908 | 18.9 |
| net_15bps | 2000 | 0.48 | -625,870 | 16.6 |

### D2 MFE/MAE from entry (medians, % of entry px)

| horizon min | MFE med | MFE p75 | MAE med | MAE p25 | %MFE>=0.5 | %MAE<=-0.7 |
|---|---|---|---|---|---|---|
| 15 | 0.069 | 0.317 | -0.359 | -0.568 | 16.1 | 16.1 |
| 30 | 0.194 | 0.527 | -0.446 | -0.724 | 26.1 | 27.0 |
| 60 | 0.322 | 0.822 | -0.555 | -0.982 | 38.7 | 38.6 |
| 120 | 0.491 | 1.124 | -0.678 | -1.222 | 49.9 | 48.4 |

### D3 retest/limit fill feasibility (pullback depth after signal, ATR units)

| window min | fill@0.3 ATR | fill@0.6 ATR | fill@1.0 ATR | median depth |
|---|---|---|---|---|
| 15 | 79.1% | 65.6% | 46.7% | 0.92 |
| 30 | 84.3% | 74.3% | 59.1% | 1.307 |

### D4 fade (side flipped, statutory @15bps)

| exit | TRAIN n/PF/net | TEST n/PF/net |
|---|---|---|
| 0.7/1.0 | 1703/0.311/Rs-681,400 | 740/0.282/Rs-319,495 |
| 0.9/1.25 | 1585/0.36/Rs-645,396 | 666/0.358/Rs-270,617 |
| 1.2/1.5 | 1440/0.391/Rs-608,850 | 596/0.371/Rs-259,099 |

## B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK (LONG) — baseline exit 1.5/2.0

### D1 cost anatomy (broad TRAIN book)

| pass | n | PF | sum Rs | target-fill % |
|---|---|---|---|---|
| gross_0bps | 596 | 1.304 | 88,529 | 23.3 |
| net_5bps | 596 | 0.96 | -13,587 | 22.5 |
| net_15bps | 596 | 0.696 | -124,159 | 21.6 |

### D2 MFE/MAE from entry (medians, % of entry px)

| horizon min | MFE med | MFE p75 | MAE med | MAE p25 | %MFE>=0.5 | %MAE<=-0.7 |
|---|---|---|---|---|---|---|
| 15 | 0.155 | 0.58 | -0.462 | -0.703 | 27.9 | 25.5 |
| 30 | 0.276 | 0.816 | -0.57 | -0.923 | 37.2 | 37.8 |
| 60 | 0.475 | 1.185 | -0.699 | -1.161 | 48.3 | 49.7 |
| 120 | 0.648 | 1.48 | -0.874 | -1.461 | 55.4 | 58.2 |

### D3 retest/limit fill feasibility (pullback depth after signal, ATR units)

| window min | fill@0.3 ATR | fill@0.6 ATR | fill@1.0 ATR | median depth |
|---|---|---|---|---|
| 15 | 81.7% | 66.9% | 47.0% | 0.924 |
| 30 | 86.7% | 76.3% | 59.4% | 1.261 |

### D4 fade (side flipped, statutory @15bps)

| exit | TRAIN n/PF/net | TEST n/PF/net |
|---|---|---|
| 0.7/1.0 | 552/0.357/Rs-206,035 | 164/0.301/Rs-67,135 |
| 0.9/1.25 | 542/0.401/Rs-214,630 | 164/0.36/Rs-63,121 |
| 1.2/1.5 | 530/0.428/Rs-226,421 | 162/0.381/Rs-65,306 |
