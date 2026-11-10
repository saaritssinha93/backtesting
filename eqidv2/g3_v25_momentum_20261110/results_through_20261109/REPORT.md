# G3 V25: full-universe momentum entry research

68 completed available sessions, 2026-07-29 through 2026-11-09. New scanner arms create opportunities across the available historical stock universe; the published V24 baseline is retained for comparison.

**No automatic promotion.** The report describes historical research. Final-day mover rankings, extremes and first 3% crossing times are joined only after entry decisions and filled trades are fixed.

All reported net PnL includes Rs 250 modeled round-trip costs per fill on Rs 500,000 exposure, with unlimited concurrent capital. New arms use the stated fixed target and 1.50% initial stop; completed-close 80%-of-target breakeven protection and 15:15 square-off are retained. Matched-exit controls, when present, retain the original baseline entries with the corresponding fixed target.

## Frozen selection decision

Validation-chosen arm: **NONE**. Exploratory comparison: **leader_pullback__tp150**.

The catalogue displays **leader_pullback__tp150**. A displayed exploratory arm is not a recommended replacement. Selection details and acceptance criteria are recorded in config/summary; reporting does not select an arm.

Qualification is a quality/risk shortlist: sufficient fills and active dates, positive validation net PnL, mean trade profit and profit factor no worse than baseline, and no larger daily-close drawdown. It does not require total net profit to exceed baseline. A qualifying lower-trade arm can therefore still sacrifice total profit; the tables must be checked before treating it as an improvement.

## Full-history results

| Arm | Orders | Fills | Win rate | Net Rs | Losing PnL Rs | Mean/fill Rs | Median/fill Rs | Profit factor | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline_v24 | 930 | 824 | 65.66% | 1,221,079.86 | 864,226.26 | 1,481.89 | 2,549.56 | 2.41 | 31,338.39 |
| matched_exit_080 | 930 | 824 | 65.90% | 874,462.17 | 859,845.02 | 1,061.24 | 3,750.00 | 2.02 | 34,396.72 |
| matched_exit_150 | 930 | 824 | 60.32% | 1,100,512.43 | 1,047,398.58 | 1,335.57 | 1,182.15 | 2.05 | 49,833.86 |
| baseline_v24__stress | 930 | 824 | 62.62% | 924,480.86 | 946,673.63 | 1,121.94 | 1,941.77 | 1.98 | 36,043.92 |
| breakout_control__tp080 | 816 | 816 | 55.88% | -240,134.59 | 1,835,794.37 | -294.28 | 3,386.59 | 0.87 | 269,692.40 |
| breakout_control__tp080__stress | 816 | 816 | 55.15% | -368,974.33 | 1,903,244.35 | -452.17 | 2,246.61 | 0.81 | 388,015.74 |
| breakout_control__tp150 | 816 | 816 | 47.43% | -22,619.87 | 2,134,885.88 | -27.72 | 0.00 | 0.99 | 207,710.73 |
| breakout_control__tp150__stress | 816 | 816 | 46.69% | -151,725.61 | 2,211,429.67 | -185.94 | -192.75 | 0.93 | 243,230.04 |
| leader_breakout__tp080 | 814 | 812 | 53.08% | -155,786.42 | 1,544,722.28 | -191.86 | 647.60 | 0.90 | 200,264.57 |
| leader_breakout__tp080__stress | 814 | 812 | 51.85% | -314,972.85 | 1,636,083.25 | -387.90 | 380.29 | 0.81 | 336,728.68 |
| leader_breakout__tp150 | 814 | 812 | 47.78% | -97,059.42 | 1,735,337.98 | -119.53 | -36.04 | 0.94 | 161,359.45 |
| leader_breakout__tp150__stress | 814 | 812 | 46.55% | -247,632.30 | 1,826,216.25 | -304.97 | -256.05 | 0.86 | 286,654.88 |
| leader_pullback__tp080 | 815 | 814 | 52.33% | -149,952.45 | 1,563,690.61 | -184.22 | 452.11 | 0.90 | 247,104.16 |
| leader_pullback__tp080__stress | 815 | 814 | 51.60% | -321,096.02 | 1,674,119.24 | -394.47 | 251.87 | 0.81 | 399,562.58 |
| leader_pullback__tp150 | 815 | 814 | 47.67% | -92,644.36 | 1,834,055.62 | -113.81 | -30.81 | 0.95 | 228,858.20 |
| leader_pullback__tp150__stress | 815 | 814 | 45.95% | -256,702.98 | 1,916,195.17 | -315.36 | -230.85 | 0.87 | 350,662.43 |
| leader_union__tp080 | 816 | 815 | 52.64% | -209,474.19 | 1,656,901.02 | -257.02 | 692.87 | 0.87 | 236,899.74 |
| leader_union__tp080__stress | 816 | 815 | 51.78% | -370,499.00 | 1,758,664.68 | -454.60 | 486.74 | 0.79 | 375,075.77 |
| leader_union__tp150 | 816 | 815 | 46.50% | -174,039.18 | 1,948,040.15 | -213.55 | -187.49 | 0.91 | 197,394.28 |
| leader_union__tp150__stress | 816 | 815 | 44.91% | -327,718.60 | 2,030,593.87 | -402.11 | -387.48 | 0.84 | 333,497.98 |

## Chronological checks

The first 44 dates are the development slice, the following 15 are validation, and the final nine are the frozen later check. These histories have been examined previously, including in earlier G3 research; none is claimed to be untouched prospective evidence.

### fit_44

44 sessions: 2026-07-29 through 2026-10-05.

| Arm | Orders | Fills | Win rate | Net Rs | Losing PnL Rs | Mean/fill Rs | Median/fill Rs | Profit factor | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline_v24 | 569 | 496 | 69.56% | 889,147.16 | 426,032.89 | 1,792.64 | 2,550.00 | 3.09 | 15,638.86 |
| matched_exit_080 | 569 | 496 | 69.15% | 614,644.81 | 434,308.15 | 1,239.20 | 3,040.61 | 2.42 | 15,666.28 |
| matched_exit_150 | 569 | 496 | 64.31% | 816,055.40 | 524,817.55 | 1,645.27 | 1,398.44 | 2.55 | 17,071.93 |
| baseline_v24__stress | 569 | 496 | 65.32% | 652,008.35 | 477,739.67 | 1,314.53 | 1,700.27 | 2.36 | 22,464.48 |
| breakout_control__tp080 | 528 | 528 | 55.68% | -121,734.79 | 1,146,591.43 | -230.56 | 2,521.01 | 0.89 | 219,254.48 |
| breakout_control__tp080__stress | 528 | 528 | 55.30% | -196,317.55 | 1,184,509.43 | -371.81 | 2,001.02 | 0.83 | 252,721.18 |
| breakout_control__tp150 | 528 | 528 | 45.27% | -77,234.11 | 1,339,302.88 | -146.28 | -366.63 | 0.94 | 207,710.73 |
| breakout_control__tp150__stress | 528 | 528 | 44.70% | -158,991.82 | 1,387,838.55 | -301.12 | -566.64 | 0.89 | 243,230.04 |
| leader_breakout__tp080 | 527 | 525 | 52.19% | -74,199.60 | 923,551.63 | -141.33 | 352.22 | 0.92 | 162,931.08 |
| leader_breakout__tp080__stress | 527 | 525 | 50.86% | -178,606.44 | 981,763.99 | -340.20 | 133.87 | 0.82 | 238,653.75 |
| leader_breakout__tp150 | 527 | 525 | 48.00% | -82,417.08 | 1,039,607.54 | -156.98 | -33.26 | 0.92 | 161,359.45 |
| leader_breakout__tp150__stress | 527 | 525 | 46.86% | -186,090.32 | 1,102,110.33 | -354.46 | -235.80 | 0.83 | 236,136.00 |
| leader_pullback__tp080 | 527 | 527 | 48.20% | -202,574.21 | 1,026,555.78 | -384.39 | 0.00 | 0.80 | 226,340.37 |
| leader_pullback__tp080__stress | 527 | 527 | 47.25% | -317,098.04 | 1,100,774.14 | -601.70 | -99.95 | 0.71 | 329,730.70 |
| leader_pullback__tp150 | 527 | 527 | 44.97% | -215,153.27 | 1,197,027.49 | -408.26 | -369.96 | 0.82 | 228,858.20 |
| leader_pullback__tp150__stress | 527 | 527 | 43.07% | -325,639.94 | 1,254,527.13 | -617.91 | -570.05 | 0.74 | 325,639.94 |
| leader_union__tp080 | 528 | 528 | 51.89% | -95,168.22 | 998,611.80 | -180.24 | 592.16 | 0.90 | 142,733.14 |
| leader_union__tp080__stress | 528 | 528 | 51.33% | -196,573.60 | 1,064,057.84 | -372.30 | 472.00 | 0.82 | 220,070.32 |
| leader_union__tp150 | 528 | 528 | 47.54% | -106,986.50 | 1,176,065.07 | -202.63 | -67.63 | 0.91 | 189,583.66 |
| leader_union__tp150__stress | 528 | 528 | 45.64% | -212,915.12 | 1,233,123.89 | -403.25 | -313.62 | 0.83 | 250,462.18 |

### validation_15

15 sessions: 2026-10-06 through 2026-10-27.

| Arm | Orders | Fills | Win rate | Net Rs | Losing PnL Rs | Mean/fill Rs | Median/fill Rs | Profit factor | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline_v24 | 208 | 191 | 62.83% | 224,593.11 | 239,930.47 | 1,175.88 | 2,520.08 | 1.94 | 31,338.39 |
| matched_exit_080 | 208 | 191 | 63.87% | 187,463.02 | 228,060.49 | 981.48 | 3,750.00 | 1.82 | 30,138.57 |
| matched_exit_150 | 208 | 191 | 55.50% | 184,950.37 | 289,954.09 | 968.33 | 621.55 | 1.64 | 49,833.86 |
| baseline_v24__stress | 208 | 191 | 60.73% | 179,215.42 | 260,632.19 | 938.30 | 2,237.99 | 1.69 | 36,043.92 |
| breakout_control__tp080 | 180 | 180 | 52.78% | -121,957.61 | 461,331.83 | -677.54 | 2,334.29 | 0.74 | 170,361.66 |
| breakout_control__tp080__stress | 180 | 180 | 51.67% | -163,294.62 | 485,067.75 | -907.19 | 742.48 | 0.66 | 200,354.81 |
| breakout_control__tp150 | 180 | 180 | 48.89% | -5,923.11 | 509,735.11 | -32.91 | 0.00 | 0.99 | 93,691.15 |
| breakout_control__tp150__stress | 180 | 180 | 48.89% | -39,029.97 | 531,039.08 | -216.83 | -100.05 | 0.93 | 119,895.68 |
| leader_breakout__tp080 | 179 | 179 | 53.07% | -110,780.17 | 444,078.43 | -618.88 | 1,819.37 | 0.75 | 119,104.91 |
| leader_breakout__tp080__stress | 179 | 179 | 53.07% | -139,717.74 | 461,772.17 | -780.55 | 1,620.15 | 0.70 | 144,779.88 |
| leader_breakout__tp150 | 179 | 179 | 46.37% | -53,379.58 | 476,302.86 | -298.21 | -458.25 | 0.89 | 79,788.72 |
| leader_breakout__tp150__stress | 179 | 179 | 46.37% | -82,303.54 | 495,146.85 | -459.80 | -658.12 | 0.83 | 95,048.33 |
| leader_pullback__tp080 | 180 | 179 | 57.54% | -17,167.66 | 367,918.22 | -95.91 | 2,755.89 | 0.95 | 41,509.50 |
| leader_pullback__tp080__stress | 180 | 179 | 56.42% | -49,150.61 | 386,816.20 | -274.58 | 2,102.15 | 0.87 | 49,983.32 |
| leader_pullback__tp150 | 180 | 179 | 50.84% | 17,807.23 | 425,145.72 | 99.48 | 30.87 | 1.04 | 34,281.56 |
| leader_pullback__tp150__stress | 180 | 179 | 48.60% | -16,185.65 | 438,821.64 | -90.42 | -99.95 | 0.96 | 46,094.48 |
| leader_union__tp080 | 180 | 179 | 52.51% | -129,685.03 | 460,847.06 | -724.50 | 1,634.26 | 0.72 | 129,685.03 |
| leader_union__tp080__stress | 180 | 179 | 50.84% | -163,240.74 | 480,787.95 | -911.96 | 463.85 | 0.66 | 163,240.74 |
| leader_union__tp150 | 180 | 179 | 44.13% | -87,827.94 | 520,618.19 | -490.66 | -802.41 | 0.83 | 90,407.79 |
| leader_union__tp150__stress | 180 | 179 | 43.02% | -118,639.27 | 535,602.86 | -662.79 | -1,002.14 | 0.78 | 119,416.95 |

### later_9

9 sessions: 2026-10-28 through 2026-11-09.

| Arm | Orders | Fills | Win rate | Net Rs | Losing PnL Rs | Mean/fill Rs | Median/fill Rs | Profit factor | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline_v24 | 153 | 137 | 55.47% | 107,339.58 | 198,262.90 | 783.50 | 2,297.68 | 1.54 | 26,246.72 |
| matched_exit_080 | 153 | 137 | 56.93% | 72,354.34 | 197,476.38 | 528.13 | 3,532.27 | 1.37 | 34,396.72 |
| matched_exit_150 | 153 | 137 | 52.55% | 99,506.66 | 232,626.95 | 726.33 | 355.14 | 1.43 | 40,343.83 |
| baseline_v24__stress | 153 | 137 | 55.47% | 93,257.09 | 208,301.77 | 680.71 | 2,190.72 | 1.45 | 29,242.41 |
| breakout_control__tp080 | 108 | 108 | 62.04% | 3,557.81 | 227,871.12 | 32.94 | 3,750.00 | 1.02 | 22,177.11 |
| breakout_control__tp080__stress | 108 | 108 | 60.19% | -9,362.16 | 233,667.17 | -86.69 | 3,649.20 | 0.96 | 28,403.57 |
| breakout_control__tp150 | 108 | 108 | 55.56% | 60,537.36 | 285,847.89 | 560.53 | 1,432.98 | 1.21 | 20,155.67 |
| breakout_control__tp150__stress | 108 | 108 | 52.78% | 46,296.18 | 292,552.04 | 428.67 | 1,232.34 | 1.16 | 23,273.26 |
| leader_breakout__tp080 | 108 | 108 | 57.41% | 29,193.35 | 177,092.22 | 270.31 | 1,970.20 | 1.16 | 26,304.89 |
| leader_breakout__tp080__stress | 108 | 108 | 54.63% | 3,351.32 | 192,547.09 | 31.03 | 1,385.43 | 1.02 | 41,331.82 |
| leader_breakout__tp150 | 108 | 108 | 49.07% | 38,737.23 | 219,427.59 | 358.68 | 0.00 | 1.18 | 14,714.34 |
| leader_breakout__tp150__stress | 108 | 108 | 45.37% | 20,761.55 | 228,959.08 | 192.24 | -124.45 | 1.09 | 18,521.07 |
| leader_pullback__tp080 | 108 | 108 | 63.89% | 69,789.43 | 169,216.61 | 646.20 | 3,750.00 | 1.41 | 27,362.28 |
| leader_pullback__tp080__stress | 108 | 108 | 64.81% | 45,152.63 | 186,528.89 | 418.08 | 3,649.20 | 1.24 | 33,313.93 |
| leader_pullback__tp150 | 108 | 108 | 55.56% | 104,701.68 | 211,882.41 | 969.46 | 752.77 | 1.49 | 43,809.08 |
| leader_pullback__tp150__stress | 108 | 108 | 55.56% | 85,122.62 | 222,846.39 | 788.17 | 552.80 | 1.38 | 45,710.39 |
| leader_union__tp080 | 108 | 108 | 56.48% | 15,379.07 | 197,442.16 | 142.40 | 3,750.00 | 1.08 | 30,916.41 |
| leader_union__tp080__stress | 108 | 108 | 55.56% | -10,684.67 | 213,818.90 | -98.93 | 3,649.20 | 0.95 | 37,520.14 |
| leader_union__tp150 | 108 | 108 | 45.37% | 20,775.26 | 251,356.89 | 192.36 | -147.95 | 1.08 | 65,222.95 |
| leader_union__tp150__stress | 108 | 108 | 44.44% | 3,835.79 | 261,867.12 | 35.52 | -347.92 | 1.01 | 72,544.66 |

## Long and short results

### LONG

| Arm | Orders | Fills | Win rate | Net Rs | Losing PnL Rs | Mean/fill Rs | Median/fill Rs | Profit factor | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline_v24 | 408 | 373 | 65.95% | 628,891.75 | 378,118.69 | 1,686.04 | 3,081.33 | 2.66 | 20,311.71 |
| matched_exit_080 | 408 | 373 | 65.68% | 452,013.27 | 377,131.69 | 1,211.83 | 3,750.00 | 2.20 | 28,461.71 |
| matched_exit_150 | 408 | 373 | 60.05% | 592,275.37 | 473,916.30 | 1,587.87 | 1,529.16 | 2.25 | 37,908.82 |
| baseline_v24__stress | 408 | 373 | 63.54% | 488,705.77 | 415,349.34 | 1,310.20 | 2,457.70 | 2.18 | 22,706.04 |
| breakout_control__tp080 | 408 | 408 | 55.64% | -118,222.63 | 924,188.66 | -289.76 | 3,750.00 | 0.87 | 184,123.64 |
| breakout_control__tp080__stress | 408 | 408 | 55.15% | -172,619.67 | 951,858.10 | -423.09 | 3,649.20 | 0.82 | 223,554.44 |
| breakout_control__tp150 | 408 | 408 | 47.06% | 20,585.57 | 1,077,546.74 | 50.45 | 0.00 | 1.02 | 152,613.82 |
| breakout_control__tp150__stress | 408 | 408 | 46.32% | -37,708.86 | 1,116,102.54 | -92.42 | -230.02 | 0.97 | 200,397.49 |
| leader_breakout__tp080 | 407 | 406 | 48.77% | -202,490.87 | 848,365.01 | -498.75 | 0.00 | 0.76 | 268,426.26 |
| leader_breakout__tp080__stress | 407 | 406 | 47.04% | -292,476.67 | 900,173.76 | -720.39 | -100.05 | 0.68 | 350,701.01 |
| leader_breakout__tp150 | 407 | 406 | 41.87% | -223,921.91 | 957,381.98 | -551.53 | -701.84 | 0.77 | 314,860.63 |
| leader_breakout__tp150__stress | 407 | 406 | 40.64% | -301,153.72 | 1,005,123.66 | -741.76 | -906.95 | 0.70 | 381,471.33 |
| leader_pullback__tp080 | 408 | 407 | 49.88% | -142,151.91 | 819,098.81 | -349.27 | 0.00 | 0.83 | 230,011.30 |
| leader_pullback__tp080__stress | 408 | 407 | 49.63% | -228,820.99 | 877,633.47 | -562.21 | -83.02 | 0.74 | 308,649.36 |
| leader_pullback__tp150 | 408 | 407 | 44.23% | -118,424.53 | 957,643.56 | -290.97 | -644.02 | 0.88 | 254,811.48 |
| leader_pullback__tp150__stress | 408 | 407 | 42.75% | -203,095.22 | 999,035.65 | -499.01 | -843.82 | 0.80 | 329,449.48 |
| leader_union__tp080 | 408 | 407 | 52.09% | -113,874.92 | 836,811.11 | -279.79 | 687.07 | 0.86 | 187,107.95 |
| leader_union__tp080__stress | 408 | 407 | 51.35% | -210,680.33 | 901,253.80 | -517.64 | 476.48 | 0.77 | 275,189.33 |
| leader_union__tp150 | 408 | 407 | 42.75% | -158,685.48 | 1,002,420.89 | -389.89 | -742.23 | 0.84 | 263,389.95 |
| leader_union__tp150__stress | 408 | 407 | 41.28% | -240,714.50 | 1,044,102.22 | -591.44 | -941.99 | 0.77 | 334,599.99 |

### SHORT

| Arm | Orders | Fills | Win rate | Net Rs | Losing PnL Rs | Mean/fill Rs | Median/fill Rs | Profit factor | Daily-close DD Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| baseline_v24 | 522 | 451 | 65.41% | 592,188.10 | 486,107.57 | 1,313.06 | 1,814.76 | 2.22 | 43,336.40 |
| matched_exit_080 | 522 | 451 | 66.08% | 422,448.90 | 482,713.32 | 936.69 | 2,212.36 | 1.88 | 33,484.45 |
| matched_exit_150 | 522 | 451 | 60.53% | 508,237.07 | 573,482.29 | 1,126.91 | 952.73 | 1.89 | 79,986.82 |
| baseline_v24__stress | 522 | 451 | 61.86% | 435,775.09 | 531,324.29 | 966.24 | 1,292.41 | 1.82 | 56,203.88 |
| breakout_control__tp080 | 408 | 408 | 56.13% | -121,911.96 | 911,605.71 | -298.80 | 2,330.02 | 0.87 | 126,707.20 |
| breakout_control__tp080__stress | 408 | 408 | 55.15% | -196,354.66 | 951,386.25 | -481.26 | 1,822.46 | 0.79 | 196,946.83 |
| breakout_control__tp150 | 408 | 408 | 47.79% | -43,205.44 | 1,057,339.14 | -105.90 | 0.00 | 0.96 | 116,499.49 |
| breakout_control__tp150__stress | 408 | 408 | 47.06% | -114,016.76 | 1,095,327.13 | -279.45 | -179.92 | 0.90 | 121,216.12 |
| leader_breakout__tp080 | 407 | 406 | 57.39% | 46,704.45 | 696,357.27 | 115.04 | 1,350.36 | 1.07 | 72,526.75 |
| leader_breakout__tp080__stress | 407 | 406 | 56.65% | -22,496.18 | 735,909.49 | -55.41 | 1,150.96 | 0.97 | 100,830.84 |
| leader_breakout__tp150 | 407 | 406 | 53.69% | 126,862.49 | 777,956.00 | 312.47 | 519.77 | 1.16 | 60,130.37 |
| leader_breakout__tp150__stress | 407 | 406 | 52.46% | 53,521.42 | 821,092.59 | 131.83 | 334.04 | 1.07 | 82,759.11 |
| leader_pullback__tp080 | 407 | 407 | 54.79% | -7,800.54 | 744,591.79 | -19.17 | 1,242.45 | 0.99 | 127,115.53 |
| leader_pullback__tp080__stress | 407 | 407 | 53.56% | -92,275.03 | 796,485.76 | -226.72 | 1,036.53 | 0.88 | 176,931.85 |
| leader_pullback__tp150 | 407 | 407 | 51.11% | 25,780.18 | 876,412.06 | 63.34 | 46.16 | 1.03 | 92,164.68 |
| leader_pullback__tp150__stress | 407 | 407 | 49.14% | -53,607.75 | 917,159.51 | -131.71 | -99.95 | 0.94 | 136,054.25 |
| leader_union__tp080 | 408 | 408 | 53.19% | -95,599.28 | 820,089.91 | -234.31 | 780.99 | 0.88 | 114,677.69 |
| leader_union__tp080__stress | 408 | 408 | 52.21% | -159,818.67 | 857,410.88 | -391.71 | 581.36 | 0.81 | 159,818.67 |
| leader_union__tp150 | 408 | 408 | 50.25% | -15,353.70 | 945,619.27 | -37.63 | 10.57 | 0.98 | 98,814.60 |
| leader_union__tp150__stress | 408 | 408 | 48.53% | -87,004.09 | 986,491.65 | -213.25 | -99.95 | 0.91 | 136,566.47 |

## Did entries capture the daily movers?

Mover labels cover up to five stocks per direction/day with at least a 3% move from the first available session open to the observed daily high/low. They are hindsight evaluation labels, not a list available to a scanner in advance. Coverage is the available sealed G3 universe, not every NSE security.

**Useful capture** requires an actual entry strictly before the final extreme, at least 0.8% directional distance from entry to that extreme, and positive realized net PnL on that trade. A case counts once even if the baseline enters it repeatedly. Top-five precision divides unique top-five stock/day/direction cases traded by all unique stock/day/direction cases traded.

For fair timing comparisons, the table uses the same conservative entry-minute end label for both baseline and new arms, excluding all same-minute crossings/extremes. CSV columns containing `conservative` reproduce this common-clock comparison; other timing columns additionally use known next-open timing where available. Filled-case coverage and PnL do not depend on this distinction.

### full_history

| Arm | Top5 cases filled | Top5 precision | Before first 3% (common clock) | Useful top5 capture (common clock) | Top2 filled | Near-extreme entries (common clock) | Non-top5 net Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v24 | 124/652 | 16.62% | 41/652 | 94/652 | 66/271 | 195 | 686,034.94 |
| matched_exit_080 | 124/652 | 16.62% | 41/652 | 93/652 | 66/271 | 195 | 543,994.88 |
| matched_exit_150 | 124/652 | 16.62% | 41/652 | 83/652 | 66/271 | 195 | 593,751.28 |
| baseline_v24__stress | 124/652 | 16.62% | 41/652 | 88/652 | 66/271 | 204 | 506,020.40 |
| breakout_control__tp080 | 202/652 | 24.75% | 91/652 | 162/652 | 91/271 | 195 | -646,586.51 |
| breakout_control__tp080__stress | 202/652 | 24.75% | 91/652 | 161/652 | 91/271 | 200 | -750,498.46 |
| breakout_control__tp150 | 202/652 | 24.75% | 91/652 | 151/652 | 91/271 | 195 | -793,220.56 |
| breakout_control__tp150__stress | 202/652 | 24.75% | 91/652 | 151/652 | 91/271 | 200 | -897,761.46 |
| leader_breakout__tp080 | 78/652 | 9.61% | 21/652 | 53/652 | 31/271 | 300 | -296,104.62 |
| leader_breakout__tp080__stress | 78/652 | 9.61% | 21/652 | 52/652 | 31/271 | 309 | -431,012.07 |
| leader_breakout__tp150 | 78/652 | 9.61% | 21/652 | 49/652 | 31/271 | 300 | -332,393.04 |
| leader_breakout__tp150__stress | 78/652 | 9.61% | 21/652 | 48/652 | 31/271 | 309 | -468,376.37 |
| leader_pullback__tp080 | 89/652 | 10.93% | 39/652 | 67/652 | 39/271 | 240 | -333,239.05 |
| leader_pullback__tp080__stress | 89/652 | 10.93% | 39/652 | 68/652 | 39/271 | 248 | -496,427.74 |
| leader_pullback__tp150 | 89/652 | 10.93% | 39/652 | 61/652 | 39/271 | 240 | -355,214.00 |
| leader_pullback__tp150__stress | 89/652 | 10.93% | 39/652 | 61/652 | 39/271 | 248 | -508,383.67 |
| leader_union__tp080 | 82/652 | 10.06% | 36/652 | 62/652 | 32/271 | 247 | -377,913.95 |
| leader_union__tp080__stress | 82/652 | 10.06% | 36/652 | 62/652 | 32/271 | 257 | -527,592.89 |
| leader_union__tp150 | 82/652 | 10.06% | 36/652 | 56/652 | 32/271 | 247 | -432,750.02 |
| leader_union__tp150__stress | 82/652 | 10.06% | 36/652 | 55/652 | 32/271 | 257 | -579,419.01 |

### validation_15

| Arm | Top5 cases filled | Top5 precision | Before first 3% (common clock) | Useful top5 capture (common clock) | Top2 filled | Near-extreme entries (common clock) | Non-top5 net Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v24 | 22/143 | 12.50% | 6/143 | 14/143 | 12/59 | 41 | 177,961.63 |
| matched_exit_080 | 22/143 | 12.50% | 6/143 | 15/143 | 12/59 | 41 | 147,237.55 |
| matched_exit_150 | 22/143 | 12.50% | 6/143 | 13/143 | 12/59 | 41 | 144,689.54 |
| baseline_v24__stress | 22/143 | 12.50% | 6/143 | 14/143 | 12/59 | 42 | 147,030.17 |
| breakout_control__tp080 | 40/143 | 22.22% | 14/143 | 30/143 | 18/59 | 49 | -185,987.73 |
| breakout_control__tp080__stress | 40/143 | 22.22% | 14/143 | 30/143 | 18/59 | 50 | -223,147.41 |
| breakout_control__tp150 | 40/143 | 22.22% | 14/143 | 30/143 | 18/59 | 49 | -154,765.58 |
| breakout_control__tp150__stress | 40/143 | 22.22% | 14/143 | 30/143 | 18/59 | 50 | -183,293.68 |
| leader_breakout__tp080 | 22/143 | 12.29% | 5/143 | 14/143 | 8/59 | 65 | -156,540.78 |
| leader_breakout__tp080__stress | 22/143 | 12.29% | 5/143 | 13/143 | 8/59 | 66 | -179,089.69 |
| leader_breakout__tp150 | 22/143 | 12.29% | 5/143 | 13/143 | 8/59 | 65 | -121,694.91 |
| leader_breakout__tp150__stress | 22/143 | 12.29% | 5/143 | 12/143 | 8/59 | 66 | -150,306.76 |
| leader_pullback__tp080 | 19/143 | 10.61% | 8/143 | 14/143 | 9/59 | 55 | -61,667.66 |
| leader_pullback__tp080__stress | 19/143 | 10.61% | 8/143 | 14/143 | 9/59 | 57 | -91,750.76 |
| leader_pullback__tp150 | 19/143 | 10.61% | 8/143 | 15/143 | 9/59 | 55 | -66,497.56 |
| leader_pullback__tp150__stress | 19/143 | 10.61% | 8/143 | 15/143 | 9/59 | 57 | -97,991.80 |
| leader_union__tp080 | 19/143 | 10.61% | 7/143 | 12/143 | 9/59 | 64 | -156,144.75 |
| leader_union__tp080__stress | 19/143 | 10.61% | 7/143 | 11/143 | 9/59 | 65 | -183,814.06 |
| leader_union__tp150 | 19/143 | 10.61% | 7/143 | 12/143 | 9/59 | 64 | -132,984.69 |
| leader_union__tp150__stress | 19/143 | 10.61% | 7/143 | 11/143 | 9/59 | 65 | -164,092.32 |

### later_9

| Arm | Top5 cases filled | Top5 precision | Before first 3% (common clock) | Useful top5 capture (common clock) | Top2 filled | Near-extreme entries (common clock) | Non-top5 net Rs |
|:--|--:|--:|--:|--:|--:|--:|--:|
| baseline_v24 | 23/89 | 18.40% | 4/89 | 15/89 | 10/36 | 39 | 52,813.50 |
| matched_exit_080 | 23/89 | 18.40% | 4/89 | 14/89 | 10/36 | 39 | 31,715.18 |
| matched_exit_150 | 23/89 | 18.40% | 4/89 | 11/89 | 10/36 | 39 | 38,419.30 |
| baseline_v24__stress | 23/89 | 18.40% | 4/89 | 15/89 | 10/36 | 39 | 42,132.41 |
| breakout_control__tp080 | 33/89 | 30.56% | 11/89 | 28/89 | 11/36 | 26 | -81,942.19 |
| breakout_control__tp080__stress | 33/89 | 30.56% | 11/89 | 27/89 | 11/36 | 26 | -87,804.26 |
| breakout_control__tp150 | 33/89 | 30.56% | 11/89 | 28/89 | 11/36 | 26 | -96,192.01 |
| breakout_control__tp150__stress | 33/89 | 30.56% | 11/89 | 28/89 | 11/36 | 26 | -106,919.60 |
| leader_breakout__tp080 | 10/89 | 9.26% | 2/89 | 7/89 | 3/36 | 43 | -1,427.73 |
| leader_breakout__tp080__stress | 10/89 | 9.26% | 2/89 | 7/89 | 3/36 | 44 | -26,069.28 |
| leader_breakout__tp150 | 10/89 | 9.26% | 2/89 | 7/89 | 3/36 | 43 | -4,883.85 |
| leader_breakout__tp150__stress | 10/89 | 9.26% | 2/89 | 7/89 | 3/36 | 44 | -21,660.65 |
| leader_pullback__tp080 | 7/89 | 6.48% | 3/89 | 5/89 | 3/36 | 22 | 58,789.43 |
| leader_pullback__tp080__stress | 7/89 | 6.48% | 3/89 | 5/89 | 3/36 | 23 | 34,847.08 |
| leader_pullback__tp150 | 7/89 | 6.48% | 3/89 | 5/89 | 3/36 | 22 | 83,951.68 |
| leader_pullback__tp150__stress | 7/89 | 6.48% | 3/89 | 5/89 | 3/36 | 23 | 65,065.12 |
| leader_union__tp080 | 7/89 | 6.48% | 2/89 | 7/89 | 3/36 | 31 | -10,870.93 |
| leader_union__tp080__stress | 7/89 | 6.48% | 2/89 | 7/89 | 3/36 | 32 | -36,237.07 |
| leader_union__tp150 | 7/89 | 6.48% | 2/89 | 6/89 | 3/36 | 31 | -14,974.74 |
| leader_union__tp150__stress | 7/89 | 6.48% | 2/89 | 6/89 | 3/36 | 32 | -31,215.71 |

The near-extreme label is retrospective: entry at/after the final extreme, or less than 0.3% directional distance to it. It measures historical timing and must never become an entry-time input. Baseline intrabar fills have only an entry-minute end label, so equality with a crossing/extreme minute is conservatively excluded. New next-open entries have known opening times: a crossing or extreme in that entry minute counts as later only when the raw opening price was strictly short of that level. An opening gap already at/beyond 3% never counts as entry before the 3% crossing. OHLC data does not otherwise resolve within-minute event ordering.

Stocks outside the top-five list can still produce profitable trades. The non-top-five PnL column and trade-context tables quantify the profit sacrificed by an oracle-like desire to avoid all other stocks. A higher capture percentage alone does not establish a better strategy.

## Verification and limits

Every filled trade belongs to an admitted order with matching identity. Daily counts, wins/losses, costs and net PnL reconcile to overall totals. Top-five/non-top-five and near-extreme/other partitions also reconcile. Parent manifests and code/source provenance are verified by the runner.

Inherited source coverage, excluded sessions, missing opening minutes and archived universe limitations remain listed in config and source provenance. Smaller drawdown in a low-trade arm is not by itself an improvement. The modeled costs are not a guarantee of live fills or market impact. Existing versions and live/paper settings remain unchanged.

[Day-wise results](DAY_WISE_RESULTS.md) | [Every daily top-five mover](FULL_HISTORY_DAILY_TOP5_MOVERS.md) | [Mover coverage](mover_summary.csv) | [Direction coverage](mover_by_side.csv) | [Trade context contributions](trade_contexts.csv) | [Missed-case reasons](missed_cases.csv)
