# V10 FNO Backtesting — full historical study report

Generated: 2026-08-30T17:05:53.348956+05:30
Verified source run: `C:\TradingData\eqidv2\fno_oi\strategy_research\v10_max050_gap2_full_history_v1\run_20260830T163837220506+0530`
Profile: `V10 Stage 7 + 09:35 LONG <= 0.50% + Gap2`, current mixed per-setup limits.

> This is an auditable research report. It does not convert a diagnostic backtest into live-proof. The exact run marks itself `research_only=true`, `headline_valid=false`, and `promotion_eligible=false`.

## Executive conclusion

Across 65 usable sessions, the model produced **232 fills, 116-116, WR 50.00%, PF 1.8327, +73.0544 summed trade-return points and Rs +36,312.05 modeled net P&L**.

The result is profitable under all three modeled cost cases, but stability weakened in the six-session forward extension (PF 1.3172) and the strategy was developed through repeated testing on much of the same history. The correct next step is frozen forward validation and execution-data repair—not immediate parameter optimization on these 65 sessions.

## 1. Data contract, scope and honesty checks

- Usable sessions: **65**, 2026-05-27 through 2026-08-28.
- Expected regular sessions in the span: **66**.
- Missing validated sessions: **2026-08-26**.
- Base-qualified 5-minute candidates before overlays: **1,241**.
- Candidates after Stage 7 and `.50` overlays: **1,134**.
- Completed fills with finite economics: **232**.
- Candidate/path input files, output artifacts and the pinned benchmark are SHA-256 bound.
- The candidate cache retains candidates that passed each base 5-minute setup; it does not retain one row for every universe symbol that failed base eligibility.
- Strict upstream completeness failed for many symbol-sessions; the run uses `LAST_REAL_BAR_SENSITIVITY`.
  Under the engine's strict full-session definition, 7,172 of 13,522 symbol-sessions (53.04%) were incomplete. This does not mean every bar was absent; it means the symbol-session failed the complete 09:16–15:30 cash path plus required futures-OI checks.
- Futures OI drives selection, while entry/exit prices are underlying NSE cash 1-minute bars with `lot_size=1`. Therefore P&L is Rs 50,000 cash-equivalent notional per fill, not actual futures-lot P&L.

### Source segments

| Period | Sessions | Contract | Raw Base 5M Candidates | Source Incomplete Symbol Sessions | Expected Symbol Sessions |
|---|---|---|---|---|---|
| AUG_CORE_59 | 59 | 26AUG | 1,126 | 5,922 | 12,272 |
| AUG_EXTENSION_20_21 | 2 | 26AUG | 27 | 416 | 416 |
| SEP_ROLLOVER_24_25 | 2 | 26SEP | 48 | 414 | 414 |
| SEP_DIAGNOSTIC_27 | 1 | 26SEP | 22 | 210 | 210 |
| SEP_DIAGNOSTIC_28 | 1 | 26SEP | 18 | 210 | 210 |

## 2. Exact strategy parameters

### 5-minute selection and 1-minute entry book

| Setup Id | Max Entries | Picker | Base Move Rule | Effective Move Rule | Oi Change Pct | Volume Ratio | Min Traded Value Cr | Body Ratio | Max Wick Ratio | Effective Max Confirmation Minute | Effective Buffer Bps | Stop Pct | Target Pct |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 09:25_LONG | 4 | max_move | >= +0.3% | >= +0.3% | 0.10% | 3.000 | 0.000 | 0.000 | 0.500 | 3 | 0.000 | 0.40% | 1.00% |
| 09:25_SHORT | 4 | max_move | <= -0.2% | <= -0.2% | 0.10% | 1.500 | 2.500 | 0.600 | 0.600 | 3 | 2.000 | 0.50% | 3.00% |
| 09:30_LONG | 1 | max_move | >= +0.65% | >= +0.65% | 0.10% | 1.000 | 0.000 | 0.500 | 0.500 | 1 | 0.000 | 1.00% | 2.50% |
| 09:30_SHORT | 4 | max_volume | <= -0.2% | <= -0.2% | 1.00% | 1.000 | 2.500 | 0.450 | 0.300 | 3 | 0.000 | 1.00% | 4.00% |
| 09:35_LONG | 1 | max_liquidity | >= +0.2% | +0.20% to +0.50% | 0.10% | 1.000 | 0.000 | 0.600 | 0.500 | 1 | 0.000 | 1.00% | 2.50% |
| 09:35_SHORT | 2 | max_liquidity | <= -0.5% | <= -0.5% | 1.00% | 1.000 | 0.000 | 0.400 | 0.500 | 1 | 0.000 | 1.00% | 3.00% |
| 09:40_LONG | 1 | max_liquidity | >= +0.2% | >= +0.40% | 0.10% | 2.000 | 0.000 | 0.500 | 0.500 | 1 | 0.000 | 0.50% | 2.50% |
| 09:40_SHORT | 1 | max_move | <= -0.2% | <= -0.2% | 0.10% | 1.000 | 0.000 | 0.400 | 0.500 | 1 | 0.000 | 1.00% | 3.00% |
| 09:45_LONG | 1 | max_move | >= +0.65% | >= +0.65% | 0.10% | 1.000 | 0.000 | 0.400 | 0.500 | 1 | 0.000 | 1.00% | 3.00% |
| 09:45_SHORT | 1 | max_volume | <= -0.2% | <= -0.2% | 0.75% | 1.000 | 0.000 | 0.400 | 0.300 | 1 | 0.000 | 1.00% | 2.00% |

Parameter interpretation:

- Every 5-minute LONG requires `EMA9 > EMA20 > EMA50`; every SHORT requires the reverse.
- `body_ratio` and `max_wick_ratio` are 1-minute confirmation gates, not 5-minute filters.
- Stage 7 changes 09:40 LONG to a minimum +0.40% move.
- `.50` imposes a maximum +0.50% move on 09:35 LONG, after which candidates are reranked.
- 09:30 SHORT additionally uses midpoint invalidation and close-location >= 0.50.
- Confirmed stop orders can fill only on a later bar through S+5. Gap2 rejects an adverse gap through the trigger greater than 2 bps.
- Portfolio: Rs 120,000 modeled capital, Rs 10,000 reserved margin per pending/open entry, maximum 12 concurrent positions, pending orders reserve margin, one concurrent position per symbol.
- Economics: Rs 50,000 modeled exposure per fill; 15 bps total cost in reference; zero slippage; stop-first when both stop and target are touched in one 1-minute candle.

## 3. Selection and entry funnel

![Selection funnel](report_v10_assets/selection_funnel.png)

| Stage | Count |
|---|---|
| Base 5m | 1,241 |
| Post-overlay | 1,134 |
| 1m confirmed | 409 |
| Filled | 232 |
| Winners | 116 |

### Post-selection overlay rejections

| Selection Reason | Rejections | Affected Sessions | Median Price Change Pct |
|---|---|---|---|
| 0935_LONG_MOVE_ABOVE_CHALLENGER_MAX | 77 | 40 | 0.67% |
| STAGE7_0940_LONG_MOVE_BELOW_040 | 30 | 22 | 0.28% |

### Final candidate states

| Status | Count |
|---|---|
| NO_CONFIRMATION | 693 |
| STOPPED | 103 |
| POSTCONF_CANCELLED | 86 |
| SQUARE_OFF | 69 |
| WINDOW_EXPIRED | 63 |
| TARGETED | 60 |
| PRECONF_INVALIDATED | 32 |
| DUPLICATE_REJECTED | 28 |

### 1-minute rejection-code occurrences

| Reason | Count |
|---|---|
| CLOSE_NOT_BEYOND_FIVE_MINUTE_CLOSE | 926 |
| WRONG_CANDLE_DIRECTION | 843 |
| BODY_RATIO_BELOW_MINIMUM | 740 |
| ADVERSE_WICK_RATIO_ABOVE_MAXIMUM | 305 |
| CLOSE_LOCATION_BELOW_MINIMUM | 87 |
| PRECONF_MIDPOINT_INVALIDATED | 32 |
| NONPOSITIVE_RANGE | 1 |

A confirmation candle can contain multiple rejection codes, so rejection-code counts do not sum to candidates.

## 4. Headline performance and risk

| Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Average Return Points | Average Pnl Rs | Payoff Ratio | Max Daily Drawdown Points | Max Daily Drawdown Rs | Recovery Factor Pnl |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 232 | 116 | 116 | 50.00% | 1.8327 | +73.0544 | +36,312.05 | +0.3149 | +156.52 | 1.8327 | +9.3513 | +4,416.14 | 8.223 |

- Gross winning points: **160.7831**; gross losing points: **87.7287**.
- Average win/loss: **+1.3861 / -0.7563 points**; payoff ratio **1.8327**.
- Best day: **2026-07-07 Rs +4,714.73**; worst day: **2026-08-12 Rs -1,332.44**.
- Longest winning/losing trade streaks: **6 / 6**.
- Longest positive/negative day streaks: **5 / 4**.
- `net_return_points` sums trade percentage returns; it is not portfolio percentage return because positions can overlap and each fill receives its own Rs 50,000 modeled notional.

![Equity and drawdown](report_v10_assets/equity_and_drawdown.png)

## 5. Stability through time

### Core, forward and half-sample comparison

| Period | Sessions | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| FULL_65 | 65 | 1,241 | 1,134 | 409 | 232 | 116 | 116 | 50.00% | 1.8327 | +73.0544 | +36,312.05 |
| CORE_59 | 59 | 1,126 | 1,035 | 371 | 211 | 108 | 103 | 51.18% | 1.8862 | +70.4389 | +35,007.42 |
| FORWARD_6 | 6 | 115 | 99 | 38 | 21 | 8 | 13 | 38.10% | 1.3172 | +2.6155 | +1,304.63 |
| FIRST_HALF_32 | 32 | 497 | 466 | 154 | 96 | 51 | 45 | 53.12% | 2.0529 | +38.6499 | +19,138.07 |
| SECOND_HALF_33 | 33 | 744 | 668 | 255 | 136 | 65 | 71 | 47.79% | 1.6743 | +34.4045 | +17,173.98 |
| LAST_14_USABLE | 14 | 196 | 173 | 64 | 41 | 17 | 24 | 41.46% | 1.4799 | +7.9691 | +4,132.24 |

The six-session extension remained profitable but had lower WR/PF. It is too small to establish forward robustness.

### Monthly

| Period | Sessions | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| 2026-05 | 2 | 7 | 5 | 2 | 71.43% | 8.2258 | +8.0446 | +3,977.37 |
| 2026-06 | 21 | 48 | 21 | 27 | 43.75% | 1.1539 | +3.7057 | +1,878.73 |
| 2026-07 | 23 | 119 | 66 | 53 | 55.46% | 2.2980 | +51.3701 | +25,234.87 |
| 2026-08 | 19 | 58 | 24 | 34 | 41.38% | 1.4327 | +9.9340 | +5,221.09 |

![Monthly P&L](report_v10_assets/monthly_net_pnl.png)

### Weekly

| Period | Sessions | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| 2026-W22 | 2 | 7 | 5 | 2 | 71.43% | 8.2258 | +8.0446 | +3,977.37 |
| 2026-W23 | 5 | 15 | 6 | 9 | 40.00% | 1.1318 | +1.2250 | +505.87 |
| 2026-W24 | 5 | 8 | 2 | 6 | 25.00% | 0.1643 | -4.8112 | -2,191.67 |
| 2026-W25 | 5 | 8 | 3 | 5 | 37.50% | 1.1329 | +0.4737 | +257.08 |
| 2026-W26 | 4 | 9 | 5 | 4 | 55.56% | 2.1020 | +3.3127 | +1,610.63 |
| 2026-W27 | 5 | 18 | 11 | 7 | 61.11% | 2.0539 | +6.9299 | +3,549.09 |
| 2026-W28 | 5 | 27 | 16 | 11 | 59.26% | 3.4578 | +18.0728 | +8,766.00 |
| 2026-W29 | 5 | 20 | 9 | 11 | 45.00% | 1.4533 | +3.7606 | +1,851.48 |
| 2026-W30 | 5 | 39 | 20 | 19 | 51.28% | 1.9891 | +14.6786 | +7,222.36 |
| 2026-W31 | 5 | 23 | 15 | 8 | 65.22% | 3.2984 | +11.4337 | +5,542.76 |
| 2026-W32 | 5 | 17 | 7 | 10 | 41.18% | 1.3093 | +1.9649 | +1,088.84 |
| 2026-W33 | 5 | 15 | 6 | 9 | 40.00% | 1.0028 | +0.0215 | +192.92 |
| 2026-W34 | 5 | 8 | 3 | 5 | 37.50% | 1.9806 | +2.9689 | +1,462.25 |
| 2026-W35 | 4 | 18 | 8 | 10 | 44.44% | 1.8463 | +4.9788 | +2,477.08 |

### Full 65-session day-wise audit

| Session Date | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Cumulative Net Pnl Rs | Drawdown Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-05-27 | 9 | 8 | 5 | 4 | 3 | 1 | 75.00% | 5.9875 | +2.7824 | +1,386.50 | +1,386.50 | +0.00 |
| 2026-05-29 | 24 | 20 | 4 | 3 | 2 | 1 | 66.67% | 10.4738 | +5.2622 | +2,590.87 | +3,977.37 | +0.00 |
| 2026-06-01 | 9 | 9 | 1 | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1520 | -565.65 | +3,411.71 | -565.65 |
| 2026-06-02 | 13 | 13 | 6 | 4 | 1 | 3 | 25.00% | 0.8223 | -0.6157 | -360.87 | +3,050.84 | -926.52 |
| 2026-06-03 | 18 | 17 | 11 | 5 | 4 | 1 | 80.00% | 5.9012 | +5.6647 | +2,752.90 | +5,803.75 | +0.00 |
| 2026-06-04 | 12 | 12 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.5567 | -277.84 | +5,525.91 | -277.84 |
| 2026-06-05 | 12 | 12 | 4 | 4 | 1 | 3 | 25.00% | 0.2860 | -2.1153 | -1,042.68 | +4,483.23 | -1,320.52 |
| 2026-06-08 | 27 | 27 | 3 | 0 | 0 | 0 | — | — | +0.0000 | +0.00 | +4,483.23 | -1,320.52 |
| 2026-06-09 | 10 | 9 | 4 | 4 | 1 | 3 | 25.00% | 0.2158 | -2.3166 | -973.67 | +3,509.56 | -2,294.19 |
| 2026-06-10 | 8 | 6 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.9956 | -488.38 | +3,021.18 | -2,782.57 |
| 2026-06-11 | 6 | 6 | 2 | 2 | 1 | 1 | 50.00% | 0.2676 | -0.8440 | -402.17 | +2,619.00 | -3,184.74 |
| 2026-06-12 | 10 | 9 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.6549 | -327.44 | +2,291.56 | -3,512.19 |
| 2026-06-15 | 15 | 14 | 6 | 4 | 2 | 2 | 50.00% | 1.7060 | +0.7022 | +349.48 | +2,641.05 | -3,162.70 |
| 2026-06-16 | 20 | 18 | 6 | 2 | 0 | 2 | 0.00% | 0.0000 | -1.4119 | -682.73 | +1,958.31 | -3,845.43 |
| 2026-06-17 | 10 | 9 | 1 | 0 | 0 | 0 | — | — | +0.0000 | +0.00 | +1,958.31 | -3,845.43 |
| 2026-06-18 | 3 | 3 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1583 | -570.71 | +1,387.61 | -4,416.14 |
| 2026-06-19 | 11 | 11 | 1 | 1 | 1 | 0 | 100.00% | ∞ | +2.3418 | +1,161.03 | +2,548.64 | -3,255.11 |
| 2026-06-22 | 3 | 2 | 0 | 0 | 0 | 0 | — | — | +0.0000 | +0.00 | +2,548.64 | -3,255.11 |
| 2026-06-23 | 15 | 12 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.5525 | -274.27 | +2,274.37 | -3,529.37 |
| 2026-06-24 | 14 | 14 | 6 | 3 | 1 | 2 | 33.33% | 1.2984 | +0.5380 | +244.19 | +2,518.56 | -3,285.19 |
| 2026-06-25 | 19 | 19 | 7 | 5 | 4 | 1 | 80.00% | 6.1137 | +3.3271 | +1,640.71 | +4,159.27 | -1,644.48 |
| 2026-06-29 | 23 | 22 | 11 | 7 | 5 | 2 | 71.43% | 4.5566 | +4.6595 | +2,271.58 | +6,430.86 | +0.00 |
| 2026-06-30 | 13 | 13 | 3 | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1540 | -574.76 | +5,856.10 | -574.76 |
| 2026-07-01 | 13 | 12 | 4 | 3 | 2 | 1 | 66.67% | 3.2076 | +2.5413 | +1,221.89 | +7,077.99 | +0.00 |
| 2026-07-02 | 17 | 17 | 5 | 2 | 1 | 1 | 50.00% | 1.8692 | +1.0045 | +675.41 | +7,753.40 | +0.00 |
| 2026-07-03 | 29 | 28 | 6 | 5 | 3 | 2 | 60.00% | 0.9327 | -0.1215 | -45.04 | +7,708.36 | -45.04 |
| 2026-07-06 | 19 | 17 | 3 | 3 | 2 | 1 | 66.67% | 4.8335 | +2.1324 | +1,065.76 | +8,774.12 | +0.00 |
| 2026-07-07 | 39 | 38 | 15 | 7 | 6 | 1 | 85.71% | 15.7816 | +9.6513 | +4,714.73 | +13,488.85 | +0.00 |
| 2026-07-08 | 24 | 24 | 13 | 7 | 5 | 2 | 71.43% | 6.9127 | +7.7076 | +3,627.04 | +17,115.89 | +0.00 |
| 2026-07-09 | 10 | 9 | 4 | 4 | 0 | 4 | 0.00% | 0.0000 | -2.5823 | -1,250.56 | +15,865.33 | -1,250.56 |
| 2026-07-10 | 19 | 17 | 7 | 6 | 3 | 3 | 50.00% | 1.5154 | +1.1639 | +609.03 | +16,474.36 | -641.53 |
| 2026-07-13 | 23 | 19 | 6 | 4 | 3 | 1 | 75.00% | 115.1783 | +5.4024 | +2,663.71 | +19,138.07 | +0.00 |
| 2026-07-14 | 33 | 29 | 16 | 4 | 2 | 2 | 50.00% | 0.4795 | -0.8854 | -443.77 | +18,694.30 | -443.77 |
| 2026-07-15 | 26 | 23 | 9 | 6 | 2 | 4 | 33.33% | 1.0467 | +0.1644 | +95.39 | +18,789.68 | -348.38 |
| 2026-07-16 | 27 | 27 | 7 | 4 | 1 | 3 | 25.00% | 0.4228 | -1.7224 | -861.24 | +17,928.44 | -1,209.62 |
| 2026-07-17 | 21 | 18 | 5 | 2 | 1 | 1 | 50.00% | 19.7384 | +0.8016 | +397.40 | +18,325.84 | -812.23 |
| 2026-07-20 | 30 | 28 | 11 | 6 | 2 | 4 | 33.33% | 0.4212 | -1.6316 | -814.48 | +17,511.36 | -1,626.71 |
| 2026-07-21 | 15 | 14 | 5 | 4 | 2 | 2 | 50.00% | 1.1115 | +0.1472 | +133.74 | +17,645.10 | -1,492.97 |
| 2026-07-22 | 41 | 41 | 17 | 10 | 6 | 4 | 60.00% | 2.6906 | +5.9358 | +2,979.54 | +20,624.64 | +0.00 |
| 2026-07-23 | 65 | 60 | 19 | 10 | 6 | 4 | 60.00% | 2.8034 | +6.1584 | +2,897.10 | +23,521.74 | +0.00 |
| 2026-07-24 | 58 | 56 | 20 | 9 | 4 | 5 | 44.44% | 2.0781 | +4.0688 | +2,026.46 | +25,548.20 | +0.00 |
| 2026-07-27 | 34 | 29 | 14 | 7 | 2 | 5 | 28.57% | 0.4912 | -1.7545 | -919.61 | +24,628.59 | -919.61 |
| 2026-07-28 | 53 | 46 | 26 | 9 | 7 | 2 | 77.78% | 9.9664 | +8.7274 | +4,296.52 | +28,925.11 | +0.00 |
| 2026-07-29 | 17 | 13 | 3 | 1 | 1 | 0 | 100.00% | ∞ | +0.8497 | +413.42 | +29,338.53 | +0.00 |
| 2026-07-30 | 25 | 22 | 6 | 2 | 2 | 0 | 100.00% | ∞ | +1.2706 | +627.44 | +29,965.97 | +0.00 |
| 2026-07-31 | 18 | 16 | 4 | 4 | 3 | 1 | 75.00% | 5.2329 | +2.3406 | +1,125.00 | +31,090.97 | +0.00 |
| 2026-08-03 | 23 | 19 | 8 | 4 | 2 | 2 | 50.00% | 2.6235 | +1.9752 | +1,036.24 | +32,127.20 | +0.00 |
| 2026-08-04 | 11 | 10 | 5 | 2 | 0 | 2 | 0.00% | 0.0000 | -1.3049 | -567.42 | +31,559.78 | -567.42 |
| 2026-08-05 | 15 | 11 | 3 | 2 | 0 | 2 | 0.00% | 0.0000 | -1.4217 | -695.06 | +30,864.72 | -1,262.49 |
| 2026-08-06 | 21 | 19 | 12 | 8 | 4 | 4 | 50.00% | 1.7751 | +1.8673 | +922.47 | +31,787.18 | -340.02 |
| 2026-08-07 | 15 | 14 | 1 | 1 | 1 | 0 | 100.00% | ∞ | +0.8489 | +392.62 | +32,179.81 | +0.00 |
| 2026-08-10 | 9 | 9 | 3 | 3 | 1 | 2 | 33.33% | 2.5545 | +1.7315 | +864.22 | +33,044.03 | +0.00 |
| 2026-08-11 | 12 | 10 | 4 | 3 | 1 | 2 | 33.33% | 0.4655 | -0.9674 | -334.32 | +32,709.71 | -334.32 |
| 2026-08-12 | 17 | 17 | 7 | 5 | 1 | 4 | 20.00% | 0.2344 | -2.7677 | -1,332.44 | +31,377.27 | -1,666.76 |
| 2026-08-13 | 12 | 11 | 3 | 3 | 2 | 1 | 66.67% | 1.4691 | +0.5414 | +259.84 | +31,637.11 | -1,406.92 |
| 2026-08-14 | 7 | 5 | 2 | 1 | 1 | 0 | 100.00% | ∞ | +1.4836 | +735.62 | +32,372.73 | -671.30 |
| 2026-08-17 | 10 | 9 | 2 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.6556 | -326.24 | +32,046.49 | -997.54 |
| 2026-08-18 | 8 | 8 | 3 | 2 | 2 | 0 | 100.00% | ∞ | +4.3505 | +2,149.48 | +34,195.97 | +0.00 |
| 2026-08-19 | 6 | 5 | 2 | 2 | 1 | 1 | 50.00% | 187.3564 | +1.6372 | +811.45 | +35,007.42 | +0.00 |
| 2026-08-20 | 14 | 11 | 3 | 2 | 0 | 2 | 0.00% | 0.0000 | -1.7125 | -847.14 | +34,160.27 | -847.14 |
| 2026-08-21 | 13 | 12 | 4 | 1 | 0 | 1 | 0.00% | 0.0000 | -0.6508 | -325.30 | +33,834.97 | -1,172.44 |
| 2026-08-24 | 16 | 14 | 4 | 3 | 1 | 2 | 33.33% | 2.7917 | +1.1506 | +577.16 | +34,412.14 | -595.28 |
| 2026-08-25 | 32 | 28 | 16 | 6 | 3 | 3 | 50.00% | 1.7615 | +1.7384 | +871.28 | +35,283.42 | +0.00 |
| 2026-08-27 | 22 | 19 | 8 | 6 | 2 | 4 | 33.33% | 1.3921 | +0.9435 | +477.23 | +35,760.65 | +0.00 |
| 2026-08-28 | 18 | 15 | 3 | 3 | 2 | 1 | 66.67% | 3.0764 | +1.1462 | +551.40 | +36,312.05 | +0.00 |

### Sequential 10-session blocks

| Period | Sessions | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| B1: 2026-05-27..2026-06-10 | 10 | 27 | 12 | 15 | 44.44% | 1.4150 | +5.9574 | +3,021.18 |
| B2: 2026-06-11..2026-06-24 | 10 | 15 | 5 | 10 | 33.33% | 0.8655 | -1.0397 | -502.62 |
| B3: 2026-06-25..2026-07-09 | 10 | 44 | 28 | 16 | 63.64% | 3.2048 | +27.1659 | +13,346.77 |
| B4: 2026-07-10..2026-07-23 | 10 | 56 | 28 | 28 | 50.00% | 1.7185 | +15.5343 | +7,656.42 |
| B5: 2026-07-24..2026-08-06 | 10 | 48 | 25 | 23 | 52.08% | 2.1005 | +16.6185 | +8,265.44 |
| B6: 2026-08-07..2026-08-20 | 10 | 23 | 10 | 13 | 43.48% | 1.4459 | +4.4901 | +2,373.09 |
| B7: 2026-08-21..2026-08-28 | 5 | 19 | 8 | 11 | 42.11% | 1.6624 | +4.3280 | +2,151.78 |

### Day of week

| Weekday | Sessions | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| Monday | 13 | 43 | 20 | 23 | 46.51% | 1.9000 | +12.5601 | +6,202.17 |
| Tuesday | 13 | 49 | 25 | 24 | 51.02% | 1.7867 | +15.4063 | +7,953.94 |
| Wednesday | 13 | 49 | 26 | 23 | 53.06% | 2.1886 | +22.6362 | +11,016.42 |
| Thursday | 13 | 50 | 23 | 27 | 46.00% | 1.3064 | +6.5366 | +3,290.53 |
| Friday | 13 | 41 | 22 | 19 | 53.66% | 2.1526 | +15.9152 | +7,848.98 |

Rolling 10-session best: **2026-06-24..2026-07-08**, Rs +14,841.51, PF 3.6240.
Rolling 10-session worst: **2026-06-05..2026-06-18**, Rs -4,138.30, PF 0.2841.

## 6. Setup, side, slot, picker and rank

### Setup contribution

| Setup Id | Max Entries | Picker | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Confirmation Rate Pct | Fills | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 09:25_LONG | 4 | max_move | 122 | 122 | 78 | 63.93% | 61 | 50.00% | 32 | 29 | 52.46% | 1.6844 | +11.0095 | +5,289.78 |
| 09:25_SHORT | 4 | max_move | 261 | 261 | 114 | 43.68% | 62 | 23.75% | 28 | 34 | 45.16% | 2.0559 | +22.3665 | +10,997.77 |
| 09:30_LONG | 1 | max_move | 65 | 65 | 17 | 26.15% | 10 | 15.38% | 4 | 6 | 40.00% | 1.5393 | +3.0676 | +1,489.08 |
| 09:30_SHORT | 4 | max_volume | 101 | 101 | 41 | 40.59% | 30 | 29.70% | 14 | 16 | 46.67% | 1.3610 | +5.9447 | +3,075.48 |
| 09:35_LONG | 1 | max_liquidity | 248 | 171 | 35 | 20.47% | 17 | 9.94% | 9 | 8 | 52.94% | 1.5052 | +3.7417 | +1,914.06 |
| 09:35_SHORT | 2 | max_liquidity | 36 | 36 | 15 | 41.67% | 8 | 22.22% | 4 | 4 | 50.00% | 2.4690 | +6.7764 | +3,413.11 |
| 09:40_LONG | 1 | max_liquidity | 106 | 76 | 25 | 32.89% | 13 | 17.11% | 8 | 5 | 61.54% | 4.8812 | +12.6555 | +6,080.76 |
| 09:40_SHORT | 1 | max_move | 178 | 178 | 50 | 28.09% | 16 | 8.99% | 8 | 8 | 50.00% | 1.3728 | +3.1307 | +1,821.05 |
| 09:45_LONG | 1 | max_move | 36 | 36 | 13 | 36.11% | 6 | 16.67% | 4 | 2 | 66.67% | 4.5887 | +4.2894 | +2,059.70 |
| 09:45_SHORT | 1 | max_volume | 88 | 88 | 21 | 23.86% | 9 | 10.23% | 5 | 4 | 55.56% | 1.0211 | +0.0723 | +171.26 |

![Setup contribution](report_v10_assets/setup_net_pnl.png)

### Side

| Side | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|
| LONG | 577 | 470 | 168 | 107 | 57 | 50 | 53.27% | 2.0335 | +34.7637 | +16,833.38 |
| SHORT | 664 | 664 | 241 | 125 | 59 | 66 | 47.20% | 1.7079 | +38.2908 | +19,478.67 |

### Signal slot

| Signal End | Raw Base 5M Candidates | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|
| 09:25 | 383 | 383 | 192 | 123 | 60 | 63 | 48.78% | 1.8956 | +33.3760 | +16,287.55 |
| 09:30 | 166 | 166 | 58 | 40 | 18 | 22 | 45.00% | 1.4068 | +9.0123 | +4,564.56 |
| 09:35 | 284 | 207 | 50 | 25 | 13 | 12 | 52.00% | 1.8751 | +10.5181 | +5,327.18 |
| 09:40 | 284 | 254 | 75 | 29 | 16 | 13 | 55.17% | 2.3540 | +15.7863 | +7,901.81 |
| 09:45 | 124 | 124 | 34 | 15 | 9 | 6 | 60.00% | 1.9426 | +4.3617 | +2,230.96 |

### Picker

| Picker | Post Overlay Selected | One Minute Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|
| max_move | 662 | 272 | 155 | 76 | 79 | 49.03% | 1.8347 | +43.8637 | +21,657.38 |
| max_liquidity | 283 | 75 | 38 | 21 | 17 | 55.26% | 2.5166 | +23.1736 | +11,407.94 |
| max_volume | 189 | 62 | 39 | 19 | 20 | 48.72% | 1.3024 | +6.0171 | +3,246.74 |

### Recalculated rank

| Rank Bucket | Selected | Confirmed | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 398 | 145 | 103 | 51 | 52 | 49.51% | 1.7613 | +29.4533 | +14,503.44 |
| 2 | 243 | 82 | 41 | 19 | 22 | 46.34% | 2.0107 | +16.1780 | +8,336.83 |
| 3 | 148 | 55 | 31 | 11 | 20 | 35.48% | 0.5885 | -7.0653 | -3,409.44 |
| 4 | 104 | 41 | 25 | 12 | 13 | 48.00% | 2.3203 | +11.5573 | +5,665.84 |
| 5+ | 241 | 86 | 32 | 23 | 9 | 71.88% | 4.2251 | +22.9311 | +11,215.37 |

## 7. Five-minute indicator study

These tables are descriptive on the same tested history. A favorable bin is a hypothesis for a new frozen test, not permission to optimize the current sample.

### directional_move_pct

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.30 | 244 | 83 | 40 | 34.02% | 16.39% | 23 | 17 | 57.50% | 2.3046 | +18.4321 | +9,159.34 |
| 0.30-0.50 | 393 | 123 | 72 | 31.30% | 18.32% | 38 | 34 | 52.78% | 1.5365 | +14.1752 | +7,462.70 |
| 0.50-0.75 | 252 | 107 | 59 | 42.46% | 23.41% | 30 | 29 | 50.85% | 2.2308 | +28.0075 | +13,926.24 |
| 0.75-1.00 | 141 | 58 | 35 | 41.13% | 24.82% | 14 | 21 | 40.00% | 1.2931 | +4.3913 | +2,016.02 |
| 1.00-1.50 | 76 | 30 | 21 | 39.47% | 27.63% | 9 | 12 | 42.86% | 1.7129 | +5.1153 | +2,279.79 |
| 1.50+ | 28 | 8 | 5 | 28.57% | 17.86% | 2 | 3 | 40.00% | 2.2956 | +2.9331 | +1,467.96 |

### oi_change_pct

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.50 | 377 | 135 | 78 | 35.81% | 20.69% | 47 | 31 | 60.26% | 2.4643 | +33.3013 | +16,391.80 |
| 0.50-1.00 | 237 | 90 | 42 | 37.97% | 17.72% | 14 | 28 | 33.33% | 0.9812 | -0.3232 | -3.76 |
| 1.00-2.00 | 248 | 85 | 51 | 34.27% | 20.56% | 25 | 26 | 49.02% | 1.4442 | +9.9908 | +4,964.04 |
| 2.00-5.00 | 179 | 61 | 38 | 34.08% | 21.23% | 17 | 21 | 44.74% | 1.8045 | +13.0327 | +6,700.92 |
| 5.00+ | 93 | 38 | 23 | 40.86% | 24.73% | 13 | 10 | 56.52% | 2.8761 | +17.0528 | +8,259.06 |

### volume_ratio

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <1.50 | 214 | 56 | 29 | 26.17% | 13.55% | 12 | 17 | 41.38% | 1.0947 | +1.4470 | +889.07 |
| 1.50-2.00 | 218 | 78 | 41 | 35.78% | 18.81% | 27 | 14 | 65.85% | 3.8040 | +32.7038 | +16,134.37 |
| 2.00-3.00 | 251 | 88 | 50 | 35.06% | 19.92% | 20 | 30 | 40.00% | 1.0947 | +2.2715 | +1,270.33 |
| 3.00-5.00 | 297 | 128 | 69 | 43.10% | 23.23% | 38 | 31 | 55.07% | 2.4350 | +29.5228 | +14,589.98 |
| 5.00+ | 154 | 59 | 43 | 38.31% | 27.92% | 19 | 24 | 44.19% | 1.4383 | +7.1094 | +3,428.31 |

### traded_value_cr

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <2.5cr | 49 | 8 | 1 | 16.33% | 2.04% | 1 | 0 | 100.00% | ∞ | +1.0377 | +501.16 |
| 2.5-5cr | 174 | 53 | 22 | 30.46% | 12.64% | 13 | 9 | 59.09% | 2.7177 | +11.1789 | +5,646.03 |
| 5-10cr | 222 | 83 | 51 | 37.39% | 22.97% | 24 | 27 | 47.06% | 1.3610 | +8.0546 | +4,351.29 |
| 10-25cr | 358 | 141 | 74 | 39.39% | 20.67% | 39 | 35 | 52.70% | 2.0352 | +27.3157 | +13,316.32 |
| 25cr+ | 331 | 124 | 84 | 37.46% | 25.38% | 39 | 45 | 46.43% | 1.7832 | +25.4675 | +12,497.26 |

### five_min_range_pct

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.30 | 12 | 2 | 1 | 16.67% | 8.33% | 0 | 1 | 0.00% | 0.0000 | -1.1584 | -577.93 |
| 0.30-0.50 | 242 | 77 | 33 | 31.82% | 13.64% | 16 | 17 | 48.48% | 1.3985 | +6.4286 | +3,053.57 |
| 0.50-0.75 | 388 | 131 | 77 | 33.76% | 19.85% | 46 | 31 | 59.74% | 2.2304 | +29.2353 | +15,046.56 |
| 0.75-1.00 | 253 | 97 | 56 | 38.34% | 22.13% | 26 | 30 | 46.43% | 1.8821 | +17.9812 | +8,593.25 |
| 1.00-1.50 | 172 | 77 | 49 | 44.77% | 28.49% | 23 | 26 | 46.94% | 2.0912 | +19.6392 | +9,719.14 |
| 1.50+ | 67 | 25 | 16 | 37.31% | 23.88% | 5 | 11 | 31.25% | 1.1119 | +0.9286 | +477.46 |

### ema_total_gap_pct

| Bin | Selected | Confirmed | Fills | Confirmation Rate Pct | Fill Rate Pct | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| <0.10 | 17 | 7 | 6 | 41.18% | 35.29% | 5 | 1 | 83.33% | 18.6960 | +7.0035 | +3,511.18 |
| 0.10-0.25 | 116 | 47 | 25 | 40.52% | 21.55% | 16 | 9 | 64.00% | 2.7384 | +12.5298 | +6,080.28 |
| 0.25-0.50 | 274 | 102 | 60 | 37.23% | 21.90% | 28 | 32 | 46.67% | 1.6232 | +13.0035 | +6,561.18 |
| 0.50-1.00 | 395 | 135 | 78 | 34.18% | 19.75% | 36 | 42 | 46.15% | 1.2839 | +8.7269 | +4,580.21 |
| 1.00+ | 332 | 118 | 63 | 35.54% | 18.98% | 31 | 32 | 49.21% | 2.1147 | +31.7907 | +15,579.21 |

### Winner-versus-loser indicator medians

| Indicator | Winners | Losers |
|---|---|---|
| confirmation_adverse_wick_ratio | 0.145 | 0.145 |
| confirmation_body_ratio | 0.738 | 0.730 |
| confirmation_close_location | 0.855 | 0.855 |
| directional_move_pct | 0.478 | 0.550 |
| ema_fast_gap_pct | 0.306 | 0.322 |
| ema_slow_gap_pct | 0.259 | 0.335 |
| ema_total_gap_pct | 0.642 | 0.727 |
| five_min_adverse_wick_ratio | 0.166 | 0.179 |
| five_min_body_ratio | 0.675 | 0.712 |
| five_min_directional_close_location | 0.834 | 0.821 |
| five_min_range_pct | 0.718 | 0.801 |
| oi_change_pct | 0.869 | 0.964 |
| traded_value_cr | 15.224 | 18.077 |
| trigger_distance_c5_bps | 21.598 | 25.189 |
| volume_ratio | 2.759 | 2.745 |

### Spearman correlation with filled-trade net return

| Indicator | Observations | Spearman Vs Net Return |
|---|---|---|
| holding_minutes | 232 | +0.364 |
| confirmation_close_location | 232 | -0.081 |
| confirmation_adverse_wick_ratio | 232 | +0.081 |
| confirmation_body_ratio | 232 | -0.080 |
| ema_slow_gap_pct | 232 | -0.062 |
| oi_change_pct | 232 | -0.057 |
| ema_total_gap_pct | 232 | -0.052 |
| trigger_distance_c5_bps | 232 | -0.050 |
| ema_fast_gap_pct | 232 | -0.041 |
| five_min_body_ratio | 232 | -0.037 |
| five_min_adverse_wick_ratio | 232 | +0.037 |
| five_min_directional_close_location | 232 | -0.037 |
| volume_ratio | 232 | +0.033 |
| five_min_range_pct | 232 | +0.026 |
| directional_move_pct | 232 | -0.025 |
| traded_value_cr | 232 | +0.015 |
| confirmation_minute | 232 | +0.015 |
| entry_minute | 232 | +0.005 |

Correlations are univariate, non-causal and affected by setup mix. Values near zero mean the indicator did not order outcomes monotonically in this sample.


## 8. One-minute confirmation and entry quality

### confirmation_body_ratio

| Bin | Selected | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| <0.40 | 15 | 11 | 7 | 4 | 63.64% | 2.6837 | +3.7271 | +1,733.76 |
| 0.40-0.50 | 22 | 12 | 4 | 8 | 33.33% | 1.3395 | +1.7083 | +893.63 |
| 0.50-0.60 | 43 | 23 | 17 | 6 | 73.91% | 5.2849 | +20.2032 | +9,592.87 |
| 0.60-0.75 | 138 | 76 | 32 | 44 | 42.11% | 1.3356 | +11.3572 | +5,819.89 |
| 0.75+ | 191 | 110 | 56 | 54 | 50.91% | 1.8600 | +36.0585 | +18,271.90 |

### confirmation_adverse_wick_ratio

| Bin | Selected | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| <0.10 | 164 | 88 | 43 | 45 | 48.86% | 1.6800 | +24.0878 | +12,089.81 |
| 0.10-0.20 | 93 | 58 | 26 | 32 | 44.83% | 1.4647 | +10.8584 | +5,245.10 |
| 0.20-0.30 | 88 | 56 | 31 | 25 | 55.36% | 2.2681 | +26.1128 | +13,386.02 |
| 0.30-0.40 | 49 | 22 | 11 | 11 | 50.00% | 1.9125 | +6.0093 | +2,695.35 |
| 0.40-0.50 | 14 | 8 | 5 | 3 | 62.50% | 4.4012 | +5.9862 | +2,895.77 |
| 0.50+ | 1 | 0 | 0 | 0 | — | — | +0.0000 | +0.00 |

### confirmation_close_location

| Bin | Selected | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| 0.50-0.60 | 15 | 8 | 5 | 3 | 62.50% | 4.4012 | +5.9862 | +2,895.77 |
| 0.60-0.75 | 84 | 43 | 23 | 20 | 53.49% | 2.1516 | +18.1666 | +8,749.61 |
| 0.75-0.90 | 145 | 92 | 44 | 48 | 47.83% | 1.7032 | +24.4496 | +12,408.57 |
| 0.90+ | 165 | 89 | 44 | 45 | 49.44% | 1.6903 | +24.4520 | +12,258.10 |

### trigger_distance_c5_bps

| Bin | Selected | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|---|
| 0-10 | 57 | 31 | 20 | 11 | 64.52% | 3.0571 | +15.2926 | +7,539.89 |
| 10-20 | 122 | 62 | 32 | 30 | 51.61% | 1.6680 | +16.3864 | +8,059.51 |
| 20-30 | 107 | 62 | 27 | 35 | 43.55% | 1.5684 | +14.2284 | +7,056.51 |
| 30-50 | 86 | 52 | 23 | 29 | 44.23% | 1.6572 | +14.2929 | +7,140.13 |
| 50+ | 37 | 25 | 14 | 11 | 56.00% | 2.4308 | +12.8540 | +6,516.01 |

### Confirmation minute

| Confirmation Minute | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| 1 | 172 | 86 | 86 | 50.00% | 1.8271 | +56.0985 | +27,795.82 |
| 2 | 44 | 21 | 23 | 47.73% | 1.5395 | +7.9911 | +4,011.27 |
| 3 | 16 | 9 | 7 | 56.25% | 2.7605 | +8.9648 | +4,504.96 |

### Entry minute

| Entry Minute | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| 2 | 140 | 71 | 69 | 50.71% | 1.8562 | +47.8251 | +23,948.17 |
| 3 | 62 | 30 | 32 | 48.39% | 1.6625 | +13.7677 | +6,659.85 |
| 4 | 20 | 9 | 11 | 45.00% | 1.7643 | +5.7863 | +2,828.73 |
| 5 | 10 | 6 | 4 | 60.00% | 2.6116 | +5.6754 | +2,875.31 |

## 9. Exit, holding-time, gaps and OHLC ambiguity

### Exit reason

| Exit Reason | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| STOP | 103 | 0 | 103 | 0.00% | 0.0000 | -82.9813 | -39,894.96 |
| LAST_REAL_BAR_SENSITIVITY | 69 | 56 | 13 | 81.16% | 12.1135 | +52.7597 | +25,984.87 |
| TARGET | 60 | 60 | 0 | 100.00% | ∞ | +103.2760 | +50,222.14 |

### Holding time

| Holding Bin | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| <15 | 66 | 19 | 47 | 28.79% | 0.6880 | -10.0091 | -4,834.05 |
| 120+ | 102 | 73 | 29 | 71.57% | 5.1169 | +81.9877 | +39,927.85 |
| 60-120 | 23 | 8 | 15 | 34.78% | 0.9976 | -0.0362 | +330.56 |
| 15-30 | 21 | 9 | 12 | 42.86% | 0.8254 | -1.6141 | -712.24 |
| 30-60 | 20 | 7 | 13 | 35.00% | 1.2332 | +2.7260 | +1,599.92 |

### Gap versus trigger-touch fill

| Gap Group | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| TRIGGER_TOUCH | 208 | 105 | 103 | 50.48% | 1.9081 | +69.5851 | +34,535.23 |
| GAP_FILL | 24 | 11 | 13 | 45.83% | 1.3125 | +3.4693 | +1,776.82 |

- Median/average holding: **73.0 / 152.2 minutes**.
- Ambiguous entry bars: **2**; ambiguous excursion boundaries: **225**.
- Median MFE lower/upper bounds: **0.9927% / 1.0016%**.
- Median MAE lower/upper bounds: **0.4085% / 0.4901%**.
- Same-bar stop/target ambiguity is resolved stop-first, which is conservative but cannot recover tick order from 1-minute OHLC.

## 10. Symbol concentration and extreme trades

The 232 fills span **133 unique symbols**. The 10 largest absolute symbol contributions account for **22.01%** of total absolute symbol P&L, so concentration must be monitored.

### Top symbols

| Symbol | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| MCX | 3 | 3 | 0 | 100.00% | ∞ | +6.3164 | +3,084.43 |
| TCS | 3 | 2 | 1 | 66.67% | 10.3343 | +5.1433 | +2,463.88 |
| OFSS | 3 | 2 | 1 | 66.67% | 9.4030 | +4.6322 | +1,948.35 |
| TATAELXSI | 2 | 2 | 0 | 100.00% | ∞ | +3.8874 | +1,864.59 |
| BANDHANBNK | 3 | 2 | 1 | 66.67% | 7.3754 | +3.5379 | +1,765.70 |
| CGPOWER | 2 | 1 | 1 | 50.00% | 11.9384 | +3.5234 | +1,742.47 |
| INDUSTOWER | 3 | 3 | 0 | 100.00% | ∞ | +3.3938 | +1,690.80 |
| PERSISTENT | 3 | 2 | 1 | 66.67% | 9.3217 | +3.2934 | +1,622.15 |
| ADANIGREEN | 1 | 1 | 0 | 100.00% | ∞ | +3.1097 | +1,514.16 |
| ANGELONE | 3 | 2 | 1 | 66.67% | 62.5540 | +2.9125 | +1,447.71 |
| VMM | 1 | 1 | 0 | 100.00% | ∞ | +2.8489 | +1,423.26 |
| TRENT | 1 | 1 | 0 | 100.00% | ∞ | +2.8485 | +1,381.64 |
| SIEMENS | 2 | 2 | 0 | 100.00% | ∞ | +2.6415 | +1,278.02 |
| MARUTI | 2 | 2 | 0 | 100.00% | ∞ | +2.9511 | +1,264.13 |
| TECHM | 1 | 1 | 0 | 100.00% | ∞ | +2.5989 | +1,263.57 |

### Bottom symbols

| Symbol | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| DMART | 2 | 0 | 2 | 0.00% | 0.0000 | -2.3036 | -1,090.98 |
| POWERINDIA | 3 | 0 | 3 | 0.00% | 0.0000 | -2.9712 | -994.72 |
| BSE | 7 | 2 | 5 | 28.57% | 0.6218 | -1.5661 | -800.30 |
| LAURUSLABS | 3 | 1 | 2 | 33.33% | 0.1667 | -1.6024 | -774.13 |
| HDFCAMC | 3 | 1 | 2 | 33.33% | 0.2254 | -1.3968 | -675.74 |
| WAAREEENER | 3 | 1 | 2 | 33.33% | 0.2266 | -1.3963 | -674.95 |
| CANBK | 2 | 0 | 2 | 0.00% | 0.0000 | -1.2093 | -603.65 |
| POLICYBZR | 2 | 0 | 2 | 0.00% | 0.0000 | -1.2084 | -594.12 |
| DIXON | 4 | 1 | 3 | 25.00% | 0.1348 | -1.3458 | -592.86 |
| GODFRYPHLP | 2 | 0 | 2 | 0.00% | 0.0000 | -1.2049 | -589.84 |
| SUZLON | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1675 | -583.20 |
| BPCL | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1649 | -580.33 |
| BAJFINANCE | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1608 | -579.48 |
| GODREJCP | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1574 | -576.98 |
| BHARATFORG | 1 | 0 | 1 | 0.00% | 0.0000 | -1.1541 | -576.30 |

### Best trades

| Session Date | Setup Id | Side | Symbol | Entry Time | Exit Reason | Net Return Pct | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| 2026-07-24 | 09:30_SHORT | SHORT | CGPOWER | 2026-07-24 09:32:00+05:30 | TARGET | 3.85% | +1,902.59 |
| 2026-05-29 | 09:30_SHORT | SHORT | MCX | 2026-05-29 09:35:00+05:30 | TARGET | 3.85% | +1,884.96 |
| 2026-07-22 | 09:30_SHORT | SHORT | ADANIGREEN | 2026-07-22 09:34:00+05:30 | LAST_REAL_BAR_SENSITIVITY | 3.11% | +1,514.16 |
| 2026-07-23 | 09:25_SHORT | SHORT | VMM | 2026-07-23 09:27:00+05:30 | TARGET | 2.85% | +1,423.26 |
| 2026-07-07 | 09:35_SHORT | SHORT | MOTHERSON | 2026-07-07 09:37:00+05:30 | TARGET | 2.85% | +1,422.30 |
| 2026-07-22 | 09:35_SHORT | SHORT | BANDHANBNK | 2026-07-22 09:37:00+05:30 | TARGET | 2.85% | +1,421.17 |
| 2026-07-15 | 09:25_SHORT | SHORT | PATANJALI | 2026-07-15 09:27:00+05:30 | TARGET | 2.84% | +1,418.90 |
| 2026-07-24 | 09:35_SHORT | SHORT | MOTILALOFS | 2026-07-24 09:37:00+05:30 | TARGET | 2.85% | +1,418.59 |
| 2026-07-08 | 09:25_SHORT | SHORT | JIOFIN | 2026-07-08 09:28:00+05:30 | TARGET | 2.84% | +1,416.30 |
| 2026-08-10 | 09:40_SHORT | SHORT | PFC | 2026-08-10 09:42:00+05:30 | TARGET | 2.85% | +1,413.77 |

### Worst trades

| Session Date | Setup Id | Side | Symbol | Entry Time | Exit Reason | Net Return Pct | Net Pnl Rs |
|---|---|---|---|---|---|---|---|
| 2026-07-16 | 09:30_SHORT | SHORT | SUZLON | 2026-07-16 09:32:00+05:30 | STOP | -1.17% | -583.20 |
| 2026-07-16 | 09:35_LONG | LONG | BPCL | 2026-07-16 09:37:00+05:30 | STOP | -1.16% | -580.33 |
| 2026-07-24 | 09:30_SHORT | SHORT | SWIGGY | 2026-07-24 09:32:00+05:30 | STOP | -1.16% | -579.84 |
| 2026-06-02 | 09:30_SHORT | SHORT | BAJFINANCE | 2026-06-02 09:32:00+05:30 | STOP | -1.16% | -579.48 |
| 2026-07-15 | 09:40_SHORT | SHORT | PATANJALI | 2026-07-15 09:42:00+05:30 | STOP | -1.16% | -579.16 |
| 2026-08-25 | 09:35_LONG | LONG | LICI | 2026-08-25 09:39:00+05:30 | STOP | -1.16% | -577.93 |
| 2026-08-12 | 09:30_SHORT | SHORT | GODREJCP | 2026-08-12 09:32:00+05:30 | STOP | -1.16% | -576.98 |
| 2026-08-13 | 09:35_LONG | LONG | JIOFIN | 2026-08-13 09:37:00+05:30 | STOP | -1.15% | -576.75 |
| 2026-06-05 | 09:30_LONG | LONG | ETERNAL | 2026-06-05 09:32:00+05:30 | STOP | -1.16% | -576.49 |
| 2026-07-23 | 09:40_SHORT | SHORT | BHARATFORG | 2026-07-23 09:42:00+05:30 | STOP | -1.15% | -576.30 |

## 11. Cost sensitivity and economic assumptions

| Period | Scenario | Cost Bps | Slippage Bps | Fills | Wins | Losses | Win Rate Pct | Profit Factor | Net Return Points | Net Pnl Rs | Max Daily Drawdown Points |
|---|---|---|---|---|---|---|---|---|---|---|---|
| FULL_USABLE | REFERENCE_15_0 | 15.000 | 0.000 | 232 | 116 | 116 | 50.00% | 1.8327 | +73.0544 | +36,312.05 | +9.3513 |
| CORE_59 | REFERENCE_15_0 | 15.000 | 0.000 | 211 | 108 | 103 | 51.18% | 1.8862 | +70.4389 | +35,007.42 | +9.3513 |
| FORWARD_EXTENSION | REFERENCE_15_0 | 15.000 | 0.000 | 21 | 8 | 13 | 38.10% | 1.3172 | +2.6155 | +1,304.63 | +2.3632 |
| FULL_USABLE | STRESS_20_2 | 20.000 | 2.000 | 232 | 112 | 120 | 48.28% | 1.5923 | +56.5340 | +28,322.68 | +10.6275 |
| CORE_59 | STRESS_20_2 | 20.000 | 2.000 | 211 | 104 | 107 | 49.29% | 1.6366 | +55.0610 | +27,574.88 | +10.6275 |
| FORWARD_EXTENSION | STRESS_20_2 | 20.000 | 2.000 | 21 | 8 | 13 | 38.10% | 1.1647 | +1.4731 | +747.79 | +2.5130 |
| FULL_USABLE | STRESS_25_5 | 25.000 | 5.000 | 231 | 105 | 126 | 45.45% | 1.2961 | +31.4655 | +16,141.87 | +11.0797 |
| CORE_59 | STRESS_25_5 | 25.000 | 5.000 | 210 | 97 | 113 | 46.19% | 1.3226 | +31.1633 | +15,960.60 | +11.0797 |
| FORWARD_EXTENSION | STRESS_25_5 | 25.000 | 5.000 | 21 | 8 | 13 | 38.10% | 1.0313 | +0.3023 | +181.28 | +2.6627 |

- Reference estimated costs deducted: **Rs 16,862.45**.
- Gross modeled P&L before that cost: **Rs 53,174.50**.
- Approximate additional one-way-equivalent cost headroom before modeled net P&L reaches zero: **32.30 bps per entry notional**. This is arithmetic headroom, not a live fill guarantee.
- Reference slippage is zero; the 20/2 and 25/5 scenarios are more appropriate planning cases.
- Actual brokerage, taxes, futures basis, lot rounding, margin changes, impact and rejected/partial orders are not fully modeled.

## 12. What the evidence currently says

1. **The edge is real inside the tested simulator but not yet independently validated.** PF 1.8327 and positive stress cases are encouraging; repeated parameter search and incomplete source coverage prevent promotion.
2. **Setup contribution is uneven.** Best contribution: `09:25_SHORT` Rs +10,997.77; weakest: `09:45_SHORT` Rs +171.26. Removing or tightening a setup must be tested through the global portfolio ledger because contributions are not additive counterfactuals.
   Every setup is net-positive in the full reference sample, so this report does not identify an obvious safe deletion.
3. **Forward behavior is weaker.** The six-session extension is 8-13, PF 1.3172; treat this as a warning, not a conclusion.
4. **July contributes 69.49% of full net P&L.** June was only marginal and August had lower WR, so calendar-regime concentration is material.
5. **End-of-day handling is material.** `69` fills use the sensitivity exit and contribute Rs +25,984.87. Exact square-off data and policy should be required before judging exits.
6. **The indicator relationship is mostly non-monotonic.** For example, OI change 0.50–1.00% was roughly flat while both lower and much higher OI bins were profitable; volume ratio 1.50–2.00 and 3.00–5.00 outperformed neighboring bins. Directly choosing the best bin would be in-sample overfitting.
7. **Rank is also non-monotonic.** Rank 3 was negative while ranks 4 and 5+ were strong, which points to setup/cap mixture rather than a simple global rank cutoff.
8. **Short holding periods were weak while 120+ minute holds carried the result.** Holding time is outcome-dependent, so it can motivate an exit experiment but cannot be used as an entry-time predictor.
9. **OHLC excursion precision is limited.** 225 of 232 filled trades have an ambiguous MFE/MAE boundary, so do not tune exits to tiny excursion differences.
10. **Actual FnO economics remain untested.** Selection uses futures OI, but execution is modeled on cash bars at lot size one.

## 13. Safe improvement test plan

### Stage A — repair validity before optimizing

1. Rebuild missing/incomplete symbol-sessions, including 26-Aug, and require exact 15:30 paths.
2. Add an actual near-month futures execution replay using historical futures 1-minute prices, dated lot sizes, tick sizes, basis, rollover and realistic margin. Keep cash-execution results as a separate diagnostic.
3. Freeze the present strategy hash and collect at least 20–30 genuinely new sessions without changing thresholds.

### Stage B — one-factor setup ablations

4. Test each weak setup as `ON` versus `OFF`, one at a time, through the same global portfolio ledger and all three cost cases.
5. Test LONG and SHORT caps separately. Do not infer a cap from raw trade addition because duplicate-symbol and margin ordering interact.
6. Re-test `.50` and Gap2 independently on the frozen forward window to determine whether each adds value outside the development sample.

### Stage C — confirmation and exit tests

7. For setups with three confirmation minutes, compare S+1-only, S+1..2 and S+1..3 without changing the five-minute selection.
8. Test one confirmation gate at a time: body threshold, adverse wick, close location, and trigger-distance ceiling. Use predeclared values derived from market logic, not the best bin in this report.
9. Split exits into target/stop/EOD cohorts and test an earlier time stop or trailing rule only where the full OHLC path is exact.

### Stage D — acceptance criteria

10. Require improvement in PF, net P&L, drawdown and forward-window behavior under both stress cases; reject changes that improve only WR.
11. Apply a multiple-testing penalty or keep a final untouched validation block. Record every attempted parameter set, including failures.
12. Promote only after paper/live parity confirms candidate ranking, timestamp availability, order placement, gap behavior and actual costs.

## 14. Reproducibility and supporting files

Report command:

```powershell
python -u fno_v10_full_historical_report.py --source-run "C:\TradingData\eqidv2\fno_oi\strategy_research\v10_max050_gap2_full_history_v1\run_20260830T163837220506+0530" --stress-run "C:\TradingData\eqidv2\fno_oi\strategy_research\v10_max050_gap2_full_history_v1\run_20260830T153606247643+0530" --report report_v10.md --assets-dir report_v10_assets
```

Supporting CSVs in `report_v10_assets/` contain every table used here, including daily, rolling, setup, indicator-bin, correlation, symbol, cost-sensitivity and extreme-trade outputs.

## 15. Glossary

- **Raw/base 5m candidate:** passed the setup's EMA, move, futures-OI, relative-volume and traded-value rules.
- **Selected:** passed Stage 7 and `.50` post-selection overlays and was reranked.
- **Confirmed:** a completed eligible 1-minute candle passed direction, close, body, wick and optional close-location gates.
- **Filled:** a later 1-minute bar crossed the stop-entry trigger and survived Gap2 and portfolio constraints.
- **PF:** gross positive net-return points divided by absolute gross negative net-return points.
- **Net return points:** sum of per-trade percentage returns after modeled cost; not portfolio percent return.
- **MDD:** maximum drawdown of cumulative daily summed trade-return points unless explicitly marked Rs.
- **MFE/MAE:** OHLC-derived favorable/adverse excursion bounds; boundary ambiguity is explicitly flagged.
