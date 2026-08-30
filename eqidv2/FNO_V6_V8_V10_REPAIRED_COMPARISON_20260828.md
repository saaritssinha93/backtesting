# FnO V6, V8 and V10 repaired-data comparison

Generated 2026-08-28 IST. Research only; no live, paper, production, scheduler, or frozen strategy configuration was changed.

## Decision

The best clean shadow candidate is **V10 Stage 7 plus `09:35 LONG five-minute move <= 0.50%`** (`0935_LONG_MOVE_MAX_050`). It improves the repaired V8 and Stage 7 controls on the full window, the fixed test slice, profit factor, win rate, rupee P&L, and cost stress, without increasing historical daily drawdown.

If V6 must remain the base, use **V6 A1 plus A2 with the `09:35 LONG <= 0.50%` cap** as the shadow candidate. The `.40%` cap has a slightly higher historical result, but both `.40%` families failed badly on 27 August while `.50%` retained the control trades.

Do not promote either rule yet. The loaded variants are a selected research universe, the fixed test slice contains only 10 sessions, and the current-day source fails the strict closing-slot completeness contract.

## Scope and economics

- Historical locked comparison: 59 sessions, 2026-05-27 through 2026-08-19.
- Fixed split: 49 TRAIN sessions before 2026-08-06 and 10 TEST sessions from 2026-08-06.
- Latest completed trading session: 2026-08-27, evaluated separately with the correct 210-stock/26SEP snapshot.
- Reference costs: 15 bps, zero additional slippage.
- Stress cases: 20 bps + 2 bps slippage and 25 bps + 5 bps slippage.
- Historical runs use the locked static 208-stock/26AUG research universe. They are not a point-in-time rolling-universe simulation.

## Primary historical comparison

| Strategy | Fills | W/L | WR | PF | Net points | Net P&L | Daily DD | TRAIN net | TEST net |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| V6 control | 165 | 78/87 | 47.273% | 1.6307 | 46.8200 | Rs 23,862.48 | 9.1217 | 38.4547 | 8.3653 |
| V6 A1 + A2 `.50` | 158 | 78/80 | 49.367% | 1.7718 | 52.7328 | Rs 26,738.94 | 8.7885 | 42.5268 | 10.2060 |
| V8 combined control | 228 | 110/118 | 48.246% | 1.6652 | 61.1489 | Rs 30,482.85 | 8.5840 | 54.9198 | 6.2291 |
| V10 Stage 7 | 224 | 110/114 | 49.107% | 1.7003 | 63.0419 | Rs 31,404.13 | 8.5840 | 56.1595 | 6.8824 |
| **V10 Stage 7 + `.50`** | **220** | **110/110** | **50.000%** | **1.7933** | **67.7148** | **Rs 33,678.79** | **8.5840** | **59.6449** | **8.0698** |
| V10 `.50` + Gap2 | 211 | 108/103 | 51.185% | 1.8862 | 70.4389 | Rs 35,007.42 | 9.3513 | 62.3691 | 8.0698 |

`V10 .50 + Gap2` is the highest historical-net row, but it is an explicitly post-selection combination. Against the isolated `.50` rule it improves TRAIN, only ties TEST and 27 August, and raises drawdown. It is therefore exploratory, not the recommended rule.

## Latest completed session: 27 August 2026

| Strategy | Fills | W/L | WR | PF | Net points | Net P&L | Decision |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| V6 control | 3 | 1/2 | 33.333% | 1.5077 | +0.7900 | +Rs 419.32 | Diagnostic |
| V6 A1 + A2 `.40` | 3 | 0/3 | 0.000% | 0.0000 | -2.7187 | -Rs 1,146.72 | Reject `.40` for shadow |
| V6 A1 + A2 `.50` | 3 | 1/2 | 33.333% | 1.5077 | +0.7900 | +Rs 419.32 | V6 shadow candidate |
| V8 combined | 6 | 2/4 | 33.333% | 1.3921 | +0.9435 | +Rs 477.23 | Comparator |
| V10 Stage 7 | 6 | 2/4 | 33.333% | 1.3921 | +0.9435 | +Rs 477.23 | Control |
| V10 Stage 7 + `.40` | 6 | 1/5 | 16.667% | 0.2812 | -2.5652 | -Rs 1,088.81 | Reject `.40` for shadow |
| **V10 Stage 7 + `.50`** | **6** | **2/4** | **33.333%** | **1.3921** | **+0.9435** | **+Rs 477.23** | **V10 shadow candidate** |
| V10 `.50` + Gap0/reject-all | 5 | 2/3 | 40.000% | 1.9107 | +1.5965 | +Rs 799.70 | Today-only diagnostic |

The `.40` row is particularly unstable: it removes ADANIPOWER and backfills a losing POWERINDIA trade. The gap-zero/reject-all result skips a LUPIN loss, but the research cancellation is not equivalent to live resting stop-market/stop-limit behavior. It must not be copied directly into live order handling.

## V10 individual-filter matrix

| V10 variant | Fills | WR | PF | Net points | 27-Aug net | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Stage 7 control | 224 | 49.107% | 1.7003 | 63.0419 | +0.9435 | Baseline |
| `09:35 LONG max .40` | 219 | 49.772% | 1.8032 | 68.0474 | -2.5652 | Reject: boundary-sensitive today |
| **`09:35 LONG max .50`** | **220** | **50.000%** | **1.7933** | **67.7148** | **+0.9435** | **Best clean shadow** |
| `09:35 LONG max .60` | 222 | 49.550% | 1.7462 | 65.4090 | +0.9435 | Inferior to `.50` |
| `09:25 body/range >= .50` | 215 | 48.837% | 1.6945 | 60.9993 | +0.9399 | Reject |
| Prior-10 volume ratio `>= 1.00` | 165 | 45.455% | 1.4847 | 35.1887 | -1.9127 | Reject |
| Prior-10 volume ratio `>= 1.25` | 135 | 45.185% | 1.5022 | 30.2564 | -1.9127 | Reject |
| Prior-10 range ratio `>= 1.00` | 137 | 44.526% | 1.3972 | 24.8095 | -2.3653 | Reject |
| Prior-10 range ratio `>= 1.25` | 100 | 46.000% | 1.5288 | 24.7171 | -2.3653 | Reject |
| Stage 7 + Gap2 | 216 | 50.000% | 1.7574 | 64.6133 | +0.9435 | Secondary exploratory filter |

Market/sector alignment was not run because neither immutable snapshot contains a causally bound NIFTY or sector 1-minute/5-minute series. A generic or current-membership proxy would introduce unmeasured look-ahead and classification drift.

## V6 decomposition

| V6 variant | Fills | WR | PF | Net points | Net P&L | 27-Aug net |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Control | 165 | 47.273% | 1.6307 | 46.8200 | Rs 23,862.48 | +0.7900 |
| A1: `09:40 LONG min .40` | 162 | 48.148% | 1.6584 | 48.0599 | Rs 24,464.28 | +0.7900 |
| A2 only: `09:35 LONG max .40` | 160 | 48.125% | 1.7519 | 51.8255 | Rs 26,298.80 | -2.7187 |
| A2 only: `09:35 LONG max .50` | 161 | 48.447% | 1.7402 | 51.4929 | Rs 26,137.14 | +0.7900 |
| A2 only: `09:35 LONG max .60` | 163 | 47.853% | 1.6844 | 49.1871 | Rs 25,002.00 | +0.7900 |
| A1 + A2 `.40` | 157 | 49.045% | 1.7840 | 53.0655 | Rs 26,900.60 | -2.7187 |
| **A1 + A2 `.50`** | **158** | **49.367%** | **1.7718** | **52.7328** | **Rs 26,738.94** | **+0.7900** |
| A1 + A2 `.60` | 160 | 48.750% | 1.7140 | 50.4270 | Rs 25,603.80 | +0.7900 |

A2 supplies most of the V6 historical lift. Adding A1 contributes another 1.2399 points and about Rs 601.80 to both `.40` and `.50` variants.

## Cost stress

| Strategy | 20+2 PF | 20+2 net | 25+5 PF | 25+5 net |
| --- | ---: | ---: | ---: | ---: |
| V6 control | 1.4628 | 36.7005 | 1.1874 | 16.6525 |
| V6 A1 + A2 `.50` | 1.5903 | 43.0556 | 1.2835 | 23.3120 |
| V8 combined | 1.4484 | 44.7582 | 1.1804 | 19.9693 |
| V10 Stage 7 | 1.4801 | 46.8962 | 1.2066 | 22.3674 |
| **V10 Stage 7 + `.50`** | **1.5586** | **51.8162** | **1.2652** | **27.3817** |
| V10 `.50` + Gap2 | 1.6366 | 55.0610 | 1.3226 | 31.1633 |

Every recommended clean challenger remains positive at the harsh 25+5 setting. The V10 `.50` rule also improves the fixed TEST slice under stress: 5.0227 points at 20+2 and 2.0329 at 25+5, versus Stage 7's 3.7602 and 0.7797.

## Repaired-source drift

### V10

- One new unfilled `2026-08-13 | 09:25 LONG | IDEA` candidate; no reference-cost fill, win, loss, or status change.
- Stage 7 and `.50` gain only 0.007716 historical points versus the frozen package; rupee P&L falls Rs 32.61 due to adjusted-price share rounding.
- Five retained fills have split-adjusted paths (INFY, TECHM, LICI and two INDUSTOWER rows).
- 27-August candidates, trades, and metrics are unchanged.

### V6

- Every full-window series loses one fill and one win versus frozen v2: the repaired source removes a winning 2026-06-03 INFY fill.
- Base net drift is -0.860664 points / -Rs 463.90; 20+2 drift is -0.780650 / -Rs 423.20; 25+5 drift is -0.705803 / -Rs 385.11.
- The fixed 10-session TEST slice is unchanged. The extra IDEA candidate is unfilled.
- 27-August candidates, trade states, and metrics are unchanged.

## Data repair and limitations

- Equity repair added 720 timestamps and removed none. The source vendor restated 101,895 overlapping rows across 14 symbols; original files were backed up before both targeted and full-window repair.
- The repaired 27-August equity set has 75,600 rows (210 symbols x 360 rows) and ends at the latest real 15:15 end-labeled bar. It does not contain required 15:20/15:25/15:30 cash bars.
- Futures backfill wrote and verified 216 live 26SEP contracts. The 213 expired 26AUG instruments could not be intraday-backfilled because the live instrument master no longer exposes valid tokens for them.
- Historical strict coverage: 6,350 of 12,272 symbol-sessions complete; 5,922 incomplete. Current-day strict coverage: 0 of 210 complete under the required closing-slot definition.
- Consequently historical and 27-August outputs are diagnostics and `promotion_eligible=false`.

## Robustness interpretation

- All 59 dates align; missing sessions are rejected rather than imputed as zero.
- 100 raw series deduplicate to 88 canonical series only after exact daily-series parity.
- Strict CSCV/PBO is unavailable: 59 is prime and cannot form equal even contiguous blocks without discarding dates.
- Deflated Sharpe is withheld because the complete historical trial universe is unknown and Stage 7 was selected retrospectively.
- The 49/10 split is one fixed diagnostic holdout, not an independent prospective test.
- Win rate is not sufficient by itself. The decision uses PF, net return, drawdown, fixed-slice behavior, today boundary sensitivity, and stress costs together.

## Safe next action

1. Shadow V10 Stage 7 + `.50` with the existing strategy unchanged.
2. If retaining V6, shadow A1 + A2 `.50` in parallel.
3. Freeze thresholds before the shadow period; do not tune on each new day.
4. Require at least 20 untouched sessions and 100 fills before reconsidering promotion.
5. Keep V10 `.50` + Gap2 and Gap0/reject-all in a separate exploratory ledger. Do not use their results to replace the primary shadow rule.
6. Add a causally archived market/sector series and a live-equivalent gap-order model before testing those two ideas again.

## Stored artifacts

- V10 repaired package: `C:\TradingData\eqidv2\fno_oi\strategy_research\v10_repaired_snapshot_reruns_20260827_v1\runs\repaired_20260827T231648973625+0530`
  - `repaired_primary_comparison.csv`
  - `individual_filter_suite\comparison\all_period_metrics.csv`
  - `combo_suite\runs\combo_20260827T232116989611+0530\all_period_metrics.csv`
  - `drift\individual_old_vs_repaired.csv`
- V6 repaired package: `C:\TradingData\eqidv2\fno_oi\strategy_research\v6_isolated_challengers_repaired_full59_today_20260828T0005_v4`
  - `comparison_summary.csv`
  - `daywise.csv`
  - `cost_stress_summary.csv`
  - `reference_metric_drift.csv`
  - `reference_drift_report.md`
- Combined repaired robustness audit: `C:\TradingData\eqidv2\fno_oi\strategy_research\v6_v10_challenger_robustness_audit_v1\comparisons\audit_20260828T000743293640+0530`
  - `report.md`
  - `aligned_daily_returns.csv` (complete day-wise comparison)
  - `variant_metrics.csv`
  - `today_metrics.csv`
  - `cost_stress.csv`
- Historical repair audit: `C:\TradingData\eqidv2\fno_oi\strategy_research\fno_historical_repair_20260827\audit\repair_audit.json`

## Verification

- Final cross-suite regression: 144 tests passed.
- V10: 21 focused tests; 347/347 wrapper artifacts and 160/160 combo artifacts verified; exact Stage 7/MAX050/Gap2 parity passed for history and today.
- V6: 108 combined runner/data/engine tests; 256/256 manifest artifacts verified; direct parity passed across all 129 engine-audit columns for history and today.
- Robustness audit: 14 focused tests; all 10 final audit artifacts verified.
- V10 provenance SHA-256: `f06cd9b128fadc52058d7e4ab3c08f27865d251e9053763c22d320ae15900ae4`.
- V6 manifest SHA-256: `3ff17d4d1463dde1d4cfb2ce236cab053904be3b53c190afbb869e1e0b92d98f`.
- Combined audit manifest SHA-256: `f2952f7e84f9913c710cc3b456ea4868b907372fac80cf892cc6dad01ba2a12`.
