# A_PULLBACK_C2_THEN_BREAK_C2_LOW Baseline

Status before loop: parked by `_LIVE_SURVIVAL_DEMOTION_2026_06_29`.

Pinned split:

| Period | Dates |
|---|---|
| TRAIN | 2026-05-25..2026-06-05 |
| TEST | 2026-06-08..2026-06-12 |

Baseline config:

| Field | Value |
|---|---|
| Side | SHORT |
| Detection | `bear_pullback_c2_break_low` raw 5-minute setup |
| Mask filters | none |
| Pre-momentum gates | none |
| Entry guards | none |
| Exit | SL 1.20 / Target 1.50 |

## Baseline Metrics

| Period | Trades | Win % | Gross profit | Gross loss | Net PnL | PF | Avg win | Avg loss | Max DD | Day block p | Outcomes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| TRAIN | 236 | 44.07 | 92,861 | 120,437 | -27,576 | 0.771 | 893 | -912 | -57,996 | 0.7489 | EOD 121, SL 61, TARGET 54 |
| TEST | 101 | 33.66 | 26,210 | 65,283 | -39,073 | 0.402 | 771 | -974 | -39,365 | 0.9394 | EOD 55, SL 32, TARGET 14 |

## Day-Wise Results

TRAIN:

| Day | Trades | Net PnL |
|---|---:|---:|
| 2026-05-25 | 29 | -11,224 |
| 2026-05-26 | 28 | 14,226 |
| 2026-05-27 | 28 | -724 |
| 2026-05-29 | 26 | 22,127 |
| 2026-06-01 | 30 | 1,477 |
| 2026-06-02 | 16 | -19,260 |
| 2026-06-03 | 32 | -23,880 |
| 2026-06-04 | 21 | -6,023 |
| 2026-06-05 | 26 | -4,294 |

TEST:

| Day | Trades | Net PnL |
|---|---:|---:|
| 2026-06-08 | 22 | 292 |
| 2026-06-09 | 30 | -23,203 |
| 2026-06-10 | 23 | 1,773 |
| 2026-06-12 | 26 | -17,935 |

## Failure Modes

Observed from TRAIN losers and TEST collapse:

- Bad time / chop: losses cluster by day more than by a single intraday slot; simple morning/afternoon caps did not fix it.
- Trend against trade: market/RS filters did not separate winners from losers; loss/win medians were similar.
- Fake break / poor follow-through: high EOD share (TRAIN 51.3%, TEST 54.5%) shows many breaks did not resolve cleanly to target.
- Volatility noise: ATR and VWAP-distance filters improved TRAIN only slightly, then collapsed on TEST.
- Ambitious target and tight target alternatives both failed; exit-only changes reduced PF further.
- Low robustness: baseline TEST was worse than TRAIN, not just noisy.

Full symbol-wise detail is in `A_PULLBACK_C2_THEN_BREAK_C2_LOW_loop_details.json` iteration `0`.
