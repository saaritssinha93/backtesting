# FNO V13-v2: 5-minute selection and 1-minute entry research audit

Generated: 2026-09-03  
Decision scope: research only; `fno_v13_corrected_v2_backtest.py` was not edited.

## 1. Decision — passed results first

No newly tested rule passed the promotion screen. The correct action is to make **no change** to V13-v2 at this stage.

| Evidence block | Result | Decision |
|---|---|---|
| Source integrity | PASS: V6 and V13-v2 SHA-256 hashes remained unchanged | The research did not alter either protected strategy |
| Direct V13-v2 replay | PASS: 72 orders and returns matched the published V13-v2 output to `1e-12` | Research engine parity is established |
| Selected-row OI audit | PASS: all 80 unique selected signal rows had finite positive OI pairs and exact same-contract, same-session `t-5m` matching | Selected trades use valid five-minute OI changes |
| Existing V13-v2 tests | PASS: 39 tests plus 26 subtests | No regression detected |
| 213 bounded parameter variants | **0 passed** | Do not change current 5m/1m parameters |
| 09:50 LONG | REJECT | Do not add |
| 09:50 SHORT | REJECT for all three declared pickers | Do not add |
| 09:55 SHORT | REJECT for all three declared pickers | Do not add |
| Wider 1-minute confirmation window (`S+1` to `S+2..S+4`) | REJECT | Keep the single next-minute confirmation |
| Existing 09:55 LONG | Positive retrospectively, but fails the strict sample/stability screen | Keep experimental only; do not call it validated |

The result is not “nothing works.” It is more specific: the current causal structure is sensible, the positive-OI gate is useful, but none of the newly tested ways to loosen, tighten, rerank, increase entries, alter brackets, shift the entry trigger, or add the omitted late slots has enough stable evidence to improve the strategy.

## 2. Frozen comparator and sample

At 5 bps round-trip cost over all 24 available eligible sessions (2026-07-29 through 2026-09-02):

| Policy | Fills | PF | Net | Max drawdown | Status |
|---|---:|---:|---:|---:|---|
| Corrected V6 parity | 66 | 2.043 | +27.081% | -5.601% | Validated comparator |
| V13-v2 combined shadow | 72 | 2.357 | +34.622% | -3.581% | Retrospectively stronger, not independently validated |

At 20 bps, V6 remains positive at PF 1.549 / +17.181%, while V13-v2 remains positive at PF 1.776 / +23.822%.

V13-v2 differs from corrected V6 by:

- changing 09:35 LONG minimum five-minute OI change from 0.10% to 0.15%;
- changing 09:40 LONG minimum five-minute OI change from 0.10% to 0.075%;
- applying a policy-wide maximum OI change of 1.00% before ranking; and
- adding a 09:55 LONG / 09:56 confirmation setup.

These are still explicitly labelled `V13_V2_COMBINED_SHADOW`; the stronger retrospective numbers do not convert them into independent validation.

For the new 213-variant screen, September 2 was excluded from selection and the frozen sample was:

- TRAIN: 12 sessions, 37 fills, PF 2.595, +19.953%;
- chronological TEST: 11 sessions, 33 fills, PF 2.480, +16.519%;
- frozen total through September 1: 23 sessions, 70 fills, PF 2.541, +36.472%.

September 2 contributed two losing baseline trades and no omitted late-slot trades. It was not used to choose a candidate.

## 3. Exact current setup book

Price thresholds below are signed by side: LONG requires at least `+x%`; SHORT requires at most `-x%`. OI must rise for both directions.

| 5m signal | 1m confirm | Side | Daily cap | Picker | Price move | OI change | 5m volume ratio | 1m body | Adverse wick max | Stop / target |
|---|---|---|---:|---|---:|---:|---:|---:|---:|---|
| 09:25 | 09:26 | LONG | 1 | liquidity | +0.30% | 0.10% | 3.0x | 0.60 | 0.50 | 0.50% / 3.00% |
| 09:25 | 09:26 | SHORT | 2 | volume | -0.20% | 0.10% | 1.5x | 0.40 | 0.50 | 0.75% / 3.00% |
| 09:30 | 09:31 | LONG | 1 | move | +0.65% | 0.10% | 1.0x | 0.50 | 0.50 | 1.00% / 2.50% |
| 09:30 | 09:31 | SHORT | 1 | move | -0.20% | 0.25% | 1.0x | 0.40 | 0.50 | 1.00% / 3.00% |
| 09:35 | 09:36 | LONG | 1 | liquidity | +0.20% | **0.15%** | 1.0x | 0.60 | 0.50 | 1.00% / 2.50% |
| 09:35 | 09:36 | SHORT | 2 | liquidity | -0.50% | 1.00% | 1.0x | 0.40 | 0.50 | 1.00% / 3.00% |
| 09:40 | 09:41 | LONG | 1 | liquidity | +0.20% | **0.075%** | 2.0x | 0.50 | 0.50 | 0.50% / 2.50% |
| 09:40 | 09:41 | SHORT | 1 | move | -0.20% | 0.10% | 1.0x | 0.40 | 0.50 | 1.00% / 3.00% |
| 09:45 | 09:46 | LONG | 1 | move | +0.65% | 0.10% | 1.0x | 0.40 | 0.50 | 1.00% / 3.00% |
| 09:45 | 09:46 | SHORT | 1 | volume | -0.20% | 0.75% | 1.0x | 0.40 | 0.30 | 1.00% / 2.00% |
| 09:55 | 09:56 | LONG | 1 | liquidity | +0.20% | 0.10% | 1.0x | 0.40 | 0.50 | 1.00% / 3.00% |

The exact values are not all consequences of market theory; many are historically selected parameters. Their *directions* are economically coherent, but an isolated value such as 0.65%, 0.075%, or a 0.30 wick cap should be treated as an empirical hypothesis, not a natural law.

## 4. Is the trading logic coherent?

Yes, as a momentum/positioning continuation strategy. The causal sequence is:

1. Use the nearest unexpired stored stock-futures contract for OI and the cash equity for price, volume, indicators, confirmation, entry, and exit.
2. Construct a completed five-minute cash candle from five real one-minute bars.
3. Require EMA9 > EMA20 > EMA50 for LONG, or EMA9 < EMA20 < EMA50 for SHORT.
4. Require a same-direction five-minute cash-price impulse.
5. Require rising near-month futures OI. Price up + OI up represents long buildup; price down + OI up represents short buildup.
6. Require five-minute relative volume, then rank eligible candidates by the setup’s declared picker.
7. On the immediately following completed one-minute candle, require directional colour and a close beyond the five-minute signal close.
8. Reject a weak body or an adverse rejection wick. For LONG the adverse wick is the upper wick; for SHORT it is the lower wick.
9. Enter only if a later one-minute bar crosses the confirmation high for LONG or confirmation low for SHORT.
10. Apply the setup’s fixed stop and target, pessimistically choose the stop if both are touched in one bar, and square off in the same session.

### Layer-by-layer assessment

| Layer | Logical role | Assessment |
|---|---|---|
| Rolling near-month contract | Prevents historical OI from being read from the wrong expiry | Strong and necessary |
| EMA stack | Establishes short/medium trend direction | Sensible; exact lengths and overnight-continuous calculation are not independently proven |
| Five-minute price impulse | Requires current momentum, not merely old EMA alignment | Sensible; fixed percentages are not volatility-normalized |
| Positive OI change | Confirms new positioning rather than price movement alone | Strongly supported by the corrected ablation |
| Relative volume | Confirms participation and liquidity | Sensible, but the current prior-20-bar denominator mixes time-of-day regimes |
| One-minute direction/close | Confirms continuation after the completed five-minute signal | Causal and sensible |
| Body and adverse wick | Rejects indecision/rejection candles | Sensible, but exact slot-specific cutoffs are likely sample-sensitive |
| Picker and entry cap | Controls daily candidate competition | Operationally sensible; current raw metrics are not sector/capital aware |
| Breakout stop-entry | Avoids entering before price proves continuation | Causal, but the simulated fill price needs a gap-aware realism audit |
| Fixed bracket | Gives deterministic intraday risk | Simple and usable, but not scaled to each stock’s volatility |

## 5. Is OI useful, and can more advanced OI logic improve it?

The corrected ungated ablation answers the first question clearly: **OI is useful**. Removing it adds trades but destroys most of the edge.

| OI policy | Fills | PF | Net | Interpretation |
|---|---:|---:|---:|---|
| No OI gate | 142 | 1.022 | +1.538% | Many more trades, almost no edge |
| Positive buildup only | 113 | 1.196 | +10.434% | Directionally useful, still much weaker |
| Current V6 slot-specific OI thresholds | 64 | 2.200 | +28.931% | Best tested balance |
| Two consecutive positive OI bars | 62 | 1.079 | +2.519% | Persistence damages results |
| V6 plus prior positive bar | 32 | 1.728 | +10.150% | Too restrictive and lower net |
| Positive ten-minute OI change | 76 | 1.296 | +10.810% | More trades, substantially weaker PF/net |
| Falling-OI mirror logic | 75 | 0.531 | -19.425% | Clearly invalid for this strategy |

Cross-sectional OI ranks, OI/volume positioning share, persistence, and V6 unions/intersections were also studied previously; none beat current V6 in both chronological segments. A V6-and-top-half-OI-rank rule changed no trades because the absolute thresholds already selected relatively high OI changes.

The 1.00% V13-v2 OI cap is an experimental outlier guard, not a universal market principle. In the new neighbour test:

- cap 0.95% changed no decisions;
- cap 0.90% removed one TEST trade and reduced frozen net by 2.95%;
- caps 1.05% and 1.10% admitted one TEST trade and reduced frozen net by 0.834%;
- no cap neighbour passed.

Therefore the honest OI conclusion is: retain positive, slot-specific five-minute OI as the core; do not add persistence, ten-minute aggregation, falling OI, or a rank overlay. Gather new expiries before treating the V13-v2 cap or its exact boundary as validated.

## 6. The 213-variant parameter screen

The screen changed one item at a time so that any effect remained attributable. It covered:

| Family | Variants |
|---|---:|
| Per-setup price/OI/volume/body/wick neighbours | 110 |
| Per-setup stop/target neighbours | 44 |
| Per-setup alternative rankers | 33 |
| Per-setup maximum-entry increases | 11 |
| Global one-minute confirmation quality filters | 8 |
| Global OI-cap neighbours | 4 |
| Breakout-entry buffers | 3 |
| **Total** | **213** |

Candidates were ranked on TRAIN net change only. A survivor then had to satisfy all of these protections:

- at least one additional TRAIN fill and at least two additional fills overall;
- TRAIN net improvement of at least +0.25%, PF improvement of at least +0.10, drawdown worsening no greater than 0.10%, and positive incremental net at 10 bps;
- the same +0.25% net / +0.10 PF confirmation on the untouched chronological TEST with no worse drawdown and positive 10-bps incremental net;
- support from a separate neighbouring setting in the same parameter family;
- an exact paired 12-session TRAIN sign-flip test passing Benjamini-Hochberg FDR at q <= 0.10 across all 213 variants.

Result: **0 of 213 survived**. Only one variant passed the initial TRAIN screen, and none passed the multiplicity control.

### The apparent TRAIN winner and why it fails

Lowering 09:25 SHORT price movement from 0.20% to 0.10% was the top TRAIN-ranked result:

| Segment | Extra fills | PF change | Net change |
|---|---:|---:|---:|
| TRAIN | +1 | +0.157 | +1.959% |
| TEST | +2 | **-0.085** | only +0.168% |
| Frozen total | +3 | — | +2.127% |

This is the kind of full-sample result that looks attractive but is not reliable. Its exact TRAIN sign-flip p-value was 0.50, BH-adjusted q-value was 1.00, no neighbouring threshold supported it, and the TEST PF deteriorated. It is rejected.

Other examples reinforce the same point:

- 09:35 LONG price threshold 0.10% added three total fills, but reduced PF in both TRAIN and TEST and lost 0.112% in TEST.
- 09:45 LONG volume ratio 0.80x added one profitable TRAIN fill and changed nothing in TEST: too little independent evidence.
- 09:45 LONG wick cap 0.60 added one profitable TRAIN fill and changed nothing in TEST: the same one-trade illusion.
- Tightening the 09:55 LONG stop from 1.00% to 0.75% added +0.50% in each segment but added no trades, had only p=0.25 / q=1.00 evidence, and fails the user’s dual objective.
- Increasing the per-setup entry cap produced zero or negative frozen net in every setup. The largest count gain, 09:40 LONG cap 1 to 2, added four TRAIN fills but lost 2.20% and reduced PF by 0.388.
- Adding a 0.05% breakout buffer reduced frozen net by 8.986%; a 0.10% buffer reduced it by 8.723%.
- Stronger global confirmation-body, wick, or displacement filters mostly deleted profitable trades. None passed.

## 7. One-minute confirmation timing

The immediate next-minute candle is not arbitrary: widening the confirmation search increases count but materially dilutes quality. This experiment used the corrected-V6 setup book, with `S+1` reproducing corrected V6 exactly.

| Maximum confirmation step | Fills | TRAIN PF | TEST PF | ALL PF | ALL net |
|---|---:|---:|---:|---:|---:|
| S+1 | 66 | 2.041 | 2.045 | 2.043 | +27.08% |
| S+2 | 90 | 1.580 | 1.279 | 1.442 | +18.56% |
| S+3 | 101 | 1.461 | 1.531 | 1.492 | +22.73% |
| S+4 | 112 | 1.413 | 1.325 | 1.372 | +19.56% |

The wider window does not merely append trades. Late-confirming candidates also compete with and displace some S+1 candidates under the daily picker caps. This is why more fills do not translate into more profit. Keep S+1.

## 8. Why 09:50 LONG, 09:50 SHORT, and 09:55 SHORT are not included

They have now been tested directly against the V13-v2 cache. The prior omission of the SHORT legs was a research gap; it is now closed.

### Standalone 5-bps results under the current 1.00% OI-cap context

| Leg | TRAIN fills / PF / net | TEST fills / PF / net | ALL fills / PF / net | Decision |
|---|---|---|---|---|
| 09:50 LONG modal | 5 / 1.114 / +0.104% | 5 / 0.000 / -3.888% | 10 / 0.212 / -3.784% | Reject decisively |
| Existing 09:55 LONG | 5 / 1.993 / +2.084% | 5 / 1.740 / +1.553% | 10 / 1.866 / +3.637% | Positive but too small to validate |
| 09:50 SHORT, max-volume | 2 / 0.000 / -2.100% | 3 / inf / +2.277% | 5 / 1.084 / +0.177% | Reject |
| 09:50 SHORT, max-move | 2 / 0.064 / -0.983% | 3 / inf / +2.277% | 5 / 2.232 / +1.294% | Reject |
| 09:50 SHORT, max-liquidity | 2 / 0.543 / -0.480% | 3 / inf / +2.277% | 5 / 2.712 / +1.797% | Reject |
| 09:55 SHORT, every declared picker | 2 / inf / +2.354% | 5 / 0.663 / -1.205% | 7 / 1.322 / +1.149% | Reject |

The apparent 09:50 SHORT max-liquidity PF of 2.712 is only five fills and loses in TRAIN. The 09:55 SHORT reverses from two TRAIN wins to a losing five-trade TEST. At 20 bps, 09:55 SHORT falls to only +0.099% / PF 1.024. These are exactly the unstable, small-sample shapes that should not be promoted.

Appending each leg to the current V13-v2 book does not rescue the evidence:

| Book | Fills | PF | Net | 20-bps PF / net |
|---|---:|---:|---:|---:|
| Current V13-v2 | 72 | 2.357 | +34.622% | 1.776 / +23.822% |
| +09:50 LONG | 82 | 2.017 | +30.838% | 1.505 / +18.538% |
| +09:50 SHORT max-liquidity | 77 | 2.371 | +36.420% | 1.780 / +24.870% |
| +09:55 SHORT | 79 | 2.230 | +35.771% | 1.686 / +23.921% |

The 09:50 SHORT max-liquidity combined row is superficially the best. It is still rejected because its whole apparent benefit comes from five standalone trades, it loses in TRAIN, and its picker was not allowed to be selected from full-sample P&L.

## 9. Important backtest realism issues

These matter more than inventing another indicator because each can make a profitable-looking backtest less executable.

### 9.1 Gap-through entry price

The engine fills a touched stop-entry at the recorded trigger. If the next one-minute bar opens above a LONG buy-stop or below a SHORT sell-stop, a live fill would normally be at the worse open/available price, not at the trigger. The current path cache does not retain the forward opens needed to model this.

### 9.2 Unknown intrabar ordering

After a bar touches the entry trigger, the same OHLC bar is eligible to touch the stop or target. One-minute OHLC cannot reveal whether the high/low occurred before or after entry. Stop-first handling is conservative when both exit levels are touched, but it cannot solve pre-entry versus post-entry ordering.

### 9.3 Candidate-level OI continuity

V13-v2 validates every selected row against an exact raw five-minute predecessor and all 80 audited selected rows passed. The initial candidate builder, however, computes prior OI with a row shift. Exact same-session, same-contract `t-5m` continuity should become a build-time invariant for every candidate, not only a selected-row audit.

### 9.4 One-minute source quality

The strict-V6 confirmation path does not explicitly fail closed on every flagged/stale one-minute source condition, although the newer V7 path contains such checks. The exact confirmation candle should be held to the same completeness rule as the five-minute construction.

### 9.5 Time-of-day volume bias

The current volume ratio divides the current five-minute volume by the previous 20 five-minute bars. Early-morning bars can therefore be compared partly with prior-session afternoon bars. A same-clock-slot historical RVOL feature is theoretically cleaner.

### 9.6 Portfolio realism

Reported returns add trade percentages. They are not lot-sized, margin-constrained, sector-capped, or capital-constrained portfolio returns. Overlapping entries and multiple signals in the same stock can make a higher trade count look more deployable than it is.

## 10. What can theoretically improve both profit and trade count?

It is possible, but the completed evidence says it is unlikely to come from simply lowering thresholds or accepting later confirmations. The most credible next hypotheses are structural:

1. **Execution-realistic replay first.** Rebuild forward paths with one-minute opens and test optimistic/base/conservative intrabar bounds. An “improvement” must survive this before parameter work matters.
2. **Same-slot RVOL.** Compare 09:25 with prior 09:25 bars, 09:30 with prior 09:30 bars, and so on. This can reject false morning volume without imposing an arbitrary blanket threshold.
3. **Volatility-normalized impulse and risk.** Express price impulse and stop/target distances in ATR or recent five-minute range units. This may admit valid lower-percentage moves in quiet stocks while rejecting the same percentage in noisy stocks.
4. **Fallback ranking, not a blind second entry.** If the top-ranked candidate never triggers, allow a pre-ranked second candidate only under an explicit non-overlap/capital rule. Directly raising `max_entries` failed; a causal fallback is a different hypothesis and needs a fresh path-aware test.
5. **Composite, normalized candidate score.** A train-frozen score using percentile-normalized move, RVOL, OI, and liquidity may be less dominated by raw stock price/volume than a single picker. Existing simple picker swaps failed, so this needs strong regularization and true walk-forward validation.
6. **Exact OI quality plus absolute-liquidity guard.** Large OI percentage changes on small denominators can be noisy. Test an absolute OI/notional OI floor while preserving the positive five-minute OI thesis; do not use full-sample P&L to choose its cutoff.

Market regime, breadth, sector conflict, EMA slope/separation, and opening-gap filters are also logical, but they normally improve quality by reducing trades. They do not directly satisfy the request for more trades unless paired with a genuinely broader opportunity set.

## 11. Proper next experiment

Create a separate V13-v3 research engine; keep V13-v2 frozen. The next engine should:

1. retain one-minute forward opens and exact timestamps;
2. enforce exact candidate-level OI continuity and stale-row rejection;
3. publish optimistic/base/conservative gap and intrabar fill bounds;
4. add same-slot RVOL and volatility-normalized impulse as separate, predeclared feature families;
5. test fallback ranking under capital/symbol/sector concurrency constraints;
6. select settings on rolling TRAIN windows only and evaluate each on the next unseen expiry;
7. require enough changed trades and active days, 10/15/20-bps resilience, daily bootstrap support, neighbourhood stability, and multiple-testing correction;
8. make no production edit without explicit approval.

The current sample contains only 24 sessions and two contract regimes. At least one additional untouched expiry is the minimum useful next checkpoint; roughly 40 or more total sessions across additional expiries would make the comparison materially more credible. This is a practical checkpoint, not a guarantee of statistical sufficiency.

## 12. Reproducibility and artifacts

Protected source hashes after all testing:

- `fno_v6_corrected_backtest.py`: `06BAF32C33156F21BCE1DC786E5687A250B9711A1BCA3A186283C824EDFCF62D`
- `fno_v13_corrected_v2_backtest.py`: `5368FD36A2B67CE9B2513D3D1AE5EC3201BAFF93E9A01DF25861C1DF085C8A9A`

Research code and primary outputs:

- `fno_v13_v2_parameter_robustness_research.py`
- `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v2_parameter_robustness\FNO_V13_V2_PARAMETER_ROBUSTNESS.md`
- `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v2_parameter_robustness\all_bounded_variants.csv`
- `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v2_parameter_robustness\strictly_passed_variants.csv`
- `fno_v13_corrected_v2_late_slot_research.py`
- `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v2_late_slot_research\FNO_V13_V2_LATE_SLOT_RESEARCH.md`
- `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v2_late_slot_research\late_slot_metrics.csv`
- `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v2_late_slot_research\late_slot_cost_stress.csv`
- `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_confirmation_window\confirmation_window_results.csv`
- `C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_ungated_oi_research\ungated_oi_ablation_report.md`

Final research decision: **no V13-v2 edit; no new late slot; keep the current file frozen and experimental while collecting genuinely unseen sessions.**
