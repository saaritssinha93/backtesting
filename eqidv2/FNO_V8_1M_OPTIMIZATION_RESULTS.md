# FNO V8 1-Minute Entry Optimization Results

Generated: 2026-08-19 IST  
Optimizer: `FNO_V8_SIDE_ENTRY_OPTIMIZER_20260819_V1`  
Result: **NO QUALIFYING CONFIGURATION**

## Executive verdict

The requested target was **not achieved honestly**.

- 192 LONG and 192 SHORT one-minute configurations were evaluated: **384 trials**, representing 186 distinct execution behaviours per side.
- The objective was to maximize closed trades per official session while retaining the frozen V8 five-minute setup book, exits, ranking, caps and portfolio rules.
- **0/192 LONG** configurations passed every preregistered training robustness guard.
- **25/192 SHORT** configurations passed the side guard; the optimizer retained eight unique frontier representatives.
- No LONG/SHORT pair could therefore be formed. No pair was scored on validation and the retrospective TEST/STRESS outcomes were not opened.
- Some LONG trials reached PF above 1.5 and increased frequency, but their profits were too concentrated in one day/block. No SHORT configuration with at least 15 fills reached PF 1.5.
- No configuration is approved for live use, promotion, or a claim of validated PF >= 1.5.

This is the correct fail-closed result. Loosening the rules after seeing these outcomes would turn the exercise into curve fitting.

## Data scope and honesty limits

The full 208-stock historical source is not complete enough for honest full-universe optimization:

| Full-source coverage | Count |
|---|---:|
| Expected symbol-sessions | 11,856 |
| Complete symbol-sessions | 6,350 |
| Incomplete symbol-sessions | 5,506 |
| Complete coverage | 53.56% |

The optimizer therefore used an explicitly diagnostic, fixed rectangular panel selected from **TRAIN source availability only**. Later coverage was used only as a pass/fail check and did not alter panel membership.

| Split | Dates | Sessions | Frozen symbols | Complete / expected | Outcome access |
|---|---|---:|---:|---:|---|
| TRAIN | 2026-07-13 to 2026-07-22 | 8 | 132 | 1,056 / 1,056 | Grid scored |
| VALIDATION | 2026-07-23 to 2026-07-27 | 3 | 132 | 396 / 396 | Not scored: no eligible LONG frontier |
| TEST/STRESS | 2026-07-28 to 2026-07-31 | 4 | 132 | 528 / 528 | **Not accessed** |

Watermark:

> FROZEN_RECTANGULAR_SOURCE_COMPLETE_SYMBOL_PANEL; PANEL_DERIVED_ONLY_FROM_SOURCE_AVAILABILITY; STATIC_LATER_DATED_UNIVERSE_AND_STATIC_AUGUST_OI; TINY_ALREADY_INSPECTED_RETROSPECTIVE_SAMPLE; DIAGNOSTIC_ONLY

Even a qualifying result from this panel would still require prospective confirmation. The universe and August futures-OI contract history are later-dated/static, and the 8/3/4-session split is too small for a promotion claim.

## What was optimized

Only the one-minute entry seam changed. The V8 five-minute candidate layer, setup-specific five-minute thresholds, pickers, caps, stop percentages and target percentages remained fixed.

Each side tested this 192-point grid:

| Parameter | Values |
|---|---|
| Latest confirmation candle | S+1, S+2, S+3, S+4 |
| Directional breakout buffer | 0, 2, 5 bps |
| Midpoint pre-confirmation invalidation | Off, on |
| Directional close-location minimum | None, 0.75 |
| Confirmation morphology | STRICT, MODERATE, RELAXED, DIRECTIONAL_ONLY |

Morphology meanings:

| Preset | Setup body threshold | Setup adverse-wick allowance | Direction and close beyond 5m close |
|---|---:|---:|---|
| STRICT | 100% of V8/V6 threshold | +0.00 | Required |
| MODERATE | 75% | +0.10 | Required |
| RELAXED | 50% | +0.20 | Required |
| DIRECTIONAL_ONLY | Disabled | Disabled | Required |

`DIRECTIONAL_ONLY` is not the weak V7 any-candle rule. It still requires the correct one-minute candle direction and a close beyond the five-minute signal close.

## Frozen execution and economics

- Confirmation can occur only through S+4.
- Entry is a later one-minute high breakout for LONG or low breakdown for SHORT, with the selected directional buffer and tick rounding.
- Entry order expires at S+5.
- Post-confirmation close reversal cancels the pending entry.
- Gap-through entry uses the adverse bar open; brackets are recomputed from the actual modeled fill.
- Same-bar ambiguity is stop-first.
- Paths are exact, consecutive, same-session paths with exact 15:30 square-off.
- LONG and SHORT results are combined through one global V8 portfolio ledger; the independent side search cannot bypass final duplicate/capital/concurrency constraints.
- Selection economics: **15 bps round-trip cost + 1 bp adverse entry slippage**.
- Severe stress, if a pair qualifies: **20 bps cost + 2 bps entry slippage**.
- Rupee P&L below is the V8 cash-equity sizing proxy using approximately Rs 50,000 exposure per filled entry, not account-level portfolio return.

## Preregistered qualification rules

### Side training guard

Each LONG and SHORT configuration had to satisfy all of these on TRAIN:

| Guard | Required |
|---|---:|
| Closed fills | >= 15 |
| Active trading days | >= 4 |
| Profit factor | >= 1.10 |
| PF after removing best day | >= 1.00 |
| Largest positive-day contribution | <= 50% |
| Positive contiguous time blocks | >= 2 |
| Incomplete candidates / unresolved fills | 0 / 0 |

### Pair and validation guard

A surviving LONG/SHORT pair would then need:

| Guard | TRAIN | VALIDATION |
|---|---:|---:|
| Combined closed fills | >= 40 | >= 16 |
| Combined PF | >= 1.50 | >= 1.50 |
| Robust PF, concentration and block tests | Pass | Pass |
| Severe-stress PF and net | PF >= 1 and net positive | PF >= 1 and net positive |
| Both individual sides | Positive net and PF >= 1 | Positive net and PF >= 1 |

Only after those gates could one frozen pair be evaluated once on TEST/STRESS.

## Search funnel

| Funnel stage | LONG | SHORT |
|---|---:|---:|
| Raw configurations | 192 | 192 |
| Distinct behaviours | 186 | 186 |
| Fills >= 15 | 13 | 184 |
| Active days >= 4 | 192 | 192 |
| Training PF >= 1.10 | 182 | 83 |
| Robust PF excluding best day >= 1.00 | 81 | 25 |
| Top-day share <= 50% | 17 | 192 |
| At least two positive contiguous blocks | **0** | 170 |
| Passed every side guard | **0** | **25** |
| Fills >= 15 and PF >= 1.50 | 6 | **0** |

The decisive LONG failure was time stability: none of the 192 configurations produced at least two positive contiguous blocks. The decisive SHORT limitation was quality: no sufficiently active SHORT configuration reached PF 1.5.

## B0 baseline on TRAIN

B0 uses S+1 confirmation, zero buffer, no midpoint invalidation, no close-location threshold and STRICT morphology.

| Side | Config hash | Candidates | Closed fills | Trades/day | PF | Robust PF | Net %-points | P&L proxy | W/L | Top-day share | Positive blocks | Result |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| LONG | `5d2abcfd7c3d` | 54 | 11 | 1.375 | 1.207 | 0.517 | +1.275 | +Rs 699.69 | 3/8 | 69.92% | 1 | Fail |
| SHORT | `5d2abcfd7c3d` | 126 | 18 | 2.250 | 1.278 | 1.063 | +2.401 | +Rs 1,359.68 | 8/10 | 35.34% | 2 | Side guard pass; PF below 1.5 |

## Best LONG training-only near-misses

These are diagnostic hypotheses, not selected configurations.

| Purpose | Hash | Confirmation | Buffer | Midpoint | CLV | Morphology | Fills | Trades/day | PF | Robust PF | Net %-points | P&L proxy | Top-day | Blocks | Failure |
|---|---|---:|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Best PF with required fills | `23a585ee8196` | S+1 | 0 bps | Off | None | RELAXED | 15 | 1.875 | **2.291** | 1.332 | +8.151 | +Rs 4,115.62 | 50.42% | 1 | Concentration narrowly over limit; only one positive block |
| Highest trade count | `33683c847f6e` | S+3 | 0 bps | Off | None | MODERATE | 17 | **2.125** | 1.212 | 0.602 | +1.963 | +Rs 1,044.33 | 84.72% | 1 | Weak after best day; concentrated; one block |
| Highest PF with top-day <= 50% | `bada1d5f8d26` | S+3 | 2 bps | Off | 0.75 | RELAXED | 13 | 1.625 | **2.419** | 1.524 | +7.951 | +Rs 4,018.07 | 46.25% | 1 | Too few fills; one positive block |

The `23a...` LONG trial is the closest quality/frequency hypothesis, but its headline is supported by one profitable time block and just exceeds the concentration ceiling. It must not be called validated.

## Best SHORT training-only near-misses

| Purpose | Hash | Confirmation | Buffer | Midpoint | CLV | Morphology | Fills | Trades/day | PF | Robust PF | Net %-points | P&L proxy | Top-day | Blocks | Failure |
|---|---|---:|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| Best frequency among side-guard passers | `50eacd6275b9` | S+4 | 5 bps | Off | 0.75 | MODERATE | 19 | **2.375** | 1.378 | 1.200 | +3.250 | +Rs 1,798.66 | 39.07% | 2 | PF below 1.5 |
| Highest PF with required fills | `30b38ec8511a` | S+1 | 5 bps | Off | None | STRICT | 15 | 1.875 | **1.441** | 1.238 | +3.062 | +Rs 1,714.42 | 32.65% | 3 | PF below 1.5 |
| Highest raw trade count | `8ff8a569f117` | S+4 | 0 bps | Off | 0.75 | MODERATE | 25 | **3.125** | 1.154 | 0.981 | +1.950 | +Rs 1,228.40 | 41.79% | 2 | Robust PF below 1.0; PF below 1.5 |

Increasing SHORT frequency to 3.125 trades/day materially diluted quality. The more balanced SHORT hypothesis is `50e...`, while `30b...` is closest to PF 1.5. Neither meets the requested PF threshold.

## Validation and TEST result

| Stage | Status | Reason |
|---|---|---|
| TRAIN grid | Completed | 384 configurations evaluated |
| TRAIN side qualification | Failed | No LONG configuration passed every robustness guard |
| VALIDATION pair evaluation | Not run | No eligible LONG/SHORT pair existed |
| Retrospective TEST/STRESS | **Sealed / not accessed** | No qualifying frozen pair existed |
| Promotion | Not eligible | No honest PF >= 1.5 configuration was established |

Reporting validation or TEST numbers for one of the attractive training near-misses now would use the holdout to choose a model and would invalidate the honest split.

## Recommended next steps

1. **Do not loosen the guards or run TEST on these training near-misses.** That would be post-result selection.
2. Repair/backfill the missing historical symbol-sessions and rebuild a complete full-universe V8 cache. The present full source is only 53.56% complete.
3. Freeze a new optimizer family before seeing the repaired outcomes. Keep the tried-family registry so multiple testing is visible.
4. Use more training sessions. Eight TRAIN days cannot establish stable LONG behaviour, even when raw PF looks high.
5. If historical repair is delayed, shadow `23a...` LONG and either `50e...` or `30b...` SHORT prospectively without live orders. Do not modify their parameters during collection.
6. Require at least **20 new complete prospective sessions and 100 filled orders**, whichever takes longer, before considering promotion. Apply the same 15/20 bps economics and global portfolio ledger.

## Reproducibility and artifact verification

Search run:

`C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1\optimizer_runs\fno_v8_entry_search_20260819T132331893522+0530_e2700806a17c`

Important artifacts:

| Artifact | Location / identity |
|---|---|
| Frozen selection | `selection.json` in the search run |
| Search report | `report.md` in the search run |
| All LONG trials | `train_long_trials.csv` in the search run |
| All SHORT trials | `train_short_trials.csv` in the search run |
| Provenance | `provenance.json` in the search run |
| Optimizer SHA-256 | `b69dc5977403c63b610c1facf94ec740718e1fc5b08205494f255dd98dd569f5` |
| V8 engine SHA-256 | `992596aa13095ec15625268237ec6109b354070a5fb396e766217e00365a94bb` |
| Search fingerprint | `e2700806a17cf68ba709cc634e5ee4f9210199fc279b42b0bab59d2707bd110b` |
| Panel hash | `5fdfe70cc295b39f76a8130cb6b6443d31bb126d9f68f5bd9659d5c658da948b` |
| Panel-symbol-set SHA-256 | `77fe46b7fe578cdd9060de8f70bbc624c8e41f4991a4ddafe5888d75a24244e4` |

Verification completed:

- Authenticated `selection.json` against immutable provenance.
- All seven provenance-listed output files match their recorded size and SHA-256.
- Broad V8 cache and derived panel candidate/path artifacts match their frozen hashes.
- Search fingerprint independently recomputes exactly.
- No retrospective test claim/evaluation directory exists.
- Maintained test suite: **803 passed, 213 subtests passed**.

## Bottom line

V8 can be made more active, but this dataset does not support the requested statement that the higher-frequency one-minute configurations retain honest PF >= 1.5. The defensible conclusion is **no qualifying configuration**, with the named LONG and SHORT rows retained only as prospective research hypotheses.
