# Train/Test Results - consolidated 2026-06-25

Source artifacts:
- `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\_manifest.json`
- `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\proposals\proposal_family_*.json`
- `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool\proposals\family_*_setup_summary.csv`

Pool coverage:
- Pool built UTC: 2026-06-22T14:53:10
- Candidate date range: 2025-06-02 10:30:00 to 2026-06-22 13:05:00
- Rows: 256,925 after de-dupe
- Families with generated train/test proposals: A, B, C, D, E, G, L

Evaluation window used by the generated proposals:
- TRAIN: 2026-03-01 to 2026-05-31
- TEST: 2026-06-01 to 2026-06-22
- Costs: net of the Train_and_Test cost model
- Gate encoded in proposal verdicts: train PF >= 2.0, test PF >= 1.5, test day_block_p <= 0.05, OOS/IS PF ratio >= 0.65, test trades >= 10

## Family Results

| Family | Train trades | Train PF | Train net Rs | Test trades | Test PF | Test net Rs | OOS/IS | day_block_p | Verdict |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| A | 218 | 1.844 | 43,329 | 50 | 0.618 | -8,893 | 0.33 | 0.9442 | REJECT |
| B | 279 | 2.226 | 68,296 | 18 | 0.310 | -6,223 | 0.14 | 1.0000 | REJECT |
| C | 38 | 1.451 | 4,182 | 8 | 0.435 | -2,212 | 0.30 | 0.9852 | REJECT |
| D | 133 | 2.334 | 47,188 | 13 | 1.145 | 832 | 0.49 | 0.2886 | REJECT |
| E | 250 | 2.075 | 75,596 | 43 | 0.551 | -10,674 | 0.27 | 0.8920 | REJECT |
| G | 221 | 2.225 | 66,133 | 59 | 0.766 | -5,474 | 0.34 | 0.7299 | REJECT |
| L | 176 | 1.853 | 48,741 | 17 | 0.377 | -4,720 | 0.20 | 0.9837 | REJECT |

## Result

0 of 7 generated family configs passed the out-of-sample gate.

The TRAIN side can still show PF near or above 2 in several families, but the TEST period does not confirm it. Most families lose money in TEST, and all families fail at least one robustness criterion. D is the closest family: it is marginally positive in TEST, but the PF, OOS/IS ratio, and day-block significance are all below the required gates.

## Individual TEST Callouts

These are not promotion recommendations by themselves because the family-level gate failed, but they are the only individual setups with positive TEST net/PF above 1.0 in the generated proposal files:

| Family | Setup | Side | TEST trades | TEST PF | TEST net Rs | Win % |
|---|---|---|---:|---:|---:|---:|
| D | D_EMA20_BOUNCE | LONG | 5 | 4.486 | 3,222 | 80.0 |
| A | A_PULLBACK_C2_THEN_BREAK_C2_HIGH | LONG | 13 | 1.799 | 2,281 | 53.8 |
| D | D_EMA20_REJECTION | SHORT | 3 | 1.508 | 343 | 66.7 |
| L | L_DOUBLE_BOTTOM_VWAP | LONG | 3 | 1.476 | 588 | 66.7 |
| G | G_HIGHER_HIGH_BREAK | LONG | 11 | 1.425 | 1,678 | 54.5 |

The strongest-looking individual rows are still sample-thin except A_PULLBACK_C2_THEN_BREAK_C2_HIGH and G_HIGHER_HIGH_BREAK. They should be treated as watchlist/research inputs, not approved config changes.

## Recommendation

Do not approve any of the generated retuned family configs into `final_setup_conf.py`.

Keep the current production config unchanged until a newer pool is rebuilt with post-2026-06-22 data and the Train_and_Test run is regenerated. The current pool is stale relative to 2026-06-25, so a fully current rolling test should first rebuild `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool`.

## Run Note

An attempted full current `train_test_conf.py` evaluation pinned to a 2026-06-22 pool end did not complete within the available command timeout. This report therefore consolidates the latest generated Train_and_Test proposal artifacts rather than claiming a fresh full-book evaluator run.

## Corrected Strategy Applied After Review

The June 22/25 evidence says the old search could manufacture high TRAIN PF but did not generalize. The Train_and_Test harness has therefore been corrected to prefer modest, trade-rich, harder-to-overfit proposals.

Code-side defaults now changed:
- `train_test_window.py`: default TEST window changed from 2 weeks to 4 weeks.
- `setup_train_test.py`: TRAIN PF target band changed from `1.50..2.00` to `1.40..1.70`.
- `setup_train_test.py`: minimum TRAIN trades changed from `15` to `50`.
- `setup_train_test.py`: minimum TEST trades changed from `8` to `20`.
- `setup_train_test.py`: OOS/IS PF ratio gate changed from `0.55` to `0.65`.
- `setup_train_test.py`: worse-half TRAIN PF floor changed from `1.00` to `1.10`.
- `setup_train_test.py`: per-setup search now excludes `market_ret_pct`, `market_abs_ret_pct`, `signal_minute`, and `notional` by default.
- `walk_forward.py`: matching stricter defaults were applied: 4-week folds, train PF `1.40..1.70`, `min_train_trades=50`, `minhalf_pf=1.10`, and the same default banned feature set.

New promotion bar:
1. Rebuild the unified pool first; do not promote from stale `date_max`.
2. Native setups must be judged on production-faithful live-gated candidates; readmit/tier-c setups may use raw only when raw is their live basis.
3. Prefer TEST PF `>=1.30` with adequate trades over TRAIN PF above 2.
4. Require OOS/IS PF ratio `>=0.65`, `day_block_p<=0.10`, and at least 20 TEST trades.
5. Require walk-forward confirmation before approval; single-window success is research-only.
6. Treat live paper as the final out-of-sample filter before sizing.

Research priority:
- Focus on slower, trend-confirmed G/D-style mechanisms with wider exits and momentum confirmation.
- Keep high-frequency/tight-scalp setups and thin high-PF pockets out of production unless they survive the stricter gate and live paper shadow.

## Corrected Priority Run - 2026-06-25

Run basis:
- Pool: `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool`
- Pool `date_max`: 2026-06-22 13:05:00
- TRAIN: 2026-02-26 to 2026-05-25
- TEST: 2026-05-26 to 2026-06-22
- Execution realism: statutory costs, 15 bps adverse slippage per leg, max positions 20, no daily loss stop, no regime overlay.
- Strict gates: train PF >= 1.40, target band 1.40..1.70, min train trades 50, min worse-half train PF 1.10, test PF >= 1.30, test trades >= 20, test/train PF ratio >= 0.65, day_block_p <= 0.10, BH-FDR alpha 0.10.
- Default banned per-setup search features: `market_ret_pct`, `market_abs_ret_pct`, `signal_minute`, `notional`.
- No `--approve` was used; `final_setup_conf.py` was not updated.

Why the TEST end is 2026-06-22, not 2026-06-25:
- The available unified pool ends at 2026-06-22 13:05:00, so a 2026-06-25 holdout would contain an artificial empty tail for June 23-25.
- This is the latest honest 4-week holdout possible from the current pool. A fully current run still requires rebuilding the unified pool with post-2026-06-22 data.

### Family Verdicts

| Family | Searched setups | Result |
|---|---|---|
| A | `A_PULLBACK_C2_THEN_BREAK_C2_HIGH` | REJECT: TRAIN candidate found, dropped by TEST/FDR |
| B | `B_HUGE_RED_FAILED_BOUNCE` | REJECT: no robust TRAIN edge |
| C | `C_OR_BREAKDOWN` | REJECT: no TRAIN edge |
| D | `D_AVWAP_LOSE_REVERSAL`, `D_EMA20_BOUNCE`, `D_EMA20_REJECTION` | REJECT: no setup survived robust TRAIN edge |
| E | `E_VWAP_LOSE_EARLY_SHORT` | REJECT: TRAIN candidate found, dropped by TEST/FDR |
| G | `G_HIGHER_HIGH_BREAK`, `G_LOWER_LOW_BREAK` | REJECT: TRAIN candidates found, both dropped by TEST/FDR |
| L | `L_DOUBLE_BOTTOM_VWAP`, `L_PRESSURE_BURST_VWAP` | REJECT: one TRAIN candidate found, dropped by TEST/FDR; one no-edge |
| P | `P_PDH_BREAK_RETEST_LONG` | REJECT: no robust TRAIN edge |
| S | `S_BB_SQUEEZE_SHORT`, `S_MACD_HIST_FLIP` | REJECT: one no-edge; one dropped by TEST/FDR with zero TEST rows |
| V | `V_RECLAIM_PULLBACK_LONG` | REJECT: no robust TRAIN edge |

Final corrected-run result: **0 of 10 tested families passed**.

### Setup-Level Evidence

| Family | Setup | Status | Train PF | Min-half PF | Train n | Train net Rs | TEST p |
|---|---|---|---:|---:|---:|---:|---:|
| A | `A_PULLBACK_C2_THEN_BREAK_C2_HIGH` | DROP_FDR | 1.439 | 1.177 | 80 | 14,126 | 0.8644 |
| B | `B_HUGE_RED_FAILED_BOUNCE` | DROP_NO_EDGE | 1.189 | 1.072 | 60 | 4,139 |  |
| C | `C_OR_BREAKDOWN` | DROP_NO_EDGE | 0.888 | 0.804 | 108 | -7,353 |  |
| D | `D_AVWAP_LOSE_REVERSAL` | DROP_NO_EDGE | 1.401 | 1.005 | 58 | 6,890 |  |
| D | `D_EMA20_BOUNCE` | DROP_NO_EDGE | 0.605 | 0.583 | 61 | -13,471 |  |
| D | `D_EMA20_REJECTION` | DROP_NO_EDGE | 0.634 | 0.433 | 55 | -10,338 |  |
| E | `E_VWAP_LOSE_EARLY_SHORT` | DROP_FDR | 1.472 | 1.223 | 50 | 6,395 | 0.9974 |
| G | `G_HIGHER_HIGH_BREAK` | DROP_FDR | 1.444 | 1.173 | 67 | 11,757 | 0.5574 |
| G | `G_LOWER_LOW_BREAK` | DROP_FDR | 1.538 | 1.357 | 56 | 8,960 | 0.8334 |
| L | `L_DOUBLE_BOTTOM_VWAP` | DROP_FDR | 1.954 | 1.384 | 61 | 15,507 | 1.0000 |
| L | `L_PRESSURE_BURST_VWAP` | DROP_NO_EDGE | 1.117 | 1.073 | 81 | 3,540 |  |
| P | `P_PDH_BREAK_RETEST_LONG` | DROP_NO_EDGE | 1.182 | 0.916 | 52 | 2,878 |  |
| S | `S_BB_SQUEEZE_SHORT` | DROP_NO_EDGE | 0.988 | 0.973 | 51 | -285 |  |
| S | `S_MACD_HIST_FLIP` | DROP_FDR | 1.542 | 1.345 | 102 | 12,816 | 1.0000 |
| V | `V_RECLAIM_PULLBACK_LONG` | DROP_NO_EDGE | 1.000 | 0.717 | 60 | -5 |  |

Interpretation:
- The corrected gate did what it was supposed to do: it separated train-only patterns from out-of-sample-confirmed patterns.
- Strong-looking TRAIN pockets in `A`, `E`, `G`, `L`, and `S_MACD_HIST_FLIP` did not survive TEST/FDR.
- `D_AVWAP_LOSE_REVERSAL` technically reached overall TRAIN PF 1.401, but worse-half PF was only 1.005, below the 1.10 robustness floor, so it was correctly treated as no-edge.
- `C`, `P`, `V`, and most `D` rows are not close enough even in TRAIN.
- `S` has no TEST rows in this holdout, so it cannot be promoted regardless of TRAIN.

### Promotion Decision

Do not promote any of these configs.

The honest action is to keep `final_setup_conf.py` unchanged, rebuild the unified pool through the latest available market date, and rerun the corrected gate. If the rebuilt pool still shows the same pattern, the next work should shift away from retuning these pockets and toward improving candidate generation: fewer raw/tier-c firehose candidates, better production-faithful setup definitions, and only then another train/test pass.

### Run Issue Fixed

During the `S` run, the harness hit an empty-TEST bug (`KeyError: 'tt_fill'`) because `S` had zero rows in the 2026-05-26..2026-06-22 holdout. `setup_train_test.py` now handles empty period dataframes cleanly, and `S` was rerun successfully after the fix.

## Immediate Next-Step Execution - G/D Focus

Follow-up run performed after the deeper audit recommendation:
- Rebuilt `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool` using both recent raw sources:
  - `C:\TradingData\eqidv2\outputs_ID_v11_unified_recent_raw`
  - `C:\TradingData\eqidv2\outputs_ID_v11_unified_recent_raw_0622`
- Rebuilt rows: 256,925 across 30 setups.
- Rebuilt basis mix: raw 249,319 rows; tier_c 7,606 rows.
- Latest available candidate timestamp remained `2026-06-22 13:05:00`; no source data beyond June 22 was available on disk.
- Therefore the honest evaluation window remained pinned to:
  - TRAIN: `2026-02-26..2026-05-25`
  - TEST: `2026-05-26..2026-06-22`

### Re-run: Family G

| Setup | Status | Train PF | Min-half PF | Train n | Train net Rs | TEST p | Selected filters |
|---|---|---:|---:|---:|---:|---:|---|
| `G_HIGHER_HIGH_BREAK` | DROP_FDR | 1.444 | 1.173 | 67 | 11,757 | 0.5574 | `upper_wick_pct>=0.17`, `body_pct>=0.5531` |
| `G_LOWER_LOW_BREAK` | DROP_FDR | 1.538 | 1.357 | 56 | 8,960 | 0.8334 | `vol_ratio>=4.0723`, `lower_wick_pct>=0.0683`, premom `pre3_range_r>=0.1494` |

G remains rejected. Both setups found train-side pockets, but neither survived BH-FDR on TEST.

### Re-run: Family D

| Setup | Status | Train PF | Min-half PF | Train n | Train net Rs | Selected filters |
|---|---|---:|---:|---:|---:|---|
| `D_AVWAP_LOSE_REVERSAL` | DROP_NO_EDGE | 1.401 | 1.005 | 58 | 6,890 | `atr_pct>=0.0038`, `vol_ratio<=1.7897`, premom `sig5_adx_calc>=22.9321` |
| `D_EMA20_BOUNCE` | DROP_NO_EDGE | 0.605 | 0.583 | 61 | -13,471 | `vol_ratio<=1.7224` |
| `D_EMA20_REJECTION` | DROP_NO_EDGE | 0.634 | 0.433 | 55 | -10,338 | `close_loc<=0.0` |

D remains rejected. `D_AVWAP_LOSE_REVERSAL` is closest, but its worse-half PF is only 1.005, below the 1.10 robustness floor.

### Current `final_setup_conf.py` Replay: G/D Only

This replay evaluated the current production config as-is, with no re-search and no approval.

| Setup | Basis note | Side | TRAIN n/PF/net Rs | TEST n/PF/net Rs |
|---|---|---|---:|---:|
| `D_EMA20_REJECTION` | native raw/ungated in this replay | SHORT | 22 / 0.42 / -6,085 | 2 / 0.00 / -1,957 |
| `G_HIGHER_HIGH_BREAK` | native raw/ungated in this replay | LONG | 35 / 1.14 / 2,385 | 3 / 0.46 / -951 |
| `G_LOWER_LOW_BREAK` | readmit/raw faithful | SHORT | 15 / 3.01 / 4,963 | 6 / 0.75 / -510 |
| G/D book, family-deduped | mixed | mixed | 71 / 1.03 / 1,006 | 11 / 0.41 / -3,418 |

Focused replay verdict:
- G/D current config does not pass.
- TEST PF is 0.41, OOS/IS PF ratio is 0.39, and TEST day-block p is 0.806.
- `G_LOWER_LOW_BREAK` still shows a high TRAIN PF, but only 15 TRAIN trades and 6 TEST trades; TEST PF is below 1.0.

Full-book `train_test_conf.py` was attempted, but the 12-setup replay exceeded a 20-minute command timeout. Partial output already showed several native/raw firehose configs losing heavily, so the useful actionable replay was narrowed to the G/D focus set above.

### Updated Action

No promotion. Keep `final_setup_conf.py` unchanged.

The next honest work is not more threshold tuning. It is:
1. Generate candidate data beyond 2026-06-22.
2. Rebuild the unified pool again after the fresh candidate source exists.
3. Rewrite candidate generation for the few worthwhile mechanisms, especially `G_HIGHER_HIGH_BREAK` and `D_AVWAP_LOSE_REVERSAL`.
4. Only after that, rerun train/test and walk-forward.

## Check-Now Execution - June 24 Source Folded In

Additional check found a newer usable source:
- `C:\TradingData\eqidv2\backtesting_result_v11\2026-06-24\live_parity_raw_candidates.csv`
- Rows: 1,309 raw candidates.
- Date coverage: `2026-06-24 12:35:00..2026-06-24 14:55:00`.

June 25 live entry-engine audit files existed, but the candidate CSVs were empty 2-byte files, so June 25 was not usable for honest train/test.

The June 24 raw candidates were staged as:
- `C:\TradingData\eqidv2\outputs_ID_v11_unified_recent_raw_0624\historical_all_available_raw_candidates.csv`

The unified pool was rebuilt with June 24 included:
- Rows: 258,100 across 30 setups.
- Basis mix: raw 250,494 rows; tier_c 7,606 rows.
- New pool `date_max`: `2026-06-24 14:30:00`.

Because the pool now ends on June 24, the updated honest 4-week window was pinned to:
- TRAIN: `2026-02-28..2026-05-27`
- TEST: `2026-05-28..2026-06-24`

### June 24-Inclusive Re-run: Family G

| Setup | Status | Train PF | Min-half PF | Train n | Train net Rs | TEST p | Selected filters |
|---|---|---:|---:|---:|---:|---:|---|
| `G_HIGHER_HIGH_BREAK` | DROP_FDR | 1.502 | 1.343 | 68 | 13,044 | 0.7170 | `upper_wick_pct>=0.1714`, `body_pct>=0.5531` |
| `G_LOWER_LOW_BREAK` | DROP_FDR | 1.421 | 1.252 | 74 | 9,295 | 0.8980 | `vol_ratio>=4.0906`, `lower_wick_pct>=0.0798` |

G remains rejected. Adding June 24 did not rescue the family; both setups again failed TEST/FDR.

### June 24-Inclusive Re-run: Family D

| Setup | Status | Train PF | Min-half PF | Train n | Train net Rs | Selected filters |
|---|---|---:|---:|---:|---:|---|
| `D_AVWAP_LOSE_REVERSAL` | DROP_NO_EDGE | 1.434 | 1.027 | 59 | 7,457 | `atr_pct>=0.0038`, `vol_ratio<=1.7879`, premom `sig5_adx_calc>=23.0068` |
| `D_EMA20_BOUNCE` | DROP_NO_EDGE | 0.833 | 0.693 | 50 | -4,290 | `vol_ratio<=1.7268`, `atr_pct>=0.0017` |
| `D_EMA20_REJECTION` | DROP_NO_EDGE | 0.989 | 0.887 | 51 | -273 | `atr_pct>=0.0029`, `lower_wick_pct<=0.0797` |

D remains rejected. `D_AVWAP_LOSE_REVERSAL` is still the closest setup, but worse-half PF is only 1.027, still below the 1.10 robustness floor.

### June 24-Inclusive Current Config Replay: G/D Only

| Setup | Basis note | Side | TRAIN n/PF/net Rs | TEST n/PF/net Rs |
|---|---|---|---:|---:|
| `D_EMA20_REJECTION` | native raw/ungated in this replay | SHORT | 20 / 0.45 / -5,200 | 2 / 0.00 / -1,957 |
| `G_HIGHER_HIGH_BREAK` | native raw/ungated in this replay | LONG | 35 / 1.14 / 2,385 | 3 / 0.46 / -951 |
| `G_LOWER_LOW_BREAK` | readmit/raw faithful | SHORT | 16 / 2.66 / 4,640 | 3 / 0.00 / -1,710 |
| G/D book, family-deduped | mixed | mixed | 70 / 1.05 / 1,569 | 8 / 0.15 / -4,618 |

Focused replay verdict:
- G/D current config does not pass.
- TEST PF is 0.15, OOS/IS PF ratio is 0.14, and TEST day-block p is 0.933.

### Priority Walk-Forward

Walk-forward was run only on the two research-priority mechanisms:

| Setup | Folds | Evaluated folds | Folds TEST PF >= 1.30 | Median TRAIN PF | Median TEST PF | Verdict |
|---|---:|---:|---:|---:|---:|---|
| `G_HIGHER_HIGH_BREAK` | 5 | 0 | 0 | 0.00 | 0.00 | INSUFFICIENT_DATA |
| `D_AVWAP_LOSE_REVERSAL` | 5 | 4 | 0 | 1.56 | 0.78 | DEAD |

Walk-forward conclusion:
- No robust setup found.
- `D_AVWAP_LOSE_REVERSAL` is not merely failing the latest window; its retrained method fails across folds.
- `G_HIGHER_HIGH_BREAK` still lacks enough evaluated folds under the current method, so it cannot be promoted or trusted.

### Research-Only G/D v2 Proxy Probe

A research-only v2 proxy was tested using fixed, causal bar-level filters. This did not edit production code or `final_setup_conf.py`.

Purpose:
- Check whether cleaner G/D mechanics can be approximated using existing pool columns before doing a deeper scanner rewrite.
- Avoid TEST tuning; the tested filters were hand-authored mechanism proxies.

G proxy examples:
- Above VWAP but not stretched: `vwap_dist_atr>=0`, `vwap_dist_atr<=3.0`.
- Acceptance candle: `close_loc>=0.55`, `body_pct>=0.45`, `upper_wick_pct<=0.25`.
- Basic participation/relative strength: `vol_ratio>=1.2`, `rs_pct>=0`.

D proxy examples:
- Below VWAP/weak close: `vwap_dist_atr<=-0.3`, `close_loc<=0.35`.
- Pressure candle: `body_pct>=0.35`, `vol_ratio>=1.2`, `rs_pct<=0`.

Best non-premomentum proxy results:

| Variant | Setup | Train n/PF/net Rs | TEST n/PF/net Rs | Note |
|---|---|---:|---:|---|
| `G_v2_acceptance`, SL/TGT 1.20/2.50 | `G_HIGHER_HIGH_BREAK` | 158 / 0.575 / -40,511 | 34 / 0.932 / -1,356 | Still net loser |
| `D_v2_pressure`, SL/TGT 1.20/1.50 | `D_AVWAP_LOSE_REVERSAL` | 1,166 / 0.411 / -432,206 | 302 / 0.651 / -55,044 | Large loser |

Fixed pre-momentum was also tested:

| Variant | Setup | Train n/PF/net Rs | TEST n/PF/net Rs | Note |
|---|---|---:|---:|---|
| `G_v2_acceptance_broad_g_premom`, SL/TGT 1.20/2.50 | `G_HIGHER_HIGH_BREAK` | 33 / 0.971 / -574 | 6 / 0.357 / -4,087 | Still fails |
| `G_v2_acceptance_old_g_premom`, SL/TGT 0.90/2.50 | `G_HIGHER_HIGH_BREAK` | 8 / 6.956 / 9,297 | 2 / 0.000 / -1,766 | Classic tiny TRAIN pocket |
| `D_v2_pressure_trend_pressure`, SL/TGT 1.20/1.50 | `D_AVWAP_LOSE_REVERSAL` | 451 / 0.456 / -156,769 | 120 / 0.622 / -23,136 | Still large loser |

v2 proxy conclusion:
- Current pool columns are not enough to rescue G/D.
- Adding fixed pre-momentum does not solve the problem.
- The old G-style momentum gate creates exactly the bad pattern: high TRAIN PF on tiny sample, zero TEST confirmation.
- Therefore the required improvement is a true candidate-generation/state rewrite, not more selected-strategy mask terms.

### Updated Decision After June 24 Check

No promotion. No more threshold search on the current definitions.

The honest next step is now narrowed:
1. Do not continue family-wide tuning.
2. Do not approve any G/D config.
3. Build a research-only candidate-definition v2 for `G_HIGHER_HIGH_BREAK` and maybe `D_AVWAP_LOSE_REVERSAL`.
4. The v2 work must change candidate mechanics, not search filters:
   - `G_HIGHER_HIGH_BREAK`: above-VWAP trend continuation, higher-high after controlled pullback/consolidation, no overextension, momentum improving.
   - `D_AVWAP_LOSE_REVERSAL`: AVWAP lose plus failed reclaim, rejection near AVWAP/VWAP, short pressure confirmation, no low-volume random chop.
5. After v2 candidate generation exists, rebuild the pool and repeat train/test plus walk-forward.

## GDV2 Research Candidate Layer - Implemented

A research-only sequence/state candidate layer was implemented:
- Script: `Train_and_Test\gd_v2_candidate_layer.py`
- Output pool: `C:\TradingData\eqidv2\outputs_ID_v11_gd_v2_pool`
- Source pool: `C:\TradingData\eqidv2\outputs_ID_v11_unified_pool`
- Production code untouched.
- `final_setup_conf.py` untouched.
- No `--approve` used.

The script loads surrounding 5-minute bars for each source G/D raw candidate and emits new `GDV2_*` setup names using sequence features, not just flat masks.

Generated sequence features include:
- `seq_vwap_dist_atr`
- `seq_ema20_dist_atr`
- `seq_pullback_depth_atr`
- `seq_break_margin_atr`
- `seq_breakdown_margin_atr`
- `seq_momentum_3_atr`
- `seq_vol_expansion`
- `seq_adx`
- `seq_adx_slope_5`
- `seq_ema_slope_5_atr`
- candle close/body/wick structure

Generated GDV2 pool:
- Source rows inspected: 9,277.
- Feature misses: 1.
- V2 rows generated: 1,701.
- Date range: `2025-11-04 11:20:00..2026-06-24 14:30:00`.

| GDV2 setup | Rows |
|---|---:|
| `GDV2_D_AVWAP_LOSE_REVERSAL_LITE` | 855 |
| `GDV2_D_AVWAP_LOSE_REVERSAL_BASE` | 615 |
| `GDV2_G_HIGHER_HIGH_BREAK_LITE` | 96 |
| `GDV2_D_AVWAP_LOSE_REVERSAL_BREAKDOWN` | 70 |
| `GDV2_G_HIGHER_HIGH_BREAK_BASE` | 42 |
| `GDV2_D_AVWAP_LOSE_REVERSAL_FAIL_RECLAIM` | 12 |
| `GDV2_G_HIGHER_HIGH_BREAK_TREND` | 6 |
| `GDV2_G_HIGHER_HIGH_BREAK_RETEST` | 5 |

### GDV2 Train/Test Result

Window:
- TRAIN: `2026-02-28..2026-05-27`
- TEST: `2026-05-28..2026-06-24`

Result: **REJECT**.

| Setup | Status | Train PF | Min-half PF | Train n | Train net Rs | Notes |
|---|---|---:|---:|---:|---:|---|
| `GDV2_D_AVWAP_LOSE_REVERSAL_BASE` | DROP_NO_EDGE | 0.718 | 0.674 | 50 | -7,226 | no TRAIN edge |
| `GDV2_D_AVWAP_LOSE_REVERSAL_LITE` | DROP_NO_EDGE | 0.758 | 0.583 | 50 | -5,091 | no TRAIN edge |
| `GDV2_D_AVWAP_LOSE_REVERSAL_BREAKDOWN` | TOO_FEW_TRAIN |  |  | 40 |  | sparse |
| `GDV2_D_AVWAP_LOSE_REVERSAL_FAIL_RECLAIM` | TOO_FEW_TRAIN |  |  | 4 |  | too sparse |
| `GDV2_G_HIGHER_HIGH_BREAK_BASE` | TOO_FEW_TRAIN |  |  | 18 |  | too sparse |
| `GDV2_G_HIGHER_HIGH_BREAK_LITE` | TOO_FEW_TRAIN |  |  | 37 |  | too sparse |
| `GDV2_G_HIGHER_HIGH_BREAK_RETEST` | TOO_FEW_TRAIN |  |  | 5 |  | too sparse |
| `GDV2_G_HIGHER_HIGH_BREAK_TREND` | TOO_FEW_TRAIN |  |  | 3 |  | too sparse |

Interpretation:
- The D sequence rewrite did not improve the edge; it is still a loser even after the harness tries to filter it.
- The G sequence rewrite is directionally cleaner, but too sparse to validate honestly.
- No GDV2 setup reached production viability.

### GDV2 Walk-Forward

Walk-forward was run on the only GDV2 variants with enough rows to be worth checking:

| Setup | Folds | Evaluated folds | Median TRAIN PF | Median TEST PF | Verdict |
|---|---:|---:|---:|---:|---|
| `GDV2_D_AVWAP_LOSE_REVERSAL_BASE` | 5 | 0 | 0.00 | 0.00 | INSUFFICIENT_DATA |
| `GDV2_D_AVWAP_LOSE_REVERSAL_LITE` | 5 | 1 | 1.78 | 0.47 | INSUFFICIENT_DATA |
| `GDV2_G_HIGHER_HIGH_BREAK_LITE` | 5 | 0 | 0.00 | 0.00 | INSUFFICIENT_DATA |

GDV2 conclusion:
- No robust setup found.
- D should be deprioritized further; even a sequence-aware rewrite remains negative.
- G remains the only possible research path, but it needs a scanner that generates more valid controlled-pullback/higher-high cases. Current source labels do not produce enough trainable G v2 candidates.

Updated action:
- Do not promote GDV2.
- Do not tune D further for now.
- If continuing, focus only on increasing valid `G_HIGHER_HIGH_BREAK` sequence candidate count through scanner-side detection, not mask search.
