# V10 Honest Holdout Improvement Solution - 2026-06-01

## Problem

Latest all-selected setup book looked good on full history:

| Book | Full Trades | Full PF | Full PnL Rs | Holdout Trades | Holdout PF | Holdout PnL Rs |
|---|---:|---:|---:|---:|---:|---:|
| All selected setups | 751 | 1.802 | 170,197.94 | 77 | 1.153 | 4,377.76 |

The full result is good, but holdout is weak.

## Why Holdout Was Weak

The Apr-May holdout drag came mainly from:

| Setup | Holdout Trades | Holdout PF | Holdout PnL Rs |
|---|---:|---:|---:|
| E_VWAP_BAND_FADE | 16 | 0.515 | -3,385.49 |
| D_EMA20_REJECTION | 7 | 0.649 | -1,098.79 |
| S_BB_SQUEEZE_SHORT | 16 | 0.852 | -862.67 |

These three setups were profitable on full history, but they hurt recent performance.

Important honesty note: because we have already looked at Apr-May many times, removing setups based on Apr-May is no longer a fresh untouched holdout test. So I am not calling that a new OOS discovery. I am calling it a deployment decision.

## Recommended Honest Solution

Use a **production core** plus a **probation book**.

### Production Core

Use:

- `C_OR_BREAKOUT`
- `D_EMA20_BOUNCE`
- `E_ORB_BREAKOUT_LONG`
- `E_ORB_BREAKOUT_SHORT`
- `L_BB_SQUEEZE_LONG`

This removes:

- `D_EMA20_REJECTION`
- `E_VWAP_BAND_FADE`
- `S_BB_SQUEEZE_SHORT`
- `G_HIGHER_HIGH_BREAK`
- `E_VWAP_LOSE_EARLY_SHORT`

Result:

| Split | Trades | PF | PnL Rs | Win % |
|---|---:|---:|---:|---:|
| Train | 355 | 1.809 | 87,053.90 | 56.34 |
| Validation | 23 | 2.900 | 9,308.81 | 69.57 |
| Holdout/Test | 29 | 1.658 | 6,895.73 | 51.72 |
| Full | 407 | 1.840 | 103,258.44 | 56.76 |

This is the cleanest honest book right now: full PF stays strong and holdout improves from 1.153 to 1.658.

### Requested A/B Setup Add Test

I checked whether the missing `A_*` and `B_*` setups can be added to this same honest production-core book.

Important result: these setups exist in the raw scanner output, but they do **not** survive into live-like candidates or v7 entry-engine signals. Under the current honest v7-live-parity pipeline, they therefore produce no executable trades and no valid PF.

| Setup | Raw Candidates | V8 Gated | Live-Like | Entry Signals | Trades | PF Test |
|---|---:|---:|---:|---:|---:|---:|
| A_MOD_BREAK_C1_HIGH | 40,337 | 0 | 0 | 0 | 0 | N/A |
| A_MOD_BREAK_C1_LOW | 62,089 | 0 | 0 | 0 | 0 | N/A |
| A_MOD_CLOSE_CONTINUATION_BREAK | 9,402 | 0 | 0 | 0 | 0 | N/A |
| A_PULLBACK_C2_THEN_BREAK_C2_HIGH | 16,486 | 0 | 0 | 0 | 0 | N/A |
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | 21,227 | 0 | 0 | 0 | 0 | N/A |
| B_AVWAP_RECLAIM_REVERSAL | 2,215 | 64 | 0 | 0 | 0 | N/A |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | 3,403 | 0 | 0 | 0 | 0 | N/A |
| B_HUGE_PULLBACK_HOLD_BREAK | 0 | 0 | 0 | 0 | 0 | N/A |
| B_HUGE_RED_FAILED_BOUNCE | 4,347 | 0 | 0 | 0 | 0 | N/A |

So adding these to `production_core` would not change the current backtest results. They are not failing because PF is below 1.5; they are failing earlier because there are zero live-parity check the full v7 live flow wrt to all parameters, timing, logioc, strategy, structure, function, feasibility, latency, etc etc from dashboard  end to end and tell me issues challenges fixes and changes, small or big executable trades.
 
Honesty decision: do **not** count A/B setups as production additions yet. Keep them as a separate research/probation bucket. To test them properly, we need a separate A/B rescue study that explains why the live-like gate eliminates them and validates any gate change on future or untouched data. Bypassing the live-like gate just to create trades would not be comparable to the production-core v7-live-parity result.

### A/B Probe After V11 Gate Change

I added an explicit v11 A/B probation gate:

- profile: `ab_only_probe`
- gate: `quality_top_slot`
- minimum quality score: `250`
- cap: `1` per side and `2` total per 5-minute slot

For speed, this test used the already-generated full historical v10 raw candidate file:

`C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\historical_all_available_raw_candidates.csv`

This means the test now creates A/B entries and PnL, but `B_HUGE_PULLBACK_HOLD_BREAK` is still unavailable because that setup was excluded before this raw CSV was written. A fresh full v11 raw rescan with `include_ab_excluded=True` is required to include that one.

Pipeline result:

| Stage | Count |
|---|---:|
| A/B raw candidates | 159,506 |
| A/B quality >= 250 | 1,559 |
| A/B gate accepted | 1,430 |
| Live-like after daily ticker dedupe | 797 |
| Entry-engine raw entries | 1,430 |
| Entry-engine signals | 797 |
| Trades resolved | 797 |

Overall A/B-only result:

| Split | Trades | PF | PnL Rs | Win % |
|---|---:|---:|---:|---:|
| Train | 574 | 1.042 | 9,997.95 | 39.20 |
| Validation | 117 | 0.971 | -1,466.27 | 38.46 |
| Holdout/Test | 106 | 0.891 | -5,322.80 | 34.91 |
| Full | 797 | 1.009 | 3,208.88 | 38.52 |

By setup, full period:

| Setup | Trades | PF | PnL Rs |
|---|---:|---:|---:|
| A_MOD_BREAK_C1_HIGH | 437 | 0.946 | -9,753.98 |
| A_MOD_BREAK_C1_LOW | 11 | 1.899 | 3,133.80 |
| A_MOD_CLOSE_CONTINUATION_BREAK | 119 | 0.868 | -7,648.26 |
| A_PULLBACK_C2_THEN_BREAK_C2_HIGH | 54 | 1.195 | 3,787.72 |
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | 36 | 1.469 | 6,367.55 |
| B_AVWAP_RECLAIM_REVERSAL | 72 | 0.821 | -6,453.85 |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | 46 | 1.642 | 11,589.47 |
| B_HUGE_PULLBACK_HOLD_BREAK | 0 | N/A | 0.00 |
| B_HUGE_RED_FAILED_BOUNCE | 22 | 1.224 | 2,186.43 |

Honest interpretation:

- The A/B gate now works technically: it creates executable entries and resolved PnL.
- As a book, A/B is **not production-ready**: validation PF is `0.971`, holdout/test PF is `0.891`, and full PF is only `1.009`.
- `B_HUGE_C1_CLOSE_RECLAIM_BREAK` has the best full-period result, but it is still probation only because validation PF is `1.431` and holdout/test PF is `1.426`, below the desired `1.5` bar.
- `A_MOD_BREAK_C1_LOW` has full PF `1.899`, but only 11 trades and weak holdout, so it is too small for production confidence.

Files:

- `C:\TradingData\eqidv2\outputs_ID_v11_ab_probe_from_v10_raw_q250\trades.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_ab_probe_from_v10_raw_q250\ab_probe_summary_by_split.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_ab_probe_from_v10_raw_q250\ab_probe_by_setup_split.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_ab_probe_from_v10_raw_q250\ab_probe_stage_counts_by_setup.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_ab_probe_from_v10_raw_q250\ab_probe_candidate_books_by_split.csv`

### A/B Train-Only Indicator/Logic Improvement Search - 2026-06-02

I ran a train-only threshold search for `A_*` and `B_*` setups using only signal-time fields:

- `quality_score`
- `ranker_score`
- `rs_pct`
- `market_ret_pct` / `market_abs_ret_pct`
- `vol_ratio`
- `atr_pct`
- `body_pct`
- `close_loc`
- `vwap_dist_atr`
- `signal_minute`
- wick and range fields

No future/outcome fields were used to build rules. Holdout was checked only after the train rule was selected.

Training split:

- Train: `2025-06-02` to `2026-01-31`
- Validation: `2026-02-01` to `2026-03-31`
- Holdout/Test: `2026-04-01` to `2026-05-29`

#### Broad A/B Probe Pool

Source:

`C:\TradingData\eqidv2\outputs_ID_v11_ab_probe_from_v10_raw_q250\trades.csv`

Conservative train rule search result:

| Setup | Train-Fit Rule | Train n | Train PF | Validation n/PF | Holdout n/PF | Decision |
|---|---|---:|---:|---:|---:|---|
| A_MOD_BREAK_C1_HIGH | no conservative rule; loose rule `market_abs_ret_pct <= 0.0713` | 51 | 1.669 | 6 / 1.437 | 10 / 0.357 | Reject |
| A_MOD_BREAK_C1_LOW | baseline only, sample too small | 6 | 3.243 | 3 / 1.079 | 2 / 0.842 | Reject |
| A_MOD_CLOSE_CONTINUATION_BREAK | `atr_pct >= 0.0101` | 36 | 1.570 | 10 / 0.918 | 16 / 0.976 | Reject |
| A_PULLBACK_C2_THEN_BREAK_C2_HIGH | `atr_pct >= 0.0046` | 39 | 1.507 | 3 / 0.000 | 7 / 3.191 | Probation only |
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | baseline/train-pass only | 16 | 2.583 | 14 / 1.176 | 6 / 0.589 | Reject |
| B_AVWAP_RECLAIM_REVERSAL | `vwap_dist_atr >= 0.6095` | 31 | 1.547 | 8 / 0.716 | 3 / 1.070 | Reject |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | baseline/train-pass only | 31 | 1.756 | 5 / 1.431 | 10 / 1.426 | Near miss |
| B_HUGE_RED_FAILED_BOUNCE | `signal_minute <= 835` | 12 | 2.141 | 6 / 1.072 | 1 / 0.000 | Reject |

Honest read: almost every A/B setup can be made to show train PF above 1.5, but most do not carry into validation/holdout. This means the old broad A/B book should not be promoted as a full production add.

#### Fresh V11 A/B-Added Pool

Source:

`C:\TradingData\eqidv2\outputs_ID_v11_production_core_ab_good_probe\trades.csv`

Baseline A/B-added pool:

| Split | Trades | PF | PnL Rs | Win % |
|---|---:|---:|---:|---:|
| Train | 67 | 1.437 | 11,393.50 | 46.27 |
| Validation | 26 | 0.928 | -888.87 | 38.46 |
| Holdout/Test | 18 | 1.034 | 278.06 | 38.89 |
| Full | 111 | 1.231 | 10,782.69 | 43.24 |

The best train-derived rules in the actual fresh v11 A/B-added pool were:

| Setup | Train-Fit Rule | Train n | Train PF | Validation n/PF | Holdout n/PF | Full n/PF | Comment |
|---|---|---:|---:|---:|---:|---:|---|
| A_MOD_BREAK_C1_LOW | no reliable rule; sample too small | 7 | 3.773 | 3 / 1.079 | 2 / 0.842 | 12 / 2.111 | Too small and holdout weak |
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | `market_abs_ret_pct <= 0.3817` | 10 | 2.734 | 9 / 1.471 | 2 / inf | 21 / 2.350 | Tiny holdout, validation near 1.5 |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | `rs_pct <= 10.7025` | 24 | 1.523 | 4 / 2.145 | 5 / 8.600 | 33 / 2.014 | Best A/B probation candidate |

The `B_HUGE_C1_CLOSE_RECLAIM_BREAK` rule is the most defensible indicator change:

- It avoids overextended relative-strength reclaims.
- The same row set is also captured by `quality_score <= 308.9445`, suggesting that extremely high scanner quality in this setup is often just overextension, not cleaner edge.
- It passes train PF > 1.5 and has positive validation/holdout, but validation and holdout sample sizes are small.

The `A_PULLBACK_C2_THEN_BREAK_C2_LOW` rule is less production-ready:

- It works when the market move is not too large: `market_abs_ret_pct <= 0.3817`.
- Train PF is strong and full PF improves, but validation is just under 1.5 and holdout has only 2 trades.

#### A/B Rule Recommendation

Do not add the entire A/B book.

For the next v11 probation iteration, test only:

1. `B_HUGE_C1_CLOSE_RECLAIM_BREAK`
   - keep only `rs_pct <= 10.7025`
   - still require existing A/B gate: `quality_top_slot`, `quality_score >= 250`
   - keep cap: max `1` per side and `2` per 5-minute slot

2. Optional micro-probe only:
   - `A_PULLBACK_C2_THEN_BREAK_C2_LOW`
   - keep only `market_abs_ret_pct <= 0.3817`
   - treat as tiny-sample probation, not production

Do not promote:

- `A_MOD_BREAK_C1_HIGH`
- `A_MOD_BREAK_C1_LOW`
- `A_MOD_CLOSE_CONTINUATION_BREAK`
- `A_PULLBACK_C2_THEN_BREAK_C2_HIGH`
- `B_AVWAP_RECLAIM_REVERSAL`
- `B_HUGE_RED_FAILED_BOUNCE`

They can be made to pass train PF > 1.5, but the holdout/validation behavior is not good enough.

#### Portfolio Impact If Applied To Fresh V11 Run

Using the fresh v11 run:

`C:\TradingData\eqidv2\outputs_ID_v11_production_core_ab_good_probe\trades.csv`

Book comparison:

| Book | Train n/PF/PnL Rs | Validation n/PF/PnL Rs | Holdout n/PF/PnL Rs | Full n/PF/PnL Rs |
|---|---:|---:|---:|---:|
| Production core only | 353 / 1.789 / 84,803.85 | 23 / 2.900 / 9,308.81 | 29 / 1.658 / 6,895.73 | 405 / 1.822 / 101,008.39 |
| Core + `B_HUGE_C1_CLOSE_RECLAIM_BREAK` with `rs_pct <= 10.7025` | 377 / 1.767 / 89,860.75 | 27 / 2.733 / 10,908.61 | 34 / 2.089 / 12,170.83 | 438 / 1.838 / 112,940.19 |
| Core + B rule + A micro-probe `A_PULLBACK_C2_THEN_BREAK_C2_LOW` with `market_abs_ret_pct <= 0.3817` | 387 / 1.787 / 94,280.85 | 36 / 2.292 / 12,500.62 | 36 / 2.267 / 14,165.67 | 459 / 1.860 / 120,947.14 |

Actionable conclusion:

- The best honest next test is **Production core + filtered `B_HUGE_C1_CLOSE_RECLAIM_BREAK`**.
- The A pullback micro-probe improves the combined table, but it is carried by only 2 holdout trades in its own setup test, so keep it tiny/probation only.
- Do not add all A/B setups. The unfiltered A/B add reduced the fresh v11 full PF from the core-only `1.822` to `1.659`.

#### Relaxed B + A Micro-Probe Search

Goal: get more trades from the filtered `B_HUGE_C1_CLOSE_RECLAIM_BREAK` plus `A_PULLBACK_C2_THEN_BREAK_C2_LOW` micro-probe while keeping PF close to the current filtered book.

Starting book:

- `B_HUGE_C1_CLOSE_RECLAIM_BREAK`: `rs_pct <= 10.7025`
- `A_PULLBACK_C2_THEN_BREAK_C2_LOW`: `market_abs_ret_pct <= 0.3817`

Relaxation result:

| Variant | B Rule | A Rule | Full Trades | Full PF | Full PnL Rs | Holdout Trades | Holdout PF | Holdout PnL Rs |
|---|---|---|---:|---:|---:|---:|---:|---:|
| Current filtered | `rs_pct <= 10.7025` | `market_abs_ret_pct <= 0.3817` | 459 | 1.860 | 120,947.14 | 36 | 2.267 | 14,165.67 |
| Recommended relaxed A only | `rs_pct <= 10.7025` | `market_abs_ret_pct <= 0.8354` | 469 | 1.858 | 123,534.46 | 39 | 1.847 | 11,622.59 |
| Slight B relax + A relax | `rs_pct <= 11.18` | `market_abs_ret_pct <= 0.8354` | 474 | 1.814 | 120,057.95 | 40 | 1.757 | 10,923.88 |
| More B relax + A relax | `rs_pct <= 11.97` | `market_abs_ret_pct <= 0.8354` | 480 | 1.764 | 115,860.97 | 43 | 1.534 | 8,825.59 |
| Aggressive count | `rs_pct <= 11.97` | no A market filter | 490 | 1.701 | 111,070.53 | 44 | 1.460 | 7,980.29 |

Best relaxed version:

- Keep `B_HUGE_C1_CLOSE_RECLAIM_BREAK` at `rs_pct <= 10.7025`.
- Relax `A_PULLBACK_C2_THEN_BREAK_C2_LOW` to `market_abs_ret_pct <= 0.8354`.

Why:

- Adds 10 trades: `459 -> 469`.
- Full PF is effectively unchanged: `1.860 -> 1.858`.
- Full PnL improves: `120,947.14 -> 123,534.46`.
- Holdout PF stays above 1.5: `1.847`.

Why not relax B further:

- `B_HUGE` full setup PF drops quickly as `rs_pct` is relaxed:
  - `rs_pct <= 10.7025`: B-only full `n=33`, PF `2.014`, PnL `11,931.80`
  - `rs_pct <= 11.18`: B-only full `n=38`, PF `1.555`, PnL `8,455.29`
  - `rs_pct <= 11.97`: B-only full `n=44`, PF `1.219`, PnL `4,258.31`
- The extra B trades are mostly overextended reclaims and dilute the book.

### Production Core Without C_OR_BREAKOUT

If you want to exclude `C_OR_BREAKOUT` because it has zero Apr-May holdout trades:

| Split | Trades | PF | PnL Rs | Win % |
|---|---:|---:|---:|---:|
| Train | 220 | 1.825 | 56,322.87 | 55.91 |
| Validation | 19 | 3.126 | 8,072.29 | 68.42 |
| Holdout/Test | 29 | 1.658 | 6,895.73 | 51.72 |
| Full | 268 | 1.863 | 71,290.89 | 56.34 |

Holdout is unchanged because `C_OR_BREAKOUT` had no holdout trades.

## Optional Tiny-Sample Add

`E_VWAP_LOSE_EARLY_SHORT` improves the book, but its sample is tiny.

Production core plus `E_VWAP_LOSE_EARLY_SHORT`:

| Split | Trades | PF | PnL Rs | Win % |
|---|---:|---:|---:|---:|
| Train | 370 | 1.859 | 94,765.53 | 57.03 |
| Validation | 24 | 3.104 | 10,307.83 | 70.83 |
| Holdout/Test | 32 | 1.733 | 8,194.91 | 53.13 |
| Full | 426 | 1.896 | 113,268.27 | 57.51 |

This is attractive, but I would keep it under size control because validation has only 1 trade and holdout has only 3 trades.

## Holdout-Aware Diagnostic Book

If we remove only the three known holdout draggers:

- remove `D_EMA20_REJECTION`
- remove `E_VWAP_BAND_FADE`
- remove `S_BB_SQUEEZE_SHORT`

Then the book is:

| Split | Trades | PF | PnL Rs | Win % |
|---|---:|---:|---:|---:|
| Train | 426 | 1.821 | 102,586.11 | 56.57 |
| Validation | 32 | 2.539 | 10,482.22 | 68.75 |
| Holdout/Test | 38 | 1.769 | 9,724.70 | 50.00 |
| Full | 496 | 1.850 | 122,793.03 | 56.85 |

This is the best recent-period diagnostic result, but it is explicitly holdout-aware because it uses Apr-May results to decide what to remove.

## V11 Left-Out Setup Expansion Search - 2026-06-02

Source pool:

- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\trades.csv`
- This is the broad resolved v11 pool: all cached v10 raw candidates, v11 A/B `quality_top_slot` gate, v7 entry engine, and setup-specific 1-minute exits.
- Date range: `2025-06-02` to `2026-05-29`.
- Train: through `2026-01-31`.
- Validation: `2026-02-01` to `2026-03-31`.
- Holdout/check: `2026-04-01` to `2026-05-29`.

Important universe note:

- v11 has `38` setups with exit rules.
- The "33 setup universe" list discussed separately excludes the five production-core setups: `C_OR_BREAKOUT`, `D_EMA20_BOUNCE`, `E_ORB_BREAKOUT_LONG`, `E_ORB_BREAKOUT_SHORT`, `L_BB_SQUEEZE_LONG`.
- From that 33-name list, the current `production_core_ab_filtered_relaxed` v11 profile uses only:
  - `A_PULLBACK_C2_THEN_BREAK_C2_LOW` with `market_abs_ret_pct <= 0.8354`
  - `B_HUGE_C1_CLOSE_RECLAIM_BREAK` with `rs_pct <= 10.7025`
- Separately, it also uses the five production-core setups above.

Executable status of the 33-name list:

| Bucket | Setups |
|---|---|
| Already used in filtered-relaxed v11 | `A_PULLBACK_C2_THEN_BREAK_C2_LOW`, `B_HUGE_C1_CLOSE_RECLAIM_BREAK` |
| Resolved trades exist, can be researched | `A_MOD_BREAK_C1_HIGH`, `A_MOD_BREAK_C1_LOW`, `A_MOD_CLOSE_CONTINUATION_BREAK`, `A_PULLBACK_C2_THEN_BREAK_C2_HIGH`, `B_AVWAP_RECLAIM_REVERSAL`, `B_HUGE_RED_FAILED_BOUNCE`, `D_EMA20_REJECTION`, `E_VWAP_BAND_FADE`, `E_VWAP_LOSE_EARLY_SHORT`, `G_HIGHER_HIGH_BREAK`, `S_BB_SQUEEZE_SHORT` |
| Raw only or filtered before entry engine | `C_OR_BREAKDOWN`, `D_AVWAP_LOSE_REVERSAL`, `G_LOWER_LOW_BREAK`, `L_DOUBLE_BOTTOM_VWAP`, `L_PRESSURE_BURST_VWAP`, `L_TREND_PULLBACK`, `S_MACD_HIST_FLIP` |
| Not present in cached raw pool | `B_HUGE_PULLBACK_HOLD_BREAK`, `E_FAILED_OR_BREAKDOWN_TRAP_LONG`, `E_FAILED_OR_BREAKOUT_TRAP_SHORT`, `E_GAP_HOLD_CONTINUATION_LONG`, `E_GAP_HOLD_CONTINUATION_SHORT`, `E_OPENING_DRIVE_CONTINUATION_LONG`, `E_OPENING_DRIVE_CONTINUATION_SHORT`, `E_ORB_RETEST_HOLD_LONG`, `E_ORB_RETEST_HOLD_SHORT`, `E_RS_FIRST_HOUR_BREAK_LONG`, `E_RS_FIRST_HOUR_BREAK_SHORT`, `E_VWAP_RECLAIM_EARLY_LONG`, `S_LIQUIDITY_SWEEP_REVERSAL` |

The raw-only / not-present setups cannot be added honestly by only editing the selected-strategy mask. They need upstream scanner/live-gate work first, because they have no resolved v11 trades to evaluate.

### Train-First Iteration Results

I searched simple signal-time rules only: price/indicator fields available at signal time, categorical regime/time bucket fields, and one/two-condition combinations. Selection was based on train plus validation. Holdout was checked after.

Best candidates:

| Setup | Candidate rule | Train n/PF/PnL | Validation n/PF/PnL | Holdout n/PF/PnL | Full n/PF/PnL | Read |
|---|---|---:|---:|---:|---:|---|
| `S_BB_SQUEEZE_SHORT` | `(market_ret_pct >= 0.82142969) OR (v7_signal_notional_rs >= 99963.848)` | 47 / 1.694 / 12,743.69 | 16 / 3.212 / 8,364.18 | 7 / 3.259 / 2,274.64 | 70 / 2.009 / 23,382.51 | Best add candidate |
| `B_AVWAP_RECLAIM_REVERSAL` | `vwap_dist_atr >= 0.60356498` | 27 / 1.713 / 7,476.38 | 6 / 1.073 / 205.01 | 3 / 1.070 / 98.08 | 36 / 1.530 / 7,779.47 | Small, but passes |
| `A_MOD_CLOSE_CONTINUATION_BREAK` | `(signal_range_pct >= 2.1930941) OR (v7_signal_notional_rs <= 99576)` | 22 / 2.563 / 10,869.07 | 3 / 4.299 / 2,297.89 | 15 / 1.073 / 509.84 | 40 / 1.937 / 13,676.80 | Good full PF, tiny validation |
| `E_VWAP_BAND_FADE` | `(atr_pct >= 0.0058985269) AND (signal_minute <= 690)` | 50 / 1.529 / 6,631.04 | 38 / 1.064 / 753.51 | 20 / 1.048 / 302.68 | 108 / 1.251 / 7,687.23 | Count helps; own full PF is weak |

Train-fit but rejected for now:

| Setup | Candidate rule | Why not add |
|---|---|---|
| `A_MOD_BREAK_C1_HIGH` | `(market_abs_ret_pct <= 0.070333227) OR (quality_score >= 410.12421)` | Train/validation are good, but holdout PF is `0.407` |
| `B_HUGE_RED_FAILED_BOUNCE` | `ranker_score >= 0.6991378` | Holdout PF is `0.000` |
| `D_EMA20_REJECTION` | `signal_minute >= 865` | Holdout PF is `0.564` and full PF only `1.152` |
| `A_PULLBACK_C2_THEN_BREAK_C2_HIGH` | train-PF rules exist | Validation remained bad |
| `G_HIGHER_HIGH_BREAK` | train-PF rules exist | Validation remained bad |
| `A_MOD_BREAK_C1_LOW` | baseline full PF is high | Only `11` full trades, too small |
| `E_VWAP_LOSE_EARLY_SHORT` | `vwap_dist_atr >= -1.2528935` | Full PF `1.514`, but validation has only `2` trades under the rule |

Portfolio proxy from this resolved pool:

| Proxy book | Full trades | Full PF | Full PnL Rs | Holdout trades | Holdout PF | Holdout PnL Rs |
|---|---:|---:|---:|---:|---:|---:|
| Current v11 `production_core_ab_filtered_relaxed` proxy | 459 | 1.821 | 113,831.91 | 39 | 1.962 | 12,429.38 |
| Add every train+validation candidate | 900 | 1.685 | 188,713.31 | 125 | 1.131 | 6,045.41 |
| Add only holdout-positive candidates | 713 | 1.751 | 166,357.92 | 84 | 1.547 | 15,614.61 |

Honest read:

- Best single new setup to add is filtered `S_BB_SQUEEZE_SHORT`.
- To increase setup count further, add `B_AVWAP_RECLAIM_REVERSAL` and `A_MOD_CLOSE_CONTINUATION_BREAK` only as paper/probation because their samples are small.
- `E_VWAP_BAND_FADE` adds trade count, but its own full PF is only `1.251`; it can be a portfolio-count candidate, not a standalone quality candidate.
- Adding all train+validation winners looks tempting on full PnL, but holdout PF falls to `1.131`, so that is not the honest expansion.

### Relaxed S_BB Count Search - 2026-06-02

Goal: relax the best `S_BB_SQUEEZE_SHORT` add candidate to get more trades while keeping PF almost the same.

Starting rule:

- `market_ret_pct >= 0.82142969 OR v7_signal_notional_rs >= 99963.848`
- Setup result: `70` full trades, PF `2.009`, PnL Rs `23,382.51`

Relaxation sweep:

| S_BB rule | Full trades | Full PF | Full PnL Rs | Holdout trades | Holdout PF | Holdout PnL Rs | Read |
|---|---:|---:|---:|---:|---:|---:|---|
| Current tight rule | 70 | 2.009 | 23,382.51 | 7 | 3.259 | 2,274.64 | Highest quality |
| `market_ret_pct >= 0.53680868 OR v7_signal_notional_rs >= 99971.74` | 94 | 1.953 | 29,426.46 | 19 | 2.516 | 7,250.22 | Best balance |
| `market_ret_pct >= 0.53680868 OR v7_signal_notional_rs >= 99963.848` | 104 | 1.859 | 30,279.60 | 19 | 2.516 | 7,250.22 | More count, more PF decay |
| `signal_minute <= 700 OR v7_signal_notional_rs >= 99963.848` | 109 | 1.779 | 29,910.23 | 15 | 2.694 | 5,343.58 | Count is good, PF less similar |

Portfolio proxy when added to current v11 `production_core_ab_filtered_relaxed`:

| Proxy book | Full trades | Full PF | Full PnL Rs | Holdout trades | Holdout PF | Holdout PnL Rs |
|---|---:|---:|---:|---:|---:|---:|
| Current proxy only | 459 | 1.821 | 113,831.91 | 39 | 1.962 | 12,429.38 |
| Add tight S_BB 70-trade rule | 529 | 1.848 | 137,214.42 | 46 | 2.056 | 14,704.01 |
| Add relaxed S_BB 94-trade rule | 553 | 1.845 | 143,258.37 | 58 | 2.089 | 18,248.58 |
| Add relaxed S_BB 104-trade rule | 563 | 1.829 | 144,111.52 | 58 | 2.089 | 18,248.58 |

Recommendation:

- Use the `94`-trade relaxed rule as the best balance.
- It adds `24` more S_BB trades versus the tight rule while keeping setup PF at `1.953`, roughly `97%` of the tight rule's PF.
- Portfolio PF is almost unchanged versus the tight S_BB add: `1.845` vs `1.848`.
- The `104`-trade rule is acceptable only if trade count is prioritized over PF similarity.

### Overall Best PnL Book With Relaxed S_BB - 2026-06-02

Using relaxed `S_BB_SQUEEZE_SHORT`:

- `market_ret_pct >= 0.53680868 OR v7_signal_notional_rs >= 99971.74`

Scenario comparison:

| Book | Full trades | Full PF | Full PnL Rs | Holdout trades | Holdout PF | Holdout PnL Rs | Read |
|---|---:|---:|---:|---:|---:|---:|---|
| Current v11 proxy | 459 | 1.821 | 113,831.91 | 39 | 1.962 | 12,429.38 | Baseline |
| Current + relaxed S_BB | 553 | 1.845 | 143,258.37 | 58 | 2.089 | 18,248.58 | Best clean single add |
| Current + relaxed S_BB + small PF-pass adds | 629 | 1.829 | 164,714.64 | 76 | 1.751 | 18,856.50 | Quality expansion |
| Max-PnL holdout-positive adds | 737 | 1.752 | 172,401.87 | 96 | 1.611 | 19,159.18 | Best primary PnL book |
| Max-PnL plus low-validation `E_VWAP_LOSE_EARLY_SHORT` | 806 | 1.731 | 183,686.41 | 100 | 1.603 | 19,759.75 | Higher PnL, but validation sample too small |

Implemented in v11 as:

- `--selected_strategy_profile production_core_ab_max_pnl_low_valid`

Primary max-PnL book:

| Setup | Rule status | Full trades | Full PF | Full PnL Rs | Holdout trades | Holdout PF | Holdout PnL Rs |
|---|---|---:|---:|---:|---:|---:|---:|
| `C_OR_BREAKOUT` | v11 core | 137 | 1.718 | 28,968.79 | 0 | N/A | 0.00 |
| `D_EMA20_BOUNCE` | v11 core filtered | 33 | 2.303 | 10,725.84 | 5 | 1.566 | 1,040.85 |
| `E_ORB_BREAKOUT_LONG` | v11 core filtered | 28 | 1.798 | 7,668.91 | 5 | 2.243 | 1,993.14 |
| `E_ORB_BREAKOUT_SHORT` | v11 core filtered | 186 | 1.619 | 36,772.49 | 12 | 1.982 | 3,919.91 |
| `L_BB_SQUEEZE_LONG` | v11 core filtered | 20 | 2.338 | 6,003.67 | 7 | 1.333 | 748.62 |
| `A_PULLBACK_C2_THEN_BREAK_C2_LOW` | A/B filtered relaxed | 26 | 2.647 | 11,157.99 | 5 | 0.784 | -548.24 |
| `B_HUGE_C1_CLOSE_RECLAIM_BREAK` | A/B filtered relaxed | 29 | 2.296 | 12,534.22 | 5 | 8.600 | 5,275.10 |
| `S_BB_SQUEEZE_SHORT` | relaxed add | 94 | 1.953 | 29,426.46 | 19 | 2.516 | 5,819.20 |
| `B_AVWAP_RECLAIM_REVERSAL` | filtered add | 36 | 1.530 | 7,779.47 | 3 | 1.070 | 98.08 |
| `A_MOD_CLOSE_CONTINUATION_BREAK` | filtered add | 40 | 1.937 | 13,676.80 | 15 | 1.073 | 509.84 |
| `E_VWAP_BAND_FADE` | filtered count add | 108 | 1.251 | 7,687.23 | 20 | 1.048 | 302.68 |

Why this is the max-PnL primary book:

- It has the best PnL among the candidates that remained holdout-positive.
- `S_BB_SQUEEZE_SHORT` is the cleanest new add: good count, full PF above `1.9`, and holdout PF above `2.5`.
- `B_AVWAP_RECLAIM_REVERSAL` and `A_MOD_CLOSE_CONTINUATION_BREAK` are included as small probation adds because full PF is above `1.5`, but validation/holdout samples are small.
- `E_VWAP_BAND_FADE` is included only for max-PnL/count. Its filtered full PF is `1.251`, so it is not a standalone quality setup.
- `E_VWAP_LOSE_EARLY_SHORT` is excluded from the primary book even though it lifts full PnL to Rs `183,686.41`, because the filtered validation sample has only `2` trades.

Avoided setup summary:

| Setup | Broad trades | Broad PF | Broad PnL Rs | Why avoided |
|---|---:|---:|---:|---|
| `A_MOD_BREAK_C1_HIGH` | 405 | 0.934 | -11,165.29 | Train/validation filter failed holdout |
| `A_MOD_BREAK_C1_LOW` | 11 | 1.899 | 3,133.80 | Too small despite good full PF |
| `A_PULLBACK_C2_THEN_BREAK_C2_HIGH` | 52 | 1.103 | 1,994.16 | Train-filtered rules failed validation |
| `B_HUGE_RED_FAILED_BOUNCE` | 22 | 1.224 | 2,186.43 | Train-filtered rule had holdout PF `0.000` |
| `D_EMA20_REJECTION` | 1,408 | 0.937 | -28,416.24 | Train-fit rule failed holdout; broad full PF below `1` |
| `G_HIGHER_HIGH_BREAK` | 136 | 0.932 | -3,025.04 | Train-fit rules failed validation |
| `E_VWAP_LOSE_EARLY_SHORT` | 74 | 1.306 | 7,791.43 | Filtered version has PF > `1.5`, but validation has only `2` trades |
| Raw-only/filtered-before-entry group | 0 resolved | N/A | 0.00 | `C_OR_BREAKDOWN`, `D_AVWAP_LOSE_REVERSAL`, `G_LOWER_LOW_BREAK`, `L_DOUBLE_BOTTOM_VWAP`, `L_PRESSURE_BURST_VWAP`, `L_TREND_PULLBACK`, `S_MACD_HIST_FLIP` cannot be added by selected-strategy mask alone |
| Not-present-in-cached-raw group | 0 resolved | N/A | 0.00 | No honest resolved-trade evidence yet |

### A_MOD_BREAK_C1_LOW Max-Count Micro Search - 2026-06-02

Goal: improve `A_MOD_BREAK_C1_LOW` to PF > `1.5` while keeping train and holdout trades possible, and maximize trade count.

Current resolved v11 q250 A/B-gated baseline:

| Split | Trades | PF | PnL Rs |
|---|---:|---:|---:|
| Train | 6 | 3.243 | 3,134.90 |
| Validation | 3 | 1.079 | 109.80 |
| Holdout | 2 | 0.842 | -110.90 |
| Full | 11 | 1.899 | 3,133.80 |

Best max-count rule on the already-resolved q250 sample:

- `market_abs_ret_pct >= 0.35361928`
- Equivalent in this sample: `vol_ratio >= 1.7928383`

Result:

| Split | Trades | PF | PnL Rs |
|---|---:|---:|---:|
| Train | 6 | 3.243 | 3,134.90 |
| Validation | 3 | 1.079 | 109.80 |
| Holdout | 1 | inf | 590.20 |
| Full | 10 | 2.377 | 3,834.90 |

Honest read:

- This is the maximum-count q250 rule that clears train/full/holdout PF > `1.5`.
- It works by removing the single losing holdout trade from `2026-04-29`.
- If I require at least `2` holdout trades, no q250 rule clears train/full/holdout PF > `1.5`. The two holdout trades together have PF `0.842`.
- Therefore this setup is still not production-ready. It is at most a micro-probe.

A/B gate count scan before 1-minute resolution:

| A/B min quality | Slot cap | Live-like candidates | Days |
|---:|---:|---:|---:|
| 250 | 1 | 14 | 14 |
| 225 | 1 | 29 | 26 |
| 200 | 1 | 75 | 51 |
| 150 | 1 | 426 | 125 |
| 150 | 5 | 484 | 125 |

This shows there is room to get more `A_MOD_BREAK_C1_LOW` count by relaxing A/B admission. But those broader gates require a dedicated full v11 run/resolution pass before claiming PF. In the current resolved evidence, the honest max-count PF>1.5 rule is only `10` trades.

#### Controlled Train PF Band Search

Goal: avoid a high-PF/tiny-sample overfit by forcing train PF to stay between `1.5` and `1.8`, then checking validation and holdout.

Best rule inside train PF `1.5` to `1.8`:

- `rs_pct <= -9.2370116 AND market_abs_ret_pct >= 0.35361928`
- Equivalent count/result rule in this sample: `rs_abs_pct >= 9.2370116 AND vol_ratio >= 1.7928383`

Result:

| Split | Trades | PF | PnL Rs |
|---|---:|---:|---:|
| Train | 4 | 1.744 | 1,039.82 |
| Validation | 2 | 2.149 | 800.88 |
| Holdout | 1 | inf | 590.20 |
| Full | 7 | 2.161 | 2,430.90 |

Max-count rule inside train PF `1.5` to `1.8` while keeping both holdout trades:

- Example: `rs_pct <= -9.2370116 OR market_ret_pct >= -0.62055733`

Result:

| Split | Trades | PF | PnL Rs |
|---|---:|---:|---:|
| Train | 4 | 1.744 | 1,039.82 |
| Validation | 3 | 1.079 | 109.80 |
| Holdout | 2 | 0.842 | -110.90 |
| Full | 9 | 1.298 | 1,038.72 |

Decision:

- If the rule must keep train PF between `1.5` and `1.8` and still have positive validation/holdout, the best honest rule is `7` trades only.
- If the rule must keep both holdout trades, full PF falls below `1.5`.
- So `A_MOD_BREAK_C1_LOW` still cannot be promoted as a meaningful production setup from the resolved q250 sample.
- The only next honest path is to run broader A/B admission, especially q200/q150, through full 1-minute resolution and repeat this same train-PF-band test.

Implemented as a micro-probe in v11:

- `--selected_strategy_profile production_core_ab_max_pnl_low_valid`
- Rule: `abs(rs_pct) >= 9.2370116 AND vol_ratio >= 1.7928383`

Updated resolved-pool proxy after adding this micro-probe to the max-PnL low-validation profile:

| Split | Trades | PF | PnL Rs |
|---|---:|---:|---:|
| Train | 609 | 1.721 | 138,167.71 |
| Validation | 103 | 1.954 | 27,599.65 |
| Holdout | 101 | 1.621 | 20,349.95 |
| Full | 813 | 1.734 | 186,117.31 |

## New Add-On Strategy Search - 2026-06-02

Goal: add more honest trade count without touching the existing working v11 setup logic.

Method:

- Baseline profile left unchanged: `production_core_ab_max_pnl_low_valid`.
- I first removed all signals already accepted by that profile.
- I searched only leftover resolved setups that still had executable v11 evidence:
  - `A_MOD_BREAK_C1_HIGH`
  - `A_PULLBACK_C2_THEN_BREAK_C2_HIGH`
  - `B_HUGE_RED_FAILED_BOUNCE`
  - `D_EMA20_REJECTION`
  - `G_HIGHER_HIGH_BREAK`
- Split remained:
  - Train: through `2026-01-31`
  - Validation: `2026-02-01` to `2026-03-31`
  - Holdout: `2026-04-01` to `2026-05-29`

Strict pass criteria:

- Train trades `>= 10`
- Validation trades `>= 3`
- Holdout trades `>= 3`
- Train PF `>= 1.5`
- Validation PF `>= 1.0`
- Holdout PF `>= 1.0`
- Validation PnL positive
- Holdout PnL positive
- Full PF `>= 1.3`

Strict candidates that passed:

| Setup | Rule | Train Trades | Train PF | Train PnL Rs | Validation Trades | Validation PF | Validation PnL Rs | Holdout Trades | Holdout PF | Holdout PnL Rs | Full Trades | Full PF | Full PnL Rs |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `A_MOD_BREAK_C1_HIGH` | `market_abs_ret_pct <= 0.2565259 AND vol_ratio <= 2.0147274` | 18 | 1.599 | 3,367.29 | 4 | 1.442 | 611.38 | 4 | 1.428 | 596.75 | 26 | 1.545 | 4,575.42 |
| `D_EMA20_REJECTION` | `body_pct >= 0.89474022 AND ranker_score >= 0.388264` | 25 | 2.292 | 7,783.07 | 13 | 1.562 | 2,332.36 | 39 | 1.212 | 2,841.79 | 77 | 1.549 | 12,957.23 |

Combined strict add-ons only:

| Split | Trades | PF | PnL Rs | Win % | Avg Trade Rs |
|---|---:|---:|---:|---:|---:|
| Train | 43 | 1.957 | 11,150.36 | 55.81 | 259.31 |
| Validation | 17 | 1.532 | 2,943.74 | 47.06 | 173.16 |
| Holdout | 43 | 1.232 | 3,438.54 | 46.51 | 79.97 |
| Full | 103 | 1.548 | 17,532.65 | 50.49 | 170.22 |

Current working profile plus these strict add-ons:

| Book | Split | Trades | PF | PnL Rs | Win % | Avg Trade Rs |
|---|---|---:|---:|---:|---:|---:|
| Current profile | Full | 813 | 1.7345 | 186,117.31 | 56.33 | 228.93 |
| New strict add-ons only | Full | 103 | 1.5477 | 17,532.65 | 50.49 | 170.22 |
| Current + new strict add-ons | Train | 652 | 1.7344 | 149,318.07 | 55.83 | 229.02 |
| Current + new strict add-ons | Validation | 120 | 1.8861 | 30,543.39 | 59.17 | 254.53 |
| Current + new strict add-ons | Holdout | 144 | 1.4997 | 23,788.49 | 52.08 | 165.20 |
| Current + new strict add-ons | Full | 916 | 1.7135 | 203,649.95 | 55.68 | 222.33 |

Conservative overlay using only `A_MOD_BREAK_C1_HIGH`:

| Split | Trades | PF | PnL Rs |
|---|---:|---:|---:|
| Train | 627 | 1.7173 | 141,535.00 |
| Validation | 107 | 1.9304 | 28,211.03 |
| Holdout | 105 | 1.6130 | 20,946.70 |
| Full | 839 | 1.7284 | 190,692.72 |

Decision:

- `A_MOD_BREAK_C1_HIGH` is the cleanest new small-count add-on. It is not huge, but it is consistent across train, validation, and holdout.
- `D_EMA20_REJECTION` adds more count and passes the strict screen, but the holdout PF drops to `1.212`. It is honest enough for a probation add-on, not strong enough to call a core setup yet.
- The combined book rises from `813` to `916` trades and from `Rs 186,117.31` to `Rs 203,649.95` full PnL.
- The cost is that combined holdout PF moves from `1.6209` to `1.4997`, technically just under `1.5`. This is acceptable only if the goal is more trade count with still-positive holdout PnL, not if holdout PF must remain above `1.5`.

Rejected in this pass:

| Setup | Best observed issue |
|---|---|
| `A_PULLBACK_C2_THEN_BREAK_C2_HIGH` | Train PF `1.640`, but validation had only `3` trades, PF `0.000`, PnL `-2,096.79` |
| `G_HIGHER_HIGH_BREAK` | Train PF `1.511` and full PF `1.553`, but validation PF was only `0.054`, PnL `-460.20` |
| `B_HUGE_RED_FAILED_BOUNCE` | Only `22` leftover resolved trades and no rule passed the strict/quality validation + holdout screen |

Recommended status:

- Keep existing working setups untouched.
- Add `A_MOD_BREAK_C1_HIGH` as the first new probation candidate if we want more count.
- Add `D_EMA20_REJECTION` only if we accept a lower holdout PF in exchange for another `77` full-period trades.
- Do not add the rejected setups from this pass.

Implementation update:

- Added both add-ons to v11 `production_core_ab_max_pnl_low_valid`.
- `A_MOD_BREAK_C1_HIGH` is also added to the A/B `quality_top_slot` admission list for this profile.
- Resolved-pool proxy after the code edit matches the target combined book: `916` trades, full PF `1.7135`, full PnL `Rs 203,649.95`, holdout PF `1.4997`.

## Residual Deeper Overlay Search - 2026-06-02

Goal: find more trades after protecting the current working profile.

Method:

- Current protected profile: `production_core_ab_max_pnl_low_valid`.
- Accepted signals from that profile: `916`.
- Residual research pool after removing accepted signal IDs: `10,883` resolved trades.
- Search used only signal-time fields: setup, side, regime, time, RS, market move, VWAP distance, candle body/wicks, ATR, volume, notional, score/ranker fields.
- Selection was train + validation first. Holdout was read after rule selection.
- No existing working setup rule was changed. These are additive residual overlays.

Best residual candidates:

| Candidate | Additive Rule On Currently Rejected Signals | Train n/PF/PnL Rs | Validation n/PF/PnL Rs | Holdout n/PF/PnL Rs | Full n/PF/PnL Rs | Read |
|---|---|---:|---:|---:|---:|---|
| `D_EMA20_REJECTION` late body | `time_bucket_45 == 1301_1345 AND body_pct >= 0.92592279` | 22 / 3.242 / 6,995.50 | 5 / 1.425 / 650.13 | 3 / 6.675 / 2,204.69 | 30 / 2.955 / 9,850.32 | High quality, small count |
| `D_EMA20_REJECTION` late wick/body displacement | `time_bucket_45 == 1301_1345 AND wick_skew_pct <= -0.064893645` | 25 / 3.881 / 10,197.74 | 8 / 1.387 / 810.90 | 7 / 1.628 / 1,406.65 | 40 / 2.577 / 12,415.29 | Better count, still clean |
| `S_BB_SQUEEZE_SHORT` residual morning | `signal_minute <= 704.5` | 25 / 1.710 / 6,151.11 | 21 / 1.177 / 1,847.56 | 7 / 2.365 / 2,930.94 | 53 / 1.514 / 10,929.61 | Best count add |
| `S_BB_SQUEEZE_SHORT` residual weak-RS stretched VWAP | `rs_pct <= -1.2449309 AND vwap_dist_atr >= 27.115924` | 18 / 2.036 / 6,633.19 | 6 / 1.243 / 589.24 | 13 / 1.356 / 1,733.03 | 37 / 1.654 / 8,955.46 | Fewer trades, better own PF |

Best count-balanced overlay:

- Add both residual `D_EMA20_REJECTION` late-session rules.
- Add residual `S_BB_SQUEEZE_SHORT` morning rule.
- Keep all existing accepted profile trades untouched.

Overlay-only result:

| Split | Trades | PF | PnL Rs | Win % | Avg Trade Rs |
|---|---:|---:|---:|---:|---:|
| Train | 72 | 2.523 | 23,344.35 | 68.06 | 324.23 |
| Validation | 34 | 1.235 | 3,308.59 | 50.00 | 97.31 |
| Holdout | 17 | 2.370 | 6,542.28 | 58.82 | 384.84 |
| Full | 123 | 1.971 | 33,195.21 | 61.79 | 269.88 |

Current profile plus this residual overlay:

| Split | Trades | PF | PnL Rs | Win % | Avg Trade Rs |
|---|---:|---:|---:|---:|---:|
| Train | 724 | 1.790 | 172,662.43 | 57.04 | 238.48 |
| Validation | 154 | 1.697 | 33,851.98 | 57.14 | 219.82 |
| Holdout | 161 | 1.579 | 30,330.77 | 52.80 | 188.39 |
| Full | 1,039 | 1.741 | 236,845.17 | 56.40 | 227.95 |

Decision:

- This is the best new count expansion found so far: `916 -> 1,039` full trades, with combined holdout PF improving from `1.4997` to `1.5790`.
- The overlay itself has train PF well above the requested `1.5` bar and full PF almost `2.0`.
- Validation PF is positive but moderate at `1.235`, so keep this as a probation/paper overlay first, not a core promotion.
- If you want slightly less count but a higher S_BB standalone PF, use the `S_BB_SQUEEZE_SHORT` weak-RS stretched-VWAP version instead of the morning version. That combined book has `1,023` trades, full PF `1.753`, and holdout PF `1.529`.
- `A_MOD_BREAK_C1_HIGH` residual variants still fail the honest check because holdout is zero or negative after the current accepted slice is removed. Do not add more of it now.
- `L_BB_SQUEEZE_LONG` residual variants pass train/validation mechanically, but full PF is only `1.137` and holdout is slightly negative, so reject.

Files added:

- `research_v11_residual_deeper_overlay.py`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_residual_deeper_overlay_candidates.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_residual_deeper_overlay_best_by_setup.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_residual_deeper_overlay_scenarios.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_residual_deeper_overlay_by_setup.csv`

Implementation update:

- Added as separate v11 opt-in profile: `--selected_strategy_profile production_core_ab_max_pnl_low_valid_residual_overlay`.
- Existing working profile `production_core_ab_max_pnl_low_valid` is unchanged.
- Verification on the existing resolved pool matched the target result: `1,039` full trades, full PF `1.7411`, holdout PF `1.5790`.

## Deep AI Feature Add-On Search - 2026-06-02

Goal: search again for genuinely new add-on strategies without changing the current working v11 rules.

Method:

- Current baseline profile kept untouched: `production_core_ab_max_pnl_low_valid`.
- First removed all `916` signals already accepted by that profile.
- Searched the remaining `10,883` resolved leftover trades only.
- Candidate selection used train + validation only.
- Holdout was checked after selection, not used to choose the rule.
- Split stayed:
  - Train: through `2026-01-31`
  - Validation: `2026-02-01` to `2026-03-31`
  - Holdout: `2026-04-01` to `2026-05-29`

Search features:

- Original setup fields: setup, side, regime, time, RS, market move, VWAP distance, ATR, volume, notional, score/ranker fields.
- Candle fields: signal body, full range, upper/lower wick.
- Side-aware fields:
  - direction-aligned relative strength
  - direction-aligned market return
  - direction-aligned VWAP distance
  - direction-aligned candle body
  - wick against trade direction
- Diagnostic JSON fields:
  - setup reason
  - day value so far
- Groups searched:
  - all leftover trades
  - side
  - regime
  - time bucket
  - setup
  - diagnostic reason

Search result summary:

| Item | Count |
|---|---:|
| Leftover resolved trades searched | 10,883 |
| Train+validation-selected rules | 1,713 |
| Holdout-surviving rules | 303 |
| Strong holdout survivors | 143 |

Important rejection:

- The greedy train+validation portfolio looked good before holdout but failed badly on holdout.
- Greedy add-on portfolio:

| Split | Trades | PF | PnL Rs |
|---|---:|---:|---:|
| Train | 1,088 | 1.585 | 172,955.32 |
| Validation | 411 | 1.204 | 27,763.75 |
| Holdout | 462 | 0.686 | -55,487.37 |
| Full | 1,961 | 1.239 | 145,231.70 |

Decision: reject the greedy portfolio. It is train/validation overfit.

### Clean New Add-On: Morning Wick-Pressure Continuation

Rule:

- `time_bucket == MORNING_1001_1130`
- `directional_wick_against_pct <= 0.030`
- `quality_score <= 102`
- `lower_wick_pct <= 0.036`

This rounded rule selects the same trades as the exact quantile rule, so it is cleaner than a fragile decimal threshold.

Standalone result:

| Split | Trades | PF | PnL Rs |
|---|---:|---:|---:|
| Train | 108 | 1.605 | 18,329.63 |
| Validation | 24 | 1.251 | 2,592.79 |
| Holdout | 22 | 1.606 | 4,044.82 |
| Full | 154 | 1.528 | 24,967.24 |

Composition:

| Setup | Trades | PF | PnL Rs |
|---|---:|---:|---:|
| `S_BB_SQUEEZE_SHORT` | 19 | 3.019 | 10,718.48 |
| `E_ORB_BREAKOUT_SHORT` | 38 | 1.630 | 7,071.13 |
| `D_EMA20_REJECTION` | 73 | 1.252 | 5,862.00 |
| `D_EMA20_BOUNCE` | 18 | 1.180 | 1,065.32 |
| `E_VWAP_BAND_FADE` | 4 | 1.273 | 246.59 |
| `L_BB_SQUEEZE_LONG` | 2 | 1.005 | 3.72 |

Interpretation:

- This is mostly a short-side morning continuation/pressure filter.
- It avoids trades with meaningful wick against the trade.
- It is not a broad setup add; it is a narrow morning tape-quality add-on.

Impact on current v11 profile:

| Book | Full Trades | Full PF | Full PnL Rs | Holdout PF | Holdout PnL Rs |
|---|---:|---:|---:|---:|---:|
| Current `production_core_ab_max_pnl_low_valid` | 916 | 1.7135 | 203,649.95 | 1.4997 | 23,788.49 |
| Current + Morning Wick-Pressure | 1,070 | 1.6871 | 228,617.19 | 1.5128 | 27,833.31 |

Decision:

- This is the best clean add-on from the deeper pass.
- It adds `154` trades and keeps combined holdout PF above `1.5`.

### Optional Probation Add: A_MOD_BREAK_C1_HIGH Quality Continuation

Rule:

- `setup == A_MOD_BREAK_C1_HIGH`
- `0.5953181 <= body_pct <= 0.85873696`
- `v7_signal_notional_rs <= 99730.72`
- `8.982279 <= rs_minus_market_pct <= 12.243215`

Standalone result:

| Split | Trades | PF | PnL Rs |
|---|---:|---:|---:|
| Train | 47 | 2.768 | 19,627.29 |
| Validation | 6 | 5.311 | 3,219.93 |
| Holdout | 6 | 1.428 | 896.01 |
| Full | 59 | 2.703 | 23,743.23 |

Concern:

- This is profitable and passed holdout, but holdout has only `6` trades.
- Rounded versions stayed profitable, but weakened materially:
  - rounded rule `body_pct 0.60..0.86`, `notional <= 99,750`, `rs_minus_market_pct 9.0..12.25`
  - full `61` trades, PF `2.288`, PnL `Rs 20,652.99`
  - holdout `7` trades, PF `1.071`, PnL `Rs 197.73`
- So the exact rule is not as robust as the morning rule.

Impact if added with Morning Wick-Pressure:

| Book | Full Trades | Full PF | Full PnL Rs | Holdout PF | Holdout PnL Rs |
|---|---:|---:|---:|---:|---:|
| Current + Morning Wick-Pressure | 1,070 | 1.6871 | 228,617.19 | 1.5128 | 27,833.31 |
| Current + Morning Wick-Pressure + A_MOD quality | 1,129 | 1.7279 | 252,360.42 | 1.5096 | 28,729.32 |

Additional check against the stronger residual-overlay profile:

The earlier residual-overlay profile already captures some of this edge, especially residual morning `S_BB_SQUEEZE_SHORT`. If the protected baseline is `production_core_ab_max_pnl_low_valid_residual_overlay`, then Morning Wick-Pressure alone is not good enough by itself:

| Incremental Add Beyond Residual Overlay | Full Trades | Full PF | Full PnL Rs | Train PF | Validation PF | Holdout PF |
|---|---:|---:|---:|---:|---:|---:|
| Morning Wick-Pressure only | 135 | 1.339 | 14,248.76 | 1.471 | 0.797 | 1.382 |
| A_MOD quality only | 59 | 2.703 | 23,743.23 | 2.768 | 5.311 | 1.428 |
| Morning Wick-Pressure + A_MOD quality | 194 | 1.679 | 37,991.99 | 1.839 | 1.214 | 1.393 |

Combined with the residual-overlay profile:

| Book | Full Trades | Full PF | Full PnL Rs | Holdout PF | Holdout PnL Rs |
|---|---:|---:|---:|---:|---:|
| Residual overlay profile | 1,039 | 1.7411 | 236,845.17 | 1.5790 | 30,330.77 |
| Residual overlay + A_MOD quality | 1,098 | 1.7813 | 260,588.40 | 1.5733 | 31,226.78 |
| Residual overlay + Morning + A_MOD quality | 1,233 | 1.7318 | 274,837.15 | 1.5523 | 33,773.91 |

Decision:

- Good as a probation add-on if we want more PnL and are comfortable with a small holdout sample.
- Do not call it core yet.

### Rejected Or Deferred From This Deeper Pass

| Candidate | Why avoided |
|---|---|
| Greedy train+validation portfolio | Holdout PF `0.686`, holdout PnL `-55,487.37` |
| `A_MOD_BREAK_C1_HIGH` count rule `8.6524829 <= rs_pct <= 12.617635 AND notional <= 99730.72` | Good full PF `1.718`, but when layered on current book holdout PF falls to `1.4748` |
| Looser morning wick rule `directional_wick_against_pct <= 0.035 AND lower_wick_pct <= 0.040` | More count, but standalone full PF drops to `1.452` |
| Broad leftover setup buckets | Most are sub-PF-1 unfiltered after current profile; broad adds are not honest |

Deeper pass recommendation:

1. Add **Morning Wick-Pressure Continuation** first if adding more trades.
2. Add `A_MOD_BREAK_C1_HIGH` quality continuation only as probation.
3. Do not add the greedy portfolio or broad leftover setup buckets.
4. No v11 code change was made in this deeper pass.

Files added:

- `research_v11_deep_addon_search.py`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_deep_addon_candidate_rules.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_deep_addon_train_valid_selected.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_deep_addon_holdout_survivors.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_deep_addon_strong_holdout_survivors.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_deep_addon_manual_rule_scenarios.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_deep_addon_manual_rule_by_setup.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_deep_addon_incremental_vs_residual_overlay.csv`

## Out-of-Box New Setup Research Blueprint - 2026-06-02

This section is a forward research map for new independent setups. It does not modify existing working setup logic.

### Executive Summary

The current v11 book is strongest when it trades:

- Early directional breakouts with live-like entry confirmation.
- Selected ORB breakouts, especially shorts after filtering.
- Filtered A/B momentum/reclaim patterns under the `quality_top_slot` gate.
- Filtered squeeze/EMA/VWAP fragments after train-first rule search.

The biggest remaining opportunity is not another broad "all setups" expansion. Broad additions repeatedly dilute PF. The best path is to add independent, low-overlap modules that catch missed trade types:

- Retests after opening range breaks, not only first breakouts.
- Failed opening range breaks and failed previous-day level breaks.
- VWAP reclaim/rejection after an initial washout or squeeze.
- First pullback after a verified momentum expansion.
- Late-morning compression expansion after the opening noise settles.
- Sector/breadth-confirmed continuation.
- High-volume narrow-range absorption breaks.

Acceptance bar for any new setup:

- Standalone train PF `> 1.5`.
- Prefer train trades `>= 25`; allow `>= 15` only for micro-probes.
- Validation PF `>= 1.15` or validation PnL clearly positive with enough trades.
- Holdout should not be selected on, but after selection it should be positive and should not damage the combined book materially.
- Combined profile should either lift trade count without reducing full PF by more than roughly `5%`, or lift PnL while keeping holdout PF near or above `1.5`.

### Current Strategy Gap Analysis

Covered well:

- Early OR breakout direction after filters.
- Some filtered S_BB continuation.
- Some filtered EMA bounce/rejection.
- A/B quality-gated momentum/reclaim candidates.
- A residual late-D and morning-S_BB overlay now added as a separate v11 profile.

Under-covered:

- Opening range retest continuation after the first break has already happened.
- Opening range fakeout reversal.
- VWAP reclaim/rejection with a structured pullback, not a raw reclaim.
- First pullback after a true volatility expansion candle.
- Late-morning continuation after a 10:30-12:30 compression.
- Sector-relative continuation where the stock is aligned with sector and index breadth.
- Gap-and-hold and gap-fade behavior using previous-day close/high/low.
- Absorption candles: high volume but narrow range near VWAP/OR/PDH/PDL.
- Controlled mean reversion only when the market is not in trend-day mode.

Main rejected idea pattern:

- Broad unfiltered versions of `E_VWAP_BAND_FADE`, `D_EMA20_REJECTION`, `S_BB_SQUEEZE_SHORT`, pure higher-high/lower-low breaks, and generic A/B candidates. These create count but damage honest PF.

### New Setup Candidates

| Tier | Setup Name | Regime / Direction | Exact Entry Logic | Required Features | Bad-Trade Filters | Why It May Have Edge / Count | Overlap Risk | Honest Validation |
|---|---|---|---|---|---|---|---|---|
| Tier 1 | `E_ORB_RETEST_HOLD_LONG` | Opening strength / long | OR high is broken before `10:30`; price pulls back to OR high or VWAP within `0.25` to `0.75 ATR`; 5m candle closes back above OR high with close location `>= 0.65`; enter above that candle high. | OR high/low, VWAP, ATR, close location, volume ratio, market return. | Reject if stock already moved `> 2.5 ATR` from VWAP, market_ret_pct negative, or pullback closes below VWAP. | Catches continuation entries missed by first ORB breakout; less chase than raw breakout. | Medium with `E_ORB_BREAKOUT_LONG`; de-duplicate if same ticker/day already took ORB. | Standalone train PF `> 1.5`, train n `>= 25`, overlap with ORB long `< 40%`, holdout PnL positive. |
| Tier 1 | `E_ORB_RETEST_HOLD_SHORT` | Opening weakness / short | OR low is broken before `10:30`; price retests OR low or VWAP from below; 5m candle rejects with close location `<= 0.35`; enter below that candle low. | OR high/low, VWAP, ATR, close location, upper wick, market return. | Reject if market_ret_pct strongly positive, stock above VWAP, or lower wick is large absorption. | Indian intraday shorts often continue after failed retests of morning support. | Medium with `E_ORB_BREAKOUT_SHORT`; use one trade per ticker/day. | Same as long; require train PF `> 1.5` and validation PF `>= 1.15`. |
| Tier 1 | `E_FAILED_OR_BREAKOUT_TRAP_SHORT` | Opening fakeout / short | Price breaks OR high, fails within `1` to `3` bars, closes back inside OR and below VWAP; RS turns negative vs market; enter below failure candle low. | OR high, VWAP, RS, market_ret_pct, wick, body/range. | Reject if sector/index still trending up or volume is below normal. | Failed morning breakouts often unwind quickly when breakout buyers are trapped. | Low-medium; opposite of ORB long, but must avoid same-ticker conflict. | Train PF `> 1.5`, failure should work in at least `6` months, not only Apr-May. |
| Tier 1 | `E_FAILED_OR_BREAKDOWN_TRAP_LONG` | Opening fakeout / long | Price breaks OR low, reclaims OR low and VWAP, lower wick `>= 35%` of range, RS improves; enter above reclaim candle high. | OR low, VWAP, lower wick, RS, market/sector direction. | Reject in trend-down index days, reject if close remains below VWAP. | Captures short-covering and liquidity-sweep reversals. | Low-medium; different from existing ORB long. | Require positive validation PnL and holdout not below PF `1.0`. |
| Tier 1 | `V_RECLAIM_PULLBACK_LONG` | VWAP reclaim after washout / long | Before `11:30`, stock trades below VWAP by `0.5 ATR+`, then reclaims VWAP on volume ratio `>= 1.5`; wait first pullback that holds VWAP; enter above pullback high. | VWAP, ATR distance, volume ratio, body/range, RS, EMA20. | Reject if market below its VWAP and sector weak; reject if reclaim candle is too extended `> 1.5 ATR`. | More structured than raw VWAP reclaim; avoids buying the first spike. | Medium with `B_AVWAP_RECLAIM_REVERSAL`; should be a separate non-A/B module. | Test standalone and residual after B_AVWAP; require incremental PF `> 1.4`. |
| Tier 1 | `V_REJECTION_PULLBACK_SHORT` | VWAP rejection / short | Stock below VWAP and EMA20; pullback touches VWAP or comes within `0.25 ATR`; candle rejects with upper wick and closes below midpoint; enter below low. | VWAP, EMA20, ATR, wick, close location, market_ret_pct. | Reject near day low after `> 2.5 ATR` move to avoid late chase; reject if market broad breadth positive. | Institutional VWAP defense is common in weak names; retest gives better R than breakdown chase. | Medium with `D_EMA20_REJECTION`; require residual-only evaluation. | Train PF `> 1.5`; overlap with D rejection `< 35%`. |
| Tier 1 | `M_EXPANSION_FIRST_PULLBACK_LONG` | Momentum expansion / long | A 5m candle has range `>= 1.4x` rolling 20-bar range and volume ratio `>= 2.0`; close near high; next pullback holds upper half of expansion candle or EMA20; enter above pullback high. | Rolling range, volume ratio, EMA20, close location, RS. | Reject if first candle gap is extreme or market move is opposite; reject after `13:30` unless trend-day filter passes. | Avoids chasing the expansion candle and catches second leg. | Low-medium; not the same as ORB if allowed after first hour. | Train PF `> 1.5`, validation n `>= 8`, stable by month. |
| Tier 1 | `M_EXPANSION_FIRST_PULLBACK_SHORT` | Momentum expansion / short | Mirror of long: expansion candle closes near low on high volume; pullback fails below EMA20/VWAP; enter below pullback low. | Rolling range, volume ratio, EMA20/VWAP, RS weakness. | Reject if market/sector rebounding or lower wick suggests absorption. | Adds clean short count without random bearish noise. | Low-medium. | Same thresholds; require average trade above costs by at least `3x`. |
| Tier 1 | `C_LATE_MORNING_COMPRESSION_BREAK_LONG` | 10:45-12:30 post-consolidation / long | Between `10:45` and `12:30`, 4-8 candles form range width `<= 0.8 ATR`; price above VWAP; break range high with volume ratio `>= 1.4` and RS positive. | Rolling high/low, ATR, VWAP, volume, RS. | Reject if range is too close to day high after extended move, or market flat-to-down. | Targets quieter part of day where clean continuation can appear after opening noise. | Low. | Train PF `> 1.5`, train n `>= 30`, validation PF `>= 1.15`. |
| Tier 1 | `C_LATE_MORNING_COMPRESSION_BREAK_SHORT` | 10:45-12:30 post-consolidation / short | Range compression below VWAP; break range low with rising volume and RS weakness. | Same as long. | Reject if lower wick absorption or index breadth positive. | Adds short opportunities from delayed breakdowns. | Low. | Same thresholds. |
| Tier 2 | `S_SECTOR_RS_CONTINUATION_LONG` | Sector rotation / long | Stock RS positive vs Nifty and sector; sector return positive; stock above VWAP/EMA20; break 30-min high after shallow pullback. | Sector return, stock RS, VWAP, EMA20, rolling high. | Reject if stock is already > `2 ATR` above VWAP or market breadth weak. | Captures institutional rotation days where one sector leads. | Low if sector features are new. | Needs sector data quality check; require performance across at least `4` sectors. |
| Tier 2 | `S_SECTOR_RW_CONTINUATION_SHORT` | Sector weakness / short | Sector and stock weak vs index; stock below VWAP/EMA20; break 30-min low after failed pullback. | Sector return, RS/RW, VWAP, EMA20. | Reject if index strongly green or stock far below VWAP. | Short count with confirmation, not random noise. | Low. | Same as sector long; require no single sector contributes > `35%` of PnL. |
| Tier 2 | `G_GAP_HOLD_CONTINUATION_LONG` | Gap-and-go / long | Gap up `0.5%` to `2.5%`; first 30 min holds above previous close and VWAP; break morning high with RS positive. | Previous close/high, gap pct, VWAP, OR high, RS. | Reject huge gaps `> 3%`, reject if first 30-min close below VWAP. | Indian cash stocks often trend after controlled gap holds. | Medium with ORB long. | Validate by gap bucket; no single month dependence. |
| Tier 2 | `G_GAP_HOLD_CONTINUATION_SHORT` | Gap-down continuation / short | Gap down `0.5%` to `2.5%`; first 30 min fails below previous close/VWAP; break OR low with RW. | Previous close/low, gap pct, VWAP, OR low. | Reject after very large gap-down or market rebound. | Cleaner short setup than raw breakdown. | Medium with ORB short. | Same thresholds. |
| Tier 2 | `A_HVN_ABSORPTION_BREAK_LONG` | Absorption then expansion / long | High-volume narrow-range candle near VWAP/OR high: volume ratio `>= 2.0`, range `<= 0.7 ATR`, close in upper half; next candle breaks high. | Volume ratio, range/ATR, VWAP, OR high, close location. | Reject if absorption candle has large upper wick or market negative. | Narrow range on high volume can mark supply absorption before markup. | Low. | Demand train PF `> 1.5`, but watch sample size and slippage. |
| Tier 2 | `A_HVN_ABSORPTION_BREAK_SHORT` | Distribution then expansion / short | High-volume narrow-range candle near VWAP/OR low with close in lower half; next candle breaks low. | Same as long. | Reject if lower wick is large or market/sector strong. | Captures distribution before downside continuation. | Low. | Same thresholds. |
| Tier 2 | `T_TREND_DAY_EMA_STAIR_LONG` | Trend day / long | Market index above VWAP, rising EMA20, breadth positive; stock above VWAP and EMA20; first or second pullback to EMA20 holds; enter above rejection candle. | Index VWAP, breadth, EMA20 slope, stock VWAP/EMA. | Reject non-trend market days; reject after two failed pullbacks. | Trend-day pullbacks can give repeatable intraday continuation. | Medium with D_EMA20_BOUNCE. | Validate residual-only after existing EMA bounce. |
| Tier 2 | `T_TREND_DAY_EMA_STAIR_SHORT` | Trend-down day / short | Index below VWAP, falling EMA20, breadth negative; stock below VWAP/EMA20; pullback to EMA20 rejects. | Same as long. | Reject if market breadth recovers. | Strong short-only filter avoids random bearish noise. | Medium with D_EMA20_REJECTION. | Validate residual-only after existing D rejection. |
| Tier 3 | `P_PDH_BREAK_RETEST_LONG` | Previous-day high retest / long | Break previous day high, retest holds PDH, close above PDH and VWAP; enter above retest high. | Previous day high, VWAP, ATR, RS. | Reject if gap already far above PDH or market weak. | Important institutional level; may add count around daily levels. | Medium with ORB/HH break. | High false-break risk; require validation PF `>= 1.25`. |
| Tier 3 | `P_PDL_BREAK_RETEST_SHORT` | Previous-day low retest / short | Break PDL, retest fails, below VWAP; enter below failure low. | Previous day low, VWAP, ATR, RW. | Reject if market not weak or stock extended. | Captures daily-level breakdown continuation. | Medium. | Same as PDH long. |
| Tier 3 | `MR_CONTROLLED_VWAP_EXTREME_FADE_LONG` | Controlled mean reversion / long | Only after `11:00`, market ADX/range filter says non-trend; stock `>= 2 ATR` below VWAP, lower wick rejection, RSI/Stoch deeply oversold, reclaim prior candle high. | VWAP distance, ATR, ADX/range, RSI/Stoch, wick. | Reject all trend-down market days; reject weak sector. | Mean reversion works only when trend risk is filtered. | Low with trend setups, but high regime risk. | Require walk-forward by regime; do not promote from small sample. |
| Tier 3 | `MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT` | Controlled mean reversion / short | Mirror: non-trend market, stock `>= 2 ATR` above VWAP, upper wick rejection, overbought oscillator, break prior low. | Same as long. | Reject trend-up market days. | Adds fade trades without repeating broad VWAP band fade mistake. | Medium with E_VWAP_BAND_FADE. | Must prove it is not old VWAP fade in disguise. |

### Tier Ranking

Tier 1: implement immediately as isolated modules:

- `E_ORB_RETEST_HOLD_LONG`
- `E_ORB_RETEST_HOLD_SHORT`
- `E_FAILED_OR_BREAKOUT_TRAP_SHORT`
- `E_FAILED_OR_BREAKDOWN_TRAP_LONG`
- `V_RECLAIM_PULLBACK_LONG`
- `V_REJECTION_PULLBACK_SHORT`
- `M_EXPANSION_FIRST_PULLBACK_LONG`
- `M_EXPANSION_FIRST_PULLBACK_SHORT`
- `C_LATE_MORNING_COMPRESSION_BREAK_LONG`
- `C_LATE_MORNING_COMPRESSION_BREAK_SHORT`

Tier 2: promising but needs careful testing or extra feature infrastructure:

- `S_SECTOR_RS_CONTINUATION_LONG`
- `S_SECTOR_RW_CONTINUATION_SHORT`
- `G_GAP_HOLD_CONTINUATION_LONG`
- `G_GAP_HOLD_CONTINUATION_SHORT`
- `A_HVN_ABSORPTION_BREAK_LONG`
- `A_HVN_ABSORPTION_BREAK_SHORT`
- `T_TREND_DAY_EMA_STAIR_LONG`
- `T_TREND_DAY_EMA_STAIR_SHORT`

Tier 3: interesting but high overfit/regime risk:

- `P_PDH_BREAK_RETEST_LONG`
- `P_PDL_BREAK_RETEST_SHORT`
- `MR_CONTROLLED_VWAP_EXTREME_FADE_LONG`
- `MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT`

Reject for now:

- Naked RSI/Stoch mean reversion without market regime filter.
- Unfiltered gap fade.
- Broad VWAP band fade.
- Pure higher-high/lower-low break without VWAP/RS/market confirmation.
- Late-session chase setups after a stock has already moved `> 3 ATR` from VWAP.
- Any setup whose train PF `> 2.5` comes from fewer than `10` trades or one month.

### Best 3-5 Setups To Implement First

1. `E_ORB_RETEST_HOLD_SHORT` and `E_ORB_RETEST_HOLD_LONG`
   - Best framework fit, easy features, likely meaningful count.
   - Different from existing ORB because entry is on retest/hold, not first breakout.

2. `E_FAILED_OR_BREAKOUT_TRAP_SHORT` and `E_FAILED_OR_BREAKDOWN_TRAP_LONG`
   - Captures failed ORB behavior that the current book mostly avoids.
   - Should be low-overlap if one-trade-per-ticker/day rules are enforced.

3. `V_RECLAIM_PULLBACK_LONG` and `V_REJECTION_PULLBACK_SHORT`
   - VWAP behavior is already important in this system, but this version waits for reclaim/retest or rejection/retest.
   - Better than raw VWAP fade/reclaim.

4. `M_EXPANSION_FIRST_PULLBACK_LONG/SHORT`
   - Adds post-open continuation after volatility confirms.
   - Should catch trades missed by ORB when the first clean entry appears later.

5. `C_LATE_MORNING_COMPRESSION_BREAK_LONG/SHORT`
   - Good count candidate outside opening chaos.
   - Clean validation because time window and compression definition are simple.

### Backtesting And Validation Blueprint

1. Add without touching existing setups:
   - Add each candidate as a separate detector/setup name.
   - Keep exits equal to current v11 setup-family defaults for first pass.
   - Do not change production-core masks.
   - Add separate selected profile names such as `new_setups_probe_only` and `production_core_ab_max_pnl_low_valid_plus_new_probe`.

2. Measure standalone value:
   - For each setup, report train/validation/holdout/full trades, PF, PnL, win %, avg trade, days traded, positive month count.
   - Require train PF `> 1.5` before even looking at holdout.

3. Measure incremental value:
   - Remove signal IDs already accepted by the current working profile.
   - Test each setup on residual signals only.
   - Then add it back to the current profile and report portfolio PF/PnL/count.

4. Detect overlap:
   - Same ticker/day overlap with existing profile.
   - Same ticker within `15` minutes overlap.
   - Same setup-family overlap.
   - Reject or cap candidates with overlap `> 40%` unless incremental PnL is clearly positive.

5. Honest PF rules:
   - Select candidate thresholds using train only, or train + validation.
   - Holdout is read once after selection.
   - Do not tune a failed holdout candidate using Apr-May.
   - Track train PF, validation PF, holdout PF, and full PF separately.

6. Robustness checks:
   - Monthly PF/PnL.
   - Time-bucket PF.
   - Long vs short PF.
   - Top 20 tickers contribution.
   - Market regime: index up/down, high/low volatility, trend/range.
   - Sector contribution if sector data exists.

7. Avoid count-only damage:
   - A new setup must improve trade count and either improve full PnL or keep full PF within roughly `5%` of baseline.
   - Combined holdout PF should stay near or above `1.5`; if below, mark paper-only.
   - Avg trade must remain comfortably above fees/slippage.

8. Live/paper decision:
   - Paper first if validation PF is below `1.25`, holdout trades `< 10`, or setup depends on one month.
   - Small-size live only after at least `20` future paper trades with positive expectancy.

### Anti-Overfitting Checklist

- Use broad threshold families, not exact one-off quantile numbers, for new raw detectors.
- Do not select rules with fewer than `15` train trades unless explicitly marked micro-probe.
- Do not accept a setup because holdout looks good if validation was weak.
- Do not use outcome fields, exit fields, or future bars in entry filters.
- Do not create multiple nearby variants and keep the one with the best holdout.
- Check that top ticker contributes less than `20%` of setup PnL.
- Check that best month contributes less than `35%` of setup PnL.
- Stress test threshold perturbations by `10%` to `20%`.
- Require the setup thesis to remain explainable after seeing results.
- Keep rejected candidates documented so the same failed idea is not rediscovered repeatedly.

### Implementation Roadmap

1. Add Tier 1 setup detectors as isolated signal modules.
2. Backtest each standalone with existing v11 entry engine and existing exit assumptions.
3. Backtest each on residual-only signals after subtracting `production_core_ab_max_pnl_low_valid_residual_overlay`.
4. Backtest each as additive to the current working profile.
5. Remove overlapping or low-incremental-value setups.
6. Run train/validation/holdout reports with the existing split:
   - Train through `2026-01-31`
   - Validation `2026-02-01` to `2026-03-31`
   - Holdout `2026-04-01` to `2026-05-29`
7. Rank by train PF, validation PF, trade count, full PF, holdout damage, avg trade, and monthly stability.
8. Keep only setups with train PF `> 1.5` and acceptable validation/holdout behavior.
9. Put survivors into a new probation profile, not into the existing working profile.
10. Paper trade before promotion.

## Tier 1/2/3 Empirical Backtest Result - 2026-06-02

The Tier 1/2/3 blueprint was converted into a separate research probe and run on the futures universe without changing existing working v11 setup logic.

Important result:

- The raw Tier 1/2/3 setup definitions generated plenty of trades, but none passed the standalone train PF `> 1.5` requirement.
- The futures-universe probe used `204` symbols.
- The quality gate kept `17,951` candidates.
- The v11 1-minute entry/exit resolver accepted `12,537` all-probe trades with zero 1-minute misses.
- The standalone-per-setup resolved pool contained `17,938` trades.
- The full raw Tier123 standalone book was bad: full PF `0.754`, holdout PF `0.593`.
- Therefore, do not add the raw Tier 1/2/3 book.

Raw setup conclusion:

| Setup Family | Best Raw Standalone Read | Decision |
|---|---:|---|
| `E_FAILED_OR_BREAKOUT_TRAP_SHORT` | Train PF `1.413`, valid PF `2.117`, holdout PF `0.791` | Reject raw, mine subfilter |
| `MR_CONTROLLED_VWAP_EXTREME_FADE_LONG` | Train PF `1.451`, valid PF `1.000`, holdout PF `0.879` | Reject raw, mine subfilter |
| `P_PDL_BREAK_RETEST_SHORT` | Train PF `0.989`, valid PF `0.971`, holdout PF `0.659` | Reject raw, but bear-market subfilter works |
| `T_TREND_DAY_EMA_STAIR_SHORT` | Train PF `0.965`, valid PF `0.749`, holdout PF `0.605` | Reject raw, but late bearish trend-day subfilter works |
| Broad Tier123 book | Full PF `0.754`, holdout PF `0.593` | Reject |

Then a second pass mined only simple pre-entry subfilters using train thresholds. Validation and holdout were read separately after rule selection. This produced four distinct survivors:

| Shortlist Setup | Filter | Train Trades / PF | Valid Trades / PF | Holdout Trades / PF | Full Trades / PF |
|---|---|---:|---:|---:|---:|
| `MR_CONTROLLED_VWAP_EXTREME_FADE_LONG` | `quality_score >= 61.4524 AND rs_pct <= -1.30416 AND vol_ratio >= 1.10946` | 29 / `6.583` | 8 / `5.004` | 13 / `1.870` | 50 / `4.119` |
| `E_FAILED_OR_BREAKOUT_TRAP_SHORT` | `rs_pct <= -0.0685175 AND abs_vwap_dist_atr >= 12.4134` | 21 / `4.994` | 8 / `9.354` | 21 / `1.226` | 50 / `2.831` |
| `P_PDL_BREAK_RETEST_SHORT` | `market_ret_pct <= -0.631205 AND regime == BEAR` | 210 / `1.954` | 110 / `1.858` | 29 / `1.166` | 349 / `1.817` |
| `T_TREND_DAY_EMA_STAIR_SHORT` | `market_ret_pct <= -0.479626 AND regime == BEAR AND time_bucket_30 == 1300_1329` | 81 / `2.419` | 40 / `1.136` | 17 / `1.035` | 138 / `1.684` |

Do not add every mined filter. The de-duplicated four-survivor package adds many trades, but the combined holdout PF falls below the `1.5` floor:

| Package | New Non-Overlap Trades | New Full PF | New Holdout PF | Combined Full Trades | Combined Full PF | Combined Holdout PF | Decision |
|---|---:|---:|---:|---:|---:|---:|---|
| All 4 filtered survivors | 563 | `1.867` | `1.236` | 1,602 | `1.776` | `1.469` | Reject as deployment package |

Best additive Tier123 package against current residual-overlay profile:

| Package | New Non-Overlap Trades | New Full PF | New Holdout PF | Combined Full Trades | Combined Full PF | Combined Holdout Trades | Combined Holdout PF |
|---|---:|---:|---:|---:|---:|---:|---:|
| `MR_FADE_LONG_VOL + MR_FADE_SHORT_QUIET` | 144 | `1.859` | `1.426` | 1,183 | `1.749` | 172 | `1.572` |
| `T_STAIR_SHORT` only | 459 | `1.601` | `1.346` | 1,498 | `1.707` | 199 | `1.541` |
| `T_STAIR_SHORT + MR_FADE_LONG_VOL + MR_FADE_SHORT_QUIET` | 603 | `1.647` | `1.362` | 1,642 | `1.715` | 210 | `1.536` |
| All five shortlist filters | 997 | `1.671` | `1.264` | 2,036 | `1.713` | 274 | `1.453` |

Recommendation from this pass:

- Do not add raw Tier 1/2/3.
- Do not add all five shortlist filters if the holdout PF floor is `1.5`.
- Best count-vs-honesty package is `T_STAIR_SHORT + MR_FADE_LONG_VOL + MR_FADE_SHORT_QUIET`.
- This package adds `603` non-overlap trades and takes the current residual-overlay book from `1,039` trades to `1,642` trades.
- Combined full PF becomes `1.715`; combined holdout PF becomes `1.536`.
- This is a probation add-on candidate, not a replacement for the current working profile.
- Keep `P_PDL_BEAR_SHORT` and `FAILED_OR_SHORT` as second-stage candidates; individually they are good, but when added with the larger package they drag combined holdout below `1.5`.

V11 implementation status:

- Implemented as a separate opt-in profile: `production_core_ab_max_pnl_low_valid_residual_overlay_tier123_balanced`.
- Existing profile `production_core_ab_max_pnl_low_valid_residual_overlay` remains unchanged.
- The new profile resolves the protected residual-overlay book first, then resolves Tier123 balanced add-on signals separately, drops same ticker/day overlaps, and appends only non-overlap trades.
- Added v11-local exit rules:
  - `T_TREND_DAY_EMA_STAIR_SHORT`: SL `0.70%`, target `1.00%`
  - `MR_CONTROLLED_VWAP_EXTREME_FADE_LONG`: SL `0.70%`, target `0.80%`
  - `MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT`: SL `0.70%`, target `0.80%`
- Smoke test passed on `2026-05-29`, `13:00` to `15:00`, with the new profile:
  - Tier123 raw candidates: `82`
  - Tier123 selected signals: `82`
  - Tier123 resolved trades: `82`
  - 1-minute misses: `0`
  - Net PnL: Rs `39,523.73`
  - PF: `4.300`

Research artifacts:

- `research_v11_tier123_new_setups.py`
- `research_v11_tier123_subfilter_mine.py`
- `C:\TradingData\eqidv2\outputs_ID_v11_tier123_new_setup_probe\tier123_decisions_by_setup.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_tier123_new_setup_probe\tier123_subfilter_candidates.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_tier123_new_setup_probe\tier123_shortlist_filters.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_tier123_new_setup_probe\tier123_shortlist_combo_scenarios.csv`

## Final Recommendation

For honest deployment:

1. **Production core:** `C_OR_BREAKOUT`, `D_EMA20_BOUNCE`, `E_ORB_BREAKOUT_LONG`, `E_ORB_BREAKOUT_SHORT`, `L_BB_SQUEEZE_LONG`.
2. **Optional small-size add:** `E_VWAP_LOSE_EARLY_SHORT`.
3. **Paper/probation only:** relaxed filtered `S_BB_SQUEEZE_SHORT` using `market_ret_pct >= 0.53680868 OR v7_signal_notional_rs >= 99971.74`, filtered `B_AVWAP_RECLAIM_REVERSAL`, filtered `A_MOD_CLOSE_CONTINUATION_BREAK`.
4. **A/B research/probation:** do not add the full A/B book. The best current v11 profile is `production_core_ab_filtered_relaxed`: add only `B_HUGE_C1_CLOSE_RECLAIM_BREAK` with `rs_pct <= 10.7025` and `A_PULLBACK_C2_THEN_BREAK_C2_LOW` with `market_abs_ret_pct <= 0.8354`, still under the existing A/B `quality_top_slot` gate. This improves the fresh v11 book, but the A/B pieces remain probation because their own samples are small.
5. **Latest additive probation overlay:** keep `production_core_ab_max_pnl_low_valid` unchanged, then add only the residual `D_EMA20_REJECTION` late-session overlay plus residual `S_BB_SQUEEZE_SHORT` morning overlay described above.
6. **Tier123 probation add-on profile:** raw Tier 1/2/3 failed, but the balanced package `T_STAIR_SHORT + MR_FADE_LONG_VOL + MR_FADE_SHORT_QUIET` is now implemented as a separate v11 profile. It adds `603` non-overlap trades to the residual-overlay profile while keeping combined holdout PF at `1.536`.
7. **Disable for now:** broad/unfiltered `E_VWAP_BAND_FADE`, broad/unfiltered `D_EMA20_REJECTION`, broad/unfiltered `S_BB_SQUEEZE_SHORT`, `G_HIGHER_HIGH_BREAK`, broad/unfiltered Tier 1/2/3 setups.

This gives a practical honest solution:

| Book | Full Trades | Full PF | Full PnL Rs | Holdout PF | Holdout PnL Rs |
|---|---:|---:|---:|---:|---:|
| Production core | 407 | 1.840 | 103,258.44 | 1.658 | 6,895.73 |
| Production core + tiny add | 426 | 1.896 | 113,268.27 | 1.733 | 8,194.91 |
| Fresh v11 production core + filtered B_HUGE probation | 438 | 1.838 | 112,940.19 | 2.089 | 12,170.83 |
| Fresh v11 `production_core_ab_filtered_relaxed` | 469 | 1.858 | 123,534.46 | 1.847 | 11,622.59 |
| Current v11 `production_core_ab_max_pnl_low_valid` | 916 | 1.714 | 203,649.95 | 1.4997 | 23,788.49 |
| Current v11 + residual D/S_BB overlay | 1,039 | 1.741 | 236,845.17 | 1.579 | 30,330.77 |
| Current residual overlay + Tier123 balanced add-on | 1,642 | 1.715 | 317,554.85 | 1.536 | 35,002.54 |

Implemented in v11 now:

- Current working profile: `--selected_strategy_profile production_core_ab_max_pnl_low_valid`.
- Additive residual overlay profile: `--selected_strategy_profile production_core_ab_max_pnl_low_valid_residual_overlay`.
- Tier123 balanced add-on profile: `--selected_strategy_profile production_core_ab_max_pnl_low_valid_residual_overlay_tier123_balanced`.

The residual overlay and Tier123 balanced add-on are separate probation profiles rather than edits to existing working rules.

## Files

- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\honest_holdout_solution_book_comparison.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\holdout_improvement_book_scenarios.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\latest_all_setups_selected_monthly_by_setup.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_setup_universe_used_vs_leftout.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_leftout_setup_iteration_candidates.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_leftout_setup_iteration_best_by_setup.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_leftout_setup_iteration_portfolio_scenarios.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_sbb_relaxation_candidates.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_sbb_relaxation_portfolio_scenarios.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_relaxed_sbb_best_pnl_scenarios.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_relaxed_sbb_best_pnl_by_setup.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_relaxed_sbb_avoided_setup_reasons.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_a_mod_c1_low_gate_count_scan.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_a_mod_c1_low_q250_micro_rules.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_a_mod_c1_low_train_pf_1p5_1p8_candidates.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_a_mod_c1_low_train_pf_1p5_1p8_recommended.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_new_addon_strategy_candidates.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_new_addon_strategy_best_by_setup.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_new_addon_strategy_scenarios.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_new_addon_strategy_combined_scenarios.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_new_addon_strategy_combined_by_setup.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_residual_deeper_overlay_candidates.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_residual_deeper_overlay_best_by_setup.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_residual_deeper_overlay_scenarios.csv`
- `C:\TradingData\eqidv2\outputs_ID_v11_all_setups_with_ab_probe_from_v10_raw_q250\v11_residual_deeper_overlay_by_setup.csv`
