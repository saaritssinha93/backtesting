# V23 fixed 1.50% baseline and trailing study through 2026-11-05

**The fixed 1.50% SL is now the V23 backtesting baseline, as requested by the user.** That adoption does not depend on whether a new trailing alternative qualifies. The baseline keeps the original per-setup target and the 15:15 time exit, with no automatic two-hour tightening. LONG stop = actual entry × 0.985; SHORT stop = actual entry × 1.015.

All **790 entries across 66 completed sessions** from 2026-07-29 through 2026-11-05 remain. Baseline net profit after modelled costs is **Rs 1,220,580.55**. Entry identities, fill prices, times and exposure are preserved in every comparison.

**No trailing alternative qualified for replacement.** The adopted fixed 1.50% baseline remains the current V23 backtest policy. Alternatives are retained as research comparisons; no improvement is claimed merely from selecting a different benchmark.

Recorded qualification checks: No fit44 trailing model improved losses while preserving fixed1.50% baseline profit and drawdown..

## Full-history comparison

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| fixed_1p50 | 790 | 540 | 250 | 68.35% | 1,220,580.55 | 837,339.00 | 33,894.47 |
| fixed_1p50_runner | 790 | 474 | 316 | 60.00% | 1,149,944.81 | 1,100,260.35 | 83,358.11 |
| breakeven_1r_runner | 790 | 458 | 304 | 57.97% | 1,154,645.37 | 1,037,940.59 | 76,891.09 |
| step_r_runner | 790 | 465 | 304 | 58.86% | 1,187,447.09 | 1,037,940.59 | 76,891.09 |
| chandelier3_runner | 790 | 532 | 258 | 67.34% | 997,675.97 | 862,691.75 | 38,101.29 |
| swing_be_runner | 790 | 524 | 265 | 66.33% | 1,008,005.46 | 877,834.82 | 54,029.96 |
| ema20_runner | 790 | 526 | 264 | 66.58% | 968,390.25 | 872,280.30 | 53,122.37 |
| hybrid_runner | 790 | 465 | 304 | 58.86% | 1,164,768.76 | 1,037,940.59 | 76,891.09 |
| hybrid_native_target | 790 | 540 | 249 | 68.35% | 1,196,385.98 | 836,376.41 | 33,894.47 |
| hybrid_adaptive_runner | 790 | 351 | 346 | 44.43% | 368,280.24 | 788,238.48 | 24,305.80 |

Loss amount is the absolute sum of negative after-cost trade P&L before offsetting winners. Flat trades are neither wins nor losses; win rate uses all fills. Daily-close drawdown is not intraday drawdown.

## Expectancy and payoff

| Policy | Mean net/trade (Rs) | Median net/trade (Rs) | Profit factor | Mean winner (Rs) | Mean losing amount (Rs) |
|:--|--:|--:|--:|--:|--:|
| fixed_1p50 | 1,545.04 | 2,550.00 | 2.46 | 3,810.96 | 3,349.36 |
| fixed_1p50_runner | 1,455.63 | 1,027.84 | 2.05 | 4,747.27 | 3,481.84 |
| breakeven_1r_runner | 1,461.58 | 851.53 | 2.11 | 4,787.31 | 3,414.28 |
| step_r_runner | 1,503.10 | 922.46 | 2.14 | 4,785.78 | 3,414.28 |
| chandelier3_runner | 1,262.88 | 1,980.15 | 2.16 | 3,496.93 | 3,343.77 |
| swing_be_runner | 1,275.96 | 1,726.25 | 2.15 | 3,598.93 | 3,312.58 |
| ema20_runner | 1,225.81 | 1,717.81 | 2.11 | 3,499.37 | 3,304.09 |
| hybrid_runner | 1,474.39 | 922.46 | 2.12 | 4,737.01 | 3,414.28 |
| hybrid_native_target | 1,514.41 | 2,550.00 | 2.43 | 3,764.37 | 3,358.94 |
| hybrid_adaptive_runner | 466.18 | 0.00 | 1.47 | 3,294.93 | 2,278.15 |

## What is being changed

- **Target-removal control:** `fixed_1p50_runner` keeps the 1.50% stop and removes the native profit target from entry. It measures the effect of holding for the rest of the session without a target. Other runner models also remove the native target from entry; a target is never cancelled retrospectively after it was hit.
- **Native-target control:** `hybrid_native_target` retains the original target while adding hybrid management. Original targets below 1R can exit before the 1R breakeven stage activates. Activation counts below make an inactive or rarely active control visible.
- **R definition:** fixed-risk arms start with R = 1.50% of actual entry. The adaptive hybrid uses its own initial stop distance as R. The latter changes initial risk as well as trade management, so it is a separate combined experiment, not evidence about trailing alone.
- **Cost-covered breakeven:** a +0.05% entry-relative stop offsets the modelled 5-bps round-trip cost at an exact fill. Gaps or additional slippage can still produce a loss.
- **Causal features:** ATR14 uses the completed execution-path true-range prefix, with confirmed entry ATR during warm-up. EMA indicators are confirmation-path anchored; they are not asserted to be full-session or overnight indicators. A swing pivot needs two left and two completed right bars, and its protective stop starts only after confirmation.
- **Execution timing:** stage triggers use completed post-entry closes. Entry-bar extrema cannot activate a trail. Stops ratchet only in the favorable direction and new decisions apply from the next bar. All positions still exit by 15:15.

- **fixed_1p50:** User-adopted baseline: fixed1.50% SL from entry with native target.
- **fixed_1p50_runner:** Control: fixed1.50% SL, original target disabled from entry; hold until stop or15:15.
- **breakeven_1r_runner:** No fixed target. After a completed post-entry close reaches+1R, set entry-relative+0.05% cost-covered stop from next bar.
- **step_r_runner:** No fixed target. Completed-close stages:1R=>+0.05% entry return;1.5R=>+0.5R;2R=>+1R. Stops only tighten.
- **chandelier3_runner:** No fixed target. After completed close>=0.5R, trail post-entry favorable extreme by3 ATR; next-bar activation and favorable ratchet only.
- **swing_be_runner:** No fixed target. After completed close>=0.5R, trail behind confirmed post-entry2-left/2-right swing by0.25 ATR; add cost-covered breakeven at1R.
- **ema20_runner:** No fixed target. Arm after a completed close>=0.5R; two completed adverse EMA20 closes schedule next-open exit.
- **hybrid_runner:** No fixed target. Use R steps; after2R, directional EMA9/EMA20 alignment and signedER10>=0.30 permit a ratcheted3ATR or confirmed-swing stop.
- **hybrid_native_target:** Same fixed1.50% hybrid but retain original target, isolating target-removal effects.
- **hybrid_adaptive_runner:** Separate compound change: initial risk from opposing completed confirmation edge plus0.25 confirmed ATR, clipped0.40–1.00%; invalid edge falls back1.50%. No fixed target; same hybrid. R uses the actual initial distance.

## Period checks

Fitting-period candidate: **fixed_1p50**. Current policy: **fixed_1p50**. Diagnostic comparison: **hybrid_native_target**. Selection uses only the first44 sessions through 5 October: net profit at least the fixed1.50 baseline, lower total loss amount and no worse daily-close drawdown. The one selected candidate is checked separately on the next15 sessions through 27 October, final7 through 5 November, and the full history. A failed candidate is not replaced using later results. All periods were previously observed in research and are retrospective checks, not an untouched forward holdout.

### First44: selection

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| fixed_1p50 | 496 | 358 | 138 | 72.18% | 910,214.75 | 433,544.39 | 11,888.86 |
| hybrid_native_target | 496 | 358 | 137 | 72.18% | 886,020.18 | 432,581.80 | 11,888.86 |

### Next15: validation

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| fixed_1p50 | 191 | 121 | 70 | 63.35% | 219,768.93 | 248,354.66 | 33,894.47 |
| hybrid_native_target | 191 | 121 | 70 | 63.35% | 219,768.93 | 248,354.66 | 33,894.47 |

### Final7: later check

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| fixed_1p50 | 103 | 61 | 42 | 59.22% | 90,596.87 | 155,439.95 | 26,246.72 |
| hybrid_native_target | 103 | 61 | 42 | 59.22% | 90,596.87 | 155,439.95 | 26,246.72 |

### 5 November only

| Policy / period | Trades | Wins | Losses | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|
| fixed_1p50 | 19 | 11 | 8 | 57.89% | 28,300.36 | 23,140.81 | 0.00 |
| hybrid_native_target | 19 | 11 | 8 | 57.89% | 28,300.36 | 23,140.81 | 0.00 |

## Original winners and losers

The labels below are post-replay diagnostics against the fixed1.50 baseline. They never determine which individual trade receives a trailing rule.

| Policy | Baseline winners worsened | Baseline losers improved | Winners became losses | Losers became wins | Winner P&L change (Rs) | Loser P&L change (Rs) |
|:--|--:|--:|--:|--:|--:|--:|
| fixed_1p50 | 0 | 0 | 0 | 0 | +0.00 | +0.00 |
| fixed_1p50_runner | 191 | 0 | 66 | 0 | -70,635.73 | +0.00 |
| breakeven_1r_runner | 193 | 1 | 55 | 0 | -66,897.78 | +962.60 |
| step_r_runner | 196 | 1 | 55 | 0 | -34,096.06 | +962.60 |
| chandelier3_runner | 263 | 7 | 15 | 7 | -277,235.17 | +54,330.59 |
| swing_be_runner | 275 | 7 | 22 | 7 | -265,274.15 | +52,699.06 |
| ema20_runner | 278 | 7 | 21 | 7 | -305,259.97 | +53,069.67 |
| hybrid_runner | 193 | 1 | 55 | 0 | -56,774.38 | +962.60 |
| hybrid_native_target | 2 | 1 | 0 | 0 | -25,157.16 | +962.60 |
| hybrid_adaptive_runner | 385 | 158 | 136 | 10 | -1,236,221.67 | +383,921.36 |

## Activation and exit diagnostics

| Policy | Trades with activation | Stop updates | Changed exits | Exit followed by later native-target touch | Target mode |
|:--|--:|--:|--:|--:|:--|
| fixed_1p50 | 0 | 0 | 0 | 2 | native |
| fixed_1p50_runner | 0 | 0 | 346 | 4 | runner |
| breakeven_1r_runner | 166 | 166 | 347 | 10 | runner |
| step_r_runner | 166 | 282 | 347 | 19 | runner |
| chandelier3_runner | 395 | 3,369 | 416 | 254 | runner |
| swing_be_runner | 395 | 3,514 | 413 | 238 | runner |
| ema20_runner | 395 | 0 | 410 | 242 | runner |
| hybrid_runner | 166 | 446 | 347 | 43 | runner |
| hybrid_native_target | 38 | 62 | 3 | 4 | native |
| hybrid_adaptive_runner | 416 | 1,987 | 709 | 295 | runner |

| Policy | Mean session MFE upper bound | Mean opportunity capture |
|:--|--:|--:|
| fixed_1p50 | 1.12% | 42.09% |
| fixed_1p50_runner | 1.12% | 37.81% |
| breakeven_1r_runner | 1.12% | 37.25% |
| step_r_runner | 1.12% | 37.50% |
| chandelier3_runner | 1.12% | 36.88% |
| swing_be_runner | 1.12% | 36.19% |
| ema20_runner | 1.12% | 35.61% |
| hybrid_runner | 1.12% | 37.53% |
| hybrid_native_target | 1.12% | 41.99% |
| hybrid_adaptive_runner | 1.12% | 22.73% |

Positive gross realized price return divided by the full-session favorable-excursion upper bound from the entry bar through15:15. Entire entry-bar extremes are included and may precede entry. This common ex-post OHLC bound is never an entry or exit signal.

A later original-target touch is only a retrospective diagnostic. It does not establish that a trade could safely have waited through the intervening adverse prices. Session MFE and any capture ratios use future prices only after the replay for research reporting; they are never inputs to an entry or exit decision. One-minute OHLC gives bounds rather than exact intrabar opportunity capture. Full diagnostic fields are saved in [diagnostics.csv](diagnostics.csv).

The diagnostic candidate increases daily net P&L on 1 days and decreases it on 2 days. Every session remains listed, including unchanged and zero-trade sessions.

## Scope and limits

- Fixed exposure Rs500,000 per trade, Rs100,000 capital at5x, unlimited concurrent capital and Rs250 modelled round-trip cost per fill. No extra leverage, entries or partial exits are used to manufacture an improvement. Extra changed-exit slippage is reported separately.
- The 1.50% initial stop permits Rs7,500 nominal price loss before Rs250 costs per fixed-size trade; an adverse opening gap can exceed that amount. The adaptive arm has a separately declared lower initial-risk range and a 1.50% invalid-structure fallback.
- All73 configured pairs through14:00 remain enabled, with final confirmation at14:01. Original pairs retain cap3; new pairs retain cap1 and their existing volume gate.
- Inherited conservative entry-bar stop-first and adverse-open fills remain. One-minute OHLC does not reveal true intrabar ordering, bid/ask fills or queue position.
- The frozen paths contain OHLC rather than a verified volume/order-flow/VIX series. This study does not claim to implement VWAP, CVD, OFI or VIX-based management from those paths.
- LIVE execution, approvals, scheduling and broker order handling are unchanged. No claim about broker-native trailing-stop support is needed for this offline backtest.
- All 218 stock sources contain all 360 one-minute bars from 09:16 through the strategy exit cutoff 15:15 on 5 November.
- 217 stocks are eligible in the sealed feature ledger. SAIL is excluded for the day because its 09:20 futures OI observation is missing.
- KAYNES is excluded only from 09:20 opening setups for missing opening OI evidence; later setups retain valid source data.
- All 73 setup/direction pairs through 14:00 are evaluated using the same sealed snapshot; all stock futures use November contracts.
- October 1 remains excluded from older history by the immutable source. No November 6 intraday data are included.
- Inherited earlier-source limits remain: some archived cohorts use after-dated membership/full-day eligibility; October 30 reconstructed opening OI does not prove contemporaneous availability.

[Current day-wise results](DAY_WISE_RESULTS.md) · [Current CSV](day_wise_results.csv) · [Candidate day-wise results](CANDIDATE_DAY_WISE_RESULTS.md) · [Candidate CSV](candidate_day_wise_results.csv)

[All totals](overall.csv) · [All daily rows](daily.csv) · [Trade changes](trade_changes.csv) · [Trade effect summary](trade_effect_summary.csv) · [Stop updates](stop_updates.csv) · [Extra-slippage sensitivity](cost_stress.csv) · [Adopted baseline trades](baseline_trades.csv) · [Selected trades](selected_trades.csv) · [Configuration](config.json)
