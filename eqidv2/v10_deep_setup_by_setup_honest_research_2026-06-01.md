# V10 Deep Setup-by-Setup Honest Research - 2026-06-01

## What Changed In This Pass

This was a deeper setup-by-setup search than the prior pass.

I added derived pre-trade features:

- Signal candle body/range/wicks
- Absolute market move and RS move
- RS minus market move
- Absolute VWAP distance
- SL/target ratio
- Candle color
- Time bucket

Logic searched:

- Single conditions
- Double `AND`
- Double `OR`
- Triple `AND`
- Coarse exit-grid retunes for the top train/validation candidates only

The holdout stayed untouched for selection:

| Split | Dates |
|---|---|
| Train | through 2026-01-31 |
| Validation | 2026-02-01 to 2026-03-31 |
| Holdout/Test | 2026-04-01 to 2026-05-29 |

Strict pass still means:

- Train PF >= 1.15
- Validation PF >= 1.15
- Full PF > 1.50
- Holdout trades >= 5
- Holdout PF >= 1.20

## Portfolio-Level Result

| Scenario | Full Trades | Full PF | Full PnL Rs | Holdout Trades | Holdout PF | Holdout PnL Rs |
|---|---:|---:|---:|---:|---:|---:|
| Baseline all setups | 11,067 | 0.850 | -534,856.86 | 2,016 | 0.815 | -126,906.22 |
| Deep dev-selected by setup | 975 | 1.439 | 124,062.86 | 182 | 0.893 | -6,952.38 |
| Deep strict selected pass only | 186 | 1.766 | 46,144.30 | 12 | 1.650 | 3,113.12 |

The important honest point: the deep dev-selected portfolio improved full-history PF but failed holdout. So it is not tradable as a system.

## Setup-by-Setup Selected Result

These are the rules selected by train/validation scoring only. Holdout was used only afterward to accept/reject.

| Setup | Selected Rule | Exit | Full Trades | Full PF | Holdout Trades | Holdout PF | Decision |
|---|---|---|---:|---:|---:|---:|---|
| C_OR_BREAKOUT | No stable dev edge | n/a | 0 | n/a | 0 | n/a | Reject |
| D_EMA20_BOUNCE | `close_loc <= 0.79998779 AND body_pct <= 0.56521422 AND market_abs_ret_pct <= 0.57825513` | current | 117 | 1.357 | 43 | 0.793 | Reject |
| D_EMA20_REJECTION | `body_pct >= 0.78761227 AND vwap_dist_abs_atr <= 12.95657 AND upper_wick_pct <= 0.014292285` | current | 83 | 1.653 | 7 | 0.649 | Reject |
| E_ORB_BREAKOUT_LONG | No stable dev edge | n/a | 0 | n/a | 0 | n/a | Reject |
| E_ORB_BREAKOUT_SHORT | `market_ret_pct >= -0.63438346 AND quality_score >= 97.873364 AND upper_wick_pct <= 0.014647435` | `SL=0.80,TGT=1.50` | 186 | 1.766 | 12 | 1.650 | Pass |
| E_VWAP_BAND_FADE | `signal_body_ret_pct <= -0.42471267 AND signal_minute <= 715 AND atr_pct >= 0.0025679186` | current | 396 | 1.096 | 90 | 0.835 | Reject |
| E_VWAP_LOSE_EARLY_SHORT | No stable dev edge | n/a | 0 | n/a | 0 | n/a | Reject |
| G_HIGHER_HIGH_BREAK | `signal_close <= 1684.2 AND close_loc >= 0.62185993 AND ranker_score <= 0.48827` | current | 70 | 1.527 | 6 | 2.045 | Reject: validation PF 1.091 |
| L_BB_SQUEEZE_LONG | `lower_wick_pct >= 0.058816109 AND v7_signal_notional_rs >= 99000.72 AND quality_score >= 50.526557` | current | 47 | 1.498 | 8 | 0.600 | Reject |
| S_BB_SQUEEZE_SHORT | `(lower_wick_pct >= 0.073751386 OR signal_minute <= 710) AND rs_abs_pct <= 1.971159` | `SL=0.80,TGT=1.00` | 76 | 2.297 | 16 | 0.852 | Reject |

## Strict Survivor

Only one deep-selected setup passed the full honest audit:

| Setup | Rule | Exit | Train PF | Valid PF | Holdout PF | Full PF | Full PnL Rs |
|---|---|---|---:|---:|---:|---:|---:|
| E_ORB_BREAKOUT_SHORT | `market_ret_pct >= -0.63438346 AND quality_score >= 97.873364 AND upper_wick_pct <= 0.014647435` | `SL=0.80,TGT=1.50` | 1.772 | 1.883 | 1.650 | 1.766 | 46,144.30 |

This is a better E_ORB_BREAKOUT_SHORT rule than the previous pass, but the holdout sample is still only 12 trades. Good candidate, not a holy grail.

## What Failed Honestly

Several setups can be made to show attractive full-history PF, but still failed the audit:

| Setup | Full PF | Why Rejected |
|---|---:|---|
| D_EMA20_REJECTION | 1.653 | Holdout PF only 0.649 |
| G_HIGHER_HIGH_BREAK | 1.527 | Validation PF only 1.091 |
| S_BB_SQUEEZE_SHORT | 2.297 | Holdout PF only 0.852 |
| L_BB_SQUEEZE_LONG | 1.498 | Just below full PF target and holdout PF only 0.600 |

This is the main lesson from the deeper pass: optimizing the indicator logic harder improves the in-sample story, but most setups do not survive the recent holdout.

## Research Leads Only

These candidates passed the numerical strict audit, but they were not the single selected rule for their setup under the pre-declared train/validation scoring. I would not treat them as production rules without fresh unseen data.

| Setup | Rule | Exit | Full Trades | Full PF | Holdout Trades | Holdout PF |
|---|---|---|---:|---:|---:|---:|
| L_BB_SQUEEZE_LONG | `lower_wick_pct >= 0.058816109 AND v7_signal_notional_rs >= 99000.72 AND v7_signal_notional_rs <= 99872.643` | current | 53 | 1.715 | 7 | 1.328 |
| S_BB_SQUEEZE_SHORT | `lower_wick_pct >= 0.073751386 AND ranker_score >= 0.3169858 AND signal_volume >= 11802` | current | 74 | 1.982 | 18 | 1.312 |
| S_BB_SQUEEZE_SHORT | `atr_pct <= 0.0030712949 AND market_abs_ret_pct >= 0.20230961 AND signal_volume >= 11802` | `SL=0.80,TGT=0.75` | 83 | 2.138 | 18 | 1.475 |

These are useful for future research, but using holdout to pick them now would be leakage.

## Honest Conclusion

I cannot honestly make every setup PF > 1.5.

The strongest production-grade conclusion from this deeper pass is:

1. Keep improving only `E_ORB_BREAKOUT_SHORT`.
2. Disable `E_VWAP_BAND_FADE` unless a fresh future OOS confirms it. It remains the biggest risk.
3. Treat `S_BB_SQUEEZE_SHORT` as promising but unstable. The previous simpler filter passed; the deeper optimized version overfit.
4. Treat `L_BB_SQUEEZE_LONG` and `G_HIGHER_HIGH_BREAK` as research leads, not live candidates.
5. Do not force weak setups into the system just to fill a table.

## Probation Update

Per follow-up, `D_EMA20_REJECTION`, `G_HIGHER_HIGH_BREAK`, and `S_BB_SQUEEZE_SHORT` are not rejected outright. They are reclassified as probation candidates.

The honest interpretation is:

- They may be useful.
- They did not pass the strict setup-level OOS audit.
- They can be kept in a paper-trade/probation book, not merged into the strict production book yet.

### Probation Basket

Basket:

1. Strict `E_ORB_BREAKOUT_SHORT`
2. Probation `D_EMA20_REJECTION`
3. Probation `G_HIGHER_HIGH_BREAK`
4. Probation `S_BB_SQUEEZE_SHORT`

| Basket | Split | Trades | PF | PnL Rs | Win % |
|---|---|---:|---:|---:|---:|
| Strict only | Full | 186 | 1.766 | 46,144.30 | 53.76 |
| Strict only | Holdout | 12 | 1.650 | 3,113.12 | 50.00 |
| Strict + D/G/S probation | Full | 415 | 1.794 | 94,872.36 | 56.39 |
| Strict + D/G/S probation | Holdout | 41 | 1.176 | 2,681.46 | 43.90 |

This is the trade-off: adding the three probation setups increases full-history PnL and trade count, but lowers holdout quality. It is not a disaster; holdout is still positive. But it fails the original strict holdout PF >= 1.20 threshold.

### Probation Rules

| Setup | Rule | Exit | Full Trades | Full PF | Holdout Trades | Holdout PF |
|---|---|---|---:|---:|---:|---:|
| D_EMA20_REJECTION | `body_pct >= 0.78761227 AND vwap_dist_abs_atr <= 12.95657 AND upper_wick_pct <= 0.014292285` | current | 83 | 1.653 | 7 | 0.649 |
| G_HIGHER_HIGH_BREAK | `signal_close <= 1684.2 AND close_loc >= 0.62185993 AND ranker_score <= 0.48827` | current | 70 | 1.527 | 6 | 2.045 |
| S_BB_SQUEEZE_SHORT | `(lower_wick_pct >= 0.073751386 OR signal_minute <= 710) AND rs_abs_pct <= 1.971159` | `SL=0.80,TGT=1.00` | 76 | 2.297 | 16 | 0.852 |

Practical recommendation:

- Strict book: only `E_ORB_BREAKOUT_SHORT`.
- Probation book: add `D_EMA20_REJECTION`, `G_HIGHER_HIGH_BREAK`, and `S_BB_SQUEEZE_SHORT`.
- Promote a probation setup only after fresh future paper/live data confirms PF > 1.2 to 1.5 without new tuning.

## Files

- `research_v10_setup_deep_dive.py`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\deep_setup_final_by_setup.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\deep_setup_option_results.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\deep_setup_exit_grid_results.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\deep_setup_portfolio_scenarios.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\deep_setup_probation_baskets_D_G_S.csv`
