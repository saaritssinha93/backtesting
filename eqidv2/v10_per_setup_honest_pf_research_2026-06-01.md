# V10 Per-Setup Honest PF Research - 2026-06-01

## Method

Source run:

`C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\trades.csv`

Splits:

| Split | Dates |
|---|---|
| Train | through 2026-01-31 |
| Validation | 2026-02-01 to 2026-03-31 |
| Holdout/Test | 2026-04-01 to 2026-05-29 |

Rules were selected only from train plus validation. Holdout was used only as an audit.

Entry search used existing pre-trade/live-parity fields such as `ranker_score`, `quality_score`, `atr_pct`, `body_pct`, `close_loc`, `market_ret_pct`, `rs_pct`, `vol_ratio`, `vwap_dist_atr`, `signal_volume`, `signal_close`, `signal_minute`, `regime`, and related v7 signal fields.

Exit retunes were re-resolved on 1-minute bars, not estimated from old outcomes. Coarse grid only:

- SL: `0.70, 0.80, 0.90, 1.00, 1.20, 1.50`
- Target: `0.50, 0.60, 0.75, 1.00, 1.20, 1.50, 2.00`

Strict pass definition:

- Train PF >= 1.15
- Validation PF >= 1.15
- Full PF > 1.50
- Holdout trades >= 5
- Holdout PF >= 1.20

## Portfolio Result

| Scenario | Full Trades | Full PF | Full PnL Rs | Holdout Trades | Holdout PF | Holdout PnL Rs |
|---|---:|---:|---:|---:|---:|---:|
| Baseline all setups | 11,067 | 0.850 | -534,856.86 | 2,016 | 0.815 | -126,906.22 |
| Dev-selected best by setup | 1,085 | 1.340 | 118,602.34 | 198 | 0.947 | -4,287.89 |
| Strict OOS pass only | 265 | 1.662 | 55,672.36 | 38 | 1.925 | 11,423.71 |

Trade reduction versus baseline: 97.61%.

Important: the dev-selected portfolio looks profitable on full history, but holdout PF is below 1.0. I would not trade it. The strict OOS portfolio is the honest smaller system.

## Strict Survivors

| Setup | Side | Rule | Exit | Full Trades | Full PF | Full PnL Rs | Holdout Trades | Holdout PF |
|---|---|---|---|---:|---:|---:|---:|---:|
| E_ORB_BREAKOUT_SHORT | SHORT | `regime == NEUTRAL AND vol_ratio <= 2.8715297` | `SL=0.70,TGT=1.20` | 180 | 1.557 | 31,589.36 | 7 | 10.259 |
| S_BB_SQUEEZE_SHORT | SHORT | `vwap_dist_atr >= 33.288587 AND signal_volume >= 7421` | current | 85 | 1.879 | 24,083.00 | 31 | 1.425 |

The E_ORB_BREAKOUT_SHORT current-exit version also passed strict OOS:

| Setup | Rule | Exit | Full Trades | Full PF | Full PnL Rs | Holdout PF |
|---|---|---|---:|---:|---:|---:|
| E_ORB_BREAKOUT_SHORT | `regime == NEUTRAL AND vol_ratio <= 2.8715297` | current | 180 | 1.623 | 35,225.02 | 8.987 |

If I were turning this into a production candidate, I would seriously consider using the current exit for E_ORB_BREAKOUT_SHORT first because it avoids extra exit tuning and still passes the same train/validation/holdout audit.

## Setup-by-Setup Verdict

| Setup | Best Honest Attempt | Full PF | Holdout PF | Decision |
|---|---|---:|---:|---|
| C_OR_BREAKOUT | Baseline already PF 1.792, but validation had only 4 trades and holdout had 0 trades | 1.792 | n/a | Reject as unproven |
| D_EMA20_BOUNCE | `signal_close >= 1414.55 AND vol_ratio <= 1.9291683`, `SL=0.70,TGT=1.50` | 1.228 | 0.721 | Reject |
| D_EMA20_REJECTION | `body_pct >= 0.94593834 AND signal_close <= 1242.7`, current exit | 1.547 | 0.939 | Reject despite full PF > 1.5 |
| E_ORB_BREAKOUT_LONG | all signals, `SL=0.90,TGT=0.75` | 1.006 | 0.490 | Reject |
| E_ORB_BREAKOUT_SHORT | `regime == NEUTRAL AND vol_ratio <= 2.8715297` | 1.557 | 10.259 | Pass |
| E_VWAP_BAND_FADE | `signal_minute <= 705 AND signal_volume >= 29626.75`, `SL=0.80,TGT=1.00` | 1.200 | 0.847 | Reject |
| E_VWAP_LOSE_EARLY_SHORT | No stable train/validation edge under stricter minimum sample | n/a | n/a | Reject |
| G_HIGHER_HIGH_BREAK | No stable train/validation edge under stricter minimum sample | n/a | n/a | Reject |
| L_BB_SQUEEZE_LONG | `body_pct <= 0.80476395 AND signal_close <= 1007.1`, current exit | 1.414 | 0.571 | Reject |
| S_BB_SQUEEZE_SHORT | `vwap_dist_atr >= 33.288587 AND signal_volume >= 7421`, current exit | 1.879 | 1.425 | Pass |

## Honest Conclusion

I could not honestly make every setup PF > 1.5. The setups that failed holdout should not be kept just because train/validation or full-history numbers look attractive.

The profit-table version is:

1. Keep `E_ORB_BREAKOUT_SHORT` only when `regime == NEUTRAL AND vol_ratio <= 2.8715297`.
2. Keep `S_BB_SQUEEZE_SHORT` only when `vwap_dist_atr >= 33.288587 AND signal_volume >= 7421`.
3. Disable the other setups for now.

This gives a much smaller system: 265 trades instead of 11,067, PF 1.662 instead of 0.850, and full PnL Rs +55,672 instead of Rs -534,857.

## Files

- `research_v10_per_setup_honest.py`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\per_setup_honest_logic_summary.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\per_setup_option_results.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\per_setup_exit_grid_results.csv`
- `C:\TradingData\eqidv2\outputs_ID_v10_full_historical_v7_live_logic\per_setup_honest_portfolio_scenarios.csv`
