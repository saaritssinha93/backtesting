# B_HUGE_C1_CLOSE_RECLAIM_BREAK Optimization Report

Status: **NOT SELECTED**

## Windows

- FIT: 2026-05-29..2026-06-04 (2026-05-29, 2026-06-01, 2026-06-02, 2026-06-03, 2026-06-04)
- VAL: 2026-06-05..2026-06-11 (2026-06-05, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11)
- TRAIN: 2026-05-29..2026-06-11
- TEST: 2026-06-12..2026-06-24

TEST was evaluated only after the FIT/VAL search loop finished.

## Engine

- Trials run: 300 of requested 300
- Search engine: Optuna TPE
- Best FIT/VAL score: 0.476443
- Command: `python Train_and_Test\optimize_setup_card_loop.py --setup B_HUGE_C1_CLOSE_RECLAIM_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_C1_CLOSE_RECLAIM_BREAK --trials 300 --time_budget_min 20 --search_mode all --min_trades_train 15 --min_trades_test 5`

## Metrics

| Book | net Rs | PF | trades |
|---|---|---|---|
| Baseline TRAIN | 198.18 | 1.0063 | 72 |
| Baseline TEST | 8619.79 | 1.3807 | 61 |
| Best TRAIN | -7057.32 | 0.5507 | 28 |
| Best TEST | 461.3 | 1.2548 | 6 |

## Clean OOS Verdict

- Clean TEST verdict: **no train-side pass**
- Train gate: FAIL (net -7057.32<=0.0; PF 0.5507<1.3; max trades/day 8>6)
- Test gate: FAIL (PF 1.2548<1.3; top trade gross-profit share 0.416>0.4; top day net share 2.3877>0.4; top symbol net share 2.093>0.4)

## Changed Knobs vs Original

sl: 1.0 -> 0.6<br>tgt: 1.5 -> 1.2<br>mask_terms: [['regime', '!=', 'BULL']] -> [['regime', '!=', 'NEUTRAL'], ['atr_pct', '>=', 0.003449]]<br>guard: {} -> {'min_slot': '09:30', 'max_slot': '14:00', 'top_n': 5}<br>max_positions: 20 -> 10<br>daily_loss_rs: 0.0 -> 2500.0<br>regime_align: False -> True<br>regime_band: 0.0 -> 0.05

## Best Config

```json
{
  "name": "trial_37",
  "sl": 0.6,
  "tgt": 1.2,
  "mask_terms": [
    [
      "regime",
      "!=",
      "NEUTRAL"
    ],
    [
      "atr_pct",
      ">=",
      0.003449
    ]
  ],
  "premom_terms": [],
  "guard": {
    "min_slot": "09:30",
    "max_slot": "14:00",
    "top_n": 5
  },
  "max_positions": 10,
  "daily_loss_rs": 2500.0,
  "regime_align": true,
  "regime_band": 0.05
}
```

## Live Crosscheck / Known Mismatch Notes

### B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — *active*
| **B_HUGE_C1_CLOSE_RECLAIM_BREAK** | mask `regime ≠ BULL`; exit 1.00/1.50 | `rs_pct ≤ 10.7` (the no-op) + A/B top-slot gate | ⚠️ Overlay uses the no-op rs_pct mask the conf replaced; SL user-overridden from 0.70 to 1.00 |
Parked on 2026-06-29: `B_AVWAP_RECLAIM_REVERSAL`, `B_HUGE_C1_CLOSE_RECLAIM_BREAK`, `D_EMA20_REJECTION`, `E_VWAP_LOSE_EARLY_SHORT`, `G_HIGHER_HIGH_BREAK`, `L_DOUBLE_BOTTOM_VWAP`, `L_PRESSURE_BURST_VWAP`.

## Source Card

```text
### B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — *active*
- **Logic:** momentum continuation — break of a prior HUGE GREEN bar's high in a non-bear regime.
- **Detection (min quality 7.0):** `prev_range≥1.80×prev_ATR`, `prev_close>prev_open`, `close>open`, `close_loc≥0.60`, `close>prev_bar_high`, `close>VWAP`, `vol_ratio≥1.3`, `regime≠BEAR`. (reason `huge_green_reclaim_then_break`)
- **Indicators:** VWAP, ATR, vol_ratio, close_loc, regime.
- **Filters (mask = CATEGORICAL):** `regime≠BULL` (replaces no-op `rs_pct≤10.7`). Effective regime universe = {NEUTRAL, TREND}. *Apply as string inequality, not numeric.*
- **Gates (pre-mom):** none. **Guards:** none.
- **Exit:** SL 1.00 / Tgt 1.50 (user override 2026-06-29; prior SL 0.70). **Status:** PROBATION (WF-uncertifiable at ~34 trades; test 5/5 winners).

```

## Artifacts

- trials.csv: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\results\B_HUGE_C1_CLOSE_RECLAIM_BREAK\trials.csv`
- best_config.json: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\results\B_HUGE_C1_CLOSE_RECLAIM_BREAK\best_config.json`
- equity_train.png: written
- equity_test.png: written

No live execution was performed. No final_setup_conf.py edit was made.
