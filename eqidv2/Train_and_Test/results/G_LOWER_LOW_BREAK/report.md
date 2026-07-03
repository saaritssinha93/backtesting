# G_LOWER_LOW_BREAK Optimization Report

Status: **NOT SELECTED**

## Windows

- FIT: 2026-05-27..2026-06-03 (2026-05-27, 2026-05-29, 2026-06-01, 2026-06-02, 2026-06-03)
- VAL: 2026-06-04..2026-06-10 (2026-06-04, 2026-06-05, 2026-06-08, 2026-06-09, 2026-06-10)
- TRAIN: 2026-05-27..2026-06-10
- TEST: 2026-06-12..2026-06-24

TEST was evaluated only after the FIT/VAL search loop finished.

## Engine

- Trials run: 300 of requested 300
- Search engine: Optuna TPE
- Best FIT/VAL score: 1.383405
- Command: `python Train_and_Test\optimize_setup_card_loop.py --setup G_LOWER_LOW_BREAK --pool C:\TradingData\eqidv2\setup_pools_2026_06_29\G_LOWER_LOW_BREAK --trials 300 --time_budget_min 20 --search_mode all --min_trades_train 30 --min_trades_test 5`

## Metrics

| Book | net Rs | PF | trades |
|---|---|---|---|
| Baseline TRAIN | -942.46 | 0.4487 | 4 |
| Baseline TEST | 0.0 | 0.0 | 0 |
| Best TRAIN | 9775.95 | 1.3949 | 63 |
| Best TEST | -3839.86 | 0.5353 | 14 |

## Clean OOS Verdict

- Clean TEST verdict: **no train-side pass**
- Train gate: FAIL (max trades/day 19>6; top day net share 1.3628>0.4)
- Test gate: FAIL (net -3839.86<=0.0; PF 0.5353<1.3; max trades/day 7>6)

## Changed Knobs vs Original

tgt: 1.0 -> 2.5<br>mask_terms: [['vol_ratio', '>=', 4.129044], ['quality_score', '>=', 76.444124]] -> [['quality_score', '>=', 36.759405]]<br>premom_terms: [['sig5_rsi_dir', '>=', 68.747209]] -> [['pre_entry_momentum_score', '<=', 66.00901]]<br>guard: {} -> {'min_slot': '10:00', 'max_slot': '14:00', 'top_n': 3}<br>daily_loss_rs: 0.0 -> 2500.0

## Best Config

```json
{
  "name": "trial_253",
  "sl": 1.1,
  "tgt": 2.5,
  "mask_terms": [
    [
      "quality_score",
      ">=",
      36.759405
    ]
  ],
  "premom_terms": [
    [
      "pre_entry_momentum_score",
      "<=",
      66.00901
    ]
  ],
  "guard": {
    "min_slot": "10:00",
    "max_slot": "14:00",
    "top_n": 3
  },
  "max_positions": 20,
  "daily_loss_rs": 2500.0,
  "regime_align": false,
  "regime_band": 0.0
}
```

## Live Crosscheck / Known Mismatch Notes

### G_LOWER_LOW_BREAK (SHORT) — *active — mined short, SELECTIVE*
`B_HUGE_RED_FAILED_BOUNCE`, `C_OR_BREAKDOWN`, `G_LOWER_LOW_BREAK`, `G_HIGHER_HIGH_BREAK`, `L_DOUBLE_BOTTOM_VWAP`, `L_PRESSURE_BURST_VWAP` — plus the Tier-C longs (`L_RS_LEADER_VWAP_HOLD`, `P_PDH_BREAK_RETEST_LONG`, `E_ORB_RETEST_HOLD_LONG`, `V_RECLAIM_PULLBACK_LONG`, now demoted) which are emitted by the **conf-mode Tier-C live scanner** and readmitted past v8/research before the final conf gate.
| G_LOWER_LOW_BREAK | SHORT | train 51 / PF 2.25, test 9 / PF 9.12 |

## Source Card

```text
### G_LOWER_LOW_BREAK (SHORT) — *active — mined short, SELECTIVE*
- **Logic:** lower-low break on a volume climax (~4×) = capitulation / exhaustion short.
- **Detection:** production clean-pool scanner raw_candidates (corrected VWAP).
- **Indicators:** vol_ratio, quality_score, RSI-direction.
- **Filters (mask):** `vol_ratio≥4.129044` AND `quality_score≥76.444124`.
- **Gates (pre-mom, missing→block):** `sig5_rsi_dir≥68.747209`.
- **Guards:** none. **Exit:** SL 1.10 / Tgt 1.00. **Status:** WEAK / SELECTIVE (fires rarely, test n=9; 100% months but `sig5_rsi_dir` cliff).

---

## 2. ACTIVE BOOK — LONGS

```

## Artifacts

- trials.csv: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\results\G_LOWER_LOW_BREAK\trials.csv`
- best_config.json: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\Train_and_Test\results\G_LOWER_LOW_BREAK\best_config.json`
- equity_train.png: written
- equity_test.png: written

No live execution was performed. No final_setup_conf.py edit was made.
