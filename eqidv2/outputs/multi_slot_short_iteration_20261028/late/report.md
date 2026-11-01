# 13:21 SHORT fixed-grid audit

No predeclared rule qualifies for G3 or G7 V4 lock2. The honest result is to retain the current baseline for this slot.

The audit starts from the accepted portfolios that already contain the `09:26 SHORT` lower-wick rule. Every trial runs before max-volume ranking and the two-entry quota, admits replacement candidates, simulates fresh one-minute execution, and reallocates the complete portfolio. G7 additionally recomputes Nifty permission and the V4 low-efficiency veto at confirmation, replays `+0.1%` protection after a completed-close `+0.6%`, and reapplies the causal two-realized-loss lock.

| Strategy | Period | Trades | W-L | Win rate | Wilson 95% lower | Net P&L |
|---|---:|---:|---:|---:|---:|---:|
| G3 baseline | Development | 10 | 9-1 | 90.00% | 59.58% | ₹20,124.20 |
| G3 baseline | All | 22 | 14-8 | 63.64% | 42.95% | ₹16,292.64 |
| G3 baseline | October | 7 | 1-6 | 14.29% | 2.57% | -₹17,811.50 |
| G7 V4 lock2 baseline | Development | 6 | 5-1 | 83.33% | 43.65% | ₹10,236.26 |
| G7 V4 lock2 baseline | All | 16 | 10-6 | 62.50% | 38.64% | ₹10,308.01 |
| G7 V4 lock2 baseline | October | 6 | 1-5 | 16.67% | 3.01% | -₹14,543.60 |

G3’s development baseline already has one loss. `body<=0.90` and `down-move<=0.75%` reproduce the same 9/10 result, so they do not strictly reduce loss amount. Every rule that changes development selection either lowers setup P&L, lowers full-portfolio P&L, falls below eight executed trades, or fails the strict win-rate gate.

G7 has only six executed development trades in the baseline. Every tested rule leaves six or fewer after Nifty permission, V4 veto, allocation, and the loss lock, so none reaches the protocol minimum of eight. No rule removes its single development loss while preserving P&L.

The highest all-history point estimate remains below 75%:

- G3 `wick>=0.30`: 8/11, 72.73%, Wilson lower 43.44%, net ₹10,480.36, which is ₹5,812.28 below baseline.
- G7 `wick>=0.10` and `wick>=0.20`: both 9/13, 69.23%, Wilson lower 42.37%. Their net changes are +₹7,430.98 and +₹5,669.29, but neither passed development.

At 15 bps, the retained G3 baseline falls to 12/22 and ₹5,292.64. G7’s after-cost loss chronology changes the daily lock and produces 6/13 with -₹3,036.29. These figures reinforce the decision not to promote a rule.

The frozen five-slot grid contains 11 nonbaseline rules for two strategies, or 110 planned comparisons. Unadjusted 95% Wilson intervals are descriptive after selection. The familywise contextual lower bounds for the all-history baselines are 29.53% for G3 and 24.70% for G7. This history has also been inspected repeatedly and is not an independent holdout. Individually selected slot filters must not be combined without one simultaneous pre-quota replay because replacement ranking, capital contention, symbol exclusion, Nifty permission, V4 protection, and the loss lock interact.

All four baseline controls reproduced exactly. Thirteen newly selected G7 replacement features were recomputed causally. No replacement remained unprovable. A pandas metadata mismatch initially made identical timestamp sequences compare unequal; `feature_compatibility_audit.json` records the exact 246/246-minute PIIND proof and the output-only value-level compatibility check.

Detailed evidence is in `fixed_grid_metrics.csv`, `development_selection.csv`, `selected_cost_stress.csv`, `selected_chronology_results.csv`, `october_daywise_portfolio.csv`, `candidate_variant_decisions.csv`, `selection_changes.csv`, `g7_causal_features.csv`, `baseline_reproduction.json`, and `source_provenance.json`.
