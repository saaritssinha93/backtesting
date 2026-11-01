# Missing-midday replacement-aware audit

The frozen addendum was applied before rank and quota, including replacement candidates. Every baseline replay matched the accepted 09:26-adjusted books exactly. G7 used fresh Nifty permission, causal low-efficiency features for replacements, the existing 11:46 SHORT 4.0 ATR guard, +0.6/+0.1 protection, and the causal two-realized-loss lock.

## Decision

No tested rule honestly sustains a win rate above 75%. Keep the accepted baseline for all three slots in both strategies. No main configuration was changed.

The frozen development gate required at least 8 executed trades, a point win rate strictly above 75%, setup PnL at least baseline, losing-trade amount strictly below baseline, and full-portfolio PnL at least baseline. Ties used setup PnL, then fewer losses, then frozen rule order.

Two G3 rules passed the frozen development gate and remain diagnostic only:

- 1051_LONG `move_magnitude_le_0p75`: development 10/13 (76.92%, Wilson lower 49.74%), later 4/6 (66.67%), all 14/19 (73.68%).
- 1146_SHORT `extension_le_3p5`: development 7/9 (77.78%, Wilson lower 45.26%), later 5/8 (62.50%), all 12/17 (70.59%).

For 10:51 LONG the rule improved development PnL by Rs 2,900 and reduced losing amount by Rs 6,500, but later results were unchanged at 4/6 and October was 3/5. For 11:46 SHORT the development PnL gain was Rs 7,970.60, but validation lost Rs 6,840.25 versus baseline and later fell to 5/8. The 11:11 SHORT grid produced no development qualifier in either strategy. No G7 variant passed all development gates.

The closest rejected cases show why. G7 10:51 move<=0.75 reached 7/9, but lost Rs 3,598.50 versus baseline in both the slot and full portfolio and did not reduce loss amount. At 11:11, G3 had only 6 development trades and no loss reduction, while G7 had only 4 trades and exactly 75%, which is not strictly above the gate. G7 11:46 rules all stayed below 8 trades; extension<=2.5 was 4/4 but was rejected for sample size. G3 11:46 body<=0.90 also passed development, and the frozen tie-break chose extension<=3.5 because its development setup PnL was higher.

The two diagnostics were not combined because both failed the >75% target outside development. Summing their isolated gains would ignore shared capital, same-symbol exclusion, and the G7 loss lock.

All 12 variants, replacement decisions, 5/9/15 bps results, October/daywise rows, Wilson bounds, and failure reasons remain in the CSV artifacts. The 95% Wilson lower bounds and the 110/220-comparison context bounds are all below 75%, and this 59-session history is not an independent holdout.
