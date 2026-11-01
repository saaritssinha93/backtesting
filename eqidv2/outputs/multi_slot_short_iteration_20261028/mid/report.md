# 12:31 and 13:01 SHORT replacement-aware audit

The shared frozen grid was applied before native ranking and quota. Every replacement was simulated from one-minute paths. G7 replacements received freshly computed Nifty direction permission and the current V4 low-efficiency veto, then +0.6/+0.1 protection and the causal two-loss lock.

All six baseline parity checks passed against the accepted 09:26-adjusted G3 and G7 ledgers. No active config or core file was changed.

## Honest result

No predeclared variant passed all five development gates for either slot in either strategy. The retained result is the current baseline in all four cases. No point estimate above 75% is claimed as a qualified improvement.

| Slot | Strategy | Baseline all history (5 bps) | Baseline October (5 bps) | Closest failed development rule |
|---|---|---:|---:|---|
| 1231_SHORT | G3 | 13 trades, 69.23%, Rs 17,170.28 | 5 trades, 40.00%, -Rs 5,523.24 | extension_le_3p0: 4 trades, 100.00%, 2/5 gates |
| 1231_SHORT | G7_V4_LOCK2 | 9 trades, 66.67%, Rs 8,842.24 | 5 trades, 40.00%, -Rs 5,354.85 | extension_le_3p5: 3 trades, 100.00%, 3/5 gates |
| 1301_SHORT | G3 | 25 trades, 72.00%, Rs 21,689.06 | 10 trades, 70.00%, Rs 4,130.13 | extension_le_3p5: 11 trades, 72.73%, 4/5 gates |
| 1301_SHORT | G7_V4_LOCK2 | 14 trades, 71.43%, Rs 8,575.21 | 5 trades, 80.00%, Rs 2,421.02 | extension_le_3p5: 8 trades, 62.50%, 3/5 gates |

## What failed and why

- 12:31 G3 filters that printed 80-100% in development retained only 3-6 trades, below the fixed minimum of 8, and reduced development PnL. The best all-history Wilson lower bound among every tested rule was only 56.55%.
- 12:31 G7 had only 3 executed development trades at baseline and zero development losing-trade amount. Because the shared loss gate requires a strict reduction, no rule can improve below zero; the small 2-3 trade 100% results therefore cannot qualify.
- 13:01 G3 `extension_le_3p5` was the genuine near miss: 8/11 wins (72.73%), setup PnL +Rs 4,375.41 and full-portfolio PnL +Rs 7,925.41 versus baseline in development. It failed the strict >75% gate. Across all history it was exactly 75.00% (18/24), with a 55.10% Wilson lower bound.
- 13:01 G7 did not exceed 66.67% in development under any rule. Its existing configuration already applies the 3.5 ATR extension cap, so that grid row correctly reproduces baseline rather than creating a false improvement.

## Cost stress

The retained baselines were rerun with cost-dependent chronology at 5, 9, and 15 bps. Win rates and PnL deteriorated materially at higher costs, especially 13:01 G7 (71.43% at 5 bps to 38.46% at 15 bps). See `cost_stress_summary.csv` for exact all-history and October values.

## Statistical limit

This history was repeatedly inspected and is not an independent holdout. Every 95% Wilson lower bound remained below 75%, including the descriptive 100% results with very small samples.

Full evidence is in `honesty_gate_matrix.csv`, `variant_setup_metrics.csv`, `variant_portfolio_metrics.csv`, `variant_trade_details.csv`, `selected_variant_daywise.csv`, and `diagnostic_period_matrix.csv`.
