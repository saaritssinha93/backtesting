# G5 ML research results — 18 October 2026

G5 is an isolated research replica of the frozen G4 cohort. The control exactly reproduces G4's native trades, execution and portfolio allocation. G4 configuration, live settings, strategy registry, dashboard and default result pointers were not edited.

The completed study provides a small historical ML gain, with insufficient evidence for promotion. Fixed percentage trailing provides a defensive trade-off on the evaluation portion, while reducing profit substantially over the entire studied history.

## Saved artifacts

- [Main HTML report](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_5/research/20261018T231209_ml_walkforward_v1/report.html)
- [All scenario comparisons](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_5/research/20261018T231209_ml_walkforward_v1/comparison.csv)
- [Daily comparisons](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_5/research/20261018T231209_ml_walkforward_v1/daily_comparison.csv)
- [Per-order predictions](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_5/research/20261018T231209_ml_walkforward_v1/predictions.csv)
- [Every trade change](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_5/research/20261018T231209_ml_walkforward_v1/trade_changes.csv)
- [Block resampling intervals](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_5/research/20261018T231209_ml_walkforward_v1/bootstrap_intervals.csv)
- [Main run verification](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_5/research/20261018T231209_ml_walkforward_v1_verification.json)
- [Disabled-ML rollback run](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_5/research/20261018T231312_ml_walkforward_v1/report.html)
- [Rollback verification](C:/TradingData/eqidv2/backtesting_result_v13_v10_g_5/research/20261018T231312_ml_walkforward_v1_verification.json)

## Dataset and method

The sealed source is `20261017_g4_53_session_sealed_for_atr`: 53 sessions from 29 July through 16 October 2026, 543 selected orders, 478 native fills, 475 portfolio-executed trades. All 543 feature rows passed the confirmation-time input checks.

The first 30 sessions (through 10 September) form the initial training period. The primary evaluation contains 23 sessions, 11 September–16 October. Five successive evaluation blocks contain 5, 5, 5, 5 and 3 sessions. Training expands through the preceding block; completed outcomes from an evaluation block may enter training only in a later block.

A pooled Ridge regression predicts native net return divided by the initial 1.2% price risk, conditional on filling. Unfilled orders have no training label. Native fills rejected by the original portfolio remain eligible for training labels; allocation is always recomputed. The model uses 12 explicitly allowed confirmation-time features: ATR percentage, directional EMA and VWAP distance, directional momentum and EMA slope, two relative-volume measures, candle body and rejection wick, side, clock and target/risk ratio. No exit, MFE, MAE, final daily range, symbol identity or future outcome enters the inputs.

Standardization fits on training observations only. Ridge alpha is selected from 1, 10 and 100 using the last two five-session validation blocks wholly inside the training window. The selected alphas are 1, 10, 100, 1 and 1. A predicted net risk-unit return at or below zero halves the position in the sizing variant or skips it in the filtering variant. Missing/invalid scores retain the baseline position.

Nine variants were evaluated: control, uniform half-size control, ML sizing, ML filtering, adaptive exits, sizing plus adaptive exits, filtering plus adaptive exits, fixed protective exits and fixed percentage trailing. Each has complete portfolio replay at 5, 10 and 15 bps round-trip costs, plus a separate one-minute entry-delay stress at 5 bps: 36 portfolio ledgers in each run. Costs are proportional to exposure; fractional notional sizes preserve the G4 accounting convention.

## Primary evaluation: 23 sessions, 5 bps costs

| Variant | Trades | Losing trades | Net P&L | Change vs G4 | Sum of losing-trade losses | Daily-close max drawdown |
|---|---:|---:|---:|---:|---:|---:|
| G4 / G5 control | 192 | 59 | ₹383,286 | — | ₹219,843 | ₹37,913 |
| Every trade half-size | 192 | 59 | ₹191,643 | −₹191,643 | ₹109,922 | ₹18,957 |
| ML sizing | 192 | 59 | ₹385,122 | +₹1,835 | ₹217,218 | ₹38,703 |
| ML filtering | 190 | 58 | ₹386,957 | +₹3,670 | ₹214,593 | ₹39,493 |
| Adaptive exits | 192 | 59 | ₹383,286 | ₹0 | ₹219,843 | ₹37,913 |
| Combined sizing | 192 | 59 | ₹385,122 | +₹1,835 | ₹217,218 | ₹38,703 |
| Combined filtering | 190 | 58 | ₹386,957 | +₹3,670 | ₹214,593 | ₹39,493 |
| Fixed protective exit | 192 | 64 | ₹359,152 | −₹24,135 | ₹223,439 | ₹33,672 |
| Fixed percentage trailing | 192 | 52 | ₹377,293 | −₹5,993 | ₹194,388 | ₹25,143 |

The adaptive exit selector requires training-period net improvement, lower total losing-trade loss, no worse daily drawdown, at least 95% retention of profit on original winning trades and at least three changed trades. It retained baseline exits in every block. Consequently the combined variants have the same results as their respective ML-only variants. No eligible exit was forced into use.

ML filtering changed two decisions: it skipped BAJAJ-AUTO SHORT on 16 September, saving ₹5,250, and CGPOWER SHORT on 15 October, sacrificing ₹1,579.71. Its gain is 0.96%, with one fewer loss and higher drawdown. The descriptive paired three-session block-resampling 95% interval for its incremental P&L is approximately −₹4,739 to +₹15,750. This does not establish an ML edge.

The preregistered growth screen requires at least 5% higher net profit, 10% less losing-trade loss and no worse daily drawdown. Neither ML variant passes. Fixed trailing passes the evaluation-period defensive screen (at least 95% net retention and at least 20% drawdown reduction): net retention is about 98.4%, losing-trade losses fall about 11.6%, and daily drawdown falls about 33.7%. This is a historical screening result requiring future validation.

Minute-close marked drawdown is higher than daily-close drawdown: approximately ₹49,785 for control, ₹51,364 for ML filtering and ₹36,716 for fixed trailing. These figures mark positions at observed minute closes and recognize full modeled costs at entry; they do not capture every intraminute extreme.

## 15 and 16 October

| Variant | 15 October | 16 October |
|---|---:|---:|
| Control | −₹18,081 | −₹19,832 |
| ML sizing | −₹18,871 | −₹19,832 |
| ML filtering | −₹19,661 | −₹19,832 |
| Fixed protective exit | −₹13,840 | −₹19,832 |
| Fixed percentage trailing | −₹9,333 | −₹15,810 |

These dates are diagnostics; their outcomes were not used to choose special thresholds.

## Full-history context and stresses

The entire 53-session control earns ₹1,136,569. ML filtering earns ₹1,140,239, but its first 30 sessions use baseline decisions as training warm-up. This total is not 53 sessions of evaluated ML decisions.

Applying fixed trailing throughout all 53 sessions earns ₹1,021,409: ₹115,160 less than control, retaining about 89.9% of profit. It does not meet the 95% defensive profit-retention threshold across the entire cohort. Earlier exits free capital for one additional portfolio trade, demonstrating why full allocation replay is required.

At 10/15 bps, evaluation net profit is ₹335,286/₹287,286 for control and ₹339,457/₹291,957 for ML filtering. The filtering gain remains dependent on the same two decisions. With one-minute-delayed entries, control earns ₹328,643 and fixed trailing earns ₹345,034; this is a different execution assumption, not a substitute for the primary result. All predictions and exit selections remain frozen during stress tests.

## Exit definitions

The protective policy observes the first five post-confirmation minute closes. After filling, two consecutive closes at least 0.30% adverse to the actual entry request an exit at the next observed open. Native stop/target exits take priority. Pre-fill adverse closes do not count; pending orders are not canceled by this particular exit experiment. This differs from earlier G4 research that also canceled pending orders.

Percentage trailing activates after a completed post-entry candle gains at least the greater of 0.60% or half the setup target. It trails 0.50% behind the best completed close, with a +0.05% entry-price profit floor. Updates become effective on the next candle, and the stop only tightens. Original targets, 1.2% initial stop, 1.0% timed tightening after 120 minutes, gap handling and native stop-first ambiguity convention remain in force.

## Verification and rollback

The targeted test suite passed 48 tests, including G5 causality, past-only preprocessing, model JSON scoring parity, size-dependent capital allocation, cost arithmetic, protective exit timing, native gap precedence and future-only shadow scoring.

Independent verification passed for both runs: each has 139 artifact hashes, 10 implementation/configuration hashes, 36 saved portfolio ledgers and five fold models. Predictions reproduce from saved numeric models; daily P&L, minute-close curves and trade accounting reconcile. The disabled-ML run asserts exact native/allocation parity for all five adaptive variants at every cost/timing stress.

To reproduce the main experiment:

```powershell
python fno_v13_v10_g_5_backtest.py
```

To verify rollback behaviour in a new isolated run:

```powershell
python fno_v13_v10_g_5_backtest.py --ml-disabled
```

To audit an existing run:

```powershell
python -m research.g5_ml_verify C:\TradingData\eqidv2\backtesting_result_v13_v10_g_5\research\20261018T231209_ml_walkforward_v1
```

The run contains `models/next_block_shadow_model.json`, fitted using data through 16 October. Its selected alpha is 100 and its next-block exit choice is baseline. The model can score later order/context snapshots through `python -m research.g5_ml_shadow`; it rejects sessions on or before its training cutoff, writes suggested actions to a fresh CSV, and has no order-submission authority. The context file must contain timestamped OHLCV features built with the same trailing calculations as the research data, plus an order SID column. This scorer is available; an automatic collector/retraining scheduler has not been connected.

## Decision and outstanding evidence

Keep G4 as the operational benchmark. ML filtering is an exploratory shadow candidate; its two adjusted trades provide inadequate support for deployment. Percentage trailing is a defensive research candidate with material full-history profit sacrifice. No candidate has been promoted.

All existing sessions were previously examined and helped select G4. Walk-forward model training prevents direct future-label leakage but does not remove the baseline's strategy-selection bias. There is no untouched validation in this run. Historical input conventions, equity-versus-futures execution differences, whole-lot sizing, broker charges, impact and live publication/queue delays remain limitations.

The outstanding phase requires new market sessions: freeze the recipe and shadow predictions before outcomes, then review after at least 30 new sessions and 200 completed eligible fills, whichever takes longer. This checkpoint does not guarantee sufficient statistical evidence. Future shadow results cannot be produced from the current 53-session dataset.
