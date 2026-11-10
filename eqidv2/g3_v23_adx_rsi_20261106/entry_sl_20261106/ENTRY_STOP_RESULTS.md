# V23 entry-price stop study through 5 November 2026

**A fixed 1.50% stop from actual entry improved aggregate historical profit and losses while preserving every entry and all 535 original winning trades.** It remains an exploratory research candidate: individual trade risk is higher, and drawdown worsened in the calibration period and the final seven sessions. The current V23 default remains the original stop schedule.

[Candidate day-wise results: all 66 sessions](recovery_room_20261106/results_through_20261105/CANDIDATE_DAY_WISE_RESULTS.md) · [Candidate daily CSV](recovery_room_20261106/results_through_20261105/candidate_day_wise_results.csv) · [All candidate trades](recovery_room_20261106/results_through_20261105/constant_1p50_trades.csv) · [Every entry and stop price](recovery_room_20261106/results_through_20261105/stop_levels.csv)

## Tested logic

Calculate the stop when the actual entry fills:

- LONG: `SL = entry_price × 0.985`.
- SHORT: `SL = entry_price × 1.015`.
- Keep that price throughout the strategy session; do not automatically tighten it after two hours.
- Exit at the original target, the stop, or the existing 15:15 time exit. Entry selection and sizing remain unchanged.

For an entry of Rs 1,000, the LONG stop is Rs 985 and the SHORT stop is Rs 1,015. The stop is anchored to actual entry, not recalculated from the latest traded price. Gaps can fill beyond the stop price.

The baseline already uses an entry-relative 1.25% stop, tightened to 1% after two hours. The candidate changes its distance and removes that timed tightening. It is not a rule that selectively changes future losing trades: the same policy runs on all 790 entries.

## Full-history comparison

66 completed sessions from 29 July through 5 November 2026, with signals through 14:00 and execution through 15:15. Exposure remains Rs 500,000 per trade, unlimited concurrent capital, and Rs 250 modelled round-trip costs per trade.

| Measure | Baseline: 1.25% then 1% | Candidate: fixed 1.50% |
|:--|--:|--:|
| Trades | 790 | 790 |
| Wins / losses | 535 / 255 | 540 / 250 |
| Win rate | 67.72% | 68.35% |
| Net profit after costs | Rs 1,177,522.10 | **Rs 1,220,580.55** |
| Sum of losing-trade amounts after costs | Rs 863,772.35 | **Rs 837,339.00** |
| Daily-close drawdown | Rs 35,384.36 | **Rs 33,894.47** |
| Worst single trade | −Rs 6,639.33 | **−Rs 7,750.00** |

Net profit improves by **Rs 43,058.45** and total loss amount falls by **Rs 26,433.34**. All 535 baseline winners retain exactly their original entry and exit outcomes. Among the 95 changed baseline losers, 36 improve and 59 become worse; five turn into winners. This does not mean each losing trade or each day improves.

## Why it is not the default

The planned price risk rises from 1.25% to 1.50%, a **20% increase at the same position size**. Nominal loss including the modelled cost rises from Rs 6,500 to Rs 7,750 per trade. The profit improvement therefore does not establish a better return for the same stop-risk budget.

Although aggregate profit and loss amount improve in each period, daily-close drawdown worsens in two of them:

| Period | Baseline drawdown | Fixed 1.50% drawdown |
|:--|--:|--:|
| First 44 sessions | Rs 9,785.32 | Rs 11,888.86 |
| Next 15 sessions | Rs 35,384.36 | Rs 33,894.47 |
| Final 7 sessions | Rs 24,615.85 | Rs 26,246.72 |

The predeclared checks required no drawdown deterioration, so the candidate was not promoted. Extra 2-bps adverse slippage on changed exits still leaves full-history profit at Rs 1,211,080.55, loss amount at Rs 846,339.00 and drawdown at Rs 34,894.47. At 5 bps the profit advantage remains, but full-history drawdown rises above baseline. [All slippage checks](recovery_room_20261106/results_through_20261105/cost_stress.csv).

## How the logic was developed

The first phase studied entry-relative adverse excursions on **496 trades in the first 44 sessions**, including 356 native winners. The 95th percentile of winner adverse excursion was 0.868%; the 97.5th percentile was 0.985%. Seven fixed or fitted stop models covered 0.70%, 0.90%, 1.00%, 1.20%, ATR scaling, two volatility groups and confirmation-candle structure. All seven increased full-history total loss amount and reduced net profit, despite some lower drawdowns.

That finding motivated a separate, predeclared second phase: keep the initial 1.25% stop throughout, delay tightening to four hours, or keep a 1.50% stop throughout. Keeping 1.25% or delaying its tightening slightly reduced full-history profit; the 1.50% model produced the aggregate improvement above. No thresholds were retuned after observing these four results.

[Training adverse-excursion study](analysis_source/STUDY.md) · [Seven tighter-stop comparisons](results_through_20261105/REPORT.md) · [Recovery-room comparison and period checks](recovery_room_20261106/results_through_20261105/REPORT.md)

Fitting and candidate selection use the first 44 sessions through 5 October. The next 15 and final seven sessions are checked separately, with no reselection on their outcomes. All history was already observed in earlier work, so these are retrospective checks, not independent forward validation.

## Verification and limits

- All 790 native baseline exits reproduce exactly. Entry identities, prices, times and all 66 daily trade counts are preserved.
- Fifty tests pass across calibration, execution, reporting and selection. Independent reconciliation checks every daily row, period total and cost-stress result.
- Source ATR and confirmation-candle values are available at confirmation; stop prices use actual fills. OHLC excursion bounds distinguish uncertain entry/exit-bar extremes.
- One-minute OHLC cannot reveal actual intrabar ordering. The inherited conservative entry-bar and stop-first conventions remain. Daily-close drawdown does not measure intraday risk.
- SAIL retains its November 5 source exclusion and KAYNES its opening-slot exclusion. October 1 remains excluded by the older immutable source. No November 6 intraday data are included.
- Previous reports and LIVE execution, approvals and scheduling are unchanged.

Reproduce into a fresh folder with:

```powershell
python -m g3_v23_adx_rsi_20261106.entry_sl_20261106.recovery_room_20261106.run --output g3_v23_adx_rsi_20261106/entry_sl_20261106/recovery_room_20261106/recheck
```
