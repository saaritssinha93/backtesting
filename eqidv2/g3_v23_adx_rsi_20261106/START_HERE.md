# V23 current baseline: 80%-target breakeven

**The user explicitly adopted this rule as the V23 backtesting baseline.** This supersedes the previous fixed-stop default; it does not rewrite the archived research qualification decision.

- Initial stop:1.50% from actual entry. Original target retained; no two-hour tightening.
- Trigger: completed postentry one-minute close reaches80% of the original target distance.
- From the next bar, tighten to+0.05% signed return from entry: LONG entry x1.0005; SHORT entry x0.9995.
- Entry-bar triggers excluded. Stop only tightens. Conservative stop-first ambiguous bars and adverse gap-open fills retained. Exit by15:15.
- Example: LONG entry100,target100.80; qualifying close100.64 activates stop100.05 from the following bar.

| Policy | Trades | Wins | Losses | Flat | Win rate | Net P&L (Rs) | Loss amount (Rs) | Daily-close drawdown (Rs) |
|:--|--:|--:|--:|--:|--:|--:|--:|--:|
| Previous fixed1.50% stop | 790 | 540 | 250 | 0 | 68.35% | 1,220,580.55 | 837,339.00 | 33,894.47 |
| Current adopted80%-target breakeven | 790 | 520 | 241 | 29 | 65.82% | 1,202,036.50 | 808,171.11 | 31,338.39 |

Net profit is lower by Rs18,544.05; loss amount is lower by Rs29,167.90; daily-close drawdown is lower by Rs2,556.08. This trade-off is adopted by user instruction.

All790 saved candidate exits and all66 daily aggregates were independently replayed and reconciled before publication. 891 selected orders, fixedRs500,000 exposure per filled trade, Rs250 modeled round-trip costs, unlimited concurrent capital. The73 configured slot/direction pairs through14:00, their existing caps and all entry filters remain. ADX/RSI entry vetoes remain disabled.

All history through5November2026 was previously observed. These are retrospective results. Daily-close drawdown is not intraday drawdown. The29 flat trades become losing trades under one extra basis point of adverse exit slippage; exact cost-covered exits are not guaranteed.

[All66 day-wise results](breakeven_baseline_20261106/results_through_20261105/DAY_WISE_RESULTS.md) | [Daily CSV](breakeven_baseline_20261106/results_through_20261105/day_wise_results.csv) | [Current config](breakeven_baseline_20261106/results_through_20261105/config.json) | [All current trades](breakeven_baseline_20261106/results_through_20261105/selected_trades.csv) | [Slippage sensitivity](breakeven_baseline_20261106/results_through_20261105/cost_stress.csv)

Earlier results are preserved. LIVE execution, schedules and position management are unchanged.

Reproduce the current baseline into a fresh result directory:

```powershell
python -m g3_v23_adx_rsi_20261106 --output g3_v23_adx_rsi_20261106/breakeven_baseline_20261106/recheck
```

The default runner now replays the adopted breakeven policy and verifies all saved exits before publishing. It refuses to overwrite existing output folders. The complete strategy history ends on5November2026; no6November intraday data were added.

Archived research decisions describe the default at the time of each study. The user adoption above supersedes those earlier defaults:

- [Previous fixed1.50 baseline and runner comparisons](trailing_20261106/results_through_20261105/REPORT.md)
- [Earlier target-based trailing study and qualification decision](target_trailing_20261106/results_through_20261105/REPORT.md)
- [Entry-stop study](entry_sl_20261106/ENTRY_STOP_RESULTS.md)
- [Earlier loss-control study](loss_control_20261106/results_through_20261105/REPORT.md)
- [Previous native-stop baseline](through_20261105/results_through_20261105/REPORT.md)
