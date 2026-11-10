# G3 V24 selection-ranking research

V24 tests selection changes against the adopted V23 breakeven baseline. This package has no trading authority. Existing versions and live/paper configurations are unchanged.

**Completed result:** Across 68 available sessions, 29 July–9 November 2026, none of the four ranking changes improved the baseline. All four lowered net profit and win rate and increased total losing-trade PnL and daily-close drawdown. The baseline produced 824 filled trades, 65.66% win rate and Rs 1,221,079.86 net modeled PnL. Keep the existing selection as the research reference; no new ranking is adopted.

The inherited 66-session baseline reproduces exactly: 891 admitted orders, 790 fills and Rs 1,202,036.50 net PnL. All 29 automated tests pass. The final reports include the additional November 6 and November 9 sessions and the displaced/replacement-trade attribution.

- [Full comparison](results_through_20261109/REPORT.md)
- [Day-wise results](results_through_20261109/DAY_WISE_RESULTS.md)
- [Changed trades and their outcomes](results_through_20261109/trade_changes.csv)
- [Configuration and baseline parity](results_through_20261109/config.json)

## Fixed comparison

The 34 original slot/direction pairs retain three entries; the 39 added pairs retain one, through 14:00. All candidates must first pass the existing G3 selection rules. Core candidates retain priority. Ranking changes operate within these groups before quotas; the existing volume and G7 vetoes remain after quotas, with no replacement after a veto.

The baseline retains native targets, a 1.50% initial stop and the adopted 80%-of-target completed-close breakeven rule. Modeled exposure is Rs 500,000 per filled trade, round-trip cost Rs 250, with unlimited concurrent capital. Only selection ranking varies.

## Four fixed hypotheses

1. **Stretch:** prefer smaller directional distance from the completed one-minute close to EMA20, divided by ATR14.
2. **Breakout position:** prefer a confirmation close beyond the earlier same-session high/low; otherwise prefer more room to that level, measured in ATR.
3. **Combined:** average the within-group stretch and breakout-position ranks equally.
4. **Combined with volume:** average those two ranks and the existing confirmation-volume rank equally.

Levels exclude the confirmation candle. Features use completed observations only. Missing feature evidence restores the original ranking for that entire slot/direction/day. No loss/win labels enter selection and no numerical thresholds are optimized.

## Reproduction

From the repository root:

```powershell
python -m pytest g3_v24_selection_20261109 -q
python -m g3_v24_selection_20261109.run --output g3_v24_selection_20261109/results_recheck
```

Use a fresh result-directory name. The runner refuses to overwrite reports, validates source and code hashes, and checks that all inherited V23 entries, exits and PnL reproduce exactly. Caches and checkpoints are isolated inside V24.

Results are retrospective. The November 9 paper-trade review informed the hypotheses; the historical periods are not untouched out-of-sample evidence. Archived universe/source limitations and excluded dates remain inherited and disclosed in the report and provenance.
