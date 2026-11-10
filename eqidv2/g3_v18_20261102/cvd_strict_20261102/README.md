# G3 V18: stricter estimated CVD experiment

This is a separate backtest experiment requested after the earlier small CVD ranking adjustment changed no selected trades. Existing G3, basic CVD and advanced CVD files are preserved. No live trading configuration is changed.

Open `START_HERE.html` for the results. The underlying rules are frozen in `methodology.json`; the selector implements them in `selection.py`.

## What makes this stricter

CVD eligibility is checked on the complete native G3 candidate pool before ranking and selection quotas. A rejected candidate cannot enter. An eligible candidate further down the original ranking can replace it. The original G3 filters, ranking of surviving candidates, core and extra quotas, confirmation, execution, exits and portfolio rules are retained.

| Experiment | Requirement for the trade direction |
|---|---|
| Original G3 | Original selection, without CVD restrictions |
| CVD-ready control | Exact completed CVD features available; no directional thresholds |
| RVOL-ready control | CVD features and 20-session relative volume available; no directional thresholds |
| Aligned CVD | Recent 15-minute pressure and its acceleration both support the trade |
| Strict CVD | Signed pressure at least 0.10 and signed acceleration at least 0.05 |
| Strict CVD + volume | Strict CVD plus same-clock relative volume of at least 1.20 |

The sign is +1 for long trades and -1 for short trades. A short therefore needs pressure at or below -0.10 and acceleration at or below -0.05. Threshold equality passes the strict gates. Missing or invalid features fail closed.

For each one-minute candle, estimated delta is `volume * (2*close-high-low)/(high-low)`; a flat candle contributes zero. Recent pressure is summed estimated delta divided by summed volume over the latest 15 completed minutes. Acceleration is recent pressure minus pressure in the preceding, non-overlapping 15 minutes. The 0.05 threshold uses this raw difference, not the version divided by two. Relative volume compares the latest 15-minute volume with the same clock window over exactly 20 strictly prior observed stock sessions.

This uses actual Kite OHLCV data, but delta is inferred from the candle location. It is not observed buyer-initiated versus seller-initiated trade volume.

## Fair comparison

Thirty consecutive valid minutes are needed: the first eligible signal is 09:45 IST. Relative volume additionally needs 20 prior sessions. Aligned and strict CVD are compared with the CVD-ready control. Strict CVD plus volume is compared with the RVOL-ready control. Comparisons with original G3 show the separate cost of skipping opening trades and historical warmup.

The replay retains all 62 sealed G3 sessions from 29 July to 30 October 2026, including zero-trade days. November 2 has no sealed G3 replay source and is not represented as a tested trading day. The previously captured current-universe features extend through 15:15 IST on November 2.

Costs are evaluated at 5, 9 and 15 basis points using G3's existing cost convention. Drawdown is calculated from daily realized portfolio P&L; it does not measure intraday open-position drawdown. Results use the existing fixed capital and portfolio allocation rules.

The frozen October descriptive screen requires net P&L no lower than the matched control, daily realized drawdown no higher, at least the greater of 20 trades or 50% of control trades, a changed selection, and a strict improvement in P&L or drawdown. Passing this screen would justify prospective evaluation only.

All 62 sessions were already examined in previous research; there is no unseen holdout. This is an iterative test with thresholds fixed before this run and no search or retuning afterward. A stronger effect does not automatically mean better performance.

## Reproduce

Run from the repository root, using a new output directory to preserve the published results:

```powershell
python -B -m pytest -p no:cacheprovider g3_v18_20261102/cvd_strict_20261102/tests_selection.py g3_v18_20261102/cvd_strict_20261102/tests_runner.py -q
python -B -m g3_v18_20261102.cvd_strict_20261102.run --features g3_v18_20261102/cvd_advanced_20261102/calculated_20261102/features.parquet --feature-provenance g3_v18_20261102/cvd_advanced_20261102/calculated_20261102/configuration.json --output g3_v18_20261102/cvd_strict_20261102/results_reproduction
```

The feature file is reused without edits or new downloads. Its SHA-256 is `5c368ea9a4150a75bf923390e89dff005e68f1748ab8b27af0a47568b21ddae9`.

`preservation_before.json` records hashes and modification times of prior files. The final verification report checks preservation and generated artifact hashes.
