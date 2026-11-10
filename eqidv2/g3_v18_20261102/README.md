# G3 V18 — isolated market-participation research

This is a new backtest version built beside the existing G3 V17. It has no execution authority and never publishes a current pointer or changes live settings. All new files and results stay inside this directory. Existing result directories are refused as output.

Open `START_HERE.html` for the reading guide, `results_20261102_complete/report.html` for the completed full comparison, or `results_20261102_complete/daily_results.csv` for day-by-day results. The report states the actual last complete source date and separates missing/partial dates from zero-trade sessions. `current_20261102_latest/report.html` contains a separately timestamped November 2 market/stock indicator snapshot, without an execution P&L claim.

## Run

From the repository root:

```powershell
python -B -m g3_v18_20261102.run --output g3_v18_20261102/results_NEW --through 2026-11-02
```

Alternatively run `RUN_G3_V18.ps1`; it chooses a new timestamped result folder and today's date in India. The runner reads available sealed historical sources. It does not download missing minutes or turn an unfinished trading session into a completed result.

The delivered result combines three disjoint replay partitions. `merge_runs.py` verifies matching configurations, computational code, and historical Nifty input, remaps candidate IDs, recomputes global time-of-day RVOL and cost stress, and records the source partition manifests. Every position is closed within its own session, so partition boundaries do not change capital allocation. The failed initial full run has no completed result manifest and is not a performance result.

Dependencies are the repository's existing Python environment: pandas, numpy, pyarrow, pytest for checks. There is no new install or external service dependency.

## Fixed comparisons

| Variant | Change versus V17 |
|---|---|
| `control` | Original V17 selections and execution rules, freshly replayed |
| `market_only` | Directional breadth from session open at least 50%; source universe coverage at least 90% |
| `strength_only` | Stock return since open at least as strong as Nifty spot in the trade direction |
| `vwap_pressure` | Directional VWAP distance, VWAP slope and volume-weighted close-location pressure |
| `advanced` | All three added feature groups |
| `simplified` | Remove EMA9/20/50 stacking and body/wick thresholds |
| `simplified_advanced` | Reduced original rules plus all three added groups |

All filtering happens before ranking and quotas, so alternative eligible stocks can replace rejected candidates. Simplification starts with the full pre-selection pool, not a previously selected trade list. Price/OI/volume gates, directional one-minute confirmation, validity and timing checks, original picker and core-first quotas, targets and stops remain. A body-based picker can still use body size for ranking; its hard threshold is removed.

VWAP is a five-minute typical-price approximation. Pressure is a candle-volume proxy, not bid/ask order flow. Breadth covers the source F&O universe and compares with the current session open. Relative volume by time of day, price efficiency and range extension are exported as diagnostic features. They are not tuned into additional entry gates.

## Interpretation

No variant is assumed better in advance. The frozen comparison screen requires October P&L at least as high as control, no worse daily realized drawdown, at least half the control's October trades and at least 20 trades. Passing means a candidate warrants further paper evaluation; it does not validate future returns or authorize live use.

The inherited V17 slots were chosen using results through October 27. Earlier history is therefore not independent holdout. Historical opening OI, after-dated early universes and stock-day eligibility also carry retrospective limitations. These are recorded in source provenance and the report. Missing market/stock history fails the required new gate; first-bar VWAP slope is unavailable.

October 28 has a restricted research cohort of 215/218 stocks. COROMANDEL, KARURVYSYA and SAIL are excluded for missing or invalid exact OI. The blocked original full-universe recovery is retained in `sources/2026-10-28_blocked`; the separately labelled restricted source is in `sources/2026-10-28`. No OI values were invented or changed. October statistics excluding this restricted session are also included.

Modeled costs are 5, 9 and 15 bps round trip. Capital and exits match G3: ₹10 lakh portfolio, ₹1 lakh per entry with 5× exposure, 10-minute trigger expiry, 1.25% stop tightening to 1% after 120 minutes, and 15:15 square-off. Minute-bar stop-first ambiguity is retained. Drawdown is daily realized P&L, not mark-to-market.

## Checks

```powershell
python -B -m pytest -q -p no:cacheprovider g3_v18_20261102/tests_features.py g3_v18_20261102/tests_selection.py g3_v18_20261102/tests_runner.py g3_v18_20261102/tests_data.py
```

The input loader verifies source hashes and minute continuity. The runner blocks Python writes outside this new directory, freezes its configuration before computing results, snapshots the exact Nifty bytes used and records input/code hashes. `preservation_check.json` compares the content and timestamps of 2,000 existing source/configuration/launcher/pointer files with their state at task start.
