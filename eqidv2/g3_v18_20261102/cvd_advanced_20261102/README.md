# G3 V18 advanced estimated CVD test

Open `START_HERE.html` for the delivered comparison, stock features and evidence. This experiment is isolated in this new folder. The existing G3 strategy and earlier five-minute CVD experiment are preserved.

## Fixed experiment

Each completed one-minute candle contributes:

```text
delta_estimate = volume * (2*close - high - low) / (high - low)
```

Flat candles contribute zero. This is an estimate of candle-volume pressure; buy/sell aggressor classification is unavailable from these candles.

At each completed five-minute signal clock:

```text
recent_pressure = sum(delta over latest 15 minutes) / sum(volume over those minutes)
prior_pressure = sum(delta over preceding nonoverlapping 15 minutes) / sum(volume over those minutes)
acceleration = recent_pressure - prior_pressure
directional_acceleration = side_sign * acceleration / 2
```

The feature requires 30 consecutive valid minutes in the same session. Its first possible availability is 09:45 IST. Positive acceleration means pressure became more positive; it does not necessarily mean recent pressure is positive. Short trades use the opposite sign.

Three variants are frozen in `methodology.json`:

| Variant | Selection method |
|---|---|
| `base_control` | Original G3 selections |
| `acceleration_rank` | Native normalized picker plus 0.10 × directional acceleration |
| `acceleration_rvol_rank` | Same adjustment multiplied by `clip(RVOL20 - 1, 0, 1)` |

The native picker is divided by its positive finite maximum across the original eligible candidates for that date/setup. Ties retain the full original ranking key. Eligibility, raw ranking measurements, core-first quotas, extra quotas, entry confirmation, exits, capital and execution remain unchanged.

RVOL20 compares current 15-minute volume with the mean at the same clock across exactly 20 previous observed stock sessions. The current date is excluded. Missing required historical clocks disable the reading; older available dates do not replace them. The initial 20-session warmup is neutral. A date with no provider observations is absent from this explicitly disclosed stock-session calendar.

Before 09:45, without selection competition, with an invalid picker maximum, or if any eligible competitor lacks the required feature, the entire group's original ranking remains. Missing features do not reject otherwise valid G3 candidates. The adjustment is bounded at ±0.10 and was not tuned after results.

## Data and validation

The new minute capture covers the 221-symbol union of G3 historical and current universes, including 218 current members. Kite's minute start timestamps become completed minute timestamps by adding one minute. Only completed minutes are retained. The adapter independently reconstructs five-minute OHLCV and compares overlapping bars with the frozen earlier Kite capture.

All requests, source tokens, verified aliases, cutoff timestamps and hashes are recorded. ICICIPRULI's historical label maps to the same verified equity token now named ICICILIFE. Dated membership controls historical labels. Requested cutoff and actual last returned data are reported separately; missing late-session minutes are not fabricated.

Only the existing 62 sealed G3 replay sessions, July 29 through October 30, enter P&L. Current November 2 features remain descriptive because no sealed G3 execution source was available. September 14 has a dated universe file but no returned candles and is outside the replay calendar. Early universe, historical opening OI and restricted October 28 limitations are inherited from G3.

Native control trades must match the saved G3 V18 baseline. The runner also verifies every original source/filter field, selection quotas, neutral-group behavior, weight bound, execution chronology, costs and accounting. `selection_substitutions.csv` records replaced stocks; `pnl_attribution.csv` reconciles added, removed and shared-trade effects against the control. Costs are tested at 5, 9 and 15 bps round trip. Drawdown is daily realized P&L.

The frozen October screen requires at least the control's net P&L, no worse drawdown, at least 90% of its executed trades, a changed selected stock, and a strict improvement in P&L or drawdown. This is a descriptive screen. All 62 sessions were already inspected in earlier research; there is no unseen holdout. Any apparent improvement needs prospective evidence, especially if it comes from very few substitutions.

## Reproduce

Run from the repository root with `python -B` and fresh output directories. Existing result folders are refused. Fetching can resume verified successful chunks in its own directory; use a new capture directory to refresh the cutoff.

```powershell
python -B -m g3_v18_20261102.cvd_advanced_20261102.data --output g3_v18_20261102/cvd_advanced_20261102/data_NEW
python -B -m g3_v18_20261102.cvd_advanced_20261102.calculate --data g3_v18_20261102/cvd_advanced_20261102/data_NEW --output g3_v18_20261102/cvd_advanced_20261102/calculated_NEW
python -B -m g3_v18_20261102.cvd_advanced_20261102.run --features g3_v18_20261102/cvd_advanced_20261102/calculated_NEW/features.parquet --feature-provenance g3_v18_20261102/cvd_advanced_20261102/calculated_NEW/configuration.json --output g3_v18_20261102/cvd_advanced_20261102/results_NEW --through 2026-11-02
python -B -m g3_v18_20261102.cvd_advanced_20261102.publish --features g3_v18_20261102/cvd_advanced_20261102/calculated_NEW/features.parquet --data g3_v18_20261102/cvd_advanced_20261102/data_NEW --results g3_v18_20261102/cvd_advanced_20261102/results_NEW --output g3_v18_20261102/cvd_advanced_20261102/dashboard_NEW
```

Tests:

```powershell
python -B -m pytest -q -p no:cacheprovider g3_v18_20261102/cvd_advanced_20261102/tests_data.py g3_v18_20261102/cvd_advanced_20261102/tests_features.py g3_v18_20261102/cvd_advanced_20261102/tests_selection.py g3_v18_20261102/cvd_advanced_20261102/tests_runner.py
```

Data reference: [Kite historical candle API](https://kite.trade/docs/connect/v3/historical/). No live execution authority is granted by these files.
