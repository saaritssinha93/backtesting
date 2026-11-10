# G3 V18: estimated CVD using Kite five-minute candles

Open `START_HERE.html` for the delivered report and all-stock data links. This is a separate research experiment. All code and outputs are inside this folder; existing G3 V18 sources, reports, settings and original strategy remain in place.

## Calculation

For each completed five-minute candle:

```text
estimated_delta = volume * (2*close - high - low) / (high - low)
estimated_buy_volume = (volume + estimated_delta) / 2
estimated_sell_volume = (volume - estimated_delta) / 2
session_CVD = sum(estimated_delta since the 09:15 session open)
last_15m_delta = sum(estimated_delta over three consecutive completed candles)
```

Flat-range candles contribute zero. Pressure ratios divide each delta sum by volume over the same window. Session totals reset daily. A missing or invalid bar disables the session estimate for the remainder of that session. Current zero-volume bars are unavailable for filtering. Recent-window estimates may recover after three valid consecutive bars; the common backtest readiness gate still requires an unbroken session prefix.

These are close-location volume estimates, similar to an accumulation/distribution calculation. The buy/sell fields are mathematical allocations. They do not identify actual market buys/sells, institutions, hidden orders or exchange aggressor volume. The separately named body/range formula is a fixed sensitivity comparison.

## Data fetching

Kite's official historical API supplies five-minute OHLCV. The adapter shifts the native candle opening timestamp forward five minutes to match G3's completed-bar clock. It requests the union of dated G3 universe symbols and archived historical cohorts, verifies equity instrument tokens, and records explicit historical symbol renames. It freezes one completed-bar cutoff for the run, rejects incomplete bars and saves separate cached chunks with hashes and request receipts.

Requests are divided into at most 60 calendar days and paced at no more than two calls per second, below the published historical endpoint limit of three per second. Each request gets at most three attempts with backoff. Successful chunks are reused on a resume; refreshes use a new output folder. Credential contents are never written to the reports.

`kite_data_20261102/coverage.csv` records missing bars for each expected member stock and date. A dated universe file does not by itself prove that date was a trading session. Missing provider history is not converted to a zero-trade or zero-profit day. The full fetched symbol union includes symbols that were outside G3 on particular dates; `is_g3_member_on_date` and the dated membership file distinguish those rows.

Official references: [historical candle schema](https://kite.trade/docs/connect/v3/historical/) and [request limits](https://kite.trade/docs/connect/v3/exceptions/).

## Backtest

The runner compares the original G3 control, an identical-data-coverage control, positive directional session CVD, positive directional recent-15-minute delta, both together, and the body/range sensitivity. Thresholds are fixed at greater than zero. Filtering happens before native ranking and quotas. Short trades require negative raw delta; long trades require positive raw delta.

G3's original sealed one-minute execution, confirmation, capital and exits are retained. The original control must exactly reproduce the saved V18 trade identities, fills, clocks, prices and P&L. New Kite candles supply only CVD features; discrepancies versus original G3 signal candles are counted in `candle_source_comparison.csv`. This is not a reconstruction of every original G3 indicator from the freshly fetched candles.

The common three-bar warmup makes 09:30 the first possible CVD-ready clock. Compare filter effects against `covered_control`; compare the combined warmup/filter effect against `base_control`. Reports include 5, 9 and 15 bps modeled round-trip costs, all sealed sessions including zero-trade days, monthly results and daily realized drawdown. No live orders are placed.

Inherited limitations remain: early universe membership is partly retrospective; opening OI and October 30 include retrospective recovery; October 28 uses a restricted 215-stock execution cohort. Original G3 slots were chosen using outcomes through October 27. The three later sealed sessions are a small descriptive check, not sufficient independent validation. Today's five-minute CVD observations do not constitute a completed G3 execution replay.

## Reproduce or refresh

From the repository root, choose fresh output names. Use `python -B` to avoid writing bytecode into existing packages.

```powershell
python -B -m g3_v18_20261102.cvd_kite_proxy_20261102.data_fetch --start 2026-07-29 --through 2026-11-02 --output g3_v18_20261102/cvd_kite_proxy_20261102/kite_data_NEW
python -B -m g3_v18_20261102.cvd_kite_proxy_20261102.calculate --data g3_v18_20261102/cvd_kite_proxy_20261102/kite_data_NEW --output g3_v18_20261102/cvd_kite_proxy_20261102/calculated_NEW
python -B -m g3_v18_20261102.cvd_kite_proxy_20261102.run --features g3_v18_20261102/cvd_kite_proxy_20261102/calculated_NEW/features.parquet --feature-provenance g3_v18_20261102/cvd_kite_proxy_20261102/calculated_NEW/configuration.json --output g3_v18_20261102/cvd_kite_proxy_20261102/results_NEW --through 2026-11-02
python -B -m g3_v18_20261102.cvd_kite_proxy_20261102.publish --features g3_v18_20261102/cvd_kite_proxy_20261102/calculated_NEW/features.parquet --membership g3_v18_20261102/cvd_kite_proxy_20261102/kite_data_NEW/universe_membership.parquet --backtest g3_v18_20261102/cvd_kite_proxy_20261102/results_NEW --output g3_v18_20261102/cvd_kite_proxy_20261102/dashboard_NEW --session-date 2026-11-02
```

The fetch requires an existing valid local Kite API session. No account upgrade or purchase is automated. The backtest uses available sealed G3 sources through the requested date and refuses to manufacture missing execution history.

```powershell
python -B -m pytest -q -p no:cacheprovider g3_v18_20261102/cvd_kite_proxy_20261102/tests_features.py g3_v18_20261102/cvd_kite_proxy_20261102/tests_data_fetch.py g3_v18_20261102/cvd_kite_proxy_20261102/tests_selection.py g3_v18_20261102/cvd_kite_proxy_20261102/tests_runner.py
```
