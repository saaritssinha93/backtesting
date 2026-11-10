# V14 morning AVWAP, RVOL and volume-pressure check

**Decision:** The tested AVWAP, same-clock RVOL and candle-pressure gates do
not improve the full 63-session V14 result. Keep the current V14 entry rules.
True buyer-initiated minus seller-initiated **volume delta was not tested**:
the frozen stock inputs contain completed OHLCV bars, without aggressor-side
trade prints or bid/ask event data. NSE offers separate [historical order and
trade data](https://betanseapi.nseindia.com/static/market-data/eod-historical-data-subscription),
but that data is not present in this V14 snapshot.

This is an isolated backtest-only V14 V5 research variant. It retains all
V14 V3 combined OI/body/wick rules, exits, original volume gate, G7 Nifty
and market-quality permissions, allocation and two-loss lock. Each extra
signal-time gate is applied before stock ranking and slot quota; fills and
portfolio are replayed separately for each arm.

## What was measured

* **AVWAP side:** close must be on the trade's side of a VWAP anchored to the
  first completed 09:20 cash bar. This is `sum(HLC3 × bar volume) / sum(bar
  volume)` from completed five-minute bars, **a bar-price approximation**, not
  trade-level exchange VWAP. The stored V18 VWAP matches all 77,812
  comparable rows; 1,724 previously unenriched late-slot rows are rebuilt
  from frozen OHLCV. Flagged or synthetic prior bars break the cumulative
  feature.
* **Same-clock RVOL ≥1:** current stock 5m volume divided by the median of
  up to 20 strictly prior valid sessions of the *same stock at the same slot*.
  At least five prior observations are required. The first five represented
  sessions retain native rules while this additional indicator warms up;
  afterwards missing evidence fails. Native 5m `volume_ratio` is different:
  it compares with the previous 20 five-minute bars, and remains required.
  A separate **RVOL-ready control** uses identical warmup/coverage without
  the ≥1 threshold. All 71,282 comparable stored RVOL values match exactly;
  1,678 missing late-slot values are reconstructed from prior frozen days.
* **Candle volume pressure:** require positive side-signed volume-weighted
  close location across the latest **up to three** completed 5m bars. This is
  a price/volume proxy, **not** measured buy-minus-sell volume delta.

These three conditions were tested separately on the 09:20–09:45 LONG and
SHORT setups. The thresholds and rules were fixed before this replay in
`configs/strategies/v13_v10_g_14_flow_v5_research.json`. Related G3
indicator research on the same history had already been viewed, so this is
exploratory evidence, not an independent validation period.

## Results at 9 bps round-trip costs

63 represented sessions, 29 July–2 November 2026:

| Rule | Selected | Trades | Win rate | Net P&L | Losing days | Worst day |
|---|---:|---:|---:|---:|---:|---:|
| V14 V3 control | 282 | 183 | 62.30% | ₹76,315 | 30 | −₹12,825 |
| AVWAP side | 279 | 181 | 62.43% | ₹73,650 | 31 | −₹12,825 |
| RVOL-ready matched control | 281 | 182 | 62.09% | ₹70,615 | 30 | −₹12,825 |
| Same-clock RVOL ≥1 | 266 | 171 | 63.74% | ₹54,602 | 30 | −₹19,120 |
| Candle volume pressure | 267 | 176 | 60.80% | ₹53,458 | 31 | −₹15,905 |

AVWAP changed very few entries and lost **₹2,664** versus V3. RVOL ≥1
raised the trade win rate by 1.45 percentage points but reduced total net
profit by **₹21,713** versus V3. Compared with its own RVOL-ready matched
control, the threshold itself lost **₹16,014**. Pressure lost **₹22,857**
versus V3. All three filtered arms also earned less than V3 at both 5 and
15 bps. The simpler unchanged-rule full-pool V14 control from the preceding
experiment earned ₹100,589 at 9 bps, above every arm here.

The RVOL threshold did improve the later 1 October–2 November slice from
V3's **−₹18,709** to **−₹8,651**, taking 58 instead of 62 trades; it still
had **11 losing days**. It lost **₹26,072** against the matched-ready control
in July–September, offsetting that later improvement. AVWAP made no
executed-trade difference in the later slice; pressure made it worse.
This is not evidence of dependable profits or a robust regime switch.

## Verification and files

The replay's V3 control matches the sealed V3 ledger on all **846**
day/order/cost rows for fills, permissions, execution, prices, quantity,
costs and net P&L. All selected orders satisfy their stated feature gate;
output totals and file/code hashes reconcile. The source audits cover all
79,536 candidate rows, including reconstructed missing late-slot AVWAP and
RVOL values.
The 63 sessions were already researched, and fills use one-minute bars,
stop-first ambiguity and assumed 5/9/15 bps costs. Paper and live trading
were not changed.

Run `python -B -m research.g14_flow_backtest --output-dir
C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/runs/2026-11-03/NEW_UNIQUE_DIR`.
The completed run is
`C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/runs/2026-11-03/avwap_rvol_pressure_v5_research_20261103/`.
Its `daywise_9bps_comparison.csv` has daily results;
`overall_by_period_and_cost.csv` covers 5/9/15 bps and both date slices;
`slot_side_results.csv`, `selected_order_features.csv` and
`permission_audit.csv` explain slot, stock and gate decisions;
`artifact_manifest.json` records file and code hashes.
