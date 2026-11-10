# V14 morning Nifty and stock momentum check (V4 research)

**Decision:** Do not add a Nifty-level entry threshold to V14. The tested
Nifty filters cut losing days by taking fewer trades, but every Nifty-only
version reduced total net profit. A two-consecutive-stock-bar rule helped the
later October–November slice, yet lost substantial July–September profit.
The 63 sessions have been researched repeatedly, so the apparent later-slice
benefit needs new forward sessions before promotion.

This is a backtest-only experiment on the V14 V3 combined OI/body/wick rule.
The current V14 V1 and V3 entrypoints/configurations remain unchanged.

## Indicator definitions and timing

The relevant Nifty **level** is the dated near-month Nifty futures percentage
change from its 09:20 session open, signed in the proposed trade direction.
An absolute Nifty index point threshold would move with the index and contract.
The current 5-minute Nifty candle return is a separate signal. Both are read
from the exact completed bar at the 09:20–09:45 stock signal, before the
09:21–09:46 confirmation minute; no later Nifty bar or forward fill is used.
The original G7 rule already rejects only when **both** Nifty directions
oppose the trade, and remains in every arm.

All added gates act on the full pre-quota stock candidate pool before core
preference, stock picker and setup quota. Fills, G7 Nifty and low-efficiency
permissions, V14 exits, costs, integer allocation and the two-loss lock are
then replayed independently for every arm. Missing exact feature evidence
fails that branch. The one missing 09:30 Nifty futures bar on 18 September
cannot be treated as a flat market.

| Arm | Additional five-minute signal rule |
|---|---|
| V3 combined | None; exact V3 control |
| Nifty session 0.05 | Signed Nifty move from open ≥0.05% |
| Nifty session 0.10 | Signed Nifty move from open ≥0.10% |
| Nifty bar 0.05 | Signed current completed candle move ≥0.05% |
| Nifty both align | Signed session and candle moves each ≥0 |
| Stock relative open | Signed stock return from open minus Nifty futures return from open ≥0 |
| Stock two bar | At 09:25–09:45, current and immediately previous completed stock 5m returns both favor the trade; 09:20 keeps native rules because there is no previous completed in-session bar |
| Nifty OR stock relative | Signed Nifty session move ≥0.05% **OR** signed stock-minus-Nifty open return ≥0.20% |

All eight arms and their thresholds were fixed in
`configs/strategies/v13_v10_g_14_momentum_v4_research.json` before the full
replay. Prior G3 research and descriptive V3 screens had already been viewed,
so this is exploratory multiple-comparison evidence, not an independent test.

## Full 63-session result, 9 bps round-trip costs

| Arm | Selected | Trades | Win rate | Net P&L | Losing days |
|---|---:|---:|---:|---:|---:|
| V3 combined | 282 | 183 | 62.30% | ₹76,315 | 30 |
| Nifty session 0.05 | 171 | 136 | 62.50% | ₹58,027 | 23 |
| Nifty session 0.10 | 138 | 110 | 59.09% | ₹25,907 | 22 |
| Nifty bar 0.05 | 99 | 86 | 62.79% | ₹32,905 | 21 |
| Nifty both align | 153 | 122 | 63.93% | ₹52,392 | 25 |
| Stock relative open | 269 | 173 | 61.27% | ₹52,425 | 30 |
| Stock two bar | 228 | 150 | 62.67% | ₹80,225 | 25 |
| Nifty OR stock relative | 279 | 181 | 62.43% | ₹77,428 | 29 |

The Nifty 0.05% session filter is a useful example of the trade-off: it
saved 7 losing days, but lost ₹18,288 of net profit. The stronger 0.10%
threshold lost still more. Nifty agreement did not consistently predict
the better V14 entries in this history.

The stock two-bar rule is the best of these eight on full-period 9 bps net,
but its gain over V3 is only **₹3,910**. It removed 33 executed trades.
Gross P&L fell from ₹158,392 to ₹147,488; transaction cost fell from
₹82,077 to ₹67,263. It preserved fewer winners and avoided fewer losers:
positive-trade profit fell by ₹70,717, while absolute losing-trade P&L fell
by ₹74,626. Its worst day worsened from −₹12,825 to −₹16,207. At 5 bps
it trails V3 by ₹2,674; at 15 bps it leads by ₹13,785.

## Time-slice check, 9 bps

| Arm | July–September (43 sessions) | 1 October–2 November (20 sessions) |
|---|---|---|
| V3 combined | 121 trades, 64.46% wins, +₹95,024, 19 losing days | 62 trades, 58.06% wins, −₹18,709, 11 losing days |
| Nifty session 0.05 | 94 trades, 67.02% wins, +₹92,633, 12 losing days | 42 trades, 52.38% wins, −₹34,606, 11 losing days |
| Nifty both align | 82 trades, 67.07% wins, +₹67,433, 16 losing days | 40 trades, 57.50% wins, −₹15,041, 9 losing days |
| Stock two bar | 103 trades, 62.14% wins, +₹43,336, 19 losing days | 47 trades, 63.83% wins, +₹36,889, 6 losing days |

The two-bar change is strongly regime-dependent: **−₹51,688** versus V3 in
July–September, then **+₹55,598** in October–November. October alone is
+₹34,140 for two-bar versus −₹6,870 for V3, and 2 November contributes
another +₹14,588 to their difference. This later period has already been
studied; it is not a virgin holdout. No rule here establishes a reliable
probability of profit on every day or trade. The unchanged-rule full-pool
V14 control from the preceding experiment earned ₹100,589 at 9 bps, above
all eight arms here; archived V14 V1 is a different selected-ancestor cohort.

## Evidence and reproduction

The strict V4 replay exactly matches the sealed V3 control on all 846
day/order/cost rows for fills, execution, prices, size, costs and P&L.
All 1,619 selected feature rows match the independently recomputed Nifty
permission source; every selected order satisfies its named added gate.
The output ledger, daily totals and recorded file/code hashes reconcile.
Nine focused momentum tests pass, in addition to the previous V14 tests.

Run `python -B -m research.g14_momentum_backtest --output-dir
C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/runs/2026-11-03/NEW_UNIQUE_DIR`.
The completed frozen run is
`C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/runs/2026-11-03/nifty_stock_momentum_v4_research_20261103/`.
Use `daywise_9bps_comparison.csv` for each session,
`overall_by_period_and_cost.csv` for 5/9/15 bps and the two periods,
`slot_side_results.csv` for slot/side detail,
`selected_order_features.csv` and `permission_audit.csv` for causal gate
decisions, and `artifact_manifest.json` for source/output hashes.

The data use completed five-minute bars and modeled one-minute fills with
stop-first ambiguous bars and fixed assumed costs. Prior related G3 research
on Nifty spot, breadth and relative strength also failed to establish a
robust general improvement (`g3_indicator_lab_20261103/README.md`); Nifty
spot and the dated futures used here are different inputs. More indicators
should not be promoted solely on this reused history.
