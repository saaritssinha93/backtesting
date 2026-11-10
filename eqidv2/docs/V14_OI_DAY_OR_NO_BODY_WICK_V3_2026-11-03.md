# V14 V3: OI day OR, no minimum body or maximum wick

Backtest only. The rule is implemented in
`configs/strategies/v13_v10_g_14_oi_day_or_v3.json` and
`research/g14_oi_day_or_selection.py`. Run it with
`python -B fno_v13_v10_g_14_oi_day_or_v3_backtest.py`. The inherited G7
Nifty-futures and market-quality permissions are enforced. The original V14
V1 entrypoint and configuration remain available for rollback.

## Effective entry change

For LONG and SHORT at each of 09:20, 09:25, 09:30, 09:35, 09:40 and 09:45:

* Replace the previous per-slot **minimum OI change** with `(same-contract OI
  day change > 1.00%) OR (OI rose over the immediately previous completed 5m
  bar)`. The day reference is the first completed 09:20 OI. At 09:20 the day
  branch is zero by definition; its rising branch uses the observed 09:16 to
  09:20 four-minute OI comparison. Exactly +1.00% does not pass the day branch.
  Missing or stale comparison evidence fails that branch.
* Remove the 5m minimum candle-body ratio and the 1m maximum directional-wick
  ratio from both active and core eligibility. The separate 09:25 SHORT
  **minimum** lower-wick rule remains.
* Retain OI validity and upper cap, price direction, EMA, volume, confirmation,
  extension, picker/quota, targets, stops, profit protection, capital limits,
  and the two-realized-loss daily lock.

The OI day branch cannot admit a row with completely missing previous OI,
because the inherited OI validity/upper-cap checks still need it. There were
no such >1% rows in this frozen candidate source.

## Strict backtest result

63 represented sessions, 29 July through 2 November 2026; same frozen
candidate/minute source and execution model for all four experimental arms.
The 9 bps round-trip cost comparison is:

| Arm | Selected | Executed trades | Wins | Win rate | Net P&L | Losing days |
|---|---:|---:|---:|---:|---:|---:|
| Reconstructed unchanged-rule control | 119 | 82 | 60 | 73.17% | ₹100,589.47 | 14 |
| Remove body/max-wick only | 177 | 109 | 75 | 68.81% | ₹70,083.18 | 21 |
| OI day OR only | 196 | 142 | 88 | 61.97% | ₹33,223.97 | 27 |
| **Combined requested change** | **282** | **183** | **114** | **62.30%** | **₹76,314.95** | **30** |

Compared with the same-source control, the combined change made 101 more
trades but reduced net P&L by **₹24,274.52**, reduced win rate by **10.88
percentage points**, and added **16 losing days**. The daywise P&L improved
on 29 days, worsened on 27, and was unchanged on 7; the larger losses drove
the lower total.

| Round-trip cost | Reconstructed control | Combined change | Difference |
|---|---:|---:|---:|
| 5 bps | ₹116,918.34 | ₹112,793.51 | −₹4,124.83 |
| 9 bps | ₹100,589.47 | ₹76,314.95 | −₹24,274.52 |
| 15 bps | ₹76,096.16 | ₹21,597.10 | −₹54,499.06 |

The later assessment slice (1 October through 2 November, 20 represented
sessions) at 9 bps was **28 trades, 64.29%, +₹1,843.38 and 7 losing days**
for the control, versus **62 trades, 58.06%, −₹18,709.15 and 11 losing
days** for the combined change. October alone was −₹6,869.81 for the
combined rule versus +₹1,843.38 for the reconstructed control.

## Comparator and audit

The archived V14 V1 result is 73 trades, 76.71% win rate, +₹131,434.73 and
11 losing days at 9 bps. It started from stocks already selected by G4/G6/G7,
so it is **context**, not the direct causal comparison for a rule that changes
selection. The fresh unchanged-rule full-pool control includes all 105
archived V14 identities plus 14 previously excluded ancestor candidates.
On the 105 overlaps, permission, fill, execution, price, quantity, costs, and
per-trade P&L match exactly. Nine of the 14 additions execute and explain
the 5 bps control/V1 P&L gap of −₹29,047.05.

The strict run has a candidate, minute-source, permission, trade, and output
hash audit. All 774 selected arm/day candidates had valid inherited-permission
source decisions. The archived 105/105 morning permission decisions match;
the archived sample has no positive low-efficiency-veto example, so that one
branch lacks positive-case parity evidence. The history was researched before
this experiment and is not an untouched holdout. One-minute bar fills,
stop-first ambiguous bars, and assumed 5/9/15 bps costs are modeled outcomes.

## Files and rollback

Results are in
`C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/runs/2026-11-03/oi_day_or_no_body_wick_v3_strict_fixed/`:

* `daywise_9bps_comparison.csv` — daily control and each ablation;
* `overall_by_period_and_cost.csv` — all-period and later-slice totals;
* `slot_side_results.csv`, `executed_trades.csv`, `permission_audit.csv`;
* `summary.json`, `artifact_manifest.json` — method, limitations and hashes.

Run the unchanged V14 V1 with `python -B fno_v13_v10_g_14_backtest.py`.
Its frozen reference result is
`C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/runs/2026-11-03/morning12_config_complete_v1/`.
The new combined rule is kept as an explicit V14 V3 backtest variant because
it did not improve the tested risk/profit objective. It does not change G3,
G7, live trading, or paper trading.
