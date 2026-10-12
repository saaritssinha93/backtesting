# G-3 09:20 joint entry search: 1% LONG/SHORT targets and 70% win-rate screen

The requested **1.00% target is applied to both new 09:20 LONG and SHORT
orders**. The initial stop remains 1.25%, tightening to 1.00% after 120
minutes; open trades square off at 15:15 IST. All percentages are measured
from the actual entry price. This is a backtest experiment; the published
G-3 setups and live/paper configuration were not changed.

The expanded search evaluated **702 configurations** over all 49 published
G-3 sessions, July 29–October 12, 2026. The largest observed full-history
sample meeting the 70% screen has **10 added trades, 7 wins (70.00%), and
+₹18,803.43 incremental net P&L**. The stricter nine-trade alternative earns
more, but contains fewer entries. No tested configuration validates both
LONG and SHORT at 70%, or reaches 70% on its nonempty recent-session sample.

## Best full-history configuration by number of entries

Profile: `5m_reference_5m__1m_volume_180`.

| Entry gate | LONG | SHORT |
| --- | ---: | ---: |
| 09:15–09:20 directional price magnitude, active/core | ≥0.30% / ≥0.30% | ≥0.13% / ≥0.20% |
| 09:16→09:20 OI increase | ≥0.10% | ≥0.05% |
| Five-minute volume ratio | ≥3.0 | ≥1.5 |
| 09:21 candle body ratio | ≥0.75 | ≥0.65 |
| 09:21 directional wick ratio | ≤0.35 | ≤0.35 |
| 09:21 confirmation volume ratio | ≥1.8 | ≥1.8 |
| Profit target | **1.00%** | **1.00%** |
| Initial stop / stop after 120 minutes | 1.25% / 1.00% | 1.25% / 1.00% |

The completed 09:21 candle still confirms direction relative to the signal
close. Entries begin at 09:22 or later, subject to the existing trigger and
10-minute expiry. Core-first ranking, one LONG/two SHORT daily limits, EMA
direction, NIFTY SHORT condition, data-source checks, and portfolio allocation
retain their existing rules.

The OI change here spans **four minutes, 09:16 to 09:20**. The price signal
spans the completed five-minute cash bar, 09:15 to 09:20.

## Results and trade-offs

The table below uses the reference five-minute gates except the last row.
One-minute settings are stage names in `trials.csv`; the best row is expanded
above. Every row uses the new 1% targets on both sides.

| Configuration | Added trades | Wins | Win rate | Incremental net P&L |
| --- | ---: | ---: | ---: | ---: |
| Strict confirmation: body 0.75/0.65, wick 0.35, volume ≥2.0 | 9 | 7 | 77.78% | +₹24,053.43 |
| **Volume ≥1.8 both sides, same strict body/wick** | **10** | **7** | **70.00%** | **+₹18,803.43** |
| Volume ≥1.6 LONG / ≥1.7 SHORT, same strict body/wick | 12 | 8 | 66.67% | +₹22,559.38 |
| Copied reference confirmation | 19 | 8 | 42.11% | −₹11,999.55 |
| Coarse relaxed confirmation | 20 | 11 | 55.00% | +₹10,478.73 |
| Contract-floor five-minute gates + coarse relaxed confirmation | 23 | 11 | 47.83% | −₹10,829.90 |

There are 75 parameter combinations clearing the pooled full-history screen,
but they represent only **two distinct selected order sets**. Parameter count
must not be mistaken for independent validation. The largest qualifying sample
is ten trades. The predeclared tie-break is larger trade count, then higher
incremental net P&L, then earlier grid step.

Relative to the same ten-trade entry configuration with the old 0.97% LONG /
2.67% SHORT targets, the 1% targets increase net P&L from ₹17,903.43 to
₹18,803.43 (**+₹900**) with the same seven wins. The target change is not
uniformly beneficial: the broader copied reference configuration loses
₹7,300 of net P&L while keeping the same win count.

| Full-period portfolio | Executed trades | Wins | Win rate | Net P&L | Daily-equity max drawdown |
| --- | ---: | ---: | ---: | ---: | ---: |
| Published G-3 baseline | 112 | 80 | 71.43% | ₹286,682.16 | ₹14,142.14 |
| Baseline + best ten-trade 09:20 configuration | 122 | 87 | 71.31% | ₹305,485.58 | ₹14,142.14 |

These rupee results use the existing **₹100,000 backtest margin per entry,
5× exposure (₹500,000), ₹1,000,000 portfolio capital, and 5 bps round-trip
cost**. The best configuration neither displaces old trades nor changes their
P&L. Wins are trades with positive net P&L after costs.

## Why the observed 70% is not a live approval

For the best ten-trade configuration:

| Slice | Added wins / fills | Win rate | Added net P&L |
| --- | ---: | ---: | ---: |
| LONG, full period | 7/8 | 87.50% | +₹29,303.43 |
| SHORT, full period | 0/2 | 0% | −₹10,500.00 |
| Earlier 43 sessions through September 30 | 7/8 | 87.50% | +₹24,779.16 |
| Six recent sessions, October 5–12 | 0/2 | 0% | −₹5,975.74 |

The pooled screen requires at least ten added fills, ≥70% wins on added and
combined trades, and positive incremental net P&L. A separate two-side check
also requires at least five fills and ≥70% wins on each side. No profile passes
that two-side check; the highest SHORT win rate among profiles with at least
five SHORT fills is only 20%.

Choosing solely on the earlier period selects
`5m_reference_5m__1m_body_072_062`: 10/14 wins (71.43%) and +₹22,444.35
there. It then loses all four recent trades (−₹13,469.79), leaving 10/18
(55.56%) and +₹8,974.56 overall. Across the entire grid, the maximum recent
selected count is six and the maximum nonempty recent win rate is 40%.

The dates and prior outcomes have already been examined during earlier
searches, so this chronological split is not an untouched holdout. All 09:16
OI was assembled retrospectively and has not been proven available at the
live 09:22 decision. The feature export includes 3,696 complete stock-days in
the 49-session baseline period; 20 sessions have no valid opening OI for any
stock. Some earlier universes use sealed monthly mappings, which can carry
hindsight bias. October 1 is excluded because it is absent from the published
paired baseline. These limitations apply to the historical results above.

## Implementation and verification

`research/g3_0920_joint_70_search.py` declares 27 five-minute tiers × 26
one-minute tiers. It first sweeps five-minute filters at strict confirmation,
then repeats the sweep at each one-minute tier. The search plan is written
before replay. All 37 unique selected candidate paths were verified and
simulated using real cash-minute data; the 702 configurations produce 75
distinct selected portfolios when core status and rank are retained.

The reusable selection audit retains every stock passing the broad native
gates, including ranked-out candidates, then applies the original contract
predicates for each profile. It was checked against the original full-universe
selector on five boundary/reference profiles and both chosen finalists, with
matching selections, ranks, core status, and filter-pass counts. All new order
targets are 1%; all 14 target exits in the candidate pool occur at exactly 1%
gross return. All **51 focused tests passed**. Artifact and source hashes
verified, and the published backtest pointer hash is unchanged.

Saved run:
`C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\research_0920\runs\2026-10-12\joint70_target1_20261012T2355`.

- `search_plan.json`: all predeclared gates and the 1% target override.
- `trials.csv` / `trials.json`: all 702 configurations and period/side results.
- `best_observed_70_orders.csv`, `best_observed_70_variant_trades.csv`, and
  `best_observed_70_daily_comparison.csv`: the best ten-trade result.
- `best_discovery_70_*`: the separate earlier-period selection and its replay.
- `selected_membership_all_profiles.csv`: actual selected identities and ranks.
- `summary.json`: best profiles, exit settings, coverage, and source evidence.
- `manifest.json`: artifact and source SHA-256 hashes.

Reproduce into a new directory:

```powershell
python -m research.g3_0920_joint_70_search --output-dir C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\research_0920\runs\2026-10-12\joint70_target1_reproduction_NEW
```
