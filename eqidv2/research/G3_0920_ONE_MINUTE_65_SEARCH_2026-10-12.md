# G-3 09:20: gradual 09:21 confirmation relaxation to a 65% bar

**Best exploratory full-history result:** keep the copied 09:25 five-minute
signal filters at the 09:20 clock, keep the strict 09:21 body and wick gates,
and reduce only the 09:21 confirmation-volume ratio from **2.0 to 1.6 LONG / 1.7
SHORT**. Across all 49 published G-3 backtest sessions (July 29–October 12,
2026), this adds 12 executed 09:20 trades with 8 wins (**66.67%**), versus 9
trades with 7 wins (77.78%) under the previous strict confirmation. The new
profile earns +₹21,509.38 incremental net P&L after costs. It is **not approved
for live/paper trading**: SHORT wins 0/3, and the three October 5–12 additions
all lose. The production strategy, published backtest pointer, and dashboard
configuration were not changed.

## What was varied

The full dated stock universe was reranked at each of 24 predeclared profiles.
The cumulative sequence relaxes confirmation volume, then candle body, then
directional wick; smaller axis probes test body changes at higher volume
requirements. The 09:20 signal-bar gates never change:

| Side | First-bar price magnitude, active / core | 09:16→09:20 OI rise | Five-minute volume ratio |
| --- | ---: | ---: | ---: |
| LONG | ≥0.30% / ≥0.30% | ≥0.10% | ≥3.0 |
| SHORT | ≥0.13% / ≥0.20% | ≥0.05% | ≥1.5 |

The chosen **09:21** confirmation gates are:

| Side | Body ratio | Directional wick maximum | One-minute volume ratio |
| --- | ---: | ---: | ---: |
| LONG | ≥0.75 | ≤0.35 | **≥1.6** |
| SHORT | ≥0.65 | ≤0.35 | **≥1.7** |

The body/wick and price-direction tests use the real completed 09:21 candle;
the close must exceed the 09:20 signal close for LONG or fall below it for
SHORT. Same-contract 09:16-to-09:20 OI evidence, EMA direction, NIFTY SHORT
gate, one LONG/two SHORT daily quota, picker, stops, targets, 09:22-or-later
entry, 10-minute entry expiry, backtest capital, 5× exposure, and costs stay
unchanged. Confirmation-volume floors cannot go below the native G-3 LONG
1.10 / SHORT 1.20 limits. Looser selection can **replace** a previously picked
stock, so every profile was selected and portfolio-replayed afresh.

## Comparison

| 09:21 setting with reference 09:20 five-minute gates | Added trades | Wins | Added win rate | Incremental net P&L |
| --- | ---: | ---: | ---: | ---: |
| Strict: volume ≥2.0 both sides | 9 | 7 | 77.78% | +₹23,153.43 |
| Volume ≥1.8 both sides | 10 | 7 | 70.00% | +₹17,903.43 |
| **Volume ≥1.6 LONG / ≥1.7 SHORT** | **12** | **8** | **66.67%** | **+₹21,509.38** |
| Volume ≥1.4 LONG / ≥1.5 SHORT | 16 | 9 | 56.25% | +₹10,774.56 |
| Native volume floors, strict body/wick | 17 | 9 | 52.94% | +₹4,274.56 |
| Copied reference 09:21 filters | 19 | 8 | 42.11% | −₹4,699.55 |

Four of the 24 profiles satisfy the **observed full-history** screen of at
least 10 added fills, at least 65% wins on those fills, at least 65% combined
G-3 win rate, and positive incremental net P&L. The selected profile has the
largest added-trade count among them. Compared with the published G-3 baseline
(112 executed, 80 wins, 71.43%, ₹286,682.16 net P&L), its combined ledger has
**124 executed trades, 88 wins, 70.97%, ₹308,191.53 net P&L**. The
daily-equity maximum drawdown remains ₹14,142.14.

The added LONG side is **8/9 wins (88.89%), +₹33,003.43 net**; the added SHORT
side is **0/3 wins, −₹11,494.05 net**. None of the 24 profiles reaches 65%
on its added SHORT trades with a meaningful sample; the pooled threshold
therefore does not approve both sides. On the 43 earlier sessions, the chosen
profile is 8/9 and +₹28,479.16; on October 5–12 it is **0/3 and
−₹6,969.79**. A profile chosen strictly from those earlier sessions instead
won 10/14 there (71.43%) but only 0/4 in October, leaving 10/18 (55.56%)
overall. Thus the full-history 66.67% is a tuned, historical observation, not
evidence that either side will maintain 65% going forward.

There are 3,696 complete opening-feature stock-days in the published period;
20 sessions have no valid 09:16 OI for any stock. All opening OI in this
research was assembled retrospectively, with no proof it arrived by the live
09:22 decision. Keeping the five-minute gates fixed, the loosest **tested**
one-minute profile selected 19 historical signals under the daily quota; no
tested profile selected more than four in the six recent October sessions. A meaningful
recent ≥65% validation sample cannot be obtained from the saved data. Some
early universe dates also rely on sealed monthly mapping, with potential
hindsight bias. October 1 is absent from the published baseline and was
excluded from this comparison.

## Artifacts and verification

Immutable run:
`C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\research_0920\runs\2026-10-12\search_1m65_20261012T2340`.
`trials.csv` and `trials.json` record all 24 gates and outcomes;
`selected_membership_all_profiles.csv` records the actual stock picks;
`best_observed_65_orders.csv`, `best_observed_65_variant_trades.csv`, and
`best_observed_65_daily_comparison.csv` are the selected research ledger.
`best_discovery_*` records the separate earlier-period choice. `summary.json`
records coverage, source evidence, and the fixed signal gates. Artifact and
source SHA-256 hashes in `manifest.json` verify, including the G-3 contract
and trade simulator. The published current-backtest pointer hash is unchanged.
All 34 focused 09:20 tests pass.

Reproduce into a new directory with:

```powershell
python -m research.g3_0920_one_minute_65_search --output-dir C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\research_0920\runs\2026-10-12\search_1m65_reproduction_NEW
```
