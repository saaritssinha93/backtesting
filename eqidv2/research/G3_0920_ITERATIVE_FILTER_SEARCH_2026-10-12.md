# G-3 09:20 five-minute / one-minute filter search

**Result: no 09:20 LONG+SHORT configuration is approved at a 70% win-rate bar.**
Sixteen backtest-only profiles were replayed over every session in the published
G-3 backtest, July 29–October 12, 2026 (49 sessions). The profile with the
largest *observed full-history* 09:20 sample above 70% won 7 of 9 trades
(77.78%). Its two October 5–12 trades both lost, and its lone SHORT trade
lost. Loosening the filters to obtain 20–23 entries reduced 09:20 win rate to
55.00%–52.17%. This search does not change live or paper trading, the main
dashboard, the published backtest pointer, or the production G-3 contract.

## Search and decision rules

The 09:20 signal uses the completed 09:15–09:20 cash bar and the same dated
futures contract's **09:16-to-09:20 OI change** (four minutes). The completed
09:21 cash minute confirms direction; orders can first enter at 09:22. This is
the existing 09:25 LONG/SHORT setup copied to a research-only 09:20 clock.
The search changes only numeric first-bar price/OI/volume and confirmation
body/wick/volume gates. EMA direction, source checks, NIFTY SHORT gate, one
LONG/two SHORT per-day quota, rank picker, target/stop, entry expiry,
₹100,000 backtest margin per entry, 5× exposure, costs, and portfolio
allocation stay fixed. Every profile reranks the full dated stock universe;
it does not filter the 19 trades selected by the reference profile.

The grid begins with strict five-minute and strict 09:21 rules, relaxes the
five-minute rules through balanced, reference, and relaxed tiers, then repeats
those four five-minute tiers at progressively looser 09:21 tiers. That gives
16 combinations. The final 09:21 tier relaxes candle body and wick but keeps
the production side-specific minimum confirmation-volume ratio. The reference
combination reproduces the earlier 19-trade, 8-win, −₹4,699.55 result.

Profiles were ranked using the 43 published sessions through September 30.
The six October 5–12 sessions were reported separately. This is a
chronological check, **not a pristine prospective holdout**: the earlier
reference outcomes were already viewed before this grid was set. A profile
needed at least 10 discovery 09:20 fills, at least 70% wins on those fills and
on the combined strategy, positive incremental P&L, and no side with five or
more fills below 70%. None qualified. The diagnostic profile chosen by that
rule, `5m_reference__1m_relaxed`, had the highest discovery win rate among
profiles with at least 10 fills: 10/16 (62.50%). Its later result was 1/4
(25.00%), so it also failed. A separate, explicitly exploratory full-history
ranking identifies the largest 09:20 sample above 70% for comparison below.

## Results

| Profile | Added 09:20 trades | 09:20 wins | 09:20 win rate | Incremental net P&L | Combined G-3 win rate |
| --- | ---: | ---: | ---: | ---: | ---: |
| Strict 5m + strict 1m | 4 | 2 | 50.00% | +₹3,224.26 | 70.69% |
| Balanced 5m + strict 1m | 7 | 5 | 71.43% | +₹17,024.26 | 71.43% |
| **Reference 5m + strict 1m** | **9** | **7** | **77.78%** | **+₹23,153.43** | **71.90%** |
| Reference 5m + reference 1m | 19 | 8 | 42.11% | −₹4,699.55 | 67.18% |
| Reference 5m + relaxed 1m | 20 | 11 | 55.00% | +₹17,478.73 | 68.94% |
| Relaxed 5m + relaxed 1m | 23 | 12 | 52.17% | +₹6,170.10 | 68.15% |

The stricter of two tied *full-history observed-70%* configurations is
`5m_reference__1m_strict`. `5m_relaxed__1m_strict` selects the same nine
trades and has identical results, so the reference five-minute tier wins the
predeclared stricter-tier tie-break:

| Side | 09:20 price magnitude (active / core) | OI rise | 5m volume ratio | 09:21 body | Maximum wick | 09:21 volume ratio |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| LONG | ≥0.30% / ≥0.30% | ≥0.10% | ≥3.0 | ≥0.75 | ≤0.35 | ≥2.0 |
| SHORT | ≥0.13% / ≥0.20% | ≥0.05% | ≥1.5 | ≥0.65 | ≤0.35 | ≥2.0 |

Against the published baseline's 112 executed trades, 80 wins (71.43%),
₹286,682.16 net P&L, and ₹14,142.14 daily-equity drawdown, this profile has
121 executed trades, 87 wins (71.90%), ₹309,835.58 net P&L, and the same
₹14,142.14 drawdown. Its added LONG side is 7/8 wins (87.50%) and
+₹28,403.43; its SHORT side is **0/1** and −₹5,250.00. The early 43-session
portion is 7/7 wins and +₹29,129.16, but October 5–12 is **0/2** and
−₹5,975.74. The apparent full-period 77.78% is therefore based on nine trades
and does not validate the LONG or SHORT setup for live use.

The full grid reaches at most **six selected 09:20 orders** in the six recent
sessions, before fill. No profile could meet the stated ten-fill validation
minimum there, and no searched profile reaches 70% over those recent trades.
Across the 49 sessions, 3,696 dated stock-days have complete opening research
features; 20 sessions have no valid 09:16 OI for any stock. Every opening OI
observation in this backtest was acquired retrospectively, not proven
available at the 09:22 live decision. Several early dates also use a sealed
monthly universe mapping, which can carry hindsight bias. October 1 is in
the feature export but not the published G-3 baseline and was excluded.

## Reproduce and inspect

The immutable research run is
`C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\research_0920\runs\2026-10-12\search_full_20261012T2334`.
`trials.csv` and `trials.json` contain every threshold and result;
`selected_membership_all_profiles.csv` records each selected signal ID;
`union_raw_trades.csv` contains once-simulated candidate outcomes;
`best_observed_70_*` contains the exploratory nine-trade configuration;
`selected_*` contains the discovery-ranked diagnostic configuration.
`summary.json` records the baseline, dates, source evidence, and selection
reason. `manifest.json` hashes all eleven artifacts and the three research
source modules. Its artifact and code hashes were verified after the run.

Reproduce to a **new** directory with:

```powershell
python -m research.g3_0920_parameter_search --output-dir C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\research_0920\runs\2026-10-12\search_reproduction_NEW
```

The search also corrected research source parity for futures symbols with
`&` in their names: raw files use `safe_contract_stem`, as the feature
exporter already did. The matching M&M and GVT&D source bars and OI values
were verified. All 25 focused 09:20 selection, replay, and search tests pass.
