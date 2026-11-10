# V14 morning price action check (V6 research)

**Decision: keep the existing V14 configuration.** The best new rule in this
replay increased full-period net profit, but it did not deliver a win rate above
70% for every slot and direction. Its later assessment period still lost money,
and at 15 bps it earned less than the unchanged-rule control. It is a research
candidate for new forward sessions, not a live or paper-trading change.

The replay covers 63 represented sessions from 29 July to 2 November 2026.
It uses the six V14 morning signals (09:20, 09:25, 09:30, 09:35, 09:40 and
09:45), both LONG and SHORT. Forty-three sessions through 30 September are
the development slice; 20 sessions from 1 October are the assessment slice.
This is **not an untouched holdout**: related G3 and V14 research has already
used this history.

## Rules and controls

The three price action rules were specified before the full replay in
`configs/strategies/v13_v10_g_14_price_action_v6_research.json`. Each was
tested separately on the unchanged-rule full-pool V14 selector (original)
and on the relaxed V14 V3 selector. The frozen five-minute candidate and
one-minute confirmation inputs are used. Original V14 targets, stops, G7
market permissions, fills, allocation, two-loss lock and 5/9/15 bps round-trip
costs are retained. Both control arms reproduce the sealed V3 replay's daily
selection, fills, trades, wins and net P&L.

| Additional rule | Clock and decision |
|---|---|
| Failed breakout veto | At the completed 5m signal, reject a long that breaks the immediately previous 5m high but closes back at/below it; reverse high/low for a short. This acts before ranking and quota. The 09:20 opening bar retains the base rule because it has no prior intraday bar. |
| Confirmation breakout | After native selection, require the next completed 1m close above the signal high for a long, or below the signal low for a short. A vetoed selection does not refill the quota. |
| Touch and hold | After native selection, require the next 1m range to touch the signal close and finish on the trade's side of that close. This is an OHLC range proxy; the order of touch and close within that minute is unknown. A veto does not refill the quota. |

Missing required feature evidence fails a rule. The confirmation breakout arm
vetoed 93 of V3's 282 reserved orders, leaving 189 selections and 125
executed trades. All accepted and vetoed reservations are in the audit file.

## Full period, 9 bps round-trip costs

| Selector and rule | Selected | Trades | Wins | Win rate | Net P&L | Losing days |
|---|---:|---:|---:|---:|---:|---:|
| Original control | 119 | 82 | 60 | 73.17% | +₹100,589 | 14 |
| Original + failed breakout veto | 110 | 79 | 55 | 69.62% | +₹79,138 | 14 |
| Original + confirmation breakout | 93 | 63 | 47 | 74.60% | +₹69,696 | 12 |
| Original + touch and hold | 104 | 71 | 53 | 74.65% | +₹86,523 | 12 |
| V3 control | 282 | 183 | 114 | 62.30% | +₹76,315 | 30 |
| V3 + failed breakout veto | 267 | 174 | 106 | 60.92% | +₹65,973 | 30 |
| **V3 + confirmation breakout** | **189** | **125** | **86** | **68.80%** | **+₹107,984** | **23** |
| V3 + touch and hold | 252 | 160 | 100 | 62.50% | +₹67,626 | 26 |

The V3 confirmation breakout earns **₹31,669 more than V3** and **₹7,395
more than the original control** at 9 bps over the full 63 sessions. Its 23
losing days are fewer than V3's 30 but more than the original control's 14.
It is the only new arm above both controls on this full-period 9 bps measure.
The original control is a full-pool reconstruction; the older selected-ancestor
V14 V1 result is a different cohort and is not a causal control for these
pre-selection rules.

## Per-slot LONG and SHORT result

The table compares the strongest new arm with its own V3 control at 9 bps.
Win rates are *observed* rates. A cell is counted toward the requested
`>70%` goal only if it has **at least 10 executed trades** and a rate strictly
above 70%.

| Slot | Side | V3 trades / wins | V3 net | Confirmation trades / wins | Confirmation win rate | Confirmation net | >70%, n≥10? |
|---|---|---:|---:|---:|---:|---:|---|
| 09:20 | LONG | 8 / 7 | +₹32,798 | 5 / 5 | 100.00% | +₹19,170 | No, n=5 |
| 09:20 | SHORT | 13 / 7 | −₹10,374 | 7 / 3 | 42.86% | −₹18,369 | No |
| 09:25 | LONG | 17 / 14 | +₹23,214 | 13 / 10 | 76.92% | +₹14,319 | **Yes** |
| 09:25 | SHORT | 14 / 11 | +₹18,028 | 10 / 9 | 90.00% | +₹15,598 | **Yes** |
| 09:30 | LONG | 10 / 3 | −₹25,910 | 8 / 3 | 37.50% | −₹14,013 | No |
| 09:30 | SHORT | 23 / 14 | +₹6,075 | 16 / 12 | 75.00% | +₹19,942 | **Yes** |
| 09:35 | LONG | 14 / 7 | −₹23,060 | 11 / 7 | 63.64% | −₹3,729 | No |
| 09:35 | SHORT | 13 / 8 | −₹602 | 8 / 5 | 62.50% | +₹779 | No |
| 09:40 | LONG | 19 / 10 | −₹19,118 | 8 / 5 | 62.50% | −₹7,563 | No |
| 09:40 | SHORT | 23 / 14 | +₹52,926 | 16 / 11 | 68.75% | +₹46,997 | No |
| 09:45 | LONG | 7 / 5 | +₹10,871 | 5 / 5 | 100.00% | +₹22,755 | No, n=5 |
| 09:45 | SHORT | 22 / 14 | +₹11,467 | 18 / 11 | 61.11% | +₹12,100 | No |

Only **3 of 12** confirmation breakout cells clear the descriptive `>70%,
n≥10` bar. The original control clears 2 of 12; V3 clears 2 of 12. All
eight arms fail the 12-of-12 objective. A 100% rate from five trades is not
evidence of a dependable 70% probability. Also, 09:40 SHORT is profitable
despite a win rate below 70%, so removing it just to raise a reported win
rate could discard much of the profit.

**Research lead:** The 09:30 SHORT confirmation breakout improves from
V3's 11 wins in 19 development trades and +₹5,609 to 10 wins in 14 trades
and +₹17,804. The later slice has just **2 confirmed trades**, both wins,
versus 4 V3 trades. This is too little later evidence to switch only that
slot on. A slot-specific switch chosen after viewing these results would
also add another layer of selection bias.

## Later-period and cost checks

| Arm | July–September, 9 bps | October–2 November, 9 bps | Full 63 sessions, 15 bps |
|---|---|---|---:|
| Original control | 54 trades, 77.78%, +₹98,746, 7 losing days | 28 trades, 64.29%, +₹1,843, 7 losing days | +₹76,096 |
| V3 control | 121 trades, 64.46%, +₹95,024, 19 losing days | 62 trades, 58.06%, −₹18,709, 11 losing days | +₹21,597 |
| V3 + confirmation breakout | 80 trades, 70.00%, +₹108,692, 15 losing days | 45 trades, 66.67%, **−₹708**, 8 losing days | +₹70,615 |

In the later 20 sessions, confirmation breakout improves V3 by ₹18,002,
but it still loses ₹708 and trails the original control by ₹2,551. No
slot/direction cell in that later slice has 10 executed trades, so its
per-cell win rates cannot support the target. At 15 bps, the rule still
improves V3, but earns **₹5,482 less than the original control**. The
15 bps win rates are 42.40% for the candidate and 45.12% for the original
control; observed win rate is sensitive to the assumed costs.

## Reproduction and limits

Run `python -B -m research.g14_price_action_backtest --output-dir
C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/runs/2026-11-03/NEW_UNIQUE_DIR`.
The completed run is
`C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/runs/2026-11-03/price_action_v6_checked_20261103/`.
Use `daywise_9bps_comparison.csv` for each day,
`overall_by_period_and_cost.csv` for all arms and cost assumptions,
`slot_side_results.csv` and `slot_70_coverage.csv` for the 12 setups,
`selected_order_features.csv` for each confirmation veto, and
`artifact_manifest.json` for file and code hashes. Eleven focused V6
feature/reporting tests pass.

An independent artifact check matched all 14 output and eight code hashes,
both controls' order ledgers and all 189 daily rows per control against the
sealed replay. All 1,512 arm/day/cost rows and 864 slot/side/cost/period rows
reconcile to executed trades and P&L. The confirmation audit includes eight
day/arm cases where every reserved order was vetoed; none refilled a quota.

One-minute fills and stops are modeled from bars; stop-first handling resolves
ambiguous bars conservatively. Fixed transaction costs are assumptions, not
observed broker fills. Repeated research on the same 63 sessions creates
selection bias. No target or stop adjustment was justified by this test.
Current V14, G3, paper and live configurations remain as they were.
