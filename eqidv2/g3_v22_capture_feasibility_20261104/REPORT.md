# G3 V22: can the daily top-two mover capture reach 70%?

**Finding:** The frozen G3 signal family cannot reach 70% of the 255 observed
top-two mover cases. An isolated broader momentum path selected 215 cases
(84.3%), but only 69 (27.1%) became portfolio trades. Its 64-session net P&L
was Rs 40,648.98 versus Rs 10,04,970.75 for uncapped V19. The broader path is
not a candidate for the running G3.

The cohort is 64 sealed G3 sessions, 29 July-3 November 2026. Each day has up
to two long and two short stocks with at least a 3% move from the first
available 09:16 open to the daily high or low. The 70% target is **179 of 255**
stock/day/direction cases. A selected order, a filled trade, and an entry
before the day's final extreme are separate outcomes.

## V19 and V18 check

V19 has **no per-slot entry quota**. It selected all 631 filter-passing orders
across 64 days; one day and setup selected 28 stocks. The inherited portfolio
reserves Rs 1 lakh per entry from Rs 10 lakh capital, so fills can still be
rejected for insufficient capital.

| Original G3 signal reach on 255 cases | Cases | Share |
|:---|---:|---:|
| V19 orders selected | 50 | 19.6% |
| V19 portfolio trades executed | 44 | 17.3% |
| Any strict same-side candidate in 34 enabled setups | 125 | 49.0% |
| Same, with confirmation before the final extreme | 114 | 44.7% |
| Any strict candidate in all 61 original donor setups | 154 | 60.4% |
| Any strict candidate at any observed feature clock and side, including unconfigured combinations | 178 | 69.8% |

These upper bounds waive later setup filters, ranking and entry fills. Even the
broadest strict-signal bound is one case short of 179. Reaching 70% requires a
new native signal condition; changing quotas or one-minute confirmation
thresholds cannot do it.

The earlier V20 missed-stock audit labelled 53 cases as having signals only
in disabled G3 slots. The precise split is **29** with a strict signal in an
actual disabled donor setup and **24** with strict signals at clock/side pairs
that have no configured donor setup. The earlier label was too broad.

On V18's published 62 dates (247 mover cases), its frozen control executed
42 top-two cases. The `simplified` branch executed 54, while total backtest
net P&L fell from Rs 9,83,769.00 to Rs 5,09,596.82 and daily-close drawdown
rose from Rs 25,644.57 to Rs 49,580.26. V18's extra coverage does not
support replacing the baseline.

## V22 causal alert trial

The exploratory signal watches every complete one-minute stock history. It
records the **first** minute high/low crossing 1% from the 09:16 open by
13:25. Within the next five completed minutes, it requires a directional
candle close beyond the crossing-minute close. The alert becomes available
only when that confirmation minute completes. The stock's full-day top-two
rank and final high/low are joined **after** generating all stock alerts.

| Selection stage | Total all-stock alerts | Top-two cases before final extreme |
|:---|---:|---:|
| Exact next-minute continuation | 4,920 | 164 / 255 (64.3%) |
| Continuation within five minutes | 7,906 | 208 / 255 (81.6%) |

The five-minute rule generated about 124 alerts per session; only 2.63% of
alerts corresponded to a daily top-two case. An alert is not an order fill.

For a portfolio trial, the five-minute alerts were added beside V19, except
when V19 already had a same-stock, same-direction order. This created 7,496
new orders. The trial used the inherited G3 staged 1.25% stop, ten-minute
entry-trigger expiry, 5 bps costs and fixed portfolio model. Its **1.5%
target** is an explicit new research assumption, since an arbitrary one-minute
alert has no original G3 setup target.

| 64-session portfolio result | V19 baseline | V22 alert trial |
|:---|---:|---:|
| Orders | 631 | 8,127 |
| Executed trades | 515 | 1,259 |
| Top-two cases with an order | 50 / 255 | 215 / 255 |
| Top-two cases executed | 44 / 255 | 69 / 255 |
| Top-two cases entered before final extreme | 43 / 255 | 68 / 255 |
| Win rate | 71.46% | 49.17% |
| Net P&L (Rs) | 10,04,970.75 | 40,648.98 |
| Daily-close drawdown (Rs) | 28,166.32 | 2,43,998.42 |

Only 95 of V19's original 515 executed positions still executed in the
combined trial; the alert orders consumed portfolio capital. The trial
missed the 179-trade target by **110 cases** and reduced net P&L by
Rs 9,64,321.77. It should not be promoted as a G3 entry rule.

## Files and limits

- [Causal scanner](scanner.py), [alert summary](results_full_20261104/summary.json), [all five-minute alerts](results_full_20261104/alerts_within_five_minutes.csv), and [top-two alert audit](results_full_20261104/top2_cases_within_five_minutes.csv).
- [Portfolio trial code](replay_alerts.py), [portfolio summary](portfolio_trial_20261104/summary.json), [daily results](portfolio_trial_20261104/daily_results.csv), and [top-two executed audit](portfolio_trial_20261104/top2_portfolio_comparison.csv).

The 09:15 opening minute is absent from the sealed source. Early historical
cohorts are not strictly point-in-time universes, and 28 October has a
restricted cohort. The frozen G3 slot allowlist used outcomes through 27
October, while this alert threshold was inspected against these 255 cases.
This is a retrospective feasibility test, not an independent validation.
No existing G3 configuration, live order path, or current-result pointer was
changed.
