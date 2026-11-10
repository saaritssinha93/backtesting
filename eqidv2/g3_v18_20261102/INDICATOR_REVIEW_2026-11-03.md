# Review of indicator necessity, combinations and news data

Reviewed 3 November 2026 against `FULL_SLOT_CONFIGURATION_2026-11-03.md`, the G3/G7 source contracts, and completed research results. This is a research recommendation. No new backtest was run for this review, and no entry or exit configuration was changed.

The current evidence supports **testing a simpler, grouped decision process**, while keeping the present control for comparison. It does not establish that an individual existing indicator can safely be deleted, or that another indicator will increase profit.

## 1. What the completed tests actually show

G3 V18's October comparison, 19 represented sessions through 30 October 2026, at 5 bps modeled round-trip cost:

| Variant | What changed | Trades | Win rate | Net profit | Daily realized drawdown |
|---|---|---:|---:|---:|---:|
| V17 control | Original selection | 112 | 66.96% | ₹165,581 | ₹25,645 |
| Simplified | Removed EMA stacking and body/wick thresholds together | 226 | 53.98% | ₹67,304 | ₹42,232 |
| Advanced | Added breadth, relative strength and VWAP/pressure groups together | 70 | 72.86% | ₹127,277 | ₹26,454 |
| VWAP + pressure | Added VWAP distance/slope and candle-volume pressure | 99 | 69.70% | ₹147,116 | ₹20,500 |

Removing several conditions together admitted more losing trades. Adding several conditions together removed profitable trades as well as losers. VWAP/pressure produced a historical risk/profit tradeoff, with approximately ₹5,145 lower drawdown and ₹18,465 less profit. None passed all of the frozen October comparison requirements. Win rate alone would have selected a different policy from profit preservation.

The simplified arm cannot tell us whether EMA, body, maximum wick or minimum lower wick was individually useful: all were changed together.

The earlier G7 OR arm, `or_oi10_vol12`, tested LONG OI ≥0.10% **OR** volume ratio ≥1.20 on an already selected, OI-filtered opportunity list. It executed exactly the same portfolio as control: 338 trades and ₹811,977.64 net over 59 represented sessions. This did not test the stocks removed by the original OI gates, so it supplies no evidence that OR improves full selection. Earlier full-pool G7 V5 research also retained V4: the apparent best improvement was concentrated in two trades and did not improve October or losing-day count.

**Evidence limits:** G3's 34-slot allowlist was selected using outcomes through 27 October, so these October and earlier results are not independent holdout. There are only three represented post-selection sessions in the completed V18 result. October 28 uses a declared restricted 215/218-stock cohort. Drawdown above is computed from daily realized P&L within October; it is not intraday mark-to-market drawdown. Costs and execution assumptions remain modeled.

Local evidence:

- [V18 saved metrics](results_20261102_complete/summary.json)
- [V18 experiment definitions](README.md)
- [G7 OR experiment](../outputs/g7_v6_20261102/selected_opportunity_iteration_or/summary.json)
- [G7 V5 research conclusion](../outputs/g7_v5_20261102/report_summary.json)

## 2. Are so many indicators necessary?

The 61 G3 rows are slot/direction configurations, not 61 different indicators. The economic questions are fewer:

| Question | Existing measurements | Assessment |
|---|---|---|
| Is the price moving in the intended direction? | EMA9/20/50 stack, five-minute move, one-minute directional close | Related price measurements at different horizons. Test incremental value; do not count them as three independent votes. |
| Is there participation? | Five-minute volume, one-minute volume, futures OI | Different but related activity measures. OI is not a directional buyer/seller count. |
| Is the entry candle acceptable? | Body ratio, adverse wick maximum, 09:26 SHORT lower-wick minimum | Derived from the same candle. Body plus the two wick ratios sum to one for a valid nonzero-range candle. These gates interact. |
| Is the market context acceptable? | Opening Nifty gate, G7 slot/session opposition veto | Different horizons, but partially overlapping market information. |
| Is the move stretched or reversing? | EMA/ATR extension; G7 VWAP extreme, efficiency and RSI weakening | Potentially useful as a combined exhaustion condition. More overlap with price features; test as a group and then its components. |
| Can the order be executed and its risk controlled? | Data validity, liquidity, timing, capital, stops, square-off | Required operational constraints. These should not be traded off against a good indicator score. |

A feature is useful if it improves later, unseen portfolio results after costs, or produces an explicitly acceptable reduction in risk. Its familiarity, correlation with profit on reused history, or ability to reject a known loser is insufficient.

## 3. What should be kept, removed or changed?

| Component | Recommendation for the next controlled research |
|---|---|
| Valid candles, exact timestamps, verified OI source, no future data | Keep mandatory. Missing OI and valid but falling OI are different situations. |
| Liquidity, capital/exposure, order expiry, stop and square-off controls | Keep mandatory. Add measured spread/slippage diagnostics if historical data exists. |
| Directional price trigger and exact one-minute confirmation | Keep in the first experiments so changes remain attributable. |
| EMA stacking | Test removal alone or one replacement trend measure. Do not add MACD to count the same trend twice. |
| Body and wick rules | Test one rule at a time. Include the special 09:26 SHORT minimum lower wick separately. |
| Five-minute and one-minute volume | Compare each contribution separately; investigate same-clock historical RVOL before adding another volume indicator. |
| OI minimum and positive-increase requirement | Test supporting-score and alternate-path versions. A strong Nifty move does not establish that OI is unnecessary. |
| OI upper limit of 1% | Audit rejected candidates and rollover/contract continuity first; distinguish a sound economic cap from an invalid data value. |
| Preferred core ranking | Test its contribution separately. Five G3 SHORT setups have stricter preferred core moves than their minimum eligibility thresholds. |
| Slot allowlist and exact slot-specific constants | Revalidate with pooled rules and later observations before increasing the number of free parameters. |

The weakest evidence is the precision of slot selection. The saved G3 policy permits a slot with one executed historical trade. **21 of 34 enabled pairs had ten or fewer trades** in the frozen selection ledger; examples include 09:36 SHORT at 1/1 and 10:36 LONG at 2/2. These do not support precise win-rate estimates or extensive separate weight fitting for every slot.

Repeatedly selecting the best parameters on the same history can select noise. Record every trial and compare performance on later data that did not choose the slot policy, feature thresholds or model weights. This issue is discussed in [Bailey et al., The Probability of Backtest Overfitting](https://carmamaths.org/jon/backtest2.pdf).

## 4. Which additional features deserve priority?

These are candidate hypotheses, not proven upgrades. Add or substitute one group at a time.

| Priority | Candidate | Definition and purpose | Important limitation |
|---|---|---|---|
| 1 | Same-clock relative volume | Current completed five-minute volume divided by the median volume for that same clock across earlier eligible sessions | Prevents the normal opening/midday volume pattern from acting like a surprise. Historical sessions only; handle insufficient history explicitly. |
| 2 | Entry extension plus remaining room | Signed distance from VWAP or EMA divided by completed stock ATR; distance to already known resistance/support divided by planned stop distance | Directly tests entries near extremes. Use only prior-session levels or session levels formed before entry, never the eventual day's high/low. |
| 3 | Breadth and stock-relative strength | Directional share of eligible stocks advancing; signed stock return minus market/sector return | V18 already tested simple forms without establishing an upgrade. Try replacement or bounded ranking before stacking another hard gate. |
| 4 | Relevant sector context | Completed sector return/breadth using membership known on that date | A stock's primary size index is not its sector. Preserve the existing Nifty rule as control; do not assume the sector alternative is better. |
| 5 | Stock volatility and trend efficiency | Completed stock ATR/price and net move divided by total absolute moves over the same window | Distinguishes size of movement from persistence. Short opening histories need a separately declared policy. |
| 6 | Event-risk information | Earnings schedule and timestamped material announcements | Start with event flags and diagnostics before trying directional news sentiment. |

India VIX can provide market volatility context, but should not become a universal LONG/SHORT switch or directly set every stock's stop. NSE defines it as option-implied expected market volatility over the next 30 calendar days. For a one-minute stock entry, completed stock ATR and actual price behavior are more directly matched to the trading horizon; whether VIX adds value beyond them is a testable question. See [NSE's India VIX definition](https://www.nseindia.com/static/products-services/indices-indiavix-index).

Do not add RSI, MACD, stochastic, ADX and multiple moving-average votes together as the first change. Start by checking whether the existing information is used well. Candle-volume pressure and candle-signed volume are proxies; label them accordingly rather than describing them as measured buyer/seller order flow.

## 5. AND, OR, weighted ranking, or all three?

Use them at different stages. The architecture below is a proposed test, not an implemented policy:

```text
required = valid_asof_data
           AND executable_liquidity
           AND portfolio_risk_allowed
           AND directional_price_confirmation
           AND existing_market_veto_passes

build_up_path = OI_confirmation AND adequate_volume

market_led_path = strong_directional_market
                  AND broad_participation
                  AND stock_relative_strength
                  AND adequate_volume

eligible = required
           AND acceptable_entry_extension
           AND (build_up_path OR market_led_path)

rank eligible candidates using a small grouped score;
apply the existing quota and replay allocation and exits.
```

**AND:** use for required data, execution and risk conditions, and within a coherent entry path. Strong volume cannot compensate for a stale candle or a rejected risk condition.

**OR:** use between complete, economically meaningful paths. Rising OI can confirm a build-up trade; a market-led move could be admitted by a separately validated path even when valid OI is flat or falling. Price rising with falling OI can be consistent with short covering, and price falling with falling OI with long liquidation; these are possible interpretations rather than identification of who is trading. Open interest counts outstanding contracts, distinct from traded volume. See [CME's explanation](https://www.cmegroup.com/education/lessons/open-interest).

Avoid a broad rule such as `EMA OR volume OR OI OR positive news`: one weak condition would admit an otherwise poor candidate. Also avoid changing the existing G7 opposition veto accidentally: it rejects when **both** market directions oppose; replacing that AND with OR would reject whenever **either** opposes and would create a substantially different filter.

**Weighted ranking:** use to rank candidates that clear the required conditions and one valid path. Group the inputs into market context, stock strength, participation and entry quality. Cap the influence of each group so several correlated price indicators cannot overwhelm the rest. Start with equal group weights as a comparison arm; estimate alternative bounded weights only from earlier training windows. OI belongs within participation, not a second full vote duplicating that group's score. No particular weights or admission cutoff are established by the current data, and a score of 80 is not an 80% probability of winning.

The recommended research direction is therefore **mandatory AND guards, narrowly defined OR entry paths, and a small grouped score for ranking**. Whether it improves the existing control must be measured.

## 6. Should F&O stock news be added?

**Collect it as event context; do not make generic news sentiment mandatory for every entry.** Start with scheduled results known before the session and material announcements such as guidance, regulatory action, rating changes, major financing, acquisitions and significant corporate actions. Do not assume all positive headlines imply a profitable long, or all negative headlines a profitable short: expectations, timing and the price response matter.

For historical replay, store symbol/ISIN, source and event ID, event category, original text/hash, first-publication time, exchange dissemination time where available, our first-received time, revision time and classifier version. The feature becomes eligible only after the information could have reached and been processed by the strategy. An announcement timestamp is not proof that our live system received it at that moment. Unknown news coverage must be distinguishable from verified absence of news.

NSE's [corporate announcements page](https://www.nseindia.com/companies-listing/corporate-filings-announcements) exposes broadcast information. Its [corporate data documentation](https://www.nseindia.com/static/market-data/corporate-data-subscription) describes separate online and end-of-day delivery; the EOD product is available after 20:00 IST. An EOD archive's availability cannot be treated as evidence of intraday receipt.

First compare: unchanged strategy with event flags only; a predeclared pause after a newly received material event; and an event-aware ranking/size policy. Any pause length is a research parameter, not a known optimum. Test earnings and unscheduled events separately. A blanket all-day exclusion could remove profitable news trends. A news classifier should preserve the exact source text and its timestamp, abstain when uncertain, and never infer historical sentiment from the eventual price outcome. Purchasing a feed is premature until usable timestamp history and incremental value have been established.

## 7. A controlled testing sequence

1. Freeze the current control, universe, execution assumptions and all trial definitions. Keep targets/stops unchanged during selection experiments.
2. Replay the full candidate pool before EMA/OI/body/wick/volume selection. A selected-trade file cannot measure new admissions. Record the first failed gate and all independent gate results for each candidate.
3. Run individual changes: remove EMA; remove body; remove maximum wick; remove 09:26 minimum lower wick; soften OI; compare five-minute versus one-minute volume; disable the preferred core tier. Do not tune thresholds inside each arm initially.
4. For each arm, record losers avoided, winners removed, new winners and losers admitted, replacements, cost, portfolio net P&L, losing days, worst day and drawdown. Recompute ranking, allocation and G7 loss locks rather than summing retained historical trades.
5. Test one narrowly defined OR path, one grouped score, and their combination against the same control. Introduce same-clock RVOL, extension/room and event information separately afterward so attribution remains clear.
6. Pool related time windows and directions where evidence is sparse, with any finer slot deviations justified on earlier data. Evaluate later contiguous sessions with weights and thresholds frozen. Split by whole trading day; fit normalizations only on earlier data. If labels overlap a boundary, purge them; dependent observations cannot be randomly scattered across train and test.
7. Report paired daily changes and uncertainty using whole-day blocks, including cost stresses at 5, 9 and 15 bps plus fill/slippage diagnostics. Count all attempted arms. Treat the reused 62-session history as development evidence, and collect additional unseen sessions for confirmation.

Promote a change only when its later evidence supports the chosen objective: improved net expectancy and profit with acceptable downside, or an explicitly accepted reduction in profit in return for lower risk. A higher historical win rate alone does not satisfy that objective.

## Corrections made to the configuration reference during this review

- Added the five stricter G3 preferred-core move thresholds to the slot rows and explained the preference.
- Clarified that G7's session direction uses the latest completed five-minute close versus the 09:20 bar's open; it does not use the confirmation minute's latest quote. Missing required Nifty direction data fails the run.

These are documentation corrections; the underlying strategy behavior was not changed.
