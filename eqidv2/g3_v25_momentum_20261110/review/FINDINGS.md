# V25 findings: what the full-universe test actually showed

**No new strategy passed validation. Keep the existing G3 baseline as the research reference.** All eight new family/target variants lost money over the full 68-session history. This study created and tested a separate V25; it did not improve or deploy the live strategy.

Coverage: July 29–November 9, 2026; **14,353 stock-days**, **652 daily top-five cases** and **271 top-two cases** reaching at least 3% in the relevant direction. These are all complete available G3 source histories, not every NSE stock or every exchange session.

| Strategy | Fills | Win rate | Net PnL Rs | Mean/fill Rs | Daily-close drawdown Rs | Top-five cases traded | Useful top-five captures |
|:--|--:|--:|--:|--:|--:|--:|--:|
| G3 baseline | 824 | 65.66% | 1,221,079.86 | 1,481.89 | 31,338.39 | 124/652 | 94/652 |
| Broad breakout, target 1.5% | 816 | 47.43% | -22,619.87 | -27.72 | 207,710.73 | 202/652 | 151/652 |
| Leader pullback, target 1.5% | 814 | 47.67% | -92,644.36 | -113.81 | 228,858.20 | 89/652 | 61/652 |

Useful capture requires an actual fill before the final extreme, at least 0.8% price room remaining and positive realized net PnL. The main comparison uses conservative entry-minute end labels for every arm. More precise known next-open times are supplied separately for the new scanner; G3 intrabar trigger timing is not known.

## Why more mover entries did not produce more profit

The broad breakout/1.5% strategy traded **202** top-five cases versus G3’s **124**, and increased useful captures from **94 to 151**. However, its trades outside the top-five lists lost **Rs 793,220.56**. The resulting total was **Rs -22,619.87**. Capturing the biggest movers and selecting profitable trades are different objectives.

G3 earned **Rs 686,034.94 net outside the top-five lists**, or **56.18% of its total net profit**. Those 676 trades include 424 winners. Excluding every non-top-five stock in hindsight would discard substantial genuine baseline profit; the final daily top-five list is not known at entry time.

The extension-capped leader rules did not solve entry timing in this test. Using the common retrospective near-extreme definition, the baseline had **195/824 (23.67%)** near-extreme fills; broad breakout had **195/816 (23.90%)**; leader pullback had **240/814 (29.48%)**. A fixed EMA-distance cap did not identify the end of a move reliably here. This does not prove that every such rule is ineffective.

## Why G3 missed the daily movers

| First failed stage for a top-five stock/day/direction | Cases |
|:--|--:|
| No native G3 signal in a configured slot/direction | 249 |
| Native candidate existed but G3 filters rejected it | 222 |
| Rejected by G7 confirmation | 34 |
| Quota or added-pair volume gate | 15 |
| Admitted order did not fill | 8 |
| Had a filled G3 trade | 124 |

These are whole-day stage diagnoses, not proof that every passing candidate occurred before the useful part of the move. The daily catalogue supplies entry, first-3% and extreme times. Native signal and confirmation rules embody much of the baseline’s selection edge; waiving them broadly increased false positives.

The displayed exploratory pullback missed **444** top-five cases because its conditions never aligned at one scan, **118** because of ranking/daily admission caps, and **one** because the next-open gap was too large; **89** filled. Individual gates passing at different times is insufficient. Do not assign those 444 cases to one indicator.

The new scanner starts at 09:35 to establish same-session features. **49/652** top-five cases had already reached their recorded daily extreme by that time, and **203/652** had already crossed 3%. It cannot capture those 49 before their extreme. For top-two cases, the corresponding counts are 16/271 and 103/271.

Each new family was explicitly capped at six admitted orders per direction/day, so its opportunity coverage also depends on that design choice. The broad control exhausted its daily allowances by 10:30 on every date (median last admission 10:00). Its results should not be generalized to uncapped or later-entry momentum strategies.

## Validation, exits and slippage

The first 44 dates form the development slice, the next 15 select among the eight predefined variants, and the final nine check the frozen choice. All these dates had appeared in earlier research; this is chronological historical testing, not untouched out-of-sample evidence.

The best validation net belonged to leader pullback with a 1.5% target, at **Rs 17,807.23** across 179 fills, compared with **Rs 224,593.11** across 191 G3 fills. It failed the required mean-profit, profit-factor and drawdown comparisons. No variant qualified; “best exploratory” is not an adoption recommendation.

That pullback variant earned Rs 104,701.68 in the latest nine sessions, near the baseline’s Rs 107,339.58, but its drawdown was Rs 43,809.08 versus Rs 26,246.72. Its full-history result was negative. Selecting it based only on the recent positive slice would misrepresent the evidence.

| Full-history strategy | Standard modeled net Rs | With 2 bps adverse slippage each side, net Rs |
|:--|--:|--:|
| G3 baseline | 1,221,079.86 | 924,480.86 |
| Broad breakout, target 1.5% | -22,619.87 | -151,725.61 |
| Leader pullback, target 1.5% | -92,644.36 | -256,702.98 |

Matched-exit G3 controls remained profitable: **Rs 874,462.17** with a uniform 0.8% target and **Rs 1,100,512.43** with a 1.5% target. Target changes alone therefore do not explain the new scanners’ losses. Entry execution still differs: G3 uses its trigger fills, while the new scanner assumes the next minute open with a 0.15% adverse-gap cancellation.

## Decision

Preserve G3’s current baseline. Keep V25 as a reproducible rejected experiment and mover audit. The evidence supports neither relaxing all G3 gates to chase daily movers nor excluding all smaller movers. A future entry change would need to demonstrate incremental profit and improved loss/drawdown behavior using information available at entry; this study did not establish such a change.

This conclusion is deliberately bounded to the eight tested rules. Repeated selection of rules on the same history can overfit; the rationale for limited, chronological testing is discussed in [Bailey et al., The Probability of Backtest Overfitting](https://www.davidhbailey.com/dhbpapers/backtest-prob.pdf). No future trading performance is inferred from these backtests.

## Artifacts and verification

- [Every daily top-five long/short mover and entry comparison](../results_through_20261109/FULL_HISTORY_DAILY_TOP5_MOVERS.md)
- [Day-wise strategy results](../results_through_20261109/DAY_WISE_RESULTS.md)
- [Full report: all variants, directions and periods](../results_through_20261109/REPORT.md)
- [Every missed-case reason and gate count](../results_through_20261109/missed_cases.csv)
- [Frozen research plan](../RESEARCH_PLAN.md)
- [Equity and drawdown chart](EQUITY_AND_DRAWDOWN.png)

**37 tests passed.** Independent audits verified all 100 result hashes, frozen code/baseline hashes, all 824 baseline trade identities/entries/exits/PnL, 80 arm/period metric rows, all daily totals, all mover rankings and 114,824 gate-audit rows. Existing versions and live/paper settings remain unchanged.

Source limitations: eight days lack minute volume, so the new strategy uses price only; the 09:15 minute is absent; older universes and complete-stock-day eligibility are retrospective; October 1 and the November 8 special schedule are excluded; November 10 was still in progress. Full details remain in the frozen configuration and provenance.
