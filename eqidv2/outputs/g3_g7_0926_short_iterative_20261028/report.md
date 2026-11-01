# 09:26 SHORT - G-3 and G7 V4 two-loss-lock audit

## Honest verdict

One replacement-aware entry rule reaches the requested **backtest point estimate above 75% in both strategies at the normal 5 bps cost**:

> For `0926_SHORT`, require the SHORT confirmation candle's **directional lower-wick ratio to be at least 0.20**, while retaining the existing maximum of 0.60. Apply this rule before max-volume ranking and the two-entry quota.

The result is numerically strong but **does not establish that the true win rate is above 75%**. It leaves only 11 executions per strategy, the exact confidence intervals remain wide, the threshold had already been seen in an earlier exploratory diagnostic, and only one post-development execution survives in each strategy. G-3 loses that one trade. At 15 bps, G7 falls to 7/11 wins.

The rule is therefore saved as **`RESEARCH_SPECIFICATION_NOT_IMPLEMENTED_OR_ADOPTED`**. No active G-3 or G7 V4 configuration was changed. The live schema does not yet have this minimum-wick gate, so even shadow execution requires implementation and parity testing first.

The source ledgers end on **27 October 2026**. There is no 28 October session in these sealed inputs.

## Main result at 5 bps

| Strategy | Rule | Selected | Executed | Wins-losses | Win rate | 09:26 net P&L | Change vs baseline |
|---|---|---:|---:|---:|---:|---:|---:|
| G-3 | Current | 22 | 18 | 10-8 | 55.6% | Rs 19,535 | - |
| G-3 | Lower wick >= 0.20 | 14 | 11 | 9-2 | **81.8%** | **Rs 31,930** | **+Rs 12,395** |
| G7 V4 lock2 | Current | 22 | 18 | 13-5 | 72.2% | Rs 19,941 | - |
| G7 V4 lock2 | Lower wick >= 0.20 | 14 | 11 | 10-1 | **90.9%** | **Rs 31,068** | **+Rs 11,127** |

The candidate leaves **11 executions, 61.1% as many as the 18 baseline executions**. That is a count ratio, not identity retention, because pre-quota ranking admits replacements. G-3 shares 9/18 baseline execution identities and 7/10 baseline-winning identities; its other two wins are replacement symbols FORCEMOT and WIPRO. G7 shares 10/18 baseline execution identities and 9/13 baseline-winning identities; WIPRO is its additional replacement win.

## Full-portfolio effect at 5 bps

The complete portfolio was replayed, rather than adding isolated 09:26 trade P&L to a fixed total.

| Strategy | Rule | Portfolio trades | Wins-losses | Portfolio win rate | Portfolio net P&L | Change vs baseline |
|---|---|---:|---:|---:|---:|---:|
| G-3 | Current | 553 | 395-158 | 71.43% | Rs 1,212,277 | - |
| G-3 | Lower wick >= 0.20 | 546 | 394-152 | 72.16% | **Rs 1,224,672** | **+Rs 12,395** |
| G7 V4 lock2 | Current | 400 | 319-81 | 79.75% | Rs 842,867 | - |
| G7 V4 lock2 | Lower wick >= 0.20 | 397 | 320-77 | 80.60% | **Rs 859,639** | **+Rs 16,772** |

The G7 portfolio delta differs from the slot delta because changed 09:26 selections alter capital availability and two-loss-lock chronology elsewhere in the day.

## Why this rule can reduce extreme SHORT entries

For a SHORT setup, `wick_ratio` is the confirmation candle's lower wick divided by its full range. The existing configuration only rejects a very large lower wick above 0.60. The research rule accepts a band of **0.20 to 0.60**.

Requiring at least a 0.20 lower wick means the confirmation candle did not close directly at its extreme low. The existing entry trigger still requires a later break below that candle's low. The sequence is therefore:

1. Downward confirmation occurs.
2. Price shows some rejection from the low instead of closing at the extreme.
3. A fresh break of the low is required before entry.

This is a simple anti-chase condition. It uses only information available by the original 09:26 decision time and does not use future outcome data during selection.

## Chronology check

The aggregate improvement is dominated by the previously inspected development period.

| Strategy and rule | Development through 18 Sep | Post-development from 21 Sep | October only |
|---|---:|---:|---:|
| Current G-3 | 9/12, 75.0%, +Rs 39,376 | 1/6, 16.7%, -Rs 19,841 | 1/3, 33.3%, -Rs 5,698 |
| Wick >= 0.20, G-3 | 9/10, 90.0%, +Rs 37,180 | **0/1, -Rs 5,250** | **0/1, -Rs 5,250** |
| Current G7 V4 lock2 | 9/12, 75.0%, +Rs 23,030 | 4/6, 66.7%, -Rs 3,089 | 2/3, 66.7%, +Rs 54 |
| Wick >= 0.20, G7 | 9/10, 90.0%, +Rs 30,819 | **1/1, +Rs 249** | **1/1, +Rs 249** |

This later sample is too small to validate the rule. It shows the central honesty problem: the full-history rate rises because the filter removes most later trades, not because it demonstrates repeatable later performance.

## Candidate daywise executions at 5 bps

Both strategies select the same 14 candidate opportunities under this rule. Eleven fill across eight dates; three remain unfilled.

| Date | Executed symbols | G-3 day P&L | G7 V4 lock2 day P&L |
|---|---|---:|---:|
| 12 Aug | AMBER | +Rs 2,282 | +Rs 249 |
| 14 Aug | IREDA | +Rs 650 | +Rs 650 |
| 19 Aug | SOLARINDS, FORCEMOT | +Rs 7,607 | +Rs 10,072 |
| 21 Aug | PNB | **-Rs 4,141** | **-Rs 4,141** |
| 7 Sep | MPHASIS, WIPRO | +Rs 3,794 | +Rs 1,870 |
| 11 Sep | GODREJPROP, DELHIVERY | +Rs 15,737 | +Rs 12,369 |
| 15 Sep | IDEA | +Rs 11,252 | +Rs 9,750 |
| 27 Oct | HINDUNILVR | **-Rs 5,250** | +Rs 249 |

ADANIENT on 31 August, ASHOKLEY on 1 September, and INDIANB on 24 September were selected but did not fill. The complete 59-session comparison, including zero-trade days and the symbols displaced by the rule, is in `candidate_daywise_comparison.csv`.

## Cost stress

Costs are total round-trip costs. G-3 allocation and the complete G7 integer-share portfolio with the two-net-loss lock were rerun at each cost.

### 09:26 SHORT only

| Strategy | Rule | 5 bps | 9 bps | 15 bps |
|---|---|---:|---:|---:|
| G-3 | Current | 10/18, 55.6%, Rs 19,535 | 10/18, 55.6%, Rs 15,935 | 10/18, 55.6%, Rs 10,535 |
| G-3 | Wick >= 0.20 | **9/11, 81.8%, Rs 31,930** | **9/11, 81.8%, Rs 29,730** | **9/11, 81.8%, Rs 26,430** |
| G7 V4 lock2 | Current | 13/18, 72.2%, Rs 19,941 | 13/18, 72.2%, Rs 16,355 | 8/18, 44.4%, Rs 10,976 |
| G7 V4 lock2 | Wick >= 0.20 | **10/11, 90.9%, Rs 31,068** | **10/11, 90.9%, Rs 28,877** | **7/11, 63.6%, Rs 25,590** |

Native path fills and the 11 candidate slot executions do not change at higher costs. At 15 bps, several G7 gross +0.10% protected exits become net -0.05% losses. This lowers the slot win rate and causes earlier two-loss locks in the full portfolio.

### Full portfolio

| Strategy and rule | 9 bps portfolio | Delta vs same-cost baseline | 15 bps portfolio | Delta vs same-cost baseline |
|---|---:|---:|---:|---:|
| G-3 current | 553 trades, Rs 1,101,677 | - | 553 trades, Rs 935,777 | - |
| G-3 wick >= 0.20 | 546 trades, Rs 1,115,472 | **+Rs 13,795** | 546 trades, Rs 951,672 | **+Rs 15,895** |
| G7 current | 400 trades, Rs 763,108 | - | 358 trades, Rs 576,219 | - |
| G7 wick >= 0.20 | 397 trades, Rs 780,478 | **+Rs 17,370** | 362 trades, Rs 618,956 | **+Rs 42,737** |

At 15 bps, the newly recalculated G7 loss lock removes 42 baseline and 35 candidate full-portfolio executions. The positive P&L delta remains descriptive because the portfolio itself changes materially under that stress.

## Replacement-aware grid

The rule was applied to the complete candidate pool before ranking and quota. All fixed grid rows are reported below; no failed row was removed.

| Pre-quota rule | G-3 result | G7 V4 lock2 result | Reading |
|---|---:|---:|---|
| Current | 10/18, 55.6%, Rs 19,535 | 13/18, 72.2%, Rs 19,941 | Baseline |
| Extension <= 3.5 ATR | 10/16, 62.5%, Rs 19,776 | 11/16, 68.8%, Rs 9,959 | Fail |
| Extension <= 3.0 ATR | 8/13, 61.5%, Rs 13,449 | 9/13, 69.2%, Rs 16,483 | Fail |
| Extension <= 2.5 ATR | 5/9, 55.6%, Rs 3,254 | 5/9, 55.6%, -Rs 415 | Fail |
| Down move <= 0.75% | 9/16, 56.3%, Rs 8,270 | 9/15, 60.0%, Rs 7,108 | Fail |
| Down move <= 0.50% | 10/13, 76.9%, Rs 26,614 | 9/12, **75.0%**, Rs 21,606 | G7 does not exceed 75% |
| Body ratio <= 0.85 | 9/14, 64.3%, Rs 22,227 | 11/14, 78.6%, Rs 23,024 | G-3 fails |
| **Wick ratio >= 0.20** | **9/11, 81.8%, Rs 31,930** | **10/11, 90.9%, Rs 31,068** | Numerical target; research specification |
| Extension <= 3.0 and down move <= 0.75% | 8/13, 61.5%, Rs 13,449 | 9/13, 69.2%, Rs 16,483 | Fail |
| Extension <= 3.0 and wick >= 0.20 | 7/8, 87.5%, Rs 22,603 | 8/8, 100%, Rs 25,114 | Only eight trades; reject |

The combination row has an even higher point estimate, but it leaves only eight executions, **44.4% as many as baseline**, and has the same single post-development execution. It is a weaker candidate than the simple wick rule.

## Statistical honesty

These exact calculations treat trades as independent. That assumption is optimistic because executions cluster on a small number of dates and the same history has been inspected repeatedly.

| Result | Point estimate | Exact 95% interval | One-sided p-value against 75% |
|---|---:|---:|---:|
| Current G-3, 10/18 | 55.6% | 30.8%-78.5% | 0.981 |
| Current G7, 13/18 | 72.2% | 46.5%-90.3% | 0.717 |
| Wick candidate G-3, 9/11 | 81.8% | **48.2%-97.7%** | **0.455** |
| Wick candidate G7, 10/11 | 90.9% | **58.7%-99.8%** | **0.197** |

Neither candidate interval has a lower bound above 75%, and neither one-sided test rejects a 75% null. The backtest therefore supports the statement **"the observed rate is above 75%"**, not **"the strategy's true rate is above 75%"**.

The replacement-aware grid was frozen before its own outcome replay. However, `wick >= 0.20` had already appeared in an earlier selected-ledger exploratory diagnostic. It is therefore not an untouched hypothesis or an independent validation result.

## Earlier iterations and why they were rejected

The earlier fixed exit ladder found two rules with point estimates above 75% in both books. Protecting +0.10% or +0.15% after only +0.20% gross profit was rejected because it manufactured small wins while damaging expectancy:

| Exit rule | G-3 | G7 V4 lock2 |
|---|---:|---:|
| Current | 10/18, 55.6%, +Rs 19,535 | 13/18, 72.2%, +Rs 19,941 |
| Protect +0.10 after +0.20 | 14/18, 77.8%, **-Rs 17,447** | 15/18, 83.3%, **-Rs 933** |
| Protect +0.15 after +0.20 | 14/18, 77.8%, **-Rs 14,504** | 15/18, 83.3%, +Rs 2,002 |

Fixed post-selection vetoes also failed to make both strategies exceed 75%. Those tests did not allow quota replacements, which is why the final study rebuilt the candidate pool and reran selection before drawing a conclusion.

## Baseline integrity and provenance

The replay reconstructed 213 current setup-assigned candidates across 25 candidate days in a 59-session calendar. Four exact parity gates pass:

- G-3 current full allocator: 634 rows.
- G7 V4 two-loss-lock current portfolio: 603 rows.
- Reconstructed G-3 baseline selection, paths, and portfolio: 634 rows.
- Reconstructed G7 extension <= 4 ATR, V4 protection, two-loss-lock baseline: 603 rows.

The initial audit exposed one simulator error: G7 was temporarily using G-3's 1.25% stop. Correcting it to G7's declared 1.20% stop and 2.00% target changed two stop-outs and restored exact parity. Only the corrected results are published here.

Pinned source hashes:

- G-3 source manifest: `90272456c0e6e8b75ac7fae8bac24be7e3dd83f635680814e49219b4e38759a6`
- G7 research manifest: `daa7440e6c2e0a8d60a6bc89841da802d59639aa1c2f43f78a371622ca43f56b`
- Minute-path input-cache manifest: `31fb98ab092aa8c3ea5e59f7e65e377d41095306ba71cc93ae964aeedc998c51`

Key machine-readable artifacts:

- `prequota_reselection/candidate_decision.json`: final disposition and exact statistics.
- `prequota_reselection/research_candidate_config.json`: research-only overlay; it has no execution authority.
- `prequota_reselection/candidate_daywise_comparison.csv`: all 59 sessions for both strategies.
- `prequota_reselection/variant_setup_metrics.csv`: every replacement-aware grid result.
- `prequota_reselection/variant_portfolio_metrics.csv`: full-portfolio results.
- `prequota_reselection/cost_stress_metrics.csv`: 5/9/15 bps slot and portfolio stress.
- `prequota_reselection/variant_0926_trade_details.csv`: selected opportunity and exit details.
- `prequota_reselection/candidate_variant_decisions.csv`: every candidate's gate/rank decision.
- `prequota_reselection/baseline_reproduction.json`: exact parity gates.
- `prequota_reselection/source_provenance.json`: source hashes and calendar.

## Disposition

- Numerical backtest target above 75% at 5 bps: **reached**.
- Evidence that the true repeatable rate exceeds 75%: **not reached**.
- Research specification for a future frozen shadow test, after implementation and parity validation: **minimum directional lower wick 0.20**.
- Active G-3 config changed: **no**.
- Active G7 V4 config changed: **no**.
- Exit logic changed: **no**.

Promotion should require untouched forward executions under the exact frozen rule. The lower confidence bound, net P&L, loss amount, trade retention, and cost stress must be judged together; a point win rate alone is not sufficient.
