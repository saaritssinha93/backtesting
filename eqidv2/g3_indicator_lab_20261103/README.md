# G3 indicator study — completed 3 November 2026

**Decision: keep the current G3 V17 control. No tested alternative establishes a robust overall improvement that also protects October performance.** Do not remove a gate simply because it overlaps another indicator, and do not add AI or news to execution based on this study.

This isolated study covers **63 represented sessions, 2026-07-29–2026-11-02**, ending with yesterday's completed session. It does not include partial 3 November data. There are **24 primary arms (22 rules including control, 2 models), 4 additional fill-stress scenarios, and 8 separately declared diagnostic feature subsets**. The diagnostics involved 72 additional prior-only model refits; they are not eight new portfolio strategies. No thresholds were retuned after the results. No paper/live configuration, schedule or current-result pointer was changed.

## Main results

All amounts below are native simulated net P&L at **5 bps assumed round-trip cost charged on entry exposure**, with ₹10 lakh fixed capital, ₹1 lakh margin per entry and 5× target exposure. This is a fixed allocation experiment, not a compounded return or measured live execution result.

| Design | Trades | Win % | Net ₹ | Δ vs control ₹ | Losing days | October net ₹ | October Δ ₹ |
| --- | --- | --- | --- | --- | --- | --- | --- |
| G3 V17 control | 392 | 76.79 | 984,038.33 | 0.00 | 7 | 165,581.34 | 0.00 |
| Complete OR path | 453 | 72.85 | 960,692.96 | -23,345.36 | 7 | 187,206.47 | 21,625.14 |
| Stricter OR neighbor | 425 | 74.82 | 991,854.05 | 7,815.72 | 8 | 178,889.81 | 13,308.47 |
| Grouped ranking | 389 | 73.01 | 864,378.85 | -119,659.48 | 10 | 110,087.92 | -55,493.42 |
| OR + grouped ranking | 452 | 70.35 | 900,798.04 | -83,240.29 | 11 | 133,371.42 | -32,209.92 |
| Logistic ranking* | 432 | 72.69 | 951,826.32 | -32,212.01 | 9 | 127,404.79 | -38,176.55 |
| Histogram boosting* | 431 | 72.39 | 935,791.79 | -48,246.54 | 10 | 116,118.32 | -49,463.01 |
| Restore 51 donor slots | 590 | 70.17 | 1,224,720.28 | 240,681.95 | 13 | 115,883.55 | -49,697.79 |

*The models use the original control on their first 20 training sessions. Their genuine chronological evaluation is the subsequent **43 sessions**, shown separately below. Full-period model totals must not be interpreted as 63 days of model predictions.*

The complete OR path improved October by **₹21,625**, reduced October losing days from **3 to 2**, and raised October win rate from **66.96% to 67.91%**. However, it lost **₹23,345** versus control overall; October's worst day also worsened from **−₹25,644.57 to −₹26,605.34**. This is a conditional research lead, not evidence that OI becomes generally unnecessary in a rising market.

The stricter OR neighbor gained **₹7,815.72** overall at 5 bps, but lost **₹8,684.28** versus control at 15 bps. Removing its best improvement day changes its 5-bps advantage to **−₹4,534.28**. Its paired 95% block-bootstrap interval crosses zero. It is too fragile to replace the control.

Restoring all 51 donor slots increased total net by **₹240,681.95**, with more trades, but reduced October profit by **₹49,697.79**, increased October losing days from **3 to 7**, and increased October daily drawdown from **₹25,644.57 to ₹41,976.75**. Full-period losing days rose **7→13**. That conflicts with the requested loss-reduction objective. The 51 slots exclude ten tiny-target rows removed by the earlier donor policy; this is not a test of all 61 original rows.

The separately saved **G7 V4 Lock** reference has net **₹812,854.07** across the same represented dates and **₹171,331.64** in October. It has different exits and portfolio restrictions, so it is a benchmark, not an indicator-only ablation. Its day-wise figures are included in the comparison CSV.

## Which indicators should remain?

The table shows the effect of removing each gate or priority before ranking and quotas. Replacement candidates and capital allocation were recalculated. A positive value means removal made more money on that period, not that removal is proven safe.

| Family | Decision | Removal Δ total ₹ | Removal Δ October ₹ | Changed positions |
| --- | --- | --- | --- | --- |
| EMA stack | KEEP | -297,581.16 | -132,157.78 | 313 |
| OI lower/increase | KEEP | -214,332.31 | -89,785.67 | 587 |
| OI upper cap | KEEP | -35,826.98 | -4,968.14 | 37 |
| 5m volume | KEEP | -129,642.32 | -7,127.21 | 337 |
| 1m volume | KEEP | -117,353.84 | 11,420.61 | 265 |
| Body minimum | KEEP | -127,203.69 | 8,335.34 | 136 |
| Maximum wick | KEEP | -16,271.51 | -6,597.59 | 10 |
| 09:26 SHORT minimum wick | KEEP | -16,495.50 | -4,549.40 | 12 |
| Core preference | INSUFFICIENT EVIDENCE | 0.00 | 0.00 | 0 |
| Slot allowlist | KEEP | 240,681.95 | -49,697.79 | 208 |

**KEEP means retain the existing setting until better evidence exists; it does not mean that every threshold is optimal.** The maximum-wick and special minimum-wick effects involve only 10 and 12 changed positions. Core-first priority changed no executed positions, so this sample cannot prove its value. The slot-allowlist row tests restoring excluded slots rather than removing a numeric indicator.

Keep data/timestamp/opening-evidence/OI validity and portfolio protections mandatory. Minimum directional move, confirmation direction, original exits and capital settings were held fixed, not proved optimal by these ablations. Broad extension caps, efficiency, opening breakout and replacement same-clock RVOL all reduced total profit here. We have no basis to add RSI, MACD and ADX as separate votes.

The [full feature inventory](FEATURE_INVENTORY.md) maps every family to its formula, signal availability, warmup, missing-data policy, source and overlap. Exact slot/side thresholds remain in [the original configuration reference](../g3_v18_20261102/FULL_SLOT_CONFIGURATION_2026-11-03.md). [Slot/side/regime results](results/slot_side_regime.csv) show the measured breakdown.

## AND, OR, ranking and AI

The tested OR logic is:

```text
all unchanged data, price, EMA, candle, volume, opening, risk and OI-upper gates
AND
(
    original rising-OI/minimum-OI requirement
    OR
    complete valid market evidence
    AND signed Nifty spot return from open >= 0.30%
    AND same-direction F&O breadth >= 60%, with >=90% coverage
    AND signed stock relative strength versus Nifty >= 0
    AND native 5m volume ratio >= 1.20
)
```

Valid positive current and previous OI are still compulsory; missing OI never becomes an alternative signal. The loose/strict neighbors are 0.20%/55% and 0.40%/65%. Existing early-short Nifty-futures guards remain. The OR market context uses **Nifty spot**, distinct from the retained dated-futures guard.

Fixed ranking averages four bounded groups: market, relative strength, participation and candle/extension quality. It operates after eligibility and before quotas, retaining core priority. It reduced profit both alone and after OR admission. Combining many correlated indicator votes did not rescue it.

Both models rank **exactly the same pre-quota OR-eligible pool**. Their predictions use only earlier whole-day filled outcomes and fixed hyperparameters, with training-only imputation/scaling. Unfilled orders have no win/loss label. The prediction target is a native independent fill with positive net return at 5 bps; final portfolio results still reapply capital and duplicate-symbol restrictions.

| Model | Evaluation days | Net ₹ | OR reference ₹ | Δ vs same pool ₹ |
| --- | --- | --- | --- | --- |
| logistic_or_rank | 43 | 512,195.04 | 541,845.15 | -29,650.12 |
| histgb_or_rank | 43 | 496,160.51 | 541,845.15 | -45,684.64 |

There are **895 OR-eligible candidates**, **793 valid filled labels** and **575 later evaluation labels**. Logistic/boosting AUCs were **0.465/0.517**. Their log losses, **0.6783/0.6790**, were worse than the earlier-training base-rate benchmark, **0.6142**. These scores are not validated win probabilities.

Joint within-day group permutation and **72 prior-only drop-group refits** were also completed. Dropping the market group improved pooled predictive loss, but neither model then beat the base-rate benchmark; improvement was inconsistent across folds. These diagnostic subsets were not selected for a new trading strategy or presented as additional P&L. See [model diagnostics](results/models/model_diagnostics.json), [same-pool comparison](results/model_vs_same_pool.csv) and [group ablations](results/models/group_ablation/group_ablation_metrics.csv).

## Costs, fills and uncertainty

| Variant | Cost bps | Net ₹ | Δ vs control ₹ |
| --- | --- | --- | --- |
| control | 5 | 984,038.33 | 0.00 |
| control | 9 | 905,638.33 | 0.00 |
| control | 15 | 788,038.33 | 0.00 |
| all51_slots | 5 | 1,224,720.28 | 240,681.95 |
| all51_slots | 9 | 1,106,720.28 | 201,081.95 |
| all51_slots | 15 | 929,720.28 | 141,681.95 |
| or_market | 5 | 960,692.96 | -23,345.36 |
| or_market | 9 | 870,092.96 | -35,545.36 |
| or_market | 15 | 734,192.96 | -53,845.36 |
| or_market_strict | 5 | 991,854.05 | 7,815.72 |
| or_market_strict | 9 | 906,854.05 | 1,215.72 |
| or_market_strict | 15 | 779,354.05 | -8,684.28 |

Moving entry triggers **1 bp adversely and rerunning native fills**, rather than merely subtracting an extra fee, changed full-period control profit by **₹-170,695.75** and OR profit by **₹-174,420.61**. Trigger stress changes which orders fill and their later exits; it is not a measured bid/ask execution model. This material sensitivity is another reason not to treat the headline profit as guaranteed executable profit.

[Paired uncertainty](results/paired_uncertainty.csv) uses 2,000 paired whole-day resamples in five-session circular blocks. Intervals are conditional on this reused history and are **not adjusted for selecting the best of many variants**. Short four-day extensions have no confidence interval. October OR's interval spans negative and positive outcomes. All51's positive full-period interval does not remove its October deterioration or historical-policy bias.

Reports include expectancy, profit factor, losing days, worst day, daily realized drawdown, maximum within-day minute-close marked drawdown, entry/round-trip turnover and native-clock exposure. Minute-close drawdown misses unobserved intrabar extremes/spreads; native capital capacity does not establish order-book capacity or market impact. Missing market context is explicitly separated from flat-market context in the regime report.

## Tools, news and new hypotheses

Use the **existing native replay engine, pandas/NumPy and installed scikit-learn**. They were sufficient for this study. No collection of new packages was installed. The [primary-source tool review](TOOLS_RESEARCH.md) checks Qlib, vectorbt, TA-Lib, scikit-learn, LightGBM, RD-Agent and FinGPT, including versions, licenses, Windows/Python constraints, data needs and replay limitations. LightGBM was not installed; the tested flexible baseline is scikit-learn histogram gradient boosting.

**Do not make news a required entry filter yet.** No receipt-complete historical NSE/BSE event corpus was verified for this universe and period. EOD announcements cannot be retroactively used for an earlier intraday decision. Event pauses, event-aware ranking/sizing and FinGPT/FinBERT classification therefore have **no claimed backtest result or manually validated classifier**. The [separate news/data report](NEWS_AND_DATA_AUDIT.md) records official sources, timing, quoted pricing/terms and the required publication/receipt/revision/ISIN/coverage schema.

Prioritized next hypotheses, without claiming they improve profit:

1. **Fresh confirmation of the fixed OR path:** existing data/implementation; smallest incremental experiment, no new indicator.
2. **Breakout failure/retest and pullback quality:** causally defined completed-bar structures, using existing full minute paths; moderate implementation cost, potential overlap with wick/extension.
3. **Genuine previous-session levels/gaps and same-contract OI roll context:** needs full cash-session/contract metadata audit. Native `prev_close` means previous 5m close, not yesterday's close.
4. **Sector breadth or event-risk flags:** needs effective-dated membership or verified receipt-time events. Current memberships and hindsight headlines are insufficient.

The inventory ranks 14 families with formulas and data requirements. Broader compression/retest/sector/VIX/depth/news experiments remain explicitly untested, rather than being silently counted as successes. Candle-derived CVD studies were reviewed; their pressure proxies are not measured aggressor flow and provided no robust reason to add another mandatory vote.

## Evidence limits and verification

- G3's 34-slot policy used history through **27 October**. The following four sessions had also already been inspected. **There is no untouched strategy holdout.** Earlier-only model fitting prevents model-label leakage but cannot undo the inherited slot-selection bias.
- This is a frozen available-data cohort, not proof of complete point-in-time F&O membership. Some source runs exclude entire stock-days using after-close coverage information. Oct28 and Nov2 each retain 215 stocks; separate opening exclusions remain. Results are conditional on that cohort.
- Weekdays absent from the represented calendar are **2026-08-24, 2026-08-25, 2026-09-14, 2026-10-01, 2026-10-02, 2026-10-20**. They were not inserted as zero-return days; this study does not assert whether each is a holiday or unavailable data. See [calendar exclusions](results/calendar_exclusions.csv).
- The all51 test required seven missing signal clocks on four late dates. **6,034 rows** were reconstructed from hash-verified sealed sources within the existing stock cohorts. **23,274 existing non-opening rows** passed native feature parity; original rows were not replaced. See [expansion proof](expanded_slots/README.md).
- **63/63 fresh native control checks passed** against saved selection, fills, prices, exits and net P&L. **43 unit/causality tests passed.** Independent audits reconcile all primary day/variant/cost cells. Failed final exports were recovered by verifying existing daily artifacts and normalizing Boolean dtypes; trading outcomes and frozen thresholds were not changed.
- The [independent audit](FINAL_AUDIT.md), [numeric verification](results/VERIFICATION.md), input manifests, per-day decisions, source hashes and export-recovery records are included. Financial estimates are backtest evidence under the stated assumptions, not definite future profits.

## Smallest decision-changing next experiment

Freeze **control versus the existing default OR path**, retaining the strict neighbor only as a declared sensitivity check. Collect an initial **20 genuinely new complete sessions**, then extend if fewer than **50 changed executed trades** are observed. This is a prospective research design, not a statistically guaranteed sample size. No new scheduled job or live strategy was activated.

Record the actual as-of stock universe, first data receipt, readiness, exclusions and candidate ranking on every slot. Never exclude a whole day using information learned after close. Use actual observed spreads/fills where available, with unchanged native risk limits.

Before considering paper promotion, require positive paired net improvement at 9 bps, nonnegative improvement at 15 bps, no worsening of losing-day count/worst-day loss/drawdown, and no dependence on the best one or two days. For a production decision, require additional independent paper evidence and a positive lower bound on a predeclared paired confidence interval, with multiple comparisons accounted for. If evidence is inconclusive, retain the control instead of retuning until it passes.

## Files and reproduction

- [Interactive HTML tables](results/report.html)
- [Day-wise comparison: every rule/model plus saved G7](results/daywise_comparison.csv)
- [All periods and cost assumptions](results/summary_comparison.csv)
- [Winners removed, losers avoided and new trades](results/trade_changes.csv)
- [Per-slot, side and market regime](results/slot_side_regime.csv)
- [Every daily scenario/cost result](results/all_daily_results.csv)
- [Frozen experiment protocol](protocol.json), [frozen input manifest](inputs/manifest.json)

Run these commands from the repository root. The existing results are preserved: use **new** output names. The delivered input and supplemental hashes allow an exact replay of this frozen study. Recreating from upstream sources may fail or differ if those files were changed or removed.

```powershell
# Tests: these are pytest tests, not unittest test cases.
python -B -m pytest g3_indicator_lab_20261103/tests_features.py g3_indicator_lab_20261103/tests_selection.py g3_indicator_lab_20261103/tests_model.py -q -p no:cacheprovider

# Replay the preserved frozen inputs. New output path required.
python -B -m g3_indicator_lab_20261103.run --inputs g3_indicator_lab_20261103/inputs --output g3_indicator_lab_20261103/replay_repeat

# Export finalizer validates every completed day. It also handles the known
# mixed bool/float Arrow export issue if replay finished all days first.
python -B -m g3_indicator_lab_20261103.finalize_partition --partition g3_indicator_lab_20261103/replay_repeat --inputs g3_indicator_lab_20261103/inputs

python -B -m g3_indicator_lab_20261103.merge --parts g3_indicator_lab_20261103/replay_repeat --output g3_indicator_lab_20261103/results_repeat
python -B -m g3_indicator_lab_20261103.run_models --results g3_indicator_lab_20261103/results_repeat
python -B -m g3_indicator_lab_20261103.model_group_ablation --results g3_indicator_lab_20261103/results_repeat
python -B -m g3_indicator_lab_20261103.report --results g3_indicator_lab_20261103/results_repeat
```

The delivered run used five disjoint partitions: `part_a`, `part_b`, `part_c`, `part_d30`, `part_d_remaining`. Their calendars union exactly to the frozen 63 dates; positions close each session. `merge.py` rejects missing/duplicate dates or differing frozen computation hashes. `day_artifacts.json` links the exact daily ledgers. Export-only fixes and report hardening are recorded separately from frozen strategy code.

For an independent input reconstruction audit, use `python -B -m g3_indicator_lab_20261103.prepare --output g3_indicator_lab_20261103/inputs_rebuilt`, then `python -B -m g3_indicator_lab_20261103.expand_slots --inputs g3_indicator_lab_20261103/inputs_rebuilt --output g3_indicator_lab_20261103/expanded_slots_rebuilt --workers 4`. Compare hashes and provenance before using rebuilt inputs. The final replay/model runners intentionally consume the delivered `inputs`/`expanded_slots` for exact replication; changing the research universe or input root is a new study, not a transparent substitution.
