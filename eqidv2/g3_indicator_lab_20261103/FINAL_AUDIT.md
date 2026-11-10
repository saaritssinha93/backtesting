# Independent aggregation and model audit

Audit date: **2026-11-03**. Reviewed `report.py`, `run_models.py`, `merge.py`, their frozen model/selection/replay dependencies, then the completed merged run at [results](results). The final artifact checks below were repeated after reporting and the actual-data group diagnostics completed. Primary frozen strategy files were not edited by this auditor.

## Confirmed properties

- **Native cost arithmetic reconciles.** Independently recomputed all **4,536** primary day/variant/cost cells in the completed merged run: 63 sessions ×24 primary variants ×3 cost assumptions. Net P&L equals accepted gross P&L minus actual modeled exposure ×cost_bps/10,000; maximum absolute difference **₹1.46×10^-11**, with no discrepancy at ₹0.00001 tolerance. An earlier1,452-cell check independently passed the first partition. Portfolio selection is not P&L-dependent in this G3 engine, so repricing an accepted fixed-capital book is valid here. The reporting code additionally compares stressed summaries against native rerun daily totals; this shortcut would not be valid for G7's realized-loss lock without a new replay.
- **Minute-close closing balances reconcile.** The largest difference between primary 5bps daily MTM closing P&L and native daily net P&L in that partition was approximately **₹1.46×10^-11**. This establishes accounting consistency, not tick-level drawdown accuracy.
- **Daily drawdown uses chronological daily P&L with an initial zero-equity origin**, including represented zero-trade days. Positive drawdown magnitude is reported. Per-period comparisons restart at that period's zero; they do not pretend to retain earlier peak equity.
- **Candidate label construction is independent of quota/portfolio acceptance.** Model labels are independently filled native candidates from the complete OR-market eligible pre-quota pool, net5bps>0. Losing or capital-rejected potential candidates remain available for training. Unfilled candidates are not fabricated losing labels.
- **Chronological training is prior-only.** Training imputation/scaling and model fitting use earlier whole days; labels must exit before the first test decision. Test blocks are five sessions. Model fallback days are separately flagged and removed from `model_evaluation`. Both model families use the same readiness decision and candidate set.
- **Final selection remains native.** Learned scores change within-eligibility ranking, retain declared core priority/quotas, and then rerun allocation. Missing model scores cannot admit an otherwise invalid candidate.
- **Attribution separates membership changes from retained-position P&L changes.** The day/setup/symbol key identifies removed and added executed positions; retained net change is recorded separately. This is appropriate for the frozen one-candidate-per-identity protocol.
- **Saved G7 comparison is labelled separately** and its exact calendar must match. Its distinct stops, protection and loss lock mean it cannot be interpreted as a pure G3 indicator ablation.

## Concrete issues found and responses

| Finding | Risk | Status at audit handoff |
|---|---|---|
| Rule merge renamed the exposure column, whereas model output retained its old name; concatenating then renaming created duplicate `minute_close_max_exposure_rs` columns. | Ambiguous exported capacity column. Reproduced in memory. | Parent fixed report normalization **before** concatenation and added duplicate-column/day-variant-cost checks; change inspected. |
| Initial model P&L deltas compared only with `control`, although model admission uses the larger OR-market pool. | OR admission gains could be misattributed to AI ranking. | Parent added `model_vs_same_pool.csv`, paired model versus `or_market` on the same model-ready days at5/9/15bps; change inspected. Control comparisons remain useful as a separate end-to-end comparison. |
| Initial merge accepted any disjoint partition calendar. Existing first three configs covered59 of63 sessions, missing Oct28/29/30 andNov2. | A partial merge could appear complete, especially with hard-coded63-session prose. | Parent added equality to the63-day input-manifest calendar before merge. Final merged results include all63dates and all63control parity records pass. |
| Primary runner initially did not compare the live `model.py`/protocol with frozen hashes before fitting. | Accidental later code/protocol changes could silently alter an ostensibly frozen model run. | Parent added frozen input-manifest, protocol and model-code hash checks before fitting; change inspected. Parent also reports external verification of all63minute-input hashes during this run. The diagnostic runner independently verified primary model/protocol and saved model-source hashes. |
| Joint group permutation was described as feature importance but did not provide the requested **refitted** group ablation. | Permuting a trained model and retraining without a group answer different questions. | A separate, predeclared diagnostic implementation now supplies refitted ablation, without changing primary models or portfolio strategy arms. See below. |
| Frozen `features.grouped_scores` maps unavailable Nifty returns to its default `mixed_flat` diagnostic regime. | Missing context may be misrepresented as an observed flat market in regime summaries. | Parent added reporting-only `UNKNOWN_MISSING_CONTEXT` classification; change inspected. Frozen selection/features remain unchanged. |

## Distinctions the final interpretation must retain

1. **Minute-close drawdown is not full intraday maximum drawdown.** Native intrabar stop/target fills are recognized at their labeled minute end; unobserved intra-minute peaks, bid/ask spreads and executable mark prices are absent. The report computes the worst within-day minute-close drawdown, not the drawdown of one concatenated multi-day minute equity curve. It provides daily-close drawdown separately. Native clock exposure and minute-close exposure also differ when positions open and close inside one minute.

2. **Model scores are uncalibrated outputs used for ranking.** Brier score, log-loss, ROC-AUC and score-bin observed win rates audit calibration/discrimination; they do not calibrate the scores or establish live probabilities. The target is probability of a positive net independently filled candidate, not expected rupee P&L or probability an order fills. Final portfolio results are required because win/loss magnitude, quotas and capital matter.

3. **Grouped permutation is predictive diagnostic evidence.** Joint feature-group shuffling within a test day measures change in prediction loss. It can create unusual feature combinations and does not measure trading P&L contribution. The additional refitted group diagnostics below supply a different, complementary check.

4. **Same underlying data is not independent validation.** Earlier slot choices used history throughOct27; later sessions were also previously inspected. Chronological model fitting prevents direct future-label fitting, but cannot remove historical strategy-selection reuse. The report must call this exploratory rather than an untouched holdout.

5. **Uncertainty is conditional on reused history.** The paired circular five-session block bootstrap resamples whole-day differences, preserving within-day trade dependence. Its positive-resample fraction is not a probability that the rule will profit in the future. It does not correct for multiple strategy/diagnostic attempts, and short four-day extensions cannot provide reliable bootstrap intervals. No live promotion follows from a favorable interval.

6. **Warmup/availability can explain reduced trading.** Opening breakout, efficiency, extension and same-clock-volume arms can exclude entries because data/warmup is unavailable. Compare coverage counts and, where possible, availability-matched controls before attributing all improvement to the economic condition. Existing narrow source cohorts and whole-stock-day coverage exclusions are inherited limitations.

7. **A result label is not an automatic production decision.** The report's coarse family verdict rule is exploratory: few changed positions =>insufficient, positive total/October =>test next, otherwise keep. It is not a statistical acceptance test. Meaningful promotion still needs later-day downside, costs/fills, retained winner and concentration evidence, plus fresh paper evaluation.

## Additional refitted group diagnostics

`group_ablation_protocol.json` was written **before primary-model results**. It declares two fixed model families × dropping each of four existing groups = **8 diagnostic feature subsets**. These add **zero portfolio strategy arms**. There is no threshold search, feature subset selected for deployment, or change to `model.py`, `run.py`, `selection.py`, `features.py`, or `prepare.py`.

Run after the merged primary model stage has completed:

```powershell
python -m g3_indicator_lab_20261103.model_group_ablation --results <merged-results-directory>
```

Output: `<merged-results-directory>/models/group_ablation/`:

- `group_ablation_metrics.csv`: same-fold refitted drop-group versus saved full-model log-loss, Brier and AUC, plus pooled ready-label comparison.
- `group_ablation_calibration.csv`: separate full/drop score-bin observed rates; diagnostic only, no test-set calibration fit.
- `fold_stability.csv`: how consistently dropping the group improved/worsened later prediction loss.
- `predictions.parquet`: paired full/drop scores and valid-filled-label flags.
- `fit_inventory.json`, `complete.json`, `manifest.json`: exact attempted/successful refit counts and source/code/protocol hashes.

The runner checks frozen model/protocol compatibility, identical candidate IDs, saved fold counts, prior training class counts, filled evaluation counts and matured-label boundaries. It refuses existing output. Smaller drop-minus-full log-loss/Brier is better; larger drop-minus-full AUC is better. These diagnostics concern predictions conditional on fills; they are **not** additional portfolio P&L results.

A separate synthetic plumbing check completed successfully: **8 refits**, **16 metric rows** (8fold+8aggregate), **85 later valid filled labels per subset**. Its artifacts reside in `group_ablation_synthetic_check/` and are prominently marked **SYNTHETIC TEST ONLY**. They must not be included in trading results, financial performance claims or historical strategy-attempt counts.

The **actual-data** diagnostics completed **72/72 attempted refits**, covering9READYfolds and575later independently filled candidate labels. See [completion record](results/models/group_ablation/complete.json), [paired predictive metrics](results/models/group_ablation/group_ablation_metrics.csv), [fold stability](results/models/group_ablation/fold_stability.csv), and [diagnostic manifest](results/models/group_ablation/manifest.json).

| Model | Full-feature pooled log-loss | Drop market pooled log-loss | Drop-market improvement across folds | Interpretation |
|---|---:|---:|---:|---|
| Logistic |0.678306|0.648360|6of9|Market-feature removal improved prediction loss here, but pooled AUC remained0.478086; it does not establish a useful ranking model. |
| Histogram boosting |0.678976|0.668738|5of9|Modest predictive improvement, with pooled AUC0.533762 and mixed folds; not grounds for deployment. |

Dropping entry features also slightly improved pooled loss in both models; dropping participation worsened boosting loss from0.678976 to0.696560 and worsened6of9folds. These inconsistent family/model effects do not support a universal “remove this indicator” conclusion. Even the best displayed drop-market losses remain worse than the full models' same earlier-training-base-rate reference log-loss **0.614229**. No dropped-group model was substituted into a portfolio backtest or selected for deployment.

Primary model evidence is also unfavorable: [same-pool comparison](results/model_vs_same_pool.csv) shows, over43model-ready days at5bps, logistic ranking **₹29,650.12 below** `or_market`, and boosting **₹45,684.64 below**. At9/15bps both remain below that same-pool rule. Both primary models have worse Brier/log-loss than the earlier-training-base-rate reference. This supports retaining the simpler rule reference for further research rather than claiming AI improved this run.

## Final artifact checks

- **PASS:**63represented dates,24primary variants×3costs =4,536rows, plus252rows for4trigger-stress scenarios at5bps; **4,788daily rows** total, no duplicate day/variant/cost keys, unique exported column names.
- **PASS:** all63native control parity records report true; report checks saved benchmark calendar equality.
- **PASS:** independent final cost reconciliation above and completed reporting assertions; primary totals are not inferred from incomplete partitions.
- **PASS:**43model-ready days; initial20days excluded from model assessment; `model_vs_same_pool.csv` separates ranking from OR admission.
- **PASS:**72expected/attempted/successful diagnostic refits,9READYfolds, zero skipped non-READY folds;575filled labels assessed per diagnostic subset.
- **PASS:** diagnostic input/source hashes verified unchanged at completion; primary frozen code/protocol checked. Reporting-only corrections are listed above.
- **Scope limitation:** this auditor did not independently re-audit every live/paper/current-pointer filesystem path; that broader no-production-write verification belongs to the parent task's final source-integrity checks. The files created by this auditor are isolated research documentation, a diagnostic protocol/runner, and explicitly marked synthetic test artifacts.

Primary source documentation: [feature inventory](FEATURE_INVENTORY.md), [tools research](TOOLS_RESEARCH.md), [news/data audit](NEWS_AND_DATA_AUDIT.md), [frozen strategy protocol](protocol.json), [diagnostic protocol](group_ablation_protocol.json), [main report](results/report.html), [main artifact manifest](results/manifest.json), and [diagnostic manifest](results/models/group_ablation/manifest.json).
