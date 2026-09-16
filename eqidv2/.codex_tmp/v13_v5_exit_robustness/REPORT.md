# V13-v5 Frozen-Entry Exit and Robustness Audit

## Scope and parity

- Read-only replay of 79 frozen V13-v3 selected orders across 25 sessions; no entry selection was changed.
- Chronological reporting: TRAIN = 12 sessions through 2026-08-13; TEST = 11 sessions 2026-08-14 through 2026-09-01; LATEST = 2 sessions 2026-09-02 through 2026-09-03.
- Exact parity: V3 max return delta 2.2e-14; V4 max return delta 1.89e-14; cache/source hashes passed.
- **Critical terminal-data caveat:** 69/79 selected paths infer an end before 15:30 (69 end at 15:15). Consequently 25/31 native square-offs and 34/39 V4 EOD-dependent exits use a truncated terminal close. Rebuild/replay through true 15:30 data before promotion.
- Path cache contains one-minute high/low/close arrays only. There is no open or timestamp array, so gap-aware fills and exact wall-clock holding cannot be reconstructed; array-index minutes are used.
- A separate raw-minute audit found 3/78 fills where the bar open had already gapped through the trigger; published replay nevertheless fills at the trigger. It found no fill-bar stop/target events or first stop/target same-bar ties in this sample.
- **Cache concurrency defect observed:** V3's cache-hit path still calls `_store_cached` (`fno_v13_corrected_v3_backtest.py:379`), while the NPZ writer is a direct non-atomic `np.savez_compressed` (`fno_v6_corrected_backtest.py:336`). A concurrent probe transiently changed the September NPZ hash from provenance `a2b0c856...` to `bc9a92fc...`; the hash guard stopped this replay until a byte-identical cache was restored. Avoid concurrent V3 loader calls.
- Same-minute stop/target ambiguity follows published pessimistic rules: stop wins ties. V4 also permits same-bar post-T1 breakeven/runner checks, again with runner stop winning ties.

## Core comparison

| Config | Role | Fills | Win % | First-objective % | PF | Net % | DD % | Train PF | Test PF | Latest net % | Median hold min | Mean capital-min |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| V3_NATIVE | baseline | 78 | 56.410 | 21.795 | 2.770 | 46.034 | -3.041 | 2.992 | 2.899 | -1.600 | 229.0 | 210.2 |
| V4 | conservative | 78 | 70.513 | 55.128 | 2.880 | 42.000 | -2.867 | 2.904 | 3.440 | -1.600 | 290.0 | 225.7 |
| SO_S1.500_T11.075_P0.20_R2.60_BREAKEVEN0.00 | balanced | 78 | 70.513 | 55.128 | 2.889 | 42.215 | -2.867 | 2.913 | 3.453 | -1.600 | 290.0 | 226.0 |
| SO_S1.500_T11.075_P0.10_R2.60_BREAKEVEN0.00 | numeric_best | 78 | 70.513 | 55.128 | 2.984 | 44.333 | -2.867 | 3.025 | 3.538 | -1.600 | 290.0 | 233.2 |
| SAFE_CAP210_T11.100_P0.20_R2.60_BE | terminal_safe | 78 | 65.385 | 52.564 | 3.063 | 41.699 | -2.652 | 3.148 | 3.072 | -0.147 | 210.0 | 166.7 |
| P10_CAP_90M | capacity | 78 | 64.103 | 39.744 | 3.265 | 32.327 | -1.546 | 5.002 | 1.896 | 0.224 | 90.0 | 81.9 |

## Representative rejected / experimental variants

| Config | Why not preferred | Win % | Objective % | PF | Net % | DD % | Train PF | Test PF | Latest net % |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| FIXED_S1.500_T1.000 | metric-targeting: net/expectancy collapse | 70.513 | 56.410 | 2.018 | 22.746 | -2.867 | 1.932 | 2.587 | -1.600 |
| SO_S1.250_T11.075_P0.10_R2.60_ORIGINAL0.00 | lower WR/PF and worse DD | 62.821 | 55.128 | 2.598 | 44.815 | -4.460 | 2.477 | 3.119 | -1.350 |
| SO_S1.500_T11.075_P0.10_R2.60_LOCK0.20 | extra complexity; trails BE P10 | 70.513 | 55.128 | 2.963 | 43.862 | -2.867 | 3.017 | 3.490 | -1.600 |
| SIDE_LONG_P10_SHORT_V4 | side-specific sparse-cell overfit risk | 70.513 | 55.128 | 2.970 | 44.016 | -2.867 | 3.008 | 3.526 | -1.600 |
| P10_CAP_180M | only 50% objective; weaker test net | 64.103 | 50.000 | 3.198 | 38.906 | -2.316 | 3.798 | 2.452 | -0.188 |

## Excursion and holding-time findings

- Full-session MFE: median 1.315%, q75 2.624%, q90 3.949%.
- Full-session MAE: median 0.700%, q75 1.329%, q90 2.071%.
- V3 median elapsed hold 229.0 index-minutes versus V4 290.0; V4 mean size-weighted capital minutes 225.7.
- Full-session excursions after a strategy exit are counterfactual and are diagnostic only; pre-exit excursion columns are separately stored in `mae_mfe_trade_audit.csv`.

## Stress conclusions

- **V3_NATIVE:** at 20 bps PF 2.096, net 34.334%; at +1 bar / +5 bps adverse execution PF 2.216, net 36.255%, fills 78; after removing top 3 trades PF 2.430, net 37.184%; day-bootstrap 2.5% net bound 21.294%, P(net>0) 100.0%, and P(first-objective rate>50%) 0.0%.
- **V4:** at 20 bps PF 2.166, net 30.300%; at +1 bar / +5 bps adverse execution PF 2.056, net 27.901%, fills 78; after removing top 3 trades PF 2.579, net 35.280%; day-bootstrap 2.5% net bound 19.222%, P(net>0) 100.0%, and P(first-objective rate>50%) 74.8%.
- **SO_S1.500_T11.075_P0.20_R2.60_BREAKEVEN0.00:** at 20 bps PF 2.174, net 30.515%; at +1 bar / +5 bps adverse execution PF 2.064, net 28.111%, fills 78; after removing top 3 trades PF 2.588, net 35.480%; day-bootstrap 2.5% net bound 19.422%, P(net>0) 100.0%, and P(first-objective rate>50%) 75.2%.
- **SO_S1.500_T11.075_P0.10_R2.60_BREAKEVEN0.00:** at 20 bps PF 2.209, net 32.633%; at +1 bar / +5 bps adverse execution PF 2.135, net 30.006%, fills 78; after removing top 3 trades PF 2.662, net 37.140%; day-bootstrap 2.5% net bound 21.178%, P(net>0) 100.0%, and P(first-objective rate>50%) 75.5%.
- **SAFE_CAP210_T11.100_P0.20_R2.60_BE:** at 20 bps PF 2.233, net 29.999%; at +1 bar / +5 bps adverse execution PF 2.074, net 26.861%, fills 78; after removing top 3 trades PF 2.729, net 34.949%; day-bootstrap 2.5% net bound 20.586%, P(net>0) 100.0%, and P(first-objective rate>50%) 62.2%.
- **P10_CAP_90M:** at 20 bps PF 2.077, net 20.627%; at +1 bar / +5 bps adverse execution PF 2.112, net 19.656%, fills 78; after removing top 3 trades PF 2.761, net 25.134%; day-bootstrap 2.5% net bound 14.884%, P(net>0) 100.0%, and P(first-objective rate>50%) 3.0%.

## Recommendation interpretation

- **Balanced:** `SO_S1.500_T11.075_P0.20_R2.60_BREAKEVEN0.00` is the best tested operationally meaningful scale-out (at least 20% booked) under all/train/test PF, >=50% first-objective, drawdown and fill guardrails. Treat it as a candidate, not a promotion, because it was selected from a neighborhood on the same 25 sessions.
- **Conservative:** `V4` keeps the documented 20% at +1.05%, 80% breakeven runner to +2.60% rule. It is simpler and already independently published, so it is more defensible than a numerical neighborhood winner even if the latter scores higher in-sample.
- **Terminal-data-safe candidate:** `SAFE_CAP210_T11.100_P0.20_R2.60_BE` is the best tested >=20%-partial capped rule with >50% first-objective and train/test PF guardrails. Its cap exits before 15:15 for every active setup, so its path outcomes do not depend on the missing terminal interval; it is still selected on the same small sample.
- **Capacity/high-frequency relevant:** `P10_CAP_90M` is the strongest <=90-minute cap by the train/test PF-floor screen. It does not increase selected orders in this frozen replay; it only releases capital sooner. A portfolio-level concurrency replay is required before claiming additional trades.
- **Numerical best:** `SO_S1.500_T11.075_P0.10_R2.60_BREAKEVEN0.00` is retained for comparison. If its partial is only 10%, its >50% first-objective rate should not be marketed as a full target-hit rate.
- Latest Sep 2-3 contains only two fills. Its negative result is reported but is far too small to validate or reject an exit.

## Key rejected ideas / cautions

- Uniform +1.00% target / 1.50% stop raises target-hit rate but materially compresses net and expectancy; it is metric optimization, not the preferred economic exit.
- Original-stop runners and +0.20% locked runners are included in the neighborhood ledger; a single best row should not be trusted unless its train/test floor and local-neighborhood minima are stable.
- Side/setup/time overrides are exploratory on only 1-15 observations per cell. They are recorded but excluded from automatic recommendations due multiple-testing and sparse-cell overfit risk.
- Delay/worse-fill tests are synthetic, not reconstructed exchange fills. The adverse-price floor is conservative, but delaying entry is not mathematically monotone because it changes which subsequent path is exposed. The cache cannot model gaps, spread, partial fills, queue position or broker latency.
- No exit candidate is promotion-ready while the 15:15-versus-15:30 terminal-path defect remains. Relative results may change most for EOD_NO_T1 and T1_THEN_EOD trades.
- Bootstrap resamples whole days to preserve within-day trade clustering, but 25 sessions remains a very small empirical distribution. Monte Carlo reshuffling changes drawdown order, not PF or total return.

## Artifact map

- `baseline_parity.json`: immutable input hashes and exact V3/V4 reproduction checks.
- `path_end_audit.csv`: inferred terminal minute and affected V3/V4 EOD exit flags for every selected order.
- `mae_mfe_trade_audit.csv`, `mae_mfe_summary.csv`, `excursion_thresholds.csv`: trade-level and grouped excursion/holding evidence.
- `experiment_ledger.csv`: fixed, scale-out neighborhood, timing/side and time-cap metrics for all/train/test/latest.
- `cost_stress.csv`, `execution_stress.csv`, `best_trade_removal.csv`: deterministic robustness tests.
- `bootstrap_monte_carlo.csv`, `parameter_neighborhood_summary.csv`: stochastic and local-parameter robustness.
- `timing_side_comparison.csv`, `recommendations.json`, `artifact_manifest.json`: sliced results, chosen roles, and hashes.
