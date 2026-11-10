# G3 V18: relaxed estimated CVD

This separate research experiment tests six fixed, relaxed CVD rules after the strict experiment excluded many profitable trades. Existing G3 and prior CVD experiments are preserved. The experiment does not change live trading settings.

Open `START_HERE.html` for the result comparison and daily results once the replay completes. `methodology.json` records the rules and evaluation policy fixed before the new outcomes.

## Rules

For long trades, P is recent 15-minute estimated delta divided by volume, and A is its change from the preceding non-overlapping 15 minutes. For short trades, both signs are reversed. A uses the raw pressure difference, not the value divided by two.

| Rule | CVD requirement after warmup |
|---|---|
| Relaxed AND | P ≥ 0 AND A ≥ −0.05 |
| Either nonnegative | P ≥ 0 OR A ≥ 0 |
| Either soft | P ≥ −0.05 OR A ≥ −0.05 |
| Opposing-pressure veto | P > −0.10 OR A > −0.05 |
| Strong opposing-pressure veto | P > −0.15 OR A > −0.10 |
| Pressure tolerance | P ≥ −0.10 |

The last two OR rules reject a candidate only when both inputs cross their opposing thresholds. A valid OR branch never overrides missing or invalid data: after 09:45, exact feature timestamps, complete 30-minute readiness and finite inputs remain mandatory.

Before 09:45, all six variants retain original G3 opening candidates through an explicitly recorded native-G3 warmup bypass. They still require all original eligibility, ranking, core and extra quota rules. These opening trades have no CVD confirmation. This prevents a relaxation test from confusing the cost of skipping opening trades with the effect of CVD thresholds.

`base_control` reproduces original G3. `available_control` uses the same opening bypass and later readiness requirement without a directional threshold. Each rule is compared with this availability control. Its equivalence to original G3 is verified on the actual capture.

Eligibility is applied to the full native candidate pool before ranking; eligible alternatives can replace rejected candidates. Futures OI contracts and values, cash inputs, ranking of survivors, confirmation, execution, exits and capital rules are unchanged.

## Selection and evaluation

The user requested higher profit, fewer losses and a good win rate. These can conflict. The experiment reports every rule, daily and monthly results, losing trades, losing days, loss amounts, daily realized drawdown, trade retention and costs at 5, 9 and 15 bps. It also identifies non-dominated tradeoffs rather than pretending every metric has one common optimum.

A training rule must retain at least 80% of reference trades and at least 40 trades. At 5 bps it must not worsen net profit, losing-trade count, win rate, losing-trade rupees, losing-day count, losing-day rupees or daily realized drawdown. At 15 bps it must not worsen profit or drawdown. It must change selected stocks and improve at least one objective. Eligible rules are ranked by profit, fewer losing trades, higher win rate, lower losing-trade rupees, lower drawdown, more retained trades, then fixed rule order. If none qualifies, the availability control is retained.

September's choice uses July–August only. October's choice uses July–September only. Each is saved before the next period's replay is loaded. The main October evaluation and rolling October evaluation are the same fold. The full-history profit leader is explicitly labeled as a retrospective result, separately from these prior-period choices.

All 62 sealed sessions from July 29 through October 30 were viewed in earlier research, so these are retrospective chronological comparisons, not unseen validation. Original retrospective universe and opening-OI limitations remain. October 28 uses the inherited restricted 215-stock cohort; the report also evaluates its exclusion. November 2 has no sealed G3 execution replay.

Features reuse verified Kite one-minute OHLCV. They estimate pressure from candle close location; they do not observe buyer-initiated versus seller-initiated trade volume. No fetch, feature recalculation, threshold search or outcome-dependent rule revision is performed in this experiment.

## Reproduce

From the repository root, run the focused tests and use a new output directory:

```powershell
python -B -m pytest -p no:cacheprovider g3_v18_20261102/cvd_relaxed_20261102/tests_selection.py g3_v18_20261102/cvd_relaxed_20261102/tests_policy.py g3_v18_20261102/cvd_relaxed_20261102/tests_runner.py -q
python -B -m g3_v18_20261102.cvd_relaxed_20261102.run --features g3_v18_20261102/cvd_advanced_20261102/calculated_20261102/features.parquet --feature-provenance g3_v18_20261102/cvd_advanced_20261102/calculated_20261102/configuration.json --output g3_v18_20261102/cvd_relaxed_20261102/results_reproduction
```

The frozen feature SHA-256 is `5c368ea9a4150a75bf923390e89dff005e68f1748ab8b27af0a47568b21ddae9`. The result manifest records all replay artifact hashes. `preservation_before.json` and `preservation_verified.json` check previous G3 files by content and modification time. Runtime caches and dependency junctions are excluded from that preservation inventory.
