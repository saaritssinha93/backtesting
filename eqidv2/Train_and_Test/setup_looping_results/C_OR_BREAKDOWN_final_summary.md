# C_OR_BREAKDOWN — Final Summary

**Verdict: REJECT for sizing / keep unsized (parked).** Loser at realistic execution cost; the apparent paper-cost
positivity is breakeven on TRAIN and the published conf edge is not reproducible.

## What was done (≈20 evaluations)
- Verified backtest == live (bootstrap-only faithful port; no overlay contradiction — doc §5.4). ✔
- Baseline at 5 + 15 bps/leg; 12 hand iterations (exit grid, vol/quality masks, ADX tightening, time guard,
  close_loc); maxpf tuner @15 bps; band tuner @5 bps; 4 lead configs ×2 slippages; loss analysis.

## Key numbers (conf gate + best lead)

| Config | Slippage | TRAIN n/PF | TEST n/PF |
|---|---|---:|---:|
| conf gate (0.9/2.0) | 15 bps | 83 / 0.64 | 53 / 0.62 |
| conf gate (0.9/2.0) | 5 bps | 83 / 0.94 | 53 / 1.12 |
| `rs_pct≤-4.92` (1.2/1.5) | 15 bps | 121 / 0.64 | 76 / 0.93 |
| `rs_pct≤-4.92` (1.2/1.5) | 5 bps | 121 / 1.00 | 76 / 1.33 |

Conf published claim: TRAIN 2.78 / TEST 5.26 — not reproduced on the readmit pool basis.

## Why REJECT (against the acceptance bar)
- **Realistic cost:** loser everywhere (15 bps/leg) → not deployable (live paper PF 0.25 corroborates).
- **TRAIN ≥ 1.2:** never met robustly; best lead is breakeven (1.00) at paper cost, with train net concentrated
  in one day (top1day 54×).
- **TEST robust:** only the best lead clears it (1.33) and only at paper cost; the conf gate's TEST 1.12 @5 bps
  is one-day-dominated (185%).
- **Reproducibility:** conf 2.78/5.26 not reproducible (basis-size discrepancy, n=83/121 vs conf n=29).

## Honest uncertainties / leads
- **`rs_pct ≤ -4.92`** (deep RS weakness instead of the ADX gate) is a real structural lead: it produced the only
  well-distributed, near-significant positive TEST (1.33, n=76, dbp 0.20) — but only at paper cost and with a
  breakeven TRAIN. WATCH for re-validation when more data and realistic-cost profitability are available.
- **Tooling caveat surfaced:** the tuner's per-setup search PF (1.49, n=169) overstates the deployable book PF
  (1.00, n=121 after dedupe + 20-position overlay). Leads must be confirmed in the full-pipeline runner.

## Action
- **No config change.** Recommend parking/demoting (fails its re-promotion trigger at realistic cost). Flagged for review.
