# FNO V9 extended-slot honest optimization feasibility

Generated 2026-08-20 IST.

## Requested objective

Optimize every 5-minute LONG and SHORT leg in the V9 extended morning
(09:50-10:30) and afternoon (12:30-14:00) blocks, targeting an honestly
validated PF above 1.50.

## Eligibility verdict

**The current history is not sufficient for an honest per-slot parameter
search.** A per-slot sweep can be executed mechanically, but selecting its
highest PF would be curve fitting and cannot be described as an honest result.

The 56 extended slot-side legs were split at 2026-07-17:

| Evidence test | Legs passing |
|---|---:|
| At least 20 TRAIN fills | 0 / 56 |
| At least 15 TRAIN fills | 1 / 56 |
| At least 8 TEST fills | 13 / 56 |
| At least 20 TRAIN and 8 TEST fills | **0 / 56** |
| PF >=1.5 in both splits with even a lax 10 TRAIN / 5 TEST minimum | **0 / 56** |

Most legs have only 3-16 TRAIN fills. Optimizing many thresholds, brackets,
pickers, caps and entry parameters against samples this small will produce
large PF values by chance.

## Existing broader search

The existing block tuner pooled the thin legs to make the sample larger and
evaluated 50,688 configurations using TRAIN-only robust-PF ranking, minimum
trade/day guards and one-shot TEST evaluation.

| Block | Side | Configurations surviving TRAIN guards |
|---|---|---:|
| 09:50-10:30 | LONG | 0 |
| 09:50-10:30 | SHORT | 0 |
| 12:30-14:00 | LONG | 0 |
| 12:30-14:00 | SHORT | 0 |

Thus even the statistically safer pooled search did not establish an extended
slot edge.

## Tiny-sample observations, not selections

These baseline legs happened to show PF above 1.5 in both splits, but all fail
the minimum evidence requirement:

| Leg | TRAIN fills / PF | TEST fills / PF | Reason rejected |
|---|---:|---:|---|
| 12:45 LONG | 5 / 1.936 | 10 / 1.657 | Only 5 TRAIN fills |
| 12:50 LONG | 7 / 1.547 | 6 / 2.181 | Only 13 total fills |
| 13:25 LONG | 4 / 4.252 | 4 / 13.178 | Only 8 total fills |
| 13:30 LONG | 6 / 1.692 | 6 / 1.820 | Only 12 total fills |

Other visually attractive examples are still weaker statistically: 10:25
SHORT has 3/4 fills across the splits, and 12:30 SHORT has 8/4.

## Additional honesty blockers

1. Current V9 uses the legacy V6 signal cache and inherits its known
   cross-session path bug and optimistic exact-trigger fill mechanics.
2. The available universe and August futures-OI history are later-dated/static,
   not point-in-time rolling contracts for the whole historical period.
3. The V8 completeness audit shows extensive missing symbol-session coverage.
4. Trying many parameter combinations separately for 56 legs creates a severe
   multiple-testing burden that cannot be corrected with this sample size.

## What is required before an honest per-slot optimizer can run

- Rebuild under the same-session V8 engine, not the V6/V9 legacy cache.
- Obtain a point-in-time rolling futures universe and OI history.
- Repair missing 1-minute equity coverage and exact session endings.
- Collect enough history for at least 40 TRAIN and 15 validation fills per
  enabled leg; 60/20 is preferable.
- Preregister a bounded parameter family before inspecting outcomes.
- Select only on TRAIN, use validation once, and keep a final untouched test.
- Correct for all attempted slot-side configurations, then replay frozen
  winners together through one global capital/duplicate ledger.

## Current recommendation

Keep the V9 extended morning and afternoon blocks disabled. Retain 12:45,
12:50, 13:25 and 13:30 LONG as prospective shadow hypotheses only. Do not
promote or claim PF above 1.5 until the evidence requirements above are met.
