# A_MOD_BREAK_C1_HIGH — Recovery-Loop Iteration Log

_Generated 2026-07-03. **233 iterations**, every one logged with config JSON + FIT/VAL (+TRAIN/TEST
where earned) in `iterations.csv`. Engine: `scripts/path_engine.py` (per-trade validated 400/400
vs canonical resolver). Discipline: FIT→VAL each iteration; TRAIN confirm at FIT&VAL≥1.10;
TEST once only in TRAIN band [1.30, 1.80]. **TEST was never touched — no config reached the band.**_

| block | iters | group changed | best (FIT/VAL) | keep/reject |
|---|---:|---|---|---|
| A | 1-44 | exits (36 brackets, 3 time-caps, 2 BE, 6 trail) | SL1.5/T1.75 → 0.506/0.547 | keep wide bracket |
| B | 45-118 | entry → confirmation stop-buy (K×SL×TGT ×trail/timecap) | 0.49/0.51 | reject (pays up > filters) |
| C | 119-145 | entry → retest limit (depth×window×bracket) | 0.45/0.50 | reject (adverse selection) |
| D | 146-150 | time windows | am 0.577/0.532 | neutral |
| E | 151-165 | single masks (15 features) | rcomp 0.551/0.563 | keep rcomp only |
| F | 166-176 | crowding/risk guards | dloss4k 0.748/0.705* | *leaky; see H |
| G | 177-183 | 2-way stable combos | dl4k+rcomp 0.802/0.797* | *leaky; see H |
| H | 184-233 | leak-free re-run + loss-count stops + 3/4-stacks + exit retunes + confirm/retest stacks + 2nd masks | **ml1+rcomp+mtd3 → 0.822/0.751** (n=92/63) | closest robust; still PF<1 |

*Leak caught & fixed mid-loop:* Block F/G's rupee day-stop initially credited entries with the
final PnL of still-open positions (lookahead). Block H re-ran everything with realized-only
accounting (`path_engine.py` day-stop uses only trades already closed at entry time).

## Verdict per acceptance gate

- TRAIN band 1.30-1.80 reached: **0 / 233**
- TEST evaluated: 0 (correctly — nothing earned it)
- CANDIDATES: **0**
- Closest robust config (documented, NOT a candidate): see `APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md`

## Failure classification across all 233

| failure | share | detail |
|---|---|---|
| core negative expectancy | dominant | every n≥100 config PF 0.3-0.65 |
| tight-SL noise death | Block A lows | gaps through stops kill MFE-matched brackets |
| entry-improvement paradox | B, C | better fills ↔ worse population; net wash minus fees |
| risk-shaping ≠ edge | F, G, H | day-stops raise PF by truncating bleed-days; net stays negative |
| FIT-VAL stable but sub-1.0 | best stacks | consistent — honest ceiling ≈ 0.8 |
