# A_MOD_BREAK_C1_HIGH — Recovery-Loop Final Recommendation

_Generated 2026-07-03. Research-only. No live trades. No config edits made._

## Verdict: **GOAL NOT ACHIEVABLE — the setup has no real edge. Recommendation: NO. Retire the expression.**

This was the from-scratch redesign loop (new working area, new 1-minute path engine, entry/exit
redesigns — not parameter re-tuning). It confirms the two prior campaigns with stronger evidence:

| proof point | data |
|---|---|
| excursion geometry | median MFE +0.87% vs ~0.30-0.35% cost toll; EOD drift ≈ 0 |
| 42 exit geometries incl. trail/BE/time | best PF 0.55 (SL1.5/T1.75) |
| confirmation entry (63 variants) | ≈ base — proof premium eats the trash filter |
| retest-limit entry (29 variants) | ≈ base — adverse selection eats the better fill |
| risk overlays (day stops, leak-free) | PF 0.5→0.8, net still negative — loss shaping, not edge |
| honest ceiling after 233 iterations | **FIT 0.822 / VAL 0.751** (n=92/63), PF<1 |
| TRAIN band reached | 0/233 → TEST never touched |
| cumulative (3 campaigns) | ~3,200 configs, 0 positive-expectancy books |

## Closing exhibit: the 0-bps (frictionless) result

Zero slippage, zero costs, same engine (gross PnL only):

| config | FIT | VAL | TRAIN | TEST |
|---|---|---|---|---|
| baseline 0.70/1.00 (no dedupe) | 1757/1.100 | 1262/1.029 | 2973/**1.138** | 1098/**0.831** |
| R1 base 0.70/1.00 | 1673/1.038 | 1215/1.049 | 2930/1.107 | 951/0.832 |
| R1 best bracket 1.5/1.75 | 932/1.095 | 685/1.068 | 1596/1.080 | 620/0.774 |
| closest robust (ml1+rcomp+mtd3) | 93/1.527 | 63/1.484 | 156/1.309 | 66/0.604 |
| confirm10 1.5/1.75 | 914/1.153 | 660/1.113 | 1586/1.143 | 561/0.857 |
| retest0.30 1.5/1.75 | 910/1.108 | 673/0.891 | 1577/0.976 | 585/0.754 |

Two independent, sufficient causes of failure:
1. **Cost dominance** — gross TRAIN alpha is only PF ~1.04-1.15; the ~30bps round-trip toll needs
   gross ≳1.35 to net breakeven. Filters/entries/exits cannot manufacture the missing 0.25 PF.
2. **Alpha decay** — TEST (June) is **sub-1.0 at ZERO cost for every variant**: the gross edge
   itself disappeared out-of-sample. Even a free-trading account loses on this in June.

## Closest robust candidate (documented loser — NOT proposed)

First-per-ticker-day + genuine 20-bar-high + range_compress3≥0.76 + max 3 trades/day +
stop-after-1-realized-loss + SL 1.5 / Tgt 1.75, EOD 15:20. FIT PF 0.822 / VAL 0.751. Its only
use is as the definitive proof that even the best disciplined expression of this idea loses.

## Actions (require your approval)

1. **No config block exists to promote.** `final_setup_conf.py` + mirror untouched (verified).
2. **Remove `A_MOD_BREAK_C1_HIGH` from the live v11 overlay universe** — it still trades live
   at measured TEST PF 0.216 (campaign-1). This is the third independent condemnation.
3. Redirect research: the reusable assets from this loop are the **1-minute path engine**
   (`scripts/path_engine.py` — validated per-trade against the canonical resolver, supports
   confirmation/retest entries + trail/BE/time exits + leak-free day stops) and the
   **loss-clustering finding** (1-loss day stop lifted every book tested) — both applicable to
   setups that already have positive expectancy, e.g. the conf book's survivors.

## Warning

`DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES` — and in this case there is nothing to move.

## Rerun commands

```powershell
# path store (one-time)
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_BREAK_C1_HIGH\scripts\extract_1m_paths.py
# 183-iteration main driver
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_BREAK_C1_HIGH\scripts\recovery_iterations.py
# 50-iteration rescue block (leak-free day stops)
py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\A_MOD_BREAK_C1_HIGH\scripts\rescue_block.py
```
