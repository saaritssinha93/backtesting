# A_MOD_BREAK_C1_HIGH — Recovery-Loop Candidate Configs

_Generated 2026-07-03._

## PASSING CANDIDATES: **NONE (0 / 233 iterations)**

No configuration reached full-TRAIN PF 1.30 — TEST was therefore never evaluated for any config
(the anti-overfit discipline held; there was nothing to test-fit).

## Closest robust configuration (NOT a candidate — a documented loser)

"**Disciplined first-break**": first signal per ticker per day, break must be a genuine 20-bar
high, `range_compress3 ≥ 0.76` (coiled before the break), max 3 trades/day, stop trading for the
day after 1 realized loss, SL 1.5% / Tgt 1.75%, EOD 15:20.

| window | n | PF | net |
|---|---:|---:|---:|
| FIT | 92 | 0.822 | negative |
| VAL | 63 | 0.751 | negative |

- FIT/VAL consistent (no cliff), all pieces individually stable — this is the honest ceiling.
- Still **PF < 1**: it loses money slowly instead of quickly. Not tradeable.

## Why nothing passes (proof summary)

1. Per-trade population geometry: median MFE 0.87% vs cost+slip toll ~0.3-0.35% and median
   adverse-through-stop behavior kill every bracket: 42 exit combinations, best PF 0.55.
2. Entry redesigns are a wash: confirmation entry buys proof but pays the proof premium;
   retest entry buys cheaper but adversely selects. Measured, both ≈ base.
3. Risk overlays (day-stops) raise PF by amputating bleed-days but cannot make the residual
   positive — the remaining trades are still cost-negative.
4. Across two prior campaigns + this loop: **~3,200 configurations, zero honest positive-
   expectancy books.** The 5-min moderate-impulse chase-long expression has no exploitable edge
   at NSE intraday cost structure in Mar-Jul 2026.

`candidates/NO_CANDIDATES.md` stands.
