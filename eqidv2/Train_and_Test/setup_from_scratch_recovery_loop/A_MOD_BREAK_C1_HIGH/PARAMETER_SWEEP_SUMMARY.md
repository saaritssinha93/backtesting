# A_MOD_BREAK_C1_HIGH — Recovery-Loop Sweep Summary

_Generated 2026-07-03. 233 iterations (blocks A-H), full log `iterations.csv`. All on the R1
structural base (first-per-day + genuine 20-bar-high) unless noted. FIT/VAL leak-safe;
TEST touched zero times (no config ever reached the TRAIN band)._

## Block A — exit geometry (36 brackets + 8 exotic)

| finding | value |
|---|---|
| best bracket | **SL 1.5 / Tgt 1.75 → FIT 0.506 / VAL 0.547** |
| MFE-matched tight brackets (SL 0.35-0.55 × Tgt 0.8-1.25) | FIT 0.32-0.45 — worse: tight SLs die to 1-min noise even below MAE-before-MFE medians, because losers gap through |
| time caps 60/120/180 | ≤0.45 — cutting time cuts winners more |
| breakeven 0.3/0.5 | ~0.40 — BE stop harvests losses early |
| trail-only 0.6-1.25 | 0.35-0.48 — trailing locks in noise |

*Lesson: the geometry insight (tight MAE-before-MFE) does NOT convert to profit — adverse moves
gap through tight stops faster than the favorable moves pay. Wide brackets lose slowest.*

## Block B — confirmation entry (36)

Best: confirm≤10m, SL 0.7/Tgt 1.75 ≈ FIT 0.49/VAL 0.51. **Never beats next-open**: the
trash-filter benefit (skipping the 10% never-confirmed) is fully paid back by entering higher.

## Block C — retest-limit entry (27)

Best ≈ FIT 0.45/VAL 0.50 (depth 0.30, window 30m). Adverse selection confirmed: limit fills
concentrate in the weaker half; the ~0.3% better basis does not compensate.

## Block D — time windows (5)

Morning-only (≤11:05): FIT 0.577/VAL 0.532 — no lift over base on this pool. Mid/late worse.

## Block E — single masks (15)

Only `range_compress3 ≥ 0.76` holds both windows (FIT 0.551/VAL 0.563). RSI/ADX/EMA-stack/
gap/day-ret/VWAP-hold/quality all fail one side or both.

## Block F — crowding/risk guards (11)

max_trades_day 10: 0.57/0.59; max_open 10: 0.54/0.61; top_n: no lift.
daily_loss 4k *initially* 0.75/0.71 — **but see Block H: with leak-free realized-only
accounting it drops to ~0.6**. (The first pass credited the stop with knowledge of open trades'
final PnL; caught and fixed.)

## Block G/H — stacks, loss-count stops, exit retunes (60)

| config | FIT n / PF | VAL n / PF |
|---|---|---|
| **ml1 + rcomp + mtd3** (1-loss day stop, range-compression, ≤3 trades/day) | 92 / **0.822** | 63 / **0.751** |
| ml2 + rcomp + mtd3 | 93 / 0.806 | 63 / 0.751 |
| dl4k + rcomp + SL1.75/T3.0 | 709 / 0.603 | 520 / 0.614 |
| confirm10 + dl4k + rcomp | 810 / 0.626 | 602 / 0.549 |

## Stable ranges (for the record)

- Exits: SL 1.5-1.75 × Tgt 1.75-3.0 (wide) — least bad, robust across blocks.
- Masks: range_compress3 ≥ ~0.76 only.
- Risk: loss-count day stop (1-2) + trade-count caps — biggest honest lift, but it is *loss
  shaping*, not edge creation: PF rises because fewer bleed-days complete, net stays negative.

## Rejected ranges

Tight SLs (<0.7), breakeven/trailing exits, time caps, confirmation & retest entries, all
momentum/trend/candle masks except range compression, top_n, BULL-regime filters.
