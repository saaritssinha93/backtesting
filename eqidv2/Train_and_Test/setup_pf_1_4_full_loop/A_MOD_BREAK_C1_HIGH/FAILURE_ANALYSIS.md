# A_MOD_BREAK_C1_HIGH — Failure Analysis

_Generated 2026-07-02. Basis: RAW TRAIN book (3,538 trades, Mar-May 2026, exit 0.70/1.00, 15 bps)._
_Artifacts: `failure_segments.csv`, `train_raw_book_with_features.csv`, `failure_study_stdout.txt`._

## Headline

The raw detector loses **Rs-1.84L over TRAIN at PF 0.224**. This is not a filter problem at the edges — the core trade expression is negative expectancy everywhere:

- **No single feature quintile exceeds PF 0.29** across rs_pct, atr_pct, vol_ratio, body, close_loc, vwap_dist, quality, wick, range, time.
- Outcome mix: **SL 2,426 (69%) / TARGET 664 (19%) / EOD 448 (13%)**.
- Realized avg loss **Rs-927** vs avg win **Rs+761** — the 0.70% SL loses more than the 1.00% target wins once costs/slippage/gap-through are paid. Negative asymmetry on top of a sub-30% hit rate.

## Loss classifications

| class | evidence | verdict |
|---|---|---|
| SL too tight | 69% SL-rate at 0.70%; exit sweep shows every SL≥1.0 beats it | primary mechanical flaw |
| target too small for costs | 1.00% target nets ~0.7% after 30bps round trip + slippage asymmetry | secondary mechanical flaw |
| weak momentum after break | pre-momentum features unavailable, but 19% target-fill says the break rarely follows through | structural |
| wrong time window (production gate) | gate caps at 11:10 (morning); mornings are where n is smallest and hour-10 PF worst (0.112); yet **as a guard** max11:05 improves book PF to ~0.35 — because midday crowding is even worse | production gate half-right for the wrong reason |
| chase / overextension | vwap_dist ≤2.8 lifts FIT but collapses VAL — not robust | inconclusive |
| fake breakout share | close>prev_high with moderate impulse fires ~68/day across the universe — most breaks are noise | structural overtrading |
| bad-volatility regime | atr_pct ≤0.004 lifts FIT, collapses VAL | not exploitable |
| day/symbol concentration | worst symbols only Rs-6..7.5k each (diffuse); worst days diffuse; day-block p = 1.0 | loss is EVERYWHERE, not event-driven |
| market-condition | regime==BULL *worse* than !=BEAR; mret≥0 worse | no macro rescue |

## Worst segments (for guard design)

- Hour 10 (early): PF 0.112 (n=62) — worst per-trade, but tiny n.
- Hours 11-12 (midday bulk): PF 0.21-0.23 on n=2,441 — the volume of the bleeding.
- Hour 14: PF 0.274 — least bad, contradicting the production ≤11:10 gate at the per-trade level.
- atr_pct 0.0043-0.0059: PF 0.187 — the current gate's ≤0.006 keeps most of this worst band **in**.

## Why the current production gate fails

`rs_pct≥2.0 & atr_pct≤0.006 & ≤11:10 + top2/slot` → TRAIN PF 0.315 (67 trades), TEST PF 0.201 (30). The rs_pct sweep is flat (no information), the atr cap includes the worst vol band, and the time cap points at the thinnest part of the day. Its 2026-06-09 "PF 3.25 over 10 sessions" validation was a 32-trade window artifact — over 52 TRAIN + 22 TEST sessions it is PF 0.2-0.3.

## Rejected-candidate autopsy (Stage 3)

Every FIT-improving knob except `max_slot 11:05` fails VAL. The failure signature is uniform: threshold tightening removes trades roughly proportionally from winners and losers — i.e., the features carry almost no conditional information about outcome. Combined with unavailable rsi/adx/ranker/macd/ema20_slope columns (NaN in raw pool) and empty pre-momentum features, the searchable space cannot express "momentum quality into the break", which is exactly what this setup would need.

## Implication for Stage 4-6

Any Optuna candidate that lands in the 1.30-1.80 TRAIN band will have done so by compounding 2 masks + exits + guards into a **small-n pocket**. The gate must therefore lean hard on: n≥20 TRAIN floor (prefer ≥40), day-block p, neighborhood ±1-quantile stability, term dropout, and honest TEST-once discipline.
