# C_OR_BREAKOUT — FROM_SCRATCH_LOGIC_REVIEW

_Generated 2026-07-03. Research-only._

## 1. What the setup is trying to capture

Bull-side continuation: a relatively strong stock (rs_pct>0) above session VWAP, printing a
bullish candle (close>open, close_loc≥0.60), breaks the opening-range high on volume
(vol_ratio≥1.5) outside a BEAR regime → ride the continuation up.

Exact detector (reason `opening_range_breakout`):

```
np.isfinite(or_high) AND long_struct AND above_vwap AND close > or_high
AND rs_pct > 0.00 AND vol_ratio >= 1.5 AND regime != "BEAR"
```

## 2. Structural observations

1. **Scan starts ≈11:00 IST** (bar `max(VWAP_LOOKBACK=20, RS_LOOKBACK=6)`): every "OR
   breakout" is at least ~75 minutes stale; most are afternoon "price is above the morning
   high" states, not breakout events.
2. **No first-break requirement** — re-fires on every qualifying bar (raw pool ≈ 36-52
   fires/day); which re-fire survives dedupe is decided by quality_score, not freshness.
3. **Never promoted, no config of record.** It was one of the 12 pre-pooled setups that all
   failed the 2026-07-01 PF-band June-OOS campaign. Close cousins with the same skeleton
   (E_ORB_BREAKOUT_LONG "breakout-chase, 22% immediate-fail"; DOC5B momo breakout;
   FAST_MOMENTUM_LONG) were each independently proven edgeless.

## 3. Why previous optimization failed

No tuned config ever existed; the pooled campaigns rejected it at the population level.
This campaign's baseline quantifies why: TRAIN PF 0.29-0.42 at every exit shape
(tight/mid/wide), TEST 0.27-0.31 — a cost-dominated churn hose in both regimes, with VAL
(Apr-May) consistently weaker than FIT (Mar-Apr) in all 13 redesign versions, i.e. whatever
residual momentum edge existed decays toward the present.

## 4. Redesign axes tested

Freshness (first fire), fresh+morning, RS-leader percentile, not-overextended
(vwap_dist_atr cap), volume conviction band, candle quality (body/wick), bull-tape mask,
top-N ranking, late-drift window, broad ADX-pause premom gate, fresh+volume — plus
one-knob sweeps over every live mask feature both directions, 12 premom knobs, 35 exit
combos, 9 guards, then 400 Optuna TPE combination trials and a 230-combo rescue round.

## 5. Pool / backtest integrity

Same honest engine as the other campaigns (1-min fills/exits, statutory costs, 15 bps).
Raw-pool indicator columns are empty at source (documented); premom features verified
present. No leakage: quantile grids from FIT only; TEST untouched (and in the end never
earned an evaluation).

## 6. Verdict before looping (confirmed after)

A breakout detector that fires hours late, re-fires indefinitely, and pays 5-min-next-open
costs has to overcome ~0.3 PF of structural drag before any filter adds value. The loop
confirmed no filter stack closes that gap: best round-2 TRAIN 21/PF 1.17/+Rs2k. REJECT.
