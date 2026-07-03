# DOC5D_AVWAP_RECLAIM_LONG (LONG) — FAILURE_ANALYSIS

_Generated 2026-07-01. Research-only._

Analysis uses the **raw-detection book** (full 1-min OHLC exit detail, SL 0.70 / Tgt 1.25,
one-per-day dedupe) — richer than the best-config fast-mode book, which was empty because the
best config sat below the robust floors.

## Exit / SL / target behaviour (the core problem)

| Window | n | PF | win% | SL | EOD | TARGET | avg win | avg loss | bars held |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| TRAIN | 66 | 0.158 | 21.2% | **43** | 19 | 4 | Rs481 | −Rs820 | ~118 |
| TEST  | 19 | 0.339 | 21.1% | **11** | 4 | 4 | Rs1,005 | −Rs789 | ~126 |

- **65% stop-out on TRAIN (43/66), 6% target-fill (4/66).** The 0.70% SL is knifed almost every
  trade; the 1.25% target almost never prints. This is the dominant failure mode.
- **avg win < avg loss** on TRAIN (Rs481 vs −Rs820) → even a coin-flip win rate would lose; at 21%
  win rate it is a rout.
- Widening the bracket (search drifted to SL 1.1 / Tgt 2.5) only converts SLs into EOD holds — the
  book stays net-negative because the underlying directional call is wrong ~4 times in 5.

## Losing-trade classification

- **Fake reclaim / immediate reversal (primary):** "prev_close ≤ prev_VWAP, close > VWAP" fires on
  the first up-bar back through VWAP; on this tape price whipsaws straight back under VWAP and hits
  the 0.70 SL. This is the textbook late-May/June "reclaim that doesn't hold."
- **Trend misalignment / weak leadership:** the loose `rs_pct > 0.15` RS floor (doc's "looser
  because you're early") lets in laggards whose reclaim has no follow-through.
- **Bad volatility regime:** long-only reclaim in a chop/down-drift June is fighting the tape;
  `regime ≠ BEAR` + `market_ret ≥ −0.35` is too permissive to filter it.

## Worst days (raw book)

- **TRAIN:** 05-19 −Rs3,817 · 05-25 −Rs3,727 · 06-10 −Rs3,705 · 06-19 −Rs3,521. Best day only
  **+Rs280** (05-26) — i.e. essentially **no green days**; the loss is broad, not one blow-up.
- **TEST:** 06-23 −Rs2,561 · 06-25 −Rs2,138 · 06-22 −Rs1,858 · 06-24 −Rs778 · 06-30 −Rs486.
  **Every one of the 5 TEST sessions is negative.** There is no clean day to gate toward.

## Worst symbols (raw book)

- TRAIN: PAGEIND −Rs1,486 · ICICIGI −Rs1,390 · ONGC −Rs1,364 · CAMS −Rs933 (spread broadly; no
  single symbol dominates the loss — a symbol filter cannot fix it).
- TEST: GMRAIRPORT −Rs933 · CROMPTON −Rs932 · SBIN −Rs931 · PFC −Rs930.

## Time-window / guard behaviour

- Trades spread 09:45–13:00; no positive sub-window emerged. `min_slot` 10:00 (skip open) and
  tighter `max_slot` did not create an edge — the failure is direction, not timing.
- `daily_loss_rs = 4000` and `top_n = 2` (favoured by the search) only cap the bleed; they cannot
  manufacture positive expectancy.

## Filter / pre-momentum weakness

- No mask (`quality_score, ranker_score, vol_ratio, atr_pct, vwap_dist_atr, rs_pct, body_pct,
  close_loc, wick_*`) and no pre-momentum term (`pre1_adx, sig5_adx_calc, sig5_rsi_dir,
  sig5_vol_ratio20, pre*_mom_r, pre3_*, pre_entry_momentum_score`) lifted **both** TRAIN halves into
  the band. The only in-band TRAIN results came from ≤16-trade, high-concentration pockets that went
  to **PF 0.00 on TEST** (see PARAMETER_SWEEP_SUMMARY.md).
- Term-dropout robustness: removing the single mask term collapses the book to the PF-0.16 base →
  the "edge" is entirely the filter cutting sample, not a real conditioning signal.

## Classified overall failure reasons

- TRAIN PF too low (< 1.30) at meaningful sample; **or** TRAIN too few trades (< 20) when forced in-band.
- TEST PF below 1.40 (in fact 0.00–0.47) — **OOS collapse**.
- TEST too few trades (< 6) for any in-band TRAIN config.
- One-day / one-symbol concentration in every in-band pocket.
- Neighborhood + term-dropout robustness both fail.
- Weak pre-momentum; fake-breakout/reversal; wrong-direction in the June regime; poor avg-win/avg-loss.

**Not a cost artifact:** at 5 bps/leg TRAIN PF is still 0.417 and TEST 0.462 — losing regardless.
