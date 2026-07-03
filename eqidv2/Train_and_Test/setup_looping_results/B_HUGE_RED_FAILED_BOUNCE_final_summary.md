# B_HUGE_RED_FAILED_BOUNCE - Final Summary

## Rolling 2-Week/1-Week Audit Update - 2026-06-29

Final rolling verdict: **REJECTED / no config change**.

Pinned split:

| Period | Dates |
|---|---|
| TRAIN | 2026-05-25..2026-06-05 |
| TEST | 2026-06-08..2026-06-12 |

| Config | TRAIN n/PF/net | TEST n/PF/net | Verdict |
|---|---:|---:|---|
| Baseline current conf @15 bps | 10 / 0.647 / -Rs1,246 | 5 / 0.873 / -Rs295 | Reject |
| Current conf @5 bps | 10 / 1.747 / +Rs1,749 | 5 / 1.294 / +Rs525 | Watch only; too thin and below 1.3 |
| Best hand-loop TRAIN candidate | 7 / 1.896 / +Rs1,078 | 2 / 0.000 / -Rs1,193 | Reject TEST collapse |
| Best hand-loop TEST-looking candidate | 6 / 1.055 / +Rs108 | 3 / 18.301 / +Rs1,919 | Reject tiny/lucky TEST and TRAIN PF failed |
| Official maxpf tuner | 15 / 3.494 / +Rs5,352 | 3 / 0.447 / -Rs1,190 | Reject overfit |
| Official band tuner | 27 / 1.440 / +Rs2,932 | 9 / 0.140 / -Rs6,919 | Reject TEST collapse |

Backtest/live logic check for B remains: **no overlay mismatch for this setup under final-conf mode**. B is readmitted through the final-conf/bootstrap path, and `eqidv2_final_conf_live_bootstrap.py` installs the conf pre-momentum terms and exit levels when the flag is active.

Action from this rolling pass:

- No `final_setup_conf.py` or `Train_and_Test/final_setup_conf.py` change.
- No live trades placed.
- Added rolling runner and metrics: `run_B_HUGE_RED_FAILED_BOUNCE_loop.py`, `B_HUGE_RED_FAILED_BOUNCE_rolling_loop_metrics.csv`, `B_HUGE_RED_FAILED_BOUNCE_rolling_loop_details.json`, `B_HUGE_RED_FAILED_BOUNCE_rolling_slippage_check.json`.
- Keep B as **not accepted for sizing**. If left active by user choice, it should be paper-watch only with measured fill slippage.

---

## Prior Active-Book Final Summary

**Verdict: REJECT for sizing / keep unsized (parked).** Marginal and slippage-fragile, not robust at realistic
execution cost; the conf's published edge is not reproducible on the current live-faithful pool.

## What was done
- Verified **backtest == live** logic (bootstrap-only; faithful port of conf gate; no overlay contradiction — doc §5.4). ✔
- Baseline on the fresh rolling window; 12 hand iterations (exit grid, time guards, volume/quality masks,
  momentum tightening, ADX gate); 2 automated tuner searches (band → DROP_NO_EDGE, maxpf → TRAIN 2.31 but
  TEST p=0.52 FDR-dropped); sample-sensitivity (longer train) and slippage sensitivity (5 vs 15 bps/leg);
  original-conf-window reproduction. ≈18 distinct evaluations.

## Key numbers (conf gate)

| Window | Slippage | TRAIN PF (n) | TEST PF (n) |
|---|---|---:|---:|
| Fresh 04-13..05-25 / 05-26..06-24 | 15 bps/leg | 0.79 (28) | 0.52 (16) |
| Fresh | 5 bps/leg | 1.63 (28) | 1.10 (16) |
| Original 11-01..04-30 / 05-01..06-10 | 15 bps/leg | 0.55 (73) | 1.17 (21) |
| Original | 5 bps/leg | 1.28 (75) | 2.39 (21) |
| Feb–May train | 15 bps/leg | 0.78 (48) | 0.52 (16) |

Conf published claim: TRAIN 2.90 (n30) / TEST 3.49 (n20) — **not reproduced** at any slippage on this pool.

## Why REJECT (against the acceptance bar)
- **TEST PF ≥ 1.3:** fails at both slippages on the fresh window (0.52 @15, 1.10 @5).
- **Robust / not one-day-dominated:** fresh TEST @5 bps has one day = 362% of net → fails.
- **Realistic cost:** loser at 15 bps/leg; live paper PF was 0.25 → realistic fills are on the loser side.
- **Reproducibility:** published edge not reproducible on the unified-pool readmit basis (basis discrepancy).
- **Recent regime:** degradation concentrated in mid–late June.

## Caveats / honest uncertainties
- At **paper slippage (5 bps/leg)** the gate is genuinely marginally positive (TRAIN 1.63), and the original
  window TEST @5 bps is strong (2.39, significant). So this is **not "never worked"** — it worked through early
  June at paper costs. It is "marginal, slippage-fragile, recently-degraded, and unreproducible on the current
  basis." If the live executor could guarantee sub-8 bps/leg fills on small-cap shorts (unlikely), it could be
  reconsidered.
- The **basis discrepancy** (readmit n=75 vs conf n=30) should be investigated separately: the unified pool's
  "readmit = faithful" claim does not hold for B_HUGE_RED — its candidate population is ~2.5× the clean-pool mine.

## Action
- **No config change** (none would pass `--approve`; flagged for user review).
- Recommend B_HUGE_RED be **parked/demoted** in `FINAL_SETUP_CONF` along with the others (it fails its own
  re-promotion trigger), pending (a) a fix to the readmit basis discrepancy and (b) a realistic-slippage
  re-validation.
- **Methodology takeaway for the rest of the book:** evaluate every tight-target scalp at BOTH 5 and 15 bps/leg —
  slippage is decisive.
