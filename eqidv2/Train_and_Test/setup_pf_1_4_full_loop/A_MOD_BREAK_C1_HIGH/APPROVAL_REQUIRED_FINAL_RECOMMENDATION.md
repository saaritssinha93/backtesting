# A_MOD_BREAK_C1_HIGH — Final Recommendation

_Generated 2026-07-03; updated same day after the user-directed CAMPAIGN 2 (enriched feature space)._

## Verdict: **NO APPROVABLE CANDIDATE — recommend DO NOT TRADE and REMOVE from the live overlay**

### Campaign 2 update (exhaustive indicator/premom/structure expansion — verdict UNCHANGED)

Per user direction, the pool was re-enriched with 40 true features recomputed from the raw parquet
(every indicator, engineered pre-momentum, and structural context) and the entire search stack was
re-run: 284 sweeps, 1,258 Optuna trials on three pool variants, 82 staged combos, 811-config strict
rescores. **Zero configs reached the TRAIN band honestly; zero passed TEST.**

Two genuinely valuable structural findings surfaced (both explainable, both insufficient):

1. **First-signal-per-ticker-per-day dedupe doubles book PF** (0.28 → 0.56 FIT / 0.48 VAL).
2. **Requiring the break to be a genuine 20-bar high** is the only term that holds VAL above FIT
   (0.517 → 0.550, n=819).

Best honest expression — "first genuine 20-bar-high moderate-impulse break of the day, SL 1.2 /
Tgt 1.5" — reaches **PF ≈ 0.55 on both FIT and VAL**: ~2.5× better than the raw detector and ~2×
better than today's production gate, but still a consistent loser, 2.4× below the 1.30 floor.
The Optuna bests on that pool converge to the *unmasked* base (PF 0.53–0.54), i.e. no mask,
pre-momentum term, guard, or exit reshapes this book into a winner. The failure is the entry
expression itself (buying a 5-min prior-high break pays a spread+slippage toll the follow-through
cannot cover), not the filtering around it.

## Best candidate

None passed. The single best TRAIN-band config (vol_ratio≥3.28, 11:00–12:30 window, top-2,
SL 1.5/Tgt 2.0: TRAIN PF 1.66 on 23 trades) fails on **every** out-of-sample and robustness axis:
TEST PF 0.277 / net −Rs15,311, neighborhood fail, dropout fail, target-fill fail.
It is NOT proposed for approval. There is no config block to move.

## Evidence summary

| checkpoint | result |
|---|---|
| pool recreated | 26,277 rows / 74 sessions / ~1,280 symbols (Mar-04..Jul-01) |
| baseline raw | TRAIN PF 0.224 (3,538 tr) / TEST PF 0.176 (1,395 tr) |
| baseline live config | TRAIN PF 0.315 (67) / TEST PF 0.216 (38) — the running production gate loses money |
| iterations | ~1,340 configs: 119 sweeps + 25 full-pool trials + 1,000 morning trials + 193 combos + 606 strict rescores |
| TRAIN-band configs found | 4 (one family) |
| of those, TEST PF > 1.40 | **0** |
| honest (non-mined) ceiling | PF ≈ 0.5–0.9 (still a loser) |

## Why this is a structural reject, not a tuning failure

1. The raw expression (moderate-impulse prior-high break above VWAP) has PF ~0.2 across 74
   sessions and every feature quintile — there is no hidden good region for masks to find.
2. Loss anatomy is mechanical and cost-dominated: 69% SL-rate, avg loss > avg win at every exit
   combination tried (7×7 grid); best exit only reaches PF 0.29.
3. The only VAL-stable knob (morning ≤11:05) triples PF to ~0.35 — still 4× below the floor.
4. Search coverage was adequate: 1,000 Optuna trials fully explored the reduced pool; the only
   in-band pockets are 2-slot × 1-threshold mines that die OOS — the DOC5B failure signature.

## Recommended actions (require your approval — DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES)

1. **Nothing to add** to `final_setup_conf.py` / `Train_and_Test/final_setup_conf.py` — unchanged, verified.
2. **Live-risk flag:** `A_MOD_BREAK_C1_HIGH` is an *overlay-only* setup (§5.3 of
   `SETUP_CARDS_AND_LIVE_CROSSCHECK.md`) currently tradeable via the v11 live overlay with the
   gate this study measured at **TEST PF 0.216 / −Rs21.7k**. Recommend removing it from
   `eqidv2_v11_live_overlay.py` / `avwap_5min_ID_v11_backtesting.py` overlay universe (files NOT touched).
3. Revisit only with a structurally different detector (e.g. confirmed retest-hold instead of
   break-chase, or genuine relative-strength leadership context) — not with tighter thresholds.

## Remaining risks / caveats

- 05-28 and 06-26 sessions unrecoverable (raw-store holes) — 2 of 74 sessions missing; immaterial to a PF-0.2 verdict.
- Harness lacks trailing/break-even/time exits; a fundamentally different exit engine is untested (and untestable here).
- Pre-momentum terms were searchable in the engine path but featured in no surviving config.

## Rerun commands

```powershell
# pool recreation
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_HIGH\scripts\build_pool_a_mod_c1_high.py --tail_dir Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_HIGH\pools --out Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_HIGH\pools\pool_full
# baseline
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_HIGH\scripts\eval_baseline.py --pool Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_HIGH\pools\pool_full --tag baseline_final
# best (rejected) candidate re-check, morning pool
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup A_MOD_BREAK_C1_HIGH --pool Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_HIGH\pools\pool_morning --trials 500 --time_budget_min 12 --seed 23 --train_start 2026-03-01 --test_start 2026-06-01 --test_pf_min 1.4 --max_mask_terms 2 --max_pm_terms 2 --out Train_and_Test\setup_pf_1_4_full_loop\A_MOD_BREAK_C1_HIGH\deep_runs\morning_seed23
```
