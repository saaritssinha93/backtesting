# DOC5C_ORB_GAP_GO_LONG (LONG) — PARAMETER_SWEEP_SUMMARY

_Generated 2026-07-01. Research-only. NO `final_setup_conf.py` edits, NO live trades._

Two independent searches were run through the **same repo pipeline**
(`setup_train_test.eval_family` → guards → pre-momentum → dedupe → mask → portfolio
overlay → resolve; statutory NSE intraday cost + 15 bps/leg slippage; next-1-min-open fill):

1. **Canonical engine** `_engine/pf_band_fitval_loop.py` — Optuna TPE, 300 FIT/VAL trials,
   standard mask/pre-momentum feature list, ≤2 mask + ≤1 pre-mom + exits + guards.
2. **Custom gap-knob staged sweep** `scripts/gap_knob_sweep.py` — adds the Setup-C
   structural columns the engine's `MASK_FEATS` omits: **`gap_pct`, `orh_dist_atr`
   (extension-from-ORH), `vwap_slope_atr`** — these are the doc's own gap-and-go levers
   (`gap_min/gap_max`, `ext_max`, `slope_min`).

## Split (task-mandated, printed before running)

| window | dates | sessions | attached entries |
|---|---|---|---|
| FIT | 2026-05-18 .. 2026-05-29 | 7 | 49 |
| VAL | 2026-06-02 .. 2026-06-18 | 8 | 54 |
| **TRAIN** | 2026-05-18 .. 2026-06-18 | 15 | 103 |
| **TEST** | 2026-06-22 .. 2026-06-30 | 4 (06-22,23,25,30) | 23 |

Search is done ONLY on FIT/VAL; a candidate is confirmed on full TRAIN, and TEST is
scored ONCE — but **only if TRAIN PF lands in [1.30, 1.70]**. No config ever qualified,
so TEST was correctly never tuned to.

---

## Stage 1 — RAW baseline (no mask / no pre-mom / no guard)

Doc-suggested exit SL 0.85 / Tgt 1.50:

| window | n | net PF | net Rs | win% | tgt-fill% | SL/TGT/EOD |
|---|---:|---:|---:|---:|---:|---:|
| FIT | 47 | **0.103** | −36,134 | 12.8 | 4.3 | 35/2/10 |
| VAL | 54 | **0.305** | −26,526 | 25.9 | 13.0 | 34/7/13 |
| TRAIN | 101 | **0.202** | −62,660 | 19.8 | 8.9 | 69/9/23 |
| TEST | 23 | **0.140** | −16,558 | 17.4 | 8.7 | 17/2/4 |

Raw DOC5C is a **catastrophic loser in every window**: ~20% win rate, ~9% target-fill,
dominated by SL and EOD exits. The full **7×7 exit grid (49 combos)** was swept — TRAIN PF
**never exceeds ≈0.25** (best SL 1.20 / Tgt 2.00 → TRAIN PF 0.253; FIT PF tops out ≈0.155).
Reference exit chosen for the knob sweeps: **SL 1.20 / Tgt 2.00** (highest min(FIT,VAL) PF = 0.153).

**Diagnosis:** this is exactly the failure the source doc predicts — gap-and-go is *"hit
hardest by 5-min-only"* because the earliest fill is the next-bar open, i.e. you buy ~5 min
into the gap breakout (09:45–11:00 window), deep into the move, and it mean-reverts.

---

## Stage 2 — Individual knob sweep (one mask column at a time, ref exit 1.20/2.00, FIT/VAL)

Every important indicator, non-indicator, and pre-mom knob was swept across a realistic
range. `min(FIT_PF, VAL_PF)` never reached the 1.15 neighborhood floor — **not once**.
FIT and VAL PF move in **opposite directions** under almost every filter (the signature of
noise, not a stable edge).

| knob (feat, op) | range tested | best FIT PF | best VAL PF | best stable min(FIT,VAL) | note |
|---|---|---:|---:|---:|---|
| **gap_pct ≥** | 0.5–1.5 | 0.153@0.5 | 0.478@0.7 | ~0.05 | tightening gap kills FIT (n→2–8) |
| **gap_pct ≤** (controlled gap) | 3.0→1.0 | 0.205@1.0 | 0.368@1.5 | ~0.20 | best-ish but still a deep loser |
| **orh_dist_atr ≤** (ext_max) | 3.0→0.6 | 0.161@3.0 | 0.497@1.0(n4) | ~0.04 | not-chasing does not rescue it |
| **orh_dist_atr ≥** | 0.0–1.0 | 0.185@1.0 | 0.360@0.6 | ~0.18 | flat, no signal |
| **vwap_slope_atr ≥** (slope_min) | 0.1–0.9 | 0.167@0.2 | 0.428@0.5 | ~0.16 | rising-VWAP filter: no edge |
| **vol_ratio ≥** | 1.5–3.5 | 0.153@1.5 | 0.637@3.5(n7) | ~0.13 | VAL spikes as FIT dies = noise |
| **vol_ratio ≤** | 5.0→2.5 | 0.204@2.5 | 0.335@5.0 | ~0.20 | — |
| **close_loc ≥** | 0.6–0.9 | 0.177@0.8 | 0.542@0.9 | ~0.17 | — |
| **rs_pct ≥** | 0.5–2.0 | 0.288@1.5 | 0.330@0.5 | ~0.25 | strongest single FIT lever, still <0.3 |
| **body_pct ≥** | 0.4–0.8 | **0.409@0.8** | 0.313@0.8 | ~0.31 | best single-knob FIT; n→17, VAL flat |
| **atr_pct ≤ / ≥** | 0.004–0.008 | 0.218@≥0.003 | 0.450@≤0.004 | ~0.16 | — |
| **quality_score ≥** | 70–120 | 0.283@100 | 0.365@85 | ~0.20 | — |
| **vwap_dist_atr ≤** | 4.0–1.5 | 0.169@4.0 | 0.375@4.0 | ~0.06 | tighter = worse |
| **market_ret_pct ≥** (tailwind) | −0.15..0.2 | 0.170@−0.15 | 0.381@−0.15 | ~0.17 | up-market gate doesn't help |

### Stage 2b — pre-momentum single-term sweep

| gate | range | best FIT | best VAL | note |
|---|---|---:|---:|---|
| pre3_range_r ≥ | 0.2–0.5 | 0.291@0.3 | **1.032@0.5 (n6)** | VAL "edge" is 6 trades — a fluke; FIT 0.056 there |
| pre5_mom_r ≥ | 0.0–0.6 | 0.428@0.2 | 0.547@0.2 | best *joint* single gate; both still <0.6, n=11/19 |
| sig5_adx_calc ≥ | 15–30 | 0.153@15 | 0.368@20 | trend-strength filter: no edge |
| pre_entry_momentum_score ≥ | 50–85 | 0.179@50 | 0.551@65(n7) | collapses to n≤2 when tightened |

**Best-stable ranges (evidence): none.** No indicator, non-indicator, filter, guard, or
pre-mom threshold produced FIT and VAL PF both ≥1.15. The `body_pct≥0.8` (FIT 0.41) and
`pre5_mom_r≥0.2` (FIT 0.43 / VAL 0.55) pockets are the closest, and they are still deep
losers (PF « 1.0). **Overfit-risk values flagged & rejected:** `pre3_range_r≥0.5`,
`vol_ratio≥3.5`, `pre_entry_momentum_score≥65` — all show a high VAL PF on ≤7 trades while
FIT dies, i.e. small-sample noise, not signal.

---

## Stage 3 — Best train-side combinations (≤2 mask + ≤1 pre-mom + guard + exit, FIT/VAL)

**2,185** combinations with ≥5 trades in both FIT and VAL were evaluated. Ranked by
worse-half PF `min(FIT_PF, VAL_PF)`:

| rank | exit | mask | pre-mom | FIT n/PF | VAL n/PF | min-half PF |
|---|---|---|---|---|---|---:|
| 1 | 1.00/1.50 | vwap_slope_atr≥0.5 ; vol_ratio≥2.5 | pre3_range_r≥0.3 | 5/0.512 | 11/0.529 | **0.512** |
| 2 | 1.20/2.00 | vwap_slope_atr≥0.5 ; vol_ratio≥2.5 | pre3_range_r≥0.3 | 5/0.503 | 10/0.768 | 0.503 |
| 3 | 1.20/2.00 | vwap_slope_atr≥0.5 ; vol_ratio≥2.0 | pre3_range_r≥0.3 | 7/0.478 | 13/0.473 | 0.473 |

The single best combination in the entire search reaches **worse-half PF 0.51** — barely a
third of the 1.30 lower band, and on 5 FIT trades. No combination of the gap-and-go levers
(`gap_pct`, `orh_dist_atr`, `vwap_slope_atr`) with volume/momentum gates and any exit
bracket crosses even 0.55.

---

## Stage 4/5 — Full-TRAIN confirmation (top-12 FIT/VAL combos)

Every promising FIT/VAL combo was confirmed on the full 15-session TRAIN window:

| mask | pre-mom | exit | TRAIN n | TRAIN PF | TRAIN net Rs | in band? |
|---|---|---|---:|---:|---:|---|
| vwap_slope_atr≥0.5 ; vol_ratio≥2.5 | pre3_range_r≥0.3 | 1.20/2.00 | 15 | **0.683** | −3,188 | ✗ |
| vwap_slope_atr≥0.5 ; vol_ratio≥2.5 | pre3_range_r≥0.3 | 1.00/1.50 | 16 | 0.524 | −5,026 | ✗ |
| gap_pct≥0.7 ; vwap_slope_atr≥0.5 | pre3_range_r≥0.3 | 1.20/2.00 | 17 | 0.497 | −6,877 | ✗ |
| gap_pct≤1.5 ; vwap_slope_atr≥0.5 | pre3_range_r≥0.3 | 1.20/2.00 | 25 | 0.443 | −10,339 | ✗ |
| vwap_slope_atr≥0.5 ; vol_ratio≥2.0 | pre3_range_r≥0.3 | 1.20/2.00 | 20 | 0.474 | −8,277 | ✗ |
| … (all other top-12) | … | … | 15–28 | 0.32–0.68 | −3k to −14k | ✗ |

**Best full-TRAIN PF achieved anywhere = 0.683** (n=15, net −Rs 3,188). Nothing reaches the
1.30 lower band, so **no candidate was eligible for TEST** and none was tuned to TEST
(anti-overfit rule respected). The canonical engine agrees: its best FIT/VAL config
(SL 0.5 / Tgt 2.5, `pre1_adx≤34.1`, top_n 1) confirmed at **TRAIN PF 0.614 / TEST PF 0.245 (n=2)**.

---

## Bottom line

| knob group | best result | verdict |
|---|---|---|
| exit SL/Tgt (49 combos) | TRAIN PF 0.25 | dead |
| single indicator/non-indicator mask | FIT PF 0.41 (body_pct≥0.8) | dead |
| single pre-momentum gate | FIT 0.43 / VAL 0.55 (pre5_mom_r≥0.2) | dead |
| gap-and-go levers (gap_pct / orh_dist_atr / vwap_slope_atr) | min-half 0.51 | dead |
| best confirmed full-TRAIN combo | PF 0.683, net −Rs 3,188 | dead |

**No band-eligible candidate exists.** DOC5C_ORB_GAP_GO_LONG carries **no long edge** on the
mandated 2026-05-18 → 2026-06-30 split, under every one of the knobs in the task
specification. Approval recommendation: **NO**. Keep as a research artifact; do not promote.

> **DO NOT MOVE ANYTHING TO FINAL CONFIG UNTIL USER APPROVES.**
