# DOC5D_AVWAP_RECLAIM_LONG — REINVENTION_RESULTS (goal evaluated @ 5 bps/leg)

_Generated 2026-07-01. Research-only. NO `final_setup_conf.py` edits. NO live trades._

Follows the user directive: *"improve and reinvent this setup, rules, conf, style, until it
achieves the goal in 5 bps."* The 15-bps track (BASELINE_RESULT / PARAMETER_SWEEP_SUMMARY /
ITERATION_LOG) rejected the setup on the existing pool. This track **redesigns the detector** and
re-evaluates the whole gate at **5 bps/leg**.

## What was reinvented

The doc's raw Setup-D fires on the *first* up-bar back through VWAP → a 21%-win, 65%-stop loser
(see BASELINE_RESULT.md). The redesign keeps the idea (reclaim session VWAP from below = catch the
turn) but demands the reclaim be **confirmed** and backed by **momentum + leadership + a supportive
regime**. Built as a standalone multi-variant scan over the **full ~1,287-name parquet universe**
(not just the 204 F&O), `scripts/reinvent_doc5d_scan.py`, emitting four graded detectors:

| Variant | Added confirmation over raw reclaim |
|---|---|
| **vA** | held reclaim (`low ≥ VWAP−0.45·ATR`), `close>EMA20`, `close_loc≥0.58`, `vol_ratio≥1.35`, `rs_pct>0.05`, `regime≠BEAR`, non-climax |
| **vB** | vA + `close_loc≥0.62`, `body_pct≥0.35`, near-value `vwap_dist≤1.2`, rising VWAP slope `≥0` |
| **vC** | vB + `vol_ratio≥1.5`, prior-bar-high thrust (or clear reclaim ≥0.05·ATR), EMA20≥0.995·EMA50, non-climax≤2.0·ATR |
| **vD** | vC + BULL/TREND regime lean, `rs_pct>0.20`, `50≤RSI≤72`, `vwap_dist≤0.85` |

**Effect on base quality (@5 bps, raw detection, one-per-day dedupe):** win-rate lifted from the
raw **21% → 37–47%** — the confirmation genuinely helps — but Profit Factor stays a **loser (~0.6–0.9)**.

| Variant | TRAIN n / PF / win% | TEST n / PF / win% |
|---|---|---|
| vA | 344 / 0.73 / 40% | 100 / 0.60 / 33% |
| vB | 144 / 0.75 / 40% | 36 / 0.69–0.76 / 33% |
| vC | 104 / 0.67 / 39% | 23 / 0.30 / 13% |
| vD | 22 / 0.68 / 36% | 13 / 0.56–0.63 / 31% |

## PF-band loop @ 5 bps on the reinvented pools (Optuna TPE, mask/premom/guard/exit search)

Split (unchanged): TRAIN 2026-05-18…06-19, TEST 2026-06-20…06-30. Gate: TRAIN PF∈[1.30,1.70],
TEST PF>1.40, meaningful & non-concentrated, robust. Studies (seeds × term budgets):

| Study | best cfg (@5 bps) | TRAIN n / PF | TEST n / PF | Verdict — why |
|---|---|---|---:|---:|---|
| vA s7 (2m/1p) | mask `ranker_score≤64.44`, SL1.1/T2.5, min_slot10:30, top_n2 | 38 / **1.23** | 24 / 0.73 | REJECT — TRAIN just below band, TEST loser |
| vA s11 (1m/1p) | none, SL0.7/T1.25, max_slot12:00, top_n3 | 237 / 0.76 | 66 / 0.51 | REJECT — loser both |
| **vA s23 (1m/1p)** | premom `sig5_adx_calc≤20.87`, SL1.0/T2.5, min_slot11:00, top_n3 | 95 / 0.99 | 30 / **1.45** | REJECT — **TEST passes but TRAIN is a loser** |
| vB s7 (2m/1p) | premom `sig5_adx_calc≤15.94`, SL0.85/T2.0, 10:30–14:00, top_n3 | 20 / 0.92 | 6 / 1.83 | REJECT — TEST = one trade (63% of gross) |
| vB s11 (1m/1p) | mask `wick_skew≥0.045` + premom `pre1_adx≤18.77`, SL0.7/T2.0 | 20 / 1.69 | 2 / 0.00 | REJECT — overfit, thin TEST |
| **vB s23 (1m/1p)** | mask `vwap_dist_atr≥1.028`, SL0.7/T2.5, min_slot10:00, top_n2 | 29 / **1.33** | 9 / 0.54 | REJECT — **TRAIN in-band but TEST collapses** |
| vC s7 (2m/1p) | premom `pre1_adx≤20.16`, SL1.0/T1.5, top_n1 | 38 / 1.00 | 5 / 0.00 | REJECT — TEST too few / 0 |

## The wall: TRAIN and TEST are anti-correlated for this long archetype

- Every config that pushes **TRAIN into [1.30,1.70]** (vB s23 1.33, vB s11 1.69, vA s7 1.23) **loses
  on TEST** (0.54, 0.00, 0.73).
- The single config that clears **TEST > 1.40** (vA s23, 1.45 on n=30) is a **TRAIN loser** (0.99).
- No config satisfies **both** gates with meaningful, non-concentrated trades. The near-misses fail
  the *opposite* gate — i.e. what pays in TRAIN (05-18…06-19) is punished in TEST (06-20…06-30).
- This is a **regime shift**, not a tunable edge. It is not a cost artifact — the whole gate here is
  already at **5 bps** (the lenient regime the directive asked for). Picking the seed that happens to
  fit TEST (vA s23) would be TEST-tuning — explicitly disallowed by the anti-overfit rule and by the
  "not dominated by one day/symbol / simple, robust" acceptance criteria.

## Verdict

**NO candidate achieves the goal, even reinvented, even at 5 bps.** The AVWAP-reclaim LONG has no
robust positive expectancy on 2026-05-18…06-30: the confirmation redesign raised win-rate (21%→~45%)
but not PF, and the OOS June window is hostile to long reclaims. Consistent with prior evidence
(wider-window doc5 DOC5D REJECT; memory `project_5min_long_setups_doc_2026_07_01` — 0/4 doc archetypes
carry June OOS edge; the working live book is shorts).

Best-found near-misses recorded (flagged, **NOT passing, NOT for promotion**) in
`candidates/` as `*_NEAR_MISS_*.json`. Per-study artifacts under `_sweeps/reinvent/`.

> **DO NOT MOVE ANYTHING TO FINAL CONFIG UNTIL USER APPROVES.**
