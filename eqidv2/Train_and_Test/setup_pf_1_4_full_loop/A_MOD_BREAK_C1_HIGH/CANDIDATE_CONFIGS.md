# A_MOD_BREAK_C1_HIGH — Candidate Configs

_Generated 2026-07-03. Acceptance gate: full-TRAIN PF 1.30–1.80 (n≥20) AND TEST PF > 1.40 (n≥6)
AND positive net both sides AND domination/robustness checks._

## PASSING CANDIDATES: **NONE (0)**

~1,340 configs were evaluated across 6 stages (see `ITERATION_LOG.md`). The strict TRAIN-first
audits re-scored every unique config the searches ever tried:

| audit | unique configs | in TRAIN band (n≥20) | TEST PF>1.40 & net>0 |
|---|---:|---:|---:|
| `rescore_morning/` (1,000 trials → dedup) | 581 | 4 | **0** |
| `rescore_fullpool/` (25 trials → dedup) | 25 | **0** (best TRAIN PF 0.404, n=302) | **0** |

## The 4 in-band configs — and why they are rejected

All four are one family: `vol_ratio ≥ 3.28` + entry window 11:00–12:30 (on the ≤11:05 morning pool
this means **only the 11:00 and 11:05 slots**) + top-2 per slot + SL 1.5 / Tgt 1.5–2.0.

| TRAIN n | TRAIN PF | TRAIN net | day-p | TEST n | TEST PF | TEST net |
|---:|---:|---:|---:|---:|---:|---:|
| 23 | 1.660 | +8,474 | 0.085 | 17 | 0.277 | **-15,311** |
| 20 | 1.416 | +3,939 | 0.216 | 17 | 0.261 | **-14,377** |
| 20 | 1.416 | +3,939 | 0.216 | 17 | 0.261 | -14,377 (dup guard variant) |
| 20 | 1.416 | +3,939 | 0.216 | 17 | 0.261 | -14,377 (dup guard variant) |

Reject reasons (each individually disqualifying):

1. **TEST catastrophic** — PF 0.26–0.28, net −Rs14–15k; June+ has zero carry-over of the pocket.
2. **Robustness fails** — engine neighborhood (±1 quantile) and term-dropout checks both fail on
   the family head (morning_seed23 `run_summary.json`).
3. **Sample pathology** — a 2-slot time window × one volume threshold on 20–23 trades over 3 months
   is a textbook mined pocket, structurally identical to the DOC5B retest-v3 thin-pocket rejection.
4. Target-fill rate below the 12% floor on TRAIN.

## Nearest honest (non-mined) configurations, for the record

From the 193-combo evidence grid (`stage4_combo_results.csv`) — the best PF achievable without
threshold mining, on the morning pool:

| config | FIT PF | VAL PF |
|---|---:|---:|
| vol≥3.0, SL 1.0 / Tgt 2.0 | 0.864 | 0.510 |
| vol≥3.0, SL 1.5 / Tgt 2.0 | 0.799 | 0.578 |
| vol≥2.6, top2, SL 1.0 / Tgt 1.5 | 0.837 | 0.514 |

Honest ceiling ≈ **PF 0.5–0.9** — far below breakeven, let alone the 1.30 floor.

## Campaign 2 (2026-07-03) — enriched feature space

| audit | unique configs | in TRAIN band (n≥20) | TEST PF>1.40 & net>0 |
|---|---:|---:|---:|
| `rescore_ext_am/` (1,200 trials, 60-feat space, morning-dedupe pool) | 760 | **0** | 0 |
| `rescore_ext_20bh/` (58 trials, first-20bar-high pool) | 51 | **0** | 0 |
| `stage4e_combo_results.csv` (staged stable-term combos) | 82 | 0 (none ≥ FIT 1.05) | 0 |

Best honest expression found in campaign 2 (NOT a candidate — documented for reuse):
**first-per-ticker-day dedupe + is_20bar_high + SL 1.2/Tgt 1.5** → FIT PF 0.517 / VAL PF 0.550
(n=1,201/819). Structural, explainable, ~2× better than production — still a loser.

## Candidate files

`candidates/NO_CANDIDATES.md` — no config earned a candidate JSON under the acceptance gate
(both campaigns, ~2,970 configs total).
