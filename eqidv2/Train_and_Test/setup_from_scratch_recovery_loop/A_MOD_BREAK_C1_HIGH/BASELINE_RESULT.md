# A_MOD_BREAK_C1_HIGH — Baseline (recovery loop)

_Generated 2026-07-03. Windows: TRAIN 2026-03-04..05-29 (52 sessions; FIT 31 / VAL 21),
TEST 2026-06-01..07-01 (22 sessions)._

## Original rules (config of record)

Detector: moderate-impulse (0.60-2.20 ATR) break of prior 5-min bar high, above VWAP,
green/strong close, rs_pct>0.05, vol_ratio≥1.5, regime≠BEAR (v2 `_scan_day`).
Production gate: rs_pct≥2.0 & atr_pct≤0.006 & signal ≤11:10 + top-2/slot; SL 0.70% / Tgt 1.00%;
no pre-momentum; EOD 15:20.

## Canonical harness numbers (campaign 1, `setup_train_test` + engine)

| config | TRAIN | TEST |
|---|---|---|
| raw detector | n=3,538 PF 0.224, −Rs18.4L | n=1,395 PF 0.176 |
| production config | n=67 PF 0.315, −Rs30k | n=38 PF 0.216 |

## Path-engine equivalents (this loop; per-trade validated 400/400 vs canonical)

| config | FIT | VAL | TRAIN | TEST |
|---|---|---|---|---|
| raw mirror (0.70/1.00, uncapped chronological book) | n=1,893 PF 0.414 | n=1,425 PF 0.330 | n=3,244 PF 0.391 | n=1,227 PF 0.313 |

(Book-level PF differs from the canonical harness because tt's 20-slot booking selects a
different — worse — subset under signal flood; per-trade prices/outcomes are identical.
All redesign comparisons in this loop are engine-internal, so consistent.)

## Diagnosis

Same as `FROM_SCRATCH_LOGIC_REVIEW.md` §3-§7: chase entry + noise-level SL + cost toll.
The redesign blocks attack entry timing, exit geometry, and loss clustering.
