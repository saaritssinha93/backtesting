# E_VWAP_LOSE_EARLY_SHORT Best Config Decision

## 2026-06-29 Six-Week Rerun Decision

Decision: **REJECTED - no accepted config**.

Keep the existing survival demotion. Do not promote `E_VWAP_LOSE_EARLY_SHORT` back into `FINAL_SETUP_CONF`.

Pinned split:

| Period | Dates | Raw setup rows |
|---|---|---:|
| TRAIN | 2026-04-27..2026-06-05 | 265 |
| TEST | 2026-06-08..2026-06-12 | 60 |

Current / old config evaluated:

| Field | Old/current value |
|---|---|
| Status | Parked by `_LIVE_SURVIVAL_DEMOTION_2026_06_29` |
| Mask | `vol_ratio >= 1.8` and `vol_ratio <= 3.2` |
| Pre-momentum | none |
| Guard | `min_slot=09:45` |
| Exit | SL 0.70 / Target 1.00 |

New config:

| Field | New value |
|---|---|
| Status | Keep parked / research-watch only |
| Mask | unchanged in parked research record |
| Pre-momentum | unchanged |
| Guard | unchanged |
| Exit | unchanged |
| Files changed in production config | none |

Best observed six-week candidates:

| Candidate | TRAIN n/PF/net | TEST n/PF/net | Decision |
|---|---:|---:|---|
| Current documented conf | 54 / 0.362 / -Rs20,407 | 10 / 0.643 / -Rs1,698 | Reject |
| Hand-loop least-bad TEST: `sig5_adx_calc >= 20` | 43 / 0.424 / -Rs13,609 | 8 / 1.054 / +Rs157 | Reject; TRAIN failed, TEST < 1.3 and thin |
| Hand-loop broad volume: `vol_ratio 1.8..4.0` | 69 / 0.376 / -Rs25,602 | 11 / 0.804 / -Rs932 | Reject; still losing |
| Official max-PF overfit: `close_loc >= 0.2709` + `quality_score >= 80.4218`, SL 0.85 / Tgt 0.80 | 25 / 1.876 / +Rs4,856 | 11 / 0.129 / -Rs7,564 | Reject; catastrophic TEST collapse |
| Official band objective | 0 / no edge | 0 / no edge | Reject |

Rationale:

- Baseline TRAIN and every hand-loop TRAIN variant with enough trades stayed below PF 1.0.
- The only TRAIN PF > 1.2 came from official max-PF optimization and collapsed immediately on TEST.
- TEST was not rescued by volume, VWAP, candle, time, trend, ADX, quality, volatility, or exit changes.
- Live-paper evidence remains contradictory: 31 live-paper trades from 2026-06-16 to 2026-06-29 had net -Rs790 and PF 0.79; post-2026-06-22 had PF 0.37.

Result: no robust improvement found. Honest action is to reject and keep demoted.

---

## Prior Thin 2wk/1wk Decision

Decision: **REJECTED - no accepted config**.

Keep the existing survival demotion. Do not promote `E_VWAP_LOSE_EARLY_SHORT` back into `FINAL_SETUP_CONF` from this audit.

## Current / Old Config Evaluated

| Field | Old/current value |
|---|---|
| Status | Parked by `_LIVE_SURVIVAL_DEMOTION_2026_06_29` |
| Mask | `vol_ratio >= 1.8` and `vol_ratio <= 3.2` |
| Pre-momentum | none |
| Guard | `min_slot=09:45` |
| Exit | SL 0.70 / Target 1.00 |

## New Config

No production config change.

| Field | New value |
|---|---|
| Status | Keep parked / research-watch only |
| Mask | unchanged in parked research record |
| Pre-momentum | unchanged |
| Guard | unchanged |
| Exit | unchanged |
| Files changed in production config | none |

## Best Observed Candidates

| Candidate | TRAIN n/PF/net | TEST n/PF/net | Decision |
|---|---:|---:|---|
| Current documented conf | 19 / 0.353 / -Rs7,273 | 10 / 0.643 / -Rs1,698 | Reject |
| Hand loop least-bad TEST: `vol_ratio 1.8..4.0` | 25 / 0.454 / -Rs7,531 | 11 / 0.804 / -Rs932 | Reject; still losing and TRAIN failed |
| Official tuner TRAIN overfit: `rs_pct <= -0.8958`, SL 0.70 / Tgt 0.80 | 16 / 2.477 / +Rs3,790 | 10 / 0.068 / -Rs7,787 | Reject; catastrophic TEST collapse |

## Rationale

The setup fails the acceptance rules:

- TRAIN baseline and all hand-loop candidates stayed below PF 1.0.
- The official tuner found TRAIN PF above 1.2 only by overfitting relative weakness; TEST PF collapsed to 0.068.
- TEST behavior contradicts the old "strongest edge" label.
- Existing live-paper evidence already contradicts promotion: 31 live-paper trades from 2026-06-16 to 2026-06-29 had net -Rs790 and PF 0.79; post-2026-06-22 had PF 0.37.

Result: no robust improvement found, so the honest action is to reject and keep demoted.
