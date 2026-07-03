# A_MOD_BREAK_C1_HIGH — Baseline Result

_Generated 2026-07-02. Pool: `pools/pool_full` (26,277 rows / 74 sessions). 15 bps slippage, repo cost model._
_JSON: `baseline_final_result.json` (final), `baseline_result.json`, `baseline_pre_tail_result.json` (earlier pool iterations)._

## Current Rules (config of record)

| element | value | source |
|---|---|---|
| side | LONG | catalog |
| detector | moderate-impulse (0.60-2.20 ATR) break of prev-bar high, above VWAP, `long_struct`, `rs_pct>0.05`, `vol_ratio>=1.5`, regime != BEAR, qs>=7 (v2 scale) | `avwap_5min_ID_v2_backtesting.py:689` |
| production gate (2026-06-09) | `rs_pct>=2.0 & atr_pct<=0.006 & signal_minute<=670` | `avwap_5min_ID_v11_backtesting.py:158` |
| pre-momentum | none | — |
| guards | reject >= 11:10; top-2 per (day,slot) by `vwap_dist_atr` desc | `avwap_5min_ID_v11_backtesting.py:378,1332` |
| SL / target | 0.70% / 1.00% | `avwap_5min_ID_v6_backtesting.py:46` |
| exit logic | bracket SL/target, EOD flat 15:20 | v6 resolution |
| portfolio | max_positions 20 | executor |

## Windows (task split: FIT = first 60% of TRAIN sessions)

| window | sessions | dates |
|---|---|---|
| TRAIN | 52 | 2026-03-04..2026-05-29 |
| FIT | 31 | 2026-03-04..~2026-04-2x |
| VAL | 21 | ..2026-05-29 |
| TEST | 22 | 2026-06-01..2026-07-01 |

(The approval-loop engine internally uses its own 26/26 FIT/VAL convention; both splits are chronological and non-overlapping with TEST.)

## Baseline Metrics

### Raw detector (exit 0.70/1.00, no gate)

| window | trades | PF | net Rs | win% | SL/TGT/EOD | tr/day |
|---|---:|---:|---:|---:|---|---:|
| FIT | 2,013 | 0.244 | -1,008,535 | 23.5 | 1367/411/235 | 64.9 |
| VAL | 1,525 | 0.198 | -833,990 | 20.3 | 1059/253/213 | 72.6 |
| TRAIN | 3,538 | 0.224 | -1,842,525 | 22.2 | 2426/664/448 | 68.0 |
| TEST | 1,395 | 0.176 | -810,453 | 18.5 | 1007/214/174 | 63.4 |

### Current production config (gate + guards + exit as live)

| window | trades | PF | net Rs | win% | SL/TGT/EOD | tr/day |
|---|---:|---:|---:|---:|---|---:|
| FIT | 42 | 0.298 | -19,731 | 26.2 | 30/11/1 | 2.33 |
| VAL | 25 | 0.344 | -10,369 | 32.0 | 17/7/1 | 2.27 |
| TRAIN | 67 | 0.315 | -30,100 | 28.4 | 47/18/2 | 2.31 |
| TEST | 38 | 0.216 | -21,671 | 21.1 | 30/7/1 | 2.38 |

## Initial Diagnosis

1. **The setup loses money under every window and both configurations.** Raw PF 0.18-0.24; gated PF 0.22-0.34. Nothing is close to breakeven (PF 1.0), let alone the 1.30 band floor.
2. The production gate improves PF only ~0.09 while cutting 98% of trades — and still loses Rs-30k TRAIN / Rs-21.7k TEST.
3. The gate's 2026-06-09 validation (PF 3.25, 32 trades, 10 sessions) is exposed as a small-window artifact.
4. Mechanical flaws: 69-79% SL-rate at 0.70% SL; avg realized loss exceeds avg win; 1.00% target too small net of ~30bps costs.
5. Temporal decay: TEST (June) is *worse* than TRAIN (Mar-May) in both configs — the edge is not fading; it never existed in this window.

Full loss anatomy: see `FAILURE_ANALYSIS.md`.
