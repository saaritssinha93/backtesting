# E_ORB_BREAKOUT_LONG — BASELINE_RESULT

- **Side:** LONG  |  basis: native=SCREENING-ONLY (firehose; v11 conf backtest is the live-faithful arbiter)  |  optimizer: Optuna TPE
- **TRAIN** 2026-05-18..2026-06-19 (22 sessions) — FIT 2026-05-18..2026-06-04 / VAL 2026-06-05..2026-06-19
- **TEST**  2026-06-22..2026-06-24 (3 sessions)  ⚠️ only 3 sessions available after 2026-06-20 (pool ends 2026-06-24)
- entries @ 15.0bps/leg: FIT=76 VAL=79 TRAIN=155 TEST=33

## Current card rules (baseline config)
```
SL/Tgt = 0.7/1.5
mask_terms = [['vwap_dist_atr', '<=', 1.0]]
pre_momentum_terms = []
entry_guards = {}
```
Config source: final_setup_conf.py (RESEARCH_WATCH_CONF — parked 2026-06-29) / SETUP_CARDS §2.

## Baseline metrics

| window | metrics |
|---|---|
| TRAIN | n=29 PF=0.518 net=Rs-9,402 win=27.6% t/s/e=8/21/0 tpd=1.81 dayDom=9.99 symDom=9.99 trDom=0.125 dd=Rs-11,000 dbp=0.9724 |
| TEST | n=6 PF=0.68 net=Rs-1,189 win=33.3% t/s/e=2/4/0 tpd=2.0 dayDom=9.99 symDom=9.99 trDom=0.501 dd=Rs-2,792 dbp=0.8528 |

## Initial diagnosis
- Baseline TRAIN PF 0.518 (OUT of the 1.30-1.70 band); TEST PF 0.68 (<= 1.4).
- **Structural limit:** TEST has only 3 sessions, so the 'no single day dominates' check (day-dominance ≤ 0.4) is effectively impossible (1 day ≥ 0.33 by construction). Any TEST PF here is low-confidence; treat as directional only.