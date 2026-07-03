# LAST-3-MONTHS RESULT — B_AVWAP_RECLAIM_REVERSAL (LONG)

Replay of fixed configs over **2026-03-24..2026-06-30** (no tuning), net of cost. Best-candidate config = pf_band_search loop winner (which FAILED the approval gate — overfit/2-day TEST); shown here purely as a longer-window read. Nothing written to final_setup_conf.py.

- window sessions: 53  (2026-03-24..2026-06-24)

## BEST_CANDIDATE
- exit SL 0.9 / Tgt 3.0 · mask [vwap_dist_atr<=1.0; vol_ratio>=3.537825; atr_pct<=0.003921] · premom [pre1_adx>=30.675856; pre5_mom_r>=0.317166] · guard {'max_slot': '14:00'}

| cost | n | PF | net Rs | win% | tgt/sl/eod | maxDD | tpd | dayDom | symDom |
|---|---:|---:|---:|---:|---|---:|---:|---:|---:|
| 15bps | 94 | 0.6079 | -22,254 | 30.85 | 4/41/49 | -26,879 | 2.47 | 9.99 | 9.99 |
| 5bps | 94 | 0.9156 | -3,842 | 37.23 | 5/35/54 | -15,678 | 2.47 | 9.99 | 9.99 |

- monthly @15bps: 2026-03: n11 PF0.07 Rs-6,666 | 2026-04: n32 PF0.44 Rs-11,727 | 2026-05: n25 PF0.44 Rs-8,998 | 2026-06: n26 PF1.40 Rs5,137

## CARD_BASELINE
- exit SL 0.7 / Tgt 1.5 · mask [vwap_dist_atr<=1.0] · premom [(none)] · guard {}

| cost | n | PF | net Rs | win% | tgt/sl/eod | maxDD | tpd | dayDom | symDom |
|---|---:|---:|---:|---:|---|---:|---:|---:|---:|
| 15bps | 1783 | 0.329 | -730,530 | 23.56 | 227/1044/512 | -747,458 | 33.64 | 9.99 | 9.99 |
| 5bps | 1697 | 0.561 | -365,534 | 30.41 | 265/902/530 | -388,823 | 32.02 | 9.99 | 9.99 |

- monthly @15bps: 2026-03: n126 PF0.50 Rs-38,713 | 2026-04: n651 PF0.33 Rs-266,664 | 2026-05: n587 PF0.28 Rs-261,348 | 2026-06: n419 PF0.35 Rs-163,804
