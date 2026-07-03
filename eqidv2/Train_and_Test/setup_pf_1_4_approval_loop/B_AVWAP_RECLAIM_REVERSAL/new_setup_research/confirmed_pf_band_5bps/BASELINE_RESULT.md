# BASELINE_RESULT — B_AVWAP_CONFIRMED_RECLAIM_LONG (LONG)

## Current rules (card)
- **Source:** `Train_and_Test/SETUP_CARDS_AND_LIVE_CROSSCHECK.md` §2 (card of record); config NOT taken from `final_setup_conf.py`.
- **Exit:** SL 0.9 / Tgt 1.5
- **mask_terms (filters):** (none)
- **pre_momentum_terms (gates):** (none)
- **entry_guards:** {}
- **max_positions:** 20  ·  **daily_loss_rs:** 0.0
- **Detection (raw, unchanged):** `|low−intraday_low_8|≤0.40×ATR`, `close>VWAP`, `close>open`, `close_loc≥0.60`, `vol_ratio≥1.5` (double-bottom VWAP reclaim).

## Exact sessions (inferred from the setup pool)
- **FIT**   2026-05-18..2026-06-02  (8 sessions): 2026-05-18, 2026-05-19, 2026-05-20, 2026-05-25, 2026-05-26, 2026-05-27, 2026-05-29, 2026-06-02
- **VAL**   2026-06-04..2026-06-19  (8 sessions): 2026-06-04, 2026-06-09, 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19
- **TRAIN** 2026-05-18..2026-06-19  (16 sessions)
- **TEST**  2026-06-22..2026-06-24  (3 sessions): 2026-06-22, 2026-06-23, 2026-06-24

## Baseline metrics (card config, net of cost)
| window | 5 bps/leg run | 5 bps verification |
|---|---|---|
| TRAIN | n=48 PF=1.1166 net=Rs1,875 win=50.0% t/s/e=6/11/31 avgW/L=748/-670 maxDD=Rs-7,432 tpd=3.0 domTr/Day/Sym=0.076/2.064/0.728 dbp=0.4029 | n=48 PF=1.1166 net=Rs1,875 win=50.0% t/s/e=6/11/31 avgW/L=748/-670 maxDD=Rs-7,432 tpd=3.0 domTr/Day/Sym=0.076/2.064/0.728 dbp=0.4029 |
| TEST  | n=6 PF=0.2659 net=Rs-2,042 win=33.33% t/s/e=0/2/4 avgW/L=370/-695 maxDD=Rs-1,297 tpd=2.0 domTr/Day/Sym=0.677/9.99/9.99 dbp=0.9624 | n=6 PF=0.2659 net=Rs-2,042 win=33.33% t/s/e=0/2/4 avgW/L=370/-695 maxDD=Rs-1,297 tpd=2.0 domTr/Day/Sym=0.677/9.99/9.99 dbp=0.9624 |

## Initial diagnosis
- Card TRAIN PF 1.1166 (net Rs1,875).
- Card TEST PF 0.2659 on n=6.
- Search target: bring full-TRAIN PF into [1.30,1.70] (not higher) and TEST PF >1.40 using exit tuning + repo-supported filters/gates only.