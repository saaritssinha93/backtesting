# BASELINE_RESULT — B_AVWAP_RECLAIM_PULLBACK_BREAK_LONG (LONG)

## Current rules (card)
- **Source:** `Train_and_Test/SETUP_CARDS_AND_LIVE_CROSSCHECK.md` §2 (card of record); config NOT taken from `final_setup_conf.py`.
- **Exit:** SL 0.9 / Tgt 1.5
- **mask_terms (filters):** (none)
- **pre_momentum_terms (gates):** (none)
- **entry_guards:** {}
- **max_positions:** 20  ·  **daily_loss_rs:** 0.0
- **Detection (raw, unchanged):** `|low−intraday_low_8|≤0.40×ATR`, `close>VWAP`, `close>open`, `close_loc≥0.60`, `vol_ratio≥1.5` (double-bottom VWAP reclaim).

## Exact sessions (inferred from the setup pool)
- **FIT**   2026-05-18..2026-06-03  (9 sessions): 2026-05-18, 2026-05-19, 2026-05-20, 2026-05-25, 2026-05-26, 2026-05-27, 2026-05-29, 2026-06-02, 2026-06-03
- **VAL**   2026-06-04..2026-06-19  (10 sessions): 2026-06-04, 2026-06-08, 2026-06-09, 2026-06-11, 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19
- **TRAIN** 2026-05-18..2026-06-19  (19 sessions)
- **TEST**  2026-06-22..2026-06-24  (3 sessions): 2026-06-22, 2026-06-23, 2026-06-24

## Baseline metrics (card config, net of cost)
| window | 5 bps/leg run | 5 bps verification |
|---|---|---|
| TRAIN | n=68 PF=1.035 net=Rs765 win=50.0% t/s/e=7/15/46 avgW/L=665/-643 maxDD=Rs-9,659 tpd=3.58 domTr/Day/Sym=0.06/5.069/1.784 dbp=0.4652 | n=68 PF=1.035 net=Rs765 win=50.0% t/s/e=7/15/46 avgW/L=665/-643 maxDD=Rs-9,659 tpd=3.58 domTr/Day/Sym=0.06/5.069/1.784 dbp=0.4652 |
| TEST  | n=12 PF=0.2657 net=Rs-4,170 win=33.33% t/s/e=0/4/8 avgW/L=377/-710 maxDD=Rs-4,073 tpd=4.0 domTr/Day/Sym=0.373/9.99/9.99 dbp=0.9624 | n=12 PF=0.2657 net=Rs-4,170 win=33.33% t/s/e=0/4/8 avgW/L=377/-710 maxDD=Rs-4,073 tpd=4.0 domTr/Day/Sym=0.373/9.99/9.99 dbp=0.9624 |

## Initial diagnosis
- Card TRAIN PF 1.035 (net Rs765).
- Card TEST PF 0.2657 on n=12.
- Search target: bring full-TRAIN PF into [1.30,1.70] (not higher) and TEST PF >1.40 using exit tuning + repo-supported filters/gates only.