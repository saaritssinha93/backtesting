# BASELINE_RESULT — B_AVWAP_RECLAIM_REVERSAL (LONG)

## Current rules (card)
- **Source:** `Train_and_Test/SETUP_CARDS_AND_LIVE_CROSSCHECK.md` §2 (card of record); config NOT taken from `final_setup_conf.py`.
- **Exit:** SL 0.7 / Tgt 1.5
- **mask_terms (filters):** vwap_dist_atr<=1.0
- **pre_momentum_terms (gates):** (none)
- **entry_guards:** {}
- **max_positions:** 20  ·  **daily_loss_rs:** 0.0
- **Detection (raw, unchanged):** `|low−intraday_low_8|≤0.40×ATR`, `close>VWAP`, `close>open`, `close_loc≥0.60`, `vol_ratio≥1.5` (double-bottom VWAP reclaim).

## Exact sessions (inferred from the setup pool)
- **FIT**   2026-05-18..2026-06-02  (10 sessions): 2026-05-18, 2026-05-19, 2026-05-20, 2026-05-22, 2026-05-25, 2026-05-26, 2026-05-27, 2026-05-29, 2026-06-01, 2026-06-02
- **VAL**   2026-06-03..2026-06-16  (10 sessions): 2026-06-03, 2026-06-04, 2026-06-05, 2026-06-08, 2026-06-09, 2026-06-10, 2026-06-11, 2026-06-12, 2026-06-15, 2026-06-16
- **TRAIN** 2026-05-18..2026-06-16  (20 sessions)
- **TEST**  2026-06-22..2026-06-24  (2 sessions): 2026-06-22, 2026-06-24

## Baseline metrics (card config, net of cost)
| window | 5 bps/leg run | 5 bps verification |
|---|---|---|
| TRAIN | n=573 PF=0.5374 net=Rs-131,275 win=29.49% t/s/e=83/306/184 avgW/L=902/-702 maxDD=Rs-138,558 tpd=28.65 domTr/Day/Sym=0.009/9.99/9.99 dbp=0.9941 | n=573 PF=0.5374 net=Rs-131,275 win=29.49% t/s/e=83/306/184 avgW/L=902/-702 maxDD=Rs-138,558 tpd=28.65 domTr/Day/Sym=0.009/9.99/9.99 dbp=0.9941 |
| TEST  | n=60 PF=0.4798 net=Rs-15,844 win=26.67% t/s/e=9/33/18 avgW/L=913/-692 maxDD=Rs-16,904 tpd=30.0 domTr/Day/Sym=0.094/9.99/9.99 dbp=None | n=60 PF=0.4798 net=Rs-15,844 win=26.67% t/s/e=9/33/18 avgW/L=913/-692 maxDD=Rs-16,904 tpd=30.0 domTr/Day/Sym=0.094/9.99/9.99 dbp=None |

## Initial diagnosis
- Card is a **net loser on TRAIN** (PF 0.5374, net Rs-131,275); SL rate 53.0% — stops dominate.
- Card TEST PF 0.4798 on n=60.
- Search target: bring full-TRAIN PF into [1.30,1.70] (not higher) and TEST PF >1.40 using exit tuning + repo-supported filters/gates only.