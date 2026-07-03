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
- **FIT**   2026-05-18..2026-05-29  (7 sessions): 2026-05-18, 2026-05-19, 2026-05-20, 2026-05-25, 2026-05-26, 2026-05-27, 2026-05-29
- **VAL**   2026-06-02..2026-06-19  (8 sessions): 2026-06-02, 2026-06-09, 2026-06-12, 2026-06-15, 2026-06-16, 2026-06-17, 2026-06-18, 2026-06-19
- **TRAIN** 2026-05-18..2026-06-19  (15 sessions)
- **TEST**  2026-06-22..2026-06-24  (2 sessions): 2026-06-22, 2026-06-24

## Baseline metrics (card config, net of cost)
| window | 5 bps/leg run | 5 bps verification |
|---|---|---|
| TRAIN | n=36 PF=1.1174 net=Rs1,521 win=47.22% t/s/e=5/9/22 avgW/L=852/-682 maxDD=Rs-5,063 tpd=2.4 domTr/Day/Sym=0.094/2.404/0.897 dbp=0.4066 | n=36 PF=1.1174 net=Rs1,521 win=47.22% t/s/e=5/9/22 avgW/L=852/-682 maxDD=Rs-5,063 tpd=2.4 domTr/Day/Sym=0.094/2.404/0.897 dbp=0.4066 |
| TEST  | n=3 PF=0.1856 net=Rs-1,050 win=33.33% t/s/e=0/1/2 avgW/L=239/-645 maxDD=Rs-270 tpd=1.5 domTr/Day/Sym=1.0/9.99/9.99 dbp=None | n=3 PF=0.1856 net=Rs-1,050 win=33.33% t/s/e=0/1/2 avgW/L=239/-645 maxDD=Rs-270 tpd=1.5 domTr/Day/Sym=1.0/9.99/9.99 dbp=None |

## Initial diagnosis
- Card TRAIN PF 1.1174 (net Rs1,521).
- Card TEST PF 0.1856 on n=3.
- Search target: bring full-TRAIN PF into [1.30,1.70] (not higher) and TEST PF >1.40 using exit tuning + repo-supported filters/gates only.