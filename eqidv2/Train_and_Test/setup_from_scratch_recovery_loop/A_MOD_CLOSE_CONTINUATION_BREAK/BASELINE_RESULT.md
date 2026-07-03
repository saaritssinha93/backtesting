# A_MOD_CLOSE_CONTINUATION_BREAK (LONG) — BASELINE_RESULT (recovery loop)

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Original rules (card, re-detected uncollapsed)

- Entry trigger: 5-min bar, close>open, close_loc >= 0.75, close > prev bar high, range 0.60-2.20 x ATR (moderate impulse), above causal session VWAP, rs_pct > 0 vs NIFTYBEES, vol_ratio >= 1.4 (common floor 1.5), quality >= 6.8; liquidity floors px>=80 / bar>=Rs1M / day>=Rs20M after 10:00; scan 10:00-14:30.
- Pre-momentum: none (original). Filters: none beyond detector. Guards: none.
- SL/Target: 0.70% / 1.50% (production exit rule). Exit: first-touch on 1-min bars else EOD 15:20; entry next 1-min open + 15 bps/leg; statutory NSE costs.
- Pool: 42,757 rows / 1142 tickers / 81 sessions; regime mix {'NEUTRAL': 16339, 'BULL': 13928, 'BEAR': 9976, 'TREND': 2514}.

## Windows

- TRAIN 2026-03-02..2026-05-29 (58 sessions); FIT 2026-03-02..2026-04-24 (35); VAL 2026-04-27..2026-05-29 (23)
- TEST 2026-06-01..2026-07-01 (22 sessions; 07-02 excluded — truncated 1-min data)

## Baseline metrics (@15 bps/leg, statutory costs)

- **FIT**: n=2980 PF=0.244 net=Rs-1,628,901 win%=17.8 avgW=Rs993 avgL=Rs-879 SL/TGT/EOD=2232/364/384 tpd=85.14 domT/D/S=0.002/9.99/9.99 dbp=1.0
- **VAL**: n=2208 PF=0.196 net=Rs-1,323,919 win%=14.5 avgW=Rs1,007 avgL=Rs-872 SL/TGT/EOD=1705/225/278 tpd=96.0 domT/D/S=0.004/9.99/9.99 dbp=1.0
- **TRAIN**: n=5188 PF=0.223 net=Rs-2,952,820 win%=16.4 avgW=Rs998 avgL=Rs-876 SL/TGT/EOD=3937/589/662 tpd=89.45 domT/D/S=0.001/9.99/9.99 dbp=1.0
- **TEST**: n=2106 PF=0.173 net=Rs-1,316,717 win%=13.6 avgW=Rs966 avgL=Rs-875 SL/TGT/EOD=1649/192/265 tpd=95.73 domT/D/S=0.005/9.99/9.99 dbp=1.0

## TRAIN regime slices

- BULL: n=2169 PF=0.22 net=Rs-1,229,489 win%=16.3 avgW=Rs981 avgL=Rs-868 SL/TGT/EOD=1634/242/293 tpd=60.25 domT/D/S=0.004/9.99/9.99 dbp=1.0
- BEAR: n=2343 PF=0.296 net=Rs-1,149,287 win%=20.3 avgW=Rs1,016 avgL=Rs-874 SL/TGT/EOD=1696/341/306 tpd=55.79 domT/D/S=0.003/9.99/9.99 dbp=1.0
- NEUTRAL: n=2448 PF=0.28 net=Rs-1,217,651 win%=19.8 avgW=Rs978 avgL=Rs-861 SL/TGT/EOD=1724/321/403 tpd=53.22 domT/D/S=0.003/9.99/9.99 dbp=1.0
- TREND: n=448 PF=0.302 net=Rs-217,793 win%=21.2 avgW=Rs991 avgL=Rs-884 SL/TGT/EOD=324/67/57 tpd=44.8 domT/D/S=0.013/9.99/9.99 dbp=1.0

## Diagnosis

- The uncollapsed card is uniformly and heavily negative in EVERY regime (BULL 0.22 / BEAR 0.30 / NEUTRAL 0.28 / TREND 0.30) — the collapse-shadowing hypothesis is refuted: the trigger itself has no edge at production exits.
- MFE/MAE (1-min paths): median MFE +0.37% vs median MAE -1.05%; close-to-EOD median -0.47%. All 49 SL x target brackets are physically infeasible — the perfect-exit hit-rate ceiling is ~half the win rate needed for PF 1.3 (see WINNER_LOSER_STUDY.md).
- Recovery therefore depends entirely on filters finding a sub-pocket with a several-fold different forward distribution.