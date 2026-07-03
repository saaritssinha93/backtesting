# A_MOD_BREAK_C1_LOW (SHORT) — BASELINE_RESULT (from-scratch recovery)

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

- config source: FINAL_SETUP_CONF (active) — mask `vol_ratio>=1.955814`, premom `pre5_mom_r>=0.425861 & pre3_range_r<=0.202087`, exit SL 1.10 / Tgt 1.00, no guards.
- original detection (v2 scanner): red bar (close<open, close_loc<=0.40), impulse range 0.60-2.20x ATR, close < PREV BAR low ("C1" = prior candle, not first-of-day), close < session VWAP, ADX>=19.12, RSI>=23.22, atr_pct<=0.0063, vol_ratio>=1.5, regime!=BULL; entry = next 1-min open.

- windows: TRAIN 2026-03-02..2026-05-29 (58 sessions; FIT 35 / VAL 23), TEST 2026-06-01..2026-06-30 (21 sessions)

- **FIT**: n=108 PF=0.43 net=Rs-36,689 win%=39.8 avgW=Rs642 avgL=Rs-989 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=4.0 tradeDom=0.028 dayDom=9.99 symDom=9.99 dbp=0.9997
- **VAL**: n=56 PF=0.832 net=Rs-4,205 win%=55.4 avgW=Rs671 avgL=Rs-1,000 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=4.0 tradeDom=0.037 dayDom=9.99 symDom=9.99 dbp=0.6889
- **TRAIN**: n=164 PF=0.542 net=Rs-40,894 win%=45.1 avgW=Rs654 avgL=Rs-992 SL/TGT/EOD=56/59/49 tgt%=36.0 tpd=4.0 tradeDom=0.016 dayDom=9.99 symDom=9.99 dbp=0.9992
- **TEST**: n=36 PF=0.337 net=Rs-14,426 win%=36.1 avgW=Rs564 avgL=Rs-946 SL/TGT/EOD=13/8/15 tgt%=22.2 tpd=2.25 tradeDom=0.105 dayDom=9.99 symDom=9.99 dbp=0.994

Baseline verdict: loser everywhere (TRAIN PF 0.542, TEST PF 0.337) — matches phase-1/2 findings and live paper (PF ~0.25).