# DOC5A_AVWAP_PULLBACK_LONG — RELAXED-BAR RESULT (5 bps)

_Generated 2026-07-01. Research-only. NO conf edits, NO live trades._

Per user direction ("relax the bar"): instead of TRAIN PF 1.30–1.70, look for the best TRADEABLE config —
meaningful trade count (~25+ TRAIN), highest TRAIN PF that keeps the trades, and a **positive TEST**,
non-dominated. Candidates shortlisted ONLY on FIT/VAL (from the 900-trial `reinvent_5bps` trials.csv,
min 15 trades/half); TRAIN + TEST measured once. Cost 5 bps/leg. (`scripts/relaxed_eval.py`)

## Result: NO tradeable config exists — even relaxed

Every shortlisted config with a meaningful trade count is a **net loser on BOTH TRAIN and TEST**:

| Representative config (n≥25 TRAIN) | TRAIN PF (n) | TEST PF (n) |
|---|---|---|
| vol_ratio≥1.09 & orh_dist_atr≤-0.10, SL/Tgt 1.5/1.5 | 0.64 (138) | 0.39 (42) |
| adx_sig≥14.5 & orh_dist_atr≤-0.10, SL/Tgt 1.5/1.5 | 0.72 (130) | 0.37 (39) |
| rsi_sig≥54.7 & orh_dist_atr≤-0.10, SL/Tgt 1.1/2.5 | 0.69 (136) | 0.39 (44) |
| rsi_sig≥57.4 & signal_range_pct≤0.207 + premom, 1.1/2.5 | 0.88 (40) | 0.10 (19) |
| close_loc≥0.68 & signal_range_pct≤0.207 + premom, 1.1/2.5 | 0.67 (41) | 0.13 (13) |

- **Best TRAIN PF at n≥25 = 0.88** (still a loser); its TEST = 0.10.
- **Higher trade count ⇒ more clearly negative** (n=138 ⇒ TRAIN 0.64 / TEST 0.39).
- The only PF>1.30 configs are the n≤13 overfit pockets (TEST 0.002) documented in REINVENT_5BPS_READOUT.md.

## Verdict
DOC5A is not "below the goal" — it is **net-negative at every tradeable size, in-sample and out**. There
is no config that is simultaneously meaningfully-traded and positive. Relaxing the acceptance bar does not
help because the problem is directional (no edge), not a threshold-tightness issue.

**Recommendation: NO — do not trade DOC5A at any bar.** The AVWAP-trend-pullback long, as a mechanical
5-min detector on this F&O universe / Apr–Jun data, has no extractable edge (consistent with the whole
doc5 batch and the P_PDH / FAST_MOMENTUM structural-wall findings).

> DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES.
