# doc5_long_setups — SCOREBOARD

_Generated 2026-07-01. Research-only. NO `final_setup_conf.py` edits, NO live trades._

Four LONG archetypes from `~/Downloads/5min_long_setups.md`, built as **new distinct detectors**
(standalone raw-5min scan `scan_doc5_long_setups.py`, no edits to the live v2/v11 engine), mined
over the F&O universe (204 tickers) and run through the shared PF-band approval loop
(`setup_pf_1_4_approval_loop/_engine/pf_band_fitval_loop.py`, Optuna TPE, 200 trials).

**Split (as requested):** TRAIN `2026-04-01…05-29` (~33 sess) · TEST `2026-06-02…06-30` (18–19 sess).
**Gate:** TRAIN PF ∈ [1.30,1.70] AND TEST PF > 1.30 (robust), net of NSE costs @15 bps/leg, next-open fill.
**Pool:** `pool/historical_all_available_pre_dedupe_live_candidates.csv` (2,142 rows: B 1162 / A 583 / C 236 / D 161).

## Result: 0 of 4 PASSED

| Setup | Doc | Best TRAIN PF (n) | Best TEST PF (n) | Verdict | Note |
|---|---|---|---|---|---|
| DOC5A_AVWAP_PULLBACK_LONG | A pullback | 0.91 (52) | 0.47 (18) | REJECT | loser both windows |
| DOC5B_MOMO_BREAKOUT_LONG | B breakout | 0.74 (41) | 0.55 (21) | REJECT | loser both windows |
| DOC5C_ORB_GAP_GO_LONG | C gap-and-go | 0.58 (15) | 0.26 (8) | REJECT | loser + thin (worst) |
| DOC5D_AVWAP_RECLAIM_LONG | D reclaim | 1.25 (22) | 0.47 (8) | REJECT | TRAIN near band, **OOS collapses** |

**Read:** DOC5D got closest — the only one to build a genuine train-side band config (PF 1.25, 36% target
fills, day-block p 0.28, non-dominated) — but its June OOS is a clean loser (PF 0.47). Same structural
wall seen in P_PDH / FAST_MOMENTUM / the doc-proxy runs: force TRAIN PF up ⇒ collapses OOS.

**Approximations (see scan header):** RS = `rs_pct` return-vs-index (not cross-sectional percentile);
breadth = NIFTYBEES-VWAP regime proxy; only `next_open` exec mode modelled. A true `rs_rank`+`breadth`
two-pass scan is the only untested lever — but with all four net-negative OOS, the prior is poor.

**Promote:** none. Keep as research artifacts. Per-setup detail under `results/<SETUP>/`.

> **DO NOT MOVE ANYTHING TO FINAL CONFIG UNTIL USER APPROVES.**
