# DOC5C_ORB_GAP_GO_LONG (LONG) — ITERATION_LOG

_Generated 2026-07-01. Optimizer: Optuna TPE. 300 FIT/VAL trials (each trial = one logical config: exit SL/Tgt + <=2 mask + <=1 pre-momentum + guards)._

Search protocol: optimise ONLY on FIT/VAL with the band objective `reward(min(FIT_PF,VAL_PF)) - 0.80*|FIT_PF-VAL_PF|`, where reward tents at PF 1.70 and penalises overshoot (anti-overfit). The single best FIT/VAL config is then confirmed on full TRAIN and scored ONCE on TEST. Quantile thresholds drawn from TRAIN only (never TEST). The robust gate then checks TEST PF+day-block p, TRAIN target-fill, threshold-neighborhood stability, and term-dropout stability.

Exit grid searched: SL [0.5, 0.7, 0.85, 1.0, 1.1, 1.2, 1.5] x Tgt [0.6, 0.8, 1.0, 1.25, 1.5, 2.0, 2.5] (small/large SL x small/large target, asymmetric combos). Mask feats: ['atr_pct>=0.005042', 'body_pct>=0.408753', 'body_pct>=0.600523', 'body_pct>=0.651126', 'body_pct>=0.706505', 'close_loc<=0.831061', 'close_loc>=0.831061', 'lower_wick_pct>=0.029714', 'lower_wick_pct>=0.158894', 'quality_score<=82.451887', 'quality_score>=89.722833', 'ranker_score<=75.265042'].

## Top 20 FIT/VAL trials (by band score)

| # | SL | Tgt | mask | premom | guard | FIT n/PF | VAL n/PF | score |
|---|----|-----|------|--------|-------|----------|----------|-------|
| 1 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 2 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 3 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 4 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 5 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 6 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 7 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 8 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 9 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 10 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 11 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 12 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"max_slot": "14:00", "top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 13 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 14 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 15 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 16 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 17 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 18 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 19 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |
| 20 | 0.5 | 2.5 | - | pre1_adx<=34.126353 | {"top_n": 1} | 8/0.632 | 11/0.601 | 0.5757 |

## Best config — confirmation runs

- **Best (FIT/VAL):** SL 0.5/Tgt 2.5 | mask [(none)] | premom [pre1_adx<=34.126353] | guard {'top_n': 1} | maxpos 10 | dloss 4000.0
- **TRAIN @15bps:** n=19 PF=0.614 net=Rs-3,921 win%=26.3 avgW=Rs1,249 avgL=Rs-726 maxDD=Rs-6,186 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.11 tradeDom=0.363 dayDom=9.99 symDom=9.99 dbp=0.8101
- **TEST  @15bps:** n=2 PF=0.245 net=Rs-548 win%=50.0 avgW=Rs178 avgL=Rs-726 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=None
- **TRAIN @5bps:**  n=19 PF=0.614 net=Rs-3,921 win%=26.3 avgW=Rs1,249 avgL=Rs-726 maxDD=Rs-6,186 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.11 tradeDom=0.363 dayDom=9.99 symDom=9.99 dbp=0.8101
- **TEST  @5bps:**  n=2 PF=0.245 net=Rs-548 win%=50.0 avgW=Rs178 avgL=Rs-726 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=None

## Robustness diagnostics

- neighborhood pass: `False` (PF floor 1.15)
- term-dropout pass: `False` (PF floor 1.0)
- hard reasons: `TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 10.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed`
- insufficient reasons: `TEST too few trades (test_n<6)`
- warnings: `-`

- **Keep/reject:** REJECT  — TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 10.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6)

## Command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5C_ORB_GAP_GO_LONG --pool c:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2/Train_and_Test/doc5_long_setups/pool --trials 300 --time_budget_min 12.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 10.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

---

# ADDENDUM — Staged gap-knob iterations (`scripts/gap_knob_sweep.py`)

The canonical engine above searches the standard feature list only. This addendum logs the
**staged one-knob-at-a-time** search including the Setup-C structural columns the engine omits
(`gap_pct`, `orh_dist_atr`, `vwap_slope_atr`). Same repo pipeline / cost model. Each iteration
changes ONE logical group, evaluated on FIT then VAL; promising configs confirmed on full TRAIN;
TEST scored only if TRAIN PF ∈ [1.30,1.70] (never reached ⇒ TEST correctly never tuned).

| # | group | change | old→new | FIT n/PF | VAL n/PF | full-TRAIN n/PF | keep? |
|---|---|---|---|---|---|---|---|
| 1 | baseline | raw, doc exit | — | 47/0.103 | 54/0.305 | 101/0.202 | reject (no edge) |
| 2 | exit SL/Tgt | 7×7 grid | 0.85/1.50→1.20/2.00 | 44/0.153 | 54/0.355 | 98/0.253 | reject (best exit still 0.25) |
| 3 | non-indicator | gap_pct band ≤1.0 | —→≤1.0 | 37/0.205 | 28/0.328 | — | reject |
| 4 | non-indicator | orh_dist_atr (ext_max) ≤ | 3.0→1.0 | 9/0.035 | 4/0.497 | — | reject (extension not the cause) |
| 5 | indicator | vwap_slope_atr ≥ (slope_min) | 0.1→0.5 | 28/0.165 | 31/0.428 | — | reject |
| 6 | filter | vol_ratio ≥ | 1.5→3.0 | 13/0.128 | 18/0.570 | — | reject (VAL up, FIT dead = noise) |
| 7 | non-indicator | body_pct ≥ | 0.4→0.8 | 17/0.409 | 30/0.313 | — | best single FIT, still «1.0 |
| 8 | filter | rs_pct ≥ | 0.4→1.5 | 21/0.288 | 26/0.251 | — | reject |
| 9 | pre-mom | pre5_mom_r ≥ | —→0.2 | 11/0.428 | 19/0.547 | — | best joint single gate, still «1.0 |
| 10 | pre-mom | pre3_range_r ≥ | —→0.3 | 20/0.291 | 21/0.468 | — | reject |
| 11 | guard | max_slot | —→10:30 | 15/0.361 | — | 15/0.361 | reject |
| 12 | guard | top_n | —→1 | — | — | — | reject (shrinks OOS to ≤2) |
| 13 | **combo (best)** | vwap_slope_atr≥0.5 & vol_ratio≥2.5 + pre3_range_r≥0.3, exit 1.20/2.00 | — | 5/0.503 | 10/0.768 | **15/0.683 (net −Rs 3,188)** | reject (not in band) |

Combination search: **2,185** FIT/VAL combos (≤2 mask + ≤1 pre-mom + guard + exit) with ≥5 trades
in both halves. Best worse-half PF = **0.512**. Top-12 confirmed on full TRAIN: PF range **0.32–0.68**,
all net-negative. **No config reached TRAIN PF 1.30 ⇒ no TEST validation warranted.**

**Keep/reject: REJECT** — no band-eligible candidate under any indicator / non-indicator /
pre-momentum / filter / guard / exit combination. See `PARAMETER_SWEEP_SUMMARY.md`.

## Addendum command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\DOC5C_ORB_GAP_GO_LONG\scripts\gap_knob_sweep.py
```