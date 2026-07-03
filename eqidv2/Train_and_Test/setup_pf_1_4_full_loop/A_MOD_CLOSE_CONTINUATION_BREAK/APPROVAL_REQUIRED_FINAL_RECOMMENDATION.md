# A_MOD_CLOSE_CONTINUATION_BREAK (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-03. Research-only; NO live trades; NO final_setup_conf.py edits._

## Approval recommendation: **NO**

No configuration met TRAIN PF in [1.30, 1.80] AND TEST PF > 1.40 with positive PnL, meaningful trades, domination caps and robustness.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES** (nothing to promote)

## Evidence summary (why NO)

- Baseline: TRAIN PF 0.315 (n=1,887, -Rs846k) / TEST PF 0.252 (n=613, -Rs316k); FIT/VAL/TRAIN/TEST uniformly negative, so the loss is structural, not one bad month.
- 443 logged iterations: 49 exit combos, 198 single indicator/price-action masks, 144 pre-momentum gates, 17 guards, 3 regime slices, 46 Optuna TPE combinations, and a full rescue loop (guard-only, single-term, simplified + 250-trial TPE round).
- Best single knob: FIT/VAL PF 0.45 (sig5_vol_ratio20>=4.8). Best combination: FIT 0.56 / VAL 0.59 (SL0.85/T2.0, wick_skew>=0.042 & rs_pct<=3.02, pre3_range_r>=0.63, min_slot 09:45). Best rescue config: PF ~0.51.
- v1: ZERO configs reached the TRAIN band floor of 1.30 -> the 10-run TEST budget was never spent; TEST remained completely untouched by the v1 search (no test-fitting).
- Failure study: winner/loser feature medians are nearly identical, and losers have HIGHER rs_pct and quality_score than winners — signal strength is anti-predictive; every hour bucket and every regime slice with n>=20 is negative.

### v2 expanded-feature campaign (1,230 more iterations)

- Pool re-enriched with ~41 causal indicator/price-action/day-context features computed from OHLCV (RSI+slope, ADX+slope, MACD, Bollinger, Keltner, Stochastic, Williams %R, CCI, MFI, OBV, ROC 3/6/12, EMA20/50/200 structure+slopes, session VWAP, day high/range/position, bar index, opening range, prev-day H/L/C, gap, candle/volume structure) + the 8 pre-momentum features as searchable columns.
- 18 hand-written STRUCTURAL HYPOTHESIS packs (trend-alignment, fresh-at-day-high, PDH break, squeeze-expansion, not-exhausted, MACD turn, volume+MFI thrust, OBV accumulation, Keltner breakout, gap-up continuation, OR breakout, RSI momentum zone, low-vol name, premom confirm, time windows) x 2 exit anchors: ALL 36 reject (best PF ~0.5). There are ZERO signals in the first trading hour at all.
- 1,116 single-term sweeps over 59 features: 0 keeps; best single term x_range_vs_avg20<=0.91 (quiet-bar breakouts) FIT 0.55 / VAL 0.60.
- 3,000 Optuna TPE trials (up to 3 AND-terms + slot guards + exits): exactly ONE config reached the TRAIN band — sig5_adx_calc<=22.35 (weak 5-min trend) in the 10:00-11:00 window, top-3/slot, SL0.7/T2.0: TRAIN n=20 PF 1.56 (+Rs4,459), identical through the true pre-momentum path. Scored ONCE on TEST: n=3, PF 0.31, -Rs1,278, single trade = 100% of gross -> thin-pocket overfit, rejected by the gate. Its relaxed neighbor (<=28.03) already drops to TRAIN PF 1.13 (knife-edge).

## Closest confirmations (full TRAIN, none in band)

- SL0.7/T2.0 mask[x_pm_sig5_adx_calc<=22.348337] pm[-] guard={'min_slot': '10:00', 'max_slot': '11:00', 'top_n': 3}: verdict REJECT: test_pf_gt_1.40;test_net_pos;test_n_ge_5;train_dom_ok;test_dom_ok
  - TRAIN n=20 PF=1.564 net=Rs4,459 win%=55.0 avgW=Rs1,124 avgL=Rs-878 maxDD=Rs-3,698 SL/TGT/EOD=8/5/7 tpd=1.43 domT/D/S=0.143/0.757/0.396 dbp=0.1865
  - TEST  n=3 PF=0.313 net=Rs-1,278 win%=33.3 avgW=Rs582 avgL=Rs-930 maxDD=Rs-929 SL/TGT/EOD=2/0/1 tpd=1.0 domT/D/S=1.0/9.99/9.99 dbp=0.736
- SL0.7/T2.0 mask[x_pm_sig5_adx_calc<=22.348337] pm[-] guard={'max_slot': '11:00', 'top_n': 3}: verdict REJECT: test_pf_gt_1.40;test_net_pos;test_n_ge_5;train_dom_ok;test_dom_ok
  - TRAIN n=20 PF=1.564 net=Rs4,459 win%=55.0 avgW=Rs1,124 avgL=Rs-878 maxDD=Rs-3,698 SL/TGT/EOD=8/5/7 tpd=1.43 domT/D/S=0.143/0.757/0.396 dbp=0.1865
  - TEST  n=3 PF=0.313 net=Rs-1,278 win%=33.3 avgW=Rs582 avgL=Rs-930 maxDD=Rs-929 SL/TGT/EOD=2/0/1 tpd=1.0 domT/D/S=1.0/9.99/9.99 dbp=0.736
- SL1.0/T2.5 mask[x_adx<=22.348337;x_pm_sig5_vol_ratio20>=1.745488] pm[-] guard={'min_slot': '10:00', 'max_slot': '11:00'}: verdict REJECT: TRAIN PF outside [1.30,1.80]
  - TRAIN n=30 PF=1.254 net=Rs3,660 win%=56.7 avgW=Rs1,062 avgL=Rs-1,107 maxDD=Rs-3,699 SL/TGT/EOD=9/4/17 tpd=2.0 domT/D/S=0.125/0.615/0.619 dbp=0.2346
  - TEST  (not run)

## Rerun commands

```
cd <repo root>
py -3.12 avwap_5min_ID_v11_backtesting.py --mode historical_all_available --start_date 2026-06-25 --end_date 2026-07-02 --workers 8 --out Train_and_Test\setup_pf_1_4_full_loop\A_MOD_CLOSE_CONTINUATION_BREAK\pools\_tail_raw_gen
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\build_pool_amccb.py
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\eval_baseline.py
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\campaign_amccb.py --stages 2,3,4,5,6 --trials 500 --seed 7
py -3.12 Train_and_Test\setup_pf_1_4_full_loop\A_MOD_CLOSE_CONTINUATION_BREAK\scripts\render_reports.py
```

## Remaining risks

- This setup exists as the same-bar-collapse residual of A_MOD_BREAK_C1_HIGH: 96.8% of its signals occur in BEAR regime (bear-day continuation LONG). Regime shift changes its firing rate structurally.
- June TEST is thin on several days; 2026-06-26 missing (no 5-min data), 2026-07-02 excluded (1-min truncation).
- Screening-basis pool (raw candidates): live gate parity must be confirmed on the v11 conf backtest before any live watch.
- 15 bps/leg slippage assumed; illiquid small-caps may be worse.