# DOC5C_ORB_GAP_GO_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN too few trades (train_n<20); TRAIN PF too low (<1.30); TRAIN target-fill rate below 10.0%; TRAIN concentrated (one trade/day/symbol dominates); neighborhood robustness failed; term-dropout robustness failed; TEST too few trades (test_n<6))

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 0.5,
    "tgt_pct": 2.5
  },
  "mask_terms": [],
  "pre_momentum_terms": [
    [
      "pre1_adx",
      "<=",
      34.126353
    ]
  ],
  "entry_guards": {
    "top_n": 1
  },
  "max_positions": 10,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=19 PF=0.614 net=Rs-3,921 win%=26.3 avgW=Rs1,249 avgL=Rs-726 maxDD=Rs-6,186 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.11 tradeDom=0.363 dayDom=9.99 symDom=9.99 dbp=0.8101 | n=19 PF=0.614 net=Rs-3,921 win%=26.3 avgW=Rs1,249 avgL=Rs-726 maxDD=Rs-6,186 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=2.11 tradeDom=0.363 dayDom=9.99 symDom=9.99 dbp=0.8101 |
| TEST  | n=2 PF=0.245 net=Rs-548 win%=50.0 avgW=Rs178 avgL=Rs-726 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=None | n=2 PF=0.245 net=Rs-548 win%=50.0 avgW=Rs178 avgL=Rs-726 maxDD=Rs0 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=1.0 tradeDom=1.0 dayDom=9.99 symDom=9.99 dbp=None |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5C_ORB_GAP_GO_LONG --pool c:/Users/Saarit/OneDrive/Desktop/Trading/backtesting/eqidv2/backtesting/eqidv2/Train_and_Test/doc5_long_setups/pool --trials 300 --time_budget_min 12.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 10.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 2 trades / 2 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 1.0 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).

---

## Final recommendation (both searches): **NO — do not promote**

Two independent searches through the same repo pipeline agree DOC5C_ORB_GAP_GO_LONG has **no
tradeable long edge** on the mandated 2026-05-18 → 2026-06-30 split:

| search | best FIT/VAL | best full-TRAIN PF | in [1.30,1.70]? | TEST |
|---|---|---:|---|---|
| canonical engine (Optuna, 300 trials) | 0.63 / 0.60 | 0.614 (n=19) | ✗ | 0.245 (n=2) |
| gap-knob staged sweep (2,185 combos + gap levers) | 0.51 (worse-half) | 0.683 (n=15, net −Rs 3,188) | ✗ | not triggered (TRAIN gate unmet) |

Raw baseline TRAIN PF 0.20 / TEST PF 0.14. No indicator, non-indicator, pre-momentum, filter,
guard, or SL/target combination lifts full-TRAIN PF to the 1.30 lower band, so **TEST PF > 1.40
is unreachable** and no candidate config is proposed for `final_setup_conf.py`.

Root cause is structural (see `FAILURE_ANALYSIS.md`): a 5-min-only next-open fill enters the gap
breakout ~5 minutes late, deep into an extended move that reverts — exactly the tradeoff the
source doc flags for Setup C. Fixing it needs an execution-layer change (`stop_confirm` / 1-min
entry), not a knob tune. **Keep as a research artifact; do not add to the book.**

## Rerun command (gap-knob sweep)
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\DOC5C_ORB_GAP_GO_LONG\scripts\gap_knob_sweep.py
```

---

## REINVENTION ROUND (5 bps/leg) — still **NO**

Per the follow-up ask, DOC5C was **reinvented** (new rules/structure, not knob-tuning) and
re-evaluated at **5 bps/leg**. Three enter-at-value gap detectors were built and scanned
(`scripts/scan_doc5c_reinvent.py` → `reinvent_pool/`, 861 candidates): RETEST_HOLD, RECLAIM,
PULLBACK_HOLD. RETEST_HOLD is strongest and yielded the first **genuine in-sample edge** of the
whole effort — `retest_depth_atr ≥ 0.4–0.5` (buy the hold after a real ≥0.5 ATR pullback to the
ORH), dialable to **full-TRAIN PF ∈ [1.30,1.70]** with healthy FIT/VAL, day-block p ≈ 0.00, and
no single trade/day/symbol dominating.

**But all 6 band-eligible configs fail the mandated TEST holdout (Jun 22–30): best TEST PF 0.808,
the rest 0.48–0.65, every one net-negative.** 5 bps vs 15 bps barely moved it — the failure is
directional (the OOS window has no gap-long edge), not cost. Reaching TEST > 1.40 would require
fitting to the 12–18 TEST trades (forbidden). **Approval remains NO.** Full write-up +
reproduce commands: `REINVENT_RESULT.md`.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**