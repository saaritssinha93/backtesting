# DOC5B_MOMO_BREAKOUT_LONG (LONG) — APPROVAL_REQUIRED / FINAL RECOMMENDATION

_Generated 2026-07-01._

## Approval recommendation: **NO**

- Verdict: **REJECT**  (TRAIN PF too low (<1.30); TRAIN target-fill rate below 12.0%; TRAIN concentrated (one trade/day/symbol dominates); TRAIN too many trades/day; neighborhood robustness failed; term-dropout robustness failed; TEST PF below 1.40; TEST day-block p above 0.10; TEST concentrated (one trade/day/symbol dominates); TEST too many trades/day)

## Best candidate config (proposed)

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 2.0
  },
  "mask_terms": [
    [
      "signal_range_pct",
      "<=",
      0.388188
    ]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {
    "max_slot": "12:30",
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

## Metrics

| window | @15 bps/leg | @5 bps/leg |
|---|---|---|
| TRAIN | n=125 PF=0.488 net=Rs-37,044 win%=36.0 avgW=Rs786 avgL=Rs-905 maxDD=Rs-39,690 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.25 tradeDom=0.05 dayDom=9.99 symDom=9.99 dbp=0.9818 | n=125 PF=0.488 net=Rs-37,044 win%=36.0 avgW=Rs786 avgL=Rs-905 maxDD=Rs-39,690 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=6.25 tradeDom=0.05 dayDom=9.99 symDom=9.99 dbp=0.9818 |
| TEST  | n=48 PF=0.211 net=Rs-38,729 win%=22.9 avgW=Rs941 avgL=Rs-1,326 maxDD=Rs-37,509 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=8.0 tradeDom=0.171 dayDom=9.99 symDom=9.99 dbp=0.9864 | n=48 PF=0.211 net=Rs-38,729 win%=22.9 avgW=Rs941 avgL=Rs-1,326 maxDD=Rs-37,509 SL/TGT/EOD=0/0/0 tgt%=0.0 tpd=8.0 tradeDom=0.171 dayDom=9.99 symDom=9.99 dbp=0.9864 |

## No promotion proposed

- This setup did **not** clear the robust TRAIN+TEST gate. No edit to `final_setup_conf.py` is recommended.
- `INSUFFICIENT_OOS` means the train-side config was not confirmed or rejected because the OOS sample lacked enough statistical power.
- Expanded follow-up search also failed: 600 trials with up to 2 mask terms and 2 pre-momentum terms produced best TRAIN PF 0.763 / TEST PF 0.662.
- Full TRAIN rescore of 453 unique tried configs found zero meaningful train-band configs (`n >= 20`, TRAIN PF 1.30-1.70), so no candidate legitimately reached TEST acceptance.
- RS/breadth v2 detector repair also failed: 353-row refined pool, 800 trials, best TRAIN PF 0.653 / TEST PF 0.329.
- V2 rescore found zero `n >= 20` train-band configs; the only exploratory `n >= 15` near-miss had TRAIN PF 1.636 over 18 trades but TEST PF 0.000 over 5 trades.
- Retest v3 detector also failed: 74-row pool, 700 trials, best selected config had TRAIN PF 1.816 over 12 trades and TEST PF 0.000 over 4 trades.
- V3 rescore found zero `n >= 20` train-band configs; exploratory `n >= 12` produced TRAIN PF 1.303 over 12 trades but TEST PF 0.000 over 4 trades.

> **DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES**

## Rerun command
```
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool Train_and_Test\doc5_long_setups\pool --trials 200 --time_budget_min 10.0 --seed 7 --gap_lambda 0.8 --max_mask_terms 1 --max_pm_terms 1 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500
```

## Remaining risks

- TEST sample = 48 trades / 6 day(s) (thin June data).
- Robustness: neighborhood=False, dropout=False.
- TEST concentration: top-day 9.99, top-symbol 9.99, top-trade 0.171 (cap 0.5).
- Native/screening vs live-faithful: readmit setups are live-faithful; others are screening-only (confirm on the v11 conf backtest before any live use).
