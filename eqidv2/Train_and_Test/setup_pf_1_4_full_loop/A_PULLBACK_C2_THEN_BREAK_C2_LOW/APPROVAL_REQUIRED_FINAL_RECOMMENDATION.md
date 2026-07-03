# A_PULLBACK_C2_THEN_BREAK_C2_LOW - APPROVAL_REQUIRED_FINAL_RECOMMENDATION

Approval recommendation: NO

## Baseline Reference (No Passing Candidate)

```json
{
  "side": "SHORT",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 1.5
  },
  "mask_terms": [
    [
      "quality_score",
      ">=",
      123.7606
    ]
  ],
  "pre_momentum_terms": [
    [
      "sig5_adx_calc",
      ">=",
      21.4683
    ]
  ],
  "entry_guards": {},
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Metrics

- No passing staged candidate. Baseline retained for reference only.
- Baseline TRAIN: n=238 PF=0.538 net=Rs-77,654 win%=36.6 avgW=Rs1,039 avgL=Rs-1,113 SL/TGT/EOD=103/62/73 tpd=5.41 domT/D/S=0.014/9.99/9.99
- Baseline TEST: n=64 PF=0.897 net=Rs-3,443 win%=50.0 avgW=Rs932 avgL=Rs-1,040 SL/TGT/EOD=19/19/26 tpd=4.27 domT/D/S=0.042/9.99/9.99

## Final File That Would Need Approval Before Edit

- `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\final_setup_conf.py`

## Proposed Patch

- Do not apply automatically. If approved, replace only this setup block with the JSON-equivalent block above.

DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES

## Rerun Commands

```powershell
python Train_and_Test\setup_pf_1_4_full_loop\A_PULLBACK_C2_THEN_BREAK_C2_LOW\scripts\staged_rescue_sweeps.py --top_mask_terms 50 --top_pm_terms 25 --max_configs 6000 --min_split_trades 5
```

## Risk Notes

- TRAIN sessions: 2026-03-02..2026-05-29 (53).
- TEST sessions: 2026-06-01..2026-06-24 (17).
- 5-minute enriched filters require the same feature fields to be available before any live promotion.
- No live trades, order placement, or final config edits were performed.