# DOC5B_MOMO_BREAKOUT_LONG - RS/Breadth V2 Rescan

_Generated 2026-07-01. Research-only; no live trades; no final config edits._

## Why This Was Run

The original DOC5B scanner used `rs_pct` (stock return minus NIFTYBEES return) as a proxy for the doc's cross-sectional `rs_rank`, and did not model breadth. Parameter searches on that pool found no meaningful TRAIN-band candidate.

This v2 scan rebuilds the DOC5B pool with:

- Cross-sectional `rs_rank` per 5-minute timestamp.
- Breadth above session VWAP per timestamp.
- Breadth with positive intraday return per timestamp.
- Breakout strength in ATR units.

## Pool

- Scanner: `Train_and_Test/doc5_long_setups/scan_doc5b_rs_breadth_v2.py`
- Pool: `Train_and_Test/doc5_long_setups/pool_rs_breadth_v2/historical_all_available_pre_dedupe_live_candidates.csv`
- Window scanned: 2026-05-15..2026-06-30
- Universe: 204 F&O tickers
- Output rows after detector/probe gate: 353

Feature distribution in the emitted pool:

| feature | mean | median | p75 |
|---|---:|---:|---:|
| `rs_rank` | 0.868 | 0.877 | 0.951 |
| `breadth_above_vwap` | 0.550 | 0.534 | 0.618 |
| `breadth_pos_ret` | 0.534 | 0.505 | 0.583 |
| `breakout_strength_atr` | 0.540 | 0.458 | 0.763 |

## Main V2 Optimization

Command:

```powershell
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool Train_and_Test\doc5_long_setups\pool_rs_breadth_v2 --trials 800 --time_budget_min 12.0 --seed 17 --train_start 2026-05-18 --test_start 2026-06-20 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500 --out Train_and_Test\setup_pf_1_4_approval_loop\DOC5B_MOMO_BREAKOUT_LONG\deep_runs\rs_breadth_v2_seed17
```

Best selected config:

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 2.0
  },
  "mask_terms": [
    ["quality_score", ">=", 109.099605],
    ["lower_wick_pct", ">=", 0.022843]
  ],
  "pre_momentum_terms": [
    ["pre_entry_momentum_score", "<=", 68.3712],
    ["sig5_vol_ratio20", "<=", 2.777304]
  ],
  "entry_guards": {
    "max_slot": "13:00",
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

| window | trades | PF | net PnL | win% | trades/day | day-block p |
|---|---:|---:|---:|---:|---:|---:|
| TRAIN | 29 | 0.653 | Rs-5,767 | 41.4 | 2.23 | 0.8792 |
| TEST | 10 | 0.329 | Rs-7,157 | 20.0 | 3.33 | 1.0000 |

## V2 Train-Band Rescore

Strict audit:

- Trial rows read: 800.
- Unique configs rescored on TRAIN: 736.
- Meaningful TRAIN-band configs with `n >= 20` and TRAIN PF 1.30-1.70: 0.
- TEST PF > 1.40 configs: 0.
- Best `n >= 20` TRAIN rescore: PF 1.283 over 22 trades, just below band, not accepted.

Gray-zone audit (`n >= 15`, exploratory only):

- Train-band configs found: 1.
- Candidate: TRAIN PF 1.636 over 18 trades, net Rs4,441.
- TEST: 5 trades, PF 0.000, net Rs-4,163.
- Verdict: near-miss rejected; too thin and OOS failed.

Gray-zone config:

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.2,
    "tgt_pct": 2.0
  },
  "mask_terms": [
    ["stock_ret_pct", ">=", 1.540024],
    ["close_loc", ">=", 0.831579]
  ],
  "pre_momentum_terms": [
    ["sig5_vol_ratio20", ">=", 1.959559],
    ["sig5_rsi_dir", "<=", 76.419963]
  ],
  "entry_guards": {
    "min_slot": "10:30",
    "max_slot": "13:00",
    "top_n": 2
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

## Conclusion

The RS/breadth repair made the detector more faithful and more selective, but it still did not produce a candidate that satisfies the required gate. The failure is not just a missing `rs_rank` or breadth feature; DOC5B remains a weak/noisy breakout-chase setup on this sample.
