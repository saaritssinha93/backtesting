# DOC5B_MOMO_BREAKOUT_LONG - Retest V3 Rescan

_Generated 2026-07-01. Research-only; no live trades; no final config edits._

## Why This Was Run

Direct DOC5B breakout entries failed even after adding real `rs_rank` and breadth. This v3 scan tests the next structural idea: do not buy the breakout candle; wait for the first controlled retest/hold after a strong breakout.

The scanner tracks breakout states, then emits a candidate only when price:

- Pulls back toward the breakout reference.
- Holds/reclaims above that level.
- Remains above session VWAP.
- Keeps acceptable RS/breadth context.

## Pool

- Scanner: `Train_and_Test/doc5_long_setups/scan_doc5b_retest_v3.py`
- Pool: `Train_and_Test/doc5_long_setups/pool_retest_v3/historical_all_available_pre_dedupe_live_candidates.csv`
- Window scanned: 2026-05-15..2026-06-30
- Universe: 204 F&O tickers
- Output rows after detector/probe gate: 74
- TRAIN rows before entry attachment: 50
- TEST rows before entry attachment: 19

Feature distribution in the emitted pool:

| feature | mean | median | p75 |
|---|---:|---:|---:|
| `rs_rank` | 0.885 | 0.900 | 0.950 |
| `breadth_above_vwap` | 0.542 | 0.544 | 0.603 |
| `retest_depth_atr` | 0.130 | 0.113 | 0.391 |
| `pullback_from_breakout_high_atr` | 0.853 | 0.699 | 1.217 |
| `breakout_age_bars` | 3.23 | 3.00 | 4.75 |

## Main V3 Optimization

Command:

```powershell
py -3.12 Train_and_Test\setup_pf_1_4_approval_loop\_engine\pf_band_fitval_loop.py --setup DOC5B_MOMO_BREAKOUT_LONG --pool Train_and_Test\doc5_long_setups\pool_retest_v3 --trials 700 --time_budget_min 10.0 --seed 23 --train_start 2026-05-18 --test_start 2026-06-20 --gap_lambda 0.8 --max_mask_terms 2 --max_pm_terms 2 --test_pf_min 1.4 --max_test_day_block_p 0.1 --min_train_target_rate 12.0 --neighborhood_pf_min 1.15 --dropout_pf_min 1.0 --pm_quantile_sample 1500 --out Train_and_Test\setup_pf_1_4_approval_loop\DOC5B_MOMO_BREAKOUT_LONG\deep_runs\retest_v3_seed23
```

Best selected config:

```json
{
  "side": "LONG",
  "exit": {
    "sl_pct": 1.5,
    "tgt_pct": 0.8
  },
  "mask_terms": [
    ["retest_depth_atr", ">=", 0.350946]
  ],
  "pre_momentum_terms": [
    ["sig5_adx_calc", ">=", 21.266639]
  ],
  "entry_guards": {
    "min_slot": "10:00",
    "max_slot": "12:30"
  },
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

| window | trades | PF | net PnL | win% | trades/day | day-block p |
|---|---:|---:|---:|---:|---:|---:|
| TRAIN | 12 | 1.816 | Rs2,384 | 83.3 | 1.50 | 0.1455 |
| TEST | 4 | 0.000 | Rs-5,857 | 0.0 | 2.00 | n/a |

Reject reasons:

- TRAIN too few trades.
- TRAIN PF above the preferred 1.70 ceiling.
- TEST too few trades.
- TEST PF failed badly.
- Robustness checks failed.

## Train-Band Rescore

Strict audit (`n >= 20`):

- Trial rows read: 700.
- Unique configs rescored on TRAIN: 444.
- Meaningful TRAIN-band configs with TRAIN PF 1.30-1.70: 0.
- Best `n >= 20` TRAIN rescore: PF 1.141 over 20 trades, below the band.

Exploratory thin audit (`n >= 12`):

- Train-band configs found: 1.
- Candidate: TRAIN PF 1.303 over 12 trades, net Rs1,103.
- TEST: 4 trades, PF 0.000, net Rs-4,364.
- Verdict: rejected; too thin and failed OOS.

## Conclusion

The retest structure improved the in-sample shape, especially around `retest_depth_atr >= 0.35`, but it did not produce a meaningful, robust, OOS-positive candidate. The edge only appears when the sample shrinks to 12 trades or fewer, and those pockets collapse on TEST.
