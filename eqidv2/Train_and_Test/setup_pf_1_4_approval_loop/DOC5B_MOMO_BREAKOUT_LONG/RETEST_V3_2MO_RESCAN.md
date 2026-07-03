# DOC5B_MOMO_BREAKOUT_LONG - Retest V3 on Last 2 Months

_Generated 2026-07-01. Research-only; no live trades; no final config edits._

## Why This Was Run

Re-run the confirmed Retest V3 pipeline over the **last two months** (2026-05-01 → 2026-07-01)
to see whether the larger sample — in particular a fatter out-of-sample TEST window — changes
the earlier REJECT. The prior confirmed run used 2026-05-15 → 2026-06-30 and failed partly on a
thin OOS (4 TEST trades).

## Pool

- Scanner: `Train_and_Test/doc5_long_setups/scan_doc5b_retest_v3.py`
- Pool: `Train_and_Test/doc5_long_setups/pool_retest_v3_2mo/`
- Window scanned: 2026-05-01..2026-07-01, universe 204 F&O tickers
- Rows after detector/probe gate: **96** (vs 74 in the 1.5-month pool)
- TRAIN rows (2026-05-01..06-19): 70
- TEST rows (2026-06-20..07-01): 26

### Data caveat (important)

The train/test scoring harness (`setup_train_test.attach_entries`) **cannot resolve outcomes for
2026-07-01** yet — all 7 of that day's signals are dropped in entry-attachment (26 raw → 19
scorable). So although the scan window ends 07-01, **scorable TEST effectively ends 2026-06-30**
(sessions 06-22, 06-23, 06-25, 06-29, 06-30). TRAIN resolves fully (70/70).

## Main Fitval Selection

Command mirrors the confirmed run (700 trials, 10-min budget, seed 23), on the 2-month pool with
`--train_start 2026-05-01 --test_start 2026-06-20`.
Out: `deep_runs/retest_v3_2mo_seed23/`.

Best selected config:

```json
{
  "side": "LONG",
  "exit": {"sl_pct": 1.5, "tgt_pct": 0.8},
  "mask_terms": [["breakout_strength_atr", "<=", 0.774244], ["ranker_score", ">=", 112.177566]],
  "pre_momentum_terms": [],
  "entry_guards": {"min_slot": "09:30", "top_n": 2},
  "max_positions": 20,
  "daily_loss_rs": 4000.0
}
```

| window | trades | PF | net PnL | win% | trades/day |
|---|---:|---:|---:|---:|---:|
| TRAIN (05-04..06-19) | 24 | 0.527 | Rs-5,898 | 50.0 | 1.71 |
| TEST  (06-22..07-01) | 7  | 0.434 | Rs-2,937 | 57.1 | 1.75 |

Verdict: **REJECT** — all 8 hard reasons (TRAIN PF <1.30, target-fill <12%, TRAIN concentrated,
neighborhood + term-dropout robustness fail, TEST PF <1.40, TEST day-block p >0.10, TEST
concentrated). No "too few trades" escape hatch this time.

## Strict TRAIN-First Audit (2-month window)

Rescore of all 487 unique tried configs on full TRAIN first, TEST evaluated only for in-band
configs. Windows: TRAIN 70 entries / TEST 19 scorable entries.
Out: `deep_train_band_rescore_retest_v3_2mo_win/` (n>=20) and `..._win_min12/` (n>=12).

- Unique configs rescored: 487
- Configs in TRAIN PF band 1.30-1.70 with n>=20: **1**
- Of those, TEST PF > 1.40: **0**

Single in-band near-miss:

| | TRAIN | TEST |
|---|---:|---:|
| n | 23 | 5 |
| PF | 1.402 | **1.388** |
| net | Rs3,765 | Rs766 |

TEST PF 1.388 fails the 1.40 floor by 0.012, on just 5 trades, with `test_sym_dom` 2.286 (one
symbol dominates the OOS result). Config = generic mask (`wick_skew_pct<=0.041 & vol_ratio>=1.36`,
tgt 2.0), not the retest structure's own features.

## Conclusion

Two months of data does **not** rescue DOC5B; it makes the rejection cleaner. The optimizer's best
selectable config is now a clear negative-expectancy loser (TRAIN PF 0.527 over 24 trades) rather
than the earlier thin PF-1.82-on-12-trades illusion. The only in-band config across 487 candidates
misses the OOS floor by a whisker on a concentrated 5-trade sample. This confirms and strengthens
the earlier verdict: do not force parameters on DOC5B. It needs a structural detector redesign or
genuinely more OOS sessions (with 07-01+ outcomes resolvable), not tighter fitting.
No `final_setup_conf.py` edits made.
