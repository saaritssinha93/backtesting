# A_PULLBACK_C2_THEN_BREAK_C2_LOW Final Summary

Decision: USER_REPROMOTED_WATCHLIST.

Reason: the raw setup fails badly, but the user chose to keep the stricter high-quality + ADX subset after the 1-month replay improved to 30 trades / PF 3.491.

## Evidence

Baseline:

| Period | Trades | PF | Net PnL |
|---|---:|---:|---:|
| TRAIN 2026-05-25..2026-06-05 | 236 | 0.771 | -27,576 |
| TEST 2026-06-08..2026-06-12 | 101 | 0.402 | -39,073 |

Best non-accepted candidate:

| Config | TRAIN n/PF/net | TEST n/PF/net | Verdict |
|---|---:|---:|---|
| `quality_score >= 123.7606`, SL/Tgt 1.20/1.50 | 54 / 1.507 / 10,928 | 10 / 4.069 / 5,856 | Reject: tiny TEST sample, TRAIN p=0.2411 |
| `quality_score >= 105`, SL/Tgt 1.20/1.50 | 80 / 0.923 / -3,241 | 23 / 1.301 / 2,957 | Reject: TRAIN PF below 1.2 |
| `quality_score >= 123.7606` + `sig5_adx_calc >= 21.4683` | 37 / 2.210 / 13,906 | 8 / 3.195 / 4,188 | User-kept after follow-up replay |

Follow-up last-1-month replay through available pool data:

| Window | Trades | PF | Net PnL | Day-block p |
|---|---:|---:|---:|---:|
| Requested 2026-05-30..2026-06-29, available 2026-06-01..2026-06-24 | 30 | 3.491 | 14,750 | 0.0547 |

## Live/Backtest Mismatch

Conf/research definition is now `quality_score >= 123.7606`, `sig5_adx_calc >= 21.4683`, exit 1.20/1.50. The live overlay path has historically used an extra `market_abs_ret_pct <= 0.84` style filter plus A/B quality-top-slot gate and different exit behavior; use the final-conf path for this setup.

## Final Action

- Re-promoted in config as a watchlist setup.
- Config: `quality_score >= 123.7606`, `sig5_adx_calc >= 21.4683`, SL/Tgt 1.20/1.50.
- Monitor paper/live-paper before sizing.

## Artifacts

- Baseline: `A_PULLBACK_C2_THEN_BREAK_C2_LOW_baseline.md`
- Loop log: `A_PULLBACK_C2_THEN_BREAK_C2_LOW_experiment_log.md`
- Best config note: `A_PULLBACK_C2_THEN_BREAK_C2_LOW_best_config.md`
- Raw loop metrics: `A_PULLBACK_C2_THEN_BREAK_C2_LOW_loop_metrics.csv`
- Per-trade details: `A_PULLBACK_C2_THEN_BREAK_C2_LOW_loop_details.json`
