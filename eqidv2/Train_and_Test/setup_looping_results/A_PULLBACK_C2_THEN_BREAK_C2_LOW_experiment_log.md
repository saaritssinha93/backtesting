# A_PULLBACK_C2_THEN_BREAK_C2_LOW Experiment Log

Loop command:

```powershell
python Train_and_Test\setup_looping_results\run_A_PULLBACK_C2_THEN_BREAK_C2_LOW_loop.py
```

Decision rule used by the loop: run TEST only when TRAIN PF improved over baseline PF 0.771 and retained at least 30 TRAIN trades. Final acceptance still required robust TRAIN PF, preferably TEST PF >= 1.3, reasonable TEST count, and no day/symbol domination.

Full machine-readable metrics: `A_PULLBACK_C2_THEN_BREAK_C2_LOW_loop_metrics.csv`

Full per-trade detail: `A_PULLBACK_C2_THEN_BREAK_C2_LOW_loop_details.json`

| Iter | Group | Variant | TRAIN n | TRAIN PF | TRAIN net | TEST n | TEST PF | TEST net | Decision |
|---:|---|---|---:|---:|---:|---:|---:|---:|---|
| 0 | baseline | `current_conf_raw_1p20_1p50` | 236 | 0.771 | -27,576 | 101 | 0.402 | -39,073 | BASELINE |
| 1 | exit | `tighter_scalp_0p70_1p00` | 335 | 0.535 | -73,256 | - | - | - | TRAIN reject |
| 2 | exit | `balanced_0p85_1p00` | 306 | 0.568 | -62,434 | - | - | - | TRAIN reject |
| 3 | exit | `balanced_0p90_1p25` | 271 | 0.681 | -42,745 | - | - | - | TRAIN reject |
| 4 | exit | `small_target_1p20_1p00` | 269 | 0.581 | -55,290 | - | - | - | TRAIN reject |
| 5 | exit | `runner_1p20_2p00` | 219 | 0.679 | -37,595 | - | - | - | TRAIN reject |
| 6 | time | `min_0945` | 236 | 0.771 | -27,576 | - | - | - | TRAIN reject |
| 7 | time | `min_1000` | 236 | 0.771 | -27,576 | - | - | - | TRAIN reject |
| 8 | time | `max_1230` | 176 | 0.812 | -17,137 | 46 | 0.165 | -34,035 | REJECT_TEST_COLLAPSE |
| 9 | time | `max_1300` | 190 | 0.783 | -22,033 | 68 | 0.319 | -33,689 | REJECT_TEST_COLLAPSE |
| 10 | trend | `market_aligned_le_0` | 193 | 0.691 | -31,824 | - | - | - | TRAIN reject |
| 11 | trend | `market_down_le_neg_0p05` | 173 | 0.724 | -23,266 | - | - | - | TRAIN reject |
| 12 | trend | `market_abs_le_0p56` | 164 | 0.934 | -5,009 | 101 | 0.402 | -39,073 | REJECT_TEST_COLLAPSE |
| 13 | trend | `rs_lagger_le_neg_0p50` | 204 | 0.637 | -40,498 | - | - | - | TRAIN reject |
| 14 | trend | `rs_lagger_le_neg_1p00` | 192 | 0.681 | -32,720 | - | - | - | TRAIN reject |
| 15 | volume | `vol_min_2p0` | 225 | 0.760 | -27,115 | - | - | - | TRAIN reject |
| 16 | volume | `vol_min_3p0` | 187 | 0.859 | -11,723 | 91 | 0.357 | -38,563 | REJECT_TEST_COLLAPSE |
| 17 | volume | `vol_band_1p8_4p0` | 221 | 0.841 | -16,699 | 96 | 0.345 | -41,051 | REJECT_TEST_COLLAPSE |
| 18 | candle | `close_loc_le_0p20` | 215 | 0.708 | -31,037 | - | - | - | TRAIN reject |
| 19 | candle | `close_loc_le_0p10` | 200 | 0.671 | -32,491 | - | - | - | TRAIN reject |
| 20 | candle | `body_ge_0p75` | 209 | 0.794 | -20,518 | 100 | 0.355 | -45,201 | REJECT_TEST_COLLAPSE |
| 21 | candle | `body_ge_0p85` | 188 | 0.651 | -33,267 | - | - | - | TRAIN reject |
| 22 | candle | `range_ge_0p45` | 224 | 0.811 | -20,769 | 98 | 0.525 | -27,549 | REJECT_TEST_COLLAPSE |
| 23 | volatility | `atr_le_0p0030` | 197 | 0.841 | -13,728 | 91 | 0.324 | -39,402 | REJECT_TEST_COLLAPSE |
| 24 | volatility | `atr_band_0p0020_0p0035` | 201 | 0.776 | -20,268 | 96 | 0.446 | -35,312 | REJECT_TEST_COLLAPSE |
| 25 | vwap | `vwap_not_extended_ge_neg4` | 226 | 0.698 | -34,991 | - | - | - | TRAIN reject |
| 26 | vwap | `vwap_below_le_neg1p8` | 225 | 0.844 | -16,901 | 103 | 0.338 | -47,265 | REJECT_TEST_COLLAPSE |
| 27 | vwap | `vwap_band_neg4_to_neg1p5` | 203 | 0.748 | -24,573 | - | - | - | TRAIN reject |
| 28 | quality | `quality_ge_75` | 181 | 0.697 | -29,720 | - | - | - | TRAIN reject |
| 29 | quality | `quality_ge_90` | 130 | 0.817 | -12,710 | 44 | 0.607 | -10,271 | REJECT_TEST_COLLAPSE |
| 30 | quality | `quality_ge_105` | 80 | 0.923 | -3,241 | 23 | 1.301 | 2,957 | WATCHLIST only; TRAIN PF failed |
| 31 | quality | `quality_ge_123p76` | 54 | 1.507 | 10,928 | 10 | 4.069 | 5,856 | WATCHLIST only; TEST sample too small |
| 32 | confirmation | `sig5_adx_ge_20` | 201 | 0.958 | -4,021 | 94 | 0.408 | -34,352 | REJECT_TEST_COLLAPSE |
| 33 | confirmation | `sig5_adx_ge_25` | 185 | 0.809 | -17,487 | 84 | 0.521 | -21,382 | REJECT_TEST_COLLAPSE |
| 34 | confirmation | `pre5_mom_ge_0p10` | 208 | 0.656 | -38,060 | - | - | - | TRAIN reject |
| 35 | confirmation_on_quality | `quality_123p76_plus_sig5_adx_21p47` | 37 | 2.210 | 13,906 | 8 | 3.195 | 4,188 | WATCHLIST only; TEST n=8 |

## Keep/Reject Notes

- Exit-only changes were all worse on TRAIN, so none reached TEST.
- Time caps slightly improved TRAIN but collapsed badly on TEST.
- Trend, RS, volume, candle, volatility, and VWAP filters did not produce a robust edge. Most remained below TRAIN PF 1.0.
- `quality_score >= 105` had TEST PF 1.301 on 23 trades, but TRAIN PF was only 0.923, so it fails the TRAIN acceptance rule.
- `quality_score >= 123.7606` passed TRAIN PF with 54 trades and TEST PF looked strong, but TEST n=10 and TRAIN day-block p=0.241 are too thin/unstable for a high-frequency setup.
- Adding `sig5_adx_calc >= 21.4683` improved TRAIN PF but reduced TEST to 8 trades, so it is not deployable.
