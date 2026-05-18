# AVWAP 5-Min Momentum v1 Result Comparison

Generated from the CSV outputs in `outputs_avwap_mom_v1_5min`.

## Executive Read

All 10 bps experiment runs are net-negative. The best 10 bps run is `step3a_quality`, but it is still below breakeven:

| Run | Trades | Days | Win % | Target % | SL % | EOD % | Net P&L Rs | ROC % | Expectancy Rs | PF | Max DD Rs |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `step0_baseline` | 20 | 20 | 40.00 | 40.00 | 60.00 | 0.00 | -1,493.16 | -14.93 | -74.66 | 0.706 | -2,960.84 |
| `step1a_ret3` | 67 | 57 | 43.28 | 43.28 | 56.72 | 0.00 | -3,097.80 | -30.98 | -46.24 | 0.807 | -3,800.62 |
| `step1b_ret2p5` | 129 | 94 | 42.64 | 42.64 | 55.81 | 1.55 | -5,935.38 | -59.35 | -46.01 | 0.806 | -5,587.19 |
| `step3a_quality` | 64 | 54 | 43.75 | 43.75 | 56.25 | 0.00 | -2,702.60 | -27.03 | -42.23 | 0.823 | -2,956.47 |

`step3a_quality` is the best of the 10 bps folders by expectancy, profit factor, Sharpe, and net loss control. It improves on `step1a_ret3` by cutting 3 trades and reducing loss by about Rs 395, but the edge is still negative.

## Cost Sensitivity

The strategy is very cost-sensitive. At 10 bps, average winners are around Rs 448 and average losers are around -Rs 423. That implies a breakeven win rate of roughly 48.6%.

All 10 bps runs have win rates between 40.0% and 43.75%, so none are close enough to breakeven.

## Run Notes

| Run | Read |
|---|---|
| `step0_baseline` | Too few trades and weakest win rate. Not tradable as-is. |
| `step1a_ret3` | More coverage than baseline, but still negative expectancy. |
| `step1b_ret2p5` | Most trades, but lower threshold adds volume without improving edge. Worst total loss and worst day. |
| `step3a_quality` | Best current candidate, but still loses after realistic cost. Needs a stronger filter or better exit model before live consideration. |

## Reporting Caveats Found

The `Max DD (% of peak)` field in `summary.txt` is unreliable when cumulative P&L starts near zero or goes negative. Use `Max drawdown` in rupees instead.

The top-level `outputs_avwap_mom_v1_5min/trades.csv` has only 4 trades at 0 bps and is older than the current script. It should not be compared directly with the 10 bps experiment folders unless rerun with the same code, universe, and cost.

