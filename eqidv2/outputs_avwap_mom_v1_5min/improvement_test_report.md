# AVWAP 5-Min Momentum v1 Improvement Test Report

Backend tests completed against `avwap_5min_mom_v1_backtesting.py` with 10 bps round-trip cost.

## Backend Fix Applied

Updated `avwap_5min_mom_v1_backtesting.py` so parquet files missing `date_only` no longer error. The loader now reads OHLCV columns, derives `date_only` from `date` when absent, and normalizes existing `date_only` values. This removed the repeated `SANGHIIND` skip seen during sweeps.

Also updated the script defaults to the practical recommended setup:

```text
RET_5M_MIN = 3.0
PREV_TOTAL_MAX = 1.0
DEFAULT_COST_BPS = 10.0
```

Verification run with no strategy overrides:

```text
outputs_avwap_mom_v1_5min/patched_default_recommended
11 trades, 72.73% win, Rs 2,313.79 net, Rs 210.34 expectancy, PF 2.821
```

## Best Tested Candidates

| Rank | Output folder | Key changes | Trades | Win % | Net Rs | Expectancy Rs | PF | Max DD Rs | 20 bps stress Rs |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | `patched_ret3_prev1_avwap3` | `ret_min=3`, `prev_total_max=1.0`, `avwap_dist_max=3.0` | 8 | 87.50 | 2,713.70 | 339.21 | 7.426 | -422.33 | 2,315.57 |
| 2 | `patched_ret3_prev1` | `ret_min=3`, `prev_total_max=1.0` | 11 | 72.73 | 2,313.79 | 210.34 | 2.821 | -424.39 | 1,766.03 |
| 3 | `patched_ret3_prev1_tgt12` | `ret_min=3`, `prev_total_max=1.0`, target `1.2%`, SL `0.75%` | 11 | 63.64 | 2,137.46 | 194.31 | 2.261 | -847.99 | 1,589.70 |
| 4 | `patched_ret3_avwap3` | `ret_min=3`, `avwap_dist_max=3.0` | 10 | 70.00 | 1,866.09 | 186.61 | 2.469 | -424.84 | 1,368.24 |

Full comparison CSV: `experiment_comparison_all_backend.csv`.

## Recommendation

Use `patched_ret3_prev1` as the practical candidate, not the highest-PF result.

Reason: `patched_ret3_prev1_avwap3` has the best stats, but only 8 trades. That is too small to trust as the main configuration. `patched_ret3_prev1` has 11 trades, positive first-half and second-half P&L, survives 20 bps cost stress, and keeps drawdown near one stopped trade.

Suggested config:

```text
RET_5M_MIN = 3.0
PREV_TOTAL_MAX = 1.0
TARGET_PCT = 1.0
SL_PCT = 0.75
AVWAP_DIST_MAX_PCT = 4.5
```

Optional stricter variant for paper tracking only:

```text
RET_5M_MIN = 3.0
PREV_TOTAL_MAX = 1.0
AVWAP_DIST_MAX_PCT = 3.0
TARGET_PCT = 1.0
SL_PCT = 0.75
```

## What Improved Results

The strongest improvement came from avoiding already-extended moves before the signal:

```text
prev_5_total_ret < 1.0
```

This changed the current-code `ret_min=3` run from:

```text
19 trades, 57.89% win, Rs 1,535.29 net, Rs 80.80 expectancy
```

to:

```text
11 trades, 72.73% win, Rs 2,313.79 net, Rs 210.34 expectancy
```

Tight AVWAP distance also helped, but it cut the sample heavily. Treat that as a quality overlay, not production default yet.

## What Did Not Help Enough

| Change | Result |
|---|---|
| `ret_min=2.5` | More trades, edge almost disappears: Rs 33.04 net across 44 trades. |
| `vol_min=5`, `vol_max=12` | Too neutral: Rs 193.03 net across 16 trades. |
| `body_pct_min=0.85` | Profitable but weaker than prior-momentum filtering. |
| target `0.8%`, SL `0.5%` | Faster exits and lower DD, but lower total P&L than default exit. |
| target `1.2%`, SL `0.75%` | Good, but lower win rate and worse DD than default exit on the practical candidate. |

## Next Backend Step

Before promoting to live/paper defaults, run a walk-forward split by date or month. Current best candidates are profitable, but the sample is still small. The next test should validate that `prev_total_max=1.0` remains useful outside this exact sample.
