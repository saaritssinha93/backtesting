# A_PULLBACK_C2_THEN_BREAK_C2_LOW Best Config

Final decision: user re-promoted watchlist config on 2026-06-29.

Live/research state: kept in `FINAL_SETUP_CONF` with the high-quality + ADX gate below. Treat as paper/live-paper watchlist before sizing.

## Kept Config

This is the config kept for the setup:

```python
{
    "setup": "A_PULLBACK_C2_THEN_BREAK_C2_LOW",
    "side": "SHORT",
    "exit": {"sl_pct": 1.20, "tgt_pct": 1.50},
    "mask_terms": [["quality_score", ">=", 123.7606]],
    "pre_momentum_terms": [["sig5_adx_calc", ">=", 21.4683]],
    "entry_guards": {},
}
```

Metrics:

| Period | Trades | Win % | Gross profit | Gross loss | Net PnL | PF | Avg win | Avg loss | Max DD | Day block p | Outcomes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| TRAIN | 54 | 59.26 | 32,473 | 21,545 | 10,928 | 1.507 | 1,015 | -979 | -7,514 | 0.2411 | TARGET 23, EOD 19, SL 12 |
| TEST | 10 | 80.00 | 7,764 | 1,908 | 5,856 | 4.069 | 971 | -954 | -1,908 | 0.0000 | EOD 5, TARGET 4, SL 1 |

Why watchlist rather than fully accepted:

- Original split TEST had only 8 trades after adding ADX, so this remains sample-thin.
- Latest 1-month replay improved to 30 trades, PF 3.491, net Rs 14,750, day-block p 0.0547, but it overlaps prior tuning data and only runs through available pool date 2026-06-24.
- The threshold is an extreme quality tail; monitor paper/live-paper before sizing.

## Stronger But Thinner Candidate

`quality_score >= 123.7606` plus `sig5_adx_calc >= 21.4683`:

| Period | Trades | PF | Net PnL | Day block p |
|---|---:|---:|---:|---:|
| TRAIN | 37 | 2.210 | 13,906 | 0.0932 |
| TEST | 8 | 3.195 | 4,188 | 0.0000 |

This is the user-kept config.

## Config Files Changed

Changed `final_setup_conf.py` and `Train_and_Test/final_setup_conf.py`: updated `mask_terms`, `pre_momentum_terms`, provenance, and removed this setup from `_LIVE_SURVIVAL_DEMOTION_2026_06_29`.
