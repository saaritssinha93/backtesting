# B_AVWAP_RECLAIM_REVERSAL - Filter Logic / OOS Rescue

No config was promoted. No edits were made to `final_setup_conf.py` or
`Train_and_Test/final_setup_conf.py`.

## Problem

`B_AVWAP_RECLAIM_REVERSAL` is supposed to be a near-VWAP reclaim from below.
The recent 5 bps validation shows the current card filter is not enough:

- Current card: `vwap_dist_atr <= 1.0`, SL 0.70%, target 1.50%
- TRAIN: 573 trades, PF 0.5374, net Rs -131,275
- TEST: 60 trades, PF 0.4798, net Rs -15,844

Claude's high-momentum filter stack also failed OOS:

- TRAIN: 30 trades, PF 2.2789, net Rs 13,049
- TEST: 4 trades, PF 0.6027, net Rs -926

## Live-Reproducibility Constraint

The live/final-conf mask path only honors:

- `mask_terms`
- `entry_guards.min_slot`

It does not honor `entry_guards.max_slot` or `entry_guards.top_n`.
So the rescue search intentionally avoided `max_slot` and `top_n`.

## Rescue Search

Searched 27,337 live-reproducible combinations:

- exits around the known near-miss zones
- tighter `vwap_dist_atr` caps
- close-location caps
- volume caps
- RS bounds
- optional `min_slot`
- no pre-momentum gates

Result:

- practical passing configs: 0
- strict approval configs: 0

## Best TEST-Looking Variant Was a Mirage

```json
{
  "exit": {"sl_pct": 0.45, "tgt_pct": 2.50},
  "mask_terms": [
    ["vwap_dist_atr", "<=", 0.20],
    ["rs_pct", ">=", 0.20]
  ],
  "pre_momentum_terms": [],
  "entry_guards": {},
  "max_positions": 20,
  "daily_loss_rs": 0.0
}
```

- FIT: 58 trades, PF 0.6400, net Rs -7,766
- VAL: 44 trades, PF 0.2545, net Rs -14,429
- TRAIN: 102 trades, PF 0.4577, net Rs -22,195
- TEST: 6 trades, PF 1.5512, net Rs 785

This is not a fix. It only looks good on the 2-session TEST window while
failing badly on FIT, VAL, and full TRAIN.

## Diagnosis

Filter-only repair is not enough. The current detector is buying the first
5-minute VWAP reclaim bar, and the raw population has poor follow-through.
When filters are tightened enough to improve the two TEST sessions, they do
not hold up on TRAIN. When filters are tuned to TRAIN, OOS still fails.

The high pre-momentum / high ADX path is especially suspect: it creates
high TRAIN PF by selecting a small, excited subset, but that subset did not
repeat OOS.

## Recommended Fix

Do not approve the current setup.

The next real fix should be a detector-level rewrite, not another mask tweak:

1. Require a post-reclaim confirmation bar, such as the next 5-minute close
   holding above VWAP or breaking the reclaim candle high.
2. Keep the trade close to VWAP at confirmation; avoid selecting the most
   stretched reclaim.
3. Do not use `top_n` for this setup unless the live conf-mask path is changed,
   because `top_n` currently ranks by highest `vwap_dist_atr`, which is the
   opposite of the near-VWAP thesis.
4. Keep B_AVWAP parked until a rewritten detector creates a fresh pool and
   passes the TRAIN/TEST approval loop.

Artifacts:

- `filter_logic_rescue_small_results.json`
- `claude_exit_sweep_results.json`
- `FIVE_BPS_APPROVAL_READOUT.md`
