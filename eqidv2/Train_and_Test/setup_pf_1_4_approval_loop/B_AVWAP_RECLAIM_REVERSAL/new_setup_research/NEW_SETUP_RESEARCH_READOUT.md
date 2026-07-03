# B_AVWAP Replacement Research - Same Principle, Different Trigger

No setup was promoted. No edits were made to `final_setup_conf.py` or
`Train_and_Test/final_setup_conf.py`.

## Goal

Replace `B_AVWAP_RECLAIM_REVERSAL` with a different setup that still uses the
same principle: a long entry after price reclaims intraday VWAP from below.

The failed old setup buys the first reclaim bar. The new research variants wait
for confirmation after the reclaim.

## New Research Variants Scanned

All variants were scanned from corrected 5-minute session VWAP data over
2026-05-18 through 2026-06-24.

1. `B_AVWAP_CONFIRMED_RECLAIM_LONG`
   - Reclaim happens first.
   - Later bar holds near VWAP and breaks upward.
   - Strong close, limited upper wick, not extended from VWAP.

2. `B_AVWAP_RECLAIM_PULLBACK_BREAK_LONG`
   - Broader version of the confirmed reclaim.
   - Allows a looser VWAP pullback/hold and confirmation break.

3. `B_AVWAP_RECLAIM_RS_MOMENTUM_LONG`
   - Reclaim followed by a strong RS momentum break.

4. `B_AVWAP_RECLAIM_RANGE_BREAK_LONG`
   - Reclaim, then near-VWAP compression, then range break.

Family pool:

- total candidates: 190
- confirmed reclaim: 54
- pullback break: 80
- RS momentum: 44
- range break: 12

## Baseline Exit Grid Result

Best raw/baseline result by setup, 5 bps:

| Setup | Best Exit | TRAIN | TEST | Verdict |
|---|---:|---:|---:|---|
| `B_AVWAP_CONFIRMED_RECLAIM_LONG` | SL 0.50 / Tgt 2.00 | 48 trades, PF 1.3610, net Rs 4,996 | 6 trades, PF 0.3463, net Rs -1,396 | Reject |
| `B_AVWAP_RECLAIM_PULLBACK_BREAK_LONG` | SL 1.00 / Tgt 1.25 | 68 trades, PF 1.0848, net Rs 1,801 | 12 trades, PF 0.4501, net Rs -3,340 | Reject |
| `B_AVWAP_RECLAIM_RS_MOMENTUM_LONG` | SL 0.45 / Tgt 2.00 | 41 trades, PF 0.4041, net Rs -9,141 | 3 trades, PF 0.4004, net Rs -695 | Reject |
| `B_AVWAP_RECLAIM_RANGE_BREAK_LONG` | SL 1.00 / Tgt 1.25 | 11 trades, PF 0.7536, net Rs -933 | 1 trade, PF inf, net Rs 1,114 | Reject, too thin |

## Optimizer Checks

### `B_AVWAP_CONFIRMED_RECLAIM_LONG`

- Baseline @5 bps: TRAIN 48 trades, PF 1.1166, net Rs 1,875
- Baseline @5 bps: TEST 6 trades, PF 0.2659, net Rs -2,042
- 300-trial approval loop: 0 passing candidates
- Best FIT/VAL config still failed TEST gate.

### `B_AVWAP_RECLAIM_PULLBACK_BREAK_LONG`

- Baseline @5 bps: TRAIN 68 trades, PF 1.0350, net Rs 765
- Baseline @5 bps: TEST 12 trades, PF 0.2657, net Rs -4,170
- 200-trial approval loop: 0 passing candidates
- Best FIT/VAL config still failed TEST gate.

## Conclusion

Yes, we can define cleaner same-principle replacements, but none currently
works on the recent approval window.

The old setup and the new confirmation variants all show the same OOS behavior:
VWAP reclaim longs are getting faded/stopped in the June TEST regime. Requiring
confirmation improves the idea structurally, but it does not rescue the out-of-
sample result.

Recommendation:

- Keep `B_AVWAP_RECLAIM_REVERSAL` parked.
- Do not promote the new variants yet.
- If this theme is revisited, use a broader historical rebuild and require at
  least 20 TEST trades before approval, or flip the research question to the
  opposite side: failed VWAP reclaim / VWAP rejection short.

## Artifacts

- `scripts/scan_confirmed_vwap_reclaim_long.py`
- `scripts/scan_vwap_reclaim_family.py`
- `family_pool/historical_all_available_pre_dedupe_live_candidates.csv`
- `family_baseline_exit_grid.json`
- `confirmed_pf_band_5bps/`
- `pullback_break_pf_band_5bps/`
