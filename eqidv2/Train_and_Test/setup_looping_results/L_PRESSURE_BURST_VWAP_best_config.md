# L_PRESSURE_BURST_VWAP — Best Config

**No viable config. No change recommended. Keep parked — no defensible edge.**

```
exit: 0.70/1.25 ; mask quality_score≤25 ; premom pre1_adx≥44   (conf of record — least-bad, still a loser)
```

| Config | 5 bps TRAIN/TEST | 15 bps TRAIN/TEST |
|---|---|---|
| conf gate | 0.84 / 0.79 | 0.51 / 0.39 |
| best @5 bps (exit 0.9/1.5) | 0.95 / 0.79 | 0.54 / 0.46 |
| ungated firehose | 0.32–0.61 | 0.32 | 

## Why no change
- **Loser at every gate/exit/slippage** on a LARGE, robust sample (n=138 train / 67 test) — not noise.
- The conf gate barely beats the ungated firehose (TRAIN 0.51 vs 0.32) → almost no edge from the gate.
- Loosening, flipping quality, or moving the ADX threshold all make it worse; pre1_adx is non-monotonic (confirmed).

## Recommendation
Keep parked permanently absent a new thesis. This is the weakest setup in the book and the doc's
USER_APPROVED_OVERRIDE_WEAK label is vindicated. No `final_setup_conf.py` change.
