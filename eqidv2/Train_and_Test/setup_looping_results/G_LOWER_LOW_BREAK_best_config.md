# G_LOWER_LOW_BREAK — Best Config

**Best research candidate found in the whole active-book audit** (but still REJECT for *sizing* at realistic cost).

## Current conf gate (too selective → uncertifiable on fresh window: n=6/6)
```
exit: 1.10/1.00 ; mask vol_ratio≥4.129044 & quality_score≥76.444124 ; premom sig5_rsi_dir≥68.747209
```

## Recommended RESEARCH config (i03) — WATCH, do NOT size yet
```
exit:               SL 1.10 / Tgt 1.00
mask_terms:         vol_ratio    >= 3.0      (loosened from 4.129044 — gains tradeable sample)
                    quality_score >= 50.0    (loosened from 76.444124)
pre_momentum_terms: sig5_rsi_dir >= 68.747209   (KEEP — load-bearing; dropping it kills the edge)
entry_guards:       {}
```

| Slippage | TRAIN n/PF | TEST n/PF | distribution |
|---|---:|---:|---|
| 5 bps/leg (paper) | 31 / **3.07** (dbp 0.021) | 25 / **1.40** (dbp 0.28) | top1day 0.46 / 0.90 — well-distributed |
| 15 bps/leg (realistic) | 31 / 1.66 | 25 / **0.93** | TEST a slight loser |

## Why WATCH (not accept, not flat-reject)
- **Passes the full acceptance bar at paper cost** with a healthy, well-distributed sample — the only config in
  the four-short audit to do so.
- **Fails at realistic 15 bps/leg** (TEST 0.93) and the paper-cost TEST significance is weak (dbp 0.28, n=25).
- **But the slippage assumption is genuinely favourable here:** vol_ratio≥3 selects volume-climax bars, which are
  the most liquid moments — true fills likely beat the 15 bps generic-small-cap assumption. The verdict literally
  hinges on measured climax-bar fills.

## Recommendation
- **No `final_setup_conf.py` change.** Keep G_LOWER_LOW_BREAK unsized.
- **Action item:** forward paper-trade config i03 and **measure actual fill slippage on the climax bars**. If real
  fills land ≤ ~8 bps/leg and the TEST edge holds on more data, i03 is a re-promotion candidate. This is the one
  lead worth live-watching out of the four active shorts.
