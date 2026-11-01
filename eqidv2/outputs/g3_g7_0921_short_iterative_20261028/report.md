# 09:21 SHORT: G-3 and G7 V4 two-loss-lock research

**Honest conclusion: no 09:21 SHORT change in this study establishes a reliable win rate above 75% for both strategies.** The exit-only version reaches 8/9 wins in both backtests but still loses money at this slot and wins only 1/2 trades in the previously designated validation dates. The 8/8 version adds an OI threshold chosen after observing the one remaining loser. Its apparent 100% rate is a retrospective fit, and all eight trades become break-even at 15 bps modeled round-trip cost. Do not adopt either rule on this evidence. The active configurations were not changed.

The 59 available sessions end on 27 October 2026. The figures below are accurate conditional replays of the frozen selected opportunities, not an independent test of a newly configured selector or a forecast of live win rate.

The files `g3_candidate_config.json` and `g7_v4_lock2_candidate_config.json` record the rejected research hypothesis for reproducibility. They describe a **post-selection veto with no replacement** and an exit override for `0921_SHORT` only. They are not drop-in production configuration files.

## Iterations at 5 bps

P&L is after modeled costs. The portfolio columns include all slots; every variant replays portfolio admission. G7 uses conservative entry-minute-open capacity, integer cash shares, and recalculates its two-realized-loss lock.

| Slot rule | G-3 09:21 SHORT | G-3 slot net | G-3 portfolio net | G7 lock 09:21 SHORT | G7 slot net | G7 portfolio net |
|---|---:|---:|---:|---:|---:|---:|
| Current baseline | 1/9 (11.1%) | -Rs30,590.77 | Rs1,212,277.50 | 4/9 (44.4%) | -Rs18,333.37 | Rs842,866.75 |
| Protect +0.1% after +0.6% | 4/9 (44.4%) | -Rs18,590.77 | Rs1,224,277.50 | 4/9 (44.4%) | -Rs18,333.37 | Rs842,866.75 |
| Protect +0.1% after +0.4% | 6/9 (66.7%) | -Rs11,244.05 | Rs1,231,624.22 | 6/9 (66.7%) | -Rs10,999.10 | Rs851,411.95 |
| Protect +0.1% after +0.3% | 7/9 (77.8%) | -Rs4,583.88 | Rs1,238,284.40 | 7/9 (77.8%) | -Rs4,591.62 | Rs857,819.43 |
| Protect +0.1% after +0.2% | 8/9 (88.9%) | -Rs3,339.83 | Rs1,239,528.44 | 8/9 (88.9%) | -Rs3,347.66 | Rs859,063.40 |
| Protect +0.15% after +0.2% | 8/9 (88.9%) | -Rs1,250.00 | Rs1,241,618.27 | 8/9 (88.9%) | -Rs1,265.80 | Rs861,145.25 |
| OI cap +0.60% only | 1/8 (12.5%) | -Rs25,340.77 | Rs1,217,527.50 | 4/8 (50.0%) | -Rs13,083.44 | Rs848,116.69 |
| Confirmation body >=0.60 only | 1/6 (16.7%) | -Rs17,244.05 | Rs1,225,624.22 | 3/6 (50.0%) | -Rs10,733.51 | Rs850,466.61 |
| Nifty opening move <=-0.15% only | 1/5 (20.0%) | -Rs11,346.73 | Rs1,231,521.54 | 3/5 (60.0%) | -Rs6,091.84 | Rs855,108.28 |
| Protect +0.1% after +0.2% and OI cap | 8/8 (100%) | +Rs1,910.17 | Rs1,244,778.44 | 8/8 (100%) | +Rs1,902.28 | Rs864,313.33 |
| Protect +0.15% after +0.2% and OI cap | 8/8 (100%) | +Rs4,000.00 | Rs1,246,868.27 | 8/8 (100%) | +Rs3,984.13 | Rs866,395.18 |

The G-3 current stop is 1.25% and G7's is 1.20%; targets are 1.50%, with the existing timed tightening preserved. For the candidate, the completed post-entry close must reach +0.20%; protection takes effect only at the next minute open. An intrabar high/low before that close cannot arm it. The +0.60% OI cap rejects IOC on 6 October (recorded OI increase +0.6744%), the sole loss under the tighter exit. Because this cap was picked after examining that trade, its apparently perfect result is overfit evidence, not a validated selection edge.

## Chronology and October

The earlier labeled date blocks have also been used in prior research, so they are descriptive partitions, **not independent holdouts**. The simpler +0.2%/+0.1% exit-only rule has 3/3 development wins but only **1/2 validation wins**, with a -Rs5,000 slot result in validation for G-3 (about the same for G7). The OI cap then removes that validation loss. Under the resulting +0.2%/+0.15% plus OI-cap fit, each strategy shows 3/3 development wins, 1/1 validation win, 3/3 assessment wins, and 1/1 on 26 October. Those are not prospective validations because the losing IOC trade and the later blocks were already visible when the cap was selected. The full October cohort has 5/5 wins: G-3 slot net +Rs2,500.00 versus baseline -Rs30,994.05; G7 slot net +Rs2,486.90 versus baseline -Rs11,747.72. October portfolio net changes from Rs114,099.46 to Rs147,593.50 for G-3 and from Rs129,985.96 to Rs144,220.58 for G7 lock.

Eight trades occur on only six dates. All eight candidate G-3 exits are `PROTECTED_STOP` for exactly +Rs500 net at 5 bps; G7 integer-share sizing makes them approximately +Rs488 to +Rs500. No candidate winner reaches the original 1.50% target. This is a small-profit scalp with a narrow fee/slippage cushion, not evidence of better stock selection.

## Cost sensitivity of the 8/8 candidate

| Total modeled round-trip cost | G-3 wins/trades | G-3 slot net | G7 wins/trades | G7 slot net |
|---:|---:|---:|---:|---:|
| 5 bps | 8/8 | +Rs4,000.00 | 8/8 | +Rs3,984.13 |
| 9 bps | 8/8 | +Rs2,400.00 | 8/8 | +Rs2,390.48 |
| 15 bps | 0/8, all flat | Rs0.00 | 0/8, all flat | Rs0.00 |
| 30 bps | 0/8 | -Rs6,000.00 | 0/8 | -Rs5,976.20 |

The 5-to-9-bps difference is a fee approximation, not a trigger/fill/slippage simulation. A stop gap can fill worse than the protected level. With only eight successes, the two-sided 95% Wilson lower bound for the observed win probability is about 67.6%, below the requested 75%.

## Verification and limits

The replay reproduced both sealed baseline portfolios exactly: G-3 553 executed trades and Rs1,212,277.50 net; G7 two-loss lock 400 trades and Rs842,866.75 net. The nine filled 09:21 SHORT entries match the saved native G-3 exits and the saved G7 +0.6%/+0.1% protected exits against verified minute paths. All variants preserve the original entries and other slots, then recalculate full portfolio admission and the G7 loss lock. The post-selection veto does not regenerate the pre-quota candidate pool or allow replacement symbols. Thus it is narrower than changing the original selector's configuration. Source sessions missing 24/25 August and 1 October remain excluded.

**Decision: no change qualifies.** The 09:21 SHORT setup should not be promoted to any tested variant based on these records. Freeze a simple rule before evaluating future, untouched sessions, regenerate the full pre-quota candidate pool if changing original selection thresholds, and include realistic trigger execution, spread/slippage, and costs. The current backtest does not establish a reliable improvement in either win rate or slot expectancy.

Reproducible calculations: `research/g3_g7_0921_short_experiments_20261028.py`; machine-readable `results.json`, `trade_details.json`, `cost_stress.json`, and `protocol.json` in this directory.
