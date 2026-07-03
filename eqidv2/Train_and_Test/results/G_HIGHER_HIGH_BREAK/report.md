# G_HIGHER_HIGH_BREAK — FIT/VAL→TRAIN/TEST code-loop report

**Verdict: NOT SELECTED**  |  faithfulness: native=SCREENING-ONLY (firehose; use v11 conf backtest for live-faithful)  |  optimizer: Optuna TPE

- FIT 2026-05-19..2026-05-29 | VAL 2026-06-02..2026-06-09 | TRAIN 2026-05-19..2026-06-09 | TEST 2026-06-10..2026-06-24
- trials: 300  | objective on FIT/VAL only = min(PF) − 0.5·|gap|; TEST scored once

## Best config (only knobs the engine honors)
```
exit: SL 1.2 / Tgt 2.0
mask_terms: atr_pct>=0.002102; upper_wick_pct>=0.022387
pre_momentum_terms: sig5_vol_ratio20<=3.826296
entry_guards: {'top_n': 3}  (NB: conf-mask honors only min_slot; top_n/max_slot NOT enforced live)
max_positions: 20  daily_loss_rs: 0.0
```

## Metrics (best config)

| window | 15 bps/leg | 5 bps/leg |
|---|---|---|
| TRAIN | n=17 PF=1.358 net=Rs2,862 tradeDomGross=0.163 dayDom=0.815 symDom=0.617 tpd=2.83 dbp=0.2417 | n=17 PF=1.634 net=Rs4,566 tradeDomGross=0.159 dayDom=0.643 symDom=0.409 tpd=2.83 dbp=0.1346 |
| TEST  | n=20 PF=1.315 net=Rs3,070 tradeDomGross=0.138 dayDom=2.69 symDom=0.575 tpd=4.0 dbp=0.3714 | n=20 PF=1.566 net=Rs5,072 tradeDomGross=0.133 dayDom=1.846 symDom=0.368 tpd=4.0 dbp=0.3129 |

Selection gate (TRAIN & TEST @15bps): **FAIL** (PF≥1.3, train_n≥15, test_n≥5, gross-profit/day/symbol dominance≤0.4, trades/day≤6.0).

## Baseline (original card config, same windows @15bps)
Card gate = `pre2_mom_r≥0.55 AND sig5_adx_calc≥26.0`, exit SL 0.90 / Tgt 2.50, no mask, no guard.

| window | n | net PF | net Rs | note |
|---|---:|---:|---:|---|
| TRAIN | 1 | inf | +815 | gate too tight to fire in this 10-session window (single trade) |
| TEST  | 2 | 0.00 | −1,766 | 2 trades, both losers |

The card's STRONG-PROBATION stats (train 2.38 / test 2.66, p 0.005) came from the original long windows (TRAIN 2025-11..2026-04, TEST 2026-05-01..2026-06-10), **not** this last-15-session window. On the recent sessions the documented gate barely produces a testable sample.

## Knobs changed vs original card
| knob | card | best (this loop) |
|---|---|---|
| mask_terms | (none) | `atr_pct≥0.002102 AND upper_wick_pct≥0.022387` |
| pre_momentum_terms | `pre2_mom_r≥0.55 AND sig5_adx_calc≥26.0` | `sig5_vol_ratio20≤3.826296` (card terms dropped) |
| SL % | 0.90 | 1.20 |
| Tgt % | 2.50 | 2.00 |
| entry_guards | (none) | `top_n: 3` (NB: top_n NOT enforced by the live conf mask) |

## Clean TEST verdict
**Not clean — lucky-day artifact.** TEST PF 1.315 only marginally clears 1.30, but:
- **day dominance = 2.69** (> 1.0): a single session's net exceeds the entire window's net, i.e. every other test day nets negative. This is exactly the "one lucky day" pattern the DOMINANCE_CAP=0.40 rule rejects.
- **symbol dominance = 0.575** (> 0.40): one symbol carries the majority of net.
- **day_block_p = 0.3714**: a daily-block bootstrap mean is ≤0 in 37% of resamples → no day-level significance.
TRAIN is the same story (dayDom 0.815, symDom 0.617, dbp 0.2417). The PF clears on both windows only because of concentrated, non-repeatable days/symbols.

## Live-crosscheck
- **No overlay contradiction.** Per [SETUP_CARDS_AND_LIVE_CROSSCHECK.md](../../SETUP_CARDS_AND_LIVE_CROSSCHECK.md) §5.4, `G_HIGHER_HIGH_BREAK` has **no v11-live-overlay representation** — it fires live only via the conf bootstrap / Tier-C scanner. So there is no contradictory live gate to violate (unlike B_AVWAP / D_EMA20 / A_MOD / E_VWAP_LOSE).
- The found winner uses a different gate than the card, but since the verdict is **NOT SELECTED nothing is promoted**, so the mismatch is moot.
- Card status (§6.3): the setup was **PARKED on 2026-06-29**; this loop does not re-promote it and the re-promotion trigger (fresh live-gated test PF ≥ 1.30, test n ≥ 20, day_block_p ≤ 0.10, then live-paper PF ≥ 1.20) is **not** met (day_block_p 0.37 ≫ 0.10).
- Basis caveat: native firehose pool = SCREENING-ONLY; the v11 conf backtest remains the live-faithful arbiter.

## Command
```
py -3.12 Train_and_Test\optuna_fitval_loop.py --setup G_HIGHER_HIGH_BREAK --pool C:/TradingData/eqidv2/setup_pools_2026_06_29/G_HIGHER_HIGH_BREAK --trials 300 --time_budget_min 20.0 --seed 7 --out Train_and_Test/results
```

## Files changed
- Loop artifacts under this folder (trials.csv, best_config.json, equity_*.png, this report). No live trades executed.
- **2026-06-29 USER-DIRECTED FORCE-PROMOTE:** despite the NOT SELECTED verdict, this best config was promoted into `final_setup_conf.py` (active book, `enabled=True`) at explicit user direction, so it now drives the v11 conf backtest and v7 live (paper) bootstrap. The config replaced the 2026-06-12 PM-gated config (pre2_mom_r≥0.55 & adx≥26, 0.90/2.50); G_HIGHER was also removed from `_LIVE_SURVIVAL_DEMOTION_2026_06_29`. A WARNING block + `provenance.force_promote_2026_06_29` in the conf records the gate failure, the SCREENING-ONLY firehose basis, and the `top_n`-not-enforced-live caveat. Import + both wiring validators (`validate_conf_wiring.py`, `validate_final_conf_live_wiring.py`) pass. Last-1-month replay also written to `setup_looping_results/G_HIGHER_HIGH_BREAK_last_1_month_to_2026-06-29_{summary,trades}.csv`. To reverse: restore the prior config block and the survival-demotion entry.

## Why NOT SELECTED
- PF, net, and sample all pass on both windows (TRAIN 17 tr / PF 1.358 / +Rs2,862; TEST 20 tr / PF 1.315 / +Rs3,070), but the **binding failure is the DOMINANCE_CAP=0.40 rule**: day dominance 0.815 (TRAIN) and 2.69 (TEST), symbol dominance 0.617 / 0.575 — the profit rests on one day and one symbol, not a repeatable edge.
- Supporting evidence: TEST day_block_p 0.3714 (no day-level significance). The marginal PF (just over 1.30) collapses once the dominant day/symbol is removed.
- Conclusion: the 20-bar-higher-high breakout, re-tuned on the last 15 sessions, does **not** produce a robust, deconcentrated edge under realistic (15 bps/leg) costs. Keep PARKED.