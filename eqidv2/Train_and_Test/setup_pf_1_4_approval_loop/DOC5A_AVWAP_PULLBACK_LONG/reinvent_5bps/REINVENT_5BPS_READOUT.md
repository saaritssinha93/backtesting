# DOC5A_AVWAP_PULLBACK_LONG — REINVENT @ 5 bps READOUT

_Generated 2026-07-01. Research-only. NO conf edits, NO live trades._

## What was tried (genuine detector reinvention, not just re-filtering)
1. **Re-mined a RICH superset pool** (`scripts/mine_rich_pool.py`) with a LOOSER base entry (9,969 rows
   vs 583 in the first cut) and emitted the doc's own structural knobs as searchable columns:
   `vwap_slope_atr`, `established_bars`, `pullback_depth_atr`, `orh_dist_atr`, `ema20_dist_atr`,
   `adx_sig`, `rsi_sig`.
2. **Ran the band search at 5 bps/leg** (`scripts/structural_pf_band.py`) with those 7 structural
   features added to the mask search (19 mask feats + 8 pre-momentum feats), 2 mask + 1 premom terms,
   wide exit grid, guards — 900 Optuna-TPE trials. Split: TRAIN 2026-05-18…06-19 (22 sess, FIT 11 / VAL 11),
   TEST 2026-06-22…06-30 (6 sess).

## Result: REJECT — no robust in-band region exists

Best FIT/VAL config: `SL 1.2 / Tgt 2.5`, mask `pullback_depth_atr>=0.28 & signal_range_pct<=0.207`,
premom `pre3_close_pos<=0.25`, guard `max_slot 12:00, top_n 3`:
- **TRAIN @5bps: PF 1.62 (n=13)** — inside the band, BUT only 13 trades, one day dominates (dayDom 1.16),
  robustness (neighborhood + dropout) fails.
- **TEST @5bps: PF 0.002 (n=8)** — total collapse; win% 12.5%, one trade = the whole book (tradeDom 1.0).

## The wall (why it can't be tuned into the goal)

FIT/VAL trial landscape (900 trials) — configs reaching the TRAIN band as a function of trade count:

| min trade count (per FIT/VAL half) | configs with min(FIT,VAL) PF ∈ [1.30,1.90] | max achievable min(FIT,VAL) PF |
|---|---|---|
| ≥ 6  | 8  | 1.72 |
| ≥ 10 | 2  | 1.72 |
| ≥ 15 | **0** | **0.99** |
| ≥ 20 | **0** | 0.93 |

**Interpretation:** the band is only reachable by gating down to ~6–13 trades (overfit pockets that die
OOS). The moment the setup carries a meaningful, trustworthy trade count (≥15/half ⇒ ~30+ TRAIN), its
in-sample PF is **below 1.0** — a loser before TEST is ever consulted. There is no config that is
simultaneously (a) TRAIN PF 1.30–1.70, (b) meaningfully traded (n≥20), (c) non-dominated, (d) TEST PF>1.40.

## Verdict
Even reinvented (new detector, 7 structural knobs, more forgiving 5 bps costs), DOC5A has **no robust,
tradeable edge**. Same structural wall as P_PDH / FAST_MOMENTUM / the doc5 batch: *enough trades ⇒ loser;
force PF up ⇒ tiny overfit pocket ⇒ TEST collapse.* Reaching the goal from here would require selecting a
config on its TEST number — overfitting, which the campaign rules forbid.

**Approval: NO.** Not tradeable. Artifacts: `reinvent_5bps/DOC5A_AVWAP_PULLBACK_LONG/` (trials.csv,
run_summary.json, standard MD set) + `scripts/mine_rich_pool.py` + `scripts/structural_pf_band.py`.

> DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES.
