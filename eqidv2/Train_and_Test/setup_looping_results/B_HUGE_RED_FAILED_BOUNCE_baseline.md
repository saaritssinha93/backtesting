# B_HUGE_RED_FAILED_BOUNCE - Baseline

## Rolling 2-Week/1-Week Audit Update - 2026-06-29

Requested setup-only rerun using the current rule: latest completed available week as TEST and the two immediately preceding weeks as TRAIN.

Pinned split:

| Period | Dates | Raw B rows | Entry rows |
|---|---|---:|---:|
| TRAIN | 2026-05-25..2026-06-05 | 273 | 273 |
| TEST | 2026-06-08..2026-06-12 | 82 | 82 |

Later available weeks are partial for this pool, so `2026-06-08..2026-06-12` is the latest completed TEST week.

Baseline config evaluated:

| Field | Value |
|---|---|
| Side | SHORT |
| Detection | `huge_red_failed_bounce_short` |
| Mask filters | none |
| Pre-momentum gates | `pre3_close_pos <= 0.581797`, `sig5_rsi_dir <= 64.104659`, `pre5_mom_r <= 0.284145` |
| Entry guards | none |
| Exit | SL 0.90 / Target 1.25 |
| Cost realism | default tuner cost, 15 bps per leg + statutory intraday costs |

### Rolling Baseline Metrics

| Period | Trades | Win % | Gross profit | Gross loss | Net PnL | PF | Avg win | Avg loss | Max DD | Day block p | Outcomes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| TRAIN | 10 | 40.00 | 2,283 | 3,529 | -1,246 | 0.647 | 571 | -588 | -1,898 | 0.7764 | EOD 8, TARGET 1, SL 1 |
| TEST | 5 | 40.00 | 2,030 | 2,325 | -295 | 0.873 | 1,015 | -775 | -1,313 | 0.6877 | TARGET 2, SL 2, EOD 1 |

### Rolling Rejection / Filter Counts

| Period | Entry rows | After guard | After pre-mom | After dedupe | After mask | Resolved trades |
|---|---:|---:|---:|---:|---:|---:|
| TRAIN | 273 | 273 | 10 | 10 | 10 | 10 |
| TEST | 82 | 82 | 5 | 5 | 5 | 5 |

### Rolling Day-Wise Results

TRAIN:

| Day | Trades | Net PnL |
|---|---:|---:|
| 2026-05-25 | 1 | 652 |
| 2026-05-26 | 1 | -545 |
| 2026-05-27 | 1 | 407 |
| 2026-06-01 | 4 | -111 |
| 2026-06-03 | 1 | -1,132 |
| 2026-06-04 | 1 | 208 |
| 2026-06-05 | 1 | -725 |

TEST:

| Day | Trades | Net PnL |
|---|---:|---:|
| 2026-06-08 | 1 | 1,018 |
| 2026-06-09 | 2 | -1,243 |
| 2026-06-10 | 1 | 1,012 |
| 2026-06-12 | 1 | -1,082 |

### Rolling Failure Modes

- Sample too thin: only 10 TRAIN and 5 TEST trades after the conf pre-momentum gate.
- Poor follow-through: TRAIN has 8 EOD exits out of 10; the failed-bounce thesis rarely reaches target.
- Tight scalp cost sensitivity: the same trade list is marginal at paper-like 5 bps but loses at 15 bps.
- TEST remains below the PF acceptance bar and is not enough sample to trust.
- Existing larger-window/live-paper evidence already showed decay, so this small split does not rescue B.

Full rolling per-trade detail: `B_HUGE_RED_FAILED_BOUNCE_rolling_loop_details.json`, iteration `0`.

---

## Prior Active-Book Audit Baseline

**Side:** SHORT  **Status in source-of-truth:** ACTIVE (one of the 4 survival-book mined shorts).
**Processed:** 2026-06-29.  **Faithfulness:** readmit basis → fast pool harness is **live-faithful**.

## Split (printed)
- TRAIN = 2026-04-13 .. 2026-05-25 (6 weeks)
- TEST  = 2026-05-26 .. 2026-06-24 (latest ~4 weeks of available data)
- Pool: per-setup slice of `outputs_ID_v11_unified_pool` (3,900 raw rows total; train=583 / test=376 raw, pre-entry).
- Cost: net of v6 cost model + 15 bps/leg slippage; live one-ticker/day dedupe + pipeline.

## Conf gate of record (what is live today)
- exit SL 0.90 / Tgt 1.25
- mask_terms: none
- pre_momentum_terms (ALL required, missing→block): `pre3_close_pos ≤ 0.581797`, `sig5_rsi_dir ≤ 64.104659`, `pre5_mom_r ≤ 0.284145`
- entry_guards: none (live 09:30–14:30 + 1-ticker/day dedupe)

## Backtest-vs-live logic check
**No mismatch.** `eqidv2_final_conf_live_bootstrap.py` reads `pre_momentum_terms` and `mask_terms`
directly from `FINAL_SETUP_CONF` (faithful port; README guarantees "conf mask bit-identical,
pre-momentum features identical"). Per cross-check doc §5.4, B_HUGE_RED_FAILED_BOUNCE fires **only via the
bootstrap / Tier-C scanner** — it is **not** in the v11 live-overlay universe, so there is no contradictory
overlay gate. Backtest == live for this setup.

## Baseline result (conf gate, fresh window)

| Period | n | net PF | net Rs | win% | TARGET/SL/EOD% | day_block_p | avg win / avg loss |
|---|---:|---:|---:|---:|---:|---:|---:|
| TRAIN | 28 | **0.79** | -2,098 | 46.4 | 21/21/57 | 0.679 | 621 / -678 |
| TEST  | 16 | **0.52** | -3,422 | 31.2 | 19/25/56 | 0.892 | 732 / -644 |

Reference — **ungated raw** (no gate): TRAIN PF 0.32 (n=512, −Rs213,799), TEST PF 0.45 (n=312, −Rs99,039).
The pre-momentum gate lifts PF from 0.32→0.79 but **does not reach profitability** on the fresh window.

## Loss-mode analysis (baseline)
- 57% of trades are **EOD time-outs** (target rarely reached): the short thesis does not follow through in this window.
- Losers cluster in the **afternoon** (TRAIN losers by hour 11:3, 12:4, 13:2, 14:6).
- Loss size ≈ full −Rs1,130 SL hits + small EOD bleeds; no single symbol dominates losses.

## Context vs conf claim
Conf provenance claims **train 2.90 / test 3.49** — but that was on the OLD window
(train 2025-11-01..2026-04-30, test 2026-05-01..2026-06-10). On the fresh rolling window the edge is absent.
This matches the §6 live narrative (the conf book bled in June live paper).
