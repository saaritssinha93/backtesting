# A_MOD_BREAK_C1_LOW — Baseline

**Side:** SHORT  **Status in source-of-truth:** ACTIVE (survival-book mined short).  **Processed:** 2026-06-29.
**Faithfulness:** readmit basis → fast pool harness live-faithful (same basis-size caveat as setups 1–2).

## Split (printed)
- TRAIN = 2026-04-13 .. 2026-05-25 (6 weeks); TEST = 2026-05-26 .. 2026-06-24.
- Pool: per-setup slice of `outputs_ID_v11_unified_pool` (44,842 raw rows total — largest in the book).
- Net of v6 cost; reported at both 5 and 15 bps/leg.

## Conf gate of record (live today)
- exit SL **1.10 / Tgt 1.00** (asymmetric — SL wider than target → high-win-rate scalp)
- mask_terms: `vol_ratio ≥ 1.955814`
- pre_momentum_terms (ALL required, missing→block): `pre5_mom_r ≥ 0.425861`, `pre3_range_r ≤ 0.202087`
- entry_guards: none

## Backtest-vs-live logic check
**No mismatch in the conf path** (bootstrap reads the conf mask + pre_momentum_terms verbatim). Note: doc §5.2
flags that the *v11 live overlay* uses a DIFFERENT A_MOD gate (`abs(rs_pct)≥9.2 & vol_ratio≥1.80`, no
pre-momentum) — but the overlay is suppressed when the conf flag is on, and A_MOD fires via the conf path. The
conf gate (this audit) is what the survival book trades.

## Baseline result (conf gate, fresh window)

| Slippage | Period | n | net PF | net Rs | win% | T/SL/EOD% | day_block_p | top1day |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 15 bps/leg | TRAIN | 72 | **0.62** | -12,966 | 47.2 | 33/28/39 | 0.96 | — |
| 15 bps/leg | TEST  | 23 | **0.57** | -4,668 | 47.8 | 30/26/44 | 0.90 | — |
| 5 bps/leg  | TRAIN | 71 | **1.23** | +5,577 | 59.2 | 44/21/35 | 0.30 | 1.18 (good) |
| 5 bps/leg  | TEST  | 22 | **1.06** | +460 | 54.5 | 41/23/36 | 0.43 | 2.43 (one-day) |

Ungated raw: catastrophic both slippages (PF 0.29–0.72).

## Observations
- **Best of the three active shorts at paper cost:** TRAIN 1.23 @5 bps clears the 1.2 bar and is well-distributed
  (top1day 1.18, dbp 0.30). But TEST 1.06 < 1.3 bar and is one-day-dominated (243%).
- **Loser at realistic 15 bps/leg** (0.62/0.57) — the asymmetric 1.10/1.00 exit needs ~55%+ win to profit; costs
  push the win rate's edge away.
- More balanced outcomes than B_HUGE_RED/C_OR (33% TARGET vs ~10%) because the target is tight (1.0%).
- Loss-mode (15 bps): losers spread across hours; a couple of repeat-symbol clusters (ACI n2 −2,654, JARO n2 −1,667).

## Loss-mode analysis + iterations
See `A_MOD_BREAK_C1_LOW_experiment_log.md`.
