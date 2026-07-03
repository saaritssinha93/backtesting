# C_OR_BREAKDOWN — Experiment Log

Window: TRAIN 2026-04-13..2026-05-25, TEST 2026-05-26..2026-06-24. Net of v6 cost; reported at the realistic
15 bps/leg and the paper 5 bps/leg slippage (slippage is decisive for these short scalps — see setup 1).
Acceptance bar: TRAIN PF>1.2 (pref ≥1.4), TEST PF≥1.3, adequate non-concentrated sample, no TRAIN→TEST collapse,
**robust at realistic cost**.

## Baseline (conf gate: sig5_adx_calc≥39.67 & pre1_adx≤21.37, exit 0.9/2.0)
- @15 bps: TRAIN 0.64 (n83) / TEST 0.62 (n53) — clear loser.
- @5 bps : TRAIN 0.94 (n83) / TEST 1.12 (n53, one day=185% of net) — TRAIN still <1.

## Automated searches
- **maxpf @15 bps (realistic):** best TRAIN cfg = mask `rs_pct≤-4.92` (deep RS weakness) + premom `pre1_adx≤26.6`,
  exit 1.0/1.5 → TRAIN PF 1.95 (n=34) but **TEST p=0.686 → BH-FDR-dropped** (no OOS edge at realistic cost).
- **band @5 bps (honest):** _see capstone below_

## Hand iterations (batch 1 @5 bps; one change-group each; loss-mode driven)

Loss modes (baseline @5 bps): 53% EOD time-outs, ~30% SL, losers spread across **all** hours (11:13/12:17/13:9/14:9
train) — no clean time pocket; the wide 2.0% target hits only ~10%.

| # | Change group | TRAIN n/PF | TEST n/PF | Keep? | Reason |
|---|---|---:|---:|---|---|
| 1 | baseline conf 0.9/2.0 | 83 / 0.94 | 53 / 1.12 | — | TRAIN loser; TEST one day=185% net |
| 2 | exit 0.9/1.0 | 83 / 0.81 | 53 / 1.04 | reject | worse train; TEST top1day 4.6× |
| 3 | exit 0.9/1.25 | 83 / 0.95 | 53 / 1.17 | reject | train <1 |
| 4 | exit 0.9/1.5 | 83 / 1.08 | 53 / 1.08 | reject | train net carried by 1 day (top1day 4.2×) |
| 5 | exit 0.7/1.25 | 83 / 0.94 | 53 / 1.09 | reject | train <1 |
| 6 | exit 1.2/2.0 | 83 / 1.04 | 53 / 1.22 | reject | train top1day 7.9× (one day) |
| 7 | + vol_ratio≥2.0 | 51 / **0.98** | 30 / **1.42** (top1day 0.89, dbp 0.19) | **watch, not accept** | TEST genuinely well-distributed + significant-ish, but TRAIN 0.98 < 1.2 bar |
| 8 | + quality_score≤40 | 5 / 0.47 | 3 / 0.03 | reject | n too small |
| 9 | + quality_score≥70 | 55 / 1.09 | 34 / 1.01 | reject | TEST top1day 16.3× (one day) |
| 10 | + sig5_adx≥45 | 40 / 1.01 | 32 / 1.13 | reject | train top1day 28.8× (one day) |
| 11 | guard max_slot 12:30 | 38 / 0.90 | 18 / 0.39 | reject | TEST collapse |
| 12 | + close_loc≤0.30 (0.9/1.25) | 71 / 0.90 | 45 / 1.17 | reject | train loser |

**Read:** no config clears TRAIN PF ≥ 1.2 with a non-concentrated sample. Every TRAIN PF >1 is a single-day
artifact (top1day 4–29×). The one well-distributed TEST winner (i07 vol≥2.0) has a losing TRAIN. The volume-
conviction direction (vol_ratio≥2.0) is the only signal worth re-checking if more data arrives — flagged WATCH.

## Capstone — band @5 bps (honest anti-overfit search)
Best TRAIN cfg = mask `rs_pct ≤ -4.92` (deep relative weakness), **no pre-momentum**, exit 1.2/1.5 →
search reports TRAIN PF **1.49 (n=169)**, but **TEST p = 0.121 → BH-FDR-dropped** (just misses 0.10).
Structural insight: the OR-breakdown short prefers a **deep RS-weakness filter** over the conf's ADX gate.

## Lead evaluation — `rs_pct ≤ -4.92` direct (deployable book, dedupe + overlay)

| Config | Slippage | TRAIN n/PF | TEST n/PF | Read |
|---|---|---:|---:|---|
| rs_weak only, 1.2/1.5 | 15 bps | 121 / 0.64 | 76 / 0.93 | **loser at realistic cost** |
| rs_weak only, 1.2/1.5 | 5 bps | 121 / **1.00** (top1day 54×) | 76 / **1.33** (top1day 0.72, dbp 0.20) | TEST clears bar + well-distributed, but TRAIN only breakeven |
| rs_weak + conf premom | 5/15 | 4 / — | 2 / — | sample destroyed |
| rs_weak, 0.9/2.0 | 15 / 5 | 121 / 0.59–0.85 | 76 / 0.74–0.99 | loser |
| vol_ratio≥2.0, 0.9/2.0 | 15 bps | 51 / 0.66 | 30 / 0.71 | loser at realistic cost (was 1.42 @5 bps) |

**Note (tuner vs deployable):** the band search reported rs_weak TRAIN PF 1.49 (n=169), but the deployable book
(one-ticker/day dedupe + 20-position overlay) is TRAIN 1.00 (n=121). The tuner's per-setup search PF **overstates**
the deployed PF — always confirm the lead in the full-pipeline runner.

## Verdict
**REJECT for sizing / keep unsized.** Loser at realistic 15 bps/leg across every config tried; at paper 5 bps the
best TRAIN is breakeven (1.00) and only TEST clears the bar. The deep-RS-weakness mask (`rs_pct ≤ -4.92`) is the
one real structural lead (TEST 1.33 well-distributed @5 bps) — flagged WATCH for re-validation if more data and
better fills materialize. No config change made.
