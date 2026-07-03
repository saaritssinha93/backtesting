# B-family from-scratch recovery — CAMPAIGN CONCLUSION (2026-07-03)

_Research-only. `final_setup_conf.py` untouched. Windows: TRAIN 2026-03-01..05-30 / TEST 2026-06-01..07-02 (07-02 excluded: 1-min EOD sync incomplete). Full per-setup artifacts (9 reports + iteration logs + trades) in each setup folder; shared diagnostics + validated resolver2 in `_shared/`._

## Verdict: 0 / 5 — and the diagnostics now PROVE why (not just fail to find)

Four escalating, independent methods agree:
1. **Round 1** — repo-schema mask/premom/guard/exit search (~700 configs/setup): no robust in-band candidate.
2. **Round 2** — enriched space (~38 point-in-time indicator/price-action features, 3-term masks, 800 TPE trials/setup): no candidate.
3. **Round 3** — disciplined local refinement of the 2 near-band anchors (one TEST shot each): both failed OOS.
4. **Round 4 (this loop)** — execution-layer redesign (validated resolver2: break-even / trailing / time-stop exits; retest/limit entries; FIT-mined filters; windows; fade), ~90 iterations/setup: **zero configs even earned a TEST evaluation.**

## The decisive evidence (execution diagnostics, broad TRAIN books)

| Setup | GROSS PF (0 cost) | net @5bps | net @15bps | Diagnosis |
|---|---|---|---|---|
| B_AVWAP_RECLAIM_REVERSAL | **0.883** | 0.544 | 0.324 | **Gross-negative. The detection is directionless before a single rupee of cost. Dead by construction.** |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | 1.073 | 0.720 | 0.472 | Gross noise; costs finish it |
| B_HUGE_RED_FAILED_BOUNCE | 1.137 | 0.712 | 0.431 | Thin gross edge fully consumed by costs (ACTIVE conf setup — demote signal) |
| B_HUGE_FAILED_BOUNCE | 1.192 | 0.762 | 0.480 | Thin gross edge fully consumed by costs |
| B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK | **1.304** | 0.960 | 0.696 | Real gross edge — but it lives in 1–2 trending days |

Supporting structure (per-setup MFE/MAE + retest + fade tables in `_shared/DIAGNOSTICS.md`):
- **The median trade cannot pay the toll**: median MFE at 60 min is +0.20..+0.48% vs median MAE −0.55..−0.70%, against a ~0.30–0.36% statutory+slippage round trip. Only tail trades are net-viable, which is why every "good" config collapses to n≈25–45 and then fails concentration or OOS.
- **Follow-through is weak by construction**: after these huge-bar signals price retraces a MEDIAN of 1.2–1.4 ATR — retest limit entries fill 75–87% of the time precisely because the premise usually fails. Retest entries adversely select (tested: PF drops).
- **Both directions lose net**: every fade variant is PF 0.28–0.54 — the signals are near-symmetric noise after costs, so there is no "other side" to harvest.
- **GREEN's real gross edge is day-concentrated**: every in-band TRAIN config across all 4 rounds carries 47–125% of net in its single best day. No guard (top_n / max_positions / daily-loss / windows / exits) dissolves it without destroying the sample. That is a luck profile, not a harvestable edge.

## What this means

- **No candidate is proposed for approval.** Forcing a "pass" from here would be test-fitting by exhaustion — the exact failure mode the campaign rules prohibit.
- **B_HUGE_RED_FAILED_BOUNCE (active in the live conf book) failed re-validation in every round** — recommend demote review (user decision; conf untouched).
- If the B-family idea is revisited, the data says the ONLY defensible direction is B_HUGE_GREEN-style events **on strong-trend days as a day-type bet** (its edge is day-clustered) — i.e., a regime/day-selection product, not a per-signal scalp — and that requires a different evaluation design (day-level sizing), out of scope for this per-signal book.

## Reusable assets built this campaign

- `_shared/resolver2.py` — BE/trailing/time-stop resolver + retest-limit fills, validated 300/300 vs production.
- `_shared/recovery_engine.py` + `_shared/run_setup_recovery.py` — variant-family recovery harness with FIT-only mining, band discipline, domination caps, TEST-once budget.
- `_shared/diagnose_execution.py` + `DIAGNOSTICS.md` — cost-anatomy / MFE-MAE / retest / fade diagnostic, reusable for ANY setup before spending optimization compute.

> **DO NOT MOVE ANYTHING TO FINAL CONFIG — nothing passed; nothing is proposed.**
