# Phase 0 — Early-Emit Architecture Design Decisions

Date: 2026-04-22
Scope: Re-architect V16 5-min live from "SE scans at entry_bar_close" to "SE emits CANDIDATE at trigger_iso; DE confirms at entry_bar_close; executor places intrabar orders".

All decisions below are binding inputs to Phases 2+. They MUST NOT be changed silently during implementation — amend this document first.

---

## D0.1 — signal_id key

**Decision: `hash(ticker, side, trigger_iso, setup)`**

- `trigger_iso` = C1 bar close stamp (the bar that defines the primary trigger condition — C1 for A_MOD/HUGE setups, C2 for A_PULLBACK setups).
- `side` ∈ {SHORT, LONG}.
- `setup` is the full setup name string (e.g. "A_MOD_BREAK_C1_HIGH").

Why:
- Under early-emit, multiple CANDIDATE emissions for the same trigger bar across multiple scans must deduplicate to ONE pool row.
- `entry_time` is unknown at emit time for setups with lag≥2 and for dynamic-lag setups (HUGE branches), so keying on `entry_time_ts` is not feasible.
- Using the trigger bar (not source_slot) makes the ID deterministic across SE re-scans within the window.

Rejected alternative:
- `hash(ticker, side, future_entry_time, setup)` — requires computing a projected entry time at emit, which is wrong for dynamic-lag setups and duplicates when the same C1 produces multiple setup hypotheses.

## D0.2 — Pool file naming during shadow mode

**Decision: separate file `candidate_signals_<YYYY-MM-DD>_v16_5min.json`**

- Current production pool file `pending_signals_<date>_v16_5min.json` is UNTOUCHED.
- New shadow pool file lives at `C:/TradingData/eqidv2/live_signals/candidate_signals_<date>_v16_5min.json`.
- Executor code MUST ignore `candidate_signals_*.json` until flipped in Phase 5.

Why:
- Shadow mode requires zero risk to live orders. A separate file guarantees the executor cannot accidentally consume early-emit candidates.
- Diff-based parity checks (candidate CONFIRMED vs pending DE_PASSED for same ticker/side/trigger) need both pools present.

Flip plan (Phase 5):
- One-line change: executor reads `candidate_signals_*.json` and treats `state=CANDIDATE_CONFIRMED` as the analogue of today's `DE_PASSED`.

## D0.3 — CANDIDATE lifecycle states

**Decision: all three final states — CONFIRMED, EXPIRED, INVALIDATED**

State machine:
```
CANDIDATE_WRITTEN                     (written by SE at trigger_iso)
  ├─> CANDIDATE_CONFIRMED            (DE at entry_close: break_level hit + all gates pass)
  ├─> CANDIDATE_EXPIRED              (entry_window_end passed without break_level hit)
  └─> CANDIDATE_INVALIDATED          (structural invalidator: stop-level violated pre-entry,
                                       opposite-side impulse overwrites, or session-end reached)
```

Transition rules:
- WRITTEN → CONFIRMED: DE scans the entry-bar close, finds `break_level` hit with close-confirm and all Phase-1 per-setup gates satisfied.
- WRITTEN → EXPIRED: entry_window end reached (`trigger_iso + max_lag_bars*5min` for fixed-lag setups; `trigger_iso + 40min` hard cap for dynamic-lag HUGE setups).
- WRITTEN → INVALIDATED: pre-entry bar prints a level that would have stopped the trade out (e.g. for LONG, low < C1.low − 0.5*ATR; or session cutoff 15:15 reached mid-window).
- Terminal states are written once and never mutated.

Why three states:
- EXPIRED ≠ INVALIDATED for analytics: "break never occurred" (market drifted sideways) vs "setup structurally broken" (stop hit pre-entry) have different diagnostic signal.
- Forensics: during shadow parity we need to distinguish "DE rejected" (gate failure) from "candidate was doomed early" (structural invalidation) from "market moved on" (expiry).

## D0.4 — DE role

**Decision: runner-replay confirmer**

- DE still calls `v16_runner.scan_short_prepared` / `scan_long_prepared` through the same `_scan_partition_worker` path as today.
- DE scans at the entry-bar close stamp (source_slot), filters its output to rows whose `(ticker, side, trigger_iso, setup)` match a WRITTEN candidate, and confirms.
- The scanner keeps its full gate ladder (avwap_rejection, avwap_distance, bars_left, entry_time_cutoff, close_confirm, signal-window, regime, VIX). DE does not re-implement these — it replays the scan and matches.

Why:
- Parity with backtest path is the primary correctness anchor. Any DE-side gate reimplementation is a second source of truth that will drift.
- The runner's gate set is already proven across 160+ days of backtest data.

Implication for Phase 2:
- SE's early-emit does NOT need to replicate scanner gates — it just needs enough to detect that a C1 impulse + structural preconditions exist. The "heavy" gates can fail later and be handled via WRITTEN → EXPIRED/INVALIDATED.

## D0.5 — Setups with >1-bar trigger criteria (A_PULLBACK, HUGE)

**Decision: emit CANDIDATE at the *earliest* bar whose close alone satisfies the structural precondition. DE confirms the full gate ladder at entry_close.**

Per-setup emit bar (details in Phase 1 map):
- `A_MOD_BREAK_C1_HIGH/LOW`: emit at C1 close (trigger_iso = C1).
- `A_MOD_CLOSE_CONTINUATION_BREAK`: emit at C1 close (same C1 impulse, only break_level differs).
- `A_PULLBACK_C2_BREAK_C2_HIGH/LOW`: emit at **C2** close (requires C2's pullback shape). trigger_iso = C2.
- `B_HUGE_C1_CLOSE_RECLAIM_BREAK`: emit at C1 close. trigger_iso = C1.
- `B_HUGE_PULLBACK_HOLD_BREAK`: emit at pull_end close (i+3). trigger_iso = pull_end bar.
- `B_HUGE_FAILED_BOUNCE`: emit at bounce_end close (i+3). trigger_iso = bounce_end bar.

Why earliest-satisfiable:
- Gives executor the maximum lead time to arm GTT / WebSocket ticker subscription.
- If a later gate (regime, VIX, RS) fails, DE produces CANDIDATE_EXPIRED cleanly.

Why not "always emit at C1 for all setups":
- For A_PULLBACK the break_level (C2.low/high) is unknown at C1 close. Writing a candidate with `break_level=null` would force the executor to do level-inference, which violates the "SE is single source of truth for levels" contract.
- Same reason for HUGE: bounce_low / pull_high aren't known until the bounce/pullback window closes.

## D0.6 — Entry-window semantics

**Decision: per-setup fixed window, authored in Phase 1 map.**

- Fixed-lag setups (lag ∈ {1, 2}): entry_window = [trigger_iso + 5min, trigger_iso + lag*5min + 5min). Exactly one bar — the entry bar. CONFIRMED or EXPIRED at that bar's close.
- Dynamic-lag setups (HUGE branches with `lag_bars=-1` or `lag_bars=999`): entry_window = [trigger_iso + 5min, trigger_iso + 40min). Max 7 bars, then EXPIRED. Matches scanner's `j_iter = range(bounce_end+1, len(df_day))` with a hard cap.

No open-ended windows. Every candidate has a deterministic terminal time.

## D0.7 — Out of scope for this plan

- Changing the scanner's gate ladder.
- Changing v16_runner's LAG_BARS constants.
- Touching PF (portfolio filter) at all during shadow.
- Consuming candidates in the executor prior to Phase 5 flip.

---

## Deliverables produced by Phase 0

This file. No code changes.

## Next — Phase 1

`setup_early_emit_map.md` — one row per setup with: trigger_iso anchor, precondition checklist (what's checkable at emit), break_level formula, entry_window formula, DE gate list to replay.
