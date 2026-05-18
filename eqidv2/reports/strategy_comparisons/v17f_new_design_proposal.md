# V17G Backtesting Proposal

## Scope

This document is the merged, judgment-driven proposal for the next backtesting version (`v17g`).

It is synthesized from two independent inputs:
- the Codex architectural redesign proposal (`v17f_new_design_proposal.md` prior draft)
- a mechanical and alpha-expansion critique of the same v17f matrix

It does not blindly combine both. It keeps what materially improves edge, robustness, or interpretability, and defers everything else to later versions with explicit reasoning.

Input references:
- `v17f_setup_trigger_entry_matrix_20260422.md`
- `v17f_setup_trigger_entry_matrix_20260422.csv`

## Core Thesis

**v17g is a surgical version, not an expansion version.**

The current v17f core is a working, calibrated strategy (V16 Run-12 canonical: 527 trades, 74.6% day-win, PF 1.878, MaxDD 28.4%, LONG day-win 80%). The right discipline for a working strategy is to change few variables per release so A/B results remain interpretable.

For v17g the mandate is:
- fix what is clearly broken
- close one structural asymmetry
- add targeted robustness levers
- defer every rearchitecture, scoring model, regime router, and new-setup expansion to later versions

**What v17g is deliberately not:**
- a regime router
- a quality-score ranking model
- a new-setup expansion pass beyond one symmetry-closing addition
- an entry-lag or exit-logic change

Those are v17h / v17i scope.

## What Stays Out Of v17g (And Why)

| Proposal | Source | Decision | Reason |
|---|---|---|---|
| Fix `B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK` (lag 999 → -1 dynamic) | mechanical critique | deferred to v17h | The lag=999 disable is almost certainly intentional anti-exhaustion protection from v16's theme. Re-enabling without studying why it was killed risks a drawdown spike. Needs a dedicated research sprint. |
| Re-enable SHORT `A_PULLBACK_C2_THEN_BREAK_C2_LOW` | both | deferred to v17h | v17d cleanup removed it deliberately. Re-enabling requires understanding the original failure mode, not "add stricter gates and hope." |
| Regime router (trend / range / reversal classifier) | Codex | deferred to v17h | High value but high risk — changes routing upstream of every setup, far too many variables moving at once. |
| Quality-score ranking model | Codex | deferred to v17i | Correct long-term direction but needs calibration samples we don't yet have. |
| ORB / NR7 / EMA20-pullback / second-touch / gap-fill setups | mechanical critique | deferred to v17i+ | Each adds overfit risk. Expansion should happen after current inventory is clean and scored. |
| Rename setups to `LONG_CONT_HIGH_BREAK`, etc. | Codex | rejected | Loses `A_/B_` (moderate vs huge) and `C1/C2` (trigger-bar) information. Instead: add an orthogonal `module_tag` column (`CORE_CONT` / `PULLBACK` / `HUGE_EVENT` / `REVERSAL`) without renaming. |
| Entry buffer change (`0.02% price` → ATR-scaled) | mechanical critique | deferred | Invalidates parity with live paper-trade. Change only after live-vs-backtest parity is re-established. |
| Volume gate `0.90×` → `1.25×` | mechanical critique | deferred | Part of calibrated v16 core. Don't touch until a scoring model can replace the hard floor. |
| Delete disabled branches from live spec | Codex | accepted partially | Keep in code with explicit `disabled=True` flag and a comment pointing to the research sprint; don't physically delete. Preserves optionality. |
| Early-entry mode (trigger break instead of close confirm) | mechanical critique | rejected for v17g | A meaningful future variant but must be A/B'd in isolation. Mixing with new setups in the same release would make attribution impossible. |
| Exit / SL / target parameter changes | both | rejected for v17g | Same release as new entry logic would make A/B noise-dominated. |

## What Goes Into v17g — Five Changes

### Change 1 — Fix LONG `A_PULLBACK_C2_THEN_BREAK_C2_HIGH` (PRIMARY)

**Bug**: current `lag = 1` tests `C2.high > C2.high + buffer`, which is always false.

**Fix**: set `lag = 2`. After a valid green moderate C1 and a small-red pullback C2 (body ≤ 0.20 ATR, close > AVWAP), test whether `C3.high > C2.high + buffer` with `C3.close > C2.high + buffer`.

**Why this deserves primary status:**
- This is a real bug, not a tuning call.
- The pullback-then-break pattern is structurally orthogonal to `A_MOD_BREAK_C1_HIGH`. On days where C2 breaks C1.high, the C1_HIGH setup fires. On days where C2 pulls back and holds, the PULLBACK setup fires on C3. Different days, not duplicate signals.
- The C2 pullback itself is a cheap quality filter — it rejects cases where the impulse exhausted immediately.
- Expected behavior: lower trade frequency than `A_MOD_BREAK_C1_HIGH`, higher per-trade quality, meaningful contribution to total P&L.

**Additional quality constraints added during the fix (from Codex's recommendations, both adopted):**
- `C2.volume < C1.volume` — the pullback should be lower-participation than the impulse
- `C2.close` in the upper half of C2 range — the pullback is being bought, not sold into

**Hypothesis**: H1 — fixing the bug adds 8-15% additional trade count with PF ≥ baseline parity.

### Change 2 — Add LONG Reversal Setup `B_AVWAP_RECLAIM_REVERSAL` (PRIMARY)

**Motivation**: SHORT side already has a reversal gate (`high >= AVWAP & close < AVWAP` on reversal-mode days). LONG side has no symmetric mean-reversion entry path. Gap-down-then-reclaim days are currently skipped entirely.

**Trigger logic:**
- prior bar or bars show `low <= AVWAP` — AVWAP was tested from above
- reclaim bar: `close > AVWAP`, `body >= 0.30 ATR`, close in upper 40% of range
- `StochK > StochD`, `RSI >= 37` (rising 2 bars), `ADX >= 22`
- EMA20 can be above or below EMA50 — reversal mode does not require trend alignment
- volume gate: reclaim-bar volume >= 0.90× 20-bar average (shared-gate parity)

**Entry logic:**
- Primary: `lag = 1` (C+1 bar breaks reclaim-bar high with close confirmation)
- Alternative: `lag = -1` dynamic (first subsequent bar that breaks reclaim-bar high and closes above it, capped at 3 bars) — decide between these during implementation based on which produces better expectancy in backtest

**Regime restriction:**
- only active when `NIFTY 5-bar return < +0.30%` — on clean trend-up days, regular continuation setups already fire and a reversal setup there is noise
- not active after `13:00` — reversal reliability degrades into close

**Why primary:**
- closes the most important asymmetry in v17f
- captures missing participation on gap-down-reclaim days
- reuses gates already in the code; marginal implementation cost

**Sizing**: 0.5× base until validated (see Change 3).

**Hypothesis**: H2 — LONG reversal adds 15-30 trades per 100 trading days with PF ≥ 1.4, improves LONG participation on reversal-regime days, and is non-correlated with existing LONG setups.

### Change 3 — Setup-Level Position Sizing (PRIMARY for DD control)

**Current**: uniform size per trade across all setups.

**Proposed tiers:**

| Tier | Setups | Multiplier | Reason |
|---|---|---|---|
| Core | `A_MOD_BREAK_C1_HIGH`, `A_MOD_BREAK_C1_LOW` | 1.00× | Highest-validated edge |
| Core-subtype | `A_MOD_CLOSE_CONTINUATION_BREAK` | 0.85× | Looser trigger (C1 close not C1 high); lower expectancy per trade |
| Pullback | `A_PULLBACK_C2_THEN_BREAK_C2_HIGH` (newly fixed) | 0.75× | New live inventory, expectancy unproven |
| Event | `B_HUGE_C1_CLOSE_RECLAIM_BREAK`, `B_HUGE_RED_FAILED_BOUNCE` | 0.60× | Event-driven, higher tail variance, few samples per year |
| Reversal | `B_AVWAP_RECLAIM_REVERSAL` (new) | 0.50× | New, unvalidated, counter-regime |

**Why this deserves primary status:**
- Standard quant risk practice — size proportional to validated edge, not uniform.
- Reduces MaxDD sensitivity to tail outcomes on rare event setups (huge-impulse P&L is fat-tailed).
- Gives new setups an on-ramp — they earn their way to 1.00× through OOS validation rather than entering at full size.
- **Crucial for backtest interpretability** — separates the question "did the new setup add alpha?" from "did position sizing change MaxDD?"

**Implementation**: multiplier applied at entry-order quantity. No other risk logic change.

### Change 4 — Consolidate Fragmented SHORT Time-Pocket Filters (SECONDARY)

**Current v17f short post-scan filter stack (fragmented time pockets):**
- Drop BOTH-mode at `10:30-11:00`
- Drop BOTH-mode at `11:30-12:00`
- Drop BOTH-mode at `12:15-12:45`
- After `13:30` → `SHORT_ONLY` or `RS <= -1.0%`
- Entry cutoff `< 14:00`

**Problem**: three discrete time-bans + a layered post-13:30 rule. Classic over-fit pattern — every ban looks like a statistical dead-zone cavity rather than a causal rule.

**Proposed replacement:**
- **Keep structural gates** (these are causal, not time-pocket cavities):
  - entry cutoff `< 14:00` — late shorts lack bars to run
  - post-13:30 `SHORT_ONLY or RS <= -1.0%` — regime-based, causal
- **Replace the three mid-day time pockets with one causal rule:**
  - Drop BOTH-mode shorts when `NIFTY_5b_ret > -0.10%` AND `stock_vol_ratio_vs_20bar < 1.0`
  - Reading: "if NIFTY isn't showing weakness AND the stock isn't unusually active, a BOTH-mode short is statistical noise — skip it"
- **Keep all other post-scan SHORT filters** (RSI dead-zone, NIFTY RS missing, ADX chop, BOTH AVWAP dead-zone, v17f ATR% and SHORT_ONLY-RS rules) — those are causally motivated, not time-pocket cavities

**Why secondary**: reduces filter fragmentation without expanding signal surface. If A/B shows the single causal rule matches or beats the three-pocket stack, it's a clear robustness win and reduces overfit exposure.

**Hypothesis**: H4 — consolidated rule retains ≥ 95% of current SHORT day-win and PF while eliminating three empirical time-pocket bans.

### Change 5 — LONG AVWAP Distance Cap (SECONDARY)

**Current**: SHORT has `signal AVWAP distance <= 2.10 ATR`. LONG is uncapped.

**Proposed**: LONG cap at **2.25 ATR** — slightly looser than SHORT because momentum on the long side persists more on strong names, but still filters the most overextended entries.

**Why**: cheap, causal, symmetric guardrail. Prevents far-from-AVWAP longs that have limited room-to-run before mean-reverting.

**Expected effect**: small trade-count reduction (~3-5%), MaxDD improvement driven by clipping the worst-outcome LONG trades.

**Hypothesis**: H5 — LONG AVWAP distance cap clips the worst 3-5% of LONG trades by outcome with no material impact on the core edge.

## Long vs Short Treatment

### LONG side in v17g

Active setups after changes:

| Setup | Status | Size multiplier |
|---|---|---|
| `A_MOD_BREAK_C1_HIGH` | active | 1.00× |
| `A_MOD_CLOSE_CONTINUATION_BREAK` | active (subtype) | 0.85× |
| `A_PULLBACK_C2_THEN_BREAK_C2_HIGH` | **active (fixed)** | 0.75× |
| `B_HUGE_C1_CLOSE_RECLAIM_BREAK` | active | 0.60× |
| `B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK` | **disabled in code with flag** | — |
| `B_AVWAP_RECLAIM_REVERSAL` | **new, active** | 0.50× |

Additional gates:
- AVWAP distance cap: ≤ 2.25 ATR (new)
- signal window `12:00-13:00` subject to audit — measure actual P&L contribution in current backtest. If net positive, keep. If net negative, cut to `09:15-12:00`. Decide from data, not opinion.

### SHORT side in v17g

Active setups (unchanged):

| Setup | Status | Size multiplier |
|---|---|---|
| `A_MOD_BREAK_C1_LOW` | active | 1.00× |
| `A_PULLBACK_C2_THEN_BREAK_C2_LOW` | **disabled in code with flag** (deferred to v17h) | — |
| `B_HUGE_RED_FAILED_BOUNCE` | active | 0.60× |

Filter stack:
- structural gates retained (entry cutoff, post-13:30 rule)
- three mid-day time pockets replaced by the single causal NIFTY + volume rule
- all other post-scan SHORT filters retained

**Deliberate SHORT asymmetry**: no new SHORT setup in v17g. NIFTY's upward drift gives LONG a natural base-rate advantage. SHORT's narrower opportunity set means it needs tighter gates and benefits less from inventory expansion. This asymmetry is intentional, not laziness. New SHORT signal surface is a v17h question.

## Time-Window Logic

| Window | LONG in v17g | SHORT in v17g |
|---|---|---|
| `09:15-10:30` | all setups active | all setups active |
| `10:30-12:00` | all setups active (lunch-window audit pending) | core + consolidated causal gate |
| `12:00-13:00` | core continuation + pullback active; reversal off post-13:00 | core + consolidated gate |
| `13:00-13:30` | core continuation only | core + consolidated gate |
| `13:30-14:00` | core continuation only | only `SHORT_ONLY` or `RS <= -1.0%` |
| `14:00+` | core continuation only (no signal-window extension) | **no new entries** |

## Entry Logic

**Philosophy**: bar-close confirmation is retained. No early-entry mode in v17g. At 5-minute resolution, confirmation beats speed — early entries on trigger-break generate false-breakouts and slippage the bar-close discipline filters out.

**Entry triggers for existing setups are unchanged.**

**New reversal entry**: bar close above reclaim-bar high + buffer, with all reclaim-bar quality conditions met.

**Buffer**: `max(0.05, 0.02% of reference price)` — unchanged (parity preservation).

## Exit Logic

**Unchanged from v17f.**

SL and target philosophy is part of the calibrated core. Changing exits in the same release as new entry logic would make A/B noise-dominated.

**Deferred to v17h**: setup-specific SL / target parameters. Huge-event setups probably deserve wider stops given their variance, but that is its own study.

## SL / Target Philosophy (For Reference, Unchanged)

- SL = structural (C1 low for long continuation, C1 high for short continuation, reclaim-bar low for reversal)
- Target = ATR-based, as validated in v16
- v17g tests no SL/target changes. If Change 3 (sizing) reduces MaxDD as predicted, that is v17g's risk-side contribution.

## Risk Management

- Change 3 (setup-level sizing) — first-line DD control
- Change 5 (LONG AVWAP distance cap) — second-line tail control
- Existing day-level caps, per-ticker cooldowns, daily-loss cutoffs from v17f preserved
- Intraday portfolio caps preserved

## Confirmation vs Early-Entry vs Balanced

The user asked whether v17g should prioritize early momentum capture, better confirmation, or a balanced middle ground.

**Decision: confirmation-heavy with diversified entry types.**
- v17f is already confirmation-heavy (all setups require close past trigger)
- v17g stays confirmation-heavy — entry lag is not a variable in this release
- Balanced participation is achieved through entry-type diversification (continuation + pullback + reversal), not by shortening the confirmation lag
- An early-entry mode is a meaningful future variant (v17h candidate) but must be A/B-tested against confirmation in isolation

## Architecture Decision — Merged With Explicit Toggles

**v17g is a single merged strategy with explicit config toggles, not a modular framework with selectable components.**

A modular framework invites permutation explosion and loses the clarity of "here is the single proposed strategy." At the same time, each of the five changes needs to be independently toggleable so the A/B matrix can attribute effect to change.

Toggle structure:

```python
V17G_FLAGS = {
    "fix_long_pullback_lag": True,        # Change 1 — PRIMARY
    "enable_long_reversal_setup": True,   # Change 2 — PRIMARY
    "setup_level_sizing": True,           # Change 3 — PRIMARY
    "consolidate_short_pockets": True,    # Change 4 — SECONDARY
    "long_avwap_distance_cap": True,      # Change 5 — SECONDARY
}
```

Toggles default to ON in final v17g config. They exist to make the A/B matrix below tractable — not as live operational switches.

**If scope must be cut**: drop Change 4 first, then Change 5. Keep the three primaries.

## Backtesting Plan

Seven configurations run over the same backtest window as v17f canonical. Minimum 2+ years of 5-minute bars, split into in-sample (IS) and out-of-sample (OOS) halves. OOS is strictly held out until IS calibration is complete.

| Run | Config | Tests |
|---|---|---|
| `baseline` | v17f as-is | Reference |
| `v17g-1` | baseline + Change 1 only | H1 — pullback bug fix adds alpha |
| `v17g-2` | baseline + Change 2 only | H2 — LONG reversal adds alpha |
| `v17g-3` | baseline + Change 3 only | H3 — sizing tiers reduce MaxDD |
| `v17g-4` | baseline + Change 4 only | H4 — consolidated SHORT rule matches fragmented stack |
| `v17g-5` | baseline + Change 5 only | H5 — LONG AVWAP cap reduces tail risk |
| `v17g-all` | all five enabled | Combined effect |

Metrics tracked for every run:
- PF, day-win%, trade-win%, MaxDD, Sharpe, Calmar
- **per-setup P&L, trade count, expectancy** — critical for validating sizing tiers
- per-time-bucket trade count and expectancy
- per-NIFTY-regime day breakdown (up / flat / down days)
- R-multiple distribution (tail-shape check)
- day-level max consecutive losing days

OOS discipline:
- reserve the last 4 months of the backtest window as pure OOS
- a change advances to v17g-final only if it beats baseline on **both** halves
- any change that fails OOS is toggled off (code retained for v17h rework)

Decision rules (per-change):
- OOS PF ≥ baseline − 0.05
- OOS day-win ≥ baseline − 2 percentage points
- MaxDD no worse than +10% of baseline
- every criterion must pass — no averaging

Decision rule (`v17g-all`):
- must beat baseline on at least 3 of {PF, MaxDD, Sharpe} OOS to ship as new canonical

## Expected Behavioral Profile

If v17g passes OOS validation:
- **Trade count**: +10-20% vs v17f canonical (pullback bug fix + LONG reversal)
- **Day-win**: ±1 percentage point vs baseline — target unchanged, we are diversifying, not overtrading
- **PF**: ±0.05 vs baseline — target unchanged or slight positive
- **MaxDD**: −2 to −5 percentage points vs baseline — sizing tiers + AVWAP cap contribution
- **Sharpe**: +5-10% vs baseline — more trades of comparable expectancy with lower variance
- **Per-setup P&L contribution**: core continuation dominates (~60-70%), pullback adds 10-15%, reversal adds 5-10%, event setups 5-10%

Strategy character is unchanged: trend-continuation with AVWAP confirmation. Diversification is orthogonal, not regime-shifting.

## Hypotheses (Prioritized)

1. **H1** — Fixing the LONG pullback lag bug unlocks an orthogonal alpha source with expectancy ≥ LONG baseline.
2. **H2** — LONG reversal captures missing participation on gap-down-reclaim days without cannibalizing existing LONG setups.
3. **H3** — Setup-level sizing reduces MaxDD materially (target: −2 to −5 pp) without reducing PF.
4. **H4** — One causal SHORT gate replaces three time-pocket filters with equivalent-or-better selectivity.
5. **H5** — LONG AVWAP distance cap clips the worst 3-5% of LONG trades by outcome.

## Overfitting & Over-Complication Risks

Main risks:
- **Change 2 (LONG reversal) is the highest overfit risk** — new setup, new parameters. Mitigations: 0.5× sizing, separate-report tracking, OOS discipline, explicit regime restriction.
- **Change 4 (SHORT pocket consolidation) risks over-compression** — if the single causal rule is too loose, noise the fragmented rules were catching returns. Mitigation: A/B test vs current; don't ship if it loses.
- **Stacking five changes in one release** inflates combinatorial risk. Mitigation: the A/B matrix forces per-change attribution. `v17g-all` only ships if combined OOS beats baseline.

Discipline enforced by the design:
- no new setup without separate-report tracking
- no new setup at full size
- no new gate without causal justification
- no change to calibrated parameters (buffer, volume gate, exit logic) in the same release as new setups
- every change toggleable so any single item can be reverted without rebuilding

## Conflicts Resolved Between The Two Inputs

| Conflict | Codex position | Mechanical critique position | Resolution |
|---|---|---|---|
| `B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK` | remove from live spec | fix by setting lag = -1 dynamic | **Defer** — disable=True flag in code, slated for v17h research sprint. Both inputs miss that it was deliberately killed, likely for cause. |
| Setup renaming | rename to `LONG_CONT_*` | keep `A_/B_` + `C1/C2` | **Hybrid** — keep names, add orthogonal `module_tag`. |
| Scoring model vs blacklist filters | replace blacklists with quality score | keep causal blacklists, replace only cavity-shaped ones | **Mechanical-critique position wins** — causal gates stay. Only the fragmented SHORT time-pockets are consolidated. Scoring model is v17i. |
| New setup expansion (ORB / NR7 / EMA20 / etc.) | not proposed | proposed | **Mechanical-critique position rejected for v17g** — one new setup is the discipline. Expansion is v17i. |
| Pullback volume < impulse volume constraint | proposed | not proposed | **Codex position wins** — adopted into Change 1. |
| Huge-reclaim "too far from C1 close" reject | proposed | not proposed | **Deferred to v17h** — good quality gate but adds variable to a setup we aren't otherwise changing. |
| Setup-level sizing | proposed (core / event separation) | not proposed | **Codex position wins** — adopted and extended into five-tier scheme (Change 3). |
| Early-entry mode | not proposed | mentioned as variant | **Rejected for v17g** — must be A/B'd in isolation in a later version. |

## What v17g Is Not

- not a rearchitecture — Codex's full vision maps to v17h/v17i
- not a scoring model
- not a regime router
- not an expansion of setup inventory beyond the one LONG reversal
- not a live-execution change — bar-close confirmation preserved
- not an SL/target study — deliberately held constant

## Summary

v17g fixes one clear bug, closes one structural asymmetry, adds three robustness levers, and defers everything else. It tests whether the v17f core strategy can absorb modest orthogonal additions — a revived pullback setup and a new LONG reversal setup — while setup-level sizing and a LONG AVWAP cap reduce tail risk. It does not attempt to rearchitect the filter stack, build a scoring model, or introduce new alpha classes — those are correct next-version work, not same-version work. The backtesting plan uses a seven-way A/B matrix with IS/OOS split so every change is individually attributable, and the shipping criterion is OOS survival, not best-case IS tuning.

## Files To Produce Next

1. `v17g_implementation_checklist.md` — tight list of file-level code changes per toggle
2. `v17g_ab_runner.py` — A/B harness driving the seven configurations
3. `v17g_research_notes.md` — findings dossier populated as each run completes
4. `v17h_parked_items.md` — explicit list of what was deferred, with conditions for re-opening each (huge-green-pullback, SHORT pullback, regime router, scoring model, new setup classes, early-entry mode, exit-parameter tuning)
