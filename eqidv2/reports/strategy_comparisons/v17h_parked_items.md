# V17H Parked Items

Items deferred from `v17g` design to keep the v17g release surgical. Each item lists why it was parked and the explicit condition under which it should be re-opened in v17h or later.

Cross-references:
- v17g proposal: `v17f_new_design_proposal.md`
- v17g engineering checklist: `v17g_implementation_checklist.md`
- v17g findings: `v17g_research_notes.md`

## Triage Buckets

- **B1 — Activate after v17g passes**: candidates for v17h primary scope, presumed valuable but held back to keep v17g attribution clean
- **B2 — Activate after a research sprint**: requires a dedicated investigation before code change is justified
- **B3 — Activate only if v17g fails**: backup paths if v17g doesn't beat baseline
- **B4 — Long-tail / v17i+**: genuinely later-version work

## B1 — v17h Primary Candidates

### Re-enable `B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK`

- **Current state**: lag = 999 (effectively disabled in code)
- **Why parked**: the disable is almost certainly intentional anti-exhaustion protection from v16's theme. Re-enabling without studying the original failure mode risks a drawdown spike.
- **Re-open condition**: dedicated research sprint that (a) recovers the original disable rationale from v16/v17b commit history or run logs, and (b) demonstrates per-day expectancy on a held-out sample.
- **Implementation if approved**: change lag to `-1` dynamic (mirror of SHORT `B_HUGE_RED_FAILED_BOUNCE`); cap pullback hold to 3 bars; require pullback close above C1 midpoint.

### Re-enable SHORT `A_PULLBACK_C2_THEN_BREAK_C2_LOW`

- **Current state**: removed by v17d post-scan cleanup
- **Why parked**: same reason — removed deliberately, almost certainly for a measured DD or win-rate failure. "Add stricter gates" without understanding the original failure mode is guessing.
- **Re-open condition**: recover and document the v17d removal rationale; if redesigned, only with strict gates: `ADX>=28 rising`, `RS<=-0.5%`, trend-day mode only, entry `<13:00`, `pullback_volume < impulse_volume`.
- **Implementation if approved**: keep removed by default; add a v17h sub-flag and run as a standalone A/B against v17g-final.

### Setup-specific SL / target parameters

- **Current state**: uniform SL/target across setups
- **Why parked**: changing exits in the same release as new entries makes A/B noise-dominated.
- **Re-open condition**: v17g shipped, then study P&L distribution per setup. Huge-event setups likely deserve wider stops.
- **Implementation if approved**: per-setup SL multiplier, similar in shape to v17g's per-setup sizing.

## B2 — Research Sprint Required

### Quality-score ranking model

- **Why parked**: correct long-term direction but needs calibration data we don't have yet. Building it without per-setup-per-regime expectancy estimates is reverse-engineering.
- **Re-open condition**: at least 6 months of v17g live or backtest data with stable per-setup attribution; clear specification of what the score should optimize for (PF? Sharpe? per-trade expectancy?); a held-out OOS sample reserved for score validation.
- **Components when built** (Codex's list, adopted): impulse quality, continuation quality, AVWAP quality, structure quality, trend strength, context quality, time-of-day quality, exhaustion penalty.

### Regime router

- **Why parked**: high value but high risk — changes routing upstream of every setup.
- **Re-open condition**: the quality-score model exists (so regime can route by score, not by hard switches); per-regime per-setup expectancy estimates are stable.
- **Initial design when revisited**: regime classifier (trend-up / trend-down / range / reversal / opening-expansion) computed once per day from NIFTY + breadth; setups gated by regime-conditional weights, not hard bans.

### LONG signal window `12:00-13:00` audit

- **Why parked**: included in v17g as a noted question but not changed. The window overlaps lunch hour when SHORT explicitly avoids `12:15-12:45`.
- **Re-open condition**: measure LONG P&L contribution in that exact 60-minute window in v17g backtest. If negative, cut to `09:15-12:00` in v17h.
- **Implementation if approved**: change `LONG_SIGNAL_WINDOW_END` from `13:00` to `12:00`. One-line change, fast to test.

### Volume gate tightening

- **Current state**: `>= 0.90× 20-bar avg`
- **Why parked**: part of calibrated v16 core. Loose by traditional impulse-filter standards (1.5-2.0×) but works.
- **Re-open condition**: paired with the quality-score model so volume is one component of a smooth score, not a hard floor.

### Entry buffer change (price-% → ATR-scaled)

- **Current state**: `max(0.05, 0.02% of reference price)`
- **Why parked**: invalidates parity with live paper-trade.
- **Re-open condition**: re-establish live-vs-backtest parity baseline first; then run pure-A/B on buffer formula.
- **Proposed alternative when revisited**: `max(0.05, 0.05× ATR_5min)`.

## B3 — Backup Paths If v17g Fails

### Roll back individual changes

If `v17g-all` fails OOS, the toggle architecture allows shipping the subset of changes that did pass individually. Define `v17g-final-subset` from the per-change A/B results.

### Conservative-only mode

If multiple v17g changes fail, ship only Change 5 (LONG AVWAP cap) — it's the lowest-risk addition and provides robustness without touching alpha generation.

### Revert plan

If v17g causes regression, the runner is import-isolated from v17f, so reverting is `make v17f canonical` — no rollback of v17f code required.

## B4 — Long-Tail / v17i+

### New setups (expansion pass)

Pure expansion — only after v17g + scoring + regime work is done.

- ORB 9:15-9:45 (both sides)
- NR7 / inside-bar compression breakouts
- EMA20 pullback continuation (both sides, ADX>=28)
- Second-touch AVWAP rejection / reclaim
- Gap-fill reversion

Each is its own A/B project with its own hypothesis; do not bundle.

### Setup renaming

Codex proposed renaming to `LONG_CONT_*` form. Rejected for v17g because it loses `A_/B_` and `C1/C2` information. If revisited:
- keep current names
- add orthogonal `module_tag` column populated by setup → tag map
- never replace, only augment

### Early-entry mode

Mentioned in mechanical critique. Variant where entry fires on trigger break, not bar-close confirmation.
- High slippage and false-breakout risk at 5-min resolution
- Must be A/B'd in isolation against confirmation
- Do not bundle with new setups

### "Reject huge-reclaim too far from C1 close"

Codex's quality gate for `B_HUGE_C1_CLOSE_RECLAIM_BREAK`. Good idea but adds a variable to a setup we aren't otherwise changing — bundle with the setup-specific SL/target work in B1.

### Per-side quality score floors

Adding a SHORT QS floor (currently absent) once a unified scoring model exists — coupled to the B2 quality-score model item.

## Conditions To Re-Visit This File

- after `v17g_ab_runner.py` finishes the matrix and `v17g_research_notes.md` is populated
- after any item moves between buckets (B1 ↔ B2 ↔ B3 ↔ B4)
- before opening any v17h research sprint — confirm the prerequisite condition is met

## Items Explicitly Closed (Do Not Re-Open Without New Evidence)

- **Setup renaming to `LONG_CONT_*`**: rejected; replaced by the `module_tag` augmentation plan
- **Replacing all blacklist filters with a quality score**: rejected; structural causal gates stay, only cavity-shaped filters get consolidated
- **Bundling new setups with rearchitecture in one release**: rejected as a working pattern; one major change per release going forward
