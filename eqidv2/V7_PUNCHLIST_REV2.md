# V7 ID 5-min — Punch-List Rev 2

Companion to `v7_live_strategy_full_documentation_today.md` (rev 2, 2026-06-10).
Numbering continues from rev 1 (P0-1 … P2-15, of which 13/15 are Done,
P0-3 partial, P1-8 deferred).

**Context for rev 2.** The 21-day record (681 trades, 26.9% WR, PF 0.82,
₹-54k and likely ₹-85–90k true net) falsifies the OLD configuration. The NEW
configuration is unmeasured (n=9). Rev 2 therefore has one organizing goal:
**stop hand-tuning the losing config; rebuild the accepted set through the
gate on clean data, then qualify it under live-identical conditions.**

Code shipped with this list:

- `gate_promotion.py` — P0-17 (sole author of `accepted_rules.csv`)
- `qualification_tracker.py` — P1-22 (live-enable scoring)
- (already wired from rev 1: `nse_intraday_costs.py`, `walkforward_gate.py`)

Priority key: **P0** = before anything else · **P1** = before the
qualification clock starts · **P2** = during qualification. Effort: S (<½ day),
M (1–3 days), L (>3 days).

---

## P0 — Truth and enforcement

### P0-16 · One aggregation, regenerated NET, contradictions reconciled
- **Problem.** The doc disagrees with itself on its most important numbers:
  period "Apr 21–Jun 10" (§1) vs "May 12–Jun 10" (§20.6); "Day win rate 7/22"
  vs "21 trading days"; T_TREND 40 trades/₹-8,406 (§1) vs 79/₹-21,464
  (§19/§20.6); C_OR_BREAKOUT 207 trades/PF 0.98/₹-20,521 (§1) vs
  61/PF 0.67/₹-19,127 (§19). And the cumulative table is labelled GROSS in the
  preamble while its own line item and §20.6 say "Net PnL: Rs -54,122".
  Disable/enable decisions are being made off conflicting tables.
- **Action.** One aggregation script (extend `v7_nse_id_cost_report.py`) is the
  sole producer of cumulative stats, computed from `net_pnl_rs`, with explicit
  window, side filter, and executed-vs-all-rows definition printed in the
  output. §1 and §20.6 both consume its output verbatim.
- **Acceptance.** Re-published table where every figure in §1 == §20.6; the
  word GROSS appears nowhere near a number derived from `net_pnl_rs`; the
  T_TREND and C_OR_BREAKOUT discrepancies are explained (window? side?) in a
  footnote.
- **Effort.** S–M.

### P0-17 · Gate becomes the gatekeeper (closes rev-1 P0-3)
- **Problem.** `walkforward_gate.py` runs report-only. The accepted set —
  including every hand exemption — is still human-authored, which is the
  process that produced the falsified config.
- **Action.** (1) Regenerate all backtest outcomes on the causal-VWAP pipeline
  (post P1-6 — pre-Jun-10 outcomes are inadmissible). (2) Run the gate on the
  regenerated, net-of-cost outcomes. (3) Wire `gate_promotion.py`: dry-run
  daily for 2–3 sessions, diff against the incumbent set, then `--apply`.
  Demotions land in `shadow_rules.csv` for shadow tracking; promotion audit
  JSON archived per run; `EQIDV2_BLOCKED_SETUPS` overrides PROMOTE.
- **Acceptance.** `accepted_rules.csv` carries
  `schema_version=v7_accepted_gate_*` and `promoted_by=gate_promotion.py`; no
  setup trades without a PROMOTE row behind it; a deliberate hand edit to the
  file is detectable (next run's diff flags it).
- **Effort.** M (plus L for the backtest regeneration it depends on).

### P0-18 · Brake watches MTM and can act
- **Problem.** Jun 4: ₹10k brake, ₹-32k day. 76 EOD closes realized at 15:20 in
  one burst — a realized-only brake whose only action is refusing new entries
  is blind to exactly this shape.
- **Action.** Both executors: brake input = realized + open MTM, evaluated
  every poll cycle; breach action = halt entries AND (flag-gated) tighten/
  flatten open positions; add a per-day new-entry throttle (e.g. 60) and
  per-setup concurrency caps generally (not only C_OR_BREAKOUT). Mirror the
  same semantics into the live auto-kill (₹10k daily / ₹5k per-trade).
- **Acceptance.** Synthetic test: a book of open losers totalling -₹10k MTM
  with zero realized losses trips the brake within one poll cycle; event log
  shows MTM-trigger; throttle and per-setup caps observable in reject reasons.
- **Effort.** S–M.

### P0-19 · Paper mirrors live for the qualification window
- **Problem.** Paper runs 100 positions/₹2M/realized-brake; live will run
  20/₹500k/auto-kill. §1's claim that "the new 20-position cap" prevents a
  Jun-4 recurrence is wrong for paper — that cap is live-only. The
  qualification run is currently testing a different machine than the one
  being switched on.
- **Action.** Set paper env to live values for the window: 20 positions,
  ₹500k, identical brake semantics incl. ₹5k per-trade, same short cap. Snapshot
  both configs (config attestation diff) at window start.
- **Acceptance.** `qualification_tracker.py --attest` checklist confirmed
  against the attestation diff; any later env change restarts the clock.
- **Effort.** S.

---

## P1 — Before the clock starts

### P1-20 · Re-derive (don't just round) every fitted cut
- **Problem.** §9.7 momentum gates still carry six decimals
  (`64.7678`, `16.2111`, `-0.187227`, `42.3138`…) — rev-1 P0-4 missed that
  layer. Deeper: rounding V11 cuts doesn't fix that the cuts were *selected*
  on pre-causal-VWAP backtests.
- **Action.** All thresholds (V11 + momentum gates) re-fit on regenerated
  causal data via the gate's train-only fitter, scored OOS; gates whose
  setups the gate REJECTs are deleted, not tuned.
- **Acceptance.** No active threshold predates the causal-VWAP regeneration;
  none carries >2 decimals without documented natural units.
- **Effort.** M–L (rides on P0-17's regeneration).

### P1-21 · Emergency setup actions — by block, not by patch
- **Problem.** §19's "Act Now" regime-gating of C_OR_BREAKOUT is hand-tuned
  in-sample patching — the old methodology. Meanwhile T_TREND (PF 0.35) is
  only *recommended* disabled, its gates still configured; the one
  short-focus-exempt LONG (A_MOD_BREAK_C1_HIGH) is a PF-0.70 loser; the
  fallback list allows C_OR_BREAKDOWN which §9.7 shadow-blocks.
- **Action.** `EQIDV2_BLOCKED_SETUPS=T_TREND_DAY_EMA_STAIR_SHORT,C_OR_BREAKOUT`
  now (C_OR also has the unresolved zero-entry-row bug — suspend, root-cause,
  let the gate re-admit it later). Revoke the A_MOD_BREAK_C1_HIGH exemption
  pending its gate verdict. Remove C_OR_BREAKDOWN from the fallback list or
  unshadow it — one or the other.
- **Acceptance.** Blocked setups produce zero signal rows (verify via funnel
  card); exemption list is empty until gate-justified; no setup is
  simultaneously allowed in one layer and blocked in another.
- **Effort.** S.

### P1-22 · Replace the coin-flip live-enable criterion
- **Problem.** Simulated: "5 clean days, positive cumulative PF" passes a
  zero-edge system ~49% of the time and the measured PF-0.82 config ~26%.
- **Action.** Adopt `qualification_tracker.py` as §19.3: ≥150 trades under
  mirror config, net PF ≥ 1.15, bootstrap p < 0.10, zero brake trips
  (any trip restarts the clock), 100% of trades from gate-PROMOTEd setups,
  human-attested config parity. At the new lower trade frequency expect
  ~2–3 weeks — that's the cost of knowing.
- **Acceptance.** §19.3 rewritten to reference the tracker; live enable
  requires its QUALIFIED verdict (exit 0) plus a human go.
- **Effort.** S.

### P1-23 · Honest target fills
- **Problem.** TARGET legs fill at the exact limit on touch while SL/EOD legs
  take 5 bps — asymmetric optimism; touched ≠ filled for resting limits.
- **Action.** Count a target fill only when price trades through the limit by
  ≥1 tick (or apply a fill-probability haircut); calibrate against real fills
  once live runs.
- **Acceptance.** Paper target-hit rate recomputed under trade-through;
  per-setup expectancy re-ranked; calibration TODO logged for live.
- **Effort.** S–M.

### P1-24 · Sizing floor must not inflate risk
- **Problem.** Risk sizing targets ₹500 risk (0.25% of ₹200k), but the ₹50k
  min-notional clamp silently raises risk to ~₹600 (0.30%) on wide-SL setups
  (1.2% SL ⇒ ₹41.7k correct notional ⇒ clamped up).
- **Action.** Skip trades whose risk-correct notional is below the floor;
  log `skipped_min_notional` as a funnel reject reason.
- **Acceptance.** No executed trade's at-stop risk exceeds
  `EQIDV2_RISK_PCT_PER_TRADE` ± rounding.
- **Effort.** S.

### P1-28 · Point-in-time universe (rev-1 P1-8, priority raised)
- **Problem.** Deferred in rev 1 — but P0-17 reruns *all* backtests, and a
  survivorship-biased universe contaminates the rerun exactly when its output
  becomes load-bearing.
- **Action.** At minimum, bound the bias before trusting the rerun: compare
  today's universe against historical NSE listing/suspension data for the
  backtest window; if delta is material, rebuild point-in-time membership.
- **Acceptance.** A written estimate of survivorship impact (names added/
  removed per month of history) accompanies the first gate-authored
  accepted set.
- **Effort.** L (bounding pass: M).

---

## P2 — During qualification

### P2-25 · Sign off open config questions
09:40-vs-09:45 for E_VWAP_LOSE_EARLY_SHORT (one line in the doc with an
owner and a date); ranker-weights-sum-1.16 note acknowledged as heuristic.
**Effort.** S.

### P2-26 · C_OR_BREAKOUT zero-entry-row root cause
Still unresolved from 06-09; the setup is blocked (P1-21) so this is now a
bug hunt, not a live risk. Add §19.4–.5 monitors (`no_entry_row_count`,
pre-window alert) as part of the fix.
**Effort.** M.

### P2-27 · LONG-flow health + funnel completeness
§19.10's LONG health check (gated LONG candidates exist but zero LONG signal
rows ⇒ warning), plus stale-lock monitor (§19.11) and broker-reconciliation
card (§19.12).
**Effort.** M.

---

## Suggested order

1. **P0-16** (truth) and **P1-21** (emergency blocks) — same afternoon.
2. **P0-18 + P0-19** (brake MTM, mirror config) — the safety net under
   everything that follows.
3. **P0-17 ← P1-20 ← P1-28-bounding** as one work package: regenerate on
   causal VWAP → re-fit → gate → `gate_promotion.py --apply`. This is the
   critical path; everything else is parallelizable around it.
4. **P1-22/23/24** the day the first gate-authored accepted set goes live in
   paper — then the qualification clock starts.
5. P2 items during the window.

**Definition of done for rev 2:** `qualification_tracker.py` returns
QUALIFIED on ≥150 mirror-config trades drawn exclusively from a
gate-authored accepted set computed on causal, net-of-cost data. Until that
sentence is true, live stays off — and that's the system working, not the
system failing.
