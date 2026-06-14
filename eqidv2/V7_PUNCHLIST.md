# V7 ID 5-min — Prioritized Improvement Punch-List

Companion to `v7_live_strategy_full_documentation_today.md`.
Two code files ship with this list:

- `nse_intraday_costs.py` — NSE intraday-equity cost model (P0-1, P0-3).
- `walkforward_gate.py` — walk-forward OOS promotion gate (P0-3, P0-4, P1-9).

Priority key: **P0** = do before trading another rupee live · **P1** = edge
integrity · **P2** = execution realism + ops hardening. Effort: S (<½ day),
M (1–3 days), L (>3 days).

---

## P0 — Correctness & capital safety

### P0-1 · Make the headline P&L NET, not gross
- **Problem.** Paper executor fills at `ltp_on_signal` and reports `Gross paper
  P&L`. At 0.6–1.5% targets, costs are 10–25% of gross edge per trade; some
  setups go negative net. The number you optimise against is currently fiction.
- **Action.** Wire `nse_intraday_costs.cost_trades_frame()` into
  `avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.py`. Add `gross_pnl`,
  `total_cost`, `net_pnl`, `cost_bps_of_turnover` to `paper_trades_<date>.csv`.
  Drive `paper_trade_summary_*.json` and the dashboard headline off **net**.
- **Acceptance.** Dashboard P&L card reads "Net"; per-trade cost column present;
  setup-level expectancy table re-ranked by net. STT charged on the correct leg
  (entry for shorts, exit for longs).
- **Effort.** S.

### P0-2 · Live daily-loss circuit breaker → kill switch
- **Problem.** Paper runner has a ₹10k daily loss brake; the **live** control
  list (§3.8) shows max positions/capital/timeouts but no explicit daily-loss
  limit. On the real-money path this is the most dangerous gap.
- **Action.** Add a hard per-day realised+unrealised loss limit and a per-trade
  loss cap to `avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.py`; on breach,
  write `kill_switch_false_id_5min_v7.json` and stop new entries (keep managing
  open exits).
- **Acceptance.** Forced synthetic loss in a dev run trips the kill switch and
  blocks new orders within one poll cycle; event logged.
- **Effort.** S.

### P0-3 · Walk-forward gate as the ONLY path into `accepted_rules.csv`
- **Problem.** It's unclear how `accepted_rules.csv` / V11 thresholds were
  derived. If fit and evaluated on the same window, every backtest stat is
  inflated.
- **Action.** Feed resolved candidate trades into `walkforward_gate.run_gate()`.
  Only `decision == PROMOTE` rows may write the V8/V11 accepted contract. Persist
  the full decision report (incl. `net_pf_is`, `oos_is_ratio`, `overfit_flag`,
  `p_value`, `fdr_significant`) per cycle.
- **Acceptance.** `accepted_rules.csv` regenerates solely from gate PROMOTE
  output; rejected/probation setups are quarantined (shadow-only).
- **Effort.** M.

### P0-4 · Round the thresholds and re-run
- **Problem.** `quality >= 97.873364`, `vol ratio <= 1.698991`,
  `pre2_mom_r >= -0.187227` — 6-decimal cuts are optimiser artifacts.
- **Action.** Round every V11/pre-momentum threshold to 1–2 sig figs (natural
  units), then re-evaluate through the gate (P0-3). Keep only edges that survive
  rounding **and** clear the OOS bar.
- **Acceptance.** No promoted rule carries >2 decimal places without a documented
  reason; each promoted threshold has a gate decision row behind it.
- **Effort.** M.

---

## P1 — Edge integrity

### P1-5 · Decide short-only vs long+short, then validate THAT
- **Problem.** Research filter is short-focused; 2026-06-09 had zero longs and a
  fragile `C_OR_BREAKOUT` long funnel. Trading a short-only subset of a
  long+short-validated system means your live distribution is untested.
- **Action.** Pick one. If short-only: re-run the gate on short-only data and
  rebuild expectations. If both: fix the long funnel (the `C_OR_BREAKOUT`
  candidate→entry drop) and remove blanket short-focus.
- **Acceptance.** The validated universe == the traded universe; an explicit
  signed-off decision recorded in the doc.
- **Effort.** M.

### P1-6 · VWAP / market-return causality audit
- **Problem.** Live adapter reuses the parquet VWAP column for V2/V7 parity. If
  that column is a full-session VWAP computed EOD, using it at 09:30 is
  look-ahead and inflates every backtest.
- **Action.** Verify the VWAP column is cumulative-to-the-bar (anchored, causal)
  at signal time; confirm `market_ret_pct` is return-to-now not return-to-close;
  confirm `day_value_so_far_rs` semantics. Add a unit test that recomputes VWAP
  causally and asserts equality with the parquet column at each bar.
- **Acceptance.** Test passes on a sample day; any mismatch fixed before further
  backtests are trusted.
- **Status 2026-06-10.** Done for the production feature path. `_prepare_5m`
  now preserves incoming parquet VWAP as `VWAP_source` and always recomputes
  `VWAP` as causal session VWAP before V7 uses it. Audit runner:
  `bat/run_v7_causality_audit.bat`. Latest report:
  `C:\TradingData\eqidv2\v7_causality_audit\latest\v7_causality_audit.md`.
  Sample 2026-06-09 audit on `stocks_indicators_5min_eq_live`:
  `PASS_WITH_SOURCE_WARNINGS` because source VWAP is bad/missing, but prepared
  VWAP, `day_value_so_far_rs`, and market return all pass.
- **Effort.** M.

### P1-7 · Risk-based sizing + exposure governors
- **Problem.** Fixed ₹10k margin with 0.70–1.20% stops → rupee-risk varies by
  setup; up to 20 correlated shorts = one bet; no regime gate on a structurally
  short-vol book.
- **Action.** Size each trade to a fixed % of equity at the stop (e.g.
  0.25–0.5%). Add a gross-short-exposure cap and a per-sector / correlation cap.
  Add a NIFTY regime gate (you already fetch NIFTY guard data) that halves or
  suppresses short sizing when NIFTY is above a rising 20-day on the daily.
- **Acceptance.** Per-trade rupee risk constant across setups; gross short
  exposure capped; regime gate observable on the dashboard.
- **Effort.** M.

### P1-8 · Point-in-time universe (survivorship) check
- **Problem.** A 1,000+ name NSE universe backtested with *today's* listed names
  drops blow-ups/delistings and includes names only recently liquid.
- **Action.** Confirm `universe.csv` is reconstructed as-of each historical date.
  If not, rebuild a point-in-time universe and re-run the gate.
- **Acceptance.** Backtest universe membership matches what was tradable on each
  historical day.
- **Effort.** L.

### P1-9 · Multiple-testing correction
- **Problem.** ~26 setups × gates × thresholds → some win by chance.
- **Action.** Already implemented in the gate (Benjamini-Hochberg FDR across
  setups). Keep `fdr_alpha` honest (≤0.10); never promote a non-FDR-significant
  setup.
- **Acceptance.** Promotion requires `fdr_significant == True`.
- **Effort.** S (config/discipline).

---

## P2 — Execution realism & ops hardening

### P2-10 · Model exit slippage
- **Problem.** Paper fills at the stop/target price via 5-sec polling; real stops
  gap through and fill worse. LTP entry fills are optimistic vs the touch+impact.
- **Action.** In the paper executor, fill exits at `stop ± slippage_bps` (and
  optionally entries at touch + impact_bps), parameterised per liquidity bucket.
  Re-run the gate with slippage on.
- **Acceptance.** Net expectancy reported under a realistic slippage assumption;
  sensitivity table (0 / 5 / 10 / 20 bps) produced.
- **Effort.** M.

### P2-11 · Participation / ADV liquidity cap
- **Problem.** `Early min traded value Rs 1,000,000 / 5-min` is a low bar; thin
  names show great untradeable backtest stats.
- **Action.** Add a per-trade size cap as a fraction of recent ADV / rolling
  volume, and drop candidates where intended notional exceeds participation
  limits.
- **Acceptance.** No candidate sized above X% of its recent ADV; thin-name edges
  re-checked under the cap.
- **Effort.** M.

### P2-12 · Borrow / F&O-ban pre-trade filter (shorts)
- **Problem.** Names in F&O ban or with restricted intraday shorting reject or
  fill poorly (`mis_rejected_symbols` already observes this after the fact).
- **Action.** Pull the daily F&O ban list and shortability status pre-open;
  exclude blocked names from short candidates before signal write.
- **Acceptance.** Zero short signals on ban-list names; pre-trade exclusion
  logged.
- **Effort.** M.

### P2-13 · Replay → separate namespace by default
- **Problem.** Replay writes to production candidate/dashboard paths; one stray
  env var corrupts live state (§16.3, §20.5).
- **Action.** Default replay output to a `replay/` namespace; require an explicit
  flag to target production paths (invert the current default).
- **Acceptance.** Replay run leaves production `latest/` untouched unless
  deliberately overridden.
- **Effort.** S.

### P2-14 · Schema versioning for candidate/audit CSVs
- **Problem.** Repeated schema-backup churn (§16.1) → downstream readers silently
  miss columns and the reality-gap analysis breaks without erroring.
- **Action.** Stamp a `schema_version` column and write a wide union schema;
  readers validate expected columns and warn on drift.
- **Acceptance.** No silent column loss across a schema change; readers log
  version mismatches.
- **Effort.** M.

### P2-15 · Funnel card + reject-reason surfacing
- **Problem.** Candidate→entry drops (e.g. `C_OR_BREAKOUT`) and stale-reject
  no-trade slots are only visible post-hoc.
- **Action.** Add the dashboard funnel card: raw → V8 passed → V11 selected →
  research passed → entry rows → signal rows → trades, per slot and per setup;
  surface writer reject reasons (`skipped_stale_entry`, freshness, pre-momentum)
  inline so a quiet day ≠ a broken day.
- **Acceptance.** A candidate→entry drop is visible live within one refresh;
  reject reasons shown on the card.
- **Effort.** M.

---

## Suggested order

1. P0-1 (net costs) and P0-2 (live loss brake) — fast, and everything downstream
   depends on a real P&L and a safety net.
2. P0-3 + P0-4 + P1-9 (gate, rounding, FDR) — one work package: stand up the gate
   and let it re-decide every rule.
3. P1-5 / P1-6 (short-only decision, VWAP causality) — settle what you trade and
   that the backtest isn't peeking.
4. P1-7 (sizing/exposure/regime), then P2 in any order, with P2-13 early (cheap
   foot-gun removal) and P2-10/P2-11 before scaling size.

Close P0-1 and P0-3 first and you move from *hoping* the edge is real to
*knowing* — net of costs, out of sample.
