# v17D-r1 Production Baseline (Cand-E4 refined)

Run date: 2026-05-13
Frozen artifact: [trades_candE4_production_FINAL.csv](trades_candE4_production_FINAL.csv) (n=227)

This is the new v17D production baseline after a full diligence + refinement pass on the canonical 987-trade Cand-E4 trades CSV.

---

## What's in the baseline

### Setups (5 active)

| Side | Setup | SL % | TGT % | R:R | Size mult | Lab PF (claim) | Production PF (this run) |
|---|---|---|---|---|---|---|---|
| LONG | A_MOD_BREAK_C1_HIGH | 0.91 | 1.02 | 1.12 | 1.30x | 2.38 | **1.35** |
| LONG | B_AVWAP_RECLAIM_REVERSAL | 0.93 | 1.00 | 1.07 | 1.30x | 2.41 | **1.19** |
| LONG | B_HUGE_C1_CLOSE_RECLAIM_BREAK | 0.92 | 0.95 | 1.03 | 1.30x | 2.49 | **1.62** (n=4) |
| LONG | D_EMA20_BOUNCE | 1.02 | 1.02 | 1.00 | 1.00x | 1.89 | **1.60** |
| SHORT | A_MOD_BREAK_C1_LOW | 0.88 | 0.90 | 1.02 | 1.00x | 2.10 | **2.11** |

### Setups dropped from v17C-Cand-E4

| Setup | Reason | Drop trigger |
|---|---|---|
| SHORT G_LOWER_LOW_BREAK | NOT_ROBUST | Phase 0.2: PF ~1.42 across ±10% perturbation, fragile |
| SHORT C_OR_BREAKDOWN | NO_EDGE | Inherited from v17C (PF 1.00 raw) |
| SHORT D_EMA20_REJECTION | NO_EDGE | Inherited from v17C (PF 0.83) |
| SHORT C_OR_BREAKOUT | NOT_IN_CANDE | Was in experimental run, not Cand-E4 |

### Pareto-derived per-setup filters (Step 2.0)

| Setup | Filter rule | Effect |
|---|---|---|
| LONG B_HUGE_C1_CLOSE_RECLAIM_BREAK | `atr_pct_rank_60d >= 0.30` | IS PF 1.83 → 2.27; OOS held at 3.10 |

All other 4 setups are tier-1 (IS PF >= 2.20) and receive no additional filters.

### Universe filter (Phase 1.1)

ADV-bucketed filter — **drop long_tail (< Rs 50 cr ADV)**. This is binding:
- long_tail in the input: PF 0.83, Rs -42,231 (LOSING)
- mid (Rs 50–500 cr): PF 1.24, Rs +22,941
- top100 (≥ Rs 500 cr): PF 2.32, Rs +9,425

Without the filter, production PF = 0.972 (losing). With the filter, PF = 1.319.

### Cost model (Phase 1.2)

Per-row realistic costs from `v17D_cost_model.costs_pct_for_v17C(adv_bucket, outcome)`:
- top100 (TARGET / SL): 0.207% / 0.237%
- mid (TARGET / SL): 0.307% / 0.337%
- (long_tail filtered out so its cost never applies)

These costs include: brokerage (Rs 20 cap), STT (sell leg), NSE+SEBI+GST charges, stamp duty (buy leg), ADV-bucketed slippage.

---

## Production performance metrics

| Metric | Value | Roadmap gate | Status |
|---|---|---|---|
| Trades | 227 over 127 trading days | n ≥ 30/setup over 60d | mixed |
| Trades/day | **1.79** | **>= 4.5** | **FAIL** |
| PF (sized, realistic costs) | **1.319** | >= 1.50 GREEN / >= 1.30 RED gate | **YELLOW (just above RED)** |
| Win % | 67.84 | n/a | strong |
| Day-win % | 65.35 | n/a | strong |
| Sum PnL Rs (eff, sized) | +32,367 | positive | positive |

---

## Binding constraint

**Trades/day = 1.79 is well below the 4.5/day floor.** The roadmap (line 318–322) calls this the binding constraint for the whole strategy.

The honest read: Cand-E4 lab numbers (PF 2.16 @ 4.4 trades/day) were inflated by illiquid trades that don't survive realistic costs. Once the long_tail bucket is correctly priced and filtered out, count collapses to 1.79/day.

To clear the count floor while keeping PF, the strategy needs more high-quality signals on the Rs 50cr+ ADV universe. Options:

1. **Tune existing setups for more frequent signals on liquid stocks** — relax `bars_from_open`, drop strict avwap_dist gates that were Pareto-tuned on the wider universe
2. **Add new setups specifically targeting liquid names** — different design from the 20-detector library that just failed Tier-A
3. **Accept 1.79/day operationally** — proceed at 0.3x pilot size per YELLOW exit decision; this is a viable but low-volume strategy

---

## What's been built

### Phase 0 (diligence)
- Step 0.1 slippage stress test (sizing-aware patch)
- Step 0.2 threshold perturbation (5/6 setups robust)
- Step 0.4 MAE/MFE analyzer (wider SL/TGT picks)
- Step 0.5 correlation audit (max Jaccard 0.0)

### Phase 2 (Pareto)
- Step 2.0 Pareto search per setup
- Step 2.10 library Tier-A (0 of 20 survived)

### Phase 1 (foundations) — partial
- 1.1 universe filter (Rs 50 cr ADV mandatory)
- 1.2 cost model applied per-row via `v17D_apply_realistic_costs`
- 1.3 YAML config externalized (need to update with refined values)
- 1.4–1.6: scaffolded, not exercised

### Phase 3 (risk/exits) — partial
- 3.1 MAE/MFE SL/TGT applied via `v17D_reresolve_with_mae_mfe`
- 3.2–3.5: deferred

---

## What's NOT done

- **Phase 0.3 walk-forward holdout** — should re-run with the post-tuning split
- **Phase 1.2 runner integration** — costs are applied as a post-resolve step, not inside the live runner. Live trades won't auto-use realistic costs until the runner is patched.
- **Sector RS feature** — sector ETF parquets don't exist on this laptop; Pareto search ran without sector_rs dimension
- **Walk-forward OOS validation** — IS/OOS split was naive 80/20 by date; the proper walk-forward sliding window hasn't been run
- **Phase 4 live infra** — paper-trading harness, broker integration, monitoring dashboard, drift detection cron — all scaffolded in modules but not exercised
- **Phase 4b tax analytics** — module written but not run on the refined CSV

---

## Files / commands to reproduce

```powershell
$base = "c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting"
$tr = "$base\eqidv2\outputs_v17D_phase0\trades_candE4_refined_alladv.csv"
$enr = "$base\eqidv2\outputs_v17D_phase0\trades_candE4_refined_enriched.csv"
$prod = "$base\eqidv2\outputs_v17D_phase0\trades_candE4_production.csv"
$u = "$base\eqidv2\configs\universe.csv"

# Enrich with DI+/-, ATR rank
python -m eqidv2.v17D_enrich_features --trades $tr --indicator-dir "C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2" --daily-dir "C:\TradingData\eqidv2\stocks_indicators_daily_eq" --output $enr

# Pareto search (verifies B_HUGE wants atr_rank>=0.30)
python -m eqidv2.v17D_pareto_search --raw-trades $enr --is-end-date 2026-03-15 --output "$base\eqidv2\outputs_v17D_phase0\pareto_candE4_enriched.json"

# Apply realistic costs + Pareto filter
python -m eqidv2.v17D_apply_realistic_costs --trades $enr --universe $u --output $prod --apply-pareto

# Final: filter to mid+top100 ADV (see inline filter script in chat)
```

Frozen final CSV: [trades_candE4_production_FINAL.csv](trades_candE4_production_FINAL.csv)

---

## Recommended next session work

1. **Investigate the count cliff.** Trades/day was 4.4 in lab, 3.7 on all-ADV refined, 1.79 on mid+top100. Where does the count loss come from per setup? Some setups (D_EMA20_BOUNCE, B_HUGE) may be near-extinct on liquid universe — they need to be either tuned for liquid stocks or graveyarded.
2. **Phase 0.3 walk-forward.** Run sliding-window OOS instead of a single split — B_AVWAP_RECLAIM_REVERSAL OOS=1.30 and SHORT A_MOD_BREAK_C1_LOW OOS=1.40 are at the gate; need to see if they hold across multiple windows.
3. **Patch the live runner to use `v17D_cost_model`** for honest live PnL.
4. **Phase 4b tax analytics** on the production CSV — confirms net-per-trade after Indian intraday tax is still > Rs 50.
5. **Decide go/no-go on pilot.** YELLOW at PF 1.32 with 1.79 trades/day = small-size pilot is plausible per roadmap. Or queue up signal expansion work first to clear the count floor.
