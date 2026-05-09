# v17D Production Roadmap

Sequenced phases. **Each phase has a gate**: don't proceed to the next until the gate passes. The earlier phases are diligence (cheap, decide whether to continue); later phases are feature work (expensive, only worth it if diligence passes).

---

## Binding constraint: minimum trade count floor

Intraday strategies need volume. v17C lab numbers are ~4.4 trades/day (978 trades / 222 days). v17D is built around a hard constraint:

- **Floor:** average >= 4.5 trades/day across the backtest window
- **Target:** 5–6 trades/day at higher PF
- **Worst-day floor:** 2–3 trades on the quietest days is acceptable; <= 1 is not

This constraint is **binding on every filter decision** in Phase 2. Filters that violate the floor are rejected even if they lift PF.

### Per-setup tier-based filtering (instead of global filters)

Apply gating selectively by setup PF, not uniformly. The mistake is applying the same filter set to every setup.

| Setup PF tier | Filter strategy | Rationale |
|---|---|---|
| PF >= 2.20 (elite) | **No new filters** | Already clean, gating only kills count |
| PF 1.80–2.20 (good) | **1 high-impact gate** | Marginal lift, controlled count cost |
| PF 1.40–1.80 (borderline) | **2–3 gates** OR drop | Where filters actually pay |
| PF < 1.40 | **Drop or rebuild** | Filters won't save these |

Mapping to current Cand-E4 setups:

| Setup | PF | Action in v17D |
|---|---|---|
| LONG B_HUGE_*_RECLAIM_BREAK | 2.49 | Don't touch |
| LONG B_AVWAP_RECLAIM_REVERSAL | 2.41 | Don't touch |
| LONG A_MOD_BREAK_C1_HIGH | 2.38 | Don't touch |
| SHORT A_MOD_BREAK_C1_LOW | 2.10 | Add 1 gate (ADX) |
| LONG D_EMA20_BOUNCE | 1.89 | Add 2 gates (ADX + sector RS) |
| SHORT G_LOWER_LOW_BREAK | 1.44 | Add multi-TF gate or drop |

### Trade count budget

| Lever | Direction | Magnitude (trades/day) |
|---|---|---|
| Selective gating on weak setups | Cost | -0.4 to -0.7 |
| ATR rank replacing fixed ATR% (Step 2.4) | Recovery | +0.5 to +1.0 |
| Time-stop freeing capital (Step 3.3) | Recovery | +0.3 to +0.5 |
| Re-enable C_OR_BREAKDOWN with strict filter | Recovery | +0.5 to +1.0 |
| Re-enable H_FAILED_BREAKOUT_TRAP after backtest | Recovery | +0.5 to +1.0 |
| One new setup (ORB-failure-and-reverse) | Recovery | +0.3 to +0.5 |
| Loosen `bars_from_open <= 9` to `<= 11` | Recovery | +0.3 to +0.5 |

**Net budget:** roughly +2.0 to +3.5/day recovery vs -0.4 to -0.7/day cost.

### Landing zone targets

| Metric | v17C now | v17D realistic | v17D stretch |
|---|---|---|---|
| Trades/day avg | 4.4 | 5.0–5.5 | 6.0+ |
| Trades/day worst day | 1–2 | 2–3 | 3–4 |
| PF | 2.16 (claimed) | 2.30–2.60 | 2.80+ |
| Active setups | 6 | 10–14 | 16+ |

---

## Setup expansion philosophy: wide net, then filter

**Strategy:** spec a large library of diverse setup candidates (15–25), backtest all on the same window/cost model, ship only those that clear an objective bar. This is the right research methodology for two reasons:

1. **Diversification across logic types.** Your current 8 setups are 80% breakout-flavored (`A_MOD_BREAK_*`, `B_HUGE_*`, `C_OR_*`, `G_LOWER_LOW_BREAK`). When breakouts globally fail (chop regime), the entire portfolio fails together. Adding mean-reversion, volume-climax, and pattern setups creates regime diversification.
2. **Objective elimination beats subjective curation.** "Spec 20, ship the top 10" produces better strategies than "spec 8 and hope." The cost is backtest time; the benefit is robustness.

### Funnel: spec → backtest → ship → monitor → drop

```
Library spec (20-25 candidates)
  │
  ▼
Tier-A backtest (60 days, lab cost model)
  │   Drop if PF < 1.30 or n < 30
  ▼
Tier-B backtest (220 days, realistic cost model from Phase 1.2)
  │   Drop if PF < 1.50 OR OOS PF < 1.30
  ▼
Shadow mode (engine-scan enabled, sized 0.0)
  │   30 trading days minimum
  │   Drop if live PF < 1.40 or feature drift detected
  ▼
Pilot live (size_mult = 0.25x, cap=2-3/day)
  │   60 trading days minimum
  │   Drop if PF < 1.40 OR drawdown > 2x backtest DD
  ▼
Production (size_mult per Cand-E sizing tier)
  │   Continuous monitoring per Phase 4.5 drift, Phase 4.4 rolling PF
  ▼
Setup graveyard (drop log: setup name, reason, date, supporting numbers)
```

**Key principle:** dropping a setup is success, not failure. The graveyard should be longer than the production list.

### Setup library — 20 candidates across 6 logic families

#### Family 1: Trend continuation (4 candidates)
| ID | Setup | Entry trigger | Indicators |
|---|---|---|---|
| TC-1 | Pullback-to-EMA20-bounce | Price tags EMA20 + reversal candle in trend direction | EMA20, ADX>=22, di+/- |
| TC-2 | Pullback-to-EMA50-bounce | Same as TC-1 but deeper pullback | EMA50, ADX>=25 |
| TC-3 | Higher-high higher-low momentum | Confirmed HH+HL pattern + breakout | Swing detection, volume |
| TC-4 | Trend-day first-pullback (ADX>30) | First pullback to VWAP on trend day | ADX>=30, VWAP, ATR rank |

#### Family 2: Mean reversion (4 candidates)
| ID | Setup | Entry trigger | Indicators |
|---|---|---|---|
| MR-1 | Bollinger touch + RSI extreme | BB touch + RSI <=20 (long) or >=80 (short) | BB, RSI |
| MR-2 | VWAP fade | Price >= 2 ATR from VWAP + reversal candle | VWAP, ATR |
| MR-3 | Overextended EMA reversion | Price >= 3 ATR from EMA20 + first counter-bar | EMA20, ATR |
| MR-4 | 3-bar drive + reversal | 3 consecutive same-direction bars + opposite-direction close | Candle pattern, vol |

#### Family 3: Breakout (4 candidates, refines existing)
| ID | Setup | Entry trigger | Indicators |
|---|---|---|---|
| BO-1 | Donchian 20-bar high/low | New 20-bar high/low + volume confirmation | Donchian, vol ratio |
| BO-2 | Squeeze release (BB inside Keltner) | BB-inside-Keltner for >=10 bars + first directional break | BB, Keltner |
| BO-3 | Higher-TF level break | Daily/weekly S/R level cleared on 5-min | HTF pivots |
| BO-4 | OR-15min breakout (existing C_OR_BREAKOUT, restored) | Break above 15-min OR-high | OR, vol |

#### Family 4: Pattern-based (3 candidates)
| ID | Setup | Entry trigger | Indicators |
|---|---|---|---|
| PT-1 | Gap-and-go | Open gap >=1.5% + first 5-min hold above (or below) prior close | Gap %, vol |
| PT-2 | Gap-fill fade | Open gap >=2% + price reaches 50% gap fill within 60min + reversal | Gap %, time |
| PT-3 | Inside-bar breakout | Bar fully inside prior bar range + break direction next bar | Bar geometry |

#### Family 5: Volume / order flow (3 candidates)
| ID | Setup | Entry trigger | Indicators |
|---|---|---|---|
| VO-1 | Climax volume reversal | Volume >=3x SMA20 + opposite-direction close | Volume, vol_ratio |
| VO-2 | Low-volume drift + acceleration | 5-bar volume <= 0.7x SMA + vol spike >=1.5x with directional bar | Volume regime |
| VO-3 | OBV divergence | Price HH + OBV LL (short) or price LL + OBV HH (long) | OBV |

#### Family 6: Time-of-day (2 candidates)
| ID | Setup | Entry trigger | Indicators |
|---|---|---|---|
| TD-1 | Opening drive (first 15 min) | First 5-min bar > 1.5x ATR + continuation | ATR, time |
| TD-2 | Late-day reversal | After 14:00 IST + reversal of intraday trend with vol spike | Time, vol, prior trend |

### Setup expansion in roadmap phases

This library plugs into the existing roadmap as follows:

- **Phase 2 setup work:** Steps 2.6 (C_OR re-enable), 2.7 (FBT ship), 2.8 (ORB-failure-and-reverse) are already in scope. **NEW** Phase 2.10 below.
- **Phase 2.10 — Wide-net setup library backtest** (1 week)
  - Spec the 20 candidates above as detector functions in `eqidv2/v17D_setup_library/`
  - Each detector returns the same row schema as existing setups (compatible with `_v17C_E_post_resolve_pipeline`)
  - Run Tier-A backtest on full window with lab cost model
  - Drop fail-fast: anything with PF < 1.30 or n < 30 over 60 days
  - Tier-B backtest on survivors with realistic cost model from Step 1.2
  - **Deliverable:** ranked list of survivors with PF/win/n per setup
  - **Gate:** at least 5 new setups clear PF >= 1.50 and OOS PF >= 1.30
- **Phase 2.11 — Add survivors to filter spec** (2 days)
  - Add survivors to `CANDIDATE_E_FILTER_SPEC` with conservative initial sizing (0.25x–0.50x)
  - Set per-setup daily caps (cap=3 typical for new entrants)
  - Run integrated backtest to confirm interaction with existing setups doesn't cause new correlation issues (re-run Step 0.5 audit)
- **Phase 4.8 — Setup graveyard log** (half day)
  - Markdown file logging every dropped setup with date, reason, supporting numbers
  - Periodic re-evaluation: setups dropped for in-sample reasons may be re-tested when market regime changes

### Expected impact

| Outcome | v17C now | After Phase 2.10/2.11 |
|---|---|---|
| Setups in spec | 8 (6 active) | 25–28 specced, 12–16 active |
| Trades/day | 4.4 | 6–8 (more setups, each cap=3) |
| Logic-family diversification | 1 (breakouts) | 6 (full library) |
| Regime robustness | Low (breakouts fail in chop) | High (mean-rev catches chop, breakouts catch trend) |
| Backtest cost | One-time ~3 days compute | Worth it |
| Maintenance overhead | Low | Higher (more rolling-PF monitors needed) |

**The "wide net" approach is what the existing v17 honesty-fix infrastructure was built for.** Step 0.5 (correlation audit) and Step 4.5 (drift detection) already protect against the failure modes of having too many setups. Use them.

### What you do NOT do

- **Don't ship all 20 to live.** Production list = top 10–14 by OOS PF, with diversification rule (at most 4 setups per logic family).
- **Don't keep low-PF setups for "completeness."** PF < 1.40 → graveyard. Strategy size matters less than strategy quality.
- **Don't run more setups than you can monitor.** Each setup needs rolling-PF tracking. If you can't look at 16 charts a day, ship 8.
- **Don't skip Tier-A → Tier-B → Shadow → Pilot funnel.** Skipping straight from backtest to production is how setups that look great in-sample silently lose money.

---

## Phase 0 — Diligence: is the edge real? (3–4 days)

**Goal:** prove PF=2.16 isn't a costs/overfit illusion before investing weeks of build work.

### Step 0.1 — Slippage stress test
- Add a CLI flag `--cost-multiplier` to `_v17C_E_post_resolve_pipeline`.
- Re-run backtest at `_E3_TGT_COSTS_PCT = 0.30%`, `_E3_SL_COSTS_PCT = 0.40%` (realistic Indian intraday round-trip).
- **Gate:** combined PF stays >= 1.50. If it collapses to 1.0–1.3, the strategy is largely a slippage-arbitrage artifact — **stop here**, fix costs assumption first.

### Step 0.2 — Threshold perturbation sensitivity
- Script that iterates each numeric threshold in `CANDIDATE_E_FILTER_SPEC` by +/-5%, +/-10%, +/-15%.
- Re-run, plot PF vs perturbation per setup.
- **Gate:** at least 4 of 6 active setups must hold PF >= 1.50 under +/-10% perturbation. Drop the ones that don't — they're overfit.

### Step 0.3 — Walk-forward holdout audit
- Identify dates used to tune Cand-E4 thresholds.
- Re-run on the **post-tuning** period only (truly out-of-sample, never seen by tuner).
- **Gate:** OOS PF >= 1.40 and OOS win-rate >= 60%. If OOS halves the in-sample number, you're curve-fit.

### Step 0.4 — MAE/MFE per setup
- For each closed trade, compute MAE (Maximum Adverse Excursion) and MFE (Maximum Favorable Excursion) from 1-min bars.
- Plot MAE/MFE distributions per (side, setup).
- **Deliverable:** new SL/TGT recommendations per setup based on where 80% of winners' MFE sits and where 80% of losers' MAE sits. Feeds into Phase 3.

### Step 0.5 — Setup correlation audit
- Pairwise (date, ticker, bar) overlap for every setup pair.
- **Gate:** any pair with Jaccard > 0.5 must be merged or one dropped. No double-counting.

### Phase 0 exit decision

After Phase 0 you have one of three outcomes:

- **Green:** PF survives, OOS holds, <= 1 setup dropped → proceed to Phase 1.
- **Yellow:** PF degrades but stays >= 1.30 → proceed but with reduced expectations. Productionise as a 0.3x-size pilot.
- **Red:** PF collapses below 1.20 → **don't productionise**. Re-do tuning with proper walk-forward before any live capital.

---

## Phase 1 — Production foundations (1 week)

**Goal:** make the runner safe to deploy regardless of how good the signal is. These changes protect capital.

### Step 1.1 — Universe hard filter
File: new `eqidv2/v17D_universe_filter.py`. Pre-filter at scan time:
- ADV (20-day) >= Rs.50 cr
- Price >= Rs.50, <= Rs.5000
- F&O membership (load NSE F&O list weekly)
- Price-band check: skip 5%/10%/20% circuit-limit stocks

### Step 1.2 — Realistic cost model (replace ad-hoc constants)
- New module `eqidv2/v17D_cost_model.py` computing per-trade costs from price, side, qty.
- Replace `_E3_TGT_COSTS_PCT` / `_E3_SL_COSTS_PCT` with `cost_model.estimate(trade)` returning brokerage + STT + GST + exch fees + stamp duty + per-stock slippage estimate.
- Slippage based on ADV bucket: top-100 ADV → 0.05%, 101–300 → 0.10%, rest → 0.20%.

### Step 1.3 — Externalize all tunables to YAML
- Move `CANDIDATE_E_FILTER_SPEC`, `CANDIDATE_E3_SL_TGT`, `CANDIDATE_E_SETUP_CONFIG`, governor constants into `eqidv2/configs/v17D.yaml`.
- Loaded at runtime, validated against a schema.
- **Why:** in production you'll tune these without code redeploys. Hardcoded constants are a deployment bottleneck.

### Step 1.4 — Cluster / sector risk caps
Add to governors:
- max 4 concurrent trades per sector per side
- max 6 LONGs in any single sector per day
- soft net-beta cap: if `net_long_count - net_short_count > 8`, throttle next signals at 0.5x size

### Step 1.5 — Kill-switch infrastructure
- Env var `V17D_KILL_SWITCH=1` → process exits cleanly without taking new trades, exits all open positions at market.
- File-based switch `~/v17D_kill` → same.
- Daily DD kill: if intraday PnL <= -2.5% capital, halt new entries, hold open positions.
- Weekly DD kill: if 5-day rolling PnL <= -5%, halt for next session, require manual re-arm.

### Step 1.6 — Logging discipline
- Structured JSON logs (one event per line) for every signal, every filter rejection, every governor decision, every order, every fill.
- Separate streams: `signals.log`, `decisions.log`, `orders.log`, `fills.log`, `pnl.log`.
- Persist to disk + push to a daily summary file.

### Phase 1 gate
- Run a 30-day historical backtest with all Phase 1 changes wired in. Compare to Phase 0 baseline.
- **Acceptable degradation:** 10–20% PF drop is normal (real costs + tighter universe). PF still >= 1.40 is the green flag.

---

## Phase 2 — Signal improvements with count floor (the v17D core, 2.5 weeks)

**Goal:** lift PF while respecting the >= 4.5 trades/day floor. Apply filters by setup tier; let backtest data choose which filter combination wins per setup.

### Step 2.0 — Constrained Pareto search per setup (4 days)

Don't prescribe filters globally. Extend `_v17C_candE_setup_tuner.py` to:

1. **Filter candidate menu** (per Steps 2.1–2.5 below): ADX, DI+/-, sector RS, multi-TF stack, ATR rank, two-stage confirmation, two-bar volume confirmation
2. **Search space:** each filter ON/OFF per setup independently
3. **Constraints:**
   - Total trades/day average across all setups >= 4.5
   - Per-setup floor: tier-1 (PF >= 2.20) setups not gated
   - Per-setup OOS PF >= 1.40 (no overfit)
4. **Objective:** maximize combined PF
5. **Output:** per-setup filter chain that wins under the constraints

This replaces my earlier prescriptive list. Backtest data picks; I don't.

### Step 2.1 — Filter candidate: ADX/DI (1 day)
Available to the Pareto search:
- `adx_signal >= 22`
- `di_plus - di_minus >= 5` for LONGs, `<= -5` for SHORTs

Likely winners (based on filter character): breakout setups (`A_MOD_BREAK_*`, `B_HUGE_*`, `G_LOWER_LOW_BREAK`).

### Step 2.2 — Filter candidate: Sector RS (replace synthetic NIFTY) (2 days)
- Compute per-stock sector ETF mapping (BANKNIFTY, NIFTYIT, NIFTYAUTO, etc.).
- Features: `sector_rs_5d`, `sector_intraday_rs_pct`, `sector_above_vwap` (boolean).
- Replace `_v17C_no_nifty_context` with `_v17D_sector_context` whose thresholds are tunable in the search.

### Step 2.3 — Filter candidate: Multi-timeframe trend stack (3 days)
- Pre-compute per ticker: 15-min EMA20 slope, 60-min EMA50 above EMA200, daily close above EMA20.
- Search-tunable: require 1, 2, or 3 of 3 HTFs aligned (or skip entirely for that setup).
- **Note:** this is the most count-expensive filter. Likely only chosen by the search for the weakest setups.

### Step 2.4 — Filter candidate: ATR percentile rank (1 day, **count expander**)
- `atr_pct_rank_60d >= 0.30` (per-stock relative) replacing fixed `atr_pct_signal >= 0.0040`.
- Unlocks low-priced/low-ATR stocks systematically excluded today.
- **Expected: +0.5 to +1.0 trades/day.** Apply broadly unless PF degrades materially per setup.

### Step 2.5 — Filter candidate: Two-stage entry confirmation (1 day)
- Signal at bar T, enter only if bar T+1 closes in signal direction.
- Likely chosen by the search for noisiest setups only (`D_EMA20_BOUNCE`).

### Step 2.6 — Re-enable C_OR_BREAKDOWN with strict filter (2 days, **count expander**)

Currently sized 0 (PF 1.00 raw). Build a strict filter chain:
- ADX >= 25 (trend confirmation)
- Sector RS <= 0 (sector also weak)
- ATR rank >= 0.40 (volatility regime)
- Cap = 3 trades/day

**Target:** OOS PF >= 1.60 with cap=3/day. If achieved, re-enable at 0.50x.
**Expected: +0.5 to +1.0 trades/day** if shipped.

### Step 2.7 — Phase 2b: H_FAILED_BREAKOUT_TRAP backtest and ship (2 days, **count expander**)

Engine-scan already enabled (`PHASE2B_FBT_ENGINE_SCAN_ENABLED = True`), sized 0. Run 60-day backtest, evaluate using v17D cost model.

**Target:** PF >= 1.80, cap=3/day. If achieved, set `enabled=True, size_mult=0.50`.
**Expected: +0.5 to +1.0 trades/day** if shipped.

### Step 2.8 — New setup: ORB-failure-and-reverse (3 days, **count expander**)

Different from `H_FAILED_BREAKOUT_TRAP` (wick-rejection at top). ORB-failure-and-reverse:
- Stock breaks above 15-min OR-high
- Fails to hold for 2 bars
- Closes back below OR-high → SHORT entry
- Mirror for LONG: break below OR-low, fail, reclaim

Backtest spec end-to-end. Ship if PF >= 1.80 with cap=3/day.
**Expected: +0.3 to +0.5 trades/day** if shipped.

### Step 2.9 — Loosen `bars_from_open` (half day, **count expander**)

`A_MOD_BREAK_C1_HIGH` has `bars_from_open <= 9.0` (= 09:15–10:00 IST). Test loosening to `<= 11.0` (= 09:15–10:10 IST). Often a small extra window unlocks 0.3–0.5 trades/day with negligible PF impact. Search-tunable in Step 2.0.

### Phase 2 gate
- 60-day backtest with Phase 1 + Phase 2. Compare to Phase 1 baseline.
- **Required:** average trades/day >= 4.5 (the binding constraint).
- **Required:** PF lift >= 0.20 OR drawdown reduction >= 30% with PF flat.
- **Required:** worst-day count >= 2 trades on >= 90% of trading days.
- If count drops below 4.5/day, the Pareto search must be re-run with tighter constraints OR more count-expander steps shipped.

---

## Phase 3 — Risk/exit refinements (1 week)

### Step 3.1 — Per-setup SL/TGT from MAE/MFE (Phase 0.4 output)
- Replace Pareto-picked `CANDIDATE_E3_SL_TGT` with values derived from MAE/MFE distributions.
- Maintain R:R >= 1.0 invariant.

### Step 3.2 — ATR-anchored stops with floor
- `SL = max(0.75%, k_sl x ATR%)` per setup, k tuned per setup.
- Same for TGT. Honors your invariant *and* adapts to volatility.

### Step 3.3 — Time-stop
- New exit type `TIME_STOP`: if trade hasn't reached 0.3R favorable in 45 min, exit at market.
- Backtest impact: usually frees ~15% of capital with negligible PF impact.

### Step 3.4 — Trailing stop post-1R
- After 1R favorable: stop to breakeven.
- After 1.5R: trail at 0.5R behind highest favorable price.
- Apply to setups with highest MFE (B_HUGE_*, A_MOD_BREAK_*).

### Step 3.5 — Volatility-targeted sizing
- Replace fixed Rs.20K x multiplier with `qty = (target_risk_rs) / (entry_price x SL%)` so per-trade Rs-risk is constant.
- Multiplier still applies on top.

### Phase 3 gate
- Sharpe ratio improvement and max-DD reduction. PF roughly flat is fine.
- If max-DD doesn't drop >= 20%, the work isn't paying off — skip the parts that didn't help.

---

## Phase 4 — Live infrastructure (1.5 weeks, can parallel Phase 3)

**Goal:** make the strategy observable and manageable in live trading.

### Step 4.1 — Paper-trading shadow mode
- Run v17D against live market data for 20 trading days, **no real orders**.
- Log every signal, simulated fill at next-bar open, simulated PnL.
- **Gate:** paper PF within 70% of backtest PF. Significant gap = backtest unrealistic.

### Step 4.2 — Pre-market sanity check (cron 08:30 IST)
Daily automated checklist that aborts trading if any fail:
- Universe file present and updated within 7 days
- All required parquets exist for today's universe
- Broker connection healthy
- No earnings events on watchlist for today (mark those tickers as restricted, don't halt all trading)
- Yesterday's trade reconciliation complete (broker fills match expected)

### Step 4.3 — Broker integration with retry/idempotency
- Order placement with client-side idempotency token (so retries don't double-fill).
- Reconcile orders every 30s against broker status; recover from partial fills.
- Order timeout: cancel + retry once after 5s; log + skip after 2 failures.

### Step 4.4 — Live monitoring dashboard
Minimal: a single HTML page or terminal UI showing:
- Open positions, P&L per position, time in trade
- Today's stats: trades taken, win/loss, PnL
- Each governor's drop count today
- Per-setup rolling PF (last 30 trades) with bootstrap CI
- Feature distribution drift flags (Phase 4.5)

### Step 4.5 — Feature drift detection (KS test)
Daily job: KS-test each feature in today's signals against the in-sample distribution. Alert if `p < 0.01` for any feature on 3 consecutive days. **This is your early warning** that thresholds are degrading before PnL tells you.

### Step 4.6 — Daily reconciliation
End-of-day script:
- Backtest predicted vs actual fills (expected entry/exit price vs actual).
- Slippage actuals fed back into Phase 1.2 cost model (auto-tune).
- PnL match (broker statement vs internal ledger). Mismatch > Rs.100 → alert.

### Step 4.7 — Runbook
Plain-text doc covering:
- How to start the system in the morning
- How to kill it
- What to do if a position is stuck open
- What to do on disconnect
- Manual override procedures
- On-call escalation

### Phase 4 gate
- 20 paper-trading days complete. Daily reconciliation runs cleanly. Drift alerts work. Kill-switches tested at least once.

---

## Phase 5 — Pilot live (2 weeks at reduced size)

### Step 5.1 — Capital ramp
- Day 1–3: 0.10x of intended size. Monitor every trade.
- Day 4–10: 0.30x if no incidents.
- Day 11–14: 0.50x if PF and slippage match paper.
- Day 15+: 1.0x only after passing exit gate.

### Step 5.2 — Daily journal
Manual: 5 min per evening logging anomalies, surprises, broker behavior, market structure observations. Compounds into invaluable diagnostic over weeks.

### Phase 5 exit gate
- 14 live days. Live PF within 60% of paper PF. No production incidents (kill-switch trips, broker errors, reconciliation gaps, missed exits). Then full size.

---

## Phase 6 — ML meta-filter (deferred, only after live track record exists)

**Don't do this until Phase 5 is complete with at least 100 live trades.**

You need real live trades as ground truth labels — backtest labels are biased by your own slippage/fill assumptions. Wire `ml_meta_filter.py` in as discussed earlier:

1. Shadow mode 30 days (log `p_win` only, no gating)
2. Compare gated-vs-ungated PF
3. Wire as Phase 2c stage if and only if Phase 5 still leaves room (PF < 1.7 and you want it higher)

---

## Summary timeline

| Phase | Duration | Type | Skip if |
|---|---|---|---|
| 0 — Diligence | 3–4 days | Investigation | Never skip |
| 1 — Foundations | 1 week | Capital safety | Never skip |
| 2 — Signal (with count floor) | 2.5 weeks | Feature work + setup expansion | Phase 0 says PF marginal — skip to Phase 3 |
| 3 — Risk/Exits | 1 week | Refinement | Phase 2 already lifted PF and DD acceptable |
| 4 — Live infra | 1.5 weeks (parallel 3) | Operational | Never skip |
| 5 — Pilot | 2 weeks | Validation | Never skip |
| 6 — ML | 4+ weeks | Future work | Strategy already at target performance |

**Total realistic timeline to live capital at full size: 9–11 weeks.**

---

## What to start tomorrow

Three things, in order:

1. **Step 0.1 (slippage stress test) — half day.** This single experiment determines whether you're building on rock or sand.
2. **Step 0.4 (MAE/MFE analysis) — 1 day.** Even if you do nothing else, this rewrites your SL/TGT picks with real data not Pareto search.
3. **Step 1.5 (kill-switch + Step 1.6 logging) — 1 day.** Even if you keep running v17C, these protect capital today.

If you only have a week, do those three. Everything else can wait.

---

## Quick reference: count vs PF tradeoff cheatsheet

| Change type | PF | Trades/day |
|---|---|---|
| Diligence (Phase 0) | Reveals truth | Same |
| Universe filter (1.1) | Up | Down |
| Realistic cost model (1.2) | Honest reset down | Same |
| ADX gate (2.1) | Up | Down |
| Sector RS (2.2) | Up | Down |
| Multi-TF stack (2.3) | Up | **Big down** |
| **ATR rank (2.4)** | Flat/up | **Up** |
| Two-stage entry (2.5) | Up | Down |
| **Re-enable C_OR (2.6)** | Up if filter works | **Up** |
| **Re-enable FBT (2.7)** | Up if backtest passes | **Up** |
| **New ORB-failure (2.8)** | Up if backtest passes | **Up** |
| **Loosen bars_from_open (2.9)** | Slight down | **Up** |
| MAE/MFE SL/TGT (3.1) | Up | Same |
| ATR-anchored stops (3.2) | Slight up | Same |
| **Time-stop (3.3)** | Flat | **Up** (capital recycles) |
| Trailing stop (3.4) | Up | Same |
| Vol-targeted sizing (3.5) | Flat (Sharpe up) | Same |

Rule of thumb: a v17D plan that ships **steps 2.4, 2.6, 2.7, 2.8, 2.9, 3.3** alongside selective gating in 2.1–2.3 is the count-floor-respecting recipe. Skip 2.3 (multi-TF) on tier-1 and tier-2 setups; let the Pareto search apply it only where the count budget allows.
