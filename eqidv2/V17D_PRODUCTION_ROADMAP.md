# v17D Production Roadmap

Sequenced phases from v17C noNF Cand-E4 to live production. Each phase has a gate; don't proceed until it passes.

---

## TL;DR

- **9–11 weeks** from start to full-size live capital.
- **Floor:** average >= 4.5 trades/day, target 6–8/day.
- **Quality bar:** PF >= 1.50 after realistic costs (Phase 1.2 cost model), OOS PF >= 1.30.
- **Approach:** wide-net 20-setup library, filter via objective backtest, ship only the survivors.
- **Day-1 actions:** slippage stress test (Step 0.1), MAE/MFE analysis (Step 0.4), kill-switch (Step 1.5).

---

## Roles: what Claude does vs what you do manually

### Claude-automatable (delegate freely)

| Task | Phase | Notes |
|---|---|---|
| Write all 20 setup detectors | 2.10 | Per spec in Setup Library section |
| Extend Pareto search tuner | 2.0 | Code on `_v17C_candE_setup_tuner.py` |
| Cost model module | 1.2 | `eqidv2/v17D_cost_model.py` |
| Universe filter module | 1.1 | `eqidv2/v17D_universe_filter.py` |
| YAML config externalization + schema | 1.3 | `eqidv2/configs/v17D.yaml` |
| KS-test drift detection script | 4.5 | Daily cron job |
| Reconciliation script | 4.6 | EOD broker vs internal ledger |
| Monitoring dashboard (HTML/terminal) | 4.4 | Open positions, PnL, governor logs |
| Paper-trading harness | 4.1 | Simulated fills against live data |
| Slippage stress test runner | 0.1 | CLI flag + analysis |
| MAE/MFE analyzer | 0.4 | Per-setup distribution plots |
| Threshold perturbation script | 0.2 | +/-5/10/15% sweeps |
| Walk-forward holdout splitter | 0.3 | Date-based split utility |
| Setup correlation audit | 0.5 | Pairwise Jaccard matrix |
| Backtest analysis reports | All | PF, win, DD, day-win tables |
| Kill-switch + logging plumbing | 1.5/1.6 | Env-var + file-based switches |
| Sector ETF mapping | 2.2 | Per-stock sector lookup table |
| Multi-TF feature precompute | 2.3 | 15-min/60-min/daily snapshots |
| ATR rank precompute | 2.4 | Per-stock 60-day rolling rank |
| Setup library detectors | 2.10 | All 20 specs |
| Graveyard log auto-append | 4.8 | On setup drop event |

### Manual (your decisions/actions)

| Task | Phase | Why manual |
|---|---|---|
| Broker API setup (Kite keys, tokens) | 1/4 | Account-level credentials |
| Capital allocation decisions | 5 | Risk tolerance is yours |
| Approve/reject setup library candidates after Tier-B | 2.10 | Final judgment call |
| Decide go/no-go to Phase 5 live | 5 | Cannot be automated |
| Pilot ramp pacing (0.10x → 1.0x) | 5.1 | Daily incident review |
| Calibrate kill-PF thresholds (G6) | post-pilot | Need >= 30 live trades per setup |
| Read graveyard, decide re-eval timing | ongoing | Regime judgment |
| Tax + regulatory compliance | ongoing | Legal/CA scope |
| Maintain data feeds (parquet pipelines) | ongoing | Operational ownership |
| Daily P&L journal entries | 5.2+ | Manual observation |
| Halt/restart after DD events | ongoing | Discretionary risk control |
| Approve YAML config diffs before deploy | ongoing | Change management |
| Earnings calendar maintenance | 4.2 | Curated source |

### Joint (Claude proposes, you approve)

| Task | Notes |
|---|---|
| Filter chain selections from Pareto search | Claude runs search, you approve final per-setup chain |
| Setup library survivors | Claude reports Tier-B PF/OOS PF/n; you decide which ship |
| SL/TGT changes from MAE/MFE | Claude proposes new picks; you approve |
| Sizing tier changes per setup | Claude calculates, you approve |
| Live model artifact deploys (Phase 6) | Claude generates, you approve before swap |

---

## Feedback architecture: how the loop closes

Every phase produces a **decision** that feeds the next. Don't proceed to a phase whose input wasn't validated.

```
Phase 0 (diligence)
    │ output: is edge real? PF estimate, OOS verdict
    ▼
Decision: proceed / pilot-only / halt
    │
Phase 1 (foundations)
    │ output: PF after realistic costs
    ▼
Decision: target PF level for Phase 2 work
    │
Phase 2 (signal + library)
    │ output: per-setup filter chain, library survivors, count/day
    ▼
Decision: which setups go live, which sized 0, which graveyard
    │
Phase 3 (risk/exits)
    │ output: new SL/TGT, sizing methodology, DD profile
    ▼
Decision: max-DD acceptable? Sharpe target met?
    │
Phase 4 (live infra)
    │ output: paper PF vs backtest PF
    ▼
Decision: live deployment safe? Slippage matches?
    │
Phase 5 (pilot)
    │ output: live PF vs paper PF, incident log
    ▼
Decision: ramp to full size? Halt? Retune?
    │
Phase 6 (ML, optional)
    │ output: gated PF lift in shadow mode
    ▼
Decision: wire ML into production?
```

### Continuous feedback (post-deployment)

Once live, three loops run continuously:

| Loop | Frequency | Trigger | Action |
|---|---|---|---|
| Drift detection (KS test) | Daily | p < 0.01 on 3 consecutive days | Alert; review feature |
| Per-setup rolling PF (G6) | Per-trade | Rolling PF < kill_pf_30 or kill_pf_60 | Auto-halt setup |
| Reconciliation | EOD | Mismatch > Rs.100 | Alert; investigate |
| Daily DD | Real-time | Intraday PnL <= -2.5% | Halt new entries |
| Weekly DD | EOW | 5-day rolling <= -5% | Halt next session, manual re-arm |
| Graveyard re-eval | Quarterly | Regime shift detected | Re-run Tier-A on graveyard candidates |

---

## P&C (permutations & combinations): the search space

The Pareto search in Step 2.0 is the heart of v17D. Here is what it actually searches.

### Per-setup filter combinations

For each setup independently:

| Filter | States |
|---|---|
| ADX gate | ON (with threshold {20, 22, 25, 28}) / OFF |
| DI+/- separation | ON (with min separation {3, 5, 8}) / OFF |
| Sector RS | ON (LONG threshold {-0.5%, -0.3%, 0%}) / OFF |
| Multi-TF stack | OFF / 1-of-3 / 2-of-3 / 3-of-3 |
| ATR rank gate | ON (threshold {0.20, 0.30, 0.40, 0.50}) / OFF |
| Two-stage entry | ON / OFF |
| `bars_from_open` | {<=4, <=6, <=9, <=11, no gate} |
| Volume ratio | ON (threshold {1.0, 1.3, 1.5, 2.0}) / OFF |

Per-setup search space: ~5 × 4 × 4 × 4 × 5 × 2 × 5 × 5 = **40,000 combinations per setup**, all 20 setups = **800,000 total combinations**. With pruning (don't gate tier-1 setups, drop combinations that violate count floor early) the actual evaluated space is ~50K backtests, doable overnight.

### Per-setup SL/TGT grid

| SL % | TGT % | Constraint |
|---|---|---|
| 0.75, 0.80, 0.85, 0.90, 1.00 | 0.80, 0.85, 0.90, 1.00, 1.20, 1.50 | TGT >= SL, TGT >= 0.80, SL >= 0.75 |

Plus ATR-anchored variants (Step 3.2): `k_sl × ATR%` for `k_sl ∈ {0.7, 0.8, 1.0, 1.2}` floored at 0.75%.

### Sizing tier × cap grid

For each setup post-Pareto:

| size_mult | max_daily_trades |
|---|---|
| {0.0, 0.25, 0.50, 1.00, 1.30} | {None, 2, 3, 5} |

Constraint: size_mult > 0 if and only if PF >= 1.40 in Tier-B.

### Search constraints (binding)

- **Trade count:** total avg >= 4.5/day, worst-day >= 2 on 90% of days
- **OOS:** every setup OOS PF >= 1.30
- **Per-tier filter rule:** tier-1 (PF >= 2.20) setups receive zero filters
- **Diversification:** at most 4 setups per logic family in production
- **Correlation:** max pair Jaccard < 0.5 in active set
- **Risk:** max-DD <= 50%

---

## Master pre-live deployment checklist

Every box must be ticked before any live capital. Run through this in order; mark date next to each box. Halt deployment if any check fails.

### Diligence (Phase 0)
- [ ] Step 0.1 slippage stress test passes (PF >= 1.50 at 0.30%/0.40% costs) — date: ____
- [ ] Step 0.2 threshold perturbation passes (>= 4 of 6 setups hold at +/-10%) — date: ____
- [ ] Step 0.3 walk-forward OOS passes (PF >= 1.40, win >= 60%) — date: ____
- [ ] Step 0.4 MAE/MFE analysis complete; new SL/TGT picks proposed — date: ____
- [ ] Step 0.5 setup correlation audit clean (max Jaccard < 0.5) — date: ____

### Foundations (Phase 1)
- [ ] Step 1.1 universe filter live; F&O list refreshed — date: ____
- [ ] Step 1.2 cost model integrated; backtest re-run — date: ____
- [ ] Step 1.3 YAML config externalized + schema-validated — date: ____
- [ ] Step 1.4 cluster/sector caps integrated — date: ____
- [ ] Step 1.5 kill-switches (env-var, file, daily DD, weekly DD) tested — date: ____
- [ ] Step 1.6 logging streams writing to disk — date: ____
- [ ] Phase 1 backtest gate: PF >= 1.40 — date: ____

### Signal + library (Phase 2)
- [ ] Step 2.0 Pareto search complete; per-setup chains chosen — date: ____
- [ ] Step 2.4 ATR rank backtested vs fixed thresholds — date: ____
- [ ] Step 2.6 C_OR re-enable backtest passed — date: ____
- [ ] Step 2.7 FBT backtest passed; sized 0.50x — date: ____
- [ ] Step 2.8 ORB-failure backtest passed; sized 0.50x — date: ____
- [ ] Step 2.10 library Tier-A backtest complete — date: ____
- [ ] Step 2.10 library Tier-B backtest complete; survivors identified — date: ____
- [ ] Step 2.11 survivors integrated; correlation audit re-run — date: ____
- [ ] Phase 2 gate: avg >= 4.5/day, PF lift >= 0.20 — date: ____

### Risk/exits (Phase 3)
- [ ] Step 3.1 SL/TGT updated from MAE/MFE — date: ____
- [ ] Step 3.2 ATR-anchored stops integrated — date: ____
- [ ] Step 3.3 time-stop integrated — date: ____
- [ ] Step 3.4 trailing stop integrated — date: ____
- [ ] Step 3.5 vol-targeted sizing integrated — date: ____
- [ ] Phase 3 gate: max-DD down >= 20%, Sharpe up — date: ____

### Live infrastructure (Phase 4)
- [ ] Step 4.1 paper-trading harness live; 20 days completed — date: ____
- [ ] Step 4.1 paper PF >= 70% of backtest PF — date: ____
- [ ] Step 4.2 pre-market sanity check cron deployed — date: ____
- [ ] Step 4.3 broker integration with idempotency tested — date: ____
- [ ] Step 4.4 monitoring dashboard accessible — date: ____
- [ ] Step 4.5 KS drift detection running daily — date: ____
- [ ] Step 4.6 daily reconciliation script running EOD — date: ____
- [ ] Step 4.7 runbook complete; reviewed — date: ____
- [ ] Step 4.8 graveyard log file initialized — date: ____
- [ ] Phase 4 gate: reconciliation clean, kill-switches tested — date: ____

### Tax + brokerage analytics (Phase 4b)
- [ ] Step 4b.1 daily cost breakdown table generated for full backtest — date: ____
- [ ] Step 4b.2 per-setup efficiency: every kept setup net_per_trade >= Rs 50 — date: ____
- [ ] Step 4b.3 tax-optimal recommendations executed (drops moved to graveyard) — date: ____
- [ ] Step 4b.4 year-end speculative income estimate computed; CA briefed — date: ____
- [ ] Step 4b.5 Kite contract-note reconciliation matches cost model — date: ____
- [ ] Vol-targeted sizing floor recalibrated for cost amortization — date: ____

### Pre-live operational
- [ ] Capital allocation decided and ring-fenced — date: ____
- [ ] Pilot ramp schedule reviewed (0.10 → 0.30 → 0.50 → 1.00) — date: ____
- [ ] Daily journal template created — date: ____
- [ ] On-call/escalation contacts confirmed — date: ____
- [ ] Tax/compliance review (CA consulted on intraday GST/STT) — date: ____
- [ ] Backup broker credentials available — date: ____
- [ ] Power/internet failover plan reviewed — date: ____

### Pilot live (Phase 5)
- [ ] Day 1–3 at 0.10x: zero incidents — date: ____
- [ ] Day 4–10 at 0.30x: PF + slippage match paper — date: ____
- [ ] Day 11–14 at 0.50x: live PF >= 60% of paper PF — date: ____
- [ ] Phase 5 gate: full-size approval — date: ____

**Only after every box ticked: ramp to 1.0x.**

---

## Quick reference: count vs PF tradeoff

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
| **Wide-net library (2.10)** | Up | **Big up** |
| MAE/MFE SL/TGT (3.1) | Up | Same |
| ATR-anchored stops (3.2) | Slight up | Same |
| **Time-stop (3.3)** | Flat | **Up** (capital recycles) |
| Trailing stop (3.4) | Up | Same |
| Vol-targeted sizing (3.5) | Flat (Sharpe up) | Same |

**Rule of thumb:** ship steps marked **bold** (count expanders) alongside selective gating in 2.1–2.3 (let Pareto search apply only where count budget allows).

---

## Master acceptance criteria

Every gate in one place. Phase doesn't ship until its row passes.

| Phase | Gate | Required | Reject if |
|---|---|---|---|
| 0.1 | Slippage stress test | PF >= 1.50 at 0.30%/0.40% costs | PF < 1.30 |
| 0.2 | Threshold perturbation | >= 4 of 6 setups hold PF >= 1.50 at +/-10% | < 4 hold |
| 0.3 | Walk-forward OOS | OOS PF >= 1.40, win >= 60% | OOS halves IS |
| 0.4 | MAE/MFE per setup | Distribution complete, new SL/TGT picks proposed | Data missing > 10% |
| 0.5 | Setup correlation | Max pair Jaccard < 0.5 | Any pair >= 0.5 |
| 1 (overall) | Phase 1 backtest | PF >= 1.40 with realistic costs | PF < 1.30 |
| 2 (overall) | Phase 2 backtest | Avg trades/day >= 4.5 AND (PF lift >= 0.20 OR DD reduction >= 30%) | Either fails |
| 2.6 | C_OR re-enable | OOS PF >= 1.60 with cap=3/day | < 1.60 |
| 2.7 | FBT ship | OOS PF >= 1.80 with cap=3/day | < 1.80 |
| 2.8 | ORB-failure ship | OOS PF >= 1.80 with cap=3/day | < 1.80 |
| 2.10 | Library Tier-A | Drop fail-fast: PF >= 1.30 and n >= 30 over 60d | Below either |
| 2.10 | Library Tier-B | PF >= 1.50 and OOS PF >= 1.30 | Below either |
| 3 (overall) | Risk/exit gate | Sharpe up AND max-DD down >= 20% | DD doesn't drop |
| 4.1 | Paper-trading | Paper PF within 70% of backtest PF over 20d | < 70% |
| 4 (overall) | Live infra gate | Reconciliation clean, drift detection live, kill-switches tested | Any gap |
| 4b.1 | Daily cost breakdown | Cost > 40% of gross flagged on < 20% of trading days | More flagged days |
| 4b.2 | Per-setup efficiency | Each kept setup has net_per_trade >= Rs 50 and cost_pct_of_gross <= 35% | Below either |
| 4b.3 | Tax-optimal recommendations | Setups flagged DROP actually dropped or resized | Ignored flags |
| 4b.5 | Kite reconciliation | Broker actuals match cost model within +/-5% per component | Larger drift |
| 5.1 | Pilot ramp | Live PF within 60% of paper PF over 14d, no production incidents | Either fails |
| 6 (overall) | ML shadow gate | Gated PF lift >= 0.20 over 30d shadow | No lift |

---

## Binding constraint: minimum trade count floor

Intraday strategies need volume. v17C lab: ~4.4 trades/day (978 trades / 222 days). v17D adds a hard constraint:

- **Floor:** avg >= 4.5 trades/day across the backtest window
- **Target:** 5–6 trades/day at higher PF; stretch 6–8/day with library expansion
- **Worst-day floor:** >= 2 trades on >= 90% of trading days

This constraint is **binding on every filter decision** in Phase 2.

### Per-setup tier-based filtering

Apply gating selectively by setup PF, not uniformly.

| Setup PF tier | Filter strategy | Rationale |
|---|---|---|
| PF >= 2.20 (elite) | No new filters | Already clean; gating only kills count |
| PF 1.80–2.20 (good) | 1 high-impact gate | Marginal lift, controlled count cost |
| PF 1.40–1.80 (borderline) | 2–3 gates OR drop | Where filters actually pay |
| PF < 1.40 | Drop or rebuild | Filters won't save these |

Mapping to current Cand-E4:

| Setup | PF | Action |
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
| ATR rank replacing fixed ATR% (2.4) | Recovery | +0.5 to +1.0 |
| Time-stop freeing capital (3.3) | Recovery | +0.3 to +0.5 |
| Re-enable C_OR_BREAKDOWN (2.6) | Recovery | +0.5 to +1.0 |
| Re-enable H_FAILED_BREAKOUT_TRAP (2.7) | Recovery | +0.5 to +1.0 |
| ORB-failure-and-reverse (2.8) | Recovery | +0.3 to +0.5 |
| Loosen `bars_from_open <= 9` to `<= 11` (2.9) | Recovery | +0.3 to +0.5 |
| Wide-net library survivors (2.10) | Recovery | +1.5 to +3.0 |

**Net budget:** +3.5 to +7.0/day recovery vs -0.4 to -0.7/day cost.

### Landing zone targets

| Metric | v17C now | v17D realistic | v17D stretch |
|---|---|---|---|
| Trades/day avg | 4.4 | 6–7 | 8+ |
| Trades/day worst day | 1–2 | 2–3 | 3–4 |
| PF | 2.16 (claimed) | 2.30–2.60 | 2.80+ |
| Active setups | 6 | 12–14 | 16+ |
| Logic-family diversification | 1 (breakouts) | 5–6 families | 6 families |

---

## Phase 0 — Diligence: is the edge real? (3–4 days)

**Goal:** prove PF=2.16 isn't a costs/overfit illusion before investing weeks of build work.

### Step 0.1 — Slippage stress test
- Add CLI flag `--cost-multiplier` to `_v17C_E_post_resolve_pipeline`.
- Re-run at `_E3_TGT_COSTS_PCT = 0.30%`, `_E3_SL_COSTS_PCT = 0.40%`.
- **Gate:** combined PF stays >= 1.50.

### Step 0.2 — Threshold perturbation sensitivity
- Iterate each numeric threshold in `CANDIDATE_E_FILTER_SPEC` by +/-5%, +/-10%, +/-15%.
- Plot PF vs perturbation per setup.
- **Gate:** at least 4 of 6 active setups hold PF >= 1.50 under +/-10% perturbation.

### Step 0.3 — Walk-forward holdout audit
- Identify dates used to tune Cand-E4.
- Re-run on the **post-tuning** period only.
- **Gate:** OOS PF >= 1.40 and OOS win-rate >= 60%.

### Step 0.4 — MAE/MFE per setup
- For each closed trade, compute MAE/MFE from 1-min bars.
- Plot distributions per (side, setup).
- **Deliverable:** new SL/TGT recommendations from where 80% of winners' MFE sits and where 80% of losers' MAE sits.

### Step 0.5 — Setup correlation audit
- Pairwise (date, ticker, bar) overlap for every setup pair.
- **Gate:** max Jaccard < 0.5.

### Phase 0 exit decision

- **Green:** PF survives, OOS holds, <= 1 setup dropped → proceed.
- **Yellow:** PF >= 1.30 → proceed at 0.3x pilot size.
- **Red:** PF < 1.20 → halt; re-tune with proper walk-forward before any live capital.

---

## Phase 1 — Production foundations (1 week)

**Goal:** runner is safe to deploy regardless of signal quality.

### Step 1.1 — Universe hard filter
File: `eqidv2/v17D_universe_filter.py`. Pre-filter at scan time:
- ADV (20-day) >= Rs.50 cr
- Price >= Rs.50, <= Rs.5000
- F&O membership (NSE F&O list, refreshed weekly)
- Skip 5%/10%/20% circuit-limit stocks

### Step 1.2 — Realistic cost model
File: `eqidv2/v17D_cost_model.py`. Per-trade costs from price/side/qty: brokerage + STT + GST + exchange fees + stamp duty + ADV-bucketed slippage (top-100 → 0.05%, 101–300 → 0.10%, rest → 0.20%).

### Step 1.3 — Externalize tunables to YAML
Move `CANDIDATE_E_FILTER_SPEC`, `CANDIDATE_E3_SL_TGT`, `CANDIDATE_E_SETUP_CONFIG`, governor constants to `eqidv2/configs/v17D.yaml`. Schema-validated. Tune without redeploys.

### Step 1.4 — Cluster / sector risk caps
Governor additions:
- max 4 concurrent trades per sector per side
- max 6 LONGs in any single sector per day
- soft net-beta cap: throttle to 0.5x size if `net_long − net_short > 8`

### Step 1.5 — Kill-switch infrastructure
- Env var `V17D_KILL_SWITCH=1` → clean exit, close open positions.
- File switch `~/v17D_kill` → same.
- Daily DD kill: intraday PnL <= -2.5% capital → halt new entries.
- Weekly DD kill: 5-day rolling PnL <= -5% → halt next session, manual re-arm.

### Step 1.6 — Logging discipline
Structured JSON, one event per line, separate streams: `signals.log`, `decisions.log`, `orders.log`, `fills.log`, `pnl.log`. Persist to disk + daily summary.

### Phase 1 gate
30-day backtest with Phase 1 wired in. Acceptable: 10–20% PF drop from honest costs. PF >= 1.40 = green.

---

## Phase 2 — Signal improvements with count floor (2.5 weeks)

**Goal:** lift PF respecting >= 4.5 trades/day floor. Per-setup tier filtering; data-driven Pareto search.

### Step 2.0 — Constrained Pareto search per setup (4 days)
Extend `_v17C_candE_setup_tuner.py`:
- **Filter menu:** Steps 2.1–2.5 candidates
- **Search space:** each filter ON/OFF per setup independently
- **Constraints:** total avg >= 4.5/day, tier-1 setups not gated, per-setup OOS PF >= 1.40
- **Objective:** max combined PF
- **Output:** per-setup filter chain that wins under constraints

### Step 2.1 — Filter candidate: ADX/DI (1 day)
- `adx_signal >= 22`
- `di_plus - di_minus >= 5` for LONG, `<= -5` for SHORT
- Likely winners: breakout-flavored setups.

### Step 2.2 — Filter candidate: Sector RS (2 days)
Replace synthetic NIFTY context. Compute per-stock sector ETF mapping. Features: `sector_rs_5d`, `sector_intraday_rs_pct`, `sector_above_vwap`. Search-tunable thresholds.

### Step 2.3 — Filter candidate: Multi-TF trend stack (3 days)
Per-ticker pre-compute: 15-min EMA20 slope, 60-min EMA50 above EMA200, daily close above EMA20. Search-tunable: 1/2/3 of 3 alignment. **Most count-expensive filter — likely chosen only for weakest setups.**

### Step 2.4 — Filter candidate: ATR rank (1 day, count expander)
`atr_pct_rank_60d >= 0.30` replacing fixed `atr_pct_signal >= 0.0040`. Unlocks low-priced/low-ATR stocks. **Expected: +0.5 to +1.0 trades/day.**

### Step 2.5 — Filter candidate: Two-stage entry (1 day)
Signal at bar T, enter only if T+1 closes in signal direction. Likely chosen only for noisiest setups (`D_EMA20_BOUNCE`).

### Step 2.6 — Re-enable C_OR_BREAKDOWN (2 days, count expander)
Strict filter chain: ADX >= 25, sector RS <= 0, ATR rank >= 0.40, cap=3/day. Target: OOS PF >= 1.60. Ship at 0.50x.

### Step 2.7 — Phase 2b H_FAILED_BREAKOUT_TRAP ship (2 days, count expander)
Engine-scan already enabled. Run 60-day backtest. Target: PF >= 1.80, cap=3/day. Ship at 0.50x.

### Step 2.8 — New setup: ORB-failure-and-reverse (3 days, count expander)
- Stock breaks above 15-min OR-high, fails to hold 2 bars, closes back below → SHORT
- Mirror for LONG
Target: PF >= 1.80, cap=3/day. Ship at 0.50x.

### Step 2.9 — Loosen `bars_from_open` (half day, count expander)
Test `<= 11` (vs `<= 9`) on `A_MOD_BREAK_C1_HIGH`. Often unlocks 0.3–0.5 trades/day with negligible PF cost.

### Step 2.10 — Wide-net setup library backtest (1 week, count expander)
- Spec all 20 candidates from setup library (see Setup Library section below) as detector functions in `eqidv2/v17D_setup_library/`.
- Each detector returns same row schema as existing setups.
- **Tier-A backtest:** 60 days, lab cost model. Drop if PF < 1.30 or n < 30.
- **Tier-B backtest:** 220 days, realistic cost model. Drop if PF < 1.50 or OOS PF < 1.30.
- **Deliverable:** ranked list of survivors with PF/win/n.
- **Gate:** at least 5 new setups clear Tier-B.

### Step 2.11 — Add survivors to filter spec (2 days)
Add survivors to `CANDIDATE_E_FILTER_SPEC` at conservative initial sizing (0.25x–0.50x), per-setup cap=3/day. Re-run Step 0.5 correlation audit. Drop any survivor with Jaccard >= 0.5 vs existing setups.

### Phase 2 gate
60-day backtest with Phase 1 + Phase 2:
- Avg trades/day >= 4.5 (binding)
- PF lift >= 0.20 OR DD reduction >= 30% with PF flat
- Worst-day count >= 2 on >= 90% of trading days

---

## Phase 3 — Risk/exit refinements (1 week)

### Step 3.1 — Per-setup SL/TGT from MAE/MFE
Replace Pareto-picked `CANDIDATE_E3_SL_TGT` with values from Step 0.4 distributions. Maintain R:R >= 1.0.

### Step 3.2 — ATR-anchored stops with floor
`SL = max(0.75%, k_sl × ATR%)` per setup. Honors invariant + adapts to volatility.

### Step 3.3 — Time-stop
New exit `TIME_STOP`: trade hasn't reached 0.3R favorable in 45 min → exit at market. Frees ~15% of capital.

### Step 3.4 — Trailing stop post-1R
After 1R favorable: stop to breakeven. After 1.5R: trail at 0.5R. Apply to highest-MFE setups (B_HUGE_*, A_MOD_BREAK_*).

### Step 3.5 — Volatility-targeted sizing
`qty = target_risk_rs / (entry_price × SL%)` so per-trade Rs-risk is constant. Multiplier still applies on top.

### Phase 3 gate
Sharpe up; max-DD down >= 20%. Skip parts that didn't help.

---

## Phase 4 — Live infrastructure (1.5 weeks, parallel Phase 3)

### Step 4.1 — Paper-trading shadow mode
Run v17D against live data 20 trading days, no real orders. Log signals + simulated fills. **Gate:** paper PF >= 70% of backtest PF.

### Step 4.2 — Pre-market sanity check (cron 08:30 IST)
Daily checklist that aborts trading if any fail:
- Universe file present, refreshed within 7 days
- Required parquets exist for today's universe
- Broker connection healthy
- No earnings on watchlist (mark restricted, don't halt all)
- Yesterday's reconciliation complete

### Step 4.3 — Broker integration
Idempotent order placement (client-side token). Reconcile every 30s. Order timeout: cancel + retry once after 5s; log + skip after 2 failures.

### Step 4.4 — Live monitoring dashboard
HTML or terminal UI:
- Open positions, P&L per position, time in trade
- Today's stats: trades, win/loss, PnL
- Per-governor drop counts today
- Per-setup rolling PF (last 30) with bootstrap CI
- Feature distribution drift flags

### Step 4.5 — Feature drift detection (KS test)
Daily: KS-test each feature vs in-sample distribution. Alert if `p < 0.01` for 3 consecutive days. Early warning before PnL degrades.

### Step 4.6 — Daily reconciliation
End-of-day script:
- Backtest predicted vs actual fill prices
- Slippage actuals fed back to cost model (auto-tune)
- PnL match (broker vs internal). Mismatch > Rs.100 → alert.

### Step 4.7 — Runbook
Plain-text doc: morning startup, kill, stuck position recovery, disconnect handling, manual override, on-call escalation.

### Step 4.8 — Setup graveyard log (ongoing)
`eqidv2/SETUP_GRAVEYARD.md` (template at end of this doc). Log every dropped setup with date, reason, supporting numbers. Periodic re-evaluation when regimes shift.

### Phase 4 gate
20 paper-trading days complete. Reconciliation clean. Drift alerts working. Kill-switches tested at least once.

---

## Phase 4b — Brokerage & tax analytics (NEW, parallel Phase 4, 3 days)

**Goal:** every trade and every setup has a clear cost-and-tax footprint;
strategy choices optimize for **net-after-cost-after-tax** PnL, not gross.

Indian intraday equity is taxed as **speculative business income** at slab
rates. Speculative losses can ONLY offset future speculative gains, carried
forward 4 years. Tax audit is mandatory at turnover > Rs 10 cr.

### Step 4b.1 — Daily cost breakdown
Per session, decompose every charge: brokerage (Zerodha cap Rs 20/order)
+ STT + GST + exchange + SEBI + stamp duty + slippage. Output: cost
as % of gross PnL per day. Flags days where cost > 40% of gross.

Module: `eqidv2.v17D_tax_analytics daily`.

### Step 4b.2 — Per-strategy tax efficiency
Per (side, setup): gross_per_trade, net_per_trade, cost_per_trade,
cost_pct_of_gross, tax_efficiency_score (= net/gross).

**Key insight surfaced:** Zerodha caps brokerage at Rs 20/order. Setups
producing higher absolute PnL per trade have lower effective cost%.
A setup with PF=2.0 doing Rs 80/trade is cost-inefficient (40% cost),
while a setup with PF=2.0 doing Rs 400/trade is cost-efficient (8% cost).

Module: `eqidv2.v17D_tax_analytics efficiency`.

### Step 4b.3 — Tax-optimal setup recommendations
Auto-classify each setup as KEEP / RESIZE_OR_DROP / DROP based on:

- HIGH_COST_BURDEN: cost > 40% of gross
- LOW_NET_PER_TRADE: net per trade < Rs 50
- ORDER_TOO_SMALL_TO_AMORTIZE_BROKERAGE: gross per trade < Rs 100
- TAX_EFFICIENCY_BELOW_50%: net / gross < 0.50

Module: `eqidv2.v17D_tax_analytics recommend`.

### Step 4b.4 — Year-end speculative-income estimator
Quarterly + year-end summary:
- Gross / net speculative income (after carry-forward setoff)
- Tax due at slab rate (configurable, default 30%)
- Turnover (sum of |abs PnL|) — flags audit requirement at Rs 10 cr
- Loss carry-forward eligibility (4-year window)

Module: `eqidv2.v17D_tax_analytics yearend --fy-start 2026-04-01 --slab-pct 30`.

### Step 4b.5 — Kite contract-note reconciliation
Pull Zerodha's daily contract note CSV via Kite API, reconcile broker-actual
charges against internal cost model. Auto-tunes the cost model parameters
when actuals drift from estimates (e.g. exchange fee structure changes).

Module: `eqidv2.v17D_tax_analytics kite-reconcile`.

### Phase 4b gate
- Per-setup cost burden table printed; setups flagged DROP/RESIZE actually
  re-evaluated and decisions logged in `SETUP_GRAVEYARD.md`.
- Year-end FY estimate matches CA's reconciliation within Rs 1,000.
- Kite contract-note actuals match cost model within +/-5% on each component.

### What changes in production after Phase 4b

1. **Setup selection criterion shifts** from "highest PF" to "highest
   net-per-trade-Rs after costs and tax". A PF=1.8 setup doing Rs 250/trade
   often beats a PF=2.4 setup doing Rs 60/trade.
2. **Vol-targeted sizing recalibration** (Step 3.5) uses target_risk_rs
   that ensures gross-per-trade > 2.5x cost — i.e. minimum order size
   floor to amortize brokerage cap.
3. **Setup library Tier-B gate** adds: net_per_trade_rs >= 50 in addition
   to PF and OOS PF requirements.
4. **Loss-harvesting logic** (NEW): if year-to-date is in profit and a
   setup has been losing, drop it before FY end so the loss can be carried
   forward (4-year window) for offset.

---

## Phase 5 — Pilot live (2 weeks reduced size)

### Step 5.1 — Capital ramp
- Day 1–3: 0.10x. Monitor every trade.
- Day 4–10: 0.30x if no incidents.
- Day 11–14: 0.50x if PF + slippage match paper.
- Day 15+: 1.0x only after exit gate passes.

### Step 5.2 — Daily journal
5 min/evening logging anomalies, surprises, broker behavior, regime observations.

### Phase 5 exit gate
14 live days. Live PF >= 60% of paper PF. Zero production incidents. Then full size.

---

## Phase 6 — ML meta-filter (deferred)

**Don't start until Phase 5 done with >= 100 live trades.** Real live trades are the only unbiased labels.

1. Shadow mode 30 days (log `p_win`, no gating)
2. Compare gated-vs-ungated PF
3. Wire as Phase 2c stage iff Phase 5 PF < 1.7 and you want it higher

---

## Setup expansion philosophy: wide net, then filter

Spec a large library (15–25 candidates), backtest all, ship only survivors. Two reasons:

1. **Diversification across logic types.** Current 8 setups are 80% breakout-flavored. When breakouts fail (chop regime), the entire portfolio fails together. Adding mean-reversion, climax, pattern, time-of-day setups creates regime diversification.
2. **Objective elimination beats subjective curation.** "Spec 20, ship the top 10" produces better strategies than "spec 8 and hope."

### Funnel

```
Library spec (20-25 candidates)
  │
  ▼  Tier-A: 60 days, lab cost. Drop if PF<1.30 or n<30
  │
  ▼  Tier-B: 220 days, realistic cost. Drop if PF<1.50 or OOS<1.30
  │
  ▼  Shadow mode: engine-scan, sized 0. 30 days. Drop if live PF<1.40
  │
  ▼  Pilot live: 0.25x, cap=2-3/day. 60 days. Drop if PF<1.40 or DD>2x backtest
  │
  ▼  Production: per Cand-E sizing tier. Continuous monitoring.
  │
  └──> Setup graveyard (drop log)
```

**Dropping a setup is success, not failure.** Graveyard list should be longer than production list.

### What you do NOT do

- Don't ship all 20 to live. Production = top 10–14 by OOS PF, with diversification rule (max 4 per logic family).
- Don't keep low-PF setups for "completeness." PF < 1.40 → graveyard.
- Don't run more setups than you can monitor. Each setup needs rolling-PF tracking.
- Don't skip Tier-A → Tier-B → Shadow → Pilot funnel.

---

## Setup library — 20 candidate detailed specs

Each spec: trigger conditions, entry rule, SL/TGT rule, indicators required, expected PF tier, suggested cap. Detector lives at `eqidv2/v17D_setup_library/<id>.py`.

### Family 1: Trend continuation

#### TC-1 — Pullback to EMA20 bounce
- **Side:** LONG (mirror for SHORT below EMA20)
- **Trigger:**
  - Trend filter: 5-bar EMA20 slope > 0 AND price > EMA50
  - Price low touches or crosses below EMA20 within last 2 bars
  - Current bar closes back above EMA20 with bullish body (`close > open`)
  - ADX >= 22, di+ > di-
- **Entry:** close of trigger bar
- **SL:** below trigger bar low OR -0.75%, whichever wider
- **TGT:** prior swing high OR +1.0%, whichever closer (R:R >= 1.0)
- **Time-stop:** 45 min (Phase 3.3)
- **Indicators:** EMA20, EMA50, ADX, DI+/DI-
- **Expected PF tier:** 1.6–2.0 (borderline-good)
- **Cap:** 3/day per side
- **Notes:** classic pullback. Works on trend days. Will fail on chop — ADX gate critical.

#### TC-2 — Pullback to EMA50 bounce
- **Side:** LONG and SHORT
- **Trigger:** same as TC-1 but pullback to EMA50 (deeper) with ADX >= 25
- **Entry:** close of bullish reversal bar
- **SL:** below trigger bar low / above trigger bar high; -0.80% floor
- **TGT:** +1.2% / R:R >= 1.2
- **Indicators:** EMA50, ADX, candle pattern
- **Expected PF tier:** 1.7–2.1 (deeper pullbacks have better R:R)
- **Cap:** 2/day per side
- **Notes:** rarer than TC-1 (deeper pullback) but better R:R.

#### TC-3 — Higher-high higher-low momentum break
- **Side:** LONG (mirror SHORT)
- **Trigger:**
  - Last 3 swings show HH + HL pattern (intraday swing detection on 5-min)
  - Latest HH broken with volume >= 1.5x SMA20
  - Time within 09:30–13:00 IST
- **Entry:** close of breakout bar
- **SL:** latest HL or -0.80%
- **TGT:** measured-move (HH − HL distance projected) OR +1.0% min
- **Indicators:** Custom swing high/low, volume_sma20
- **Expected PF tier:** 1.8–2.2 (clean structure trades)
- **Cap:** 2/day per side
- **Notes:** purer trend setup than EMA-based. Requires reliable swing-detection module.

#### TC-4 — Trend-day first-pullback to VWAP
- **Side:** LONG and SHORT
- **Trigger:**
  - ADX >= 30 (strong trend day signal)
  - Price has moved >= 1.5% from open in trend direction
  - First pullback touches VWAP +/- 0.3 ATR
  - Reversal bar closes back in trend direction
- **Entry:** close of reversal bar
- **SL:** beyond VWAP by 0.5 ATR; -0.85% floor
- **TGT:** +1.5% (trend days run)
- **Indicators:** ADX, VWAP, ATR
- **Expected PF tier:** 2.0–2.5 (high-quality, low-frequency)
- **Cap:** 2/day per side
- **Notes:** trend-day-only. Daily ADX gate prevents firing on chop days.

### Family 2: Mean reversion

#### MR-1 — Bollinger touch + RSI extreme
- **Side:** LONG (BB lower + RSI <= 20) and SHORT (BB upper + RSI >= 80)
- **Trigger:**
  - Price touches Lower_Band (or Upper_Band)
  - RSI <= 20 (or >= 80)
  - Reversal candle (bullish for long, bearish for short)
  - Daily ADX <= 25 (chop regime preferred)
- **Entry:** close of reversal bar
- **SL:** beyond extreme of trigger bar; -0.75% floor
- **TGT:** mid-Band (BB middle line) OR +0.85%
- **Indicators:** Upper_Band, Lower_Band, RSI, ADX
- **Expected PF tier:** 1.5–1.9 (mean-rev typical)
- **Cap:** 3/day per side
- **Notes:** counter-trend. Critical: ADX <= 25 gate. Will lose money on trend days if not gated.

#### MR-2 — VWAP fade
- **Side:** LONG (price >= 2 ATR below VWAP) and SHORT (>= 2 ATR above)
- **Trigger:**
  - `|close - VWAP| / ATR >= 2.0`
  - Reversal bar (engulf or pin) toward VWAP
  - Daily ADX <= 28
- **Entry:** close of reversal bar
- **SL:** beyond extreme by 0.5 ATR; -0.80% floor
- **TGT:** VWAP itself
- **Indicators:** VWAP, ATR
- **Expected PF tier:** 1.6–2.0
- **Cap:** 3/day per side
- **Notes:** institutions use VWAP as reference. Reverts often. Cleaner than MR-1.

#### MR-3 — Overextended EMA reversion
- **Side:** LONG and SHORT
- **Trigger:**
  - `|close - EMA20| / ATR >= 3.0` (extreme stretch)
  - First counter-direction bar with body >= 50% of range
- **Entry:** close of counter bar
- **SL:** beyond trigger extreme; -0.80% floor
- **TGT:** EMA20 itself
- **Indicators:** EMA20, ATR
- **Expected PF tier:** 1.5–1.8
- **Cap:** 2/day per side
- **Notes:** rare (3 ATR stretch) but high win-rate when fires.

#### MR-4 — Three-bar drive + reversal
- **Side:** LONG (3 red bars) and SHORT (3 green bars)
- **Trigger:**
  - 3 consecutive same-direction bars
  - Volume on bar 3 >= 1.2x SMA20 (climax)
  - Bar 4 closes opposite direction with body
- **Entry:** close of bar 4
- **SL:** beyond bar 3 extreme; -0.75% floor
- **TGT:** start of bar 1 OR +1.0%
- **Indicators:** candle pattern, volume_sma20
- **Expected PF tier:** 1.5–1.9
- **Cap:** 3/day per side
- **Notes:** climactic exhaustion. Volume gate critical.

### Family 3: Breakout

#### BO-1 — Donchian 20-bar high/low breakout
- **Side:** LONG (new 20-bar high) and SHORT (new 20-bar low)
- **Trigger:**
  - Bar high (low) exceeds 20-bar Donchian channel
  - Volume on breakout >= 1.5x SMA20
  - Time 09:30–13:00 IST (avoid late-day fakeouts)
- **Entry:** close of breakout bar
- **SL:** opposite Donchian boundary OR -0.80%
- **TGT:** +1.0% / R:R >= 1.2
- **Indicators:** Donchian high/low, volume_sma20
- **Expected PF tier:** 1.7–2.0
- **Cap:** 3/day per side
- **Notes:** classic. Cleaner than `Recent_High`/`Recent_Low` ad-hoc.

#### BO-2 — Squeeze release (BB inside Keltner)
- **Side:** LONG and SHORT
- **Trigger:**
  - BB fully inside Keltner channel for >= 10 consecutive bars (squeeze condition)
  - Bar closes outside Keltner in either direction
  - Volume >= 1.3x SMA20
- **Entry:** close of break bar
- **SL:** middle of squeeze range; -0.75% floor
- **TGT:** +1.2% (post-squeeze moves run)
- **Indicators:** Bollinger Bands, Keltner Channels, volume_sma20
- **Expected PF tier:** 1.9–2.3 (squeeze releases are high-quality)
- **Cap:** 2/day per side
- **Notes:** rare but high-quality. Requires Keltner addition to indicator parquets.

#### BO-3 — Higher-TF level break
- **Side:** LONG and SHORT
- **Trigger:**
  - Daily/weekly S/R level pre-computed (yesterday's high/low, 5-day high/low, prior week high/low)
  - 5-min bar breaks level with volume >= 1.5x SMA20
  - First touch of level today
- **Entry:** close of break bar
- **SL:** level + 0.3 ATR (false-break buffer); -0.80% floor
- **TGT:** next HTF level OR +1.0%
- **Indicators:** HTF pivots (precomputed), volume_sma20
- **Expected PF tier:** 1.8–2.2
- **Cap:** 2/day per side
- **Notes:** institutional reference levels. Higher conviction than ad-hoc S/R.

#### BO-4 — OR-15min breakout (existing C_OR_BREAKOUT, restored)
- **Side:** LONG (above OR-high) and SHORT (below OR-low)
- **Trigger:**
  - 15-min OR established (09:15–09:30)
  - Bar after 09:30 breaks OR-high (or low) with body >= 50% of range
  - Volume >= 1.5x SMA20
  - ADX >= 22
- **Entry:** close of break bar
- **SL:** OR midpoint; -0.80% floor
- **TGT:** OR range projected (high + range OR low - range)
- **Indicators:** OR-15min, volume, ADX
- **Expected PF tier:** 1.6–2.0 (was 1.8 in earlier C_OR; ADX gate should lift)
- **Cap:** 3/day per side
- **Notes:** restoration of dropped C_OR with stricter filters.

### Family 4: Pattern-based

#### PT-1 — Gap-and-go
- **Side:** LONG (gap up >= 1.5%) and SHORT (gap down >= 1.5%)
- **Trigger:**
  - Gap from prior close >= 1.5%
  - First 5-min bar closes in gap direction (no fill)
  - Volume on first bar >= 2x SMA20
- **Entry:** close of first 5-min bar
- **SL:** prior close (gap fill point); -0.85% floor
- **TGT:** +1.5% (gap-and-go can run)
- **Indicators:** gap %, volume
- **Expected PF tier:** 1.7–2.1
- **Cap:** 1/ticker/day, 4/day total
- **Notes:** opening-only setup. Time gate: 09:15–09:30 IST.

#### PT-2 — Gap-fill fade
- **Side:** SHORT (gap up + fill) and LONG (gap down + fill)
- **Trigger:**
  - Gap from prior close >= 2.0%
  - Within 60 min, price reaches 50% of gap fill
  - Reversal bar at fill point (engulf or pin)
- **Entry:** close of reversal bar
- **SL:** beyond gap extreme; -0.85% floor
- **TGT:** full gap fill (prior close)
- **Indicators:** gap %, time, candle pattern
- **Expected PF tier:** 1.6–2.0
- **Cap:** 1/ticker/day, 3/day total
- **Notes:** counter-trend variant. Needs ADX <= 25 (chop) gate.

#### PT-3 — Inside-bar breakout
- **Side:** LONG (above inside-bar high) and SHORT (below inside-bar low)
- **Trigger:**
  - Bar fully contained in prior bar range (high <= prior high, low >= prior low)
  - Next bar breaks inside-bar extreme with body >= 50%
  - Volume on break >= 1.3x SMA20
- **Entry:** close of break bar
- **SL:** opposite extreme of inside bar; -0.75% floor
- **TGT:** prior bar range projected; +0.90% min
- **Indicators:** bar geometry, volume
- **Expected PF tier:** 1.5–1.8
- **Cap:** 4/day per side
- **Notes:** very high frequency, lower PF. Volume gate critical to filter false breaks.

### Family 5: Volume / order flow

#### VO-1 — Climax volume reversal
- **Side:** LONG (after sell climax) and SHORT (after buy climax)
- **Trigger:**
  - Volume bar >= 3x SMA20
  - Bar in opposite direction to recent trend (5-bar slope)
  - Reversal candle structure (long wick on extreme side)
- **Entry:** close of climax bar
- **SL:** climax bar extreme; -0.85% floor
- **TGT:** mid-range of recent trend OR +1.0%
- **Indicators:** volume_sma20, EMA slope, candle wick
- **Expected PF tier:** 1.6–2.0
- **Cap:** 2/day per side
- **Notes:** exhaustion play. Counter-trend, needs ADX moderate (20–28).

#### VO-2 — Low-volume drift + acceleration
- **Side:** LONG and SHORT
- **Trigger:**
  - 5-bar volume avg <= 0.7x SMA20 (drift)
  - Latest bar volume >= 1.5x SMA20 with directional body (>= 60% range)
  - Trend direction matches drift direction
- **Entry:** close of acceleration bar
- **SL:** drift range extreme; -0.75% floor
- **TGT:** +1.0%
- **Indicators:** volume regime, body %
- **Expected PF tier:** 1.7–2.1
- **Cap:** 3/day per side
- **Notes:** "quiet then loud" — institutional accumulation/distribution proxy.

#### VO-3 — OBV divergence
- **Side:** LONG (price LL + OBV HL) and SHORT (price HH + OBV LL)
- **Trigger:**
  - Price makes new 10-bar low/high
  - OBV does NOT confirm (divergence)
  - Reversal candle at extreme
- **Entry:** close of reversal bar
- **SL:** extreme + 0.3 ATR; -0.80% floor
- **TGT:** +1.0%
- **Indicators:** OBV, swing detection
- **Expected PF tier:** 1.5–1.8
- **Cap:** 2/day per side
- **Notes:** classic divergence. Lower frequency. ADX <= 28 gate (works in ranges, not strong trends).

### Family 6: Time-of-day

#### TD-1 — Opening drive
- **Side:** LONG (first bar > +1.5% ATR) and SHORT (< -1.5% ATR)
- **Trigger:**
  - First 5-min bar (09:15–09:20)
  - Bar range >= 1.5x daily ATR
  - Bar closes in same direction as range
  - Continuation: 09:20–09:25 bar in same direction with body >= 50%
- **Entry:** close of 09:20 bar
- **SL:** open (09:15); -1.0% floor (wider given volatility)
- **TGT:** +1.5%
- **Indicators:** ATR, time
- **Expected PF tier:** 1.7–2.1
- **Cap:** 1/ticker/day, 3/day total
- **Notes:** opening-only. High volatility, needs wider stops.

#### TD-2 — Late-day reversal
- **Side:** LONG (after intraday downtrend) and SHORT (after intraday uptrend)
- **Trigger:**
  - Time after 14:00 IST
  - Intraday trend established (price moved >= 1% from open in one direction)
  - Reversal bar with body >= 50% in opposite direction
  - Volume >= 1.3x SMA20
- **Entry:** close of reversal bar
- **SL:** intraday extreme; -0.85% floor
- **TGT:** 50% retracement of intraday trend OR +1.0%
- **Indicators:** time, intraday trend tracking, volume
- **Expected PF tier:** 1.5–1.9
- **Cap:** 2/day per side
- **Notes:** late-session profit-taking pattern. Time-gated, won't compete with morning setups for daily cap.

### Library summary

| Family | Count | Expected combined trades/day | Expected family PF |
|---|---|---|---|
| Trend continuation | 4 | 1.5–2.0 | 1.8 |
| Mean reversion | 4 | 1.0–1.5 | 1.7 |
| Breakout | 4 | 1.5–2.0 | 1.9 |
| Pattern-based | 3 | 0.8–1.2 | 1.7 |
| Volume / order flow | 3 | 0.8–1.2 | 1.7 |
| Time-of-day | 2 | 0.5–0.8 | 1.7 |
| **Total** | **20** | **6–9/day** | **~1.8 weighted** |

After Tier-A + Tier-B funnel, expect 10–14 survivors. Combined with 6 existing setups → 16–20 active in production.

---

## Setup graveyard template

Maintain `eqidv2/SETUP_GRAVEYARD.md` from Phase 4.8 onward. Every dropped setup logged. Re-evaluate periodically.

```markdown
# Setup Graveyard

| Setup ID | Date dropped | Phase | Reason | n at drop | PF at drop | Win % | OOS PF | Notes / re-eval condition |
|---|---|---|---|---|---|---|---|---|
| SHORT C_OR_BREAKDOWN | 2026-05-04 | E4 retune | Raw PF 1.00, no robust filter found | ~250 | 1.00 | ~50% | n/a | Re-eval if ADX-strict variant clears Tier-B in next library cycle |
| SHORT D_EMA20_REJECTION | 2026-05-04 | E4 retune | PF 0.83, losing | ~180 | 0.83 | ~46% | n/a | Re-eval only if a specific filter raises OOS PF >= 1.30 |
| (template row) | YYYY-MM-DD | Phase X.Y | One-line reason | int | float | float% | float | Re-eval condition or "do not revisit" |
```

### Drop-reason taxonomy

Use one of these standard labels for the "Reason" column:

- `OVERFIT` — IS PF good, OOS PF collapses
- `COSTS` — PF degrades below 1.40 under realistic cost model
- `NO_EDGE` — Raw PF < 1.30, no filter raises it
- `CORRELATION` — Jaccard >= 0.5 with existing setup
- `LIVE_DEGRADE` — Live PF < 1.40 over 60-day pilot
- `DRIFT` — KS test fails on >= 2 features for 3 consecutive days
- `LOW_FREQ` — n < 30 in 60-day window even if PF ok (statistically unreliable)
- `OPERATIONAL` — Detection too brittle, false-positive rate high

### Re-evaluation cadence

- **Quarterly:** review graveyard, identify candidates whose drop reason was regime-specific
- **Trigger-based:** if rolling 60-day market regime metric (e.g. avg daily ADX, NIFTY realized vol) shifts > 20% from drop date, re-test mean-reversion-flavored drops in trend regimes and vice versa
- **Library cycle:** every 6 months, run a fresh wide-net backtest including 1–2 graveyard re-tests

---

## Summary timeline

| Phase | Duration | Type | Skip if |
|---|---|---|---|
| 0 — Diligence | 3–4 days | Investigation | Never skip |
| 1 — Foundations | 1 week | Capital safety | Never skip |
| 2 — Signal + library | 2.5 weeks | Feature work | Phase 0 says PF marginal — skip to Phase 3 |
| 3 — Risk/Exits | 1 week | Refinement | Phase 2 lifted PF, DD acceptable |
| 4 — Live infra | 1.5 weeks (parallel 3) | Operational | Never skip |
| 4b — Tax + brokerage analytics | 3 days (parallel 4) | Cost optimization | Never skip if trading taxable |
| 5 — Pilot | 2 weeks | Validation | Never skip |
| 6 — ML | 4+ weeks | Future work | Strategy already at target |

**Total realistic timeline to live capital at full size: 9–11 weeks.**

---

## What to start tomorrow

In order:

1. **Step 0.1 (slippage stress test) — half day.** Determines whether you're building on rock or sand.
2. **Step 0.4 (MAE/MFE analysis) — 1 day.** Rewrites SL/TGT picks with real data not Pareto search.
3. **Step 1.5 + 1.6 (kill-switch + logging) — 1 day.** Protects capital today, even running v17C.

If you only have a week, do those three. Everything else can wait.
