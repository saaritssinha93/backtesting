# Setup Cards + Live-vs-Conf Cross-Check

**Generated:** 2026-06-29
**Source of truth:** [final_setup_conf.py](final_setup_conf.py) — `FINAL_SETUP_CONF` (the gate of record)
**Cross-check sources:** [../eqidv2_final_conf_live_bootstrap.py](../eqidv2_final_conf_live_bootstrap.py), [../eqidv2_v11_live_overlay.py](../eqidv2_v11_live_overlay.py), [../conf_adherence_check.py](../conf_adherence_check.py)

For every setup: **logic** (the idea), **detection** (raw 5-min definition + non-indicator threshold values), **indicators**, **filters** (`mask_terms`), **gates** (`pre_momentum_terms`), **guards** (`entry_guards`), and **exit**.

---

## 0. What applies to the whole book

**Common gate** — every *natively-detected* setup must pass `_passes_common`:
- `close ≥ Rs 80` (MIN_PRICE)
- 5-min traded value ≥ `MIN_5M_TRADED_VALUE_RS` (liquidity floor)
- day-value floor after 10:00 IST (`MIN_DAY_VALUE_BY_1000_RS`)
- `range ≤ MAX_CANDLE_RANGE_ATR × ATR` (no blow-off bar)

**Entry model:** next 1-min open after the 5-min signal + 5 bps paper slippage.
**Exit model:** resolve SL / Target / EOD on 1-min OHLC to 15:20 IST.
**Default window / dedupe:** live 09:30–14:30 entry window + one-ticker-per-day dedupe (unless a setup sets `entry_guards.min_slot`).
**Cost basis:** net of statutory NSE intraday costs. **Train:** 2025-11-01..2026-04-30. **Test:** 2026-05-01..2026-06-10.

### Indicator / feature glossary
| Feature | Definition |
|---|---|
| VWAP | intraday session VWAP, resets daily (corrected 2026-06-13: typical-price × volume) |
| ATR | per-day ATR; `atr_pct` = ATR/close |
| vwap_dist_atr | (close − VWAP) / ATR |
| EMA20 / EMA50 | 5-min exponential moving averages; `ema20_slope_3bar` = EMA20 − EMA20[−3] |
| RSI / ADX | 5-min RSI, 5-min ADX |
| vol_ratio | volume / Volume_SMA20 (SMA20 = volume.shift(1).rolling(20, min 8).mean) |
| close_loc | (close − low) / (high − low) |
| body_pct | abs(close − open) / (high − low) |
| pressure_ratio | up-volume / down-volume proxy (buy pressure) |
| rs_pct | stock_intraday_ret% − NIFTY_intraday_ret% (RS_LOOKBACK_BARS = 6) |
| regime | BULL / BEAR / TREND / NEUTRAL from NIFTY ret vs VWAP |
| quality_score | scanner composite quality metric |
| signal_minute | IST minute-of-day of the signal bar |
| **Pre-momentum (1-min @ entry)** | `pre2/pre5/pre10_mom_r` (risk-norm N-bar momentum), `pre3_range_r`, `pre3_close_pos`, `pre1_adx`, `sig5_adx_calc`, `sig5_rsi_dir`, `pre_entry_momentum_score` (composite) |

> **Live status:** 4 setups were **demoted to `enabled=False` on 2026-06-22** (lost money in live conf paper). The **currently-tradeable book is 12 setups**. Demoted + rejected sets are in §3–§4.

---

## 1. ACTIVE BOOK — SHORTS

### A_PULLBACK_C2_THEN_BREAK_C2_LOW (SHORT) — *active*
- **Logic:** after a 2-bar up-pullback in a non-bull regime, price loses VWAP and breaks the prior bar low on volume.
- **Detection:** `close<open`, `close_loc≤0.40`, `close<VWAP`, `close<prev_bar_low`, `prev_close>prev2_close`, `vol_ratio≥1.4`, `regime≠BULL`. (reason `bear_pullback_c2_break_low`)
- **Indicators:** VWAP, ATR, vol_ratio, close_loc, regime.
- **Filters (mask):** `quality_score>=123.7606` (user re-promoted high-quality tail on 2026-06-29).
- **Gates (pre-mom):** `sig5_adx_calc>=21.4683`. **Guards:** none.
- **Exit:** SL 1.20 / Tgt 1.50. **Status:** USER_REPROMOTED_WATCHLIST (last-1-month available replay 30 trades / PF 3.491; monitor live-paper before sizing).

### E_VWAP_LOSE_EARLY_SHORT (SHORT) - *REJECTED / parked after 2026-06-29 six-week rerun*
- **Logic:** early-session VWAP failure — was at/above VWAP, loses it, breaks prior low on a weak close while lagging the market.
- **Detection (EARLY engine, min quality 6.0):** `close<open`, `prev_close≥prev_VWAP`, `close<VWAP`, `close<prev_low`, `close_loc≤0.35`, `rs_pct≤-0.10`, `vwap_dist_atr≥-1.80`. Early extra gate: `rs_pct≥-1.20`, `close_loc≥0.08`, `atr_pct≤0.008`.
- **Indicators:** VWAP, ATR, vol_ratio, close_loc, vwap_dist_atr, rs_pct.
- **Filters (mask = THE EDGE):** `vol_ratio≥1.8` AND `vol_ratio≤3.2` (volume-conviction band).
- **Gates (pre-mom):** none (dropped — diluted the band). **Guards:** signal time ≥ **09:45 IST**.
- **Exit:** SL 0.70 / Tgt 1.00. **Status:** REJECTED / keep parked. The old STRONG PROBATION label is stale: the 2026-06-29 six-week rerun used TRAIN 2026-04-27..2026-06-05 and TEST 2026-06-08..2026-06-12; baseline was TRAIN 54/PF 0.362 and TEST 10/PF 0.643. Hand variants failed TRAIN, and the official TRAIN-optimized candidate collapsed on TEST 11/PF 0.129.

### D_EMA20_REJECTION (SHORT) — *active — the gate IS the edge*
- **Logic:** in a downtrend stack, price retests a falling EMA20 and rejects (resumes down).
- **Detection:** `|close−EMA20|≤0.35×ATR`, `close<open`, `close_loc≤0.40`, `close<EMA20`, `EMA20≤EMA50`, `rs_pct<0.10`, `vol_ratio≥1.3`, `regime≠BULL`. (reason `ema20_trend_rejection`)
- **Indicators:** EMA20, EMA50, ATR, vol_ratio, rs_pct, regime; ADX (in gate).
- **Filters (mask):** none (production `body≥0.89 & ranker≥0.39` DROPPED — over-tightens to n=6).
- **Gates (pre-mom, ALL required, missing→block):** `pre10_mom_r≤0.156614`, `pre5_mom_r≥0.12493`, `sig5_adx_calc≥20.0`.
- **Guards:** none. **Exit:** SL 0.75 / Tgt 1.30. **Status:** PROBATION (without gate train PF 0.71; Mar was a losing month).

### B_HUGE_RED_FAILED_BOUNCE (SHORT) — *active — mined short*
- **Logic:** after a huge red bar, price bounces weakly and fails → resume down.
- **Detection:** production clean-pool scanner raw_candidates (corrected VWAP); gate is the edge.
- **Indicators:** RSI-direction, ADX, pre-momentum.
- **Filters (mask):** none.
- **Gates (pre-mom, ALL, missing→block):** `pre3_close_pos≤0.581797`, `sig5_rsi_dir≤64.104659`, `pre5_mom_r≤0.284145`.
- **Guards:** none. **Exit:** SL 0.90 / Tgt 1.25. **Status:** STRONG PROBATION (5/5 exits, even halves, train 2.90 / test 3.49).

### C_OR_BREAKDOWN (SHORT) — *active — mined short*
- **Logic:** opening-range-low break, continuation down in a strong downtrend after a low-ADX pause.
- **Detection:** production clean-pool scanner raw_candidates (corrected VWAP).
- **Indicators:** ADX (sig5 + pre1).
- **Filters (mask):** none.
- **Gates (pre-mom, simple 2-term, missing→block):** `sig5_adx_calc≥39.670518`, `pre1_adx≤21.368044`.
- **Guards:** none. **Exit:** SL 0.90 / Tgt 2.00. **Status:** STRONG PROBATION (5/5 exits; halves imbalanced h1 1.71 / h2 4.21).

### A_MOD_BREAK_C1_LOW (SHORT) — *active — mined short*
- **Logic:** break of the prior C1 (first-candle) low — momentum-down continuation out of a TIGHT pre-break range.
- **Detection:** production clean-pool scanner raw_candidates (corrected VWAP).
- **Indicators:** vol_ratio, pre-momentum, range features.
- **Filters (mask):** `vol_ratio≥1.955814`.
- **Gates (pre-mom, ALL, missing→block):** `pre5_mom_r≥0.425861`, `pre3_range_r≤0.202087`.
- **Guards:** none. **Exit:** SL 1.10 / Tgt 1.00 (alt 0.90/1.00 cleaner day-spread). **Status:** STRONG PROBATION (monotone sensitivity, 88% months).

### G_LOWER_LOW_BREAK (SHORT) — *active — mined short, SELECTIVE*
- **Logic:** lower-low break on a volume climax (~4×) = capitulation / exhaustion short.
- **Detection:** production clean-pool scanner raw_candidates (corrected VWAP).
- **Indicators:** vol_ratio, quality_score, RSI-direction.
- **Filters (mask):** `vol_ratio≥4.129044` AND `quality_score≥76.444124`.
- **Gates (pre-mom, missing→block):** `sig5_rsi_dir≥68.747209`.
- **Guards:** none. **Exit:** SL 0.80 / Tgt 0.80 (user override 2026-06-29; prior tested primary 1.10/1.00). **Status:** WEAK / SELECTIVE (fires rarely, test n=9; 100% months but `sig5_rsi_dir` cliff).

---

## 2. ACTIVE BOOK — LONGS

### B_AVWAP_RECLAIM_REVERSAL (LONG) — *active*
- **Logic:** a stock below session VWAP reclaims it on a strong up-bar in a non-bear regime (reversal from weakness).
- **Detection (min quality 6.0):** `close>open`, `close_loc≥0.60`, `prev_close<prev_VWAP`, `close>VWAP`, `rs_pct>-0.10`, `vol_ratio≥1.4`, `regime≠BEAR`. (reason `reclaim_session_vwap_from_below`)
- **Indicators:** VWAP, ATR, vol_ratio, close_loc, rs_pct, vwap_dist_atr, regime.
- **Filters (mask = un-inverted edge):** `vwap_dist_atr≤1.0` (near-VWAP). **Replaces the inverted production `≥0.60`.**
- **Gates (pre-mom):** none (6-decimal momentum gate dropped). **Guards:** none.
- **Exit:** SL 0.70 / Tgt 1.50. **Status:** PROBATION (test n=5, PF 1.20). ⚠️ See cross-check §5 — **live overlay still uses the inverted `≥0.60`.**

### B_HUGE_C1_CLOSE_RECLAIM_BREAK (LONG) — *active*
- **Logic:** momentum continuation — break of a prior HUGE GREEN bar's high in a non-bear regime.
- **Detection (min quality 7.0):** `prev_range≥1.80×prev_ATR`, `prev_close>prev_open`, `close>open`, `close_loc≥0.60`, `close>prev_bar_high`, `close>VWAP`, `vol_ratio≥1.3`, `regime≠BEAR`. (reason `huge_green_reclaim_then_break`)
- **Indicators:** VWAP, ATR, vol_ratio, close_loc, regime.
- **Filters (mask = CATEGORICAL):** `regime≠BULL` (replaces no-op `rs_pct≤10.7`). Effective regime universe = {NEUTRAL, TREND}. *Apply as string inequality, not numeric.*
- **Gates (pre-mom):** none. **Guards:** none.
- **Exit:** SL 1.00 / Tgt 1.50 (user override 2026-06-29; prior SL 0.70). **Status:** PROBATION (WF-uncertifiable at ~34 trades; test 5/5 winners).

### G_HIGHER_HIGH_BREAK (LONG) — *active — the gate IS the edge*
- **Logic:** 20-bar higher-high breakout; only pays with a genuine ADX-confirmed momentum thrust (ungated = late chase, net loser).
- **Detection:** `close>open`, `close_loc≥0.60`, `close>VWAP`, `close>prev_20bar_high`, `rs_pct>0.00`, `vol_ratio≥1.4`, `regime≠BEAR`. (reason `twenty_bar_higher_high_break`)
- **Indicators:** VWAP, ATR, vol_ratio, close_loc, vwap_dist_atr, rs_pct, prev_20bar_high, regime, ADX.
- **Filters (mask):** none.
- **Gates (pre-mom, ALL, missing→block):** `pre2_mom_r≥0.55`, `sig5_adx_calc≥26.0`. *(Dropped production gate `pre3_close_pos≤0.985 & sig5_rsi_dir≤67.878` — it hurt.)*
- **Guards:** none. **Exit:** SL 0.90 / Tgt 2.50 (wide; lets the runner run). **Status:** STRONG PROBATION (train 2.38 / test 2.66, p 0.005).

### L_DOUBLE_BOTTOM_VWAP (LONG) — *active — RAW-POOL caveat*
- **Logic:** retest of the intraday 8-bar low (double bottom) that holds above VWAP, closes strong on volume; only pays with a momentum/ADX thrust.
- **Detection:** `|low−intraday_low_8|≤0.40×ATR`, `close>VWAP`, `close>open`, `close_loc≥0.60`, `vol_ratio≥1.5`. (reason `double_bottom_vwap_reclaim`)
- **Indicators:** intraday_low_8, VWAP, ATR, vol_ratio, close_loc, ADX, pre-mom.
- **Filters (mask):** none.
- **Gates (pre-mom, ALL, missing→block):** `pre_entry_momentum_score≥79.0`, `sig5_adx_calc≥28.0`. (Alt G-style: `pre2_mom_r≥0.42 & sig5_adx_calc≥28.0`.)
- **Guards:** none. **Exit:** SL 0.90 / Tgt 1.50. **Status:** STRONG PROBATION **but evaluated on RAW pre-gate pool** — live research-layer currently **blocks the L* family**; reconcile gating before sizing.

### L_PRESSURE_BURST_VWAP (LONG) — *active — WEAK, user override*
- **Logic:** a buying-pressure burst above VWAP & EMA20 in a mid-RSI band.
- **Detection:** `pressure_ratio≥3.0`, `close>VWAP`, `close>EMA20`, `vol_ratio≥1.5`, `50≤RSI≤75`. (reason `buy_pressure_burst_vwap`)
- **Indicators:** pressure_ratio, VWAP, EMA20, RSI, vol_ratio, quality_score, ADX.
- **Filters (mask):** `quality_score≤25.0` (selects LOW scanner quality — counterintuitive).
- **Gates (pre-mom, missing→block):** `pre1_adx≥44.0` (very high pre-entry ADX).
- **Guards:** none. **Exit:** SL 0.70 / Tgt 1.25. **Status:** WEAK / CAUTION, RAW-POOL, **USER_APPROVED_OVERRIDE_WEAK** (fails monotonic-sensitivity + multi-exit checks; non-monotonic `pre1_adx`).

---

## 3. DEMOTED 2026-06-22 — now `enabled=False` (NOT traded live)

Kept in `RESEARCH_WATCH_CONF` for re-validation after losing money in live conf paper (06-16…06-22; conf-era book net −Rs 29,053 / PF 0.25). Re-validation trigger: band-objective re-tune on regenerated June pool must show test PF ≥ 1.30 + day_block_p < 0.10.

| Setup | Side | Filters (mask) | Gates (pre-mom) | Exit | Live result |
|---|---|---|---|---|---|
| **P_PDH_BREAK_RETEST_LONG** | LONG | `body_pct≤0.749993` | `pre_entry_momentum_score≥75.07`, `pre3_range_r≥0.499787` | 0.50/0.60 | −Rs 14,497, 40 tr, PF 0.25, win 25% (over-fires ~13×/day on a 0.50/0.60 scalp = death-by-cost) |
| **L_RS_LEADER_VWAP_HOLD** | LONG | `quality_score≥97.121`, `vol_ratio≥2.1643`, `vwap_dist_atr≤1.4934`, `signal_minute≤660` | none | 0.50/1.25 | −Rs 6,619, 13 tr, PF 0.15, win 7.7% |
| **V_RECLAIM_PULLBACK_LONG** | LONG | `rs_pct≥0.372426` | `pre_entry_momentum_score≤58.013`, `sig5_adx_calc≥33.933` | 0.50/0.80 | −Rs 1,937, 3 tr, PF 0.00, win 0% |
| **E_ORB_RETEST_HOLD_LONG** | LONG | `vol_ratio≥2.4238`, `quality_score≥86.575`, `signal_minute≥605` | `sig5_adx_calc≥42.416` | 0.90/1.25 | −Rs 1,442, 5 tr, PF 0.01, win 20% |

**Detection logic:** P_PDH = prev-day-high break-and-retest continuation (Tier 3). L_RS_LEADER = RS-leader VWAP test-and-hold (full structural list: `rs_pct≥0.75`, `stock_ret≥0.30`, `close>EMA20`, `EMA20≥EMA50`, `ema20_slope_3bar>0`, `low≤VWAP+0.30·ATR`, `close>VWAP`, `close>open`, `close_loc≥0.60`, `close>prev_bar_high`, `vol_ratio≥1.3`, `ADX≥20`, `50≤RSI≤72`, `regime≠BEAR`; window 09:45–14:00). E_ORB = OR-high retest-and-hold (Tier 1). V = reclaim-pullback in a strong-ADX RS leader (Tier 1).

---

## 4. RESEARCH_WATCH_CONF — rejected, never traded (`enabled=False`)

Recorded with best-found config + re-validation trigger only.

| Setup | Side | Why rejected |
|---|---|---|
| D_AVWAP_LOSE_REVERSAL | SHORT | train 2.98 on n=26 collapsed to 1.06 on deeper mine — small-sample fluke; only down-market gates work |
| E_ORB_RETEST_HOLD_SHORT | SHORT | works only at tight 0.6 target / narrow time cliff; imbalanced halves |
| T_TREND_DAY_EMA_STAIR_SHORT | SHORT | train-2.53 edge was a broken-VWAP artifact; corrected data → train loser 0.49 |
| S_UPTHRUST_TRAP_FADE | SHORT | train-1.67 edge was a broken-regime artifact; corrected data → train loser 0.59 |
| E_ORB_BREAKOUT_SHORT | SHORT | the big churn/cost-sink; best train 1.04 / test 0.94, p 0.586 (random) |
| E_ORB_BREAKOUT_LONG | LONG | breakout-chase, 22% immediate-fail; best train 0.96 / test 0.91 |
| E_VWAP_BAND_FADE | SHORT | closest of three but train still <1.5, test n=7 (p 0.276) |
| L_BB_SQUEEZE_LONG | LONG | marginal NEUTRAL+low-range config, p 0.121, only 55 raw test trades |
| L_TREND_PULLBACK | LONG | best train 1.49 / test 1.71, p 0.232 — no significant edge |
| S_BB_SQUEEZE_SHORT | SHORT | near-breakeven baseline; sample-capped at 16 test trades |
| S_MACD_HIST_FLIP | SHORT | gaudy PFs but a single-day (2026-05-12 crash) artifact |
| T_TREND_DAY_EMA_STAIR_LONG | LONG | only a fragile 4-term config (p right at 0.100) |
| MR_CONTROLLED_VWAP_EXTREME_FADE_LONG | LONG | 11 test trades; single-day artifacts (top1day 548%) |
| MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT | SHORT | 12 test trades; p 0.266, day-concentrated |

---

## 5. LIVE-vs-CONF CROSS-CHECK

> **Bottom line:** the conf book is *not* the only strategy layer in the live pipeline. A second layer — the **v11 live overlay** — selects a **different, broader universe** with **older / contradictory per-setup gates**, including several confirmed RESEARCH_WATCH rejects. Which layer actually drives a given live day depends on the `EQIDV2_USE_FINAL_SETUP_CONF` env flag and which scheduled `.bat` runs. `conf_adherence_check.py` exists precisely to catch overlay leakage in the day's paper trades.

### 5.1 Two parallel selection layers

| | (A) Conf bootstrap | (B) v11 live overlay |
|---|---|---|
| File | [../eqidv2_final_conf_live_bootstrap.py](../eqidv2_final_conf_live_bootstrap.py) | [../eqidv2_v11_live_overlay.py](../eqidv2_v11_live_overlay.py) |
| Active when | `EQIDV2_USE_FINAL_SETUP_CONF` truthy | the v11 selected-strategy profile is wired in |
| Universe | the 12 conf setups (+ Tier-C readmits) | `production_core_ab_max_pnl_low_valid_residual_overlay_tier123_balanced` profile (broader) |
| Gates | conf `mask_terms` + `pre_momentum_terms` (faithful port) | OLD production thresholds (`MAX_PNL_*`, `RESIDUAL_*`, `TIER123_*`) |
| Exits | conf per-setup SL/Tgt | `SELECTED_STRATEGY_EXIT_OVERRIDES` / `TIER123_BALANCED_EXIT_RULES` |

The bootstrap pushes conf masks/gates/exits into the existing v7 globals and adds `apply_conf_gate` as the final scanner filter. The overlay independently runs `selected_strategy_mask` on its own universe. **When both are wired, overlay-universe setups with non-conf gates can reach the entry engine** — this is the leak.

### 5.2 Same-name setups with CONTRADICTORY gates (conf vs overlay)

| Setup | Conf (gate of record) | v11 overlay (live) | Verdict |
|---|---|---|---|
| **B_AVWAP_RECLAIM_REVERSAL** | mask `vwap_dist_atr ≤ 1.0` (near-VWAP) | `vwap_dist_atr ≥ 0.60` (`MAX_PNL_B_AVWAP_MIN_VWAP_DIST_ATR`) | ⛔ **Direct contradiction** — overlay uses the *inverted* mask the conf explicitly diagnosed as wrong (PF 0.6, 43% immediate-fail) |
| **D_EMA20_REJECTION** | no mask + pre-mom (`pre10≤0.157, pre5≥0.125, adx≥20`) | `body_pct≥0.89 & ranker_score≥0.39` + residual late-D (signal_minute 780–825) | ⛔ Different gate; overlay uses the dropped production mask, **no pre-momentum** |
| **A_MOD_BREAK_C1_LOW** | mask `vol_ratio≥1.956` + pre-mom (`pre5_mom_r≥0.426, pre3_range_r≤0.202`) | `abs(rs_pct)≥9.2 & vol_ratio≥1.80` | ⛔ Different gate; **no pre-momentum** |
| **E_VWAP_LOSE_EARLY_SHORT** | mask volume band `1.8 ≤ vol_ratio ≤ 3.2` | `vwap_dist_atr ≥ −1.25` (`MAX_PNL_E_VWAP_LOSE`) | ⛔ Different gate; **no volume band** (the edge) |
| **A_PULLBACK_C2_THEN_BREAK_C2_LOW** | raw detection only; exit 1.20/1.50 | `market_abs_ret_pct ≤ 0.84` + A/B top-slot gate | ⚠️ Extra filter + A/B quality gate; exit differs |
| **B_HUGE_C1_CLOSE_RECLAIM_BREAK** | mask `regime ≠ BULL`; exit 1.00/1.50 | `rs_pct ≤ 10.7` (the no-op) + A/B top-slot gate | ⚠️ Overlay uses the no-op rs_pct mask the conf replaced; SL user-overridden from 0.70 to 1.00 |

### 5.3 Overlay-only setups (in live overlay, NOT in conf book → leakage if wired)

`selected_strategy_mask` admits these even though they are **not** in `FINAL_SETUP_CONF`:

`C_OR_BREAKOUT`, `D_EMA20_BOUNCE`, `E_ORB_BREAKOUT_LONG`, `E_ORB_BREAKOUT_SHORT`, `L_BB_SQUEEZE_LONG`, `S_BB_SQUEEZE_SHORT`, `A_MOD_BREAK_C1_HIGH`, `A_MOD_CLOSE_CONTINUATION_BREAK`, `E_VWAP_BAND_FADE`, `T_TREND_DAY_EMA_STAIR_SHORT`, `MR_CONTROLLED_VWAP_EXTREME_FADE_LONG`, `MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT`.

⛔ **Of these, the following are explicit RESEARCH_WATCH rejects (confirmed losers) that would leak into live:**
`E_ORB_BREAKOUT_SHORT` (the biggest churn/cost-sink), `E_ORB_BREAKOUT_LONG`, `E_VWAP_BAND_FADE`, `S_BB_SQUEEZE_SHORT`, `T_TREND_DAY_EMA_STAIR_SHORT`, `MR_CONTROLLED_VWAP_EXTREME_FADE_LONG`, `MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT`.

The overlay also runs its own **Tier-123 live scan** (`_scan_tier123_ticker_slot`) that *re-emits* `T_TREND_DAY_EMA_STAIR_SHORT` and the `MR_*` fades as fresh candidates — a second injection route for rejected setups.

### 5.4 Conf setups NOT in the overlay universe (fire only via bootstrap / Tier-C scanner)

These have no overlay representation, so they only ever trade when the conf bootstrap path is active:
`B_HUGE_RED_FAILED_BOUNCE`, `C_OR_BREAKDOWN`, `G_LOWER_LOW_BREAK`, `G_HIGHER_HIGH_BREAK`, `L_DOUBLE_BOTTOM_VWAP`, `L_PRESSURE_BURST_VWAP` — plus the Tier-C longs (`L_RS_LEADER_VWAP_HOLD`, `P_PDH_BREAK_RETEST_LONG`, `E_ORB_RETEST_HOLD_LONG`, `V_RECLAIM_PULLBACK_LONG`, now demoted) which are emitted by the **conf-mode Tier-C live scanner** and readmitted past v8/research before the final conf gate.

### 5.5 Runtime arbiter

`conf_adherence_check.py` (EOD guard) reads the day's `paper_trades_<date>_id_5min_v7.csv`, marks each trade `in_conf` against `FINAL_SETUP_CONF`, and:
- **GREEN** if ≥ 95% of trades are conf setups (`EQIDV2_CONF_ADHERENCE_MIN`);
- **RED / exit 3** if legacy / non-conf setups leaked — and lists them by setup + PnL.

This is the authoritative check of *what actually traded*. The static analysis above predicts *which* setups leak; the adherence JSON confirms it per day. (See memory `project_conf_overfit_and_live_overlay_leak_2026_06_22` — live conf paper PF 0.25 was traced to exactly this overlay leak + P_PDH over-firing.)

### 5.6 Action items implied by the cross-check
1. **B_AVWAP_RECLAIM_REVERSAL** — if the overlay is live, it trades the *inverted* (wrong) mask. Either disable the overlay path or sync `MAX_PNL_B_AVWAP_MIN_VWAP_DIST_ATR` to the conf's `≤ 1.0` near-VWAP rule.
2. **Suppress the overlay universe when the conf flag is on** so D/A_MOD/E_VWAP_LOSE use conf gates (with pre-momentum), not the old production thresholds.
3. **Block the overlay's reject re-injection** (E_ORB_BREAKOUT_*, S_BB_SQUEEZE_SHORT, E_VWAP_BAND_FADE, T_TREND_DAY_EMA_STAIR_SHORT, MR_* via Tier-123 scan).
4. Keep the demoted-4 (§3) `enabled=False`; the bootstrap trades only `FINAL_SETUP_CONF`, so they stay out as long as no `--approve` regenerates the file (which would also drop the demotion block — re-apply it after any `--approve`).
5. Run `conf_adherence_check.py` daily; treat any RED as a live-book breach.

---

## 6. 2026-06-29 live survival audit

**Actual live paper holdout checked:** `paper_trades_2026-06-16_id_5min_v7.csv` through `paper_trades_2026-06-29_id_5min_v7.csv`.

### 6.1 Live counts and PF

| Window | Trades | Days | Net Rs | Win % | PF |
|---|---:|---:|---:|---:|---:|
| 2026-06-16..2026-06-29 | 114 | 7 | -29,602 | 24 | 0.25 |
| Post-2026-06-22 demotion: 2026-06-23 + 2026-06-29 | 4 | 2 | -549 | 25 | 0.37 |

Setup breakdown for the full live paper window:

| Setup | Trades | Net Rs | Win % | PF | Decision |
|---|---:|---:|---:|---:|---|
| P_PDH_BREAK_RETEST_LONG | 40 | -14,497 | 25.0 | 0.25 | already demoted 2026-06-22 |
| L_RS_LEADER_VWAP_HOLD | 13 | -6,619 | 7.7 | 0.15 | already demoted 2026-06-22 |
| V_RECLAIM_PULLBACK_LONG | 3 | -1,937 | 0.0 | 0.00 | already demoted 2026-06-22 |
| MR_CONTROLLED_VWAP_EXTREME_FADE_LONG | 3 | -1,850 | 0.0 | 0.00 | non-conf leak on 2026-06-16 only |
| MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT | 8 | -1,452 | 12.5 | 0.00 | non-conf leak on 2026-06-16 only |
| E_ORB_RETEST_HOLD_LONG | 5 | -1,442 | 20.0 | 0.01 | already demoted 2026-06-22 |
| E_VWAP_LOSE_EARLY_SHORT | 31 | -790 | 32.3 | 0.79 | demoted 2026-06-29 |
| E_ORB_BREAKOUT_SHORT | 9 | -524 | 44.4 | 0.64 | non-conf leak on 2026-06-16 only |
| D_EMA20_BOUNCE | 1 | -396 | 0.0 | 0.00 | non-conf leak on 2026-06-16 only |
| D_EMA20_REJECTION | 1 | -95 | 0.0 | 0.00 | demoted 2026-06-29 survival pass |

Day-by-day:

| Day | Trades | Net Rs | Win % | PF | Notes |
|---|---:|---:|---:|---:|---|
| 2026-06-16 | 30 | -4,442 | 26.7 | 0.29 | one-day legacy/non-conf leakage present |
| 2026-06-17 | 16 | -9,223 | 0.0 | 0.00 | demoted Tier-C longs bled |
| 2026-06-18 | 26 | -7,294 | 23.1 | 0.21 | demoted Tier-C longs + E short |
| 2026-06-19 | 19 | -5,789 | 21.1 | 0.23 | demoted Tier-C longs + E short |
| 2026-06-22 | 19 | -2,304 | 42.1 | 0.65 | last day before first demotion |
| 2026-06-23 | 3 | -255 | 33.3 | 0.55 | conf-clean, only E short fired |
| 2026-06-29 | 1 | -294 | 0.0 | 0.00 | conf-clean, only E short filled |

### 6.2 E_VWAP_LOSE_EARLY_SHORT rescue check

The gentler fixes did not hold:

| Variant | Trades | Net Rs | Win % | PF |
|---|---:|---:|---:|---:|
| first E trade per day | 6 | -1,074 | 0.0 | 0.00 |
| quality_score >= 90 | 15 | ~0 | 33.3 | 1.00 |
| quality_score >= 100 | 4 | -15 | 50.0 | 0.98 |

So the 2026-06-29 action is not to add another tiny live-fit threshold. The setup is parked.

### 6.3 Survival book now active

`final_setup_conf.py` and `Train_and_Test/final_setup_conf.py` now apply a second reversible demotion block, `_LIVE_SURVIVAL_DEMOTION_2026_06_29`.

The tradeable book is the four corrected-VWAP mined shorts plus the user-repromoted A pullback watchlist setup:

| Setup | Side | Prior train/test evidence |
|---|---|---|
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | SHORT | user-repromoted 2026-06-29; `quality_score>=123.7606` + `sig5_adx_calc>=21.4683`; last-1-month available replay 30 / PF 3.491 |
| B_HUGE_RED_FAILED_BOUNCE | SHORT | train 30 / PF 2.90, test 20 / PF 3.49 |
| C_OR_BREAKDOWN | SHORT | train 29 / PF 2.78, test 19 / PF 5.26 |
| A_MOD_BREAK_C1_LOW | SHORT | train 38 / PF 2.58, test 30 / PF 2.83 |
| G_LOWER_LOW_BREAK | SHORT | train 51 / PF 2.25, test 9 / PF 9.12 |

Parked on 2026-06-29: `B_AVWAP_RECLAIM_REVERSAL`, `B_HUGE_C1_CLOSE_RECLAIM_BREAK`, `D_EMA20_REJECTION`, `E_VWAP_LOSE_EARLY_SHORT`, `G_HIGHER_HIGH_BREAK`, `L_DOUBLE_BOTTOM_VWAP`, `L_PRESSURE_BURST_VWAP`.

Re-promotion trigger: fresh live-gated rolling train/test must show test PF >= 1.30, test trades >= 20, day_block_p <= 0.10, and subsequent live paper holdout PF >= 1.20.

### 6.4 Runtime fix

The weekday scheduler now points to conf wrappers:

- `run_conf_paper_signal_discovery.bat`
- `run_conf_paper_entry_engine.bat`
- `run_conf_paper_executor.bat`
- `run_conf_live_executor.bat`

This prevents the scheduled live stack from accidentally running the legacy non-conf universe.
