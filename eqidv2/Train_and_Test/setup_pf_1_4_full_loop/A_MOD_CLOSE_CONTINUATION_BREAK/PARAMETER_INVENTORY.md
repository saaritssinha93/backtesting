# A_MOD_CLOSE_CONTINUATION_BREAK (LONG) — PARAMETER_INVENTORY

_Generated 2026-07-02. Research-only campaign; no live trades; final_setup_conf.py untouched._

## 1. Current setup rules (config of record)

| item | value | source |
|---|---|---|
| setup name | `A_MOD_CLOSE_CONTINUATION_BREAK` | `all_setups_catalog.py:81` |
| side | LONG | catalog / detector |
| entry trigger | moderate-impulse bar, long structure, above session VWAP, `close_loc >= 0.75` (close near bar high), `close > prev_bar_high`, `rs_pct > 0.00`, `vol_ratio >= 1.4`, quality_score >= 6.8 (catalog qs) | `avwap_5min_ID_v2_backtesting.py:704-711` |
| detector reason | `moderate_close_near_high_continuation` | same |
| indicator rules (detector) | session-VWAP position (above), relative strength `rs_pct > 0`, relative volume `vol_ratio >= 1.4` | same |
| non-indicator rules (detector) | moderate impulse body, long candle structure, close in top 25% of bar, breakout above previous bar high | same |
| pre-momentum rules | none currently | — |
| filters (live overlay gate) | `signal_range_pct >= 2.2` OR `notional <= Rs 100,000` (max_pnl_low_valid OR-gate) | `eqidv2_v11_live_overlay.py:61-62,374-377` |
| guards | none setup-specific (book-level 20-position cap) | — |
| SL / target | SL 0.70% / Tgt 1.50% | `v6.SETUP_EXIT_RULES` |
| exit logic | first-touch SL/TARGET on 1-min bars, else EOD force-exit by 15:20 IST | `v11.er.resolve` |
| time windows | scan slots 09:15–15:00 | v11 scan defaults |
| portfolio limits | MAX_POSITIONS=20, DAILY_LOSS_RS=0 (off) | `setup_train_test.py` |
| config source | **NOT in `FINAL_SETUP_CONF` or `RESEARCH_WATCH_CONF`** — an unpromoted catalog setup; baseline = raw detector + production exit (+ overlay OR-gate variant) | `final_setup_conf.py` |
| entry model | next 1-min open after 5-min signal close, +15 bps/leg adverse slippage, Rs 100k notional sizing | `setup_train_test._entry` |
| cost model | statutory NSE intraday (brokerage cap, STT, exch, stamp, GST) + 15 bps/leg slippage | `nse_intraday_costs.py` |

## 2. Available columns/features in the recreated pool (raw basis)

Verified non-null coverage on this setup's rows (14,378 master rows):

| group | columns (coverage) |
|---|---|
| price/OHLC | `signal_open, signal_high, signal_low, signal_close` (100%) |
| volume | `signal_volume` (100%), `vol_ratio` (100%), `notional` (computed at entry attach) |
| VWAP | `vwap_dist_atr` (100%) — signed distance from session VWAP in ATR units |
| relative strength | `rs_pct` (100%), `market_ret_pct` (100%, excluded from search — overfit vector) |
| volatility | `atr_pct` (100%), `signal_range_pct` (derived by load_pool, 100%) |
| candle structure | `body_pct, close_loc` (100%); `upper_wick_pct, lower_wick_pct, wick_skew_pct` (derived by load_pool from signal OHLC) |
| quality/ranker | `quality_score` (100%); `ranker_score` (~0% — not usable) |
| setup reason | `reason` = `moderate_close_near_high_continuation` |
| time/session | `signal_time_ist`, `_slot`, `signal_minute` (derived), `_day` |
| symbol | `ticker` (~1,280-name NSE universe) |
| regime | `regime` categorical (BULL/BEAR/NEUTRAL) |
| **empty for this setup** | `rsi, rsi3max, adx, macd_hist, macd_hist_delta, ema20_slope, upper/lower_wick_pct (pool-native), stock_ret` — populated only for other setups' rows; **cannot be swept from the pool** |

### Requested indicators NOT available in the recreated pool
RSI/ADX/MACD/BB/Keltner/MFI/OBV/CCI/Stochastic/Williams %R/ROC/Supertrend columns are not
emitted by the raw candidate scan for this setup (0% non-null). Their causal equivalents ARE
available through the repo's pre-momentum feature engine (computed from 1-min bars before entry):

| pre-momentum feature | proxies |
|---|---|
| `pre_entry_momentum_score` | composite pre-entry momentum |
| `sig5_adx_calc` | ADX/trend strength at signal |
| `sig5_rsi_dir` | RSI direction before trigger |
| `sig5_vol_ratio20` | pre-signal relative volume |
| `pre1_adx` | trend strength 1 bar before |
| `pre3_range_r` | 3-bar range compression/expansion (R-normalised) |
| `pre5_mom_r` | 5-bar momentum (R-normalised) |
| `pre3_close_pos` | close positioning of prior 3 bars |

## 3. Supported knobs in the repo pipeline (all searched)

| knob | supported | search range |
|---|---|---|
| `mask_terms` (AND, post-dedupe) | yes | up to 2 terms over 11 features x q10..q90 x {>=,<=} + categorical `regime` |
| `pre_momentum_terms` | yes | up to 1 term over 8 features x q10..q90 x {>=,<=} |
| `min_slot` | yes | 09:30, 09:45, 10:00, 10:30, 11:00 |
| `max_slot` | yes | 11:00, 12:00, 13:00, 14:00, 14:30 |
| `top_n` (per-slot, by vwap_dist_atr) | yes | 1, 2, 3 |
| `max_positions` | yes | 10, 20 |
| max trades/day / per-symbol | indirect (top_n + family dedupe: one ticker/day) | via top_n |
| `daily_loss_rs` | yes | 0 (off), 4000 |
| regime_align / regime band | book-level opt-in (`REGIME_ALIGN`); per-setup via `regime` mask term | categorical mask |
| SL | yes | 0.40, 0.50, 0.60, 0.70, 0.85, 1.00, 1.20 (%) |
| target | yes | 0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50 (%) |
| trailing SL | **not supported** by `er.resolve` (first-touch SL/TGT/EOD only) | — |
| break-even SL | **not supported** | — |
| time exit | EOD force-exit 15:20 only; earlier time exit not supported | — |
| EOD exit | yes (always on) | — |

## 4. Candidate parameter ranges and why they are realistic

- **SL 0.40–1.20% / Tgt 0.60–2.50%**: brackets the production 0.70/1.50 with tight-scalp and
  wide-runner variants; anything tighter than 0.40% is inside the 15 bps/leg slippage noise, and
  SL > 1.2% on a Rs 100k intraday clip risks more per trade than the book's daily loss tolerance.
- **Mask thresholds at TRAIN quantiles (q10–q90)**: thresholds always correspond to real deciles
  of the setup's own TRAIN distribution — never magic numbers, never TEST-informed.
- **vol_ratio >= floors (relaxed 1.6 / medium 2.4 / strict 4.8 = q10/q50/q90)**: relative volume is
  the natural confirmation for a continuation break.
- **body_pct / close_loc / wick floors**: continuation quality — a strong body closing near the
  high with a small upper wick is the structural definition of this setup, so tightening these is
  defensible, not curve-fit.
- **vwap_dist_atr / atr_pct caps**: overextension and volatility control (avoid chasing an
  already-stretched move; avoid uncontrolled ATR names where 0.7% SL is noise).
- **min_slot/max_slot**: this is a momentum-continuation setup; morning slots carry the real
  follow-through, late-day breaks tend to fade — a time guard is structural.
- **top_n per slot**: caps same-slot signal bursts (overtrading guard), mirrors live executor.
- **market_ret_pct / notional / signal_minute masks excluded** from the numeric search — the repo's
  documented dominant overfit vectors (`DEFAULT_EXCLUDED_FEATURES`, setup_train_test.py:101-105).
  Time-of-day is expressed through the explicit slot guards instead.

## 5. Search protocol

- FIT = first 60% of TRAIN sessions, VAL = last 40%; quantile grids from TRAIN only.
- Band objective `reward(min(PF_fit, PF_val)) − 0.80·|PF_fit − PF_val|`, tent at PF 1.70,
  overshoot above 1.70 penalised 1.5x (anti-overfit); trade count only a tiebreaker in-band.
- Full TRAIN confirmation required to be in PF [1.30, 1.80] before TEST is scored ONCE.
- TEST evaluation budget: 10 configs max for the whole campaign (no tuning against TEST).
- Domination caps: top trade <= 35% of gross profit, top day <= 40% of net, top symbol <= 40% of net.
- Robustness: +/-1 quantile-step neighborhood stability + single-term dropout (engine defaults).
