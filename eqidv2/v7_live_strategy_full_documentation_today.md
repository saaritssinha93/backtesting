# V7 Live Strategy Full Documentation

Document date: 2026-06-10  
Document revision: rev 2 — reconciled against the codebase after the full P0/P1 punchlist batch (net cost model, walk-forward gate, causal VWAP). See changelog below.  
Timezone used by the live system: Asia/Kolkata / IST  
Workspace: `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2`  
Runtime root: `C:\TradingData\eqidv2`

This document describes the V7 ID 5-minute live strategy ecosystem as it exists in the codebase and runtime tree on 2026-06-10. It separates confirmed facts from items that need verification. Confirmed facts were taken from the dashboard server, batch runners, live runtime outputs, and the V7 pipeline scripts.

**Changelog since 2026-06-09:**
- V7_PUNCHLIST.md implemented for P0-1 through P2-15 except P1-8 (point-in-time universe). P0-3 is wired report-only (the gate runs and reports but does not yet author `accepted_rules.csv`).
- **Net cost accounting (P0-1):** `nse_intraday_costs.py` (NSE intraday-equity cost model) is wired into the paper executor. `paper_trades_<date>.csv` now carries `gross_pnl`, `total_cost`/`total_cost_rs`, `net_pnl`/`net_pnl_rs`, `net_pnl_pct`, and `cost_bps_of_turnover`; STT is charged on the correct leg (entry for shorts, exit for longs). Headline P&L is NET going forward. The 21-day cumulative table below was computed GROSS, before this wiring.
- **Causal VWAP (P1-6):** `v2._prepare_5m` now preserves the incoming parquet VWAP as `VWAP_source` and always recomputes `VWAP` as a causal session VWAP before V7 consumes it. This reverses the earlier "trust the parquet VWAP column as-is" behavior. Audit: `v7_causality_audit.py` (task `EQIDV2_v7_causality_audit_1605`).
- **Walk-forward gate (P0-3/P0-4/P1-9):** `walkforward_gate.py` provides OOS PROMOTE/PROBATION/REJECT decisions with Benjamini-Hochberg FDR. `v7_walkforward_gate_report.py` runs it as a report (task `EQIDV2_v7_walkforward_gate_1620`); it does NOT write the production `accepted_rules.csv` yet. All V11 overlay thresholds rounded to ≤2 decimals (P0-4); §9.5 below updated to match the live overlay source.
- **NSE ID cost report:** `v7_nse_id_cost_report.py` writes a daily net-cost breakdown from the paper-trade CSV (task `EQIDV2_v7_nse_id_cost_1605`).
- Risk-based position sizing, NIFTY regime gate, and gross short notional cap added to entry engine and executors.
- Exit slippage (5 bps) now modelled in paper executor on SL/time-stop/EOD-close outcomes.
- ADV liquidity cap (1% of 20-day average daily traded value) added to entry engine.
- F&O ban pre-trade filter added to entry engine (NSE fo_secban.csv, lazy daily fetch, fail-open).
- Candidate schema versioning (`CANDIDATE_SCHEMA_VERSION = "v7_candidate_2026_06_10"`) stamped on every CSV row.
- ADV cap and F&O ban reject reasons surfaced in dashboard funnel card (`adv_rej`, `fno_rej` columns).
- Replay `--replay-slots` now hard-aborts unless `EQIDV2_REPLAY_OUTPUT_ROOT` is set or `--production-replay` is passed.

## 1. Executive Summary

The V7 live strategy is a two-stage intraday equity pipeline:

```text
Live 5-minute data fetch
-> 5-minute indicator parquet files
-> Signal Discovery V7 5mins ID candidate scan
-> Candidate ticker CSV/JSON outputs
-> 1-minute entry engine
-> Live signal CSVs
-> Paper executor and optional live Zerodha executor
-> Dashboard, research layer, logs, audits, and EOD reports
```

The active production path is not the legacy persistent scanner. The current production writer is:

```text
eqidv2_signal_discovery_v7_5min_id_persistent.py
-> eqidv2_entry_engine_1min_v5_id.py
-> eqidv2_live_signal_writer.py
-> live_signals/signals_<date>_id_5min_v7_<side>.csv
```

The most critical live-trading components are the 5-minute fetcher, Signal Discovery V7, the 1-minute entry engine, the signal writer, and the paper/live executors. Research jobs, dashboard jobs, Kite export, backtesting, and V16 jobs are operationally useful but should not be required for V7 signal generation unless a runner explicitly depends on their files.

**Cumulative paper results as of 2026-06-10** (21 trading days, Apr 21 – Jun 10) — these are **GROSS** P&L, computed before the P0-1 net cost model was wired. Net P&L is materially worse (costs run ~10–25% of gross edge per trade); regenerate net figures from the `net_pnl`/`net_pnl_rs` columns in `paper_trades_<date>.csv` or the `v7_nse_id_cost` report:

```text
Executed trades:   681    (993 total rows including skipped/entry-rejected)
Win rate:          26.9%  (183 TARGET / 304 SL / 194 EOD-close or time-stop)
Profit factor:     0.82   — currently a net-losing system
Net PnL:           Rs -54,122
Average per trade: Rs -79

Side split:
  SHORT:  148 trades  win%=27.7%  PF=0.77  PnL=Rs -8,512
  LONG:   533 trades  win%=26.6%  PF=0.83  PnL=Rs -45,610

Day win rate: 7/22 = 31.8%
Best day: 2026-05-27 +Rs 6,539 (25 trades, 28% win — EOD-close heavy day worked)
Worst day: 2026-06-04 -Rs 32,177 (141 trades, 16% win, 76 EOD closes)
```

**Top performing setups** (by PnL, confirmed edge):

| Setup | Side | Trades | Win% | PF | PnL Rs |
|---|---|---|---|---|---|
| A_MOD_BREAK_C1_LOW | SHORT | 13 | 62% | 3.28 | +3,486 |
| D_EMA20_REJECTION | SHORT | 20 | 30% | 1.16 | +1,629 |
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | SHORT | 2 | 100% | — | +1,491 |
| A_MOD_CLOSE_CONTINUATION_BREAK | LONG | 5 | 40% | 2.09 | +1,099 |
| E_ORB_BREAKOUT_SHORT | SHORT | 34 | 29% | 0.89 | +941 |

**Worst performing setups** (should be reviewed for disabling or shadow-blocking):

| Setup | Side | Trades | Win% | PF | PnL Rs | Note |
|---|---|---|---|---|---|---|
| C_OR_BREAKOUT | LONG | 207 | 17% | 0.98 | -20,521 | 61% EOD-close; Jun 3–4 drove disaster |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | LONG | 85 | 32% | 0.58 | -9,416 | |
| A_MOD_BREAK_C1_HIGH | LONG | 126 | 34% | 0.70 | -9,119 | Only LONG exempt from short-focus |
| T_TREND_DAY_EMA_STAIR_SHORT | SHORT | 40 | 12% | 0.35 | -8,406 | Strongly recommend disabling |
| E_VWAP_LOSE_EARLY_SHORT | SHORT | 20 | 25% | 0.59 | -3,636 | |

**Critical findings:**

- The Jun 3–4 disaster (combined -Rs38k) was driven by `C_OR_BREAKOUT` LONG hitting 127/207 EOD closes across those two days. Paper executor was running at 100-concurrent limit; C_OR_BREAKOUT effectively clogged the book with flat positions. New 20-position cap and session cap (50) should prevent recurrence.
- `T_TREND_DAY_EMA_STAIR_SHORT` is confirmed unviable: 12% win rate, PF 0.35. Recommend immediate shadow-block or disable.
- Most LONG losses come from three setups: C_OR_BREAKOUT, B_HUGE_C1_CLOSE_RECLAIM_BREAK, A_MOD_BREAK_C1_HIGH. All three have PF < 0.75.
- **Post-punchlist data is very thin** (Jun 9: +Rs466 on 8 trades; Jun 10: -Rs83 on 1 trade so far). Cannot draw conclusions yet on the new risk controls.
- SHORT setups are better positioned: 4 of the top 5 by PnL are SHORT.

Important current risks:

- `C_OR_BREAKOUT` had gated candidates but zero entry rows in late slots on 2026-06-09 — still unresolved.
- LONG setup flow was effectively absent from paper trades on 2026-06-09 (short-focus filter is the likely cause — intentional by design but worth monitoring).
- Candidate and audit CSV schema-backup churn is now resolved by P2-14 (`CANDIDATE_SCHEMA_VERSION` stamped on all rows).
- Replay tooling: now hard-aborts unless `EQIDV2_REPLAY_OUTPUT_ROOT` is set (P2-13 fix).
- Current code blocks `E_VWAP_LOSE_EARLY_SHORT` before 09:45; live review requested 09:40. Still needs explicit operator sign-off.

## 2. Complete System Overview

V7 Live Strategy is an ID 5-minute intraday strategy stack. It scans each completed 5-minute candle, generates signal-only candidates, waits for the T+1 minute entry moment, enriches candidates with 1-minute data, writes executable signal rows, then paper-trades or live-trades those rows.

The dashboard is a live operator console. It does not generate signals. It reads logs, runtime status, heartbeats, latest JSON/CSV outputs, paper results, live trade files, and scheduled-task state. It also exposes restart and kill-switch controls.

The critical path is:

1. Authentication and Kite access tokens are available.
2. Live 5-minute fetcher updates `stocks_indicators_5min_eq_live`.
3. Slot-ready markers and status JSON confirm the slot was written.
4. Signal Discovery V7 scans the completed slot and writes candidate tickers.
5. Entry engine wakes at T+1, waits for the candidate snapshot if needed, fetches 1-minute raw OHLCV, applies entry filters, and writes signal CSV rows.
6. Paper and live executors watch the signal CSVs and process new `signal_id` values.
7. Dashboard reads all files and renders operator cards.

Critical live-trading jobs:

- `eqidv2_eod_scheduler_for_5mins_data_live_minimal.py`
- `eqidv2_signal_discovery_v7_5min_id_persistent.py`
- `eqidv2_entry_engine_1min_v5_id.py`
- `eqidv2_live_signal_writer.py`
- `avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.py`
- `avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.py` if real orders are enabled

Monitoring-only jobs:

- `log_dashboard_server.py`
- `v7_research_layer/eqidv2_daily_live_v7_research_session.py`
- `v7_research_layer/eqidv2_v7_research_layer.py` in `--light-ops` loop mode
- `v7_research_layer/eqidv2_v7_pre_momentum_filter_analyst.py`
- `preopen_session_healthcheck.py`
- `check_pf_supervisor_health.ps1`

Research-only jobs:

- `v7_research_layer/eqidv2_v7_research_layer.py` full report mode
- `v7_research_layer/eqidv2_v7_pre_momentum_filter_analyst.py`
- `backtesting_result_v11_daily.py`
- replay and backtesting helper scripts

Maintenance-only or administrative jobs:

- Dashboard public link jobs
- Kite export jobs
- Authentication jobs
- Backtesting data mover jobs
- Scheduled-task hardening helpers

## 3. End-to-End Live Flow

### 3.1 Data Source and Live Fetch

The live feed is pulled through Zerodha Kite using multiple app sessions. The 5-minute fetcher is:

```text
eqidv2_eod_scheduler_for_5mins_data_live_minimal.py
runner: bat\run_eqidv2_eod_scheduler_for_5mins_data_live_minimal.bat
scheduled task: \EQIDV2_eod_5mins_data_0900
```

Confirmed runner configuration:

- Runtime root: `C:\TradingData\eqidv2`
- 5-minute output directory: `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live`
- 5-minute cache directory: `C:\TradingData\eqidv2\stocks_cache_5min_eq_live`
- Market window in code: 09:15 to 15:35 IST
- First completed 5-minute close: 09:20 IST
- Opening-slot fetch enabled
- Base and quarter-hour buffer: 2 seconds
- Max workers: 384
- Max workers per app: 48
- Kite timeout: 8 seconds
- Partition timeout: 150 seconds
- Slot SLA warn in runner: 50 seconds
- Ready marker enabled
- Ready-marker sample size from code default: 24
- Verification sample size in runner: 32

Runtime outputs:

```text
C:\TradingData\eqidv2\stocks_indicators_5min_eq_live\<TICKER>_stocks_indicators_5min.parquet
C:\TradingData\eqidv2\slot_ready_5m\slot_<YYYYMMDD>_<HHMM>.json
logs\eqidv2_eod_scheduler_for_5mins_data_live_minimal.status.json
logs\eqidv2_eod_scheduler_for_5mins_data_live_minimal.log
logs\eqidv2_eod_scheduler_for_5mins_data_live_minimal.supervisor.status
logs\eqidv2_eod_scheduler_for_5mins_data_live_minimal.supervisor.heartbeat
```

Example healthy slot marker from 2026-06-09 14:30:

```text
tickers_expected=1253
tickers_written=1253
tickers_failed=0
verification_failed_count=0
fresh_ratio=1.0
duration_ms=30485.797
```

### 3.2 Candle and Indicator Creation

The 5-minute fetcher writes indicator parquet files directly to the live 5-minute directory. The V7 scanner reads those parquet files. The live scan adapter keeps the V2/V7 backtest parity logic:

```text
avwap_5min_ID_v7_live_scan.py
avwap_5min_ID_v7_candidate_scan.py
avwap_5min_ID_v2_backtesting.py
```

Important confirmed behavior:

- The live adapter reads from `_eq_live` files.
- It recomputes session VWAP causally. As of 2026-06-10 (P1-6), `v2._prepare_5m` preserves the incoming parquet VWAP as `VWAP_source` and always overwrites `VWAP` with a cumulative-to-the-bar session VWAP, so the value used at 09:30 is causal (no full-session look-ahead). The earlier behavior of trusting the parquet VWAP column as-is has been removed. `day_value_so_far_rs` and `market_ret_pct` are likewise return-to-now, not return-to-close.
- It uses `v2._prepare_5m` for derived features such as VWAP, volume ratio, VWAP distance in ATR, Bollinger bands, prior-day values, and opening range.
- It uses `v2._scan_day` to detect setups.

Needs verification:

- The exact formulas for every V2 setup must be read from `avwap_5min_ID_v2_backtesting.py`. The dashboard does not show the full boolean expressions.

### 3.3 Signal Discovery V7 5mins ID

The signal discovery session is:

```text
script: eqidv2_signal_discovery_v7_5min_id_persistent.py
runner: bat\run_eqidv2_signal_discovery_v7_5min_id_persistent.bat
scheduled task: \EQIDV2_signal_discovery_v7_5mins_ID
runtime root: C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID
```

Purpose:

- Scan completed 5-minute signal candles.
- Write candidate tickers only.
- Do not write entry price, stop, target, or trade signal CSV rows.

Key timing:

- Slot interval: 5 minutes
- Runner post-slot delay: 75 seconds
- Feed gate enabled
- Feed gate status JSON default path in code: `logs\eqidv2_eod_scheduler_for_5mins_data_live_minimal.status.json`
- Feed gate max wait: 90 seconds
- Feed gate min delay in runner: 1 second
- Feed gate poll in runner: 0.5 second
- Entry window in runner: 09:30 to 14:30
- Entry lag: 1 minute
- Hard stop in code: 15:30
- Runner cutoff: 15:30

Selection and filtering:

- Selection mode: `v8_setup_compatible`
- V8 live gate enabled
- V8 rules CSV: `C:\TradingData\eqidv2\outputs_ID_v8_5min_research_restore\accepted_rules.csv`
- V11 overlay enabled
- V11 profile: `production_core_ab_max_pnl_low_valid_residual_overlay_tier123_balanced`
- Tier123 scan enabled
- Research live filters enabled in `active` mode
- Short focus enabled with allowed side `SHORT`
- `A_MOD_BREAK_C1_HIGH` is exempt from short-focus despite being LONG
- LONG anti-chase thresholds: close location > 0.97 and VWAP distance ATR > 3.50
- `B_AVWAP_RECLAIM_REVERSAL` requires ranker score >= 0.65
- `L_TREND_PULLBACK` probation block enabled

Signal discovery outputs:

```text
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\csv\candidate_tickers_<date>.csv
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\csv\raw_candidate_tickers_<date>.csv
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\csv\research_filter_rejected_candidate_tickers_<date>.csv
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\csv\v11_overlay_candidate_tickers_<date>.csv
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\csv\v11_overlay_rejected_candidate_tickers_<date>.csv
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\json\candidate_tickers_<YYYYMMDD>_<HHMM>.json
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\latest\latest_candidate_tickers.json
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\latest\latest_candidate_tickers.csv
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\audit\candidate_tickers_audit_<date>.csv
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\heartbeat\candidate_tickers.status.json
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\heartbeat\candidate_tickers.heartbeat.json
C:\TradingData\eqidv2\runtime_status\signal_discovery_v7_5mins_ID.status
C:\TradingData\eqidv2\runtime_status\signal_discovery_v7_5mins_ID.heartbeat
```

### 3.4 Candidate Ticker Generation

Candidate rows include:

- `candidate_id`
- `signal_time_ist`
- `ticker`
- `side`
- `setup`
- `selection_mode`
- `candidate_family`
- `quality_score`
- `ranker_score`
- `rs_pct`
- `market_ret_pct`
- `regime`
- `vol_ratio`
- `atr_pct`
- `body_pct`
- `close_loc`
- `vwap_dist_atr`
- `diagnostics_json`
- research shadow metadata

Dedup behavior:

- Candidate ID is based on ticker, side, setup, and signal time.
- Candidate frames are deduped so the strongest ticker candidate per signal candle is retained.
- Daily candidate CSV append logic skips duplicate `candidate_id`.

### 3.5 Entry Engine

The entry engine is:

```text
script: eqidv2_entry_engine_1min_v5_id.py
runner: bat\run_eqidv2_entry_engine_1min_v5_id.bat
scheduled task: \EQIDV2_entry_engine_1min_v5_ID
runtime root: C:\TradingData\eqidv2\entry_engine_1min_v5_ID
```

Purpose:

- Consume candidate tickers for the previous 5-minute slot.
- Fetch raw 1-minute OHLCV for candidate tickers only.
- Store raw 1-minute data for audit.
- Build executable entry rows.
- Attach stop/target values from V6/V11 setup exit rules.
- Apply pre-entry momentum gates.
- Write live signal CSV rows via `eqidv2_live_signal_writer.py`.

Key timing:

- Entry engine delay: 60 seconds after the 5-minute slot.
- Entry lag contract: T+1 minute.
- Candidate wait: 30 seconds.
- Candidate wait poll in runner: 0.5 second.
- Max signal handoff lag: 30 seconds.
- Entry due grace is capped by max lag minus process reserve.
- Entry search max delay: 3 minutes.
- Poll interval: 1 second.
- Runner cutoff: 15:35.

**New entry engine features (2026-06-10):**

Risk-based position sizing (P1-7):

- `EQIDV2_RISK_SIZING_ENABLED=1` (default on): qty = (equity × risk_pct%) / |entry − SL|, clamped to [min_notional, max_notional] / entry_price.
- `EQIDV2_RISK_PCT_PER_TRADE=0.25` (0.25% of equity per trade).
- `EQIDV2_RISK_EQUITY_RS=200000` (reference equity for sizing).
- `EQIDV2_RISK_MAX_NOTIONAL_RS=150000` (per-trade notional ceiling).
- `EQIDV2_RISK_MIN_NOTIONAL_RS=50000` (per-trade notional floor).

NIFTY regime gate (P1-7):

- `EQIDV2_NIFTY_REGIME_GATE_ENABLED=1` (default on): halves SHORT position size when NIFTY close > rising 20-day MA (bullish regime).
- `EQIDV2_NIFTY_REGIME_SHORT_SIZE_MULT=0.5` (multiplier applied in bullish regime).
- NIFTY 5-min parquet read once per day, cached in memory.

ADV liquidity cap (P2-11):

- `EQIDV2_ADV_CAP_ENABLED=1` (default on): rejects any signal whose proposed notional exceeds participation% of ticker's 20-day average daily traded value.
- `EQIDV2_ADV_PARTICIPATION_PCT=1.0` (1% of ADV).
- `EQIDV2_ADV_LOOKBACK_DAYS=20`.
- Source: `stocks_indicators_daily_eq\{TICKER}_stocks_indicators_daily.parquet`.
- Fail-open: if daily parquet missing, cap is not applied.

F&O ban pre-trade filter (P2-12):

- `EQIDV2_FNO_BAN_FILTER_ENABLED=1` (default on): blocks SHORT entries on F&O-banned securities.
- Fetches `fo_secban.csv` from NSE URL on first SHORT signal each morning, writes local cache to `C:\TradingData\eqidv2\runtime_status\fo_secban_today.csv`.
- `EQIDV2_FNO_BAN_URL` can be overridden if NSE changes the URL.
- Fail-open: network failure means no trades are blocked.

Entry outputs:

```text
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\latest\latest_summary.json
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\latest\latest_entry_engine_rows.csv
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\latest\setup_exit_rules_v8.csv
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\audit\entry_engine_audit_<date>.jsonl
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\audit\entry_rows_<YYYYMMDD>_<HHMM>.csv
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\audit\entry_rows_raw_candidates_<YYYYMMDD>_<HHMM>.csv
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\audit\entry_rejected_candidates_<YYYYMMDD>_<HHMM>.csv
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\audit\entry_rejected_v11_overlay_<YYYYMMDD>_<HHMM>.csv
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\audit\entry_rejected_pre_momentum_<YYYYMMDD>_<HHMM>.csv
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\audit\entry_rejected_freshness_<YYYYMMDD>_<HHMM>.csv
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\audit\entry_rejected_adv_cap_<YYYYMMDD>_<HHMM>.csv   ← NEW
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\audit\entry_rejected_fno_ban_<YYYYMMDD>_<HHMM>.csv   ← NEW
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\heartbeat\entry_engine.status.json
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\heartbeat\entry_engine.heartbeat.json
C:\TradingData\eqidv2\stocks_raw_1min_entry_v5_id_live\<TICKER>_stocks_raw_1min.parquet
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\slot_raw_1min\<YYYYMMDD>_<HHMM>\<TICKER>_raw_1min.parquet
```

### 3.6 Signal CSV Writer

The signal writer is:

```text
script: eqidv2_live_signal_writer.py
schema version: v7_signal_contract_2026_06_07
pipeline version: v7_live_1min_entry
```

Only the entry engine should call `write_side_signals()` for production writes. The legacy persistent scanner must not write production CSVs unless a legacy debug flag is explicitly enabled.

Production signal CSV outputs:

```text
C:\TradingData\eqidv2\live_signals\signals_<date>_id_5min_v7_short.csv
C:\TradingData\eqidv2\live_signals\signals_<date>_id_5min_v7_long.csv
C:\TradingData\eqidv2\live_signals\signals_<date>_id_5min_v7_short.csv.lock
C:\TradingData\eqidv2\live_signals\signals_<date>_id_5min_v7_long.csv.lock
```

Writer validation:

- Entry window check.
- Stale/future entry check against max lag.
- Contract timing validation through `eqidv2_v7_signal_contract.py`.
- Duplicate signal key check.
- Duplicate signal ID check.
- One ticker per day across both side CSVs.

Timing contract:

- `signal_bar_time_ist`: completed 5-minute signal candle.
- `intended_entry_ist`: signal bar time plus 1 minute.
- `detected_time_ist`: wall clock at CSV write.
- `deadline_ist`: intended entry plus 30 seconds.
- Reject if lag is negative, cross-day, malformed, or > 30 seconds.

### 3.7 Paper Executor

The paper executor is:

```text
script: avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.py
runner: bat\run_avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.bat
scheduled task: \EQIDV2_paper_trade_id_5min_v7_0900
```

Purpose:

- Watch both V7 ID signal CSVs.
- Deduplicate by `signal_id`.
- Optionally poll Kite LTP and simulate fills.
- Track stop/target hits using 5-second polling.
- Apply the NSE intraday cost model (`nse_intraday_costs.py`) to every resolved trade and write gross/net P&L columns (`gross_pnl`, `total_cost_rs`, `net_pnl_rs`, `net_pnl_pct`, `cost_bps_of_turnover`); the headline summary P&L is NET (P0-1).
- Write paper trade results and summary.

Confirmed runner defaults:

- Entry price source: `ltp_on_signal`
- Fallback stop/target: short SL 0.75%, short target 1.00%, long SL 0.75%, long target 1.00%
- Default signal margin fallback: Rs 10,000
- Max concurrent trades: 100
- Max open positions: 100
- Max capital deployed: Rs 2,000,000
- Daily loss brake enabled in runner at Rs 10,000
- `C_OR_BREAKOUT` time stop enabled at 30 minutes
- `C_OR_BREAKOUT` session cap enabled at 50
- Forced close: 15:20 IST
- Market close: 15:30 IST

Paper outputs:

```text
C:\TradingData\eqidv2\live_signals\paper_trades_<date>_id_5min_v7.csv
C:\TradingData\eqidv2\live_signals\paper_trade_summary_id_5min_v7.json
C:\TradingData\eqidv2\live_signals\paper_trade_execution_<date>_id_5min_v7.log
C:\TradingData\eqidv2\live_signals\executed_signals_paper_id_5min_v7.json
C:\TradingData\eqidv2\live_signals\open_trades_state_<date>_id_5min_v7.json
C:\TradingData\eqidv2\live_signals\kill_switch_true_id_5min_v7.json
C:\TradingData\eqidv2\live_signals\candE4_g2_counters_id_5min_v7_paper.json
C:\TradingData\eqidv2\live_signals\c_or_breakout_session_cap_counter_id_5min_v7_paper.json
```

### 3.8 Live Zerodha Executor

The real-order executor is:

```text
script: avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.py
runner: bat\run_avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.bat
scheduled task: \EQIDV2_live_trade_id_5min_v7_0900
```

Purpose:

- Watch V7 ID signal CSVs.
- Place real MIS market entry orders through KiteConnect.
- Place target LIMIT order and SL-M stop order.
- Monitor broker order state.
- Cancel opposite leg when one exit leg fills.
- Force close at 15:20 IST.

Confirmed live runner controls:

- Max trades: 20
- Max concurrent trades: 20
- Max open positions: 20
- Config attestation is written before startup.
- Supervisor monitors open positions state.
- NTP drift guard is configured with max drift 30 seconds.
- Cutoff: 15:45.

Live outputs:

```text
C:\TradingData\eqidv2\live_signals\live_trades_<date>_id_5min_v7.csv
C:\TradingData\eqidv2\live_signals\live_trade_summary_id_5min_v7.json
C:\TradingData\eqidv2\live_signals\open_live_trades_state_<date>_id_5min_v7.json
C:\TradingData\eqidv2\live_signals\executed_signals_live_id_5min_v7.json
C:\TradingData\eqidv2\live_signals\kill_switch_false_id_5min_v7.json
C:\TradingData\eqidv2\live_signals\mis_rejected_symbols_id_5min_v7.json
logs\avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7_<date>.log
```

Needs verification before enabling real orders:

- Confirm which account/app token is primary.
- Confirm live runner is intentionally enabled, not only scheduled.
- Confirm max open positions and max capital match current risk appetite.
- Confirm kill-switch card writes the intended JSON.

### 3.9 Dashboard and Monitor

Dashboard server:

```text
script: log_dashboard_server.py
runner: bat\run_log_dashboard_server.bat
scheduled start task: \EQIDV2_log_dashboard_start_0855
scheduled stop task: \EQIDV2_log_dashboard_stop_1700
host/port: 127.0.0.1:8787
refresh interval in browser: 15 seconds
```

The dashboard reads:

- `logs\*.log`
- `logs\*.status`
- `logs\*.heartbeat`
- `C:\TradingData\eqidv2\runtime_status\*.status`
- `C:\TradingData\eqidv2\runtime_status\*.heartbeat`
- V7 latest candidate files
- V7 entry-engine latest summary
- V7 signal CSVs
- paper/live trade CSVs
- Kite holdings and day positions exports
- task scheduler state

Dashboard API endpoints:

- `/api/snapshot`
- `/api/log`
- `/api/kill`
- `/api/restart`

Do not document or copy dashboard password/token values into notes. The server supports basic auth and API token auth through environment variables and runner arguments.

## 4. Dashboard Sections Used by V7

Confirmed dashboard groups:

| Section | Dashboard title | V7 relevance |
|---|---|---|
| Market | Market Data Readiness | NIFTY 5-minute guard and 5-minute live fetch. Critical upstream. |
| V7 Flow | Core V7 Live Flow | Signal discovery, candidate tickers, entry engine, monitor, signal CSVs, paper/live executor cards. Critical path. |
| Backtesting | Data & Backtesting | EOD data movement and V11 backtesting result. Research/support. |
| Research | Research & Suggestions | V7 research layer, daily live V7 research, pre-momentum analyst. Monitoring/advisory. |
| V16 | V16 / Parallel Strategy | Parallel strategy stack, not a direct V7 dependency. Useful comparison/legacy risk. |
| Admin | Admin & Exports | Auth, preopen healthcheck, Kite holdings/positions export, dashboard operation. |

Most important V7 dashboard cards:

| Card ID | Display title | Primary file |
|---|---|---|
| `signal_discovery_v7_5min_id` | Signal discovery v7 5mins ID | `signal_discovery_v7_5mins_ID\heartbeat\candidate_tickers.status.json` |
| `candidate_tickers_v7_5min_id` | Candidate tickers | `signal_discovery_v7_5mins_ID\latest\latest_candidate_tickers.csv` |
| `entry_engine_1min_v5_id` | Entry engine 1min v7 ID | `entry_engine_1min_v5_ID\latest\latest_entry_engine_rows.csv` |
| `v7_live_5min_monitor` | V7 ID 5min Live Monitor | `entry_engine_1min_v5_ID\latest\latest_summary.json` |
| `live_signals_csv_id_5min_v7_short` | Live Entries CSV ID 5mins v7 Short | `live_signals\signals_<date>_id_5min_v7_short.csv` |
| `live_signals_csv_id_5min_v7_long` | Live Entries CSV ID 5mins v7 Long | `live_signals\signals_<date>_id_5min_v7_long.csv` |
| `paper_trade_id_5min_v7` | V7 ID 5min Papertrade Runner Log | paper executor log/result view |
| `live_papertrade_result_csv_id_5min_v7` | V7 ID 5min Papertrade Results | `live_signals\paper_trades_<date>_id_5min_v7.csv` |
| `kite_trade_id_5min_v7` | V7 ID 5min Live Trade Runner Log | live executor log |
| `live_kite_trades_csv_id_5min_v7` | V7 ID 5min Live Kite Trades CSV | `live_signals\live_trades_<date>_id_5min_v7.csv` |

Healthy dashboard indicators:

- 5-minute fetch status `overall_state=OK`.
- Slot-ready marker for the latest completed slot exists and is fresh.
- Signal Discovery status is `RUNNING` during market or `STOPPED_AFTER_CUTOFF` after cutoff.
- Candidate latest JSON slot matches the expected latest scan slot.
- Entry engine latest summary slot is one minute after or close behind discovery.
- Signal CSVs update only when entries are actually selected.
- Paper executor log shows watchdog active and processed new signals.
- Paper summary updates when trades close.

Broken or stale indicators:

- Missing `slot_ready_5m\slot_<slot>.json`.
- `verification_failed_count` above configured tolerance.
- Candidate latest JSON stuck several slots behind.
- Candidate count > 0 but entry rows = 0 repeatedly.
- Signal CSV does not update while entry engine reports rows written.
- Paper executor open state remains non-empty after market close.
- Runtime heartbeat file age grows during market hours.

## 5. All Active Jobs and Sessions

| Job/session | Directness | Criticality | Runner/task | Frequency/window | Inputs | Outputs | Verify |
|---|---|---|---|---|---|---|---|
| Authentication V2 | Indirect | Critical before market | `bat\run_authentication_v2.bat`, `\EQIDV2_authentication_v2_0900` | Pre-market | Kite credentials/token files | auth logs/status | Dashboard `Auth_V2`, token age, no Kite auth errors |
| Preopen healthcheck | Support | Important | `bat\run_preopen_session_healthcheck.bat`, `\EQIDV2_preopen_session_healthcheck_0905` | 09:05 | scheduled tasks/status files | `logs\preopen_session_healthcheck_latest.*` | Latest report OK |
| Preopen autofix | Support | Important | `bat\run_preopen_session_autofix.bat` | 09:05-09:30 retry loop | healthcheck results | autofix log | No repeated failed action |
| NIFTY guard 5min | Upstream | Critical for market context | `bat\run_eqidv2_nifty_guard_fetcher_supervised_v16_5min.bat` | market slots | Kite/NIFTY data | `nifty_slot_ready_5m`, status | Latest marker fresh |
| 5min live data fetch | Upstream | Critical | `bat\run_eqidv2_eod_scheduler_for_5mins_data_live_minimal.bat`, `\EQIDV2_eod_5mins_data_0900` | 09:15-15:35 | Kite live data, universe | 5m parquet, slot-ready markers, status JSON | Slot status OK, all tickers written |
| Signal Discovery V7 | Direct | Critical | `bat\run_eqidv2_signal_discovery_v7_5min_id_persistent.bat`, `\EQIDV2_signal_discovery_v7_5mins_ID` | 09:00-15:30 | 5m indicator parquet, V8 rules, V11 overlay | candidate CSV/JSON/audit | Latest slot, counts, no feed timeout |
| Candidate tickers card | Direct output | Critical monitor | dashboard dynamic card | every refresh | latest candidate CSV/JSON | dashboard projection | Candidate counts and setup mix |
| Entry engine 1min | Direct | Critical | `bat\run_eqidv2_entry_engine_1min_v5_id.bat`, `\EQIDV2_entry_engine_1min_v5_ID` | 09:00-15:35 | candidates, 1m data, setup rules | entry rows, live signal CSVs | `latest_summary.json`, rows/written counts |
| Signal writer | Direct module | Critical | imported by entry engine | per entry slot | entry rows | side signal CSVs | writer stats in entry summary |
| Paper executor | Downstream | Critical for paper | `bat\run_avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.bat`, `\EQIDV2_paper_trade_id_5min_v7_0900` | 09:00-15:40 | signal CSVs, Kite LTP | paper trades, summary, open state | paper summary and executor log |
| Live executor | Downstream | Critical if live orders on | `bat\run_avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.bat`, `\EQIDV2_live_trade_id_5min_v7_0900` | 09:00-15:45 | signal CSVs, KiteConnect | real orders, live trades, summary | live log, broker positions, kill switch |
| V7 live monitor | Monitor | Important | dashboard dynamic formatter | every refresh | entry summary, task state, trade files | dashboard card | Slot flow and warning lines |
| V7 research layer | Research | Advisory | `bat\run_eqidv2_v7_research_layer.bat`, `\EQIDV2_v7_research_layer_0917` | every 15m to 16:00 | candidates, audits, paper trades | truth table, reality gap, suggestions | latest summary/report |
| Daily live V7 research | Monitor/research | Advisory | `bat\run_eqidv2_daily_live_v7_research_session.bat`, `\EQIDV2_daily_live_v7_research_0917` | every 15m to 16:00 | live ops latest snapshot | daily report latest files | latest daily markdown/json |
| V7 pre-momentum analyst | Research | Advisory | `bat\run_eqidv2_v7_pre_momentum_filter_analyst.bat`, `\EQIDV2_v7_pre_momentum_filter_analyst_0917` | every 5m to 15:37, suggestions every 15m | candidates, entry audit, paper trades | shadow suggestions, dynamic profile JSON | latest analyst md/json/csv |
| Suggestions V7 research EOD | Research | Advisory | `bat\run_eqidv2_suggestions_v7_live_research_1605.bat`, `\EQIDV2_suggestions_v7_live_research_1615` | 16:15 | research layer data | EOD suggestions logs/latest | suggestions log |
| V7 NSE ID cost report | Research/EOD | Advisory | `bat\run_v7_nse_id_cost_report.bat`, `\EQIDV2_v7_nse_id_cost_1605` | 16:05 weekday | paper trades CSV | net-cost md/json/csv in `v7_nse_id_cost\latest` | net P&L vs gross, cost bps |
| V7 causality audit | Research/EOD | Advisory | `bat\run_v7_causality_audit.bat`, `\EQIDV2_v7_causality_audit_1605` | 16:05 weekday | live 5m parquet | causality md/json in `v7_causality_audit\latest` | VWAP/day-value/market-ret causal |
| V7 walk-forward gate report | Research/EOD | Advisory | `bat\run_v7_walkforward_gate_report.bat`, `\EQIDV2_v7_walkforward_gate_1620` | 16:20 weekday | resolved V11 trades | gate decision md/json/csv in `v7_walkforward_gate\latest` | PROMOTE/PROBATION/REJECT, FDR (report-only) |
| Data for backtesting | Indirect | Support | `bat\run_moving_files_1545.bat`, `\EQIDV2_data_for_backtesting_1545` | EOD | runtime data | backtesting input files/logs | latest log no errors |
| Backtesting Result v11 | Research | Support | `bat\run_backtesting_result_v11_1600.bat`, `\EQIDV2_backtesting_result_v11_1600` | 16:00 | outputs/daily data | latest backtesting result | dashboard card/report |
| Kite export scheduler | Admin | Support | `bat\run_zerodha_kite_export_scheduler.bat`, `\EQIDV2_kite_export_start_0915` | poll 90s positions, 300s holdings | Kite account | `kite_exports` CSVs | holdings/positions current |
| Dashboard server | Monitor/control | Important | `bat\run_log_dashboard_server.bat` | 08:55-17:00 scheduled public wrapper | logs/runtime files | localhost dashboard | page loads, API snapshot works |
| PF supervisor healthcheck | Ops | Support | `bat\run_pf_supervisor_healthcheck.bat` | scheduled | supervisor/status files | PF health logs | no stale supervisors |
| V16 parallel strategy | Independent | Not direct V7 | V16 runners/tasks | market | 5m data | V16 signals/trades | keep separate from V7 |

## 6. Direct V7 Dependencies

Direct upstream dependencies:

- Kite authentication and access tokens.
- `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live`.
- `C:\TradingData\eqidv2\slot_ready_5m`.
- `logs\eqidv2_eod_scheduler_for_5mins_data_live_minimal.status.json`.
- `C:\TradingData\eqidv2\outputs_ID_v8_5min_research_restore\accepted_rules.csv`.
- `avwap_5min_ID_v2_backtesting.py`.
- `avwap_5min_ID_v6_backtesting.py`.
- `avwap_5min_ID_v7_backtesting.py`.
- `eqidv2_v11_live_overlay.py`.
- `eqidv2_v7_signal_contract.py`.
- `eqidv2_live_signal_writer.py`.

Direct downstream outputs:

- Candidate ticker CSV/JSON files.
- Entry engine audit files.
- Live signal CSVs.
- Paper trade CSV/summary.
- Live trade CSV/summary if real executor is running.
- Dashboard cards and monitor summaries.

## 7. Indirect V7 Dependencies

Indirect but operationally important:

- NIFTY guard files for market context and stale-market protection.
- `configs\universe.csv` and related universe files, through candidate scanner universe loading.
- `kite_exports` for operator account reconciliation.
- `runtime_status` files for dashboard and supervisors.
- `logs` folder under repo root for log dashboard.
- Backtesting result outputs for research recommendations.
- Research truth table outputs used by live ranker memory.

Needs verification:

- The exact universe file used by `v2._load_universe()` on any given day should be verified in `avwap_5min_ID_v2_backtesting.py` and active environment variables.

## 8. Independent Jobs and Sessions

Independent or parallel jobs visible in the dashboard:

- V16 5-minute two-stage strategy:
  - `eqidv2_signal_early_engine_v16_5min.py`
  - `eqidv2_pending_data_fetcher_v16_5min.py`
  - `eqidv2_detection_engine_v16_5min.py`
  - V16 paper/live executors
- V15 new persistent scanner and executors.
- 15-minute live data fetch.
- Legacy V7 sweep jobs.
- Data for backtesting and V11 backtesting results.
- Kite holdings/positions export.
- Dashboard public link and Gmail/public URL helpers.

These should remain separated from V7 ID 5-minute production decisions unless explicitly used as comparison or reporting sources.

## 9. Strategy Logic and Setup Rules

### 9.1 Setup Universe

Confirmed setup exit rules from `setup_exit_rules_v8.csv`:

| Setup | SL % | Target % |
|---|---:|---:|
| A_MOD_BREAK_C1_HIGH | 0.70 | 1.00 |
| A_MOD_BREAK_C1_LOW | 0.70 | 1.50 |
| A_MOD_CLOSE_CONTINUATION_BREAK | 0.70 | 1.50 |
| A_PULLBACK_C2_THEN_BREAK_C2_HIGH | 0.70 | 0.90 |
| A_PULLBACK_C2_THEN_BREAK_C2_LOW | 0.85 | 1.00 |
| B_AVWAP_RECLAIM_REVERSAL | 0.70 | 1.50 |
| B_HUGE_C1_CLOSE_RECLAIM_BREAK | 0.70 | 1.50 |
| B_HUGE_PULLBACK_HOLD_BREAK | 0.70 | 1.10 |
| B_HUGE_RED_FAILED_BOUNCE | 0.70 | 1.50 |
| C_OR_BREAKDOWN | 0.70 | 1.30 |
| C_OR_BREAKOUT | 1.20 | 1.50 |
| D_AVWAP_LOSE_REVERSAL | 1.00 | 1.50 |
| D_EMA20_BOUNCE | 0.70 | 1.50 |
| D_EMA20_REJECTION | 0.75 | 1.30 |
| E_ORB_BREAKOUT_LONG | 0.80 | 1.20 |
| E_ORB_BREAKOUT_SHORT | 0.80 | 1.50 |
| E_VWAP_BAND_FADE | 0.70 | 0.60 |
| E_VWAP_LOSE_EARLY_SHORT | 0.70 | 1.00 |
| G_HIGHER_HIGH_BREAK | 0.90 | 1.50 |
| G_LOWER_LOW_BREAK | 0.85 | 0.90 |
| L_BB_SQUEEZE_LONG | 0.75 | 0.75 |
| L_TREND_PULLBACK | 0.70 | 0.90 |
| MR_CONTROLLED_VWAP_EXTREME_FADE_LONG | 0.70 | 0.80 |
| MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT | 0.70 | 0.80 |
| S_BB_SQUEEZE_SHORT | 1.00 | 1.50 |
| T_TREND_DAY_EMA_STAIR_SHORT | 0.70 | 1.00 |

Additional exit-rule rows exist for early/gap/liquidity/MACD setups. See:

```text
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\latest\setup_exit_rules_v8.csv
avwap_5min_ID_v6_backtesting.py
```

### 9.2 Setup Families

Confirmed setup families:

- `A_`: modified candle-break and pullback continuation setups.
- `B_`: AVWAP reclaim and large-candle reclaim/bounce setups.
- `C_`: opening range breakout/breakdown setups.
- `D_`: AVWAP loss reversal and EMA20 bounce/rejection setups.
- `E_`: early-session ORB, VWAP, gap, first-hour RS, and failed-break trap setups.
- `G_`: higher-high/lower-low break setups.
- `L_`: long-side squeeze/reversal/trend pullback setups.
- `MR_`: mean-reversion controlled VWAP extreme fade setups.
- `S_`: short-side squeeze/liquidity/MACD setups.
- `T_`: trend-day EMA stair short setup.

Needs verification:

- Exact non-early setup boolean expressions come from `avwap_5min_ID_v2_backtesting.py` and are not fully represented in the dashboard.

### 9.3 Early-Mode Rules

Early mode is enabled from 09:30 to 11:00. Confirmed early common filters:

- Minimum 5-minute traded value: Rs 1,000,000.
- Max VWAP distance ATR: 2.80.
- Max candle range: 3.80 ATR.
- Minimum body percentage: 0.42.
- Minimum volume ratio: 1.10.
- Early tight filters enabled.

Confirmed early specific thresholds:

- `E_ORB_BREAKOUT_LONG`: max vol ratio 2.00, min RS% 4.00, max VWAP distance ATR 1.80.
- `E_GAP_HOLD_CONTINUATION_LONG`: min RS% 3.00 and min quality 160.00.
- `E_ORB_BREAKOUT_SHORT`: min RS% -1.50, max ATR% 0.0065, min body 0.82.
- `E_VWAP_LOSE_EARLY_SHORT`: min RS% -1.20, min close location 0.08, max ATR% 0.008.
- Early live gate minimum score: 95.
- Early live gate max per side: 4.
- Early live gate max per slot: 8.

Blocked early setups in runner:

```text
E_RS_FIRST_HOUR_BREAK_LONG
E_RS_FIRST_HOUR_BREAK_SHORT
E_VWAP_RECLAIM_EARLY_LONG
E_FAILED_OR_BREAKOUT_TRAP_SHORT
E_ORB_RETEST_HOLD_SHORT
E_ORB_RETEST_HOLD_LONG
E_FAILED_OR_BREAKDOWN_TRAP_LONG
E_GAP_HOLD_CONTINUATION_LONG
E_GAP_HOLD_CONTINUATION_SHORT
E_OPENING_DRIVE_CONTINUATION_LONG
E_OPENING_DRIVE_CONTINUATION_SHORT
```

### 9.4 V8 Gate

V8 live gate is enabled and points to:

```text
C:\TradingData\eqidv2\outputs_ID_v8_5min_research_restore\accepted_rules.csv
```

The V8 gate evaluates live-safe fields including:

```text
_signal_hour
atr_pct
body_pct
close_loc
day_value_so_far_rs
market_ret_pct
quality_score
rs_pct
signal_close
signal_high
signal_low
signal_open
signal_volume
vol_ratio
vwap_dist_atr
```

V8 output statuses include `PASSED`, `EARLY_PASSED`, `UNCOVERED_FALLBACK_PASSED`, and rejected states with reason fields.

### 9.5 V11 Overlay

V11 overlay profile:

```text
production_core_ab_max_pnl_low_valid_residual_overlay_tier123_balanced
version: v11_backtesting_tier123_balanced_2026_06_03
```

Selected profile includes:

- `C_OR_BREAKOUT`
- `D_EMA20_BOUNCE`
- `E_ORB_BREAKOUT_LONG`
- `E_ORB_BREAKOUT_SHORT`
- `L_BB_SQUEEZE_LONG`
- `E_VWAP_LOSE_EARLY_SHORT`
- `S_BB_SQUEEZE_SHORT`
- `B_AVWAP_RECLAIM_REVERSAL`
- `A_MOD_CLOSE_CONTINUATION_BREAK`
- `A_MOD_BREAK_C1_LOW`
- `A_MOD_BREAK_C1_HIGH`
- `D_EMA20_REJECTION`
- `E_VWAP_BAND_FADE`
- `T_TREND_DAY_EMA_STAIR_SHORT`
- `MR_CONTROLLED_VWAP_EXTREME_FADE_LONG`
- `MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT`

Important confirmed V11 thresholds (rounded per P0-4; values read from `eqidv2_v11_live_overlay.py` on 2026-06-10):

- `A_MOD_BREAK_C1_HIGH`: RS% >= 2.0, ATR% <= 0.006, signal minute <= 670 (11:10 IST).
- `D_EMA20_REJECTION`: body/ranker gate (body >= 0.89 AND ranker >= 0.39) or late residual gate (signal minute 780–825, body >= 0.93, wick skew <= -0.065).
- `E_ORB_BREAKOUT_LONG`: notional >= 100000.
- `E_ORB_BREAKOUT_SHORT`: market return >= -0.63, quality >= 97.9, upper wick <= 0.015.
- `L_BB_SQUEEZE_LONG`: market abs return <= 0.74 OR vol ratio <= 3.0, and ranker score >= 0.73.
- `MR_CONTROLLED_VWAP_EXTREME_FADE_SHORT`: vol ratio <= 1.70.
- `T_TREND_DAY_EMA_STAIR_SHORT`: signal minute 780 to 840, market return <= -0.39.

### 9.6 Research Live Filters

Research filters are active in discovery. Confirmed filters:

- Short-focus enabled: only side `SHORT`, except exempt setups.
- Exempt setup: `A_MOD_BREAK_C1_HIGH`.
- Long anti-chase: reject/shadow when close location > 0.97 and VWAP distance ATR > 3.50.
- `B_AVWAP_RECLAIM_REVERSAL` ranker minimum: 0.65.
- `L_TREND_PULLBACK` probation block.

Uncovered fallback:

- Enabled.
- Window: 11:05 to 13:55 in runner.
- Min ranker: 0.65.
- Min quality: 125.
- Max per slot: 1.
- Allowed side: SHORT.
- Allowed setups: `A_MOD_BREAK_C1_LOW`, `C_OR_BREAKDOWN`, `A_PULLBACK_C2_THEN_BREAK_C2_LOW`, `B_HUGE_RED_FAILED_BOUNCE`, `D_AVWAP_LOSE_REVERSAL`, `G_LOWER_LOW_BREAK`.

### 9.7 Pre-Entry Momentum Gates

Pre-entry momentum gates run inside the 1-minute entry engine after candidate-to-entry-row construction and before live signal CSV writing.

Enabled in runner:

```text
EQIDV2_ENTRY_ENGINE_PRE_MOMENTUM_GATES=1
EQIDV2_ENTRY_ENGINE_PRE_MOMENTUM_MISSING_ACTION=block
```

Current gate version:

```text
v7_pre_entry_momentum_2026_06_04_t_probation
```

Confirmed setup gates:

| Setup | Gate |
|---|---|
| B_AVWAP_RECLAIM_REVERSAL | `pre_entry_momentum_score <= 64.7678` |
| C_OR_BREAKOUT | `sig5_adx_calc >= 16.2111` and `pre2_mom_r >= -0.187227` |
| D_EMA20_BOUNCE | `pre3_range_r >= 0.292349` and `pre_entry_momentum_score <= 78.3448` |
| D_EMA20_REJECTION | `pre10_mom_r <= 0.156614`, `pre5_mom_r >= 0.12493`, `sig5_adx_calc >= 20.0` |
| E_ORB_BREAKOUT_LONG | `pre15_vol_ratio20 <= 1.08301`, `pre1_adx >= 42.3138` |
| E_ORB_BREAKOUT_SHORT | `pre10_dir_count >= 5.0`, `pre5_vol_ratio20 >= 1.65561` |
| E_VWAP_LOSE_EARLY_SHORT | `sig5_vol_ratio20 >= 1.5643`, `pre3_body_sum_r <= 0.797498` |
| G_HIGHER_HIGH_BREAK | `pre3_close_pos <= 0.985417`, `sig5_rsi_dir <= 67.878` |
| L_TREND_PULLBACK | `pre_entry_momentum_score >= 73.021`, `pre2_mom_r >= 0.233909` |
| T_TREND_DAY_EMA_STAIR_SHORT | `pre3_close_pos >= 0.662492`, `pre5_dir_count <= 3.0`, `pre1_adx <= 31.0`, `pre5_range_r >= 0.35` |

Shadow-blocked setup:

- `C_OR_BREAKDOWN`

Current special filters:

- `E_VWAP_LOSE_EARLY_SHORT` is blocked before 09:45 in current code. Needs sign-off because the 2026-06-09 review requested earliest entry 09:40.
- `A_MOD_BREAK_C1_HIGH` is capped to top 2 per slot by descending `vwap_dist_atr`.
- `A_MOD_BREAK_C1_HIGH` is blocked after 11:10 signal time.

### 9.8 Ranking and Scoring

Discovery assigns heuristic live ranker scores using:

- Quality score
- Signed RS score
- Volume score
- ATR score
- Close location score
- VWAP extension score
- Market score
- Setup memory from research truth tables

Confirmed ranker weights in discovery:

```text
quality: 0.40
rs_score: 0.16
vol_score: 0.14
atr_score: 0.10
close_score: 0.14
vwap_score: 0.12
market_score: 0.04
setup_score: 0.06
```

Note: weights sum over 1.0 in code terms. Treat the score as a heuristic ranker, not a calibrated probability.

### 9.9 Duplicate Prevention and Limits

Confirmed duplicate prevention:

- Candidate append skips duplicate `candidate_id`.
- Signal writer rejects duplicate signal keys.
- Signal writer rejects duplicate `signal_id`.
- Signal writer rejects repeated ticker for the same trading day across both LONG and SHORT signal CSVs.
- Paper/live executors track executed signal IDs.
- Paper/live executors keep open trade state for crash recovery.

Confirmed paper risk controls:

- Max open positions 100 in paper runner.
- Max capital Rs 2,000,000 in paper runner.
- Daily loss brake Rs 10,000 in paper runner.
- `C_OR_BREAKOUT` session cap 50 in paper runner.
- `C_OR_BREAKOUT` time stop 30 minutes in paper runner.
- Forced close 15:20.
- Exit slippage 5 bps on SL/time-stop/EOD-close outcomes (TARGET fills at exact limit price). `EQIDV2_PAPER_EXIT_SLIPPAGE_BPS=5.0`.
- Gross SHORT notional cap: Rs 1,500,000 (`EQIDV2_MAX_GROSS_SHORT_NOTIONAL_RS=1500000`). Rejects new SHORT entries if total open SHORT notional would exceed this.

Confirmed live risk controls:

- Max open positions 20 in live runner.
- Max concurrent trades 20 in live runner.
- Default max capital in live code: Rs 500,000 unless env overrides.
- Forced close 15:20.
- Order fill timeout default: 90 seconds.
- Entry retry attempts default: 2.
- Exit leg retry attempts default: 6.
- Kill switch file: `kill_switch_false_id_5min_v7.json`.
- Auto kill-switch: daily loss > Rs 10,000 OR single trade loss > Rs 5,000 trips `dispatch_lockdown` in-memory immediately and writes kill-switch JSON. Env overrides: `EQIDV2_LIVE_DAILY_LOSS_LIMIT_RS`, `EQIDV2_LIVE_PER_TRADE_LOSS_LIMIT_RS`, `EQIDV2_LIVE_KILL_SWITCH_AUTO=1`.
- Gross SHORT notional cap: Rs 1,500,000 (`EQIDV2_MAX_GROSS_SHORT_NOTIONAL_RS=1500000`), same env var as paper.

## 10. Parameters, Values, and Thresholds

### 10.1 Core Timing

| Parameter | Value | Source |
|---|---:|---|
| 5m fetch market window | 09:15-15:35 | fetcher code |
| First completed 5m close | 09:20 | fetcher code |
| Signal discovery entry window | 09:30-14:30 | discovery runner |
| Signal discovery post-slot delay | 75 sec | discovery runner |
| Signal discovery entry lag | 1 min | discovery runner |
| Entry engine delay | 60 sec | entry runner |
| Entry engine candidate wait | 30 sec | entry runner |
| Entry engine max handoff lag | 30 sec | entry runner/contract |
| Signal writer max lag | 30 sec | signal contract |
| Paper forced close | 15:20 | paper executor |
| Live forced close | 15:20 | live executor |
| Discovery cutoff | 15:30 | discovery runner |
| Entry engine cutoff | 15:35 | entry runner |
| Paper executor cutoff | 15:40 | paper runner |
| Live executor cutoff | 15:45 | live runner |

### 10.2 Fetch and Freshness

| Parameter | Value | Source |
|---|---:|---|
| 5m max workers | 384 | fetch runner |
| 5m max workers per app | 48 | fetch runner |
| 5m buffer | 2 sec | fetch runner |
| 5m quarter-hour buffer | 2 sec | fetch runner |
| 5m Kite timeout | 8 sec | fetch runner |
| 5m partition timeout | 150 sec | fetch runner |
| 5m SLA warn | 50 sec | fetch runner |
| Ready marker sample | 24 default | fetch code |
| Ready marker min fresh ratio | 0.70 default | fetch code |
| Signal discovery feed verification tolerance | 5 | discovery runner/dashboard |
| Signal discovery feed max wait | 90 sec default | discovery code |

### 10.3 Discovery, Gates, and Ranking

| Parameter | Value |
|---|---:|
| Discovery scan workers | 24 |
| Tier123 scan workers | 24 |
| Tier123 latest start lag | 40 sec |
| Early min traded value | Rs 1,000,000 |
| Early max VWAP distance ATR | 2.80 |
| Early max candle range ATR | 3.80 |
| Early min body pct | 0.42 |
| Early min volume ratio | 1.10 |
| Early gate min score | 95 |
| Early max per side | 4 |
| Early max per slot | 8 |
| Uncovered fallback start/end | 11:05-13:55 |
| Uncovered fallback min ranker | 0.65 |
| Uncovered fallback min quality | 125 |
| Uncovered fallback max per slot | 1 |
| B_AVWAP reclaim ranker min | 0.65 |
| Long anti-chase close loc | > 0.97 |
| Long anti-chase VWAP dist ATR | > 3.50 |

### 10.4 Entry and Execution

| Parameter | Paper | Live |
|---|---:|---:|
| Fallback short stop | 0.75% | 0.75% |
| Fallback long stop | 0.75% | 0.75% |
| Fallback short target | 1.00% | 1.00% |
| Fallback long target | 1.00% | 1.00% |
| Max concurrent trades | 100 | 20 |
| Max open positions | 100 | 20 |
| Max capital deployed | Rs 2,000,000 | code default Rs 500,000 unless env override |
| Entry slip cap | 0.30% | 0.30% |
| Near-entry retry band | 0.30% | 0.30% |
| Near-entry retry wait | 300 sec paper | 30 sec live default |
| LTP poll interval | 5 sec trade loop | broker/order dependent |
| Forced close | 15:20 | 15:20 |

## 11. Paths, Folders, and Files Used

### 11.1 Workspace

| Path | Type | Purpose | Created/read by | Risk if stale/missing |
|---|---|---|---|---|
| `C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2` | workspace | scripts, runners, docs, logs | all batch runners | production scripts fail |
| `logs` | log dir | dashboard log source | runners/dashboard | dashboard blind or stale |
| `bat` | runner dir | scheduled-task batch files | Windows Task Scheduler | tasks fail |
| `configs` | config dir | universe/config files | scanners/backtests | universe mismatch |
| `v7_research_layer` | code dir | V7 research/advisory scripts | research jobs | no suggestions/reports |

### 11.2 Runtime Root

| Path | Type | Purpose | Producer | Consumer | Freshness |
|---|---|---|---|---|---|
| `C:\TradingData\eqidv2` | runtime root | off-OneDrive runtime | all jobs | all jobs | constant |
| `C:\TradingData\eqidv2\runtime_status` | state | status and heartbeat | runners/scripts | dashboard/supervisors | seconds-minutes during market |
| `C:\TradingData\eqidv2\live_signals` | live output | signal/trade CSVs and state | entry/executors | executors/dashboard/research | immediate |
| `C:\TradingData\eqidv2\stocks_indicators_5min_eq_live` | data | live 5m indicator parquet | 5m fetcher | scanner | current slot |
| `C:\TradingData\eqidv2\stocks_indicators_1min_eq` | data | 1m indicator data | backtesting/data fetch | entry/research | current day/history |
| `C:\TradingData\eqidv2\stocks_raw_1min_entry_v5_id_live` | cache/audit | raw 1m fetched for entries | entry engine | entry/debug | current slot |
| `C:\TradingData\eqidv2\slot_ready_5m` | marker | 5m slot completion | 5m fetcher | scanner/dashboard | current slot |
| `C:\TradingData\eqidv2\nifty_slot_ready_5m` | marker | NIFTY 5m guard ready | NIFTY guard | scanners/monitors | current slot |
| `C:\TradingData\eqidv2\nifty_slot_fail_5m` | marker | NIFTY slot fail neutralization | NIFTY guard | detection/monitoring | current slot |
| `C:\TradingData\eqidv2\nifty_open_slot_5m` | marker | opening-slot handling | NIFTY guard | detection/monitoring | 09:15 slot |

### 11.3 V7 Runtime Paths

| Path | Purpose |
|---|---|
| `C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\csv` | daily raw/gated/rejected candidate CSVs |
| `C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\json` | per-slot candidate JSON snapshots |
| `C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\latest` | dashboard latest candidate snapshots |
| `C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\audit` | slot-level candidate audit CSV |
| `C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\heartbeat` | JSON status/heartbeat for candidate session |
| `C:\TradingData\eqidv2\entry_engine_1min_v5_ID\latest` | latest entry rows and summary |
| `C:\TradingData\eqidv2\entry_engine_1min_v5_ID\audit` | per-slot entry/reject audit files |
| `C:\TradingData\eqidv2\entry_engine_1min_v5_ID\slot_raw_1min` | per-slot raw 1m parquet files |
| `C:\TradingData\eqidv2\entry_engine_1min_v5_ID\heartbeat` | JSON entry status/heartbeat |

### 11.4 Research Runtime Paths

| Path | Purpose |
|---|---|
| `C:\TradingData\eqidv2\live_research_v7_research_layer\truth_table` | daily truth tables |
| `C:\TradingData\eqidv2\live_research_v7_research_layer\reports` | reality-gap reports |
| `C:\TradingData\eqidv2\live_research_v7_research_layer\latest` | latest ranker, exit lab, suggestions, deep analysis |
| `C:\TradingData\eqidv2\daily_live_v7_research_session\latest` | latest daily live research report and copied live-ops CSVs |
| `C:\TradingData\eqidv2\v7_pre_momentum_filter_analyst\latest` | latest pre-momentum markdown/json/csv |
| `C:\TradingData\eqidv2\dynamic_pre_momentum\latest` | dynamic pre-momentum profile JSON |

## 12. Input/Output File Matrix

| File/path | Kind | Producer | Consumer | Update frequency | Validate |
|---|---|---|---|---|---|
| `stocks_indicators_5min_eq_live\<TICKER>_stocks_indicators_5min.parquet` | data | 5m fetcher | discovery | every 5m | latest timestamp equals slot |
| `slot_ready_5m\slot_<slot>.json` | marker | 5m fetcher | discovery/dashboard | every 5m | `complete=true`, failures=0 |
| `eqidv2_eod_scheduler_for_5mins_data_live_minimal.status.json` | status | 5m fetcher | discovery/dashboard | every slot | `overall_state=OK` |
| `candidate_tickers_<date>.csv` | candidate output | discovery | entry/research | append per slot | row counts and candidate IDs |
| `latest_candidate_tickers.json` | candidate latest | discovery | entry/dashboard | every scan | slot, total candidates |
| `entry_rows_<slot>.csv` | entry audit | entry engine | dashboard/research | every entry slot | non-empty when candidates accepted |
| `entry_rejected_*_<slot>.csv` | reject audit | entry engine | research/debug | every entry slot | reason counts |
| `latest_summary.json` | entry monitor | entry engine | dashboard/daily research | every entry slot | slot and written counts |
| `signals_<date>_id_5min_v7_short.csv` | executable signal | signal writer | paper/live executors | when entries pass | signal IDs, lag <= 30s |
| `signals_<date>_id_5min_v7_long.csv` | executable signal | signal writer | paper/live executors | when entries pass | exists on long-entry days |
| `paper_trades_<date>_id_5min_v7.csv` | paper result | paper executor | dashboard/research | on exit/skip | P&L/outcome columns |
| `paper_trade_summary_id_5min_v7.json` | paper summary | paper executor | dashboard/research | on trade update | totals reconcile to CSV |
| `open_trades_state_<date>_id_5min_v7.json` | crash state | paper executor | paper executor/dashboard | live while open | empty after close |
| `live_trades_<date>_id_5min_v7.csv` | live result | live executor | dashboard/research | on live events | reconcile with broker |
| `open_live_trades_state_<date>_id_5min_v7.json` | live crash state | live executor | live executor/dashboard | live while open | reconcile with broker |
| `latest_daily_live_v7_research.md` | monitor report | daily live research | dashboard/operator | 15m | report timestamp |
| `latest_multi_window_suggestions.md` | research suggestion | research layer | dashboard/operator | 15m/EOD | suggestion levels |

## 13. Dependency Map

### 13.1 Main Flow

```text
Kite auth/session files
-> 5-minute live fetcher
-> stocks_indicators_5min_eq_live parquet
-> slot_ready_5m marker/status JSON
-> Signal Discovery V7 5mins ID
-> raw/gated/v11/rejected candidate CSV/JSON
-> Entry Engine 1min V7 ID
-> raw 1-minute fetch for selected tickers
-> entry rows and reject audit
-> neutral live signal writer
-> live_signals/signals_<date>_id_5min_v7_<side>.csv
-> paper executor and/or live executor
-> paper_trades/live_trades CSV and summaries
-> dashboard + research layer
```

### 13.2 Monitoring Flow

```text
Runtime status + heartbeat files
-> log_dashboard_server.py
-> /api/snapshot
-> dashboard cards
```

### 13.3 Research Flow

```text
candidate CSVs + entry audits + signal CSVs + paper trades
-> v7 research layer
-> truth table + reality gap + exit lab + suggestions
-> daily live research/session latest reports
-> dashboard Research section
```

### 13.4 Jobs That Must Run Before V7

- Authentication V2.
- 5-minute live fetch.
- NIFTY guard if market context is required.
- Preopen healthcheck is strongly recommended, not a data dependency.

### 13.5 Jobs That Must Run After V7

- Paper executor or live executor must run after signal CSVs exist, but can start before them and watch files.
- Research layer and daily live research can run after partial outputs exist.
- Backtesting and data movement run after market close.

### 13.6 Jobs That Can Run Alone

- Dashboard server can run alone, but cards will be stale/missing.
- Research jobs can run alone against historical runtime files.
- Backtesting result can run alone after data is available.
- Kite export can run alone for account monitoring.

## 14. Monitoring and Health Checks

### 14.1 Pre-Market Validation

Before 09:15 IST:

1. Confirm authentication task completed and token files are fresh.
2. Open dashboard at `http://127.0.0.1:8787`.
3. Confirm Market section shows 5-minute fetch and NIFTY guard scheduled/running.
4. Confirm V7 tasks are scheduled:
   - `\EQIDV2_signal_discovery_v7_5mins_ID`
   - `\EQIDV2_entry_engine_1min_v5_ID`
   - `\EQIDV2_paper_trade_id_5min_v7_0900`
   - `\EQIDV2_live_trade_id_5min_v7_0900` if real orders intended.
5. Confirm `C:\TradingData\eqidv2\runtime_status` is writable.
6. Confirm `C:\TradingData\eqidv2\live_signals` is writable.
7. Confirm no stale open trade state from prior day.
8. Confirm `accepted_rules.csv` exists for V8 gate.
9. Confirm `setup_exit_rules_v8.csv` exists after entry engine first runs or from previous day.
10. Confirm dashboard does not show stale heartbeat ages for live sessions.

### 14.2 During Market

At each slot:

- Fetch should publish slot-ready marker within expected latency.
- Discovery should scan after feed completion.
- Candidate latest JSON slot should match the expected completed slot.
- Entry engine should wake at T+1 and write entry rows or explicit reject audits.
- Signal CSV write should occur within 30 seconds of intended entry.
- Paper executor should dispatch new signals and update state.

Key thresholds:

- 5m fetch warning: > 50 seconds from runner config.
- Scanner publish delay: warn around 55 seconds, alert around 70 seconds based on 2026-06-09 review.
- Signal handoff lag: hard reject > 30 seconds.
- No-progress open trade: flag if open > 60 minutes without +0.20R.
- Feature gaps: flag if > 50% accepted rows have blank pre-momentum fields.
- Candidate-to-entry gap: alert if `candidate_count > 0` and `entry_rows = 0` repeatedly.

### 14.3 Post-Market

After 15:30:

- Confirm paper open state is empty or contains only reconciled records.
- Confirm live open state is empty if real executor was running.
- Confirm paper summary matches paper trade CSV.
- Confirm `STOPPED_AFTER_CUTOFF` or clean stopped state for discovery and entry engine.
- Run/review daily live V7 research and V7 research layer.
- Review setup-level P&L, no-entry rows, rejected candidates, freshness rejects, and pre-momentum rejects.

## 15. Logs and Debugging Guide

### 15.1 Primary Logs

| Log | Use |
|---|---|
| `logs\eqidv2_eod_scheduler_for_5mins_data_live_minimal.log` | 5m fetch failures, slot SLA, partition stats |
| `logs\eqidv2_signal_discovery_v7_5min_id_persistent_<date>_<hms>.log` | discovery slots, candidate counts, feed gate state |
| `logs\eqidv2_entry_engine_1min_v5_id_<date>.log` | entry row building, raw fetch, writer stats |
| `logs\avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7_<date>.log` | paper dispatch, skips, exits, P&L |
| `logs\avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7_<date>.log` | live orders, rejects, fills, kill-switch actions |
| `logs\daily_live_v7_research_<date>.log` | live research report generation |
| `logs\eqidv2_v7_research_layer_<date>.log` | truth table/reality gap/suggestion generation |
| `logs\v7_pre_momentum_filter_analyst_<date>.log` | shadow pre-momentum suggestions |
| `logs\log_dashboard_server.log` | dashboard server/API errors |

### 15.2 Debugging Common Failures

#### Candidate count is zero

Check:

1. 5m slot marker exists for the expected slot.
2. `stocks_indicators_5min_eq_live` files have current timestamps.
3. Discovery log did not report feed timeout.
4. V8 gate did not reject all rows.
5. Research filters did not remove all rows.
6. V11 overlay did not produce zero selected rows.

Files:

```text
logs\eqidv2_signal_discovery_v7_5min_id_persistent_<date>_<hms>.log
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\latest\latest_raw_candidate_tickers.json
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\latest\latest_research_filter_rejected_candidate_tickers.json
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\latest\latest_v11_overlay_rejected_candidate_tickers.json
```

#### Candidates exist but entry rows are zero

Check:

1. Entry latest summary `candidate_count`.
2. `entry_rejected_candidates_<slot>.csv` for `missing_1min_fetch`, `missing_1min_entry_bar`, or `missing_v8_setup_exit_rule`.
3. `entry_rejected_v11_overlay_<slot>.csv`.
4. `entry_rejected_pre_momentum_<slot>.csv`.
5. `entry_rejected_freshness_<slot>.csv`.
6. `entry_rejected_adv_cap_<slot>.csv` — if non-empty, position sizes are too large relative to ADV. Check `EQIDV2_ADV_PARTICIPATION_PCT` (default 1%) and whether daily parquet files are present in `EQIDV2_ADV_DAILY_PARQUET_DIR`. If parquet is missing, ADV check is skipped (fail-open) so no rejects will appear even though oversized.
7. `entry_rejected_fno_ban_<slot>.csv` — if non-empty, SHORT entries were blocked because ticker is on NSE F&O ban list for the day. Network failure causes fail-open (no rejects). If unexpectedly many rejects, fetch `fo_secban_today.csv` from `EQIDV2_FNO_BAN_LOCAL_FILE` and verify contents.
8. Whether special setup filters removed rows, especially `E_VWAP_LOSE_EARLY_SHORT` before 09:45 and `A_MOD_BREAK_C1_HIGH` after 11:10.

Files:

```text
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\latest\latest_summary.json
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\audit\entry_rejected_*_<slot>.csv
```

#### Entry rows exist but signal CSV does not update

Check:

1. Signal writer result fields in entry summary.
2. `skipped_stale_entry`, `skipped_future_entry`, `skipped_entry_window`, `skipped_intraday_ticker`, duplicate counts.
3. `.lock` files are not stuck.
4. `live_signals` directory is writable.

#### Signal CSV updated but paper executor did not trade

Check:

1. Paper executor log for watchdog start and signal CSV sources.
2. `executed_signals_paper_id_5min_v7.json` for duplicate state.
3. Entry window gate.
4. Research paper gate.
5. Daily loss brake.
6. Capacity reservation.
7. `C_OR_BREAKOUT` cap.
8. LTP/Kite availability if `ltp_on_signal`.

#### Live executor order problem

Check:

1. Live log for entry order ID, order status, target/SL order IDs.
2. Broker positions in Kite export and Zerodha UI.
3. `open_live_trades_state_<date>_id_5min_v7.json`.
4. `live_trade_summary_id_5min_v7.json`.
5. Kill switch command file.
6. NTP drift warning in supervisor.

Manual broker reconciliation is required if live executor reports orphaned broker positions or stale tracked positions.

## 16. Failure Points and Risks

### 16.1 Structural Issues

- `C_OR_BREAKOUT` no-entry-row issue was observed on 2026-06-09. Discovery accepted candidates, but entry rows stayed zero.
- LONG flow is currently fragile. On 2026-06-09, no long signal CSV was present and all paper trades were short.
- The code has current production changes for `D_EMA20_REJECTION`, `E_VWAP_LOSE_EARLY_SHORT`, and `A_MOD_BREAK_C1_HIGH`. Some differ from the review wording and need explicit operator confirmation.
- Candidate and audit CSV schema-backup files were created repeatedly on 2026-06-09. This indicates schema changes or mismatched append headers and can confuse downstream readers.

### 16.2 Timing Risks

- Fetcher, discovery, and entry engine are all trying to satisfy a T+1 plus 30-second signal handoff contract.
- 5-minute fetch can consume 30-36 seconds on clean slots.
- Discovery Tier123 scan can consume around 25 seconds.
- Any feed delay can push discovery and entry writer beyond the max-lag threshold.
- Signal writer correctly rejects stale rows, but this can produce silent no-trade slots unless dashboard highlights the reject reason.

### 16.3 File Risks

- Replay script writes to production paths. It is blocked during market hours unless `EQIDV2_REPLAY_FORCE_MARKET_HOURS=1`; overriding this can corrupt live dashboard/candidate state.
- `.lock` files in `live_signals` are required for safe signal CSV appends. A stuck lock would block writes.
- OneDrive workspace logs can be locked by sync/Defender. Runtime status is intentionally off-OneDrive under `C:\TradingData\eqidv2\runtime_status`.
- Partial CSV writes are mitigated in many places but still possible if external sync/AV locks a file.

### 16.4 Monitoring Gaps

The live monitor should add or improve:

- `no_entry_row` count per slot and per setup.
- Candidate-to-entry funnel: raw, V8 passed, V11 selected, research-filter passed, entry rows, signal CSV written.
- Path quality live feed for open trades: best R, worst R, giveback R, time-to-0.25R.
- Pre-momentum feature blank-rate alert.
- SL hold-time alert.
- Rolling 3/7/20-session setup PF next to daily PF.
- Capital utilization minutes by open trade.
- C_OR_BREAKOUT gated-but-no-entry dedicated warning.

### 16.5 Backtest vs Live Mismatch Risks

- Live executor fills with LTP, while some audit prices use signal close or forming T+1 open reference.
- Stop/target prices are rebased around actual fill in executors.
- V7 live adapter now recomputes a causal session VWAP in `v2._prepare_5m` (P1-6) and keeps the original parquet value only as `VWAP_source`. The `v7_causality_audit.py` report (task `EQIDV2_v7_causality_audit_1605`) verifies VWAP, `day_value_so_far_rs`, and `market_ret_pct` are causal at signal time. Backtests run before 2026-06-10 used the parquet VWAP column directly and are not VWAP-parity-comparable with current live behavior.
- Current short-focus filter can suppress many LONG opportunities unless exempted or disabled.
- Research suggestions are advisory unless separately promoted into code/env.

## 17. How to Validate the System Before Market Open

Use this checklist before 09:15 IST:

- [ ] Authentication completed and Kite token files are valid.
- [ ] Dashboard is reachable at `http://127.0.0.1:8787`.
- [ ] Market Data Readiness section shows NIFTY guard and 5m fetch scheduled.
- [ ] V7 Flow section shows discovery, entry engine, paper executor, and live executor cards.
- [ ] `C:\TradingData\eqidv2\runtime_status` is writable.
- [ ] `C:\TradingData\eqidv2\live_signals` is writable.
- [ ] No stale `open_trades_state_<prior_date>_id_5min_v7.json` affects today.
- [ ] No stale `open_live_trades_state_<prior_date>_id_5min_v7.json` affects today.
- [ ] `accepted_rules.csv` exists for V8 gate.
- [ ] `EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS` setting is intentional.
- [ ] `E_VWAP_LOSE_EARLY_SHORT` earliest slot is intentional: current code blocks before 09:45.
- [ ] If real orders are not intended, confirm live executor is disabled or harmless.
- [ ] Confirm kill switch files are absent or inactive unless deliberately set.
- [ ] Confirm backtesting/replay scripts are not running during market.

## 18. How to Debug During Live Market

### Step 1: Verify Feed

Open:

```text
logs\eqidv2_eod_scheduler_for_5mins_data_live_minimal.status.json
C:\TradingData\eqidv2\slot_ready_5m\slot_<slot>.json
```

Healthy:

```text
overall_state=OK
complete=true
tickers_written close to tickers_expected
verification_failed_count <= 5
```

### Step 2: Verify Discovery

Open:

```text
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\latest\latest_candidate_tickers.json
C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\audit\candidate_tickers_audit_<date>.csv
```

Check:

- `slot_ist`.
- `total_candidates`.
- `long_candidates` and `short_candidates`.
- `v11_live_overlay_selected_total`.
- `v8_live_gate_rejected`.
- `research_live_filter_rejected`.

### Step 3: Verify Entry Engine

Open:

```text
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\latest\latest_summary.json
C:\TradingData\eqidv2\entry_engine_1min_v5_ID\audit\entry_rows_<slot>.csv
```

Check:

- `candidate_snapshot_ready`.
- `candidate_count`.
- `raw_entry_rows`.
- `v11_filtered_entry_rows`.
- `pre_momentum_filtered_entry_rows`.
- `entry_rows`.
- `short_written` and `long_written`.
- reject CSV paths.

### Step 4: Verify Signal CSVs

Open:

```text
C:\TradingData\eqidv2\live_signals\signals_<date>_id_5min_v7_short.csv
C:\TradingData\eqidv2\live_signals\signals_<date>_id_5min_v7_long.csv
```

Check:

- New `signal_id`.
- `signal_bar_time_ist`.
- `intended_entry_ist`.
- `detected_time_ist`.
- `detection_lag_sec <= 30`.
- `writer_name=entry_engine_1min_v5_id`.

### Step 5: Verify Executor

Paper:

```text
C:\TradingData\eqidv2\live_signals\paper_trade_summary_id_5min_v7.json
C:\TradingData\eqidv2\live_signals\paper_trades_<date>_id_5min_v7.csv
logs\avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7_<date>.log
```

Live:

```text
C:\TradingData\eqidv2\live_signals\live_trade_summary_id_5min_v7.json
C:\TradingData\eqidv2\live_signals\live_trades_<date>_id_5min_v7.csv
logs\avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7_<date>.log
```

## 19. Recommended Improvements

### Completed as of 2026-06-10

| Item | Description | Status |
|------|-------------|--------|
| P0-1 | Net cost model — `nse_intraday_costs.py` wired into paper executor; net P&L columns + correct STT leg | Done |
| P0-2 | Live daily-loss + per-trade kill switch in `avwap_trade_execution_PAPER_TRADE_FALSE` | Done |
| P0-3 | Walk-forward gate (`walkforward_gate.py`) + report (`v7_walkforward_gate_report.py`) | Partial — report-only; does not yet author `accepted_rules.csv` |
| P0-4 | Threshold rounding — V11 overlay cuts rounded to ≤2 decimals | Done |
| P1-6 | Causal VWAP recompute in `v2._prepare_5m` + `v7_causality_audit.py` audit | Done |
| P1-7 | Risk-based sizing, gross-short notional cap, NIFTY regime gate | Done |
| P1-9 | Benjamini-Hochberg FDR significance gate in `walkforward_gate.py` | Done |
| P2-10 | Exit slippage (5 bps) in paper executor | Done |
| P2-11 | ADV participation cap in entry engine | Done |
| P2-12 | F&O ban pre-trade filter in entry engine | Done |
| P2-13 | Replay ABORT — refuses `--replay-slots` without `EQIDV2_REPLAY_OUTPUT_ROOT` | Done |
| P2-14 | Schema versioning — `CANDIDATE_SCHEMA_VERSION` stamped in every candidate CSV row | Done |
| P2-15 | Funnel card — ADV cap + F&O ban reject counts in dashboard setup table | Done |
| #7 | Replay outputs isolated to separate namespace via `EQIDV2_REPLAY_OUTPUT_ROOT` | Done |
| #8 | Schema versioning via `CANDIDATE_SCHEMA_VERSION` constant | Done |

### Priority — Act Now

1. **Disable `T_TREND_DAY_EMA_STAIR_SHORT`**: 21-day paper shows 12% win rate, PF=0.35, 79 trades, Rs -21,464. The setup consistently loses under current NIFTY conditions. Disable via `EQIDV2_BLOCKED_SETUPS=T_TREND_DAY_EMA_STAIR_SHORT` env var (or remove from scanner config) until a regime filter is built. This single change removes the pipeline's worst drag.

2. **Gate `C_OR_BREAKOUT` LONG to high-NIFTY-momentum days only**: 21-day paper shows PF=0.67, 61 trades, Rs -19,127. LONG ORBs in the current sideways/bearish NIFTY environment are low-probability. Add a NIFTY daily trend check (e.g., price > 20-day SMA) as an entry gate for LONG `C_OR_BREAKOUT` until regime conditions improve.

3. **Enable live executor after 5 clean paper trading days post-punchlist**: Only 9 post-punchlist trades observed (2026-06-09). Need 5 full trading days with ≥10 trades/day, no daily-loss-brake trips, and positive cumulative PF before switching from paper to live. Current paper process PID is safe to leave running.

### Near-term

4. Add `no_entry_row_count` to the live monitor per slot and per setup.
5. Add `C_OR_BREAKOUT` no-entry-row alert before the 14:20-14:35 window.
6. Add blank pre-momentum feature count and accepted-row percentage to the dashboard.
7. Add live path-quality fields for open trades: best R, worst R, time to +0.25R, time to +0.5R, giveback.
8. Add rolling setup PF to the V7 Flow section.
9. Confirm and align `E_VWAP_LOSE_EARLY_SHORT` min time: code currently 09:45, review requested 09:40.
10. Add an explicit LONG-flow health check: if raw/gated LONG candidates exist but no LONG signal CSV rows are written, show a warning.
11. Add a monitor line for stale `.lock` files in `live_signals`.
12. Add broker reconciliation status to the live executor card.

### Deferred

13. P1-8: Point-in-time universe check — validate that tickers in candidates were actually listed/tradeable on signal_bar date. Large effort; not blocking live deployment.

## 20. Appendix: Raw Job/Session Notes

### 20.1 Core V7 Runners

```text
bat\run_eqidv2_signal_discovery_v7_5min_id_persistent.bat
bat\run_eqidv2_entry_engine_1min_v5_id.bat
bat\run_avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.bat
bat\run_avwap_trade_execution_PAPER_TRADE_FALSE_id_5min_v7.bat
bat\schedule_eqidv2_id_5min_v7_live_weekday.bat
```

### 20.2 Market Data Runners

```text
bat\run_eqidv2_eod_scheduler_for_5mins_data_live_minimal.bat
bat\run_eqidv2_nifty_guard_fetcher_supervised_v16_5min.bat
bat\run_authentication_v2.bat
```

### 20.3 Dashboard Runners

```text
bat\run_log_dashboard_server.bat
bat\run_log_dashboard_public_link.bat
bat\run_log_dashboard_public_link_scheduled.bat
bat\run_log_dashboard_stop.bat
bat\schedule_log_dashboard_weekday.bat
```

The dashboard runner contains auth configuration. Do not copy secrets into documentation or tickets.

### 20.4 Research Runners

```text
bat\run_eqidv2_v7_research_layer.bat
bat\run_eqidv2_daily_live_v7_research_session.bat
bat\run_eqidv2_v7_pre_momentum_filter_analyst.bat
bat\run_eqidv2_suggestions_v7_live_research_1605.bat
bat\run_v7_nse_id_cost_report.bat
bat\run_v7_causality_audit.bat
bat\run_v7_walkforward_gate_report.bat
```

EOD report scheduled tasks (created/hardened once by the helper bats below):

```text
\EQIDV2_v7_nse_id_cost_1605        16:05 weekday  -> v7_nse_id_cost_report.py
\EQIDV2_v7_causality_audit_1605    16:05 weekday  -> v7_causality_audit.py
\EQIDV2_v7_walkforward_gate_1620   16:20 weekday  -> v7_walkforward_gate_report.py
```

Scheduling / maintenance helper bats (run once, as Administrator):

```text
bat\schedule_v7_nse_id_cost_weekday.bat
bat\schedule_v7_walkforward_gate_weekday.bat
bat\create_task_v7_causality_audit_1605.bat
bat\fix_task_trigger_backtesting_1545.bat   (corrects \EQIDV2_data_for_backtesting_1545 to a single 15:45 weekday trigger)
```

### 20.5 Replay Runners

```text
bat\run_eqidv2_signal_discovery_v7_replay_today.bat
bat\run_eqidv2_entry_engine_v7_replay_today.bat
```

Warning: this replay runner writes to production candidate CSV/JSON/latest/audit/status/heartbeat paths. It is blocked during market hours unless `EQIDV2_REPLAY_FORCE_MARKET_HOURS=1` is set. Use only with intent.

### 20.6 Current 2026-06-10 Runtime Observations

Confirmed runtime facts as of 2026-06-10:

- Scanner process PID: 42104. Entry engine PID: 24832. Both running with all punchlist changes deployed.
- Punchlist P0-1, P0-2, P0-4, P1-6, P1-7, P1-9 and all P2 items implemented and live. P0-3 (walk-forward gate) runs report-only and does not yet author `accepted_rules.csv`. P1-8 (point-in-time universe) remains deferred.
- Paper executor running with the NSE intraday net cost model wired (P0-1; gross/net columns in `paper_trades` CSV), exit slippage 5 bps, gross SHORT cap Rs 15L, risk-based position sizing.
- Live executor code updated with all risk controls but executor process remains disabled pending clean paper run.
- `v2._prepare_5m` now recomputes causal session VWAP (P1-6); parquet VWAP retained as `VWAP_source`. Causality audit task `EQIDV2_v7_causality_audit_1605` active.
- V11 overlay thresholds rounded to ≤2 decimals (P0-4). Walk-forward gate report task `EQIDV2_v7_walkforward_gate_1620` and NSE cost report task `EQIDV2_v7_nse_id_cost_1605` active (both report-only/advisory).
- `CANDIDATE_SCHEMA_VERSION="v7_candidate_2026_06_10"` stamped in all candidate rows from scanner.
- Dashboard surfacing `adv_rej` and `fno_rej` columns in setup table funnel card.
- ADV liquidity cap and F&O ban filter active in entry engine.

21-day paper cumulative results (2026-05-12 to 2026-06-10):

- 681 executed trades, 26.9% win rate, PF=0.82, net PnL Rs -54,122.
- Top drag setups: `C_OR_BREAKOUT LONG` (PF=0.67, Rs -19,127) and `T_TREND_DAY_EMA_STAIR_SHORT` (PF=0.35, Rs -21,464).
- Jun 3 and Jun 4 accounted for Rs -38,000 — driven by paper executor running at 100-position limit before punchlist fix.
- Post-punchlist (2026-06-09): 9 trades, Rs +466. Too thin for statistical inference.

Needs follow-up:

- Confirm whether `T_TREND_DAY_EMA_STAIR_SHORT` should be disabled immediately (recommended) or shadow-blocked first.
- Confirm whether `C_OR_BREAKOUT` LONG entries should be gated to high-NIFTY-momentum days only.
- Accumulate 5 clean paper trading days post-punchlist before enabling live executor.
- Confirm final intended start time for `E_VWAP_LOSE_EARLY_SHORT` (code: 09:45, review: 09:40).
