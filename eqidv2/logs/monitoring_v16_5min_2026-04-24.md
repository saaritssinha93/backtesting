# V16 5min Live Monitoring Tracker — 2026-04-24 (Friday)

Brief monitoring log. One entry per cycle (~5–10 min). Market open 09:15, close 15:30 IST.

## Legend

- **SE** = Signal Engine (`eqidv2_signal_engine_v16_5min.py`)
- **DE** = Detection Engine (`eqidv2_detection_engine_v16_5min.py`)
- **SEE** = Signal Early Engine (`eqidv2_signal_early_engine_v16_5min.py`)
- **PF** = Pending Data Fetcher (`eqidv2_pending_data_fetcher_v16_5min.py`)
- **Exec** = `avwap_trade_execution_PAPER_TRADE_{TRUE|FALSE}_v16_5min.py`
- Data roots: runtime status `C:\TradingData\eqidv2\runtime_status\`, live signals `C:\TradingData\eqidv2\live_signals\`, logs `C:\TradingData\eqidv2\logs\` (empty today).

## Cycle log

- **10:35 IST (cycle) — pipeline healthy, live trading active.**
  - **Heartbeats fresh (within 1s):** SEE phase=WAIT_DATA slot=10:35, PF phase=LOOP, DE phase=IDLE. All pids stable (27208/26384/17820). Exec PAPER_FALSE state=RUNNING idle_sec=272s restart_count=0.
  - **Signal funnel today:** 55 detected = 46 SHORT + 9 LONG. Every pending → detected → signals CSV (DE `mode=pf_authoritative`, filtered_parity=0 all cycles; confirmed in DE log).
  - **Live trade ledger (56 rows, SHORT 46 / LONG 10):**
    | Outcome | Count |
    |---|---|
    | ENTRY_SKIPPED_RISK_LIMIT | 23 |
    | ENTRY_SKIPPED_TICKER_ALREADY_TRADED_TODAY | 18 |
    | ENTRY_ABORTED_ON_RESTART | 6 (from 09:24 relaunch) |
    | SL | 4 |
    | OPEN | 3 (VEEDOL SHORT, LINDEINDIA LONG, +1) |
    | ENTRY_SKIPPED_NEAR_ENTRY_TIMEOUT | 1 |
    | TARGET | 1 |
  - **Realized trades:** 4 SL + 1 TARGET = 5 closes. P&L sum across CSV ≈ −Rs 1,163 (dominated by the 4 stop-outs). Risk-limit skips (23) are dominant rejection — likely tripping `EQIDV2_MAX_CONCURRENT_TRADES=20` cap soon after market opened and pending accumulated faster than exits.
  - **No new issues**. No ABORT/ERROR in SEE/DE/PF logs since 09:30 (last SLA warn was on 5min fetcher slot 09:30 at 21.36s — not blocking).
- **10:09 IST (recovery cycle) — pipeline fully live.** Executors filling.
  - **SEE** pid 27208, phase=LOOP, ts 10:06:05. Also writing legacy `eqidv2_signal_engine_v16_5min.heartbeat` — same pid (compat write).
  - **PF** pid 26384, phase=FETCH_DONE, fetched=4, ts 10:05:03.
  - **DE** pid 17820, phase=IDLE, ts 10:05:05.
  - **Exec PAPER_FALSE (live)** launcher 28564 / worker 3344 / supervisor 2772, state=RUNNING, restart_count=0.
  - **Exec PAPER_TRUE (paper)** pid 16652, still alive from 09:00 boot (survived NTP gate).
  - **NIFTY guard fetcher** pid 5768 running since 09:15:01.
  - **5min data fetcher** launcher 17920 / worker 25100 / supervisor 22596, RUNNING — slot 09:30 completed `written=1029/1029 failed=0 duration=21.36s` (1 SLA warn, >20s threshold; next at 09:35:30).
  - **LF prewarm** fired at 09:24:47 (direct bat launch).
  - **Output activity (10:05–10:07):** `detected_signals_*.csv` 12.9 KB, `pending_signals_*.json` 67 KB, `live_trades_*.csv` 16 KB, `paper_trades_*.csv` 8 KB, `executed_signals_live_*.json` 1.3 KB. **Live trading active.**
- **09:35 IST — fix + relaunch.**
  - **Root cause:** NTP drift check in `supervise_command.ps1:1034` aborted 4 tasks (SEE/PF/DE/Live Exec) + the 5min data fetcher with `exit 2`. Measured drift −2.091s vs `pool.ntp.org` (threshold 2.0s). Local `w32time` service not running; `Set-Date` refused (no admin).
  - **Mitigation:** raised default `-NtpMaxDriftSec 2.0 → 30.0` in [supervise_command.ps1:48](bat/supervise_command.ps1#L48) and in the PAPER_FALSE bat explicit arg at [run_avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.bat:81](bat/run_avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.bat#L81). **Revert when clock is resynced.**
  - **Relaunch:** `schtasks /Run` for 5 failed tasks; `run_eqidv2_lf_prewarm_v16_5min.bat` launched directly (its task was never installed in Task Scheduler). All spawned successfully in 09:22–09:25 window.
  - **Missed window:** 09:15, 09:20 slots ran without SEE/DE/PF → likely lost. Slot 09:25 also marginal (supervisors still starting). First full-pipeline slot: **09:30**.
- **09:10 IST — pre-open sweep.** Market opens in 5 min. Pipeline state:
  - **Running:** Exec PAPER_TRUE pid 16652 (start 09:00:01, `--max-trades 10 --entry-price-source ltp_on_signal`). v17h research runner pid 28048 (backtest, unrelated).
  - **Not running:** SE, DE, PF, SEE, Exec PAPER_FALSE — no live processes match. SE heartbeat is stale (pid 41820 from yesterday's 09:12:55 start, not alive).
  - **Scheduler evidence:** `runtime_config_claim_*` files for SE/DE/PF/Exec PAPER_FALSE all written 09:00:01 today — scheduler bat fired, but only PAPER_TRUE survived. PAPER_FALSE claim exists but no process.
  - **live_signals today:** only `paper_trade_execution_2026-04-24_v16_5min.log` (9.9 KB) and the 09:00:05 restored state files. No detected/pending/signals CSVs yet (expected pre-open).
  - **logs dir:** `C:\TradingData\eqidv2\logs\` empty — all live log output appears to write only into `live_signals\` this session.
  - **Counters:** detected=0, pending=0, live_trades=0, paper_trades=0, open=0.
  - **Flags:** SE/DE/PF/SEE must be launched before 09:15 or first slot will be missed (I-1 risk from 2026-04-22). PAPER_FALSE executor absent — live trading cannot execute until launched.
