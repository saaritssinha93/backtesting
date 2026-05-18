# Strategy Map — v16 5min pipeline

**Purpose:** canonical reference for how a signal flows from trigger bar → executor order in the V16 5-min stack.

> **2026-04-27 update:** PF/DE now verify the **just-closed trigger bar**, not the forming entry bar. This re-enables the 2026-04-23 PF-TRIGGER-FIX after a 2026-04-24 reversion. End-to-end latency: **~6s** after entry bar opens (was 56s spec, 38-46s reality, infinite during stall). See "Stale-marker expiry" section below.

## Core math

```
trigger_iso   = OPEN time of trigger bar (SEE's signal-triggering bar)
lag           = 1 or 2 (from setup family)
entry_ist     = trigger_iso + lag × 5min
source_slot   = entry_ist                                       ← marker filename
trigger_slot  = source_slot                                     ← (DISABLE_LAG_SHIFT=1)
verify_slot   = trigger_slot − 5min = entry_ist − 5min          ← bar PF verifies

Lag=1:  verify_slot = entry_ist − 5min = trigger_iso        (the SEE trigger bar itself)
Lag=2:  verify_slot = entry_ist − 5min = trigger_iso + 5min (one bar after the SEE trigger)

In BOTH cases: verify_slot is the bar that CLOSES at entry_ist.
PF asks Kite for date=verify_slot at entry_ist + ~1s; Kite serves the
just-closed bar within ~1-2s; no 52s forming-bar wait.
```

## Component roster

| Short | Process | Role |
|---|---|---|
| **NF** | `trading_data_continous_run_historical_alltf_v3_parquet_niftyonly_5minonly.py` | NIFTYBEES/index 5-min fetcher for RS calcs |
| **LF** | `eqidv2_eod_scheduler_for_5mins_data_live_minimal.py` | Full-universe (1044 tickers × 8 Kite apps) historical_data fetcher → canonical parquet + indicators |
| **SEE** | `eqidv2_signal_early_engine_v16_5min.py` | Scans canonical at slot+45s, detects setups, writes pool JSON |
| **Pool** | `pending_signals_{date}_v16_5min.json` | Shared state — SEE writes, PF reads/updates, DE reads |
| **PF** | `eqidv2_pending_data_fetcher_v16_5min.py` | Re-fetches pending tickers + writes `.ready` markers |
| **DE** | `eqidv2_detection_engine_v16_5min.py` | Watches markers, rescans, promotes signals to confirmed |
| **Executor** | `avwap_trade_execution_*.py` | Reads confirmed signals, places orders via Kite |

## Generic 5-min slot clock (every T = slot boundary)

```
T + 0s         ▶ Slot boundary — bar T-5 closes, bar T opens

T + 2s         ▶ NF wakes — fetches NIFTYBEES historical_data (fast, small)
T + 2s         ▶ PF cycle: reads pool (1s poll when empty; fetches when non-empty)
T + 4s         ▶ DE wakes — polls marker dir, rescans if PF data ready

T + 30s        ▶ LF wakes — fetches 1044 tickers × 8 Kite apps
T + 30-45s     ▶ LF finishes writing canonical parquets (date = T-5 rows)
T + 45s        ▶ LF publishes slot_ready_5m\{date}_{T}.ready (source=final)

T + 45s        ▶ SEE wakes on LF marker, scans canonical for setups
T + 52s        ▶ SEE detection runs against bar T-5 (just-closed) + forming bar T
T + 54s        ▶ SEE writes detected signals to pool JSON (pending += N)

T + 5min       ▶ Next slot boundary; cycle repeats
```

## LAG=1 flow

**Example: trigger_iso = 9:35, entry_ist = 9:40, verify_slot = 9:35**

```
9:35:00 (T)    ▶ Trigger bar 9:35 opens
9:35:02        ▶ NF wakes, PF cycle (polling, nothing to fetch yet), DE polling
9:35:30        ▶ LF wakes, fetches 1044 tickers
9:35:30-45     ▶ LF writes date=9:30 to canonical
9:35:45        ▶ LF marker published; SEE wakes
9:35:52        ▶ SEE runs detection on forming 9:35 bar + closed 9:30 bar
9:35:54        ▶ SEE writes signal to pool:
                 {
                   source_slot:  9:40,      ← entry bar (marker filename)
                   trigger_iso:  9:35,
                   entry_ist:    9:40:00,
                   verify_slot:  9:35,      ← trigger bar; closes at 9:40:00
                   lag:          1 (implicit from setup),
                   status:       pending
                 }
                 Pool: pending=1

9:35:56 → 9:39:59  ▶ PF cycle every ~2s:
                     Checks canonical for date=9:35: PARTIAL/UNVERIFIED
                     (bar 9:35 still forming — closes at 9:40:00)
                     Marker WITHHELD until close. Retries.
                     DE cycle sees no marker. Idle.

═══ Slot 9:40 — THE RELEVANT SLOT ═══

9:40:00 (T+5)  ▶ Entry bar 9:40 opens. Trigger bar 9:35 CLOSES — fully baked.
9:40:01        ▶ PF wakes (slot_offset=1s). Fetches Kite for date=9:35 (closed).
9:40:02-03     ▶ Kite returns closed 9:35 bar (~1-2s — closed bars are fast).
                 PF writes canonical row for 9:35, writes
                 slot_ready_5m_pending\{date}_0940.ready (filename=entry_slot
                 for DE compatibility; payload tickers verified on closed bar).
9:40:04        ▶ DE cycle sees new marker. Rescans canonical date=9:35.
                 Validates setup condition against CLOSED trigger bar. Promotes.
9:40:05-06     ▶ Executor polls, sees confirmed signal, places order at market.
```

**Lag=1 end-to-end: ~6s from entry bar open. Pool→executor ≈ 4m 12s (most time spent waiting for the trigger bar to close).**

## LAG=2 flow

**Example: trigger_iso = 9:35, entry_ist = 9:45, verify_slot = 9:40**

```
9:35:00 (T)    ▶ Trigger bar 9:35 opens
9:35:02-45     ▶ Same as lag=1: NF, LF, SEE run
9:35:54        ▶ SEE writes signal to pool:
                 {
                   source_slot:  9:45,      ← entry bar (marker filename)
                   trigger_iso:  9:35,
                   entry_ist:    9:45:00,
                   verify_slot:  9:40,      ← bar before entry; closes at 9:45:00
                   lag:          2,
                   status:       pending
                 }
                 Pool: pending=1

                 NOTE: For lag=2, verify_slot (9:40) is NOT the SEE trigger bar
                 (9:35) — it's the bar IMMEDIATELY BEFORE entry. Validating on
                 9:40's close confirms the setup hasn't broken in the gap
                 between trigger and entry. Stronger filter than lag=1.

9:35:56 → 9:44:59  ▶ PF retries; verify_slot=9:40 hasn't closed yet.

═══ Slot 9:40 (intermediate — ignored for this signal) ═══

9:40:00 (T+5)  ▶ Bar 9:40 opens. Bar 9:35 closes — but for THIS signal's
                 lag=2, we need 9:40 closed (at 9:45:00), not 9:35.
9:40:02-45     ▶ NF/LF/SEE run for slot 9:40. PF checks date=9:40 in canonical:
                 PARTIAL/UNVERIFIED (still forming). Marker withheld.

═══ Slot 9:45 — THE RELEVANT SLOT ═══

9:45:00 (T+10) ▶ Entry bar 9:45 opens. Bar 9:40 CLOSES — fully baked.
9:45:01        ▶ PF wakes. Fetches Kite for date=9:40 (closed).
9:45:02-03     ▶ Kite returns closed 9:40 bar. PF writes canonical row,
                 writes slot_ready_5m_pending\{date}_0945.ready
9:45:04        ▶ DE rescans canonical date=9:40. Validates on closed bar. Promotes.
9:45:05-06     ▶ Executor places order.
```

**Lag=2 end-to-end: ~6s from entry bar open. Pool→executor ≈ 9m 12s.**

## Comparison table

| Lag | trigger_iso | entry_ist | verify_slot | marker filename | verify available | executor fires |
|---|---|---|---|---|---|---|
| **1** | T | T+5 | T (= trigger bar) | `{T+5}.ready` | T+5min+2s | T+5min+5-6s |
| **2** | T | T+10 | T+5 (= bar before entry) | `{T+10}.ready` | T+10min+2s | T+10min+5-6s |

**Both lags verify the bar that closes AT entry_ist. The only difference is which lag-shift puts the trigger relative to entry.**

## Timing decomposition

For any signal (lag=1 or lag=2), from SEE write to Executor:

```
SEE write (T + 54s)
    ↓ wait for verify bar to close (5 × lag minutes after trigger)
Verify bar closes = entry bar opens (entry_ist)
    ↓ ~1s PF wake (slot_offset)
    ↓ ~1-2s Kite serves the just-closed bar
PF marker written (entry_ist + 2-3s)
    ↓ ~1-2s DE rescan + promote
DE confirmed (entry_ist + 4s)
    ↓ ~1-2s executor poll + place order
Executor fires (entry_ist + 5-6s)
```

**Bottleneck is gone:** PF asks Kite for the just-closed bar (typical historical_data, ~1-2s) instead of the forming entry bar (52s publish delay). The pool→executor time is now dominated by waiting for `entry_ist` itself, not by post-entry latency.

## Slot-by-slot roles

| Slot | NF | LF | SEE | Role for a pending signal |
|---|---|---|---|---|
| **T** (trigger) | Runs | Runs | Detects → writes pool at T+54s | Signal born here |
| **T+5** | Runs | Runs (writes date=T) | May emit new signals | **Relevant if lag=1** (trigger bar T closes here → verify+marker+execute) |
| **T+10** | Runs | Runs (writes date=T+5) | May emit new signals | **Relevant if lag=2** (T+5 bar closes here → verify+marker+execute) |
| **T+15** | Runs | Runs (writes date=T+10) | — | Only if lag=3 (not in SETUP_LAG_BARS) |

## Setup families and their lag values

From `SETUP_LAG_BARS` in `eqidv2_pending_data_fetcher_v16_5min.py` (mirrors DE):

| Setup | Side | Lag |
|---|---|---|
| `A_MOD_BREAK_C1_HIGH` | LONG | 1 |
| `A_MOD_BREAK_C1_LOW` | SHORT | 1 |
| `A_PULLBACK_C2_BREAK_C2_HIGH` | LONG | 2 |
| `A_PULLBACK_C2_BREAK_C2_LOW` | SHORT | 2 |
| `A_MOD_CLOSE_CONTINUATION_BREAK` | LONG | 2 |
| `B_HUGE_C1_CLOSE_RECLAIM_BREAK` | LONG | 2 |
| `B_HUGE_PULLBACK_HOLD_BREAK` | LONG | 999 (disabled) |
| `B_HUGE_FAILED_BOUNCE` | SHORT | -1 (disabled) |

Setups with lag <= 0 or lag > 4 default to lag=1 in PF's `_setup_lag_shift`. Setups whose SEE emit-name doesn't match this table also default to lag=1.

> **Runtime config attestation** stamps these timing flags at boot for both PF and DE:
> `EQIDV16_5MIN_PF_VERIFY_TRIGGER_BAR`, `EQIDV16_5MIN_DE_VERIFY_TRIGGER_BAR`,
> `EQIDV16_5MIN_DISABLE_LAG_SHIFT`, `EQIDV16_5MIN_MAX_PF_MARKER_LAG_SEC`,
> `EQIDV2_PENDING_FETCH_INTERVAL_SEC`, `EQIDV2_PENDING_FETCH_SLOT_OFFSET_SEC`,
> `EQIDV2_5M_KITE_TIMEOUT_SEC`, `EQIDV2_DETECTION_CHECK_INTERVAL_SEC`,
> `EQIDV2_DETECTION_SLOT_OFFSET_SEC`, `EQIDV2_DETECTION_MAX_DATA_AGE_SEC`.
> Drift between BAT-set values and worker `os.environ` triggers
> `[STARTUP.CONFIG] DRIFT` warnings; `EQIDV2_CONFIG_CHECK_STRICT=1` makes drift a boot exit.

## Why this is fast (and why the previous design wasn't)

PF asks Kite for the **just-closed** bar (`date = entry_ist − 5min`), not the **forming entry bar** (`date = entry_ist`). The forming bar has a ~52s Kite publish delay; closed bars are normal historical_data and serve in ~1-2s.

**Pre-2026-04-27 behavior (now reverted):** the bat config set `PF_VERIFY_TRIGGER_BAR=0`, making PF verify the forming entry bar. Kite withheld the partial bar; PF retried until either the forming bar was served (38-52s) or fell through to LF's parquet write of the closed bar (T+45s). Either way, the PF marker landed at `entry_ist + 38-46s`, not `entry_ist + 3s`.

The KiteTicker WebSocket plan in [kite_ticker_pf_plan.md](kite_ticker_pf_plan.md) is no longer required for sub-10s execution latency, but remains relevant if a setup ever needs partial-bar evaluation (e.g., breakout entry on the forming entry bar instead of the trigger close).

## Stale-marker expiry

PF refuses to publish a marker for a slot whose `entry_ist` is already past `MAX_PF_MARKER_LAG_SEC` (default 90s, env: `EQIDV16_5MIN_MAX_PF_MARKER_LAG_SEC`). Without this guard, a PF stall + recovery publishes markers for 30-40min-old slots that the executor's late-detection gate then silently rejects (a single such cascade on 2026-04-27 cost 36 ENTRY_SKIPPED_STALE_DETECTION rows).

The guard fires before the parquet verify check, in both the per-slot emit path AND the startup-reconcile path. Expired slots are logged as `[PENDING_FETCH] marker_expired_pre_verify | slot=... | lag=...s > 90s | tickers=N`. Code: [eqidv2_pending_data_fetcher_v16_5min.py:697-714](eqidv2_pending_data_fetcher_v16_5min.py#L697-L714).

## Failure modes

| Mode | Symptom | Mitigation |
|---|---|---|
| **PF supervisor silent death** | Worker python alive but unsupervised; heartbeat goes stale | Tomorrow-morning healthcheck cron alerts via Gmail; investigation pending |
| **NF supervisor silent death** | DE aborts every slot with `NF_STALE` waiting for nifty_ready_*.json | Same supervisor pattern; same mitigation |
| **PF stuck on opening-snapshot escalation** | All 8 Kite apps timeout 150s on bulk fetch; cascade across retries | NEEDS FIX (B02): gate `intraday_ts=start` mode to first 60s of process life |
| **Recovery cascade publishes stale markers** | Markers for 30-40min-old slots → executor late-skip | Stale-marker expiry guard (above) |
| **Burst entry placement at slot boundary** | 8+ orders/sec collide with Kite's 10/sec rate cap | Rate-limit-retry matcher (fixed 2026-04-27 09:57); throttler still pending (B05) |
| **DE slot-polled, not marker-driven** | DE recovers from PF outage with up to 5min extra latency | Architectural; option to add FileSystemWatcher (B04) |

## File paths

```
C:\TradingData\eqidv2\
├── stocks_indicators_5min_eq_live\
│   └── {TICKER}_stocks_indicators_5min.parquet    (canonical, OHLCV + indicators)
│
├── slot_ready_5m\
│   └── {YYYYMMDD}_{HHMM}.ready                    (LF completion marker)
│
├── slot_ready_5m_pending\
│   └── {YYYYMMDD}_{HHMM}.ready                    (PF trigger-slot marker)
│
├── live_signals\
│   ├── pending_signals_{date}_v16_5min.json       (pool)
│   ├── pending_signals_{date}_v16_5min.csv        (CSV mirror)
│   ├── confirmed_signals_{date}_v16_5min.json     (DE output)
│   ├── pool_lifecycle_{date}_v16_5min.jsonl       (audit trail)
│   └── open_live_trades_state_{date}_v16_5min.json (executor state)
│
└── runtime_status\
    └── {process}.status / {process}.heartbeat      (per-process liveness)
```
