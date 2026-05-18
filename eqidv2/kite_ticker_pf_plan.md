# KiteTicker bar builder for PF — full plan (Option B: DE-only fork)

**Author:** Saarit
**Date drafted:** 2026-04-24
**Approach:** DE-only fork + feature-flagged PF branch + new tick builder daemon. Original DE stays byte-identical for instant rollback.

For pipeline flow details (slot clock, lag=1 vs lag=2 lifecycles), see [strategy_map_v16_5min.md](strategy_map_v16_5min.md).

## 1. TL;DR

- **Goal**: cut PF marker latency from `entry_ist + 52s` to `entry_ist + 1-3s` for BOTH lag=1 and lag=2 setups
- **Approach**: new tick-builder daemon + forked DE (`_KT`) + 1-line PF branch + env-var config additions
- **Coexistence**: old and new DE never run simultaneously — switched via Windows scheduled-task enable/disable. Shared state (pool JSON, markers dir) is identical format, so either stack can resume work the other started.
- **Will it hamper detection/entries?**: **No** — if rolled out in stages with `PARTIAL_BAR_ALLOWED_SETUPS = {}` initially empty, behavior is byte-identical to today's. Latency wins only apply once setups are explicitly opted into partial-bar mode.

## 2. Problem being solved

Per [strategy_map_v16_5min.md](strategy_map_v16_5min.md), PF's `verify_slot = entry_ist` for BOTH lag=1 and lag=2. The entry bar opens at `entry_ist:00` and is a forming bar at that moment. Kite's `historical_data` API does not serve this forming bar until ~52s of ticks have accumulated.

Observed on 2026-04-24:

| Signal | Lag | trigger_iso | entry_ist | Pool→Executor actual |
|---|---|---|---|---|
| APTUS SHORT (`A_MOD_BREAK_C1_LOW`) | 1 | 09:35 | 09:40 | SEE write 09:36:04 → DE promote 09:36:15 (11s) — but note: verified forming 09:35 bar early; entry 09:40 still ~4m away |
| RANEHOLDIN LONG (`B_HUGE_C1_CLOSE_RECLAIM_BREAK`) | 2 | 11:35 | 11:45 | SEE write 11:36:03 → DE promote 11:46:00 (9m 57s) |

The 52s-after-entry-bar-open floor is Kite's external constraint. To beat it, we need live tick streaming.

## 3. How the current system works (OLD)

See [strategy_map_v16_5min.md](strategy_map_v16_5min.md) for the full slot clock and per-lag flows. Summary:

```
T + 0s       Slot boundary, trigger bar T opens
T + 30s      LF fetches and writes canonical date=(T-5)
T + 45s      LF marker, SEE wakes
T + 54s      SEE writes signal to pool (pending=1)
...
entry_ist   Entry bar opens (T+5min for lag=1, T+10min for lag=2)
entry_ist + 52s   Kite finally serves forming entry bar
entry_ist + 54s   PF writes marker; DE promotes
entry_ist + 56s   Executor places order
```

**Bottleneck**: 52s Kite publish delay after entry bar opens. Identical floor for lag=1 and lag=2.

## 4. How it will work with KT (NEW)

```
┌─── SEE (slot+45s) ────────────────────────────────┐
│  UNCHANGED                                        │
└────────────────────┬──────────────────────────────┘
                     ▼
┌─── Tick Builder Daemon (NEW, long-lived) ────────┐
│  • Reads pool JSON every 1s                      │
│  • Subscribes KiteTicker for pending tickers     │
│  • Aggregates ticks → in-memory 5-min bars       │
│  • Flushes partial bar to scratch parquet every 1s│
└────────────────────┬──────────────────────────────┘
                     ▼
┌─── PF (1-line branch added) ──────────────────────┐
│  • SAME flow, but `_slot_row_present_in_parquet` │
│    checks scratch parquet FIRST                  │
│  • If scratch has row → verify passes earlier    │
│  • If scratch missing → fallback to canonical    │
│    (IDENTICAL to current behavior)               │
│  • Marker write can happen at entry_ist+1-3s     │
└────────────────────┬──────────────────────────────┘
                     ▼
┌─── DE_KT (FORKED — runs INSTEAD of DE) ──────────┐
│  • Watches same marker dir                       │
│  • On rescan: reads BOTH parquets (scratch       │
│    preferred for forming bars, canonical for     │
│    closed bars)                                  │
│  • Applies PARTIAL_BAR_ALLOWED_SETUPS gating:    │
│    - Setup in allowlist → use partial bar        │
│    - Setup NOT in allowlist → wait for closed    │
│      canonical bar (current behavior preserved)  │
│  • Writes same confirmed_signals JSON            │
└────────────────────┬──────────────────────────────┘
                     ▼
┌─── Live Trade Executor ───────────────────────────┐
│  UNCHANGED — consumes confirmed_signals          │
└───────────────────────────────────────────────────┘
```

## 5. How OLD and NEW coexist

**The switching mechanism is a Windows scheduled-task pair.** Only one is enabled at any moment:

| Task | State when running OLD | State when running NEW |
|---|---|---|
| `EQIDV2_detection_engine_v16_5min_0900` | **Enabled** | Disabled |
| `EQIDV2_detection_engine_v16_5min_KT_0900` | Disabled | **Enabled** |
| `EQIDV2_tick_bar_builder_v16_5min_0913` | Disabled | **Enabled** |
| `EQIDV2_signal_early_engine_v16_5min_0900` | Enabled | Enabled (shared) |
| `EQIDV2_pending_data_fetcher_v16_5min_0900` | Enabled | Enabled (shared) |
| `EQIDV2_lf_prewarm_v16_5min_0913` | Enabled | Enabled (shared) |

**Shared state is identical format in both modes**:
- Pool JSON: `pending_signals_{date}_v16_5min.json` — unchanged schema
- Markers dir: `slot_ready_5m_pending/` — unchanged marker format
- Confirmed signals: same file, same schema
- Lifecycle JSONL: same file

So if you run NEW for a few hours, stop it, flip tasks, run OLD — OLD picks up exactly where NEW left off. **Rollback = 2 scheduler commands + next-slot recovery.**

**PF behavior with/without tick builder**:
- Tick builder running → PF's scratch check often succeeds → markers written faster
- Tick builder NOT running → PF's scratch check returns False → falls through to canonical → **byte-identical to current PF**

## 6. KT flow overlay for lag=1 and lag=2

### LAG=1 with KT (whitelisted setup)

```
9:35:00 (T)    Trigger bar 9:35 opens
9:35:54        SEE writes signal (entry_ist=9:40, verify_slot=9:40)
9:35:56        PF sees pending → KT daemon subscribes XYZ ticker
                Builder starts receiving ticks for forming 9:35 bar,
                but verify_slot for THIS signal is 9:40 (not 9:35)
                → builder's 9:35 row is irrelevant for this signal
9:35:56–9:39:59  KT accumulates ticks for 9:35 bar (harmless, unused);
                 PF retries, marker withheld (9:40 doesn't exist)

═══ Slot 9:40 — THE RELEVANT SLOT ═══

9:40:00        Entry bar 9:40 OPENS. Subscription already active
                (since 9:35:56). First tick arrives within ~100ms.
9:40:00.2      KT flushes partial row date=9:40 to scratch parquet
                (close = first tick price, tick_count = 1-5)
9:40:01        PF cycle: checks scratch parquet for date=9:40: FOUND.
                Writes slot_ready_5m_pending\{date}_0940.ready
9:40:02        DE_KT sees marker. Setup in PARTIAL_BAR_ALLOWED_SETUPS?
                  • YES: reads scratch OHLCV + canonical indicators,
                    applies tick-count gate (e.g., ≥ 30 ticks), validates
                    condition, promotes if passes
                  • NO: waits for canonical (falls back to today's behavior
                    ~9:40:52)
9:40:03-08     Executor fires (if promoted)
```

**Lag=1 with KT+whitelist: executor fires at `entry_ist + 3-8s`** (vs. `entry_ist + 56s` today).

### LAG=2 with KT (whitelisted setup)

```
9:35:00 (T)    Trigger bar opens
9:35:54        SEE writes signal (entry_ist=9:45, verify_slot=9:45)
9:35:56        KT subscribes XYZ ticker
9:35:56–9:44:59  KT accumulates ticks for 9:35 and 9:40 bars
                 (all irrelevant for this signal's verify_slot=9:45)

═══ Slot 9:45 — THE RELEVANT SLOT ═══

9:45:00        Entry bar 9:45 OPENS. First tick within ~100ms.
9:45:00.2      KT flushes date=9:45 row to scratch
9:45:01        PF checks scratch: FOUND. Writes marker {date}_0945.ready
9:45:02        DE_KT sees marker. Whitelisted → promotes.
9:45:03-08     Executor fires.
```

**Lag=2 with KT+whitelist: executor fires at `entry_ist + 3-8s`** (vs. `entry_ist + 56s` today).

### KT win is identical for both lags

Because `verify_slot = entry_ist` in both cases, KT's contribution is:
- **Kite delay eliminated**: 52s → 1-3s
- **Net saving**: ~50s per signal
- **Applies equally to lag=1 and lag=2**

Today's ~5s lag=1 latency (SEE write at T+54s, promote at T+5min+56s) is PRE-ENTRY latency — the signal is promoted 4+ minutes before its planned entry time. With KT+whitelist, promotion happens just seconds AFTER entry_ist, meaning orders fire at the entry bar's actual open price instead of waiting for Kite.

## 7. Files — consolidated table

### NEW files (5)

| File | Purpose |
|---|---|
| `eqidv2_tick_bar_builder_v16_5min.py` | WebSocket daemon |
| `eqidv2_detection_engine_v16_5min_KT.py` | **Forked DE** with dual-source reads + partial-bar gating |
| `bat/run_eqidv2_tick_bar_builder_v16_5min.bat` | Supervisor launcher for builder |
| `bat/run_eqidv2_detection_engine_v16_5min_KT.bat` | Supervisor launcher for DE_KT |
| `tests/test_tick_bar_aggregation.py` | OHLCV correctness vs historical_data |

### MODIFIED files (7)

| File | Change | Backward-compat? |
|---|---|---|
| `eqidv2_pending_data_fetcher_v16_5min.py` | L626: wrap `_slot_row_present_in_parquet` with optional scratch-first branch. Keep existing function body byte-identical, just rename internally. | ✅ When scratch empty/missing → canonical path, zero change |
| `eqidv2_runtime_paths.py` | Add `TICK_SCRATCH_DIR` constant (~5 lines) | ✅ Pure addition |
| `bat/run_eqidv2_pending_data_fetcher_v16_5min.bat` | Export `EQIDV2_TICK_SCRATCH_DIR` | ✅ If unset → PF skips scratch check |
| `bat/schedule_eqidv2_v16_5min_two_stage_weekday.bat` | Add 2 scheduled tasks (builder + DE_KT), both **disabled** by default | ✅ Disabled tasks don't run |
| `bat/stop_eqidv2_v16_5min_two_stage_stack.ps1` | Add 2 entries (builder + DE_KT) to stop list | ✅ No-op if processes aren't running |
| `replay_today_full_pipeline_v16_5min.py` | L239-254: monkey-patch wrapper forces canonical-only in replay mode | ✅ Replay behavior unchanged |
| `eqidv2_config_attestation.py` | Allow `EQIDV2_TICK_SCRATCH_DIR` in whitelist (~3 lines) | ✅ Attestation is soft-mode by default |

### UNCHANGED files (critical to preserve)

| File | Why |
|---|---|
| `eqidv2_detection_engine_v16_5min.py` | **NOT modified** — forked to `_KT` instead. Original stays byte-identical for easy rollback. |
| `eqidv2_signal_early_engine_v16_5min.py` | SEE uses closed bars only — doesn't need forming-bar data |
| `eqidv2_eod_scheduler_for_5mins_data_live_minimal.py` | LF uses historical_data exclusively — stays that way |
| `eqidv2_pool_lock.py` | Shared primitive, source-agnostic |
| `bat/supervise_command.ps1` | Shared supervisor infra |
| `avwap_combined_runner_v16_5min.py` | Receives dataframes from callers, doesn't read parquets in live mode |
| Live trade executor | Consumes `confirmed_signals.json` — same schema either way |

**Total: 5 new files, 7 modified, 1 forked. Original DE stays untouched.**

## 8. Will this hamper detection/entries?

### Stage 1 — Deploy scaffolding, all flags OFF/empty

- `EQIDV2_TICK_SCRATCH_DIR` set but directory is empty (builder not running yet)
- Scheduled tasks for builder and DE_KT both **disabled**
- PF runs with the 1-line branch, but scratch dir is empty → 100% fallback to canonical
- DE (original) runs unchanged
- **Behavior: byte-identical to today. Zero detection/entry risk.**

### Stage 2 — Start tick builder daemon, DE_KT still disabled

- Builder writes to scratch parquet for pending tickers
- PF now sometimes finds scratch rows → writes markers earlier
- **But DE (original) reads canonical only** — it still does the same rescan with the same data it always had
- If PF wrote the marker faster than canonical had the bar, DE just tries to read canonical, finds no row, waits. Same outcome as today.
- **Behavior: decisions identical to today; only marker-write timing differs. Zero detection/entry risk.**

### Stage 3 — Disable DE, enable DE_KT, `PARTIAL_BAR_ALLOWED_SETUPS = {}` empty

- DE_KT runs but its allowlist is empty → every setup still requires closed canonical bar
- DE_KT's decisions = DE's decisions for every setup
- Any setup whose partial-bar gate is not whitelisted waits for canonical just like today
- **Behavior: decisions identical to today. Zero detection/entry risk.**

### Stage 4 — Opt in setups one by one after Phase 4 audit

- Add first setup: `PARTIAL_BAR_ALLOWED_SETUPS = {"A_MOD_BREAK_C1_HIGH": "tick_count_gate_30"}`
- That setup now fires on partial bars when tick-count threshold is met (fast path)
- All other setups continue to wait for canonical (current behavior)
- **Scoped risk**: only the whitelisted setup has decision changes. Monitor for false positives on that setup specifically.
- **Kill switch**: remove from allowlist → behavior reverts to current.

### Rollback — if anything goes wrong

- **Quick rollback** (no shared state divergence): `schtasks /Change /DISABLE` on DE_KT and builder, `/ENABLE` on original DE. Takes <10s. Next DE cycle picks up existing markers from shared state. Zero entry disruption.
- **Cold rollback** (worst case): stop _KT processes manually, stop builder, run the original DE bat manually. <1 min.

### What could actually go wrong (honest list)

| Scenario | Impact | Detection |
|---|---|---|
| Tick builder crashes silently | Scratch parquet stops updating → PF falls back to canonical → **no impact on detection, just loses latency win** | Supervisor heartbeat alerts |
| Scratch parquet schema mismatch with canonical | DE_KT's merge might produce wrong indicator values | Schema-diff test before enabling any allowlist setup |
| Partial bar's close is wildly off from actual close | A whitelisted setup promotes a bad signal | Per-setup allowlist starts empty + audit gate; paper-trade soak before live |
| PF's 1-line branch has a typo | PF returns False incorrectly → marker never written → signal expires | Unit test + code review; 1 line is inspectable |
| Both DE and DE_KT tasks enabled simultaneously by mistake | Double-promotion of signals → duplicate orders | Add guard in both bat files: refuse to start if the other lock file exists |

## 9. Risk mitigation assessment

### Fully mitigable (standard patterns, just engineering)

| Risk | Why it's solvable |
|---|---|
| **WS disconnects silently** | Every WS lib has `on_disconnect` callback. Heartbeat file + 10s staleness alert is boilerplate. Builder exits → supervisor restarts. Zero residual risk. |
| **Concurrent parquet corruption** | Atomic rename (`os.replace`) is all you need. pandas tolerates it because the read sees either old or new inode, never partial. Windows supports atomic rename the same way. |
| **Subscription leak** | Every 30s, compare subscribed set to pending pool set, subscribe/unsubscribe diff. Cheap, reliable. |
| **Ticker count unbounded** | Pending pool is structurally capped by signal volume (usually <10). Hard cap at 50 is paranoia insurance. |
| **Canonical parquet overwrites tick row later** | Not a risk — it's the design. Canonical wins post-close. Eventual consistency is correct behavior. |
| **Startup gap (mid-bar restart)** | On boot, fetch the in-progress bar once from `historical_data` to seed the in-memory aggregator, then take over. Standard cold-start pattern. |

### Mitigable with real engineering cost

| Risk | What it costs |
|---|---|
| **Indicators missing in scratch parquet** | AVWAP needs session-VWAP cumulatives, RS needs index. Can't just aggregate ticks and have these. Three paths: (a) **Duplicate indicator logic in the builder** — high maintenance, drift risk. (b) **DE reads both parquets and merges** — touches every place DE reads indicator columns. (c) **Keep indicators NULL in scratch, change DE to compute on-demand from raw rows when indicators absent** — bounded work but needs careful perf check. Pick (b) or (c), accept 2–4 days of DE refactor. |
| **Token / 8-app rotation for WS** | Kite REST rotation is per-request; WS rotation is per-session (reconnect with new app). Not hard but different from existing PF path — more code. Mitigable with ~1 day of auth plumbing. |

### Mitigable WITH TRADE-OFF (the real decisions)

| Risk | The trade-off |
|---|---|
| **Partial bar → wrong DE decision on close-based setups** | **This is the one that actually matters.** Two levers: (1) Tick-count gate ("bar has ≥ 100 ticks, probably representative") — heuristic, can be wrong when a late-bar spike hits after the gate fires. (2) Bar-age gate ("wait until 4m30s into bar") — near-certain correctness but kills ~90% of the latency win (marker at `entry_ist+4m30s` instead of `entry_ist+1s`). **No mitigation gets BOTH entry_ist+1s latency AND guaranteed-correct close.** Choosing between speed and certainty. |
| **First-tick-of-bar delay for illiquid tickers** | Liquid names tick within 100ms. Illiquid might not tick for 3–10s into the bar. Mitigation: measure tick arrival on the actual ticker universe first. If low-volume names appear in pending pool regularly, KiteTicker won't help them — fall back to `historical_data` for those. |

### Not actually risks (on inspection)

| "Risk" | Why it's not |
|---|---|
| **Kite WS instrument-subscription limits** | Kite Connect v3 allows ~3000 instruments per session. Pending pool is <10. Non-issue. |
| **Clock drift from exchange time** | PF already NTP-syncs via supervisor. Extend the same check to builder. Done. |

### Summary

- **6 of 10 risks**: fully mitigable, cheap.
- **2 risks**: mitigable, real engineering cost (indicators, WS auth rotation).
- **2 risks**: mitigable only via trade-off — speed vs certainty.

The trade-off risk (partial-bar correctness) is the one that decides whether this project is worth building. **The Phase 4 DE audit resolves this question.**

## 10. Phases and effort

| Phase | Scope | Effort | Risk to production |
|---|---|---|---|
| **0. Pre-work** | DE audit: classify each setup as "needs close" / "partial OK" / "partial+gate" | 1 day | None |
| **1. PoC** | Standalone tick→bar aggregator, validate OHLCV vs historical_data | 1-2 days | None |
| **2. Builder daemon** | Production daemon with subscription management, reconnect handling | 3-5 days | None (not wired in) |
| **3. Runtime paths + PF branch** | Trivial additions | 0.5 day | Minimal (1-line PF change) |
| **4. DE_KT fork** | Copy DE, modify 7 parquet-read sites + add allowlist gate | 3-4 days | None (not enabled by default) |
| **5. Scheduler + stop-script additions** | New disabled tasks, updated stop list | 0.5 day | None (tasks disabled) |
| **6. Stage 1-2 rollout** | Deploy code + start builder, keep DE_KT disabled | Same-day | None |
| **7. Stage 3 rollout** | Enable DE_KT with empty allowlist | Same-day | None (allowlist empty) |
| **8. Stage 4 rollout** | Opt in setups one by one | 1-2 weeks calendar | Scoped per setup |

**Total dev effort: 9-13 days. Total calendar (including soaks): 3-5 weeks.**

## 11. Runtime environment additions

New env vars (for `bat/run_eqidv2_tick_bar_builder_v16_5min.bat` and PF/DE_KT bats):

```bat
set "EQIDV2_TICK_SCRATCH_DIR=C:\TradingData\eqidv2\stocks_indicators_5min_eq_live_tick"
set "EQIDV2_TICK_FLUSH_INTERVAL_SEC=1"
set "EQIDV2_TICK_PENDING_POLL_SEC=1"
set "EQIDV2_TICK_MAX_SUBSCRIPTIONS=50"
set "EQIDV2_TICK_WS_RECONNECT_DELAY_SEC=2"
set "EQIDV2_TICK_WS_MAX_RECONNECTS=100"
```

## 12. Success criteria

- **Latency**: for whitelisted setups, p50 executor order at `entry_ist + 5s`, p99 at `entry_ist + 10s`
- **Correctness**: over 5 trading days in Stage 3 (empty allowlist), zero decision diff vs Stage 2 baseline
- **Stability**: zero unhandled KiteTicker disconnects reaching PF verification path (fallback always caught)
- **No regression**: pre-entry lag=1 promotion (T+56s SEE-write → T+60s promote) still works when setup NOT whitelisted

## 13. The honest bottom line

**This plan is fully backward-compatible IF you follow the staged rollout.** At every stage you can flip two scheduler tasks and be exactly where you are today. The only place where decisions actually change is Stage 4, when you explicitly opt a setup into partial-bar mode — and that decision is per-setup, reversible, and auditable.

**What guarantees no hamper to current detection/entries:**
1. Original DE file is never touched (it's forked, not modified)
2. PF's 1-line branch falls through to existing code when scratch is empty
3. Shared state (pool, markers, confirmed signals) has identical format across modes
4. `PARTIAL_BAR_ALLOWED_SETUPS = {}` preserves every current decision
5. Scheduler-task switch gives instant rollback

**What could still go wrong:** Stage 4 (opt-in) is where real-world partial-bar decisions happen. Every setup added to the allowlist must first pass the Phase 4 audit and a paper-trade soak. That's the only place judgment is required — the infra plumbing is safe.

## 14. Proposed next step

**Do the DE audit first.** 2–3 hours reading each setup's rescan logic in `eqidv2_detection_engine_v16_5min.py` and classifying as "needs close" / "can use partial" / "can use with tick-count gate". That determines whether this is a 5-day or 3-week project, and whether the latency win is 30s or 50s.

Do not begin Phase 1 coding until the audit is done.

## 15. Related docs

- [strategy_map_v16_5min.md](strategy_map_v16_5min.md) — canonical reference for the pipeline's slot clock and lag=1/lag=2 flows. The KT plan sits on top of this model.
