# V16 5min Live Monitoring Tracker — 2026-04-22

Live monitoring by Claude until market close (15:30 IST). Updated each cycle.

## Pipeline processes (snapshot 09:25 IST)

| Process | PID | Supervisor PID | Status | Notes |
|---|---|---|---|---|
| log_dashboard_server.py | 31244 | — | LISTENING :8787 | auth required (token wraps include literal quotes — `--api-token \"eqidv2\"` in launch cmd) |
| eqidv2_eod_scheduler_for_5mins_data_live_minimal.py | 42420 | 6252 | RUNNING | restart_count=0, idle since 09:05 ts (heartbeat) |
| eqidv2_signal_engine_v16_5min.py | 43240 | — | RUNNING | restarted at 09:17 (supervisor.log size > 0) |
| eqidv2_pending_data_fetcher_v16_5min.py | 47992 | — | RUNNING | restart_count=0 |
| eqidv2_detection_engine_v16_5min.py | 45504 | 36584 | RUNNING | 3 startup banners visible — restarted before stabilising |
| avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min.py | 45856 | — | RUNNING | heartbeat OK every ~5s |
| avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py | 46648 | 4644 | RUNNING | open=0, no trades yet — log silent since 09:00:48 (no signals) |
| zerodha_kite_export.py | 34044 | — | RUNNING | live mode, poll 90s |

Pre-open healthcheck @ 09:07: **PASS 24/24**. Authentication v2 SUCCESS @ 08:57.

## Issues found this session

### [I-1] CRITICAL — 09:15 slot lost (Stage 1 marker_incomplete + Stage 2 NF_STALE)
- **Where:** signal engine + detection engine
- **Symptom (signal engine line 429):** `[ABORT] LF_INCOMPLETE slot=09:15 reason=marker_incomplete tickers_written=1033/1033 partition_failures=8 verify_failed=1033 duration_ms=10275`
- **Symptom (detection engine):** `[ABORT] NF_STALE slot=09:15 waited=90.0s timeout=90s marker=...nifty_ready_20260422_0915.json reason=no_nf_slot_ready_marker`
- **Impact:** First slot of the day skipped. No signals scanned at 09:15. 09:20 and 09:25 ran clean (raw_short=0, raw_long=0).
- **Root cause hypothesis:** Although tickers_written=1033/1033, every ticker failed `verify_failed=1033` and 8 partition_failures — suggests freshness-marker write race or filesystem flush issue at the very first slot. The Nifty ready marker was likewise never written, so detection engine independently aborted.
- **Status:** Recovered automatically on 09:20 slot. No restart needed. Worth investigating LF marker race in pre-open period.

### [I-2] Detection engine restarted ~3× during startup
- **Where:** eqidv2_detection_engine_v16_5min_2026-04-22.log
- **Symptom:** 3 separate "EQIDV2 V16 5min DETECTION ENGINE (Stage 2)" banners; supervisor `restart_count=0` though, so restarts happened pre-supervisor or were graceful self-resets.
- **Plus:** `[STARTUP] signal_id proof not yet written by Stage 1; retrying in 3s (budget=60s)` then passed.
- **Impact:** None — recovered. But explains worker_stale_s=360 in supervisor view.

### [I-3] Live executor (PAPER_FALSE) idle_sec ≈ 1370s in heartbeat
- **Where:** avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.heartbeat
- **Symptom:** Last log line was 09:00:48 ("Watchdog started"). Process is alive (heartbeat refreshes). Idle is normal — no new signals to act on yet.
- **Status:** Will stop being an issue as soon as first signal fires. Re-evaluate after 10:00 if still no progress.

### [I-4] Dashboard token auth — minor docs note
- Launch cmd: `--api-token \"eqidv2\"` — the actual token sent must include the literal double-quotes. `?token=eqidv2` returns 401.

### [I-7] CRITICAL — Same 6 long tickers re-detected every slot with stale entry_bar (perf + bug)
- **Where:** signal engine — `[SKIP] SE_PAST_SLOT` repeated for IOLCP, VINATIORGA, VOLTAMP, GRANULES, ENGINERSIN, NMDC across slots **09:45, 09:50, 09:55, 10:00** (lines 976-1032 + 1054-1060). BANSALWIRE SHORT now also showing same pattern (slot 10:00 entry_bar 09:55-10:00).
- **Symptom:** signal engine re-emits the same setup with the same (already-past) entry_bar each slot. F3 guard drops them every time, but the work and the noise repeat indefinitely.
- **Hypothesis:** raw scanner is reading the window without advancing the trigger-bar pointer for tickers that already triggered earlier — or the dedupe key isn't including (ticker, entry_slot) so re-emission isn't suppressed.
- **Impact:** (a) wasted scan + filter work every slot; (b) false-positive log noise hides real issues; (c) if F3 guard ever loosens, would create duplicate orders.
- **Suggested fix:** add a per-(ticker, side, entry_slot) suppression in signal engine after the first SE_PAST_SLOT drop OR persist a "consumed" set across slots.
- **Status:** open — not blocking trading today (F3 guard catches it) but high-priority cleanup.

### [I-6] WARN — Slot 09:55 lost 6 long signals to F3 guard (entry_slot < now)
- **Where:** signal engine — `[INFO] SE dropped 6 row(s) with entry_slot < now (F3 guard).`
- **Tickers dropped:** IOLCP, VINATIORGA, VOLTAMP, GRANULES, ENGINERSIN, NMDC (all LONG)
- **Symptom:** scan at 09:56:04 detected patterns whose entry bars were 09:35–09:50. F3 guard correctly dropped them.
- **Root cause:** raw scan finished too late for these tickers — patterns detected on bars already past the actionable entry window. Confirms the perf wins listed in §Speed-up analysis (especially DE parquet cache + Exec batched LTP) would directly recover signals here.
- **Survivors:** raw_short=1, raw_long=6 → new_pending=1 (the one short or one in-window long).
- **Status:** open — not a process failure but a real signal-loss issue. Direct revenue cost.

### [I-5] ADVISORY — worker_stale_s frozen at 360 across multiple workers
- **Where:** detection_engine, signal_engine, pending_data_fetcher all show `worker_stale_s=360` in both .status and .heartbeat
- **Symptom:** `idle_sec` moves correctly (e.g. detection 116s, signal 56s, fetcher 70s) but `worker_stale_s` is stuck at 360 across cycles
- **Impact:** Cosmetic — supervisor staleness metric unreliable. The actual workers are healthy (logs advancing every 5min slot). Don't trust `worker_stale_s` alone for liveness — use `idle_sec` or log timestamps.
- **Status:** Carry-over from startup; will continue to monitor whether it ever drops.

### [I-8] CRITICAL — CEIGALL LONG lost to ENTRY_SKIPPED_STALE_DETECTION (pipeline race)
- **Where:** executor log at 10:20:20 — `[STALE.DETECT] Skipping CEIGALL LONG: detected 318s after entry slot (threshold 300s)`
- **Chain:** signal_bar=10:10 → entry_bar=10:15 → SE wrote pending at **10:16:04** (slot 10:15 scan done) → DE saw new pending at 10:16:19 but `waiting_ready_marker` → PF's 10:15:02 cycle had `pending_tickers=0` (SE hadn't written yet), so NO fetch → PF's next cycle at 10:20:02 finally fetched slot 10:15 data → DE promoted at 10:20:06 (`CEIGALL LONG -> CONFIRMED written=1`) → exec got it 10:20:20 → **318s after 10:15 entry = over 300s stale threshold → REJECTED.**
- **Root race:** PF runs at x:15:02 (2s after slot close) BEFORE SE can finish scanning and writing pending (SE completes at ~x:16:04). So PF's slot-aligned cycle always misses the just-generated pending for that slot. PF's next cycle is 5 min later → full slot delay → exec staleness guard fires.
- **Impact:** DIRECT REVENUE LOSS. Every in-window long like CEIGALL will hit the same stale guard. `quality_score=7.4` is a high-quality setup. Today's first tradable signal was killed by pipeline orchestration, not strategy.
- **Suggested fix (pick one or combine):**
  - (a) Drop PF cadence to 30s and/or make it re-poll pending list right after SE slot scan window (push PF to x:16:10).
  - (b) Make PF always pre-fetch the just-closed bar for the universe (not only pending list) so marker is always ready when DE arrives.
  - (c) Raise exec `STALE.DETECT` threshold only when the detection lag is entirely inside the next 5min bar (signal still actionable on that bar's close), OR base staleness on `entry_bar end` not `entry_bar start`.
  - (d) Event-trigger PF on pending-file mtime change (fs watcher).
- **Status:** OPEN — urgent. Same category as I-6 but manifested at DE→Exec handoff instead of at SE.

## Verdict on I-8 mitigation (`EQIDV2_LATE_DETECTION_MAX_LAG_SEC=900`) — REVISED 10:42 IST

**Summary: the 900 s override is the correct fix, not a band-aid. The 300 s default was actively misclassifying normal pipeline lag as staleness.**

### Why 300 s was wrong (signal-timing math)

V16 5min signal timeline for CEIGALL-class signals:
- `signal_datetime` = signal bar's **open** timestamp (10:10:00 = signal bar runs 10:10→10:15)
- `signal_entry_datetime_ist` = entry bar's open (10:15:00) — entry is at next bar open by design (lag = +1 bar)
- Pattern can only be confirmed **after** signal bar closes → earliest detection ≥ 10:15:00
- SE scan runs at slot+45 s = ~10:16:04 (this is the floor on pending write)
- DE polls every 5 min on the :05/:10/:15 tick → if SE wrote at 10:16:04, DE picks up at **10:20:06**
- Executor receives → 10:20:18

**Result:** lag-from-entry-slot is **306–318 s on the happy path** for any signal where SE finishes scanning after the DE cycle's tick. That is normal pipeline behavior, not staleness. The 300 s threshold was rejecting valid signals before SLIP GATE could even check the price.

### Why 900 s is correct
- `lag = -1`: detected before entry bar opens (impossible in live, only replay) → still accepts
- `lag = 0`: detected during entry bar (very fast pipeline) → accepts
- `lag = +1` bar (~300–600 s): detected at next DE cycle (the typical case) → accepts ← **CEIGALL was here**
- `lag = +2` bars (~600–900 s): detected when PF/DE catches a missed cycle → accepts
- `lag = +3` bars or more: signal too old, rejects

The actual staleness question — "has price moved too far from `entry_price` to honor the setup?" — is correctly answered by **SLIP GATE** at [`_entry_price_within_retry_band` (line 203-209)](eqidv2/avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py#L203-L209), which compares LTP to `signal_entry_price` with a 0.30 % directional band. That's the right gate. The time-based 300 s guard was redundant AND wrong.

### Caveats (still worth doing later, but not blocking)
1. **SLIP GATE is directional**, not symmetric. For a LONG, it blocks LTP > entry × 1.003 (chasing up) but allows LTP < entry (favorable drift). For a slow-decaying setup, that's fine. For a violent reversal it could enter into a falling knife. A symmetric band is a reasonable upgrade but not urgent.
2. **The orchestration race (PF at x:15:02 missing SE's x:16:04 write) is still there.** It doesn't cause incorrect rejections anymore (900 s absorbs the lag) but it does add ~4 min latency to every signal, which means a worse fill price than it could be. Worth fixing for execution quality, not for correctness.

### Verification plan for today
Watch `live_trades_2026-04-22_v16_5min.csv`: for any future trade with `lag_from_signal_sec ∈ [600, 900]`, confirm the trade entered (no `ENTRY_SKIPPED_STALE_DETECTION`) and that `filled_price` is within 0.30 % of `entry_price` (SLIP GATE working). If both true → fix is validated.

## Speed-up analysis (PF / DE / Executor) — 09:35 IST

Source-file scan of the three live components (PF=Pending Data Fetcher, DE=Detection Engine, Exec=avwap_trade_execution). All citations are file:line.

### Top 3 highest-impact wins (≤30 min each)
1. **DE — cache parquet reads per slot** ([eqidv2_detection_engine_v16_5min.py:1366-1403](eqidv2/eqidv2_detection_engine_v16_5min.py#L1366-L1403)). Freshness check + drift-hash both reopen the same parquet per ticker. Build `{ticker: df}` once at slot start, reuse. **~40-60 ms / slot.**
2. **Exec — batch LTP fetches** ([avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py:2052-2079](eqidv2/avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py#L2052-L2079)). `_safe_get_entry_ltp()` calls `reader.ltp(NSE:TICKER)` per signal serially. Replace with single `kite.quote(symbols=[…])` for the burst. **~400 ms when ≥3 signals fire concurrently.**
3. **PF — drop FETCH_INTERVAL_SEC 60→30 s** ([eqidv2_pending_data_fetcher_v16_5min.py:108, 923, 1057](eqidv2/eqidv2_pending_data_fetcher_v16_5min.py#L108)). Pending bursts arriving mid-slot wait up to 60 s for next fetch. Pool is small so cost is trivial. **Up to 30 s latency cut on bursty arrivals.**

### PF — Pending Data Fetcher
- **[E-PF1]** Token resolution loop is fine ([line 679-686](eqidv2/eqidv2_pending_data_fetcher_v16_5min.py#L679-L686)) — already O(1) per ticker against the global cache. No action.
- **[E-PF2]** Retry backoff 1s/2s/4s ([line 726-771](eqidv2/eqidv2_pending_data_fetcher_v16_5min.py#L726-L771), const at [line 106](eqidv2/eqidv2_pending_data_fetcher_v16_5min.py#L106)) burns up to 7 s before marker is written. Switch to 0.1/0.5/1 s adaptive — **saves ~4-5 s** on first-attempt-flaky cycles.
- **[E-PF3]** Older-slot parquet presence check ([line 514-540](eqidv2/eqidv2_pending_data_fetcher_v16_5min.py#L514-L540), called from [line 565](eqidv2/eqidv2_pending_data_fetcher_v16_5min.py#L565)) reads each parquet for each (ticker, slot). 8 tickers × 3 slots = 24 file opens. Add a 100-entry LRU cache keyed by ticker+ts. **~200-400 ms** per cycle when many older slots are checked.
- **[E-PF4]** Slot loop sleep 60 s — see top-3.

### DE — Detection Engine
- **[E-DE1]** Parquet re-reads — see top-3.
- **[E-DE2]** Nifty marker poll loop ([line 632-650](eqidv2/eqidv2_detection_engine_v16_5min.py#L632-L650)) is `min(0.5s, remaining)` over a 90 s timeout = up to 180 stat calls. Today's [I-1] NF_STALE abort came out of this. Two levers: drop `NF_READY_POLL_SEC` 0.5→0.2, **and** consider proceeding with a degraded `allow_long/allow_short=True` after 30 s instead of full abort. **Saves a whole slot when the NF feed is slow.**
- **[E-DE3]** Repeated `pending_sigs` filtering ([line 1179, 1233, 1257, 1286, 1333, 1447, 1980, 2045, 2083](eqidv2/eqidv2_detection_engine_v16_5min.py#L1179)). Compute `pending_set = {sig_id…}` once per slot iteration, reuse. **~5-10 ms per 50-signal slot** — small but free.
- **[E-DE4]** `live_v16._scan_slot()` ([line 1479-1494](eqidv2/eqidv2_detection_engine_v16_5min.py#L1479-L1494)) runs full universe scan even when only N pending tickers are interesting. Refactor to accept an explicit ticker subset. **10-20% slot compute** (~50-100 ms).
- **[E-DE5]** Per-event JSONL append in lifecycle ([line 1416-1429, 1635-1646](eqidv2/eqidv2_detection_engine_v16_5min.py#L1416-L1429)) = 20-30 syscalls per slot. Buffer to a list and `writelines()` once at end of slot. **~50-100 ms.**

### Executor
- **[E-EX1]** Per-signal serial LTP — see top-3.
- **[E-EX2]** CSV signal poll re-reads file every cycle ([line 3795 / 3762](eqidv2/avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py#L3762), called from [3069](eqidv2/avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py#L3069)). Cache by `(path, mtime)` → only re-parse on change. **~50-100 ms per heartbeat** (every 5 s today, so ~1-2% CPU constant).
- **[E-EX3]** `get_tick_size()` fallback ([line 729, 743](eqidv2/avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py#L729)) re-runs full `kite.instruments("NSE")` (~500 ms) just to find one missing symbol. Replace fallback with single `kite.quote("NSE:TICKER", fields=["instrument_token","tick_size"])`. **~400 ms saved when triggered.**
- **[E-EX4]** N concurrent trade threads each call `kite.orders()` independently. Move to a single 500 ms-cached background poller. **~900 ms** when ≥10 open trades.
- **[E-EX5]** Main dispatch loop launches threads serially ([line 3071, 3824-3969](eqidv2/avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py#L3071)). On a 20-signal burst, pre-filter for capital+capacity, then `ThreadPoolExecutor(max_workers=4)` for thread creation. **~500 ms per burst.**

### Quick wins worth trying first
- **DE parquet cache** — local change in `_scan_slot` setup, low risk.
- **Exec CSV mtime cache** — wrap `read_signals_csv_multi` in a tiny memoizer.
- **PF retry backoff** — three constants at line 106; just lower them.

Total realistic headline: **~1-2 s shaved off the per-slot critical path** plus elimination of one entire slot loss when NIFTY feed is slow.

## Resolved / not-issues
- (none yet)

## SE Raw-Pool log glossary (slot output)

For lines like:
```
[SCAN] Done: raw_short=5 raw_long=14 scan_elapsed=7.8s total_elapsed=18.5s
[INFO] SE F3 suppressed 3 row(s) previously dropped (no re-log).
[SIGNAL_ENGINE] slot=10:40 raw_short=5 raw_long=14 new_pending=0 (16 deduped) total_pending=0 total_elapsed=63.6s
```

| Field | Meaning |
|---|---|
| `raw_short / raw_long` | Tickers that fired the entry condition this slot across the 1044-ticker universe (sharded x10). Pre-filter, pre-dedup. |
| `scan_elapsed` | Just the parallel sharded scan portion. Healthy ~3-8 s. |
| `total_elapsed` *(scan line)* | Scan + reduce + filter setup. Healthy <20 s. |
| `SE F3 suppressed N row(s)` | Zombie-suppression: signal IDs in the persistent `dropped_past_slot_ids` set (entry slot already past at first emission) -- silently dropped without re-logging. This is the **I-7 fix at work**; growing slowly = healthy. |
| `new_pending` | Net adds to the pending-pool JSON this slot (after F3 suppress, dedup, F3 fresh-drop). What the DE will see next cycle. |
| `(N deduped)` | Of the post-F3 candidates, how many were already in the pool from earlier slots. |
| `total_pending` | Pool size after this slot's adds and the DE's consumes since last slot. Often 0 if DE keeps up. |
| `total_elapsed` *(SIGNAL_ENGINE line)* | Whole slot: 45 s slot+45 wait + freshness probe + scan + write. Healthy ~63-67 s. |

Worked example (slot 10:40):
- 19 raw candidates (5 SHORT + 14 LONG) from the scan
- 3 of those 19 were in the zombie set -> silently dropped -> leaves 16
- All 16 already in pool from earlier slots -> "16 deduped" -> `new_pending=0`
- Pool was 1 before slot, DE consumed 1 (NEOGEN parity-filtered in DE 10:35 cycle), so `total_pending=0`

`new_pending=0` for many slots in a row is normal once the day's setups have all been seen -- every subsequent re-emission is a dedup.

## End-to-end latency target — 5–10 s from bar close to order (analysis 11:09 IST)

User goal: shrink PF → DE → executor pipeline so entry orders fire **within 5–10 s of the signal bar closing**.

### Current measured timing (CEIGALL today; signal bar 10:10 → entry slot 10:15)

| Stage | Wall time | Lag from entry-slot open | Why this lag |
|---|---|---|---|
| Signal bar closes | 10:15:00 | t+0 | OHLC complete |
| PF publishes ready marker | 10:15:02 | +2 s | already fast |
| SE detects + writes pending | 10:16:04 | +64 s | `[WAIT] Delaying 45s for 5min slot offset` then sharded scan (~16 s for 1044 tickers × 10 shards) |
| DE picks up marker | 10:20:06 | **+5 min** | DE only sweeps once per 5-min slot (next was 10:20) — **the killer** |
| DE writes detected CSV | 10:20:18 | +12 s after pickup | scan + parity + write |
| Executor places order | 10:20:20 | +2 s | watchdog already fast |

**Total: 5 min 20 s from entry-slot open.** ~5 min of that is the DE sweep gap (idle wait), not actual work. Real compute = ~30 s.

### What is reachable with code-only changes (no architecture rewrite)

| # | Change | Saves | Effort | Risk |
|---|---|---|---|---|
| 1 | **DE: file-watch the PF marker (event-driven)** instead of slot-poll. Triggers as soon as marker file appears. | ~5 min → ~2 s | ~30 min | Low — same code path, different trigger |
| 2 | **SE: shorten `[WAIT] Delaying 45s for 5min slot offset`**. PF proves data ready at +2 s; replace with 5–10 s probe loop with early-exit. | ~35 s | ~20 min | Low — already have probe machinery |
| 3 | **SE: cache last-bar features per ticker** so per-slot scan only re-evaluates tickers whose latest bar changed. | ~13 s of 16 s scan | ~1–2 h | Medium — needs cache invalidation |
| 4 | Executor: shave order-placement to <500 ms (Kite session already pre-warmed) | marginal | trivial | Low |

**Realistic floor with all four: ~10–15 s from bar close to order placed.** That gets very close to the 10 s target.

### What strictly requires architecture change for ≤10 s

Single-digit-second latency requires **tick-driven entry**, not bar-close-driven. Two options:
- **Provisional bar** computed every N seconds from intra-bar ticks
- **Trigger on tick price crossing planned entry level** (skip bar entirely once entry level is known)

Either approach decouples entry from the 5-min OHLC cadence. Larger lift; only path to truly single-digit latency.

### Recommendation

Start with **#1 (DE event-driven marker watch)** — single biggest win, ~30 min effort, no logic risk. Re-measure. If still > target, do **#2** (shorten SE delay). Architecture change is only worth it if (#1 + #2) still miss the target.

### [I-9] CRITICAL — Option A shift insufficient: SUZLON lag-2 still `not_in_live_parity_final_set`

DE 11:50:06 cycle (slot=11:45) checked SUZLON LONG `A_MOD_CLOSE_CONTINUATION_BREAK` (trigger=11:30, entry=11:40, lag=2) at the post-Option-A shifted slot 11:45 (source_slot 11:40 + 5min). Result: still **FILTERED `not_in_live_parity_final_set`**. Runner DID find 1 LONG signal at slot=11:45 (`final_long=1, unmatched_final=1`) but it wasn't SUZLON.

This means the slot-shift hypothesis was incomplete. Possible causes (untested):
1. Runner fires lag-2 signals at a DIFFERENT slot than `source_slot+5min` — perhaps `source_slot-5min` (i.e., where the runner's "next bar = entry bar"), making the +5min shift the wrong direction.
2. Runner's V16 filter recomputes avwap_dist / RS / ADX at the NEW (later) data view — values diverge from SE's signal-time computation → setup classification differs → no match.
3. `signal_id` hash includes setup, and runner reclassifies the trigger under a different setup name (e.g. `A_MOD_BREAK_C1_HIGH` vs `A_MOD_CLOSE_CONTINUATION_BREAK`) → IDs don't match.

UNICHEMLAB lag=1 also FILTERED at slot=11:45 with same reason — lag-1 path (no shift) is unaffected by Option A but still failed parity, suggesting (2) or (3) is at play more broadly.

- **Status:** OPEN — Option A is in production but does NOT close the parity-mismatch problem. Need to either (a) capture runner's intended slot for each pending lag-2 signal by instrumenting `_scan_slot` to log signal_ids it produces, or (b) compare SE's vs runner's avwap_dist for the same trigger bar to confirm hypothesis (2). Defer further changes until hypothesis is confirmed by data.
- **Trades NOT lost yet** — Option A doesn't *break* anything; lag-2 signals were already being filtered with the original code. It just didn't *fix* the issue.

## Cycle log
- 16:06 IST (EOD — monitor loop stopped) — **Market closed 15:30 IST. Clean EOD shutdown confirmed for all pipelines:**
  - **Executor** exited at 15:30:00 (`state=EXITED exit_code=0`, last_log 15:30:00 IST). Daily Live Trade Summary: `Total trades=1, Wins=0, Losses=0, Win rate=0.0%, Total P&L=Rs.+0.00`. Entry cutoff at 15:20:00 blocked new entries (one CSV change at 15:21:59 was correctly rejected with `[SAFETY] New entries blocked: entry cutoff reached at 15:20:00 IST; EOD flatten in progress`). Forced close at 15:20 had no positions to flatten (open=0).
  - **DE** exited at 15:20:00 (`state=EXITED exit_code=0`, `[STOP] Hard-stop 15:20 reached (aligned with executor entry cutoff). Exiting.`). Final stretch 13:05→15:15 all `checked=0 | no pending signals` (26 consecutive empty cycles).
  - **SE** already in overnight sleep since 13:30 (`Past END_TIME. Sleeping until 2026-04-23 09:15:00+05:30`). Internal END_TIME aligns with V17D late-gate filter — NOT a bug.
  - **PF** remained in poll loop to ~15:30 per supervisor design.
  - **Final restart counts**: SE=1 (09:17 supervisor restart, pre-market), DE=0 (post 11:41 manual relaunch — Option A deploy), PF=0, Exec=0. No restarts during trading hours.
  - **Final counters**: raw_short=7 raw_long=35 (last SE scan 13:25), detected_csv=1, pending_csv=39 (38 of which were filtered by parity), live_trades=1, paper_trades=1, open=0.
  - **Session outcome**: ZERO valid LONG entries today. All 38 LONG pending signals were killed by I-9 `not_in_live_parity_final_set`. Only CEIGALL (SHORT→ detected 10:20, rejected STALE.DETECT 318s past entry slot, pre-I-8 fix) reached the executor. Net P&L = Rs.0.
  - **Open items for tomorrow**:
    - **I-9 (CRITICAL)** — parity mismatch bug kills all LONG. Need instrumentation (runner signal_id log) to isolate root cause. Consider reverting Option A's (lag-1)*5min shift since it was shown not to fix parity and is likely semantically wrong (source_slot is already SE scan slot = entry slot, no shift should be needed to match SE emission).
    - **I-8** — 900s late-detection threshold deployed. Needs a real trade landing in [600, 900]s window to validate. Zero trades qualified today.
    - **I-7** — zombie re-detection silenced (SE F3 suppressed visible), confirmed working.
    - **I-5** — cosmetic heartbeat worker_stale_s=360 freeze; worker writers keep updating but supervisor reader doesn't decrement past 360. Not blocking; cleanup when convenient.
  - **Monitor loop stopped** — no further ScheduleWakeup scheduled. ScheduleWakeup was used (dynamic /loop mode), not CronCreate — so no CronDelete needed.
- **EOD CUTOFF (post-15:30 IST)** — monitor loop terminated. Cron jobs `577d08b1` + `ba0e5c12` cancelled via CronDelete. Session-final state already captured in the 15:19 cycle entry below (live_trades=1 STALE-rejected CEIGALL, pending_csv=39 all I-9 parity-killed, 0 valid entries). No further cycles for 2026-04-22. Open carry-over items: I-9 (LONG parity mismatch, dominant session-killer), I-8 (900s threshold awaiting validation trade), I-5 (cosmetic heartbeat freeze). Reverification of bug-fix status from continuation: Bug 4+5 SE writer source_slot anchor — **landed** at signal_engine_v16_5min.py:1298-1354. Bug 7 (DE setup-name table missing `_THEN_`/`_RED_` keys) — **still open**, now sole I-9 blocker. Bug 1 (manifest/effective_config) — open. Bug 2 (open-bar prewarm + session_integrity) — open.
- 15:19 IST (cycle) — **SE EOD sleep confirmed**: SE last scan at slot 13:25 (raw_short=7 raw_long=35 new_pending=0), then `[DONE] Past END_TIME. Sleeping until 2026-04-23 09:15:00+05:30.` SE's internal END_TIME is 13:30 (correlates with V17D_FILTER `late gate after 13:30` — signals after 13:30 would be filtered anyway). Supervisor cutoff_hhmm=1535 is unchanged (governs auto-restart, not scan loop). DE continues cycling healthy: 13:00 filtered JUBLFOOD+MANAPPURAM LONG (`not_in_live_parity_final_set`, unmatched_final=1) — **last DE activity of the day**. From 13:05 to 15:15 (27 consecutive DE cycles) all show `checked=0 | no pending signals`. PF idle in same window. Executor idle since 10:20 (idle_sec=3437). All RUNNING, restart counts unchanged (SE=1, DE=0, PF=0, Exec=0). **Session outcome**: live_trades=1 (CEIGALL rejected STALE.DETECT), paper_trades=1 (same), detected_csv=1 (CEIGALL only), pending_csv=39 (accumulated 38 filtered; all LONG filtered by I-9 parity mismatch). **0 valid entries today.** I-9 is the dominant session-killer. No new issues (no ABORT/ERROR beyond pre-existing 09:15 I-1). 11 min until 15:30 cutoff.
- 12:15 IST (cycle) — DE 12:10:06 cycle: 3 LONG pending (ONMOBILE + SUDARSCHEM + MOTHERSON) all FILTERED `not_in_live_parity_final_set`, 0 promoted, unmatched_final=1 (runner produced 1 LONG final never in pending). DE 12:15:06 cycle: 2 LONG pending (DCXINDIA + DYNAMATECH) both FILTERED same reason, unmatched_final=1. **5 LONG signals burned to parity mismatch in 10 min** — every lag-1 AND lag-2 LONG filtered. I-9 is the dominant bug of the session. SE healthy through slot 12:10 (raw_short=6, raw_long=28/30; slot 12:10 in flight). PF handshake healthy. Executor idle 3251s. All RUNNING, restart counts unchanged (SE=1, DE=0, PF=0, Exec=0). Heartbeat freshness: SE=232s, DE=3.5s, PF=151s, Exec=3251s (expected). Counters: short_csv=MISSING long_csv=1 detected_csv=1 **pending_csv=35 (+2)** live_trades=1 open=0. **No new issues** (all ABORT/ERROR matches are the pre-existing 09:15 I-1). I-9 still OPEN — deferring instrumentation until user decides on revert path.
- 12:10 IST (cycle) — SE through slot 12:05 healthy. Slots 11:45, 11:50, 11:55 scans clean (raw_short=6 raw_long=27/27/27). Slot 12:00: raw_short=6 raw_long=28 → new_pending=1 (29 deduped). Slot 12:05: raw_short=6 raw_long=30 → new_pending=2 (30 deduped). DE currently processing slot=12:05 parity scan (marker arrived at 12:10:06, ready_tickers=3). PF slot 12:10 fetched 3 tickers in 3.06s (all parity OK). Executor (pid 44640) idle since 11:20 CEIGALL rejection. All RUNNING: SE restart_count=1, DE=0 (since 11:41 manual relaunch), PF=0, Exec=0. Heartbeat freshness: SE idle 232s, DE idle 0.2s, PF idle 151s, Exec idle 2867s (expected — no new signals to act on). Counters: pending_csv=33 (+4 since 11:50 cycle), detected_csv=1, live_trades=1, paper_trades=1, open=0. **No new issues** (no ABORT/ERROR/Exception/Traceback beyond the pre-existing 09:15 I-1). I-9 still OPEN — need in-flight instrumentation to confirm hypothesis; deferring change. User raised lag-slot logic clarification (see response below).
- 11:50 IST (cycle) — slot 11:45 ran with Option A active. **SUZLON + UNICHEMLAB both FILTERED parity** (`not_in_live_parity_final_set`). 1 unmatched_final LONG (runner found a signal not in pending pool). DE V16_FILTER LONG: 2→1 raw→final (-1 dist 1.0-1.5ATR dead). New pending entries since 11:34 cycle (pending_csv 26→29): SUPRIYA LONG `B_HUGE_C1_CLOSE_RECLAIM_BREAK` @11:25 (filtered), SUZLON LONG `A_MOD_CLOSE_CONTINUATION_BREAK` @11:30 (filtered post-Option-A), UNICHEMLAB LONG `A_MOD_BREAK_C1_HIGH` @11:40 (filtered). All RUNNING — restart counts: SE=1, **DE=0 (fresh supervisor since 11:41 manual relaunch)**, PF=0, Exec=0. No new ABORT/ERROR/Exception/Traceback/RESTART/RETRY. Counters: short_csv=MISSING long_csv=1 detected_csv=1 pending_csv=29 live_trades=1 paper_trades=1 open=0. **New issue [I-9] OPENED — Option A shift insufficient.**
- 11:48 IST — **OPTION A DEPLOYED** (lag-aware DE parity slot shift). [eqidv2_detection_engine_v16_5min.py:1100-1149](eqidv2/eqidv2_detection_engine_v16_5min.py#L1100-L1149) added local mirror of `SETUP_LAG_BARS` (sourced via `getattr(v16_runner, ...)` so v16_runner remains source-of-truth) + new `_setup_lag_shift(setup) → timedelta((lag-1)*5min)`. `_pending_signal_source_slot` now adds the shift to whichever timestamp source it picks (source_slot / added_at / fallback). Effect: lag-2 setups (`A_MOD_CLOSE_CONTINUATION_BREAK`, `B_HUGE_C1_CLOSE_RECLAIM_BREAK`, `A_PULLBACK_C2_BREAK_C2_*`) now scan parity at slot=entry_slot+5min so the snapshot includes the lag-1 confirmation bar the runner needs. Lag-1 setups unchanged. Lag=-1/999 fall to no-shift (safe default). Also bumped [run_avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min.bat](eqidv2/bat/run_avwap_trade_execution_PAPER_TRADE_TRUE_v16_5min.bat#L17) `EQIDV2_LATE_DETECTION_MAX_LAG_SEC=900` (matches LIVE bat) to accommodate the +5min lag-2 shift without false STALE.DETECT rejection. **Restart**: killed DE worker pid 27180 (taskkill /T → wiped supervisor 36584 + launcher 24360 too); manually relaunched bat in detached PowerShell; new supervisor=37772 launcher=40684 worker=37564 at 11:41:53, restart_count reset to 0. Verification (live): SUZLON LONG `A_MOD_CLOSE_CONTINUATION_BREAK` (trigger=11:30, source_slot=11:40, lag=2) → DE pending_slots show `2026-04-22T11:45:00+05:30` confirming +5min shift active. Parity outcome will land on 11:45 marker (~11:45:06).
- 11:34 IST (cycle) — slot 11:30 in progress, no new SE/DE activity since 11:31. SATIA + GODFRYPHLP still in pending state (will be DE-checked at 11:35:06). PF idle. Executor pid 44640 idle (heartbeat 0s). All RUNNING — restart counts unchanged (SE=1, DE=1, PF=0, Exec=0). Heartbeat ages: SE=360s DE=360s PF=360s (cosmetic I-5). Counters: short_csv=MISSING long_csv=1 detected_csv=1 pending_csv=26 live_trades=1 paper_trades=1 open=0 dropped_past_slot_ids=4. **No new issues** (no ABORT/ERROR/Exception/Traceback/RESTART/RETRY/verify_failed in any of the 4 logs).
- 11:31 IST (cycle) — slots 11:25 + 11:30 in progress. SE pending pool grew: **3 new entries** since 11:24 cycle (pending_csv 23→26): TMPV LONG A_MOD_BREAK_C1_HIGH @11:20 (filtered_parity, `not_in_live_parity_final_set`), **SATIA LONG A_MOD_BREAK_C1_HIGH @11:25** (status=pending), **GODFRYPHLP LONG A_MOD_BREAK_C1_HIGH @11:25** (status=pending). NIFTY context flipped LONG-allowed (`day_move=+0.13% rs=+0.03%`, BOTH or LONG mode). PF healthy (idle, ts 09:38:20 stale but expected — no fresh pending arrivals into PF window since restart). DE 11:20/11:25 cycles: parity-filtered TMPV; SATIA + GODFRYPHLP still in pending state (lag-1 setups, will be parity-checked at 11:30 DE cycle). Executor pid 44640 idle (heartbeat 0s, no new orders). All RUNNING — restart counts unchanged (SE=1, DE=1, PF=0, Exec=0). Heartbeat ages: SE=360s DE=360s PF=360s — frozen at 360 (cosmetic I-5, real worker ts within last cycle). Counters: short_csv=MISSING long_csv=1 detected_csv=1 pending_csv=26 live_trades=1 paper_trades=1 open=0 dropped_past_slot_ids=4. **No new issues.** Watch SATIA/GODFRYPHLP at 11:30 DE cycle — if either promotes to detected, executor will trigger first non-CEIGALL trade attempt today.
- 11:24 IST (cycle, manual) — no slot transition since last cycle (still in 11:20→11:25 window). State unchanged: SE total_pending=0, DE last activity 11:20:05 `no pending signals`. PF idle. Executor pid 44640 idle, heartbeat 0s. All RUNNING — restart counts unchanged (SE=1, DE=1, PF=0, Exec=0). Heartbeat ages: SE=6s, DE=1s, PF=212s — all healthy. Counters: short_csv=MISSING long_csv=1 detected_csv=1 pending_csv=23 live_trades=1 paper_trades=1 open=0 dropped_past_slot_ids=4. **No new issues.**
- 11:22 IST (cycle) — slot 11:20 clean. SE 11:20: raw_short=6 raw_long=21 → new_pending=0 (23 deduped), suppressed 4. SE total_pending=0. DE 11:10/11:15/11:20 cycles: `checked=0 | no pending signals`. PF idle. Executor pid 44640 idle, heartbeat 0s. All RUNNING — restart counts unchanged (SE=1, DE=1, PF=0, Exec=0). Heartbeat ages: SE=9s, DE=2s, PF=170s — all healthy. Counters: short_csv=MISSING long_csv=1 detected_csv=1 pending_csv=23 live_trades=1 paper_trades=1 open=0 dropped_past_slot_ids=4. **No new issues** (no ABORT/ERROR/Exception/Traceback).
- 11:19 IST (cycle) — slot 11:15 clean. SE 11:15: raw_short=6 raw_long=21 → new_pending=0 (23 deduped), suppressed 4. SE total_pending stays at 0 (pending pool fully drained — no fresh inflow). DE 11:10 + 11:15 cycles: `checked=0 | no pending signals`. PF idle. Executor pid 44640 idle, heartbeat 0s. All RUNNING — restart counts unchanged (SE=1, DE=1, PF=0, Exec=0). Heartbeat ages: SE=14s, DE=0s, PF=223s — all under 600s threshold. Counters: short_csv=MISSING long_csv=1 detected_csv=1 pending_csv=23 live_trades=1 paper_trades=1 open=0 dropped_past_slot_ids=4. **No new issues** (no ABORT/ERROR/Exception/Traceback in any of the 4 logs). New cron job ba0e5c12 (every 10 min) replaces 577d08b1.
- 11:12 IST (cycle) — slot 11:10 clean. SE 11:10: raw_short=6 raw_long=21 → new_pending=0 (23 deduped), 0 F3 drops, suppressed 4 (HONASA now in dropped set). DE 11:10:05 cycle: `checked=0 | no pending signals` (last 3 from 11:00 already filtered_parity at 11:05:06). PF idle (no new pending pool entries). Executor pid 44640 idle, heartbeat 0s. All RUNNING — restart counts unchanged (SE=1, DE=1, PF=0, Exec=0). Heartbeat ages: SE=6s, DE=1s, PF=93s — all healthy. Counters: short_csv=MISSING long_csv=1 detected_csv=1 pending_csv=23 live_trades=1 paper_trades=1 open=0 dropped_past_slot_ids=4. **No new issues.** No new ABORT/ERROR/Exception/Traceback. SE total_pending=0 — pending pool fully drained (all prior entries either filtered or expired). I-8 fix still awaiting validation trade.
- 11:07 IST (cycle) — slot 11:05 clean. SE 11:05: raw_short=6 raw_long=21 → new_pending=0 (23 deduped), 1 F3 drop (HONASA SHORT, new ticker), suppressed 3. DE 11:00 cycle (ran at 11:05:06): 3 parity-filtered (CENTRALBK + TIINDIA + ONMOBILE all LONG, all `not_in_live_parity_final_set`), 0 promoted, 1 unmatched_final. PF healthy. Executor pid 44640 idle, no signals to act on (heartbeat 0s). All RUNNING — restart counts unchanged (SE=1, DE=1, PF=0, Exec=0). Heartbeat ages: SE=5s, DE=127s, PF=142s — all under 600s. Counters: short_csv=MISSING long_csv=1 detected_csv=1 pending_csv=23 live_trades=1 paper_trades=1 open=0 dropped_past_slot_ids=4 (HONASA added). **No new issues.** No new ABORT/ERROR/Exception/Traceback in any of the 4 logs. I-8 fix still awaiting validation trade.
- 11:04 IST (cycle) — slots 10:55 + 11:00 clean. SE 10:55: raw_short=5 raw_long=18 → new_pending=3 (17 deduped), suppressed 3. SE 11:00: raw_short=5 raw_long=21 → new_pending=3 (20 deduped), suppressed 3. PF marker handshake healthy: 10:55 + 11:00 + 11:05 markers each fetched 1/3/3 in ≤3.1s. DE 10:55 promoted 0 (1 parity-filtered MTNL-style); DE picked up 11:00 marker on next sweep at 11:05:06 (`pending=3 ready_slots=1 ready_age=306s`) and is scanning. NIFTY context flipped to BOTH (day_move=-0.21%, rs neutral) → both directions allowed. Executor pid 44640 still idle since 10:33 restart, no signals to act on. All RUNNING, restart counts unchanged (SE=1, DE=1, PF=0, Exec=0). Counters: short_csv=MISSING long_csv=1 detected_csv=1 pending_csv=23 live_trades=1 paper_trades=1 open=0 dropped_past_slot_ids=3. **No new issues.** I-8 fix still awaiting first lag ∈ [600, 900] trade to validate; new SE pending may exercise it if DE detection hits 11:05+ window.
- 09:25 IST — initial sweep done. 09:15 slot lost (I-1). All processes alive.
- 09:32 IST — slot 09:30 clean (raw_short=0 raw_long=0, NIFTY day_move=-0.24%, both dirs allowed). No new ABORT/ERROR. Counters: short=MISSING long=MISSING detected=0 pending=0 trades=0 open=0. Live exec idle=1870s (no signals → expected). New: I-5 added (worker_stale_s frozen at 360 — cosmetic).
- 09:58 IST — **first signals fired**. Slot 09:55: raw_short=1, raw_long=6, but **6 longs dropped by F3 guard** (entry slot < now). 1 survivor → new_pending=1, total_pending=1. PF slot 09:50 wrote marker cleanly (2.8 s, 2 fetched). DE currently `waiting_ready_marker` for 09:55 (expected). Counters: short=MISSING long=MISSING detected=0 pending=6 (cumulative csv) trades=0 open=0. Executor still idle (no surviving signal entered exec window yet). New: **I-6** logged for the 6 dropped long signals — direct cost of pipeline latency, this is what the speed-up plan would recover.
- 10:01 IST — slot 10:00 raw_short=2 raw_long=6, **7 dropped by F3 guard** (BANSALWIRE SHORT now also re-emitted with stale entry_bar). 1 survivor → new_pending=1. DE picked up 09:55 marker at 10:00:06 — its only ticker (BANSALWIRE SHORT) was filtered by parity (`not_in_live_parity_final_set`). PF cycle 10:00 cleanly fetched 1 ticker. Counters: short=MISSING long=MISSING detected=0 pending=7 (cumulative) trades=0 open=0. Live exec idle=3617s (still no signals to act on). All processes RUNNING, no restarts. New: **I-7** added — same 6 long tickers re-detected every slot since 09:45 (real bug, not just latency). Worker_stale_s still frozen at 360 (I-5).
- 10:53 IST (cycle) — slots 10:45 + 10:50 clean. SE 10:45: raw_short=5 raw_long=14 → new_pending=0 (16 deduped), suppressed 3. SE 10:50: raw_short=5 raw_long=15 → **new_pending=1** (16 deduped), suppressed 3 (pending_csv 16→17, first new pending in 4 slots). DE post-restart awaiting first cycle (next at 10:55:06). PF 10:45 + 10:50 both `pending_tickers=0 waiting for raw pool` (the new SE 10:50 pending added at ~10:50:46, after PF's 10:50:02 cycle — will fetch at 10:55:02). Executor pid 44640 idle, no new signals. All RUNNING. Restart counts: SE=1, **DE=1 (was 0, expected from 10:52 deploy)**, PF=0, Exec=0. Counters: short_csv=MISSING long_csv=1 detected_csv=1 pending_csv=17 live_trades=1 paper_trades=1 open=0 dropped_past_slot_ids=3. **No new issues.** New SE pending will exercise the I-7 + I-8 paths next slot — opportunity to validate lag_from_entry_slot_sec column on whatever DE writes if it promotes.
- 10:52 IST — **detected-CSV lag column rename deployed.** [eqidv2_detection_engine_v16_5min.py:1001-1008](eqidv2/eqidv2_detection_engine_v16_5min.py#L1001-L1008) renamed `lag_from_signal_sec` → `lag_from_signal_bar_sec` and added new `lag_from_entry_slot_sec` (parity with executor's `_detection_lag_seconds`). [eqidv2_detection_engine_v16_5min.py:1036-1063](eqidv2/eqidv2_detection_engine_v16_5min.py#L1036-L1063) computes both. Dashboard mapping updated at [log_dashboard_server.py:1674-1683](eqidv2/log_dashboard_server.py#L1674-L1683) with old name as fallback. DE killed (pid 45504) and supervisor relaunched cleanly (new pid 27180, restart_count=1, start 10:52:16). Dashboard NOT restarted (low-impact; will pick up new column on next natural restart). Verification: next detected signal will write CSV with both new columns; CEIGALL row will get rewritten on that event.
- 10:45 IST (cycle) — slot 10:45 in progress (SE phase=WAIT_DATA after probe_ok ratio=0.99, PF phase=WAIT_PENDING_POOL with 0 pending, DE phase=IDLE). No SE/DE/PF activity since last cycle 10:42 (no new slot completed in 3-min gap). Executor still idle (pid 44640, no new signals). All RUNNING — restart counts unchanged (SE=1, DE=0, PF=0, Exec=0). Counters: short_csv=MISSING long_csv=1 detected_csv=1 pending_csv=16 live_trades=1 paper_trades=1 open=0 dropped_past_slot_ids=3. **No new issues.**
- 10:42 IST (cycle) — slots 10:35 + 10:40 clean. SE 10:35: raw_short=5 raw_long=14 → new_pending=1 (15 deduped), `SE F3 suppressed 3 row(s)`. SE 10:40: raw_short=5 raw_long=14 → new_pending=0 (16 deduped), suppressed 3 (steady). DE 10:30 cycle filtered NEOGEN LONG (parity), DE 10:35 cycle filtered SOTL SHORT (parity), 0 promoted both. PF slots 10:35 + 10:40 each fetched 1 ticker in 3.1 s (healthy). NIFTY mode flipped BOTH at 10:30 (rs=-0.02%) → BOTH at 10:35 (rs=+0.00%) → SHORT_ONLY again at 10:40 (rs=+0.07%, but day_move=-0.35% so directional). Executor pid 44640 idle since 10:33 restart, 0 new signals to act on. All processes RUNNING. Counters: short_csv=MISSING long_csv=1 detected_csv=1 pending_csv=16 live_trades=1 (CEIGALL still the only entry, rejected pre-fix) paper_trades=1 open=0 dropped_past_slot_ids=3. **No new issues. No restarts since 10:33 exec relaunch.** I-8 fix `EQIDV2_LATE_DETECTION_MAX_LAG_SEC=900` loaded; awaiting first lag ∈ [600, 900] trade to validate. Added [SE Raw-Pool log glossary](#se-raw-pool-log-glossary-slot-output) above for `[SCAN] Done` / `SE F3 suppressed` / `[SIGNAL_ENGINE] slot=...` interpretation.
- 10:34 IST (cycle) — slot 10:30 SE: raw_short=4 raw_long=14 → new_pending=1 (14 deduped), `SE F3 suppressed 3 row(s)` (I-7 still clean), 0 F3 drops. DE 10:30 cycle: 2 parity-filtered (IGL LONG, HIRECT LONG), 0 promoted. PF slot 10:30 fetched 2 in 7.3s (slower than 3.3s baseline but well under 20s warn). Executor reloaded 1 signal (CEIGALL) on restart, idle since. NIFTY still `directional_daymove_short` only (long blocked). All RUNNING, restart counts unchanged (SE=1, Exec=0 since restart). Counters: short_csv=MISSING long_csv=1 detected=1 pending_csv=15 live_trades=1 paper_trades=1 open=0 dropped=3. No new issues.
- 10:34 IST — **I-8 mitigation deployed**: added `set "EQIDV2_LATE_DETECTION_MAX_LAG_SEC=900"` to [run_avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.bat](eqidv2/bat/run_avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.bat#L15). Killed old executor tree (cmd 39132 → ps 4644 → cmd 11536 → py 46648, all SUCCESS via taskkill /T /F). Re-launched bat at 10:33:44. New python pid 44640. Log shows clean startup: 8 Kite apps connected, tick sizes loaded, `[RESTORE] signals_today=1 executed=1 closed_today=1 open_restored=0`, `[SAFETY] Broker MIS positions match restored runtime state`, watchdog started 10:33:47. Threshold now allows lag −1 / 0 / +1 / +2 bars (rejects ≥+3). SLIP GATE 0.30% remains as price-drift guard. CEIGALL signal_id `af48405c3a8f72b5` already in `executed_signals_live_v16_5min.json` so it will not be re-attempted.
- 10:29 IST — **I-7 fix visibly confirmed** in two consecutive slots: SE log line `[INFO] SE F3 suppressed 2 row(s) previously dropped (no re-log).` at slot 10:20 and `suppressed 3 row(s)` at slot 10:25 — zombie re-logs eliminated. Slot 10:20: raw_short=4 raw_long=11 → new_pending=3 (9 deduped), 1 F3 drop (LTTS SHORT, a new ticker). Slot 10:25: raw_short=4 raw_long=13 → new_pending=2 (12 deduped), 0 F3 drops (all stale suppressed silently). DE 10:20 promoted 3→0 confirmed (TECHM/IDEAFORGE/IRMENERGY all FILTERED parity `not_in_live_parity_final_set`); DE 10:25 promoted 0 (3 parity-filtered). No NIFTY long allowed (`day_move=-0.47% rs=-0.08%`, directional_daymove_short only). PF slot 10:25 fetched 3 tickers in 3.3s (healthy). Counters: short_csv=MISSING long_csv=1 detected=1 pending_csv=14 live_trades=1 (CEIGALL still only trade, rejected STALE) paper_trades=1 open=0 dropped_past_slot_ids=3. All processes RUNNING — SE restart_count=1 unchanged, Exec restart_count=0, Exec idle 551s (no signals since CEIGALL). No new issues.
- 10:20 IST — **slot 10:15 scan ran at 10:15:45** (post I-7 fix). raw_short=2, raw_long=9 → new_pending=2 (7 deduped — pre-existing zombies VINATIORGA/NMDC/IOLCP/GRANULES/ENGINERSIN silently de-duplicated, no F3 re-log noise). 2 SE_PAST_SLOT logged for NEW stale tickers BIKAJI+VOLTAMP. `dropped_past_slot_ids=3` (persistent set growing correctly). **CEIGALL LONG** (first real signal of the day) promoted by DE at 10:20:06 (`CEIGALL LONG -> CONFIRMED written=1`) — then **REJECTED by executor at 10:20:20 as STALE.DETECT** (318s past 10:15 entry slot vs 300s threshold). Live + paper trade CSVs both show `ENTRY_SKIPPED_STALE_DETECTION` PnL=0. New: **I-8** added (pipeline race between SE-write/PF-cycle caused 5-min DE delay → stale rejection). Counters: short_csv=MISSING long_csv=1 detected=1 pending_csv=12 live_trades=1 paper_trades=1 open=0. All processes RUNNING, no new restarts.
- 10:14 IST — **I-7 FIX DEPLOYED AND VERIFIED LIVE**. Signal engine restarted at 10:12:06 (old pid 43240 killed; new launcher 47684 / worker 41480) to load code change in `_write_pending_pool` (persistent `dropped_past_slot_ids` set keyed by signal_id, short-circuits before F3 guard re-logs). Pending pool JSON `pending_signals_2026-04-22_v16_5min.json` now contains new top-level key `dropped_past_slot_ids` (count=2 at 10:14 — populated at startup restore). signal engine currently sleeping until slot 10:15:00 (scan at 10:15:45). DE cycles 10:10 & 10:15 `checked=0 | no pending signals` (all prior pending were parity-filtered). PF pending_tickers=0 waiting for raw pool. Counters: detected=0 pending_active=9 (JSON state) pending_csv=7 trades=0 open=0. Executor PAPER_FALSE still silent (no signals to act on). No new issues. All processes RUNNING, restart_count SE=1 DE=0 PF=0 Exec=0.
