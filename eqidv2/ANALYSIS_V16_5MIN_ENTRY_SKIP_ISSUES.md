# EQIDV2 V16 5min Live Pipeline — Comprehensive Entry Skip Analysis
**Date:** April 20, 2026  
**Status:** ANALYSIS ONLY — No changes made

---

## Executive Summary

The V16 5min live pipeline has a two-stage architecture (Signal Engine → Detection Engine) that is designed to minimize false entry skips. However, **multiple timing mismatches, configuration inconsistencies, and edge cases** exist that could cause genuine signals to be skipped without legitimate reasons.

**Critical finding:** The pending data fetcher BAT file has a 2-second interval setting which contradicts the 60-second interval specified in the reference document and Python code defaults. This could cause cascading timing issues throughout the pipeline.

---

## PART 1: CRITICAL CONFIGURATION ISSUES

### Issue #1: Pending Fetcher Interval Mismatch (SEVERITY: CRITICAL)

**Problem:**
- **BAT setting** (run_eqidv2_pending_data_fetcher_v16_5min.bat, line 12):
  ```
  set "EQIDV2_PENDING_FETCH_INTERVAL_SEC=2"
  ```
- **Python code default** (eqidv2_pending_data_fetcher_v16_5min.py, line 68):
  ```python
  FETCH_INTERVAL_SEC = int(os.getenv("EQIDV2_PENDING_FETCH_INTERVAL_SEC", "60"))
  ```
- **Reference document** (Section 8, PENDING DATA FETCHER):
  ```
  Pending fetch: ~5-30 tickers × Kite API → ~5-15 seconds per run
  → Can run every 60 seconds without overloading Kite API
  ```

**Impact:**
- Fetcher runs every 2 seconds instead of every 60 seconds
- Creates 30× more API calls to Kite than designed
- May violate Kite API rate limits (could cause 429 errors, IP blocking)
- Creates excessive CPU/network load
- Could cause timing conflicts if a fetch is still in-flight when the next one starts

**Root Cause:**
The BAT was likely modified for testing/debugging and never reverted to production setting.

---

### Issue #2: Pending Fetcher Alignment Timing Conflict

**Problem:**
The pending fetcher has conflicting timing modes:

1. **When ALIGN_TO_5MIN_BOUNDARY=1** (set in BAT):
   - Aligns to 5-minute slots
   - Runs at slot boundary + SLOT_OFFSET_SEC (2 seconds)
   - BUT also sets FETCH_INTERVAL_SEC=2 (fallback sleep)
   - **Result:** The two mechanisms fight for control

2. **Signal Engine timing:**
   - Waits for data ready at: slot_boundary + SLOT_START_OFFSET_SECONDS (45s)
   - Then scans

3. **Detection Engine timing:**
   - Checks every CHECK_INTERVAL_SEC (2 seconds)
   - Fires at: slot_boundary + DETECTION_SLOT_OFFSET_SEC (4 seconds)

**Timing Race Condition:**
```
09:40:00  Slot boundary
09:40:02  Pending fetcher wakes (SLOT_OFFSET_SEC=2) — TOO EARLY
          → At this point, Signal Engine hasn't even written pending pool yet!
          → Pending fetcher reads OLD pending pool from previous slot
          → Fetches data for WRONG tickers
          
09:40:04  Detection Engine wakes (DETECTION_SLOT_OFFSET_SEC=4)
          → Tries to find per-slot ready marker
          → May not exist because fetcher was triggered too early
          → Signals may be filtered as "waiting_ready_marker"

09:40:45  Signal Engine wakes (SLOT_START_OFFSET_SECONDS=45)
          → Writes NEW pending pool entries
          → But Pending Fetcher already ran 43 seconds ago!
          → Those new signals won't be fetched until next 2-second cycle
          → Meanwhile Detection Engine keeps looking for a marker that won't come
```

**Impact:**
Signals generated in early slots (09:40-09:50) could be skipped because the ready marker dependency chain breaks:
1. Pending Fetcher runs too early and doesn't see new signals
2. Detection Engine can't find ready marker for the slot
3. Signals remain in "pending" state, never move to "detected"

---

## PART 2: TIMING SYNCHRONIZATION ISSUES

### Issue #3: Signal Engine → Pending Fetcher Coordination Gap

**Current Flow:**
```
Signal Engine Slot 09:40:00:
  09:40:00  Slot boundary
  09:40:45  Signal Engine wakes (wait for data, then scan)
  ~09:42:00 Scan completes, pending pool written

Pending Fetcher:
  09:40:02  WAKES (2 seconds after slot!)
  09:40:02  Reads pending pool from PREVIOUS slot
  09:40:07  Fetch completes, marker written
  09:41:02  Next cycle (if FETCH_INTERVAL_SEC=2) or waits until next slot

Signal Engine Slot 09:45:00:
  09:45:00  Slot boundary
  09:45:45  Signal Engine wakes, scans
  ~09:47:00 New pending signals added
```

**Problem:**
- When Pending Fetcher runs at 09:40:02, Signal Engine hasn't written the 09:40 pending pool yet (won't happen until ~09:42)
- Fetcher reads pending pool from 09:35 slot
- Fetches data for "stale" signals that Signal Engine is about to update
- Detection Engine gets ready marker for wrong signal set

**Consequence:**
New signals from 09:40 slot are never fetched, causing Detection Engine to skip them with "no_fresh_data" or "parquet_too_old" filters.

---

### Issue #4: Detection Engine Per-Slot Marker Lookup Fragility

**Current Implementation** (eqidv2_detection_engine_v16_5min.py):
```python
per_slot_marker = _load_ready_marker_for_slot(slot_ts, now)
# Looks for exact {YYYYMMDD_HHMM}.ready file
# If not found -> signals for that slot are skipped
```

**Problem:**
The ready marker filename is based on when the Pending Fetcher runs, NOT the slot timestamp:

```python
def _write_ready_marker(tickers, ...):
    now_ist = _now_ist()  # <-- Current wall-clock time
    fname = now_ist.strftime("%Y%m%d_%H%M") + ".ready"  # Uses NOW, not slot
    path = SLOT_READY_PENDING_DIR / fname
```

**Scenario:**
```
Signal Engine generates pending signals for 09:40 slot
Pending Fetcher should run between 09:40-09:45 for that slot
Marker should be named: 20260420_0940.ready or similar

But what if:
- Pending Fetcher runs at 09:41:50
- It writes marker as: 20260420_0941.ready  ← WRONG SLOT TIME!
- Detection Engine looks for: 20260420_0940.ready
- Not found -> signals filtered as "waiting_ready_marker"
```

**Root Cause:**
Ready marker write time uses wall-clock `_now_ist()` instead of the signal's source slot timestamp. If fetcher is delayed or runs between slot boundaries, marker gets wrong filename.

---

### Issue #5: ALIGN_TO_5MIN_BOUNDARY Mismatch

**Detection Engine BAT setting:**
```
set "EQIDV2_DETECTION_ALIGN_TO_5MIN=1"  ← Expects slot-aligned runs
```

**Pending Fetcher BAT settings:**
```
set "EQIDV2_PENDING_FETCH_ALIGN_TO_5MIN=1"  ← Also slot-aligned
set "EQIDV2_PENDING_FETCH_INTERVAL_SEC=2"   ← But then sleep 2s in loop
set "EQIDV2_PENDING_FETCH_SLOT_OFFSET_SEC=2" ← Fire 2s after slot
```

**Problem:**
If both are aligned to 5-minute boundaries but Pending Fetcher runs at SLOT+2s while Signal Engine runs at SLOT+45s:
- They're aligned to same 5-min boundary but with different offsets
- Creates phase mismatch that compounds over time
- After 9 cycles (~45 minutes), they could be desynchronized by entire slot

---

## PART 3: FRESHNESS DEADLINE CALCULATION ISSUES

### Issue #6: Multiple Timestamp Anchors — Inconsistent Fallback Chain

**In _entry_retry_deadline()** (avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py, lines 245-265):

```python
# Priority 1: Stage-2 confirmation timestamp
if base_ts is None:
    base_ts = _parse_ist_signal_ts(
        signal.get("detected_time_ist") or signal.get("logtime_ist")
    )
# Priority 2: Stage-1 pending pool insertion
if base_ts is None:
    base_ts = _parse_ist_signal_ts(signal.get("received_time"))
# Priority 3: Model entry slot (legacy)
if base_ts is None:
    base_ts = _parse_ist_signal_ts(
        signal.get("signal_entry_datetime_ist")
        or signal.get("signal_bar_time_ist")
        ...
```

**Problem #1: Missing Timestamps**
If Detection Engine writes signal CSV but doesn't populate `detected_time_ist` (possible if code path skips it), executor falls back to `received_time` or `signal_entry_datetime_ist`.

- `signal_entry_datetime_ist` is **lag bar close time** — could be 30+ minutes old
- If entry happens 5+ minutes after lag bar close, freshness deadline is immediately stale
- Example:
  ```
  Signal entry datetime: 09:40:00 (lag bar close)
  Detection time (skipped): NOT SET
  Received time: 09:41:50 (pending pool insertion)
  Trade start: 09:46:00
  
  Fallback chain uses signal_entry_datetime_ist (09:40:00) as anchor!
  Deadline = 09:40:00 + 300s = 09:45:00
  Trade starts at 09:46:00 >= 09:45:00 → ENTRY_SKIPPED_STALE_SIGNAL ❌
  
  But it SHOULD use 09:41:50 (received) as anchor:
  Deadline = 09:41:50 + 300s = 09:46:50 → Would be OK ✓
  ```

**Problem #2: _parse_ist_signal_ts() Edge Cases**
The timestamp parsing function might fail silently or return None for:
- Malformed timestamp strings
- Timezone-unaware timestamps
- None/empty string fields
- Inconsistent format (some fields use ISO8601, others use different format)

If parsing fails at Priority 1 and 2, it cascades to Priority 3 (old lag bar time), causing false stale signal skips.

---

### Issue #7: Expiry Time Calculation Inconsistency

**In Signal Engine** (eqidv2_signal_engine_v16_5min.py, lines 285-301):

```python
def _compute_expires_at(slot_ist: datetime, side: str) -> str:
    slot_time = slot_ist.time()
    if slot_time < _SESSION_MORNING_END:  # 11:00
        exp_dt = IST.localize(datetime.combine(today, _SESSION_MORNING_END))
    else:
        if side_u == "SHORT":
            exp_dt = IST.localize(datetime.combine(today, _SESSION_AFTNOON_SHORT))  # 13:30
        else:
            exp_dt = IST.localize(datetime.combine(today, _SESSION_AFTNOON_LONG))  # 13:30
```

**Problem:**
This sets an expiry time, but:
1. Is it used in Detection Engine's expiry check? (Line search shows `expired_window` filter exists)
2. Is it enforced in Executor's freshness check? (NO — executor only uses ENTRY_RETRY_WAIT_SEC=300s window)
3. What happens if a signal is detected at 12:45 but expires_at=13:30? Can it still enter?

**Scenario that could cause spurious skip:**
```
Signal generated at 12:30 → expires_at = 13:30
Detection Engine checks at 13:25 → Not expired yet
Executor sees signal at 13:26 → Still valid
Executor calculates deadline = 13:26 + 300s = 13:31
Executor enters at 13:32 → ENTRY_SKIPPED_STALE_SIGNAL ❌
  (Because deadline was 13:31 but entry was 13:32)

But this signal was legitimately detected! The issue is:
- Detection Engine uses expires_at = 13:30 (session boundary)
- Executor uses detected_time_ist + 300s (independent calculation)
- Two different expiry logics can conflict
```

---

## PART 4: DETECTION ENGINE FILTERING ISSUES

### Issue #8: Per-Slot Ready Marker Dependency Fragility

**In _run_detection_cycle_live_parity()** (eqidv2_detection_engine_v16_5min.py, ~line 812):

```python
for slot_group in slot_groups:
    slot_ts = slot_group['slot']
    slot_signals = slot_group['signals']
    
    # Per-slot marker lookup
    per_slot_marker = _load_ready_marker_for_slot(slot_ts, now)
    if per_slot_marker is None:
        # Skip entire slot! All signals in this slot filtered.
        continue
    
    # Use marker to narrow signal set
    ready_tickers = per_slot_marker.get('tickers', [])
    slot_signals = [s for s in slot_signals if s['ticker'] in ready_tickers]
```

**Problems:**

1. **All-or-nothing filtering:**
   - If per-slot marker doesn't exist, ALL signals for that slot are skipped
   - A single missing marker can skip hundreds of valid signals
   - No partial handling (e.g., scan without marker if it's only missing 1 ticker)

2. **Marker file dependency:**
   - Relies on exact filename: `{YYYYMMDD_HHMM}.ready`
   - If Pending Fetcher writes marker with wrong timestamp (due to wall-clock write time), marker won't be found
   - If marker is deleted or corrupted, no signals pass through

3. **Timing of marker availability:**
   - Detection Engine polls every 2 seconds (CHECK_INTERVAL_SEC=2)
   - Marker must exist before Detection Engine polls
   - If Pending Fetcher hasn't run yet for that slot, marker missing → skip

4. **No fallback mechanism:**
   - If ready marker is missing, no attempt to rescan or retry
   - Just skips the entire slot and waits for next cycle

---

### Issue #9: Stale Data Age Check

**In Detection Engine** (implied by MAX_DATA_AGE_SEC=180 in BAT):

```
set "EQIDV2_DETECTION_MAX_DATA_AGE_SEC=180"
```

**Problem:**
If parquet file is older than 3 minutes, signals are filtered. But:
- If a signal is detected but Detection Engine runs 2+ minutes later
- And Pending Fetcher hasn't run in last 3 minutes (e.g., was busy)
- Parquet could be marked as "stale" even though it's valid for that signal slot

**Scenario:**
```
09:40 Slot: Signal generated
09:42 Pending Fetcher finishes, parquet updated
09:45 Detection Engine runs (5 minutes have passed)
Detection Engine's max_data_age_sec = 180 (3 min)
Parquet timestamp = 09:42
Current time = 09:45
Parquet age = 3 minutes → RIGHT AT THRESHOLD or slightly over
If over 180s by even 1 second → FILTERED OUT
```

---

## PART 5: RACE CONDITION SCENARIOS

### Issue #10: Pending Pool Write → Detection Read Race

**Sequence:**
```
09:40:00  Slot boundary
09:40:45  Signal Engine wakes

[SIGNAL ENGINE]
  09:40:47  Starts scanning 1044 tickers
  ~09:42:00 Scan completes
  09:42:01  Loads pending state JSON
  09:42:02  Adds new signals to JSON
  09:42:03  Writes atomic JSON (temp → rename)
  ← JSON is now visible to Detection Engine
  09:42:04  Writes CSV for dashboard

[DETECTION ENGINE] (checks every 2 seconds)
  09:42:02  Polls pending JSON (file not yet written!)
  09:42:02  Reads OLD state from previous slot
  09:42:02  Looks for per-slot marker from Signal Engine → DOESN'T EXIST
  09:42:02  Increments cycle counter, moves to next check

  09:42:04  Polls pending JSON (NOW newly written)
  09:42:04  Sees new signals
  09:42:04  Looks for per-slot marker (should have been written by Pending Fetcher)
  09:42:04  Marker might not exist yet!
  09:42:04  Increments counter, waits for marker
```

**Root Cause:**
No synchronization between Signal Engine (writes pending pool at ~09:42) and Pending Fetcher (expected to run soon after and write marker). If Pending Fetcher hasn't run yet, signals are stuck in "waiting_ready_marker" state.

---

### Issue #11: Atomic Write Visibility Lag

**In _write_pending_state_atomic()** (eqidv2_signal_engine_v16_5min.py):

```python
def _write_pending_state_atomic(state: Dict[str, Any], date_str: str) -> None:
    path = _pending_json_path(date_str)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(...))  # Write to .tmp file
    os.replace(tmp_path, path)  # Atomic rename
```

**Problem:**
Even with atomic rename, there's a window where:
1. JSON is being written (microseconds)
2. Detection Engine reads it while write is in-flight (rare, but possible on slow disks)
3. Partial JSON is read → parse error or incomplete signal list

**On Windows NTFS:**
- `os.replace()` is atomic
- But JSON serialization + file write is NOT atomic at OS level
- If power fails or process crashes during write, file is corrupted

**Mitigation:** File read lock/retry logic should exist but isn't clear in code.

---

## PART 6: TIMESTAMP PARSING ISSUES

### Issue #12: _parse_ist_signal_ts() Robustness

**Function location & behavior unknown — needs review**

**Potential issues:**
1. **Timezone handling:** 
   - Some timestamps might be UTC-aware, others IST-aware, others naive
   - Conversion errors could return None
   - If None returned, cascades to next fallback (potentially wrong anchor)

2. **Format inconsistency:**
   - CSV might have ISO8601 with timezone: "2026-04-20T09:42:00+0530"
   - CSV might have format without timezone: "2026-04-20 09:42:00"
   - Parquet might have pandas Timestamp objects
   - Each format needs separate parsing logic

3. **Silent failures:**
   - If parsing fails, function returns None
   - Code then silently falls back to next anchor
   - No logging/warning → hard to debug in production

4. **None vs Empty String:**
   - Code checks `if base_ts is None`
   - But signal dict might have empty strings `""`
   - Empty string is not None → code doesn't recognize it as missing

---

## PART 7: READY MARKER LOGIC ISSUES

### Issue #13: Marker Filename Timestamp Source Bug

**Current code in Pending Fetcher:**
```python
def _write_ready_marker(tickers, ...):
    now_ist = _now_ist()  # Wall-clock time
    fname = now_ist.strftime("%Y%m%d_%H%M") + ".ready"
```

**Should be:**
```python
# Use the slot timestamp, not wall-clock time
slot_ts = _floor_to_5m(now_ist)  # Get current 5-min slot
fname = slot_ts.strftime("%Y%m%d_%H%M") + ".ready"
```

**Impact:**
If Pending Fetcher runs across slot boundaries, marker gets wrong filename:
```
Signal Engine slot: 09:40:00
Pending Fetcher scheduled: 09:45:02 (next slot)
Pending Fetcher actually runs: 09:44:58 (a bit early)
Now_ist at write time: 09:44:58
Marker filename: 20260420_0944.ready  ← WRONG!
Detection Engine looks for: 20260420_0940.ready
Result: Not found → signals skipped
```

---

### Issue #14: Global Ready Marker vs Per-Slot Marker Confusion

**Detection Engine loads two marker types:**
1. Global marker (latest, any slot): broad freshness check
2. Per-slot marker (exact slot): narrow signal set validation

**Problem:**
If global marker is fresh but per-slot marker is missing, what happens?

Code shows:
```python
marker_payload = _load_slot_ready_marker(slot)
if marker_payload is None:
    return "waiting_ready_marker", skip_entire_slot
```

No fallback to global marker or partial rescan. Entire slot skipped.

---

## PART 8: SUMMARY TABLE OF ISSUES

| Issue | Severity | Symptom | Root Cause |
|-------|----------|---------|-----------|
| #1: Pending Fetcher 2-second interval | CRITICAL | API overload, timing chaos | BAT misconfiguration |
| #2: Pending Fetcher runs too early (SLOT+2s) | CRITICAL | Reads stale pending pool | SLOT_OFFSET_SEC=2 vs Signal Engine SLOT+45s |
| #3: Signal Engine → Fetcher coordination gap | HIGH | New signals never fetched | No synchronization mechanism |
| #4: Per-slot marker lookup fragility | HIGH | Entire slots skipped on marker miss | All-or-nothing filter, no fallback |
| #5: ALIGN_TO_5MIN_BOUNDARY offset mismatch | HIGH | Phase desynchronization | Multiple processes with different offsets |
| #6: Multiple timestamp anchors | HIGH | False stale signal skips | Inconsistent fallback chain |
| #7: Expiry time vs freshness deadline conflict | MEDIUM | Borderline signals skipped | Two independent expiry mechanisms |
| #8: Per-slot marker dependency | MEDIUM | Cascading signal loss | Single marker failure blocks entire slot |
| #9: Stale data age check threshold | MEDIUM | Valid signals filtered | 3-minute threshold could be exceeded |
| #10: Pending pool write → read race | MEDIUM | Missed signals on cycle boundary | No synchronization |
| #11: Atomic write visibility lag | LOW | Occasional parse errors | JSON write not fully atomic at OS level |
| #12: _parse_ist_signal_ts() robustness | HIGH | Silent fallback to old timestamp | No error handling/logging |
| #13: Ready marker filename timestamp source | HIGH | Wrong filename on edge-of-slot runs | Uses wall-clock time instead of slot time |
| #14: Missing per-slot marker handling | MEDIUM | Entire slot skipped | No fallback to global marker |

---

## PART 9: RECOMMENDED INVESTIGATIONS

Before making any fixes, investigate:

1. **Check production logs for patterns:**
   - How many signals have `ENTRY_SKIPPED_STALE_SIGNAL`?
   - Do they cluster around specific times (slot boundaries)?
   - What timestamp fields are populated (detected_time_ist, logtime_ist, received_time)?

2. **Verify BAT settings:**
   - Confirm EQIDV2_PENDING_FETCH_INTERVAL_SEC should be 2 or 60
   - Verify all offset settings (SLOT_START_OFFSET, SLOT_OFFSET, DETECTION_SLOT_OFFSET)
   - Ensure ALIGN_TO_5MIN_BOUNDARY settings match across all three processes

3. **Test timestamp parsing:**
   - Generate test signals with various timestamp formats
   - Verify _parse_ist_signal_ts() handles all cases
   - Check for timezone conversion bugs

4. **Measure timing:**
   - Log exact times when:
     - Signal Engine writes pending pool
     - Pending Fetcher runs
     - Ready marker is written
     - Detection Engine reads marker
   - Identify timing gaps and synchronization issues

5. **Check file system behavior:**
   - Verify atomic write of JSON on Windows NTFS
   - Test concurrent read during write scenarios
   - Check for file locking issues

6. **Monitor Kite API:**
   - Log all API errors
   - Check for 429 (rate limit) responses
   - Verify no IP whitelist/blocking

---

## PART 10: POTENTIAL QUICK WINS

Before major refactoring, these could be checked/fixed:

1. **Fix BAT interval (Issue #1):**
   - Change `EQIDV2_PENDING_FETCH_INTERVAL_SEC=2` → `60`
   - Verify this doesn't break anything else

2. **Add logging for timestamp selection (Issue #12):**
   - Log which timestamp anchor was used
   - Log if parsing failed
   - Help diagnose the fallback behavior

3. **Fix marker filename source (Issue #13):**
   - Ensure marker uses slot timestamp, not wall-clock time
   - Prevents marker file naming issues

4. **Add per-slot marker fallback (Issue #14):**
   - If per-slot marker missing, try to use global marker
   - Or re-scan without marker instead of skipping entirely

5. **Synchronize process offsets (Issue #5):**
   - Ensure Signal Engine, Pending Fetcher, Detection Engine all run in coordinated sequence
   - Not random phase offsets

---

## PART 11: ARCHITECTURE ISSUES REQUIRING DISCUSSION

These are design-level concerns that might need strategic decisions:

1. **Dependency chain too fragile:**
   - Signal Engine writes → Pending Fetcher reads → writes marker → Detection Engine reads marker
   - Single point of failure anywhere in chain skips entire signals
   - Need more resilient fallback mechanisms

2. **Two-stage pipeline latency:**
   - Signal → Pending Fetch → Detection → Executor = ~2-3 minutes
   - Is this acceptable for intraday 5-min strategy?
   - Consider if Stage 1 filtering would reduce latency while maintaining accuracy

3. **Ready marker coupling:**
   - Detection Engine tightly coupled to Pending Fetcher marker
   - What if Pending Fetcher fails but fresh data exists elsewhere?
   - Need to decouple detection from marker dependency

4. **Timestamp proliferation:**
   - Too many timestamp fields (signal_datetime, signal_entry_datetime_ist, signal_bar_time_ist, received_time, detected_time_ist, logtime_ist, etc.)
   - Confusion about which one is "canonical"
   - Consider consolidating to single definitive timestamp

---

## CONCLUSION

The V16 5min pipeline is well-designed in principle but has **multiple implementation inconsistencies** and **timing synchronization issues** that could cause genuine signals to be skipped.

**Highest priority:** Fix pending fetcher interval and per-slot marker timing.

**Next priority:** Add comprehensive logging to understand which signals are being skipped and why.

**Long-term:** Refactor to reduce fragility and dependencies, especially the ready marker coupling.

---

**Analysis completed:** April 20, 2026  
**Recommendations:** See Part 9 & 10  
**Status:** No changes made — analysis only
