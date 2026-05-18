# V16 5min Pipeline: Critical Issues Summary & Verification Guide
**Analysis Date:** April 20, 2026

---

## 🔴 CRITICAL ISSUES (Likely Causing Entry Skips)

### 1. PENDING FETCHER INTERVAL = 2 SECONDS (WRONG!)

**File:** `bat/run_eqidv2_pending_data_fetcher_v16_5min.bat`, line 12

```batch
set "EQIDV2_PENDING_FETCH_INTERVAL_SEC=2"  ❌ SHOULD BE 60
```

**Expected:** 60 seconds (design allows one fetch per 5-min slot)  
**Actual:** 2 seconds (30× more frequent than designed)

**Consequence:**
- Kite API rate limit violations (429 errors)
- Excessive CPU load
- Timing conflicts with Signal Engine and Detection Engine

---

### 2. PENDING FETCHER RUNS TOO EARLY (Reads Stale Signals)

**Timeline Problem:**

```
09:40:00  Slot begins
09:40:02  ← Pending Fetcher wakes (SLOT_OFFSET_SEC=2)
          ❌ Signal Engine hasn't written pending pool yet!
          ❌ Fetches signals from PREVIOUS slot
          ❌ Writes marker for wrong ticker set

09:40:45  ← Signal Engine wakes (SLOT_START_OFFSET_SECONDS=45)
          Scans all tickers, finds NEW signals
          ~09:42:00 Writes NEW pending pool

09:40:04  ← Detection Engine wakes (DETECTION_SLOT_OFFSET_SEC=4)
          ❌ Looks for marker from Pending Fetcher
          ❌ Marker exists but for WRONG signals (from 09:35 slot!)
          ❌ Newly detected signals have no marker
          ❌ → ENTRY_SKIPPED (waiting_ready_marker)
```

**Fix Required:**
Pending Fetcher must run AFTER Signal Engine completes, not before it starts.

---

### 3. PENDING FETCHER ALIGNMENT BROKEN

**Settings in BAT:**
```batch
set "EQIDV2_PENDING_FETCH_ALIGN_TO_5MIN=1"         ← Aligned to slots
set "EQIDV2_PENDING_FETCH_INTERVAL_SEC=2"          ← Sleep 2s (conflicts!)
set "EQIDV2_PENDING_FETCH_SLOT_OFFSET_SEC=2"       ← Run 2s after slot
```

**Problem:** Interval-based sleep conflicts with slot-aligned logic.

When `ALIGN_TO_5MIN_BOUNDARY=1`, the code should ignore FETCH_INTERVAL_SEC and only run once per 5-min slot. Currently both run, creating chaos.

---

### 4. READY MARKER FILENAME BUG

**Code Problem** (eqidv2_pending_data_fetcher_v16_5min.py):

```python
def _write_ready_marker(...):
    now_ist = _now_ist()  # ❌ Wall-clock time
    fname = now_ist.strftime("%Y%m%d_%H%M") + ".ready"
```

**Should be:**
```python
    slot_ist = _floor_to_5m(now_ist)  # ✓ Slot timestamp
    fname = slot_ist.strftime("%Y%m%d_%H%M") + ".ready"
```

**Scenario:**
```
Signal for 09:40 slot needs fetching
Fetcher writes marker at 09:44:58
Marker filename: 20260420_0944.ready  ❌ WRONG!
Detection Engine looks for: 20260420_0940.ready
Result: Not found → Entry skipped
```

---

### 5. PER-SLOT MARKER ALL-OR-NOTHING FILTERING

**Detection Engine Logic** (eqidv2_detection_engine_v16_5min.py, ~line 850):

```python
per_slot_marker = _load_ready_marker_for_slot(slot_ts, now)
if per_slot_marker is None:
    # Skip ALL signals for this slot!
    continue  # ❌ Entire slot filtered
```

**Impact:**
- Single missing marker file → ALL signals in slot skipped
- No fallback mechanism
- No retry or rescan logic
- No partial processing

---

### 6. TIMESTAMP PARSING INCONSISTENCY

**File:** Executor's `_entry_retry_deadline()` function

```python
# Priority 1: detected_time_ist (Stage-2 confirmation)
# Priority 2: logtime_ist (fallback)
# Priority 3: received_time (pending pool insertion)
# Priority 4: signal_entry_datetime_ist (⚠️ LAG BAR CLOSE TIME — can be 30+ min old!)
```

**Problematic Scenario:**
```
Signal entry bar closes:    09:40:00
Detection time (NOT SET):   (missing)
Received in pending pool:   09:41:50
Trade starts:               09:46:00

Code falls through priorities 1-2 (missing) → uses 09:40:00
Deadline = 09:40:00 + 300s = 09:45:00
Trade at 09:46:00 ≥ 09:45:00 → ❌ ENTRY_SKIPPED_STALE_SIGNAL

Should have used 09:41:50:
Deadline = 09:41:50 + 300s = 09:46:50 → ✓ Would be OK
```

---

## 🟡 HIGH-PRIORITY ISSUES (Probable Entry Skips)

### 7. No Synchronization Between Signal Engine & Pending Fetcher

**Problem:** No handshake mechanism.

Signal Engine writes pending pool every 5 minutes (~09:42, ~09:47, etc.)  
Pending Fetcher should fetch within minutes of that, but currently fires at arbitrary times.

**Missing:** Pending Fetcher should wait for Signal Engine to complete, or vice versa.

---

### 8. Stale Data Age Check (180-second threshold)

**BAT setting:**
```batch
set "EQIDV2_DETECTION_MAX_DATA_AGE_SEC=180"
```

**Problem:**
If parquet is older than 3 minutes, signals filtered. But normal pipeline latency is 2-3 minutes, so signals could age out during detection.

**Example:**
```
09:40 Signal generated
09:42 Pending Fetcher updates parquet
09:45 Detection Engine runs (3 min later)
Parquet age at 09:45 = 3 minutes
Threshold = 180 seconds = exactly 3 minutes
If > 180s even by 1 second → FILTERED
```

---

### 9. Timestamp Field Proliferation & Parsing

**Fields in signal CSV:**
- `signal_datetime`
- `signal_entry_datetime_ist`
- `signal_bar_time_ist`
- `received_time`
- `detected_time_ist`
- `logtime_ist`
- `stage2_detected_at_ist`

**Problem:** 
Too many timestamp options. If parsing fails on one, cascades to next (potentially wrong) one.

**Missing:** Robust error handling and logging for timestamp parsing failures.

---

## 🟠 MEDIUM-PRIORITY ISSUES

### 10. Expiry Time Logic Conflict
- Signal Engine sets `expires_at` = session boundary (11:00 or 13:30)
- Executor uses `detected_time_ist + 300s` as deadline
- Two different expiry mechanisms can conflict near boundaries

### 11. Atomic Write Visibility Lag
- JSON written to `.tmp` file, then `os.replace()` to final name
- Between these operations, if Detection Engine reads, could get partial data
- Rare but possible on slow disks or under load

### 12. Pending Pool Write → Detection Read Race
- No synchronization between write and read
- Detection Engine could read old state if polling at exact write moment
- File locking or version checking needed

---

## ✅ QUICK VERIFICATION CHECKLIST

Before fixing anything, verify these facts:

### Check #1: BAT Settings
```powershell
# Run this in PowerShell:
$bat = "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\bat\run_eqidv2_pending_data_fetcher_v16_5min.bat"
Select-String "EQIDV2_PENDING_FETCH_INTERVAL_SEC" $bat
```

Expected: `"60"` or commented out (defaults to 60)  
Actual: `"2"` ← **WRONG**

---

### Check #2: Pending Fetcher vs Signal Engine Timing
```bash
# Check logs for actual run times
tail -100 logs/eqidv2_signal_engine_v16_5min_2026-04-20.log | grep SLOT
tail -100 logs/eqidv2_pending_data_fetcher_v16_5min_2026-04-20.log | grep PENDING_FETCH
```

Look for pattern:
```
[SIGNAL_ENGINE] slot=09:40 [TIME]
[PENDING_FETCH] [TIME] pending_tickers=X
```

**Issue:** If PENDING_FETCH time is BEFORE SIGNAL_ENGINE time, Fetcher ran too early.

---

### Check #3: Ready Marker Files
```bash
dir C:\TradingData\eqidv2\slot_ready_5m_pending\*.ready /OD
```

Look at filenames. They should be named for their slot (e.g., `20260420_0940.ready` for 09:40 slot).

**If wrong:** Filenames don't match slot times → Detection Engine can't find markers.

---

### Check #4: Signal Timestamps in CSV
```bash
# Check a sample signals CSV
head -5 C:\TradingData\eqidv2\live_signals\signals_2026-04-20_v16_5min_long.csv
```

Check columns:
- `detected_time_ist` — should be populated (Stage-2 confirmation time)
- `logtime_ist` — should match detected_time_ist
- `received_time` — should be earlier (Stage-1 insertion time)
- `signal_entry_datetime_ist` — should be LAG BAR close time (usually earliest)

**If problem:** detected_time_ist is NULL/empty → falls back to stale signal_entry_datetime_ist

---

### Check #5: Entry Skip Reasons
```bash
# Count reasons for skipped entries in trade log
type C:\TradingData\eqidv2\live_signals\live_trades_2026-04-20_v16_5min.csv | findstr SKIPPED
```

Look for:
- `ENTRY_SKIPPED_STALE_SIGNAL` — too many = timestamp issue
- `waiting_ready_marker` — missing marker files
- `parquet_too_old` — stale data issue

---

## 📋 RECOMMENDED IMMEDIATE ACTIONS

### Priority 1: Fix Pending Fetcher Interval (CRITICAL)
```batch
OLD: set "EQIDV2_PENDING_FETCH_INTERVAL_SEC=2"
NEW: set "EQIDV2_PENDING_FETCH_INTERVAL_SEC=60"
```

**Expected effect:** Reduces API load, fixes timing chaos.

---

### Priority 2: Fix Ready Marker Filename Source (CRITICAL)
**File:** `eqidv2_pending_data_fetcher_v16_5min.py`

```python
# BEFORE:
fname = _now_ist().strftime("%Y%m%d_%H%M") + ".ready"

# AFTER:
slot_ist = _floor_to_5m(_now_ist())
fname = slot_ist.strftime("%Y%m%d_%H%M") + ".ready"
```

**Expected effect:** Markers get correct names, Detection Engine finds them.

---

### Priority 3: Add Logging for Timestamp Fallbacks
**File:** `avwap_trade_execution_PAPER_TRADE_FALSE_v16_5min.py`

Add logging to `_entry_retry_deadline()`:
```python
if base_ts is None:
    log.warning(f"[FRESHNESS] {ticker}: fell back to signal_entry_datetime_ist (lag bar time)")
    base_ts = _parse_ist_signal_ts(signal.get("signal_entry_datetime_ist"))
```

**Expected effect:** Reveals which signals are using fallback timestamps.

---

### Priority 4: Add Per-Slot Marker Fallback
**File:** `eqidv2_detection_engine_v16_5min.py`

If per-slot marker missing, either:
1. Scan signals anyway (without tight ticker filtering), OR
2. Try global marker as fallback

```python
per_slot_marker = _load_ready_marker_for_slot(slot_ts, now)
if per_slot_marker is None:
    # Fallback: use global marker instead of skipping entire slot
    per_slot_marker = _load_slot_ready_marker(slot_ts)
    if per_slot_marker is None:
        # Log and continue (don't skip)
        log.warning(f"[DETECTION] {slot_ts}: no marker found, scanning without restriction")
        # Don't filter by marker's tickers
```

---

## 📊 TIMING FLOW (CURRENT BROKEN STATE)

```
09:40:00  ════════ Slot Boundary
          
09:40:02  ⚠️ Pending Fetcher wakes (TOO EARLY!)
          • Reads pending pool from PREVIOUS slot (09:35)
          • Fetches data for OLD signals
          • Writes marker for WRONG ticker set
          
09:40:04  ⚠️ Detection Engine wakes
          • Looks for per-slot marker
          • Finds marker from 09:40:02 (but for 09:35 signals)
          • New signals have no marker yet
          • → Skips new signals
          
09:40:45  ✓ Signal Engine wakes (CORRECT TIME)
          • Scans all 1044 tickers
          • Finds NEW patterns
          • ~09:42:00 writes NEW pending pool
          • BUT: Pending Fetcher already ran!
          • Mark for these signals: NOT WRITTEN
          • Detection Engine keeps looking: NOT FOUND
          • → Signals stuck in "waiting_ready_marker"
          
09:42:00  ❌ Pending Fetcher next cycle (if 60s interval)
          But BAT has 2-second sleep, so...
          
09:40:04+2s ⚠️ Pending Fetcher runs again at 09:40:06
          (and again at 09:40:08, 09:40:10, 09:40:12... every 2s)
          • Now sees signals from ~09:42 pending pool
          • Fetches them
          • Writes marker

09:42:06  ✓ Detection Engine next cycle (~2-3 seconds later)
          • Sees marker (finally)
          • Processes signals
          • ~10+ MINUTES after signal was generated
          
09:50:00  Entry deadline expires (09:40 + 300s + pipeline latency)
          ❌ Signal too old → Entry skipped
```

---

## 📊 TIMING FLOW (CORRECTED STATE)

```
09:40:00  ════════ Slot Boundary

09:40:04  ✓ Detection Engine wakes
          • Looks for marker from PREVIOUS slot
          • Processes any pending signals
          
09:40:45  ✓ Signal Engine wakes
          • Scans all 1044 tickers
          • Finds NEW patterns
          • ~09:42:00 writes pending pool

09:42:05  ✓ Pending Fetcher wakes
          (AFTER Signal Engine completes)
          • Reads pending pool (now has NEW signals!)
          • Fetches data for NEW signals
          • Writes marker immediately
          
09:42:08  ✓ Detection Engine next cycle
          • Sees new marker
          • Processes signals immediately
          • Signal latency: ~2 minutes ✓
          
09:46:00  ✓ Executor gets signal
          • Entry deadline: 09:42 + 300s = 09:47
          • Entry at 09:46 → OK ✓
```

---

## 📞 NEED CLARIFICATION ON:

1. **Design intent for pending fetcher interval:**
   - Should it be 60 seconds (per reference doc)?
   - Or should it align to 5-min slots (one per slot)?

2. **Ready marker responsibility:**
   - Should Pending Fetcher write it (current design)?
   - Or should Signal Engine write its own marker when pending pool is ready?

3. **Detection Engine without marker:**
   - Should it rescan without restricting to marker tickers?
   - Or skip entire slot (current behavior)?

4. **Timestamp anchor strategy:**
   - Should all signals force `detected_time_ist` to be populated?
   - Or accept multi-priority fallback with better logging?

---

**Analysis completed:** April 20, 2026  
**Files involved:** 6 Python scripts, 3 BAT files, 1 reference document  
**Status:** Issues documented, no changes made
