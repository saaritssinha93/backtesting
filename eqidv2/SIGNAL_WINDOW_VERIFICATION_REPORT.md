# SIGNAL WINDOW VERIFICATION REPORT
## V16 5-Min Live Pipeline Configuration Check
### Date: 2026-04-18

---

## EXECUTIVE SUMMARY

Current code **MISMATCH** with documented specification in EQIDV2_V16_5MIN_PIPELINE_REFERENCE.txt Section 4.

**Document Specifies:**
- LONG Session 1: 09:15 – 11:00 IST → expires at 11:00
- LONG Session 2: 12:00 – 13:30 IST → expires at 13:30
- SHORT Session 1: 09:15 – 11:00 IST → expires at 11:00
- SHORT Session 2: 12:00 – 13:30 IST → expires at 13:30

**Actual Code Configuration:**
- avwap_common_v11.py: (09:30-12:00) + (13:30-15:15) — different boundaries, continuous windows
- eqidv2_signal_engine_v16_5min.py: morning_expires=12:00 (should be 11:00), long_expires=13:00 (should be 13:30)

---

## DETAILED FINDINGS

### 1. Strategy Configuration Files

#### File: avwap_v11_refactored/avwap_common_v11.py

**Location:** Lines 264-267 (SHORT config), Lines 294-297 (LONG config)

**Current Code:**
```python
# SHORT config (lines 264-267)
signal_windows=[
    (dtime(9, 30, 0), dtime(12, 0, 0)),     # ← 09:30-12:00
    (dtime(13, 30, 0), dtime(15, 15, 0)),   # ← 13:30-15:15
]

# LONG config (lines 294-297)
signal_windows=[
    (dtime(9, 30, 0), dtime(12, 0, 0)),     # ← 09:30-12:00
    (dtime(13, 30, 0), dtime(15, 15, 0)),   # ← 13:30-15:15
]
```

**Issues:**
1. First window starts at 09:30 (document says 09:15)
2. First window ends at 12:00 but afternoon starts at 12:00 (no gap, document says 11:00 end + 12:00 start = 1 hour gap)
3. Afternoon window ends at 15:15 (document says 13:30)

**Impact:** Backtesting and analysis signals use these windows; live 5-min production may use different windows.

---

### 2. Signal Engine Expiry Logic

#### File: eqidv2_signal_engine_v16_5min.py

**Location:** Lines 145-147, function `_compute_expires_at()` at lines 285-301

**Current Constants:**
```python
_SESSION_MORNING_END   = dtime(12, 0)    # Line 145 — WRONG
_SESSION_AFTNOON_LONG  = dtime(13, 0)    # Line 146 — WRONG
_SESSION_AFTNOON_SHORT = dtime(13, 30)   # Line 147 — CORRECT ✓
```

**Expected Per Document:**
```python
_SESSION_MORNING_END   = dtime(11, 0)    # Morning signals expire at 11:00
_SESSION_AFTNOON_LONG  = dtime(13, 30)   # LONG signals expire at 13:30
_SESSION_AFTNOON_SHORT = dtime(13, 30)   # SHORT signals expire at 13:30
```

**Current Function Logic (lines 285-301):**
```python
def _compute_expires_at(slot_ist: datetime, side: str) -> str:
    slot_time = slot_ist.time()
    today = slot_ist.date()
    side_u = str(side).upper().strip()

    if slot_time < _SESSION_MORNING_END:              # if < 12:00
        exp_dt = IST.localize(datetime.combine(today, _SESSION_MORNING_END))
    else:                                               # if >= 12:00
        if side_u == "SHORT":
            exp_dt = IST.localize(datetime.combine(today, _SESSION_AFTNOON_SHORT))
        else:
            exp_dt = IST.localize(datetime.combine(today, _SESSION_AFTNOON_LONG))
    return exp_dt.strftime("%Y-%m-%d %H:%M:%S%z")
```

**Issues:**
- Morning cutoff is 12:00 (should be 11:00)
- LONG afternoon expires at 13:00 (should be 13:30)

**Impact:** Pending signals generated before 12:00 get afternoon status even if generated at 11:30; LONG signals generated 13:00-13:30 expire prematurely.

---

### 3. Detection Engine Expiry Check

#### File: eqidv2_detection_engine_v16_5min.py

**Location:** Lines 1168-1180

**Code:** ✓ **CORRECT**
```python
expires_at_raw = sig.get("expires_at", "")
if expires_at_raw:
    expires_at = base_v15._parse_ist_timestamp(str(expires_at_raw))
    if expires_at is not None:
        try:
            exp_ts = pd.Timestamp(expires_at)
            if exp_ts.tzinfo is None:
                exp_ts = exp_ts.tz_localize(IST)
            else:
                exp_ts = exp_ts.tz_convert(IST)
            if now_ist >= exp_ts:
                sig["status"] = "expired_window"
                sig["filter_reason"] = f"past expires_at={expires_at_raw}"
                return "expired_window"
        except Exception:
            pass
```

**Status:** Implementation is correct; it checks `now_ist >= exp_ts` and filters accordingly.
The issue is that the `expires_at` value sent from Signal Engine is wrong.

---

## IMPACT ANALYSIS

### On Live Production (5-min live pipeline)

1. **Morning signals (09:15-11:00):**
   - Correct expiry time per document: 11:00
   - Actual expiry time: 12:00
   - **Impact:** Morning signals remain "pending" 1 extra hour, may be detected in afternoon slot

2. **LONG signals generated 13:00-13:30:**
   - Correct expiry time per document: 13:30
   - Actual expiry time: 13:00
   - **Impact:** LONG signals generated 13:00-13:30 are immediately expired at 13:30 (within 30s window)

3. **SHORT signals (all afternoon):**
   - Correct expiry time per document: 13:30
   - Actual expiry time: 13:30
   - **Impact:** ✓ Works correctly

### On Backtesting/Analysis (15-min or daily data)

The strategy files use signal_windows (09:30-12:00) + (13:30-15:15), which is different from the 5-min pipeline windows. This separation is intentional — backtest may use wider windows for analysis.

---

## REMEDIATION REQUIRED

### Priority: HIGH

These fixes are needed for production 5-min pipeline to match documented behavior:

**Fix 1: Update Signal Engine Constants** (eqidv2_signal_engine_v16_5min.py, lines 145-147)
```python
# CURRENT (WRONG)
_SESSION_MORNING_END   = dtime(12, 0)
_SESSION_AFTNOON_LONG  = dtime(13, 0)
_SESSION_AFTNOON_SHORT = dtime(13, 30)

# SHOULD BE (CORRECT)
_SESSION_MORNING_END   = dtime(11, 0)    # ← Fix: 12:00 → 11:00
_SESSION_AFTNOON_LONG  = dtime(13, 30)   # ← Fix: 13:00 → 13:30
_SESSION_AFTNOON_SHORT = dtime(13, 30)
```

**Fix 2: Update Strategy Signal Windows** (avwap_common_v11.py, lines 264-267 and 294-297)

Optional — depends on whether backtest/analysis should match production windows.

If aligning backtest to production:
```python
# CURRENT
signal_windows=[
    (dtime(9, 30, 0), dtime(12, 0, 0)),
    (dtime(13, 30, 0), dtime(15, 15, 0)),
]

# SHOULD BE
signal_windows=[
    (dtime(9, 15, 0), dtime(11, 0, 0)),    # ← Session 1: 09:15-11:00
    (dtime(12, 0, 0), dtime(13, 30, 0)),   # ← Session 2: 12:00-13:30
]
```

---

## VERIFICATION CHECKLIST

After fixes are applied:

- [ ] eqidv2_signal_engine_v16_5min.py: _SESSION_MORNING_END changed to 11:00
- [ ] eqidv2_signal_engine_v16_5min.py: _SESSION_AFTNOON_LONG changed to 13:30
- [ ] Run live simulator on historical 5-min data, verify expires_at values match documentation
- [ ] Check pending pool JSON on live dates: morning signal should show expires_at as 11:00+
- [ ] Monitor detection engine logs: should NOT see "past expires_at" messages for signals < 5 min old

---

## NEW V16 5-MIN STRATEGY FILES CREATED

To support the 5-min pipeline with documented signal windows, new strategy configuration modules have been created:

1. **avwap_long_strategy_v16_5min_config.py** — LONG side configuration with correct windows
2. **avwap_short_strategy_v16_5min_config.py** — SHORT side configuration with correct windows

These can be imported and used in backtesting or analysis that should match the live 5-min pipeline timing.

---

## REFERENCES

- Document: EQIDV2_V16_5MIN_PIPELINE_REFERENCE.txt, Section 4 (Signal Windows)
- Document: EQIDV2_V16_5MIN_PIPELINE_REFERENCE.txt, Section 17 (P0-P6 Fixes)
- Production Files:
  - eqidv2_signal_engine_v16_5min.py (lines 145-147, 285-301)
  - eqidv2_detection_engine_v16_5min.py (lines 1168-1180)
  - avwap_v11_refactored/avwap_common_v11.py (lines 264-267, 294-297)

