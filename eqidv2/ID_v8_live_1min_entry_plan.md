# ID v8 Live 1-Min Entry Flow

## Goal

Keep the v7/v8 5-minute setup logic as the signal authority, but stop waiting
for the next 5-minute candle for entry. The live system should:

1. Confirm setups on a completed 5-minute signal candle.
2. Store those signals as pending entries.
3. Watch only those tickers on the 1-minute feed.
4. Execute from the first valid 1-minute entry point.
5. Track every session state in files that a dashboard can read.

## Runtime Folders

Base:

```text
C:\TradingData\eqidv2\live_id_5min_v8_1min_entry
```

Suggested children:

```text
pending_entries
active_entries
executed_entries
closed_entries
expired_entries
rejected_entries
session_status
dashboard_snapshots
audit
heartbeat
```

## Session Files

### 1. 5-Min Data Session

Source folders already exist:

```text
C:\TradingData\eqidv2\stocks_indicators_5min_eq_live
C:\TradingData\eqidv2\slot_ready_5m
C:\TradingData\eqidv2\nifty_slot_ready_5m
```

Dashboard session file:

```text
session_status\five_min_data_YYYY-MM-DD.json
```

Fields:

```json
{
  "session": "5min_data",
  "slot": "2026-05-21 09:35:00+05:30",
  "stock_fresh_ratio": 0.98,
  "nifty_ready": true,
  "tickers_written": 1250,
  "status": "READY"
}
```

### 2. 5-Min Signal Scanner Session

New scanner should write signal-only records. It should not require a next
5-minute candle.

Suggested file:

```text
eqidv2_id_v8_signal_scanner_persistent.py
```

Pending output:

```text
pending_entries\pending_entries_YYYY-MM-DD.csv
```

Columns:

```text
signal_id
ticker
side
setup
signal_time_ist
signal_close
signal_high
signal_low
signal_vwap
quality_score
rs_pct
market_ret_pct
regime
sl_pct
target_pct
status
valid_from_ist
valid_until_ist
created_at_ist
```

Status values:

```text
PENDING
EXECUTED
EXPIRED
REJECTED
DUPLICATE
```

### 3. 1-Min Entry Watcher Session

New watcher:

```text
eqidv2_id_v8_1min_entry_watcher.py
```

Input:

```text
pending_entries\pending_entries_YYYY-MM-DD.csv
C:\TradingData\eqidv2\stocks_indicators_1min_eq
```

Logic:

```text
For each PENDING signal:
  read only that ticker's 1-min parquet
  find first 1-min bar with date >= signal_time_ist
  entry_price = that 1-min bar open
  if no bar before valid_until_ist, expire the signal
  if entry passes guard checks, write EXECUTED
```

Executed output:

```text
executed_entries\executed_entries_YYYY-MM-DD.csv
```

Columns:

```text
signal_id
ticker
side
setup
signal_time_ist
entry_time_ist
entry_price
entry_delay_seconds
sl_pct
target_pct
sl_price
target_price
order_mode
order_status
created_at_ist
```

### 4. Position / Exit Session

The executor should consume executed entries, not raw signals.

Active positions:

```text
active_entries\active_positions_YYYY-MM-DD.csv
```

Closed positions:

```text
closed_entries\closed_entries_YYYY-MM-DD.csv
```

Exit logic:

```text
LONG:
  target = entry_price * (1 + target_pct / 100)
  sl     = entry_price * (1 - sl_pct / 100)

SHORT:
  target = entry_price * (1 - target_pct / 100)
  sl     = entry_price * (1 + sl_pct / 100)
```

## Dashboard Sessions

The dashboard should read only small snapshot files, not raw parquet.

Snapshot:

```text
dashboard_snapshots\v8_live_snapshot_YYYY-MM-DD.json
```

Recommended fields:

```json
{
  "as_of": "2026-05-21 10:05:10+05:30",
  "current_slot": "2026-05-21 10:05:00+05:30",
  "five_min_data": {
    "status": "READY",
    "fresh_ratio": 0.98,
    "nifty_ready": true
  },
  "scanner": {
    "status": "RUNNING",
    "signals_today": 18,
    "signals_last_slot": 2
  },
  "entry_watcher": {
    "status": "RUNNING",
    "pending": 3,
    "executed_today": 11,
    "expired_today": 1,
    "avg_entry_delay_seconds": 42
  },
  "executor": {
    "paper_true_status": "RUNNING",
    "paper_false_status": "RUNNING",
    "active_positions": 4,
    "closed_today": 7
  }
}
```

## Dashboard Views

1. Session Health
   - 5-min fetcher status
   - Nifty guard status
   - 1-min feed freshness
   - scanner heartbeat
   - entry watcher heartbeat
   - executor heartbeat

2. Pending Entries
   - ticker
   - setup
   - side
   - signal time
   - age
   - expiry time

3. Executed Entries
   - signal time
   - entry time
   - entry delay
   - entry price
   - SL/target

4. Active Positions
   - live PnL
   - distance to SL
   - distance to target
   - bars held

5. Audit
   - duplicate signals
   - expired signals
   - missing 1-min data
   - rejected orders

## Implementation Order

1. Create v8 signal-only scanner.
2. Create v8 pending-entry writer.
3. Create v8 1-min entry watcher.
4. Add snapshot writer for dashboard.
5. Wire executor to consume executed entries.
6. Run in paper mode for at least one full market session.
7. Compare live signal/entry audit against v8 backtest assumptions.

## Key Rule

Do not enter at the 5-minute signal candle's historical open. The signal uses
completed candle information, so live entry must happen after signal
confirmation. v8 uses the first 1-minute open at or after signal time.
