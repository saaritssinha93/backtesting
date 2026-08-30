# FnO — Data to Entry, End to End

How a five-minute bar becomes a live order: **fetch → merge → analyse → scan (5m) → confirm (1m) → enter**, with every timing constant taken from source.

| | |
|---|---|
| Futures OI fetch | [fno_oi_fetch_5min.py](fno_oi_fetch_5min.py) |
| Cash 5-minute feed | `eod_5min_data` (Live Data Fetch 5mins) |
| Cash 1-minute feed | [fno_equity_fetch_1min.py](fno_equity_fetch_1min.py) |
| Merge / indicators | [fno_oi_hybrid_data.py](fno_oi_hybrid_data.py) |
| Scanner + confirmation + entry | [fno_v5_live.py](fno_v5_live.py) *(shared runtime)* |
| Universe | [fno_oi_universe.py](fno_oi_universe.py) |

---

## 1. The whole pipeline in one picture

```
                            ┌─────────────────────────────────────────┐
  08:50                     │  fno_oi_universe                        │
                            │  near-month NFO futures → cash symbols  │
                            │  unmappable stock future = HARD FAIL    │
                            └──────────────────┬──────────────────────┘
                                               │ latest_near_month.parquet
                    ┌──────────────────────────┴──────────────────────────┐
                    │                                                     │
  ══ FETCH ═════════▼═════════════════════      ══════════════════════════▼════════
   fno_oi_fetch_5min                             eod_5min_data
   NFO near-month futures 5m                     NSE cash equity 5m
   OHLCV + OI                                    OHLCV
   8 Kite apps · 0.36 s pace                     ↓
   ↓                                             slot_ready_5m/slot_<D>_<HHMM>.json
   raw_contracts_5m/*_5minute.parquet
   slot_ready/slot_<D>_<HHMM>.json
                    │                                                     │
                    └──────────────────────────┬──────────────────────────┘
                                               │  BOTH markers required
  ══ MERGE ═════════════════════════════════════▼══════════════════════════════════
   join_equity_price_with_futures_oi()
     equity side  → EMA9/20/50 · price_change_pct · volume_ratio · traded_value
     futures side → prev_oi = oi.shift(1) · oi_change_pct
     merge(on="ts", how="inner", validate="one_to_one")
     ONLY oi / prev_oi / oi_change_pct cross over from the future
                                               │
  ══ SCAN 5m ═══════════════════════════════════▼══════════════════════════════════
   scanner-5m  ·  take the EXACT slot row  ·  apply the base gate
     LONG  : ema9>ema20>ema50 AND price_change_pct >= +0.10
     SHORT : ema9<ema20<ema50 AND price_change_pct <= -0.10
     BOTH  : oi>prev_oi AND oi_change_pct >= 0.05 AND volume_ratio >= 0.80
                                               │ immutable scanner snapshot
                                               │ (fingerprinted, hashed)
  ══ FETCH 1m ══════════════════════════════════▼══════════════════════════════════
   fno_equity_fetch_1min  ·  ONLY the candidate set  ·  create-once, fsync'd
   binds: scanner hash + candidate-set hash + strategy fingerprint + bar-set hash
                                               │ slot parquet + marker
  ══ CONFIRM 1m ════════════════════════════════▼══════════════════════════════════
   confirmation-1m  ·  READ-ONLY consumer  ·  never calls historical_data
     direction · body_ratio · wick_ratio · trigger
     → per-leg filters → rank → take top max_entries
                                               │ signals/*.json
  ══ ENTER ═════════════════════════════════════▼══════════════════════════════════
   long-entry / short-entry  ·  1 s poll  ·  batched LTP
   stop order at the confirmation extreme  ·  90 s activation deadline
   THE CONFIRMATION CANDLE CAN NEVER FILL ITSELF
```

---

## 2. Timings — every constant, from source

### 2.1 Session-level

| Time | Event | Source |
|---|---|---|
| 08:50 | Universe built | scheduled task |
| 09:05 | Futures OI fetch starts, waits for first slot | scheduled task |
| **09:15** | Scanner, 1m feed, confirmation, both entry workers, both reporters start | scheduled tasks |
| 09:20 | `FIRST_SLOT` — first fetchable 5-minute slot | `fno_oi_fetch_5min.FIRST_SLOT` |
| 09:25 / 09:30 / 09:35 / 09:40 / 09:45 | The five **signal** slots | `SIGNAL_TO_CONFIRMATION` |
| 09:26 / 09:31 / 09:36 / 09:41 / 09:46 | The five **confirmation** candles | `SIGNAL_TO_CONFIRMATION` |
| **09:50** | `PIPELINE_DEADLINE` — scanner publishes BLOCKED and exits | `fno_v5_live.PIPELINE_DEADLINE` |
| 15:30 | `LAST_SLOT` / square-off | `fno_oi_fetch_5min.LAST_SLOT`, `config.SQUARE_OFF` |
| 15:32 | Reporters stop polling | trade-logger / net-result |
| 15:33 | Futures fetch end deadline (`LAST_SLOT + 3 min`) | `fno_oi_fetch_5min` |
| 15:40 | EOD data quality control | scheduled task |

### 2.2 Futures OI fetch — `fno_oi_fetch_5min.py`

| Parameter | Default | Meaning |
|---|---:|---|
| `--boundary-buffer-sec` | **3.0** | wait after the bar boundary before fetching |
| `--poll-sec` | 1.0 | idle poll interval |
| `--request-interval-sec` | **0.36** | per-app API pacing (floor 0.34) |
| `--timeout-sec` | 8.0 | per-request timeout |
| `--max-retries` | 3 | per-contract retries |
| `--slot-retry-attempts` | 2 | whole-slot retries |
| `--slot-retry-delay-sec` | 2.0 | gap between slot retries |
| `--partial-retry-sec` | 30.0 | re-attempt cadence while a slot is PARTIAL |
| `--max-apps` | 8 | parallel Kite apps |
| `--min-coverage` | `MIN_STOCK_FUTURES_COVERAGE` | exact-slot mapped-stock coverage floor |

**Throughput arithmetic.** ~210 near-month contracts across 8 apps ≈ 26 calls
per app; at 0.36 s pacing that is a **~9.4-second floor**, before latency and
retries. In practice the slot completes 10–20 s after the boundary.

### 2.3 Cash 1-minute feed — `fno_equity_fetch_1min.py`

| Parameter | Default | Meaning |
|---|---:|---|
| `--boundary-buffer-sec` | **3.0** | fingerprinted; CLI attempts to weaken it are rejected |
| `--poll-sec` | 1.0 | idle poll |
| `--request-interval-sec` | 0.36 | per-app pacing |
| `--timeout-sec` | 8.0 | per-request timeout |
| `--max-apps` | 8 | parallel apps |
| `MIN_NO_CANDLE_OBSERVATIONS` | **3** | clean empty responses before "verified absent" |
| `MIN_NO_CANDLE_VERIFICATION_AGE_SEC` | **15** | minimum publication age before verification counts |
| `DEFAULT_NO_CANDLE_OBSERVATION_SPACING_SEC` | **2.0** | minimum gap between observations |

Policy strings: `FEED_POLICY_VERSION = candidate_exact_completed_1m_verified_no_candle_v1`,
`NO_CANDLE_RESOLUTION_POLICY = ALL_WRITTEN_OR_VERIFIED_NO_CANDLE`.

**The 1-minute feed is far faster than the 5-minute one** because it fetches
*only the candidate set* — typically 5–40 symbols, not 210.

### 2.4 Confirmation and entry

| Parameter | Value | Meaning |
|---|---:|---|
| `CONFIRMATION_COMPLETED_BOUNDARY_BUFFER_SEC` | 3.0 | fingerprinted completed-candle buffer |
| `--confirmation-max-wait-sec` | **90** | slot unprocessed past this → `BLOCKED_STALE_ACTIVATION` |
| `ENTRY_ACTIVATION_GRACE_SEC` | **90** | first-time entries blocked after `confirmation_end + 90 s` |
| Entry worker poll | 1 s | LTPs in one batched `ltp()` call |
| Reporter poll | 5 s | until 15:32 |

---

## 3. One slot, second by second — the 09:25 example

The 09:25 signal bar covers **09:20:00 – 09:24:59** and is *end-labelled* 09:25.

```
TIME        LANE                    WHAT HAPPENS
─────────── ─────────────────────── ──────────────────────────────────────────────
09:25:00.0  exchange                5-minute bar closes
                                    1-minute bar 09:25 also closes

09:25:00.0  ── boundary buffer ──   3.0 s wait (both fetchers)
09:25:03.0

09:25:03    fno_oi_fetch_5min       fetch near-month futures 5m OHLCV+OI
            (8 apps, 0.36 s pace)   ~210 contracts ÷ 8 apps ≈ 26 each
09:25:13±   ...                     ≈ 9.4 s floor + latency
                                    per-contract: 8 s timeout, 3 retries
                                    slot-level: 2 retries, 2 s apart

09:25:03    eod_5min_data           cash 5m bars for the full universe
09:25:15±   ...                     writes slot_ready_5m/slot_20260901_0925.json

09:25:15±   markers published       slot_ready/slot_20260901_0925.json
                                      state = SUCCESS (complete) | PARTIAL

09:25:15±   scanner-5m GATE         _slot_marker_ready() requires BOTH:
                                      futures: schema fno_oi_fetch_slot_v2,
                                               source=final, complete=true,
                                               exact slot, full+symbol hashes match,
                                               coverage >= 99%,
                                               <= 2 verified no-candle stocks,
                                               each with 3 clean observations
                                      cash:    source=final, complete=true,
                                               tickers_written == complete == expected,
                                               tickers_failed == 0,
                                               quality_complete == true,
                                               universe sha256 matches
                                    → fails? keep waiting. 09:50 → BLOCKED.

09:25:15±   MERGE + ANALYSE         per contract:
                                      load futures 5m parquet
                                      load live equity 5m store
                                      join_equity_price_with_futures_oi()
                                      take the EXACT 09:25 row
                                      apply the base gate

09:25:16±   scanner snapshot        immutable, fingerprinted, hashed
                                    candidate set frozen here

─────────── ─────────────────────── ──────────────────────────────────────────────
09:26:00.0  exchange                confirmation candle closes
                                    (covers 09:25:00 – 09:25:59, labelled 09:26)

09:26:00.0  ── boundary buffer ──   3.0 s, fingerprinted, cannot be weakened
09:26:03.0

09:26:03    equity-1min-feed        fetch ONLY the frozen candidate set
09:26:06±   ...                     persist raw bar → read back → slot parquet
                                    marker binds: scanner hash, candidate-set hash,
                                    strategy fingerprint, bar-set sha256

09:26:06±   confirmation-1m         READ-ONLY consumer, never calls historical_data
                                      validate every attestation
                                      compute direction / body / wick / trigger
                                      apply per-leg filters
                                      rank → take top max_entries
09:26:07±   signals/*.json          authoritative signal IDs listed in the snapshot

09:26:03    ── activation window ── 90 s: 09:26:03 → 09:27:33
09:27:33                            a first-time entry after this is CANCELLED
                                    with LATE_START_NO_RETROACTIVE_ENTRY

09:26:07±   entry workers           PENDING_ENTRY, 1 s poll, batched LTP

09:27:00    exchange                first fillable 1-minute bar closes
            EARLIEST FILL           the confirmation candle can never fill itself
...
15:30       square-off              forced flat
```

### 3.1 Why the confirmation candle cannot fill itself

The order is *derived from* that candle's extreme. Allowing it to fill on the
same candle would use the candle's own high/low to both set and trigger the
order — a look-ahead. In the engine this is one guard:

```python
if runtime.order_placed_at is None or ts <= runtime.order_placed_at:
    continue
```

So for a 09:25 signal confirming on the 09:26 candle, the earliest fillable bar
is the one ending **09:27**.

---

## 4. The merge, in detail

`hybrid.join_equity_price_with_futures_oi(equity_frame, futures_frame)`

### 4.1 Equity side — every indicator

```python
out = completed_real_equity_five_minute_bars(equity_frame).sort_values("ts")

for span in (9, 20, 50):
    out[f"ema{span}"] = out["close"].ewm(span=span, adjust=False).mean()

out["prev_close"]       = out["close"].shift(1)
out["price_change_pct"] = (out["close"] / out["prev_close"] - 1.0) * 100.0

prior_volume            = out["volume"].shift(1).rolling(20, min_periods=5).mean()
out["volume_ratio"]     = out["volume"].div(prior_volume.where(prior_volume.gt(0)))

out["traded_value"]     = out["close"] * out["volume"]
```

| Indicator | α / window | Note |
|---|---|---|
| `ema9` | α = 0.200000 | recursive, seeded at bar 0 |
| `ema20` | α = 0.095238 | **no warm-up guard** |
| `ema50` | α = 0.039216 | **continuous across sessions** |
| `volume_ratio` | 20 bars, min 5 | **denominator excludes the current bar** |

> **The 20-bar volume window crosses the session boundary.** Twenty five-minute
> bars is ~100 minutes. At 09:25 only the 09:20 bar belongs to today, so **19 of
> the 20 denominator bars are the previous session's last ~95 minutes**. A
> `volume_ratio >= 3.0` filter means "three times yesterday's late-session
> per-bar average".

### 4.2 Futures side — three fields only

```python
futures["oi"]      = pd.to_numeric(futures["oi"], errors="coerce")
futures["prev_oi"] = futures["oi"].shift(1)          # prior 5-minute futures bar

valid_oi_pair = (futures["oi"].gt(0) & futures["prev_oi"].gt(0)
                 & np.isfinite(futures["oi"]) & np.isfinite(futures["prev_oi"]))

futures["oi_change_pct"] = np.where(
    valid_oi_pair, (futures["oi"] / futures["prev_oi"] - 1.0) * 100.0, np.nan)

oi_only = futures[["ts", "oi", "prev_oi", "oi_change_pct"]].drop_duplicates("ts", keep="last")
```

### 4.3 The join

```python
merged = equity.merge(oi_only, on="ts", how="inner", validate="one_to_one")
merged["price_source"] = "NSE_EQUITY"
merged["oi_source"]    = "NFO_FUTURE"
```

| Property | Consequence |
|---|---|
| `how="inner"` | a bar missing on either side is dropped, never forward-filled |
| `validate="one_to_one"` | duplicate timestamps raise instead of fanning out rows |
| `on="ts"` | exact bar-end timestamp — no tolerance, no nearest-match |
| only 4 columns cross | **`oi`, `prev_oi`, `oi_change_pct`** (and `ts`) |

**Everything priced, ordered, filled and exited is NSE cash equity.** The future
contributes open interest and nothing else.

### 4.4 Bar admission, before any of the above

A 5-minute equity bar is usable only if it is: not the 09:15 opening snapshot ·
not `gap_filled` · not `opening_snapshot` · not `provisional_stale` · built from
**exactly 5 one-minute rows** when `source_1m_count` is present · not an exact
OHLCV copy of the adjacent prior bar unless both are proven 5×1m.

---

## 5. Worked example — illustrative arithmetic

> The prices below are **constructed with round numbers** so the arithmetic is
> checkable by hand. A real recorded trade follows in §6.

### 5.1 Setup

Symbol **ACME**, slot **09:40**, leg **09:40_SHORT**
(V12 book: cap 1 · picker `max_move` · price ≤ −0.20% · OI ≥ 0.10% ·
**volume ≥ 1.50** · body ≥ 0.40 · wick ≤ 0.50 · stop 1.00% · target 3.00%)

### 5.2 Fetch — what arrives at 09:40:03

**Cash equity 5-minute bar, ts = 09:40** (covers 09:35:00–09:39:59)

| Field | Value |
|---|---:|
| open | 1,005.00 |
| high | 1,006.00 |
| low | 996.00 |
| close | **997.00** |
| volume | **300,000** |

**Near-month future 5-minute row, ts = 09:40**

| Field | Value |
|---|---:|
| oi | **1,012,000** |
| oi (ts = 09:35) | 1,000,000 |

**Prior context** (already in the store)

| Field | Value |
|---|---:|
| close at ts = 09:35 | **1,000.00** |
| mean volume, 20 bars ending 09:35 | **150,000** |
| ema9 / ema20 / ema50 at 09:40 | 998.5 / 1,002.0 / 1,008.0 |

### 5.3 Merge and analyse

```
price_change_pct = (997.00 / 1000.00 − 1) × 100          = −0.30 %
volume_ratio     = 300,000 / 150,000                     =  2.00
traded_value     = 997.00 × 300,000                      =  Rs 29.91 cr
prev_oi          = oi.shift(1)                           =  1,000,000
oi_change_pct    = (1,012,000 / 1,000,000 − 1) × 100     = +1.20 %
ema stack        = 998.5 < 1002.0 < 1008.0               →  SHORT-aligned
```

### 5.4 Base gate — the loose superset

| Test | Value | Threshold | Pass |
|---|---:|---:|:--:|
| ema9 < ema20 < ema50 | 998.5 < 1002.0 < 1008.0 | — | ✅ |
| price_change_pct ≤ −0.10 | −0.30 | −0.10 | ✅ |
| oi > prev_oi | 1,012,000 > 1,000,000 | — | ✅ |
| oi_change_pct ≥ 0.05 | +1.20 | 0.05 | ✅ |
| volume_ratio ≥ 0.80 | 2.00 | 0.80 | ✅ |

→ **candidate raised**, side SHORT.

### 5.5 Per-leg filters — 09:40_SHORT, V12

| Test | Value | Threshold | Pass |
|---|---:|---:|:--:|
| price_change_pct ≤ −0.20 | −0.30 | −0.20 | ✅ |
| oi_change_pct ≥ 0.10 | +1.20 | 0.10 | ✅ |
| **volume_ratio ≥ 1.50** | **2.00** | **1.50** | ✅ |
| traded_value ≥ 0 | 29.91 cr | 0 | ✅ |

> Under **V10 and V11** the volume floor here is **1.00**, so a candidate at
> `volume_ratio = 1.30` would pass those two and be **rejected by V12**. That
> single threshold is the entire V11→V12 difference.

### 5.6 Confirmation — the 09:41 candle

Covers 09:40:00 – 09:40:59.

| Field | Value |
|---|---:|
| open | 996.00 |
| high | 996.50 |
| low | **992.00** |
| close | 993.00 |

```
range          = 996.50 − 992.00                    = 4.50
body_ratio     = |993.00 − 996.00| / 4.50           = 0.667   (≥ 0.40 ✅)
adverse_wick   = (min(996.00, 993.00) − 992.00)/4.50= 0.222   (≤ 0.50 ✅)
direction      = close 993.00 < open 996.00                    ✅
beyond 5m close= close 993.00 < 997.00                         ✅
trigger        = low = 992.00, buffer 0 bps, tick-rounded down = 992.00
```

→ **confirmed**. Ranked by `max_move` (|−0.30|); cap is 1, so the largest mover
in the slot takes the slot.

### 5.7 Entry, brackets, sizing

```
Order          SL-M SELL, trigger 992.00, product MIS
Earliest fill  the bar ending 09:42 — the 09:41 candle cannot fill itself

Say it fills at 992.00:
  stop   = 992.00 × (1 + 1.00/100)  = 1,001.92  → tick-rounded UP   = 1,001.95
  target = 992.00 × (1 − 3.00/100)  =   962.24  → tick-rounded UP   =   962.25
  quantity = floor(50,000 / 992.00) = 50
  notional = 992.00 × 50            = Rs 49,600
```

| Outcome | Exit | Gross | Cost (15 bps) | Net % | Net Rs |
|---|---:|---:|---:|---:|---:|
| Target hit | 962.25 | +3.000% | −0.150% | **+2.850%** | **+Rs 1,413.75** |
| Stop hit | 1,001.95 | −1.003% | −0.150% | **−1.153%** | **−Rs 571.87** |

```
gross_short = (1 − exit/entry) × 100
net         = gross − cost_bps/100
net_pnl_rs  = direction × (exit − entry) × quantity − entry × quantity × cost_bps/10,000
```

**Reward:risk is 1:3.0** — a 1.00% stop against a 3.00% target. Both sides are
fixed in rupees by the Rs 50,000 exposure: roughly **−Rs 570 risked** to make
**+Rs 1,415**.

### 5.8 Same-bar collision

If a single one-minute bar reaches both 1,001.95 and 962.25, **`STOP_FIRST`
applies** — the loss is taken. Minute OHLC cannot reveal which came first, so
the engine always assumes the worse ordering.

---

## 6. A real recorded trade

From the sealed V12 run — **only fields actually stored** are shown.

| Field | Value |
|---|---|
| Session | 2026-07-24 |
| Setup | `09:40_SHORT` |
| Side | SHORT |
| Symbol | MOTILALOFS |
| Entry time | **09:42:00** |
| Exit time | **09:56:00** |
| Exit reason | `TARGET` |
| Net return | **+2.85%** |
| Net P&L | **+Rs 1,420.44** |
| MFE (OHLC lower bound) | 3.0000% |
| MAE (OHLC upper bound) | 0.0899% |

The entry at **09:42** is exactly the pattern in §5.7: signal bar 09:40,
confirmation candle 09:41, earliest fillable bar 09:42. Holding time was
**14 minutes**. The MAE of 0.09% means it barely traded against the position
before reaching target — the 1.00% stop was never seriously threatened.

---

## 7. Where it stops

Any one of these halts the pipeline rather than trading partial data.

| Gate | Condition | Result |
|---|---|---|
| Futures marker | not `final` / not `complete` / wrong slot / hash mismatch | scanner waits |
| Futures coverage | mapped-stock coverage < 99% | scanner waits |
| No-candle cap | more than 2 absent stock futures | scanner waits |
| No-candle proof | fewer than 3 clean observations for an absent contract | scanner waits |
| Cash marker | `tickers_failed > 0` or written ≠ complete ≠ expected | scanner waits |
| Universe hash | cash marker hash ≠ scanner's mapped equity set | scanner waits |
| **09:50** | any slot still unscanned | **scanner BLOCKED, exits** |
| 1m feed | API failure / invalid OHLCV / foreign symbol / unverified absence | slot blocked |
| Confirmation | not processed within 90 s of `confirmation_end` | `BLOCKED_STALE_ACTIVATION` |
| Entry | first seen after `confirmation_end + 90 s` | `CANCELLED` — no retroactive entry |
| Downstream | no artefacts + upstream BLOCKED/FAILED | `UPSTREAM_BLOCKED` |

**Nothing is ever synthesized, forward-filled, or reintroduced by a later
backfill.** A contract attested as `NO_CANDLE` is recorded as
`SKIPPED_NO_CANDLE` *before* data loads and stays skipped for that slot.

---

## 8. Timing summary card

```
BAR BOUNDARY          +0.0 s   exchange closes the candle
BOUNDARY BUFFER       +3.0 s   both fetchers wait (fingerprinted on the 1m side)
FUTURES FETCH        +10-20 s  ~210 contracts / 8 apps / 0.36 s pace
CASH 5m FETCH        +10-20 s  full universe
BOTH MARKERS          ~+15 s   scanner gate can pass
SCAN + SNAPSHOT        ~+1 s   merge, base gate, freeze the candidate set
    ── next minute ──
1m BOUNDARY           +0.0 s   confirmation candle closes
BOUNDARY BUFFER       +3.0 s   fingerprinted, cannot be weakened by CLI
1m FETCH             +3-6 s    candidate set only (5-40 symbols)
CONFIRM + SELECT       ~+1 s   read-only, morphology, rank, cap
ACTIVATION WINDOW      90 s    first-time entries only inside this
    ── next minute ──
EARLIEST FILL                  the confirmation candle can never fill itself
    ── 15:30 ──
SQUARE-OFF                     forced flat
```

---

## 9. Related documentation

- [FNO_V6_LIVE_STRATEGY.md](FNO_V6_LIVE_STRATEGY.md) — the six live roles in full
- [FNO_PAPERTRADE_DASHBOARD_OPERATIONS.md](FNO_PAPERTRADE_DASHBOARD_OPERATIONS.md) — the dashboard that surfaces all of this
- [FNO_V12_LATE_SHORT_VOLUME_BACKTEST_STRATEGY.md](FNO_V12_LATE_SHORT_VOLUME_BACKTEST_STRATEGY.md) — the current selected book
- [FNO_V8_BACKTEST_STRATEGY.md](FNO_V8_BACKTEST_STRATEGY.md) — the execution model behind the fill and bracket rules
