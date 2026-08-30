# FnO V6 — Live / Paper Trading Strategy (Complete Reference)

**Generation:** `v6` · **Strategy version:** `FNO_V6_BEST_NET_CASH_EQUITY_20260811` ·
**Selection objective:** `BEST_NET` · **Default execution mode:** `PAPER`

| | |
|---|---|
| Live entry point | [fno_v6_live.py](fno_v6_live.py) → shared runtime [fno_v5_live.py](fno_v5_live.py) |
| Live configuration | [fno_v6_live_config.py](fno_v6_live_config.py) |
| Frozen setup book | [fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py](fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py) |
| Durable 1-minute feed | [fno_equity_fetch_1min.py](fno_equity_fetch_1min.py) |
| Evidence replay | [fno_v6_parity_replay.py](fno_v6_parity_replay.py) |
| Data contract | [fno_oi_hybrid_data.py](fno_oi_hybrid_data.py) — `fno_v5_equity_real_5m_futures_oi_v4` |
| Artefact root | `C:\TradingData\eqidv2\fno_oi\v6_live\` |

---

## 1. What the strategy actually does

In the **first 30 minutes** of the NSE session, V6 hunts for F&O-listed stocks
whose **cash price is trending** (EMA9 > EMA20 > EMA50 for longs, inverted for
shorts), that just **moved hard on a completed 5-minute bar**, on **expanding
volume**, while **open interest in the mapped near-month future is rising**.

The economic thesis is narrow and explicit: a fresh directional impulse that is
*backed by new positions being opened* (OI up) rather than by short-covering or
position unwinding (OI down). OI is the only thing the derivative contributes.

That 5-minute bar raises a **candidate**. The very next **1-minute candle** must
close in the same direction and beyond the 5-minute close to **confirm**. Entry
is then a **stop order at that confirmation candle's extreme** — so the market
must trade *through* the confirmation high (LONG) or low (SHORT) before a single
rupee of risk is taken. Each fill carries a fixed percentage stop and target and
is squared off at **15:30** regardless of state.

Only five scan slots are traded — **09:25, 09:30, 09:35, 09:40, 09:45** — with at
most one LONG and one-or-two SHORT names per slot, at **Rs 10,000 capital ×
5x = Rs 50,000 target exposure** per entry.

> **The single most important structural fact:** everything priced, ordered,
> filled and exited is **NSE cash equity**. The future supplies exactly three
> fields — `oi`, `prev_oi`, `oi_change_pct` — and nothing else. Contract
> validators enforce that separation on both the backtest and the live path.

---

## 2. Instrument and data contract

| Concern | Source | Notes |
|---|---|---|
| Price, volume, OHLC, EMA9/20/50, `traded_value` | **NSE cash equity** | 5-minute bars, end-labelled |
| Confirmation candle, trigger, entry, exits, LTP | **NSE cash equity** | 1-minute bars / broker quotes |
| `oi`, `prev_oi`, `oi_change_pct` | **NFO near-month future** | joined on exact bar-end timestamp |
| Universe (live) | `latest_near_month.parquet` | may roll intra-month |
| Universe (promoted backtest) | dated `near_month_2026-08-11.parquet` | the mutable `latest_*` alias is **refused** |

### 2.1 Future → equity mapping

`hybrid.ensure_equity_mapping()` maps every stock future to its cash symbol. A
stock future that cannot be mapped is a **hard failure** — the scanner raises.
Only *index* futures may be dropped. `LTM → LTIM` is the single alias.

### 2.2 Bar quality (`completed_real_equity_five_minute_bars`)

A 5-minute equity bar is usable only if it is:

- not the **09:15 opening snapshot**;
- not flagged `gap_filled`, `opening_snapshot` or `provisional_stale`;
- built from **exactly 5 one-minute rows** when `source_1m_count` is present;
- not an exact OHLCV copy of the adjacent prior bar (unless both are proven 5×1m).

### 2.3 OI admission

OI is admitted only when **both `oi` and `prev_oi` are positive and finite**.
Otherwise `oi_change_pct` is NaN and the row is structurally incapable of
signalling.

> **Known limitation, carried in the report header:** the historical OI cache
> uses **26AUG futures OI across the whole backtest period**. It is not a
> rolling near-month OI series.

---

## 3. Strategy specification

### 3.1 Scan slots and confirmation map

Five 5-minute signal bars, each with one fixed 1-minute confirmation bar:

```
09:25 → 09:26    09:30 → 09:31    09:35 → 09:36    09:40 → 09:41    09:45 → 09:46
```

Timestamps are **candle-end labelled**. The 09:25 signal bar covers 09:20–09:25;
its confirmation candle covers 09:25–09:26. 09:20 is never a signal bar — it has
no 5-minute predecessor to diff against.

`SIGNAL_TO_CONFIRMATION` in the live config is validated at import; a mismatch
raises before anything runs.

### 3.2 Base candidate gate — the loose superset

Applied per contract on the signal bar. **Identical** in backtest
(`fno_oi_ema_confirm_sweep.build_signal_table`) and live
(`fno_v5_live._base_signal_side`):

```
LONG   : ema9 > ema20 > ema50   AND   price_change_pct >= +0.10
SHORT  : ema9 < ema20 < ema50   AND   price_change_pct <= -0.10
BOTH   : oi > prev_oi           AND   oi_change_pct >= 0.05
                                AND   volume_ratio   >= 0.80
```

| Term | Definition |
|---|---|
| `price_change_pct` | close vs previous 5-minute close, in % |
| `volume_ratio` | bar volume ÷ 20-bar prior-volume mean (min 5 periods, **shifted** — no look-ahead) |
| `traded_value` | close × volume |
| `oi_change_pct` | (oi − prev_oi) / prev_oi × 100 |

Any row with a NaN in a required column is rejected. This superset is
deliberately loose: **every tradable setup is a subset of it**, so signals are
computed once and reused across the entire parameter search.

### 3.3 Confirmation gate (1-minute)

On the candle ending at `confirmation_end`:

```
range       = high - low                          (must be > 0)
body_ratio  = |close - open| / range
wick_ratio  = (upper wick if LONG else lower wick) / range
trigger     = high if LONG else low

LONG  confirmed  <=>  close > open  AND  close > signal-bar close
SHORT confirmed  <=>  close < open  AND  close < signal-bar close
```

A candidate failing direction is dropped as `direction_rejected` and never
reaches setup filtering.

### 3.4 The frozen V6 setup book — 10 legs

`ACTIVE_SETUPS`, all in `FILTERED` mode with `min_traded_value = 0`:

| Signal | Confirm | Side | Max | Picker | Price % | OI % | Vol ratio | Body ≥ | Wick ≤ | Stop % | Target % |
|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 09:25 | 09:26 | LONG | 1 | max_liquidity | 0.30 | 0.10 | 3.0 | 0.6 | 0.5 | 0.50 | 3.00 |
| 09:25 | 09:26 | SHORT | 2 | max_volume | 0.20 | 0.10 | 1.5 | 0.4 | 0.5 | 0.75 | 3.00 |
| 09:30 | 09:31 | LONG | 1 | max_move | 0.65 | 0.10 | 1.0 | 0.5 | 0.5 | 1.00 | 2.50 |
| 09:30 | 09:31 | SHORT | 1 | max_move | 0.20 | 0.25 | 1.0 | 0.4 | 0.5 | 1.00 | 3.00 |
| 09:35 | 09:36 | LONG | 1 | max_liquidity | 0.20 | 0.10 | 1.0 | 0.6 | 0.5 | 1.00 | 2.50 |
| 09:35 | 09:36 | SHORT | 2 | max_liquidity | 0.50 | 1.00 | 1.0 | 0.4 | 0.5 | 1.00 | 3.00 |
| 09:40 | 09:41 | LONG | 1 | max_liquidity | 0.20 | 0.10 | 2.0 | 0.5 | 0.5 | 0.50 | 2.50 |
| 09:40 | 09:41 | SHORT | 1 | max_move | 0.20 | 0.10 | 1.0 | 0.4 | 0.5 | 1.00 | 3.00 |
| 09:45 | 09:46 | LONG | 1 | max_move | 0.65 | 0.10 | 1.0 | 0.4 | 0.5 | 1.00 | 3.00 |
| 09:45 | 09:46 | SHORT | 1 | max_volume | 0.20 | 0.75 | 1.0 | 0.4 | 0.3 | 1.00 | 2.00 |

For a LONG leg the price filter is `price_change_pct >= +X`; for a SHORT leg it
is `price_change_pct <= -X` (the table stores the magnitude).

**Structural invariants** enforced at import time by `validate_configuration()` /
`validate_strategy()`:

1. exactly **10 legs**;
2. exactly **one leg per (slot, side)**;
3. confirmation times must match the canonical map;
4. **LONG cap 1 / SHORT cap 2**.

Any violation raises before the runtime starts. Hard ceiling: **5 LONG + 7 SHORT
= 12 orders per day**.

### 3.5 Ranking and selection

Per (slot, side), among confirmed candidates that pass that leg's filters:

```
sort by  picker value      DESC
then     traded_value      DESC     (liquidity tie-break)
then     tradingsymbol     ASC      (deterministic)
take     top `max_entries`
```

| Picker | Sort key |
|---|---|
| `max_oi` | `oi_change_pct` |
| `max_volume` | `volume_ratio` |
| `max_move` | `abs(price_change_pct)` |
| `max_body` | `body_ratio` |
| `max_liquidity` | `traded_value` |

Backtest (`select_setup_rows`) and live (`config.rank_candidates`) implement the
same ordering — the backtest groups per day, the live path is already inside one
session.

### 3.6 Entry, exits, sizing and cost

| Item | Rule |
|---|---|
| Entry order | **Stop-market** at the confirmation candle extreme (`trigger`), tick-rounded |
| Stop | `entry × (1 ∓ stop_pct/100)`, tick-rounded |
| Target | `entry × (1 ± target_pct/100)`, tick-rounded |
| Time exit | Square-off at **15:30** |
| Same-bar tie-break | If stop and target are both reachable, **stop wins** (pessimistic) |
| Capital | Rs 10,000 per entry |
| Leverage | 5x → **Rs 50,000 target exposure** |
| Quantity | `floor(50,000 / entry_price)`; LIVE additionally floors to a lot multiple |
| Cost | **5 bps round trip**, charged on entry notional |
| Product | MIS (intraday) |

Capital and leverage are **hard-locked**. Passing any other `--capital` /
`--leverage` to the live runtime raises immediately.

---

## 4. Live / paper runtime architecture

### 4.1 Six roles

`fno_v6_live.py` sets `FNO_LIVE_GENERATION=v6` and delegates to the shared
runtime, which loads `fno_v6_live_config`. Six **independently scheduled and
independently monitored** roles:

```
 UPSTREAM FEEDS (must be final + complete)
  |-- fno_oi_universe          08:50   near-month universe
  |-- fno_oi_fetch_5min        09:05   futures 5m + slot markers
  '-- equity live 5m feed              cash slot markers
                    |
                    v
             [ scanner-5m ]                candidate superset per slot
                    |  immutable scanner evidence
                    v
        [ equity-1min-feed ]               fetch exact completed bar once,
                    |                      persist + attest
                    v  immutable marker + slot parquet
        [ confirmation-1m ]                confirm -> rank -> select
                    |  signals/*.json
          +---------+---------+
          v                   v
   [ long-entry ]       [ short-entry ]    PAPER or LIVE
          +---------+---------+
                    v  orders/<MODE>/<date>/*.json
          +---------+---------+
          v                   v
  [ trade-logger ]      [ net-result ]     CSV + md, realized/unrealized/ROC
```

Artefact root `<FNO_ROOT>/v6_live/` contains `scanner_5m/`, `confirmation_1m/`,
`evidence/`, `signals/`, `orders/{PAPER,LIVE}/` and `consolidated/`. The shared
durable feed lives under `<FNO_ROOT>/raw_equity_1m/` and
`<FNO_ROOT>/equity_1m_slot_ready/`. Strategy, arming and kill-switch manifests
remain under the generation root. Markdown status goes to
`<FNO_ROOT>/latest/latest_fno_v6_*.md`.

### 4.2 Weekday schedule

From [bat/schedule_fno_oi_weekday.ps1](bat/schedule_fno_oi_weekday.ps1), Mon–Fri:

| Time | Task |
|---|---|
| 08:50 | `run_fno_oi_universe.bat` — near-month universe |
| 09:05 | `run_fno_oi_fetch_5min.bat` — futures 5-minute fetch + slot markers |
| 09:15 | `run_fno_oi_feature_ranker.bat` |
| 09:15 | `run_fno_v6_scanner_5min.bat` |
| 09:15 | `run_fno_v6_equity_1min_feed.bat` |
| 09:15 | `run_fno_v6_confirmation_1min.bat` |
| 09:15 | `run_fno_v6_live_long.bat` / `run_fno_v6_live_short.bat` |
| 09:15 | `run_fno_v6_trade_logger.bat` / `run_fno_v6_net_result.bat` |
| 15:40 | `run_fno_oi_eod_qc.bat` |

The installer **disables the equivalent V5 tasks** — V6 replaced V5 in
production. Every `.bat` runner exports `FNO_V6_EXECUTION_MODE=PAPER`.

### 4.3 Start-up gate — run by every role

```
config.validate_strategy()          -> 10 legs, caps, confirmation map
config.attest_selected_backtest()   -> frozen CSV + provenance hashes + metrics
_write_manifest()                   -> strategy_manifest.json (payload + fingerprint)
capital == 10,000 and leverage == 5.0     (else raise)
trading-day check                   (else SKIPPED_NON_TRADING_DAY)
```

The **strategy fingerprint** is the SHA-256 of the full strategy payload — setup
book, gates, slots, cost, sizing, and the locked futures-readiness policy. It is
stamped on every scanner snapshot, confirmation snapshot, entry signal and order
state, and re-checked at every handoff. **Any config edit invalidates the day's
artefacts instead of silently mixing books.**

---

## 5. Role-by-role behaviour

### 5.1 `scanner-5m`

Waits for **slot end + 3 s**, then requires **both** feed markers for that exact
slot before scanning (`_slot_marker_ready`).

**Futures marker** — `<FNO_ROOT>/slot_ready/slot_<YYYYMMDD_HHMM>.json`:

- schema `fno_oi_fetch_slot_v2`, policy `verified_stock_no_candle_skip_v1`,
  `source == "final"`, `complete == true`, exact slot match;
- the marker's full and symbol-set hashes match the scanner's exact mapped
  stock-futures universe;
- stock coverage is **recomputed from exact symbol lists** and must be ≥ 99% —
  the marker cannot lower this floor;
- at most **two** absent stock futures may be admitted, and each must carry
  **three clean `NO_CANDLE` observations**;
- every admitted absence must be named exactly in
  `stock_verified_no_candle_symbols` — foreign, unverified or unlisted missing
  symbols **block**;
- API failures and invalid candles are global blockers; index-future `NO_CANDLE`
  outcomes do not reduce stock coverage;
- legacy/count-only markers are accepted only when `no_candle_count == 0`.

**Cash 5-minute marker** — `slot_ready_5m/slot_<YYYYMMDD_HHMM>.json`:

- `source == "final"`, `complete == true`, slot matches;
- `tickers_written == tickers_complete == tickers_expected`, `tickers_failed == 0`;
- `fno_equity_quality_complete == true`;
- `fno_equity_ready == fno_equity_expected`, `fno_equity_failed == 0`;
- `fno_equity_universe_sha256` matches the scanner's own mapped equity set.

Contracts attested in the marker are recorded as `SKIPPED_NO_CANDLE` **before
data loads**; they are never synthesized, forward-filled, or reintroduced by a
later backfill. For every other contract the scanner loads futures 5m + live
equity 5m, joins OI, takes the exact slot row and applies the base gate.

The snapshot reports verified skips and unexpected missing contracts separately.
It is `SUCCESS` when the only omissions are verified skips and `PARTIAL` for any
other missing or invalid contract.

**09:50 pipeline deadline:** if all slots are not done by `PIPELINE_DEADLINE`,
the role publishes `BLOCKED` and exits. Downstream roles see that and stop
rather than trading a half-built book.

### 5.2 `equity-1min-feed`

The dedicated `fno_equity_fetch_1min.py` producer starts before the first slot,
pre-authenticates the available Kite apps **in parallel** as soon as a valid
scanner snapshot exists, then waits until the exact confirmation boundary plus
the fingerprinted **3-second completed-candle buffer**. CLI attempts to weaken or
change that buffer are rejected.

It fetches **only the immutable candidate set**, persists each exact
end-labelled bar under `raw_equity_1m/`, reads it back, and publishes a compact
slot parquet plus an `fno_equity_1m_slot_v1` marker with **fsync'd, atomic,
create-once** semantics. Identical retries reuse the artefact; a non-identical
concurrent writer **fails closed** instead of replacing it.

The marker binds the scanner snapshot, candidate contract set, strategy
fingerprint and bar-set SHA-256, and carries exact written / no-candle /
invalid / API-failed / unexpected symbol lists plus attempts and publication
time.

**Empty-response policy:** an empty API response is *not* immediately treated as
a no-trade candle. It must be observed cleanly **three times** and only after the
configured minimum publication age (15 s, ≥ 2.0 s spacing); otherwise the
producer keeps the marker provisional. A verified no-candle candidate becomes
`INELIGIBLE_NO_CANDLE` for that slot, while written candidates continue. API
failures, invalid OHLCV, foreign/unexpected symbols, or unverified absence keep
the slot blocked. **No candle is ever fabricated or forward-filled.**

### 5.3 `confirmation-1m`

A **read-only filesystem consumer**. It rejects stale version / fingerprint /
date / slot / data-contract, candidate-set hash, scanner hash, data hash or
deadline attestations, then computes body / wick / trigger / direction (§3.3)
**only from the immutable slot parquet**. It never calls `historical_data`, and
writes signals only after the complete in-window decision has been made.

**Staleness rule:** if the slot is not processed within
`--confirmation-max-wait-sec` (default **90 s**) after the confirmation time, the
slot is written as `BLOCKED_STALE_ACTIVATION`. The same deadline is enforced by
scheduled, `--once`, and explicit `--slot` paths **before any signal file is
published**. Deadline equality is allowed; one microsecond later is stale. A
late-starting process cannot enter yesterday's — or ten minutes ago's — trade.

Before the completed boundary, and while a complete scanner is still waiting for
its final durable feed inside the 90-second window, `--once` and manual `--slot`
return `WAITING` **without committing** a confirmation snapshot or signal. A
restart therefore remains able to process the slot; only a definitively
incomplete scanner or an expired activation deadline is terminal.

**Authoritative signals:** only signal IDs listed in a valid confirmation
snapshot count. `load_signals()` reads the snapshot list first, ignores stray
JSON files, raises if a listed file is missing, and re-validates every field
(deterministic ID, setup fields, sizing, rank ≤ cap, tokens, deadline) before a
worker may act.

### 5.4 Entry workers — PAPER

Poll loop (1 s): load authoritative signals for the side, create or load order
state, quote LTPs in **one batched `ltp()` call**, advance each state.

```
 signal seen before deadline  --> PENDING_ENTRY
 first seen after deadline    --> CANCELLED   (LATE_START_NO_RETROACTIVE_ENTRY)
 quantity == 0                --> BLOCKED_SIZING

 PENDING_ENTRY -- LTP crosses trigger --> OPEN
 PENDING_ENTRY -- 15:30 untouched     --> NO_FILL
 OPEN -- stop / target / 15:30        --> CLOSED
```

On fill, stop and target are recomputed **from the observed fill price**. While
OPEN the state carries running gross/net P&L. `_close_state` charges
`entry_notional × 5 bps` and records `net_return_exposure_pct` and
`return_on_capital_pct`.

Activation deadline = `confirmation_end + 90 s` (`ENTRY_ACTIVATION_GRACE_SEC`).
A restart after that window cannot open a *new* first-time entry; existing
states continue to be managed.

### 5.5 Entry workers — LIVE

LIVE requires **all four**:

1. `--execution-mode LIVE` (default is PAPER, and every `.bat` pins PAPER);
2. env `FNO_V6_LIVE_ACK = I_UNDERSTAND_REAL_FNO_V6_EQUITY_ORDERS`;
3. `live_arm.json` with `enabled: true` **and today's `session_date`**;
4. `kill_switch.json` **not** enabled.

Broker order lifecycle:

| Phase | Action |
|---|---|
| PENDING_ENTRY | Place `SL-M` at `trigger_price`, product MIS, tagged `FV6<sha1[:14] of signal_id>` |
| Recovery | Before placing anything, look for an existing order with the same tag/symbol/side/type — restarts **adopt** working orders instead of duplicating them |
| Entry COMPLETE | Recompute stop/target from `average_price`, adopt `filled_quantity` |
| OPEN | Place protective `SL-M` stop and `LIMIT` target (both tagged) |
| Stop filled | Cancel target, close at stop's average price |
| Target filled | Cancel stop, close at target's average price |
| Stop/target rejected or cancelled | Cancel siblings, send `MARKET` square-off |
| Kill switch on, or 15:30 | Cancel siblings, send `MARKET` square-off (`SQUARE_OFF_PENDING`) |
| Disarmed / kill switch while PENDING | Cancel the entry order; if it filled meanwhile, adopt the fill |

Terminal states: `CLOSED`, `NO_FILL`, `ENTRY_REJECTED`, `BLOCKED_SIZING`,
`CANCELLED`.

### 5.6 `trade-logger` and `net-result`

Both poll every 5 s until 15:32.

- **trade-logger** merges PAPER + LIVE order states into a 32-column CSV at
  `consolidated/fno_v6_trades_<date>.csv` plus a markdown table.
- **net-result** reports, per mode: signals / pending / open / closed / no-fill /
  cancelled / blocked, capital deployed, realized, unrealized, total net Rs, and
  return on capital — net of the same 5 bps used in the backtest.

If a worker or reporter finds **no artefacts and a BLOCKED/FAILED upstream
role**, it publishes `UPSTREAM_BLOCKED` and exits — a broken feed surfaces as
one clear cause rather than six silent successes.

---

## 6. Backtest ↔ live parity: exactly where they differ

| Aspect | Backtest | Live | Impact |
|---|---|---|---|
| 5-minute equity bars | Built from 5×1m (`NSE_EQUITY_1M_CAUSAL_5X_AGGREGATION`) | Read from the live 5m store (quality-filtered) | Construction differs; both admit only completed real bars |
| Fill detection | First forward 1-minute bar trading through the trigger | PAPER: polled LTP crossing. LIVE: broker SL-M | PAPER can fill at a worse/better print; the backtest fills *at* the trigger |
| Bracket basis | Stop/target from the **trigger** | Stop/target from the **actual fill** | Live brackets shift with slippage |
| First fillable bar | Bar **after** the confirmation candle | Any tick after the worker sees the signal | Live can fill inside the confirmation minute's successor sooner |
| Stop/target same bar | **Stop wins** (pessimistic) | Whichever the broker/LTP hits first | Live may be luckier |
| Cost | 5 bps on gross return | 5 bps on entry notional | Equivalent to ~0.05% either way |
| Sizing | Not modelled (equal-weight % returns) | `floor(50,000 / price)`, lot-rounded in LIVE | Rupee P&L ≠ additive % sum when prices differ widely |
| Missing futures bar | Skip that contract, process the rest | Three clean `NO_CANDLE` checks → `SKIPPED_NO_CANDLE`; every other absence blocks | Matches contract-level backtest behaviour without inventing a bar |

**Practical reading:** the backtest is **optimistic on fills** (perfect trigger
fill) and **pessimistic on same-bar tie-breaks**. Compare live paper against it
on **order counts and hit rates first, P&L second**.

### 6.1 Completeness / deadline parity replay

Live archives append-only scanner, futures/cash marker, confirmation-feed,
bar-snapshot and confirmation-decision **evidence revisions**.
[fno_v6_parity_replay.py](fno_v6_parity_replay.py) uses the same exact
symbol/hash completeness contracts and the same 90-second activation gate:

- `--mode observed` selects the **earliest immutable evidence actually
  published** for the slot. Missing evidence is `INCOMPLETE_EVIDENCE`; strict
  mode fails instead of borrowing repaired data.
- `--mode counterfactual` may use the newest repaired revision, but labels that
  fact in every output and never reports it as observed-live parity.

Because those append-only revisions did not exist on 2026-08-17, that day's
repaired SAIL/AMBER/COCHINSHIP analysis can only be counterfactual — the
original 09:25 as-seen futures marker was overwritten and cannot honestly be
recreated.

---

## 7. Provenance and attestation

### 7.1 Frozen-result attestation

The original selected curve is retained unchanged as historical evidence, but
its source-byte inventory was never captured. A pinned dated-universe replay on
the current source files found **four additional filled orders** on 2026-06-03,
2026-06-19 and 2026-06-23. It was therefore **not** represented as the original
run and did **not** overwrite the legacy CSV.

| Metric | Legacy historical / unattested | Promoted current-source `20260818_V1` |
|---|---:|---:|
| Sessions | 53 | 53 |
| Orders / fills | 206 / 205 | 210 / 209 |
| Trade PF | 2.796 | 2.811 |
| Day PF | 5.968 | 6.062 |
| Net % | +144.003% | +146.711% |
| Window | 2026-05-27 … 2026-08-11 | 2026-05-27 … 2026-08-11 |

### 7.2 What live start-up verifies

`config.attest_selected_backtest()` verifies:

| Artefact | SHA-256 |
|---|---|
| `…selected_current_source_20260818_v1.csv` | `7ba3426c…85b7` |
| its immutable provenance JSON | `de394f5d…d296` |
| backtest input fingerprint | `199effd6…178d` |
| legacy `selected_20260811.csv` | `677470bb…a6b` |
| legacy mismatch audit JSON | `b147f63d…aa9e` |

Any missing or drifted artefact **stops the live runtime**.

Every V6 backtest run also writes an adjacent immutable provenance JSON
containing the dated-universe attestations, exact source inventory and digest,
cache artifact hashes, arguments/date window, output hashes, and one canonical
backtest-input fingerprint.

> The protected provenance is explicitly labelled a **recreation from
> then-current whole source files**. Those files can include rows after the
> replay cutoff. It does **not** claim to recover the unrecorded byte inventory
> of the original 11 August selection run.

### 7.3 How the V6 book was chosen — and the honest caveat

V6 is the `BEST_NET` portfolio produced by the V5 full-history optimizer
([fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5.py](fno_oi_ema_confirm_0925_0930_0935_0940_0945_v5.py) `--mode full-history`):

1. build per-slot per-side leg candidates over the grid — price ∈ {0.20…0.80},
   OI ∈ {0.10…1.00}, volume ∈ {1.0…3.0}, body ∈ {0.40, 0.50, 0.60},
   wick ∈ {0.30, 0.50}, traded value ∈ {0, 1e7}, 5 pickers,
   stop ∈ {0.30…1.00}, target ∈ {0.50…3.00};
2. score each with robustness guards — minimum fills/active days, day-win rate
   ≥ 0.45, top-day profit-share cap, best-whole-day-removed "robust" PF, and
   3/3 positive folds with worst-fold PF ≥ 0.80;
3. beam-search leg combinations into portfolios (a leg may be dropped entirely);
4. take the argmax for each objective — `BEST_TRADE_PF`, `BEST_DAY_PF`,
   `BEST_NET`.

> ⚠ **This is an in-sample fit.** Full-history mode deliberately fits on every
> session and keeps TRAIN/TEST as *diagnostic labels only* — the V6 report says
> so in its own header. The **+144% / PF 2.80** figures are a parameter-search
> **ceiling**, not out-of-sample evidence. Treat live paper as the real
> out-of-sample test.

---

## 8. Observed live/paper evidence

Consolidated from `v6_live/consolidated/fno_v6_trades_*.csv` (PAPER mode,
2026-08-11 … 2026-08-28, 12 session files):

| Metric | Value |
|---|---:|
| Order rows written | 13 |
| Mode | 100% PAPER |
| CLOSED / OPEN at capture | 10 / 3 |
| Wins / losses (closed) | 4 / 6 |
| Net P&L (closed) | **+Rs 1,083.25** |
| Exit mix | STOP 5 · SQUARE_OFF 3 · TARGET 2 |

Per-session closed P&L:

| Session | Closed | Net Rs |
|---|---:|---:|
| 2026-08-18 | 2 | +757.04 |
| 2026-08-19 | 2 | +769.84 |
| 2026-08-20 | 3 | −855.97 |
| 2026-08-21 | 1 | −412.50 |
| 2026-08-27 | 2 | +824.84 |

Per-leg closed P&L:

| Slot | Side | Closed | Net Rs |
|---|---|---:|---:|
| 09:25 | LONG | 1 | −293.3 |
| 09:25 | SHORT | 5 | −484.1 |
| 09:30 | LONG | 1 | −545.4 |
| 09:35 | LONG | 2 | +1,204.8 |
| 09:40 | LONG | 1 | +1,201.2 |

> **Read this honestly.** Ten closed trades over five active sessions is far
> below any statistical threshold. The sample is dominated by two winning
> 09:35 / 09:40 LONG trades; remove them and the paper book is negative. It is
> evidence that the pipeline *runs*, not evidence that the edge *survives*.
> Several scheduled sessions produced no orders at all — expected, given the
> 12-order/day ceiling and the fail-closed feed gates.

---

## 9. Running it

```powershell
# --- Backtest: replay the frozen V6 book on the full history ---
python fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py
python fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py --through-day 2026-08-11
python fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py --rebuild-cache   # after a data-contract change

# --- Live/paper roles (normally scheduled; PAPER is the default) ---
bat\run_fno_v6_scanner_5min.bat
bat\run_fno_v6_equity_1min_feed.bat
bat\run_fno_v6_confirmation_1min.bat
bat\run_fno_v6_live_long.bat
bat\run_fno_v6_live_short.bat
bat\run_fno_v6_trade_logger.bat
bat\run_fno_v6_net_result.bat

# --- Single-slot live diagnostics (the activation deadline is still enforced) ---
python fno_v6_live.py --role scanner-5m      --slot 0925 --session-date 2026-08-13
python fno_v6_live.py --role confirmation-1m --slot 0926 --session-date 2026-08-13

# --- Evidence-only gate replay (does not place orders) ---
python fno_v6_parity_replay.py --session-date 2026-08-18 --mode observed --slot all --strict
python fno_v6_parity_replay.py --session-date 2026-08-17 --mode counterfactual --slot all

# --- Install / refresh the weekday schedule (disables the V5 tasks) ---
powershell -ExecutionPolicy Bypass -File bat\schedule_fno_oi_weekday.ps1
```

**Backtest outputs** (`<FNO_ROOT>/strategy_research/`):
`…v6_best_net_daily.csv`, `…v6_best_net_trades.csv`, `…v6_best_net_setups.csv`,
versioned selected CSV/provenance artifacts, an immutable per-run provenance
under `backtest_provenance/`, and the report at
`<FNO_ROOT>/latest/latest_fno_oi_ema_confirm_v6_best_net.md`.

**Useful arguments** — backtest: `--split-day` (TRAIN/TEST labels), `--cost-bps`
(default 5.0), `--square-off` (default `1530`), `--max-forward-bars`
(default 400), `--promote-current-source-v1` (exact fail-closed, no-replace
versioned publication). Live: `--once`, `--poll-sec`,
`--confirmation-max-wait-sec`, `--max-apps`, `--ignore-fetch-marker`
(**diagnostics only** — it bypasses the feed-readiness gate).

---

## 10. Safety rails, in one list

1. **Fingerprint chaining** — scanner → confirmation → signal → order state all carry `strategy_version` + `strategy_fingerprint`; mismatch aborts.
2. **Backtest attestation** — dated universe, source inventory, cache/output hashes, protected provenance, frozen CSV SHA-256, and metric equality.
3. **Feed-readiness gate** — exact-slot markers, exact mapped-stock hashes, ≥ 99% coverage, at most two named omissions, three clean checks per omission.
4. **Durable confirmation evidence** — exact scanner/candidate hashes, immutable completed bars, and no broker polling in the consumer.
5. **Verified ineligibility only** — a repeatedly verified no-trade candidate may be skipped; unverified, invalid, API-failed or unexpected gaps block.
6. **90-second activation deadline** — no retroactive entries after a late start or manual `--slot`.
7. **09:50 pipeline deadline** — incomplete pipelines block instead of trading.
8. **Immutable parity evidence** — observed replay never substitutes later repair data; counterfactual mode is labelled.
9. **PAPER by default** — LIVE needs mode + exact ack env var + same-day arm file + no kill switch.
10. **Kill switch** — flips open LIVE positions to market square-off on the next poll.
11. **Tag-based order recovery** — restarts adopt working broker orders instead of duplicating them.
12. **Locked sizing** — Rs 10,000 × 5x only; anything else raises.

---

## 11. Related research modules (not in the live path)

| Module | Purpose |
|---|---|
| [fno_v5_hybrid_optimize.py](fno_v5_hybrid_optimize.py) | Leg grid search + guards + beam portfolio search (the engine behind V6 selection) |
| [fno_v5_hybrid_backtest.py](fno_v5_hybrid_backtest.py) | Shared replay/curve/stat helpers used by V6 |
| [fno_v5_0926_day_pf_optimize.py](fno_v5_0926_day_pf_optimize.py) | Train-only day-PF search restricted to the 09:25→09:26 leg |
| [fno_v5_0926_all_history_day_pf_optimize.py](fno_v5_0926_all_history_day_pf_optimize.py) | All-history ceiling for the same leg — explicitly *not* out-of-sample |
| `latest_fno_v6_0926_only.md` | Slice of the V6 trade file showing 09:26 entries only (68 fills, PF 2.849, +44.0%) |
| [fno_oi_ema_confirm_sweep.py](fno_oi_ema_confirm_sweep.py) | Signal-table builder + `simulate_bracket` (shared by everything) |
| [fno_oi_eod_qc.py](fno_oi_eod_qc.py) | End-of-day data quality check |

V5 remains importable ([fno_v5_live_config.py](fno_v5_live_config.py), 3 legs,
`FORCE_DAILY` on the 09:26 LONG) and its scheduled tasks are disabled. The two
generations share the runtime and write to separate roots (`v5_live/` vs
`v6_live/`), so they can run side by side if re-enabled.

---

## 12. Known limitations — read before trusting a number

1. **In-sample selection.** The V6 book is the argmax of a large grid + beam
   search over the *whole* history. TRAIN/TEST labels are diagnostic only.
2. **Static OI series.** The historical OI cache uses 26AUG futures OI across
   the entire period — not a rolling near-month series.
3. **No point-in-time universe.** The promoted replay pins a dated universe
   (2026-08-11); it does not reconstruct daily F&O membership.
4. **Fill optimism.** Backtest fills exactly at the trigger; a live SL-M does
   not.
5. **Thin live sample.** 10 closed paper trades to date. Nothing about live
   edge is established.
6. **Cost assumption.** 5 bps round trip is the V6 lineage assumption; the
   V8/V10 research line uses a more conservative **15 bps**, which materially
   reduces the same trades' net.

---

# Appendix A — Complete indicator and parameter reference

Everything below is transcribed from source. Nothing is paraphrased or rounded.

## A.1 Indicator definitions

All indicators are computed on the **NSE cash-equity 5-minute bar series** in
`hybrid.add_equity_five_minute_features()`
([fno_oi_hybrid_data.py](fno_oi_hybrid_data.py#L398-L407)) — identical code to
the V8 engine's `add_five_minute_features()`.

```python
out = frame.sort_values("ts").reset_index(drop=True)

for span in (9, 20, 50):
    out[f"ema{span}"] = out["close"].ewm(span=span, adjust=False).mean()

out["prev_close"]        = out["close"].shift(1)
out["price_change_pct"]  = (out["close"] / out["prev_close"] - 1.0) * 100.0

prior_volume             = out["volume"].shift(1).rolling(20, min_periods=5).mean()
out["volume_ratio"]      = out["volume"].div(prior_volume.where(prior_volume.gt(0)))

out["traded_value"]      = out["close"] * out["volume"]
```

| Indicator | Exact definition | Units |
|---|---|---|
| `ema9` / `ema20` / `ema50` | `close.ewm(span=n, adjust=False).mean()` — recursive EMA, `α = 2/(n+1)` → 0.2000 / 0.0952381 / 0.0392157 | price |
| `prev_close` | `close.shift(1)` — the **previous 5-minute bar's** close | price |
| `price_change_pct` | `(close / prev_close − 1) × 100` | % |
| `prior_volume` | `volume.shift(1).rolling(20, min_periods=5).mean()` — mean of the **20 preceding** bars' volume, needing at least 5 | shares |
| `volume_ratio` | `volume / prior_volume`, with `prior_volume` masked to `> 0` (else NaN) | ratio |
| `traded_value` | `close × volume` | Rs |
| `oi` / `prev_oi` | future's OI at the bar, and `oi.shift(1)` on the **futures** series | contracts |
| `oi_change_pct` | `(oi / prev_oi − 1) × 100`, only where `oi > 0 AND prev_oi > 0 AND both finite`; otherwise **NaN** | % |

### A.1.1 EMA properties that are easy to get wrong

- **`adjust=False`** means a pure recursive EMA seeded at the first bar
  (`ema[0] = close[0]`), not the bias-corrected form.
- **There is no `min_periods` and no warm-up guard.** `ema50` is a defined
  number from the very first bar of a symbol's history, where it is simply the
  close. Only the first few dozen bars of a symbol's entire loaded history are
  affected, so in practice this never touches a traded slot.
- **EMAs are continuous across sessions, not reset daily.** The frame is the
  symbol's whole loaded history sorted by `ts`, so the 09:25 EMA stack of any
  session carries state from the previous session's close.

### A.1.2 `volume_ratio` is mostly a *previous-session* comparison

`prior_volume` is 20 five-minute bars ≈ 100 minutes of trading. At the 09:25
slot only one bar of the current session (09:20) exists, so the other 19 bars
are the **previous session's last ~95 minutes**. The denominator at every V6
scan slot is therefore dominated by yesterday afternoon, not by this morning.

This is a genuine property of the signal, not a bug: a `volume_ratio ≥ 3.0`
at 09:25 means "this opening bar traded three times yesterday's late-session
per-bar average", which is what the setup intends. It is worth knowing when
interpreting the thresholds.

### A.1.3 The `oi_change_pct` sign convention

`prev_oi` is taken from the **futures** frame's own `oi.shift(1)`, i.e. the
prior 5-minute futures bar, and is then joined to the equity bar on the exact
bar-end timestamp. `oi_change_pct` is therefore a *5-minute* OI delta, not a
day-over-day one. The base gate requires **both** `oi > prev_oi` (raw
inequality) **and** `oi_change_pct >= 0.05` — a redundant-looking pair that
guards against float-noise sign flips near zero.

## A.2 Bar-admission predicates

### A.2.1 5-minute equity bar (`completed_real_equity_five_minute_bars`)

A bar is admitted only if **all** hold:

| Test | Rule |
|---|---|
| Not the opening snapshot | timestamp ≠ 09:15 and `opening_snapshot` is false |
| Not synthesized | `gap_filled` is false |
| Not stale | `provisional_stale` is false |
| Real 5×1m construction | when `source_1m_count` exists, it must equal **exactly 5** |
| Not a duplicate | not an exact OHLCV copy of the adjacent prior bar, unless both are proven 5×1m |

### A.2.2 Backtest 5-minute construction (`NSE_EQUITY_1M_CAUSAL_5X_AGGREGATION`)

```python
offset      = (ts − session_open_0915).total_seconds() / 60      # must be 1..375
slot_number = ((offset − 1) // 5) + 1
slot_end    = session_open_0915 + slot_number × 5 minutes
```

Aggregation is `open=first, high=max, low=min, close=last, volume=sum`, and a
group is kept only when **all three** hold:

- `source_1m_count == 5`
- `source_1m_first == slot_end − 4 min`
- `source_1m_last  == slot_end`

So the first constructible bar of a session ends at **09:20** (covering
09:16–09:20). That is why 09:20 can never be a signal bar: it is the first bar,
and its `prev_close` belongs to the previous session.

### A.2.3 1-minute bar validity

Timestamps must be exact minute-end labels (`second == 0`, `microsecond == 0`,
`nanosecond == 0`), offset 1..375 from 09:15, and satisfy:

```
all of open, high, low, close finite and > 0
high >= max(open, close)
low  <= min(open, close)
high >= low
volume finite and >= 0
not gap_filled, not opening_snapshot, not provisional_stale
```

## A.3 The complete gate chain, in evaluation order

### Gate 1 — base candidate gate (`fno_v5_live._base_signal_side`)

```python
required = ("ema9","ema20","ema50","price_change_pct",
            "oi_change_pct","volume_ratio","oi","prev_oi")
if any NaN in required:                       return None
if oi <= prev_oi:                             return None
if oi_change_pct  < BASE_OI_CHANGE_PCT:       return None      # 0.05
if volume_ratio   < BASE_VOLUME_RATIO:        return None      # 0.80
if ema9 > ema20 > ema50 and price_change_pct >= +BASE_PRICE_CHANGE_PCT:  return "LONG"
if ema9 < ema20 < ema50 and price_change_pct <= −BASE_PRICE_CHANGE_PCT:  return "SHORT"
return None
```

| Constant | Value |
|---|---:|
| `BASE_PRICE_CHANGE_PCT` | 0.10 |
| `BASE_OI_CHANGE_PCT` | 0.05 |
| `BASE_VOLUME_RATIO` | 0.80 |

Two further hard rejections happen after the gate, before a candidate is
emitted: `equity_instrument_token <= 0` and `signal_close <= 0` both increment
the snapshot's `invalid` counter and drop the row.

### Gate 2 — confirmation morphology (1-minute)

Computed on the candle ending at `confirmation_end`:

```
range      = high − low                            (> 0 required)
body_ratio = |close − open| / range
wick_ratio = LONG  : (high − max(open, close)) / range
             SHORT : (min(open, close) − low)  / range
trigger    = LONG  : high
             SHORT : low

LONG  direction OK  <=>  close > open  AND  close > signal-bar close
SHORT direction OK  <=>  close < open  AND  close < signal-bar close
```

Direction failure ⇒ `direction_rejected`, and the candidate never reaches gate 3.

### Gate 3 — per-leg setup filters (`config.passes_selected_filters`)

```python
side must equal setup.side
price_ok = price_change_pct >= setup.price_change_pct        if LONG
           price_change_pct <= −setup.price_change_pct       if SHORT
return (price_ok
    and oi_change_pct >= setup.oi_change_pct
    and volume_ratio  >= setup.volume_ratio
    and body_ratio    >= setup.body_ratio
    and wick_ratio    <= setup.max_wick_ratio
    and traded_value  >= setup.min_traded_value)
```

All six comparisons are **inclusive** (`>=` / `<=`) with no epsilon. Any NaN
propagates to `False`.

### Gate 4 — ranking and cap (`config.rank_candidates`)

```python
sorted(eligible, key=lambda c: (−picker_value(c, setup.picker),
                                −float(c["traded_value"]),
                                str(c["tradingsymbol"])))[: setup.max_entries]
```

| `picker` | `picker_value` returns |
|---|---|
| `max_oi` | `oi_change_pct` |
| `max_volume` | `volume_ratio` |
| `max_move` | `abs(price_change_pct)` |
| `max_body` | `body_ratio` |
| `max_liquidity` | `traded_value` |

An unknown picker raises `ValueError`. No V6 leg uses `max_oi` or `max_body`.

## A.4 `SetupSpec` — the leg dataclass

Fifteen fields, all frozen at import:

| Field | Type | Meaning |
|---|---|---|
| `signal_end` | str | 5-minute signal bar end, `HH:MM` |
| `confirmation_end` | str | 1-minute confirmation bar end, `HH:MM` |
| `side` | str | `LONG` / `SHORT` |
| `mode` | str | `FILTERED` for all ten legs |
| `max_entries` | int | per-slot cap (1 LONG / 1–2 SHORT) |
| `picker` | str | ranking key |
| `price_change_pct` | float | magnitude threshold; sign applied by side |
| `oi_change_pct` | float | minimum 5-minute OI change % |
| `volume_ratio` | float | minimum volume ratio |
| `body_ratio` | float | minimum confirmation body ratio |
| `max_wick_ratio` | float | maximum adverse wick ratio |
| `min_traded_value` | float | minimum Rs traded value — **0 for all ten legs** |
| `stop_pct` | float | stop distance % from entry |
| `target_pct` | float | target distance % from entry |
| `source_version` | str | `FNO_V6_BEST_NET_CASH_EQUITY_20260811` |

### The ten legs, with derived reward:risk

| # | Signal | Confirm | Side | Mode | Max | Picker | Price % | OI % | Vol | Body ≥ | Wick ≤ | Min TV | Stop % | Target % | R:R |
|---:|---|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 09:25 | 09:26 | LONG | FILTERED | 1 | max_liquidity | 0.30 | 0.10 | 3.0 | 0.6 | 0.5 | 0 | 0.50 | 3.00 | 1:6.0 |
| 2 | 09:25 | 09:26 | SHORT | FILTERED | 2 | max_volume | 0.20 | 0.10 | 1.5 | 0.4 | 0.5 | 0 | 0.75 | 3.00 | 1:4.0 |
| 3 | 09:30 | 09:31 | LONG | FILTERED | 1 | max_move | 0.65 | 0.10 | 1.0 | 0.5 | 0.5 | 0 | 1.00 | 2.50 | 1:2.5 |
| 4 | 09:30 | 09:31 | SHORT | FILTERED | 1 | max_move | 0.20 | 0.25 | 1.0 | 0.4 | 0.5 | 0 | 1.00 | 3.00 | 1:3.0 |
| 5 | 09:35 | 09:36 | LONG | FILTERED | 1 | max_liquidity | 0.20 | 0.10 | 1.0 | 0.6 | 0.5 | 0 | 1.00 | 2.50 | 1:2.5 |
| 6 | 09:35 | 09:36 | SHORT | FILTERED | 2 | max_liquidity | 0.50 | 1.00 | 1.0 | 0.4 | 0.5 | 0 | 1.00 | 3.00 | 1:3.0 |
| 7 | 09:40 | 09:41 | LONG | FILTERED | 1 | max_liquidity | 0.20 | 0.10 | 2.0 | 0.5 | 0.5 | 0 | 0.50 | 2.50 | 1:5.0 |
| 8 | 09:40 | 09:41 | SHORT | FILTERED | 1 | max_move | 0.20 | 0.10 | 1.0 | 0.4 | 0.5 | 0 | 1.00 | 3.00 | 1:3.0 |
| 9 | 09:45 | 09:46 | LONG | FILTERED | 1 | max_move | 0.65 | 0.10 | 1.0 | 0.4 | 0.5 | 0 | 1.00 | 3.00 | 1:3.0 |
| 10 | 09:45 | 09:46 | SHORT | FILTERED | 1 | max_volume | 0.20 | 0.75 | 1.0 | 0.4 | 0.3 | 0 | 1.00 | 2.00 | 1:2.0 |

Observations worth carrying: the strictest OI filter is leg 6 (`1.00%`), the
strictest volume filter is leg 1 (`3.0×`), the tightest wick tolerance is leg 10
(`0.30`), and the two `0.50%` stops (legs 1 and 7) produce the two highest
reward:risk ratios in the book.

## A.5 Bracket arithmetic and tick rounding

`config.bracket_levels()` — note this is **nearest-tick** rounding, not the
directional Decimal rounding the V8/V10 engine uses:

```python
def round_to_tick(value, tick_size):
    tick = float(tick_size)
    if not isfinite(tick) or tick <= 0:
        tick = 0.05                      # silent fallback
    return round(round(value / tick) * tick, 8)

stop   = entry × (1 − stop_pct/100)   if LONG else entry × (1 + stop_pct/100)
target = entry × (1 + target_pct/100) if LONG else entry × (1 − target_pct/100)
return round_to_tick(stop, tick), round_to_tick(target, tick)
```

| Aspect | V6 live | V8 / V10 engine |
|---|---|---|
| Rounding mode | `round()` — **nearest** tick | `ROUND_CEILING` / `ROUND_FLOOR` — **directional** |
| Arithmetic | binary float | `Decimal` |
| Invalid tick | silently falls back to 0.05 | raises `ValueError` |
| Bias | symmetric | always conservative (stops nearer, targets further) |

Nearest-tick rounding can place a live stop **half a tick further away** than
the engine would, and a target **half a tick nearer**. On a Rs 500 stock at
0.05 tick that is 0.005% per side — immaterial against 5 bps costs, but it is a
genuine, deliberate-to-note asymmetry between the two code paths.

`tick_size` comes from the universe row's `equity_tick_size`, defaulting to
**0.05** when absent or unparseable.

## A.6 Position sizing (`config.size_position`)

```python
exposure    = capital_rs × leverage              # 10,000 × 5.0 = 50,000
theoretical = floor(exposure / entry_price)      # 0 when entry_price <= 0

PAPER: quantity = theoretical
       state    = "PAPER_EXPOSURE_SIZED"  or "BLOCKED_PRICE_EXCEEDS_BUDGET"
LIVE : quantity = (theoretical // lot_size) × lot_size
       state    = "LIVE_LOT_SIZED"        or "BLOCKED_LOT_EXCEEDS_BUDGET"
```

`PositionSize` carries `capital_rs`, `leverage`, `target_exposure_rs`,
`theoretical_units`, `quantity`, `lot_size`, `estimated_exposure_rs`
(`quantity × entry`) and `state`.

| Constant | Value |
|---|---:|
| `CAPITAL_PER_ENTRY_RS` | 10,000.0 |
| `LEVERAGE` | 5.0 |
| `TARGET_EXPOSURE_RS` | 50,000.0 |
| `lot_size` used by the scanner | **1** (cash equity) |

Because the scanner emits `lot_size = 1`, PAPER and LIVE quantities coincide
today; the LIVE lot-flooring branch exists for a future F&O-instrument path.

**Sizing consequence:** at Rs 50,000 fixed exposure, a 1.00% stop risks
**Rs 500** and a 3.00% target earns **Rs 1,500**, before costs, regardless of
the stock's price. The observed live paper trades sit exactly on this scale
(largest win +Rs 1,204.8 across two fills; largest single loss −Rs 545.4).

## A.7 Cost model

| Path | Formula |
|---|---|
| Backtest (`simulate_bracket`) | `net_return_pct = (gross_return − 5bps) × 100` — charged on the **gross return** |
| Live (`_close_state`) | `cost_rs = entry_notional × 5 bps`, then `net = gross_rs − cost_rs` |

Both are 0.05% round trip. The live path additionally records
`net_return_exposure_pct` (net Rs ÷ Rs 50,000 exposure) and
`return_on_capital_pct` (net Rs ÷ Rs 10,000 capital) — so a 1% move on exposure
reads as 5% on capital.

## A.8 Runtime constants

| Constant | Value | Meaning |
|---|---:|---|
| `SIGNAL_TO_CONFIRMATION` | 5 pairs | 09:25→09:26 … 09:45→09:46 |
| `SQUARE_OFF` | `15:30` | forced flat |
| `ROUND_TRIP_COST_BPS` | 5.0 | both paths |
| `ENTRY_ACTIVATION_GRACE_SEC` | 90 | first-time-entry deadline after `confirmation_end` |
| `PIPELINE_DEADLINE` | 09:50 | scanner publishes BLOCKED past this |
| `CONFIRMATION_COMPLETED_BOUNDARY_BUFFER_SEC` | 3.0 | fingerprinted; CLI cannot weaken it |
| `CONFIRMATION_NO_CANDLE_OBSERVATIONS` | 3 | clean empty responses before a candle is "verified absent" |
| `CONFIRMATION_NO_CANDLE_MIN_AGE_SEC` | 15 | minimum publication age before verification counts |
| `CONFIRMATION_NO_CANDLE_OBSERVATION_SPACING_SEC` | 2.0 | minimum gap between observations |
| `MIN_STOCK_FUTURES_COVERAGE` | 0.99 | recomputed from exact symbol lists |
| `MAX_VERIFIED_NO_CANDLE_STOCKS` | 2 | named omissions allowed per slot |
| `MIN_NO_CANDLE_FETCH_ATTEMPTS` | 3 | per omitted futures contract |
| `ORDER_TAG_PREFIX` | `FV6` | + `sha1(signal_id)[:14]` |
| `LIVE_ACK_ENV` / `LIVE_ACK` | `FNO_V6_LIVE_ACK` / `I_UNDERSTAND_REAL_FNO_V6_EQUITY_ORDERS` | LIVE unlock |
| Entry-worker poll | 1 s | LTPs batched in one `ltp()` call |
| Logger / net-result poll | 5 s, until 15:32 | |
| `--confirmation-max-wait-sec` | 90 (default) | staleness deadline |

## A.9 Daily capacity arithmetic

```
LONG  caps: 1 + 1 + 1 + 1 + 1 = 5
SHORT caps: 2 + 1 + 2 + 1 + 1 = 7
                        total = 12 orders per session, hard ceiling
Maximum deployed exposure = 12 × Rs 50,000 = Rs 600,000
Maximum deployed capital  = 12 × Rs 10,000 = Rs 120,000
```

`validate_strategy()` asserts the objective is `BEST_NET`, the five signal
windows are exactly `09:25/09:30/09:35/09:40/09:45`, there are exactly ten legs,
and the backtest-source identity payload matches its expected dict — before any
role runs.

Note that V6 has **no global concurrency or margin ledger**: each leg's cap is
enforced independently, and all twelve can be open simultaneously. The V8/V10
research line adds exactly that ledger (Rs 120,000 capital, 12 concurrent slots,
one position per symbol), which is why its fill counts differ from a naive
V6 replay even on identical signals.
