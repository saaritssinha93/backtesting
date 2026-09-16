# Prompt — Build `fno_options_atm_fetch_5min.py` (ATM CE/PE 5-minute fetcher, live + historical)

---

## OBJECTIVE

Create a new production-grade Python module at the repo root:

```
fno_options_atm_fetch_5min.py
```

It is the **options counterpart** to the existing futures producer
`fno_oi_fetch_5min_fast_production.py` ("FnO Live 5-Minute Futures OI Fetch (Fast Production)").

Where the futures producer fetches **near-month NFO-FUT** 5-minute OHLCV+OI for the whole
F&O universe, this new module must fetch **NFO-OPT CE and PE contracts at (and around) the
ATM strike**, where "ATM" is resolved **from the cash-equity spot price already stored or
fetched in this repo** — not from a live quote call, and never from the option chain itself.

It must run in **two modes from the same file**:
1. **LIVE SESSION MODE** — runs continuously through the trading day, fetching each completed
   5-minute slot as it closes (same slot cadence, boundary buffer, and retry discipline as the
   futures producer).
2. **HISTORICAL MODE** — backfills a date range / list of sessions for contracts that are still
   resolvable, with explicit fail-closed handling of anything that is not.

Both modes must share one code path for contract resolution, fetching, validation, and persistence.
Do not build two parallel implementations.

---

## STEP 0 — READ BEFORE WRITING ANY CODE (mandatory)

Read these files completely and mirror their conventions. Do not invent new patterns where an
existing one applies.

| File | What to take from it |
|---|---|
| `fno_oi_fetch_5min_fast_production.py` | Overall entrypoint shape: `SESSION`, `ENGINE_VERSION`, `CanonicalArchiveCache` warm-cache preload, `_stream_fetch_and_persist` bounded network→writer pipeline, `_verify_archived_row` read-back verification, `run_session()` live slot loop, `main()` status/crash publishing |
| `fno_oi_fetch_5min_fast_shadow.py` | `AppLane`, `AppLaneSession`, `build_app_lanes()`, `_historical_call()`, `_auth_failure_text()`, `_choose_retry_lane()`, per-app pacing and 429 backoff |
| `fno_oi_fetch_5min.py` (legacy) | `build_parser()` argument surface, `run_slot()`, `latest_completed_slot()`, `_coerce_slot()`, `ensure_universe()`, `FIRST_SLOT`/`LAST_SLOT`, slot marker semantics |
| `fno_oi_common.py` | Credentials, paths, atomic writes, merge/append helpers, heartbeat/status publishing, holiday + trading-day checks, hashing helpers, `RAW_COLUMNS` schema |
| **`fno_v13_v5_derivative_data.py`** | **Reuse, do not duplicate:** `normalize_nfo_master()`, the ATM selection rule in `build_option_contract_map()`, the strike-ladder logic in `build_option_fetch_plan()`, `normalize_minute_candles()`, `_merge_candles()`, `_paced_history()`, `MASTER_COLUMNS`, and the `mapping_status` vocabulary |
| `fno_equity_fetch_1min.py` | How cash-equity candles are produced, stored and marker-gated (this is your spot-price source of truth) |
| `log_dashboard_server.py` (~line 212, ~line 8944, ~line 9212) | Session label registry and schedule card wiring |
| `FNO_PAPERTRADE_DASHBOARD_OPERATIONS.md` (~line 130, ~line 151) | Session→label→report-file table that must be extended |
| `bat/run_fno_oi_fetch_5min_fast_production.bat` | Supervisor batch pattern: `SCRIPT_NAME`, `LOG_FILE`, `STATUS_FILE`, `HEARTBEAT_FILE`, `FRESHNESS_FILE` |

After reading, **state back in one paragraph** what the futures producer does end-to-end,
and what you will change for options. Do not start coding until that summary is written.

---

## KITE APPS 1–8 — MANDATORY USAGE SPEC

Credentials are discovered by `fno_oi_common.discover_kite_credentials(max_apps=8)`, which reads
`api_key.txt`/`access_token.txt` (app1) and `api_key2..8.txt`/`access_token2..8.txt` (app2..app8)
from the script directory, returning `KiteCredential(app_name="app1".."app8", ...)`.

Requirements:

1. **Use all 8 apps as parallel lanes.** Build lanes with the same
   `build_app_lanes(args, credentials=...)` path used by the fast shadow — one `AppLane` per app,
   each holding `workers_per_app` independent `KiteConnect` clients.
2. **Respect per-app pacing.** `AppLane.pace()` serialises request *starts* per app at
   `pace_seconds = max(0.34, args.request_interval_sec)`. Never bypass `lane.pace()`. Parallelism
   comes from having 8 lanes, not from beating one app's rate limit.
3. **Reuse authenticated clients across slots** via `AppLaneSession.acquire(args)` — do not
   re-authenticate every slot. Call `invalidate_runtime_auth_failures()` after each slot.
4. **Fail over on auth errors, not on rate limits.** Copy `_auth_failure_text()` detection; on a
   definite token failure set `lane._runtime_auth_failure` and let the outer retry pick a
   different app via `_choose_retry_lane(runtimes, already_attempted)`.
5. **Retry discipline:** 429 / "too many requests" / "rate limit" → `max(2.0, 2**attempt)` backoff;
   other transient errors → `min(8.0, 0.75 * 2**(attempt-1))`.
6. **Cap concurrency.** Default `--workers-per-app 2` and `--writer-workers 8`. Hard-cap total
   writer threads at 8 (project standing rule: never starve the live V7 feed / Spyder on this
   machine). Reject `--writer-workers` above 8 with a clear error unless `--allow-high-workers`
   is explicitly passed.
7. **Log an auth banner per app** exactly like `[SHADOW][AUTH] app3 validated for <user> workers=2`,
   using a new tag e.g. `[OPT-ATM][AUTH]`.
8. **Never print, log, or write api keys or access tokens** into any status file, marker, report,
   log line, or parquet column.

Options universes are far larger than the futures universe. Before fetching, **compute and log the
projected request count and the projected wall-clock time** given 8 lanes × pace, and **refuse to
start** (clear error, non-zero exit) if the projected slot duration exceeds the 5-minute boundary
budget. Suggest the user narrow `--strike-window` or `--underlyings` instead of silently truncating.

---

## ATM RESOLUTION — SPOT COMES FROM CASH EQUITY, NOT FROM OPTIONS

This is the single most important correctness rule in the module.

1. **Spot source, in priority order** (make it explicit via `--spot-source`, default `AUTO`):
   - the completed cash-equity 5-minute candle close for the same slot, from the repo's stored
     equity data (`common.EQUITY_1M_RAW_DIR` aggregated, or the configured 5-minute equity dirs);
   - if unavailable for that symbol/slot → **the contract is skipped with an explicit state**,
     never with a guessed or stale price.
2. **Never call `kite.ltp()`/`quote()` to resolve ATM.** The whole point is that the option strike
   is anchored to the *same* cash price series the strategies already trade on, so backtests and
   live runs agree.
3. **Strike selection rule — copy exactly from `build_option_contract_map()`:**
   ```
   atm_distance = abs(to_numeric(strike) - spot_price)
   selected     = candidates.sort_values(["atm_distance", "strike", "tradingsymbol"],
                                         kind="stable").iloc[0]
   ```
   Deterministic tie-break on lower strike, then tradingsymbol. Same rule for CE and PE.
4. **Fetch both legs.** For each resolved underlying+expiry, fetch the ATM **CE and PE**, plus a
   symmetric ladder of `--strike-window N` strikes on each side (default `N=0`, i.e. ATM only;
   `N=2` gives ATM±2 → 5 strikes × 2 types = 10 contracts per underlying).
5. **Expiry selection** must be explicit and configurable: `--expiry-policy` with at least
   `NEAREST_UNEXPIRED_MONTHLY` (default, matching the futures near-month roll discipline used by
   `fno_v6_corrected_backtest.py`) and `NEAREST_UNEXPIRED_WEEKLY`. Record the chosen policy in
   every output row and in the marker. **Never** substitute a different expiry when the intended
   one is missing.
6. **Record the resolution inputs on every row**: `spot_price`, `spot_source`, `spot_slot`,
   `atm_distance`, `strike_offset` (…-2,-1,0,+1,+2 with 0 = ATM), `expiry_policy`, `underlying`.
   A downstream reader must be able to re-derive the strike choice from the row alone.

---

## THE EXPIRED-CONTRACT TRAP — FAIL CLOSED (non-negotiable)

This repo has already been burned by exactly this class of bug: OI data from one contract month was
backfilled and silently attributed to earlier months, contaminating months of backtests.

Kite's `historical_data` **cannot serve contracts that have already expired and dropped out of the
instrument master.** Therefore:

- **Never substitute a live/near expiry for a requested historical expiry.** Reuse the
  `mapping_status` vocabulary from `fno_v13_v5_derivative_data.py`:
  `MAPPED_ATM`, `EXPIRED_OPTION_NOT_IN_CURRENT_MASTER`, `NO_MATCHING_LIVE_OPTION_CONTRACT`,
  and add `NO_SPOT_PRICE_FOR_SLOT`.
- A session/slot/underlying that cannot be resolved is **recorded as unresolved and dropped from
  the data**, not filled with a proxy.
- Historical mode must **archive the instrument master it used** (date-stamped, hashed) so any
  later reader can prove which contract set was live on that day. Follow the
  `common.MASTER_DIR` + `symbol_set_sha256` / `canonical_json_sha256` pattern.
- Every persisted row carries the `master_date` and the master's SHA-256 that resolved it.
- On a re-run for the same slot, the module must be **idempotent**: re-resolving must produce the
  identical contract set, or abort with a drift error naming the differing symbols.

---

## STORAGE, SCHEMA, MARKERS

Follow the existing conventions in `fno_oi_common.py` — do not invent a new storage root.

- New roots under `common.FNO_ROOT` (i.e. `C:\TradingData\eqidv2\fno_oi\`):
  - `raw_options_5m/` — one parquet per option `tradingsymbol` (mirror `raw_contract_path()`)
  - `options_slot_ready/` — per-slot immutable markers
  - `options_atm_map/` — per-session resolved ATM contract map (audit trail)
- **Schema:** start from `common.RAW_COLUMNS` and extend with the option-specific fields:
  `instrument_type` (CE/PE), `strike`, `strike_offset`, `spot_price`, `spot_source`, `spot_slot`,
  `atm_distance`, `expiry_policy`, `mapping_status`, `master_date`, `master_sha256`.
  Define `OPTIONS_RAW_COLUMNS` as an explicit tuple and a `OPTIONS_RAW_DATA_VERSION` constant
  (e.g. `"fno_options_raw_v1"`). Never write a frame with columns outside that tuple.
- **Fetch call:** `client.historical_data(token, from_dt, to_dt, "5minute", continuous=False, oi=True)`
  — options carry OI too, and it is strategically meaningful; capture it.
- **Atomic persistence only:** `common.atomic_write_parquet` / `merge_contract_rows` /
  `append_contract_rows`. Never partial-write.
- **Read-back verification:** port `_verify_archived_row()` — after every append, re-read and assert
  the exact-slot row matches field-for-field before advancing the in-memory cache. Advance memory
  only after the atomic replace succeeds.
- **Slot markers** must mirror the v2 marker semantics (`FNO_FETCH_SLOT_SCHEMA_VERSION` style):
  a new `OPTIONS_SLOT_SCHEMA_VERSION`, with `complete` boolean, expected vs written counts,
  per-state tallies, the resolving master SHA, and the coverage ratio. Downstream consumers must be
  able to gate on the marker alone and never re-call the broker.
- **Heartbeat/status:** `common.publish_status(SESSION, ...)` and `common.publish_heartbeat(SESSION, ...)`
  at every phase — `START`, `PRELOAD_ARCHIVE_CACHE`, `RESOLVE_ATM`, `FETCH_SLOT`, `FETCH_SLOT_RETRY_n`,
  `WAIT_NEXT_SLOT`, `END_TIME`, `FAILED`. Use `SESSION = "fno_options_atm_fetch_5min"`.

---

## CLI SURFACE

Extend `legacy.build_parser()` so every existing flag keeps its meaning, then add:

```
--mode {live,historical}          default live
--session-date YYYY-MM-DD         single session (live/historical)
--from-day / --through-day        historical range
--once                            single completed slot then exit
--slot HH:MM                      explicit slot
--underlyings SYM,SYM,...         restrict universe (default: full F&O stock universe)
--include-index / --no-include-index
--expiry-policy {NEAREST_UNEXPIRED_MONTHLY,NEAREST_UNEXPIRED_WEEKLY}
--strike-window N                 strikes each side of ATM (default 0)
--spot-source {AUTO,EQUITY_5M,EQUITY_1M}
--max-apps 8                      default 8
--workers-per-app 2
--writer-workers 8                hard cap 8 unless --allow-high-workers
--request-interval-sec 0.34
--timeout-sec 8.0
--min-coverage 0.99
--slot-retry-attempts             >= MIN_NO_CANDLE_FETCH_ATTEMPTS - 1
--boundary-buffer-sec
--dry-run                         resolve + plan + print projections, zero API history calls
```

`--dry-run` must be genuinely side-effect free: it resolves the ATM map, prints the contract count,
projected requests, projected duration, and writes nothing.

---

## TESTS (required, in `tests/`)

Create `tests/test_fno_options_atm_fetch_5min.py` following the style of
`tests/test_fno_v13_v5_derivative_data.py` and `tests/test_fno_oi_pipeline.py`.
All tests must run offline with mocked Kite clients — **no test may hit the network.**

Minimum coverage:
1. ATM strike selection picks the nearest strike and applies the documented tie-break.
2. Strike-ladder generation is symmetric and correctly ordered for `--strike-window 2`.
3. A missing spot price yields `NO_SPOT_PRICE_FOR_SLOT` and writes no data row.
4. An expiry absent from the master yields `EXPIRED_OPTION_NOT_IN_CURRENT_MASTER` and
   **never** substitutes another expiry.
5. Re-running the same slot is idempotent (no duplicate rows; identical contract set).
6. Contract-set drift between resolutions aborts with a named-symbol error.
7. Read-back verification fails loudly on a corrupted/mismatched archived row.
8. An auth failure on one app fails over to another lane and still completes the slot.
9. A 429 triggers the exponential backoff path, not an auth failover.
10. Marker is `complete=False` when coverage is below `--min-coverage`.
11. `--writer-workers 16` is rejected without `--allow-high-workers`.
12. `--dry-run` performs zero `historical_data` calls.

---

## INTEGRATION (do these, and say explicitly that you did)

1. Add a supervisor batch file `bat/run_fno_options_atm_fetch_5min.bat` mirroring
   `bat/run_fno_oi_fetch_5min_fast_production.bat` (log, status, heartbeat, freshness files).
2. Register the session label in `log_dashboard_server.py` next to the existing
   `"fno_oi_fetch_5min_fast_production"` entries (there are **two** dicts, ~line 212 and ~line 8944,
   plus the schedule card list ~line 9212 — update all three consistently).
3. Extend the session table in `FNO_PAPERTRADE_DASHBOARD_OPERATIONS.md` with the new session id,
   human label, and `latest_*.md` report filename.
4. Write the human-readable per-slot report to `common.LATEST_DIR / "latest_fno_options_atm.md"`
   via `common.atomic_write_text`.
5. **Do not** modify `fno_oi_fetch_5min.py`, `fno_oi_fetch_5min_fast_shadow.py`,
   `fno_oi_fetch_5min_fast_production.py`, or `fno_oi_common.py` in any way that changes existing
   futures behaviour. If you genuinely need a helper from `fno_oi_common.py`, **import it**; only
   *add* new symbols there, never alter existing ones. State clearly if you added anything.
6. **Do not** enable, create, or re-enable any Windows scheduled task. There is a standing order
   that dashboard V7-flow / forensic / research tasks stay disabled. Deliver the `.bat` and the
   exact `schtasks` command as *documentation only*.

---

## HARD CONSTRAINTS — VIOLATION MEANS THE WORK IS REJECTED

- Interpreter is **`py`** (Python 3.12 / pandas 2.x), **not** the anaconda `python`. Verify with
  `py -c "import sys, pandas; print(sys.version, pandas.__version__)"` before running anything.
- **No orders. No `place_order`, no `modify_order`, no `cancel_order`, no order-adjacent import.**
  This module is read-only market data.
- No silent expiry/strike/contract substitution, ever. Unresolvable → explicit state + dropped row.
- No secrets in logs, markers, reports, parquet columns, or error messages.
- No network calls inside tests.
- No writes outside `common.FNO_ROOT` subdirectories and the repo's own log/status dirs.
- Deterministic ordering everywhere (`kind="stable"` sorts) so re-runs are byte-comparable.
- Timezone: everything IST via `common.IST` / `common.now_ist()`; no naive datetimes persisted.

---

## DELIVERY PROTOCOL

Work in this order and **report at each checkpoint before proceeding**:

1. **Recon** — the one-paragraph summary from Step 0, plus a list of every function you intend to
   import/reuse vs. write fresh.
2. **Design** — the exact `OPTIONS_RAW_COLUMNS` tuple, marker schema, directory layout, and the
   projected request-count arithmetic for a full-universe ATM-only slot with 8 apps. **Stop here
   and wait for my approval before writing the module.**
3. **Implement** — the module, then the tests.
4. **Verify** — run `py -m pytest tests/test_fno_options_atm_fetch_5min.py -q` and paste the real
   output. Then run `py fno_options_atm_fetch_5min.py --mode historical --session-date <a recent
   trading day> --underlyings RELIANCE,INFY --strike-window 1 --dry-run` and paste the real output.
   Do not claim success without pasted evidence.
5. **Integrate** — dashboard/label/batch/doc wiring, and say exactly which files you touched.

If any requirement here conflicts with something you find in the codebase, **stop and ask** rather
than guessing. If a requirement turns out to be impossible (e.g. an API limitation), say so plainly
with the evidence — do not quietly implement a weaker version.
