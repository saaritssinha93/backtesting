# FnO Papertrade Dashboard — Operations Reference

**Server:** [log_dashboard_server.py](log_dashboard_server.py) · `ThreadingHTTPServer` · default port **8787**
**Shared session:** [fno_multi_paper_session.py](fno_multi_paper_session.py) · session ID `fno_v10_v11_v12_paper`
**Mode:** `PAPER` only — `PAPER_ONLY = True`, and the runtime **raises** if `FNO_MULTI_PAPER_EXECUTION_MODE` is anything but `PAPER`

```powershell
python log_dashboard_server.py --port 8787
```

This document covers the four dashboard groups you asked about, in the order they
appear in the navigation:

| # | Group | Nav label | Accent | Cards |
|---|---|---|---|---:|
| 1 | [Live Market Data](#1-live-market-data) | Live Market | `market` | 4 |
| 2 | [FnO](#2-fno) | FnO | `market` | 16 |
| 2a | [V10 / V11 / V12 Shared Papertrade Session](#2a-v10--v11--v12-shared-papertrade-session) | *(subgroup)* | — | 1 |
| 2b | [V10](#2b-v10) | *(subgroup)* | — | 1 |
| 2c | [V11](#2c-v11) | *(subgroup)* | — | 1 |
| 2d | [V12](#2d-v12) | *(subgroup)* | — | 1 |
| 3 | [Data & Backtesting](#3-data--backtesting) | Backtesting | `research` | 2 |
| 4 | [Admin & Exports](#4-admin--exports) | Admin | `admin` | 6 |

Groups not covered here (they belong to other strategy families): *Forensic*,
*V7 Flow*, *Research & Suggestions*, *V16 / Parallel Strategy*.

---

## 0. How the dashboard is wired

### 0.1 Group definition shape

Every group in `ACTIVE_GROUPS` is a JS object with the same five keys:

```js
{
  key:    "fno",              // stable DOM/anchor key
  nav:    "FnO",              // short label in the top navigation
  title:  "FnO",              // heading rendered above the card grid
  accent: "market",           // colour token: market | research | v7 | v16 | admin
  ids:    [ ... ],            // ordered list of card IDs in this group
  subgroups: [ ... ]          // optional; splits the grid into titled bands
}
```

A **subgroup** adds a `note` — the one-line strapline under its title — and its
own `ids`. Cards listed in a subgroup are pulled out of the parent grid and
rendered under that band instead.

### 0.2 What a card actually is

Each card ID maps to two things:

| Mapping | Source | Purpose |
|---|---|---|
| `LOG_TITLES[id]` | `log_dashboard_server.py` | human-readable card heading |
| `FNO_OI_CARD_REPORTS[id]` | `log_dashboard_server.py` | the `latest_*.md` report the card renders |

Cards in `FNO_OI_CARD_REPORTS` render a **markdown report** from
`<FNO_ROOT>/latest/`. Cards not in that map render a **raw log tail** instead.
The `MD_REPORT_CARDS` set on the client decides which of the two renderers to
use.

### 0.3 Refresh model

The browser polls on a **15-second** cycle. `ThreadingHTTPServer` is used
specifically because several concurrent 15-second refreshes would otherwise
serialise behind one another.

---

## 1. Live Market Data

**`key: "market"` · nav `Live Market` · accent `market`**

The four upstream feeds every downstream strategy depends on. If any of these
is stale, everything below it blocks rather than trades.

| Order | Card ID | Title | Renders |
|---:|---|---|---|
| 1 | `nifty_guard_fetch_v16_5min` | NIFTY Fetch 5min | log tail |
| 2 | `eod_5min_data` | Live Data Fetch (5mins) | log tail |
| 3 | `kiteticker_5min_data` | Live Data kiteticker Fetch (5mins) | log tail |
| 4 | `eod_1min_data` | Live Data Fetch (1min) | log tail |

### What each one does

**`nifty_guard_fetch_v16_5min` — NIFTY Fetch 5min**
Pulls the NIFTY index 5-minute series used as a market-regime guard. Named for
V16 but consumed more widely.

**`eod_5min_data` — Live Data Fetch (5mins)**
The primary five-minute equity bar producer. This is the feed the FnO scanners
join against, and the one whose `slot_ready_5m/` marker gates every V6 scan.

**`kiteticker_5min_data` — Live Data kiteticker Fetch (5mins)**
The websocket-derived five-minute path, running alongside the REST fetch as a
cross-check and fallback.

**`eod_1min_data` — Live Data Fetch (1min)**
The one-minute equity bar producer. Every confirmation candle, every entry
trigger and every exit path in the FnO stack resolves against this feed.

### Reading order when something breaks

```
1min feed stale?      → confirmation and entry cannot resolve  → session DEGRADED
5min feed stale?      → no candidates are raised at all        → session BLOCKED
kiteticker divergent? → cross-check failure; REST path is authoritative
NIFTY stale?          → regime guards fall back to neutral
```

---

## 2. FnO

**`key: "fno"` · nav `FnO` · accent `market` · 17 cards, 4 subgroups**

The full FnO stack: shared upstream, the V6 live generation, and the three
modern papertrade profiles.

### 2.0 Cards in the parent grid

These render above the subgroup bands.

| Card ID | Title | Report |
|---|---|---|
| `fno_oi_universe` | FnO Near-Month Futures Universe | `latest_fno_oi_universe.md` |
| `fno_oi_fetch_5min_fast_production` | FnO Live 5-Minute Futures OI Fetch (Fast Production) | `latest_fno_oi_fast_production.md` |
| `fno_oi_fetch_5min` | FnO Live 5-Minute Futures OI Fetch (Old) | `latest_fno_oi_fetch.md` |
| `fno_oi_fetch_5min_fast_shadow` | FnO Fast Shadow OI Validator | `latest_fno_oi_fast_shadow.md` |
| `fno_oi_feature_ranker` | FnO OI Gainers, Losers & Activity Rankings | `latest_fno_oi_leaderboard.md` |
| `fno_v6_scanner_5min` | FnO V6 BEST_NET Equity 5-Minute + Futures OI Scanner | `latest_fno_v6_scanner_5min.md` |
| `fno_v6_equity_1min_feed` | FnO V6 Durable Completed Equity 1-Minute Feed | `latest_fno_v6_equity_1min_feed.md` |
| `fno_v6_confirmation_1min` | FnO V6 BEST_NET Candidate Equity 1-Minute Confirmation | `latest_fno_v6_confirmation_1min.md` |
| `fno_v6_live_long` | FnO V6 BEST_NET LONG Entry Session | `latest_fno_v6_live_long.md` |
| `fno_v6_live_short` | FnO V6 BEST_NET SHORT Entry Session | `latest_fno_v6_live_short.md` |
| `fno_v6_trade_logger` | FnO V6 BEST_NET Continuous Trade Log | `latest_fno_v6_trade_logger.md` |
| `fno_v6_net_result` | FnO V6 BEST_NET Net Result | `latest_fno_v6_net_result.md` |
| `fno_v8_combined_paper` | FnO V8-Combined Paper Shadow Session | `latest_fno_v8_combined_paper.md` |
| `fno_oi_eod_qc` | FnO EOD Data Quality Control | `latest_fno_oi_eod_qc.md` |

**Every FnO card renders a markdown report.** None of them tail a raw log.

### 2.0.1 Daily flow through the parent cards

```
08:50  fno_oi_universe          near-month futures universe, mapped to cash symbols
         │
09:05  fno_oi_fetch_5min_fast_production  fast production futures 5m OI + readiness markers
09:05  fno_oi_fetch_5min                  old production session, retained and labeled Old
         │
09:06  fno_oi_fetch_5min_fast_shadow      isolated shadow validation + exact Kite parity
         │
09:15  fno_oi_feature_ranker    OI gainers / losers / activity leaderboard
         │
       ┌─┴──────────────── V6 generation (six roles) ────────────────┐
       │ fno_v6_scanner_5min        candidate superset per slot      │
       │ fno_v6_equity_1min_feed    durable completed 1m bars        │
       │ fno_v6_confirmation_1min   confirm → rank → select          │
       │ fno_v6_live_long / _short  PAPER entry workers              │
       │ fno_v6_trade_logger        consolidated CSV + markdown      │
       │ fno_v6_net_result          realized / unrealized / ROC      │
       └─────────────────────────────────────────────────────────────┘
         │
       fno_v8_combined_paper     V8-Combined shadow session
         │
15:40  fno_oi_eod_qc            end-of-day data quality control
```

---

### 2a. V10 / V11 / V12 Shared Papertrade Session

**`key: "fno-modern-paper-session"`**
**Note:** *one task | one engine | one heartbeat | three independent ledgers*

| Card ID | Title | Report |
|---|---|---|
| `fno_v10_v11_v12_paper` | FnO V10/V11/V12 Papertrade - Shared Session | `latest_fno_v10_v11_v12_paper.md` |

This single card is the control surface for
[fno_multi_paper_session.py](fno_multi_paper_session.py). The strapline is a
precise description of the architecture, not marketing:

| Phrase | What it means |
|---|---|
| **one task** | A single scheduled process. There is no per-profile scheduler entry. |
| **one engine** | `MultiStrategyPaperEngine` holds three `ProfilePaperEngine` instances. |
| **one heartbeat** | One `heartbeat.json`, one `status.json`, one process lock. |
| **three independent ledgers** | Each profile keeps its own portfolio state, its own reservations and its own trade records. |

#### 2a.1 Why one process

The design goal is **identical source bytes**. If V10, V11 and V12 ran as three
tasks they would each fetch their own bars, and any divergence in their results
could be a data artefact rather than a strategy difference. Running one adapter
and fanning the same immutable snapshot into three policy-isolated ledgers makes
every difference attributable to policy.

The module docstring states it directly:

> It uses the shared full-universe five-minute and union one-minute adapter,
> then feeds the same immutable source bytes into three policy-isolated
> ledgers. There is no broker order API or LIVE execution mode in this module.

#### 2a.2 Runtime constants

| Constant | Value |
|---|---|
| `SESSION_ID` | `fno_v10_v11_v12_paper` |
| `SESSION_TITLE` | FnO V10/V11/V12 Papertrade |
| `SESSION_SCHEMA_VERSION` | `fno_multi_paper_session_v2` |
| `MODE` / `PAPER_ONLY` | `PAPER` / `True` |
| `SIGNAL_ENDS` | 09:25, 09:30, 09:35, 09:40, 09:45 |
| `SQUARE_OFF` | 15:30 |
| `BOUNDARY_BUFFER_SECONDS` | `DEFAULT_BOUNDARY_BUFFER_SEC` (shared with V8-Combined) |
| `DEFAULT_POLL_SECONDS` | 1.0 |
| `PREFERRED_APP_NAMES` | `app1` … `app8` |
| `MIN_HEALTHY_APP_COUNT` | **7** |
| `TERMINAL_CHECKPOINT_STATES` | `COMPLETE`, `BLOCKED`, `DEGRADED` |

#### 2a.3 Artefact layout

```
<FNO_ROOT>/multi_strategy_paper_v1/
├── status.json                          one status for all three
├── heartbeat.json                       one heartbeat for all three
├── fno_v10_v11_v12_paper.lock           single-process lock
└── sessions/<YYYY-MM-DD>/               per-session day root
                                         (checkpoints, CSVs, events)

<FNO_ROOT>/latest/
├── latest_fno_v10_v11_v12_paper.md      combined report  → shared card
├── latest_fno_v10_paper.md              per-profile      → V10 card
├── latest_fno_v11_paper.md              per-profile      → V11 card
└── latest_fno_v12_paper.md              per-profile      → V12 card
```

**One writer, four reports.** The three per-profile cards below are *views* of
this one session's output — they are not separate runners.

#### 2a.4 Execution steps

```
 1. Acquire ProcessLock                     single instance per session date
 2. Assert mode                             FNO_MULTI_PAPER_EXECUTION_MODE == PAPER, else raise
 3. parity.validate_canonical_profiles()    the three profiles match their canonical hashes
 4. Import fno_multi_paper_live_source      the shared adapter
 5. Authenticate app pool                   app1..app8, require >= 7 healthy
 6. write_manifest()                        session identity + source module hash
 7. Loop, poll every 1.0 s:
      a. wait for the completed-candle boundary + buffer
      b. pull the full-universe 5m slot at each of the five SIGNAL_ENDS
      c. _register_slot()  → engine.register_candidates()  for all three profiles
      d. pull the union 1m bar set for required_symbols()
      e. engine.process_completed_minute()  → each ledger advances independently
      f. persist_checkpoint()               resumable state
      g. publish_outputs() + publish_heartbeat()
 8. At 15:30 square-off, or on terminal state, publish final reports
```

#### 2a.5 Fail-closed behaviour

| Exception | Meaning |
|---|---|
| `MultiPaperSessionError` | base class; any of the below is fatal to the session |
| `ProspectiveStartMissed` | the process started after the window it would have needed to observe — it will **not** back-fill |
| `SourceIncompleteError` | the source snapshot for a slot or minute is incomplete — the session degrades rather than trading partial data |

`ProspectiveStartMissed` is the important one: a late start cannot manufacture a
retroactive prospective record. The session refuses rather than producing
evidence it did not observe.

#### 2a.6 Preflight

```powershell
python fno_multi_paper_session.py preflight --session-date 2026-08-31 --authenticate-apps
```

Preflight validates configuration and, optionally, authenticates the app pool.
It publishes a `PREFLIGHT_OK` status whose message states plainly:

> Configuration validated; no historical trades were replayed or fabricated.

Exit code **0** on success, **2** on failure.

---

### 2b. V10

**`key: "fno-v10"` · note:** *5m selection | 1m entry | LONG | SHORT | result | logs*

| Card ID | Title | Report |
|---|---|---|
| `fno_v10_paper` | FnO V10 Papertrade View | `latest_fno_v10_paper.md` |

#### Profile

| Field | Value |
|---|---|
| `key` | `v10` |
| `profile_id` | `V10_STAGE7_0935_LONG_MAX_050_GAP2` |
| `display_name` | V10 .50 + Gap2 |
| `lineage` | `FROZEN_V10_STAGE7_PLUS_MAX050_PLUS_GAP2` |
| **fingerprint** | `e7c34290b01a9c2af170a9fcfa52c6f33c397951fdae8ea752db06d4f33c4234` |

#### Execution definition

```python
ExecutionDefinition(
    max_adverse_gap_bps = 2.0,
    entry_not_before    = (),          # no per-leg fill delay
    same_side_symbol_limit = 1,        # one position per symbol
    prohibit_opposite_side = True,
)
```

#### Selection constraint

```python
SelectionConstraint("09:35_LONG", max_directional_move_pct=0.50)
```

The 09:40 LONG Stage-7 floor is **baked into the setup row itself**, not applied
as a constraint — the profile carries `price_change_pct = 0.40` with an inline
comment:

```python
# Frozen Stage 7: this is 0.40, not the V8 base value 0.20.
S("09:40", "LONG", 1, "max_liquidity", 0.40, 0.10, 2.0, 0.50, 0.50, 0.0, 0.50, 2.5),
```

---

### 2c. V11

**`key: "fno-v11"` · note:** *5m selection | 1m entry | LONG | SHORT | result | logs*

| Card ID | Title | Report |
|---|---|---|
| `fno_v11_paper` | FnO V11 Papertrade View | `latest_fno_v11_paper.md` |

#### Profile

| Field | Value |
|---|---|
| `key` | `v11` |
| `profile_id` | `V11_S10_POST_HOC_TOP2_1436C7D363` |
| `display_name` | V11 Stage 10 |
| `lineage` | `V10_MAX050_GAP2_PLUS_0930_SHORT_S3_PLUS_SAME_SIDE_2` |
| **fingerprint** | `f5ffd1b5b150bd18bf04b14f961f3b9d87e9e94f29c0fd9204b708b4d05e1139` |

#### Execution definition — the two deltas from V10

```python
ExecutionDefinition(
    max_adverse_gap_bps = 2.0,                    # unchanged
    entry_not_before    = (("09:30_SHORT", 3),),  # ← CHANGED: fill not before S+3
    same_side_symbol_limit = 2,                   # ← CHANGED: was 1
    prohibit_opposite_side = True,                # unchanged
)
```

**V11 shares V10's setup rows byte for byte** — `setups=_V10_SETUPS` and
`entry_policies=_V10_ENTRY_POLICIES` are literally the same objects. The only
difference is the `ExecutionDefinition`.

---

### 2d. V12

**`key: "fno-v12"` · note:** *5m selection | 1m entry | LONG | SHORT | result | logs*

| Card ID | Title | Report |
|---|---|---|
| `fno_v12_paper` | FnO V12 Papertrade View | `latest_fno_v12_paper.md` |

#### Profile

| Field | Value |
|---|---|
| `key` | `v12` |
| `profile_id` | `V12_S06_LATE_SHORT_VOLUME_MIN_150` |
| `display_name` | V12 Selected |
| `lineage` | `V11_STAGE10_PLUS_LATE_SHORT_VOLUME_MIN_150` |
| **fingerprint** | `56972fa6ae343483fa9da18cf988a64e3c428b0ebb4cad07253b3725622239a0` |

#### The setup derivation — two fields, generated in code

```python
_V12_SETUPS = tuple(
    SetupDefinition(**{
        **asdict(setup),
        "volume_ratio": (
            1.50
            if setup.setup_id in {"09:40_SHORT", "09:45_SHORT"}
            else setup.volume_ratio
        ),
    })
    for setup in _V10_SETUPS
)
```

V12 is **derived from V10's setups by a comprehension**, not written out by
hand. Two setup IDs get `volume_ratio = 1.50`; every other field of every other
leg is copied verbatim. And:

```python
execution = V11_PROFILE.execution     # the exact same object
```

So V12 = V10 setups + two volume floors + V11's execution definition, and the
code makes that structurally impossible to get wrong.

---

### 2e. The three profiles side by side

| Layer | V10 | V11 | V12 |
|---|---|---|---|
| Setup rows | `_V10_SETUPS` | *same object* | derived: 2 fields changed |
| Entry policies | `_V10_ENTRY_POLICIES` | *same object* | re-resolved from `_V12_SETUPS` |
| Portfolio | `_PORTFOLIO` | *same object* | *same object* |
| Selection constraint | `_MAX050` | *same object* | *same object* |
| Gap guard | 2.0 bps | 2.0 bps | 2.0 bps |
| Entry-not-before | — | 09:30_SHORT ≥ S+3 | 09:30_SHORT ≥ S+3 |
| Same-side symbol limit | 1 | 2 | 2 |
| Opposite side | prohibited | prohibited | prohibited |

**Shared portfolio (identical in all three):**

| Field | Value |
|---|---:|
| `capital_rs` | 120,000.0 |
| `margin_per_entry_rs` | 10,000.0 |
| `target_exposure_per_entry_rs` | 50,000.0 |
| `max_concurrent_positions` | 12 |
| `pending_reserves_margin` | True |
| `one_position_per_symbol` | True *(V11/V12 relax this to 2 same-side via `ExecutionDefinition`)* |

**Shared entry-policy defaults:**

| Field | Value |
|---|---|
| `buffer_bps` | 0.0 *(per-leg override on 09:25_SHORT = 2.0)* |
| `max_confirmation_minute` | 1 *(per-leg overrides: 09:25_L/S and 09:30_S = 3)* |
| `entry_expiry_minute` | 5 |
| `cost_bps` | 15.0 |
| `slippage_bps` | 0.0 |
| `same_bar_policy` | `STOP_FIRST` |
| `square_off` | 15:30 |
| **`eod_policy`** | **`EXACT_SQUARE_OFF`** |

> ⚠ **The single most important parity note on this page.** The papertrade
> profiles use **`EXACT_SQUARE_OFF`**, while the backtests that produced the
> V10/V11/V12 headline figures use **`LAST_REAL_BAR_SENSITIVITY`**. In the V12
> backtest, last-real-bar exits supplied **54.63% of net points**. The paper
> session does not have that crutch — it needs a real 15:30 bar. **Paper results
> and backtest results are therefore not directly comparable on P&L.** Compare
> them on candidate counts, confirmation rates and fill counts first.

---

## 3. Data & Backtesting

**`key: "backtesting"` · nav `Backtesting` · accent `research`**

| Order | Card ID | Title | Renders |
|---:|---|---|---|
| 1 | `data_for_backtesting` | Data for backtesting | log tail |
| 2 | `backtesting_result_v11` | Backtesting result v6/v8/v10/v11/v12 | log tail |

**`data_for_backtesting`**
The historical data preparation job — builds and refreshes the parquet stores
that every backtester reads. This is the upstream of the whole research line.

**`backtesting_result_v11`**
Despite the `v11` in its ID, this card's title says what it actually covers:
**v6 / v8 / v10 / v11 / v12**. It is the consolidated backtest result surface
for all five generations.

> The card ID is historical — it was named when V11 was the newest generation
> and was never renamed. Do not read the ID as a scope limit; read the title.

Neither card is in `FNO_OI_CARD_REPORTS`, so both render **raw log tails**
rather than markdown reports.

---

## 4. Admin & Exports

**`key: "admin"` · nav `Admin` · accent `admin`**

| Order | Card ID | Title | Renders |
|---:|---|---|---|
| 1 | `eod_15min_data` | Live Data Fetch (15mins) | log tail |
| 2 | `kite_positions_day_today_csv` | Kite Positions (Daily, Today) | log tail |
| 3 | `kite_holdings_today_csv` | Kite Holdings (Today) | log tail |
| 4 | `preopen_healthcheck` | Preopen Healthcheck | log tail |
| 5 | `authentication_v2` | Auth_V2 | log tail |
| 6 | `eod_1540_update` | Live EOD Data Fetch | log tail |

### Broker export paths

```
BASE_DIR/kite_exports/
├── kite_snapshot_meta.json                 snapshot metadata, read on render
├── holdings_<YYYYMMDD>.csv                 dated holdings
├── kite_holdings_today.csv                 today alias
├── positions_day_<YYYYMMDD>.csv            dated day positions
└── kite_positions_day_today.csv            today alias
```

Both export cards read the **dated file first**, then fall back to the `today`
alias. The dashboard renders `kite_snapshot_meta.json` alongside so you can see
when the snapshot was actually taken rather than assuming it is current.

### Auth state

```
BASE_DIR/auth_v2_state.json     Auth_V2 session state
BASE_DIR/access_token.txt       broker access token
```

`authentication_v2` is the card to check first when anything downstream reports
an API failure — a stale access token surfaces here before it surfaces as a
strategy fault.

### Daily admin sequence

```
pre-open   preopen_healthcheck      environment, feeds and auth are ready
pre-open   authentication_v2        broker session established
intraday   eod_15min_data           15-minute bar maintenance
post-close eod_1540_update          15:40 EOD data fetch
post-close kite_positions_day_today_csv / kite_holdings_today_csv
```

---

## 5. Worked example — reading a session end to end

A normal trading day, in the order you would actually look at the cards.

### 5.1 Pre-open

| Check | Card | Expect |
|---|---|---|
| Broker session live | `authentication_v2` | fresh token, no error |
| Environment ready | `preopen_healthcheck` | all checks pass |
| Universe built | `fno_oi_universe` | near-month contracts mapped to cash symbols |

If the app pool has fewer than **7 healthy apps**, the shared session will not
start — `MIN_HEALTHY_APP_COUNT = 7`.

### 5.2 First slot — 09:25

```
09:20–09:25   5-minute signal bar forms
09:25 + buffer  eod_5min_data publishes the completed slot
              fno_oi_fetch_5min_fast_production publishes the futures OI slot marker
              │
              ├─ fno_v6_scanner_5min      raises the V6 candidate superset
              └─ fno_v10_v11_v12_paper    registers candidates into ALL THREE ledgers
                                          from the same source bytes
09:25–09:26   confirmation candle forms
09:26 + buffer  eod_1min_data publishes it
              │
              ├─ fno_v6_confirmation_1min  confirms → ranks → selects → writes signals
              └─ shared session            process_completed_minute() advances
                                           each ledger independently
09:27 onward  earliest possible fill (S+2) — the confirmation candle
              can never fill itself
```

### 5.3 What "three independent ledgers" looks like in practice

Same slot, same candidates, three outcomes:

| Scenario | V10 | V11 | V12 |
|---|---|---|---|
| 09:40 SHORT candidate, volume ratio **1.30** | selected | selected | **rejected** — below the 1.50 floor |
| 09:30 SHORT confirms at S+2, trigger touched at S+2 | **fills** | **no fill** — earliest is S+3 | **no fill** |
| Second same-side position in one symbol | **rejected** — limit 1 | **allowed** — limit 2 | **allowed** |
| Bar opens 9 bps through the trigger | **rejected** — gap > 2 bps | **rejected** | **rejected** |

All four rows come from one source snapshot. Every divergence is attributable to
the profile, never to the data.

### 5.4 Intraday monitoring

| Card | Watch for |
|---|---|
| `fno_v10_v11_v12_paper` | one heartbeat, status `RUNNING`, source complete |
| `fno_v10_paper` / `_v11_` / `_v12_` | per-profile fills, open positions, running net |
| `eod_1min_data` | staleness — this feed drives every exit path |
| `fno_v6_*` | the V6 generation runs in parallel and independently |

### 5.5 Close and post-close

```
15:30   square-off; the paper profiles need a REAL 15:30 bar
        (EXACT_SQUARE_OFF — no last-real-bar fallback)
15:40   eod_1540_update
        fno_oi_eod_qc            data quality control
        kite_positions_day_today_csv / kite_holdings_today_csv
```

---

## 6. Troubleshooting map

| Symptom | Look at | Likely cause |
|---|---|---|
| Shared session status `BLOCKED` | `fno_v10_v11_v12_paper` | `SourceIncompleteError` — a slot or minute snapshot was incomplete |
| Shared session never started | same | `ProspectiveStartMissed` — process launched too late; it will not back-fill |
| Session refuses to start, no error in report | same | fewer than 7 healthy apps in the pool |
| All three profiles show zero candidates | `eod_5min_data`, `fno_oi_fetch_5min_fast_production` | 5-minute or futures-OI feed stale |
| Candidates raised but nothing confirms | `eod_1min_data` | 1-minute feed stale or late |
| Only V12 differs from V10/V11 | expected | the two late-SHORT volume floors are doing their job |
| Only V11/V12 differ from V10 | expected | S+3 fill delay and/or same-side limit 2 |
| Paper P&L ≠ backtest P&L | §2e parity note | `EXACT_SQUARE_OFF` vs `LAST_REAL_BAR_SENSITIVITY` |
| Broker export card empty | `kite_snapshot_meta.json` | snapshot not taken today; card fell back to a stale alias |
| Any API failure downstream | `authentication_v2` | stale access token |

---

## 7. Commands

```powershell
# Dashboard
python log_dashboard_server.py --port 8787

# Shared papertrade session — preflight (validates, trades nothing)
python fno_multi_paper_session.py preflight --session-date 2026-08-31 --authenticate-apps

# Shared papertrade session — the scheduled run
python fno_multi_paper_session.py run --session-date 2026-08-31 --poll-seconds 1.0
```

`run` is the real completed-candle session. There is **no LIVE execution mode
in this module** — no broker order API is imported, and the mode assertion
raises on anything other than `PAPER`.

---

## 8. Module map

| Module | Role |
|---|---|
| [log_dashboard_server.py](log_dashboard_server.py) | HTTP dashboard, group/card definitions, report and log rendering |
| [fno_multi_paper_session.py](fno_multi_paper_session.py) | Session orchestration, lock, checkpoints, heartbeat, publishing |
| [fno_multi_paper_profiles.py](fno_multi_paper_profiles.py) | The three frozen `StrategyProfile` definitions and their fingerprints |
| [fno_multi_paper_engine.py](fno_multi_paper_engine.py) | `MultiStrategyPaperEngine` + per-profile `ProfilePaperEngine` |
| [fno_multi_paper_live_source.py](fno_multi_paper_live_source.py) | The shared 5m + union 1m market-data adapter |
| [fno_multi_paper_parity.py](fno_multi_paper_parity.py) | `validate_canonical_profiles()` — hash check against the frozen backtests |
| [fno_multi_paper_report.py](fno_multi_paper_report.py) | Combined and per-profile markdown report rendering |

### Related strategy documentation

- [FNO_V10_MAX050_GAP2_BACKTEST_STRATEGY.md](FNO_V10_MAX050_GAP2_BACKTEST_STRATEGY.md)
- [FNO_V11_STAGE10_BACKTEST_STRATEGY.md](FNO_V11_STAGE10_BACKTEST_STRATEGY.md)
- [FNO_V12_LATE_SHORT_VOLUME_BACKTEST_STRATEGY.md](FNO_V12_LATE_SHORT_VOLUME_BACKTEST_STRATEGY.md)
- [FNO_V6_LIVE_STRATEGY.md](FNO_V6_LIVE_STRATEGY.md) — the generation behind the `fno_v6_*` cards
- [FNO_V8_BACKTEST_STRATEGY.md](FNO_V8_BACKTEST_STRATEGY.md) — behind `fno_v8_combined_paper`
