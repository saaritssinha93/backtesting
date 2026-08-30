# FnO V8 — Windowed 1-Minute Entry Backtest (Complete Reference)

**Engine:** [fno_v8_windowed_1m_entry_backtest.py](fno_v8_windowed_1m_entry_backtest.py) (~5,070 lines) ·
**Status:** research only, **not promotion eligible** ·
**Artefact root:** `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1`

| | |
|---|---|
| Core engine | [fno_v8_windowed_1m_entry_backtest.py](fno_v8_windowed_1m_entry_backtest.py) |
| Strict-V6-logic launcher | [fno_v8_strict_v6_logic_backtest.py](fno_v8_strict_v6_logic_backtest.py) |
| Best-per-leg launcher | [fno_v8_combined_best_per_leg_backtest.py](fno_v8_combined_best_per_leg_backtest.py) |
| Per-setup parameter sweep | [fno_v8_setup_param_sweep.py](fno_v8_setup_param_sweep.py) |
| Entry-window optimizer | [fno_v8_windowed_1m_entry_optimize.py](fno_v8_windowed_1m_entry_optimize.py) |
| Setup-book SHA-256 | `ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675` |

---

## 1. What V8 is, and why it exists

V6's backtest models an entry as: *"the confirmation candle sets a trigger; the
first later 1-minute bar that trades through the trigger fills exactly at the
trigger; brackets are computed from the trigger."* That is a clean abstraction
and it is **optimistic in three specific ways**:

1. it fills at the trigger price even when the bar **gapped straight past it**;
2. it computes stop/target from the *intended* level, not the *achieved* fill;
3. it lets the order live indefinitely, and it simulates each leg in isolation
   with no shared capital.

**V8 exists to remove all three optimisms and to make the entry seam itself a
research variable.** It is a from-scratch, independent re-implementation:

> It does **not** import the V6/V7 strategy modules, their caches, the legacy
> sweep builder, the legacy replay engine, or the live configuration.

Instead it owns:

- a literal, hash-locked copy of the ten V6 five-minute setup legs;
- exact 1-minute → 5-minute cash aggregation and futures-OI joining;
- a hash-locked **2026 regular-session calendar** built from NSE F&O and
  cash-market circulars, including the 15-Jan holiday amendment and the full
  regular Union Budget session on Sunday 1-Feb;
- timestamped, same-session OHLCV paths;
- the sequential **V8 confirmation/order state machine**;
- adverse-gap entry, tick rounding, actual-fill brackets, stop-first same-bar
  treatment;
- setup-cap **reassignment** during the S+1..S+5 window;
- a **global** pending-margin / concurrency / duplicate-symbol ledger with a
  conservative no-backfill rule after a portfolio rejection;
- V8-only cache, run, report, event, diagnostic-breakdown and provenance
  artifacts.

---

## 2. The five-minute selection layer (inherited, unchanged)

V8 keeps the V6 five-minute authority exactly. `five_minute_candidate_passes()`
is a hard gate — `simulate_setup_window()` **raises** if handed a candidate that
fails it.

### 2.1 Base gate

```
LONG   : ema9 > ema20 > ema50   AND   price_change_pct >= +threshold
SHORT  : ema9 < ema20 < ema50   AND   price_change_pct <= -threshold
BOTH   : oi > prev_oi           AND   oi_change_pct >= setup.oi_change_pct
                                AND   volume_ratio   >= setup.volume_ratio
                                AND   traded_value   >= setup.min_traded_value
```

### 2.2 The V8 `ACTIVE_SETUPS` book (10 legs)

Six legs keep their original V6-lineage values. **Four legs were retuned on
2026-08-19** from the per-setup parameter sweep over 2026-05-27 … 2026-08-17
(57 sessions), and each of those carries its **own entry-seam overrides**:

| Signal | Side | Max | Picker | Price % | OI % | Vol | Body ≥ | Wick ≤ | Min TV | Stop % | Tgt % | Entry overrides |
|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 09:25 | LONG | 4 | max_move | 0.30 | 0.10 | 3.0 | 0.00 | 0.50 | 0 | 0.40 | 1.0 | conf ≤ S+3, buffer 0 bps, no midpoint, no CLV |
| 09:25 | SHORT | 4 | max_move | 0.20 | 0.10 | 1.5 | 0.60 | 0.60 | 2.5 cr | 0.50 | 3.0 | conf ≤ S+3, buffer 2 bps, no midpoint, no CLV |
| 09:30 | LONG | 1 | max_move | 0.65 | 0.10 | 1.0 | 0.50 | 0.50 | 0 | 1.00 | 2.5 | *inherits global* |
| 09:30 | SHORT | 4 | max_volume | 0.20 | 1.00 | 1.0 | 0.45 | 0.30 | 2.5 cr | 1.00 | 4.0 | conf ≤ S+3, buffer 0 bps, **midpoint ON**, CLV ≥ 0.50 |
| 09:35 | LONG | 1 | max_liquidity | 0.20 | 0.10 | 1.0 | 0.60 | 0.50 | 0 | 1.00 | 2.5 | *inherits global* |
| 09:35 | SHORT | 2 | max_liquidity | 0.50 | 1.00 | 1.0 | 0.40 | 0.50 | 0 | 1.00 | 3.0 | *inherits global* |
| 09:40 | LONG | 1 | max_liquidity | 0.20 | 0.10 | 2.0 | 0.50 | 0.50 | 0 | 0.50 | 2.5 | *inherits global* |
| 09:40 | SHORT | 4 | max_volume | 0.20 | 0.75 | 1.0 | 0.00 | 0.20 | 0 | 1.00 | 4.0 | conf ≤ S+4, buffer 0 bps, no midpoint, CLV ≥ 0.50 |
| 09:45 | LONG | 1 | max_move | 0.65 | 0.10 | 1.0 | 0.40 | 0.50 | 0 | 1.00 | 3.0 | *inherits global* |
| 09:45 | SHORT | 1 | max_volume | 0.20 | 0.75 | 1.0 | 0.40 | 0.30 | 0 | 1.00 | 2.0 | *inherits global* |

Sweep provenance for the four retuned legs:

```
09:25_LONG   sweep_0925_LONG_20260819T174128830527+0530_befe1256f673
09:25_SHORT  sweep_0925_SHORT_20260819T182024291731+0530_d8c66340bc60
09:30_SHORT  sweep_0930_SHORT_20260819T190613582965+0530_cadb8948a596
09:40_SHORT  sweep_0940_SHORT_20260819T194335202048+0530_afbcb4356b45
```

> ⚠ **The source comment says it plainly:** these four were fit on the whole
> 57-session window **with no holdout left over**, and were **never simulated
> jointly through one portfolio ledger** during fitting. Treat the book above as
> a *research configuration*, not a validated one.

Note the caps: V6 allowed at most 1 LONG / 2 SHORT per slot. **V8 raises several
caps to 4**, which is the main reason V8 produces materially more fills per
session than V6.

### 2.3 Ranking

`_rank_candidates()` orders by picker value DESC → `traded_value` DESC →
`symbol` ASC. Pickers: `max_oi`, `max_volume`, `max_move` (absolute),
`max_liquidity`. The resulting `frozen_rank` is what the cap allocator uses, and
it stays fixed for the whole window.

---

## 3. The V8 entry state machine — the actual innovation

This is `simulate_setup_window()`. It runs **one (day, slot, side) occurrence at
a time**, marching minute by minute across `S+1 … S+5`, and it is *pure with
respect to disk*.

### 3.1 States

| State | Meaning |
|---|---|
| `MONITORING_CONFIRMATION` | Waiting for a strict 1-minute confirmation |
| `PRECONF_INVALIDATED` | Close crossed back through the 5-minute midpoint before confirming |
| `CONFIRMED_WAITING_CAP` | Confirmed, but the setup cap is full — queued by frozen rank |
| `PENDING_STOP` | Cap reserved; a stop order is resting at the trigger |
| `POSTCONF_CANCELLED` | A completed close reversed back through the 5-minute close |
| `WINDOW_EXPIRED` | Still unfilled at S+5 |
| `NO_CONFIRMATION` | Confirmation window closed with no strict confirmation |
| `DATA_INCOMPLETE` | A required bar was missing or invalid |
| `FILLED_OPEN` | Position open |
| `STOPPED` / `TARGETED` / `SQUARE_OFF` | Terminal exits |
| `PORTFOLIO_REJECTED` / `DUPLICATE_REJECTED` | Killed by the global ledger (§5) |

Transitions are enforced by an explicit `_ALLOWED_TRANSITIONS` map — an illegal
transition raises `AssertionError`. There is no way for the engine to reach an
undocumented state.

### 3.2 The per-minute loop (order matters, and it is deliberate)

For each minute index 1..5 (`required_times = signal_ts + 1…5 min`):

**Step 0 — bar validation.** Only the bar that has *now completed* is validated.
Any live candidate whose bar for this minute is missing or invalid transitions to
`DATA_INCOMPLETE`. A missing *future* bar can never erase a trade that already
reached a terminal state.

**Step 1 — existing pending orders may fill.** Critically:

```python
if runtime.order_placed_at is None or ts <= runtime.order_placed_at:
    continue
```

An order placed on the candle that just completed is **not eligible on that same
candle**. The confirmation candle can never fill itself.

**Step 2 — already-open positions resolve brackets** (positions that entered on
*this* bar are skipped here; their same-bar resolution is handled in step 1).

**Step 3 — post-confirmation cancellation.** Any `PENDING_STOP` or
`CONFIRMED_WAITING_CAP` whose completed close reversed back through the
five-minute signal close is cancelled (`CLOSE_REVERSED_THROUGH_SIGNAL_CLOSE`).
This is evaluated *after* fill processing, because a fill this minute takes
precedence over a cancellation this minute.

**Step 4 — monitoring candidates.** Midpoint invalidation is checked first (if
the policy enables it), then the strict confirmation gate, but only while
`minute_index <= policy.max_confirmation_minute`. The **first** passing candle
latches: it stores `confirmation_minute`, the confirmation bar, and builds the
trigger. At `minute_index == max_confirmation_minute`, everything still
`MONITORING` becomes `NO_CONFIRMATION`.

**Step 5 — capacity allocation** (only while `minute_index < entry_expiry_minute`):

```
reserved      = len(pending_symbols)
capacity_used = filled_cap + reserved      (if allow_cap_reassignment)
              = allocated_once             (otherwise)
available     = max(0, setup.max_entries - capacity_used)
```

The top `available` `CONFIRMED_WAITING_CAP` runtimes by frozen rank become
`PENDING_STOP` with `order_placed_at = ts`. **This is cap reassignment:** if a
pending order is cancelled at S+2, its slot is returned to the pool and the
next-ranked confirmed candidate gets it at S+3. No allocation happens at S+5 —
there is no later eligible entry bar.

**Step 6 — expiry.** At S+5, everything still `PENDING_STOP` /
`CONFIRMED_WAITING_CAP` / `MONITORING` expires.

### 3.3 Strict confirmation gate

`_confirmation_check()` returns a **lossless audit record** with rejection codes
in rule-evaluation order:

```
range          = high - low                       (> 0 required)
body_ratio     = |close - open| / range
adverse_wick   = LONG : (high - max(open,close)) / range
                 SHORT: (min(open,close) - low)  / range
close_location = LONG : (close - low)  / range
                 SHORT: (high - close) / range
```

| Rejection code | Condition |
|---|---|
| `INVALID_BAR` | non-finite/non-positive OHLC, high < max(o,c), low > min(o,c), negative volume, or `gap_filled` |
| `NONPOSITIVE_RANGE` | high == low |
| `WRONG_CANDLE_DIRECTION` | LONG needs close > open; SHORT needs close < open |
| `CLOSE_NOT_BEYOND_FIVE_MINUTE_CLOSE` | must close beyond the 5-minute signal close |
| `BODY_RATIO_BELOW_MINIMUM` | `body_ratio < setup.body_ratio` |
| `ADVERSE_WICK_RATIO_ABOVE_MAXIMUM` | `adverse_wick > setup.max_wick_ratio` |
| `CLOSE_LOCATION_BELOW_MINIMUM` | only if `policy.close_location_min` is set |
| `PRECONF_MIDPOINT_INVALIDATED` | injected when midpoint invalidation fires |

All comparisons carry a `1e-12` epsilon so float noise cannot flip a decision.

### 3.4 Trigger construction

```python
scale = policy.buffer_bps / 10_000
LONG : round_up_to_tick(bar.high * (1 + scale), tick)
SHORT: round_down_to_tick(bar.low  * (1 - scale), tick)
```

Rounding is **directional and Decimal-based** (`ROUND_CEILING` / `ROUND_FLOOR`),
never `round()`. A buffer makes the trigger strictly harder to reach — it is a
*noise filter*, not a slippage model.

### 3.5 Fill model — gap fills are the point

`_entry_fill()`:

```
LONG:
  if bar.open >= trigger:  fill at bar.open * (1 + slippage)   -> gap_fill = True
  elif bar.high >= trigger: fill at trigger  * (1 + slippage)  -> gap_fill = False
SHORT: mirrored on open <= trigger / low <= trigger
```

**This is the headline difference from V6.** If the bar *opens* already through
the trigger, V8 fills at the **open**, not the trigger — you pay the gap. Every
such fill is flagged `gap_fill = True` in the audit, which is exactly what makes
the V10 gap-guard research (see the V10 document) possible.

### 3.6 Brackets from the actual fill

```
LONG : stop   = round_down_to_tick(entry * (1 - stop_pct/100))
       target = round_down_to_tick(entry * (1 + target_pct/100))
SHORT: stop   = round_up_to_tick  (entry * (1 + stop_pct/100))
       target = round_up_to_tick  (entry * (1 - target_pct/100))
```

Computed from `entry_price`, **not** from the trigger — so a gap fill drags its
whole bracket with it. Rounding is always chosen to be **conservative** (stops
closer, targets further).

### 3.7 Exit resolution — `_exit_on_bar`

For a position **already open at bar start**, the bar's *open* is checked first:

| Check (LONG) | Result |
|---|---|
| `open <= stop` | `STOP_GAP` at the **open** (tick-rounded) — worse than the stop |
| `open >= target` | `TARGET` at the target |
| `low <= stop` | `STOP` at the stop |
| `high >= target` | `TARGET` at the target |

Stop is checked **before** target at every level: this is the `STOP_FIRST`
same-bar policy, and it is the only policy V8 accepts. `_exit_occurs_at_bar_open`
records whether the exit was deterministic at the open, so ambiguous intrabar
ordering is never silently assumed.

A position filled *on* a bar is re-checked against that same bar with
`position_open_at_bar_start=False` (so no open-gap logic) and, if it exits
immediately, flagged `ambiguous_entry_bar = True`.

### 3.8 P&L

```
gross = (exit/entry - 1) * 100          LONG
      = (1 - exit/entry) * 100          SHORT
net   = gross - cost_bps / 100
```

---

## 4. Post-window exit path and EOD policy

After S+5, still-open positions walk an **exact, consecutive, same-session
1-minute path** from `S+6` to the cutoff, where
`cutoff = min(requested_square_off, 15:30)`.

Any missing or invalid bar inside that path is terminal
`DATA_INCOMPLETE` (`MISSING_POST_ENTRY_PATH_BAR` / `INVALID_POST_ENTRY_PATH_BAR`).
The engine **never** skips a hole and continues.

Two EOD policies:

| Policy | Behaviour |
|---|---|
| `EXACT_SQUARE_OFF` | The bar at the exact cutoff must exist. Missing → `DATA_INCOMPLETE` (`MISSING_TERMINAL_SQUARE_OFF_BAR`). Headline-valid. |
| `LAST_REAL_BAR_SENSITIVITY` | Cutoff is moved back to the **last real valid bar** in the window; exit reason is recorded as `LAST_REAL_BAR_SENSITIVITY`. **Explicitly a sensitivity, never a headline.** |

The sensitivity policy only replaces a missing trailing session tail with the
last real bar. It does **not** repair missing full days, internal minutes,
confirmation bars, or futures/OI slots.

---

## 5. Global portfolio ledger

`apply_global_portfolio_constraints()` runs **after** all per-window simulations,
replaying every state event in strict chronological order.

### 5.1 Policy

```python
PortfolioPolicy(
    capital_rs                    = 120_000.0,
    margin_per_entry_rs           =  10_000.0,
    target_exposure_per_entry_rs  =  50_000.0,
    max_concurrent_positions      = 12,
    pending_reserves_margin       = True,   # only True supported
    one_position_per_symbol       = True,   # only True supported
)
```

```
capacity = min(max_concurrent_positions, floor(capital_rs / margin_per_entry_rs))
         = min(12, 12) = 12
```

### 5.2 Event replay

Every candidate's event stream is flattened into `RESERVE` / `RELEASE` actions:

- `→ PENDING_STOP` is a **RESERVE** (phase 2);
- `PENDING_STOP → terminal` or `FILLED_OPEN → terminal` is a **RELEASE** (phase 0).

Actions are sorted by `(event_ts, phase, signal_time, setup_id, frozen_rank,
symbol, candidate_id, sequence)`. Phase ordering guarantees **releases at a
timestamp are processed before reservations at that timestamp**, so freed
capacity is immediately reusable — but deterministically so.

Rejections:

| Reason | Condition |
|---|---|
| `DUPLICATE_SYMBOL_PENDING_OR_OPEN` | that symbol already has a pending/open position |
| portfolio capacity | active reservations already at `capacity` |

> **Conservative no-backfill rule:** a global rejection deliberately does **not**
> promote another setup candidate into the freed slot. This is stated in the
> docstring, is deterministic, and is explicitly fingerprinted. The
> unconstrained leg result is preserved in `unconstrained_*` diagnostic columns
> so the cost of the ledger is always measurable.

---

## 6. Entry-seam variants B0–B5

`VARIANT_REGISTRY` — the run-level `EntryPolicy` before per-setup overrides:

| Variant | Max conf minute | Buffer bps | Midpoint invalidation | Close-location min | Description |
|---|---:|---:|---|---|---|
| **B0** | 1 | 0.0 | off | – | S+1 strict confirmation only, raw break, S+5 expiry |
| **B1** | 4 | 0.0 | off | – | First strict confirmation S+1..S+4, raw break |
| **B2** | 4 | 2.0 | off | – | B1 + 2 bps trigger buffer |
| **B3** | 4 | 5.0 | off | – | B1 + 5 bps trigger buffer |
| **B4** | 4 | 2.0 | **on** | – | B2 + 5-minute-midpoint pre-confirmation invalidation |
| **B5** | 4 | 2.0 | **on** | 0.75 | B4 + directional close-location ≥ 0.75 |

`entry_expiry_minute` is fixed at **5** for every variant, and
`validate()` requires `entry_expiry_minute > max_confirmation_minute`.

`policy_for_setup()` then applies a leg's optional overrides
(`entry_conf_minute`, `entry_buffer_bps`, `entry_midpoint`, `entry_clv`).
**Cost, slippage, square-off and EOD policy are never overridable** — they are
run economics, not strategy. `entry_clv` uses a dedicated `ENTRY_INHERIT`
sentinel because `None` ("no close-location floor at all") is itself meaningful.

`validate_backtest_policy()` additionally refuses any `square_off` at or before
the latest leg's S+5 bar.

**Deliberately not implemented:** B6 (one-minute volume) and B7 (market/sector
context). The README is explicit that they are *not* filled in with uncertified
inputs — no previous-ten-bar ratios, sector attribution, or post-hoc
liquidity/OI/volume/volatility buckets are fabricated.

---

## 7. Data contract, calendar and cache

### 7.1 Fail-closed source contract

Every run requires an **explicit physical source-snapshot manifest**. There is no
silent fallback to a V6/V7 cache or a live directory.

- The requested window must fall inside **2026**, the year covered by the
  embedded exchange calendar.
- Expected sessions come from that **frozen hash-locked calendar** — never
  inferred from which price files happen to exist.
- Every selected symbol must have the exact **09:16–15:30 cash 1-minute grid**
  and the six required **futures-OI signal slots** on every expected session.
- A market-wide missing day, an off-grid row, invalid OHLCV/OI, or an unexpected
  holiday session makes the **headline fail closed**.

### 7.2 5-minute construction

`aggregate_equity_one_minute_to_five_minute()` groups exact 1-minute rows into
end-labelled 5-minute candles; `add_five_minute_features()` computes EMA9/20/50,
`price_change_pct`, `volume_ratio` and `traded_value`;
`join_cash_features_with_futures_oi()` attaches OI on exact bar-end timestamps.

### 7.3 Cache key

Each cache key includes: the **V8 source-code hash**, setup hash, data/path
policy, universe hashes, source-snapshot fingerprint, source-inventory
fingerprint, official-calendar hash and expected sessions, date window, and
selected symbol set. Each *run* additionally fingerprints the variant, timing,
cost, slippage, EOD and portfolio policies.

`_assert_cache_matches_active_strategy()` refuses a cache whose
`setup_book_sha256` does not match the live module — you cannot accidentally
replay a stale book.

### 7.4 Artifact isolation

V8 writes **only** under:

- `C:\TradingData\eqidv2\fno_oi\strategy_research\v8_windowed_strict_v1`
- `C:\TradingData\eqidv2\fno_oi\latest\latest_fno_v8_windowed_1m_research.md`

---

## 8. Outputs and diagnostics

Each run writes:

| Artifact | Contents |
|---|---|
| `candidate_order_audit.csv` | one terminal record per candidate — full 5-minute OHLCV/EMA/OI context, picker rank and setup cap, **every attempted confirmation candle** with ordered rejection codes and morphology, trigger and entry timing, execution economics, position notional, portfolio usage |
| `closed_trades.csv` | the filled-and-closed subset |
| `daily.csv` | per-session fills, net %, net Rs, cumulative curve |
| `diagnostic_breakdowns.csv` | side, setup, signal slot, confirmation minute, entry minute, buffer, symbol, official-calendar five-session block, gap-fill status |
| `report.md` | compact versions of the above |
| `state_events.csv` | the full transition log |
| `provenance.json` | tamper-checked, validated by `validate` |

**Excursion diagnostics:** post-fill MFE/MAE are emitted as **side-normalized
1-minute-OHLC lower and upper bounds**. Entry/exit-bar extremes contribute only
to the *upper* bound unless the position is known to span that whole bar;
ambiguity flags preserve the limitation instead of inventing an intrabar price
order.

---

## 9. Results

### 9.1 B0 full-history (2026-05-27 … 2026-08-17, 57 sessions)

**Scope and validity**

| Field | Value |
|---|---:|
| Official sessions | 57 |
| Mapped stock universe | 208 |
| Expected symbol-sessions | 11,856 |
| Source-complete symbol-sessions | 6,350 (53.56%) |
| Incomplete symbol-sessions | 5,506 (46.44%) |
| Exact-15:30 symbol-sessions | 9,568 (80.70%) |
| Unexpected non-calendar sessions | 0 |
| Five-minute candidates observed | 1,298 |
| Cache fingerprint | `8e02e775793334bd9bfeb32fe4c0c458cafea34a57adc1f84cad5fd64b9ab9cf` |

> The full-window headline is **invalid** because upstream coverage is
> incomplete. Only **five** sessions are source-complete for every mapped symbol
> (2026-07-24, 07-27, 07-28, 07-29, 07-31), and **every one of the 208 symbols**
> has at least one incomplete session. Missing whole symbol-days suppress
> candidates entirely — candidate counts do not repair or measure the deficit.

**Policy comparison**

| Policy | Candidates | Fills | Finite closed | Incomplete paths | PF | Net points | Net P&L | Pos/Neg/Flat days |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Exact 15:30 | 1,298 | 161 | 153 | 8 | 1.521 | +37.795 | +Rs 19,466.06 | 28 / 23 / 6 |
| Last-real-bar | 1,298 | 161 | 161 | 0 | 1.609 | +44.614 | +Rs 22,823.78 | 30 / 21 / 6 |

**Exact-policy monthly**

| Month | Sessions | Fills | Net points | Net P&L | Pos/Neg/Flat |
|---|---:|---:|---:|---:|---:|
| 2026-05 | 2 | 4 | +4.283 | +Rs 2,119.44 | 2 / 0 / 0 |
| 2026-06 | 21 | 38 | +2.646 | +Rs 1,450.64 | 7 / 9 / 5 |
| 2026-07 | 23 | 93 | +33.423 | +Rs 16,917.61 | 17 / 6 / 0 |
| 2026-08 | 11 | 18 | −2.559 | −Rs 1,021.63 | 2 / 8 / 1 |

**July carries the result.** August is negative.

**Exact-policy by side**

| Side | Candidates | Confirmed | Fills | Closed | W | L | Net points | Net P&L | PF |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| LONG | 487 | 120 | 69 | 65 | 28 | 37 | +17.429 | +Rs 8,470.51 | 1.614 |
| SHORT | 811 | 224 | 92 | 88 | 41 | 47 | +20.365 | +Rs 10,995.55 | 1.462 |

Both sides win **below 50%** and still return PF > 1.4 — the edge is entirely in
the payoff ratio (3:1 to 5:1 target:stop), not in hit rate.

**Exact-policy by setup**

| Setup | Cand | Conf | Fills | Closed | W | L | Net points | Net P&L | PF |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 09:25 LONG | 103 | 25 | 18 | 16 | 6 | 10 | +4.932 | +Rs 2,461.62 | 1.813 |
| 09:25 SHORT | 247 | 70 | 36 | 34 | 17 | 17 | +10.407 | +Rs 5,207.53 | 1.677 |
| 09:30 LONG | 58 | 15 | 10 | 10 | 5 | 5 | +6.559 | +Rs 3,226.20 | 2.447 |
| 09:30 SHORT | 266 | 68 | 21 | 20 | 8 | 12 | +3.611 | +Rs 2,185.44 | 1.302 |
| **09:35 LONG** | 211 | 39 | 22 | 20 | 8 | 12 | **−3.291** | **−Rs 1,563.83** | **0.728** |
| 09:35 SHORT | 36 | 15 | 7 | 7 | 4 | 3 | +5.770 | +Rs 2,911.33 | 2.668 |
| 09:40 LONG | 87 | 31 | 13 | 13 | 5 | 8 | +4.940 | +Rs 2,286.82 | 2.098 |
| 09:40 SHORT | 174 | 50 | 17 | 16 | 6 | 10 | +0.013 | +Rs 277.69 | 1.001 |
| 09:45 LONG | 28 | 10 | 6 | 6 | 4 | 2 | +4.289 | +Rs 2,059.70 | 4.589 |
| 09:45 SHORT | 88 | 21 | 11 | 11 | 6 | 5 | +0.565 | +Rs 413.56 | 1.152 |

**09:35 LONG is the only outright losing leg (PF 0.728).** This is precisely the
leg that the V10 research line later attacks with a `<= 0.50%` move ceiling.

**Exact-policy by exit**

| Exit | Trades | W | L | Net points | Net P&L | PF |
|---|---:|---:|---:|---:|---:|---:|
| Target | 26 | 26 | 0 | +69.482 | +Rs 34,124.03 | ∞ |
| Square-off | 59 | 43 | 16 | +35.117 | +Rs 17,275.94 | 7.156 |
| Stop | 66 | 0 | 66 | −64.442 | −Rs 30,793.02 | 0 |
| Stop gap | 2 | 0 | 2 | −2.363 | −Rs 1,140.89 | 0 |

Only **26 of 153** closed trades reach target, yet they contribute nearly the
whole gross profit. The strategy is a low-frequency, fat-tail payoff engine.

### 9.2 V8-Combined best-per-leg (40 sessions, 2026-06-24 … 2026-08-19)

`fno_v8_combined_best_per_leg_backtest.py` is an independent, hash-pinned
launcher using a **train-selected per-leg mapping**:

| Slot | LONG | SHORT |
|---|---|---|
| 09:25 | Retuned V8 | Retuned V8 |
| 09:30 | Common | Retuned V8 |
| 09:35 | Common | Common |
| 09:40 | Common | **V8-Strict** |
| 09:45 | Common | Common |

Split 2026-08-06 · 15/0 bps · `LAST_REAL_BAR_SENSITIVITY` · ~Rs 50,000 exposure.

| Metric | Value |
|---|---:|
| Candidates | 890 |
| Closed fills | 184 (4.60/session) |
| Wins / losses | 93 / 91 |
| Win rate | 50.54% |
| Diagnostic PF | **1.892** |
| Additive net | **+60.16 points** |
| Sizing-proxy net P&L | **+Rs 29,890** |
| Max cumulative daily drawdown | 6.00 points |
| Positive / negative / flat days | 28 / 12 / 0 |
| TRAIN fills / PF / net | 153 / 1.983 / +53.93 |
| TEST fills / PF / net | 31 / **1.494** / +6.23 |

**Three-way comparison at 15 bps**

| Strategy | Fills | Win % | PF | Net points | Max DD | Train PF | Test PF |
|---|---:|---:|---:|---:|---:|---:|---:|
| V8-Strict | 136 | 50.74% | 1.885 | +49.81 | 5.23 | 1.888 | **1.870** |
| Retuned V8 | **191** | 49.21% | 1.768 | +55.62 | **4.51** | 1.828 | 1.408 |
| **V8-Combined** | 184 | **50.54%** | **1.892** | **+60.16** | 6.00 | **1.983** | 1.494 |

> V8-Combined has the best full-window PF, net and train PF — **but the uplift
> is selected on the training history.** Its test PF (1.494) is *below*
> V8-Strict's (1.870) and fractionally below the proposed 1.50 floor. It is a
> shadow/PAPER research candidate, **not** a live-production configuration.

**V8-Combined vs V6, same 40 sessions**

At 15 bps:

| Metric | V6 | V8-Combined | Better |
|---|---:|---:|---|
| Closed fills | 176 | **184** | V8-Combined |
| Fills/session | 4.40 | **4.60** | V8-Combined |
| Win rate | **52.84%** | 50.54% | V6 |
| PF | **2.009** | 1.892 | V6 |
| Net points | **+72.04** | +60.16 | V6 |
| Sizing-proxy P&L | **+Rs 36,234** | +Rs 29,890 | V6 |
| Max drawdown | 7.10 | **6.00** | V8-Combined |
| Train PF | **2.159** | 1.983 | V6 |
| Test PF | 1.356 | **1.494** | V8-Combined |

At 5 bps:

| Metric | V6 | V8-Combined | Better |
|---|---:|---:|---|
| PF | **2.425** | 2.341 | V6 |
| Net points | **+89.64** | +78.56 | V6 |
| Sizing-proxy P&L | **+Rs 44,768** | +Rs 38,788 | V6 |
| Max drawdown | 5.60 | **4.02** | V8-Combined |
| Test PF | 1.633 | **1.848** | V8-Combined |

**V6 wins full-window profitability. V8-Combined has more fills, lower drawdown
and better held-out PF.** That pattern — V8 giving up in-sample edge and buying
back robustness — is the whole point of the harsher execution model.

This is **not** an exact execution-parity comparison: V6 uses legacy
independent-trade, exact-trigger and longer-lived order assumptions, whereas
V8-Combined uses S+5 expiry, adverse gap fills, brackets from the actual modelled
fill, same-session paths, and one global duplicate/capital ledger.

**Per-leg V8-Combined**

| Leg | Fills | Win % | PF | Net points |
|---|---:|---:|---:|---:|
| 09:25 LONG | 45 | 55.56% | 1.910 | +10.085 |
| 09:25 SHORT | 51 | 43.14% | 1.942 | +17.256 |
| 09:30 LONG | 8 | 62.50% | 3.904 | +8.250 |
| 09:30 SHORT | 21 | 52.38% | 1.515 | +5.387 |
| 09:35 LONG | 17 | 52.94% | 1.103 | +0.847 |
| 09:35 SHORT | 6 | 66.67% | 4.941 | +9.084 |
| 09:40 LONG | 13 | 38.46% | 2.099 | +4.948 |
| 09:40 SHORT | 14 | 42.86% | 1.129 | +1.086 |
| 09:45 LONG | 4 | 50.00% | 1.672 | +0.803 |
| 09:45 SHORT | 5 | 80.00% | 19.872 | +2.416 |

Late-slot PF values rest on 4–6 fills and are **not** stable edge.

**Verification artefacts**

- Run: `…\v8_combined_best_per_leg_v1\runs\fno_v8_vc_20260820T174309351502+0530_af9cdf2ca31b`
- Fingerprint: `af9cdf2ca31b830de32a3640bcf8fa0e4bb98c60da18c262ecfdd70da041ec53`
- 5 bps run: `…_20260820T193555446060+0530_5f0ee2861cde`
- 5 bps fingerprint: `5f0ee2861cde72ea8e2f987fe85bbb11bac3c559e4cab4db17c4de884456e13c`
- Focused V8 launcher/engine/data-contract tests: **92 passed**
- Official headline: **N/A** — 3,538 of 8,320 expected symbol-sessions incomplete, last-real-bar sensitivity in use. **Not promotion eligible.**

---

## 10. Commands

```powershell
# Build a cache (an explicit source-snapshot manifest is mandatory)
python fno_v8_windowed_1m_entry_backtest.py build-cache `
  --source-snapshot <manifest.json> `
  --from-day 2026-05-27 --through-day 2026-07-31

# Run B4 at the conservative 15-bps assumption
python fno_v8_windowed_1m_entry_backtest.py run `
  --source-snapshot <manifest.json> `
  --from-day 2026-05-27 --through-day 2026-07-31 `
  --variant B4 --cost-bps 15 `
  --eod-policy EXACT_SQUARE_OFF --square-off 15:30

# Fixed two-day source/chronology smoke test
python fno_v8_windowed_1m_entry_backtest.py smoke --source-snapshot <manifest.json>

# Validate an immutable run
python fno_v8_windowed_1m_entry_backtest.py validate --provenance <run-directory>\provenance.json
```

> `LAST_REAL_BAR_SENSITIVITY` is available **only** as an explicitly named
> sensitivity. It must not be presented as an exact 15:30 result.

---

## 11. Why V8 is not promotion eligible

The available historical inputs do **not** yet provide:

1. daily **point-in-time F&O universe membership** for the full history;
2. **point-in-time rolling near-month futures OI** (the prototype uses a static
   26AUG contract for OI only);
3. certified **one-minute row-lineage flags** in the legacy cash files;
4. complete **15:30 bars** for every recent session;
5. prospective evidence of at least **20 new sessions and 100 fills**.

The current prototype therefore executes NSE cash equity and uses a static 26AUG
future only for OI. That is useful for **entry-logic research**, not a claim of
historical rolling-futures performance.

Add to that the fitting caveats: four legs were retuned on the full 57-session
window with **no holdout**, and never jointly ledgered during fitting; and
V8-Combined's per-leg mapping was selected on TRAIN.

---

## 12. Planned research sequence

1. Obtain daily point-in-time universes and rolling near-month futures/OI.
2. Repair or refetch exact 15:30 cash rows and certify one-minute lineage.
3. Rebuild **B0** on the clean data contract.
4. Compare **B1** windows separately for LONG and SHORT.
5. Test buffers **0 / 2 / 5 bps** at 15-bps cost.
6. Add midpoint, close-location, volume, and range filters **one at a time**.
7. Freeze the winning rule and begin prospective shadow collection.
8. Consider promotion only after every criterion in the research plan passes.

Steps 5–6 are what became the **V10** experiment ladder — see
[FNO_V10_MAX050_GAP2_BACKTEST_STRATEGY.md](FNO_V10_MAX050_GAP2_BACKTEST_STRATEGY.md).

---

# Appendix A — Complete indicator and parameter reference

Everything below is transcribed from
[fno_v8_windowed_1m_entry_backtest.py](fno_v8_windowed_1m_entry_backtest.py).

## A.1 Indicator definitions

`add_five_minute_features()` — byte-identical logic to the V6 live data layer:

```python
out = frame.sort_values("ts", kind="stable").reset_index(drop=True)

for span in (9, 20, 50):
    out[f"ema{span}"] = out["close"].ewm(span=span, adjust=False).mean()

out["prev_close"]       = out["close"].shift(1)
out["price_change_pct"] = (out["close"] / out["prev_close"] - 1.0) * 100.0

prior_volume            = out["volume"].shift(1).rolling(20, min_periods=5).mean()
out["volume_ratio"]     = out["volume"].div(prior_volume.where(prior_volume.gt(0)))

out["traded_value"]     = out["close"] * out["volume"]
```

| Indicator | Exact definition | Notes |
|---|---|---|
| `ema9/20/50` | `close.ewm(span=n, adjust=False).mean()`, `α = 2/(n+1)` | recursive, seeded at bar 0, **no warm-up guard**, **continuous across sessions** |
| `price_change_pct` | `(close / close.shift(1) − 1) × 100` | previous **5-minute** bar |
| `volume_ratio` | `volume ÷ volume.shift(1).rolling(20, min_periods=5).mean()` | denominator masked to `> 0`, else NaN |
| `traded_value` | `close × volume` | Rs |
| `oi_change_pct` | `(oi / prev_oi − 1) × 100` where `oi > 0 AND prev_oi > 0 AND finite` | 5-minute OI delta from the **futures** series |

**The 20-bar volume window spans the session boundary.** Twenty five-minute bars
is ~100 minutes; at the 09:25 slot only 09:20 belongs to the current session, so
19 of the 20 denominator bars are the previous session's late afternoon. A
`volume_ratio ≥ 3.0` filter at 09:25 therefore means "three times yesterday's
late-session per-bar average", which is the intended reading but not the one a
casual look at the formula suggests.

`join_cash_features_with_futures_oi()` merges on `ts` with
`how="inner", validate="one_to_one"` and admits only rows where the futures side
carries `oi_valid`, stamping
`data_contract = "NSE_CASH_1M_TO_5M_PRICE_VOLUME_PLUS_STATIC_NFO_5M_OI_V1"`.

## A.2 Bar construction and validity

### A.2.1 1m → 5m aggregation (`aggregate_equity_one_minute_to_five_minute`)

```python
offset      = (ts − session_open_0915).total_seconds() / 60          # kept if 1..375
slot_number = ((offset − 1) // 5) + 1
slot_end    = session_open_0915 + slot_number × 5 minutes

agg: open=first, high=max, low=min, close=last, volume=sum,
     source_1m_count=size, source_1m_first=first(ts), source_1m_last=last(ts)

keep only if:  source_1m_count == 5
           and source_1m_first == slot_end − 4 min
           and source_1m_last  == slot_end
```

Timestamps must additionally be exact minute-end labels
(`second == 0 AND microsecond == 0 AND nanosecond == 0`).

### A.2.2 Minute-row validity (`_valid_minute_rows`)

```
finite(open, high, low, close) AND all > 0
high >= max(open, close)
low  <= min(open, close)
high >= low
finite(volume) AND volume >= 0
NOT gap_filled  AND  NOT opening_snapshot  AND  NOT provisional_stale
```

### A.2.3 `MinuteBar` validity in the state machine (`_valid_bar`)

The same predicate, applied per bar object, plus `not bar.gap_filled`. A bar
failing it inside the S+1…S+5 window sends the candidate to `DATA_INCOMPLETE`
with `INVALID_ENTRY_WINDOW_BAR`; an absent bar gives `MISSING_ENTRY_WINDOW_BAR`.

### A.2.4 Required futures clocks

`required_futures_signal_clocks()` derives, from the literal setup book, every
signal clock **and its exact t−5 predecessor**, because an OI change is causal
only when both rows exist:

```
09:20, 09:25, 09:30, 09:35, 09:40, 09:45      (six clocks)
```

Every selected symbol must have all six on every expected session, plus the
exact 09:16–15:30 one-minute cash grid, or the headline fails closed.

`load_futures_five_minute_history()` additionally requires the columns
`timestamp, oi, quality_state, tradingsymbol, instrument_token, expiry,
contract_month`, and asserts that the file contains **exactly one**
tradingsymbol matching the request — an identity mismatch raises.

## A.3 `V8Setup` — the leg dataclass

Sixteen fields. The first twelve are the five-minute/bracket authority; the last
four are optional per-leg overrides of the one-minute entry seam.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `signal_end` | str | — | 5-minute signal bar end |
| `side` | str | — | `LONG` / `SHORT` |
| `max_entries` | int | — | per-slot cap |
| `picker` | str | — | `max_oi` / `max_volume` / `max_move` / `max_liquidity` |
| `price_change_pct` | float | — | magnitude; sign applied by side |
| `oi_change_pct` | float | — | minimum 5-minute OI change % |
| `volume_ratio` | float | — | minimum volume ratio |
| `body_ratio` | float | — | minimum confirmation body ratio |
| `max_wick_ratio` | float | — | maximum adverse wick ratio |
| `min_traded_value` | float | — | minimum Rs traded value |
| `stop_pct` | float | — | stop distance % from **fill** |
| `target_pct` | float | — | target distance % from **fill** |
| `entry_conf_minute` | int \| None | `None` | override `max_confirmation_minute` |
| `entry_buffer_bps` | float \| None | `None` | override `buffer_bps` |
| `entry_midpoint` | bool \| None | `None` | override `midpoint_invalidation` |
| `entry_clv` | float \| str \| None | `ENTRY_INHERIT` | override `close_location_min` |

`entry_clv` needs the dedicated `ENTRY_INHERIT` sentinel because `None` is a
meaningful value there — "no close-location floor at all" — and must be
distinguishable from "inherit the run policy".

`overrides_entry_policy` is true when any of the four is set.

### A.3.1 The module's `ACTIVE_SETUPS` (setup-book SHA-256 `ed329371…016fb6`)

| # | Signal | Side | Cap | Picker | Price % | OI % | Vol | Body ≥ | Wick ≤ | Min TV | Stop % | Tgt % | R:R | Entry overrides |
|---:|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 09:25 | LONG | 4 | max_move | 0.30 | 0.10 | 3.0 | 0.00 | 0.50 | 0 | 0.40 | 1.0 | 1:2.5 | conf ≤ S+3, buffer 0, midpoint off, CLV none |
| 2 | 09:25 | SHORT | 4 | max_move | 0.20 | 0.10 | 1.5 | 0.60 | 0.60 | Rs 2.5 cr | 0.50 | 3.0 | 1:6.0 | conf ≤ S+3, buffer 2 bps, midpoint off, CLV none |
| 3 | 09:30 | LONG | 1 | max_move | 0.65 | 0.10 | 1.0 | 0.50 | 0.50 | 0 | 1.00 | 2.5 | 1:2.5 | *inherits* |
| 4 | 09:30 | SHORT | 4 | max_volume | 0.20 | 1.00 | 1.0 | 0.45 | 0.30 | Rs 2.5 cr | 1.00 | 4.0 | 1:4.0 | conf ≤ S+3, buffer 0, **midpoint ON**, **CLV ≥ 0.50** |
| 5 | 09:35 | LONG | 1 | max_liquidity | 0.20 | 0.10 | 1.0 | 0.60 | 0.50 | 0 | 1.00 | 2.5 | 1:2.5 | *inherits* |
| 6 | 09:35 | SHORT | 2 | max_liquidity | 0.50 | 1.00 | 1.0 | 0.40 | 0.50 | 0 | 1.00 | 3.0 | 1:3.0 | *inherits* |
| 7 | 09:40 | LONG | 1 | max_liquidity | 0.20 | 0.10 | 2.0 | 0.50 | 0.50 | 0 | 0.50 | 2.5 | 1:5.0 | *inherits* |
| 8 | 09:40 | SHORT | 4 | max_volume | 0.20 | 0.75 | 1.0 | 0.00 | 0.20 | 0 | 1.00 | 4.0 | 1:4.0 | conf ≤ S+4, buffer 0, midpoint off, **CLV ≥ 0.50** |
| 9 | 09:45 | LONG | 1 | max_move | 0.65 | 0.10 | 1.0 | 0.40 | 0.50 | 0 | 1.00 | 3.0 | 1:3.0 | *inherits* |
| 10 | 09:45 | SHORT | 1 | max_volume | 0.20 | 0.75 | 1.0 | 0.40 | 0.30 | 0 | 1.00 | 2.0 | 1:2.0 | *inherits* |

Cap total: **12 LONG + 16 SHORT = 28** theoretical orders per session, before the
global ledger clamps concurrency to 12.

Legs 1 and 8 set `body_ratio = 0.00`, i.e. **no body filter at all** — direction,
the beyond-close test and the wick ceiling do all the morphology work there.
Legs 2 and 4 are the only ones with a liquidity floor (Rs 2.5 crore traded
value).

> **Two different "V8 books" exist — do not confuse them.**
>
> | Book | SHA-256 | 09:40 SHORT |
> |---|---|---|
> | Module `ACTIVE_SETUPS` (above) | `ed329371…016fb6` | Retuned V8: cap 4, max_volume, OI 0.75, body 0.00, wick 0.20, target 4.0, conf ≤ S+4, CLV 0.50 |
> | **V8-Combined best-per-leg** | `ee97e86d…d90ee675` | V8-Strict: cap 1, max_move, OI 0.10, body 0.40, wick 0.50, target 3.0, **no overrides** |
>
> The two books are identical in all nine other legs. The V8-Combined book —
> `ee97e86d…` — is the one the V10 launcher loads and the one every V10 cache
> manifest is verified against.

## A.4 `EntryPolicy` — the run-level entry seam

| Field | Default | Overridable per leg | Meaning |
|---|---|---|---|
| `buffer_bps` | 2.0 | ✅ | trigger buffer beyond the confirmation extreme |
| `max_confirmation_minute` | 4 | ✅ | last minute a strict confirmation may latch |
| `entry_expiry_minute` | 5 | ❌ | last minute a pending order may fill |
| `close_location_min` | `None` | ✅ | directional close-location floor |
| `cost_bps` | 5.0 | ❌ | run economics |
| `slippage_bps` | 0.0 | ❌ | run economics |
| `midpoint_invalidation` | `True` | ✅ | pre-confirmation midpoint kill |
| `post_confirmation_cancel` | `True` | ❌ | close-back-through-signal-close kill |
| `allow_cap_reassignment` | `True` | ❌ | freed cap slots are reusable |
| `same_bar_policy` | `"STOP_FIRST"` | ❌ | only value accepted |
| `square_off` | `None` | ❌ | `HH:MM` |
| `eod_policy` | `"LAST_REAL_BAR_SENSITIVITY"` | ❌ | or `EXACT_SQUARE_OFF` |

`validate()` raises on: non-integer minute fields; `max_confirmation_minute < 1`;
`entry_expiry_minute <= max_confirmation_minute`; non-finite or negative
buffer/cost/slippage; buffer or slippage ≥ 10,000 bps; `close_location_min`
outside [0, 1]; any `same_bar_policy` other than `STOP_FIRST`; an unknown
`eod_policy`; `EXACT_SQUARE_OFF` without a `square_off`; and a `square_off`
later than 15:30.

`validate_backtest_policy()` adds one more: `square_off` must be **later than
the latest leg's S+5 bar**, computed from the live setup book (09:45 + 5 min +
5 min = 09:55), independent of whether the run has any candidates.

`policy_for_setup()` applies a leg's overrides with `dataclasses.replace` and
re-validates. Legs that override nothing return the base policy object
unchanged, so a run using the frozen book behaves exactly as it did before
per-setup overrides existed.

## A.5 `PortfolioPolicy`

| Field | Value | Enforcement |
|---|---:|---|
| `capital_rs` | 120,000.0 | must be finite and > 0 |
| `margin_per_entry_rs` | 10,000.0 | must be finite and > 0 |
| `target_exposure_per_entry_rs` | 50,000.0 | must be finite and > 0 |
| `max_concurrent_positions` | 12 | must be a positive non-bool integer |
| `pending_reserves_margin` | `True` | **only `True` supported** — else `ValueError` |
| `one_position_per_symbol` | `True` | **only `True` supported** — else `ValueError` |

```
capacity = min(max_concurrent_positions, floor(capital_rs / margin_per_entry_rs))
         = min(12, 12) = 12
```

Action ordering key:
`(event_ts, phase, signal_time, setup_id, frozen_rank, symbol, candidate_id, sequence)`
with `phase = 0` for RELEASE and `2` for RESERVE, so releases at a timestamp are
always processed before reservations at that timestamp.

`PORTFOLIO_MODE` is stamped as
`GLOBAL_PENDING_MARGIN_AND_DUPLICATE_RESERVATION_CONSERVATIVE_NO_BACKFILL_V1`.

## A.6 Confirmation morphology — exact formulas

```
range          = high − low                              (> 0 required)
body_ratio     = |close − open| / range
adverse_wick   = LONG  : (high − max(open, close)) / range
                 SHORT : (min(open, close) − low)  / range
close_location = LONG  : (close − low)  / range
                 SHORT : (high − close) / range
```

Comparisons, all with a `1e-12` epsilon so float noise cannot flip a decision:

```
body_ratio     + 1e-12 <  setup.body_ratio          -> BODY_RATIO_BELOW_MINIMUM
adverse_wick   − 1e-12 >  setup.max_wick_ratio      -> ADVERSE_WICK_RATIO_ABOVE_MAXIMUM
close_location + 1e-12 <  policy.close_location_min -> CLOSE_LOCATION_BELOW_MINIMUM
```

Rejection codes are appended in **rule-evaluation order**, and every attempted
candle is retained in `confirmation_checks` — including ones that failed — so
the audit shows why each minute was rejected, not just that it was.

## A.7 Trigger, fill, brackets, exits

### A.7.1 Tick rounding

```python
def round_up_to_tick(value, tick):     # ROUND_CEILING on Decimal
    return float((Decimal(str(value)) / Decimal(str(tick))).to_integral_value(ROUND_CEILING) * Decimal(str(tick)))

def round_down_to_tick(value, tick):   # ROUND_FLOOR on Decimal
    ...
```

`tick_size` must be `> 0` or `ValueError` is raised — no silent fallback. It
comes from the universe row's `equity_tick_size`; the cache builder sets
`lot_size = 1` for every candidate, so V8 never lot-rounds.

### A.7.2 Trigger

```python
scale = policy.buffer_bps / 10_000
LONG : round_up_to_tick  (bar.high × (1 + scale), tick)
SHORT: round_down_to_tick(bar.low  × (1 − scale), tick)
```

Directional rounding means a buffer always makes the trigger **strictly harder**
to reach — it is a noise filter, never an accidental improvement.

### A.7.3 Fill

```python
LONG :  bar.open >= trigger  ->  round_up_to_tick(bar.open × (1 + slip), tick),  gap_fill=True
        bar.high >= trigger  ->  round_up_to_tick(trigger  × (1 + slip), tick),  gap_fill=False
SHORT:  bar.open <= trigger  ->  round_down_to_tick(bar.open × (1 − slip), tick), gap_fill=True
        bar.low  <= trigger  ->  round_down_to_tick(trigger  × (1 − slip), tick), gap_fill=False
otherwise: None
```

Eligibility guard: `if runtime.order_placed_at is None or ts <= runtime.order_placed_at: continue`
— **the confirmation candle can never fill itself**, and neither can the candle
on which the cap was reserved.

### A.7.4 Brackets, from the actual fill

```python
LONG : stop   = round_down_to_tick(entry × (1 − stop_pct/100),   tick)
       target = round_down_to_tick(entry × (1 + target_pct/100), tick)
SHORT: stop   = round_up_to_tick  (entry × (1 + stop_pct/100),   tick)
       target = round_up_to_tick  (entry × (1 − target_pct/100), tick)
```

Every rounding direction is chosen conservatively: stops end up nearer to entry,
targets further away.

### A.7.5 Exit precedence (`_exit_on_bar`, LONG shown)

| Order | Condition | Reason | Exit price |
|---:|---|---|---|
| 1 | `open <= stop` (open-at-bar-start only) | `STOP_GAP` | `round_down_to_tick(open)` — worse than the stop |
| 2 | `open >= target` (open-at-bar-start only) | `TARGET` | the target |
| 3 | `low <= stop` | `STOP` | the stop |
| 4 | `high >= target` | `TARGET` | the target |

SHORT mirrors all four. A bar on which the position *entered* is re-checked with
`position_open_at_bar_start=False`, so checks 1–2 are skipped and
`ambiguous_entry_bar` is flagged if it exits immediately.
`_exit_occurs_at_bar_open` separately records whether the exit was deterministic
at the open.

### A.7.6 Returns

```python
gross = (exit / entry − 1) × 100      if LONG
      = (1 − exit / entry) × 100      if SHORT
net   = gross − cost_bps / 100
```

Terminal state is `STOPPED` when the reason starts with `STOP` (so `STOP_GAP`
counts as stopped), `TARGETED` for `TARGET`, else `SQUARE_OFF`.

## A.8 Rupee sizing and P&L

Applied after the state machine, in `run_v8_backtest`:

```python
quantity            = floor(target_exposure_per_entry_rs / entry_price)   # filled rows only
position_notional_rs = entry_price × quantity

direction  = +1 if LONG else −1
gross_pnl_rs = direction × (exit_price − entry_price) × quantity
estimated_cost_rs = entry_price × quantity × cost_bps / 10_000
net_pnl_rs = gross_pnl_rs − estimated_cost_rs
```

Note that the rupee cost is charged on **entry notional**, while
`net_return_pct` subtracts `cost_bps / 100` from the **percentage** return —
the two are equivalent to within the rounding of `quantity`.

Every pre-ledger execution field is mirrored into an `unconstrained_*` column
before the portfolio overlay runs, so the ledger's cost is always measurable:
`unconstrained_status`, `unconstrained_net_return_pct`, `unconstrained_net_pnl_rs`,
plus entry/exit/quantity/notional/excursion mirrors.

## A.9 Variant registry and derived policies

| Variant | `max_confirmation_minute` | `buffer_bps` | `midpoint_invalidation` | `close_location_min` |
|---|---:|---:|---|---|
| B0 | 1 | 0.0 | `False` | `None` |
| B1 | 4 | 0.0 | `False` | `None` |
| B2 | 4 | 2.0 | `False` | `None` |
| B3 | 4 | 5.0 | `False` | `None` |
| B4 | 4 | 2.0 | `True` | `None` |
| B5 | 4 | 2.0 | `True` | 0.75 |

`entry_expiry_minute = 5` in every case. Because four legs carry overrides, the
**effective** policy differs per leg; for a B0 run the resolved table is:

| Leg | conf ≤ | buffer bps | midpoint | CLV |
|---|---:|---:|---|---|
| 09:25 LONG | S+3 | 0.0 | off | none |
| 09:25 SHORT | S+3 | 2.0 | off | none |
| 09:30 LONG | S+1 | 0.0 | off | none |
| 09:30 SHORT | S+3 | 0.0 | **on** | **0.50** |
| 09:35 LONG | S+1 | 0.0 | off | none |
| 09:35 SHORT | S+1 | 0.0 | off | none |
| 09:40 LONG | S+1 | 0.0 | off | none |
| 09:40 SHORT | S+4 | 0.0 | off | **0.50** |
| 09:45 LONG | S+1 | 0.0 | off | none |
| 09:45 SHORT | S+1 | 0.0 | off | none |

## A.10 Invalidation rules

```python
midpoint = (five_min_high + five_min_low) / 2

_preconfirmation_invalidated  = close < midpoint          if LONG else close > midpoint
_postconfirmation_invalidated = close < five_min_close     if LONG else close > five_min_close
```

Pre-confirmation invalidation applies only while `MONITORING` and only when the
policy enables it; post-confirmation cancellation applies to `PENDING_STOP` and
`CONFIRMED_WAITING_CAP`, and is evaluated **after** fill processing so a fill
this minute beats a cancellation this minute.

## A.11 Cache key and identity fields

The cache key fingerprint covers: V8 module source SHA-256, setup-book SHA-256,
data/path policy versions, universe hashes, source-snapshot fingerprint,
source-inventory fingerprint, official-calendar hash and expected sessions, the
date window, and the selected symbol set. A run fingerprint adds variant,
timing, cost, slippage, EOD and portfolio policies.

`_assert_cache_matches_active_strategy()` refuses any cache whose
`setup_book_sha256` differs from the live module's, so a stale book cannot be
replayed by accident.

`MODULE_IMPORT_SOURCE_SHA256` is computed at import time from the file's own
bytes, which is what makes the source hash part of the cache key self-updating.
