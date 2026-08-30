# FnO V11 — Locked Stage-10 Full-History Backtest (Complete Reference)

```powershell
cd "C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2"
python -u fno_v11_backtest.py run --all-usable-history
```

**Profile ID:** `V11_S10_POST_HOC_TOP2_1436C7D363` ·
**Stage:** `STAGE_10_FIXED_STANDALONE` · **Family:** `LOCKED_STAGE10_COMPOSITE` ·
**Schema:** `fno_v11_stage10_locked_backtest_v1` ·
**Profile SHA-256:** `8dfc162701705c0daa89d7ba2faa8dd7ddd3ff8eb6605370d96de1fdaa1f6fe1`

`research_only = True` · `promotion_eligible = False` ·
`live_or_paper_authority = False` · `headline_valid = False`

| | |
|---|---|
| Launcher | [fno_v11_backtest.py](fno_v11_backtest.py) |
| Execution runtime | [fno_v11_execution_runtime.py](fno_v11_execution_runtime.py) |
| Gap runtime | [fno_v11_gap_runtime.py](fno_v11_gap_runtime.py) |
| Staged research runner | [fno_v11_staged_backtest.py](fno_v11_staged_backtest.py) |
| Selection variant registry | [fno_v11_variant_registry.py](fno_v11_variant_registry.py) |
| V10 base profile | [fno_v10_backtest_config.py](fno_v10_backtest_config.py) |
| History loader | [fno_v10_backtest.py](fno_v10_backtest.py) |
| **Execution engine** | [fno_v8_windowed_1m_entry_backtest.py](fno_v8_windowed_1m_entry_backtest.py) |
| Output root | `…\fno_oi\strategy_research\v11_stage10_fixed_full_history_v1\` |

---

## 1. What this command is

This is the **direct, single-strategy entry point** for the best observed V11
research configuration. Unlike the staged runner, it does **not** import the
experiment ladder and does **not** discover or rank variants at runtime. The
strategy is fixed, and every rule is asserted before a single row loads.

```
   V8-Combined ten-leg book  (setup_book_sha256 = ee97e86d…d90ee675)
              │
              ├─ V10 STAGE 7        09:40_LONG  price_change_pct >= 0.40 %
              │
              ├─ V10 .50 CEILING    09:35_LONG  price_change_pct <= 0.50 %
              │                      …then rerank within (session, setup)
              │
              ├─ V11 GAP GUARD      max 2 bps adverse gap, with a
              │                      STRONG-REFERENCE identity registry
              │
              ├─ V11 STAGE 4        09:30_SHORT pending stop cannot fill
              │                      before setup-relative minute S+3
              │
              └─ V11 STAGE 9        at most TWO concurrent same-side
                                     reservations per symbol; opposite-side
                                     for the same symbol stays prohibited
              │
              ▼
      V11_S10_POST_HOC_TOP2_1436C7D363
```

Everything else — the five-minute selection gates, one-minute confirmation
morphology, stop-entry trigger, S+5 expiry, brackets from the actual fill, the
stop-first same-bar rule, and the rupee cost model — is unchanged from the V8
engine as configured by V10.

### The four things V11 changes, and why

| # | Change | Type | Motivation |
|---|---|---|---|
| 1 | Strong-reference gap identity | **bug fix** | The V10 guard tracked rejected candidates by `id()` in a plain `set[int]`. CPython recycles object ids, so a later unrelated candidate could be falsely cancelled. |
| 2 | 09:30 SHORT no fill before S+3 | entry timing | The leg's earliest fills were the worst; delaying the pending stop removes them. |
| 3 | Same-symbol same-side limit 2 | portfolio | V10 allowed one position per symbol, period. This permits a second **same-side** reservation while still refusing an opposite-side one. |
| 4 | Nothing else | — | `exit_rule` is `None`; no exit mechanism is active in Stage 10. |

> **The honest framing, stated by the profile itself:** `post_hoc_origin: True`.
> Stage 10 is a **post-hoc combination of the two best-scoring isolated
> experiments** (Stage 9 and Stage 4), selected by looking at the same 65
> sessions it is then measured on. See §11.

---

## 2. The bug fix — strong-reference gap identity

This is the only change in V11 that is a *correctness* fix rather than a
strategy hypothesis, and it is worth understanding precisely.

### 2.1 What V10 did

[fno_v10_gap_guard_research.py](fno_v10_gap_guard_research.py):

```python
rejected_candidates: set[int] = set()
...
rejected_candidates.add(id(runtime.candidate))       # line 221
...
if id(candidate) in rejected_candidates:             # line 229
    return True                                       # -> force cancellation
```

The set holds **integers**, not references. Once the original `CandidateInput`
is garbage-collected, CPython is free to allocate a new object at the same
address — and `id()` returns the address. A later, entirely unrelated candidate
can therefore hash to a retained id and be **falsely cancelled**.

### 2.2 What V11 does

[fno_v11_gap_runtime.py](fno_v11_gap_runtime.py):

```python
class _StrongIdentityRegistry:
    """Identity set that prevents id reuse by retaining the original object."""

    def __init__(self) -> None:
        self._objects: dict[int, Any] = {}

    def add(self, value: Any) -> None:
        self._objects[id(value)] = value        # holds a STRONG reference

    def contains(self, value: Any) -> bool:
        return self._objects.get(id(value)) is value    # identity, not equality
```

Retaining the object makes the id un-recyclable for the run's lifetime, and the
`is` check confirms identity rather than trusting the integer. The module
docstring states the intent directly:

> The legacy V10 research guard intentionally remains untouched so Stage 0 can
> retain exact parity with its pinned artifact.

So V10's pinned artifact is deliberately *not* retro-fixed — the bug is
preserved there for reproducibility, and corrected only forward in V11.

### 2.3 What it does *not* explain

V11 records **24 guard rejections** where V10 recorded 14. It is tempting to
attribute that entirely to the fix, and that would be wrong. The Stage-4 and
Stage-9 mechanisms change which candidates ever reach a fill attempt, so more
candidates arrive at the gap guard in the first place. The identity fix removes
*false* cancellations; the count difference is dominated by upstream flow
changes.

Two audit columns record the guard's own state: `v11_gap_identity_policy`
(`STRONG_REFERENCE_AND_IS_CHECK`) and `v11_gap_rejected_identity_count`.

---

## 3. Stage 4 — 09:30 SHORT cannot fill before S+3

`installed_runtime_hooks()` wraps `engine._entry_fill`:

```python
def v11_entry_fill(setup, runtime, bar, policy):
    if setup.setup_id == spec.entry_setup_id:                 # "09:30_SHORT"
        relative_minute = engine._relative_minute(runtime.candidate, bar.ts)
        runtime._v11_entry_not_before_minute = spec.entry_not_before_minute
        if relative_minute < int(spec.entry_not_before_minute or 0):    # < 3
            runtime._v11_early_fill_checks_skipped += 1
            neutral_fill = original_entry_fill(setup, runtime, bar, policy)
            if neutral_fill is not None:
                runtime._v11_early_touch_observed = True     # recorded, not taken
            return None
    return original_entry_fill(setup, runtime, bar, policy)
```

Two details matter:

1. **The neutral fill is still evaluated**, purely so the run can record that a
   fill *would* have happened (`v11_early_touch_observed`). The suppression is
   observable in the audit rather than silent.
2. **Only the pending stop is delayed.** Confirmation still latches at its
   normal minute; the order simply cannot fill on S+2.

Since the 09:30 SHORT leg's resolved confirmation window is S+3 under V10's
B0-style global policy with the leg's own override, this rule bites the
*fill*, not the confirmation.

**Observed effect** — filled `entry_minute` distribution for 09:30 SHORT:

| entry_minute | S+2 | S+3 | S+4 | S+5 |
|---|---:|---:|---:|---:|
| 09:30 SHORT fills | **0** | 9 | 9 | 1 |

Exactly as specified: no S+2 fills survive. Across 101 09:30-SHORT candidates,
**24** had at least one early fill check skipped and **18** had an early touch
that was observed and suppressed.

`entry_not_before_minute` is validated into **[2, 5]**; anything outside raises.

---

## 4. Stage 9 — same-symbol, same-side maximum 2

`apply_same_side_symbol_limit()` replaces the engine's global portfolio ledger.
It replays the same chronological RESERVE/RELEASE action stream, but tracks
sides per symbol:

```python
symbol_ids   = active_by_symbol.get(symbol, set())
symbol_sides = {active[item][1] for item in symbol_ids}

if symbol_sides and symbol_sides != {side}:
    reject "DUPLICATE_SYMBOL_OPPOSITE_SIDE_PENDING_OR_OPEN"
if len(symbol_ids) >= same_side_limit:          # 2
    reject "DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2"
if len(active) >= capacity:                     # 12
    reject "CAPITAL_MARGIN_OR_CONCURRENCY_LIMIT"
```

| Rule | V10 | V11 Stage 10 |
|---|---|---|
| Same symbol, same side | 1 max | **2 max** |
| Same symbol, opposite side | prohibited | **still prohibited** |
| Global concurrency | 12 | 12 |
| Backfill after rejection | none | none (same conservative rule) |

Guard rails: the adapter raises unless `same_side_limit == 2`, unless
`pending_reserves_margin` is `True`, and unless `one_position_per_symbol` is
`True` — the parent flag is retained as *policy authority* even though the
limit is now 2.

Three extra audit columns are written: `v11_same_side_symbol_limit`,
`v11_opposite_side_same_symbol_prohibited` (always `True`), and
`v11_max_symbol_target_exposure_rs` (`2 × 50,000 = Rs 100,000` — the most any
single symbol can carry).

**Observed effect:** `DUPLICATE_REJECTED` fell from **28** candidates under V10
to **3** under V11 — and the 3 that remain are rejected with
`DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2:CONSERVATIVE_NO_BACKFILL`, i.e. a third
same-side attempt on a symbol that already had two.

---

## 5. Inherited: selection, gap threshold, history

Everything in this section is loaded from the V10 modules unchanged.

### 5.1 Selection overlay

`filters.selection_overlay(candidates, SPEC_BY_NAME["0935_LONG_MOVE_MAX_050"])`:

```python
stage7_rejected  = (setup_id == "09:40_LONG") & (move + 1e-12 <  0.40)
                                  -> "STAGE7_0940_LONG_MOVE_BELOW_040"
ceiling_rejected = passed & (setup_id == "09:35_LONG") & (move - 1e-12 > 0.50)
                                  -> "0935_LONG_MOVE_ABOVE_CHALLENGER_MAX"
```

then **filter, then rerank** — `frozen_rank` is recomputed from scratch, so a
removed rank-1 candidate promotes rank-2 into the cap. Removing trades also
*substitutes* trades.

Observed on this run: **1,134 PASSED**, 77 rejected by the `.50` ceiling, 30 by
the Stage-7 floor.

### 5.2 The 65-session usable-history contract

Loaded by `v10_backtest._load_all_usable_max050_gap2_history()`:

| Segment | Window | Contract | Sessions |
|---|---|---|---:|
| `AUG_CORE_59` | 2026-05-27 … 2026-08-19 | 26AUG | 59 |
| `AUG_EXTENSION_20_21` | 2026-08-20 … 2026-08-21 | 26AUG | 2 |
| `SEP_ROLLOVER_24_25` | 2026-08-24 … 2026-08-25 | 26SEP | 2 |
| `SEP_DIAGNOSTIC_27` | 2026-08-27 | 26SEP | 1 |
| `SEP_DIAGNOSTIC_28` | 2026-08-28 | 26SEP | 1 |
| **Total** | **2026-05-27 … 2026-08-28** | mixed | **65** |

**2026-08-26 has no validated cache.** The calendar gap is recorded in
`missing_regular_session_dates`, never filled with a flat day.

### 5.3 Cost scenarios

| Scenario | `cost_bps` | `slippage_bps` |
|---|---:|---:|
| `REFERENCE_15_0` | 15.0 | 0.0 |
| `STRESS_20_2` | 20.0 | 2.0 |
| `STRESS_25_5` | 25.0 | 5.0 |

The bare `run --all-usable-history` runs **all three**. `--reference-only`
restricts to `REFERENCE_15_0`.

---

## 6. Four independent fail-closed verification layers

This launcher is unusually strict. Each layer can abort the run on its own.

### Layer 1 — `validate_fixed_contract()`, before any data loads

Asserts, in order:

1. `locked_config.validate_locked_profile()` — the whole V10 Stage-7 chain;
2. `v10_backtest.validate_max050_gap2_contract()` — 65 sessions, no overlap, files present;
3. `filters.validate_registry()`;
4. V10 profile ID == `V10_STAGE7_LOCKED_BACKTEST_20260827`;
5. V10 profile SHA-256 == `f2b32919…0834e59c`;
6. setup-book SHA-256 == `ee97e86d…d90ee675`;
7. V10 active variant == `0940_LONG_MOVE_040`;
8. selection ceiling still `0.50`;
9. gap guard still `2.0` bps and not reject-all;
10. `gap_runtime.IDENTITY_POLICY == "STRONG_REFERENCE_AND_IS_CHECK"`;
11. the runtime spec dict equals exactly
    `{entry_setup_id: "09:30_SHORT", entry_not_before_minute: 3, exit_rule: None,
    exit_activation_r: None, same_side_symbol_limit: 2}`;
12. cost scenarios equal the expected triple;
13. `profile_sha256() == 8dfc1627…aa1f6fe1`.

### Layer 2 — input binding

```python
input_binding = sha256({
    "sessions":           [d.isoformat() for d in sessions],
    "segments":           json_ready(segment_records),
    "selected_sha256":    _frame_content_sha256(selected),
    "minute_paths_sha256":_frame_content_sha256(minute_paths),
})
must equal 24e4da6c580693637bd7ce9c50c618b07d2e8a6a8dfded4498658d8eab113f2b
```

`_frame_content_sha256` hashes columns, dtypes, row count **and** every row via
`pd.util.hash_pandas_object`. It is recomputed at the end of the run and
compared again — `"standalone V11 inputs mutated during execution"` aborts if
the frames changed underneath.

Session count must be **65** and selected candidates exactly **1,134**.

### Layer 3 — metric benchmark

Every field of the `FULL_USABLE` row is compared to a literal pinned
expectation, per scenario, with float fields at **1e-9 absolute tolerance**.

### Layer 4 — closed-trade economic fingerprint

A SHA-256 over 17 canonical columns of every closed trade:

```
candidate_id, setup_id, symbol, side, entry_time, entry_price,
stop_price, target_price, exit_time, exit_price, exit_reason,
gross_return_pct, net_return_pct, quantity,
gross_pnl_rs, estimated_cost_rs, net_pnl_rs
```

| Scenario | Expected fingerprint |
|---|---|
| `REFERENCE_15_0` | `f171f7741aad48b7…143d9833` |
| `STRESS_20_2` | `cc85008deeefcf90…a656eabe` |
| `STRESS_25_5` | `c352beb835ae4035…5ad21afb` |

This is stronger than the metric check: two different trade sets can produce the
same aggregate PF, but not the same per-trade fingerprint.

### Plus: source snapshotting

Fifteen source modules are copied into `run_dir/source/` and re-hashed at the
end (`_validate_sources_unchanged`). A run is reproducible from its own
directory, and a mid-run source edit is detected.

---

## 7. Execution flow

```
 1. validate_fixed_contract()                    layer 1
 2. v10_backtest._load_all_usable_max050_gap2_history()
 3. filters.selection_overlay(...)               Stage7 + .50, then rerank
 4. assert 65 sessions, 1134 selected
 5. _input_binding_sha256(...)                   layer 2
 6. create run_<IST timestamp>/ , snapshot 15 source modules
 7. write all_input_candidates / selected_candidates / selection_decisions /
          resolved_profile / source_segments
 8. experiment.configure_engine("0940_LONG_MOVE_040")
    engine._confirmation_check = _NEUTRAL_CONFIRMATION_CHECK
 9. for each cost scenario:
        policy = _entry_policy_for_variant(..., cost, slippage,
                                           "15:30", LAST_REAL_BAR_SENSITIVITY)
        with execution_runtime.installed_runtime_hooks(FIXED_RUNTIME_SPEC,
                                                       allow_composite=True):
            with gap_runtime.installed_gap_guard(MAX_2_BPS):
                audit = _NEUTRAL_RUN_BACKTEST(selected, minute_paths, policy,
                                              target_exposure_per_entry_rs=50_000)
        validate_full_usable_benchmark(...)      layer 3
        _closed_trade_economic_fingerprint(...)  layer 4
10. re-check input binding and source hashes
11. write metrics / daywise / benchmark_verification / report /
          provenance / artifact_inventory / latest.json
```

Note the **nesting order**: the runtime hooks are installed *outside* the gap
guard, so `v11_entry_fill` wraps `guarded_entry_fill` wraps the neutral
`_entry_fill`. A 09:30 SHORT bar before S+3 is refused by the outer hook and
never reaches the gap guard at all.

---

## 8. Results

Run directory:
`…\v11_stage10_fixed_full_history_v1\run_20260830T213455896360+0530`

### 8.1 All three scenarios, FULL_USABLE

| Scenario | Fills | W–L | Win rate | PF | Net points | Net P&L | Max daily DD |
|---|---:|---:|---:|---:|---:|---:|---:|
| **REFERENCE_15_0** | 237 | 123–114 | **51.90%** | **2.1452** | **+94.6309** | **+Rs 46,783.23** | 8.5674 |
| STRESS_20_2 | 237 | 119–118 | 50.21% | 1.8627 | +77.8776 | +Rs 38,681.92 | 9.8708 |
| STRESS_25_5 | 237 | 114–123 | 48.10% | 1.5657 | +56.8171 | +Rs 28,481.76 | 11.0343 |

**Fill count is identical across all three scenarios.** Slippage changes the
fill *price*, not whether the trigger was touched — so the trade set is fixed
and only its economics move. From reference to the harshest stress, PF falls
from 2.15 to 1.57 and net P&L falls **39%**, while drawdown rises 29%.

### 8.2 Periods, REFERENCE_15_0

| Period | Sessions | Candidates | Fills | W–L | Win rate | PF | Net points | Net P&L | Max DD |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **FULL_USABLE** | 65 | 1,134 | 237 | 123–114 | 51.90% | 2.1452 | +94.63 | +Rs 46,783 | 8.567 |
| CORE_59 | 59 | 1,035 | 215 | 113–102 | 52.56% | 2.1863 | +88.54 | +Rs 43,760 | 8.567 |
| FORWARD_EXTENSION | 6 | 99 | 22 | 10–12 | 45.45% | 1.7612 | +6.09 | +Rs 3,024 | 2.363 |
| FIRST_HALF | 32 | 466 | 100 | 54–46 | 54.00% | 2.2584 | +46.65 | +Rs 23,130 | 8.567 |
| SECOND_HALF | 33 | 668 | 137 | 69–68 | 50.37% | 2.0531 | +47.98 | +Rs 23,653 | 2.727 |
| LAST_14 | 14 | 173 | 41 | 19–22 | 46.34% | 1.8290 | +12.60 | +Rs 6,428 | 2.578 |

The first/second-half split is the most reassuring number on this page: net
points are almost exactly halved (+46.65 vs +47.98) across the two halves, so
the result is not a single-period artifact. Win rate and PF do decay in the
second half, and `LAST_14` is the weakest slice at PF 1.83.

### 8.3 Monthly

| Month | Sessions | Fills | Win rate | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|---:|
| 2026-05 | 2 | 7 | 71.43% | 8.2258 | +8.04 | +Rs 3,977 |
| 2026-06 | 21 | 50 | 44.00% | 1.2867 | +7.01 | +Rs 3,595 |
| **2026-07** | 23 | 120 | 57.50% | **2.8158** | **+63.32** | **+Rs 30,865** |
| 2026-08 | 19 | 60 | 45.00% | 1.7322 | +16.26 | +Rs 8,347 |

**July is still 67% of the whole result** on 35% of the sessions — the same
concentration V8 and V10 show. June remains near-flat across 21 sessions. This
is the single most important caveat on the headline.

### 8.4 Per segment

| Segment | Sessions | Fills | Win rate | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|---:|
| `AUG_CORE_59` | 59 | 215 | 52.56% | 2.1863 | +88.54 | +Rs 43,760 |
| `AUG_EXTENSION_20_21` | 2 | 3 | **0.00%** | **0.000** | −2.36 | −Rs 1,172 |
| `SEP_ROLLOVER_24_25` | 2 | 10 | 60.00% | 3.3754 | +6.36 | +Rs 3,167 |
| `SEP_DIAGNOSTIC_27` | 1 | 6 | 33.33% | 1.3921 | +0.94 | +Rs 477 |
| `SEP_DIAGNOSTIC_28` | 1 | 3 | 66.67% | 3.0764 | +1.15 | +Rs 551 |

The six forward sessions are 22 fills across four disconnected 1–2 day
fragments spanning a contract rollover, one of which lost every trade. **Not a
validation set.**

### 8.5 Per setup leg (REFERENCE_15_0, 237 fills)

| Setup | Fills | Wins | Win % | PF | Net points | Net P&L | vs V10 |
|---|---:|---:|---:|---:|---:|---:|---|
| 09:25 SHORT | 62 | 28 | 45.2% | 2.056 | +22.367 | +Rs 10,998 | unchanged |
| **09:30 SHORT** | 19 | 12 | **63.2%** | **3.386** | +14.746 | +Rs 7,157 | **Stage 4 target** — was 30 fills / PF 1.361 / +Rs 3,075 |
| 09:40 LONG | 18 | 9 | 50.0% | 3.111 | +12.390 | +Rs 5,942 | was 13 fills / PF 4.881 |
| 09:25 LONG | 61 | 32 | 52.5% | 1.684 | +11.010 | +Rs 5,290 | unchanged |
| 09:35 SHORT | 10 | 6 | 60.0% | 3.121 | +9.786 | +Rs 4,914 | was 8 fills / PF 2.469 |
| 09:45 LONG | 9 | 6 | 66.7% | 4.724 | +8.764 | +Rs 4,366 | was 6 fills / PF 4.589 |
| 09:40 SHORT | 18 | 9 | 50.0% | 1.506 | +4.831 | +Rs 2,671 | was 16 fills / PF 1.373 |
| 09:35 LONG | 17 | 9 | 52.9% | 1.505 | +3.742 | +Rs 1,914 | unchanged |
| 09:30 LONG | 11 | 5 | 45.5% | 1.617 | +3.512 | +Rs 1,709 | was 10 fills / PF 1.539 |
| 09:45 SHORT | 12 | 7 | 58.3% | 1.940 | +3.485 | +Rs 1,822 | was 9 fills / PF 1.021 |

**The Stage-4 target did exactly what it was meant to.** 09:30 SHORT went from
**30 fills at PF 1.361 (+Rs 3,075)** to **19 fills at PF 3.386 (+Rs 7,157)** —
it took eleven fewer trades and more than doubled the money. Every leg is
positive, and the two 09:25 legs still supply **123 of 237 fills (52%)**.

### 8.6 Candidate funnel

| Terminal status | Count | Share | V10 |
|---|---:|---:|---:|
| `NO_CONFIRMATION` | 693 | 61.1% | 693 |
| `POSTCONF_CANCELLED` | 106 | 9.3% | 86 |
| `STOPPED` | 100 | 8.8% | 103 |
| `SQUARE_OFF` (last real bar) | 71 | 6.3% | 69 |
| `TARGETED` | 66 | 5.8% | 60 |
| `WINDOW_EXPIRED` | 63 | 5.6% | 63 |
| `PRECONF_INVALIDATED` | 32 | 2.8% | 32 |
| `DUPLICATE_REJECTED` | **3** | 0.3% | **28** |

Reasons behind `POSTCONF_CANCELLED`: 82 `CLOSE_REVERSED_THROUGH_SIGNAL_CLOSE`
plus **24 `ADVERSE_GAP_GUARD_REJECTED`**.

The confirmation gate remains by far the dominant filter — 693 of 1,134
candidates (61%) never confirm, unchanged from V10 because nothing V11 changes
touches confirmation.

### 8.7 Exit mix

| Exit | Trades | Share |
|---|---:|---:|
| `STOP` | 100 | 42.2% |
| `LAST_REAL_BAR_SENSITIVITY` | 71 | 30.0% |
| `TARGET` | 66 | 27.8% |

Nearly a third of fills are still resolved by the **sensitivity** EOD policy
rather than an exact 15:30 bar. That alone disqualifies the headline.

### 8.8 Day extremes

| Best days | Fills | Net points | Net Rs |
|---|---:|---:|---:|
| 2026-07-23 | 11 | +11.842 | +5,478 |
| 2026-07-07 | 8 | +11.500 | +5,603 |
| 2026-06-03 | 7 | +8.216 | +4,025 |

| Worst days | Fills | Net points | Net Rs |
|---|---:|---:|---:|
| 2026-07-09 | 4 | −2.582 | −1,251 |
| 2026-06-09 | 4 | −2.317 | −974 |
| 2026-06-30 | 2 | −2.304 | −1,146 |

Days: **37 positive / 25 negative / 3 flat.** Gap fills surviving the guard: 24.
Guard rejections: 24. `data_incomplete_candidates`: **0** — every one of the
1,134 candidates had a complete S+1…S+5 minute path.

---

## 9. Exact attribution versus V10

From
[reports/strategy_comparisons/FNO_V10_STAGE0_VS_V11_STAGE10_DAYWISE_20260830.md](reports/strategy_comparisons/FNO_V10_STAGE0_VS_V11_STAGE10_DAYWISE_20260830.md):

| Metric | Frozen V10 Stage 0 | V11 Stage 10 | Difference |
|---|---:|---:|---:|
| Sessions | 65 | 65 | 0 |
| Fills | 232 | 237 | **+5** |
| Wins | 116 | 123 | +7 |
| Losses | 116 | 114 | −2 |
| Net points | 73.054423 | 94.630860 | **+21.576437** |
| Modeled net P&L | Rs 36,312.05 | Rs 46,783.23 | **+Rs 10,471.18** |

**Trade-level reconciliation is exact:**

```
218 fills are common and economically unchanged
Stage 9 adds     19 fills worth  +16.481129 points
Stage 4 removes  14 V10 fills worth  -5.095308 points
                 ------------------------------------
                 net              +21.576437 points
```

Stage 10 improved **15 sessions**, worsened **6**, and was economically
identical on **44**. Favourable daily deltas totalled +25.963676 points;
adverse deltas −4.387239.

Monthly deltas: May 0.000000 · June +3.302705 · July +11.949737 · August
+6.323996. Best day 23-Jul at +5.683320; worst 24-Jun at −1.157828; largest
rupee reduction 30-Jun at −Rs 570.86.

> Read that carefully. **The entire +21.58-point lift is explained by 33
> trades** — 19 added, 14 removed — out of 237. The other 218 fills are
> byte-identical to V10. A result that rests on 33 trades chosen by looking at
> this history is exactly the kind that needs forward validation.

---

## 10. Why every output says research-only

`provenance.json` carries six limitation codes:

| Code | Meaning |
|---|---|
| `POST_HOC_CONFIGURATION_REQUIRES_PROSPECTIVE_VALIDATION` | Stage 10 is a combination of the two best isolated experiments, chosen on this history |
| `SOURCE_SLOT_COVERAGE_INCOMPLETE` | the per-symbol panel is incomplete throughout |
| `LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE` | ~30% of exits use the last real bar, not an exact 15:30 bar |
| `STATIC_CONTRACT_UNIVERSES_BY_SEGMENT` | 26AUG for 61 sessions, 26SEP for 4 — not a rolling point-in-time universe |
| `2026_08_26_HAS_NO_VALIDATED_CACHE` | one regular session inside the span is absent |
| `CASH_EQUITY_PATHS_NOT_ACTUAL_ROLLING_FUTURES_EXECUTION` | everything executes NSE cash equity; the future supplies OI only |

And four flags:

```json
"headline_valid": false,
"research_only": true,
"promotion_eligible": false,
"live_or_paper_authority": false
```

---

## 11. The honest summary

1. **Post-hoc composite.** `post_hoc_origin: True`. Stage 10 is Stage 9 + Stage
   4, both selected as the top-scoring isolated experiments on the *same* 65
   sessions. The staged runner deliberately excludes
   `STAGE_10_POST_HOC_COMBINATION` rows when picking a "best isolated" variant,
   which is the right discipline — but the combination still inherits the
   selection.
2. **33 trades carry the entire improvement.** 218 of 237 fills are identical to
   V10.
3. **July concentration persists.** 67% of net from one month on 35% of the
   sessions.
4. **The forward extension is not validation.** 22 fills, four fragments, a
   contract rollover, one all-losing fragment.
5. **Cost sensitivity is material.** PF 2.15 → 1.57 and −39% P&L from reference
   to the 25/5 stress, on an *identical* trade set.
6. **The gap guard is still not implementable as written.** A research-side
   cancellation is not a resting SL-M; a real stop order fills on the gap. This
   is inherited from V10 and unchanged.
7. **The strong-identity fix is genuinely a fix**, and is the one V11 change
   that would be correct to keep regardless of whether Stage 10 survives.

**How to read the +Rs 46,783 / PF 2.15 headline:** it is what this configuration
*would have produced* on a stitched, partially incomplete panel under a
sensitivity EOD policy, with the configuration itself chosen using that same
panel. Use it to rank ideas. Do not use it as an expectation.

---

## 12. Commands and outputs

```powershell
# All three cost scenarios (the default)
python -u fno_v11_backtest.py run --all-usable-history

# Reference cost only
python -u fno_v11_backtest.py run --all-usable-history --reference-only

# Custom output root
python -u fno_v11_backtest.py run --all-usable-history --output-root <dir>

# Print the immutable profile + expected economics
python fno_v11_backtest.py profile

# Validate a completed run
python fno_v11_backtest.py validate --provenance <run-dir>\provenance.json
```

`--all-usable-history` is **required** on `run` — you are acknowledging the
pinned 65-session research-only contract.

### Output tree

```
v11_stage10_fixed_full_history_v1/
├── latest.json
└── run_<IST timestamp>/
    ├── all_input_candidates.csv
    ├── selected_candidates.csv
    ├── selection_decisions.csv
    ├── resolved_profile.json          profile_sha256 + full payload
    ├── source_segments.json
    ├── all_period_metrics.csv         15 periods x N scenarios
    ├── all_daywise.csv
    ├── benchmark_verification.json    layer 3 + layer 4 evidence
    ├── report.md
    ├── provenance.json
    ├── artifact_inventory.json
    ├── source/                        15 verbatim source modules
    └── scenarios/
        ├── reference_15_0/
        │   ├── candidate_order_audit.csv     179 columns
        │   ├── closed_trades.csv
        │   ├── daywise.csv
        │   └── summary.json
        ├── stress_20_2/
        └── stress_25_5/
```

The audit CSV carries **179 columns** — the V8/V10 set plus 13 `v11_*` runtime
columns (see A.6).

---

## 13. Where V11 sits

| | V6 live | V8 backtest | V10 max050-gap2 | **V11 Stage 10** |
|---|---|---|---|---|
| Purpose | Production paper/live | Execution-model research | Selection-rule research | Entry-timing + portfolio research |
| Setup book | 10 legs, caps 1L/2S | `ed329371…` | `ee97e86d…` | `ee97e86d…` |
| Fill | at trigger | at bar open if gapped | gaps > 2 bps rejected | same, **strong-identity guard** |
| Entry timing | S+1 fixed | variant B0–B5 | S+1 global + 3 legs S+3 | same, **plus 09:30 SHORT ≥ S+3** |
| Portfolio | 12/day cap, no ledger | 1 position/symbol, 12 slots | same | **2 same-side/symbol**, 12 slots |
| Cost | 5 bps | 15 bps | 15 / 20+2 / 25+5 | 15 / 20+2 / 25+5 |
| Fills (65 sessions) | — | — | 232 | **237** |
| PF / net (ref) | — | — | 1.833 / +Rs 36,312 | **2.145 / +Rs 46,783** |
| Status | PAPER, running | not promotable | not promotable | **not promotable, post-hoc** |

See [FNO_V6_LIVE_STRATEGY.md](FNO_V6_LIVE_STRATEGY.md),
[FNO_V8_BACKTEST_STRATEGY.md](FNO_V8_BACKTEST_STRATEGY.md) and
[FNO_V10_MAX050_GAP2_BACKTEST_STRATEGY.md](FNO_V10_MAX050_GAP2_BACKTEST_STRATEGY.md).

---

# Appendix A — Complete indicator and parameter reference

## A.1 Inherited indicators (unchanged from V10 / V8 / V6)

Computed on the NSE cash-equity 5-minute series by
`add_five_minute_features()`:

```python
for span in (9, 20, 50):
    ema{span}      = close.ewm(span=span, adjust=False).mean()   # α = 2/(n+1)
prev_close         = close.shift(1)
price_change_pct   = (close / prev_close - 1) * 100
prior_volume       = volume.shift(1).rolling(20, min_periods=5).mean()
volume_ratio       = volume / prior_volume            # denominator masked to > 0
traded_value       = close * volume
oi_change_pct      = (oi / prev_oi - 1) * 100         # only where oi>0, prev_oi>0, finite
```

| Indicator | α / window | Notes |
|---|---|---|
| `ema9` | α = 0.200000 | recursive, seeded at bar 0 |
| `ema20` | α = 0.095238 | **no warm-up guard** |
| `ema50` | α = 0.039216 | **continuous across sessions** |
| `price_change_pct` | previous 5m bar | % |
| `volume_ratio` | 20 bars, min 5 | NaN when denominator ≤ 0 |
| `traded_value` | — | close × volume, Rs |
| `oi_change_pct` | 5-minute delta | from the futures series, joined on exact bar-end ts |

The 20-bar volume denominator spans the session boundary — at 09:25, 19 of the
20 bars are the previous session's last ~95 minutes.

Confirmation morphology, all with a `1e-12` epsilon:

```
range          = high - low                              (> 0 required)
body_ratio     = |close - open| / range
adverse_wick   = LONG : (high - max(open,close))/range   SHORT: (min(open,close) - low)/range
close_location = LONG : (close - low)/range              SHORT: (high - close)/range
```

## A.2 The setup book

SHA-256 `ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675` —
the V8-Combined best-per-leg book, asserted by `validate_fixed_contract()`.

| # | Signal | Side | Cap | Picker | Price % | OI % | Vol | Body ≥ | Wick ≤ | Min TV | Stop % | Tgt % | R:R | Entry overrides |
|---:|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 09:25 | LONG | 4 | max_move | 0.30 | 0.10 | 3.0 | 0.00 | 0.50 | 0 | 0.40 | 1.0 | 1:2.5 | conf ≤ S+3, buffer 0, midpoint off, CLV none |
| 2 | 09:25 | SHORT | 4 | max_move | 0.20 | 0.10 | 1.5 | 0.60 | 0.60 | Rs 2.5 cr | 0.50 | 3.0 | 1:6.0 | conf ≤ S+3, buffer 2 bps, midpoint off, CLV none |
| 3 | 09:30 | LONG | 1 | max_move | 0.65 | 0.10 | 1.0 | 0.50 | 0.50 | 0 | 1.00 | 2.5 | 1:2.5 | *inherits* |
| 4 | 09:30 | SHORT | 4 | max_volume | 0.20 | 1.00 | 1.0 | 0.45 | 0.30 | Rs 2.5 cr | 1.00 | 4.0 | 1:4.0 | conf ≤ S+3, buffer 0, **midpoint ON**, **CLV ≥ 0.50** · **+ V11: no fill before S+3** |
| 5 | 09:35 | LONG | 1 | max_liquidity | 0.20 | 0.10 | 1.0 | 0.60 | 0.50 | 0 | 1.00 | 2.5 | 1:2.5 | *inherits* |
| 6 | 09:35 | SHORT | 2 | max_liquidity | 0.50 | 1.00 | 1.0 | 0.40 | 0.50 | 0 | 1.00 | 3.0 | 1:3.0 | *inherits* |
| 7 | 09:40 | LONG | 1 | max_liquidity | 0.20 | 0.10 | 2.0 | 0.50 | 0.50 | 0 | 0.50 | 2.5 | 1:5.0 | *inherits* |
| 8 | 09:40 | SHORT | 1 | max_move | 0.20 | 0.10 | 1.0 | 0.40 | 0.50 | 0 | 1.00 | 3.0 | 1:3.0 | *inherits* |
| 9 | 09:45 | LONG | 1 | max_move | 0.65 | 0.10 | 1.0 | 0.40 | 0.50 | 0 | 1.00 | 3.0 | 1:3.0 | *inherits* |
| 10 | 09:45 | SHORT | 1 | max_volume | 0.20 | 0.75 | 1.0 | 0.40 | 0.30 | 0 | 1.00 | 2.0 | 1:2.0 | *inherits* |

The 09:50 / 09:55 LONG and SHORT legs remain in `DISABLED_RESEARCH_LEGS` —
`status: DISABLED_RESEARCH`, `active: False`, and **not executable `V8Setup`
objects**.

## A.3 Resolved entry policy

`configure_engine("0940_LONG_MOVE_040")` overwrites the engine's variant
registry to a flat B0-style policy, then per-leg overrides apply:

```
GLOBAL  max_confirmation_minute = 1     buffer_bps = 0.0
        midpoint_invalidation   = False  close_location_min = None
        entry_expiry_minute     = 5
        square_off = 15:30   eod_policy = LAST_REAL_BAR_SENSITIVITY
```

| Leg | conf ≤ | buffer bps | midpoint | CLV | V11 earliest fill |
|---|---:|---:|---|---|---|
| 09:25 LONG | S+3 | 0.0 | off | none | S+2 |
| 09:25 SHORT | S+3 | 2.0 | off | none | S+2 |
| 09:30 LONG | S+1 | 0.0 | off | none | S+2 |
| **09:30 SHORT** | S+3 | 0.0 | **on** | **0.50** | **S+3** |
| 09:35 LONG | S+1 | 0.0 | off | none | S+2 |
| 09:35 SHORT | S+1 | 0.0 | off | none | S+2 |
| 09:40 LONG | S+1 | 0.0 | off | none | S+2 |
| 09:40 SHORT | S+1 | 0.0 | off | none | S+2 |
| 09:45 LONG | S+1 | 0.0 | off | none | S+2 |
| 09:45 SHORT | S+1 | 0.0 | off | none | S+2 |

The "earliest fill" column is S+2 for every other leg because a pending order
placed on the candle that just completed cannot fill on that same candle
(`ts <= order_placed_at` guard).

## A.4 `RuntimeSpec` — the V11 mechanism dataclass

| Field | Type | Default | Stage 10 value |
|---|---|---|---|
| `entry_setup_id` | `str \| None` | `None` | **`"09:30_SHORT"`** |
| `entry_not_before_minute` | `int \| None` | `None` | **`3`** |
| `exit_rule` | `str \| None` | `None` | `None` |
| `exit_activation_r` | `float \| None` | `None` | `None` |
| `same_side_symbol_limit` | `int` | `1` | **`2`** |

Derived: `active_mechanisms` returns `("ENTRY_NOT_BEFORE",
"PORTFOLIO_SYMBOL_LIMIT")`; `is_neutral` is `False`.

### Validation rules that raise

- `entry_setup_id` and `entry_not_before_minute` must be specified **together**;
- `entry_not_before_minute` must be a non-bool `int` in **[2, 5]**;
- `exit_rule` must be one of `None`, `BREAK_EVEN_NEXT_BAR`,
  `LATE_1430_BREAK_EVEN_NEXT_BAR`, `TRAIL_1R_AFTER_2R_NEXT_BAR`;
- a `None` exit rule cannot carry an R threshold; a non-`None` one requires a
  finite, positive `exit_activation_r`;
- `same_side_symbol_limit` must be a non-bool `int` in `{1, 2}`;
- more than one active mechanism requires `allow_composite=True` **and** the
  combination must be the allowed post-hoc pair.

Stage 10 passes `allow_composite=True` — it is by construction a two-mechanism
composite, which the validator otherwise forbids.

### Exit rules — defined but unused

Three exit mechanisms exist in the runtime and are **not active** in Stage 10:

| Rule | Behaviour |
|---|---|
| `BREAK_EVEN_NEXT_BAR` | once favourable excursion reaches R, move the stop to entry — executable on the **next** bar |
| `LATE_1430_BREAK_EVEN_NEXT_BAR` | the same, restricted to positions still open after 14:30 |
| `TRAIL_1R_AFTER_2R_NEXT_BAR` | after 2R favourable, trail the stop 1R behind the best price |

All three use the same discipline: **a threshold observed on bar *t* becomes
executable only on bar *t+1*** (`_apply_pending_dynamic_stop`), and a dynamic
stop may only ever move in the favourable direction
(`_dynamic_stop_is_better`). Their audit columns
(`v11_dynamic_stop_*`, `v11_running_mfe_r`, `v11_best_favorable_price`) are
present but empty in a Stage-10 run.

## A.5 Portfolio policy

| Field | Value | V11 change |
|---|---:|---|
| `capital_rs` | 120,000.0 | — |
| `margin_per_entry_rs` | 10,000.0 | — |
| `target_exposure_per_entry_rs` | 50,000.0 | — |
| `max_concurrent_positions` | 12 | — |
| `pending_reserves_margin` | `True` | only `True` supported |
| `one_position_per_symbol` | `True` | retained as **policy authority**, limit now 2 |

```
capacity                        = min(12, floor(120,000 / 10,000)) = 12
max exposure per symbol         = 2 × Rs 50,000 = Rs 100,000
max deployed exposure           = 12 × Rs 50,000 = Rs 600,000
max deployed capital            = 12 × Rs 10,000 = Rs 120,000
```

Rejection reasons, in evaluation order:

1. `DUPLICATE_SYMBOL_OPPOSITE_SIDE_PENDING_OR_OPEN`
2. `DUPLICATE_SYMBOL_SAME_SIDE_LIMIT_2`
3. `CAPITAL_MARGIN_OR_CONCURRENCY_LIMIT`

All three append `:CONSERVATIVE_NO_BACKFILL` — a rejection never promotes
another candidate into the freed slot.

## A.6 The thirteen `v11_*` audit columns

| Column | Meaning |
|---|---|
| `v11_runtime_schema_version` | execution-runtime schema |
| `v11_entry_not_before_minute` | the leg's configured earliest fill minute (3 for 09:30 SHORT, else null) |
| `v11_early_fill_checks_skipped` | how many pre-S+3 bars were refused |
| `v11_early_touch_observed` | `True` when a refused bar *would* have filled |
| `v11_exit_rule` | `None` in Stage 10 |
| `v11_exit_activation_r` | `None` in Stage 10 |
| `v11_dynamic_stop_activation_count` | unused in Stage 10 |
| `v11_dynamic_stop_active_at_terminal` | unused in Stage 10 |
| `v11_best_favorable_price` | unused in Stage 10 |
| `v11_running_mfe_r` | unused in Stage 10 |
| `v11_final_active_stop_price` | unused in Stage 10 |
| `v11_dynamic_stop_armed_at` | unused in Stage 10 |
| `v11_dynamic_stop_activated_at` | unused in Stage 10 |

Plus three portfolio columns — `v11_same_side_symbol_limit`,
`v11_opposite_side_same_symbol_prohibited`,
`v11_max_symbol_target_exposure_rs` — and three gap columns —
`v11_gap_runtime_schema_version`, `v11_gap_identity_policy`,
`v11_gap_rejected_identity_count`.

## A.7 Pinned constants

| Constant | Value |
|---|---|
| `PROFILE_ID` | `V11_S10_POST_HOC_TOP2_1436C7D363` |
| `STAGE_ID` | `STAGE_10_FIXED_STANDALONE` |
| `FAMILY` | `LOCKED_STAGE10_COMPOSITE` |
| `COMPONENT_VARIANT_IDS` | `V11_S9_SAME_SYMBOL_SAME_SIDE_MAX_2`, `V11_S4_0930_SHORT_ENTRY_NOT_BEFORE_S3` |
| `SELECTION_VARIANT` | `0935_LONG_MOVE_MAX_050` |
| `GAP_VARIANT` | `MAX_2_BPS` |
| `TARGET_EXPOSURE_PER_ENTRY_RS` | 50,000.0 |
| `SQUARE_OFF` | `15:30` |
| `EOD_POLICY` | `LAST_REAL_BAR_SENSITIVITY` |
| `EXPECTED_SESSION_COUNT` | 65 |
| `EXPECTED_SELECTED_CANDIDATES` | 1,134 |
| `LOCKED_PROFILE_SHA256` | `8dfc1627…aa1f6fe1` |
| `EXPECTED_INPUT_BINDING_SHA256` | `24e4da6c…b113f2b` |
| `EXPECTED_V10_PROFILE_SHA256` | `f2b32919…0834e59c` |
| `EXPECTED_SETUP_BOOK_SHA256` | `ee97e86d…d90ee675` |
| `ORIGIN_EXPERIMENT_PAYLOAD_SHA256` | `62386e07…072a79bcb` |
| `_BENCHMARK_ABS_TOLERANCE` | 1e-9 |
| `RUNTIME_SCHEMA_VERSION` (gap) | `fno_v11_strong_identity_gap_guard_v1` |
| `IDENTITY_POLICY` | `STRONG_REFERENCE_AND_IS_CHECK` |

## A.8 Pinned expected economics

Every field is compared exactly; the five float fields at 1e-9.

| Field | REFERENCE_15_0 | STRESS_20_2 | STRESS_25_5 |
|---|---:|---:|---:|
| sessions | 65 | 65 | 65 |
| candidates | 1,134 | 1,134 | 1,134 |
| fills | 237 | 237 | 237 |
| wins / losses | 123 / 114 | 119 / 118 | 114 / 123 |
| flat_trades | 0 | 0 | 0 |
| win_rate_pct | 51.89873417721519 | 50.210970464135016 | 48.10126582278481 |
| profit_factor | 2.1451722486639957 | 1.8627004050003626 | 1.5657168272703743 |
| net_return_points | 94.63085984175197 | 77.87761983466613 | 56.81711568642697 |
| net_pnl_rs | 46,783.22802111076 | 38,681.91875137453 | 28,481.757727453645 |
| max_daily_drawdown_points | 8.56737490149464 | 9.870817960114385 | 11.034340487111521 |
| positive / negative / flat days | 37 / 25 / 3 | 34 / 28 / 3 | 32 / 30 / 3 |
| remaining_gap_fills | 24 | 24 | 24 |
| guard_rejections | 24 | 24 | 24 |
| data_incomplete_candidates | 0 | 0 | 0 |

Float fields checked to tolerance: `win_rate_pct`, `profit_factor`,
`net_return_points`, `net_pnl_rs`, `max_daily_drawdown_points`.

## A.9 The fifteen snapshotted source modules

```
fno_v11_backtest.py                       fno_v11_execution_runtime.py
fno_v11_gap_runtime.py                    fno_v10_backtest.py
fno_v10_backtest_config.py                fno_v10_followup_challenger_research.py
fno_v10_gap_guard_research.py             fno_v10_experiment_backtest.py
fno_v10_experiment_config.py              fno_v10_unified_5m_1m_backtest.py
fno_v8_windowed_1m_entry_backtest.py      fno_oi_common.py
fno_oi_backtest_provenance.py             fno_oi_hybrid_data.py
eqidv2_runtime_paths.py
```

Filenames must be unique (asserted), every file must exist (else
`FileNotFoundError`), and all fifteen are re-hashed after the run.
