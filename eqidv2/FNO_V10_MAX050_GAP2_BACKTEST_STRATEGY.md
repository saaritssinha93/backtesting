# FnO V10 — `max050-gap2` Full Usable-History Backtest (Complete Reference)

```powershell
python -u fno_v10_backtest.py max050-gap2 --all-usable-history --reference-only
```

**Profile ID:** `V10_STAGE7_0935_LONG_MAX_050_GAP2` ·
**Schema:** `fno_v10_max050_gap2_full_history_v1` ·
**Authority:** `BACKTEST_ONLY` · `research_only = True` ·
`promotion_eligible = False` · `headline_valid = False`

| | |
|---|---|
| Launcher | [fno_v10_backtest.py](fno_v10_backtest.py) |
| Locked Stage-7 profile | [fno_v10_backtest_config.py](fno_v10_backtest_config.py) |
| Selection overlay | [fno_v10_followup_challenger_research.py](fno_v10_followup_challenger_research.py) |
| Gap guard | [fno_v10_gap_guard_research.py](fno_v10_gap_guard_research.py) |
| Experiment harness | [fno_v10_experiment_backtest.py](fno_v10_experiment_backtest.py) |
| **Execution engine** | [fno_v8_windowed_1m_entry_backtest.py](fno_v8_windowed_1m_entry_backtest.py) |
| Output root | `…\fno_oi\strategy_research\v10_max050_gap2_full_history_v1\` |

---

## 1. What this command is

V10 is **not a new strategy**. It is a research launcher that sits on top of the
proven V8 state machine and applies exactly **three** stacked, individually
provenanced modifications to the V8 ten-leg book — then replays the result over
every validated, non-overlapping historical cache segment that currently exists.

```
   V8-Combined ten-leg book  (the control; V10B is trade-for-trade equivalent)
              │
              ├─ (1) STAGE 7          09:40_LONG  price_change_pct >= 0.40 %
              │
              ├─ (2) 0935 CEILING     09:35_LONG  price_change_pct <= 0.50 %
              │                        …then rerank within (session, setup)
              │
              └─ (3) GAP2 GUARD       reject any entry fill whose adverse
                                       gap through the trigger exceeds 2 bps
              │
              ▼
      V10_STAGE7_0935_LONG_MAX_050_GAP2
```

Everything else — the five-minute selection gates, the one-minute confirmation
morphology, the stop-entry trigger, S+5 expiry, brackets from the actual fill,
the stop-first same-bar rule, the global portfolio ledger, and the cost model —
is **unchanged V8**.

### The three modifications, and why each exists

| # | Rule | Motivation |
|---|---|---|
| **1. Stage 7** | `09:40_LONG` requires a ≥ 0.40% five-minute move | The 09:40 LONG leg's weak candidates were low-conviction drift, not impulse |
| **2. `.50` ceiling** | `09:35_LONG` requires a ≤ 0.50% five-minute move | 09:35 LONG was V8's **only outright losing leg** (PF 0.728 on B0 full history). Diagnosis: the *biggest* 09:35 movers were exhaustion, not continuation. The rule caps the move rather than raising the floor. |
| **3. Gap2** | Reject entry fills gapping > 2 bps adverse through the trigger | V8 fills at the bar **open** when it gaps past the trigger, dragging the whole bracket with it. Gap2 refuses the worst of those. |

> **Note on the direction of rule 2.** Rule 1 is a *floor* on move size, rule 2
> is a *ceiling*. They point opposite ways on purpose, because the two legs fail
> for opposite reasons. That asymmetry is exactly the kind of thing that should
> make you suspicious of overfitting — and the codebase says so itself (§9).

---

## 2. The locked Stage 7 base

`fno_v10_backtest_config.py` pins the base with pinned hashes and a
`validate_locked_profile()` that fails closed on any drift:

| Field | Value |
|---|---|
| `PROFILE_ID` | `V10_STAGE7_LOCKED_BACKTEST_20260827` |
| `ACTIVE_VARIANT` | `0940_LONG_MOVE_040` |
| `AUTHORITY` | `BACKTEST_ONLY` |
| Registry SHA-256 | `105935648a67ff12…51cc843b` |
| Variant config SHA-256 | `f3a54e5fddbfd844…10a9bc18` |
| Stage sequence SHA-256 | `e8381c478b6843de…7fd88c2` |
| Stage 7 reference provenance | `336d8b4bef026d5b…f3f54198d` |
| Stage 7 input fingerprint | `a863048ae1f084cf…16a5d3de` |
| Locked profile SHA-256 | `f2b3291903dfb1f2…0834e59c` |

The validator asserts, among other things:

```python
spec.price_threshold_overrides == (("09:40_LONG", 0.40),)   # Stage 7 and nothing else
spec.confirmation_volume_ratio_min is None
spec.entry_expiry_minute == 5
not spec.disabled_setup_ids
spec.slot_rvol20_min is None
payload["authority"]              == "BACKTEST_ONLY"
payload["research_only"]          is True
payload["promotion_eligible"]     is False
payload["live_or_paper_authority"] is False
```

**A non-Stage-7 mechanism cannot silently enter the locked profile, and the
profile cannot silently acquire live authority.**

The `run` subcommand (the plain Stage-7 baseline) additionally *locks the CLI*:
`_inject_locked_run_contract()` forces `--cost-bps 15`, `--slippage-bps 0`,
`--square-off 15:30`, `--eod-policy LAST_REAL_BAR_SENSITIVITY` and the pinned
source snapshot, and rejects any `--variant` other than `0940_LONG_MOVE_040`.

---

## 3. Modification 2 — the `09:35 LONG ≤ 0.50%` ceiling

`filters.selection_overlay(candidates, spec)` where
`spec = SPEC_BY_NAME["0935_LONG_MOVE_MAX_050"]`.

```python
stage7_rejected = (setup_id == "09:40_LONG") & (move + 1e-12 <  0.40)
                                            # -> STAGE7_0940_LONG_MOVE_BELOW_040

ceiling_rejected = passed
                 & (setup_id == "09:35_LONG")
                 & (move - 1e-12 >  0.50)
                                            # -> 0935_LONG_MOVE_ABOVE_CHALLENGER_MAX
```

Order matters: the Stage-7 rejection is applied first, and the ceiling is only
evaluated on rows that still read `PASSED`.

### 3.1 Filter, then **rerank**

This is the subtle and important part. After filtering, surviving candidates are
re-sorted and their `frozen_rank` is **recomputed from scratch**:

```python
filtered.sort_values(
    ["session_date", "setup_id", "picker_value", "traded_value", "symbol"],
    ascending=[True, True, False, False, True],
    kind="stable",
)
filtered["frozen_rank"] = filtered.groupby(["session_date","setup_id"]).cumcount() + 1
```

Application mode: `FILTER_THEN_RERANK_WITHIN_SESSION_AND_SETUP`.

**Consequence:** removing a rank-1 candidate *promotes* rank-2 into the cap.
The rule does not merely subtract trades — it **substitutes** them. That is why
the fill count barely moves (224 → 220 on the locked 59-session window) while
the P&L changes materially, and it is why a boundary-sensitive threshold can
swing a result by backfilling a different name. The `.40` variant was rejected
for precisely this reason (§8.2).

Every decision is written to `selection_decisions.csv` with
`original_frozen_rank`, `recalculated_frozen_rank`, `selection_passed`,
`selection_reason`, and the variant config SHA-256.

**Observed on this run:**

| Reason | Rows |
|---|---:|
| `PASSED` | 1,134 |
| `0935_LONG_MOVE_ABOVE_CHALLENGER_MAX` | 77 |
| `STAGE7_0940_LONG_MOVE_BELOW_040` | 30 |

`ChallengerSpec.validate()` enforces that a challenger carries **exactly one
mechanism** — the framework structurally prevents stacking two untested rules
inside one "variant".

---

## 4. Modification 3 — the 2 bps adverse-gap guard

`gaps.installed_gap_guard(spec)` is a **context manager** that monkey-patches
four engine seams for the duration of the run and restores them afterwards:
`_entry_fill`, `_postconfirmation_invalidated`, `_CandidateRuntime.transition`,
and `_audit_record`.

### 4.1 The measurement

```python
def adverse_gap_bps(side, bar_open, trigger):
    if side == "LONG":
        return (bar_open - trigger) / trigger * 10_000  if bar_open >= trigger else None
    if side == "SHORT":
        return (trigger - bar_open) / trigger * 10_000  if bar_open <= trigger else None
```

`None` means the bar did **not** open through the trigger — an ordinary intrabar
touch fill, which the guard never touches.

### 4.2 The decision

```python
def gap_is_rejected(spec, gap_bps):
    if spec.is_control:           return False
    if spec.reject_all_gap_fills: return True
    return gap_bps > spec.max_adverse_gap_bps + 1e-12     # 2.0 for MAX_2_BPS
```

The full guard ladder (only `MAX_2_BPS` is used by this profile):

| Variant | `max_adverse_gap_bps` | `reject_all_gap_fills` |
|---|---:|---|
| `CONTROL` | – | false |
| `MAX_0_BPS` | 0.0 | false |
| **`MAX_2_BPS`** | **2.0** | **false** |
| `MAX_5_BPS` | 5.0 | false |
| `REJECT_ALL_GAP_FILLS` | – | true |

`MAX_0_BPS` still admits an exactly-at-trigger gap fill; `REJECT_ALL_GAP_FILLS`
does not. That is the only intentional difference between them.

When the guard fires, the candidate is diverted to a terminal rejection instead
of `FILLED_OPEN`, and the audit record gains
`gap_guard_variant`, `gap_guard_max_adverse_bps`, `gap_guard_reject_all`,
`gap_guard_observed`, `gap_guard_rejected`, `gap_guard_bar_open`,
`gap_guard_trigger`, `gap_guard_adverse_bps` and `gap_guard_event_ts`.

The guard also asserts hard: if the neutral engine ever labels a non-gap as a
gap fill, it raises rather than silently mis-measuring.

> ⚠ **Live-behaviour caveat, stated in the source research:** a research-side
> cancellation is **not** equivalent to a resting stop-market/stop-limit at the
> broker. A real SL-M *will* fill on the gap. Gap2 is a measurement of what the
> gap fills cost you — it is **not** a rule you can copy into live order
> handling without a different order type.

---

## 5. The pinned 65-session usable-history contract

This is the reason the command needs the mandatory `--all-usable-history` flag:
you are acknowledging a **stitched, rollover-aware** dataset, not one clean run.

`MAX050_GAP2_USABLE_SEGMENTS` — five non-overlapping segments:

| Segment | Window | Contract month | Sessions |
|---|---|---|---:|
| `AUG_CORE_59` | 2026-05-27 … 2026-08-19 | 26AUG | 59 |
| `AUG_EXTENSION_20_21` | 2026-08-20 … 2026-08-21 | 26AUG | 2 |
| `SEP_ROLLOVER_24_25` | 2026-08-24 … 2026-08-25 | 26SEP | 2 |
| `SEP_DIAGNOSTIC_27` | 2026-08-27 | 26SEP | 1 |
| `SEP_DIAGNOSTIC_28` | 2026-08-28 | 26SEP | 1 |
| **Total** | **2026-05-27 … 2026-08-28** | mixed | **65** |

`validate_max050_gap2_contract()` runs **before any data loads** and raises if:

- any segment's `from_day > through_day`;
- **any two segments claim the same expected session** (overlap → duplicated P&L);
- a pinned cache manifest file is missing;
- the union is not **exactly 65** sessions.

### 5.1 Per-segment load validation — `_load_max050_gap2_segment()`

Each manifest must satisfy:

1. `setup_book_sha256 == ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675`
   — the same V8 setup book, byte for byte;
2. the cache's `contract_month_filter` equals the segment's declared month
   (26AUG vs 26SEP is enforced, not assumed);
3. every requested regular session exists in `manifest["session_dates"]`;
4. `len(candidates) == manifest["candidate_count"]` and
   `len(paths) == manifest["path_row_count"]`;
5. no duplicate `candidate_id` inside the segment.

After concatenation, two more global assertions: **no duplicate `candidate_id`
across segments**, and **no duplicate `(candidate_id, bar_ts)` minute bar**.

### 5.2 The calendar gap is recorded, never filled

`expected_regular_session_dates(min, max)` regenerates the full official span
from the hash-locked 2026 calendar and diffs it against what the segments
actually cover:

```
Missing regular sessions inside the span: 2026-08-26
```

**2026-08-26 has no validated cache.** The launcher records the calendar gap in
`missing_regular_session_dates` and in the report — it **never substitutes a flat
day**, which would silently improve every day-based statistic.

---

## 6. Cost scenarios and `--reference-only`

```python
COST_SCENARIOS = (
    ("REFERENCE_15_0", 15.0, 0.0),   # 15 bps round trip, no slippage
    ("STRESS_20_2",    20.0, 2.0),
    ("STRESS_25_5",    25.0, 5.0),
)
scenarios = COST_SCENARIOS[:1] if args.reference_only else COST_SCENARIOS
```

`--reference-only` runs **only** `REFERENCE_15_0` — one pass instead of three.
It changes nothing about selection, entry, or exit logic; the numbers reported
for the reference scenario are identical either way.

The reference economics: **15 bps round-trip cost, zero slippage, Rs 50,000
target exposure per entry, square-off 15:30, `LAST_REAL_BAR_SENSITIVITY`.**

> `REFERENCE_15_0` must always run: the benchmark verification
> (`validate_current_mixed_benchmark`) is only computed on that scenario, and
> the launcher raises `"Reference current-mixed benchmark was not evaluated"` if
> it somehow does not.

---

## 7. Execution flow of one invocation

```
 1. validate_max050_gap2_contract()          65 sessions, no overlap, files exist
 2. _load_all_usable_max050_gap2_history()   load + verify all 5 caches, concat
 3. filters.selection_overlay(...)           Stage7 + .50 ceiling, then rerank
 4. experiment.configure_engine("0940_LONG_MOVE_040")
    engine._confirmation_check = _NEUTRAL_CONFIRMATION_CHECK
 5. create run_<timestamp>/ , copy all 6 source modules into source/
 6. write all_input_candidates.csv, selected_candidates.csv,
          selection_decisions.csv, resolved_profile.json, source_segments.json
 7. for each cost scenario:
        policy = _entry_policy_for_variant(..., 15 bps, 0 slippage,
                                           15:30, LAST_REAL_BAR_SENSITIVITY)
        with gaps.installed_gap_guard(MAX_2_BPS):
            audit = _NEUTRAL_RUN_BACKTEST(selected, minute_paths, policy,
                                          target_exposure_per_entry_rs=50_000)
        -> scenarios/<name>/{candidate_order_audit,closed_trades,daywise,summary}
 8. validate_current_mixed_benchmark(reference FULL_USABLE row)
 9. write all_period_metrics.csv, all_daywise.csv, report.md,
          provenance.json, artifact_inventory.json, latest.json
```

**Source self-archiving:** all six modules
(`fno_v10_backtest.py`, `fno_v10_backtest_config.py`,
`fno_v10_followup_challenger_research.py`, `fno_v10_gap_guard_research.py`,
`fno_v10_experiment_backtest.py`, `fno_v8_windowed_1m_entry_backtest.py`)
are copied verbatim into `run_dir/source/` — a run is reproducible from its own
directory even if the working tree moves on.

### 7.1 Regression benchmark

`MAX050_GAP2_CURRENT_MIXED_BENCHMARK` is a literal, pinned expectation of the
`FULL_USABLE / REFERENCE_15_0` row (sessions 65, candidates 1,134, fills 232,
PF 1.8327310411717306, net Rs 36,312.05263290276, …). Floats are compared to
**1e-9** absolute tolerance. **If anything upstream changes the result, the run
records the mismatch** — this is a standing regression test embedded in the
launcher itself.

---

## 8. Results — `--reference-only`, 65 sessions

Run directory:
`…\v10_max050_gap2_full_history_v1\run_20260830T163837220506+0530`

### 8.1 Headline periods

| Period | Sessions | Candidates | Fills | W/L | Win rate | PF | Net points | Net P&L | Max daily DD |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **FULL_USABLE** | 65 | 1,134 | 232 | 116/116 | 50.00% | **1.8327** | **+73.0544** | **+Rs 36,312.05** | 9.351 |
| CORE_59 | 59 | 1,035 | 211 | 108/103 | 51.18% | 1.8862 | +70.4389 | +Rs 35,007.42 | 9.351 |
| FORWARD_EXTENSION | 6 | 99 | 21 | 8/13 | 38.10% | 1.3172 | +2.6155 | +Rs 1,304.63 | 2.363 |

Days: **37 positive / 25 negative / 3 flat.** Gap fills surviving the guard: 24.
Guard rejections: 14. `data_incomplete_candidates`: **0**.

### 8.2 Per segment — where the forward extension actually came from

| Segment | Sessions | Fills | Win rate | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|---:|
| `AUG_CORE_59` | 59 | 211 | 51.18% | 1.886 | +70.439 | +Rs 35,007.42 |
| `AUG_EXTENSION_20_21` | 2 | 3 | **0.00%** | **0.000** | **−2.363** | **−Rs 1,172.44** |
| `SEP_ROLLOVER_24_25` | 2 | 9 | 44.44% | 1.988 | +2.889 | +Rs 1,448.45 |
| `SEP_DIAGNOSTIC_27` | 1 | 6 | 33.33% | 1.392 | +0.943 | +Rs 477.23 |
| `SEP_DIAGNOSTIC_28` | 1 | 3 | 66.67% | 3.076 | +1.146 | +Rs 551.40 |

**The six forward sessions are not a validation set.** They are 21 fills across
four disconnected 1–2 day fragments spanning a contract rollover, of which one
fragment lost every trade it took. A PF of 1.32 on 21 trades is noise.

### 8.3 Per setup leg (reference scenario, 232 fills)

| Setup | Fills | Wins | Win % | PF | Net points | Net P&L |
|---|---:|---:|---:|---:|---:|---:|
| 09:25 SHORT | 62 | 28 | 45.2% | 2.056 | +22.367 | +Rs 10,997.77 |
| 09:40 LONG | 13 | 8 | 61.5% | **4.881** | +12.656 | +Rs 6,080.76 |
| 09:25 LONG | 61 | 32 | 52.5% | 1.684 | +11.010 | +Rs 5,289.78 |
| 09:35 SHORT | 8 | 4 | 50.0% | 2.469 | +6.776 | +Rs 3,413.11 |
| 09:30 SHORT | 30 | 14 | 46.7% | 1.361 | +5.945 | +Rs 3,075.48 |
| 09:45 LONG | 6 | 4 | 66.7% | 4.589 | +4.289 | +Rs 2,059.70 |
| 09:35 LONG | 17 | 9 | 52.9% | **1.505** | +3.742 | +Rs 1,914.06 |
| 09:40 SHORT | 16 | 8 | 50.0% | 1.373 | +3.131 | +Rs 1,821.05 |
| 09:30 LONG | 10 | 4 | 40.0% | 1.539 | +3.068 | +Rs 1,489.08 |
| 09:45 SHORT | 9 | 5 | 55.6% | 1.021 | +0.072 | +Rs 171.26 |

**Both targeted legs did what they were supposed to.** 09:35 LONG went from
V8's **PF 0.728 (−Rs 1,564)** to **PF 1.505 (+Rs 1,914)**; 09:40 LONG sits at
PF 4.881 on 13 fills. Concentration risk is obvious: the two 09:25 legs supply
**123 of 232 fills (53%)** and Rs 16,288 of Rs 36,312 net (45%).

### 8.4 Candidate funnel (1,134 candidates → 232 fills)

| Terminal status | Count | Share |
|---|---:|---:|
| `NO_CONFIRMATION` | 693 | 61.1% |
| `STOPPED` | 103 | 9.1% |
| `POSTCONF_CANCELLED` | 86 | 7.6% |
| `SQUARE_OFF` (last-real-bar) | 69 | 6.1% |
| `WINDOW_EXPIRED` | 63 | 5.6% |
| `TARGETED` | 60 | 5.3% |
| `PRECONF_INVALIDATED` | 32 | 2.8% |
| `DUPLICATE_REJECTED` | 28 | 2.5% |

**Six in ten candidates never confirm.** The one-minute confirmation gate is by
far the dominant filter — more selective than every five-minute threshold
combined.

### 8.5 Exit mix

| Exit | Trades | Share |
|---|---:|---:|
| `STOP` | 103 | 44.4% |
| `LAST_REAL_BAR_SENSITIVITY` | 69 | 29.7% |
| `TARGET` | 60 | 25.9% |

Only a quarter of fills reach target; nearly a third are resolved by the
**sensitivity** EOD policy rather than an exact 15:30 bar. That alone disqualifies
the headline.

### 8.6 Monthly

| Month | Sessions | Fills | Net points | Net P&L |
|---|---:|---:|---:|---:|
| 2026-05 | 2 | 7 | +8.045 | +Rs 3,977.40 |
| 2026-06 | 21 | 48 | +3.706 | +Rs 1,878.70 |
| **2026-07** | 23 | 119 | **+51.370** | **+Rs 25,234.90** |
| 2026-08 | 19 | 58 | +9.934 | +Rs 5,221.10 |

**July is 70% of the entire result** on 35% of the sessions. June was near-flat
across 21 sessions. This is the same July dependence visible in the V8 B0 run,
and it is the single biggest reason to distrust the aggregate.

### 8.7 Day extremes

| Best days | Fills | Net points | Net Rs |
|---|---:|---:|---:|
| 2026-07-07 | 7 | +9.651 | +4,714.73 |
| 2026-07-28 | 9 | +8.727 | +4,296.52 |
| 2026-07-08 | 7 | +7.708 | +3,627.04 |

| Worst days | Fills | Net points | Net Rs |
|---|---:|---:|---:|
| 2026-08-12 | 5 | −2.768 | −1,332.44 |
| 2026-07-09 | 4 | −2.582 | −1,250.56 |
| 2026-06-09 | 4 | −2.317 | −973.67 |

The three best days alone are **+26.1 points of the +73.1 total (36%)**. Average
**3.57 fills/session**, max 10.

### 8.8 Lineage — how this compares to its own controls

From the 59-session locked comparison in
[FNO_V6_V8_V10_REPAIRED_COMPARISON_20260828.md](FNO_V6_V8_V10_REPAIRED_COMPARISON_20260828.md):

| Strategy | Fills | W/L | WR | PF | Net points | Net P&L | Daily DD | TRAIN net | TEST net |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| V6 control | 165 | 78/87 | 47.27% | 1.6307 | 46.820 | Rs 23,862 | 9.122 | 38.455 | 8.365 |
| V6 A1+A2 `.50` | 158 | 78/80 | 49.37% | 1.7718 | 52.733 | Rs 26,739 | 8.789 | 42.527 | 10.206 |
| V8 combined control | 228 | 110/118 | 48.25% | 1.6652 | 61.149 | Rs 30,483 | 8.584 | 54.920 | 6.229 |
| V10 Stage 7 | 224 | 110/114 | 49.11% | 1.7003 | 63.042 | Rs 31,404 | 8.584 | 56.160 | 6.882 |
| **V10 Stage 7 + `.50`** | 220 | 110/110 | 50.00% | 1.7933 | 67.715 | Rs 33,679 | **8.584** | 59.645 | **8.070** |
| **V10 `.50` + Gap2** | 211 | 108/103 | **51.19%** | **1.8862** | **70.439** | **Rs 35,007** | 9.351 | **62.369** | 8.070 |

Read the last two rows carefully. Against the isolated `.50` rule, adding Gap2:

- improves **TRAIN** (+59.6 → +62.4);
- **only ties TEST** (8.070 → 8.070) and ties 27 August;
- **raises drawdown** (8.584 → 9.351).

The repaired-comparison document's own verdict:

> `V10 .50 + Gap2` is the highest historical-net row, but it is an explicitly
> **post-selection combination**. … It is therefore **exploratory, not the
> recommended rule.** The recommended clean shadow candidate is
> **V10 Stage 7 + `.50` alone.**

**The isolated-filter matrix** (why `.40` was rejected in favour of `.50`):

| V10 variant | Fills | WR | PF | Net points | 27-Aug net | Result |
|---|---:|---:|---:|---:|---:|---|
| Stage 7 control | 224 | 49.11% | 1.7003 | 63.042 | +0.944 | Baseline |
| `09:35 LONG max .40` | 219 | 49.77% | **1.8032** | **68.047** | **−2.565** | Reject — boundary-sensitive |
| **`09:35 LONG max .50`** | 220 | 50.00% | 1.7933 | 67.715 | +0.944 | **Best clean shadow** |
| `09:35 LONG max .60` | 222 | 49.55% | 1.7462 | 65.409 | +0.944 | Inferior to `.50` |
| `09:25 body/range ≥ .50` | 215 | 48.84% | 1.6945 | 60.999 | +0.940 | Reject |
| Prior-10 volume ratio ≥ 1.00 | 165 | 45.46% | 1.4847 | 35.189 | −1.913 | Reject |
| Prior-10 volume ratio ≥ 1.25 | 135 | 45.19% | 1.5022 | 30.256 | −1.913 | Reject |
| Prior-10 range ratio ≥ 1.00 | 137 | 44.53% | 1.3972 | 24.810 | −2.365 | Reject |
| Prior-10 range ratio ≥ 1.25 | 100 | 46.00% | 1.5288 | 24.717 | −2.365 | Reject |
| Stage 7 + Gap2 | 216 | 50.00% | 1.7574 | 64.613 | +0.944 | Secondary exploratory |

`.40` had the **higher historical number** and was still rejected, because on 27
August it removed ADANIPOWER and backfilled a losing POWERINDIA trade — the
rerank substitution effect of §3.1 in action. That is a good example of the
selection discipline being applied.

Market/sector alignment was **not run**: neither immutable snapshot contains a
causally bound NIFTY or sector 1-/5-minute series, and a generic or
current-membership proxy would introduce unmeasured look-ahead and
classification drift.

---

## 9. Why every output says `research_only`

`provenance.json` carries five explicit limitations:

| Code | Meaning |
|---|---|
| `SOURCE_SLOT_COVERAGE_INCOMPLETE` | the per-symbol panel is incomplete throughout; missing symbol-days suppress candidates rather than being measured |
| `LAST_REAL_BAR_SENSITIVITY_NOT_HEADLINE` | ~30% of exits use the last real bar, not an exact 15:30 bar |
| `STATIC_CONTRACT_UNIVERSES_BY_SEGMENT` | 26AUG for the first 61 sessions, 26SEP for the last 4 — not a rolling point-in-time universe |
| `2026_08_26_HAS_NO_VALIDATED_CACHE` | one regular session inside the span is absent and recorded as a gap |
| `POST_SELECTION_COMBINATION_REQUIRES_FORWARD_VALIDATION` | Stage 7, `.50` and Gap2 were each selected on history and then stacked |

And four flags:

```json
"headline_valid": false,
"research_only": true,
"promotion_eligible": false,
"live_or_paper_authority": false
```

### The honest summary

1. **Post-selection stacking.** Three rules, each chosen by looking at the same
   history, combined into one profile. The combined number is a **ceiling**.
2. **Rerank substitution.** Filters do not just remove trades, they promote
   different ones. Small threshold moves cause discontinuous outcome changes —
   demonstrated by the `.40` vs `.50` divergence on a single session.
3. **July concentration.** 70% of net from one month; three days supply 36% of
   the total.
4. **Six-session "forward extension" is not validation.** 21 fills, four
   fragments, a contract rollover, one all-losing fragment.
5. **Gap2 buys TRAIN, not TEST.** It ties the held-out slice and raises drawdown.
6. **Gap2 is not implementable as written.** Research-side rejection ≠ a resting
   SL-M at a broker; a real stop order fills on the gap.
7. **Static universe.** No point-in-time F&O membership; OI is not a rolling
   near-month series.

**Recommended reading of the +Rs 36,312 / PF 1.83 headline:** it is the number
this configuration *would have produced* on a stitched, partially incomplete
panel under a sensitivity EOD policy, with the configuration itself chosen using
that same panel. Use it to rank ideas against each other. Do not use it as an
expectation.

---

## 10. Commands and outputs

```powershell
# The full-history research replay (all three cost scenarios)
python -u fno_v10_backtest.py max050-gap2 --all-usable-history

# Reference cost only (15 bps / 0 slippage) - one scenario instead of three
python -u fno_v10_backtest.py max050-gap2 --all-usable-history --reference-only

# Custom output root
python -u fno_v10_backtest.py max050-gap2 --all-usable-history --output-root <dir>

# The immutable Stage-7 baseline (CLI is locked; it rejects other variants)
python fno_v10_backtest.py run --source-snapshot <manifest.json> `
  --from-day 2026-05-27 --through-day 2026-08-19 --split-day 2026-08-06 `
  --cost-bps 15 --slippage-bps 0 --square-off 15:30 `
  --eod-policy LAST_REAL_BAR_SENSITIVITY --rebuild-cache

# Inspect / validate
python fno_v10_backtest.py profile
python fno_v10_backtest.py validate --provenance <run-dir>\provenance.json

# Setup-cap sensitivity sweep (uniform caps 1..5)
python fno_v10_backtest.py max050-gap2-cap-sweep --all-usable-history
```

### Output tree

```
v10_max050_gap2_full_history_v1/
├── latest.json                                  run dir + provenance sha256
└── run_<IST timestamp>/
    ├── all_input_candidates.csv                 pre-overlay candidates
    ├── selected_candidates.csv                  post-filter, reranked
    ├── selection_decisions.csv                  per-candidate pass/reject + reason
    ├── resolved_profile.json                    the full resolved profile payload
    ├── source_segments.json                     per-segment manifests, hashes, coverage
    ├── all_period_metrics.csv                   every period x scenario row
    ├── all_daywise.csv                          per-session curve
    ├── report.md                                the human summary
    ├── current_mixed_benchmark_verification.json regression check
    ├── provenance.json                          command, profile, limitations, flags
    ├── artifact_inventory.json                  sha256 of every artifact
    ├── source/                                  verbatim copies of all 6 modules
    └── scenarios/reference_15_0/
        ├── candidate_order_audit.csv            144 columns, one row per candidate
        ├── closed_trades.csv
        ├── daywise.csv
        └── summary.json
```

### The audit CSV

`candidate_order_audit.csv` is the artifact worth reading. 144 columns per
candidate covering: the full 5-minute context (OHLCV, EMA9/20/50, `ema_structure`,
`price_change_pct`, `oi`, `prev_oi`, `oi_change_pct`, `volume_ratio`,
`traded_value`), every attempted confirmation candle with ordered rejection codes
and morphology, `trigger` / `trigger_distance_c5_bps` / `entry_price` /
`gap_fill` / `intrabar_trigger_fill` / `ambiguous_entry_bar`, brackets, exit
time/price/reason/`exit_at_bar_open`, gross and net return, position notional,
portfolio ledger decisions, gap-guard fields, and the `unconstrained_*` mirror
of every execution field so the cost of the portfolio ledger is measurable.

---

## 11. Relationship to the other two documents

| | V6 live | V8 backtest | V10 max050-gap2 |
|---|---|---|---|
| Purpose | Production paper/live runtime | Honest execution-model research | Selection-rule research |
| Setup book | 10 legs, caps 1L/2S | 10 legs, caps up to 4 (`ed329371…`) | V8-**Combined** book (`ee97e86d…`) |
| Confirmation | S+1 only, fixed | run variant B0–B5, minus per-leg overrides | **S+1 global** (B0-like) + 3 legs at S+3 |
| Fill | at trigger | **at bar open if gapped** | at open, **but > 2 bps gaps rejected** |
| Brackets | from trigger (bt) / fill (live) | from actual fill | from actual fill |
| Order life | until 15:30 | **expires at S+5** | expires at S+5 |
| Portfolio | 12 orders/day cap | **global margin + duplicate ledger** | same |
| Cost | 5 bps | 15 bps | 15 bps (+ 20/2 and 25/5 stress) |
| Status | PAPER, running | research, not promotable | research, not promotable |

See [FNO_V6_LIVE_STRATEGY.md](FNO_V6_LIVE_STRATEGY.md) and
[FNO_V8_BACKTEST_STRATEGY.md](FNO_V8_BACKTEST_STRATEGY.md).

---

# Appendix A — Complete indicator and parameter reference

V10 inherits its whole indicator and execution layer from the V8 engine. This
appendix states the inherited values exactly, then everything V10 changes.

## A.1 Inherited indicators (unchanged from V8/V6)

Computed on the NSE cash-equity 5-minute series:

```python
for span in (9, 20, 50):
    ema{span}      = close.ewm(span=span, adjust=False).mean()      # α = 2/(n+1)
price_change_pct   = (close / close.shift(1) − 1) × 100
prior_volume       = volume.shift(1).rolling(20, min_periods=5).mean()
volume_ratio       = volume / prior_volume            # prior_volume masked to > 0
traded_value       = close × volume
oi_change_pct      = (oi / prev_oi − 1) × 100         # only where oi>0, prev_oi>0, finite
```

EMAs are recursive, seeded at bar 0, have **no warm-up guard**, and are
**continuous across sessions**. The 20-bar volume denominator spans the session
boundary — at 09:25 it is 19 bars of the previous session plus 09:20.

Confirmation morphology, all with a `1e-12` epsilon:

```
range          = high − low                              (> 0 required)
body_ratio     = |close − open| / range
adverse_wick   = LONG : (high − max(open,close))/range   SHORT: (min(open,close) − low)/range
close_location = LONG : (close − low)/range              SHORT: (high − close)/range
```

Brackets from the **actual fill**, directional `Decimal` tick rounding
(`ROUND_CEILING` / `ROUND_FLOOR`); exit precedence `STOP_GAP → TARGET(open) →
STOP → TARGET`; `net = gross − cost_bps/100`; rupee sizing
`quantity = floor(50,000 / entry_price)`.

## A.2 The V10 setup book

V10 loads `fno_v10_unified_5m_1m_backtest.ACTIVE_SETUPS`, whose SHA-256 is
`ee97e86d3689767df95d1bbe8cb71215cf8a0cb40934f4e08d4c0805d90ee675` — the
**V8-Combined best-per-leg** book, verified against every cache manifest before
a segment loads.

| # | Signal | Side | Cap | Picker | Price % | OI % | Vol | Body ≥ | Wick ≤ | Min TV | Stop % | Tgt % | R:R | Entry overrides |
|---:|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 09:25 | LONG | 4 | max_move | 0.30 | 0.10 | 3.0 | 0.00 | 0.50 | 0 | 0.40 | 1.0 | 1:2.5 | conf ≤ S+3, buffer 0, midpoint off, CLV none |
| 2 | 09:25 | SHORT | 4 | max_move | 0.20 | 0.10 | 1.5 | 0.60 | 0.60 | Rs 2.5 cr | 0.50 | 3.0 | 1:6.0 | conf ≤ S+3, buffer 2 bps, midpoint off, CLV none |
| 3 | 09:30 | LONG | 1 | max_move | 0.65 | 0.10 | 1.0 | 0.50 | 0.50 | 0 | 1.00 | 2.5 | 1:2.5 | *inherits* |
| 4 | 09:30 | SHORT | 4 | max_volume | 0.20 | 1.00 | 1.0 | 0.45 | 0.30 | Rs 2.5 cr | 1.00 | 4.0 | 1:4.0 | conf ≤ S+3, buffer 0, **midpoint ON**, **CLV ≥ 0.50** |
| 5 | 09:35 | LONG | 1 | max_liquidity | 0.20 | 0.10 | 1.0 | 0.60 | 0.50 | 0 | 1.00 | 2.5 | 1:2.5 | *inherits* |
| 6 | 09:35 | SHORT | 2 | max_liquidity | 0.50 | 1.00 | 1.0 | 0.40 | 0.50 | 0 | 1.00 | 3.0 | 1:3.0 | *inherits* |
| 7 | 09:40 | LONG | 1 | max_liquidity | 0.20 | 0.10 | 2.0 | 0.50 | 0.50 | 0 | 0.50 | 2.5 | 1:5.0 | *inherits* |
| 8 | 09:40 | SHORT | **1** | **max_move** | 0.20 | **0.10** | 1.0 | **0.40** | **0.50** | 0 | 1.00 | **3.0** | 1:3.0 | *inherits* |
| 9 | 09:45 | LONG | 1 | max_move | 0.65 | 0.10 | 1.0 | 0.40 | 0.50 | 0 | 1.00 | 3.0 | 1:3.0 | *inherits* |
| 10 | 09:45 | SHORT | 1 | max_volume | 0.20 | 0.75 | 1.0 | 0.40 | 0.30 | 0 | 1.00 | 2.0 | 1:2.0 | *inherits* |

> **Leg 8 is the only difference** from the V8 module's own `ACTIVE_SETUPS`
> (SHA-256 `ed329371…016fb6`), which uses the *retuned* 09:40 SHORT — cap 4,
> `max_volume`, OI 0.75, body 0.00, wick 0.20, target 4.0, conf ≤ S+4, CLV 0.50.
> V10 uses the **V8-Strict** version of that leg, per the V8-Combined per-leg
> mapping. The other nine legs are identical.

Cap total: **9 LONG + 12 SHORT = 21** theoretical orders per session, clamped to
12 concurrent by the ledger.

**Fail-closed research legs.** The 09:50 and 09:55 LONG/SHORT legs are recorded
in `DISABLED_RESEARCH_LEGS` with `status: DISABLED_RESEARCH`, reason
`NO_CONFIG_PASSED_INDEPENDENT_TRAIN_LEG_GUARDS`, `active: False`. **No entry in
that mapping is an executable `V8Setup`** — they cannot generate candidates or
orders. They exist so the contract records *why* those slots are absent.

## A.3 The effective entry policy — resolved, not assumed

This is the part most easily misread. `configure_engine()` **overwrites**
`engine.VARIANT_REGISTRY` so that *every* V10 variant maps to one flat policy:

```python
engine.VARIANT_REGISTRY = {
    item.variant: {
        "description": item.description,
        "max_confirmation_minute": 1,        # <- B0 semantics, not B4
        "buffer_bps": 0.0,
        "midpoint_invalidation": False,
        "close_location_min": None,
    }
    for item in experiment_config.EXPERIMENT_SPECS
}
```

`_entry_policy_for_variant()` then overlays `entry_expiry_minute` and
`confirmation_volume_ratio_min` from the `ExperimentSpec`, and
`policy_for_setup()` applies each leg's own overrides.

**Resolved global policy for `0940_LONG_MOVE_040`:**

```
max_confirmation_minute = 1      buffer_bps = 0.0
midpoint_invalidation   = False  close_location_min = None
entry_expiry_minute     = 5
cost_bps = 15.0   slippage_bps = 0.0   square_off = 15:30
eod_policy = LAST_REAL_BAR_SENSITIVITY
```

**Resolved per-leg policy** (verified by resolving the live objects, not read off
the table):

| Leg | conf ≤ | buffer bps | midpoint | CLV |
|---|---:|---:|---|---|
| 09:25 LONG | **S+3** | 0.0 | off | none |
| 09:25 SHORT | **S+3** | **2.0** | off | none |
| 09:30 LONG | S+1 | 0.0 | off | none |
| 09:30 SHORT | **S+3** | 0.0 | **on** | **0.50** |
| 09:35 LONG | S+1 | 0.0 | off | none |
| 09:35 SHORT | S+1 | 0.0 | off | none |
| 09:40 LONG | S+1 | 0.0 | off | none |
| 09:40 SHORT | S+1 | 0.0 | off | none |
| 09:45 LONG | S+1 | 0.0 | off | none |
| 09:45 SHORT | S+1 | 0.0 | off | none |

So **seven of ten legs confirm on S+1 only**, and only three get the wider S+3
window. This matters for reading the funnel: the 693 `NO_CONFIRMATION`
candidates (61%) are mostly legs that had exactly *one* minute to confirm.

## A.4 `ExperimentSpec` — the variant dataclass

| Field | Default | Meaning |
|---|---|---|
| `variant` | — | registry key |
| `description` | — | human label |
| `confirmation_volume_ratio_min` | `None` | RV1 gate: 1m confirmation volume ÷ (signal 5m volume / 5) |
| `entry_expiry_minute` | 5 | last fillable minute |
| `disabled_setup_ids` | `()` | legs switched off |
| `price_threshold_overrides` | `()` | `((setup_id, price_change_pct), …)` |
| `slot_rvol20_min` | `None` | causal same-`HH:MM` prior-20-session median volume ratio |

Derived properties: `selection_overlay_id` returns `"BASE_V10B_SELECTION"` only
when no selection mechanism is set; `uses_rv1` and `uses_slot_rvol20` are simple
presence tests.

The eight predeclared Stage-1 variants are `V10B`, `RV1_100`, `RV1_100_S4`,
`NO_0935_LONG`, `0940_LONG_MOVE_030`, **`0940_LONG_MOVE_040`**, `SLOT_RVOL_150`
and `SLOT_RVOL_200`. `validate_registry()` raises if that exact set changes, and
the registry SHA-256 is pinned at `105935648a67ff12…51cc843b`.

**This profile's spec:**

```python
ExperimentSpec("0940_LONG_MOVE_040",
               "V10B with 09:40 LONG directional 5m price change >= 0.40%",
               price_threshold_overrides=(("09:40_LONG", 0.40),))
```

`confirmation_volume_ratio_min` and `slot_rvol20_min` are both `None`, and
`disabled_setup_ids` is empty — asserted by `validate_locked_profile()`. So the
RV1 confirmation-volume gate is **off**, and `_confirmation_check` returns the
neutral V8 record without even adding diagnostic keys.

## A.5 `ChallengerSpec` — the selection overlay

| Field | Default | This profile |
|---|---|---|
| `variant` | — | `0935_LONG_MOVE_MAX_050` |
| `description` | — | "Stage 7 plus 09:35 LONG five-minute move <= 0.50%" |
| `move_0935_long_max` | `None` | **0.50** |
| `body_0925_long_min` | `None` | `None` |
| `previous10_volume_ratio_min` | `None` | `None` |
| `previous10_range_ratio_min` | `None` | `None` |

`validate()` enforces that a challenger carries **exactly one** mechanism (the
`STAGE7_CONTROL` spec must carry zero), that every set threshold is finite and
`> 0`, and that a body-ratio threshold lies in [0, 1].

Required candidate columns for the overlay: `candidate_id`, `session_date`,
`signal_time`, `setup_id`, `side`, `symbol`, `price_change_pct`, `picker_value`,
`traded_value`, `frozen_rank`. A missing column raises.

Rejection order and codes:

```python
stage7_rejected  = (setup_id == "09:40_LONG") & (move + 1e-12 <  0.40)
                                  -> "STAGE7_0940_LONG_MOVE_BELOW_040"
ceiling_rejected = passed & (setup_id == "09:35_LONG") & (move - 1e-12 > 0.50)
                                  -> "0935_LONG_MOVE_ABOVE_CHALLENGER_MAX"
```

Rerank key, stable sort:
`["session_date","setup_id","picker_value","traded_value","symbol"]` with
`ascending=[True, True, False, False, True]`, then
`frozen_rank = groupby(["session_date","setup_id"]).cumcount() + 1`.

## A.6 `GapGuardSpec` — the entry guard

| Field | This profile |
|---|---|
| `variant` | `MAX_2_BPS` |
| `max_adverse_gap_bps` | **2.0** |
| `reject_all_gap_fills` | `False` |

```python
def adverse_gap_bps(side, bar_open, trigger):
    # raises unless bar_open and trigger are finite and trigger > 0
    LONG :  (bar_open − trigger) / trigger × 10_000   if bar_open >= trigger else None
    SHORT:  (trigger − bar_open) / trigger × 10_000   if bar_open <= trigger else None

def gap_is_rejected(spec, gap_bps):
    if spec.is_control:           return False
    if spec.reject_all_gap_fills: return True
    return gap_bps > spec.max_adverse_gap_bps + 1e-12
```

`is_control` is true only when `max_adverse_gap_bps is None and not
reject_all_gap_fills`. `validate()` rejects a spec that sets both a threshold
and `reject_all_gap_fills`, a threshold-guard with no threshold, and any
non-finite or negative threshold.

Nine audit columns are added per candidate: `gap_guard_variant`,
`gap_guard_max_adverse_bps`, `gap_guard_reject_all`, `gap_guard_observed`,
`gap_guard_rejected`, `gap_guard_bar_open`, `gap_guard_trigger`,
`gap_guard_adverse_bps`, `gap_guard_event_ts`.

Four engine seams are monkey-patched inside the context manager and restored on
exit: `_entry_fill`, `_postconfirmation_invalidated`,
`_CandidateRuntime.transition`, `_audit_record`.

## A.7 Run economics

| Parameter | Value |
|---|---:|
| `target_exposure_per_entry_rs` | 50,000.0 |
| `square_off` | 15:30 |
| `eod_policy` | `LAST_REAL_BAR_SENSITIVITY` |
| `portfolio capital_rs` | 120,000.0 |
| `margin_per_entry_rs` | 10,000.0 |
| `max_concurrent_positions` | 12 |
| `one_position_per_symbol` | `True` |

Cost scenarios:

| Scenario | `cost_bps` | `slippage_bps` | Run by `--reference-only`? |
|---|---:|---:|---|
| `REFERENCE_15_0` | 15.0 | 0.0 | ✅ (the only one) |
| `STRESS_20_2` | 20.0 | 2.0 | ✗ |
| `STRESS_25_5` | 25.0 | 5.0 | ✗ |

Slippage enters the fill, not the cost line:
`entry = trigger × (1 ± slippage_bps/10_000)`, tick-rounded directionally. At
`REFERENCE_15_0` slippage is zero, so the only economic haircut is the 15 bps
round trip.

## A.8 Segment registry — the exact pinned inputs

| Segment | From | Through | Contract | Sessions | Cache manifest |
|---|---|---|---|---:|---|
| `AUG_CORE_59` | 2026-05-27 | 2026-08-19 | 26AUG | 59 | `v10_repaired_snapshot_reruns_20260827_v1\caches\historical_59_sessions\64744f54dbfb5f1a` |
| `AUG_EXTENSION_20_21` | 2026-08-20 | 2026-08-21 | 26AUG | 2 | `v10_unified_5m_1m_v1\cache\ad5d9c3c1c68751c` |
| `SEP_ROLLOVER_24_25` | 2026-08-24 | 2026-08-25 | 26SEP | 2 | `v10_unified_5m_1m_v1\rollover_diagnostic\cache\586e53c8cdd53098` |
| `SEP_DIAGNOSTIC_27` | 2026-08-27 | 2026-08-27 | 26SEP | 1 | `v10_repaired_snapshot_reruns_20260827_v1\caches\today_2026_08_27_sep\4f6678c068fa1bfb` |
| `SEP_DIAGNOSTIC_28` | 2026-08-28 | 2026-08-28 | 26SEP | 1 | `today_six_strategy_replays_v1\today_2026-08-28_…\v10_cache\75ae1eb8013c86f0` |

Per-segment load assertions: setup-book SHA-256 match; `contract_month_filter`
match; every requested regular session present in `manifest["session_dates"]`;
`len(candidates) == manifest["candidate_count"]`;
`len(paths) == manifest["path_row_count"]`; no duplicate `candidate_id`.
Cross-segment: no duplicate `candidate_id`, no duplicate `(candidate_id, bar_ts)`.

Also captured per segment: cache schema version, cache input fingerprint,
snapshot fingerprint, universe payload, `expected_symbol_sessions`
(`mapped_stock_futures × sessions`), `source_incomplete_symbol_sessions`,
`unexpected_source_symbol_sessions`, and a `headline_source_complete` flag that
is true only when both counts are zero.

## A.9 The Stage-7 base universe (from the locked profile)

| Field | Value |
|---|---|
| Universe master date | 2026-08-11 |
| Contract month filter | 26AUG |
| Mapped stock futures | 208 |
| Mapped symbol-set SHA-256 | `d42f87a9c5fc8ab1…499a05a3` |
| Mapped universe SHA-256 | `2cc160189f87bff4…ad5824bf` |
| Universe SHA-256 | `18c496bbf9e09b69…8cb66c19ad5` |
| Expected symbol-sessions | 12,272 |
| Expected **complete** symbol-sessions | 6,350 |
| Expected **incomplete** symbol-sessions | 5,922 |
| Source captures / bytes | 416 files / 3,116,245,273 bytes |
| Coverage authority | `EXPECTED_EXCHANGE_SESSIONS_ONLY; PER_SYMBOL_SOURCE_COVERAGE_REMAINS_INCOMPLETE` |

**Only 52% of expected symbol-sessions are source-complete.** That single number
is the strongest reason the headline is marked invalid.

## A.10 Pinned regression benchmark

`MAX050_GAP2_CURRENT_MIXED_BENCHMARK`, compared at `1e-9` absolute tolerance on
every float field:

| Field | Expected |
|---|---:|
| `dataset` / `period` / `scenario` | `ALL_USABLE_HISTORY` / `FULL_USABLE` / `REFERENCE_15_0` |
| `sessions` | 65 |
| `candidates` | 1,134 |
| `fills` | 232 |
| `wins` / `losses` / `flat_trades` | 116 / 116 / 0 |
| `win_rate_pct` | 50.0 |
| `profit_factor` | 1.8327310411717306 |
| `net_return_points` | 73.05442256172977 |
| `net_pnl_rs` | 36,312.05263290276 |
| `max_daily_drawdown_points` | 9.351281246312235 |
| `positive` / `negative` / `flat` days | 37 / 25 / 3 |
| `remaining_gap_fills` | 24 |
| `guard_rejections` | 14 |
| `data_incomplete_candidates` | 0 |

Float fields checked to tolerance: `win_rate_pct`, `profit_factor`,
`net_return_points`, `net_pnl_rs`, `max_daily_drawdown_points`.

Two derived facts worth noting: **24 gap fills still survive** the 2 bps guard
(so the guard removed 14 of 38 gap fills, about 37%), and
`data_incomplete_candidates == 0` — every one of the 1,134 candidates had a
complete S+1…S+5 minute path, which is a stronger statement than the overall
symbol-session coverage figure suggests.
