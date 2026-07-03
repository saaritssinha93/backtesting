# EXTENSION_RESULTS — wider targets, limit-entry slippage, and the SHORT side

Beyond the tight ±0.75% theme (user-approved follow-up). Same engine, same FIT/VAL→TRAIN→TEST
protocol, 1-min intrabar resolution (SL-first tie). Three axes tested together:
1. **Wider targets / R-multiples** — bracket grid up to 1.0/3.0 (the small +0.10% price-path edge
   should clear cost if the target is large enough).
2. **Limit-entry (lower slippage)** — TEST PF reported at 2 bps/leg (limit proxy), 5 bps (market), 15 bps.
3. **SHORT side** — 10 mirror families + pooled ALL_SHORT.

Universe top-250 liquid. TRAIN 2026-04-30..2026-06-12 / TEST 2026-06-15..2026-06-29. Gate = TRAIN PF≥1.15, TEST PF≥1.3@5bps, trades & dominance stable.

## Best per family×side (after 2-round FIT/VAL search)
| family | side | bracket | gross(0-cost) PF | TRAIN PF (n) | TEST PF @2bps | @5bps | @15bps | TEST win% | pass |
|---|---|---|---:|---:|---:|---:|---:|---:|:--:|
| S9_MIDDAY_LOSE | SHORT | x_125_250 | 1.708 | 1.259 (121) | 1.506 | 1.343 | 0.917 | 55.17 | ✅ |
| S8_OPENING_WEAK | SHORT | x_150_150 | 1.278 | 0.927 (87) | 1.076 | 0.974 | 0.695 | 55.56 | — |
| S1_VWAP_LOSE | SHORT | x_100_300 | 1.237 | 0.926 (960) | 0.961 | 0.868 | 0.625 | 40.07 | — |
| S6_VOLUME_DUMP | SHORT | x_075_150 | 1.045 | 0.684 (812) | 0.992 | 0.863 | 0.548 | 41.47 | — |
| S5_PULLBACK_CONT | SHORT | x_075_150 | 1.072 | 0.741 (1026) | 0.938 | 0.832 | 0.564 | 39.54 | — |
| ALL_SHORT | SHORT | x_125_250 | 1.043 | 0.793 (1094) | 0.893 | 0.817 | 0.608 | 42.43 | — |
| S10_RANGE_EXP | SHORT | x_075_200 | 1.084 | 0.758 (406) | 0.919 | 0.816 | 0.558 | 35.21 | — |
| F8_OPENING_STRENGTH | LONG | x_125_250 | 0.988 | 0.761 (505) | 0.833 | 0.767 | 0.585 | 36.15 | — |
| S3_CONSOL_BREAKDOWN | SHORT | x_075_150 | 1.014 | 0.695 (724) | 0.864 | 0.761 | 0.504 | 39.78 | — |
| F9_MIDDAY_RECLAIM | LONG | x_100_300 | 1.057 | 0.784 (630) | 0.831 | 0.754 | 0.552 | 33.33 | — |
| F3_CONSOL_EXPANSION | LONG | x_125_250 | 1.03 | 0.774 (951) | 0.798 | 0.724 | 0.527 | 39.52 | — |
| S4_FAILED_BREAKOUT | SHORT | x_100_300 | 1.819 | 1.319 (232) | 0.811 | 0.717 | 0.477 | 45.61 | — |
| ALL_LONG | LONG | x_100_300 | 1.281 | 0.991 (536) | 0.78 | 0.711 | 0.529 | 32.16 | — |
| F7_TREND_CONT | LONG | x_100_300 | 1.351 | 1.025 (354) | 0.77 | 0.708 | 0.543 | 27.0 | — |
| S2_PRESSURE_DUMP | SHORT | x_100_150 | 1.174 | 0.813 (1016) | 0.801 | 0.708 | 0.469 | 43.42 | — |
| F5_PULLBACK_CONT | LONG | x_100_300 | 1.125 | 0.836 (1374) | 0.773 | 0.706 | 0.531 | 28.57 | — |
| F10_RANGE_EXPANSION | LONG | x_125_250 | 1.242 | 0.961 (415) | 0.756 | 0.692 | 0.521 | 33.81 | — |
| S7_TREND_CONT | SHORT | x_150_150 | 1.141 | 0.816 (1055) | 0.767 | 0.683 | 0.461 | 46.67 | — |
| F2_PRESSURE_BURST | LONG | xv_t18 | 1.374 | 0.965 (757) | 0.736 | 0.652 | 0.442 | 33.65 | — |
| F1_VWAP_RECLAIM | LONG | x_125_250 | 1.111 | 0.846 (1161) | 0.693 | 0.636 | 0.481 | 31.49 | — |
| F6_VOLUME_EXPANSION | LONG | x_100_200 | 1.163 | 0.864 (871) | 0.658 | 0.598 | 0.437 | 31.58 | — |
| F4_FAILED_BREAKDOWN | LONG | x_100_300 | 1.207 | 0.887 (754) | 0.602 | 0.543 | 0.389 | 29.53 | — |

## Verdict (read the caveats — this is a WATCH, not a promote)

**Three findings:**
1. **LONG, wider targets: still REJECT.** Every LONG family is net-negative at 5 bps on every
   wider/R-multiple bracket (best TEST PF 0.77). Far targets are rarely reached intraday and the
   stop is hit first, so widening the target does not rescue the LONG side. Confirms the tight-bracket result.
2. **SHORT is systematically less-bad than LONG** (TEST net-win ~40% vs ~35%, several SHORT families
   TEST PF 0.80–0.87 vs LONG 0.54–0.77) — but **still mostly net-negative at 5 bps**.
3. **Exactly one config cleared the gate — `S9_MIDDAY_LOSE` (SHORT)** — and it is *stable* across
   windows (FIT 1.26 / VAL 1.26 / TRAIN 1.26 / TEST 1.34 @5bps) and robust across wider brackets
   (TRAIN 1.18–1.41 / TEST 1.11–1.34 for any target ≥1.5%). That stability is genuinely unusual.

**But it does NOT meet the original brief and carries real risks — classify as RESEARCH/WATCH, not promote:**
- ⚠️ **It is not a fast scalp.** ~62% of trades exit at **EOD** (18/29 in TEST), avg hold **~221 min**.
  The 1.25/2.50 bracket rarely fires; the edge is really *"short a late-morning loser below VWAP and
  hold to the close"* — a directional intraday-trend short, the opposite of the tight 0.75% theme.
- ⚠️ **Low sample:** TRAIN n=121 (~4/day), TEST n=29 (~3/day).
- ⚠️ **TEST profit is one-day concentrated:** dayDom **0.46**; 2026-06-25 alone = +Rs7,947. Removing
  that single day flips TEST net **negative**. On n=29 that is a serious fragility.
- ⚠️ **Cost-fragile:** profitable at 2 bps (PF 1.51) and 5 bps (1.34) but **loses at 15 bps (0.92)** —
  it only survives if entry slippage on shorts is genuinely small (limit/borrow-aware execution).

**Limit-entry axis:** lowering slippage 5→2 bps lifts every config ~0.07–0.16 PF (e.g. S9 1.34→1.51),
but only S9 crosses 1.0 even at 2 bps among LONGs — confirming the LONG edge is below the cost floor
regardless of execution. Reducing cost helps shorts more than longs.

**Net recommendation:** keep researching the SHORT side (it has the only real signal), but treat
`S9_MIDDAY_LOSE` as a **WATCH/paper-log** item pending more out-of-sample sessions and a fix for the
one-day concentration — **not** a promotion candidate. Nothing here meets the tight fast-momentum brief.

### Gate passer (research/watch only): SHORT S9_MIDDAY_LOSE — x_125_250
```json
{
  "family": "S9_MIDDAY_LOSE",
  "side": "SHORT",
  "bracket": "x_125_250",
  "slip_bps": 5.0,
  "min_minute": null,
  "max_minute": 660,
  "top_n": null,
  "rank_feat": "atr_pct",
  "max_per_sym_day": null,
  "max_book_concurrent": 20,
  "mask": [
    [
      "mom3_pct",
      ">=",
      0.1
    ],
    [
      "atr_pct",
      ">=",
      0.3
    ]
  ]
}
```
- TRAIN @5bps: n=121 PF=1.259 net=Rs16,560 win=52.89% exp=Rs136.9 tgt/sl/eod/time=16/40/65/0 tpd=4.03 dayDom=0.19 symDom=0.045 topTr=0.029 hold=220.5m
- TEST @5bps: n=29 PF=1.343 net=Rs4,442 win=55.17% exp=Rs153.2 tgt/sl/eod/time=3/8/18/0 tpd=2.9 dayDom=0.457 symDom=0.131 topTr=0.133 hold=221.1m
- TEST @2bps(limit): n=29 PF=1.506 net=Rs6,184 win=58.62% exp=Rs213.2 tgt/sl/eod/time=3/8/18/0 tpd=2.9 dayDom=0.45 symDom=0.127 topTr=0.129 hold=221.1m | @15bps(stress): n=29 PF=0.917 net=Rs-1,287 win=48.28% exp=Rs-44.4 tgt/sl/eod/time=3/8/18/0 tpd=2.9 dayDom=0.474 symDom=0.146 topTr=0.148 hold=221.1m

## PROMOTION (2026-06-30) — USER-DIRECTED, OVERRIDES THE REJECT VERDICT
The user explicitly directed promoting **S9_MIDDAY_LOSE** into the production book for v7 live + v11
backtesting on 2026-06-30, **despite the REJECT/break-even verdict above** (3-month PF 1.005 @5bps).
This was implemented as a flag-gated detector so default production is unchanged until the conf book
is active:
- **Detector:** `avwap_5min_ID_v2_backtesting._scan_day` → `add_catalog("S9_MIDDAY_LOSE","SHORT",...)`,
  reason_tag `midday_vwap_lose_failed_bounce_short`, gated by `v2.ENABLE_S9_MIDDAY_LOSE`
  (default OFF = `EQIDV2_USE_FINAL_SETUP_CONF`). Shared `avwap_5min_ID_v7_candidate_scan` feeds BOTH
  v11 (backtest) and `eqidv2_signal_discovery_v7_5min_id_persistent` (live).
- **Enabled** automatically in v11 `_activate_final_setup_conf` and the live conf activation when
  `S9_MIDDAY_LOSE` is in the conf whitelist.
- **Conf entry** added to `final_setup_conf.py` and `Train_and_Test/final_setup_conf.py`
  (exit 1.25/2.50, `gate_status=USER_DIRECTED_OVERRIDE_REJECT`, full evidence in `provenance`).
- **Detector encodes** the full S9 logic (structure + 10:20–11:00 window + mom3≥0.1 + atr≥0.30%)
  because the conf mask cannot enforce max-time or `mom3_pct`.

**Risk reminder:** cost-fragile (needs ≤5 bps/leg), EOD-dominated (~3.7h hold), ~4 trades/day,
loses in OOS April and at 15 bps. DO NOT SIZE UP; monitor live and demote if the cost wall holds.