# V14 cash MIS stocks outside F&O

V14 is an isolated research copy of the accepted **V13-V10-G-3, exact-next-minute LONG 1.10× / SHORT 1.20×** baseline. It replaces stock-futures OI eligibility with cash-volume participation alternatives. The universe is the available MIS stock list minus stock F&O membership for each session. It has no authority to place orders.

## Run

From the repository root:

```powershell
# Full comparison: records the protocol, stages data, repairs missing history,
# then evaluates July 29 through October 9, 2026 with 30 warmup sessions.
python -m v14 run-all --fetch-missing

# The isolated Windows launcher defaults to that same command.
bat\run_v14_backtest.bat

# Data preparation and research can also run separately.
python -m v14 prepare --start 2026-07-29 --end 2026-10-09 --warmup-sessions 30 --fetch-missing
python -m v14 study
python -m v14 latest

# Reuse only the chosen frozen replacement for subsequent completed sessions.
# This does not compare alternatives or change the selected threshold.
python -m v14 replay-selected --start 2026-10-12 --end 2026-10-12 --fetch-missing
```

Only completed sessions are eligible. Replace the example replay dates with dates that have finished. `prepare` and `run-all` require `--fetch-missing` to request broker history; without it, they use locally available data. The launcher explicitly supplies that flag only when invoked without arguments. Arguments supplied to the launcher are forwarded to `python -m v14`:

```powershell
bat\run_v14_backtest.bat latest
bat\run_v14_backtest.bat study --root C:\TradingData\eqidv2\v14
```

The default runtime root is `C:\TradingData\eqidv2\v14`. Override it with `V14_RUNTIME_ROOT` or `--root` after the subcommand. Use a separate root for a symbol-subset smoke check so its data manifest does not replace the full-universe manifest:

```powershell
python -m v14 prepare --symbols SYMBOL1,SYMBOL2 --root C:\TradingData\eqidv2\v14_smoke
```

`--retry-partial` on preparation retries provider sessions already recorded as incomplete. `--probe-mis` records current broker margin calculations; these do not establish MIS order acceptance or historical eligibility. `study --preregister-only` records the fixed study protocol without calculating returns.

## Preserved baseline

The sealed G-3 source closure and configuration are copied under `_baseline/`, with checksums in `snapshot_manifest.json`. V14's numerical execution module contains exact copies of the required inert functions. It does not import mutable V13 strategy modules, read later live policy changes, or start the existing dashboard/trading workflows.

The preserved behavior includes:

- All 14 setup times, price/EMA/native-volume/body/wick filters and native ranking.
- The 09:25 SHORT NIFTY first-bar price-return gate.
- Exact next-minute confirmation: LONG volume at least 1.10×, SHORT at least 1.20×.
- Confirmation high/low trigger, the original ten-minute entry limit, gap handling and stop-first ambiguous-bar execution.
- A 1.25% initial stop, tightening to 1.00% after 120 minutes, original setup targets and the 15:15 time exit.
- ₹100,000 capital per entry, modeled 5× exposure, ₹1,000,000 portfolio capital and the original flat 5bps round-trip cost.

The existing OI gates are removed explicitly; cash inputs are never stored as fabricated OI. No active accepted G-3 setup ranked on OI, so the liquidity/move/volume rankers remain unchanged. The old OI percentage thresholds remain only in the copied provenance/configuration.

New V14 studies have **no maximum selection per time slot or side**. Every
stock passing the configured expanded eligibility and confirmation gates is
selected. `selection.py` supplies the active rules without `max_entries` and
omits that column from new selected-order and trade ledgers. Core eligibility
and native ranks remain available for inspection; they do not exclude a
qualifying stock. Entry expiry, sizing and portfolio capital constraints still
apply, so selection does not guarantee execution.

The recorded slot policy is `V14_ALL_ELIGIBLE_PER_SLOT_V1`. Existing completed
reports retain their original capped results; their profit factors do not
describe the new uncapped configuration. Frozen-profile replay honors the
policy recorded by its selecting study (older profiles retain their original
caps). Run a new `study` to generate an uncapped selected profile. The sealed
G-3 adapter remains available only for baseline comparisons and historical
reproduction; V13 strategy files and configuration are unchanged.

## Fixed comparison

The 16 registered variants are the no-OI control; same-clock interval RVOL at 1.0/1.5/2.0; cumulative same-clock RVOL at 1.0/1.5/2.0; relative cash turnover at 1.5; ten-minute close-location-volume pressure at 0/0.2; ten-minute signed-volume pressure at 0/0.2; interval RVOL 1.5 combined with close-location pressure 0/0.2; and five-minute MFI at 55/60. Pressure and MFI are aligned with LONG/SHORT direction. Same-clock denominators use the median of the prior 20 recorded rows at that clock, excluding the current session and requiring all 20 source values valid. Entirely absent clock rows are skipped; a recorded but incomplete cumulative-volume row invalidates its reference window.

Replacement eligibility is evaluated at the completed five-minute signal. Confirmation thresholds, rankings and execution remain the frozen baseline. All variants share candidate feature availability and evaluated dates. Orders are selected before execution-data checks; another stock is never substituted afterward.

Study protocol V2 accepts real uninterrupted minute bars through a proven native stop/target, the actual 15:15 exit, or all ten minutes of an unfilled entry window. A later gap cannot change an already resolved outcome. A gap before resolution leaves the trade unknown and excludes the entire date for all alternatives. `path_evidence.json` records every proof and its consumed-bar hash; coverage reports also show the stricter full-session counterpart. No missing candle is manufactured. This improves data sufficiency without changing the trading rules, variant grid or selection criterion. V1 preliminary output remains separate; replay honors the frozen profile's policy.

The protocol records the train/validation/test boundaries and selection rules before the study. The replacement is selected using training qualification and validation performance; later test returns do not choose or retune it. If qualification fails, that status remains explicit. `selected_profile.json` freezes the resulting research candidate for `replay-selected`. Holding out dates for this replacement study does not erase G-3's previous use of overlapping historical data.

## Data and credentials

Preparation reuses raw cash one-minute bars and requests missing broker history with three workers sharing a limiter that spaces every request start by at least 0.46 seconds. This stays below the documented [Kite historical-data limit](https://kite.trade/docs/connect/v3/exceptions/#api-rate-limit). Timestamps label completed minute ends in Asia/Kolkata. Missing minutes remain missing; neither synthetic candles nor stock-futures data fill the cash series. Date selection uses the repository's exchange calendar. Data manifests record checksums, per-symbol/session coverage, source records, universe construction and calendar sessions outside the original G-3 archive. Research recomputes session coverage from hash-verified cash inputs and binds feature caches to the loader, feature code and construction logic. See the [broker's candle-data contract](https://kite.trade/docs/connect/v3/historical/).

Historical MIS membership is not available for every date: the working universe starts from the saved MIS list. Dated F&O masters are used where available; a labelled snapshot approximation is retained where a historical master is missing. Current instrument tokens may not recover every renamed or delisted symbol. These limitations remain in results rather than implying a complete historical broker-eligible universe.

Broker reads reuse the existing secure local login configuration. Credentials are not copied into V14 reports or its source snapshot. If authentication is rejected, refresh the broker session through the existing local login flow, then rerun preparation with `--fetch-missing`; add `--retry-partial` when retrying already attempted partial sessions. The V14 command-line preparation and frozen-profile replay also repair missing NIFTY opening-price context using verified contract mappings and real minute bars. Unavailable contract mappings or history remain explicit. Local-only coverage and any provider/authentication failures are recorded in the data manifest. A completed staging job does not imply every requested stock/day is complete.

## Results and monitoring

Each study creates a new timestamped directory under `runs/`; an existing completed `summary.json` is never overwritten. `latest.json` points to the latest completed run. The runtime root also keeps the latest frozen `selected_profile.json`; the selecting run retains its own copy.

Core study outputs are `report.html`, `summary.json`, `daily_results.csv`, `trades.csv`, `selected_orders.csv`, `variant_comparison.csv`, `selected_profile.json`, `protocol.json` and `evaluation_coverage.json`. Preparation writes `data/manifest.json`, `data/coverage.csv`, `data/universe_manifest.json`, per-symbol minute parquet files and source audits. Inspect the report and coverage files together.

The cash-only pullback diagnostics preserve G-3's deterministic ATR/momentum/volume warning rules. They are explicitly **uncalibrated on non-F&O stocks** and are not the learned selective-V2 classifier from the older daywise report. Warning features use completed observations; future pullbacks are separate hindsight labels, including unresolved intrabar ordering. Monitoring never changes trade entries, exits, sizing or P&L.

Historical modeled profitability, the inherited uniform 5× exposure and flat costs do not establish actual fills or current margin availability for every non-F&O stock. The report includes higher-friction sensitivity and keeps qualification/holdout results explicit; no result is described as profitable before it is calculated.

## Entry-quality diagnostic follow-up

The original V2 results remain immutable. A separate six-arm retrospective
experiment diagnoses transfer from F&O to non-F&O cash stocks:

```powershell
python -m v14.improvement_study --source C:\TradingData\eqidv2\v14\runs\20261010_participation_v2_observed --output C:\TradingData\eqidv2\v14\runs\NEW_ENTRY_QUALITY_RUN
```

`improvement_spec.py` defines the fixed controls: no additional gate; capacity;
price extension; both; both with interval RVOL >=1.5; and both with RVOL >=1.5
and side-aligned CMF10 >=0. Planned notional must be at most 5% of signal
five-minute turnover and 10% of confirmation-minute turnover. Absolute
five-minute price change is limited to the inherited 1.25% stop; signal
range/close and distance from EMA9 are limited to 2.5%. These filters act on
all eligible candidates before unchanged native rankings. No threshold search,
setup deletion, stop change, or post-outcome stock substitution is performed.

The runner verifies the archived feature pool and cash input hashes, reproduces
the original control selections/P&L, and saves separate common-date,
original-date-intersection, and per-arm coverage metrics. Per-arm totals can
only be compared directly when their evaluated date lists are identical.
All partitions, including the old TEST period, have already been observed:
this follow-up is retrospective. `research_candidate.json` records the fixed
historical selector and failure status; it does not replace the root selected
profile, update `latest.json`, or authorize execution.

The 2026-10-10 diagnostic found no qualifying candidate. All four capacity
arms had exactly the same 48 evaluated sessions. Capacity alone lost
INR187,692.58; adding price-extension guards lost INR58,770.86; adding RVOL
and CMF lost INR40,607.72. On the narrower common18-date comparison, the
historical selector chose capacity + extension + RVOL (INR13,264.84 total),
but validation remained negative and an extra5bps cost made that total
negative. It was not promoted.

`python -m v14.diagnostics_baseline` reproduces matched-date profit and
numerical feature comparisons against the sealed G3 archive.
`python -m v14.diagnostics_oi_ablation` reproduces a same-F&O-universe OI-on/off
comparison using broad pre-OI inputs. On the original18 V14 dates, OI-on
exactly reproduced G3 (INR48,069.37 /32 executed trades); OI-off returned
INR-3,964.97 /81 trades. All94 unique selected execution paths were complete.

## PF-target indicator research

```powershell
python -m v14.pf_study --source C:\TradingData\eqidv2\v14\runs\20261010_participation_v2_observed --output C:\TradingData\eqidv2\v14\runs\NEW_PF_INDICATOR_RUN
```

The separate PF study records its 14 indicator combinations and exact source
hashes before constructing features or calculating results. Every arm uses the
existing capacity and price-extension guards before native G3 selection.
New eligibility families are session VWAP acceptance/reclaim, opening-five-
minute range breakout, prior-five-minute range breakout, directional
efficiency over ten minute closes, session-reset Wilder ADX5, and native
five-minute EMA9 slope. Two combinations also use existing interval RVOL.
All new session indicators are available by the earliest 09:25 signal when
the actual opening minutes are complete. Fast ADX5 is explicitly a one-minute
session-reset indicator; it is not the ordinary cross-session ADX14.

Primary execution costs are explicitly increased from the inherited 5bps to
10bps round trip. The same fills and notional are stressed at total 15/20bps.
Net profit factor sums positive after-cost trade outcomes and divides by the
absolute sum of negative outcomes. No-loss and empty samples have undefined
PF and cannot qualify.

The requested calendar fixes the first 20 sessions as initial training and
subsequent 5-session forward blocks. At each boundary, the selector sees only
earlier common evaluable dates and requires 20 trades, 3 losses, positive net
P&L and PF>=1.3. It ranks by earlier PF, net P&L and declared grid order;
otherwise that block remains in cash. Missing dates never shift boundaries.
All arms share candidate indicator availability and date coverage. Unresolved
selected outcomes exclude their entire date, with no stock substitution.

The reported PF2 target requires at least 50 forward trades, 20 evaluated
sessions and 5 losses. Active trading days are reported separately. A stronger
fixed check requires a 95% five-session-block bootstrap lower PF above 1 and
PF>=1.5 at 15bps total costs. Bootstrap uncertainty is conditional on observed
history and does not correct for earlier strategy searches.

These dates have already been examined in earlier V14/G3 research. Even
causal walk-forward selection is explicitly retrospective here; no fresh
holdout or guaranteed profit is claimed. All 14 arms, fold decisions, costs,
coverage and uncertainty are retained. `next_research_candidate.json` can
record a rule for subsequent unseen research; it never replaces the default
profile or changes decisions in the completed forward blocks.

The completed `20261010_pf_indicators_v1` run evaluated 48 shared sessions.
The highest descriptive full-period PF was VWAP reclaim at 1.035 (54 trades,
INR 5,080.83 net), but its later-period PF was 0.710 (26 trades). The causal
walk-forward selector executed 16 trades across 8 active sessions, with
PF 0.286 and INR -43,280.39 net at 10bps. It abstained in four of seven blocks.
No arm qualified for the next research selection, and PF2 was not established.
The 30 evaluated forward dates include the cash/no-trade dates; they are not
30 actively traded sessions. These failed outcomes remain in the report.

## Fixed-selection coverage repair

The separate coverage replay keeps the archived uncapped selections and the
previously chosen profile fixed. It verifies the original input hashes and
broker repair snapshots before assessing any additional outcomes:

```powershell
python -m v14 coverage-study --source C:\TradingData\eqidv2\v14\runs\20261010_participation_uncapped_v1 --repair-dir C:\TradingData\eqidv2\v14\repairs\20261010_uncapped_gaps_v1 --output C:\TradingData\eqidv2\v14\runs\NEW_COVERAGE_RUN
```

Independent broker five-minute ranges may prove that a missing minute could
not touch a trigger, stop, or target. They never create candles or supply an
entry or exit price. Every fill, stop, target and square-off comes from a real
one-minute observation. Price-incompatible broker snapshots are withheld;
possible touches remain unresolved. Entry expiry uses elapsed clock minutes.
MFE and MAE remain unavailable when omitted post-entry observations prevent
an exact excursion calculation.

An unresolved trade can remain in a valid day's ledger only if its entry is
known and unchanged capital rules prove it was rejected. Its hypothetical
exit and return stay null. An admissible unresolved trade still excludes its
entire date. The report separates each variant's own coverage from the common
dates shared by all variants, reproduces the original evaluated dates, and
reports total round-trip costs of 5, 10, 15 and 25bps.

CSV prices use exact round-trip parsing and are checked against the archived
feature Parquet so an equality trigger does not shift through decimal parsing.
This replay is retrospective and does not promote a new default profile.
It leaves V13, the sealed baseline, canonical candles and prior reports intact.

## Test commands

```powershell
$v14Tests = @(Get-ChildItem tests/test_v14*.py | ForEach-Object FullName)
python -m pytest @v14Tests -q
```

Where the local sealed G-3 archive is available, baseline checks compare historical selections and all 93 archived executions, including fills, timestamps, stops, exits and portfolio P&L. The monitoring checks compare original fixed-grid observations and warning episodes. Tests also cover causal feature construction, data and cache integrity, missing execution bars, whole-date exclusions, selection independence from test returns, cost sensitivity, rate limiting, index-contract provenance and frozen-profile replay.
