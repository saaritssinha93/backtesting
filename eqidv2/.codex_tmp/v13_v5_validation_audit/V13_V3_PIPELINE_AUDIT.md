# V13-v3 pipeline, correctness, and reproducibility audit

Audit date: 2026-09-04 IST  
Scope: read-only audit of `fno_v13_corrected_v3_backtest.py` and behavior-bearing direct dependencies. No V13-v3 or shared source file was edited.

## Bottom line

The published V13-v3 arithmetic is internally reproducible from its current cache, and the signal/confirmation clock is causal. It is not yet a safe official F&O baseline for V13-v5, for four principal reasons:

1. The requested 15:30 square-off is not what most trades receive. Current raw equity data ends at 15:15 for every selected symbol-day from 2026-08-03 onward. The simulator silently uses the last available close. Sixty-nine of 79 selected paths end at 15:15, and 25 of 31 time exits are therefore 15:15 exits.
2. Cache reuse is not manifest-safe. V13-v3 searches all month caches in V3, V2, and V6, selects by coverage/candidate count/priority/mtime without checking the candidate's generating payload, then republishes it under the requested V3 key. It also rewrites its own cache on every hit; the NPZ write is non-atomic. During this audit, a concurrent reader observed a transient changed NPZ hash before restoration.
3. Despite the F&O name, prices, confirmations, entries, and exits are NSE cash-equity data. NFO futures contribute OI only. No option contract, CE/PE, strike, premium, lot, margin, or option fill is modeled. The reported results cannot be interpreted as options or futures trading P&L.
4. The output is a sum of independent trade percentage returns, not a capital-constrained portfolio. Same-symbol and overlapping positions are allowed, and sizing/leverage constants are unused.

The code's own report also states that the NIFTY rule and 10:00 leg were selected after inspecting the short history (`fno_v13_corrected_v3_backtest.py:667-669`). Therefore the 11-session `ORIGINAL_TEST` is a retrospective slice, not an untouched out-of-sample test.

## Source identity

V13-v3 source:

- Path: `fno_v13_corrected_v3_backtest.py`
- SHA-256: `85c2ff1c37a342e8e0bc4b73eb115de8ebc5aa7e990db261e46036b68aeafbab`
- Git status at audit start: untracked (`??`), so preservation depends on byte checksum rather than repository history.

Behavior-bearing source hashes:

| File | SHA-256 |
|---|---|
| `fno_v13_corrected_v2_backtest.py` | `5368fd36a2b67ce9b2513d3d1ae5ec3201baff93e9a01df25861c1df085c8a9a` |
| `fno_v6_corrected_backtest.py` | `06baf32c33156f21bce1dc786e5687a250b9711a1bca3a186283c824edfcf62d` |
| `fno_oi_ema_confirm_0925_0930_0935_0940_0945_v6.py` | `f62b920e32ec6f58aa0ab263b27a8fa3beb8534b1c303c20fa3c8660b420a430` |
| `fno_v5_hybrid_backtest.py` | `725e3174dec9855224e76c47164ae8445195c1b1214b1e228c5bd37c561849b4` |
| `fno_oi_ema_confirm_sweep.py` | `45d6e62652a359ac2efda6e497030dc4e0202d2460bd2aa1a24a8a4447f821a3` |
| `fno_oi_ema_confirm_backtest.py` | `b2e912f57c45fa56388e85100a66fa77d4df55ac3ccc571f8edb5c8035892602` |
| `fno_oi_hybrid_data.py` | `6c0c34487abd244f050c8ef1ab654ff089184c5deab135968eef5e83d2736ffa` |
| `fno_oi_common.py` | `cb96bd90874936b44c846f75c4a5a29be62a54d2431f34cccfD8ae9d736e26dd` |
| `fno_oi_backtest_provenance.py` | `4902b6a45b6d053fd2a777aa50b7c69280de459e682eac11086ec97d8551c049` |
| `fno_v5_live_config.py` | `daf4e70410ECB4553d43e95f6a50a246e25336750d328bde3bbf5d10c5a475cf` |
| `eqidv2_runtime_paths.py` | `94227ec082de0cf962539a8c640c9349e0a940439dc9802cdb698d3fd3927158` |

Case in a SHA-256 string is immaterial. The V13-v3 provenance records only the V13-v2 source hash, not this complete dependency set (`fno_v13_corrected_v3_backtest.py:906-949`).

Runtime used for the audit: Python 3.12.10, pandas 2.2.2, NumPy 2.0.1, pyarrow 23.0.1. No `EQIDV2*` or `FNO*` environment override was set in the audit shell.

## Concise pipeline trace

1. **Resolve roots and outputs.** `common.FNO_ROOT` derives from `EQIDV2_RUNTIME_ROOT`; V13-v3 writes to `.../strategy_research/v13_corrected_v3` (`eqidv2_runtime_paths.py:7-67`; `fno_oi_common.py:25-38`; `fno_v13_corrected_v3_backtest.py:55-75`). Equity 1m can be redirected at import time by `EQIDV2_FNO_V5_BACKTEST_EQUITY_1M_DIR` (`fno_oi_hybrid_data.py:24-35`).
2. **Resolve roll regimes and eligible sessions.** Contract expiry uses observed masters where available, otherwise an unadjusted last-Tuesday derivation; the nearest unexpired contract remains active on expiry day (`fno_v6_corrected_backtest.py:118-177`). The latest lexical dated universe holding a month is used for all sessions of that month (`fno_v6_corrected_backtest.py:183-200`). Eligibility counts contracts having any futures row that day, not complete or valid bars and not cash-equity coverage (`fno_v6_corrected_backtest.py:203-292`). V13-v3 normally reuses the pre-existing V6 eligibility CSV (`fno_v13_corrected_v3_backtest.py:395-411`).
3. **Map futures to cash equities.** A dated universe is enforced, but V13-v3 does not request persisted-only mapping, so current token-cache fallback remains allowed (`fno_v13_corrected_v3_backtest.py:347-350`; `fno_oi_backtest_provenance.py:140-263`). Current Aug/Sep universe rows have complete persisted mappings, so fallback did not alter this run.
4. **Load and align market data.** Cash 1m OHLCV is loaded from the mutable backtest root. Exact sets of five real end-labeled 1m rows are aggregated to an end-labeled cash 5m bar; flagged/nonfinite source rows are excluded from this aggregation (`fno_oi_hybrid_data.py:228-260`, `316-377`). Futures 5m files are end-labeled by adding five minutes to Kite candle starts (`fno_oi_common.py:694-720`). Cash 5m and futures OI join on exact timestamp (`fno_oi_hybrid_data.py:410-441`).
5. **Calculate features.** EMA 9/20/50 runs continuously across sessions. Price return uses the preceding retained 5m close; volume ratio uses the prior 20 retained bars with five-bar minimum (`fno_oi_hybrid_data.py:398-405`). Futures OI return uses `shift(1)` and positive finite OI, without an inline exact `t-5m` assertion (`fno_oi_hybrid_data.py:420-437`).
6. **Build survivor cache.** Only strict EMA-direction signals passing loose absolute price 0.10%, rising OI 0.05%, and volume 0.8x survive (`fno_oi_ema_confirm_sweep.py:50-60`, `231-240`). The cache scans 09:25 through 15:00 (`:66-67`). Pre-floor rejects are never serialized.
7. **Apply exact 1m confirmation.** A 5m signal at endpoint S uses the exact cash 1m candle ending S+1. V6 strict requires directional candle color and close displacement. Entry checks begin at S+2 (`fno_oi_ema_confirm_sweep.py:271-320`). V6 strict does not invoke the finite/source-flag validator that the V7 branch uses (`:250-290`).
8. **Store trigger and path.** Trigger is confirmation high for LONG and low for SHORT. Forward cache contains only cash-equity high, low, and close arrays, restricted to signal day and the requested time (`fno_oi_ema_confirm_sweep.py:306-351`; `fno_v6_corrected_backtest.py:314-336`). It omits forward timestamps and opens.
9. **Apply V13 rules.** V13-v2 changes 09:35 LONG OI to 0.15%, 09:40 LONG OI to 0.075%, caps every signal at OI <=1.00% before ranking, and adds 09:55 LONG (`fno_v13_corrected_v2_backtest.py:90-130`, `170-257`). V13-v3 first gates only 09:25 SHORT on NIFTY first-bar return <=-0.05%, then adds 10:00 LONG (`fno_v13_corrected_v3_backtest.py:126-181`, `184-264`, `433-455`).
10. **Filter and rank.** Each setup applies price, OI, volume, confirmation body, directional wick, and traded-value conditions as one AND mask, then ranks within setup/day and takes `max_entries` (`fno_v5_hybrid_backtest.py:29-73`). There is no portfolio-level cap, symbol lock, cooldown, or open-position state.
11. **Fill and exit.** First future 1m high/low touch fills exactly at trigger. Stop and target are fixed percentages from trigger. The entry bar is immediately eligible for both; stop wins equal-index ties. If neither hits, `close[-1]` is used (`fno_oi_ema_confirm_sweep.py:360-410`). One fixed round-trip cost is subtracted.
12. **Aggregate and report.** Per-trade percentage returns are summed by day and cumulatively; PF is positive-return sum / absolute negative-return sum (`fno_v5_hybrid_backtest.py:139-197`). V13-v3 computes additive daily-curve drawdown (`fno_v13_corrected_v3_backtest.py:108-123`) and overwrites a fixed set of result files (`:873-901`).

## Verified current-run facts

These are empirical checks against the current published artifacts and raw store, not just source-code inspection:

- Published trade ledger SHA-256: `104a127a4b10474ea2fdf70afa440e1ca4668de411eff59ee7cba02e8587a7aa`.
- Published daily ledger SHA-256: `4ece33e4e5e4e630bfb7a7b98c74918ee003fd685e062098b314f36d2632adce`.
- Provenance SHA-256: `d4caa4fdaffe9390276a758b66fe18132f78da4fbf3d6ddeb1632ea00f799f06`.
- Cache pairs currently match provenance: Aug parquet/NPZ `9f89e573...` / `a324a96c...`; Sep parquet/NPZ `1917d345...` / `a2b0c856...`.
- All 4,025 cached candidates have finite positive OI and exactly match raw same-contract, same-session `t` and `t-5m` OI values/formula.
- All 79 selected signal rows have exact cash 5m predecessor rows and reproduce cached price returns within float-storage tolerance.
- All 4,025 current raw confirmation candles exist at the exact timestamp, are finite/valid, and have no `gap_filled`, `opening_snapshot`, or `provisional_stale` flag.
- All 25 required NIFTY 09:20 rows are unique, positive/finite, quality `VALID`, and have `timestamp - candle_start == 5m`; no NIFTY context is missing.
- Candidate `sid` duplicates: 0. Candidate day/time/side/symbol duplicates: 0. Selected order-key duplicates: 0.
- Native raw replay matches every published return with maximum absolute delta `2.20e-14`.
- Current baseline has no first stop/target hit on the same minute and no fill bar that also touches stop or target. The generic engine remains ambiguous for other brackets/configurations.
- Three of 78 fills gap through the stop-entry trigger. When filled at actual 1m open and brackets are rebased to that fill, all three retain the same fixed target/stop result, so PF/net/win rate do not change at `1e-10`.
- Repeated-symbol exposure is real: ten overlapping trade pairs across six symbol-days. Maximum portfolio concurrency is seven positions.

## Correctness defects

### 1. Requested 15:30 becomes silent last-available close

Evidence:

- CLI/default and provenance say `1530` (`fno_v13_corrected_v3_backtest.py:735-748`, `927-932`).
- Path construction keeps whatever rows exist with `hhmm <= square_off` but never asserts that the requested terminal bar exists (`fno_oi_ema_confirm_sweep.py:306-320`).
- The exit engine uses `close[-1]` when no bracket hits (`fno_oi_ema_confirm_sweep.py:400-409`).
- Among 71 unique selected symbol-days, current raw data ends exactly 15:30 for ten (all on Jul 29-31) and exactly 15:15 for 61 (every selected symbol-day Aug 3-Sep 3). There are no interior minute gaps before the last row.
- This affects 69/79 orders and 25/31 square-off exits.

Recommended V5 treatment: use an explicit 15:15 cutoff for the current dataset and require the exact terminal bar. A uniform 15:15 plus gap-open replay changes five July square-offs and yields 78 fills, 56.4103% WR, PF 2.807527, net +46.692811%, expectancy +0.598626%, and unchanged -3.041137% additive DD. Classify this as a correctness normalization, not alpha. If the intended live rule is truly 15:30, backfill 15:16-15:30 and fail closed until complete; do not impute or silently use 15:15.

### 2. Cache identity is unchecked and cache writes are unsafe

`_load_or_build_regime` scores every month cache in three directories and never reads/verifies its manifest against `_cache_payload` (`fno_v13_corrected_v3_backtest.py:267-345`). A different universe, root, source version, time cutoff, or signal policy can therefore be selected and copied under the requested key. The current selected V2 seed bytes happen to be the exact expected bytes, so no current result mismatch was found.

Even on a cache hit, V13-v3 unconditionally rewrites the V3 cache (`fno_v13_corrected_v3_backtest.py:365-380`). Parquet uses an atomic helper, but the companion NPZ is written directly by `np.savez_compressed` (`fno_v6_corrected_backtest.py:328-336`). During this audit a concurrent reader observed a transient Sep NPZ hash `bc9a92fc...` before the protected hash `a2b0c856...` was restored; its modification time changed. This is a demonstrated read/write race, not hypothetical.

V5 must require an exact manifest match, never rewrite a cache hit, create both files under temporary names, validate them, and publish the pair atomically or behind a run lock.

### 3. Declared eligibility parameter can be inactive/misreported

When V6 eligibility exists, V13-v3 reuses it without checking which `min_coverage` produced it (`fno_v13_corrected_v3_backtest.py:395-411`). The published V3 provenance records requested `0.99`, but the reused V6 provenance says `0.80`. All 25 used rows happen to have coverage 1.0, so this did not change the current sample; it still makes the parameter/provenance contract false. Blank `through_day` likewise uses the stale eligibility maximum rather than discovering new sessions (`:761-767`).

### 4. Eligibility checks existence, not completeness

`contract_sessions` counts a contract as covered if any futures timestamp exists that day (`fno_v6_corrected_backtest.py:203-224`). It does not require exact signal bars, valid quality, a complete day, or any cash-equity data. This explains why 100% contract coverage did not reveal the 15:15 equity truncation. V5 needs per-required-timestamp validation for both cash and futures sources.

### 5. Eligible zero-candidate sessions can disappear

V13-v3 replaces the eligible-session list with `days = sorted(set(signals["day"]))` (`fno_v13_corrected_v3_backtest.py:794-798`). V13-v2 explicitly flags unreplayed days (`fno_v13_corrected_v2_backtest.py:1028-1035`), but V13-v3 omits that guard. This biases sessions, trades/day, flat days, and drawdown duration whenever an otherwise eligible day produces no cache survivor. It has no current impact because all 25 eligible days have at least one cached candidate.

### 6. V3 omits V2's exact OI validation

V13-v2 validates selected OI formula and raw exact `t/t-5m` continuity (`fno_v13_corrected_v2_backtest.py:573-656`, `1051-1057`). V13-v3 does not call either guard. The independent audit found all 4,025 current cache rows valid, but V5 should fail closed rather than rely on an external audit.

### 7. 09:35 SHORT is effectively dead under the global cap

The inherited setup requires OI >=1.00%, while the V13 policy first requires OI <=1.00%. Only floating-point exact equality can survive. In this cache, all 18 rows surviving its price test fail the lower bound after the cap; it produces zero orders. This is a conflicting rule introduced by composition, not evidence that the side/time has no opportunity.

### 8. Provenance cannot recreate the run

The JSON omits the V13-v3 source hash, most dependency hashes, raw equity/futures/NIFTY file identities, environment overrides, package versions, exact command, and an immutable eligibility-input hash (`fno_v13_corrected_v3_backtest.py:906-949`). Fixed output filenames overwrite prior runs, and files are not published transactionally. The V13-v2 parity check compares shared days only and is optional when its published ledger is missing (`:543-581`, `808-810`), so partial overlap can pass.

## Execution/model limitations (not observed arithmetic failures)

- **No direct candle look-ahead was found.** A 09:25 5m bar is complete at 09:25; exact confirmation ends 09:26; first entry bar ends 09:27. NIFTY 09:20 is known before the 09:25 decision. Same-session path filtering prevents overnight leakage.
- **Selection leakage remains.** The V3 report admits the two additions were chosen after observing this history. No current slice is untouched, regardless of its `TEST` label.
- **Universe as-of risk.** Aug uses a 2026-08-21 snapshot for Jul 29-Aug 21; Sep uses 2026-09-03 for Aug 26-Sep 3. Available Aug stock membership is stable across snapshots (only an excluded index future differs); Sep symbol sets are identical. Thus risk exists but no current stock-membership effect was demonstrated.
- **Mutable mapping/input risk.** V13-v3 allows legacy current-token fallback, although all current selected universe mappings are persisted and complete. Raw data revision/as-of state is not frozen or hashed.
- **Strict confirmation lacks a quality guard.** The V6 strict branch ignores optional source flags and does not call the full finite OHLCV validator. Current confirmations all pass an independent check, so no current metric effect exists.
- **Exact-trigger fill is optimistic on gaps.** Three current fills have adverse gap-through of 1.77-7.21 bps. Rebased bracket outcomes happen not to change, but exact trigger prices, queue, spread, partial fills, tick rounding, and latency are not modeled.
- **The order never expires before session end.** A setup can fill hours after confirmation. There is no entry window, anti-chase rule, or cancellation other than the path cutoff.
- **OHLC sequencing is unknowable.** The engine checks stop/target from the fill bar and chooses stop on same-index ties. This is conservative for ties but cannot determine whether a bar traded stop before entry. No such event affects the native baseline, but tighter/new V5 brackets can create it.
- **Forward cache loses observability.** H/L/C-only NPZ arrays omit opens and timestamps, so a frozen replay cannot prove gap fills, exact wall-clock duration, internal gaps, or requested terminal time.
- **Costs are simplified.** `cost_bps/10000` is subtracted once from each filled trade and the arithmetic is correct. Brokerage, STT, exchange charges, GST, stamp duty, bid/ask spread, market impact, and side-specific costs are not represented.
- **Portfolio state is absent.** Six symbol-days contain overlapping repeated entries; maximum global concurrency is seven. Live config's Rs10,000 capital, 5x leverage, and Rs50,000 exposure values are unused. Additive `%` net and drawdown are therefore trade-score statistics, not deployable portfolio returns.
- **Feature regime choices are implicit.** EMAs are continuous across days; volume ratio mixes time-of-day and prior-session bars. These are causal choices, but they can make early-session thresholds behave differently from their labels.
- **This is cash-equity execution with futures OI.** It cannot answer CE/PE, strike, option expiry/price, options liquidity, futures lot/margin, or option P&L questions without a different data/execution layer.

## Published V13-v3 baseline

The table below reproduces the currently published 5 bps model; it is not corrected for the 15:15/15:30 inconsistency.

| Metric | Published value | Interpretation |
|---|---:|---|
| Sessions | 25 | Jul 29-Sep 3 |
| Survivor-cache rows | 4,025 | already passed loose 5m + strict 1m confirmation |
| Active time-side survivor scope | 921 | before V13/setup filters |
| Selected orders | 79 | setup/day ranked orders |
| Fills | 78 | 3.12/day |
| Win / loss / breakeven | 44 / 34 / 0 | after 5 bps |
| Win rate | 56.410256% | net-sign definition |
| Target / stop / square-off | 17 / 30 / 31 | 21.7949% / 38.4615% / 39.7436% of fills; replay-derived |
| Net | +46.034491% | sum of trade returns, not portfolio % |
| Gross profit / loss | +72.040805 / -26.006314 points | after fixed cost |
| Profit factor | 2.770127 | trade-return PF |
| Mean trade / average win / average loss | +0.590186% / +1.637291% / -0.764892% | equal-notional assumption |
| Payoff ratio | 2.140553 | average win / absolute average loss |
| Max drawdown | -3.041137% | additive daily curve |
| Longest underwater run | 4 sessions | additive reported curve |
| Max win / loss streak | 7 / 4 | signal-order convention; overlapping trades |
| Mean / median hold | 210.154 / 229 minutes | reconstructed from current mutable raw timestamps |
| Before fixed 5 bps: net / PF | +49.934491% / 3.054384 | adds 0.05 point per fill; still no real charges |

Period slices:

| Period | Sessions | Fills | Win rate | PF | Net | DD |
|---|---:|---:|---:|---:|---:|---:|
| Original train | 12 | 41 | 58.5366% | 2.991942 | +27.503228% | -1.050000% |
| Original test | 11 | 35 | 57.1429% | 2.899343 | +20.131263% | -3.041137% |
| Sep 2+ | 2 | 2 | 0.0000% | 0.000000 | -1.600000% | -1.600000% |

The Sep 2+ cell has only two fills and cannot validate or invalidate a rule.

The complete field-by-field support assessment is in `baseline_metric_support_matrix.csv`.

## Parameter and rule registry

The complete global registry is in `parameter_rule_registry.csv`; all 12 setup rows and runtime effects are in `active_setup_registry.csv`.

Active setup summary:

| Setup | Side | Price | OI min | Vol | Body | Wick max | Picker / max | SL / target | Orders / fills |
|---|---|---:|---:|---:|---:|---:|---|---|---:|
| 09:25/09:26 | LONG | 0.30 | 0.10 | 3.0 | 0.60 | 0.50 | liquidity / 1 | 0.50 / 3.00 | 11 / 11 |
| 09:25/09:26 | SHORT | 0.20 | 0.10 | 1.5 | 0.40 | 0.50 | volume / 2 | 0.75 / 3.00 | 12 / 12 |
| 09:30/09:31 | LONG | 0.65 | 0.10 | 1.0 | 0.50 | 0.50 | move / 1 | 1.00 / 2.50 | 7 / 7 |
| 09:30/09:31 | SHORT | 0.20 | 0.25 | 1.0 | 0.40 | 0.50 | move / 1 | 1.00 / 3.00 | 5 / 5 |
| 09:35/09:36 | LONG | 0.20 | 0.15 | 1.0 | 0.60 | 0.50 | liquidity / 1 | 1.00 / 2.50 | 9 / 9 |
| 09:35/09:36 | SHORT | 0.50 | 1.00 | 1.0 | 0.40 | 0.50 | liquidity / 2 | 1.00 / 3.00 | 0 / 0 |
| 09:40/09:41 | LONG | 0.20 | 0.075 | 2.0 | 0.50 | 0.50 | liquidity / 1 | 0.50 / 2.50 | 8 / 8 |
| 09:40/09:41 | SHORT | 0.20 | 0.10 | 1.0 | 0.40 | 0.50 | move / 1 | 1.00 / 3.00 | 7 / 7 |
| 09:45/09:46 | LONG | 0.65 | 0.10 | 1.0 | 0.40 | 0.50 | move / 1 | 1.00 / 3.00 | 1 / 1 |
| 09:45/09:46 | SHORT | 0.20 | 0.75 | 1.0 | 0.40 | 0.30 | volume / 1 | 1.00 / 2.00 | 0 / 0 |
| 09:55/09:56 | LONG | 0.20 | 0.10 | 1.0 | 0.40 | 0.50 | liquidity / 1 | 1.00 / 3.00 | 10 / 10 |
| 10:00/10:01 | LONG | 0.40 | 0.05 | 1.0 | 0.40 | 0.50 | liquidity / 1 | 1.00 / 3.00 | 9 / 8 |

The setup filter funnel from the 921 active-scope survivor rows is:

`921 -> NIFTY 876 -> OI cap 844 -> price 415 -> setup OI 222 -> volume 161 -> body 102 -> wick 101 -> traded value 101 -> per-setup cap 79 -> fill 78`

This is an audit ordering, not production short-circuit ordering: `_eligible` evaluates setup filters as one vectorized AND, so per-filter attribution changes if the audit order changes.

## What current data can and cannot support

Directly supported by published V3 ledgers: selected/fill counts, net-sign wins/losses, trade-return PF, additive daily DD, day/setup/signal-time/side breakdowns, and fixed setup parameters.

Derivable but not logged: target/stop/time-exit counts and MAE/MFE from frozen HLC paths; week/month/weekday and expiry/DTE via joins.

Conditionally derivable but not reproducibly frozen: actual fill/exit timestamps, holding time, gap-aware entry, and opening-gap context from the current raw 1m files. Those files were not hashed in V3 provenance.

Unsupported: true rupee P&L; capital/margin-constrained portfolio return; separate brokerage/tax/slippage; bid/ask or queue fills; CE/PE; option strike/premium/expiry alignment; complete pre-cache rejection counts; and counterfactual P&L for candidates discarded before serialization.

## Required V5 correctness gate before optimization

1. Freeze an explicit data snapshot and hash every used cash 1m, futures 5m, NIFTY 5m, universe, eligibility, and mapping input.
2. Pin all behavior-bearing source hashes and record the exact command, environment overrides, and package versions.
3. Adopt explicit 15:15 with exact terminal-bar assertions for this dataset, or backfill to 15:30 and fail closed. Never use an undeclared last available close.
4. Store forward timestamp and open arrays; fill a gap-through stop order at the worse of trigger/open plus configured slippage, with tick rounding.
5. Require exact `t/t-5m` cash and futures pairs, full real confirmation validation, and explicit path continuity.
6. Require persisted-only point-in-time mappings and prove session-as-of universe membership.
7. Retain every eligible session, including zero-candidate and zero-trade days, with explicit data-failure reasons.
8. Resolve the 09:35 SHORT OI-min/cap conflict and label the 09:45 SHORT zero-sample setup.
9. Define order expiry, repeat-symbol/pyramiding rules, portfolio concurrency, capital allocation, quantity/lot rounding, and real charge components.
10. Separate immutable baseline reproduction from experimental run directories. Do not reuse the old `ORIGINAL_TEST` as a validation claim; wait for genuinely new sessions/expiry regimes.

## Audit artifacts and commands

- `V13_V3_PIPELINE_AUDIT.md` - this report.
- `parameter_rule_registry.csv` - global parameters/rules, source lines, runtime effect, observed impact, and V5 handling.
- `active_setup_registry.csv` - all 12 active setup parameter rows and observed order/fill effects.
- `baseline_metric_support_matrix.csv` - requested baseline fields classified as direct, derivable, limited, unsupported, or not applicable.
- `read_only_diagnostics.py` - raw confirmation/path/fill ambiguity diagnostics; it does not write data.
- `corrected_execution_replay.py` - raw 1m replay supporting explicit cutoff and gap-open/rebased-bracket execution; it does not write data.

Run from the repository root:

```powershell
python -c "import runpy; runpy.run_path(r'.codex_tmp\v13_v5_validation_audit\read_only_diagnostics.py', run_name='__main__')"
python -c "import runpy; runpy.run_path(r'.codex_tmp\v13_v5_validation_audit\corrected_execution_replay.py', run_name='__main__')"
Get-FileHash -Algorithm SHA256 fno_v13_corrected_v3_backtest.py
```

