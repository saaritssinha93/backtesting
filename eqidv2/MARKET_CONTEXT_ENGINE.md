# EQID V7/V11/V12 Market Context Engine

## Decision

The context layer must remain separate from setup detection and execution.
It produces point-in-time numeric/categorical features only. It does not emit a
side, signal, order, entry, stop, target, quantity, or position.

Keep the legacy V2 `market_ctx` unchanged because V7/V11/V12 setup detection
already consumes its NIFTY return/regime. Replacing that dictionary would change
the candidate population and violate the requirement not to redesign the
strategy. Add this engine after candidates exist, under the `mce_*` namespace.

Implementation: `market_context_engine.py`.

Tests: `tests/test_market_context_engine.py`.

The complementary stock-within-sector layer is implemented in
`sector_intelligence.py` and documented in `SECTOR_INTELLIGENCE.md`. It remains
a separate feature result so the original market-context API and setup logic do
not change.

## Repository-specific findings

- The scheduled V7 path is `eqidv2_signal_discovery_v7_5min_id_persistent.py`.
  It uses V7 mechanics and the V11 working setup book.
- The active V7 `ranker_score` is a deterministic heuristic, not a fitted ML
  model. The separate `ml_meta_filter.py` path has five hard-coded features and
  is not active in scheduled V7 discovery. A context-aware model therefore needs
  one shared, ordered training/inference schema before it can affect live scores.
- Existing general context is NIFTYBEES-first intraday return plus a coarse
  regime. `market_breadth` is populated only for the late-BB10 setup and means
  fraction above causal AVWAP, not general market breadth.
- The live store has no dedicated Bank Nifty or Midcap index series. The engine
  leaves their scores missing and marks context incomplete; it does not invent a
  constituent proxy.
- `configs/sector_etf_map.json` covers only about 9% of the current live universe
  and contains placeholder ETF mappings. Sector ranks are research-only until a
  point-in-time sector master is supplied.

## Input and output contract

`stock_bars` is a long table with one row per stock and completed 5-minute bar:

```text
date, ticker, open, high, low, close, volume
optional: sector, EMA_20, EMA_50, vol_ratio, opening_snapshot,
          gap_filled, source_1m_count, is_eligible, universe_expected
```

Any supplied EMA, ATR, relative-volume, eligibility, universe, or sector field
must itself have point-in-time/as-of provenance. The engine cannot detect a
future-aware upstream calculation; where supplied EMA/ATR values are absent it
uses its causal fallback instead.

`index_bars` has the same OHLCV shape and a ticker identifying NIFTY, Bank
Nifty, or Midcap. Official cash-index series are preferred; ETF aliases are
fallbacks. Each physical source is prepared independently so unlike price
scales cannot be mixed.

For changing historical universes, repeat that timestamp's point-in-time
`universe_expected` on the available rows. Without it or a configured live
size, the fallback is a causal running maximum of observed names—not a claim
that the inferred universe is historically complete.

The result has:

- `market`: primary key `(feature_version, timestamp)`;
- `sectors`: primary key `(feature_version, timestamp, sector)`;
- `timestamp`: completed, end-stamped 5-minute bar time in IST;
- `available_at`: earliest time the row may be joined to a decision;
- source timestamps, coverage, readiness, and version fields.

Marked 09:15 opening snapshots are removed completely. Partial 5-minute bars
are not eligible. A backward as-of join has a seven-minute default staleness
limit and never looks forward.

## Minimal Python use

```python
from pathlib import Path

from market_context_engine import (
    MarketContextConfig,
    MarketContextEngine,
    attach_context_asof,
    load_sector_map,
)

sector_map = load_sector_map(Path("eqidv2/configs/sector_etf_map.json"))

engine = MarketContextEngine(
    MarketContextConfig(
        expected_universe_size=1235,  # use the exact feed manifest, not a glob
        publish_delay_seconds=0,      # backtest convention; stamp real time live
        min_market_coverage=0.70,
        min_sector_coverage=0.70,
    ),
    sector_map=sector_map,
)

context = engine.compute(stock_bars, index_bars)

# This only appends prefixed features. It neither removes nor selects a row.
enriched_candidates = attach_context_asof(
    candidates,
    context,
    candidate_time_col="signal_time_ist",  # use decision_ready_at_ist live
    sector_map=sector_map,
    prefix="mce_",
)
```

For live use, `available_at` should be the actual atomic publication time and
the candidate join key should be the real decision-ready time. A zero-delay
bar-time convention is suitable only when the research contract explicitly
assumes inference immediately after the completed bar.

## Mathematical definitions and predictive purpose

All fractions below are emitted as percentages in columns named `pct_*`; the
composites use fractions in `[0,1]`. Missing components are not filled from the
future. Composite weights are renormalized over available components and the
component/readiness counts make that fact observable.

### 1. Advance/decline state

For point-in-time universe `U_t`, let `P_i,t` be the latest usable price and
`C_i,d-1` the previous completed session close:

```text
A_t = sum_i 1[P_i,t > C_i,d-1]
D_t = sum_i 1[P_i,t < C_i,d-1]
U_t = sum_i 1[P_i,t = C_i,d-1]

advance_decline_ratio     = (A_t + 0.5) / (D_t + 0.5)
advance_decline_log_ratio = log(advance_decline_ratio)
advance_decline_net       = (A_t - D_t) / (A_t + D_t + U_t)
```

The `0.5` Jeffreys smoothing avoids infinity when no stock declines. The raw
ratio is intuitive; the log ratio is better behaved for ML. Breadth can expose
weakness hidden by a few large-cap index constituents, which is especially
relevant to a 1,200-stock long scanner.

### 2. Percentage above session VWAP

For typical price `TP_i,k = (H_i,k + L_i,k + C_i,k)/3`:

```text
VWAP_i,t = sum_{k<=t} TP_i,k * V_i,k / sum_{k<=t} V_i,k
p_vwap,t = (1 / N_valid,t) * sum_i 1[P_i,t > VWAP_i,t]
```

Only completed, flow-valid bars add volume. A gap-filled last price can remain
in price breadth but cannot add fictitious volume. VWAP participation measures
whether strength is broadly above the session's volume-weighted cost basis,
not merely whether the capitalization-weighted index is green.

### 3. Percentage above EMA20 and EMA50

For span `n`, `alpha_n = 2/(n+1)`:

```text
EMA_n,i,t = alpha_n * P_i,t + (1-alpha_n) * EMA_n,i,t-1
p_ema_n,t = (1 / N_n,t) * sum_i 1[P_i,t > EMA_n,i,t]
```

EMA20 breadth captures faster participation; EMA50 breadth captures more
persistent structure. Their disagreement is useful: a short-lived rebound can
lift EMA20 breadth while EMA50 breadth remains poor.

### 4. New intraday highs/lows

```text
new_high_i,t = 1[H_i,t > max_{session open <= k < t} H_i,k]
new_low_i,t  = 1[L_i,t < min_{session open <= k < t} L_i,k]

p_new_high,t = mean_i(new_high_i,t)
p_new_low,t  = mean_i(new_low_i,t)
```

The first completed bar has no prior intraday extreme and is excluded from this
denominator. Expanding new highs measure participation in price discovery;
new lows distinguish genuine broad risk-off moves from isolated weakness.

### 5. Composite market breadth

Define centered breadth components:

```text
b_AD    = advance_decline_net
b_VWAP  = 2*p_vwap - 1
b_E20   = 2*p_ema20 - 1
b_E50   = 2*p_ema50 - 1
b_HL    = p_new_high - p_new_low

market_breadth =
    0.30*b_AD + 0.25*b_VWAP + 0.20*b_E20 + 0.15*b_E50 + 0.10*b_HL
```

The range is approximately `[-1,+1]`. `breadth_thrust_15m` is the within-day
three-bar change in this composite. A long setup supported by expanding breadth
has a different conditional distribution from the identical stock pattern in a
narrow or deteriorating tape.

### 6. NIFTY, Bank Nifty, and Midcap trend scores

For each index independently, let `r_h,t = log(P_t/P_t-h)`, let `sigma_t` be
the rolling standard deviation of intraday one-bar log returns, let `ATR_t` be
Wilder-style ATR, and let `Q_t` be session VWAP (or expanding TWAP when an
official cash index has no volume):

```text
z_3  = r_3  / (sigma_t * sqrt(3))
z_12 = r_12 / (sigma_t * sqrt(12))
a_t  = (P_t - Q_t) / ATR_t
e_t  = (EMA20_t - EMA50_t) / ATR_t
ER_t = r_12 / sum_{j=t-11..t} |r_1,j|

trend_score_t = 100 * [
    0.30*tanh(z_3)
  + 0.25*tanh(z_12)
  + 0.20*tanh(a_t)
  + 0.15*tanh(e_t)
  + 0.10*ER_t
]
```

The score is bounded near `[-100,+100]`; finite components are reweighted
during warm-up. Standardized returns supply direction, the anchor and EMA
spread supply structure, and the efficiency ratio separates directional travel
from a noisy path. Three indices distinguish broad-market, financial, and
mid-cap beta. This is a regime descriptor, not a trade rule.

The combined index score is:

```text
combined = 0.50*T_nifty + 0.30*T_bank + 0.20*T_midcap
```

with available-weight normalization. A trend regime is not emitted until at
least two sources have a full 12-bar intraday history.

### 7. Sector momentum, relative volume, strength, and rank

For sector `s`, using member-level 30-minute log returns:

```text
M_s,t  = median_{i in s}(100 * log(P_i,t / P_i,t-6))
RM_s,t = M_s,t - median_i(100 * log(P_i,t / P_i,t-6))

RVOL_i,t = V_i,t / median(previous 20 sessions' V_i at same bar slot)
RVOL_s,t = median_{i in s}(RVOL_i,t)
```

Let `z_cs(.)` be a same-timestamp cross-sector z-score, and `B_s,t` the
sector's fraction above VWAP:

```text
raw_strength_s,t =
    0.55*z_cs(RM_s,t)
  + 0.25*z_cs(B_s,t)
  + 0.20*z_cs(log(RVOL_s,t))

sector_strength_score = 100*tanh(raw_strength_s,t / 2)
```

Ranks are descending across reliable sectors. Momentum identifies leadership;
relative volume distinguishes active sponsorship from a low-participation move;
VWAP breadth checks whether leadership is broad inside the sector. The candidate
receives its own sector row as additional model inputs.

### 8. Intraday realized-volatility regime

For the last 12 NIFTY 5-minute log returns:

```text
RV_60m,t = 10,000 * sqrt(sum_{j=t-11..t} r_j^2)
```

The value remains missing until all 12 intraday returns exist (13 completed
price observations); a shorter warm-up window is never labeled as 60 minutes.

At each bar-of-day, this is standardized only against prior sessions at the
same slot:

```text
vol_z_t = (RV_60m,t - mean_prior_slot) / std_prior_slot
```

`HIGH`, `NORMAL`, and `LOW` use thresholds `+0.75/-0.75`; `WARMUP` and
`UNKNOWN` remain distinct with missing numeric regime codes. Volatility changes
breakout follow-through, reversal risk, slippage, and the signal-to-noise ratio.
The realized-variance construction follows the standard high-frequency
quadratic-variation estimator. The z-score also remains missing if the causal
baseline has insufficient history or zero variance; zero is reserved for a
genuinely neutral observed value.

### 9. Trend regime

```text
agreement_t = |sum sign(T_j,t)| / number_available
```

`UPTREND` requires combined score at least `+20`, agreement at least `0.5`, and
two ready indices. `DOWNTREND` is symmetric; otherwise the state is
`RANGE_MIXED`. Long-only models can learn that the same stock-level setup has
different odds across these states without the engine suppressing a trade.

### 10. Rotation regime

Let `D_t` be the cross-sector standard deviation of sector momentum, `z_D,t`
its causal same-slot z-score, and `Q_t` the mean absolute change in sector
strength percentile over three bars:

```text
rotation_score = 100 * sum_{k in available} w_k*x_k
                       / sum_{k in available} w_k

x = [logistic(1.5*z_D,t), Q_t, 1 - |market_breadth_t|]
w = [0.45, 0.35, 0.20]
```

High dispersion, changing ranks, and low market coherence describe rotation.
Strong coherent breadth with ordinary dispersion describes a broad trend.
Rotation context helps the model distinguish an isolated sector leader from a
market-wide momentum event. Before the three-bar turnover lag exists, that
component is unavailable and the remaining weights are renormalized; it is not
silently treated as zero rotation.

### 11. Risk-on/risk-off score

```text
I_t = combined_index_trend_score / 100
B_t = market_breadth
S_t = 2*sector_positive_share - 1
M_t = tanh((T_midcap - T_nifty)/40)
V_t = -tanh(vol_z/2)

risk_on_off_score = 100 * [
    0.30*I_t + 0.30*B_t + 0.15*S_t + 0.15*M_t + 0.10*V_t
]
```

The score is reweighted over available components and emits a component count.
Positive index structure, broad participation, positive sector diffusion,
midcap leadership, and non-stressed volatility move it risk-on. It is a compact
interaction feature; the underlying components should also be retained so the
model is not forced to accept these fixed weights.

## Recommended ML feature set

Do not feed every numeric output blindly. That would include duplicate
fraction/percentage representations, raw counts, raw volumes, and quality
metadata.

Suggested market features for a first ablation:

```text
nifty_trend_score
bank_nifty_trend_score
midcap_trend_score
combined_index_trend_score
index_trend_agreement
advance_decline_log_ratio
advance_decline_net
pct_above_vwap
pct_above_ema20
pct_above_ema50
pct_new_intraday_highs
pct_new_intraday_lows
up_volume_fraction
market_breadth
breadth_thrust_15m
cross_sectional_return_dispersion
intraday_volatility_z
sector_positive_share
sector_momentum_dispersion
sector_rank_turnover_mean
rotation_score
risk_on_off_score
```

Candidate-sector additions:

```text
sector_momentum_30m_pct
sector_relative_momentum_pct
log(clipped sector_relative_volume)
sector_pct_above_vwap
sector_strength_score
sector_strength_percentile
sector_rank_turnover
```

Coverage, age, source-readiness, and `context_complete` are QA/missingness
inputs, not alpha claims. Missing values must remain distinguishable from
neutral values. Fit imputation, clipping, scaling, and categorical encoding on
the training fold only.

## Five-minute computation design

### Historical/V11/V12

1. Build a long stock-by-slot panel once from the exact point-in-time universe.
2. Compute market and sector tables vectorially.
3. Persist append-only, date-partitioned Parquet artifacts.
4. Join candidate rows backward on `available_at` with a strict staleness bound.
5. Reuse the artifacts for every model experiment; do not recompute them inside
   each feature trial.

### V7 live

The active scanner already reads and prepares every symbol once per slot. The
lowest-latency design is to return a compact feature contribution from each
worker even when no setup fires, reduce the 1,235 contributions once in the
parent, and publish one atomic context snapshot. Do not start a second pass over
1,200 Parquet files.

The current implementation is a vectorized batch/reference engine, not a
persisted incremental state machine. Its `latest()` method computes a supplied
panel and then selects the latest published row. Production should retain:

- per-stock EMA/session VWAP/previous-close/intraday-extreme state;
- 30-minute close ring buffers;
- 20-session same-slot volume buffers;
- 60-session same-slot volatility/dispersion summaries;
- prior sector percentile ranks.

A local synthetic benchmark of 1,200 symbols, five sessions, and 450,000 rows
with supplied EMA/RVOL fields completed in about 13.1 seconds, or roughly 34,000
stock rows/second (excluding I/O), well inside a five-minute interval. It is
still preferable to reuse live worker frames and persisted state rather than
repeatedly read the full store.

Persist a sidecar manifest with input-universe hash, sector-map hash/effective
date, code commit, index sources, observed/eligible counts, actual publish time,
and feature version.

## Safe integration points

### Active V7 live

1. Leave `avwap_5min_ID_v7_candidate_scan.py` and its legacy `market_ctx`
   detector inputs unchanged.
2. In `eqidv2_signal_discovery_v7_5min_id_persistent.py`, attach `mce_*` only
   after setup/risk/config gates and the entry-window filter, before candidate
   snapshot publication and before any new ML inference hook.
3. Run the new probability in shadow mode first. Do not use a score threshold,
   sizing multiplier, or hard regime gate during validation.
4. Assert candidate IDs and order are identical immediately before and after
   enrichment.

If inference is moved into the entry engine, note that its row builder currently
reconstructs an explicit schema and drops arbitrary columns. Either score before
that boundary or explicitly carry an immutable context payload.

### V11/V12 backtests

Join context after historical candidate generation and before ML fit/score. Do
not join it into setup detection. Cache and replay paths must use the same
point-in-time join. Preserve a pre-enrichment candidate-ID checksum to prove the
candidate set is unchanged.

For the old five-feature meta-label path, all four contracts must change
together: label dataset serialization, training matrix construction, inference
row construction, and the exported ordered feature list. A mismatch must fail
closed to the baseline model; it must never silently fill all new features with
zero.

## Backtest every feature individually

Use one frozen candidate pool and one fixed label/execution contract. The test
is `baseline model` versus `baseline + exactly one context feature`, not a
standalone feature threshold.

1. **Freeze candidates.** Generate V7/V11/V12 long candidates once without MCE
   influence. Store candidate IDs, signal times, setup, and resolved net-of-cost
   outcomes. Every experiment uses identical rows.
2. **Build context point-in-time.** Use the historically valid universe and
   sector membership for each date. Join backward on `available_at`; never use
   nearest or forward joins.
3. **Freeze folds.** Use chronological purged walk-forward folds. Remove training
   events whose outcome horizon overlaps test and set embargo at least as long
   as the maximum holding/label horizon.
4. **Fit preprocessing inside train.** Imputation with a missing indicator,
   winsorization, standardization, categorical one-hot encoding, calibration,
   and any score threshold are learned on train only.
5. **Fit two models per fold.** Model A uses the existing ordered feature set;
   Model B adds one feature. Keep estimator, regularization, seed, class weights,
   and candidate weights identical.
6. **Compare paired OOF prediction metrics.** Report delta log loss, Brier score,
   PR-AUC, ROC-AUC, calibration intercept/slope, and decile monotonicity. Brier
   and log loss matter more than AUC if probabilities drive ranking.
7. **Compare economics without changing execution.** Learn the selection
   threshold or top fraction on train. On test, keep the same candidate budget
   and unchanged entry/exit/cost model. Report net PF, expectancy, P&L, hit rate,
   max drawdown, trade count, turnover, and setup/time-bucket stability.
8. **Test uncertainty.** Bootstrap paired Model-B minus Model-A results in blocks
   of trading days. Apply Benjamini-Hochberg correction across the feature trials.
9. **Run placebos.** Verify future-bar perturbation invariance, test a one-bar-lag
   version, and shuffle the feature within date/time buckets. An alleged edge
   that survives a destructive shuffle is probably leakage or proxying time.
10. **Confirm in a virgin forward window.** Freeze the model and schema, then
    shadow it live before allowing any strategy decision to consume its score.

For categorical regimes, treat the complete one-hot family as one feature trial.
After single-feature additions, run leave-one-feature-out ablation on the full
candidate model to identify redundant composites.

## Additional professional context families to evaluate

These are candidate features, not assumed improvements:

- India VIX level, 5-minute change, percentile, and NIFTY realized/implied
  volatility spread. NSE defines India VIX from NIFTY option quotes as a 30-day
  expected-volatility measure.
- NIFTY/Bank Nifty futures basis, basis momentum, and near/far basis slope.
- Option skew, ATM IV term structure, put/call open-interest change, strike-level
  OI concentration, and estimated dealer gamma exposure.
- Signed order-flow imbalance, quote-depth imbalance, spread, price impact,
  cancellation intensity, and volume-clock toxicity. VPIN-style measures should
  be treated cautiously because subsequent research disputes their incremental
  warning power when trade classification and volatility are controlled.
- Cross-sectional return dispersion, average pairwise correlation, first-PCA
  variance share, beta dispersion, and correlation-breakdown shocks.
- Realized upside/downside semivariance, bipower variation, jump score, volatility
  of volatility, and HAR-RV horizons.
- Equal-weight minus cap-weight index return, NIFTY 500 minus NIFTY 50, small/mid
  minus large, high-beta minus low-beta, and momentum/value/quality factor baskets.
- Breadth acceleration, cumulative A-D line slope, McClellan-style breadth
  oscillators, 52-week high/low diffusion, and volume breadth.
- Opening-gap breadth, pre-open imbalance, first-15-minute range compression,
  distance from overnight/global-futures move, and time-of-day surprise scores.
- INR, Indian rates, crude, Asian indices, and US index-futures moves aligned to
  what was actually observable at the NSE decision time.
- Scheduled-event clocks (RBI, Budget, election, major macro releases) and
  minutes-to-event interactions; these must be versioned from a point-in-time
  calendar.

Useful background: the standard realized-volatility estimator is described in
[Andersen et al.'s realized-volatility work](https://w4.stern.nyu.edu/finance/docs/pdfs/Seminars/063m-bollerslev.pdf),
time-series momentum evidence and caveats motivate treating trend as an input
rather than a rule ([Moskowitz, Ooi, and Pedersen](https://pages.stern.nyu.edu/~lpederse/papers/TimeSeriesMomentum.pdf)),
and the [NSE India VIX methodology](https://www.nseindia.com/static/products-services/indices-indiavix-index)
defines the India-specific implied-volatility feature. For flow toxicity, compare
the original [Easley, López de Prado, and O'Hara](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1695596)
proposal with the critical replication by
[Andersen and Bondarenko](https://papers.ssrn.com/sol3/Delivery.cfm/SSRN_ID2475621_code246693.pdf?abstractid=2292602).

## Promotion checklist

- [ ] Exact point-in-time universe manifests for every historical date
- [ ] Point-in-time sector membership with at least 70% usable coverage
- [ ] Point-in-time provenance checks for supplied EMA/ATR/RVOL inputs
- [ ] Dedicated NIFTY, Bank Nifty, and Midcap five-minute sources
- [ ] Actual live publication timestamps and stale-row policy
- [ ] Immutable feature version and ordered model schema
- [ ] Candidate-ID parity before/after context enrichment
- [ ] Purged walk-forward single-feature and leave-one-out ablations
- [ ] Day-block confidence intervals and multiple-testing correction
- [ ] Virgin forward holdout
- [ ] Live shadow period with drift/coverage monitoring
