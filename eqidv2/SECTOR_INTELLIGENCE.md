# EQID Sector Intelligence

## What this adds

`sector_intelligence.py` is a causal feature engine for completed five-minute
NSE stock bars. It continuously produces:

- one numerical snapshot for every supplied sector;
- one numerical stock-within-sector snapshot for every supplied stock;
- a backward-only candidate attachment using the stock's point-in-time sector.

It does not generate a side, BUY/SELL label, entry, exit, stop, target, size,
order, portfolio decision, or candidate filter. The existing setup population
and execution logic stay unchanged.

The separate `MarketContextEngine` describes the broad tape. This module adds a
middle layer between the market and the individual setup:

```text
market state -> sector state -> stock relative to its sector -> existing setup
```

That distinction matters. An identical long setup can have different forward
odds when it is a leader inside a liquid, broad, accelerating sector versus a
temporary bounce inside a weak, narrow sector.

## Data contract

The input is the same long five-minute table used by the Market Context Engine:

```text
date, ticker, sector, open, high, low, close, volume
optional: EMA_20, EMA_50, vol_ratio, opening_snapshot, gap_filled,
          source_1m_count, is_eligible, sector_expected_members
```

`date` is the completed bar-end timestamp. If a 09:15 row is an opening quote
snapshot, mark it `opening_snapshot=True`; it is removed from all rolling state.
Partial source bars, invalid prices, and invalid volume do not enter features
that require fresh observations.

Supplied `EMA_20`, `EMA_50`, and `vol_ratio` values are trusted after finite,
positive-domain validation. Historical values must therefore be calculated
causally with the same convention used live; a centered or full-sample
indicator would leak through the input. “Previous close” is corporate-action
adjusted only when the supplied OHLC history is adjusted.

For historical research, `sector` and `sector_expected_members` must be the
classification and member count known on that date. NSE uses a four-tier
classification and reviews company classifications annually as well as after
relevant corporate events, so today's map must not be backfilled over history:
[NSE industry classification](https://www.nseindia.com/static/products-services/industry-classification).

The repository's current `configs/sector_etf_map.json` contains only 118
membership declarations covering 117 unique tickers, roughly 9% of a
1,200-stock scan. It is useful for plumbing tests, but it cannot satisfy the
production requirement to score every NSE sector. Promotion therefore requires
a point-in-time security master with effective dates.

## Python use

```python
from sector_intelligence import (
    SectorIntelligenceConfig,
    SectorIntelligenceEngine,
    attach_sector_intelligence_asof,
    sector_intelligence_feature_columns,
)

engine = SectorIntelligenceEngine(
    SectorIntelligenceConfig(
        publish_delay_seconds=0,       # research convention only
        min_sector_members=5,
        min_sector_data_coverage=0.70,
        expected_universe_size=current_expected_stock_count,
        min_market_coverage=0.70,
        expected_sector_count=current_expected_sector_count,
        min_cross_sector_coverage=0.70,
        relative_volume_sessions=20,
        relative_volume_min_sessions=5,
        leader_percentile_threshold=0.90,
    ),
    # Prefer the point-in-time `sector` column in stock_bars. A static map is
    # acceptable only for current/live use when its effective date is valid.
    sector_map=sector_map,
)

intelligence = engine.compute(stock_bars)

# Adds columns only. Candidate count, order, index, setup, and execution fields
# are preserved. The stock snapshot is joined first; its historical sector key
# drives the sector join.
enriched_candidates = attach_sector_intelligence_asof(
    candidates,
    intelligence,
    candidate_time_col="signal_time_ist",  # actual decision-ready time live
    ticker_col="ticker",
    prefix="si_",
    max_staleness_minutes=7,
)

ordered_ml_columns = sector_intelligence_feature_columns(
    intelligence,
    prefix="si_",
)
```

To use it together with the broader context engine:

```python
candidates_with_market = attach_context_asof(candidates, market_context)
model_rows = attach_sector_intelligence_asof(
    candidates_with_market,
    sector_intelligence,
)
```

Both enrichment calls are feature-only. They do not remove or select a row.

## Output contract

`SectorIntelligenceResult` contains:

- `sectors`: key `(feature_version, timestamp, sector)`;
- `stocks`: key `(feature_version, timestamp, ticker)`, with `sector` as
  point-in-time join metadata;
- `available_at`: earliest time the snapshot may be consumed;
- numerical feature columns only, apart from keys and version metadata.

Unavailable warmups stay `NaN`; they are not turned into neutral zeroes. The
module validates that generated feature names have no execution vocabulary and
that every non-key output is numeric and finite-or-missing.

In this reference engine, `available_at = timestamp + publish_delay_seconds`.
That is a research convention. Production must persist the actual atomic
publication time, or use a conservatively measured latency, because a fixed
zero-delay timestamp can make a backtest consume context earlier than live.

`SECTOR_FEATURE_COLUMNS` and `STOCK_FEATURE_COLUMNS` are immutable ordered
schemas, including when the input or a timestamp has no mapped sectors.
`sector_intelligence_feature_columns(...)` excludes feed-health/count metadata
by default; pass `include_quality_metadata=True` to retrieve the full numerical
schema. Training and live inference should freeze the returned ordered list.

Coverage is evaluated separately for prices, previous closes, momentum, VWAP,
EMA20, EMA50, highs/lows, volume, RVOL, relative turnover, and Amihud impact.
A feature is missing unless its own component has both the minimum number of
members and `min_sector_data_coverage`. Gap-filled bars are not current breadth
observations. The expected-member source code is `2` for an explicit
point-in-time row value, `1` for the static-map count, `0` for the causal running
maximum fallback, and `-1` for an inconsistent manifest. An invalid manifest
disables the affected sector snapshot.

An expected count cannot create a row for a completely absent sector. To prove
that *every* sector was evaluated at every timestamp, production ingestion must
also supply a point-in-time sector-universe skeleton and materialize missing
sector rows before monitoring. Without that manifest, this module can score all
observed sectors and expose partial coverage, but it cannot distinguish a wholly
missing sector from a sector that did not exist.

## Mathematical definitions

Let stock `i` belong to sector `s` at completed bar `t`.

```text
P_i,t          = close
r_i,h,t        = log(P_i,t / P_i,t-h), using the exact timestamp t-h
d_i,t          = P_i,t / previous_session_close_i - 1
Q_i,t          = P_i,t * V_i,t, five-minute rupee turnover
M_s,t          = median_i_in_s(r_i,6,t), 30-minute sector momentum
R_s,t          = median_i_in_s(r_i,1,t), robust sector five-minute return
```

Exact timestamp lookup means a missing 09:35 bar is not silently replaced by
the previous available row. Overnight returns are excluded from intraday
rolling windows.

### Sector trend

Let `sigma_s,t` be the trailing within-session standard deviation of `R_s,t`,
using up to `W=24` bars by default and at least four. With default short and
long horizons `h_s=3` and `h_l=12`, define exact cumulative returns:

```text
z_3  = sum(last 3 R_s)  / (sigma_s,t * sqrt(3))
z_12 = sum(last 12 R_s) / (sigma_s,t * sqrt(12))
v    = 2*fraction_above_VWAP - 1
e20  = 2*fraction_above_EMA20 - 1
e50  = 2*fraction_above_EMA50 - 1

sector_trend_score = 100 * finite_weighted_average(
    [tanh(z_3), tanh(z_12), v, e20, e50],
    [0.30,       0.25,       0.20, 0.15, 0.10]
)
```

Finite weights are renormalized during warmup, but a value is published only
with at least two valid components, including at least one return component.
The result is approximately in `[-100,+100]`. Short and long standardized
direction capture travel; VWAP and EMA participation check whether that
direction is structurally supported by the sector's members.

### Sector momentum and relative strength

```text
sector_momentum_30m_pct = 100 * M_s,t
market_momentum_t       = median_all_stocks(r_i,6,t)
relative_momentum_s,t   = 100 * (M_s,t - market_momentum_t)
```

Across reliable sectors at the same timestamp, the raw relative momentum is
robustly standardized using median/MAD with a standard-deviation fallback:

```text
sector_relative_strength_score = 100 * tanh(z_cs(relative_momentum) / 2)
```

Momentum gives the sector's absolute move. Relative strength answers whether
that move is exceptional versus the tape.

### Sector volatility

```text
sector_volatility_60m_bps =
    10,000 * sqrt(sum_{j=0..11} R_s,t-j^2)
```

All 12 five-minute returns are required, so the first valid value needs 13
completed price observations in that session. `sector_return_dispersion_5m_bps`
is `10,000 * 1.4826 * MAD_i(r_i,1,t)` and measures disagreement among members.
Common sector volatility and constituent dispersion describe different risks.

### Sector participation

Let the direction proxy be the sign of the equal-weight mean constituent
30-minute log return, `g_s,t = sign(mean_i(r_i,6,t))`, while the published
robust sector momentum remains the median `M_s,t`. Let `N_expected,s,t` be the
point-in-time expected member count.

```text
aligned_i,t = 1[g_s,t * r_i,6,t > 0]
active_i,t  = 1[RVOL_i,t >= 1 and fresh volume]

sector_participation_pct =
    100 * sum(aligned_i,t) / N_valid_momentum,s,t

sector_expected_member_support_pct =
    100 * sum(aligned_i,t) / N_expected,s,t

sector_active_aligned_support_pct =
    100 * sum(aligned_i,t * active_i,t)
        / N_valid_momentum_and_RVOL,s,t

sector_signed_participation_pct =
    g_s,t * sector_participation_pct
```

The first number describes agreement among stocks whose 30-minute momentum is
actually observable. The expected-member support number penalizes missing or
unaligned constituents. Active aligned support answers whether the observable
momentum/RVOL names agree *and* have RVOL at least one. This distinguishes broad
equal-weight agreement from a move driven by a smaller set of large movers
without silently treating missing data as disagreement. It is not free-float
weighted; alignment to an official point-in-time sector index is the preferred
institutional extension.

### Sector breadth

Within sector `s`, compute advances/declines versus adjusted previous close and
fractions above VWAP/EMAs or making fresh intraday highs/lows:

```text
AD_s   = (advances - declines) / (advances + declines + unchanged)
bVWAP  = 2*p_above_VWAP - 1
bE20   = 2*p_above_EMA20 - 1
bE50   = 2*p_above_EMA50 - 1
bHL    = p_new_high - p_new_low

sector_breadth =
    0.30*AD_s + 0.25*bVWAP + 0.20*bE20 + 0.15*bE50 + 0.10*bHL
```

Weights are renormalized over finite components, and at least two components
plus a reliable sector snapshot are required. The composite is approximately
in `[-1,+1]`; underlying percentages are also emitted. Breadth helps
distinguish a durable sector move from narrow leadership.

### Sector liquidity

For each stock, same-slot relative turnover uses only earlier sessions:

```text
baseline_Q_i,slot = median(previous 20 sessions' Q_i at the same bar slot)
RTO_i,t           = Q_i,t / baseline_Q_i,slot
ILLIQ_i,t         = abs(r_i,1,t) / (Q_i,t / 1,000,000)
```

The last term is an intraday OHLCV adaptation of Amihud's return-per-dollar-
volume illiquidity measure, so it is a proxy rather than order-book liquidity:
[Amihud (2002)](https://www.sciencedirect.com/science/article/pii/S1386418101000246).

Let `z_cs` denote robust same-timestamp standardization across reliable sectors:

```text
x_s = finite_weighted_average(
    [z_cs(median_i(log(1 + Q_i))),
     z_cs(log(median_i(RTO_i))),
    -z_cs(log(median_i(ILLIQ_i)))],
    [0.45, 0.30, 0.25]
)

sector_liquidity_score = 50 * (1 + tanh(x_s / 2))
```

At least two components are required and finite weights are renormalized. The
score lies in `[0,100]`. Raw total turnover, median turnover, relative turnover,
relative volume, Amihud proxy, and the 0–100 average-rank liquidity percentile
are retained.

### Sector acceleration

```text
sector_acceleration_30m_pct = 100 * (M_s,t - M_s,t-6)
```

`M_s,t` measures `[t-30m,t]`, while `M_s,t-6` measures the preceding
non-overlapping `[t-60m,t-30m]` window. This avoids calling a one-bar wiggle
"acceleration." The raw value and a cross-sector acceleration score are emitted.

```text
sector_acceleration_score =
    100 * tanh(z_cs(sector_acceleration_30m_pct) / 2)
```

### Composite sector strength and rotation

Relative strength is deliberately kept as a primitive. A separate composite
combines price leadership, breadth, and activity:

```text
x_strength = finite_weighted_average(
    [z_cs(relative_momentum), z_cs(sector_breadth),
     z_cs(log(sector_relative_volume))],
    [0.55,                       0.25, 0.20]
)

sector_strength_score = 100 * tanh(x_strength / 2)
```

At least two components are required. `sector_strength_rank` uses descending
average ranks, so tied strongest sectors share their rank; the percentile runs
from 0 to 100 with the same neutral tie handling. With default rotation lag
`L=3`, short-horizon rotation is the absolute change in strength percentile:

```text
sector_strength_percentile_turnover_pct_points =
    abs(strength_percentile_t - strength_percentile_t-3)
```

### Numerical leaders and weakest stocks

No ticker name or encoded ticker ID is used as an ML feature. Each stock gets
numeric ranks, scores, and flags. Each sector receives:

```text
sector_leader_stock_count
sector_weakest_stock_count
sector_leader_score_mean
sector_weakest_score_mean
sector_leader_momentum_30m_pct
sector_weakest_momentum_30m_pct
sector_leadership_spread_30m_pct
```

The default leader/weakest threshold is the top/bottom decile of within-sector
leadership percentile. Ties use average ranks; an all-tied group is neutral at
the 50th percentile and produces no false leader.

## Per-stock sector features

### Distance from sector average

When an average is requested, the implementation uses a leave-one-out peer
mean so the stock does not mechanically pull its own benchmark toward itself:

```text
mean_minus_i(x) = (sum_j(x_j) - x_i) / (n_valid - 1)

stock_distance_from_sector_average_pct =
    100 * (d_i,t - mean_minus_i(d_t))
```

It is unavailable for a one-member sector.

All `stock_peer_*_valid_count` values exclude the subject stock. Component
readiness uses `(valid peers)/(expected members - 1)`, so a stock cannot satisfy
its own peer-coverage test. This matters when a nominally large sector has only
a handful of usable previous closes, VWAPs, or RVOL values.

### Relative momentum

```text
stock_relative_momentum_30m_pct =
    100 * (r_i,6,t - mean_minus_i(r_.,6,t))
```

This tells the ML model whether the stock is leading or lagging the move shared
by its peers.

### Outperformance score

Let `v_i,t = log(P_i,t / VWAP_i,t)`, and standardize each component robustly
within the stock's sector at the same timestamp:

```text
raw_outperformance = finite_weighted_average(
    [z_s(r_30m), z_s(d_intraday), z_s(vwap_distance)],
    [0.50,        0.30,            0.20]
)

stock_outperformance_score = 100 * tanh(raw_outperformance / 2)
```

Each z-score is withheld unless its corresponding sector component and
leave-one-out peer coverage are ready. At least two of the three components are
required. The bounded score remains price-performance context; it is not a
trade action.

### Sector percentile

`stock_sector_percentile` is the ascending average-rank percentile of the
outperformance score:

```text
percentile_i = 100 * (average_rank_i - 1) / (n_valid - 1)
```

The strongest is 100, the weakest is 0, and tied values receive their shared
midrank. A singleton or all-equal cross-section is neutral at 50 before the
minimum-member reliability mask is applied.

### Sector leadership score

Define:

```text
p_out   = stock_sector_percentile / 100
p_rvol  = within-sector percentile of stock RVOL
persist = mean(1[r_5m>0], 1[r_15m>0], 1[r_30m>0]) over available horizons
NH      = 1[new intraday high]
VW      = 1[close > session VWAP]

stock_sector_leadership_score = 100 * (
    0.45*p_out + 0.20*p_rvol + 0.15*persist + 0.10*NH + 0.10*VW
)
```

Weights are renormalized over available components. The score is in `[0,100]`.
The outperformance percentile is mandatory and at least two total components
are required. Its within-sector percentile produces the numeric descriptive flags
`stock_is_sector_leader` and `stock_is_sector_weakest`. Neither flag is consumed
by this module as a setup gate or execution instruction.

## Exhaustive numerical feature dictionary

This block covers the diagnostic and secondary columns that share formulas,
while the preceding sections define the primary scores. Let
`C_s,t(A) = sum_i 1[A_i,t is valid]`, `N_s,t` be the expected sector members,
`m` the minimum-member setting, and `q` the component-coverage setting. Then:

```text
coverage_s,t(A) = 100 * C_s,t(A) / N_s,t
ready_s,t(A)    = 1[C_s,t(A) >= m and coverage_s,t(A) >= 100*q]

finite_weighted_average(x, w) =
    sum_{k: finite(x_k)} w_k*x_k / sum_{k: finite(x_k)} w_k

z_cs(x) = clip((x - median(x)) / (1.4826*MAD(x)), -5, 5)
```

If MAD is zero, `z_cs` uses mean/population-standard-deviation; a constant
cross-section is zero. The ascending average-rank percentile is
`100*(average_rank-1)/(n_valid-1)`; a singleton or all-tied group is 50.

### Sector counts, coverage, and readiness

| Outputs | Mathematical definition |
|---|---|
| `sector_member_count` | Number of distinct mapped tickers observed in `(s,t)`. |
| `sector_price_eligible_count`, `sector_fresh_price_eligible_count`, `sector_flow_eligible_count` | Counts of valid complete OHLC; valid complete OHLC excluding gap fills; and fresh price plus finite non-negative volume. |
| `sector_momentum_valid_count`, `sector_rvol_valid_count`, `sector_return_5m_valid_count` | Counts with finite exact 30-minute return, usable RVOL, and exact five-minute return. |
| `sector_above_vwap_valid_count`, `sector_above_ema20_valid_count`, `sector_above_ema50_valid_count`, `sector_new_high_low_valid_count` | Fresh stocks for which each named comparison is observable. |
| `sector_advance_count`, `sector_decline_count`, `sector_unchanged_count`, `sector_previous_close_valid_count` | Sums of `1[P>Pprev]`, `1[P<Pprev]`, `1[P=Pprev]`, and their valid denominator. |
| `sector_relative_turnover_valid_count`, `sector_amihud_valid_count`, `sector_active_volume_count` | Counts with valid RTO, valid Amihud impact, and `RVOL>=1`, respectively. |
| `sector_expected_member_count` | Explicit PIT count when valid; otherwise static-map count when available; otherwise causal running maximum observed for that sector. |
| `sector_expected_member_source_code`, `sector_expected_members_valid_flag` | Source code `{2 explicit, 1 static, 0 causal fallback, -1 invalid}` and `1[source in {1,2}]`. |
| `sector_data_coverage_pct` | `100*sector_fresh_price_eligible_count/sector_expected_member_count`. |
| `sector_momentum_coverage_pct`, `sector_rvol_coverage_pct`, `sector_flow_coverage_pct`, `sector_relative_turnover_coverage_pct`, `sector_amihud_coverage_pct`, `sector_return_5m_coverage_pct` | The generic coverage formula using the correspondingly named valid/eligible count. |
| `sector_previous_close_coverage_pct`, `sector_above_vwap_coverage_pct`, `sector_above_ema20_coverage_pct`, `sector_above_ema50_coverage_pct`, `sector_new_high_low_coverage_pct` | The same coverage formula for breadth primitives. |
| `sector_reliable_flag` | `1[fresh_count>=m, data_coverage>=100*q, expected source != -1]`. |
| `sector_momentum_ready_flag`, `sector_rvol_ready_flag`, `sector_flow_ready_flag`, `sector_relative_turnover_ready_flag`, `sector_amihud_ready_flag`, `sector_return_5m_ready_flag` | Generic primitive `ready(A)`; publication also requires `sector_reliable_flag=1`. |
| `sector_previous_close_ready_flag`, `sector_above_vwap_ready_flag`, `sector_above_ema20_ready_flag`, `sector_above_ema50_ready_flag`, `sector_new_high_low_ready_flag` | `sector_reliable_flag * ready(A)`. |

Market-relative and cross-sector fields fail closed as well:

```text
market_momentum_coverage_pct =
    100 * market_momentum_valid_count / market_expected_member_count

market_momentum_ready_flag = 1[valid_count >= max(m,2),
                               coverage >= 100*min_market_coverage,
                               observed <= expected]

cross_sector_reliable_coverage_pct =
    100 * cross_sector_reliable_count / cross_sector_expected_count

cross_sector_ready_flag = 1[reliable_count >= 2,
                            coverage >= 100*min_cross_sector_coverage,
                            observed <= expected]
```

`market_expected_member_count` uses configured `expected_universe_size` or a
causal running maximum; `cross_sector_expected_count` uses configured
`expected_sector_count` or its causal running maximum. Cross-sectional scores
also require their own source-valid sector count to meet the same threshold.
`market_momentum_valid_count`, `market_momentum_coverage_pct`,
`market_momentum_ready_flag`, `cross_sector_reliable_count`,
`cross_sector_reliable_coverage_pct`, and `cross_sector_ready_flag` are repeated
on sector rows so every model row carries its comparator health.

### Sector primitives and composite metadata

```text
RVOL_i,t = supplied causal RVOL, else
           V_i,t / median(previous D same-slot session volumes), D=20 default

sector_relative_volume       = median_i(RVOL_i,t)
sector_relative_turnover     = median_i(RTO_i,t)
sector_amihud_impact         = median_i(ILLIQ_i,t)
sector_total_turnover_crore  = sum_i(Q_i,t) / 10,000,000
sector_median_turnover_lakh  = median_i(Q_i,t) / 100,000
sector_intraday_return_pct   = 100 * median_i(d_i,t)
sector_pct_above_vwap        = 100 * mean_i(1[P_i,t>VWAP_i,t])
sector_pct_above_ema20       = 100 * mean_i(1[P_i,t>EMA20_i,t])
sector_pct_above_ema50       = 100 * mean_i(1[P_i,t>EMA50_i,t])
sector_pct_new_intraday_highs = 100 * mean_i(1[new high])
sector_pct_new_intraday_lows  = 100 * mean_i(1[new low])
```

`sector_momentum_30m_pct`, `sector_return_dispersion_5m_bps`,
`sector_advance_decline_net`, `sector_breadth`, `sector_trend_score`,
`sector_volatility_60m_bps`, `sector_acceleration_30m_pct`,
`sector_relative_momentum_pct`, `sector_relative_strength_score`,
`sector_acceleration_score`, `sector_liquidity_score`, `sector_strength_score`,
`sector_participation_pct`, `sector_expected_member_support_pct`,
`sector_active_aligned_support_pct`, and `sector_signed_participation_pct` are
defined in the preceding equations.

Every `*_component_count` is the number of finite inputs entering its finite
weighted average. Thus `sector_breadth_component_count`,
`sector_trend_component_count`, `sector_trend_return_component_count`,
`sector_liquidity_component_count`, and `sector_strength_component_count` are
integer-valued diagnostics. `sector_trend_ready_flag` requires a reliable row,
at least two trend components, and at least one return component;
`sector_liquidity_ready_flag` and `sector_strength_ready_flag` require a
reliable cross-sector comparison and at least two components.

`sector_liquidity_percentile` and `sector_strength_percentile` use the 0–100
midrank formula. `sector_strength_rank` is descending average rank, and
`sector_strength_percentile_turnover_pct_points` is the exact-lag absolute
percentile change defined above.

For leader set `L={i: stock_is_sector_leader_i=1}` and weak set
`W={i: stock_is_sector_weakest_i=1}`:

```text
sector_leader_stock_count       = |L|
sector_weakest_stock_count      = |W|
sector_leader_score_mean        = mean_{i in L}(leadership_score_i)
sector_weakest_score_mean       = mean_{i in W}(leadership_score_i)
sector_leader_momentum_30m_pct  = 100*mean_{i in L}(r_i,6,t)
sector_weakest_momentum_30m_pct = 100*mean_{i in W}(r_i,6,t)
sector_leadership_spread_30m_pct = leader_momentum - weakest_momentum
```

### Stock diagnostics

| Output | Mathematical definition |
|---|---|
| `stock_fresh_price_flag` | `1[complete valid OHLC and not gap-filled]`. |
| `stock_sector_mapped_flag` | `1[point-in-time sector is known]`. |
| `stock_sector_reliable_flag`, `stock_sector_momentum_ready_flag` | The aligned sector reliability and primitive momentum-readiness values. |
| `stock_peer_momentum_valid_count`, `stock_peer_intraday_valid_count`, `stock_peer_vwap_valid_count`, `stock_peer_rvol_valid_count` | Valid same-sector counts excluding the subject stock. Readiness divides each by `sector_expected_member_count-1`. |
| `stock_outperformance_component_count`, `stock_leadership_component_count` | Number of finite primitive components. Outperformance requires at least two and a jointly comparable sector cross-section; leadership additionally requires the outperformance percentile. |
| `stock_sector_percentile`, `stock_sector_leadership_percentile` | 0–100 within-sector midrank percentiles of outperformance and leadership scores. |
| `stock_distance_from_sector_average_pct`, `stock_relative_momentum_30m_pct`, `stock_outperformance_score`, `stock_sector_leadership_score` | Defined in the preceding stock equations. |
| `stock_is_sector_leader` | `1[leadership_percentile >= 100*tau]`, with `tau=0.90` by default; missing when the rank is unavailable. |
| `stock_is_sector_weakest` | `1[leadership_percentile <= 100*(1-tau)]`; missing when unavailable. |

## Why these can improve the existing long-only system

The features do not create alpha by themselves; they let the existing model
condition the probability of its existing setup outcome on information the
stock chart alone does not contain.

- Trend, breadth, and participation measure sector confirmation. They can help
  separate a broad institutional move from an isolated stock spike.
- Relative strength and stock relative momentum separate common sector movement
  from stock-specific leadership; they do not estimate or remove regression beta.
- Acceleration tells the model whether price momentum is strengthening or
  fading; an extreme acceleration can also precede exhaustion.
- Volatility changes breakout follow-through, failure risk, stop-hit risk, and
  the noise level of five-minute patterns.
- Liquidity and active-volume participation describe observable activity and
  tradability, not the identity or intent of institutional investors; they are
  also useful interactions for estimating slippage sensitivity.
- Leader/weakest momentum spreads measure tail separation or dispersion.
  Contribution concentration requires cap weights, HHI, or entropy.

The plausible profitability path is therefore conditional, not mechanical:
better context can improve probability ranking and calibration, which may let
the existing candidate budget contain more high-expectancy trades, reduce
exposure to weak sector states, or improve sizing *in a later separately tested
policy*. It can also add noise and reduce performance. No uplift should be
claimed until the paired out-of-sample tests below improve net expectancy and
drawdown after costs.

For V7, attach these columns after existing detection, gates, and the entry-time
window, before a future ML ranker. For V11/V12, attach them after frozen
candidate generation and before training/scoring. Initially use shadow scores
only. Do not add a sector threshold or hard regime veto without a separate
out-of-sample decision study.

## Computational complexity

Let `N` be all stock-bar rows in a historical batch, `S` the stocks in one
five-minute update, `K` the sectors, and `n_k` the members of sector `k`.

```text
initial batch ordering                     O(N log N)
exact returns, same-slot baselines,
rolling state, group aggregation           O(N)
within-sector stock ranks per update       O(sum_k n_k log n_k)
cross-sector ranks per update              O(K log K)
historical reference-engine memory         O(N)
```

The public `compute(stock_bars)` API is a vectorized historical/reference
engine. Each call sorts and recomputes the supplied batch, so its actual call
bound is `O(N log N)` time and `O(N)` memory. Calling it every five minutes with
all history would pay that batch cost again.

On two synthetic runs of a five-session batch with 1,200 stocks, 20 sectors,
and 75 bars per session (450,000 stock rows), this implementation produced
450,000 stock rows and 7,500 sector rows in 15.5–25.6 seconds on the development
machine: roughly 17,600–29,100 input rows/second and 41–68 ms per 1,200-stock
slot *amortized across the batch*. That is not a claim of live end-to-end
latency; storage, serialization, upstream indicators, and publication are
excluded.

For live use, a stateful adapter can retain exact-lag prices, session state, and
same-slot history. Its intended per-update calculation bound is:

```text
O(S + sum_k n_k log n_k + K log K) <= O(S log S)
```

At roughly 1,200 stocks, the ranks are small; I/O and repeatedly rebuilding
history are more likely bottlenecks than the calculations. That incremental
adapter is not implemented in this reference module and is a live-promotion
prerequisite. It should reuse frames already loaded by the scanner, retain
ring-buffer state for exact 5/15/30-minute prices and the configured prior
same-slot sessions, and never reread 1,200 files in a second pass.

## Backtest each feature without redesigning the strategy

Use the same frozen candidate pool and execution outcomes as the baseline.

1. Generate candidates once with the current V7/V11/V12 rules and store a
   candidate-ID checksum.
2. Compute sector intelligence from the full point-in-time universe, never from
   the candidate subset.
3. Backward-join on `available_at` with the same publication delay and staleness
   policy as live.
4. Use fixed purged walk-forward folds with an embargo at least as long as the
   maximum label/holding horizon.
5. Freeze the current active model, its hyperparameters, base columns, folds,
   and candidate budget. For **every column** returned by
   `sector_intelligence_feature_columns(...)`, run exactly
   `baseline` versus `baseline + that one column`.
6. Fit imputation, clipping, scaling, calibration, and any top-fraction policy
   on the training fold only.
7. Compare paired out-of-fold log loss, Brier score, PR-AUC, calibration, and
   decile monotonicity. Then compare net expectancy, PF, drawdown, turnover,
   hit rate, and setup/time/sector stability at the same candidate budget.
8. Day-block-bootstrap the paired economic differences and correct across all
   individual-column trials with Holm or false-discovery-rate control.
9. Only after the one-column screen, test feature families and composites.
   Compare every component alone, the full composite, leave-one-component-out,
   and alternative encodings such as raw value versus score versus percentile.
10. Run leakage placebos: future-bar perturbation, one-bar lag, within-slot
   shuffle, and future-membership perturbation.
11. Freeze the winning schema and validate it in a virgin forward window, then
    shadow it live before allowing the ML score to affect selection.

The individual-column loop is intentionally model-agnostic:

```python
alpha_columns = sector_intelligence_feature_columns(intelligence)
baseline_oof = walk_forward_oof(
    model_factory, rows[base_columns], y, frozen_folds
)

results = []
for feature in alpha_columns:
    augmented_oof = walk_forward_oof(
        model_factory, rows[base_columns + [feature]], y, frozen_folds
    )
    results.append(
        paired_report(
            feature=feature,
            baseline_pred=baseline_oof,
            augmented_pred=augmented_oof,
            outcomes=execution_outcomes,
            day_blocks=rows["trade_date"],
            candidate_budget=frozen_candidate_budget,
        )
    )
```

`walk_forward_oof` must construct the entire preprocessing/model pipeline inside
each training fold; `paired_report` must compare the same candidate IDs and
apply the same cost model. The default feature list deliberately excludes the
quality/readiness columns. Use those for feed monitoring and data-contract
checks, not as hidden alpha or an untested setup filter.

Keep a feature only if it improves stable out-of-sample trade economics after
costs. An in-sample uplift or a higher AUC alone is insufficient.

## Additional institutional feature families

These are research candidates, not assumed improvements:

- free-float-cap-weighted versus equal-weight sector return divergence;
- sector breadth/turnover concentration (Herfindahl index) and leadership
  entropy;
- within-sector average correlation, first-PC variance share, and effective
  independent breadth;
- beta-neutral and multi-factor residual sector/stock momentum;
- large-cap-to-small-cap lead/lag and information-diffusion features;
- downside/upside realized semivariance, jump share, volatility-of-volatility,
  and correlation-breakdown shocks;
- same-time-of-day surprises for breadth, turnover, volatility, spread, and
  depth;
- order-flow imbalance, multi-level depth imbalance, spread, cancellations,
  queue depletion, and price impact when order-book data are available;
- commonality in liquidity and sector liquidity shocks;
- sector-index/ETF cash-flow divergence, futures basis, options skew, term
  structure, open-interest change, and gamma proxies where instruments exist;
- cross-sector rotation entropy, transition probability, and rank persistence;
- sector return skew, tail breadth, and leader-to-peer cross-impact;
- opening/closing auction imbalance and expiry, rebalance, earnings, and event
  clocks;
- causal sensitivities to INR, crude, yields, Asian sector indices, and global
  futures observable before each NSE decision time.

Every external ETF, futures, options, order-book, or event feature needs its own
exchange publication timestamp and point-in-time instrument availability; a
same-date merge is not sufficient for an intraday backtest.

Industry momentum is established as a distinct empirical return component, but
that does not prove any exact five-minute NSE feature will survive costs:
[Moskowitz and Grinblatt](https://onlinelibrary.wiley.com/doi/pdf/10.1111/0022-1082.00146).
For richer liquidity data, order-flow imbalance and depth are better-grounded
microstructure inputs than OHLCV turnover alone:
[Cont, Kukanov, and Stoikov](https://arxiv.org/abs/1011.6402).

## Verification

The focused Sector Intelligence suite has 39 passing tests; the combined Market
Context plus Sector Intelligence suite has 64. Tests are in
`tests/test_sector_intelligence.py`. They cover exact leave-one-out
math and peer counts, component-specific coverage, tie-neutral percentiles,
gap-filled bars, full-window volatility, non-overlapping acceleration,
participation, breadth, liquidity, numerical leader/weakest descriptors,
same-slot prior-session turnover, future perturbation invariance, point-in-time
reclassification, mapped/unmapped transitions, market/cross-sector feed gates,
whole-sector dropout, publication delay, staleness-safe joins, MultiIndex/row
preservation, immutable empty schemas, hostile indicator sanitation, the
numeric-only contract, and invalid configuration/manifests.
