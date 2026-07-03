# C_OR_BREAKDOWN — FROM_SCRATCH_LOGIC_REVIEW

_Generated 2026-07-03. Research-only._

## 1. What the setup is trying to capture

Bear-side trend continuation: a stock that is (a) relatively weak vs NIFTY, (b) below session
VWAP, (c) printing a bearish candle, breaks the opening-range low on volume → ride the
continuation down.

Exact detector (`avwap_5min_ID_v2_backtesting._scan_day`, reason `opening_range_breakdown`):

```
np.isfinite(or_low) AND short_struct AND below_vwap AND close < or_low
AND rs_pct < 0.10 AND vol_ratio >= 1.5 AND regime != "BULL"
```

with `short_struct = close < open AND close_loc <= 0.40`, `or_low` = low of the first
OR_MINUTES of the day, `rs_pct = stock_ret - market_ret` (6-bar), regime from NIFTY 30-min
return + VWAP position.

## 2. Structural observations (things the card does NOT say)

1. **The scan loop starts at bar `max(VWAP_LOOKBACK=20, RS_LOOKBACK_BARS=6)` ≈ 10:55-11:00 IST.**
   Every "opening-range breakdown" signal in this pool fires ≥ 11:00 — up to 5 hours after the
   opening range formed. This is NOT an OR-break timing play; it's a *"price is below the
   morning low in the afternoon"* condition.
2. **No first-break requirement.** Unlike `S_MOMENTUM_BREAKDOWN_OR` (which demands
   `prev.close >= or_low`, i.e. the actual break bar), C_OR_BREAKDOWN re-fires on EVERY
   qualifying bar while price stays below the OR low → ~170 raw signals/day pool-wide, the same
   ticker re-firing for hours. The family dedupe (best score per slot+ticker, one ticker/day)
   collapses this, but which bar survives is decided by `quality_score` — effectively random
   with respect to breakdown freshness.
3. **The promoted edge is 100% in the pre-momentum gate** (`sig5_adx_calc>=39.67 &
   pre1_adx<=21.37`); ungated the setup is a documented loser (train PF 0.84 / full net
   −Rs 94k). The detection layer is a broad hose; the 1-min gate picks ~29 train trades from
   thousands.

## 3. Why previous optimization failed

- **Promotion basis (2026-06-13):** sampled pool (1300tr/1300te), train PF 2.78 / test 5.26 —
  but train halves imbalanced (h1 1.71 / h2 4.21) and top-1-day 27-37%: the "edge" was already
  concentrated.
- **Faithful conf16 run (2026-06-16):** flagged C_OR_BREAKDOWN as THE watch-item — "train hero
  → test loser" once evaluated through the full v11 pipeline instead of the sampled probe.
- **PF-band campaign (2026-07-01):** 0/12 pre-pooled setups passed June OOS; C_OR_BREAKDOWN
  showed no positive June OOS.
- **Root causes to test in this campaign:**
  a. A 2-term knife-edge gate (`sig5_adx≥39.67`) tuned to 6 decimals on a sampled pool =
     classic threshold overfit; both terms were load-bearing (dropping either → train ≤1.12).
  b. The gate selects "strong existing downtrend + quiet 1-min pause". In Mar-May's
     trending/bear tape that meant fresh continuation; in June's choppier tape the same
     condition selects exhausted moves that mean-revert.
  c. Late-fire redundancy (obs. 2): by the time a re-fire passes the ADX gate the move can be
     hours old — no freshness control exists.
  d. Wide 0.90/2.00 exit needs a big continuation leg; target-fill collapses when volatility
     compresses.

## 4. Specific weaknesses to attack (redesign axes)

| axis | issue | redesign idea |
|---|---|---|
| freshness | re-fires for hours | `breakout_age`: require first-break-of-day or cap bars-below-OR-low |
| entry timing | scan starts 11:00 | time-window versions: 11:00-12:30 fresh-break vs 13:00+ late-drift |
| trend quality | sig5_adx knife-edge | broader ADX bands, EMA stack, VWAP slope, or drop indicator gates for structural rules |
| overextension | shorting into a hole | distance-below-VWAP / below-OR-low caps (don't chase an exhausted dump) |
| exits | 0.90/2.00 needs a monster leg | MAE/MFE-based grid incl. tighter targets, asymmetric combos |
| regime | June chop kills continuation | market_ret / regime / breadth conditioning (careful: past campaigns rejected market_ret-conditioned shorts as fragile) |
| candle quality | any red close≤0.40 loc | body%, range expansion, wick rejection quality |
| volume | vol_ratio≥1.5 fixed | conviction band (e.g. 1.8-3.2) vs climax exclusion (>4 = capitulation, late) |

## 5. Pool / backtest integrity

- Pool recreated from the SAME raw basis as production scans (see POOL_RECREATION_REPORT.md);
  premom features verified present (32/32 sample rows resolve, no blocked reasons).
- Exit simulation: 1-min OHLC path to 15:20 with entry-lag + slippage + statutory costs — the
  repo's honest model (the one that exposed the original overfit), not the sampled probe.
- Known data caveats: 05-28 and 06-26 unrecoverable store holes; 06-11/07-01 genuine detector
  silence. June has 20 completed sessions — a real OOS month, unlike the thin-June caveat of
  the 07-01 campaign.

## 6. Verdict before looping

The detection layer is salvageable only if a candidate survives WITHOUT 6-decimal indicator
knife-edges: the campaign prioritizes structural rules (freshness, time, distance,
candle/volume quality) with coarse thresholds, exits matched to measured MAE/MFE, and treats
any config whose edge lives in one quantile step as overfit by construction.
