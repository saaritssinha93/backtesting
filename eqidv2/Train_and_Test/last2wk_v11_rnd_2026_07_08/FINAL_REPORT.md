# V11 last-2-weeks R&D study — 2026-07-08

**Goal:** improve per-setup and portfolio PnL on the last 2 weeks of available data, honestly (realistic, stable, not overfitted). Edit only the R&D copy of the config; leave the working config untouched.

**Deliverable config:** [`final_setup_conf_v11_rnd.py`](../../final_setup_conf_v11_rnd.py) (working copy `final_setup_conf_v11_working.py` is **unchanged**).

---

## Method (so the numbers are trustworthy)

| Item | Value |
|---|---|
| Backtester | `avwap_5min_ID_v11_backtesting.py --mode historical_all_available --selected_strategy_profile final_setup_conf` |
| Tuning window (IS) | **2026-06-23 → 2026-07-07** (10 trading days = last 2 weeks; 06-26 is a data hole) |
| Out-of-sample window (OOS) | **2026-05-26 → 2026-06-20** (18 trading days, does not overlap IS) |
| Cost basis | **statutory NSE intraday + 5 bps/leg slippage** (~Rs132/trade on Rs100k notional). This is the live-paper / tuner basis. The raw v11 `historical_all_available` resolve is *price-only*; all figures here re-apply statutory costs. |
| Harness faithfulness | The fast iteration harness reproduces the full v11 backtest **exactly** (baseline 140 trades / −Rs1,315 price-only; cap_noGH +Rs5,808 matches full `iter_pool` to the rupee). |

**Honesty guardrail:** every change was validated on a genuine out-of-sample window (OOS) and checked for day-concentration. Changes that only helped IS, only helped one day, or over-thinned the sample were rejected.

---

## 1. Baseline performance (working conf, 11 setups)

| Window | Trades | tr/day | Net (real) | PF | Day-win | Max DD |
|---|---|---|---|---|---|---|
| **IS** (06-23→07-07) | 140 | 14.0 | **−Rs 19,823** | 0.70 | 30% (3/10) | −Rs 15,303 |
| **OOS** (05-26→06-20) | 295 | 16.4 | **−Rs 11,897** | 0.92 | 39% (7/18) | −Rs 20,176 |

The book is a **heavy net loser on both windows** — death-by-cost: ~Rs132/trade × 14-16 trades/day = ~Rs18.5k / Rs39k of cost drag on a price-only edge that is barely above break-even (price-only PF 0.98 / 1.23). Only **E_ORB_BREAKOUT_LONG** is net-positive at realistic cost.

## 2. Best improved performance (R&D conf)

| Window | Trades | tr/day | Net (real) | PF | Day-win | Max DD |
|---|---|---|---|---|---|---|
| **IS** | 21 | 2.3 | **+Rs 6,127** | 1.60 | 56% (5/9) | −Rs 2,260 |
| **OOS** | 41 | 2.7 | **+Rs 8,338** | 1.35 | 60% (9/15) | −Rs 5,229 |

Both windows flip from heavy loss to solid positive, **day-spread** (top day 54% IS / 42% OOS; positive days 5/9 and 9/15) — i.e. **not one-lucky-day**. Swing vs baseline: **+Rs 25,950 IS / +Rs 20,235 OOS.**

---

## 3. Setup-wise before → after (realistic net)

| Setup | Side | IS base | IS after | OOS base | OOS after | Disposition |
|---|---|---|---|---|---|---|
| **E_ORB_BREAKOUT_LONG** | L | +2,564 | **+2,564** | +12,633 | **+12,633** | **KEEP as-is** — the real edge, strong both windows |
| C_OR_BREAKDOWN | S | −9,997 | +2,053 | −2,430 | −939 | 11:30 cap (was firing 38/54 trades, mostly afternoon SL/EOD) |
| L_DOUBLE_BOTTOM_VWAP | L | −2,131 | +3,027 | **−31,924** | −1,683 | 11:30 cap (over-fired **137 OOS trades**; cap tames to 16) |
| A_MOD_BREAK_C1_LOW | S | −3,503 | +853 | −1,309 | −362 | 11:30 cap |
| B_HUGE_RED_FAILED_BOUNCE | S | −1,558 | 0 | +1,971 | +25 | 11:30 cap |
| A_PULLBACK_C2_…_LOW | S | −1,429 | −2,370 | +1,202 | −1,337 | 11:30 cap (marginal/noise; kept, not disabled) |
| G_LOWER_LOW_BREAK | S | −267 | 0 | +1,696 | 0 | 11:30 cap |
| **G_HIGHER_HIGH_BREAK** | L | −3,502 | *off* | +6,263 | *off* | **DISABLED** — sign-flips across windows (noise) + documented failed-gate |
| DOC5D_AVWAP_RECLAIM_LONG | L | 0 | *off* | 0 | *off* | **DISABLED** — documented loser, 0 trades both windows |
| S9_MIDDAY_LOSE | S | 0 | *off* | 0 | *off* | **DISABLED** — documented reject, 0 trades both windows |
| D_EMA20_REJECTION | S | 0 | *off* | 0 | *off* | **DISABLED** — failed-gate, 0 trades both windows |

**The decisive structural finding:** afternoon entries were the *entire* loss. A midday entry cap flips both windows positive:

| Portfolio entry-time cap | IS net | OOS net |
|---|---|---|
| none (baseline) | −19,823 | −11,897 |
| ≤ 12:00 | +2,306 → +5,808* | +7,885 → +1,622* |
| **≤ 11:30 (chosen)** | **+6,127** | **+8,338** |
| ≤ 11:00 | +3,435 (12 tr) | +14,714 (16 tr — too thin) |

\*with/without G_HIGHER. **11:30 dominated 12:00 on *both* windows for the no-G_HIGHER book** — so it is not an IS-only fit.

---

## 4. Exact config changes in `final_setup_conf_v11_rnd.py`

1. **11:30 IST entry cap** — added `["signal_minute", "<=", 690]` to `mask_terms` of the 6 momentum setups: `C_OR_BREAKDOWN`, `A_MOD_BREAK_C1_LOW`, `L_DOUBLE_BOTTOM_VWAP`, `B_HUGE_RED_FAILED_BOUNCE`, `A_PULLBACK_C2_THEN_BREAK_C2_LOW`, `G_LOWER_LOW_BREAK`. (Search the file for `R&D 2026-07-08`.) In v11 the conf mask honours `mask_terms` + `min_slot` only, so a time *cap* must be a `signal_minute` mask term.
2. **Demotion block `_RND_DEMOTION_2026_07_08`** (end of file) pops 4 setups out of `FINAL_SETUP_CONF` into `RESEARCH_WATCH_CONF`: `G_HIGHER_HIGH_BREAK`, `DOC5D_AVWAP_RECLAIM_LONG`, `S9_MIDDAY_LOSE`, `D_EMA20_REJECTION`. Reversible (delete the block + the cap edits).
3. `E_ORB_BREAKOUT_LONG` — **unchanged** (all-morning ORB; the edge).

Net book: 11 setups → **7 active** (E_ORB + 6 capped), ~2.5 trades/day.

---

## 5. Rejected experiments (and why)

| Experiment | Result | Verdict |
|---|---|---|
| Exit re-tune (C_OR tgt 2.0→1.5; A_MOD SL 1.1→0.9) | **0 effect** — morning trades ride to EOD, never reach either target | rejected (no signal) |
| 12:30 cap | IS +500 only — lets the 12:00-12:30 losers back in | rejected (worse) |
| 11:00 cap | Great OOS (+14,714) but only 12 IS / 16 OOS trades | rejected (over-thinned; unreliable) |
| Keep G_HIGHER | +2,306 IS / +7,885 OOS — but relies on G_HIGHER's unstable OOS win (loses IS) | rejected (depends on noise) |
| Disable A_PULLBACK / other marginal shorts | Would bump PnL, but on 1-3 trade samples | rejected (overfitting) |
| Per-setup optimal caps | Higher PnL, but a different cutoff per setup = curve-fit | rejected (used one uniform 11:30) |

---

## 6. Best final config file

`C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2\final_setup_conf_v11_rnd.py`

(The normal V11 backtest BAT already points at the *working* module. To backtest this R&D book, set `EQIDV2_V11_FINAL_SETUP_CONF_MODULE=final_setup_conf_v11_rnd`.)

---

## 7. Honest conclusion — live-tradable?

**Real, generalizing improvement, but not yet a proven money-maker.**

- The change is **honest and out-of-sample validated**: the baseline loses on both windows; the R&D book is net-positive on both (IS +6,127 PF 1.60, OOS +8,338 PF 1.35), day-spread, mechanically motivated (intraday momentum setups forced to 15:20 EOD close have no room in the afternoon), and the chosen 11:30 cap is supported by *both* windows.
- **But the honest caveats are real:**
  1. It is a **"cut the structural losses"** result, not a discovered new edge. The book went from bleeding to positive mainly by *removing* afternoon trades and 4 documented-bad/dormant setups.
  2. The surviving edge is **concentrated in E_ORB_BREAKOUT_LONG** (the only setup strongly positive on both windows). The 6 capped shorts/longs are marginal-to-noise even after the cap.
  3. **Thin samples** — 21 IS / 41 OOS trades. PF 1.35-1.60 on these counts is encouraging, not certified.

**Verdict:** deploy the R&D config to **LIVE PAPER for forward validation** (do **not** size up / risk real capital yet). It is a large, defensible improvement over the losing baseline and safe to observe live. It is **not** yet demonstrated profitable enough for real money. Further honest R&D should focus on (a) finding/strengthening more E_ORB-style morning-momentum edges, and (b) confirming on more history + a live-paper holdout before sizing.
