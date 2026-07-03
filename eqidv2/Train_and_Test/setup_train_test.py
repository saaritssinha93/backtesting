"""
setup_train_test.py — honest per-family train/test tuning harness.
==================================================================

For one setup FAMILY at a time (A*, B*, ...), with every other family blocked:
  - TRAIN  = the 3 months before the holdout
  - TEST   = the most recent 4 weeks by default

It auto-searches each setup's tunable knobs (selected-strategy mask thresholds,
optional pre-entry momentum gate, entry time/Top-N guards, and exit SL/Tgt),
keeps configs that reach a modest, trade-rich TRAIN PF band, and judges them on TEST.
A family config is only ACCEPTED if, out of sample:
    test net PF >= 1.30  AND  test day-block bootstrap p < 0.10
    AND test/train PF ratio >= 0.65  (not overfit).

Everything is NET of statutory NSE intraday costs (nse_intraday_costs).

The expensive scanner + live pipeline is run ONCE (see the pool builder), which
produces `pre_dedupe_live_candidates` — the live-accepted candidates BEFORE the
entry engine. This harness rebuilds entry -> guards -> pre-momentum -> dedupe ->
mask -> exits in memory, so the search is fast.

Nothing is written to final_setup_conf.py without --approve (and your say-so).

Usage:
  py -3.12 setup_train_test.py --family A
  py -3.12 setup_train_test.py --family A --approve     # after you OK the result
"""

from __future__ import annotations

import argparse
import ast
import json
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

# This script lives in Train_and_Test/; the shared core modules (v11 backtester,
# candidate_scan, final_setup_conf, bootstrap, …) stay in the repo root. Add the
# root to sys.path so they import in place (no duplication, single source of truth).
# Also add THIS folder so the sibling train_test_window module imports cleanly.
import sys
_HERE = Path(__file__).resolve().parent
_REPO_ROOT = _HERE.parent
for _p in (str(_REPO_ROOT), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import avwap_5min_ID_v11_backtesting as v11
import walkforward_gate as wfg
from nse_intraday_costs import CostConfig
from train_test_window import compute_windows

# Dynamic default window (rolls forward from today): TEST = last 4 weeks,
# TRAIN = the 3 months before TEST. Override per-run with --train_start/--train_end/
# --test_start/--test_end.
_WIN = compute_windows()
TRAIN = _WIN["train"]
TEST = _WIN["test"]
POOL_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_traintest_pool")
POOL_DIRS = [POOL_DIR]
PROPOSAL_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_traintest_pool\proposals")
# --approve writes the SHARED root final_setup_conf.py (NOT a copy in this folder),
# so the conf stays the single source of truth for v11 backtest + v7 live.
FINAL_CONF_PATH = _REPO_ROOT / "final_setup_conf.py"

# Train PF target BAND. The search reaches PF >= TRAIN_PF_MIN with the MOST
# trades possible (rather than maximising PF, which overfits to tiny subsets);
# more trades naturally pulls PF toward the lower band edge.
TRAIN_PF_MIN = 1.40
TRAIN_PF_MAX = 1.70
# Acceptance gate (judged on TEST).
TRAIN_MIN_NET_PF = TRAIN_PF_MIN          # family train floor for ACCEPT
TEST_MIN_NET_PF = 1.30
TEST_MAX_DAY_BLOCK_P = 0.10
MIN_TEST_TRAIN_PF_RATIO = 0.65
MIN_TRAIN_TRADES = 50
MIN_TEST_TRADES = 20
# Per-setup viability: drop a setup from the family if its best train config
# can't clear these (it would only bleed the family, like a gate-block).
KEEP_MIN_TRAIN_PF = TRAIN_PF_MIN
KEEP_MIN_MINHALF_PF = 1.10

# Search spaces.
SL_GRID = (0.70, 0.85, 1.00, 1.20)
TGT_GRID = (0.80, 1.00, 1.20, 1.50)
QUANTILES = (0.20, 0.35, 0.50, 0.65, 0.80)
FINE_QUANTILES = (0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90)
MAX_MASK_TERMS = 2
MAX_PREMOM_TERMS = 1
# Search objective:
#   "band"  = reach PF >= TRAIN_PF_MIN with the MOST trades (default; anti-overfit).
#   "maxpf" = MAXIMISE robust PF subject to a minimum trade floor (--min_train_trades)
#             and both-half positivity; explores pre-momentum too (use to push training
#             PF up + keep trades, then test honestly).
OBJECTIVE = "band"
FORCE_PREMOM = False        # always fit/try a pre-momentum gate (esp. for maxpf)
DEFAULT_EXCLUDED_FEATURES = {
    # Per-setup market-return bands were the dominant overfit vector. Signal-minute
    # and notional thresholds repeatedly created brittle time/rounding pockets.
    "market_ret_pct", "market_abs_ret_pct", "signal_minute", "notional",
}
SIGNAL_FEATURES = [
    "rs_pct", "market_ret_pct", "market_abs_ret_pct", "vol_ratio", "atr_pct",
    "body_pct", "close_loc", "vwap_dist_atr", "quality_score", "ranker_score",
    "signal_minute", "signal_range_pct", "upper_wick_pct", "lower_wick_pct",
    "wick_skew_pct", "notional",
]
PREMOM_FEATURES = [
    "pre_entry_momentum_score", "sig5_adx_calc", "sig5_rsi_dir", "sig5_vol_ratio20",
    "pre1_adx", "pre3_range_r", "pre5_mom_r", "pre3_close_pos",
]
# Entry-guard grids only where a setup actually has a time/Top-N guard.
GUARD_GRID = {
    "A_MOD_BREAK_C1_HIGH": [
        {"max_slot": None, "top_n": None},
        {"max_slot": "10:40", "top_n": 2}, {"max_slot": "11:10", "top_n": 2},
        {"max_slot": "11:10", "top_n": 1}, {"max_slot": "12:00", "top_n": 3},
        {"max_slot": "13:00", "top_n": None},
    ],
}

CFG = CostConfig()
EXCLUDED = set(v11.EXCLUDED_SETUPS) | set(v11.ENTRY_SHADOW_SETUPS)

# --- Execution-realism knobs (see PROJECT_OVERVIEW.md §11) ---------------------
# G2: adverse per-leg slippage / half-spread (bps) applied to BOTH the entry and
#     the exit fill — the bid-ask + impact the statutory cost model omits. The
#     resolver fills exits at the exact SL/target, so without this the book is
#     flattered (especially the tight scalps). Default 15 bps/leg = conservative
#     for the illiquid small-caps this book trades.
# G1: extra minutes of live feed+entry-engine lag beyond the backtest's signal+1min
#     open (the 5-min bar isn't readable until ~45-60s after close, plus engine lag).
#     0 = parity with the old behaviour; set 1-2 to stress the scalps.
SLIPPAGE_BPS = 15.0
ENTRY_LATENCY_MIN = 0

# G4: cost-model selector so the tuner can judge with the SAME cost function as the
#     v11 backtester (it uses v6._net_pnl_rs: a FLAT cost_bps on a fixed notional, +3bps
#     extra on SL). "statutory" = the per-trade NSE model (default, realistic); "flat_bps"
#     = v6's flat model applied to the tuner's (slipped) prices -> reconciles tuner vs v11.
COST_MODEL = "statutory"                 # "statutory" | "flat_bps"
V6_COST_BPS = float(v11.v6.DEFAULT_COST_BPS)   # 16.0; used only in flat_bps mode

# G5: portfolio overlay that makes the simulated book the DEPLOYABLE book. The live
#     executor caps concurrent positions and a daily-loss kill-switch blocks new entries.
#     MAX_POSITIONS mirrors the live 20-position cap (rarely binds for a single family,
#     binds at book level); DAILY_LOSS_RS=0 disables the kill-switch by default.
MAX_POSITIONS = 20
DAILY_LOSS_RS = 0.0

# §11.4: book-level REGIME alignment (opt-in, default off). Expresses market regime ONCE
# for the whole book ("don't fight the tape") instead of letting each setup curve-fit a
# narrow market_ret_pct band (the dominant overfit vector, which the per-setup search bans).
# When on: drop LONGs when the day's market is down more than REGIME_BAND, and SHORTs when
# it is up more than REGIME_BAND (market_ret_pct = causal cumulative market return at signal).
REGIME_ALIGN = False
REGIME_BAND = 0.0      # in market_ret_pct units; 0.0 = strict sign alignment


def _json_sanitize(obj):
    """Recursively replace non-finite floats (inf/-inf/nan) with None so output is
    STRICT JSON — Python's json emits Infinity/NaN literals that other parsers reject."""
    if isinstance(obj, dict):
        return {k: _json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_sanitize(v) for v in obj]
    if isinstance(obj, float):
        return obj if np.isfinite(obj) else None
    return obj


def _exit_with_slippage(side: str, exit_price: float) -> float:
    """Adverse per-leg slippage/half-spread on the EXIT fill (the leg the resolver and
    statutory cost model otherwise ignore): a LONG exit (sell) fills lower, a SHORT
    exit (buy) fills higher. Mirrors the entry slippage applied in _entry."""
    s = SLIPPAGE_BPS / 1e4
    if s <= 0:
        return exit_price
    return exit_price * (1.0 - s) if str(side).upper() == "LONG" else exit_price * (1.0 + s)


# ---------------------------------------------------------------------------
# Pool load + feature prep
# ---------------------------------------------------------------------------
def _family_setups(prefix: str, available: set[str] | None = None) -> list[str]:
    # Derive the family from v6 exits + the conf book + whatever is actually in the
    # pool, so tier123/new-setups (e.g. P_PDH_BREAK_RETEST_LONG) and any newly-mined
    # setup present in the unified pool are tunable, not just v6.SETUP_EXIT_RULES.
    universe = set(v11.v6.SETUP_EXIT_RULES)
    try:
        import final_setup_conf as _fc
        universe |= set(_fc.FINAL_SETUP_CONF)
    except Exception:
        pass
    if available:
        universe |= {str(s) for s in available}
    return sorted(
        s for s in universe
        if s.startswith(f"{prefix.upper()}_") and s not in EXCLUDED
    )


def _read_one_pool(pool_dir: Path) -> list[pd.DataFrame]:
    one = pool_dir / "historical_all_available_pre_dedupe_live_candidates.csv"
    frames = []
    # low_memory=False: the pool has wide, sparsely-populated feature columns that
    # otherwise infer as mixed/object dtype (the DtypeWarning), which makes
    # pd.to_numeric(..., errors="coerce") silently NaN-out valid values in the mask
    # filters. Read whole columns at once so dtypes are consistent (matches the
    # builder + walk_forward lean loader).
    if one.exists():
        frames.append(pd.read_csv(one, low_memory=False))
    # Always also sweep per-day files (a partial / in-progress build only has these).
    days = sorted((pool_dir / "historical_all_available_days").glob("*/pre_dedupe_live_candidates.csv"))
    # Chunked layout: <pool_dir>/chunk_YYYYMM/historical_all_available_days/<date>/...
    days += sorted(pool_dir.glob("chunk_*/historical_all_available_days/*/pre_dedupe_live_candidates.csv"))
    for p in days:
        try:
            frames.append(pd.read_csv(p, low_memory=False))
        except Exception:
            continue
    return frames


def load_pool() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for pd_dir in POOL_DIRS:
        frames.extend(_read_one_pool(Path(pd_dir)))
    if not frames:
        raise SystemExit(f"[train_test] no pool found under {POOL_DIRS}; run the pool builder first")
    df = pd.concat(frames, ignore_index=True)
    # Dedupe across pools (overlapping calendar days) by candidate identity.
    for c in ("ticker", "side", "setup", "signal_time_ist"):
        if c not in df.columns:
            df[c] = ""
    df = df.drop_duplicates(subset=["ticker", "side", "setup", "signal_time_ist"], keep="first").reset_index(drop=True)
    df["setup"] = df["setup"].astype(str).str.strip()
    df["side"] = df["side"].astype(str).str.upper().str.strip()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["tt_sig_ts"] = df["signal_time_ist"].map(v11._normalise_ts)
    df = df.dropna(subset=["tt_sig_ts"]).copy()
    df["_day"] = df["tt_sig_ts"].dt.normalize().dt.tz_localize(None)
    df["_slot"] = df["tt_sig_ts"].map(v11._fmt_ist)
    df = v11._selected_strategy_features(df)  # adds signal_minute, wicks, signal_range_pct, market_abs_ret_pct
    return df


def split_train_test(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    d = df["_day"]
    tr = df[(d >= pd.Timestamp(TRAIN[0])) & (d <= pd.Timestamp(TRAIN[1]))].copy()
    te = df[(d >= pd.Timestamp(TEST[0])) & (d <= pd.Timestamp(TEST[1]))].copy()
    return tr.reset_index(drop=True), te.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Entry / resolution / pre-momentum (reuse v11 helpers, cached)
# ---------------------------------------------------------------------------
@lru_cache(maxsize=200_000)
def _entry(ticker: str, side: str, sig_iso: str) -> tuple | None:
    sig_ts = v11._normalise_ts(sig_iso)
    # G1: model live feed+engine latency by pushing the intended entry later.
    if ENTRY_LATENCY_MIN:
        sig_ts = sig_ts + pd.Timedelta(minutes=int(ENTRY_LATENCY_MIN))
    bars = v11._load_1m_with_open(ticker)
    if bars is None or bars.empty:
        return None
    e = v11._first_1m_entry(bars, sig_ts, max_delay_minutes=v11.V7_ENTRY_SEARCH_MAX_DELAY_MIN)
    if e is None:
        return None
    entry_ts, raw_open = e
    signal_entry = round(float(raw_open), 2)
    if signal_entry <= 0:
        return None
    # G2: adverse entry slippage/half-spread (bps -> fraction). LONG buys higher,
    # SHORT sells lower.
    slip = SLIPPAGE_BPS / 1e4
    fill = round(signal_entry * (1 + slip), 2) if side == "LONG" else round(signal_entry * (1 - slip), 2)
    qty = max(1, int(v11.V7_SIGNAL_NOTIONAL_RS / signal_entry))
    return entry_ts.isoformat(), fill, int(qty)


@lru_cache(maxsize=400_000)
def _resolve_full(ticker: str, side: str, entry_iso: str, fill: float, qty: int, sl: float, tgt: float) -> tuple | None:
    """Resolve one trade to its exit. Cost-model-AGNOSTIC (cached once, reused by every
    cost model): returns (exit_iso, outcome, exit_px_slipped). exit_px already carries
    the G2 exit-leg slippage."""
    bars = v11._load_1m_with_open(ticker)
    if bars is None or bars.empty:
        return None
    res = v11.er.resolve(bars=bars, side=side, entry_price=fill,
                         entry_time_ist=pd.Timestamp(entry_iso), sl_pct=sl, tgt_pct=tgt)
    if res is None:
        return None
    exit_px = _exit_with_slippage(side, float(res.exit_price))   # G2: exit-leg slippage
    return res.exit_time_ist.isoformat(), str(res.outcome), float(exit_px)


def _trade_net(side: str, fill: float, qty: int, outcome: str, exit_px: float,
               model: str | None = None) -> float:
    """Net P&L for one resolved trade under the selected cost model (G4).
      statutory: per-trade NSE statutory costs (brokerage cap, STT, exch, stamp, GST).
      flat_bps : v6/v11's flat cost_bps on the fixed effective notional (+3bps on SL),
                 so the tuner can judge with the SAME cost function as the v11 backtest.
    Both use the realistic (slipped) fill/exit prices, so G2 spread is always respected."""
    model = model or COST_MODEL
    if model == "flat_bps":
        gross = (exit_px - fill) * qty if str(side).upper() == "LONG" else (fill - exit_px) * qty
        extra = v11.v6.STOP_EXTRA_BPS if str(outcome).upper() == "SL" else 0.0
        cost = v11.v6.EFFECTIVE_NOTIONAL * ((V6_COST_BPS + extra) / 10_000.0)
        return float(gross - cost)
    return float(wfg.net_pnl_vectorized(np.array([fill]), np.array([exit_px]),
                                        np.array([qty]), np.array([side]), CFG)[0])


def _resolve_net(ticker: str, side: str, entry_iso: str, fill: float, qty: int, sl: float, tgt: float) -> float | None:
    """Net P&L under the active COST_MODEL (thin wrapper over the cached resolver)."""
    r = _resolve_full(ticker, side, entry_iso, fill, qty, sl, tgt)
    if r is None:
        return None
    _exit_iso, outcome, exit_px = r
    return _trade_net(side, float(fill), int(qty), outcome, float(exit_px))


@lru_cache(maxsize=200_000)
def _premom(ticker: str, side: str, entry_iso: str, fill: float, sl: float, sig_iso: str) -> tuple:
    stop = fill * (1 - sl / 100.0) if side == "LONG" else fill * (1 + sl / 100.0)
    feats, reason = v11._pre_entry_momentum_features_v11(
        ticker, side, fill, stop, pd.Timestamp(entry_iso), v11._normalise_ts(sig_iso))
    return tuple(sorted((k, float(v)) for k, v in feats.items() if v is not None and np.isfinite(v))), reason


def attach_entries(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        out["tt_entry_ok"] = pd.Series(dtype=bool)
        out["tt_entry_iso"] = pd.Series(dtype=object)
        out["tt_fill"] = pd.Series(dtype=float)
        out["tt_qty"] = pd.Series(dtype=int)
        out["notional"] = pd.Series(dtype=float)
        return out.reset_index(drop=True)
    ent = [_entry(r.ticker, r.side, r.tt_sig_ts.isoformat()) for r in out.itertuples()]
    out["tt_entry_ok"] = [e is not None for e in ent]
    out["tt_entry_iso"] = [e[0] if e else None for e in ent]
    out["tt_fill"] = [e[1] if e else np.nan for e in ent]
    out["tt_qty"] = [e[2] if e else 0 for e in ent]
    out = out[out["tt_entry_ok"]].copy()
    out["notional"] = out["tt_fill"] * out["tt_qty"]
    return out.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Pipeline stages (parameterised)
# ---------------------------------------------------------------------------
def _hhmm_to_min(text: str | None) -> float | None:
    if not text:
        return None
    hh, mm = str(text).split(":")
    return int(hh) * 60 + int(mm)


def apply_guards(df: pd.DataFrame, guard: dict | None) -> pd.DataFrame:
    if not guard or df.empty:
        return df
    out = df
    mx = _hhmm_to_min(guard.get("max_slot"))
    mn = _hhmm_to_min(guard.get("min_slot"))
    if mn is not None:
        out = out[out["signal_minute"] >= mn]
    if mx is not None:
        out = out[out["signal_minute"] <= mx]
    top_n = guard.get("top_n")
    if top_n:
        out = (out.sort_values("vwap_dist_atr", ascending=False)
               .groupby(["_day", "_slot"], sort=False).head(int(top_n)))
    return out


def apply_mask_terms(df: pd.DataFrame, terms: list[tuple]) -> pd.DataFrame:
    """AND-combined selected-strategy mask. Mirrors the canonical conf mask
    (eqidv2_final_conf_live_bootstrap.conf_mask / v11 _final_setup_conf_mask):
    a STRING threshold is categorical (e.g. ["regime","!=","BULL"]; a missing
    column defaults to "" so != keeps all), a numeric threshold uses >=/<=/!=/==.
    (The tuner only generates numeric >=/<= terms, so its search is unchanged;
    this just lets us also evaluate hand-authored conf masks faithfully.)"""
    if df.empty or not terms:
        return df
    keep = pd.Series(True, index=df.index)
    for feat, op, thr in terms:
        if isinstance(thr, str):                       # categorical
            col = (df[feat] if feat in df.columns
                   else pd.Series("", index=df.index)).astype(str).str.upper()
            vv = thr.upper()
            keep &= col.ne(vv) if op == "!=" else col.eq(vv)
        else:                                          # numeric
            x = (pd.to_numeric(df[feat], errors="coerce") if feat in df.columns
                 else pd.Series(np.nan, index=df.index))
            if op == ">=":
                keep &= (x >= thr)
            elif op == "<=":
                keep &= (x <= thr)
            elif op == "!=":
                keep &= (x != thr)
            else:                                      # "=="
                keep &= (x == thr)
    return df[keep.fillna(False)]


def apply_premom_terms(df: pd.DataFrame, terms: list[tuple], sl: float) -> pd.DataFrame:
    if df.empty or not terms:
        return df
    keep = []
    for r in df.itertuples():
        feats, reason = _premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), float(sl), r.tt_sig_ts.isoformat())
        if reason:
            keep.append(False)
            continue
        fd = dict(feats)
        ok = True
        for feat, op, thr in terms:
            v = fd.get(feat)
            if v is None or not np.isfinite(v):
                ok = False
                break
            # L1: full operator coverage (premom features are numeric). Unknown ops
            # fail closed rather than being silently treated as "<=".
            if op == ">=":
                ok = v >= thr
            elif op == "<=":
                ok = v <= thr
            elif op == ">":
                ok = v > thr
            elif op == "<":
                ok = v < thr
            elif op == "==":
                ok = v == thr
            elif op == "!=":
                ok = v != thr
            else:
                ok = False
            if not ok:
                break
        keep.append(ok)
    return df[pd.Series(keep, index=df.index)]


def dedupe_family(df: pd.DataFrame) -> pd.DataFrame:
    """Mirror v11._select_v7_entry_engine_signals: best score per (slot,ticker),
    then one ticker per day across the family."""
    if df.empty:
        return df
    work = df.copy()
    work["_score"] = pd.to_numeric(work.get("quality_score", work.get("ranker_score", 0.0)), errors="coerce").fillna(0.0)
    # L2: when scores tie (e.g. tier-c rows with no quality_score -> all 0), break by
    # EARLIEST signal time (first-come, matching the live executor) instead of the old
    # alphabetical-by-setup bias; setup name is only the final deterministic tie-break.
    work = (work.sort_values(["_score", "tt_sig_ts", "setup"], ascending=[False, True, True])
            .drop_duplicates(subset=["_slot", "ticker"], keep="first"))
    keep = []
    for _, day_df in work.sort_values(["_day", "tt_sig_ts"]).groupby("_day", sort=True):
        used = set()
        for _, srow in day_df.sort_values(["_score", "tt_sig_ts", "ticker"], ascending=[False, True, True]).iterrows():
            t = srow["ticker"]
            if t in used:
                continue
            keep.append(srow)
            used.add(t)
    return pd.DataFrame(keep).reset_index(drop=True) if keep else df.iloc[0:0]


def resolve_book(df: pd.DataFrame, exits: dict[str, tuple]) -> np.ndarray:
    """Net per row using each row's setup exit from `exits` (under the active COST_MODEL)."""
    nets = []
    for r in df.itertuples():
        sl, tgt = exits[r.setup]
        n = _resolve_net(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), int(r.tt_qty), float(sl), float(tgt))
        nets.append(n if n is not None else np.nan)
    return np.array(nets, float)


def _book_net_both_models(df: pd.DataFrame, exits: dict[str, tuple]) -> tuple[float, float, int]:
    """G4 reconciliation: a book's total net under BOTH cost models, reusing the cached
    resolver (no re-resolve). Returns (statutory_sum, flat_bps_sum, n_resolved)."""
    s_sum = f_sum = 0.0
    n = 0
    for r in df.itertuples():
        sl, tgt = exits[r.setup]
        full = _resolve_full(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), int(r.tt_qty), float(sl), float(tgt))
        if full is None:
            continue
        _exit_iso, outcome, exit_px = full
        s_sum += _trade_net(r.side, float(r.tt_fill), int(r.tt_qty), outcome, float(exit_px), model="statutory")
        f_sum += _trade_net(r.side, float(r.tt_fill), int(r.tt_qty), outcome, float(exit_px), model="flat_bps")
        n += 1
    return s_sum, f_sum, n


def _pf(net: np.ndarray) -> float:
    net = net[np.isfinite(net)]
    return wfg._profit_factor(net) if len(net) else 0.0


def _day_block_p(df: pd.DataFrame, net: np.ndarray, n_boot=10000, seed=7) -> float:
    m = np.isfinite(net)
    if m.sum() < 3:
        return float("nan")
    daily = pd.Series(net[m], index=df["_day"].to_numpy()[m]).groupby(level=0).sum()
    if len(daily) < 3:
        return float("nan")
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(daily), size=(n_boot, len(daily)))
    return float((daily.to_numpy()[idx].mean(axis=1) <= 0).mean())


# ---------------------------------------------------------------------------
# Per-setup search
# ---------------------------------------------------------------------------
MIN_HALF_TRADES = 6


def _robust_obj(net: np.ndarray, half: np.ndarray) -> float:
    """In-train robustness objective: the WORSE of the two train-half net PFs.
    A config must work in both halves of the train window, not just overall —
    this kills the zero-loss in-sample subsets that raw-PF maximisation rewards."""
    net = np.asarray(net, float)
    half = np.asarray(half)
    m = np.isfinite(net)
    net, half = net[m], half[m]
    h1, h2 = net[half == 0], net[half == 1]
    if len(h1) < MIN_HALF_TRADES or len(h2) < MIN_HALF_TRADES:
        return 0.0
    return min(_pf(h1), _pf(h2))


def _greedy_maxobj(df: pd.DataFrame, features: list[str], net: np.ndarray, half: np.ndarray,
                   min_trades: int, max_terms: int) -> tuple[list, np.ndarray]:
    """Greedy forward selection MAXIMISING the robust objective (worse-half PF),
    subject to keeping >= min_trades. Used by the 'maxpf' objective to push
    training PF up with indicators + pre-momentum."""
    cur = pd.Series(True, index=df.index) & np.isfinite(net)
    terms: list[tuple] = []
    cur_obj = _robust_obj(net[cur.to_numpy()], half[cur.to_numpy()])
    for _ in range(max_terms):
        best = None
        for f in features:
            if f not in df.columns:
                continue
            x = pd.to_numeric(df[f], errors="coerce")
            base = cur & x.notna()
            if base.sum() < min_trades:
                continue
            for thr in np.unique(x[base].quantile(QUANTILES).to_numpy()):
                for op in (">=", "<="):
                    keep = base & ((x >= thr) if op == ">=" else (x <= thr))
                    if keep.sum() < min_trades:
                        continue
                    obj = _robust_obj(net[keep.to_numpy()], half[keep.to_numpy()])
                    if best is None or obj > best[0]:
                        best = (obj, f, op, float(thr), keep)
        if best is None or best[0] <= cur_obj + 1e-9:
            break
        cur_obj, f, op, thr, cur = best
        terms.append((f, op, round(thr, 4)))
    return terms, cur.to_numpy()


def _mask_bool(df: pd.DataFrame, terms: list[tuple]) -> np.ndarray:
    keep = pd.Series(True, index=df.index)
    for f, op, thr in terms:
        x = pd.to_numeric(df[f], errors="coerce")
        keep &= (x >= thr) if op == ">=" else (x <= thr)
    return keep.fillna(False).to_numpy()


def _in_band(net: np.ndarray, half: np.ndarray, pf_min: float) -> bool:
    return _pf(net) >= pf_min and _robust_obj(net, half) >= KEEP_MIN_MINHALF_PF


def _greedy_to_band(df: pd.DataFrame, features: list[str], net: np.ndarray, half: np.ndarray,
                    min_trades: int, max_terms: int, pf_min: float) -> list[tuple]:
    """Add the FEWEST threshold terms needed to lift net PF to >= pf_min while
    keeping the MOST trades. If the loosest (no-filter) book is already >= pf_min,
    return [] (keep everything). Among terms that reach the band, the one retaining
    the most trades wins; otherwise climb PF. This targets the [1.5, 2.0] band with
    maximum trade count instead of maximising PF (which overfits)."""
    cur = pd.Series(True, index=df.index) & np.isfinite(net)
    if _in_band(net[cur.to_numpy()], half[cur.to_numpy()], pf_min):
        return []
    terms: list[tuple] = []
    for _ in range(max_terms):
        cur_pf = _pf(net[cur.to_numpy()])
        best = None  # (score_tuple, f, op, thr, keep, pf, reaches)
        for f in features:
            if f not in df.columns:
                continue
            x = pd.to_numeric(df[f], errors="coerce")
            base = cur & x.notna()
            if base.sum() < min_trades:
                continue
            for thr in np.unique(x[base].quantile(QUANTILES).to_numpy()):
                for op in (">=", "<="):
                    keep = base & ((x >= thr) if op == ">=" else (x <= thr))
                    ka = keep.to_numpy()
                    n = int(ka.sum())
                    if n < min_trades:
                        continue
                    pf = _pf(net[ka])
                    reaches = _in_band(net[ka], half[ka], pf_min)
                    # reaching the band beats not; among reaching maximise trades;
                    # among non-reaching maximise PF (climb toward band).
                    score = (1 if reaches else 0, n if reaches else pf)
                    if best is None or score > best[0]:
                        best = (score, f, op, float(thr), keep, pf, reaches)
        if best is None:
            break
        _, f, op, thr, keep, pf, reaches = best
        if not reaches and pf <= cur_pf + 1e-9:
            break  # cannot climb further
        terms.append((f, op, round(thr, 4)))
        cur = keep
        if reaches:
            break  # in band with max trades — stop, don't over-tighten
    return terms


def search_setup(setup: str, tr: pd.DataFrame) -> dict | None:
    rows = tr[tr["setup"] == setup].copy()
    if len(rows) < MIN_TRAIN_TRADES:
        return {"setup": setup, "status": "TOO_FEW_TRAIN", "n_train": int(len(rows))}
    rows = rows.reset_index(drop=True)
    # Split the train window into two halves for the robustness objective (config must
    # work in BOTH halves, not just overall). L3: pick the whole-day split point whose
    # cumulative TRADE count is closest to 50/50 (not just the median DAY), so the
    # both-halves check isn't skewed when trades-per-day varies; both halves stay
    # non-empty by construction.
    _day_counts = rows.groupby("_day").size().sort_index()
    if len(_day_counts) < 2:
        rows["tt_half"] = 0
    else:
        _cum = _day_counts.cumsum().to_numpy()
        _order = list(_day_counts.index)
        _best_i = min(range(len(_order) - 1), key=lambda i: abs(_cum[i] - _cum[-1] / 2.0))
        rows["tt_half"] = (rows["_day"] > _order[_best_i]).astype(int)
    guards = GUARD_GRID.get(setup, [None])
    cands: list[dict] = []
    for guard in guards:
        g = apply_guards(rows, guard)
        if len(g) < MIN_TRAIN_TRADES:
            continue
        half_g = g["tt_half"].to_numpy()
        for sl in SL_GRID:
            for tgt in TGT_GRID:
                net = resolve_book(g, {setup: (sl, tgt)})
                if np.isfinite(net).sum() < MIN_TRAIN_TRADES:
                    continue
                # --- mask term selection ---
                if OBJECTIVE == "maxpf":
                    mask_terms, mkeep = _greedy_maxobj(g, SIGNAL_FEATURES, net, half_g, MIN_TRAIN_TRADES, MAX_MASK_TERMS)
                else:
                    mask_terms = _greedy_to_band(g, SIGNAL_FEATURES, net, half_g, MIN_TRAIN_TRADES, MAX_MASK_TERMS, TRAIN_PF_MIN)
                    mkeep = _mask_bool(g, mask_terms)
                keep = mkeep & np.isfinite(net)
                if keep.sum() < MIN_TRAIN_TRADES:
                    continue
                # --- pre-momentum term selection ---
                premom_terms: list[tuple] = []
                want_premom = MAX_PREMOM_TERMS > 0 and (
                    FORCE_PREMOM or (OBJECTIVE == "band" and not _in_band(net[keep], half_g[keep], TRAIN_PF_MIN))
                )
                if want_premom:
                    gk = g[keep]
                    pm_recs = []
                    for r in gk.itertuples():
                        feats, reason = _premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill),
                                                float(sl), r.tt_sig_ts.isoformat())
                        fd = dict(feats) if not reason else {}
                        pm_recs.append({f: fd.get(f, np.nan) for f in PREMOM_FEATURES})
                    pm_df = pd.DataFrame(pm_recs, index=gk.index)
                    net_k, half_k = net[keep], half_g[keep]
                    if OBJECTIVE == "maxpf":
                        premom_terms, pk = _greedy_maxobj(pm_df, PREMOM_FEATURES, net_k, half_k, MIN_TRAIN_TRADES, MAX_PREMOM_TERMS)
                    else:
                        premom_terms = _greedy_to_band(pm_df, PREMOM_FEATURES, net_k, half_k, MIN_TRAIN_TRADES, MAX_PREMOM_TERMS, TRAIN_PF_MIN)
                        pk = _mask_bool(pm_df, premom_terms)
                    if premom_terms:
                        idx = np.where(keep)[0][pk]
                        keep2 = np.zeros(len(g), dtype=bool)
                        keep2[idx] = True
                        # keep the gate only if it helps the robust objective and retains enough trades
                        if keep2.sum() >= MIN_TRAIN_TRADES and _robust_obj(net[keep2], half_g[keep2]) >= _robust_obj(net[keep], half_g[keep]):
                            keep = keep2
                        else:
                            premom_terms = []
                netk = net[keep]
                pf = _pf(netk)
                mh = _robust_obj(netk, half_g[keep])
                n = int(np.isfinite(netk).sum())
                qualifies = (pf >= TRAIN_PF_MIN and mh >= KEEP_MIN_MINHALF_PF and n >= MIN_TRAIN_TRADES)
                cands.append({"guard": guard, "sl": sl, "tgt": tgt, "mask_terms": mask_terms,
                              "premom_terms": premom_terms, "train_pf": pf, "obj": mh,
                              "train_n": n, "train_net": float(np.nansum(netk)), "qualifies": qualifies})
    if not cands:
        return {"setup": setup, "status": "NO_VIABLE_CONFIG", "n_train": int(len(rows))}

    quals = [c for c in cands if c["qualifies"]]
    if OBJECTIVE == "maxpf":
        # maximise robust PF (subject to the min-trade floor already enforced); tie-break on trades.
        best = max(quals or cands, key=lambda c: (c["train_pf"], c["train_n"]))
        best["status"] = "OK" if quals else "DROP_NO_EDGE"
    elif quals:
        # maximise trade count; tie-break toward the middle of the band, then higher min-half PF.
        best = max(quals, key=lambda c: (c["train_n"], -abs(c["train_pf"] - (TRAIN_PF_MIN + TRAIN_PF_MAX) / 2), c["obj"]))
        best["status"] = "OK"
    else:
        best = max(cands, key=lambda c: c["train_pf"])
        best["status"] = "DROP_NO_EDGE"
    best["in_band"] = bool(TRAIN_PF_MIN <= best["train_pf"] <= TRAIN_PF_MAX)
    best["setup"] = setup
    return best


# ---------------------------------------------------------------------------
# Family assembly + evaluation
# ---------------------------------------------------------------------------
def _apply_regime_align(book: pd.DataFrame) -> pd.DataFrame:
    """§11.4 book-level regime filter (opt-in): don't fight the tape. Drop LONGs when the
    market is down more than REGIME_BAND and SHORTs when it is up more than REGIME_BAND.
    Rows with no market_ret_pct are kept (unknown regime -> not filtered). No-op if off."""
    if not REGIME_ALIGN or book.empty or "market_ret_pct" not in book.columns:
        return book
    mr = pd.to_numeric(book["market_ret_pct"], errors="coerce")
    side = book["side"].astype(str).str.upper()
    drop = ((side == "LONG") & (mr < -REGIME_BAND)) | ((side == "SHORT") & (mr > REGIME_BAND))
    return book[~drop.fillna(False)]


def _apply_portfolio_overlay(book: pd.DataFrame, exits: dict[str, tuple]) -> pd.DataFrame:
    """G5: make the book the DEPLOYABLE book via an event-driven simulation —
      * concurrent-position cap (MAX_POSITIONS, the live executor limit): a new entry
        is skipped when MAX_POSITIONS are already open at its entry time;
      * daily-loss kill-switch (DAILY_LOSS_RS): once a day's REALIZED net <= -limit,
        block new entries for the rest of that day (open positions still run to exit).
    Positions are walked in entry-time order; a position's net is realized at its exit.
    Returns the subset of `book` actually taken (entry-time order). No-op if both are off."""
    cap = int(MAX_POSITIONS) if MAX_POSITIONS else 0
    limit = abs(float(DAILY_LOSS_RS)) if DAILY_LOSS_RS else 0.0
    if book.empty or (cap <= 0 and limit <= 0):
        return book
    recs = []
    for r in book.itertuples():
        sl, tgt = exits[r.setup]
        full = _resolve_full(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), int(r.tt_qty), float(sl), float(tgt))
        if full is None:
            continue
        exit_iso, outcome, exit_px = full
        net = _trade_net(r.side, float(r.tt_fill), int(r.tt_qty), outcome, float(exit_px))
        entry_ts = pd.Timestamp(r.tt_entry_iso)
        recs.append((entry_ts, pd.Timestamp(exit_iso), float(net),
                     entry_ts.normalize().tz_localize(None), r.Index))
    if not recs:
        return book.iloc[0:0]
    recs.sort(key=lambda x: (x[0], x[4]))   # entry time, then stable by original index
    open_pos: list[tuple] = []              # (exit_ts, net, day)
    realized_by_day: dict = {}
    taken_idx = []
    for entry_ts, exit_ts, net, day, idx in recs:
        # realize positions that have closed by now
        still = []
        for ex_ts, n, dy in open_pos:
            if ex_ts <= entry_ts:
                realized_by_day[dy] = realized_by_day.get(dy, 0.0) + n
            else:
                still.append((ex_ts, n, dy))
        open_pos = still
        if limit > 0 and realized_by_day.get(day, 0.0) <= -limit:
            continue                        # daily kill-switch tripped for this day
        if cap > 0 and len(open_pos) >= cap:
            continue                        # at capacity
        taken_idx.append(idx)
        open_pos.append((exit_ts, net, day))
    return book.loc[taken_idx]


def eval_family(configs: dict[str, dict], df: pd.DataFrame) -> dict:
    """Run the full pipeline (guards+premom -> family dedupe -> mask -> portfolio
    overlay -> resolve) for the family on a period and return net metrics."""
    survivors = []
    for setup, c in configs.items():
        if c.get("status") != "OK":
            continue
        rows = df[df["setup"] == setup].copy()
        rows = apply_guards(rows, c["guard"])
        rows = apply_premom_terms(rows, c["premom_terms"], c["sl"])
        survivors.append(rows)
    pre = pd.concat(survivors, ignore_index=True) if survivors else df.iloc[0:0].copy()
    deduped = dedupe_family(pre)
    kept = []
    for setup, c in configs.items():
        if c.get("status") != "OK":
            continue
        srows = deduped[deduped["setup"] == setup]
        kept.append(apply_mask_terms(srows, c["mask_terms"]))
    book = pd.concat(kept, ignore_index=True) if kept else df.iloc[0:0].copy()
    if book.empty:
        return {"trades": 0, "net_pf": 0.0, "net_pnl": 0.0, "day_block_p": float("nan"), "book": book, "net": np.array([])}
    exits = {s: (c["sl"], c["tgt"]) for s, c in configs.items() if c.get("status") == "OK"}
    book = _apply_regime_align(book)               # §11.4: book-level regime alignment (opt-in)
    book = _apply_portfolio_overlay(book, exits)   # G5: position cap + daily-loss stop
    if book.empty:
        return {"trades": 0, "net_pf": 0.0, "net_pnl": 0.0, "day_block_p": float("nan"), "book": book, "net": np.array([])}
    net = resolve_book(book, exits)
    return {"trades": int(np.isfinite(net).sum()), "net_pf": _pf(net),
            "net_pnl": float(np.nansum(net)), "day_block_p": _day_block_p(book, net),
            "book": book, "net": net}


def book_detail(book: pd.DataFrame, exits: dict[str, tuple]) -> pd.DataFrame:
    """Per-trade detail (entry/exit/outcome/gross/net) for a resolved family book."""
    rows = []
    for r in book.itertuples():
        sl, tgt = exits[r.setup]
        bars = v11._load_1m_with_open(r.ticker)
        res = None
        if bars is not None and not bars.empty:
            res = v11.er.resolve(bars=bars, side=r.side, entry_price=float(r.tt_fill),
                                 entry_time_ist=pd.Timestamp(r.tt_entry_iso), sl_pct=float(sl), tgt_pct=float(tgt))
        if res is None:
            continue
        exit_px = _exit_with_slippage(r.side, float(res.exit_price))   # G2: exit-leg slippage
        gross = ((exit_px - r.tt_fill) if r.side == "LONG" else (r.tt_fill - exit_px)) * r.tt_qty
        net = _trade_net(r.side, float(r.tt_fill), int(r.tt_qty), str(res.outcome), float(exit_px))  # G4
        rows.append({
            "trade_date": r.tt_sig_ts.date(), "ticker": r.ticker, "side": r.side, "setup": r.setup,
            "signal_time": str(r.tt_sig_ts), "entry_time": str(r.tt_entry_iso),
            "entry_price": round(float(r.tt_fill), 2), "exit_price": round(float(exit_px), 2),
            "qty": int(r.tt_qty), "sl_pct": sl, "tgt_pct": tgt, "outcome": res.outcome,
            "bars_held": int(res.bars_held), "gross_pnl_rs": round(float(gross), 2), "net_pnl_rs": round(net, 2),
        })
    return pd.DataFrame(rows)


def summarize_detail(detail: pd.DataFrame, label: str) -> pd.DataFrame:
    """Per-setup + TOTAL metrics for a period's book (net of cost)."""
    if detail.empty:
        return pd.DataFrame()
    out = []
    for key, g in list(detail.groupby("setup")) + [("__TOTAL__", detail)]:
        net = g["net_pnl_rs"].to_numpy()
        oc = g["outcome"].astype(str)
        n = len(g)
        out.append({
            "period": label, "setup": key, "side": (g["side"].mode().iloc[0] if key != "__TOTAL__" else ""),
            "n": n, "win_pct": round(float((net > 0).mean()) * 100, 1),
            "tgt_pct": round(float((oc == "TARGET").mean()) * 100, 1),
            "sl_pct_rate": round(float((oc == "SL").mean()) * 100, 1),
            "eod_pct": round(float((oc == "EOD").mean()) * 100, 1),
            "net_pf": round(_pf(net), 3), "net_pnl_rs": round(float(net.sum()), 0),
            "net_per_trade": round(float(net.mean()), 1) if n else 0.0,
            "gross_pnl_rs": round(float(g["gross_pnl_rs"].sum()), 0),
        })
    return pd.DataFrame(out)


def weekly_summary(detail: pd.DataFrame, label: str) -> pd.DataFrame:
    """Week-wise (ISO week) net metrics for a period's family book."""
    if detail.empty:
        return pd.DataFrame()
    d = detail.copy()
    d["week"] = pd.to_datetime(d["trade_date"]).dt.strftime("%G-W%V")
    out = []
    for wk, g in d.groupby("week"):
        net = g["net_pnl_rs"].to_numpy()
        out.append({
            "period": label, "week": wk, "n": int(len(g)),
            "win_pct": round(float((net > 0).mean()) * 100, 1),
            "net_pf": round(_pf(net), 3), "net_pnl_rs": round(float(net.sum()), 0),
            "setups": ",".join(sorted(g["setup"].unique())),
        })
    return pd.DataFrame(out)


def accept_verdict(train: dict, test: dict) -> tuple[bool, str]:
    ratio = (test["net_pf"] / train["net_pf"]) if train["net_pf"] > 0 else 0.0
    reasons = []
    if train["net_pf"] < TRAIN_MIN_NET_PF:
        reasons.append(f"train_pf {train['net_pf']:.2f}<{TRAIN_MIN_NET_PF}")
    if test["trades"] < MIN_TEST_TRADES:
        reasons.append(f"test_n {test['trades']}<{MIN_TEST_TRADES}")
    if test["net_pf"] < TEST_MIN_NET_PF:
        reasons.append(f"test_pf {test['net_pf']:.2f}<{TEST_MIN_NET_PF}")
    if not (np.isfinite(test["day_block_p"]) and test["day_block_p"] <= TEST_MAX_DAY_BLOCK_P):
        reasons.append(f"test_day_block_p {test['day_block_p']}>{TEST_MAX_DAY_BLOCK_P}")
    if ratio < MIN_TEST_TRAIN_PF_RATIO:
        reasons.append(f"oos/is ratio {ratio:.2f}<{MIN_TEST_TRAIN_PF_RATIO}")
    return (len(reasons) == 0), ("ACCEPT" if not reasons else "REJECT: " + "; ".join(reasons))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    global POOL_DIR, POOL_DIRS, PROPOSAL_DIR, TRAIN, TEST, MAX_MASK_TERMS, MAX_PREMOM_TERMS, QUANTILES
    global TRAIN_PF_MIN, TRAIN_PF_MAX, TRAIN_MIN_NET_PF, KEEP_MIN_TRAIN_PF
    global OBJECTIVE, FORCE_PREMOM, MIN_TRAIN_TRADES
    global SIGNAL_FEATURES, PREMOM_FEATURES, KEEP_MIN_MINHALF_PF
    global TEST_MIN_NET_PF, TEST_MAX_DAY_BLOCK_P, MIN_TEST_TRAIN_PF_RATIO, MIN_TEST_TRADES
    global SLIPPAGE_BPS, ENTRY_LATENCY_MIN, COST_MODEL, V6_COST_BPS, MAX_POSITIONS, DAILY_LOSS_RS
    global REGIME_ALIGN, REGIME_BAND
    ap = argparse.ArgumentParser(description="Honest per-family train/test setup tuning")
    ap.add_argument("--family", required=True, help="setup family prefix, e.g. A, B, C, D, E, G, L, S, T, MR")
    ap.add_argument("--setups", type=str, default="", help="comma list to restrict the family to specific setups (isolation)")
    ap.add_argument("--approve", action="store_true", help="write the accepted config into final_setup_conf.py (keeps only the OK setups from this run)")
    ap.add_argument("--pool_dir", type=str, default="", help="override pool directory (validation)")
    ap.add_argument("--train_start", type=str, default="")
    ap.add_argument("--train_end", type=str, default="")
    ap.add_argument("--test_start", type=str, default="")
    ap.add_argument("--test_end", type=str, default="")
    ap.add_argument("--max_mask_terms", type=int, default=MAX_MASK_TERMS, help="widen: max selected-strategy mask threshold terms per setup")
    ap.add_argument("--max_premom_terms", type=int, default=MAX_PREMOM_TERMS, help="widen: max pre-momentum gate terms per setup")
    ap.add_argument("--fine_quantiles", action="store_true", help="widen: use a finer threshold grid (9 quantiles)")
    ap.add_argument("--train_pf_min", type=float, default=TRAIN_PF_MIN, help="lower edge of the train PF band (search target / accept floor)")
    ap.add_argument("--train_pf_max", type=float, default=TRAIN_PF_MAX, help="upper edge of the train PF band (preference)")
    ap.add_argument("--objective", choices=["band", "maxpf"], default=OBJECTIVE,
                    help="band=max trades at PF>=min; maxpf=maximise robust PF subject to --min_train_trades")
    ap.add_argument("--min_train_trades", type=int, default=MIN_TRAIN_TRADES, help="min trades a setup must keep (trade-count floor)")
    ap.add_argument("--force_premom", action="store_true", help="always fit/try a pre-momentum gate (esp. with --objective maxpf)")
    # ---- STRICTER controls (anti-overfit) ----
    ap.add_argument("--exclude_features", type=str, default=",".join(sorted(DEFAULT_EXCLUDED_FEATURES)),
                    help="comma list of features to DROP from the mask + premom search universe "
                         "(default bans regime/time/notional overfit vectors; pass an explicit list to override)")
    ap.add_argument("--min_minhalf_pf", type=float, default=KEEP_MIN_MINHALF_PF,
                    help=f"stricter: minimum PF required in the WORSE train half (default {KEEP_MIN_MINHALF_PF})")
    ap.add_argument("--test_min_pf", type=float, default=TEST_MIN_NET_PF, help="stricter accept gate: min test PF")
    ap.add_argument("--max_day_block_p", type=float, default=TEST_MAX_DAY_BLOCK_P, help="stricter accept gate: max test day_block_p")
    ap.add_argument("--min_oos_ratio", type=float, default=MIN_TEST_TRAIN_PF_RATIO, help="stricter accept gate: min test/train PF ratio")
    ap.add_argument("--min_test_trades", type=int, default=MIN_TEST_TRADES, help="stricter accept gate: min test trades")
    # ---- Execution realism (G1/G2) + multiple-testing (G3) ----
    ap.add_argument("--slippage_bps", type=float, default=SLIPPAGE_BPS,
                    help="adverse per-leg slippage/half-spread (bps) on BOTH entry and exit fills (G2; default 15)")
    ap.add_argument("--entry_latency_min", type=int, default=ENTRY_LATENCY_MIN,
                    help="extra minutes of live feed+engine lag beyond the signal+1min open (G1 realism/stress; default 0)")
    ap.add_argument("--fdr_alpha", type=float, default=0.10,
                    help="Benjamini-Hochberg FDR level across the family's setups (G3 multiple-testing gate)")
    ap.add_argument("--no_fdr", action="store_true",
                    help="report the BH-FDR p-values but do NOT drop FDR-insignificant setups")
    # ---- cost model (G4) + portfolio overlay (G5) ----
    ap.add_argument("--cost_model", choices=["statutory", "flat_bps"], default=COST_MODEL,
                    help="statutory = per-trade NSE costs (default); flat_bps = v6/v11 flat cost_bps "
                         "(use with --slippage_bps 0 to judge with the SAME cost function as the v11 backtest)")
    ap.add_argument("--cost_bps", type=float, default=V6_COST_BPS,
                    help="flat round-trip cost (bps) used in --cost_model flat_bps (default = v6.DEFAULT_COST_BPS)")
    ap.add_argument("--max_positions", type=int, default=MAX_POSITIONS,
                    help="G5 concurrent-position cap (mirrors the live executor; 0 = off)")
    ap.add_argument("--daily_loss_rs", type=float, default=DAILY_LOSS_RS,
                    help="G5 daily-loss kill-switch in Rs (block new entries once a day's realized net <= -this; 0 = off)")
    # ---- book-level regime alignment (§11.4) ----
    ap.add_argument("--regime_align", action="store_true",
                    help="book-level 'don't fight the tape' filter: drop LONGs in a down market / SHORTs in an up market")
    ap.add_argument("--regime_band", type=float, default=REGIME_BAND,
                    help="market_ret_pct band for --regime_align (0 = strict sign alignment; e.g. 0.3 = neutral zone)")
    args = ap.parse_args()
    SLIPPAGE_BPS = float(args.slippage_bps)
    ENTRY_LATENCY_MIN = int(args.entry_latency_min)
    COST_MODEL = str(args.cost_model)
    V6_COST_BPS = float(args.cost_bps)
    MAX_POSITIONS = int(args.max_positions)
    DAILY_LOSS_RS = float(args.daily_loss_rs)
    print(f"[train_test] execution realism: slippage={SLIPPAGE_BPS:.1f} bps/leg (entry+exit) "
          f"entry_latency=+{ENTRY_LATENCY_MIN} min | cost_model={COST_MODEL}"
          f"{f'({V6_COST_BPS:.0f}bps)' if COST_MODEL == 'flat_bps' else ''} | FDR alpha={args.fdr_alpha}"
          f"{' (report-only)' if args.no_fdr else ''}")
    REGIME_ALIGN = bool(args.regime_align)
    REGIME_BAND = float(args.regime_band)
    print(f"[train_test] portfolio overlay (G5): max_positions={MAX_POSITIONS or 'off'} "
          f"daily_loss_stop={('Rs ' + format(DAILY_LOSS_RS, ',.0f')) if DAILY_LOSS_RS else 'off'} "
          f"| regime_align={'on (band ' + format(REGIME_BAND, '.2f') + ')' if REGIME_ALIGN else 'off'}")
    # Apply stricter accept gate + robustness floor + feature ban.
    KEEP_MIN_MINHALF_PF = float(args.min_minhalf_pf)
    TEST_MIN_NET_PF = float(args.test_min_pf)
    TEST_MAX_DAY_BLOCK_P = float(args.max_day_block_p)
    MIN_TEST_TRAIN_PF_RATIO = float(args.min_oos_ratio)
    MIN_TEST_TRADES = int(args.min_test_trades)
    _excl = {f.strip() for f in str(args.exclude_features).split(",") if f.strip()}
    if _excl:
        SIGNAL_FEATURES = [f for f in SIGNAL_FEATURES if f not in _excl]
        PREMOM_FEATURES = [f for f in PREMOM_FEATURES if f not in _excl]
        print(f"[train_test] STRICT: excluded features {sorted(_excl)}; "
              f"mask_universe={len(SIGNAL_FEATURES)} premom_universe={len(PREMOM_FEATURES)}")
    print(f"[train_test] STRICT gate: test_pf>={TEST_MIN_NET_PF} day_block_p<={TEST_MAX_DAY_BLOCK_P} "
          f"oos/is>={MIN_TEST_TRAIN_PF_RATIO} test_n>={MIN_TEST_TRADES} minhalf_pf>={KEEP_MIN_MINHALF_PF}")
    TRAIN_PF_MIN = float(args.train_pf_min)
    TRAIN_PF_MAX = float(args.train_pf_max)
    TRAIN_MIN_NET_PF = TRAIN_PF_MIN
    KEEP_MIN_TRAIN_PF = TRAIN_PF_MIN
    OBJECTIVE = str(args.objective)
    MIN_TRAIN_TRADES = int(args.min_train_trades)
    FORCE_PREMOM = bool(args.force_premom)
    print(f"[train_test] objective={OBJECTIVE} min_train_trades={MIN_TRAIN_TRADES} force_premom={FORCE_PREMOM}")
    if args.pool_dir:
        POOL_DIRS = [Path(p.strip()) for p in str(args.pool_dir).split(",") if p.strip()]
        POOL_DIR = POOL_DIRS[0]
        PROPOSAL_DIR = POOL_DIR / "proposals"   # keep proposals next to the pool that produced them
    TRAIN = (args.train_start or TRAIN[0], args.train_end or TRAIN[1])
    TEST = (args.test_start or TEST[0], args.test_end or TEST[1])
    _pinned = any([args.train_start, args.train_end, args.test_start, args.test_end])
    print(f"[train_test] window {'(PINNED via CLI)' if _pinned else '(dynamic: ' + _WIN['policy'] + ', today=' + _WIN['today'] + ')'}: "
          f"TRAIN {TRAIN[0]}..{TRAIN[1]}  TEST {TEST[0]}..{TEST[1]}")
    MAX_MASK_TERMS = int(args.max_mask_terms)
    MAX_PREMOM_TERMS = int(args.max_premom_terms)
    if args.fine_quantiles:
        QUANTILES = FINE_QUANTILES
    print(f"[train_test] search depth: max_mask_terms={MAX_MASK_TERMS} max_premom_terms={MAX_PREMOM_TERMS} "
          f"quantiles={'fine(9)' if args.fine_quantiles else 'coarse(5)'}")
    prefix = args.family.upper()
    pool = load_pool()
    setups = _family_setups(prefix, available=set(pool["setup"].astype(str)) if "setup" in pool.columns else None)
    if args.setups:
        want = {s.strip().upper() for s in args.setups.split(",") if s.strip()}
        setups = [s for s in setups if s in want] or sorted(want)
    if not setups:
        raise SystemExit(f"[train_test] no setups for family {prefix!r}")

    print(f"[train_test] family {prefix}* setups: {setups}")
    fam_pool = pool[pool["setup"].isin(setups)].copy()
    tr, te = split_train_test(fam_pool)
    print(f"[train_test] pool rows: train={len(tr)} test={len(te)} "
          f"(train {TRAIN[0]}..{TRAIN[1]}, test {TEST[0]}..{TEST[1]})")
    tr = attach_entries(tr)
    te = attach_entries(te)
    print(f"[train_test] with 1m entry: train={len(tr)} test={len(te)}")

    configs = {}
    print(f"\n[train_test] per-setup TRAIN search (target net PF {TRAIN_PF_MIN:.2f}..{TRAIN_PF_MAX:.2f}, max trades)")
    for s in setups:
        c = search_setup(s, tr)
        configs[s] = c
        if c.get("status") == "OK":
            terms = " AND ".join(f"{f}{op}{t}" for f, op, t in c["mask_terms"]) or "(none)"
            pm = " AND ".join(f"{f}{op}{t}" for f, op, t in c["premom_terms"]) or "(none)"
            print(f"  {s:<34} train_pf={c['train_pf']:.2f} n={c['train_n']:>3} "
                  f"net=Rs {c['train_net']:>8,.0f}  SL/Tgt={c['sl']}/{c['tgt']}  mask=[{terms}]  premom=[{pm}]")
        else:
            print(f"  {s:<34} {c['status']} (n_train={c.get('n_train','?')})")

    # ---- G3: multiple-testing correction across the family's candidate setups ----
    # The per-setup search peeks at a threshold grid AND we test several setups per
    # family, so some pass by chance. Score each OK setup's TEST trades with a
    # bootstrap p(mean net > 0), then apply Benjamini-Hochberg FDR across the family.
    # Setups that don't survive are dropped from the approved book (status DROP_FDR)
    # unless --no_fdr. This is the in-file analogue of walkforward_gate.run_gate's FDR.
    _ok_now = [s for s in setups if configs[s].get("status") == "OK"]
    if _ok_now:
        _rng = np.random.default_rng(7)
        setup_pvals = {}
        for s in _ok_now:
            m = eval_family({s: configs[s]}, te)
            net_s = m["net"][np.isfinite(m["net"])] if len(m.get("net", [])) else np.array([])
            setup_pvals[s] = (wfg._bootstrap_p_gt_zero(net_s, 5000, _rng) if len(net_s) else 1.0)
        sig = wfg._benjamini_hochberg(setup_pvals, float(args.fdr_alpha))
        for s, p in setup_pvals.items():
            configs[s]["test_p"] = round(float(p), 4)
            configs[s]["fdr_sig"] = bool(sig.get(s, False))
            if not sig.get(s, False) and not args.no_fdr:
                configs[s]["status"] = "DROP_FDR"
                configs[s]["dropped_reason"] = f"BH-FDR insignificant (test p={p:.3f}, alpha={args.fdr_alpha})"
        _kept = [s for s in setup_pvals if sig.get(s, False)]
        _drop = [s for s in setup_pvals if not sig.get(s, False)]
        print(f"\n[train_test] BH-FDR (alpha={args.fdr_alpha}) across {len(setup_pvals)} setups: "
              f"test p-values { {s: configs[s]['test_p'] for s in setup_pvals} }")
        print(f"[train_test]   FDR-significant{' (kept)' if not args.no_fdr else ''}: {sorted(_kept)}"
              f" | {'dropped' if not args.no_fdr else 'flagged'}: {sorted(_drop)}")

    train_fam = eval_family(configs, tr)
    test_fam = eval_family(configs, te)
    ok, verdict = accept_verdict(train_fam, test_fam)

    # Detailed per-setup + per-trade breakdown for both periods.
    exits = {s: (c["sl"], c["tgt"]) for s, c in configs.items() if c.get("status") == "OK"}
    train_detail = book_detail(train_fam["book"], exits) if train_fam["trades"] else pd.DataFrame()
    test_detail = book_detail(test_fam["book"], exits) if test_fam["trades"] else pd.DataFrame()
    train_sum = summarize_detail(train_detail, "TRAIN")
    test_sum = summarize_detail(test_detail, "TEST")

    PROPOSAL_DIR.mkdir(parents=True, exist_ok=True)
    if not train_detail.empty:
        train_detail.to_csv(PROPOSAL_DIR / f"family_{prefix}_train_trades.csv", index=False)
    if not test_detail.empty:
        test_detail.to_csv(PROPOSAL_DIR / f"family_{prefix}_test_trades.csv", index=False)
    combined_sum = pd.concat([train_sum, test_sum], ignore_index=True)
    if not combined_sum.empty:
        combined_sum.to_csv(PROPOSAL_DIR / f"family_{prefix}_setup_summary.csv", index=False)

    def _show(sumdf, label):
        print(f"\n[{label}] per-setup (net of cost)")
        if sumdf.empty:
            print("   (no trades)")
            return
        cols = ["setup", "side", "n", "win_pct", "tgt_pct", "sl_pct_rate", "eod_pct", "net_pf", "net_pnl_rs", "net_per_trade"]
        with pd.option_context("display.width", 200, "display.max_columns", None):
            print(sumdf[cols].to_string(index=False))

    print("\n" + "=" * 96)
    print(f"FAMILY {prefix}*  DETAILED TRAIN / TEST  (train {TRAIN[0]}..{TRAIN[1]}, test {TEST[0]}..{TEST[1]})")
    print("  chosen per-setup config:")
    for s in setups:
        c = configs[s]
        if c.get("status") != "OK":
            print(f"    {s:<34} {c.get('status')}")
            continue
        mterms = " AND ".join(f"{f}{op}{t}" for f, op, t in c["mask_terms"]) or "(none)"
        pterms = " AND ".join(f"{f}{op}{t}" for f, op, t in c["premom_terms"]) or "(none)"
        guard = c.get("guard") or {}
        print(f"    {s:<34} SL/Tgt={c['sl']}/{c['tgt']}  guard={guard}  "
              f"trainPF={c['train_pf']:.2f} minHalfPF={c.get('obj', float('nan')):.2f}")
        print(f"        mask=[{mterms}]  premom=[{pterms}]")
    _show(train_sum, "TRAIN")
    _show(test_sum, "TEST")

    # monthly net for the family book
    for lbl, det in [("TRAIN", train_detail), ("TEST", test_detail)]:
        if det.empty:
            continue
        m = det.copy()
        m["mon"] = pd.to_datetime(m["trade_date"]).dt.to_period("M").astype(str)
        by = m.groupby("mon")["net_pnl_rs"].agg(["size", "sum"])
        print(f"\n  {lbl} monthly net: " + ", ".join(f"{i}:n{int(r['size'])}/Rs{r['sum']:,.0f}" for i, r in by.iterrows()))

    # week-wise breakdown (setup-wise is the per-setup tables above)
    train_week = weekly_summary(train_detail, "TRAIN")
    test_week = weekly_summary(test_detail, "TEST")
    week_all = pd.concat([train_week, test_week], ignore_index=True)
    if not week_all.empty:
        week_all.to_csv(PROPOSAL_DIR / f"family_{prefix}_weekly_summary.csv", index=False)
    for lbl, wk in [("TRAIN", train_week), ("TEST", test_week)]:
        print(f"\n[{lbl}] week-wise (net of cost)")
        if wk.empty:
            print("   (no trades)")
            continue
        with pd.option_context("display.width", 200, "display.max_columns", None):
            print(wk[["week", "n", "win_pct", "net_pf", "net_pnl_rs"]].to_string(index=False))

    print("\n" + "-" * 96)
    print(f"FAMILY {prefix}* — combined (dedupe + mask, net of cost)")
    print(f"  TRAIN: n={train_fam['trades']} net_pf={train_fam['net_pf']:.3f} net=Rs {train_fam['net_pnl']:,.0f}")
    print(f"  TEST : n={test_fam['trades']} net_pf={test_fam['net_pf']:.3f} net=Rs {test_fam['net_pnl']:,.0f} "
          f"day_block_p={test_fam['day_block_p']}")
    if test_fam["trades"]:
        _s, _f, _n = _book_net_both_models(test_fam["book"], exits)
        print(f"  COST RECONCILIATION (TEST book, n={_n}): statutory=Rs {_s:,.0f}  "
              f"flat_bps({V6_COST_BPS:.0f})=Rs {_f:,.0f}  (active={COST_MODEL}; statutory-flat gap=Rs {(_s - _f):,.0f})")
    ratio = (test_fam['net_pf'] / train_fam['net_pf']) if train_fam['net_pf'] > 0 else 0.0
    print(f"  test/train PF ratio={ratio:.2f}")
    print(f"  VERDICT: {verdict}")
    print("=" * 96)
    proposal = {
        "family": prefix, "setups": setups, "accepted": bool(ok), "verdict": verdict,
        "train_window": TRAIN, "test_window": TEST,
        "configs": {s: {k: v for k, v in c.items() if k != "book"} for s, c in configs.items()},
        "train_family": {k: train_fam[k] for k in ("trades", "net_pf", "net_pnl")},
        "test_family": {k: (None if (isinstance(test_fam[k], float) and not np.isfinite(test_fam[k])) else test_fam[k])
                        for k in ("trades", "net_pf", "net_pnl", "day_block_p")},
    }
    ppath = PROPOSAL_DIR / f"proposal_family_{prefix}.json"
    ppath.write_text(json.dumps(_json_sanitize(proposal), indent=2, default=str), encoding="utf-8")
    print(f"\n[train_test] wrote proposal {ppath}")

    if args.approve:
        if not ok:
            print("[train_test] --approve ignored: family did NOT pass the accept gate.")
            return 1
        _write_final_conf(prefix, setups, configs, train_fam, test_fam)
        print(f"[train_test] APPROVED — merged {prefix}* into {FINAL_CONF_PATH}")
    else:
        print("[train_test] review the result; re-run with --approve to write final_setup_conf.py")
    return 0


def _serialize_setup_block(d: dict) -> str:
    """Render a setup config dict as a Python literal indented to sit at the 4-space
    key level inside FINAL_SETUP_CONF. JSON is valid Python for these str/num/list/
    bool/None/dict values; any tuples (the reference-only 'detection' conditions)
    become lists, which is harmless."""
    lines = json.dumps(d, indent=4, default=str).split("\n")
    return lines[0] + "".join("\n    " + ln for ln in lines[1:])


def _merged_setup_block(c: dict, existing_block: dict, prov_new: dict) -> dict:
    """New config for one approved setup, PRESERVING any existing non-tuned keys
    (side, detection docs, entry_model, …) and merging provenance, while overwriting
    the tuned exit/mask/premom/guards from this run."""
    merged = dict(existing_block) if isinstance(existing_block, dict) else {}
    merged["exit"] = {"sl_pct": c["sl"], "tgt_pct": c["tgt"]}
    merged["mask_terms"] = [list(t) for t in c["mask_terms"]]
    merged["pre_momentum_terms"] = [list(t) for t in c["premom_terms"]]
    merged["entry_guards"] = c["guard"] or {}
    prov = dict(merged.get("provenance", {})) if isinstance(merged.get("provenance"), dict) else {}
    prov.update(prov_new)
    merged["provenance"] = prov
    return merged


def _legacy_full_regenerate(ok_setups: dict, prov_new: dict) -> None:
    """Last-resort path used ONLY if the FINAL_SETUP_CONF literal can't be located
    for an in-place splice. Regenerates the file from the reloaded book (this loses
    RESEARCH_WATCH_CONF / the demotion transform — hence it is the fallback, never the
    default; a timestamped backup has already been written by the caller)."""
    import importlib
    import final_setup_conf as fc
    importlib.reload(fc)
    conf = dict(getattr(fc, "FINAL_SETUP_CONF", {}))
    for s, c in ok_setups.items():
        conf[s] = _merged_setup_block(c, conf.get(s, {}), prov_new)
    body = (
        '"""final_setup_conf.py — approved per-setup configs (auto-managed by '
        'setup_train_test.py --approve).\nSee module docstring history in git. Net-of-cost; train '
        f'{TRAIN[0]}..{TRAIN[1]}, test {TEST[0]}..{TEST[1]}."""\n\n'
        "from __future__ import annotations\n\n"
        'COST_BASIS = "net_of_nse_intraday_costs"\n'
        f"ACCEPT_GATE = {json.dumps({'train_min_net_pf': TRAIN_MIN_NET_PF, 'test_min_net_pf': TEST_MIN_NET_PF, 'test_max_day_block_p': TEST_MAX_DAY_BLOCK_P, 'min_test_train_pf_ratio': MIN_TEST_TRAIN_PF_RATIO}, indent=4)}\n\n"
        f"FINAL_SETUP_CONF = {json.dumps(conf, indent=4, default=str)}\n"
    )
    FINAL_CONF_PATH.write_text(body, encoding="utf-8")


def _write_final_conf(prefix, setups, configs, train_fam, test_fam) -> None:
    """Merge this family's APPROVED setups into the root final_setup_conf.py SAFELY:
      1. write a timestamped backup of the current file FIRST (never overwrite blind);
      2. splice only the approved setups' blocks IN PLACE inside the FINAL_SETUP_CONF
         literal (replace existing keys, insert new ones) so RESEARCH_WATCH_CONF, the
         _LIVE_DEMOTION transform, the module docstring/imports and every other setup's
         detection/provenance docs are preserved byte-for-byte;
      3. verify the edited file parses AND executes (the demotion transform runs at
         import) before committing — on ANY failure, restore the backup and abort.
    """
    today = pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y-%m-%d")
    ts = pd.Timestamp.now(tz="Asia/Kolkata").strftime("%Y%m%d_%H%M%S")
    ratio = (test_fam["net_pf"] / train_fam["net_pf"]) if train_fam["net_pf"] > 0 else 0.0
    prov_new = {
        "approved_on": today, "family": prefix,
        "train": {"trades": train_fam["trades"], "net_pf": round(train_fam["net_pf"], 3),
                  "net_pnl_rs": round(train_fam["net_pnl"], 2)},
        "test": {"trades": test_fam["trades"], "net_pf": round(test_fam["net_pf"], 3),
                 "net_pnl_rs": round(test_fam["net_pnl"], 2),
                 "day_block_p": (None if not np.isfinite(test_fam["day_block_p"]) else round(test_fam["day_block_p"], 4)),
                 "test_train_pf_ratio": round(ratio, 3)},
    }
    ok_setups = {s: c for s, c in configs.items() if c.get("status") == "OK"}
    if not ok_setups:
        print("[train_test] --approve: no OK setups to write; root conf unchanged.")
        return

    src = FINAL_CONF_PATH.read_text(encoding="utf-8")
    backup = FINAL_CONF_PATH.with_name(FINAL_CONF_PATH.name + f".bak_{ts}")
    backup.write_text(src, encoding="utf-8")

    def _restore_and_abort(msg: str):
        FINAL_CONF_PATH.write_text(src, encoding="utf-8")   # leave the original intact
        raise SystemExit(f"[train_test] --approve ABORTED ({msg}); root conf unchanged. Backup: {backup}")

    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        _restore_and_abort(f"existing conf does not parse: {e}")

    conf_node = next((n for n in tree.body
                      if isinstance(n, ast.Assign)
                      and any(isinstance(t, ast.Name) and t.id == "FINAL_SETUP_CONF" for t in n.targets)
                      and isinstance(n.value, ast.Dict)), None)
    if conf_node is None:
        print("[train_test] --approve: FINAL_SETUP_CONF dict literal not found; "
              "using full-regenerate fallback (backup written).")
        _legacy_full_regenerate(ok_setups, prov_new)
        print(f"[train_test] backup: {backup}")
        return

    # absolute char offset for each line start, to convert (lineno, col) -> index
    offs = [0]
    for line in src.splitlines(keepends=True):
        offs.append(offs[-1] + len(line))

    def _span(node):
        return (offs[node.lineno - 1] + node.col_offset,
                offs[node.end_lineno - 1] + node.end_col_offset)

    existing_spans = {k.value: _span(v)
                      for k, v in zip(conf_node.value.keys, conf_node.value.values)
                      if isinstance(k, ast.Constant) and isinstance(k.value, str)}
    insert_at = _span(conf_node.value)[0] + 1   # just after the opening '{'

    edits, replaced, inserted = [], [], []
    for s, c in ok_setups.items():
        if s in existing_spans:
            vs, ve = existing_spans[s]
            try:
                existing_block = ast.literal_eval(src[vs:ve])
            except Exception:
                existing_block = {}
            block = _serialize_setup_block(_merged_setup_block(c, existing_block, dict(prov_new)))
            edits.append((vs, ve, block))
            replaced.append(s)
        else:
            block = _serialize_setup_block(_merged_setup_block(c, {}, dict(prov_new)))
            edits.append((insert_at, insert_at, f'\n    "{s}": ' + block + ","))
            inserted.append(s)

    new_src = src
    for start, end, text in sorted(edits, key=lambda e: e[0], reverse=True):
        new_src = new_src[:start] + text + new_src[end:]

    # Verify before committing: must compile AND execute (runs the demotion transform).
    try:
        ns: dict = {}
        exec(compile(new_src, str(FINAL_CONF_PATH), "exec"), ns)
        book = ns.get("FINAL_SETUP_CONF")
        if not isinstance(book, dict) or not book:
            raise ValueError("FINAL_SETUP_CONF missing/empty after edit")
    except Exception as e:
        _restore_and_abort(f"edited conf failed verification: {e}")

    FINAL_CONF_PATH.write_text(new_src, encoding="utf-8")
    print(f"[train_test] backup: {backup}")
    if replaced:
        print(f"[train_test] updated in place : {sorted(replaced)}")
    if inserted:
        print(f"[train_test] inserted new     : {sorted(inserted)}")


if __name__ == "__main__":
    raise SystemExit(main())
