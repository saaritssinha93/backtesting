#!/usr/bin/env python3
"""
v11_ID_backtesting.py — V7 Live PARITY backtester (entry point).
================================================================

Goal: produce the SAME entry trades, at the SAME 5-minute signal timestamps, as
the live V7 ID 5-min strategy, then simulate exits on 1-minute data.

DESIGN DECISION (read v11_live_parity_notes.md §3):
    Parity is achieved by *invoking the live V7 modules directly* — NOT by
    re-implementing/copy-pasting their logic. A self-contained copy of ~4,000
    lines across 8 modules would inevitably DRIFT from the live source and
    silently break parity, which is the opposite of the requirement. The
    canonical in-house adaptation that already drives the live decision tree on
    historical bars is `avwap_5min_ID_v11_backtesting.py` (the "engine" below).
    This file is the clean, production-grade orchestration around it that:
      1. Sets the EXACT live env-var strategy config (copied verbatim from the
         V7 live .bat files) BEFORE importing any live module, so the imported
         V7 decision logic behaves identically to live.
      2. Drives the full V7 pipeline: 5-min scan -> live ranker -> v8 gate ->
         v11 overlay -> research filters -> entry-window -> dedupe -> 1-min entry
         engine (guards + pre-momentum) -> 1-min exit resolution.
      3. Adds a live-log PARITY CHECK comparing backtest entries vs the V7 live
         paper-trade logs for a date.

V7 LIVE DECISION FLOW (traced end-to-end; see notes for file/function map):
    dashboard/bat -> eqidv2_signal_discovery_v7_5min_id_persistent.py
      (avwap_5min_ID_v7_candidate_scan -> avwap_5min_ID_v2_backtesting._scan_day
       -> add_live_ranker_scores -> apply_v8_live_gate
       -> eqidv2_v11_live_overlay.apply_live_candidate_overlay
       -> apply_research_live_filters -> _filter_entry_window
       -> one-ticker-per-day dedupe)
    -> eqidv2_entry_engine_1min_v5_id.py (next-1m-open after 5-min signal,
       entry guards, pre-entry momentum gates, Rs 20k x 5x sizing)
    -> avwap_trade_execution_PAPER_TRADE_TRUE_id_5min_v7.py (executor; exits)

Data:
    5-minute (entry signals): C:\\TradingData\\eqidv2\\stocks_indicators_5min_eq_live2
    1-minute (exit sim):      C:\\TradingData\\eqidv2\\stocks_indicators_1min_eq
    NOTE: live signal discovery reads stocks_indicators_5min_eq_live (no "2").
          The "_eq_live2" store is the backtest copy. This is a KNOWN parity risk
          (notes §5) — run v7_causality_audit.py to confirm store equality.

Outputs (in --out):
    v11_ID_trades.csv, v11_ID_summary.csv, v11_ID_setup_summary.csv,
    v11_ID_daily_summary.csv, v11_ID_parity_debug.csv (with --parity-debug),
    plus v11_ID_parity_vs_live_<date>.csv (mode parity_check).

Run (Python 3.12 — the live modules use 3.10+ syntax):
    py -3.12 v11_ID_backtesting.py --mode backtest --date 2026-06-10 --parity-debug
    py -3.12 v11_ID_backtesting.py --mode backtest --start_date 2026-01-01 --end_date 2026-06-10
    py -3.12 v11_ID_backtesting.py --mode parity_check --date 2026-06-10 --out <backtest_out_dir>
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# 0. Python version guard (live modules require 3.10+ union syntax).
# ---------------------------------------------------------------------------
if sys.version_info < (3, 10):
    sys.stderr.write(
        f"[v11_ID_backtesting] FATAL: needs Python >= 3.10 (the live V7 modules use "
        f"`str | None` syntax); current = {sys.version.split()[0]}. Run with: "
        f"py -3.12 v11_ID_backtesting.py ...\n"
    )
    raise SystemExit(2)

# UTF-8 safety: the live .bat files set PYTHONIOENCODING=utf-8; the engine prints
# unicode (e.g. "->" arrows) that crash the Windows cp1252 console otherwise.
# Set for any spawned scan workers, and reconfigure the current streams in-process.
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
os.environ.setdefault("PYTHONUTF8", "1")
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass

# ---------------------------------------------------------------------------
# 1. V7 LIVE STRATEGY CONFIG — copied VERBATIM from the live .bat files so the
#    imported V7 decision logic matches live exactly. Source files:
#      bat/run_eqidv2_signal_discovery_v7_5min_id_persistent.bat
#      bat/run_eqidv2_entry_engine_1min_v5_id.bat
#    Only DECISION-relevant vars are set; live-only operational vars (scan
#    workers, feed-gate timing, post-slot delay, restart/logging, live data
#    dirs) are intentionally omitted — they do not affect which trades fire on
#    historical bars (see notes §3). Set with setdefault() so an explicit
#    caller-provided env var still wins.
#    MUST run before importing any live module (constants read at import time).
# ---------------------------------------------------------------------------
V7_LIVE_STRATEGY_ENV: dict[str, str] = {
    # --- signal discovery: window / timing / selection ---
    "EQIDV2_SIGNAL_DISCOVERY_V7_ENTRY_WINDOW_START": "09:30",
    "EQIDV2_SIGNAL_DISCOVERY_V7_ENTRY_WINDOW_END": "14:30",
    "EQIDV2_SIGNAL_DISCOVERY_V7_ENTRY_LAG_MIN": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_SELECTION_MODE": "v8_setup_compatible",
    # --- v8 gate + accepted rules ---
    "EQIDV2_SIGNAL_DISCOVERY_V7_V8_GATE": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_V8_ACCEPTED_RULES":
        r"C:\TradingData\eqidv2\outputs_ID_v8_5min_research_restore\accepted_rules.csv",
    # --- v11 tier123 overlay + research filters ---
    "EQIDV2_SIGNAL_DISCOVERY_V7_V11_TIER123": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_RESEARCH_FILTERS": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_RESEARCH_FILTER_MODE": "active",
    # --- anti-chase guards ---
    "EQIDV2_SIGNAL_DISCOVERY_V7_LONG_ANTI_CHASE_CLOSE_LOC_GT": "0.97",
    "EQIDV2_SIGNAL_DISCOVERY_V7_LONG_ANTI_CHASE_VWAP_DIST_ATR_GT": "3.50",
    "EQIDV2_SIGNAL_DISCOVERY_V7_ANTI_CHASE_LONG_CLOSE_LOC_MIN": "0.97",
    "EQIDV2_SIGNAL_DISCOVERY_V7_ANTI_CHASE_LONG_VWAP_DIST_ATR_MIN": "3.50",
    "EQIDV2_SIGNAL_DISCOVERY_V7_B_AVWAP_RECLAIM_RANKER_MIN": "0.65",
    "EQIDV2_SIGNAL_DISCOVERY_V7_L_TREND_PULLBACK_PROBATION_BLOCK": "1",
    # --- side focus (both LONG+SHORT live) ---
    "EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS": "0",
    "EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS_ALLOWED_SIDES": "SHORT,LONG",
    "EQIDV2_SIGNAL_DISCOVERY_V7_SHORT_FOCUS_EXEMPT_SETUPS": "A_MOD_BREAK_C1_HIGH,C_OR_BREAKOUT",
    # --- EARLY mode + sub-thresholds ---
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MODE": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MIN_5M_TRADED_VALUE_RS": "1000000",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_MAX_VWAP_DIST_ATR": "2.80",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_TIGHT_FILTERS": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_BLOCKED_SETUPS":
        "E_RS_FIRST_HOUR_BREAK_LONG,E_RS_FIRST_HOUR_BREAK_SHORT,E_VWAP_RECLAIM_EARLY_LONG,"
        "E_FAILED_OR_BREAKOUT_TRAP_SHORT,E_ORB_RETEST_HOLD_SHORT,E_ORB_RETEST_HOLD_LONG,"
        "E_FAILED_OR_BREAKDOWN_TRAP_LONG,E_GAP_HOLD_CONTINUATION_LONG,E_GAP_HOLD_CONTINUATION_SHORT,"
        "E_OPENING_DRIVE_CONTINUATION_LONG,E_OPENING_DRIVE_CONTINUATION_SHORT",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MAX_VOL_RATIO": "2.00",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MIN_RS_PCT": "4.00",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_LONG_MAX_VWAP_DIST_ATR": "1.80",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_GAP_LONG_MIN_RS_PCT": "3.00",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_GAP_LONG_MIN_QUALITY": "160.00",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MIN_RS_PCT": "-1.50",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MAX_ATR_PCT": "0.0065",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_ORB_SHORT_MIN_BODY_PCT": "0.82",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MIN_RS_PCT": "-1.20",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MIN_CLOSE_LOC": "0.08",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_VWAP_SHORT_MAX_ATR_PCT": "0.008",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MIN_SCORE": "95",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MAX_PER_SIDE": "4",
    "EQIDV2_SIGNAL_DISCOVERY_V7_EARLY_LIVE_GATE_MAX_PER_SLOT": "8",
    # --- uncovered fallback ---
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_START_TIME": "11:05",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_END_TIME": "13:55",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_MIN_RANKER": "0.65",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_MIN_QUALITY": "125",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_MAX_PER_SLOT": "1",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_ALLOWED_SIDES": "SHORT,LONG",
    "EQIDV2_SIGNAL_DISCOVERY_V7_UNCOVERED_FALLBACK_ALLOWED_SETUPS":
        "A_MOD_BREAK_C1_LOW,C_OR_BREAKDOWN,A_PULLBACK_C2_THEN_BREAK_C2_LOW,B_HUGE_RED_FAILED_BOUNCE,"
        "D_AVWAP_LOSE_REVERSAL,G_LOWER_LOW_BREAK,C_OR_BREAKOUT",
    # --- 1-min entry engine: lag / max-delay / pre-momentum ---
    "EQIDV2_ENTRY_ENGINE_1MIN_V5_ID_ENTRY_LAG_MIN": "1",
    "EQIDV2_ENTRY_ENGINE_1MIN_V7_MAX_DELAY_MIN": "3",
    "EQIDV2_ENTRY_ENGINE_PRE_MOMENTUM_GATES": "1",
    "EQIDV2_ENTRY_ENGINE_PRE_MOMENTUM_MISSING_ACTION": "block",
}


def _apply_v7_live_env(log: logging.Logger | None = None) -> None:
    applied = []
    for key, val in V7_LIVE_STRATEGY_ENV.items():
        if key not in os.environ:
            os.environ[key] = val
            applied.append(key)
    if log:
        log.info("applied %d V7-live strategy env vars (%d already set by caller)",
                 len(applied), len(V7_LIVE_STRATEGY_ENV) - len(applied))


# Apply BEFORE importing the live engine/modules (constants are read at import).
_apply_v7_live_env()

# ---------------------------------------------------------------------------
# 2. Import the canonical V7-live parity engine + libs. Any import failure here
#    is fatal and reported clearly (no silent failure).
# ---------------------------------------------------------------------------
try:
    import numpy as np
    import pandas as pd
    import avwap_5min_ID_v11_backtesting as engine  # canonical V7-live replay engine
except Exception as exc:  # pragma: no cover - import-time environment problem
    sys.stderr.write(
        "[v11_ID_backtesting] FATAL: could not import the V7-live engine / deps: "
        f"{type(exc).__name__}: {exc}\n"
        "Ensure you are in the project dir and running with py -3.12 "
        "(the live modules need 3.10+).\n"
    )
    raise

# ---------------------------------------------------------------------------
# Default paths (all overridable via CLI).
# ---------------------------------------------------------------------------
DEFAULT_5M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DEFAULT_1M_DIR = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
DEFAULT_OUT = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_5min")
DEFAULT_LIVE_PAPER_DIR = Path(r"C:\TradingData\eqidv2\live_signals")
DEFAULT_LIVE_CANDIDATE_JSON_DIR = Path(r"C:\TradingData\eqidv2\signal_discovery_v7_5mins_ID\json")

# V7-live parity profile.
# CORRECTED 2026-06-11 after the parity_check falsified the earlier "use none"
# assumption: the live v11 OVERLAY admits its setup-universe PER THE PRODUCTION
# PROFILE (e.g. E_VWAP_LOSE_EARLY_SHORT is overlay-only and live trades it). With
# profile=none the overlay admits nothing, so the backtest misses the setups live
# actually fires. Evidence (2026-06-10): profile=none matched 0/2 live entries;
# the production profile matched 2/2. So parity MUST use the live production
# profile (the engine default). ab_gate=quality_top_slot admits A/B probation
# setups live trades.
PARITY_PROFILE = engine.SELECTED_STRATEGY_DEFAULT_PROFILE  # production_..._tier123_balanced
PARITY_AB_GATE = "quality_top_slot"

log = logging.getLogger("v11_ID_backtesting")


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s [v11_ID] %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------------------------------------------------------
# 3. Backtest runner — build the engine args Namespace and dispatch to the
#    canonical V7-live pipeline. Data dirs point at the backtest stores; the
#    strategy env (set above) makes the imported live logic behave like live.
# ---------------------------------------------------------------------------
def _engine_args(**overrides) -> argparse.Namespace:
    base = dict(
        mode="historical_full_day",
        out=str(DEFAULT_OUT),
        cached_all_setups_dir=str(DEFAULT_OUT),
        live_candidate_json_dir=str(DEFAULT_LIVE_CANDIDATE_JSON_DIR),
        live_paper_dir=str(DEFAULT_LIVE_PAPER_DIR),
        candidate_5m_dir=str(DEFAULT_5M_DIR),
        fallback_candidate_5m_dir=str(DEFAULT_5M_DIR),
        live_date="",
        historical_date="",
        start_date="",
        end_date="",
        start_time="09:15",
        end_time="15:00",
        workers=8,                      # capped at 8 (shared machine; protects live feed)
        cost_bps=float(engine.v6.DEFAULT_COST_BPS),
        entry_fill_model="ltp_on_signal_1m_open",
        selected_strategy_profile=PARITY_PROFILE,
        ab_gate_profile=PARITY_AB_GATE,
        ab_gate_min_quality=250.0,
        ab_gate_max_per_side=1,
        ab_gate_max_per_slot=2,
        parity_debug=False,
    )
    base.update(overrides)
    ns = argparse.Namespace(**base)
    # mirror engine.main() normalisation
    ns.selected_strategy_profile = engine._normalise_selected_strategy_profile(ns.selected_strategy_profile)
    ns.ab_gate_profile = engine._normalise_ab_gate_profile(ns.ab_gate_profile)
    return ns


def run_backtest(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    data_5m = Path(args.data_5m_dir)
    if not data_5m.exists():
        raise SystemExit(f"[v11_ID_backtesting] 5-min data dir not found: {data_5m}")
    if not Path(args.data_1m_dir).exists():
        raise SystemExit(f"[v11_ID_backtesting] 1-min data dir not found: {args.data_1m_dir}")
    # Engine reads 1-min from engine.v6.DATA_1M_DIR; redirect if the caller overrode it.
    if str(Path(args.data_1m_dir)) != str(engine.v6.DATA_1M_DIR):
        log.info("redirecting engine 1-min dir -> %s", args.data_1m_dir)
        engine.v6.DATA_1M_DIR = Path(args.data_1m_dir)

    common = dict(
        out=str(out_dir),
        candidate_5m_dir=str(data_5m),
        fallback_candidate_5m_dir=str(data_5m),
        live_paper_dir=str(args.live_paper_dir),
        live_candidate_json_dir=str(args.live_candidate_json_dir),
        workers=int(args.workers),
        selected_strategy_profile=args.selected_strategy_profile,
        ab_gate_profile=args.ab_gate_profile,
        parity_debug=bool(args.parity_debug),
    )

    if args.date:
        log.info("BACKTEST single day %s -> %s (profile=%s ab_gate=%s)",
                 args.date, out_dir, args.selected_strategy_profile, args.ab_gate_profile)
        ns = _engine_args(mode="historical_full_day", historical_date=args.date, live_date=args.date, **common)
        rc = engine._run_historical_full_day(ns)
    elif args.start_date or args.end_date:
        log.info("BACKTEST range %s..%s -> %s", args.start_date, args.end_date, out_dir)
        ns = _engine_args(mode="historical_all_available",
                          start_date=args.start_date, end_date=args.end_date, **common)
        rc = engine._run_historical_all_available(ns)
    else:
        raise SystemExit("[v11_ID_backtesting] backtest needs --date OR --start_date/--end_date")

    log.info("backtest finished (rc=%s). Outputs in %s", rc, out_dir)
    _assert_outputs(out_dir)
    return rc


def _assert_outputs(out_dir: Path) -> None:
    expected = ["v11_ID_trades.csv", "v11_ID_summary.csv", "v11_ID_setup_summary.csv",
                "v11_ID_daily_summary.csv"]
    missing = [f for f in expected if not (out_dir / f).exists()]
    if missing:
        log.warning("expected output(s) missing (no trades, or empty run?): %s", missing)
    else:
        log.info("verified outputs: %s", ", ".join(expected))


# ---------------------------------------------------------------------------
# 4. PARITY CHECK — compare backtest entries vs V7 live paper-trade logs.
# ---------------------------------------------------------------------------
def _norm_5m_slot(ts_series: "pd.Series") -> "pd.Series":
    ts = pd.to_datetime(ts_series, errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        ts = ts.dt.tz_localize("Asia/Kolkata")
    else:
        ts = ts.dt.tz_convert("Asia/Kolkata")
    return ts.dt.floor("5min")


def _load_backtest_entries(out_dir: Path) -> "pd.DataFrame":
    """Backtest book with 5-min signal slot. Prefer the rich internal trades.csv
    (has signal_time_v8); fall back to v11_ID_trades.csv (entry_time only)."""
    rich = out_dir / "trades.csv"
    if rich.exists():
        df = pd.read_csv(rich)
        sig_col = next((c for c in ("signal_time_v8", "signal_time_ist", "bar_time_ist", "signal_datetime")
                        if c in df.columns), None)
        if sig_col is None:
            raise SystemExit(f"[parity_check] {rich} has no signal-time column")
        out = pd.DataFrame({
            "symbol": df["ticker"].astype(str).str.upper().str.strip(),
            "side": df["side"].astype(str).str.upper().str.strip(),
            "setup": df["setup"].astype(str).str.strip(),
            "slot_5m": _norm_5m_slot(df[sig_col]),
            "entry_price": pd.to_numeric(df.get("entry_price_v6", df.get("entry_price", np.nan)), errors="coerce"),
        })
        return out.dropna(subset=["slot_5m"]).reset_index(drop=True)
    canon = out_dir / "v11_ID_trades.csv"
    if not canon.exists():
        raise SystemExit(f"[parity_check] no backtest book found in {out_dir} (run --mode backtest first)")
    df = pd.read_csv(canon)
    # canonical file has entry_time (1-min); approximate the 5-min signal slot
    out = pd.DataFrame({
        "symbol": df["symbol"].astype(str).str.upper().str.strip(),
        "side": df["side"].astype(str).str.upper().str.strip(),
        "setup": df["setup_name"].astype(str).str.strip(),
        "slot_5m": _norm_5m_slot(df["entry_time"]),   # approximate (entry ~ signal+1min)
        "entry_price": pd.to_numeric(df.get("entry_price", np.nan), errors="coerce"),
    })
    return out.dropna(subset=["slot_5m"]).reset_index(drop=True)


_EMPTY_ENTRIES = ["symbol", "side", "setup", "slot_5m", "entry_price"]


def _load_live_entries(live_paper_dir: Path, date: str) -> "pd.DataFrame":
    """Read the V7 live paper-trade log for a date directly (robust to the known
    ragged-row paper-writer bug: skip bad lines with a warning, never crash)."""
    path = Path(live_paper_dir) / f"paper_trades_{date}_id_5min_v7.csv"
    if not path.exists():
        log.warning("no live paper-trade file for %s: %s", date, path)
        return pd.DataFrame(columns=_EMPTY_ENTRIES)
    try:
        live = pd.read_csv(path, engine="python", on_bad_lines="warn")
    except Exception as exc:
        log.error("could not read live paper file %s: %s", path, exc)
        return pd.DataFrame(columns=_EMPTY_ENTRIES)
    if live.empty or "ticker" not in live.columns:
        log.warning("live paper file %s empty / missing 'ticker'", path)
        return pd.DataFrame(columns=_EMPTY_ENTRIES)
    sig = live.get("signal_datetime")
    if sig is None:
        sig = live.get("signal_entry_datetime_ist", live.get("entry_time"))
    out = pd.DataFrame({
        "symbol": live["ticker"].astype(str).str.upper().str.strip(),
        "side": live.get("side", "").astype(str).str.upper().str.strip(),
        "setup": live.get("setup", "").astype(str).str.strip(),
        "slot_5m": _norm_5m_slot(sig),
        "entry_price": pd.to_numeric(live.get("entry_price", np.nan), errors="coerce"),
    })
    out = out[out["side"].isin(["LONG", "SHORT"])]
    return out.dropna(subset=["slot_5m"]).reset_index(drop=True)


def parity_check(out_dir: Path, date: str, live_paper_dir: Path) -> int:
    bt = _load_backtest_entries(out_dir)
    bt = bt[bt["slot_5m"].dt.strftime("%Y-%m-%d") == date].copy()
    lv = _load_live_entries(live_paper_dir, date)
    log.info("parity_check %s: backtest entries=%d, live entries=%d", date, len(bt), len(lv))

    def key(df):
        return (df["symbol"] + "|" + df["side"] + "|" + df["setup"] + "|"
                + df["slot_5m"].dt.strftime("%H:%M"))

    bt["_k"] = key(bt) if not bt.empty else pd.Series(dtype=str)
    lv["_k"] = key(lv) if not lv.empty else pd.Series(dtype=str)
    bt_k, lv_k = set(bt["_k"]), set(lv["_k"])

    rows = []
    for k in sorted(bt_k & lv_k):
        rows.append({"status": "BOTH", "key": k})
    for k in sorted(lv_k - bt_k):
        rows.append({"status": "LIVE_ONLY_missing_in_v11", "key": k})
    for k in sorted(bt_k - lv_k):
        rows.append({"status": "V11_ONLY_absent_in_live", "key": k})

    # secondary diagnostics on (symbol, side, setup) ignoring slot -> timestamp mismatch
    def trip(df):
        return set(df["symbol"] + "|" + df["side"] + "|" + df["setup"]) if not df.empty else set()
    bt_trip, lv_trip = trip(bt), trip(lv)
    for t in sorted((bt_trip & lv_trip)):
        bslots = set(bt.loc[bt["symbol"] + "|" + bt["side"] + "|" + bt["setup"] == t, "slot_5m"].dt.strftime("%H:%M"))
        lslots = set(lv.loc[lv["symbol"] + "|" + lv["side"] + "|" + lv["setup"] == t, "slot_5m"].dt.strftime("%H:%M"))
        if bslots != lslots:
            rows.append({"status": "TIMESTAMP_MISMATCH", "key": t,
                         "v11_slots": ",".join(sorted(bslots)), "live_slots": ",".join(sorted(lslots))})
    # symbol/setup-level mismatches (present one side only, ignoring slot)
    for sym in sorted({k.split("|")[0] for k in (lv_trip - bt_trip)}):
        rows.append({"status": "SYMBOL/SETUP_LIVE_ONLY", "key": sym})
    for sym in sorted({k.split("|")[0] for k in (bt_trip - lv_trip)}):
        rows.append({"status": "SYMBOL/SETUP_V11_ONLY", "key": sym})

    report = pd.DataFrame(rows)
    out_path = out_dir / f"v11_ID_parity_vs_live_{date}.csv"
    report.to_csv(out_path, index=False)

    n_both = sum(r["status"] == "BOTH" for r in rows)
    n_live_only = sum(r["status"] == "LIVE_ONLY_missing_in_v11" for r in rows)
    n_v11_only = sum(r["status"] == "V11_ONLY_absent_in_live" for r in rows)
    n_ts = sum(r["status"] == "TIMESTAMP_MISMATCH" for r in rows)
    print("=" * 84)
    print(f"V7-LIVE vs V11 BACKTEST PARITY — {date}")
    print(f"  backtest entries : {len(bt)}")
    print(f"  live entries     : {len(lv)}")
    print(f"  matched (BOTH)   : {n_both}")
    print(f"  LIVE only (missing in v11) : {n_live_only}")
    print(f"  V11 only (absent in live)  : {n_v11_only}")
    print(f"  timestamp mismatches       : {n_ts}")
    match_rate = (n_both / max(1, len(lv_k))) * 100.0
    print(f"  live->v11 match rate       : {match_rate:.1f}%")
    print(f"  hint: inspect v11_ID_parity_debug.csv (run backtest with --parity-debug) for per-candidate reasons")
    print(f"  wrote {out_path}")
    print("=" * 84)
    return 0


# ---------------------------------------------------------------------------
# 5. CLI
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(
        description="V7 Live parity backtester (v11_ID). Reproduces live entries; simulates 1-min exits.")
    ap.add_argument("--mode", choices=["backtest", "parity_check"], default="backtest")
    ap.add_argument("--date", type=str, default="", help="single trading day YYYY-MM-DD (backtest or parity_check)")
    ap.add_argument("--start_date", type=str, default="", help="range start (backtest)")
    ap.add_argument("--end_date", type=str, default="", help="range end (backtest)")
    ap.add_argument("--out", type=str, default=str(DEFAULT_OUT))
    ap.add_argument("--data_5m_dir", type=str, default=str(DEFAULT_5M_DIR))
    ap.add_argument("--data_1m_dir", type=str, default=str(DEFAULT_1M_DIR))
    ap.add_argument("--live_paper_dir", type=str, default=str(DEFAULT_LIVE_PAPER_DIR))
    ap.add_argument("--live_candidate_json_dir", type=str, default=str(DEFAULT_LIVE_CANDIDATE_JSON_DIR))
    ap.add_argument("--workers", type=int, default=8, help="scan workers (<=8 on the shared live machine)")
    ap.add_argument("--selected_strategy_profile", type=str, default=PARITY_PROFILE,
                    help="V7 parity uses 'none' (the profiles are v11 research layers, not live)")
    ap.add_argument("--ab_gate_profile", type=str, default=PARITY_AB_GATE)
    ap.add_argument("--parity-debug", action="store_true", dest="parity_debug",
                    help="write v11_ID_parity_debug.csv: per-candidate audit across all pipeline stages")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    _setup_logging(args.verbose)
    _apply_v7_live_env(log)
    log.info("mode=%s out=%s 5m=%s 1m=%s", args.mode, args.out, args.data_5m_dir, args.data_1m_dir)

    out_dir = Path(args.out)
    if args.mode == "parity_check":
        if not args.date:
            raise SystemExit("[v11_ID_backtesting] parity_check needs --date YYYY-MM-DD")
        if not out_dir.exists():
            raise SystemExit(f"[v11_ID_backtesting] --out {out_dir} not found; run --mode backtest first")
        return parity_check(out_dir, args.date, Path(args.live_paper_dir))

    return run_backtest(args)


if __name__ == "__main__":
    raise SystemExit(main())
