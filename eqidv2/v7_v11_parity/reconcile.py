#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
reconcile.py  —  V7-live (paper) vs V11-backtest (live_parity) parity reconciliation.

STANDALONE. No import of any live/production module (statutory costs are embedded and
byte-validated against nse_intraday_costs.py::intraday_equity_costs). Read-only: it never
writes into the live_signals or backtesting_result_v11 trees — only into --out.

WHAT IT DOES
  * Discovers the last N NSE trading sessions from the live paper-execution logs
    (present every session, even zero-trade days), or takes explicit --dates.
  * Loads the LIVE authoritative ledger  paper_trades_<date>_id_5min_v7.csv
    and the LIVE signal-intent files      signals_<date>_id_5min_v7_{long,short}.csv.
  * Loads the V11 backtest trades.csv (fallback v11_ID_trades.csv) and the backtest
    signal file live_parity_selected_strategy_signals.csv from backtesting_result_v11/<date>/
    (override any date -> dir with --bt-override DATE=PATH).
  * Applies the SAME statutory NSE intraday cost model to BOTH sides so net P&L is
    compared on an identical cost basis (backtest trades.csv is price-only, v6_cost_rs=0).
    Live's own recorded net (real fills + real slippage) is retained separately so the
    live-vs-model slippage gap is quantified, not hidden.
  * Matches trades on (date, ticker, side, setup, 5-min signal bar) within a 1-bar
    tolerance; buckets MATCHED / LIVE_ONLY / BACKTEST_ONLY (+ LIVE_STALE_SKIP for the
    live-only latency artifacts). Nothing is silently dropped.
  * Root-cause-tags every unmatched row (config-as-of drift, RAW_PRE_GATE readmit bug,
    live stale-skip, live-zero-trade day, generic live-suppression / bt-only).
  * Emits per-day and per-setup aggregate tables, signal/trade match rates, daily P&L
    correlation, total net divergence, a PASS/FAIL verdict vs tolerances, CSVs, a JSON
    summary, and a machine-generated markdown section.

USAGE
  python v7_v11_parity/reconcile.py --last-n 7
  python v7_v11_parity/reconcile.py --dates 2026-06-29,2026-06-30,...,2026-07-07 \
      --bt-override 2026-07-07=C:\\TradingData\\eqidv2\\backtesting_result_v11\\2026-07-07_conf_parity
  python v7_v11_parity/reconcile.py --last-n 7 --slippage-bps 5   # also model bt exit slippage

Exit code 0 = PASS (within tolerances), 1 = FAIL (divergence beyond tolerance), 2 = error.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta

# --------------------------------------------------------------------------------------
# Paths (overridable via CLI)
# --------------------------------------------------------------------------------------
DEFAULT_LIVE_DIR = r"C:\TradingData\eqidv2\live_signals"
DEFAULT_BT_ROOT = r"C:\TradingData\eqidv2\backtesting_result_v11"
LIVE_SUFFIX = "id_5min_v7"

# --------------------------------------------------------------------------------------
# Statutory NSE intraday cost model — EMBEDDED copy of nse_intraday_costs.CostConfig /
# intraday_equity_costs (rates_as_of 2026-06). Validated: KERNEX SHORT qty17
# entry 2018.09 exit 2033.23 -> total_cost 36.57, 5.31 bps of turnover (matches live).
# Keep in sync with nse_intraday_costs.py if the statutory rates ever change.
# --------------------------------------------------------------------------------------
COST = dict(
    rates_as_of="2026-06",
    brokerage_pct=0.0003, brokerage_cap_rs=20.0,
    stt_sell_pct=0.00025, exch_txn_pct=0.0000297,
    sebi_pct=0.000001, ipft_pct=0.000001,
    stamp_buy_pct=0.00003, gst_pct=0.18,
)


def statutory_costs(entry_price, exit_price, qty, side, slippage_bps=0.0):
    """Return (gross_pnl, total_cost, net_pnl) for one intraday trade.

    slippage_bps > 0 applies an adverse per-leg half-spread to entry+exit (used only for
    the optional apples-to-apples exit-slippage normalization of the backtest side).
    """
    side = str(side).upper()
    ep, xp, q = float(entry_price), float(exit_price), float(qty)
    if q <= 0 or ep <= 0 or xp <= 0:
        return (float("nan"), float("nan"), float("nan"))
    if slippage_bps:
        s = slippage_bps / 1e4
        if side == "LONG":          # buy higher, sell lower
            ep, xp = ep * (1 + s), xp * (1 - s)
        else:                        # SHORT: sell lower, buy higher
            ep, xp = ep * (1 - s), xp * (1 + s)
    entry_val, exit_val = ep * q, xp * q
    if side == "LONG":
        buy_value, sell_value = entry_val, exit_val
        gross = (xp - ep) * q
    else:
        buy_value, sell_value = exit_val, entry_val
        gross = (ep - xp) * q
    turnover = buy_value + sell_value
    brokerage = min(COST["brokerage_cap_rs"], COST["brokerage_pct"] * buy_value) + \
        min(COST["brokerage_cap_rs"], COST["brokerage_pct"] * sell_value)
    stt = COST["stt_sell_pct"] * sell_value
    exch = COST["exch_txn_pct"] * turnover
    sebi = COST["sebi_pct"] * turnover
    ipft = COST["ipft_pct"] * turnover
    stamp = COST["stamp_buy_pct"] * buy_value
    gst = COST["gst_pct"] * (brokerage + exch + sebi + ipft)
    total_cost = brokerage + stt + exch + sebi + ipft + stamp + gst
    return (gross, total_cost, gross - total_cost)


# --------------------------------------------------------------------------------------
# Config-as-of: date a setup first became LIVE-ENABLED (approved_on / user-directed
# promotion in final_setup_conf.py). A backtest trade for a setup whose enable date is
# AFTER the trade date is a config-as-of artifact, NOT a true parity break, because live
# could not have taken it that day. Setups not listed were enabled for the whole window.
# --------------------------------------------------------------------------------------
SETUP_ENABLE_DATE = {
    "S9_MIDDAY_LOSE": "2026-06-30",
    "E_ORB_BREAKOUT_LONG": "2026-06-30",
    "DOC5D_AVWAP_RECLAIM_LONG": "2026-07-01",
}

# Setups readmitted from the RAW_PRE_GATE pool that the live scanner v8-drops before
# readmission (known bug: detected in v11 backtest, never emitted live).
RAW_PRE_GATE_READMIT_SETUPS = {"L_DOUBLE_BOTTOM_VWAP", "L_PRESSURE_BURST_VWAP"}

# Outcome vocabulary normalization (backtest uses EOD/TIME; live uses EOD_CLOSE/…).
OUTCOME_MAP = {
    "SL": "SL", "STOP": "SL", "STOP_LOSS": "SL",
    "TARGET": "TARGET", "TP": "TARGET",
    "EOD": "EOD_CLOSE", "EOD_CLOSE": "EOD_CLOSE", "FORCED_CLOSE": "EOD_CLOSE",
    "TIME": "TIME_STOP", "TIME_STOP": "TIME_STOP", "TIMESTOP": "TIME_STOP",
    "ENTRY_SKIPPED_STALE_SIGNAL": "STALE_SKIP",
}

TOL_BAR_MIN = 5           # 1 five-minute bar
TOL_PRICE_BPS = 20.0      # entry/exit price divergence flag threshold (bps of price)
TOL_NET_BPS_OF_NOTIONAL = 10.0   # per-trade net P&L tolerance (bps of entry notional)


# --------------------------------------------------------------------------------------
# Parsing helpers
# --------------------------------------------------------------------------------------
def parse_ist(s):
    """Parse an IST timestamp regardless of '+05:30' vs '+0530' vs naive. Returns a
    tz-naive datetime of the wall-clock IST time (all inputs are already IST)."""
    if s is None:
        return None
    s = str(s).strip().strip('"')
    if not s or s.lower() in ("nan", "nat", "none", ""):
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}):(\d{2}):(\d{2})", s)
    if not m:
        # date only?
        m2 = re.match(r"(\d{4}-\d{2}-\d{2})", s)
        if m2:
            return datetime.strptime(m2.group(1), "%Y-%m-%d")
        return None
    d, hh, mm, ss = m.group(1), m.group(2), m.group(3), m.group(4)
    return datetime.strptime(f"{d} {hh}:{mm}:{ss}", "%Y-%m-%d %H:%M:%S")


def floor_5min(dt):
    if dt is None:
        return None
    return dt - timedelta(minutes=dt.minute % 5, seconds=dt.second, microseconds=dt.microsecond)


def to_float(x, default=float("nan")):
    try:
        if x is None or str(x).strip() in ("", "nan", "NaN", "None"):
            return default
        return float(x)
    except (TypeError, ValueError):
        return default


def norm_setup(s):
    return str(s or "").strip().upper()


def norm_side(s):
    s = str(s or "").strip().upper()
    if s in ("LONG", "BUY", "L"):
        return "LONG"
    if s in ("SHORT", "SELL", "S"):
        return "SHORT"
    return s


def norm_outcome(s):
    return OUTCOME_MAP.get(str(s or "").strip().upper(), str(s or "").strip().upper() or "UNKNOWN")


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def pick(row, *names, default=""):
    for n in names:
        if n in row and str(row[n]).strip() not in ("", "nan", "None"):
            return row[n]
    return default


# --------------------------------------------------------------------------------------
# Loaders
# --------------------------------------------------------------------------------------
def live_path(live_dir, family, date, side=None):
    if side:
        return os.path.join(live_dir, f"{family}_{date}_{LIVE_SUFFIX}_{side}.csv")
    return os.path.join(live_dir, f"{family}_{date}_{LIVE_SUFFIX}.csv")


def load_live_trades(live_dir, date):
    """Return (real_trades, stale_skips). Real = SL/TARGET/EOD_CLOSE/TIME_STOP."""
    rows = read_csv(live_path(live_dir, "paper_trades", date))
    real, stale = [], []
    for r in rows:
        outcome = norm_outcome(r.get("outcome"))
        sig = parse_ist(pick(r, "signal_datetime", "signal_entry_datetime_ist", "entry_time"))
        rec = {
            "date": date,
            "ticker": str(r.get("ticker", "")).strip().upper(),
            "side": norm_side(r.get("side")),
            "setup": norm_setup(r.get("setup")),
            "signal_bar": floor_5min(sig),
            "signal_time": sig,
            "entry_time": parse_ist(r.get("entry_time")),
            "exit_time": parse_ist(r.get("exit_time")),
            "entry_price": to_float(r.get("entry_price")),
            "exit_price": to_float(r.get("exit_price")),
            "qty": to_float(r.get("quantity")),
            "outcome": outcome,
            "live_gross_rs": to_float(pick(r, "gross_pnl_rs", "gross_pnl")),
            "live_cost_rs": to_float(pick(r, "total_cost_rs", "total_cost")),
            "live_net_rs": to_float(pick(r, "net_pnl_rs", "net_pnl", "pnl_rs")),
            "trade_id": r.get("trade_id", ""),
        }
        if outcome == "STALE_SKIP":
            stale.append(rec)
        else:
            real.append(rec)
    return real, stale


def load_live_signals(live_dir, date):
    out = []
    for side in ("long", "short"):
        for r in read_csv(live_path(live_dir, "signals", date, side)):
            sig = parse_ist(pick(r, "signal_datetime", "signal_bar_time_ist", "signal_entry_datetime_ist"))
            out.append({
                "date": date,
                "ticker": str(r.get("ticker", "")).strip().upper(),
                "side": norm_side(r.get("side")),
                "setup": norm_setup(r.get("setup")),
                "signal_bar": floor_5min(sig),
                "quality_score": to_float(r.get("quality_score")),
            })
    return out


def resolve_bt_dir(bt_root, date, overrides):
    if date in overrides:
        return overrides[date]
    return os.path.join(bt_root, date)


def load_bt_trades(bt_dir, date):
    trades_csv = os.path.join(bt_dir, "trades.csv")
    compact = os.path.join(bt_dir, "v11_ID_trades.csv")
    rows = read_csv(trades_csv)
    out = []
    if rows:
        for r in rows:
            sig = parse_ist(pick(r, "signal_time_ist", "signal_datetime"))
            out.append(_bt_rec(date, r,
                                ticker=pick(r, "ticker", "symbol"),
                                side=r.get("side"),
                                setup=pick(r, "setup", "setup_name"),
                                signal=sig,
                                entry_time=parse_ist(pick(r, "entry_time_v6", "v7_signal_entry_time_ist")),
                                exit_time=parse_ist(pick(r, "v6_exit_time_ist", "exit_time")),
                                entry_price=to_float(pick(r, "entry_price_v6", "entry_price")),
                                exit_price=to_float(pick(r, "v6_exit_price", "exit_price")),
                                qty=to_float(r.get("quantity")),
                                outcome=pick(r, "v6_outcome", "exit_reason"),
                                gross=to_float(pick(r, "v6_gross_pnl_rs"))))
        return out, "trades.csv"
    rows = read_csv(compact)   # fallback
    for r in rows:
        sig = parse_ist(pick(r, "signal_datetime", "entry_time"))
        out.append(_bt_rec(date, r,
                            ticker=pick(r, "symbol", "ticker"),
                            side=r.get("side"),
                            setup=pick(r, "setup_name", "setup"),
                            signal=sig,
                            entry_time=parse_ist(r.get("entry_time")),
                            exit_time=parse_ist(r.get("exit_time")),
                            entry_price=to_float(r.get("entry_price")),
                            exit_price=to_float(r.get("exit_price")),
                            qty=to_float(r.get("quantity")),
                            outcome=pick(r, "exit_reason"),
                            gross=to_float(r.get("pnl"))))
    return out, ("v11_ID_trades.csv" if rows else "NONE")


def _bt_rec(date, r, ticker, side, setup, signal, entry_time, exit_time,
            entry_price, exit_price, qty, outcome, gross):
    return {
        "date": date,
        "ticker": str(ticker or "").strip().upper(),
        "side": norm_side(side),
        "setup": norm_setup(setup),
        "signal_bar": floor_5min(signal),
        "signal_time": signal,
        "entry_time": entry_time,
        "exit_time": exit_time,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "qty": qty,
        "outcome": norm_outcome(outcome),
        "bt_gross_rs": gross,
    }


def load_bt_signals(bt_dir):
    for name in ("live_parity_selected_strategy_signals.csv",
                 "historical_full_day_selected_strategy_signals.csv"):
        rows = read_csv(os.path.join(bt_dir, name))
        if rows:
            out = []
            for r in rows:
                sig = parse_ist(pick(r, "signal_time_ist", "signal_datetime"))
                out.append({
                    "ticker": str(pick(r, "ticker", "symbol")).strip().upper(),
                    "side": norm_side(r.get("side")),
                    "setup": norm_setup(pick(r, "setup", "setup_name")),
                    "signal_bar": floor_5min(sig),
                })
            return out, name
    return [], "NONE"


# --------------------------------------------------------------------------------------
# Matching  (group by ticker/side/setup, greedy nearest signal bar within tolerance)
# --------------------------------------------------------------------------------------
def _key(rec):
    return (rec["date"], rec["ticker"], rec["side"], rec["setup"])


def match(live_list, bt_list, tol_min=TOL_BAR_MIN):
    matched, live_only, bt_only = [], [], []
    by_key_bt = defaultdict(list)
    for b in bt_list:
        by_key_bt[_key(b)].append(b)
    used_bt = set()
    for lv in live_list:
        cands = by_key_bt.get(_key(lv), [])
        best, best_dt = None, None
        for i, b in enumerate(cands):
            if id(b) in used_bt:
                continue
            if lv["signal_bar"] is None or b["signal_bar"] is None:
                dt = 0 if (lv["signal_bar"] is None and b["signal_bar"] is None) else 10 ** 9
            else:
                dt = abs((lv["signal_bar"] - b["signal_bar"]).total_seconds()) / 60.0
            if dt <= tol_min and (best is None or dt < best_dt):
                best, best_dt = b, dt
        if best is not None:
            used_bt.add(id(best))
            matched.append((lv, best, best_dt))
        else:
            live_only.append(lv)
    for b in bt_list:
        if id(b) not in used_bt:
            bt_only.append(b)
    return matched, live_only, bt_only


# --------------------------------------------------------------------------------------
# Root-cause classification
# --------------------------------------------------------------------------------------
def classify_live_only(lv, bt_had_any):
    if lv["outcome"] == "STALE_SKIP":
        return "live_stale_skip", "Signal surfaced after freshness deadline; live-only latency artifact (expected)."
    return ("live_only_real",
            "Live took it; backtest produced no matching signal — investigate feed/universe/"
            "gate divergence for this (ticker, setup, bar).")


def classify_bt_only(b, live_zero_day):
    enable = SETUP_ENABLE_DATE.get(b["setup"])
    if enable and b["date"] < enable:
        return "config_as_of_drift", (
            f"Setup {b['setup']} was not live-enabled until {enable}; not a true parity break "
            f"(backtest used the current book).")
    if b["setup"] in RAW_PRE_GATE_READMIT_SETUPS:
        return "raw_pre_gate_readmit_bug", (
            "RAW_PRE_GATE readmit setup: v11 readmits it from the full ranked frame, but the live "
            "scanner v8-drops it before readmission — known live-emission bug.")
    if live_zero_day:
        return "backtest_only_live_zero_day", (
            "Live produced zero trades this session while the 5-min scanner accepted candidates; "
            "primary suspect is the entry-engine handoff freshness race (scanner writes "
            "latest_candidate_tickers.json at slot+45-60s vs the 30s max_signal_handoff_lag_sec "
            "deadline) -> raw_entry_rows=0 every slot. Verify against the day's entry-engine log.")
    return "backtest_only", (
        "Backtest signalled; live did not take it — check late-detection drop, position/slot cap, "
        "max-entry-slip gate, or dedupe.")


# --------------------------------------------------------------------------------------
# Aggregation
# --------------------------------------------------------------------------------------
def summarize_side(trades, net_key):
    n = len(trades)
    wins = [t for t in trades if to_float(t.get(net_key)) > 0]
    losses = [t for t in trades if to_float(t.get(net_key)) < 0]
    gross = sum(to_float(t.get("_gross")) for t in trades if not math.isnan(to_float(t.get("_gross"))))
    cost = sum(to_float(t.get("_cost")) for t in trades if not math.isnan(to_float(t.get("_cost"))))
    net = sum(to_float(t.get(net_key)) for t in trades if not math.isnan(to_float(t.get(net_key))))
    return {
        "n": n,
        "win_rate": round(100.0 * len(wins) / n, 1) if n else 0.0,
        "avg_win": round(sum(to_float(t.get(net_key)) for t in wins) / len(wins), 1) if wins else 0.0,
        "avg_loss": round(sum(to_float(t.get(net_key)) for t in losses) / len(losses), 1) if losses else 0.0,
        "gross_rs": round(gross, 1),
        "cost_rs": round(cost, 1),
        "net_rs": round(net, 1),
    }


def pearson(xs, ys):
    pts = [(x, y) for x, y in zip(xs, ys)
           if not (math.isnan(x) or math.isnan(y))]
    n = len(pts)
    if n < 2:
        return float("nan")
    mx = sum(p[0] for p in pts) / n
    my = sum(p[1] for p in pts) / n
    num = sum((p[0] - mx) * (p[1] - my) for p in pts)
    dx = math.sqrt(sum((p[0] - mx) ** 2 for p in pts))
    dy = math.sqrt(sum((p[1] - my) ** 2 for p in pts))
    if dx == 0 or dy == 0:
        return float("nan")
    return num / (dx * dy)


# --------------------------------------------------------------------------------------
# Date discovery
# --------------------------------------------------------------------------------------
def discover_dates(live_dir, last_n):
    pat = re.compile(rf"paper_trade_execution_(\d{{4}}-\d{{2}}-\d{{2}})_{LIVE_SUFFIX}\.log$")
    dates = set()
    for fn in os.listdir(live_dir):
        m = pat.match(fn)
        if m:
            dates.add(m.group(1))
    return sorted(dates)[-last_n:]


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="V7-live vs V11-backtest parity reconciliation")
    ap.add_argument("--live-dir", default=DEFAULT_LIVE_DIR)
    ap.add_argument("--bt-root", default=DEFAULT_BT_ROOT)
    ap.add_argument("--dates", default="", help="comma-separated YYYY-MM-DD; overrides --last-n")
    ap.add_argument("--last-n", type=int, default=7)
    ap.add_argument("--bt-override", action="append", default=[],
                    help="DATE=DIR to point a date at a non-standard backtest dir (repeatable)")
    ap.add_argument("--slippage-bps", type=float, default=0.0,
                    help="model this per-leg slippage on the BACKTEST side for apples-to-apples net "
                         "(default 0 = report the live-vs-backtest slippage gap instead of hiding it)")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "out"))
    args = ap.parse_args()

    overrides = {}
    for o in args.bt_override:
        if "=" in o:
            k, v = o.split("=", 1)
            overrides[k.strip()] = v.strip()

    if args.dates:
        dates = [d.strip() for d in args.dates.split(",") if d.strip()]
    else:
        dates = discover_dates(args.live_dir, args.last_n)
    if not dates:
        print("ERROR: no trading dates discovered", file=sys.stderr)
        return 2

    os.makedirs(args.out, exist_ok=True)
    print(f"Reconciling {len(dates)} sessions: {', '.join(dates)}")
    print(f"  slippage model on backtest side: {args.slippage_bps} bps/leg "
          f"({'apples-to-apples' if args.slippage_bps else 'report gap'})\n")

    all_matched, all_live_only, all_bt_only, all_stale = [], [], [], []
    sig_rows = []
    per_day = []

    for date in dates:
        bt_dir = resolve_bt_dir(args.bt_root, date, overrides)
        live_real, live_stale = load_live_trades(args.live_dir, date)
        bt_trades, bt_src = load_bt_trades(bt_dir, date)
        live_sigs = load_live_signals(args.live_dir, date)
        bt_sigs, bt_sig_src = load_bt_signals(bt_dir)
        live_zero = (len(live_real) == 0)

        # cost-normalize both sides (statutory). Backtest optionally gets modeled slippage.
        for t in live_real + live_stale:
            g, c, n = statutory_costs(t["entry_price"], t["exit_price"], t["qty"], t["side"])
            t["_gross"], t["_cost"], t["_net_stat"] = g, c, n
        for t in bt_trades:
            g, c, n = statutory_costs(t["entry_price"], t["exit_price"], t["qty"], t["side"],
                                      slippage_bps=args.slippage_bps)
            t["_gross"], t["_cost"], t["_net_stat"] = g, c, n

        matched, live_only, bt_only = match(live_real, bt_trades)

        # attach tags
        for lv in live_only:
            cause, note = classify_live_only(lv, bool(bt_trades))
            lv["_cause"], lv["_note"] = cause, note
        for lv in live_stale:
            lv["_cause"], lv["_note"] = "live_stale_skip", "Skipped in live for staleness; never an executable trade."
        for b in bt_only:
            cause, note = classify_bt_only(b, live_zero)
            b["_cause"], b["_note"] = cause, note

        all_matched += [(date, lv, b, dt) for (lv, b, dt) in matched]
        all_live_only += live_only
        all_bt_only += bt_only
        all_stale += live_stale

        # ---- signal-level reconciliation ----
        sm, slo, sbo = match(
            [dict(r, date=date, signal_time=r["signal_bar"], entry_price=1, exit_price=1, qty=1, outcome="") for r in live_sigs],
            [dict(r, date=date, signal_time=r["signal_bar"], entry_price=1, exit_price=1, qty=1, outcome="") for r in bt_sigs],
        )
        sig_rows.append({"date": date, "live_signals": len(live_sigs), "bt_signals": len(bt_sigs),
                         "matched": len(sm), "live_only": len(slo), "bt_only": len(sbo)})

        # ---- per-day aggregate ----
        live_side = summarize_side(live_real, "_net_stat")
        bt_side = summarize_side(bt_trades, "_net_stat")
        live_recorded_net = round(sum(to_float(t["live_net_rs"]) for t in live_real
                                      if not math.isnan(to_float(t["live_net_rs"]))), 1)
        per_day.append({
            "date": date, "bt_src": bt_src,
            "live_signals": len(live_sigs), "bt_signals": len(bt_sigs),
            "live_trades": live_side["n"], "bt_trades": bt_side["n"],
            "matched": len(matched), "live_only": len(live_only),
            "live_stale": len(live_stale), "bt_only": len(bt_only),
            "live_win_rate": live_side["win_rate"], "bt_win_rate": bt_side["win_rate"],
            "live_net_stat_rs": live_side["net_rs"], "bt_net_stat_rs": bt_side["net_rs"],
            "live_recorded_net_rs": live_recorded_net,
            "bt_gross_rs": bt_side["gross_rs"], "live_gross_rs": live_side["gross_rs"],
        })
        print(f"  {date}: live_real={live_side['n']:>2} bt={bt_side['n']:>2} "
              f"matched={len(matched):>2} live_only={len(live_only):>2} "
              f"stale={len(live_stale)} bt_only={len(bt_only):>2}  "
              f"live_net={live_side['net_rs']:>9.1f}  bt_net={bt_side['net_rs']:>9.1f}  "
              f"[{bt_src}]")

    # ---------------- per-setup aggregate ----------------
    per_setup = defaultdict(lambda: {"live_n": 0, "bt_n": 0, "matched": 0,
                                     "live_only": 0, "bt_only": 0,
                                     "live_net": 0.0, "bt_net": 0.0})
    for (date, lv, b, dt) in all_matched:
        s = per_setup[lv["setup"]]
        s["matched"] += 1
        s["live_n"] += 1
        s["bt_n"] += 1
        s["live_net"] += to_float(lv["_net_stat"], 0)
        s["bt_net"] += to_float(b["_net_stat"], 0)
    for lv in all_live_only:
        s = per_setup[lv["setup"]]
        s["live_only"] += 1
        s["live_n"] += 1
        s["live_net"] += to_float(lv["_net_stat"], 0)
    for b in all_bt_only:
        s = per_setup[b["setup"]]
        s["bt_only"] += 1
        s["bt_n"] += 1
        s["bt_net"] += to_float(b["_net_stat"], 0)

    # ---------------- rates / correlation / divergence ----------------
    tot_matched = len(all_matched)
    tot_live_real = tot_matched + len(all_live_only)
    tot_bt = tot_matched + len(all_bt_only)
    union = tot_matched + len(all_live_only) + len(all_bt_only)
    trade_match_rate = round(100.0 * tot_matched / union, 1) if union else 0.0

    sig_matched = sum(r["matched"] for r in sig_rows)
    sig_union = sig_matched + sum(r["live_only"] for r in sig_rows) + sum(r["bt_only"] for r in sig_rows)
    signal_match_rate = round(100.0 * sig_matched / sig_union, 1) if sig_union else 0.0

    corr = pearson([d["live_net_stat_rs"] for d in per_day],
                   [d["bt_net_stat_rs"] for d in per_day])
    live_total_net = round(sum(d["live_net_stat_rs"] for d in per_day), 1)
    bt_total_net = round(sum(d["bt_net_stat_rs"] for d in per_day), 1)
    live_recorded_total = round(sum(d["live_recorded_net_rs"] for d in per_day), 1)
    denom = abs(live_total_net) if abs(live_total_net) > 1e-9 else 1.0
    net_div_pct = round(100.0 * (bt_total_net - live_total_net) / denom, 1)

    # matched-trade diffs
    price_flags = 0
    net_flags = 0
    matched_detail = []
    for (date, lv, b, dt) in all_matched:
        ep_bps = 1e4 * abs(b["entry_price"] - lv["entry_price"]) / lv["entry_price"] if lv["entry_price"] else float("nan")
        xp_bps = 1e4 * abs(b["exit_price"] - lv["exit_price"]) / lv["exit_price"] if lv["exit_price"] else float("nan")
        notional = lv["entry_price"] * lv["qty"] if lv["qty"] else float("nan")
        net_diff = to_float(b["_net_stat"]) - to_float(lv["_net_stat"])
        net_bps = 1e4 * abs(net_diff) / notional if notional and not math.isnan(notional) else float("nan")
        if (not math.isnan(ep_bps) and ep_bps > TOL_PRICE_BPS) or (not math.isnan(xp_bps) and xp_bps > TOL_PRICE_BPS):
            price_flags += 1
        if not math.isnan(net_bps) and net_bps > TOL_NET_BPS_OF_NOTIONAL:
            net_flags += 1
        matched_detail.append({
            "date": date, "ticker": lv["ticker"], "side": lv["side"], "setup": lv["setup"],
            "live_signal_bar": lv["signal_bar"], "bt_signal_bar": b["signal_bar"], "bar_dt_min": dt,
            "live_entry": lv["entry_price"], "bt_entry": b["entry_price"], "entry_bps": round(ep_bps, 1) if not math.isnan(ep_bps) else "",
            "live_exit": lv["exit_price"], "bt_exit": b["exit_price"], "exit_bps": round(xp_bps, 1) if not math.isnan(xp_bps) else "",
            "live_outcome": lv["outcome"], "bt_outcome": b["outcome"], "outcome_match": lv["outcome"] == b["outcome"],
            "live_qty": lv["qty"], "bt_qty": b["qty"],
            "live_net_stat": round(to_float(lv["_net_stat"]), 1), "bt_net_stat": round(to_float(b["_net_stat"]), 1),
            "net_diff_rs": round(net_diff, 1), "net_bps_of_notional": round(net_bps, 1) if not math.isnan(net_bps) else "",
            "live_recorded_net": round(to_float(lv["live_net_rs"]), 1),
            "live_slippage_vs_model_rs": round(to_float(lv["live_net_rs"]) - to_float(lv["_net_stat"]), 1),
        })

    # ---------------- write outputs ----------------
    def write_csv(name, rows, cols):
        p = os.path.join(args.out, name)
        with open(p, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            for r in rows:
                w.writerow({c: r.get(c, "") for c in cols})
        return p

    write_csv("parity_per_day.csv", per_day, list(per_day[0].keys()) if per_day else [])
    write_csv("parity_signal_reconciliation.csv", sig_rows, list(sig_rows[0].keys()) if sig_rows else [])
    if matched_detail:
        write_csv("parity_matched_trades.csv", matched_detail, list(matched_detail[0].keys()))
    lo_cols = ["date", "ticker", "side", "setup", "signal_bar", "entry_price", "exit_price",
               "qty", "outcome", "live_net_rs", "_cause", "_note"]
    write_csv("parity_live_only.csv", all_live_only + all_stale, lo_cols)
    bo_cols = ["date", "ticker", "side", "setup", "signal_bar", "entry_price", "exit_price",
               "qty", "outcome", "bt_gross_rs", "_net_stat", "_cause", "_note"]
    write_csv("parity_backtest_only.csv", all_bt_only, bo_cols)
    ps_rows = [dict(setup=k, **v) for k, v in sorted(per_setup.items(), key=lambda kv: -(kv[1]["live_only"] + kv[1]["bt_only"]))]
    for r in ps_rows:
        r["live_net"] = round(r["live_net"], 1)
        r["bt_net"] = round(r["bt_net"], 1)
    write_csv("parity_per_setup.csv", ps_rows,
              ["setup", "live_n", "bt_n", "matched", "live_only", "bt_only", "live_net", "bt_net"])

    # cause tallies
    cause_tally = defaultdict(int)
    for lv in all_live_only:
        cause_tally[lv["_cause"]] += 1
    for lv in all_stale:
        cause_tally["live_stale_skip"] += 1
    for b in all_bt_only:
        cause_tally[b["_cause"]] += 1

    verdict_fail = (
        trade_match_rate < 90.0 or abs(net_div_pct) > 25.0 or
        len([lv for lv in all_live_only if lv["_cause"] == "live_only_real"]) > 0 or price_flags > 0
    )
    verdict = "FAIL" if verdict_fail else "PASS"

    summary = {
        "dates": dates,
        "totals": {
            "live_real_trades": tot_live_real, "bt_trades": tot_bt,
            "matched": tot_matched, "live_only_real": len([lv for lv in all_live_only if lv["_cause"] == "live_only_real"]),
            "live_stale_skips": len(all_stale), "backtest_only": len(all_bt_only),
        },
        "rates": {"trade_match_rate_pct": trade_match_rate, "signal_match_rate_pct": signal_match_rate},
        "pnl": {"live_net_stat_rs": live_total_net, "bt_net_stat_rs": bt_total_net,
                "live_recorded_net_rs": live_recorded_total,
                "net_divergence_pct": net_div_pct, "daily_net_correlation": None if math.isnan(corr) else round(corr, 3)},
        "matched_diffs": {"price_beyond_%.0fbps" % TOL_PRICE_BPS: price_flags,
                          "net_beyond_%.0fbps_notional" % TOL_NET_BPS_OF_NOTIONAL: net_flags,
                          "matched_count": tot_matched},
        "cause_tally": dict(sorted(cause_tally.items(), key=lambda kv: -kv[1])),
        "verdict": verdict,
        "tolerances": {"trade_match_rate_min_pct": 90.0, "net_divergence_max_pct": 25.0,
                       "price_bps": TOL_PRICE_BPS, "net_bps_of_notional": TOL_NET_BPS_OF_NOTIONAL,
                       "bar_tolerance_min": TOL_BAR_MIN},
    }
    with open(os.path.join(args.out, "parity_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)

    _write_md(args.out, dates, per_day, sig_rows, ps_rows, matched_detail,
              all_live_only, all_bt_only, all_stale, summary)

    # ---------------- console verdict ----------------
    print("\n" + "=" * 78)
    print(f"VERDICT: {verdict}")
    print(f"  trade match rate     : {trade_match_rate}%   (tol >=90%)")
    print(f"  signal match rate    : {signal_match_rate}%")
    print(f"  matched / live / bt  : {tot_matched} / {tot_live_real} / {tot_bt}")
    print(f"  live_only(real)      : {summary['totals']['live_only_real']}   stale_skips: {len(all_stale)}   bt_only: {len(all_bt_only)}")
    print(f"  net (statutory)      : live {live_total_net}  bt {bt_total_net}   divergence {net_div_pct}%")
    print(f"  live recorded net    : {live_recorded_total}   (slippage/real-fill gap vs statutory model)")
    print(f"  daily net corr       : {summary['pnl']['daily_net_correlation']}")
    print(f"  matched price>{TOL_PRICE_BPS:.0f}bps : {price_flags}    net>{TOL_NET_BPS_OF_NOTIONAL:.0f}bps: {net_flags}")
    print(f"  cause tally          : {summary['cause_tally']}")
    print(f"\n  outputs -> {args.out}")
    return 1 if verdict == "FAIL" else 0


def _md_table(rows, cols, headers=None):
    headers = headers or cols
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in cols) + " |"]
    for r in rows:
        out.append("| " + " | ".join(str(r.get(c, "")).replace("|", "\\|") for c in cols) + " |")
    return "\n".join(out)


def _write_md(out_dir, dates, per_day, sig_rows, ps_rows, matched_detail,
              live_only, bt_only, stale, summary):
    L = []
    L.append("# V7-live vs V11-backtest — Parity Reconciliation (auto-generated tables)\n")
    L.append(f"Sessions: {', '.join(dates)}  \nVerdict: **{summary['verdict']}**\n")
    L.append("## Per-day\n")
    L.append(_md_table(per_day,
                       ["date", "live_signals", "bt_signals", "live_trades", "bt_trades",
                        "matched", "live_only", "live_stale", "bt_only",
                        "live_net_stat_rs", "bt_net_stat_rs", "live_recorded_net_rs"]))
    L.append("\n## Signal reconciliation\n")
    L.append(_md_table(sig_rows, ["date", "live_signals", "bt_signals", "matched", "live_only", "bt_only"]))
    L.append("\n## Per-setup (ranked by unmatched)\n")
    L.append(_md_table(ps_rows, ["setup", "live_n", "bt_n", "matched", "live_only", "bt_only", "live_net", "bt_net"]))
    L.append("\n## Root-cause tally\n")
    L.append(_md_table([{"cause": k, "count": v} for k, v in summary["cause_tally"].items()], ["cause", "count"]))
    L.append("\n## Matched trades (entry/exit/net diffs)\n")
    if matched_detail:
        L.append(_md_table(matched_detail,
                           ["date", "ticker", "side", "setup", "bar_dt_min", "live_entry", "bt_entry", "entry_bps",
                            "live_exit", "bt_exit", "exit_bps", "live_outcome", "bt_outcome",
                            "live_net_stat", "bt_net_stat", "net_bps_of_notional", "live_slippage_vs_model_rs"]))
    else:
        L.append("_No matched trades._")
    L.append("\n## Sample LIVE-ONLY (up to 15)\n")
    L.append(_md_table((live_only + stale)[:15],
                       ["date", "ticker", "side", "setup", "signal_bar", "outcome", "_cause", "_note"]))
    L.append("\n## Sample BACKTEST-ONLY (up to 25)\n")
    L.append(_md_table(bt_only[:25],
                       ["date", "ticker", "side", "setup", "signal_bar", "outcome", "bt_gross_rs", "_cause", "_note"]))
    L.append("")
    with open(os.path.join(out_dir, "parity_report_generated.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(L))


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # noqa
        import traceback
        traceback.print_exc()
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)
