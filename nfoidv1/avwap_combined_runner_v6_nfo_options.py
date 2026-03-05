# -*- coding: utf-8 -*-
"""
avwap_combined_runner_v6_nfo_options.py
=======================================

Backtests ATM NFO options using AVWAP v4 stock entries.

Workflow:
1) Read v4 stock trades (latest outputs_v4 CSV by default).
2) Read trade->option map produced by nfo_options_data_fetcher_v6_nfo_options.py.
3) For each stock entry:
   - buy ATM CE for LONG stock trades
   - buy ATM PE for SHORT stock trades
   - option entry at first available 5-min candle open at/after stock entry time
   - apply configurable option SL/TGT
   - exit on SL/TGT or at stock trade exit time (default), else EOD fallback
4) Save option trade CSV + skip CSV + metrics log in outputs_v6_nfo_options/.

This file does not modify existing v4 stock backtest behavior.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import dataclass
from datetime import time as dtime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from avwap_v11_refactored.avwap_common import IST, compute_backtest_metrics, now_ist, print_metrics
from avwap_combined_runner_v2 import _Tee  # reuse existing logging tee


THIS_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = THIS_DIR / "outputs_v6_nfo_options"
DEFAULT_OPTIONS_DATA_DIR = THIS_DIR / "options_data_v6_nfo_options"
DEFAULT_TRADE_MAP = "nfo_options_trade_map_v6.csv"


def _configure_console_encoding() -> None:
    # Guard against Windows cp1252/charmap print failures.
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None:
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Backtest ATM NFO options from AVWAP v4 entries."
    )
    p.add_argument(
        "--v4-trades-csv",
        default=None,
        help="Path to v4 trade CSV. If omitted, latest outputs_v4 file is used.",
    )
    p.add_argument(
        "--options-data-dir",
        default=str(DEFAULT_OPTIONS_DATA_DIR),
        help="Directory containing option parquet files and trade map CSV.",
    )
    p.add_argument(
        "--trade-map-csv",
        default=None,
        help="Path to trade map CSV. Default: <options-data-dir>/nfo_options_trade_map_v6.csv",
    )
    p.add_argument(
        "--option-sl-pct",
        type=float,
        default=0.15,
        help="Option stop-loss percent as decimal (0.15 = 15%%).",
    )
    p.add_argument(
        "--option-target-pct",
        type=float,
        default=0.40,
        help="Option target percent as decimal (0.40 = 40%%).",
    )
    p.add_argument(
        "--lots-per-trade",
        type=int,
        default=1,
        help="Number of option lots per trade.",
    )
    p.add_argument(
        "--use-underlying-exit-time",
        action="store_true",
        default=True,
        help="Exit option by underlying stock exit time if SL/TGT not hit.",
    )
    p.add_argument(
        "--same-bar-policy",
        choices=["sl_first", "target_first"],
        default="sl_first",
        help="If both SL and target are touched in same 5-min bar, apply this policy.",
    )
    p.add_argument(
        "--from-date",
        default=None,
        help="Filter stock trades from this date (YYYY-MM-DD).",
    )
    p.add_argument(
        "--to-date",
        default=None,
        help="Filter stock trades up to this date (YYYY-MM-DD).",
    )
    p.add_argument(
        "--recent-months",
        type=int,
        default=0,
        help="Keep only last N months of stock trades based on max trade date in v4 CSV (0=disabled).",
    )
    return p.parse_args()


def _parse_date_arg(raw: Optional[str], arg_name: str) -> Optional[pd.Timestamp]:
    if raw is None or str(raw).strip() == "":
        return None
    ts = pd.to_datetime(raw, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"Invalid {arg_name}: {raw}. Expected YYYY-MM-DD.")
    return ts


def _filter_trades_by_date(
    trades: pd.DataFrame,
    from_date: Optional[pd.Timestamp],
    to_date: Optional[pd.Timestamp],
    recent_months: int,
) -> pd.DataFrame:
    if trades.empty:
        return trades
    d = trades.copy()
    td = pd.to_datetime(d["trade_date"], errors="coerce")

    if recent_months and recent_months > 0:
        max_dt = td.max()
        start_dt = max_dt - pd.DateOffset(months=int(recent_months))
        d = d[td >= start_dt].copy()
        td = pd.to_datetime(d["trade_date"], errors="coerce")

    if from_date is not None:
        d = d[td >= from_date].copy()
        td = pd.to_datetime(d["trade_date"], errors="coerce")
    if to_date is not None:
        d = d[td <= to_date].copy()

    return d.reset_index(drop=True)


def _pick_latest_v4_csv(base_dir: Path) -> Path:
    files = sorted(
        (base_dir / "outputs_v4").glob("avwap_longshort_trades_ALL_DAYS_v4_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(
            "No v4 trades CSV found in outputs_v4. Pass --v4-trades-csv explicitly."
        )
    return files[0]


def _to_ist_series(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    try:
        if getattr(dt.dt, "tz", None) is None:
            return dt.dt.tz_localize(IST)
        return dt.dt.tz_convert(IST)
    except Exception:
        return dt


def _stable_float(v: object) -> str:
    try:
        return f"{float(v):.4f}"
    except Exception:
        return ""


def _make_trade_uid(row: pd.Series) -> str:
    payload = "|".join(
        [
            str(row.get("trade_date", "")),
            str(row.get("ticker", "")),
            str(row.get("side", "")),
            str(row.get("signal_time_ist", "")),
            str(row.get("entry_time_ist", "")),
            _stable_float(row.get("entry_price", "")),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]


def _load_v4_trades(path: Path) -> pd.DataFrame:
    d = pd.read_csv(path)
    required = {"trade_date", "ticker", "side", "entry_time_ist", "entry_price"}
    missing = sorted(required - set(d.columns))
    if missing:
        raise ValueError(f"v4 trade CSV missing required columns: {missing}")

    out = d.copy()
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["side"] = out["side"].astype(str).str.upper().str.strip()
    for c in ["signal_time_ist", "entry_time_ist", "exit_time_ist"]:
        if c in out.columns:
            out[c] = _to_ist_series(out[c])
    out["entry_price"] = pd.to_numeric(out["entry_price"], errors="coerce")
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.date
    out = out.dropna(subset=["trade_date", "ticker", "side", "entry_time_ist", "entry_price"]).copy()
    out["trade_uid"] = out.apply(_make_trade_uid, axis=1)
    return out.reset_index(drop=True)


def _load_trade_map(path: Path) -> pd.DataFrame:
    d = pd.read_csv(path)
    required = {"trade_uid", "option_file", "option_type", "instrument_token", "lot_size"}
    missing = sorted(required - set(d.columns))
    if missing:
        raise ValueError(f"trade map CSV missing required columns: {missing}")

    out = d.copy()
    out["trade_uid"] = out["trade_uid"].astype(str).str.strip()
    out["option_file"] = out["option_file"].astype(str).str.strip()
    out["option_type"] = out["option_type"].astype(str).str.upper().str.strip()
    out["instrument_token"] = pd.to_numeric(out["instrument_token"], errors="coerce").fillna(0).astype(int)
    out["lot_size"] = pd.to_numeric(out["lot_size"], errors="coerce").fillna(1).astype(int)
    if "tradingsymbol" in out.columns:
        out["tradingsymbol"] = out["tradingsymbol"].astype(str).str.upper().str.strip()
    else:
        out["tradingsymbol"] = ""
    return out.drop_duplicates(subset=["trade_uid"], keep="last").reset_index(drop=True)


def _resolve_option_cutoff(entry_ts: pd.Timestamp, stock_exit_ts: Optional[pd.Timestamp]) -> pd.Timestamp:
    if stock_exit_ts is not None and not pd.isna(stock_exit_ts):
        if stock_exit_ts >= entry_ts:
            return stock_exit_ts
    day_close = pd.Timestamp.combine(entry_ts.date(), dtime(15, 30))
    return day_close.tz_localize(IST)


def _load_option_5m(path: Path, cache: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    key = str(path)
    if key in cache:
        return cache[key]

    if not path.exists():
        cache[key] = pd.DataFrame()
        return cache[key]

    d = pd.read_parquet(path)
    if d.empty:
        cache[key] = d
        return d

    dt_col = "datetime" if "datetime" in d.columns else ("date" if "date" in d.columns else None)
    if dt_col is None:
        cache[key] = pd.DataFrame()
        return cache[key]
    if dt_col != "datetime":
        d = d.rename(columns={dt_col: "datetime"})

    d["datetime"] = _to_ist_series(d["datetime"])
    keep = [c for c in ["datetime", "open", "high", "low", "close", "volume", "oi"] if c in d.columns]
    d = d[keep].dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)

    for c in ["open", "high", "low", "close", "volume", "oi"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    d = d.dropna(subset=["open", "high", "low", "close"]).copy()
    cache[key] = d
    return d


@dataclass
class SimResult:
    row: Optional[dict]
    skip: Optional[dict]


def _simulate_one_option_trade(
    stock_row: pd.Series,
    map_row: pd.Series,
    opt_df: pd.DataFrame,
    option_sl_pct: float,
    option_target_pct: float,
    lots_per_trade: int,
    use_underlying_exit_time: bool,
    same_bar_policy: str,
) -> SimResult:
    if opt_df.empty:
        return SimResult(
            None,
            {
                "trade_uid": stock_row.get("trade_uid"),
                "ticker": stock_row.get("ticker"),
                "side": stock_row.get("side"),
                "reason": "option_data_missing_or_empty",
            },
        )

    entry_ref = stock_row.get("entry_time_ist")
    if pd.isna(entry_ref):
        return SimResult(
            None,
            {
                "trade_uid": stock_row.get("trade_uid"),
                "ticker": stock_row.get("ticker"),
                "side": stock_row.get("side"),
                "reason": "stock_entry_time_missing",
            },
        )

    bars_after = opt_df[opt_df["datetime"] >= entry_ref]
    if bars_after.empty:
        return SimResult(
            None,
            {
                "trade_uid": stock_row.get("trade_uid"),
                "ticker": stock_row.get("ticker"),
                "side": stock_row.get("side"),
                "reason": "no_option_bar_at_or_after_entry_time",
            },
        )

    first = bars_after.iloc[0]
    opt_entry_time = first["datetime"]
    opt_entry_px = float(first["open"])
    if not np.isfinite(opt_entry_px) or opt_entry_px <= 0:
        return SimResult(
            None,
            {
                "trade_uid": stock_row.get("trade_uid"),
                "ticker": stock_row.get("ticker"),
                "side": stock_row.get("side"),
                "reason": "invalid_option_entry_price",
            },
        )

    stock_exit_time = stock_row.get("exit_time_ist") if use_underlying_exit_time else None
    cutoff = _resolve_option_cutoff(opt_entry_time, stock_exit_time)

    life = opt_df[(opt_df["datetime"] >= opt_entry_time) & (opt_df["datetime"] <= cutoff)].copy()
    if life.empty:
        return SimResult(
            None,
            {
                "trade_uid": stock_row.get("trade_uid"),
                "ticker": stock_row.get("ticker"),
                "side": stock_row.get("side"),
                "reason": "no_option_bars_before_cutoff",
            },
        )

    sl_px = opt_entry_px * (1.0 - float(option_sl_pct))
    tgt_px = opt_entry_px * (1.0 + float(option_target_pct))
    outcome = "EOD"
    exit_time = life.iloc[-1]["datetime"]
    exit_px = float(life.iloc[-1]["close"])

    for r in life.itertuples(index=False):
        high = float(r.high)
        low = float(r.low)
        ts = r.datetime
        both = (low <= sl_px) and (high >= tgt_px)
        if both:
            if same_bar_policy == "target_first":
                exit_px = float(tgt_px)
                outcome = "TARGET"
            else:
                exit_px = float(sl_px)
                outcome = "SL"
            exit_time = ts
            break
        if high >= tgt_px:
            exit_px = float(tgt_px)
            outcome = "TARGET"
            exit_time = ts
            break
        if low <= sl_px:
            exit_px = float(sl_px)
            outcome = "SL"
            exit_time = ts
            break

    lot_size = int(map_row.get("lot_size", 1))
    lot_size = lot_size if lot_size > 0 else 1
    lots = max(1, int(lots_per_trade))
    qty = lot_size * lots

    pnl_pts = exit_px - opt_entry_px
    pnl_pct = (pnl_pts / opt_entry_px * 100.0) if opt_entry_px > 0 else 0.0
    pnl_rs = pnl_pts * qty

    out = {
        "trade_uid": stock_row.get("trade_uid"),
        "trade_date": stock_row.get("trade_date"),
        "ticker": stock_row.get("ticker"),
        "underlying_side": stock_row.get("side"),
        "underlying_entry_time_ist": stock_row.get("entry_time_ist"),
        "underlying_exit_time_ist": stock_row.get("exit_time_ist"),
        "underlying_entry_price": stock_row.get("entry_price"),
        "option_type": map_row.get("option_type"),
        "tradingsymbol": map_row.get("tradingsymbol"),
        "instrument_token": int(map_row.get("instrument_token", 0)),
        "option_file": map_row.get("option_file"),
        "option_entry_time_ist": opt_entry_time,
        "option_entry_price": float(opt_entry_px),
        "option_sl_price": float(sl_px),
        "option_target_price": float(tgt_px),
        "option_exit_time_ist": exit_time,
        "option_exit_price": float(exit_px),
        "outcome": outcome,
        "lot_size": lot_size,
        "lots": lots,
        "qty": qty,
        "pnl_points": float(pnl_pts),
        "pnl_pct": float(pnl_pct),
        "pnl_pct_gross": float(pnl_pct),
        "pnl_rs": float(pnl_rs),
    }
    return SimResult(out, None)


def _win_rate(df: pd.DataFrame) -> float:
    if df.empty or "pnl_rs" not in df.columns:
        return 0.0
    pnl = pd.to_numeric(df["pnl_rs"], errors="coerce").fillna(0.0)
    return float((pnl > 0).mean() * 100.0)


def _print_notional(df: pd.DataFrame) -> None:
    if df.empty:
        return
    ce_mask = df["option_type"].eq("CE")
    pe_mask = df["option_type"].eq("PE")
    ce = float(df.loc[ce_mask, "pnl_rs"].sum())
    pe = float(df.loc[pe_mask, "pnl_rs"].sum())
    all_pnl = float(df["pnl_rs"].sum())
    ce_notional = float((df.loc[ce_mask, "option_entry_price"] * df.loc[ce_mask, "qty"]).sum())
    pe_notional = float((df.loc[pe_mask, "option_entry_price"] * df.loc[pe_mask, "qty"]).sum())
    all_notional = float((df["option_entry_price"] * df["qty"]).sum())
    avg_notional = all_notional / max(1, len(df))
    print(f"\n{'=' * 20} OPTION P&L SUMMARY (Rs.) {'=' * 20}")
    print(f"CE buy leg P&L                : Rs.{ce:,.2f}")
    print(f"PE buy leg P&L                : Rs.{pe:,.2f}")
    print(f"TOTAL option P&L              : Rs.{all_pnl:,.2f}")
    print(f"CE buy notional (entry)       : Rs.{ce_notional:,.2f}")
    print(f"PE buy notional (entry)       : Rs.{pe_notional:,.2f}")
    print(f"TOTAL notional (entry)        : Rs.{all_notional:,.2f}")
    print(f"Avg notional per trade        : Rs.{avg_notional:,.2f}")
    print("=" * 62)


def _print_outcome_breakdown(df: pd.DataFrame, label: str) -> None:
    if df.empty or "outcome" not in df.columns:
        return
    vc = df["outcome"].astype(str).value_counts(dropna=False)
    total = max(1, int(len(df)))
    print(f"[INFO] outcome breakdown ({label}):")
    for k in ["TARGET", "SL", "EOD", "BE"]:
        cnt = int(vc.get(k, 0))
        print(f"  - {k:<6}: {cnt:>4} ({(cnt/total*100.0):5.2f}%)")


def main() -> None:
    _configure_console_encoding()
    args = _parse_args()

    output_dir = DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = now_ist().strftime("%Y%m%d_%H%M%S")
    log_path = output_dir / f"avwap_combined_runner_v6_nfo_options_{ts}.txt"

    _orig_stdout, _orig_stderr = sys.stdout, sys.stderr
    with open(log_path, "w", encoding="utf-8") as _fh:
        sys.stdout = _Tee(_orig_stdout, _fh)
        sys.stderr = _Tee(_orig_stderr, _fh)

        try:
            options_data_dir = Path(args.options_data_dir).resolve()
            map_csv = Path(args.trade_map_csv).resolve() if args.trade_map_csv else (options_data_dir / DEFAULT_TRADE_MAP)
            v4_csv = Path(args.v4_trades_csv).resolve() if args.v4_trades_csv else _pick_latest_v4_csv(THIS_DIR)

            if not v4_csv.exists():
                raise FileNotFoundError(f"v4 CSV not found: {v4_csv}")
            if not map_csv.exists():
                raise FileNotFoundError(f"trade map CSV not found: {map_csv}")

            print("=" * 78)
            print("AVWAP COMBINED RUNNER v6_nfo_options - ATM option backtest from v4 entries")
            print("=" * 78)
            print()
            print("WHAT THIS RUNNER DOES:")
            print("  Reads v4 stock trade signals, looks up the ATM option contract that was")
            print("  mapped for each trade (by nfo_options_data_fetcher), simulates buying CE")
            print("  (LONG stock trades) or PE (SHORT stock trades) at the first 5-min open")
            print("  after stock entry, and exits at SL/TARGET or at stock exit time (EOD fallback).")
            print()
            print("ACCURACY DEPENDS ON DATA FETCHER QUALITY:")
            print("  If the trade->option map was built using today's live instrument master")
            print("  (without historical snapshots), old trades will be mapped to wrong contracts.")
            print("  The DTE stats printed below tell you how accurate your mapping is.")
            print("  DTE > 62 = almost certainly a bad mapping. DTE 0-62 = likely correct.")
            print()
            print("PARAMETERS:")
            print(f"  v4 trades CSV                : {v4_csv}")
            print(f"  options data dir             : {options_data_dir}")
            print(f"  trade map CSV                : {map_csv}")
            print(f"  option SL / target           : {args.option_sl_pct*100:.1f}% / {args.option_target_pct*100:.1f}%")
            print(f"  lots per trade               : {args.lots_per_trade}")
            print(f"  same bar hit policy          : {args.same_bar_policy}")
            print(f"  use underlying exit cutoff   : {bool(args.use_underlying_exit_time)}")
            print(
                f"  trade-date filters           : from={args.from_date or 'ALL'} | "
                f"to={args.to_date or 'ALL'} | recent_months={int(args.recent_months) or 'DISABLED'}"
            )
            if not int(args.recent_months):
                print()
                print("  [TIP] If you have not provided historical instrument snapshots to the fetcher,")
                print("        add --recent-months 3 to this runner to limit to only recent well-mapped")
                print("        trades and avoid garbage results from old far-dated contract proxies.")
            print("=" * 78)

            stock_df = _load_v4_trades(v4_csv)
            trade_map = _load_trade_map(map_csv)
            print(
                f"[INFO] stock trades loaded      : {len(stock_df)} | "
                f"date-range={min(stock_df['trade_date'])}..{max(stock_df['trade_date'])}"
            )

            from_dt = _parse_date_arg(args.from_date, "--from-date")
            to_dt = _parse_date_arg(args.to_date, "--to-date")
            before_filter = len(stock_df)
            stock_df = _filter_trades_by_date(
                trades=stock_df,
                from_date=from_dt,
                to_date=to_dt,
                recent_months=int(args.recent_months),
            )
            if stock_df.empty:
                raise RuntimeError("No stock trades left after date filtering.")
            print(
                f"[INFO] stock trades after filter: {len(stock_df)} "
                f"(dropped={before_filter-len(stock_df)}) | "
                f"date-range={min(stock_df['trade_date'])}..{max(stock_df['trade_date'])}"
            )
            print(f"[INFO] mapped option trades     : {len(trade_map)}")

            merged = stock_df.merge(trade_map, on="trade_uid", how="left", suffixes=("", "_map"))
            unresolved = merged["option_file"].isna().sum()
            resolved = len(merged) - int(unresolved)
            print(
                f"[INFO] trade-map coverage       : resolved={resolved}/{len(merged)} "
                f"({(resolved/len(merged)*100.0 if len(merged) else 0.0):.2f}%)"
            )
            if unresolved:
                print(f"[WARN] trades without map        : {int(unresolved)}")
            if "instrument_source" in merged.columns:
                src_vc = merged["instrument_source"].fillna("NA").astype(str).value_counts(dropna=False)
                print("[INFO] mapping source breakdown:")
                for src, cnt in src_vc.items():
                    print(f"  - {src}: {int(cnt)}")
            if "days_to_expiry" in merged.columns:
                dte = pd.to_numeric(merged["days_to_expiry"], errors="coerce")
                if dte.notna().any():
                    dte_min = int(dte.min())
                    dte_p50 = float(dte.median())
                    dte_p90 = float(dte.quantile(0.9))
                    dte_max = int(dte.max())
                    over_62 = int((dte > 62).sum())
                    within_62 = int((dte <= 62).sum())
                    print(
                        f"[INFO] mapped DTE stats         : "
                        f"min={dte_min} | p50={dte_p50:.1f} | p90={dte_p90:.1f} | max={dte_max}"
                    )
                    print(
                        f"[INFO] DTE distribution         : "
                        f"within_62={within_62} ({within_62/len(dte.dropna())*100:.1f}%) | "
                        f"over_62={over_62} ({over_62/len(dte.dropna())*100:.1f}%)"
                    )
                    if over_62 > 0:
                        print()
                        print("  +======================================================================+")
                        print("  |  DATA QUALITY WARNING - BAD CONTRACT MAPPING DETECTED               |")
                        print("  +======================================================================+")
                        print(f"  |  {over_62} trades ({over_62/len(dte.dropna())*100:.1f}%) are mapped to contracts with DTE > 62 days.     |")
                        print("  |                                                                      |")
                        print("  |  THIS MEANS: the data fetcher used today's live NFO master to map   |")
                        print("  |  old trade dates. Those trades are mapped to FAR-FUTURE expiries     |")
                        print("  |  (not what was actually trading on those dates). Their option P&L   |")
                        print("  |  figures are UNRELIABLE and should not be trusted.                  |")
                        print("  |                                                                      |")
                        print("  |  HOW TO FIX:                                                        |")
                        print("  |  1. Run nfo_instruments_snapshot_builder_v6_nfo_options.py DAILY.   |")
                        print("  |  2. Re-run data fetcher with --historical-instruments-dir <dir>     |")
                        print("  |     and --max-days-to-expiry 62 to filter bad mappings.             |")
                        print("  |  3. Or pass --recent-months 3 to limit to well-mapped recent trades.|")
                        print("  +======================================================================+")
                        print()
                    elif dte_p50 <= 30:
                        print("[INFO] DTE quality: GOOD - median DTE <= 30 days, mappings look accurate.")

            option_rows: List[dict] = []
            skip_rows: List[dict] = []
            cache: Dict[str, pd.DataFrame] = {}

            for i, row in enumerate(merged.itertuples(index=False), 1):
                row_s = pd.Series(row._asdict())
                option_file = str(row_s.get("option_file", "")).strip()
                if not option_file or option_file.lower() == "nan":
                    skip_rows.append(
                        {
                            "trade_uid": row_s.get("trade_uid"),
                            "ticker": row_s.get("ticker"),
                            "side": row_s.get("side"),
                            "reason": "trade_uid_not_present_in_trade_map",
                        }
                    )
                    continue

                p = Path(option_file)
                if not p.is_absolute():
                    p = options_data_dir / option_file

                opt_df = _load_option_5m(p, cache)
                sr = _simulate_one_option_trade(
                    stock_row=row_s,
                    map_row=row_s,
                    opt_df=opt_df,
                    option_sl_pct=float(args.option_sl_pct),
                    option_target_pct=float(args.option_target_pct),
                    lots_per_trade=int(args.lots_per_trade),
                    use_underlying_exit_time=bool(args.use_underlying_exit_time),
                    same_bar_policy=str(args.same_bar_policy),
                )
                if sr.row is not None:
                    option_rows.append(sr.row)
                elif sr.skip is not None:
                    skip_rows.append(sr.skip)

                if i % 250 == 0:
                    print(f"  [INFO] processed {i}/{len(merged)} stock trades")

            options_bt = pd.DataFrame(option_rows)
            skips_df = pd.DataFrame(skip_rows)
            simulated = len(options_bt)
            sim_pct = (simulated / len(merged) * 100.0) if len(merged) else 0.0
            print(
                f"[INFO] simulation coverage      : simulated={simulated}/{len(merged)} "
                f"({sim_pct:.2f}%) | skipped={len(skips_df)}"
            )

            if options_bt.empty:
                print("[DONE] No option trades were simulated.")
                if not skips_df.empty and "reason" in skips_df.columns:
                    reason_counts = (
                        skips_df["reason"]
                        .astype(str)
                        .value_counts(dropna=False)
                        .head(10)
                    )
                    print("[INFO] Top skip reasons:")
                    for reason, cnt in reason_counts.items():
                        print(f"  - {reason}: {int(cnt)}")
                if not skips_df.empty:
                    skip_csv = output_dir / f"avwap_v6_nfo_options_skips_{ts}.csv"
                    skips_df.to_csv(skip_csv, index=False)
                    print(f"[FILE SAVED] {skip_csv}")
                return

            # Keep canonical sort
            options_bt["trade_date"] = pd.to_datetime(options_bt["trade_date"], errors="coerce")
            options_bt["option_entry_time_ist"] = _to_ist_series(options_bt["option_entry_time_ist"])
            options_bt["option_exit_time_ist"] = _to_ist_series(options_bt["option_exit_time_ist"])
            options_bt = options_bt.sort_values(
                ["trade_date", "option_entry_time_ist", "ticker", "option_type"]
            ).reset_index(drop=True)

            # For avwap_common metric function compatibility.
            options_bt["entry_time_ist"] = options_bt["option_entry_time_ist"]
            options_bt["exit_time_ist"] = options_bt["option_exit_time_ist"]
            options_bt["side"] = "LONG"  # option buying only

            ce_df = options_bt[options_bt["option_type"].eq("CE")].copy()
            pe_df = options_bt[options_bt["option_type"].eq("PE")].copy()

            print_metrics("OPTIONS CE BUY (from LONG stock entries)", compute_backtest_metrics(ce_df))
            print(f"Win rate CE                  : {_win_rate(ce_df):.2f}%")
            _print_outcome_breakdown(ce_df, "CE")

            print_metrics("OPTIONS PE BUY (from SHORT stock entries)", compute_backtest_metrics(pe_df))
            print(f"Win rate PE                  : {_win_rate(pe_df):.2f}%")
            _print_outcome_breakdown(pe_df, "PE")

            print_metrics("OPTIONS COMBINED v6_nfo_options", compute_backtest_metrics(options_bt))
            print(f"Win rate combined            : {_win_rate(options_bt):.2f}%")
            _print_outcome_breakdown(options_bt, "COMBINED")
            _print_notional(options_bt)
            if not skips_df.empty and "reason" in skips_df.columns:
                print("[INFO] Top skip reasons:")
                reason_counts = (
                    skips_df["reason"]
                    .astype(str)
                    .value_counts(dropna=False)
                    .head(10)
                )
                for reason, cnt in reason_counts.items():
                    print(f"  - {reason}: {int(cnt)}")

            out_csv = output_dir / f"avwap_v6_nfo_options_trades_{ts}.csv"
            options_bt.to_csv(out_csv, index=False)
            print(f"[FILE SAVED] {out_csv}")

            if not ce_df.empty:
                ce_csv = output_dir / f"avwap_v6_nfo_options_ce_trades_{ts}.csv"
                ce_df.to_csv(ce_csv, index=False)
                print(f"[FILE SAVED] {ce_csv}")

            if not pe_df.empty:
                pe_csv = output_dir / f"avwap_v6_nfo_options_pe_trades_{ts}.csv"
                pe_df.to_csv(pe_csv, index=False)
                print(f"[FILE SAVED] {pe_csv}")

            if not skips_df.empty:
                skip_csv = output_dir / f"avwap_v6_nfo_options_skips_{ts}.csv"
                skips_df.to_csv(skip_csv, index=False)
                print(f"[FILE SAVED] {skip_csv}")

            # ---- Result reliability summary ----
            print()
            print("=" * 78)
            print("RESULT RELIABILITY SUMMARY")
            print("=" * 78)
            if "days_to_expiry" in options_bt.columns:
                dte_sim = pd.to_numeric(options_bt["days_to_expiry"], errors="coerce")
                bad = int((dte_sim > 62).sum()) if dte_sim.notna().any() else 0
                good = int((dte_sim <= 62).sum()) if dte_sim.notna().any() else simulated
                if bad == 0:
                    print(f"  [OK] All {simulated} simulated trades have DTE <= 62 days.")
                    print("  Mappings are likely accurate (trades used near-expiry ATM options).")
                else:
                    pct_bad = bad / max(1, simulated) * 100
                    print(f"  [!]  {bad}/{simulated} simulated trades ({pct_bad:.1f}%) had DTE > 62.")
                    print("  These trades used incorrect far-dated option proxies.")
                    print("  P&L for those trades is unreliable - see notes above.")
                    print()
                    print("  Reliable trades (DTE <= 62)  :", good)
                    print("  Unreliable trades (DTE > 62) :", bad)
            if "instrument_source" in options_bt.columns:
                src_vc = options_bt["instrument_source"].fillna("NA").astype(str).value_counts(dropna=False)
                print()
                print("  Instrument source breakdown for simulated trades:")
                for src, cnt in src_vc.items():
                    icon = "[OK]" if "snapshot" in src.lower() else "[!] "
                    print(f"    {icon} {src}: {int(cnt)}")
                if "historical_snapshot" in src_vc.index:
                    snap_pct = src_vc.get("historical_snapshot", 0) / max(1, simulated) * 100
                    print(f"  Snapshot-backed trades: {snap_pct:.1f}% (most accurate)")
                else:
                    print()
                    print("  [!] 0% snapshot-backed. All trades used live instrument master.")
                    print("  Run nfo_instruments_snapshot_builder_v6_nfo_options.py DAILY to fix this.")
                    print("  Quick fix: re-run with --recent-months 3")
            print()
            print("  If results look too good or too bad, check DTE and source above.")
            print("  Only trust results where instrument_source = 'historical_snapshot'")
            print("  or where DTE <= 30 (very near expiry = accurate ATM premium).")
            print("=" * 78)
            print("[DONE]")
        finally:
            sys.stdout = _orig_stdout
            sys.stderr = _orig_stderr

    print(f"[LOG SAVED] {log_path}")


if __name__ == "__main__":
    main()



