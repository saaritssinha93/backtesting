#!/usr/bin/env python3
"""Kite holdings/positions exporter with lightweight live scheduler for dashboard."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import pytz
from kiteconnect import KiteConnect

IST = pytz.timezone("Asia/Kolkata")
BASE_DIR = Path(__file__).resolve().parent
API_KEY_TXT = BASE_DIR / "api_key.txt"
ACCESS_TOKEN_TXT = BASE_DIR / "access_token.txt"
OUT_DIR = BASE_DIR / "kite_exports"

HOLDINGS_TODAY_NAME = "kite_holdings_today.csv"
POSITIONS_DAY_TODAY_NAME = "kite_positions_day_today.csv"
META_NAME = "kite_snapshot_meta.json"

DATE_SUFFIX_RE = re.compile(r"_(\d{8})\.(csv|json)$", re.IGNORECASE)


@dataclass
class SnapshotResult:
    as_of_ist: str
    holdings_rows: int
    positions_day_rows: int
    holdings_changed: bool
    positions_changed: bool
    holdings_sig: str
    positions_day_sig: str


def now_ist() -> datetime:
    return datetime.now(IST)


def _print(msg: str) -> None:
    ts = now_ist().strftime("%Y-%m-%d %H:%M:%S%z")
    print(f"{ts} | {msg}", flush=True)


def parse_hhmm(value: str) -> dtime:
    try:
        hh, mm = value.split(":", 1)
        return dtime(hour=int(hh), minute=int(mm))
    except Exception as exc:
        raise ValueError(f"Invalid HH:MM time: {value}") from exc


def read_api_key(path: Path = API_KEY_TXT) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Missing API key file: {path}")

    lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not lines:
        raise ValueError(f"API key file is empty: {path}")

    kv: dict[str, str] = {}
    for line in lines:
        if "=" in line:
            k, v = line.split("=", 1)
            kv[k.strip().upper()] = v.strip()

    if kv.get("API_KEY"):
        return kv["API_KEY"]

    return lines[0]


def read_access_token(path: Path = ACCESS_TOKEN_TXT) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Missing access token file: {path}")
    token = path.read_text(encoding="utf-8").strip()
    if not token:
        raise ValueError(f"Access token file is empty: {path}")
    return token


def setup_kite_session() -> KiteConnect:
    api_key = read_api_key()
    access_token = read_access_token()

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def fetch_holdings_df(kite: KiteConnect) -> pd.DataFrame:
    holdings = kite.holdings() or []
    df = pd.json_normalize(holdings)
    if df.empty:
        return df
    # Normalize key columns for stable display/order
    for col in ("exchange", "tradingsymbol"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().str.strip()
    return df


def fetch_positions_day_df(kite: KiteConnect) -> pd.DataFrame:
    payload = kite.positions() or {}
    day_rows = payload.get("day", []) if isinstance(payload, dict) else []
    df = pd.json_normalize(day_rows)
    if df.empty:
        return df
    for col in ("exchange", "tradingsymbol", "product"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().str.strip()
    return df


def _to_float_or_none(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _nested_get(payload: object, path: Iterable[str]) -> object:
    cur = payload
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def fetch_funds_available(kite: KiteConnect) -> Optional[float]:
    try:
        payload = kite.margins() or {}
    except Exception as exc:
        _print(f"WARNING margins() fetch failed: {exc}")
        return None

    for path in (
        ("equity", "available", "live_balance"),
        ("equity", "available", "cash"),
        ("equity", "net"),
    ):
        val = _to_float_or_none(_nested_get(payload, path))
        if val is not None:
            return val
    return None


def _stable_signature(df: pd.DataFrame, key_order: Iterable[str]) -> str:
    if df is None or df.empty:
        return "empty"

    cols = [c for c in key_order if c in df.columns]
    if not cols:
        cols = sorted(df.columns)

    work = df[cols].copy()
    for col in cols:
        work[col] = work[col].map(lambda v: "" if pd.isna(v) else str(v))

    sort_cols = [c for c in ("exchange", "tradingsymbol", "product") if c in work.columns]
    if sort_cols:
        work = work.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    payload = work.to_json(orient="records", force_ascii=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)


def _atomic_write_json(payload: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def cleanup_old_exports(out_dir: Path, keep_ymd: str) -> int:
    deleted = 0
    if not out_dir.exists():
        return deleted

    keep_names = {
        f"holdings_{keep_ymd}.csv",
        f"positions_day_{keep_ymd}.csv",
        HOLDINGS_TODAY_NAME,
        POSITIONS_DAY_TODAY_NAME,
        META_NAME,
    }

    for p in out_dir.iterdir():
        if not p.is_file():
            continue
        if p.name in keep_names:
            continue

        # Keep only the compact current-day snapshot set; remove legacy and old artifacts.
        m = DATE_SUFFIX_RE.search(p.name)
        if m and m.group(1) == keep_ymd:
            # same-day file but not part of keep set -> remove
            pass
        elif m and m.group(1) != keep_ymd:
            # old-day file -> remove
            pass
        elif p.suffix.lower() in {".csv", ".json"}:
            # non-dated CSV/JSON artifact not in keep set -> remove
            pass
        else:
            continue
        try:
            p.unlink()
            deleted += 1
        except OSError:
            continue
    return deleted


def write_snapshot_files(
    holdings_df: pd.DataFrame,
    positions_day_df: pd.DataFrame,
    funds_available: Optional[float],
    ts_ist: datetime,
    holdings_sig: str,
    positions_day_sig: str,
    holdings_changed: bool,
    positions_changed: bool,
    cleanup_old: bool,
) -> SnapshotResult:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    ymd = ts_ist.strftime("%Y%m%d")
    as_of = ts_ist.strftime("%Y-%m-%d %H:%M:%S%z")

    holdings_dated = OUT_DIR / f"holdings_{ymd}.csv"
    positions_day_dated = OUT_DIR / f"positions_day_{ymd}.csv"
    holdings_today = OUT_DIR / HOLDINGS_TODAY_NAME
    positions_day_today = OUT_DIR / POSITIONS_DAY_TODAY_NAME

    _atomic_write_csv(holdings_df, holdings_dated)
    _atomic_write_csv(positions_day_df, positions_day_dated)
    _atomic_write_csv(holdings_df, holdings_today)
    _atomic_write_csv(positions_day_df, positions_day_today)

    deleted_files = cleanup_old_exports(OUT_DIR, keep_ymd=ymd) if cleanup_old else 0

    meta = {
        "as_of_ist": as_of,
        "holdings_rows": int(len(holdings_df)),
        "positions_day_rows": int(len(positions_day_df)),
        "holdings_sig": holdings_sig,
        "positions_day_sig": positions_day_sig,
        "holdings_changed": bool(holdings_changed),
        "positions_day_changed": bool(positions_changed),
        "holdings_file": holdings_dated.name,
        "positions_day_file": positions_day_dated.name,
        "holdings_today_file": holdings_today.name,
        "positions_day_today_file": positions_day_today.name,
        "funds_available": (float(funds_available) if funds_available is not None else None),
        "cleanup_old_enabled": bool(cleanup_old),
        "cleanup_deleted_files": int(deleted_files),
    }
    _atomic_write_json(meta, OUT_DIR / META_NAME)

    return SnapshotResult(
        as_of_ist=as_of,
        holdings_rows=int(len(holdings_df)),
        positions_day_rows=int(len(positions_day_df)),
        holdings_changed=bool(holdings_changed),
        positions_changed=bool(positions_changed),
        holdings_sig=holdings_sig,
        positions_day_sig=positions_day_sig,
    )


def run_once(
    kite: KiteConnect,
    prev_holdings_sig: Optional[str],
    prev_positions_day_sig: Optional[str],
    cleanup_old: bool,
) -> tuple[SnapshotResult, str, str]:
    holdings_df = fetch_holdings_df(kite)
    positions_day_df = fetch_positions_day_df(kite)
    funds_available = fetch_funds_available(kite)

    holdings_sig = _stable_signature(
        holdings_df,
        (
            "exchange",
            "tradingsymbol",
            "quantity",
            "t1_quantity",
            "average_price",
            "last_price",
            "pnl",
            "day_change_percentage",
        ),
    )
    positions_day_sig = _stable_signature(
        positions_day_df,
        (
            "exchange",
            "tradingsymbol",
            "product",
            "quantity",
            "buy_quantity",
            "sell_quantity",
            "average_price",
            "last_price",
            "pnl",
        ),
    )

    holdings_changed = prev_holdings_sig != holdings_sig
    positions_changed = prev_positions_day_sig != positions_day_sig

    res = write_snapshot_files(
        holdings_df=holdings_df,
        positions_day_df=positions_day_df,
        funds_available=funds_available,
        ts_ist=now_ist(),
        holdings_sig=holdings_sig,
        positions_day_sig=positions_day_sig,
        holdings_changed=holdings_changed,
        positions_changed=positions_changed,
        cleanup_old=cleanup_old,
    )
    return res, holdings_sig, positions_day_sig


def sleep_until_market_open(open_time: dtime) -> None:
    now = now_ist()
    open_dt = IST.localize(datetime(now.year, now.month, now.day, open_time.hour, open_time.minute, 0))
    if now >= open_dt:
        return
    wait_sec = max(1, int((open_dt - now).total_seconds()))
    _print(f"Outside market window. Sleeping {wait_sec}s until {open_time.strftime('%H:%M')} IST.")
    time.sleep(min(wait_sec, 300))


def run_live_scheduler(
    poll_sec: int,
    holdings_poll_sec: int,
    open_time: dtime,
    close_time: dtime,
    cleanup_old: bool,
) -> int:
    poll_sec = max(20, int(poll_sec))
    holdings_poll_sec = max(poll_sec, int(holdings_poll_sec))

    _print(
        "Kite exporter live scheduler started | "
        f"poll_sec={poll_sec} | holdings_poll_sec={holdings_poll_sec} | "
        f"window={open_time.strftime('%H:%M')}-{close_time.strftime('%H:%M')} IST"
    )

    kite = setup_kite_session()

    prev_holdings_sig: Optional[str] = None
    prev_positions_sig: Optional[str] = None
    next_holdings_due = now_ist()

    while True:
        now = now_ist()

        if now.time() > close_time:
            _print("Market close window reached. Exiting scheduler.")
            return 0

        if now.time() < open_time:
            sleep_until_market_open(open_time)
            continue

        refresh_holdings = now >= next_holdings_due
        cycle_start = time.perf_counter()
        try:
            # Refresh holdings less frequently than positions to keep API load low.
            if refresh_holdings:
                res, new_hold_sig, new_pos_sig = run_once(
                    kite,
                    prev_holdings_sig=prev_holdings_sig,
                    prev_positions_day_sig=prev_positions_sig,
                    cleanup_old=cleanup_old,
                )
                next_holdings_due = now + timedelta(seconds=holdings_poll_sec)
            else:
                # Fetch positions every cycle; keep last holdings signature unchanged.
                holdings_df = fetch_holdings_df(kite) if prev_holdings_sig is None else None
                if holdings_df is not None:
                    prev_holdings_sig = _stable_signature(
                        holdings_df,
                        (
                            "exchange",
                            "tradingsymbol",
                            "quantity",
                            "t1_quantity",
                            "average_price",
                            "last_price",
                            "pnl",
                            "day_change_percentage",
                        ),
                    )

                positions_day_df = fetch_positions_day_df(kite)
                positions_sig = _stable_signature(
                    positions_day_df,
                    (
                        "exchange",
                        "tradingsymbol",
                        "product",
                        "quantity",
                        "buy_quantity",
                        "sell_quantity",
                        "average_price",
                        "last_price",
                        "pnl",
                    ),
                )

                if holdings_df is None:
                    # Reuse last holdings by reading current file to avoid extra holdings API calls.
                    holdings_today_path = OUT_DIR / HOLDINGS_TODAY_NAME
                    holdings_df = pd.read_csv(holdings_today_path) if holdings_today_path.exists() else pd.DataFrame()
                funds_available = fetch_funds_available(kite)

                res = write_snapshot_files(
                    holdings_df=holdings_df,
                    positions_day_df=positions_day_df,
                    funds_available=funds_available,
                    ts_ist=now_ist(),
                    holdings_sig=prev_holdings_sig or "empty",
                    positions_day_sig=positions_sig,
                    holdings_changed=False,
                    positions_changed=(prev_positions_sig != positions_sig),
                    cleanup_old=cleanup_old,
                )
                new_hold_sig = prev_holdings_sig or "empty"
                new_pos_sig = positions_sig

            prev_holdings_sig = new_hold_sig
            prev_positions_sig = new_pos_sig

            elapsed = time.perf_counter() - cycle_start
            _print(
                "snapshot written | "
                f"holdings_rows={res.holdings_rows} changed={res.holdings_changed} | "
                f"positions_day_rows={res.positions_day_rows} changed={res.positions_changed} | "
                f"elapsed={elapsed:.2f}s"
            )
        except Exception as exc:
            elapsed = time.perf_counter() - cycle_start
            _print(f"ERROR while fetching/writing snapshot: {exc} | elapsed={elapsed:.2f}s")

        now_after = now_ist()
        if now_after.time() > close_time:
            _print("Market close window reached after cycle. Exiting scheduler.")
            return 0

        remaining = max(5, poll_sec)
        close_dt = IST.localize(datetime(now_after.year, now_after.month, now_after.day, close_time.hour, close_time.minute, 0))
        max_sleep = int(max(1, (close_dt - now_after).total_seconds()))
        time.sleep(min(remaining, max_sleep))


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Kite holdings and day positions for dashboard")
    parser.add_argument("--live", action="store_true", help="Run continuously during market hours")
    parser.add_argument("--poll-sec", type=int, default=90, help="Positions polling interval seconds (default: 90)")
    parser.add_argument(
        "--holdings-poll-sec",
        type=int,
        default=300,
        help="Holdings polling interval seconds in live mode (default: 300)",
    )
    parser.add_argument("--open", default="09:15", help="Market open HH:MM IST (default: 09:15)")
    parser.add_argument("--close", default="15:30", help="Market close HH:MM IST (default: 15:30)")
    parser.add_argument(
        "--no-cleanup-old",
        action="store_true",
        help="Do not delete older date-suffixed export files",
    )
    args = parser.parse_args()

    open_time = parse_hhmm(args.open)
    close_time = parse_hhmm(args.close)
    cleanup_old = not bool(args.no_cleanup_old)

    try:
        if args.live:
            return run_live_scheduler(
                poll_sec=args.poll_sec,
                holdings_poll_sec=args.holdings_poll_sec,
                open_time=open_time,
                close_time=close_time,
                cleanup_old=cleanup_old,
            )

        kite = setup_kite_session()
        res, _, _ = run_once(
            kite,
            prev_holdings_sig=None,
            prev_positions_day_sig=None,
            cleanup_old=cleanup_old,
        )
        _print(
            "one-shot export complete | "
            f"holdings_rows={res.holdings_rows} | positions_day_rows={res.positions_day_rows}"
        )
        return 0
    except Exception as exc:
        _print(f"FATAL: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
