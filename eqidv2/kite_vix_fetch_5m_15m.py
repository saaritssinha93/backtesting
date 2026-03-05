# -*- coding: utf-8 -*-
"""
Fetch India VIX candles from Zerodha Kite for 5-minute and 15-minute timeframes.

Credentials expected in the same folder as this script:
  - api_key.txt / access_token.txt
  - api_key2.txt / access_token2.txt (optional)
  - api_key3.txt / access_token3.txt (optional)
  - api_key4.txt / access_token4.txt (optional)

Examples:
  python kite_vix_fetch_5m_15m.py --from-date 2025-08-25 --to-date 2026-03-05
  python kite_vix_fetch_5m_15m.py --days 30
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
import pytz

try:
    from kiteconnect import KiteConnect
except ImportError:
    print("ERROR: kiteconnect is required. Install with: pip install kiteconnect")
    sys.exit(1)


IST = pytz.timezone("Asia/Kolkata")
ROOT = Path(__file__).resolve().parent
DEFAULT_OUT_DIR = ROOT / "live_signals"
DEFAULT_OUT_DIR_5M = ROOT / "vix_indicators_5min_eq"
DEFAULT_OUT_DIR_15M = ROOT / "vix_indicators_15min_eq"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch India VIX 5m and 15m historical data from Kite")
    p.add_argument("--from-date", type=str, default=None, help="Start date in YYYY-MM-DD (IST)")
    p.add_argument("--to-date", type=str, default=None, help="End date in YYYY-MM-DD (IST)")
    p.add_argument(
        "--days",
        type=int,
        default=None,
        help="Optional lookback days if --from-date is omitted. If not set, defaults to last 6 months.",
    )
    p.add_argument(
        "--out-dir",
        type=str,
        default=None,
        help="Optional single output directory for both timeframes.",
    )
    p.add_argument(
        "--out-dir-5m",
        type=str,
        default=str(DEFAULT_OUT_DIR_5M),
        help=f"Output directory for 5-minute data (default: {DEFAULT_OUT_DIR_5M})",
    )
    p.add_argument(
        "--out-dir-15m",
        type=str,
        default=str(DEFAULT_OUT_DIR_15M),
        help=f"Output directory for 15-minute data (default: {DEFAULT_OUT_DIR_15M})",
    )
    p.add_argument(
        "--instrument-token",
        type=int,
        default=None,
        help="Optional override instrument token for India VIX.",
    )
    p.add_argument(
        "--pause-sec",
        type=float,
        default=0.35,
        help="Pause between Kite historical calls to reduce rate-limit risk.",
    )
    return p.parse_args()


def _read_first_token(path: Path) -> str:
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        raise RuntimeError(f"Empty file: {path}")
    return raw.split()[0].strip()


def setup_kite_session() -> KiteConnect:
    specs = [
        ("app1", ROOT / "api_key.txt", ROOT / "access_token.txt"),
        ("app2", ROOT / "api_key2.txt", ROOT / "access_token2.txt"),
        ("app3", ROOT / "api_key3.txt", ROOT / "access_token3.txt"),
        ("app4", ROOT / "api_key4.txt", ROOT / "access_token4.txt"),
    ]

    errors: List[str] = []
    for app_name, key_file, token_file in specs:
        if not (key_file.exists() and token_file.exists()):
            errors.append(f"{app_name}: missing {key_file.name}/{token_file.name}")
            continue
        try:
            api_key = _read_first_token(key_file)
            access_token = _read_first_token(token_file)
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            prof = kite.profile()
            user = prof.get("user_name", "N/A")
            print(f"[KITE] Session ready: {app_name} | user={user}")
            return kite
        except Exception as exc:
            errors.append(f"{app_name}: {exc}")

    msg = "No valid Kite session found.\n" + "\n".join(f"  - {x}" for x in errors)
    raise RuntimeError(msg)


def resolve_india_vix_token(kite: KiteConnect, token_override: Optional[int]) -> int:
    if token_override:
        print(f"[INFO] Using token override: {token_override}")
        return int(token_override)

    try:
        instruments = kite.instruments("NSE")
    except Exception as exc:
        raise RuntimeError(f"Failed to fetch NSE instruments: {exc}") from exc

    candidates: List[Dict] = []
    wanted_symbols = {"INDIA VIX", "INDIAVIX", "VIX"}
    for row in instruments:
        symbol = str(row.get("tradingsymbol") or "").strip().upper()
        name = str(row.get("name") or "").strip().upper()
        segment = str(row.get("segment") or "").strip().upper()
        if "VIX" in symbol or "VIX" in name:
            if symbol in wanted_symbols or name == "INDIA VIX" or segment == "INDICES":
                candidates.append(row)

    if not candidates:
        for row in instruments:
            symbol = str(row.get("tradingsymbol") or "").strip().upper()
            if symbol == "INDIA VIX":
                candidates.append(row)
                break

    if not candidates:
        raise RuntimeError("Could not resolve India VIX token from NSE instruments list.")

    preferred = sorted(
        candidates,
        key=lambda r: (
            0 if str(r.get("segment") or "").upper() == "INDICES" else 1,
            0 if str(r.get("tradingsymbol") or "").upper() == "INDIA VIX" else 1,
        ),
    )[0]

    token = int(preferred["instrument_token"])
    tsym = preferred.get("tradingsymbol")
    seg = preferred.get("segment")
    exch = preferred.get("exchange")
    print(f"[INFO] Resolved India VIX token={token} | symbol={tsym} | segment={seg} | exchange={exch}")
    return token


def _subtract_months(d: date, months: int) -> date:
    """Calendar-aware month subtraction without external deps."""
    y = d.year
    m = d.month - months
    while m <= 0:
        m += 12
        y -= 1

    # Clamp day to target month max day.
    # Move to first day of next month, then step back one day.
    if m == 12:
        next_month_first = date(y + 1, 1, 1)
    else:
        next_month_first = date(y, m + 1, 1)
    month_last_day = (next_month_first - timedelta(days=1)).day
    day = min(d.day, month_last_day)
    return date(y, m, day)


def build_range(from_date: Optional[str], to_date: Optional[str], days: Optional[int]) -> Tuple[datetime, datetime]:
    if to_date:
        end_d = datetime.strptime(to_date, "%Y-%m-%d").date()
    else:
        end_d = datetime.now(IST).date()

    if from_date:
        start_d = datetime.strptime(from_date, "%Y-%m-%d").date()
    else:
        if days is not None:
            start_d = end_d - timedelta(days=max(1, days))
        else:
            start_d = _subtract_months(end_d, 6)

    if start_d > end_d:
        raise ValueError(f"from-date {start_d} is after to-date {end_d}")

    start_dt = IST.localize(datetime.combine(start_d, dt_time(9, 15)))
    end_dt = IST.localize(datetime.combine(end_d, dt_time(15, 30)))
    return start_dt, end_dt


def fetch_historical_chunked(
    kite: KiteConnect,
    token: int,
    interval: str,
    start_dt: datetime,
    end_dt: datetime,
    pause_sec: float,
) -> pd.DataFrame:
    max_days_per_call = 60
    rows: List[Dict] = []
    chunk_start = start_dt

    while chunk_start <= end_dt:
        chunk_end = min(chunk_start + timedelta(days=max_days_per_call) - timedelta(seconds=1), end_dt)
        print(
            f"[FETCH] interval={interval} | "
            f"{chunk_start.strftime('%Y-%m-%d %H:%M:%S')} -> {chunk_end.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        data = kite.historical_data(
            instrument_token=token,
            from_date=chunk_start,
            to_date=chunk_end,
            interval=interval,
            oi=False,
        )
        rows.extend(data or [])
        chunk_start = chunk_end + timedelta(seconds=1)
        if pause_sec > 0:
            time.sleep(pause_sec)

    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows)
    if "date" not in df.columns:
        raise RuntimeError(f"Kite response missing 'date' for interval={interval}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).copy()
    if df["date"].dt.tz is None:
        df["date"] = df["date"].dt.tz_localize(IST)
    else:
        df["date"] = df["date"].dt.tz_convert(IST)

    keep_cols = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep_cols].drop_duplicates(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


def main() -> int:
    args = parse_args()
    if args.out_dir:
        out_dir_5m = Path(args.out_dir).expanduser().resolve()
        out_dir_15m = Path(args.out_dir).expanduser().resolve()
    else:
        out_dir_5m = Path(args.out_dir_5m).expanduser().resolve()
        out_dir_15m = Path(args.out_dir_15m).expanduser().resolve()

    out_dir_5m.mkdir(parents=True, exist_ok=True)
    out_dir_15m.mkdir(parents=True, exist_ok=True)

    start_dt, end_dt = build_range(args.from_date, args.to_date, args.days)
    print(f"[INFO] Range (IST): {start_dt} -> {end_dt}")

    kite = setup_kite_session()
    token = resolve_india_vix_token(kite, args.instrument_token)

    intervals: Sequence[str] = ("5minute", "15minute")
    stamp = f"{start_dt.date()}_to_{end_dt.date()}"

    for interval in intervals:
        try:
            df = fetch_historical_chunked(
                kite=kite,
                token=token,
                interval=interval,
                start_dt=start_dt,
                end_dt=end_dt,
                pause_sec=args.pause_sec,
            )
        except Exception as exc:
            print(f"[ERROR] interval={interval} fetch failed: {exc}")
            return 1

        if interval == "5minute":
            out_csv = out_dir_5m / f"india_vix_{interval}_{stamp}.csv"
        else:
            out_csv = out_dir_15m / f"india_vix_{interval}_{stamp}.csv"
        df.to_csv(out_csv, index=False)

        if not df.empty:
            print(
                f"[OK] {interval}: rows={len(df)} | "
                f"{df['date'].iloc[0]} -> {df['date'].iloc[-1]} | saved={out_csv}"
            )
        else:
            print(f"[WARN] {interval}: no rows returned | saved empty file={out_csv}")

    print("[DONE] India VIX 5m/15m fetch completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
