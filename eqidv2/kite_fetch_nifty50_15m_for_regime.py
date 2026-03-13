# -*- coding: utf-8 -*-
"""
Fetch/build NIFTY 50 15-minute parquet for V7 market regime filter.

Why this exists:
    avwap_common_v7_sweep.build_market_regime_map() auto-enables only when
    at least one index parquet exists in stocks_indicators_15min_eq with one
    of these names:
      NIFTY50_stocks_indicators_15min.parquet
      NIFTY_50_stocks_indicators_15min.parquet
      NIFTY_stocks_indicators_15min.parquet
      NIFTYBEES_stocks_indicators_15min.parquet

This utility fetches index candles from Kite and writes the expected files.

Examples:
    python kite_fetch_nifty50_15m_for_regime.py
    python kite_fetch_nifty50_15m_for_regime.py --from-date 2025-08-25 --to-date 2026-03-05
    python kite_fetch_nifty50_15m_for_regime.py --token-override 256265
"""

from __future__ import annotations

import argparse
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
    raise SystemExit(1)


IST = pytz.timezone("Asia/Kolkata")
ROOT = Path(__file__).resolve().parent
DEFAULT_OUT_DIR = ROOT / "stocks_indicators_15min_eq"
OUT_SUFFIX = "_stocks_indicators_15min.parquet"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch NIFTY 50 15-minute candles and save parquet for regime map."
    )
    p.add_argument("--from-date", type=str, default="2025-08-25", help="YYYY-MM-DD (IST)")
    p.add_argument("--to-date", type=str, default=None, help="YYYY-MM-DD (IST). Default=today")
    p.add_argument(
        "--out-dir",
        type=str,
        default=str(DEFAULT_OUT_DIR),
        help=f"Output directory (default: {DEFAULT_OUT_DIR})",
    )
    p.add_argument(
        "--aliases",
        type=str,
        default="NIFTY50,NIFTY_50",
        help="Comma-separated output ticker aliases.",
    )
    p.add_argument(
        "--symbol",
        type=str,
        default="NIFTY 50",
        help="Preferred index symbol/name to resolve from instruments.",
    )
    p.add_argument("--token-override", type=int, default=None, help="Manual instrument token.")
    p.add_argument("--chunk-days", type=int, default=60, help="Kite historical chunk size.")
    p.add_argument("--pause-sec", type=float, default=0.30, help="Pause between API calls.")
    return p.parse_args()


def _read_first_token(path: Path) -> str:
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        raise RuntimeError(f"Empty credential file: {path}")
    return raw.split()[0].strip()


def load_kite_pool() -> List[Tuple[str, KiteConnect]]:
    specs = [
        ("app1", ROOT / "api_key.txt", ROOT / "access_token.txt"),
        ("app2", ROOT / "api_key2.txt", ROOT / "access_token2.txt"),
        ("app3", ROOT / "api_key3.txt", ROOT / "access_token3.txt"),
        ("app4", ROOT / "api_key4.txt", ROOT / "access_token4.txt"),
    ]
    pool: List[Tuple[str, KiteConnect]] = []
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
            user = kite.profile().get("user_name", "N/A")
            print(f"[KITE] Session ready: {app_name} | user={user}")
            pool.append((app_name, kite))
        except Exception as exc:
            errors.append(f"{app_name}: {exc}")
    if not pool:
        msg = "No valid Kite session available.\n" + "\n".join(f"  - {e}" for e in errors)
        raise RuntimeError(msg)
    return pool


def _norm(s: str) -> str:
    return "".join(ch for ch in str(s).upper() if ch.isalnum())


def resolve_index_token(
    kite: KiteConnect, preferred_symbol: str, token_override: Optional[int]
) -> int:
    if token_override is not None and int(token_override) > 0:
        print(f"[INFO] Using token override: {int(token_override)}")
        return int(token_override)

    pref = _norm(preferred_symbol)
    wanted = {_norm("NIFTY 50"), _norm("NIFTY50"), _norm("NIFTY"), pref}

    instruments = kite.instruments("NSE")
    candidates: List[Dict] = []
    for row in instruments:
        tsym = str(row.get("tradingsymbol") or "")
        name = str(row.get("name") or "")
        seg = str(row.get("segment") or "").upper()
        exch = str(row.get("exchange") or "").upper()
        tsym_n = _norm(tsym)
        name_n = _norm(name)

        is_index = ("INDICES" in seg) or ("INDEX" in seg) or (exch == "NSE")
        if not is_index:
            continue

        if tsym_n in wanted or name_n in wanted:
            candidates.append(row)
            continue
        if "NIFTY" in tsym_n and ("50" in tsym_n or tsym_n == "NIFTY"):
            candidates.append(row)
            continue
        if "NIFTY" in name_n and ("50" in name_n or name_n == "NIFTY"):
            candidates.append(row)

    if not candidates:
        raise RuntimeError("Could not resolve NIFTY index token from Kite instruments.")

    def _score(row: Dict) -> Tuple[int, int, int]:
        tsym_n = _norm(str(row.get("tradingsymbol") or ""))
        name_n = _norm(str(row.get("name") or ""))
        seg = str(row.get("segment") or "").upper()
        s_pref_tsym = 0 if tsym_n == pref else 1
        s_pref_name = 0 if name_n == pref else 1
        s_nifty50_tsym = 0 if tsym_n == _norm("NIFTY50") else 1
        s_nifty50_name = 0 if name_n == _norm("NIFTY 50") else 1
        s_indices = 0 if "INDICES" in seg else 1
        return (s_pref_tsym, s_pref_name, s_nifty50_tsym, s_nifty50_name, s_indices)

    pick = sorted(candidates, key=_score)[0]
    token = int(pick["instrument_token"])
    print(
        "[INFO] Resolved token="
        f"{token} | tradingsymbol={pick.get('tradingsymbol')} | "
        f"name={pick.get('name')} | segment={pick.get('segment')}"
    )
    return token


def build_range(from_date: str, to_date: Optional[str]) -> Tuple[datetime, datetime]:
    start_d = datetime.strptime(from_date, "%Y-%m-%d").date()
    end_d = datetime.strptime(to_date, "%Y-%m-%d").date() if to_date else datetime.now(IST).date()
    if start_d > end_d:
        raise ValueError(f"from-date {start_d} is after to-date {end_d}")
    start_dt = IST.localize(datetime.combine(start_d, dt_time(9, 15)))
    end_dt = IST.localize(datetime.combine(end_d, dt_time(15, 30)))
    return start_dt, end_dt


def fetch_historical_chunked(
    kite_pool: Sequence[Tuple[str, KiteConnect]],
    token: int,
    start_dt: datetime,
    end_dt: datetime,
    chunk_days: int,
    pause_sec: float,
) -> pd.DataFrame:
    rows: List[Dict] = []
    chunk_start = start_dt
    app_idx = 0
    chunk_days = max(1, int(chunk_days))

    while chunk_start <= end_dt:
        chunk_end = min(
            chunk_start + timedelta(days=chunk_days) - timedelta(seconds=1),
            end_dt,
        )
        ok = False
        tries = max(1, len(kite_pool) * 2)
        for _ in range(tries):
            app_name, kite = kite_pool[app_idx % len(kite_pool)]
            app_idx += 1
            try:
                print(
                    f"[FETCH] app={app_name} | {chunk_start:%Y-%m-%d %H:%M:%S} "
                    f"-> {chunk_end:%Y-%m-%d %H:%M:%S}"
                )
                data = kite.historical_data(
                    instrument_token=int(token),
                    from_date=chunk_start,
                    to_date=chunk_end,
                    interval="15minute",
                    oi=False,
                )
                rows.extend(data or [])
                ok = True
                break
            except Exception as exc:
                msg = str(exc)
                print(f"[WARN] app={app_name} historical fetch failed: {msg}")
                if "Too many requests" in msg or "429" in msg:
                    time.sleep(max(0.2, pause_sec))
                    continue
                time.sleep(max(0.2, pause_sec))
                continue

        if not ok:
            raise RuntimeError(
                f"Failed to fetch chunk {chunk_start:%Y-%m-%d %H:%M:%S} -> "
                f"{chunk_end:%Y-%m-%d %H:%M:%S} across available Kite apps."
            )

        chunk_start = chunk_end + timedelta(seconds=1)
        if pause_sec > 0:
            time.sleep(pause_sec)

    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows)
    if "date" not in df.columns:
        raise RuntimeError("Kite historical response missing 'date' column")

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
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    aliases = [a.strip().upper() for a in str(args.aliases).split(",") if a.strip()]
    if not aliases:
        aliases = ["NIFTY50"]

    start_dt, end_dt = build_range(args.from_date, args.to_date)
    print(f"[INFO] Range (IST): {start_dt} -> {end_dt}")

    pool = load_kite_pool()
    token = resolve_index_token(pool[0][1], args.symbol, args.token_override)
    df = fetch_historical_chunked(
        kite_pool=pool,
        token=token,
        start_dt=start_dt,
        end_dt=end_dt,
        chunk_days=int(args.chunk_days),
        pause_sec=float(args.pause_sec),
    )
    if df.empty:
        print("[WARN] No rows returned from Kite.")
        return 2

    for alias in aliases:
        out_path = out_dir / f"{alias}{OUT_SUFFIX}"
        df.to_parquet(out_path, index=False)
        print(
            f"[OK] wrote {alias}: rows={len(df)} | "
            f"{df['date'].iloc[0]} -> {df['date'].iloc[-1]} | {out_path}"
        )

    print("[DONE] Regime index parquet build completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
