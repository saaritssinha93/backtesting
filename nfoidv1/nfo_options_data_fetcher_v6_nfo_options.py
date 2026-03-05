# -*- coding: utf-8 -*-
"""
nfo_options_data_fetcher_v6_nfo_options.py
==========================================

Builds ATM NFO option contracts from AVWAP v4 stock entries and fetches
5-minute option candles for backtesting.

What this script does:
1. Loads v4 stock trade CSV (or latest from outputs_v4/).
2. Resolves ATM option contract per trade:
   - LONG stock trade -> CE option (buy)
   - SHORT stock trade -> PE option (buy)
   - nearest expiry >= trade date
   - strike nearest to underlying entry price
3. Fetches 5-minute historical option candles via Kite Connect.
4. Saves:
   - option parquet files (one per contract)
   - trade->option mapping CSV (for v6 option backtest)
   - skip report for unresolved trades
   - contract fetch status CSV

Usage example:
    python nfo_options_data_fetcher_v6_nfo_options.py \
        --options-data-dir "D:\\nfo_options_5min"
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time as time_mod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from avwap_v11_refactored.avwap_common import IST


THIS_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = THIS_DIR / "options_data_v6_nfo_options"
DEFAULT_MAP_CSV = "nfo_options_trade_map_v6.csv"
DEFAULT_SKIPS_CSV = "nfo_options_trade_map_skips_v6.csv"
DEFAULT_FETCH_STATUS_CSV = "nfo_options_contract_fetch_status_v6.csv"


class AuthError(RuntimeError):
    """Raised when Kite auth is invalid/expired during API calls."""


@dataclass
class AppCredential:
    name: str
    api_key_file: Path
    access_token_file: Path


@dataclass
class AppClient:
    name: str
    user_id: str
    kite: Any
    api_key_file: Path
    access_token_file: Path


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
        description="Fetch ATM NFO option 5-min candles aligned to AVWAP v4 trades."
    )
    p.add_argument(
        "--v4-trades-csv",
        default=None,
        help="Path to v4 trades CSV. If omitted, latest outputs_v4 avwap_longshort_trades_ALL_DAYS_v4_*.csv is used.",
    )
    p.add_argument(
        "--options-data-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory to store option parquet + mapping files.",
    )
    p.add_argument(
        "--api-key-file",
        default="api_key.txt",
        help="Path to API key file.",
    )
    p.add_argument(
        "--access-token-file",
        default="access_token.txt",
        help="Path to access token file.",
    )
    p.add_argument(
        "--single-app-only",
        action="store_true",
        help="Use only --api-key-file/--access-token-file instead of auto app1..app4 fanout.",
    )
    p.add_argument(
        "--max-days-to-expiry",
        type=int,
        default=62,
        help=(
            "Skip mapping if nearest option expiry is farther than this many days from trade date. "
            "Use 0 to disable."
        ),
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
    p.add_argument(
        "--historical-instruments-dir",
        default=None,
        help=(
            "Optional directory containing dated NFO instrument snapshots "
            "(csv/parquet, filename must include YYYYMMDD)."
        ),
    )
    p.add_argument(
        "--snapshot-match-mode",
        choices=["previous", "exact"],
        default="previous",
        help="How to select snapshot for a trade date when historical snapshots are provided.",
    )
    p.add_argument(
        "--snapshot-fallback-current-master",
        action="store_true",
        help="Fallback to current instrument master if matching historical snapshot is unavailable.",
    )
    p.add_argument(
        "--interval",
        default="5minute",
        choices=["5minute"],
        help="Kite historical interval.",
    )
    p.add_argument(
        "--sleep-sec",
        type=float,
        default=0.15,
        help="Sleep between historical API requests to reduce throttling.",
    )
    p.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Retries per contract on historical fetch errors.",
    )
    return p.parse_args()


def _parse_date_arg(raw: Optional[str], arg_name: str) -> Optional[datetime.date]:
    if raw is None or str(raw).strip() == "":
        return None
    ts = pd.to_datetime(raw, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"Invalid {arg_name}: {raw}. Expected YYYY-MM-DD.")
    return ts.date()


def _filter_trades_by_date(
    trades: pd.DataFrame,
    from_date: Optional[datetime.date],
    to_date: Optional[datetime.date],
    recent_months: int,
) -> pd.DataFrame:
    if trades.empty:
        return trades

    d = trades.copy()
    d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce").dt.date

    if recent_months and recent_months > 0:
        max_dt = max(d["trade_date"])
        start_dt = (pd.Timestamp(max_dt) - pd.DateOffset(months=int(recent_months))).date()
        d = d[d["trade_date"] >= start_dt].copy()

    if from_date is not None:
        d = d[d["trade_date"] >= from_date].copy()
    if to_date is not None:
        d = d[d["trade_date"] <= to_date].copy()

    return d.reset_index(drop=True)


def _pick_latest_v4_csv(base_dir: Path) -> Path:
    files = sorted(
        (base_dir / "outputs_v4").glob("avwap_longshort_trades_ALL_DAYS_v4_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(
            "No v4 trade CSV found in outputs_v4. Pass --v4-trades-csv explicitly."
        )
    return files[0]


def _read_single_value(path: Path) -> str:
    text = path.read_text(encoding="utf-8", errors="ignore").strip()
    if not text:
        raise ValueError(f"File is empty: {path}")
    if "=" in text:
        # Supports KEY=VALUE format.
        for line in text.splitlines():
            line = line.strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k.strip().upper() == "API_KEY":
                return v.strip()
    return text.splitlines()[0].strip().split()[0]


def _build_kite_client(api_key_file: Path, access_token_file: Path):
    try:
        from kiteconnect import KiteConnect
    except Exception as exc:
        raise RuntimeError(
            "kiteconnect is required for option data fetch. Install with: pip install kiteconnect"
        ) from exc

    api_key = _read_single_value(api_key_file)
    access_token = _read_single_value(access_token_file)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def _discover_app_credentials(
    base_dir: Path,
    primary_api_key_file: Path,
    primary_access_token_file: Path,
    single_app_only: bool,
) -> List[AppCredential]:
    primary_api = primary_api_key_file if primary_api_key_file.is_absolute() else (base_dir / primary_api_key_file)
    primary_tok = (
        primary_access_token_file
        if primary_access_token_file.is_absolute()
        else (base_dir / primary_access_token_file)
    )

    candidates: List[Tuple[str, Path, Path]] = [("app1", primary_api, primary_tok)]
    if not single_app_only:
        candidates.extend(
            [
                ("app2", base_dir / "api_key2.txt", base_dir / "access_token2.txt"),
                ("app3", base_dir / "api_key3.txt", base_dir / "access_token3.txt"),
                ("app4", base_dir / "api_key4.txt", base_dir / "access_token4.txt"),
            ]
        )

    out: List[AppCredential] = []
    seen: set[tuple[str, str]] = set()
    for name, ak, at in candidates:
        key = (str(ak.resolve()), str(at.resolve()))
        if key in seen:
            continue
        seen.add(key)
        if ak.exists() and at.exists():
            out.append(AppCredential(name=name, api_key_file=ak, access_token_file=at))
    return out


def _is_auth_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return ("incorrect `api_key` or `access_token`" in msg) or ("token is invalid or has expired" in msg)


def _validate_kite_session(kite, api_key_file: Path, access_token_file: Path) -> str:
    try:
        profile = kite.profile()
        user_id = profile.get("user_id") if isinstance(profile, dict) else ""
        if user_id:
            print(f"[INFO] Kite auth verified for user: {user_id}")
        else:
            print("[INFO] Kite auth verified.")
        return str(user_id or "")
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "Kite authentication failed. Regenerate request/access token and ensure matching api key.\n"
            f"api_key_file={api_key_file}\naccess_token_file={access_token_file}\n"
            f"original_error={exc}"
        ) from exc


def _normalize_symbol(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value).upper())


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


def _load_v4_trades(v4_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(v4_csv)
    required = {"trade_date", "ticker", "side", "entry_time_ist", "entry_price"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"v4 CSV missing required columns: {missing}")

    d = df.copy()
    d["ticker"] = d["ticker"].astype(str).str.upper().str.strip()
    d["side"] = d["side"].astype(str).str.upper().str.strip()
    for c in ["signal_time_ist", "entry_time_ist", "exit_time_ist"]:
        if c in d.columns:
            d[c] = _to_ist_series(d[c])
    d["trade_date"] = pd.to_datetime(d["trade_date"], errors="coerce").dt.date
    d["entry_price"] = pd.to_numeric(d["entry_price"], errors="coerce")
    d = d.dropna(subset=["trade_date", "ticker", "side", "entry_time_ist", "entry_price"]).copy()
    d["trade_uid"] = d.apply(_make_trade_uid, axis=1)
    return d.reset_index(drop=True)


def _normalize_nfo_option_instruments_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    d = df.copy()
    d["segment"] = d.get("segment", "").astype(str)
    d["instrument_type"] = d.get("instrument_type", "").astype(str).str.upper()
    d = d[d["segment"].str.contains("NFO-OPT", na=False) | d["instrument_type"].isin(["CE", "PE"])].copy()
    d["name"] = d.get("name", "").astype(str).str.upper().str.strip()
    d["name_norm"] = d["name"].map(_normalize_symbol)
    d["tradingsymbol"] = d.get("tradingsymbol", "").astype(str).str.upper().str.strip()
    d["tradingsymbol_norm"] = d["tradingsymbol"].map(_normalize_symbol)
    d["expiry"] = pd.to_datetime(d.get("expiry"), errors="coerce").dt.date
    d["strike"] = pd.to_numeric(d.get("strike"), errors="coerce")
    d["instrument_token"] = pd.to_numeric(d.get("instrument_token"), errors="coerce").astype("Int64")
    d["lot_size"] = pd.to_numeric(d.get("lot_size"), errors="coerce").fillna(0).astype(int)
    d = d.dropna(subset=["expiry", "strike", "instrument_token"]).copy()
    return d.reset_index(drop=True)


def _load_nfo_option_instruments(kite) -> pd.DataFrame:
    raw = kite.instruments("NFO")
    df = pd.DataFrame(raw)
    return _normalize_nfo_option_instruments_df(df)


def _extract_snapshot_date_from_name(name: str) -> Optional[datetime.date]:
    m = re.search(r"(20\d{6})", name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d").date()
    except Exception:
        return None


def _discover_snapshot_files(hist_dir: Path) -> Dict[datetime.date, Path]:
    snapshots: Dict[datetime.date, Path] = {}
    for p in sorted(hist_dir.glob("*")):
        if not p.is_file():
            continue
        if p.suffix.lower() not in {".csv", ".parquet"}:
            continue
        dt = _extract_snapshot_date_from_name(p.name)
        if dt is None:
            continue
        snapshots[dt] = p
    return snapshots


def _load_snapshot_instruments(snapshot_file: Path) -> pd.DataFrame:
    if snapshot_file.suffix.lower() == ".parquet":
        raw = pd.read_parquet(snapshot_file)
    else:
        raw = pd.read_csv(snapshot_file)
    return _normalize_nfo_option_instruments_df(raw)


def _pick_snapshot_date_for_trade(
    trade_date: datetime.date,
    snapshot_dates: List[datetime.date],
    mode: str,
) -> Optional[datetime.date]:
    if not snapshot_dates:
        return None
    if mode == "exact":
        return trade_date if trade_date in set(snapshot_dates) else None
    eligible = [d for d in snapshot_dates if d <= trade_date]
    if not eligible:
        return None
    return max(eligible)


@dataclass
class ResolveResult:
    mapping: Optional[dict]
    skip_reason: Optional[str]


def _resolve_atm_contract_for_trade(
    tr: pd.Series,
    inst_df: pd.DataFrame,
    max_days_to_expiry: Optional[int] = None,
) -> ResolveResult:
    ticker = str(tr["ticker"]).upper()
    ticker_norm = _normalize_symbol(ticker)
    trade_date = tr["trade_date"]
    side = str(tr["side"]).upper()
    entry_px = float(tr["entry_price"])
    opt_type = "CE" if side == "LONG" else "PE"

    cands = inst_df[inst_df["instrument_type"].eq(opt_type)].copy()
    if cands.empty:
        return ResolveResult(None, "no_options_instrument_master")

    # Primary match: name matches ticker.
    by_name = cands[cands["name_norm"].eq(ticker_norm)]
    if by_name.empty:
        # Fallback: tradingsymbol starts with normalized ticker text.
        by_name = cands[cands["tradingsymbol_norm"].str.startswith(ticker_norm, na=False)]
    if by_name.empty:
        return ResolveResult(None, "underlying_not_found_in_option_master")

    by_exp = by_name[by_name["expiry"] >= trade_date]
    if by_exp.empty:
        return ResolveResult(None, "no_future_expiry_for_trade_date")

    nearest_expiry = min(by_exp["expiry"])
    near = by_exp[by_exp["expiry"].eq(nearest_expiry)].copy()
    if near.empty:
        return ResolveResult(None, "nearest_expiry_empty")

    near["strike_dist"] = (near["strike"] - entry_px).abs()
    sel = near.sort_values(["strike_dist", "strike"]).iloc[0]

    instrument_token = int(sel["instrument_token"])
    tradingsymbol = str(sel["tradingsymbol"])
    lot_size = int(sel["lot_size"]) if int(sel["lot_size"]) > 0 else 1
    strike = float(sel["strike"])
    expiry = sel["expiry"]
    days_to_expiry = int((expiry - trade_date).days)
    if max_days_to_expiry is not None and max_days_to_expiry > 0 and days_to_expiry > max_days_to_expiry:
        return ResolveResult(None, f"days_to_expiry_gt_max:{days_to_expiry}>{max_days_to_expiry}")
    file_name = f"{tradingsymbol}_{instrument_token}_5min.parquet"

    return ResolveResult(
        mapping={
            "trade_uid": tr["trade_uid"],
            "trade_date": tr["trade_date"],
            "ticker": ticker,
            "side": side,
            "entry_time_ist": tr["entry_time_ist"],
            "underlying_entry_price": entry_px,
            "option_type": opt_type,
            "expiry": expiry,
            "days_to_expiry": days_to_expiry,
            "strike": strike,
            "lot_size": lot_size,
            "instrument_token": instrument_token,
            "tradingsymbol": tradingsymbol,
            "option_file": file_name,
        },
        skip_reason=None,
    )


def _fetch_contract_5min(
    kite,
    instrument_token: int,
    from_dt: datetime,
    to_dt: datetime,
    interval: str,
    max_retries: int,
):
    # Kite minute-level historical endpoints have a date-span limit.
    # Pull in <=55-day chunks and merge.
    def _chunk_ranges(start_dt: datetime, end_dt: datetime, chunk_days: int = 55):
        cur = start_dt
        while cur <= end_dt:
            chunk_end = min(cur + pd.Timedelta(days=chunk_days), end_dt)
            yield cur, chunk_end
            cur = chunk_end + pd.Timedelta(minutes=5)

    all_rows: List[dict] = []
    last_err: Optional[Exception] = None
    for c_from, c_to in _chunk_ranges(from_dt, to_dt):
        chunk_ok = False
        for _ in range(max(1, max_retries)):
            try:
                rows = kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=c_from,
                    to_date=c_to,
                    interval=interval,
                    continuous=False,
                    oi=True,
                )
                if rows:
                    all_rows.extend(rows)
                chunk_ok = True
                break
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                if _is_auth_error(exc):
                    raise AuthError(str(exc)) from exc
                time_mod.sleep(0.8)
        if not chunk_ok:
            raise RuntimeError(
                f"historical_data failed for token={instrument_token} "
                f"chunk={c_from}..{c_to}: {last_err}"
            )
    return all_rows


def _to_ist_timestamp(value: object) -> pd.Timestamp:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return pd.NaT
    if ts.tzinfo is None:
        return ts.tz_localize(IST)
    return ts.tz_convert(IST)


def _fetch_contracts_for_app(
    app: AppClient,
    contracts: List[dict],
    options_dir: Path,
    interval: str,
    max_retries: int,
    sleep_sec: float,
) -> tuple[List[dict], bool]:
    statuses: List[dict] = []
    auth_failed = False
    total = len(contracts)
    for i, row in enumerate(contracts, 1):
        token = int(row["instrument_token"])
        tradingsymbol = str(row["tradingsymbol"])
        file_name = str(row["option_file"])
        from_dt = IST.localize(datetime.combine(row["min_trade_date"], time(9, 15)))
        to_dt = IST.localize(datetime.combine(row["max_trade_date"], time(15, 30)))

        out_path = options_dir / file_name
        status = {
            "app_name": app.name,
            "user_id": app.user_id,
            "instrument_token": token,
            "tradingsymbol": tradingsymbol,
            "option_file": file_name,
            "from_dt": from_dt,
            "to_dt": to_dt,
            "rows": 0,
            "status": "UNKNOWN",
            "error": "",
        }
        try:
            hist = _fetch_contract_5min(
                kite=app.kite,
                instrument_token=token,
                from_dt=from_dt,
                to_dt=to_dt,
                interval=interval,
                max_retries=max_retries,
            )
            df = pd.DataFrame(hist)
            if df.empty:
                status["status"] = "EMPTY"
            else:
                if "date" in df.columns:
                    df = df.rename(columns={"date": "datetime"})
                if "datetime" not in df.columns:
                    raise RuntimeError("historical payload missing datetime/date field")

                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").map(_to_ist_timestamp)
                keep = [c for c in ["datetime", "open", "high", "low", "close", "volume", "oi"] if c in df.columns]
                df = df[keep].dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
                for c in ["open", "high", "low", "close", "volume", "oi"]:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce")

                df.to_parquet(out_path, index=False)
                status["rows"] = int(len(df))
                status["status"] = "OK"
            time_mod.sleep(max(0.0, float(sleep_sec)))
        except AuthError as exc:
            status["status"] = "ERROR"
            status["error"] = str(exc)
            auth_failed = True
            statuses.append(status)
            print(f"[FATAL][{app.name}] Auth failed during fetch. Stopping this app worker.")
            break
        except Exception as exc:  # noqa: BLE001
            status["status"] = "ERROR"
            status["error"] = str(exc)

        statuses.append(status)
        if i % 20 == 0 or i == total:
            print(f"[INFO][{app.name}] fetched {i}/{total} assigned contracts")
    return statuses, auth_failed


def _print_fetch_diagnostics(fetch_df: pd.DataFrame, map_df: pd.DataFrame) -> None:
    if fetch_df.empty:
        print("[INFO] fetch diagnostics: no contract fetch rows available.")
        return

    total = int(len(fetch_df))
    vc = fetch_df["status"].astype(str).value_counts(dropna=False)
    print("[INFO] fetch status distribution:")
    for k in ["OK", "EMPTY", "ERROR"]:
        cnt = int(vc.get(k, 0))
        print(f"  - {k:<5}: {cnt:>5} ({(cnt / max(1, total) * 100.0):6.2f}%)")

    if "rows" in fetch_df.columns:
        ok_rows = fetch_df.loc[fetch_df["status"].eq("OK"), "rows"]
        if not ok_rows.empty:
            ok_rows = pd.to_numeric(ok_rows, errors="coerce").dropna()
            if not ok_rows.empty:
                print(
                    "[INFO] fetched-candle rows stats (OK only): "
                    f"min={int(ok_rows.min())} | p50={float(ok_rows.median()):.1f} | "
                    f"p90={float(ok_rows.quantile(0.9)):.1f} | max={int(ok_rows.max())}"
                )

    err_df = fetch_df[fetch_df["status"].eq("ERROR")].copy()
    if not err_df.empty and "error" in err_df.columns:
        print("[INFO] top fetch error messages:")
        err_vc = err_df["error"].astype(str).value_counts(dropna=False).head(5)
        for msg, cnt in err_vc.items():
            print(f"  - {msg}: {int(cnt)}")

    empty_df = fetch_df[fetch_df["status"].eq("EMPTY")].copy()
    if empty_df.empty:
        return

    print("[WARN] EMPTY contract diagnostics:")
    if {"from_dt", "to_dt"}.issubset(empty_df.columns):
        e_from = pd.to_datetime(empty_df["from_dt"], errors="coerce").dropna()
        e_to = pd.to_datetime(empty_df["to_dt"], errors="coerce").dropna()
        if not e_from.empty and not e_to.empty:
            print(
                f"  - empty fetch window range: {e_from.min().date()}..{e_to.max().date()} "
                f"across {len(empty_df)} contracts"
            )
            span_days = (e_to - e_from).dt.days
            span_days = pd.to_numeric(span_days, errors="coerce").dropna()
            if not span_days.empty:
                print(
                    "  - empty fetch span-days stats: "
                    f"min={int(span_days.min())} | p50={float(span_days.median()):.1f} | "
                    f"p90={float(span_days.quantile(0.9)):.1f} | max={int(span_days.max())}"
                )

    if "tradingsymbol" in empty_df.columns:
        vc_sym = empty_df["tradingsymbol"].astype(str).value_counts(dropna=False).head(10)
        print("  - top empty tradingsymbols:")
        for sym, cnt in vc_sym.items():
            print(f"    {sym}: {int(cnt)}")

    if (
        (not map_df.empty)
        and ("option_file" in map_df.columns)
        and ("days_to_expiry" in map_df.columns)
        and ("option_file" in empty_df.columns)
    ):
        dte_by_contract = (
            map_df[["option_file", "days_to_expiry"]]
            .copy()
            .assign(days_to_expiry=lambda d: pd.to_numeric(d["days_to_expiry"], errors="coerce"))
            .dropna(subset=["days_to_expiry"])
            .groupby("option_file", as_index=False)["days_to_expiry"]
            .median()
        )
        if not dte_by_contract.empty:
            empty_dte = empty_df[["option_file"]].drop_duplicates().merge(
                dte_by_contract, on="option_file", how="left"
            )["days_to_expiry"].dropna()
            ok_dte = (
                fetch_df[fetch_df["status"].eq("OK")][["option_file"]]
                .drop_duplicates()
                .merge(dte_by_contract, on="option_file", how="left")["days_to_expiry"]
                .dropna()
            )
            if not empty_dte.empty:
                print(
                    "  - median DTE of EMPTY contracts: "
                    f"{float(empty_dte.median()):.1f} (p90={float(empty_dte.quantile(0.9)):.1f})"
                )
            if not ok_dte.empty:
                print(
                    "  - median DTE of OK contracts   : "
                    f"{float(ok_dte.median()):.1f} (p90={float(ok_dte.quantile(0.9)):.1f})"
                )


def main() -> None:
    _configure_console_encoding()
    args = _parse_args()

    options_dir = Path(args.options_data_dir).resolve()
    options_dir.mkdir(parents=True, exist_ok=True)

    v4_csv = Path(args.v4_trades_csv).resolve() if args.v4_trades_csv else _pick_latest_v4_csv(THIS_DIR)
    if not v4_csv.exists():
        raise FileNotFoundError(f"v4 trades CSV not found: {v4_csv}")

    print("=" * 78)
    print("NFO OPTIONS DATA FETCHER v6 - ATM contract resolver + 5-min candle fetcher")
    print("=" * 78)
    print()
    print("WHAT THIS SCRIPT DOES:")
    print("  1. Loads v4 stock trade CSV (LONG -> buy CE, SHORT -> buy PE).")
    print("  2. Resolves the ATM option contract for each trade using NFO instrument master.")
    print("  3. Fetches 5-min historical candles for each unique contract via Kite Connect.")
    print("  4. Saves: option parquet files + trade->option map CSV + skip report.")
    print()
    print("DATA ACCURACY WARNING - READ BEFORE RUNNING:")
    print("  By default, Zerodha's NFO instrument master (kite.instruments('NFO')) only")
    print("  lists CURRENTLY ACTIVE contracts. If your v4 CSV spans many months or years,")
    print("  old expiry contracts will NOT appear in the live master. The fetcher will then")
    print("  map each historical trade to the NEAREST AVAILABLE expiry (which may be months")
    print("  in the future relative to the original trade date). This inflates days-to-expiry")
    print("  (DTE), makes the ATM strike incorrect, and produces garbage option P&L results.")
    print()
    print("  FIX 1 (immediate): --max-days-to-expiry 62 (the current default).")
    print("    Any trade whose nearest available expiry is >62 days away is SKIPPED.")
    print("    This avoids silently using wrong far-dated contracts as proxies.")
    print("    Trades skipped by this filter appear in the _skips CSV with reason")
    print("    'days_to_expiry_gt_max:X>62'. This is SAFE and PREFERABLE to bad data.")
    print()
    print("  FIX 2 (full coverage): Provide --historical-instruments-dir with dated snapshots.")
    print("    Run nfo_instruments_snapshot_builder_v6_nfo_options.py EVERY trading day")
    print("    to build a dated archive (nfo_instruments_YYYYMMDD.csv/parquet).")
    print("    Then pass --historical-instruments-dir <snapshot_dir> to use the correct")
    print("    instrument master for each trade's date. This gives true ATM strikes + expiries.")
    print()
    print("  FIX 3 (quick workaround): Use --recent-months N to limit trades to only the")
    print("    last N months. Recent trades are more likely to resolve against live master")
    print("    since current expiries exist for them. Recommended: --recent-months 3.")
    print()
    print("-" * 78)
    print(f"[INFO] v4 trades CSV         : {v4_csv}")
    print(f"[INFO] options data dir      : {options_dir}")
    if int(args.max_days_to_expiry) > 0:
        print(f"[INFO] max-days-to-expiry    : {int(args.max_days_to_expiry)}  <- skips bad far-dated mappings")
    else:
        print("[WARN] max-days-to-expiry    : DISABLED - bad far-dated mappings will NOT be filtered!")
    print(
        f"[INFO] trade-date filters    : "
        f"from={args.from_date or 'ALL'} | to={args.to_date or 'ALL'} | "
        f"recent_months={int(args.recent_months) or 'DISABLED'}"
    )
    print(
        f"[INFO] historical snapshots  : "
        f"dir={args.historical_instruments_dir or 'NOT PROVIDED (using live master only)'} | "
        f"match_mode={args.snapshot_match_mode} | "
        f"fallback_current_master={bool(args.snapshot_fallback_current_master)}"
    )
    if not args.historical_instruments_dir:
        print()
        print("[WARN] No --historical-instruments-dir provided.")
        print("[WARN] ALL trades will be mapped using TODAY's live NFO instrument master.")
        print("[WARN] If your v4 CSV contains trades older than ~2 months, most of them will")
        print("[WARN] either be SKIPPED (max-days-to-expiry filter) or MAPPED TO WRONG EXPIRIES.")
        print("[WARN] Recommendation: run nfo_instruments_snapshot_builder daily, then rerun")
        print("[WARN] this script with --historical-instruments-dir <snapshot_dir>.")
        print("[WARN] For a quick test, add --recent-months 3 to limit to recent trades only.")
    print("-" * 78)

    trades = _load_v4_trades(v4_csv)
    if trades.empty:
        raise RuntimeError("No valid trades found in v4 CSV after parsing.")
    total_loaded = len(trades)
    min_loaded_dt = min(trades["trade_date"])
    max_loaded_dt = max(trades["trade_date"])
    print(
        f"[INFO] loaded stock trades: {total_loaded} | "
        f"date-range={min_loaded_dt}..{max_loaded_dt}"
    )

    from_dt = _parse_date_arg(args.from_date, "--from-date")
    to_dt = _parse_date_arg(args.to_date, "--to-date")
    trades = _filter_trades_by_date(
        trades=trades,
        from_date=from_dt,
        to_date=to_dt,
        recent_months=int(args.recent_months),
    )
    if trades.empty:
        raise RuntimeError("No trades left after date filtering.")
    print(
        f"[INFO] trades after date filters: {len(trades)} "
        f"(dropped={total_loaded-len(trades)}) | "
        f"date-range={min(trades['trade_date'])}..{max(trades['trade_date'])}"
    )

    api_key_path = Path(args.api_key_file)
    access_token_path = Path(args.access_token_file)
    cred_candidates = _discover_app_credentials(
        base_dir=THIS_DIR,
        primary_api_key_file=api_key_path,
        primary_access_token_file=access_token_path,
        single_app_only=bool(args.single_app_only),
    )
    if not cred_candidates:
        raise RuntimeError("No credential files found for any app.")

    print(
        "[INFO] app credential candidates: "
        + ", ".join([f"{c.name}({c.api_key_file.name},{c.access_token_file.name})" for c in cred_candidates])
    )

    app_clients: List[AppClient] = []
    invalid_apps: List[dict] = []
    for cred in cred_candidates:
        try:
            kite = _build_kite_client(cred.api_key_file, cred.access_token_file)
            user_id = _validate_kite_session(kite, cred.api_key_file, cred.access_token_file)
            app_clients.append(
                AppClient(
                    name=cred.name,
                    user_id=user_id,
                    kite=kite,
                    api_key_file=cred.api_key_file,
                    access_token_file=cred.access_token_file,
                )
            )
        except Exception as exc:  # noqa: BLE001
            invalid_apps.append(
                {
                    "app_name": cred.name,
                    "api_key_file": str(cred.api_key_file),
                    "access_token_file": str(cred.access_token_file),
                    "error": str(exc),
                }
            )
            print(f"[WARN] {cred.name} auth invalid: {exc}")

    if not app_clients:
        raise RuntimeError(
            "No valid authenticated app found. Refresh access tokens for app1..app4 and rerun."
        )

    print(
        "[INFO] active apps: "
        + ", ".join([f"{a.name}(user={a.user_id or 'NA'})" for a in app_clients])
    )
    if invalid_apps:
        print(f"[WARN] invalid apps skipped: {len(invalid_apps)}")

    inst_df = _load_nfo_option_instruments(app_clients[0].kite)
    if inst_df.empty:
        raise RuntimeError("NFO option instrument master is empty.")
    print(f"[INFO] option instruments loaded: {len(inst_df)}")

    snapshot_files: Dict[datetime.date, Path] = {}
    snapshot_dates: List[datetime.date] = []
    snapshot_cache: Dict[datetime.date, pd.DataFrame] = {}
    if args.historical_instruments_dir:
        snap_dir = Path(args.historical_instruments_dir).resolve()
        if not snap_dir.exists():
            raise FileNotFoundError(f"historical snapshot dir not found: {snap_dir}")
        snapshot_files = _discover_snapshot_files(snap_dir)
        snapshot_dates = sorted(snapshot_files.keys())
        print(
            f"[INFO] historical snapshots discovered: {len(snapshot_dates)} "
            f"from {snap_dir}"
        )
        if snapshot_dates:
            print(
                f"[INFO] snapshot date-range: {snapshot_dates[0]}..{snapshot_dates[-1]}"
            )
        else:
            print("[WARN] no valid snapshot files found (need YYYYMMDD in filename).")

    map_rows: List[dict] = []
    skip_rows: List[dict] = []
    map_stats = {
        "used_snapshot": 0,
        "used_current_master": 0,
        "snapshot_missing": 0,
        "snapshot_empty": 0,
    }
    snapshot_cache_hits = 0
    snapshot_cache_misses = 0
    for tr in trades.itertuples(index=False):
        tr_s = pd.Series(tr._asdict())
        trade_date = tr_s.get("trade_date")

        inst_for_trade = inst_df
        snapshot_date_used: Optional[datetime.date] = None
        source = "current_master"

        if snapshot_dates:
            chosen = _pick_snapshot_date_for_trade(
                trade_date=trade_date,
                snapshot_dates=snapshot_dates,
                mode=str(args.snapshot_match_mode),
            )
            if chosen is None:
                if bool(args.snapshot_fallback_current_master):
                    inst_for_trade = inst_df
                    source = "current_master_fallback_no_snapshot"
                    map_stats["used_current_master"] += 1
                else:
                    map_stats["snapshot_missing"] += 1
                    skip_rows.append(
                        {
                            "trade_uid": tr_s.get("trade_uid"),
                            "trade_date": tr_s.get("trade_date"),
                            "ticker": tr_s.get("ticker"),
                            "side": tr_s.get("side"),
                            "entry_time_ist": tr_s.get("entry_time_ist"),
                            "reason": "no_snapshot_for_trade_date",
                        }
                    )
                    continue
            else:
                snapshot_date_used = chosen
                if chosen in snapshot_cache:
                    inst_for_trade = snapshot_cache[chosen]
                    snapshot_cache_hits += 1
                else:
                    snap_file = snapshot_files.get(chosen)
                    if snap_file is None:
                        inst_for_trade = pd.DataFrame()
                    else:
                        inst_for_trade = _load_snapshot_instruments(snap_file)
                    snapshot_cache[chosen] = inst_for_trade
                    snapshot_cache_misses += 1
                if inst_for_trade.empty:
                    if bool(args.snapshot_fallback_current_master):
                        inst_for_trade = inst_df
                        source = "current_master_fallback_empty_snapshot"
                        map_stats["used_current_master"] += 1
                    else:
                        map_stats["snapshot_empty"] += 1
                        skip_rows.append(
                            {
                                "trade_uid": tr_s.get("trade_uid"),
                                "trade_date": tr_s.get("trade_date"),
                                "ticker": tr_s.get("ticker"),
                                "side": tr_s.get("side"),
                                "entry_time_ist": tr_s.get("entry_time_ist"),
                                "reason": f"snapshot_option_master_empty:{chosen}",
                            }
                        )
                        continue
                else:
                    source = "historical_snapshot"
                    map_stats["used_snapshot"] += 1
        else:
            map_stats["used_current_master"] += 1

        rr = _resolve_atm_contract_for_trade(
            tr_s,
            inst_for_trade,
            max_days_to_expiry=(int(args.max_days_to_expiry) if int(args.max_days_to_expiry) > 0 else None),
        )
        if rr.mapping is None:
            skip_rows.append(
                {
                    "trade_uid": tr_s.get("trade_uid"),
                    "trade_date": tr_s.get("trade_date"),
                    "ticker": tr_s.get("ticker"),
                    "side": tr_s.get("side"),
                    "entry_time_ist": tr_s.get("entry_time_ist"),
                    "reason": rr.skip_reason or "resolve_failed",
                }
            )
            continue
        rr.mapping["instrument_source"] = source
        rr.mapping["snapshot_date_used"] = snapshot_date_used
        map_rows.append(rr.mapping)

    if snapshot_dates:
        print(
            f"[INFO] snapshot cache usage: hits={snapshot_cache_hits} | "
            f"misses={snapshot_cache_misses} | cached_dates={len(snapshot_cache)}"
        )
    print(
        "[INFO] mapping source breakdown: "
        f"historical_snapshot={map_stats['used_snapshot']} | "
        f"current_master={map_stats['used_current_master']} | "
        f"snapshot_missing={map_stats['snapshot_missing']} | "
        f"snapshot_empty={map_stats['snapshot_empty']}"
    )

    map_df = pd.DataFrame(map_rows)
    skip_df = pd.DataFrame(skip_rows)
    mapped_pct = (len(map_df) / len(trades) * 100.0) if len(trades) else 0.0
    print(
        f"[INFO] mapping coverage: mapped={len(map_df)}/{len(trades)} "
        f"({mapped_pct:.2f}%) | skipped={len(skip_df)}"
    )

    map_csv = options_dir / DEFAULT_MAP_CSV
    skips_csv = options_dir / DEFAULT_SKIPS_CSV
    if not map_df.empty:
        map_df.to_csv(map_csv, index=False)
        print(f"[FILE SAVED] {map_csv}")
    if not skip_df.empty:
        skip_df.to_csv(skips_csv, index=False)
        print(f"[FILE SAVED] {skips_csv}")

    if map_df.empty:
        print("[DONE] No option contracts resolved from trades.")
        return

    # Sanity warning: if days_to_expiry is too large, mapping is likely not the true
    # historical ATM chain (today's instrument master does not include old expiries).
    if "days_to_expiry" in map_df.columns:
        dte = pd.to_numeric(map_df["days_to_expiry"], errors="coerce")
        if dte.notna().any():
            p50_dte = float(dte.median())
            over_62 = int((dte > 62).sum())
            print(
                "[INFO] mapped DTE stats: "
                f"min={int(dte.min())} | p50={p50_dte:.1f} | p90={float(dte.quantile(0.9)):.1f} | "
                f"max={int(dte.max())}"
            )
            if over_62 > 0:
                print()
                print("=" * 78)
                print("DATA QUALITY WARNING - DTE > 62 DETECTED")
                print("=" * 78)
                print(f"  {over_62}/{len(map_df)} mapped contracts ({over_62/len(map_df)*100:.1f}%) have")
                print(f"  days_to_expiry > 62 (median DTE = {p50_dte:.1f} days).")
                print()
                print("  ROOT CAUSE:")
                print("    Today's live NFO instrument master does NOT contain contracts that")
                print("    expired months or years ago. The fetcher found no expiry <= 62 days")
                print("    for those old trade dates, so it fell back to a distant future expiry.")
                print("    That contract is NOT the ATM option that was available on the trade date.")
                print("    The strike, premium, and greeks are all wrong. Option P&L will be garbage.")
                print()
                print("  IMMEDIATE FIX (applied now):")
                print("    --max-days-to-expiry 62 is the current default.")
                print("    Any contract with DTE > 62 is being SKIPPED (see _skips CSV).")
                print("    The remaining mapped trades have DTE <= 62 and are safer to use.")
                print()
                print("  PERMANENT FIX:")
                print("    Run nfo_instruments_snapshot_builder_v6_nfo_options.py every trading")
                print("    day (e.g. 9:00 AM cron) to save dated instrument archives:")
                print("      python nfo_instruments_snapshot_builder_v6_nfo_options.py \\")
                print("             --output-dir ./nfo_instrument_snapshots_v6_nfo_options")
                print("    Then rerun this fetcher with:")
                print("      --historical-instruments-dir ./nfo_instrument_snapshots_v6_nfo_options")
                print("    The fetcher will then use each date's exact instrument master")
                print("    to find the true ATM contract for that specific trade date.")
                print()
                print("  QUICK WORKAROUND (no snapshots needed):")
                print("    Limit to recent trades where live master covers the expiries:")
                print("      --recent-months 3")
                print("    This cuts the backtest to the last 3 months where ATM mapping is accurate.")
                print("=" * 78)
                print()

    # Fetch only unique contracts; bound fetch window by mapped trade dates.
    map_df["trade_date"] = pd.to_datetime(map_df["trade_date"], errors="coerce").dt.date
    fetch_status: List[dict] = []
    unique_contracts = (
        map_df.groupby(["instrument_token", "tradingsymbol", "option_file"], as_index=False)
        .agg(min_trade_date=("trade_date", "min"), max_trade_date=("trade_date", "max"))
    )
    print(f"[INFO] unique option contracts to fetch: {len(unique_contracts)}")

    # Split contracts across active apps and fetch in parallel (one worker per app).
    contracts = unique_contracts.to_dict(orient="records")
    shards: Dict[str, List[dict]] = {app.name: [] for app in app_clients}
    app_by_name: Dict[str, AppClient] = {app.name: app for app in app_clients}
    for i, c in enumerate(contracts):
        app = app_clients[i % len(app_clients)]
        shards[app.name].append(c)

    print(
        "[INFO] app shard sizes: "
        + ", ".join([f"{name}={len(rows)}" for name, rows in shards.items()])
    )

    auth_failed_apps: List[str] = []
    with ThreadPoolExecutor(max_workers=len(app_clients)) as executor:
        futures = {}
        for app_name, rows in shards.items():
            if not rows:
                continue
            app = app_by_name[app_name]
            futures[
                executor.submit(
                    _fetch_contracts_for_app,
                    app,
                    rows,
                    options_dir,
                    args.interval,
                    int(args.max_retries),
                    float(args.sleep_sec),
                )
            ] = app_name

        for fut in as_completed(futures):
            app_name = futures[fut]
            try:
                statuses, auth_failed = fut.result()
                fetch_status.extend(statuses)
                if auth_failed:
                    auth_failed_apps.append(app_name)
            except Exception as exc:  # noqa: BLE001
                fetch_status.append(
                    {
                        "app_name": app_name,
                        "user_id": "",
                        "instrument_token": 0,
                        "tradingsymbol": "",
                        "option_file": "",
                        "from_dt": "",
                        "to_dt": "",
                        "rows": 0,
                        "status": "ERROR",
                        "error": f"worker_crashed: {exc}",
                    }
                )

    if auth_failed_apps:
        print(
            "[WARN] auth failed mid-run for apps: " + ", ".join(sorted(set(auth_failed_apps)))
        )

    fetch_df = pd.DataFrame(fetch_status)
    fetch_csv = options_dir / DEFAULT_FETCH_STATUS_CSV
    fetch_df.to_csv(fetch_csv, index=False)
    print(f"[FILE SAVED] {fetch_csv}")

    ok_count = int((fetch_df["status"] == "OK").sum()) if not fetch_df.empty else 0
    err_count = int((fetch_df["status"] == "ERROR").sum()) if not fetch_df.empty else 0
    empty_count = int((fetch_df["status"] == "EMPTY").sum()) if not fetch_df.empty else 0
    ok_files = set(fetch_df.loc[fetch_df["status"].eq("OK"), "option_file"].astype(str)) if not fetch_df.empty else set()
    mapped_trades_with_data = int(map_df["option_file"].astype(str).isin(ok_files).sum()) if not map_df.empty else 0
    if not fetch_df.empty and "app_name" in fetch_df.columns:
        print("[INFO] fetch status by app:")
        app_status = (
            fetch_df.groupby(["app_name", "status"])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )
        print(app_status.to_string(index=False))
    _print_fetch_diagnostics(fetch_df, map_df)

    print("\n================ nfo_options_data_fetcher_v6 summary ================")
    print(f"Stock trades loaded             : {len(trades)}")
    print(f"Active authenticated apps       : {len(app_clients)}")
    print(f"Invalid apps skipped            : {len(invalid_apps)}")
    print(f"Contracts resolved              : {len(map_df)}")
    print(f"Trades unresolved               : {len(skip_df)}")
    print(
        "Mapping source usage            : "
        f"snapshot={map_stats['used_snapshot']} | "
        f"current={map_stats['used_current_master']} | "
        f"snapshot_missing={map_stats['snapshot_missing']} | "
        f"snapshot_empty={map_stats['snapshot_empty']}"
    )
    print(f"Unique option contracts         : {len(unique_contracts)}")
    print(f"Contracts attempted             : {len(fetch_df)}")
    print(f"Contract fetch OK               : {ok_count}")
    print(f"Contract fetch EMPTY            : {empty_count}")
    print(f"Contract fetch ERROR            : {err_count}")
    print(
        f"Mapped trades w/ option candles : {mapped_trades_with_data}/{len(map_df)} "
        f"({(mapped_trades_with_data/len(map_df)*100.0 if len(map_df) else 0.0):.2f}%)"
    )
    if not skip_df.empty and "reason" in skip_df.columns:
        print("Top trade->option map skip reasons:")
        vc = skip_df["reason"].astype(str).value_counts().head(10)
        for reason, cnt in vc.items():
            print(f"  - {reason}: {int(cnt)}")
        days_to_expiry_skips = int(
            skip_df["reason"].astype(str).str.startswith("days_to_expiry_gt_max").sum()
        )
        if days_to_expiry_skips > 0:
            print(f"  -> {days_to_expiry_skips} trades skipped because nearest expiry was too far away.")
            print(f"  -> These are CORRECTLY filtered. To recover them, use historical snapshots.")
    print("========================================================================")

    print()
    print("=" * 78)
    print("NEXT STEPS - TO GET ACCURATE OPTION BACKTEST RESULTS")
    print("=" * 78)
    if map_stats["used_current_master"] > 0 and map_stats["used_snapshot"] == 0:
        print()
        print("  [!] ALL mappings used the LIVE NFO instrument master.")
        print("      This is only accurate for trades in the last ~2 months.")
        print()
        print("  STEP 1: Build daily instrument snapshots going forward:")
        print("    python nfo_instruments_snapshot_builder_v6_nfo_options.py \\")
        print("           --output-dir ./nfo_instrument_snapshots_v6_nfo_options")
        print("    Schedule this daily at market open (9:00-9:15 AM IST).")
        print()
        print("  STEP 2: Re-run this fetcher with historical snapshots:")
        print("    python nfo_options_data_fetcher_v6_nfo_options.py \\")
        print("           --historical-instruments-dir ./nfo_instrument_snapshots_v6_nfo_options \\")
        print("           --snapshot-fallback-current-master \\")
        print("           --max-days-to-expiry 62")
        print()
        print("  STEP 3 (quick test, no snapshots): Limit to recent months only:")
        print("    python nfo_options_data_fetcher_v6_nfo_options.py --recent-months 3")
        print()
    elif map_stats["used_snapshot"] > 0:
        print()
        print("  [OK] Historical snapshots were used for mapping. Results are accurate for")
        print("       dates covered by your snapshot archive.")
        print()
        snapshot_coverage_pct = map_stats["used_snapshot"] / max(1, map_stats["used_snapshot"] + map_stats["used_current_master"]) * 100
        print(f"  Snapshot coverage: {map_stats['used_snapshot']} / "
              f"{map_stats['used_snapshot'] + map_stats['used_current_master']} trades "
              f"({snapshot_coverage_pct:.1f}%)")
        if map_stats["used_current_master"] > 0:
            print(f"  [WARN] {map_stats['used_current_master']} trades still used live master")
            print(f"  (no snapshot found for those dates). Add snapshots for those dates")
            print(f"  or accept that those trades may have inaccurate ATM mapping.")
        print()
    print("  STEP 4: Run the backtest after fetching is complete:")
    print("    python avwap_combined_runner_v6_nfo_options.py")
    print("    (optionally add --recent-months 3 to limit to well-mapped trades)")
    print("=" * 78)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print(f"[FATAL] {e}", file=sys.stderr)
        raise



