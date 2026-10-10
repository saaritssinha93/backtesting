"""Isolated, resumable cash-minute data preparation for v14 research.

Nothing in the v13 data tree is written.  ``ts`` is the end of a one-minute
candle in Asia/Kolkata, matching the native cash-minute baseline convention.
Missing/no-trade minutes remain missing; existing synthetic gap fills are
discarded. MIS membership is an explicitly labelled *current working-list
snapshot*, not a reconstructed historical broker-eligibility universe.
"""
from __future__ import annotations

import argparse
import ast
import contextlib
import hashlib
import json
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = Path(os.environ.get("V14_RUNTIME_ROOT", r"C:\TradingData\eqidv2\v14"))
SOURCE_ROOT = Path(r"C:\TradingData\eqidv2")
IST = ZoneInfo("Asia/Kolkata")
SCHEMA_VERSION = "v14_cash_native_1m_end_v1"
OHLCV = ["open", "high", "low", "close", "volume"]
DEFAULT_FROZEN = SOURCE_ROOT / "fno_oi/strategy_research/v13_corrected_v10_g_3/frozen_20261008_long110_nextminute_v1/daily_results.csv"


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")
    os.replace(temp, path)


def _parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    frame.to_parquet(temp, index=False)
    os.replace(temp, path)


def literal_symbols(path: Path) -> set[str]:
    """Read the working universe without importing/executing its module."""
    for node in ast.parse(path.read_text(encoding="utf-8-sig")).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "selected_stocks"
            for target in node.targets
        ):
            return {str(s).strip().upper() for s in ast.literal_eval(node.value)}
    raise ValueError(f"No literal selected_stocks found in {path.name}")


def trading_sessions(start: str | date, end: str | date, *, repo: Path = REPO) -> list[str]:
    holidays_path = repo / "nse_holidays.csv"
    holidays = set(pd.read_csv(holidays_path)["date"].astype(str)) if holidays_path.exists() else set()
    return [d.date().isoformat() for d in pd.date_range(start, end, freq="B") if d.date().isoformat() not in holidays]


def research_sessions(start: str, end: str, *, warmup_sessions: int = 20) -> tuple[list[str], list[str]]:
    study = trading_sessions(start, end)
    before = trading_sessions(pd.Timestamp(start).date() - timedelta(days=max(60, warmup_sessions * 3)), pd.Timestamp(start).date() - timedelta(days=1))
    return before[-warmup_sessions:] if warmup_sessions else [], study


def _ist(values: pd.Series) -> pd.Series:
    # Native archives and Kite return timezone-aware strings/datetimes. Naive
    # native values are local exchange time, never implicitly UTC.
    values = pd.to_datetime(values, errors="coerce")
    if values.dt.tz is None:
        return values.dt.tz_localize("Asia/Kolkata")
    return values.dt.tz_convert("Asia/Kolkata")


def normalize_cash_minutes(frame: pd.DataFrame, symbol: str, *, source: str,
                           start_labeled: bool = False) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame({"ts": pd.Series(dtype="datetime64[ns, Asia/Kolkata]"), **{c: pd.Series(dtype=float) for c in OHLCV}, "symbol": pd.Series(dtype=str), "source": pd.Series(dtype=str)})
    time_col = next((c for c in ("ts", "timestamp", "date", "candle_start") if c in frame), None)
    if time_col is None or not set(OHLCV).issubset(frame.columns):
        raise ValueError("Cash source lacks timestamp or OHLCV")
    out = frame.loc[:, OHLCV].apply(pd.to_numeric, errors="coerce").copy()
    out["ts"] = _ist(frame[time_col])
    if start_labeled:
        out["ts"] += pd.Timedelta(minutes=1)
    good = out[OHLCV].notna().all(axis=1) & np.isfinite(out[OHLCV]).all(axis=1)
    good &= out[["open", "high", "low", "close"]].gt(0).all(axis=1)
    good &= out["volume"].ge(0) & out["high"].ge(out[["open", "close", "low"]].max(axis=1))
    good &= out["low"].le(out[["open", "close", "high"]].min(axis=1))
    for flag in ("gap_filled", "provisional_stale", "opening_snapshot"):
        if flag in frame:
            good &= ~frame[flag].fillna(False).astype(str).str.lower().isin(["true", "1", "1.0"])
    minute = out["ts"].dt.hour * 60 + out["ts"].dt.minute
    good &= minute.between(556, 930) & out["ts"].dt.second.eq(0)
    out = out.loc[good].copy()
    out["symbol"] = str(symbol).upper()
    out["source"] = frame.loc[out.index, "source"].fillna(source).astype(str) if "source" in frame else source
    return out[["ts", *OHLCV, "symbol", "source"]].drop_duplicates("ts", keep="last").sort_values("ts").reset_index(drop=True)


def _read_cash(path: Path, symbol: str) -> pd.DataFrame:
    import pyarrow.parquet as pq
    columns = pq.ParquetFile(path).schema_arrow.names
    wanted = [c for c in ("ts", "timestamp", "date", *OHLCV, "gap_filled", "provisional_stale", "opening_snapshot", "source") if c in columns]
    return normalize_cash_minutes(pd.read_parquet(path, columns=wanted), symbol, source=f"native_cash_1m:{path.name}")


def load_symbol_1m(symbol: str, *, root: Path = DEFAULT_ROOT, start: str | None = None,
                   end: str | None = None) -> pd.DataFrame:
    frame = pd.read_parquet(Path(root) / "data/1m" / f"{symbol.upper()}.parquet")
    if start is not None:
        frame = frame.loc[frame.ts.ge(pd.Timestamp(start, tz="Asia/Kolkata"))]
    if end is not None:
        frame = frame.loc[frame.ts.lt(pd.Timestamp(end, tz="Asia/Kolkata") + pd.Timedelta(days=1))]
    return frame.reset_index(drop=True)


def coverage(frame: pd.DataFrame, sessions: Iterable[str]) -> dict[str, dict[str, Any]]:
    if frame.empty:
        counts = {}
    else:
        unique = frame.ts.drop_duplicates()
        grouped = unique.dt.normalize().value_counts()
        counts = {stamp.date().isoformat(): int(count) for stamp, count in grouped.items()}
    return {d: {"observed_minutes": int(counts.get(d, 0)), "missing_minutes": max(0, 375 - int(counts.get(d, 0))), "complete": int(counts.get(d, 0)) == 375} for d in sessions}


def _stock_nfo_symbols(frame: pd.DataFrame) -> set[str]:
    selected = frame.copy()
    if "is_index_future" in selected:
        selected = selected.loc[~selected.is_index_future.fillna(False).astype(bool)]
    if "segment" in selected:
        selected = selected.loc[selected.segment.astype(str).str.startswith("NFO")]
    col = next((c for c in ("underlying", "name", "equity_symbol") if c in selected), None)
    if col is None:
        raise ValueError("NFO archive lacks underlying identifier")
    indices = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "NIFTYMIDSELECT"}
    return set(selected[col].dropna().astype(str).str.upper()) - indices


def build_universe_manifest(sessions: list[str], *, root: Path = DEFAULT_ROOT,
                            source_root: Path = SOURCE_ROOT, repo: Path = REPO) -> dict[str, Any]:
    mis_path = repo / "filtered_stocks_MIS_v2.py"
    mis = literal_symbols(mis_path)
    archive_dir = source_root / "fno_oi/instrument_master"
    archives = {p.stem.removeprefix("instrument_master_"): p for p in archive_dir.glob("instrument_master_????-??-??.parquet")}
    # Near-month files retain the complete underlying list if a full master is
    # absent; never use filtered_fno_MIS_v2 (which omits F&O names outside MIS).
    for path in (source_root / "fno_oi/universe").glob("near_month_????-??-??.parquet"):
        archives.setdefault(path.stem.removeprefix("near_month_"), path)
    snapshots: dict[str, dict[str, Any]] = {}
    current_path = Path(root) / "data/instruments/nfo_latest.parquet"
    if current_path.exists():
        today = datetime.now(IST).date().isoformat()
        archives[today] = current_path
    records = {}
    for session in sessions:
        preceding = sorted(d for d in archives if d <= session)
        selected_day = preceding[-1] if preceding else (min(archives) if archives else None)
        if selected_day is not None:
            if selected_day not in snapshots:
                path = archives[selected_day]
                snapshots[selected_day] = {"symbols": sorted(_stock_nfo_symbols(pd.read_parquet(path))), "path": str(path), "sha256": _sha(path)}
            nfo = set(snapshots[selected_day]["symbols"])
            status = "DATED_NFO_SNAPSHOT" if selected_day == session else "PRIOR_NFO_SNAPSHOT_APPROXIMATION" if selected_day < session else "FUTURE_NFO_SNAPSHOT_APPROXIMATION"
        else:
            nfo = set()
            for name in ("filtered_stocks_NSE_futures_only.py", "filtered_stocks_NSE_options_only.py"):
                if (repo / name).exists():
                    nfo |= literal_symbols(repo / name)
            status = "STATIC_NFO_UNION_APPROXIMATION"
        records[session] = {"symbols": sorted(mis - nfo), "count": len(mis - nfo), "nfo_snapshot_date": selected_day, "nfo_membership_status": status}
    payload = {"schema_version": "v14_universe_v1", "mis_source": str(mis_path), "mis_sha256": _sha(mis_path), "mis_count": len(mis), "mis_membership_status": "WORKING_LIST_SNAPSHOT_NOT_HISTORICAL_BROKER_ELIGIBILITY", "mis_generation_header": mis_path.read_text(encoding="utf-8-sig").splitlines()[0], "limitation": "Historical MIS eligibility, delisted stocks and historical symbol aliases are not reconstructed. NFO archives use complete stock underlyings, not the MIS-filtered F&O subset.", "nfo_snapshots": snapshots, "sessions": records, "all_symbols": sorted(set().union(*(set(v["symbols"]) for v in records.values()))) if records else []}
    _json(Path(root) / "data/universe_manifest.json", payload)
    return payload


def load_universe_for_session(session: str, *, root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    manifest = json.loads((Path(root) / "data/universe_manifest.json").read_text(encoding="utf-8"))
    return {**manifest["sessions"][session], "mis_membership_status": manifest["mis_membership_status"]}


class MarketDataClient:
    """Sequential read-only broker calls; no order-placement method exposed."""
    def __init__(self, repo: Path = REPO, minimum_interval: float = 0.45):
        from kiteconnect import KiteConnect
        self._client = None
        self._last_call = 0.0
        self.minimum_interval = max(0.40, minimum_interval)
        failed = []
        for index in range(1, 9):
            suffix = "" if index == 1 else str(index)
            api, token = repo / f"api_key{suffix}.txt", repo / f"access_token{suffix}.txt"
            if not api.exists() or not token.exists():
                continue
            try:
                client = KiteConnect(api_key=api.read_text().strip().split()[0], timeout=20)
                client.set_access_token(token.read_text().strip().split()[0])
                self._wait()
                client.profile()
                self._client, self.app_name = client, f"app{index}"
                break
            except Exception as exc:
                failed.append(f"app{index}:{type(exc).__name__}")
        if self._client is None:
            raise RuntimeError("No authenticated market-data session: " + ", ".join(failed))

    def _wait(self) -> None:
        delay = self.minimum_interval - (time.monotonic() - self._last_call)
        if delay > 0:
            time.sleep(delay)
        self._last_call = time.monotonic()

    def _call(self, method: str, *args: Any, **kwargs: Any) -> Any:
        for attempt in range(3):
            self._wait()
            try:
                return getattr(self._client, method)(*args, **kwargs)
            except Exception as exc:
                # Never log server text: it can contain request identifiers or
                # credential-bearing URLs. Authentication failures stop retries.
                if type(exc).__name__ in {"TokenException", "PermissionException"} or attempt == 2:
                    raise RuntimeError(f"{method}:{type(exc).__name__}") from None
                time.sleep(2 ** attempt)

    def instruments(self, exchange: str) -> list[dict[str, Any]]:
        return self._call("instruments", exchange)

    def history(self, token: int, start: str, end: str) -> list[dict[str, Any]]:
        return self._call("historical_data", int(token), f"{start} 09:15:00", f"{end} 15:29:00", "minute", continuous=False, oi=False)

    def margin_probe(self, symbols: list[str]) -> list[dict[str, Any]]:
        # Calculation endpoint only; this does not submit orders.
        payload = [{"exchange": "NSE", "tradingsymbol": symbol, "transaction_type": "BUY", "variety": "regular", "product": "MIS", "order_type": "MARKET", "quantity": 1, "price": 0, "trigger_price": 0} for symbol in symbols]
        return self._call("order_margins", payload)


def refresh_instruments(client: MarketDataClient, *, root: Path = DEFAULT_ROOT) -> dict[str, int]:
    base = Path(root) / "data/instruments"
    snapshot_date = datetime.now(IST).date().isoformat()
    for exchange in ("NSE", "NFO"):
        records = pd.DataFrame(client.instruments(exchange))
        if records.empty:
            raise RuntimeError(f"Empty {exchange} instrument master")
        _parquet(base / f"{exchange.lower()}_{snapshot_date}.parquet", records)
        _parquet(base / f"{exchange.lower()}_latest.parquet", records)
    nse = pd.read_parquet(base / "nse_latest.parquet")
    nse = nse.loc[nse.instrument_type.eq("EQ") & nse.exchange.eq("NSE")]
    return dict(zip(nse.tradingsymbol.astype(str), nse.instrument_token.astype(int)))


def probe_mis(client: MarketDataClient, symbols: list[str], *, root: Path = DEFAULT_ROOT) -> None:
    result = []
    for offset in range(0, len(symbols), 40):
        batch = symbols[offset:offset + 40]
        try:
            rows = client.margin_probe(batch)
            if len(rows) != len(batch):
                raise RuntimeError("MarginResponseCountMismatch")
            for symbol, row in zip(batch, rows):
                result.append({"symbol": symbol, "leverage": row.get("leverage"), "margin_total": row.get("total"), "status": "CALCULATION_RETURNED", "eligibility_confirmed": False})
        except Exception as exc:
            result.extend({"symbol": s, "status": type(exc).__name__, "eligibility_confirmed": False} for s in batch)
    _json(Path(root) / "data/mis_margin_probe.json", {"observed_at": datetime.now(IST).isoformat(), "note": "Read-only calculation, no orders. A margin response or leverage value alone does not guarantee current MIS order acceptance and is not a historical eligibility record.", "rows": result})


@contextlib.contextmanager
def _fetch_lock(root: Path):
    path = root / "data/fetch.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    except FileExistsError:
        raise RuntimeError(f"Another v14 data writer holds {path}; inspect its PID before removing a stale lock.") from None
    os.write(fd, str(os.getpid()).encode())
    os.close(fd)
    try:
        yield
    finally:
        path.unlink(missing_ok=True)


def _fetch_windows(missing_days: list[str], max_calendar_days: int = 30) -> list[tuple[str, str]]:
    """Pack missing days into bounded broker requests (overlap existing is OK)."""
    windows = []
    for day in sorted(set(missing_days)):
        if windows and (pd.Timestamp(day) - pd.Timestamp(windows[-1][0])).days < max_calendar_days:
            windows[-1] = (windows[-1][0], day)
        else:
            windows.append((day, day))
    return windows


def prepare_data(start_date: str, end_date: str, *, warmup_sessions: int = 30,
                 fetch_missing: bool = False, symbols: Iterable[str] | None = None,
                 root: Path = DEFAULT_ROOT, source_root: Path = SOURCE_ROOT,
                 repo: Path = REPO, probe_current_mis: bool = False,
                 retry_partial: bool = False) -> Path:
    root = Path(root)
    if pd.Timestamp(end_date).date() > datetime.now(IST).date() or (pd.Timestamp(end_date).date() == datetime.now(IST).date() and datetime.now(IST).hour < 16):
        raise ValueError("Only completed sessions may be staged")
    warmup, study = research_sessions(start_date, end_date, warmup_sessions=warmup_sessions)
    sessions = warmup + study
    if not sessions:
        raise ValueError("No requested trading sessions")
    with _fetch_lock(root):
        client, tokens, broker_error = None, {}, None
        if fetch_missing:
            try:
                client = MarketDataClient(repo)
                tokens = refresh_instruments(client, root=root)
            except Exception as exc:
                broker_error = str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__
                print(f"BROKER_UNAVAILABLE {broker_error}; staging local data", flush=True)
                client = None
        universe = build_universe_manifest(sessions, root=root, source_root=source_root, repo=repo)
        requested = set(s.upper() for s in symbols) if symbols is not None else set(universe["all_symbols"])
        selected = sorted(requested & set(universe["all_symbols"]))
        if not selected:
            raise ValueError("No requested symbols in the non-NFO universe")
        if client and probe_current_mis:
            probe_mis(client, sorted(set(selected) & set(tokens)), root=root)
        start_ts, end_ts = pd.Timestamp(sessions[0], tz="Asia/Kolkata"), pd.Timestamp(sessions[-1], tz="Asia/Kolkata") + pd.Timedelta(days=1)
        summary: list[dict[str, Any]] = []
        failures = 0
        for number, symbol in enumerate(selected, 1):
            staged_path = root / "data/1m" / f"{symbol}.parquet"
            audit_path = root / "data/symbol_audits" / f"{symbol}.json"
            previous = json.loads(audit_path.read_text()) if audit_path.exists() else {}
            attempted = set(previous.get("fetched_sessions", [])) if not retry_partial else set()
            pieces = []
            source_records = []
            local_path = source_root / "stocks_indicators_1min_eq" / f"{symbol}_stocks_indicators_1min.parquet"
            if local_path.exists():
                try:
                    local = _read_cash(local_path, symbol)
                    # Keep earlier native history so EMA/indicator seeding is
                    # identical to a full-history run. Only fetch the required
                    # warm-up/study window; do not refill ancient history.
                    local = local.loc[local.ts.lt(end_ts)]
                    pieces.append(local)
                    source_records.append({"path": str(local_path), "size": local_path.stat().st_size, "mtime_ns": local_path.stat().st_mtime_ns, "rows_reused": len(local), "timestamp_label": "end", "native_source": "stocks_indicators_1min_eq"})
                except Exception as exc:
                    source_records.append({"path": str(local_path), "error": type(exc).__name__})
            if staged_path.exists():
                staged = _read_cash(staged_path, symbol)
                pieces.append(staged.loc[staged.ts.lt(end_ts)])
            combined = pd.concat(pieces, ignore_index=True) if pieces else normalize_cash_minutes(pd.DataFrame(), symbol, source="empty")
            combined = combined.drop_duplicates("ts", keep="last").sort_values("ts")
            before = coverage(combined, sessions)
            missing = [d for d, stats in before.items() if not stats["complete"] and d not in attempted]
            fetch_errors = []
            if client and symbol in tokens:
                for first, last in _fetch_windows(missing):
                    try:
                        raw = client.history(tokens[symbol], first, last)
                        incoming = normalize_cash_minutes(pd.DataFrame(raw), symbol, source="kite_historical_cash_1m", start_labeled=True)
                        incoming = incoming.loc[incoming.ts.ge(start_ts) & incoming.ts.lt(end_ts)]
                        # Preserve native complete bars; fetched rows repair gaps.
                        combined = pd.concat([combined, incoming], ignore_index=True).drop_duplicates("ts", keep="first").sort_values("ts")
                        attempted.update(trading_sessions(first, last, repo=repo))
                        source_records.append({"source": "kite_historical_cash_1m", "from": first, "through": last, "rows_returned": len(raw), "valid_rows": len(incoming), "instrument_token": tokens[symbol], "fetched_at": datetime.now(IST).isoformat()})
                    except Exception as exc:
                        fetch_errors.append({"from": first, "through": last, "error": str(exc) if isinstance(exc, RuntimeError) else type(exc).__name__})
                        failures += 1
                        if failures >= 10:
                            print("BROKER_ERROR_LIMIT reached; remaining symbols local-only", flush=True)
                            client = None
                        break
            combined = combined.reset_index(drop=True)
            _parquet(staged_path, combined)
            after = coverage(combined, sessions)
            audit = {"symbol": symbol, "schema_version": SCHEMA_VERSION, "source_records": source_records, "fetched_sessions": sorted(attempted), "fetch_errors": fetch_errors, "broker_symbol_available": symbol in tokens if fetch_missing and tokens else None, "coverage": after, "rows": len(combined), "sha256": _sha(staged_path), "updated_at": datetime.now(IST).isoformat()}
            _json(audit_path, audit)
            row = {"symbol": symbol, "rows": len(combined), "warmup_complete_sessions": sum(after[d]["complete"] for d in warmup), "study_complete_sessions": sum(after[d]["complete"] for d in study), "study_present_sessions": sum(after[d]["observed_minutes"] > 0 for d in study), "study_missing_minutes": sum(after[d]["missing_minutes"] for d in study), "fetch_errors": len(fetch_errors), "data_path": str(staged_path), "sha256": audit["sha256"]}
            summary.append(row)
            _json(root / "data/progress.json", {"status": "RUNNING", "pid": os.getpid(), "completed": number, "total": len(selected), "symbol": symbol, "broker_active": client is not None, "updated_at": datetime.now(IST).isoformat()})
            if number == 1 or number % 25 == 0 or number == len(selected):
                print(f"DATA {number}/{len(selected)} {symbol} rows={len(combined)} complete_study={row['study_complete_sessions']}/{len(study)}", flush=True)
        table = pd.DataFrame(summary)
        table.to_csv(root / "data/coverage.csv", index=False)
        manifest = {"schema_version": SCHEMA_VERSION, "created_at": datetime.now(IST).isoformat(), "start_date": start_date, "end_date": end_date, "warmup_sessions": warmup, "study_sessions": study, "symbols": selected, "symbol_count": len(selected), "total_rows": int(table.rows.sum()), "fetch_requested": fetch_missing, "broker_error": broker_error, "fetch_errors": failures, "timezone": "Asia/Kolkata", "timestamp_label": "end", "one_minute_source": "native_cash_1m_reused_and_Kite_historical_gap_repairs", "synthetic_bars": False, "missing_bars_policy": "Preserved gaps; partial/empty broker days cached, rerun with --retry-partial to retry", "universe_manifest": str(root / "data/universe_manifest.json"), "universe_sha256": _sha(root / "data/universe_manifest.json"), "coverage_csv": str(root / "data/coverage.csv"), "coverage_sha256": _sha(root / "data/coverage.csv"), "limitations": [universe["mis_membership_status"], "Earlier study dates without dated NFO master use an explicitly labelled snapshot approximation", "Cash no-trade minutes cannot be distinguished from missing provider bars", "A full bar count is a completeness check, not a liquidity/tradability guarantee", "Current cash instrument tokens are used for historical gap requests; renamed/delisted symbols may remain unavailable"], "files": summary}
        path = root / "data/manifest.json"
        _json(path, manifest)
        _json(root / "data/progress.json", {"status": "COMPLETE", "pid": os.getpid(), "completed": len(selected), "total": len(selected), "manifest": str(path), "updated_at": datetime.now(IST).isoformat()})
        return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2026-07-29")
    parser.add_argument("--end", default="2026-10-09")
    parser.add_argument("--warmup-sessions", type=int, default=30)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--symbols", help="Comma-separated subset for a smoke check")
    parser.add_argument("--fetch-missing", action="store_true")
    parser.add_argument("--probe-mis", action="store_true")
    parser.add_argument("--retry-partial", action="store_true")
    args = parser.parse_args()
    result = prepare_data(args.start, args.end, warmup_sessions=args.warmup_sessions, fetch_missing=args.fetch_missing, symbols=args.symbols.split(",") if args.symbols else None, root=args.root, probe_current_mis=args.probe_mis, retry_partial=args.retry_partial)
    print(f"MANIFEST {result}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
