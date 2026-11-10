"""Read-only, dated cash-minute recovery for the G7 V6 October 30 audit.

Only the October 29 overlap and October 30 session are requested. Recovered
files live in a separate research root and never replace trading archives.
"""
from __future__ import annotations

import contextlib
from datetime import datetime, timedelta
import hashlib
import io
import json
from pathlib import Path
import sys
import time
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DAY = pd.Timestamp("2026-10-30").date()
PRIOR = pd.Timestamp("2026-10-29").date()
IST = ZoneInfo("Asia/Kolkata")
DATA = Path("C:/TradingData/eqidv2")
OUT = ROOT / "outputs/g7_v6_20261102/oct30_retrieved_cash"
FIELDS = ["open", "high", "low", "close", "volume"]


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def compare(a: pd.DataFrame, b: pd.DataFrame, keys: list[str]) -> tuple[int, float]:
    overlap = a.index.intersection(b.index)
    if len(overlap) == 0:
        return 0, float("nan")
    left = a.loc[overlap, keys].astype(float).to_numpy()
    right = b.loc[overlap, keys].astype(float).to_numpy()
    return len(overlap), float(np.max(np.abs(left - right)))


def retrieve() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    universe_path = DATA / "fno_oi/universe/near_month_2026-10-30.parquet"
    universe = pd.read_parquet(universe_path)
    stocks = universe.loc[~universe.is_index_future.fillna(False).astype(bool)].copy()
    records = []
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        from kite_vix_fetch_5m_15m import setup_kite_session
        kite = setup_kite_session()
        instruments = kite.instruments("NSE")
    tokens = {str(x.get("tradingsymbol")): int(x["instrument_token"])
              for x in instruments if x.get("segment") == "NSE" and x.get("instrument_token")}
    start = datetime(2026, 10, 29, 9, 15, tzinfo=IST)
    end = datetime(2026, 10, 30, 15, 30, tzinfo=IST)
    for number, row in enumerate(stocks.itertuples(), 1):
        symbol = str(row.equity_symbol)
        rec = {"symbol": symbol, "provider": "Kite historical_data minute, read only",
               "request_start": start.isoformat(), "request_end": end.isoformat(),
               "source_archive": str(DATA / "stocks_indicators_1min_eq" /
                                     f"{symbol}_stocks_indicators_1min.parquet")}
        records.append(rec)
        try:
            token = tokens.get(symbol)
            if token is None or token != int(row.equity_instrument_token):
                rec["status"] = "rejected_instrument_token_mismatch"
                continue
            rec["instrument_token"] = token
            archive_path = Path(rec["source_archive"])
            if not archive_path.is_file():
                rec["status"] = "rejected_missing_archive"
                continue
            rec["archive_sha256"] = digest(archive_path)
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                rows = kite.historical_data(token, start, end, "minute", continuous=False, oi=False)
            fetched = pd.DataFrame(rows)
            if fetched.empty:
                rec["status"] = "rejected_empty_provider_response"
                continue
            fetched["date"] = pd.to_datetime(fetched.date) + timedelta(minutes=1)
            fetched = fetched.set_index("date").sort_index()
            if fetched.index.duplicated().any() or fetched[FIELDS].isna().any().any():
                rec["status"] = "rejected_invalid_provider_response"
                continue
            today = fetched.loc[fetched.index.date == DAY, FIELDS]
            expected = pd.date_range(datetime(2026, 10, 30, 9, 16, tzinfo=IST),
                                     datetime(2026, 10, 30, 15, 15, tzinfo=IST), freq="min")
            rec["oct30_minutes"] = len(today)
            if len(today) != 360 or not np.array_equal(today.index.asi8, expected.asi8):
                rec["status"] = "rejected_incomplete_provider_session"
                continue
            archive = pd.read_parquet(archive_path, columns=["date"] + FIELDS)
            archive["date"] = pd.to_datetime(archive.date)
            archive = archive.set_index("date").sort_index()
            if archive.index.max().date() != PRIOR:
                rec["status"] = "rejected_archive_end_date"
                continue
            prior = fetched.loc[fetched.index.date == PRIOR, FIELDS]
            prior_archive = archive.loc[archive.index.date == PRIOR, FIELDS]
            count, delta = compare(prior, prior_archive, FIELDS)
            rec["prior_day_overlap_minutes"] = count
            rec["prior_day_max_ohlcv_difference"] = delta
            # Stored archive volumes are float32, so a sub-0.001 share
            # roundoff is possible even when all minute OHLCV observations match.
            if count < 300 or not np.isfinite(delta) or delta > 1e-3:
                rec["status"] = "rejected_prior_day_overlap"
                continue
            live_path = DATA / "stocks_indicators_5min_eq_live" / f"{symbol}_stocks_indicators_5min.parquet"
            if live_path.is_file():
                live = pd.read_parquet(live_path,
                    columns=["date"] + FIELDS + ["source_1m_count"])
                live["date"] = pd.to_datetime(live.date)
                live = live.loc[(live.date.dt.date == DAY) & live.source_1m_count.eq(5)]
                live = live.set_index("date")
                agg = today.resample("5min", label="right", closed="right").agg(
                    {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
                count5, delta5 = compare(agg, live, FIELDS)
                rec["oct30_complete_5m_overlap"] = count5
                rec["oct30_max_5m_ohlcv_difference"] = delta5
                rec["live_5m_conflict"] = bool(count5 and (not np.isfinite(delta5) or delta5 > 1e-3))
            if digest(archive_path) != rec["archive_sha256"]:
                rec["status"] = "rejected_archive_changed"
                continue
            merged = pd.concat([archive, today], axis=0)
            if merged.index.duplicated().any():
                rec["status"] = "rejected_duplicate_merged_timestamps"
                continue
            path = OUT / f"{symbol}_stocks_indicators_1min.parquet"
            merged.reset_index().to_parquet(path, index=False)
            rec["output_path"] = str(path)
            rec["output_sha256"] = digest(path)
            rec["status"] = "accepted_with_live_5m_conflict" if rec.get("live_5m_conflict") else "accepted"
        except Exception as exc:
            rec["status"] = "retrieval_or_validation_error"
            rec["error_type"] = type(exc).__name__
        if number % 25 == 0 or number == len(stocks):
            accepted = sum(str(r.get("status", "")).startswith("accepted") for r in records)
            print(f"[G7 V6 cash recovery] {number}/{len(stocks)} checked, {accepted} accepted", flush=True)
        time.sleep(0.12)
    summary = {"scope": "BACKTEST_ONLY", "execution_authority": False,
               "session_date": DAY.isoformat(), "universe_count": len(stocks),
               "accepted": sum(str(r.get("status", "")).startswith("accepted") for r in records),
               "status_counts": {status: sum(r.get("status") == status for r in records)
                                 for status in sorted({r.get("status") for r in records})},
               "records": records}
    (OUT / "retrieval_provenance.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k != "records"}, indent=2))


if __name__ == "__main__":
    retrieve()
