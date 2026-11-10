"""Fetch a sealed, read-only partial-session cash/OI input set for G7 V5 research."""
from __future__ import annotations

import argparse
import contextlib
from datetime import date, datetime, timedelta
import hashlib
import io
import json
from pathlib import Path
import shutil
import time
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

import fno_oi_common as common


DATA = Path("C:/TradingData/eqidv2")
IST = ZoneInfo("Asia/Kolkata")
OHLCV = ("open", "high", "low", "close", "volume")


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def fetch(kite, token: int, start: datetime, end: datetime, *, oi: bool = False) -> list[dict]:
    for attempt in range(3):
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                return kite.historical_data(token, start, end, "minute", oi=oi)
        except Exception:
            if attempt == 2:
                raise
            time.sleep(0.5 * (attempt + 1))
    raise AssertionError("Unreachable")


def run(day: date, cutoff: pd.Timestamp, output: Path, *, reuse_opening: Path | None = None) -> dict:
    if output.exists():
        raise FileExistsError(output)
    if cutoff.tzinfo is None or cutoff.tz_convert("Asia/Kolkata").date() != day:
        raise ValueError("Cutoff must be a timezone-aware timestamp on the requested session")
    cutoff = cutoff.tz_convert(IST)
    if cutoff >= pd.Timestamp.now(tz="Asia/Kolkata").floor("min"):
        raise ValueError("Cutoff must be a completed minute")
    universe_path = DATA / "fno_oi/universe" / f"near_month_{day}.parquet"
    universe = pd.read_parquet(universe_path)
    stocks = universe.loc[~universe.is_index_future.fillna(False).astype(bool)]
    if stocks.equity_symbol.nunique() != len(stocks):
        raise ValueError("Dated stock universe contains duplicate cash symbols")
    cash_dir = output / "equity_1m"
    opening_dir = output / "opening_observer/historical" / day.isoformat()
    cash_dir.mkdir(parents=True)
    opening_dir.mkdir(parents=True)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        from kite_vix_fetch_5m_15m import setup_kite_session
        kite = setup_kite_session()
        instruments = kite.instruments("NSE")
    tokens = {str(row.get("tradingsymbol")): int(row["instrument_token"])
              for row in instruments if row.get("segment") == "NSE" and row.get("instrument_token")}
    previous_opening = {}
    if reuse_opening is not None:
        prior = json.loads((reuse_opening / "retrieval_provenance.json").read_text(encoding="utf-8"))
        if prior["day"] != day.isoformat():
            raise ValueError("Opening receipts belong to another session")
        previous_opening = {item["symbol"]: item for item in prior["records"]}
    cash_start = datetime(day.year, day.month, day.day, 9, 15, tzinfo=IST)
    # Kite's intraday end boundary is exclusive: query through the candle's
    # end stamp to include the minute that started one minute earlier.
    cash_end = cutoff.to_pydatetime()
    opening_end = datetime(day.year, day.month, day.day, 9, 20, tzinfo=IST)
    expected_cash = pd.date_range(pd.Timestamp(cash_start) + pd.Timedelta(minutes=1), cutoff, freq="min")
    expected_opening = pd.date_range(cash_start, opening_end - timedelta(minutes=1), freq="min")
    records = []
    for position, item in enumerate(stocks.itertuples(), 1):
        symbol = str(item.equity_symbol)
        contract = str(item.futures_tradingsymbol)
        rec = dict(symbol=symbol, contract=contract, cash_status="unavailable",
                   opening_status="unavailable")
        records.append(rec)
        token = tokens.get(symbol)
        if token != int(item.equity_instrument_token):
            rec["cash_status"] = "instrument_token_mismatch"
        else:
            try:
                rows = fetch(kite, token, cash_start, cash_end)
                frame = pd.DataFrame(rows)
                if not frame.empty:
                    frame["date"] = pd.to_datetime(frame.date) + pd.Timedelta(minutes=1)
                    frame = frame.sort_values("date")
                    actual = pd.DatetimeIndex(frame.date)
                    valid = (len(actual) == len(expected_cash) and
                             np.array_equal(actual.asi8, expected_cash.asi8) and
                             frame[list(OHLCV)].notna().all().all())
                    rec["cash_minutes"] = len(actual)
                    rec["cash_latest"] = str(actual.max())
                    if valid:
                        path = cash_dir / f"{symbol}_stocks_indicators_1min.parquet"
                        frame[["date", *OHLCV]].to_parquet(path, index=False)
                        rec["cash_path"] = str(path)
                        rec["cash_sha256"] = sha(path)
                        rec["cash_status"] = "accepted"
                        live = DATA / "stocks_indicators_5min_eq_live" / f"{symbol}_stocks_indicators_5min.parquet"
                        if live.is_file():
                            observed = pd.read_parquet(live, columns=["date", *OHLCV])
                            observed["date"] = pd.to_datetime(observed.date)
                            observed = observed.loc[observed.date.between(expected_cash[4], cutoff)]
                            aggregate = frame.set_index("date")[list(OHLCV)].resample(
                                "5min", label="right", closed="right").agg(
                                {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
                            joined = aggregate.join(observed.set_index("date")[list(OHLCV)],
                                                    how="inner", lsuffix="_provider", rsuffix="_live")
                            rec["live_5m_overlap"] = len(joined)
                            if len(joined):
                                difference = np.abs(joined[[f"{x}_provider" for x in OHLCV]].to_numpy(float)
                                                    - joined[[f"{x}_live" for x in OHLCV]].to_numpy(float))
                                rec["live_5m_max_price_difference"] = float(difference[:, :4].max())
                                rec["live_5m_max_volume_difference"] = float(difference[:, 4].max())
                    else:
                        rec["cash_status"] = "incomplete_provider_minutes"
                else:
                    rec["cash_status"] = "empty_provider_response"
            except Exception as exc:
                rec["cash_status"] = "provider_error"
                rec["cash_error_type"] = type(exc).__name__
        if reuse_opening is not None:
            old = previous_opening[symbol]
            rec["opening_status"] = old["opening_status"]
            rec["opening_minutes"] = old.get("opening_minutes")
            rec["opening_max_5m_difference"] = old.get("opening_max_5m_difference")
            if old["opening_status"] == "accepted_retrospective":
                original = Path(old["opening_path"])
                if sha(original) != old["opening_sha256"]:
                    raise ValueError(f"Opening receipt changed: {symbol}")
                path = opening_dir / f"{contract}.json"
                shutil.copyfile(original, path)
                rec["opening_path"] = str(path)
                rec["opening_sha256"] = sha(path)
        else:
          try:
            rows = fetch(kite, int(item.futures_instrument_token), cash_start, opening_end, oi=True)
            stamps = pd.DatetimeIndex([pd.Timestamp(x["date"]) for x in rows])
            rec["opening_minutes"] = len(stamps)
            if len(stamps) != 5 or not np.array_equal(stamps.asi8, expected_opening.asi8):
                rec["opening_status"] = "incomplete_provider_minutes"
            elif any(float(x.get("oi", 0)) <= 0 for x in rows):
                rec["opening_status"] = "invalid_oi"
            else:
                future_path = DATA / "fno_oi/raw_contracts_5m" / f"{common.safe_contract_stem(contract)}_5minute.parquet"
                future = pd.read_parquet(future_path)
                stamp = pd.to_datetime(future.timestamp, utc=True).dt.tz_convert("Asia/Kolkata")
                bar = future.loc[stamp.eq(pd.Timestamp(day.isoformat() + " 09:20", tz="Asia/Kolkata"))]
                if len(bar) != 1:
                    rec["opening_status"] = "missing_matching_five_minute_bar"
                else:
                    observed = np.array([bar.iloc[0][x] for x in (*OHLCV, "oi")], dtype=float)
                    aggregate = np.array([rows[0]["open"], max(x["high"] for x in rows),
                                          min(x["low"] for x in rows), rows[-1]["close"],
                                          sum(x["volume"] for x in rows), rows[-1]["oi"]], dtype=float)
                    rec["opening_max_5m_difference"] = float(np.abs(observed - aggregate).max())
                    if rec["opening_max_5m_difference"] > 1e-6:
                        rec["opening_status"] = "provider_archive_conflict"
                    else:
                        candles = []
                        for value in rows:
                            start = pd.Timestamp(value["date"])
                            candles.append(dict(candle_start=start.isoformat(),
                                                candle_end=(start + pd.Timedelta(minutes=1)).isoformat(),
                                                **{field: float(value[field]) for field in (*OHLCV, "oi")}))
                        payload = dict(session_date=day.isoformat(), symbol=symbol, contract=contract,
                                       source="KITE_HISTORICAL_MINUTE", state="COMPLETE",
                                       acquired_at=datetime.now(IST).isoformat(),
                                       semantics="Retrospective minute OI; no live receipt-time claim",
                                       candles=candles)
                        path = opening_dir / f"{contract}.json"
                        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
                        rec["opening_path"] = str(path)
                        rec["opening_sha256"] = sha(path)
                        rec["opening_status"] = "accepted_retrospective"
          except Exception as exc:
            rec["opening_status"] = "provider_error"
            rec["opening_error_type"] = type(exc).__name__
        if position % 25 == 0 or position == len(stocks):
            print(f"RETRIEVAL {position}/{len(stocks)} cash={sum(x['cash_status']=='accepted' for x in records)} "
                  f"opening={sum(x['opening_status']=='accepted_retrospective' for x in records)}", flush=True)
        time.sleep(.12)
    result = dict(scope="BACKTEST_ONLY", execution_authority=False, day=day.isoformat(),
                  cutoff=str(cutoff), universe_path=str(universe_path), universe_sha256=sha(universe_path),
                  universe_stocks=len(stocks), cash_accepted=sum(x["cash_status"] == "accepted" for x in records),
                  opening_accepted=sum(x["opening_status"] == "accepted_retrospective" for x in records),
                  records=records)
    (output / "retrieval_provenance.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", type=date.fromisoformat, required=True)
    parser.add_argument("--cutoff", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--reuse-opening", type=Path)
    args = parser.parse_args()
    result = run(args.day, pd.Timestamp(f"{args.day} {args.cutoff}", tz="Asia/Kolkata"),
                 args.output, reuse_opening=args.reuse_opening)
    print(json.dumps({k: v for k, v in result.items() if k != "records"}, indent=2))


if __name__ == "__main__":
    main()
