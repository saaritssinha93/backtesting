"""Read-only recovery of October 30 dated first-five futures minute candles.

The receipts are retrospective research inputs, never evidence that the
opening OI was available to the live strategy at 09:20.
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

import fno_oi_common as common

DATA = Path("C:/TradingData/eqidv2")
OUT = ROOT / "outputs/g7_v6_20261102/oct30_retrieved_opening"
OBSERVER = OUT / "opening_observer/historical/2026-10-30"
IST = ZoneInfo("Asia/Kolkata")
START = datetime(2026, 10, 30, 9, 15, tzinfo=IST)
END = datetime(2026, 10, 30, 9, 19, tzinfo=IST)


def main() -> None:
    OBSERVER.mkdir(parents=True, exist_ok=True)
    universe_path = DATA / "fno_oi/universe/near_month_2026-10-30.parquet"
    universe = pd.read_parquet(universe_path)
    stocks = universe.loc[~universe.is_index_future.fillna(False).astype(bool)]
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        from kite_vix_fetch_5m_15m import setup_kite_session
        kite = setup_kite_session()
    previous_path = OUT / "retrieval_provenance.json"
    previous = (json.loads(previous_path.read_text(encoding="utf-8"))
                if previous_path.is_file() else {"records": []})
    prior = {record["symbol"]: record for record in previous["records"]}
    records = []
    for number, item in enumerate(stocks.itertuples(), 1):
        symbol = str(item.equity_symbol)
        contract = str(item.futures_tradingsymbol)
        token = int(item.futures_instrument_token)
        old = prior.get(symbol, {})
        old_path = Path(old.get("path", ""))
        if (old.get("status") == "accepted_retrospective" and old_path.is_file()
                and hashlib.sha256(old_path.read_bytes()).hexdigest() == old["sha256"]):
            records.append(old)
            continue
        rec = {"symbol": symbol, "contract": contract, "instrument_token": token}
        records.append(rec)
        try:
            for attempt in range(3):
                try:
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                        rows = kite.historical_data(token, START, END, "minute", oi=True)
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    time.sleep(0.5)
            stamps = [pd.Timestamp(row["date"]) for row in rows]
            expected = [pd.Timestamp(START + timedelta(minutes=i)) for i in range(5)]
            if len(rows) != 5 or stamps != expected or any(float(row.get("oi", 0)) <= 0 for row in rows):
                rec["status"] = "rejected_incomplete_or_invalid_opening"
                continue
            future_path = DATA / "fno_oi/raw_contracts_5m" / f"{common.safe_contract_stem(contract)}_5minute.parquet"
            future = pd.read_parquet(future_path)
            bar = future.loc[pd.to_datetime(future.timestamp).eq(pd.Timestamp("2026-10-30 09:20", tz=IST))]
            if len(bar) != 1:
                rec["status"] = "rejected_missing_matching_five_minute_bar"
                continue
            row5 = bar.iloc[0]
            aggregate = [rows[0]["open"], max(r["high"] for r in rows),
                         min(r["low"] for r in rows), rows[-1]["close"],
                         sum(r["volume"] for r in rows), rows[-1]["oi"]]
            archived = [row5[key] for key in ("open", "high", "low", "close", "volume", "oi")]
            differences = np.abs(np.array(aggregate, dtype=float) - np.array(archived, dtype=float))
            rec["max_five_minute_difference"] = float(differences.max())
            if differences.max() > 1e-6:
                rec["status"] = "rejected_five_minute_source_conflict"
                continue
            candles = []
            for raw in rows:
                start = pd.Timestamp(raw["date"])
                candles.append({"candle_start": start.isoformat(),
                                "candle_end": (start + timedelta(minutes=1)).isoformat(),
                                **{key: float(raw[key]) for key in
                                   ("open", "high", "low", "close", "volume", "oi")}})
            payload = {"session_date": "2026-10-30", "symbol": symbol, "contract": contract,
                       "source": "KITE_HISTORICAL_MINUTE", "acquired_at": datetime.now(IST).isoformat(),
                       "universe_source": "DATED_UNIVERSE", "state": "COMPLETE",
                       "semantics": "Retrospective minute OI; unavailable as live 09:20 evidence",
                       "candles": candles}
            path = OBSERVER / f"{contract}.json"
            path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            rec["path"] = str(path)
            rec["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
            rec["status"] = "accepted_retrospective"
        except Exception as exc:
            rec["status"] = "retrieval_or_validation_error"
            rec["error_type"] = type(exc).__name__
        if number % 25 == 0 or number == len(stocks):
            accepted = sum(r.get("status") == "accepted_retrospective" for r in records)
            print(f"[G7 V6 futures opening] {number}/{len(stocks)} checked, {accepted} accepted", flush=True)
    summary = {"scope": "BACKTEST_ONLY", "execution_authority": False,
               "session_date": "2026-10-30", "universe_count": len(stocks),
               "accepted": sum(r.get("status") == "accepted_retrospective" for r in records),
               "status_counts": {status: sum(r.get("status") == status for r in records)
                                 for status in sorted({r.get("status") for r in records})},
               "records": records}
    (OUT / "retrieval_provenance.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in summary.items() if k != "records"}, indent=2))


if __name__ == "__main__":
    main()
