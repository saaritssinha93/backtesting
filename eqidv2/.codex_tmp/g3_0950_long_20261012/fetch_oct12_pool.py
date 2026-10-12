"""Preserve completed minute paths for all Oct 12 09:50 LONG candidates."""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import fno_oi_common as common
import fno_v13_v10_g_3_contract as contract


OUT = Path(r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_v10_g_3_0950_long_20261012")
MANIFEST = OUT / "oct12_0950_pool_minute_sources.json"
LIVE = Path(r"C:\TradingData\eqidv2\fno_oi\v13_v10_g_3_live")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    scanner_path = LIVE / "scanner_5m" / "2026-10-12" / "slot_0950.json"
    confirm_path = LIVE / "confirmation_1m" / "2026-10-12" / "slot_0951.json"
    scanner = json.loads(scanner_path.read_text())
    confirm = json.loads(confirm_path.read_text())
    candidates = {row["tradingsymbol"]: row for row in scanner["candidates"] if row["side"] == "LONG"}
    packets = {row["tradingsymbol"]: row for row in confirm["feature_evaluations"]}
    if candidates.keys() != packets.keys() or len(candidates) != 5:
        raise RuntimeError("October 12 scanner/confirmation candidate mismatch")
    start = pd.Timestamp("2026-10-12 09:15", tz=contract.IST)
    end = pd.Timestamp("2026-10-12 15:15", tz=contract.IST)
    expected = pd.date_range("2026-10-12 09:16", "2026-10-12 15:15", freq="min", tz=contract.IST)
    entries = []
    client = None
    for symbol, row in candidates.items():
        file = OUT / f"oct12_0950_{symbol.lower()}_minutes.parquet"
        if symbol == "SAIL":
            file = OUT / "sail_oct12_minutes.parquet"
        if file.exists():
            frame = pd.read_parquet(file)
            source = "PRESERVED_KITE_HISTORICAL_MINUTE"
        else:
            if client is None:
                client = common.make_kite_client(common.discover_kite_credentials(max_apps=1)[0])
            candles = client.historical_data(int(row["instrument_token"]),
                start.to_pydatetime(), end.to_pydatetime(), "minute", continuous=False, oi=False)
            if not candles:
                raise RuntimeError(f"No broker minute candles for {symbol}")
            frame = pd.DataFrame(candles)
            frame["ts"] = pd.to_datetime(frame.date, utc=True).dt.tz_convert(contract.IST) + pd.Timedelta(minutes=1)
            frame = frame.loc[frame.ts.le(end)].sort_values("ts").copy()
            source = "KITE_HISTORICAL_MINUTE"
        today = frame.loc[frame.ts.dt.date.eq(start.date())].sort_values("ts")
        if len(today) != len(expected) or not today.ts.reset_index(drop=True).eq(pd.Series(expected)).all():
            raise RuntimeError(f"Incomplete October 12 minute path for {symbol}: {len(today)}")
        if not (today.high.ge(today[["open", "close"]].max(axis=1)) &
                today.low.le(today[["open", "close"]].min(axis=1))).all():
            raise RuntimeError(f"Invalid OHLC path for {symbol}")
        bar = today.loc[today.ts.eq(pd.Timestamp("2026-10-12 09:51", tz=contract.IST))]
        if len(bar) != 1:
            raise RuntimeError(f"Missing 09:51 confirmation bar: {symbol}")
        for name in ("open", "high", "low", "close"):
            if abs(float(bar.iloc[0][name]) - float(packets[symbol]["confirmation_" + name])) > .011:
                raise RuntimeError(f"Saved live confirmation differs from broker minute: {symbol} {name}")
        if not file.exists():
            frame.to_parquet(file,index=False)
        entries.append(dict(symbol=symbol,instrument_token=int(row["instrument_token"]),
            path=str(file),sha256=sha(file),source=source,bars_today=len(today),
            exact_confirmation_ohlc_parity=True))
        print(symbol,"minute path verified",len(today),flush=True)
    result=dict(date="2026-10-12",retrieved_at_ist=datetime.now(contract.IST).isoformat(),
        scanner=str(scanner_path),scanner_sha256=sha(scanner_path),
        confirmation=str(confirm_path),confirmation_sha256=sha(confirm_path),
        entries=entries)
    MANIFEST.write_text(json.dumps(result,indent=2)+"\n",encoding="utf-8")
    print(MANIFEST,flush=True)


if __name__ == "__main__":
    main()
