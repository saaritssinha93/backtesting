"""Fetch and preserve completed SAIL minute candles for isolated research."""
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
RAW = OUT / "sail_oct12_broker_raw.json"
PARQUET = OUT / "sail_oct12_minutes.parquet"
META = OUT / "sail_oct12_source.json"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    if any(path.exists() for path in (RAW, PARQUET, META)):
        raise FileExistsError("Research source is already preserved")
    start = pd.Timestamp("2026-10-09 09:15", tz=contract.IST)
    end = pd.Timestamp("2026-10-12 15:15", tz=contract.IST)
    client = common.make_kite_client(common.discover_kite_credentials(max_apps=1)[0])
    rows = client.historical_data(758529, start.to_pydatetime(), end.to_pydatetime(),
                                  "minute", continuous=False, oi=False)
    if not rows:
        raise RuntimeError("No SAIL minute candles")
    frame = pd.DataFrame(rows)
    frame["candle_start"] = pd.to_datetime(frame.date, utc=True).dt.tz_convert(contract.IST)
    frame["ts"] = frame.candle_start + pd.Timedelta(minutes=1)
    frame = frame.loc[frame.ts.le(end) & frame.ts.dt.strftime("%H:%M").between("09:16", "15:30")].copy()
    today = frame.loc[frame.ts.dt.strftime("%Y-%m-%d").eq("2026-10-12")].sort_values("ts")
    expected = pd.date_range("2026-10-12 09:16", "2026-10-12 15:15", freq="min", tz=contract.IST)
    if len(today) != len(expected) or not today.ts.reset_index(drop=True).eq(pd.Series(expected)).all():
        raise RuntimeError(f"Incomplete SAIL execution path: {len(today)} bars, last {today.ts.max()}")
    if not (today.high.ge(today[["open", "close"]].max(axis=1)) &
            today.low.le(today[["open", "close"]].min(axis=1))).all():
        raise RuntimeError("Invalid SAIL OHLC")
    live = json.loads(Path(r"C:\TradingData\eqidv2\fno_oi\v13_v10_g_3_live\confirmation_1m\2026-10-12\slot_0951.json").read_text())
    record = next(r for r in live["feature_evaluations"] if r["tradingsymbol"] == "SAIL")
    bar = today.loc[today.ts.eq(pd.Timestamp("2026-10-12 09:51", tz=contract.IST))]
    if len(bar) != 1:
        raise RuntimeError("Missing exact 09:51 broker candle")
    b = bar.iloc[0]
    for field in ("open", "high", "low", "close"):
        if abs(float(b[field]) - float(record[f"confirmation_{field}"])) > .011:
            raise RuntimeError(f"SAIL live/broker confirmation disagreement: {field}")
    RAW.write_text(json.dumps(rows, indent=2, default=str) + "\n", encoding="utf-8")
    frame.to_parquet(PARQUET, index=False)
    meta = dict(symbol="SAIL", instrument_token=758529, source="KITE_HISTORICAL_MINUTE",
                retrieved_at_ist=datetime.now(contract.IST).isoformat(),
                first_today=today.ts.min().isoformat(), last_today=today.ts.max().isoformat(),
                today_bars=len(today), exact_0951_live_parity=True,
                raw_sha256=sha(RAW), parquet_sha256=sha(PARQUET))
    META.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
