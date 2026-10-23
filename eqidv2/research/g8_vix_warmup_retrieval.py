"""Read-only retrieval of isolated, pre-sample India VIX warmup candles."""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import kite_vix_fetch_5m_15m as fetcher
from research.g8_vix_data import IST, load_bars


OUT = Path(r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g_8\inputs\warmup")
START = "2026-06-01"
END = "2026-07-24"


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    paths = {interval: OUT / f"india_vix_{interval}_{START}_to_{END}.csv"
             for interval in ("5minute", "15minute")}
    provenance_path = OUT / f"india_vix_warmup_{START}_to_{END}_provenance.json"
    coverage_path = OUT / f"india_vix_warmup_{START}_to_{END}_coverage.csv"
    for path in [*paths.values(), provenance_path, coverage_path]:
        if path.exists():
            raise FileExistsError(f"Refusing to replace existing input: {path}")
    # The helper validates the local session. Suppress account-identifying output.
    with contextlib.redirect_stdout(io.StringIO()):
        kite = fetcher.setup_kite_session()
    start, end = fetcher.build_range(START, END, None)
    frames = {}
    duplicates = {}
    for interval, path in paths.items():
        rows = kite.historical_data(instrument_token=264969, from_date=start,
                                    to_date=end, interval=interval, oi=False)
        frame = pd.DataFrame(rows)
        if frame.empty:
            raise ValueError(f"No India VIX history for {interval}")
        frame["date"] = pd.to_datetime(frame["date"], utc=True).dt.tz_convert(IST)
        duplicates[interval] = int(frame.date.duplicated().sum())
        if duplicates[interval]:
            raise ValueError(f"Raw duplicate India VIX timestamps for {interval}")
        frame = frame[["date", "open", "high", "low", "close", "volume"]].sort_values("date")
        frame.to_csv(path, index=False)
        frames[interval] = load_bars(path, 5 if interval == "5minute" else 15)
    five, fifteen = frames["5minute"], frames["15minute"]
    coverage = []
    for day, group in five.groupby("day", sort=True):
        expected = pd.date_range(f"{day} 09:15", f"{day} 15:25", freq="5min", tz=IST)
        actual = pd.DatetimeIndex(group.date)
        coverage.append({"day": day, "bars_5m": len(group), "missing_5m": len(expected.difference(actual)),
                         "unexpected_5m": len(actual.difference(expected)),
                         "bars_15m": int((fifteen.day == day).sum()),
                         "session_open": float(group.iloc[0].open),
                         "session_close": float(group.iloc[-1].close)})
    audit = pd.DataFrame(coverage)
    if (audit.missing_5m != 0).any() or (audit.unexpected_5m != 0).any() or (audit.bars_15m != 25).any():
        raise ValueError("Warmup contains incomplete/unexpected sessions")
    aggregates = five.set_index("date").resample("15min", origin="start_day").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}).dropna()
    independent = fifteen.set_index("date")[["open", "high", "low", "close"]]
    if not aggregates.index.equals(independent.index):
        raise ValueError("Five-minute and 15-minute timestamp coverage differs")
    max_diff = float((aggregates - independent).abs().max().max())
    if max_diff > 0.000001:
        raise ValueError("Five-minute aggregates disagree with independent 15-minute data")
    audit.to_csv(coverage_path, index=False)
    source = Path(fetcher.__file__).resolve()
    provenance = {"schema": "g8_india_vix_warmup_v1", "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
                  "source": "Zerodha Kite Connect historical_data (read-only)", "instrument_token": 264969,
                  "request_from": start.isoformat(), "request_to": end.isoformat(), "timezone": IST,
                  "timestamp_semantics": "Candle start; OHLC known only at candle start plus interval",
                  "sessions": len(audit), "first_session": audit.iloc[0].day,
                  "last_session": audit.iloc[-1].day, "rows_5m": len(five), "rows_15m": len(fifteen),
                  "raw_timestamp_duplicates": duplicates, "all_sessions_complete": True,
                  "max_5m_to_15m_ohlc_difference": max_diff, "execution_authority": False,
                  "source_helper": str(source), "source_helper_sha256": sha(source),
                  "retrieval_script_sha256": sha(__file__),
                  "files": {str(path.name): sha(path) for path in [*paths.values(), coverage_path]}}
    provenance_path.write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(provenance, indent=2))


if __name__ == "__main__":
    main()
