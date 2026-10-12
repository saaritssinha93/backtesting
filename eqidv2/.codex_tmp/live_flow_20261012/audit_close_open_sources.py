"""Read-only cash reference audit; writes only this audit directory."""
from __future__ import annotations
import json
import sys
from pathlib import Path
from datetime import date
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import pyarrow.parquet as pq

WORKSPACE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORKSPACE))
from dashboard_flow_live_data import list_available_dates, _universe, _cash_valid, _previous_session

ROOT = Path("C:/TradingData/eqidv2")
OUT = Path(__file__).resolve().parent
DAYS = sorted(list_available_dates(ROOT))
UNIVERSES = {day: _universe(ROOT, date.fromisoformat(day))[0] for day in DAYS}
SYMBOLS = sorted({row["symbol"] for rows in UNIVERSES.values() for row in rows})
SOURCES = [
    ("live5m", "stocks_indicators_5min_eq_live", "_stocks_indicators_5min.parquet", False),
    ("historical1m", "stocks_indicators_1min_eq", "_stocks_indicators_1min.parquet", True),
    ("historical5m", "stocks_indicators_5min_eq_live2", "_stocks_indicators_5min.parquet", False),
    ("ticker5m", "stocks_indicators_5min_eq_live_kiteticker", "_stocks_indicators_5min.parquet", False),
]

def flag(series):
    return pd.to_numeric(series, errors="coerce").fillna(0).ne(0) | series.astype(str).str.lower().isin(["true", "yes"])

def inspect(symbol):
    records, summaries = {}, {}
    for key, directory, suffix, minute in SOURCES:
        path = ROOT / directory / (symbol + suffix)
        if not path.is_file():
            continue
        schema = pq.ParquetFile(path).schema_arrow
        timestamp = "date" if "date" in schema.names else "timestamp"
        columns = [c for c in [timestamp, "open", "high", "low", "close", "volume", "gap_filled", "opening_snapshot", "provisional_stale", "source_1m_count"] if c in schema.names]
        lo, hi = pd.Timestamp("2026-05-25"), pd.Timestamp("2026-10-13")
        if getattr(schema.field(timestamp).type, "tz", None):
            lo, hi = lo.tz_localize("Asia/Kolkata"), hi.tz_localize("Asia/Kolkata")
        frame = pd.read_parquet(path, columns=columns, filters=[(timestamp, ">=", lo), (timestamp, "<", hi)])
        if frame.empty:
            continue
        stamps = pd.to_datetime(frame[timestamp])
        stamps = stamps.dt.tz_localize("Asia/Kolkata") if stamps.dt.tz is None else stamps.dt.tz_convert("Asia/Kolkata")
        frame = frame.assign(ts=stamps, day=stamps.dt.strftime("%Y-%m-%d"), clock=stamps.dt.strftime("%H:%M")).sort_values("ts")
        good = _cash_valid(frame, minute=minute)
        summaries[key] = {"last_date": frame.day.max(), "last_valid_time": good.clock.iloc[-1] if not good.empty else None}
        last = good.groupby("day").clock.max().to_dict()
        close = good.loc[good.clock.eq("15:30")].drop_duplicates("day", keep="last").set_index("day").close.to_dict()
        opens = good.loc[good.clock.eq("09:16" if minute else "09:20")].drop_duplicates("day", keep="first").set_index("day").open.to_dict()
        first = good.loc[good.clock.isin(["09:16", "09:17", "09:18", "09:19", "09:20"])].groupby("day").clock.nunique().eq(5).to_dict() if minute else {day: True for day in opens}
        raw_close_days = set(frame.loc[frame.clock.eq("15:30"), "day"])
        for day in frame.day.unique():
            record = dict(close=day in close, open=day in opens, completed_first_bar=first.get(day, False),
                last_real=last.get(day), raw_1530=day in raw_close_days, bad_1530=day in raw_close_days and day not in close)
            if day in opens:
                record["open_price"] = float(opens[day])
            if day in close:
                record["close_price"] = float(close[day])
            records.setdefault(day, {})[key] = record
    return symbol, records, summaries

data, latest = {}, {}
with ThreadPoolExecutor(max_workers=2) as pool:
    for index, (symbol, rows, summaries) in enumerate(pool.map(inspect, SYMBOLS), 1):
        data[symbol], latest[symbol] = rows, summaries
        if index % 50 == 0:
            print("Scanned", index, "of", len(SYMBOLS), flush=True)

sessions = []
for index, day in enumerate(DAYS):
    prior = _previous_session(date.fromisoformat(day)).isoformat()
    rows = UNIVERSES[day]
    result = dict(day=day, previous_selectable_date=prior, universe=len(rows), verified_close_core=0,
        recoverable_ticker_close=0, session_open_core=0, completed_open_bar_core=0, gap_count_core=0,
        previous_close_source_counts={}, closing_bar_bad_or_missing=0)
    source_counts = Counter()
    for item in rows:
        symbol = item["symbol"]
        current = data.get(symbol, {}).get(day, {})
        previous = data.get(symbol, {}).get(prior, {})
        close_sources = [source for source in ["live5m", "historical1m", "historical5m"] if previous.get(source, {}).get("close")]
        open_sources = [source for source in ["live5m", "historical1m", "historical5m"] if current.get(source, {}).get("open")]
        completed = [source for source in ["live5m", "historical1m", "historical5m"] if current.get(source, {}).get("completed_first_bar")]
        result["verified_close_core"] += bool(close_sources)
        result["session_open_core"] += bool(open_sources)
        result["completed_open_bar_core"] += bool(completed)
        result["gap_count_core"] += bool(close_sources and open_sources)
        result["recoverable_ticker_close"] += not close_sources and bool(previous.get("ticker5m", {}).get("close"))
        result["closing_bar_bad_or_missing"] += not bool(close_sources)
        source_counts.update(close_sources)
    result["previous_close_source_counts"] = dict(source_counts)
    sessions.append(result)

last_times = {}
for source, _, _, _ in SOURCES:
    last_times[source] = dict(Counter(item.get(source, {}).get("last_valid_time") for item in latest.values()))
report = dict(data_root=str(ROOT), source_note="Existing-source baseline excludes new raw_equity_daily backfill. Same _cash_valid checks as dashboard, including copied-OHLCV exclusion; exchange calendar prior session. Exact 15:30 intraday closes are last prices, not official daily close.",
    selectable_dates=len(DAYS), first_selectable=min(DAYS), last_selectable=max(DAYS), symbols=len(SYMBOLS),
    daily_cache_last_dates={"2026-05-15": 1019, "2026-04-17": 12, "2026-04-09": 5, "2026-03-02": 5},
    sessions=sessions, latest_valid_times=last_times,
    historical_extra_symbols=sorted(set(SYMBOLS)-{row['symbol'] for row in UNIVERSES[max(DAYS)]}),
    missing_open_symbols={day:[item['symbol'] for item in UNIVERSES[day] if not any(data.get(item['symbol'],{}).get(day,{}).get(s,{}).get('open') for s in ['live5m','historical1m','historical5m'])] for day in ['2026-05-27','2026-09-14','2026-09-21','2026-10-12']},
    oct9_sample={symbol: data.get(symbol, {}).get("2026-10-09", {}) for symbol in ["RELIANCE", "HDFCBANK", "360ONE", "M&M", "DALBHARAT", "LTIM"]})
OUT.mkdir(parents=True, exist_ok=True)
(OUT / "close_open_source_audit.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
lines = ["# Previous-close and opening-source audit", "", "Read-only audit of local data. Exact 15:30 bars are last-price evidence; official daily close requires a dated daily source.", "", f"Selectable sessions: {len(DAYS)} ({min(DAYS)} to {max(DAYS)}). Symbols across dated/registry universes: {len(SYMBOLS)}.", "", "All 1,041 existing daily cash files end before the first selectable session (1,019 end 2026-05-15).", "", "| Session | Previous selected session | Universe | Exact previous 15:30 | Session open | Complete opening 5m | Open + previous close | Additional ticker close |", "|---|---|---:|---:|---:|---:|---:|---:|"]
for row in sessions:
    lines.append("| " + " | ".join(str(row[key]) for key in ["day", "previous_selectable_date", "universe", "verified_close_core", "session_open_core", "completed_open_bar_core", "gap_count_core", "recoverable_ticker_close"]) + " |")
(OUT / "close_open_source_audit.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
print(json.dumps({"sessions": len(sessions), "recent": sessions[-9:], "latest_valid_times": last_times}, indent=2), flush=True)
