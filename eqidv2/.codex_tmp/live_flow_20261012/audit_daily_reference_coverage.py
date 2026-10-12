"""Read the reporting-only daily cache; no data fetching or trading imports."""
import json
import sys
from collections import Counter
from pathlib import Path
from datetime import date
import pandas as pd

WORKSPACE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(WORKSPACE))
from dashboard_flow_live_data import list_available_dates, _universe, _daily_symbol, _daily_valid, _previous_session

ROOT = Path("C:/TradingData/eqidv2")
OUT = Path(__file__).resolve().parent
days = sorted(list_available_dates(ROOT))
universes = {day: _universe(ROOT, date.fromisoformat(day))[0] for day in days}
symbols = sorted({row['symbol'] for rows in universes.values() for row in rows})
daily = {}
for symbol in symbols:
    path = ROOT / "fno_oi/raw_equity_daily" / (_daily_symbol(symbol) + "_day.parquet")
    if not path.exists():
        daily[symbol] = set()
        continue
    frame = pd.read_parquet(path)
    frame['ts'] = pd.to_datetime(frame['date'])
    if frame.ts.dt.tz is None:
        frame['ts'] = frame.ts.dt.tz_localize('Asia/Kolkata')
    frame = _daily_valid(frame, broker_raw=True)
    daily[symbol] = set(frame.ts.dt.strftime('%Y-%m-%d'))

rows = []
for day in days:
    prior = _previous_session(date.fromisoformat(day)).isoformat()
    names = [item['symbol'] for item in universes[day]]
    missing_close = [symbol for symbol in names if prior not in daily[symbol]]
    missing_open = [symbol for symbol in names if day not in daily[symbol]]
    # Today's open is never read from an incomplete daily candle.
    historical = day < '2026-10-12'
    rows.append(dict(day=day, previous_session=prior, universe=len(names), daily_previous_close=len(names)-len(missing_close),
        daily_historical_open=len(names)-len(missing_open) if historical else None,
        missing_previous_close=missing_close, missing_historical_open=missing_open if historical else [],
        opening_source='VALIDATED_DAILY_BAR_AVAILABLE' if historical else 'LIVE_COMPLETED_OPENING_BAR'))
result = dict(phase='AFTER_REPORTING_ONLY_DAILY_BACKFILL', sessions=len(rows), first=days[0], last=days[-1],
    calendar_holiday_dates_omitted='2026-09-14' not in days,
    aliases={'LTIM':'LTM'}, historical_extra_symbols=sorted(set(symbols)-{r['symbol'] for r in universes['2026-10-12']}),
    full_previous_close_sessions=sum(row['daily_previous_close']==row['universe'] for row in rows),
    full_historical_daily_open_sessions=sum(row['daily_historical_open']==row['universe'] for row in rows if row['daily_historical_open'] is not None),
    missing_close_symbols=dict(Counter(symbol for row in rows for symbol in row['missing_previous_close'])),
    missing_open_symbols=dict(Counter(symbol for row in rows for symbol in row['missing_historical_open'])), rows=rows)
(OUT/'daily_reference_coverage_after_backfill.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
print(json.dumps({key:value for key,value in result.items() if key!='rows'},indent=2))
print('Today',rows[-1])
