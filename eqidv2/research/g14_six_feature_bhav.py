"""Freeze official NSE equity bhavcopies for causal prior-day-close features.

Downloads only the bounded weekday span needed for the 63 frozen G14 sessions.
The prior trading session's archived EQ close is the PDC. Current-day final
files are used only to verify symbol/date mappings after the fact, never to
construct information available at the morning decision.
"""
from __future__ import annotations

import argparse, hashlib, io, json, time, urllib.error, urllib.request, zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
INPUTS = ROOT / 'g3_indicator_lab_20261103/inputs'
DEFAULT_OUTPUT = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_14/six_feature_source')
BASE = 'https://nsearchives.nseindia.com/content/cm/'


def digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def dump(path: Path, payload):
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str, allow_nan=False) + '\n', encoding='utf-8')


def download(day: str, output: Path):
    code = day.replace('-', '')
    name = f'BhavCopy_NSE_CM_0_0_0_{code}_F_0000.csv.zip'
    url = BASE + name
    meta = dict(day=day, url=url, filename=name)
    for attempt in (1, 2):
        try:
            request = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/zip'})
            with urllib.request.urlopen(request, timeout=15) as response:
                payload = response.read()
                meta.update(http_status=response.status, http_last_modified=response.headers.get('Last-Modified'),
                    http_etag=response.headers.get('ETag'), fetched_at_utc=datetime.now(timezone.utc).isoformat())
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                members = archive.namelist()
                if len(members) != 1 or not members[0].lower().endswith('.csv'):
                    raise ValueError('Unexpected NSE ZIP member inventory')
                csv_bytes = archive.read(members[0])
            frame = pd.read_csv(io.BytesIO(csv_bytes), low_memory=False)
            columns = {'TradDt', 'TckrSymb', 'SctySrs', 'ClsPric', 'PrvsClsgPric'}
            if columns - set(frame) or set(frame.TradDt.astype(str)) != {day}:
                raise ValueError('NSE file date/schema differs from requested session')
            equity = frame.loc[frame.SctySrs.eq('EQ')]
            if equity.TckrSymb.isna().any() or equity.TckrSymb.duplicated().any():
                raise ValueError('Nonunique EQ symbol in NSE file')
            prices = pd.to_numeric(equity.ClsPric, errors='coerce')
            if prices.isna().any() or prices.le(0).any():
                raise ValueError('Invalid official EQ close')
            path = output / name
            path.write_bytes(payload)
            meta.update(status='VERIFIED_OFFICIAL_EQ', sha256=digest(payload), member_name=members[0],
                member_sha256=digest(csv_bytes), total_rows=len(frame), eq_rows=len(equity), path=str(path))
            return meta, equity.set_index('TckrSymb')
        except Exception as exc:
            meta.update(status='UNAVAILABLE', error=f'{type(exc).__name__}: {exc}', attempts=attempt)
            if attempt == 1 and isinstance(exc, (urllib.error.URLError, TimeoutError)):
                time.sleep(0.3)
            else:
                break
    return meta, None


def run(output: Path):
    output = output.resolve()
    if output.exists() or output != DEFAULT_OUTPUT.resolve():
        raise ValueError('New isolated official-data output required')
    manifest_path = INPUTS / 'manifest.json'
    frozen = json.loads(manifest_path.read_text(encoding='utf-8'))
    days = frozen['days']
    if len(days) != 63 or days != sorted(set(days)):
        raise ValueError('Frozen G14 calendar differs')
    # Include every possible intervening weekday so an absent strategy date
    # cannot cause us to use the wrong preceding exchange session.
    candidates = list(pd.bdate_range(pd.Timestamp(days[0]) - pd.Timedelta(days=7), days[-1]).strftime('%Y-%m-%d'))
    output.mkdir(parents=True)
    results = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(download, day, output): day for day in candidates}
        for future in as_completed(futures):
            day = futures[future]
            result = future.result()
            results[day] = result
            print(day, result[0]['status'], flush=True)
    available = sorted(day for day, (meta, frame) in results.items() if frame is not None)
    stocks = {}
    for day in days:
        raw = pd.read_parquet(INPUTS / day / 'raw.parquet', columns=['tradingsymbol'])
        stocks[day] = sorted(set(raw.tradingsymbol.astype(str)))
    rows = []
    date_rows = []
    for day in days:
        prior = max((d for d in available if d < day), default=None)
        current_meta, current = results[day]
        previous = results[prior][1] if prior else None
        covered = 0
        for stock in stocks[day]:
            value = np.nan
            if previous is not None and stock in previous.index:
                value = float(previous.loc[stock, 'ClsPric'])
            if np.isfinite(value) and value > 0:
                covered += 1
            # Current-day PrvsClsgPric is an after-close diagnostic only. It
            # may encode corporate-action adjustments unavailable at open.
            hindsight_compare = (float(current.loc[stock, 'PrvsClsgPric'])
                if current is not None and stock in current.index else np.nan)
            rows.append(dict(day=day, tradingsymbol=stock, pdc=value, prior_day=prior,
                pdc_source='PREVIOUS_OFFICIAL_EQ_CLOSING_PRICE' if np.isfinite(value) else 'UNAVAILABLE',
                source_sha256=results[prior][0].get('sha256') if prior else None,
                source_url=results[prior][0]['url'] if prior else None,
                current_day_final_prev_close_diagnostic=hindsight_compare))
        date_rows.append(dict(day=day,prior_session=prior,source_date_available=current is not None,
            prior_file_available=previous is not None,stock_cohort=len(stocks[day]),pdc_covered_stocks=covered,
            pdc_coverage_pct=100*covered/len(stocks[day]) if stocks[day] else 0.))
    table = pd.DataFrame(rows)
    table.to_csv(output / 'pdc.csv', index=False)
    pd.DataFrame(date_rows).to_csv(output / 'pdc_coverage_by_day.csv', index=False)
    proof = dict(source='NSE_OFFICIAL_CM_EQ_BHAVCOPY', scope='RESEARCH_ONLY', execution_authority=False,
        frozen_input_manifest_sha256=digest(manifest_path.read_bytes()), represented_days=len(days),
        requested_weekdays=len(candidates), verified_files=len(available),
        missing_or_nontrading_weekdays=sorted(set(candidates)-set(available)),
        pdc_rows=len(table), pdc_covered_rows=int(table.pdc.notna().sum()),
        pdc_coverage_pct=float(100*table.pdc.notna().mean()),
        causal_rule='Use previous completed NSE official EQ close only; current-day final file is never a signal input.',
        current_day_prev_close_disagreement_rows=int((table.pdc.notna() &
            table.current_day_final_prev_close_diagnostic.notna() &
            ~np.isclose(table.pdc,table.current_day_final_prev_close_diagnostic,atol=1e-6,rtol=0)).sum()),
        fetches=[results[d][0] for d in sorted(results)])
    dump(output / 'source_manifest.json', proof)
    dump(output / 'artifact_manifest.json', dict(files={p.name:digest(p.read_bytes()) for p in output.iterdir() if p.is_file()}))
    print(json.dumps({k:proof[k] for k in ('represented_days','verified_files','pdc_rows','pdc_covered_rows',
        'pdc_coverage_pct','current_day_prev_close_disagreement_rows')}, indent=2))
    print(output)
    return proof


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    run(args.output)
