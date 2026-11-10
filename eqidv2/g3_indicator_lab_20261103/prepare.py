"""Freeze hash-verified full inputs into this isolated lab only."""
from __future__ import annotations
from pathlib import Path
import argparse, json, hashlib, sys
import numpy as np
import pandas as pd
from g3_v18_20261102 import data, features as v18
from g3_v18_20261102.run import sha, dump, guard_writes
from .features import add_day

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = Path(__file__).resolve().parent

def run(output: Path):
    output = output.resolve()
    if not output.is_relative_to(PACKAGE) or output.exists():
        raise ValueError('New lab child output required')
    days = data.available_days('2026-11-02')
    protocol = json.loads((PACKAGE/'protocol.json').read_text())
    reference = ROOT/'g3_v18_20261102/results_20261102_complete'
    manifest = json.loads((reference/'manifest.json').read_text())
    cached_file = reference/'candidate_features.parquet'
    if sha(cached_file) != manifest['files'][cached_file.name]:
        raise ValueError('V18 feature source hash changed')
    cached = pd.read_parquet(cached_file)
    cached['signal_ts'] = pd.to_datetime(cached.signal_ts, utc=True).dt.tz_convert('Asia/Kolkata')
    cached['day'] = cached.day.astype(str).str[:10]
    spot_path = data.SPOT
    spot = pd.read_parquet(spot_path)
    spot['ts'] = pd.to_datetime(spot.date, utc=True).dt.tz_convert('Asia/Kolkata')
    guard_writes(PACKAGE)
    output.mkdir(parents=True)
    (output/'protocol.json').write_bytes((PACKAGE/'protocol.json').read_bytes())
    spot.to_parquet(output/'nifty_spot_snapshot.parquet', index=False)
    proof = dict(protocol_sha256=sha(PACKAGE/'protocol.json'), days=days,
        saved_v18_features_sha256=sha(cached_file), spot_source=str(spot_path),
        spot_source_sha256=sha(spot_path), sessions=[], frozen_before_selection=True)
    dump(output/'input_plan.json', proof)
    history = {}
    for n, day in enumerate(days):
        folder = output/day
        folder.mkdir()
        raw, minutes, provenance = data.load_day(day)
        raw = raw.copy().reset_index(drop=True)
        raw['sid'] = n*1_000_000 + np.arange(len(raw), dtype=np.int64)
        raw['signal_ts'] = pd.to_datetime(raw.signal_ts, utc=True).dt.tz_convert('Asia/Kolkata')
        old = cached.loc[cached.day.eq(day)]
        if len(old):
            extras = [c for c in old if c.startswith('v18_')]
            raw = raw.merge(old[['tradingsymbol','signal_ts',*extras]], on=['tradingsymbol','signal_ts'], how='left',validate='one_to_one')
            if len(raw) != len(old) or raw.v18_stock_ready.isna().any():
                raise ValueError(f'Historical full candidate source differs from V18: {day}')
        else:
            context = data.context_bars(raw, minutes)
            universe = provenance.get('breadth_universe',{}).get('count') or provenance.get('universe',{}).get('universe_symbols') or context.tradingsymbol.nunique()
            enriched = v18.enrich_day(context, spot.loc[spot.ts.dt.strftime('%Y-%m-%d').eq(day)], expected_universe=int(universe))
            extras = [c for c in enriched if c.startswith('v18_')]
            raw = raw.merge(enriched[['tradingsymbol','signal_ts',*extras]],on=['tradingsymbol','signal_ts'],how='left',validate='one_to_one')
        raw = add_day(raw, history)
        # Normalize object timestamps before Arrow serialization.
        for name in raw:
            if raw[name].dtype == object:
                raw[name] = raw[name].map(lambda x: str(x) if isinstance(x, (dict,list)) else x)
        raw.to_parquet(folder/'raw.parquet',index=False)
        minutes.to_parquet(folder/'minutes.parquet',index=False)
        dump(folder/'source_provenance.json',provenance)
        record = dict(day=day,rows=len(raw),symbols=raw.tradingsymbol.nunique(),
            raw_sha256=sha(folder/'raw.parquet'),minutes_sha256=sha(folder/'minutes.parquet'),
            source_provenance_sha256=sha(folder/'source_provenance.json'))
        proof['sessions'].append(record)
        print(f'Prepared {day} [{n+1}/{len(days)}] {len(raw)} raw rows',flush=True)
    dump(output/'manifest.json',proof)
    return proof

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    result = run(args.output)
    print(json.dumps(dict(sessions=len(result['sessions']),last=result['days'][-1])))
