"""Finalize verified cache after metadata Path serialization failed; replay again."""
from pathlib import Path
import sys,json
import numpy as np
import pandas as pd
from datetime import date
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from research import g3_trade_monitor_backtest as fixed
from research import g3_target_cutoff_backtest as history
from research.g4_extended_target_sweep import read_frozen_csv

def main():
    source=Path(r'C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\backtests\runs\2026-10-15\20261015T171145325060_264a0ea2_current_full_history')
    out=Path(r'C:\TradingData\eqidv2\g3_sl_target_research\20261016\source')
    if (out/'source.json').exists():raise ValueError('Do not overwrite completed cache')
    root,source,summary,manifest=fixed._resolve_source(source)
    orders=read_frozen_csv(out/'selected_orders.csv')
    pd.testing.assert_frame_equal(orders,read_frozen_csv(source/'selected_orders.csv'),check_dtype=False)
    cached=read_frozen_csv(out/'baseline.csv');saved=read_frozen_csv(source/'portfolio_trades.csv')
    paths={}
    with np.load(out/'paths.npz',allow_pickle=False) as archive:
        for name in archive.files:
            sid,key=name.split('__',1);paths.setdefault(int(sid),{})[key]=archive[name]
    days=[date.fromisoformat(x) for x in summary['days']]
    fresh=history._simulate(orders,paths,days,history.contract)
    for expected in (cached,saved):
        history._same_native(fresh,expected);history._same_allocation(fresh,expected)
    proof=dict(reconstruction_method='g3_trade_monitor_backtest._paths_for_source',
        reconstruction_completed_before_metadata_serialization_error=True,
        recovery='Original builder verified all source members and native/allocation parity, then wrote all CSV/NPZ cache files before JSON Path serialization failed. This finalizer rechecks original manifest, selected order equality, and replays cached paths against both cached and original native/allocated ledgers.',
        original_manifest=str(source/'manifest.json'),original_summary_sha256=history.sha256(source/'summary.json'),
        source_builder_sha256=history.sha256(Path(__file__).with_name('build_source.py')),
        finalizer_sha256=history.sha256(Path(__file__)))
    meta=dict(source=str(source),source_manifest_sha256=history.sha256(source/'manifest.json'),
        summary=summary,input_proof=proof,native_parity=True,allocation_parity=True,
        files={name:history.sha256(out/name) for name in ['selected_orders.csv','baseline.csv','paths.npz']})
    (out/'source.json').write_text(json.dumps(history.replay.json_ready(meta),indent=2,allow_nan=False),encoding='utf-8')
    print(json.dumps(dict(cache=str(out),days=len(days),orders=len(orders),executions=int(fresh.portfolio_executed.sum())),indent=2))

if __name__=='__main__':main()
