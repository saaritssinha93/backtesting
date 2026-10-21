"""Reconstruct and seal the existing G3 source once for exit-only research."""
from pathlib import Path
import sys, json, hashlib
import numpy as np
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from research import g3_trade_monitor_backtest as fixed
from research import g3_target_cutoff_backtest as history
from research.g4_extended_target_sweep import read_frozen_csv
from datetime import date

SOURCE = Path(r'C:\TradingData\eqidv2\backtesting_result_v13_v10_g_3\backtests\runs\2026-10-15\20261015T171145325060_264a0ea2_current_full_history')
OUT = Path(r'C:\TradingData\eqidv2\g3_sl_target_research\20261016\source')
def main():
    if OUT.exists():
        raise SystemExit('Source cache already exists; do not overwrite sealed evidence')
    root, source, summary, manifest = fixed._resolve_source(SOURCE)
    orders = read_frozen_csv(source / 'selected_orders.csv')
    saved = read_frozen_csv(source / 'portfolio_trades.csv')
    days = [date.fromisoformat(x) for x in summary['days']]
    paths, proof = fixed._paths_for_source(root, orders, days, summary)
    fresh = history._simulate(orders, paths, days, history.contract)
    history._same_native(fresh, saved)
    history._same_allocation(fresh, saved)
    OUT.mkdir(parents=True)
    orders.to_csv(OUT / 'selected_orders.csv', index=False)
    fresh.to_csv(OUT / 'baseline.csv', index=False)
    np.savez_compressed(OUT / 'paths.npz', **{f'{sid}__{key}': value for sid, path in paths.items() for key, value in path.items()})
    meta = dict(source=str(source), source_manifest_sha256=history.sha256(source/'manifest.json'),
                summary=summary, input_proof=proof, native_parity=True, allocation_parity=True,
                files={name:history.sha256(OUT/name) for name in ['selected_orders.csv','baseline.csv','paths.npz']})
    (OUT / 'source.json').write_text(json.dumps(history.replay.json_ready(meta), indent=2), encoding='utf-8')
    print(json.dumps(dict(cache=str(OUT), days=len(days), orders=len(orders), executions=int(fresh.portfolio_executed.sum())), indent=2), flush=True)


if __name__=='__main__':
    main()
