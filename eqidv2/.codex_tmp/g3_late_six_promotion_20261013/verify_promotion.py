"""Read-only verification of the historical-to-runtime G3 V4 promotion."""
from pathlib import Path
import sys
import json
import hashlib

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import pandas as pd
import numpy as np
import fno_v13_v10_g_3_contract as contract
import backtesting_result_v13_v10_g_3_daily as publisher
from research.g3_current_backtest import verified_package

root = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3')
output = root / 'readiness/2026-10-13/late_six_v4'
pointer = json.loads((root / 'latest/current_backtest.json').read_text())
historical = root / pointer['run_dir']
summary, _ = verified_package(historical)
first = root / 'backtests/runs/2026-10-12/20261013T013727281169_ed40a0e9_current_late_six'
for name in ('variant_trades.csv', 'added_six_trades.csv', 'daily_results.csv', 'setup_results.csv', 'config.json'):
    assert (first / name).read_bytes() == (historical / name).read_bytes(), name
run, manifest = publisher._read_verified_pointer(root, 'current_run.json', 'production_daily')
assert manifest['strategy_fingerprint'] == contract.strategy_fingerprint()
source = json.loads((run / 'source_manifest.json').read_text())
assert source['strategy_contract'] == contract.strategy_payload() == json.loads((historical / 'config.json').read_text())
assert all(hashlib.sha256(Path(item['path']).read_bytes()).hexdigest() == item['sha256'] for item in source['code'])
current = pd.read_csv(run / 'portfolio_trades.csv')
prior = pd.read_csv(root / 'production/runs/2026-10-12/20261013T011201132377_8407b0c0/portfolio_trades.csv')
keys = ['tradingsymbol', 'setup_id']
current = current.sort_values(keys).reset_index(drop=True)
prior = prior.sort_values(keys).reset_index(drop=True)
assert current[keys].equals(prior[keys])
for name in ('entry_price', 'exit_price', 'portfolio_net_profit_rupees'):
    assert np.allclose(current[name], prior[name], atol=1e-6, rtol=0), name
history = pd.read_csv(historical / 'variant_trades.csv')
oct12 = history[history.day.eq('2026-10-12')].sort_values(keys).reset_index(drop=True)
assert current[keys].equals(oct12[keys])
assert np.allclose(current.portfolio_net_profit_rupees, oct12.portfolio_net_profit_rupees, atol=1e-6, rtol=0)
audit = pd.read_csv(run / 'selection_audit.csv')
late = audit[audit.setup_id.isin(contract.late_setups.SETUP_IDS)]
checks = dict(strategy_version=contract.STRATEGY_VERSION, strategy_fingerprint=contract.strategy_fingerprint(),
    normal_backtest=str(historical), production_daily=str(run), two_normal_runs_identical=True,
    production_config_equals_historical=True, production_code_hashes_current=True,
    existing_daily_trades_unchanged=True, daily_matches_historical=True, daily_metrics=manifest['metrics'],
    late_candidate_counts=late.groupby('setup_id').size().to_dict(), late_selected_count=int(late.v9_selected.sum()))
(output / 'daily_replay_parity.json').write_text(json.dumps(checks, indent=2) + '\n')
print(json.dumps(checks, indent=2))
