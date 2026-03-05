import sys
import time
import pandas as pd
sys.path.insert(0, r'c:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2')
import eqidv2_live_combined_analyser_csv_v5_unified as m
import eqidv2_live_combined_analyser_csv_v2 as v2
m._apply_v5_unified_overrides()
v2._current_15m_slot_start_ist = lambda: pd.Timestamp('2026-03-02 11:30:00+05:30')
t0=time.perf_counter()
checks, signals = v2.run_one_scan('BENCH')
elapsed=time.perf_counter()-t0
print(f'BENCH_ELAPSED_SEC={elapsed:.3f}')
print(f'BENCH_CHECKS={len(checks)}')
print(f'BENCH_SIGNALS={len(signals)}')
