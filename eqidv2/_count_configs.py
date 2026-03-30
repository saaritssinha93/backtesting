import sys
sys.path.insert(0, '.')
sys.path.insert(0, '..')

# Load sweep module and count configs
exec(open('v15_long_param_sweep.py').read())
configs = _build_configs()
print(f"Total configs: {len(configs)}")

by_phase = {}
for c in configs:
    name = c['name']
    if name == 'BASELINE': ph = 'BASELINE'
    elif name.startswith('SL') and 'TGT' in name and name.count('_') == 1: ph = 'SL_TGT'
    elif name.startswith('SL') and 'TGT' in name: ph = 'ENTRY_MODE'
    elif name.startswith('ADX'): ph = 'ADX_RSI'
    elif name.startswith('AVWAP'): ph = 'AVWAP'
    elif name.startswith('EMA'): ph = 'EMA_GAP'
    elif name.startswith('QS'): ph = 'QS_MIN'
    elif name.startswith('LAG'): ph = 'LAG'
    elif name.startswith('WIN_END'): ph = 'WIN_END'
    elif name.startswith('RS_THR'): ph = 'RS_THR'
    elif name.startswith('BE'): ph = 'BE_TRAIL'
    elif name.startswith('SETUP'): ph = 'SETUP'
    elif name.startswith('COMBO'): ph = 'COMBO'
    else: ph = 'OTHER'
    by_phase[ph] = by_phase.get(ph, 0) + 1

for ph, cnt in sorted(by_phase.items(), key=lambda x: -x[1]):
    print(f"  {ph:<20}: {cnt}")

# Spot-check one config
cfg = _make_long_config(configs[0])
print(f"\nBaseline config fields:")
print(f"  stop_pct={cfg.stop_pct}, target_pct={cfg.target_pct}")
print(f"  adx_min={cfg.adx_min}, rsi_min_long={cfg.rsi_min_long}")
print(f"  signal_avwap_dist_atr_min={cfg.signal_avwap_dist_atr_min}")
print(f"  ema_gap_atr_min={cfg.ema_gap_atr_min}")
print(f"  quality_score_min={cfg.quality_score_min}")
print(f"  lag_b_huge={cfg.lag_bars_long_b_huge_c1_close_reclaim_break}")
print(f"  signal_windows={cfg.signal_windows}")
print("All OK!")
