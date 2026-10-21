from pathlib import Path
import json
import shutil

root = Path(__file__).resolve().parents[2]
backup = Path(__file__).parent / 'before'
for name in ('fno_v13_v10_g_3_contract.py', 'fno_v13_v10_g_3_policy.py',
             'fno_v13_v10_g_3_live_config.py', 'configs/strategies/active_profile.json',
             'docs/ai_platform/profile_registry.json'):
    target = backup / name
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(root / name, target)

source = json.loads((root / 'configs/strategies/v13_v10_g_4_backtest_selected_midday_v5.json').read_text())
rows, volumes, targets, stages = [], {}, {}, {}
for setup in source['setups']:
    level = setup['thresholds']
    rows.append((setup['signal_end'], setup['side'], setup['max_entries'], setup['picker'],
                 level['price_change_pct'], level['oi_change_pct'], level['five_minute_volume_ratio_min'],
                 level['one_minute_body_ratio_min'], level['one_minute_wick_ratio_max'], setup['target_pct']))
    volumes[setup['setup_id']] = level['one_minute_volume_ratio_min']
    targets[setup['setup_id']] = setup['target_pct']
    stages[setup['setup_id']] = setup['stage']
text = '''"""The twelve approved midday setups shared by all G3 decision paths.

Thresholds are effective values; SHORT prices receive no legacy multiplier.
Importing this module performs no file, clock, broker or network access.
"""
VERSION = "V13-V10-G-3-PRODUCTION-V6"
ACCEPTED_VARIANT = "G3_W1_V1p1_0950_LONG_T075_0920_LONG_T300_LATE_SIX_SELECTED_TEN_MIDDAY_TWELVE"
# signal, side, quota, picker, price, OI, 5m volume, 1m body, 1m wick, target
ROWS = (
'''
text += ''.join('    ' + repr(row) + ',\n' for row in rows) + ')\n'
for name, values in (('CONFIRMATION_VOLUME', volumes), ('TARGETS', targets), ('STAGES', stages)):
    text += name + ' = {\n' + ''.join(f'    {key!r}: {value!r},\n' for key, value in values.items()) + '}\n'
text += 'SETUP_IDS = frozenset(CONFIRMATION_VOLUME)\n'
(root / 'fno_v13_v10_g_3_midday_setups.py').write_text(text, encoding='utf-8')
print('Backed up identity files and generated exact twelve rules')
