from pathlib import Path
import hashlib
import json
import shutil
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
OUT = Path(__file__).resolve().parent
SOURCE = Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_4/target_sweeps/2026-10-13/20261013_afternoon_dual70_targets_v5')
old_version = 'V13-V10-G-3-PRODUCTION-V6'
old_fp = '7d01e80f60cc874e7c7b94a894785d134b70df71b27be98fdf3c302a96f44b4a'
new_version = 'V13-V10-G-3-PRODUCTION-V7'
names = ['fno_v13_v10_g_3_contract.py', 'fno_v13_v10_g_3_live_config.py',
         'fno_v13_v10_g_3_policy.py', 'configs/strategies/active_profile.json',
         'docs/ai_platform/profile_registry.json', 'configs/strategies/v13_v10_g_3_production_v6.json']
for name in names:
    target = OUT / 'before' / name
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        raise RuntimeError(f'Backup exists, refuse repeat promotion: {target}')
    shutil.copy2(ROOT / name, target)
archive = ROOT / 'research/archived_g3_v6/contract.py'
archive.parent.mkdir(parents=True, exist_ok=True)
assert not archive.exists()
shutil.copy2(ROOT / names[0], archive)
source_bytes = (SOURCE / 'refined_config.json').read_bytes()
source = json.loads(source_bytes)
assert source['require_native70'] is True and len(source['setups']) == 15
assert {s['setup_id']: s['target_pct'] for s in source['setups']} == {
    '1236_LONG': .37, '1236_SHORT': 1.24, '1246_LONG': .23, '1246_SHORT': .49,
    '1251_LONG': .26, '1251_SHORT': .14, '1256_SHORT': .14, '1301_SHORT': .88,
    '1306_LONG': .18, '1316_LONG': .17, '1316_SHORT': .33, '1321_SHORT': 1.01,
    '1326_LONG': .41, '1326_SHORT': .76, '1331_LONG': 1.17}
text = '''"""The fifteen approved afternoon setups shared by all G3 decision paths.

Effective thresholds and targets are copied from the sealed G4 dual-70 result.
SHORT prices receive no legacy multiplier. Import performs no I/O.
"""
'''
variant = 'G3_W1_V1p1_0950_LONG_T075_0920_LONG_T300_LATE_SIX_SELECTED_TEN_MIDDAY_TWELVE_AFTERNOON_FIFTEEN'
text += f'VERSION = {new_version!r}\nACCEPTED_VARIANT = {variant!r}\n'
text += f'SOURCE_CONFIG_SHA256 = {hashlib.sha256(source_bytes).hexdigest()!r}\n'
text += '# signal, side, quota, picker, price, OI, 5m volume, 1m body, 1m wick, target\nROWS = (\n'
for s in source['setups']:
    t = s['thresholds']
    assert (s['initial_stop_pct'], s['tightened_stop_pct'], s['tighten_after_minutes'], s['max_entries'], s['picker']) == (1.25, 1., 120, 1, 'max_liquidity')
    row = (s['signal_end'], s['side'], s['max_entries'], s['picker'], t['price_change_pct'], t['oi_change_pct'], t['five_minute_volume_ratio_min'], t['one_minute_body_ratio_min'], t['one_minute_wick_ratio_max'], s['target_pct'])
    text += f'    {row!r},\n'
text += ')\n'
for name, values in [('CONFIRMATION_VOLUME', {s['setup_id']: s['thresholds']['one_minute_volume_ratio_min'] for s in source['setups']}), ('TARGETS', {s['setup_id']: s['target_pct'] for s in source['setups']}), ('STAGES', {s['setup_id']: s['stage'] for s in source['setups']})]:
    text += name + ' = {\n' + ''.join(f'    {k!r}: {v!r},\n' for k,v in values.items()) + '}\n'
text += 'SETUP_IDS = frozenset(CONFIRMATION_VOLUME)\n'
(ROOT / 'fno_v13_v10_g_3_afternoon_setups.py').write_text(text, encoding='utf-8')

p = ROOT / names[0]
text = p.read_text(encoding='utf-8')
text = text.replace('import fno_v13_v10_g_3_midday_setups as midday_setups', 'import fno_v13_v10_g_3_midday_setups as midday_setups\nimport fno_v13_v10_g_3_afternoon_setups as afternoon_setups')
text = text.replace('STRATEGY_VERSION = midday_setups.VERSION', 'STRATEGY_VERSION = afternoon_setups.VERSION')
text = text.replace('ACCEPTED_VARIANT = midday_setups.ACCEPTED_VARIANT', 'ACCEPTED_VARIANT = afternoon_setups.ACCEPTED_VARIANT')
text = text.replace('PREVIOUS_STRATEGY_VERSION = "V13-V10-G-3-PRODUCTION-V5"', f'PREVIOUS_STRATEGY_VERSION = "{old_version}"')
text = text.replace('PREVIOUS_STRATEGY_FINGERPRINT = "fcb7c981b518b8c9e427155cdfb06e3cc34c6ed16525e78514e6ecf3afab2aea"', f'PREVIOUS_STRATEGY_FINGERPRINT = "{old_fp}"')
text = text.replace('**midday_setups.CONFIRMATION_VOLUME}', '**midday_setups.CONFIRMATION_VOLUME, **afternoon_setups.CONFIRMATION_VOLUME}')
text = text.replace('    *midday_setups.ROWS,\n)', '    *midday_setups.ROWS,\n    *afternoon_setups.ROWS,\n)')
text = text.replace(' | midday_setups.SETUP_IDS)', ' | midday_setups.SETUP_IDS | afternoon_setups.SETUP_IDS)')
text = text.replace('*selected_setups.ROWS, *midday_setups.ROWS)', '*selected_setups.ROWS, *midday_setups.ROWS, *afternoon_setups.ROWS)')
text = text.replace('*selected_setups.SETUP_IDS, *midday_setups.SETUP_IDS}', '*selected_setups.SETUP_IDS, *midday_setups.SETUP_IDS, *afternoon_setups.SETUP_IDS}')
text = text.replace('execution requires V6', 'execution requires V7')
text = text.replace('                (PREVIOUS_STRATEGY_VERSION, PREVIOUS_STRATEGY_FINGERPRINT)})', '                ("V13-V10-G-3-PRODUCTION-V5", "fcb7c981b518b8c9e427155cdfb06e3cc34c6ed16525e78514e6ecf3afab2aea"),\n                (PREVIOUS_STRATEGY_VERSION, PREVIOUS_STRATEGY_FINGERPRINT)})')
text = text.replace('len(ACTIVE_SETUPS) != 44 or len(_ACTIVE_BY_ID) != 44', 'len(ACTIVE_SETUPS) != 59 or len(_ACTIVE_BY_ID) != 59').replace('G3 V6 requires exactly 44', 'G3 V7 requires exactly 59')
p.write_text(text, encoding='utf-8')

p = ROOT / 'fno_v13_v10_g_3_live_config.py'
text = p.read_text(encoding='utf-8').replace(old_version, new_version).replace('production_v6.json', 'production_v7.json')
text = text.replace('LATE_SIX_SELECTED_TEN_MIDDAY_TWELVE"', 'LATE_SIX_SELECTED_TEN_MIDDAY_TWELVE_AFTERNOON_FIFTEEN"')
text = text.replace('midday_twelve_setups=True)', 'midday_twelve_setups=True, afternoon_fifteen_setups=True)')
p.write_text(text, encoding='utf-8')
p = ROOT / 'fno_v13_v10_g_3_policy.py'
text = p.read_text(encoding='utf-8').replace('MIDDAY_TWELVE_STAGED125_100_120M_V6', 'MIDDAY_TWELVE_AFTERNOON_FIFTEEN_STAGED125_100_120M_V7')
p.write_text(text, encoding='utf-8')

import fno_v13_v10_g_3_contract as contract
contract.validate_contract()
new_fp = contract.strategy_fingerprint()
(ROOT / 'configs/strategies/v13_v10_g_3_production_v7.json').write_bytes((json.dumps(contract.strategy_payload(), indent=2, sort_keys=True) + '\n').encode())
for name in ['configs/strategies/active_profile.json', 'docs/ai_platform/profile_registry.json']:
    p = ROOT / name
    text = p.read_text(encoding='utf-8').replace(old_version, new_version).replace(old_fp, new_fp).replace('production_v6.json', 'production_v7.json')
    text = text.replace('LATE_SIX_SELECTED_TEN_MIDDAY_TWELVE"', 'LATE_SIX_SELECTED_TEN_MIDDAY_TWELVE_AFTERNOON_FIFTEEN"').replace('G3_MIDDAY_TWELVE_PRODUCTION_2026-10-13.md', 'G3_AFTERNOON_FIFTEEN_PRODUCTION_2026-10-13.md')
    value = json.loads(text)
    def update(item):
        if isinstance(item, dict):
            for k,v in item.items():
                if k == 'active_setup_count' and v == 44: item[k] = 59
                elif k == 'signal_to_confirmation' and v == json.loads((OUT/'before/configs/strategies/v13_v10_g_3_production_v6.json').read_text())['signal_to_confirmation']: item[k] = contract.SIGNAL_TO_CONFIRMATION
                elif k == 'pipeline_deadline' and v == '12:33': item[k] = '13:33'
                else: update(v)
        elif isinstance(item, list):
            for v in item: update(v)
    update(value)
    p.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')
import fno_v13_v10_g_3_live_config as runtime
runtime.validate_strategy()
receipt = dict(version=contract.STRATEGY_VERSION, fingerprint=new_fp, setups=len(contract.ACTIVE_SETUPS), clocks=len(contract.SIGNAL_TO_CONFIRMATION), pipeline_deadline=runtime.PIPELINE_DEADLINE, source_config_sha256=hashlib.sha256(source_bytes).hexdigest(), source_manifest_sha256=hashlib.sha256((SOURCE/'manifest.json').read_bytes()).hexdigest(), previous_fingerprint=old_fp)
(OUT/'contract_promotion.json').write_text(json.dumps(receipt, indent=2)+'\n', encoding='utf-8')
print(json.dumps(receipt, indent=2))
