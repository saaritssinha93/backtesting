"""Create the isolated twelve-setup G14 configuration from G7 V4 parents."""
from pathlib import Path
import json
from research import g7_g6_replica_backtest as g7
from research.g14_morning_backtest import ROOT,CONFIG,SLOTS,sha,PARENT_RULES,inherited_gates

def main():
    if CONFIG.exists():raise FileExistsError(CONFIG)
    parent=g7.configuration(g7.DEFAULT_CONFIG)
    setups=[dict(x,source_version='V13-V10-G-14-MORNING12-V1') for x in parent['active_setups']
            if x['signal_end'] in SLOTS]
    if len(setups)!=12 or {(x['signal_end'],x['side']) for x in setups}!={(t,s) for t in SLOTS for s in ('LONG','SHORT')}:
        raise ValueError('G7 source lacks an exact morning direction pair')
    # Source V4 enforces the accepted 09:26 SHORT lower-wick floor before ranking.
    data=dict(variant_id='V13-V10-G-14-MORNING12-V1',strategy='V13-V10-G-14',scope='BACKTEST_ONLY',
        execution_authority=False,parent=dict(strategy='G7_V4_LOCK2',
            g7_v3_config_sha256=sha(g7.DEFAULT_CONFIG),
            g7_v4_lock_config_sha256=sha(ROOT/'configs/strategies/v13_v10_g_7_v4_lock_backtest.json')),
        active_setup_count=12,active_setups=setups,
        signal_to_confirmation={x['signal_end']:x['confirmation_end'] for x in setups},
        slot_selection=dict(mode='EXACT_MORNING_12',enabled_setup_ids=[x['setup_id'] for x in setups],
            excluded_other_g7_setup_count=len(parent['active_setups'])-len(setups)),
        preserved_parent_rules=PARENT_RULES,
        inherited_confirmation_gates=inherited_gates(parent,{x['setup_id'] for x in setups}),
        research_note='The G7 parent retains 51 selected opportunities in its audited ledger. Only these 12 setups are eligible in G14; exits and native fills are inherited, portfolio allocation is rerun.')
    CONFIG.write_text(json.dumps(data,indent=2)+'\n',encoding='utf-8')
    print(CONFIG)

if __name__=='__main__':main()
