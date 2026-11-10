"""Read frozen all-opportunity ledgers; derive the user's exact inclusive 70% mask."""
from pathlib import Path
import json
import hashlib
import sys
import pandas as pd

ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT))
OUT=Path(__file__).resolve().parent
SOURCE=Path('C:/TradingData/eqidv2/backtesting_result_v13_v10_g_7/v4_lock_backtest/runs/2026-10-28/20261028T143059782924_f86f1197_0926_short_wick020_lock2')
FILES={'G3':'g3_reference_portfolio_trades.csv','G7_V4_LOCK2':'portfolio_trades.csv'}
TEN={'0931_LONG','0931_SHORT','1001_LONG','1051_LONG','1111_SHORT','1146_SHORT','1226_SHORT','1231_SHORT','1301_SHORT','1321_SHORT'}

def flags(x):
    text=x.astype(str).str.lower()
    if not text.isin(['true','false']).all(): raise ValueError('invalid booleans')
    return text.eq('true')

def main():
    rows=[];sources=[];books={}
    for strategy,filename in FILES.items():
        path=SOURCE/filename
        sources.append(dict(strategy=strategy,path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
        frame=pd.read_csv(path,low_memory=False)
        books[strategy]=frame
        for slot,g in frame.groupby('setup_id',sort=True):
            executed=g.loc[flags(g.portfolio_executed)]
            pnl=pd.to_numeric(executed.portfolio_net_profit_rupees,errors='raise')
            count=len(pnl); wins=int(pnl.gt(1e-8).sum());losses=int(pnl.lt(-1e-8).sum())
            rows.append(dict(strategy=strategy,setup_id=slot,slot=slot[:2]+':'+slot[2:4],side=slot.split('_')[1],opportunities=len(g),trades=count,wins=wins,losses=losses,breakeven=count-wins-losses,win_rate_pct=100*wins/count if count else None,selected=bool(count>0 and 100*wins>=70*count),in_preceding_ten=slot in TEN,net_pnl_rs=float(pnl.sum())))
    table=pd.DataFrame(rows)
    table.to_csv(OUT/'all_slot_selection.csv',index=False)
    table[table.in_preceding_ten].to_csv(OUT/'preceding_ten_selection.csv',index=False)
    payload=dict(source_dir=str(SOURCE),period_start='2026-07-29',period_end='2026-10-27',sessions=59,threshold_pct=70,comparison='100 * net_wins >= 70 * executed_trades; executed_trades > 0',win_definition='portfolio_net_profit_rupees > 1e-8',source_hashes=sources,selection_uses_outcomes=True,selection_is_retrospective=True,all_slots={},only_preceding_ten={},keep_other_slots={})
    for strategy in FILES:
        part=table[table.strategy.eq(strategy)]
        payload['all_slots'][strategy]=part.loc[part.selected,'setup_id'].tolist()
        payload['only_preceding_ten'][strategy]=part.loc[part.selected&part.in_preceding_ten,'setup_id'].tolist()
        payload['keep_other_slots'][strategy]=part.loc[part.selected|~part.in_preceding_ten,'setup_id'].tolist()
    (OUT/'selection.json').write_text(json.dumps(payload,indent=2),encoding='utf8')
    print(table.to_string(index=False))
    print(json.dumps(payload,indent=2))

if __name__=='__main__':main()
