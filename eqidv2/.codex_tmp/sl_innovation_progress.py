"""Completed-five-minute progress/recovery ratchets; research, not live settings."""
from __future__ import annotations
import json
import numpy as np
import sl_innovation_common as common

MINUTE=common.MINUTE

def exit_progress(path,entry_index,entry,is_long,target_pct,rule):
    sign=1 if is_long else -1
    target=entry*(1+sign*target_pct/100)
    origin=int(path['timestamp_ns'][entry_index])
    hard=rule['hard_stop']
    stop_distance=hard
    close_peak=0.
    pullback_seen=False
    for j in range(entry_index,len(path['close'])):
        end=int(path['timestamp_ns'][j])
        if rule.get('tighten_minutes') is not None and (end-MINUTE-origin)/MINUTE>=rule['tighten_minutes']:
            stop_distance=min(stop_distance,1.)
        stop=entry*(1-sign*stop_distance/100)
        op,hi,lo=(float(path[key][j]) for key in ('open','high','low'))
        stop_open=op<=stop if is_long else op>=stop
        stop_hit=lo<=stop if is_long else hi>=stop
        target_hit=hi>=target if is_long else lo<=target
        if stop_hit:
            fill=op if j>entry_index and stop_open else stop
            return j,fill,'TIGHTENED_STOP' if stop_distance<hard-1e-10 else 'STOP',stop_distance,'OPEN' if j>entry_index and stop_open else 'INTRABAR'
        if target_hit:
            return j,target,'TARGET',stop_distance,'INTRABAR'
        if j==len(path['close'])-1:
            # Session close exits before any newly observed protection could
            # become active. Preserve the actually resting stop in the ledger.
            break
        # A full exchange-aligned five-minute interval must be after the
        # conservative entry-end proxy. No pre-entry partial bar can arm a stop.
        if end//MINUTE % 5 != 0 or end-origin < 5*MINUTE:
            continue
        close_return=sign*(float(path['close'][j])/entry-1)*100
        close_peak=max(close_peak,close_return)
        mode=rule['mode']
        if mode=='STEP' and close_return>=rule['arm_pct']-1e-10:
            stop_distance=min(stop_distance,-rule['lock_pct'])
        elif mode=='FRACTION' and close_peak>=rule['arm_pct']-1e-10:
            stop_distance=min(stop_distance,-close_peak*rule['retain_fraction'])
        elif mode=='RECOVERY':
            if close_return<=-rule['adverse_pct']+1e-10:
                pullback_seen=True
            if pullback_seen and close_return>=.1-1e-10:
                stop_distance=min(stop_distance,rule['recovered_stop_pct'])
        # A new distance is only checked against the NEXT bar; no retroactive
        # fill using this completed candle's low/high or its earlier open.
    return len(path['close'])-1,float(path['close'][-1]),'TIME_EXIT_1515',stop_distance,'CLOSE'

def synthetic_checks():
    def path(rows):
        return {'timestamp_ns':np.arange(1,len(rows)+1,dtype=np.int64)*MINUTE,
            **{key:np.array([r[i] for r in rows],dtype=float) for i,key in enumerate(('open','high','low','close'))}}
    base=dict(hard_stop=1.25,mode='STEP',arm_pct=.5,lock_pct=.1)
    # First full 5-minute close after entry end 1 is end10. Its low precedes
    # activation, so stop fills at end11 open, not retrospectively at end10 low.
    rows=[(100,100.2,99.8,100)]*9+[(100,100.8,99.4,100.6),(100.0,100.2,99.9,100.1)]
    p=path(rows)
    r=exit_progress(p,0,100,True,2,base)
    assert r[:3]==(10,100.,'TIGHTENED_STOP') and r[4]=='OPEN',r
    # Incomplete first5min candle cannot arm even with a high close.
    p=path([(100,100.8,99.8,100.6)]*5+[(100,100.2,99.8,100)])
    assert exit_progress(p,0,100,True,2,base)[2]=='TIME_EXIT_1515'
    # Favorable high without a favorable completed close cannot activate.
    p=path([(100,100.8,99.8,100)]*11)
    assert exit_progress(p,0,100,True,2,base)[2]=='TIME_EXIT_1515'
    # Price gap through a newly protected stop executes at actual worse open.
    rows[-1]=(99,100,98.9,99.5)
    assert exit_progress(path(rows),0,100,True,2,base)[:2]==(10,99.)
    # Short symmetry and correct protected-price sign.
    rows=[(100,100.2,99.8,100)]*9+[(100,100.6,99.2,99.4),(100.,100.1,99.8,99.9)]
    r=exit_progress(path(rows),0,100,False,2,base)
    assert r[:3]==(10,100.,'TIGHTENED_STOP') and r[4]=='OPEN',r
    # Original hard stop is active before any arming.
    p=path([(100,103,98,100)])
    assert exit_progress(p,0,100,True,2,base)[:3]==(0,98.75,'STOP')
    # Hard-stop and target touching in same minute remains stop-first.
    assert exit_progress(p,0,100,False,2,base)[:3]==(0,101.25,'STOP')
    # Recovery has to follow an observed adverse close, not merely a low wick.
    recovery=dict(hard_stop=1.25,mode='RECOVERY',adverse_pct=.75,recovered_stop_pct=.25)
    rows=[(100,100.2,99.1,100)]*9+[(100,100.2,99.1,100.1)]+[(100,100.2,99.6,100)]*5
    assert exit_progress(path(rows),0,100,True,2,recovery)[2]=='TIME_EXIT_1515'
    # Confirmed adverse and recovered closes tighten on subsequent bar only.
    rows=[(100,100.2,99.8,100)]*9+[(100,100.1,99.1,99.2)]+[(99.2,100,99.1,99.8)]*4+[(99.8,100.3,99.2,100.2),(99.6,99.9,99.5,99.8)]
    assert exit_progress(path(rows),0,100,True,2,recovery)[:3]==(15,99.6,'TIGHTENED_STOP')
    # Trailing floor cannot loosen after close peak falls.
    trail=dict(hard_stop=1.25,mode='FRACTION',arm_pct=.5,retain_fraction=.5)
    rows=[(100,100.1,99.9,100)]*9+[(100,101.1,99.9,101)]+[(101,101.1,100.6,100.7)]*5+[(100.4,100.6,100.3,100.5)]
    r=exit_progress(path(rows),0,100,True,2,trail)
    assert r[:3]==(15,100.4,'TIGHTENED_STOP') and abs(r[3]+.5)<1e-8,r
    # End-of-session close does not fabricate a future execution.
    assert exit_progress(path(rows[:10]),0,100,True,2,trail)[2]=='TIME_EXIT_1515'
    return 11

def rules():
    result=[]
    for arm in (.5,.75,1.,1.25):
        for lock in (0.,.1,.25):
            result.append(dict(name=f'STEP_A{arm:.2f}_LOCK{lock:.2f}_STAGED',family='PROGRESS',
                mode='STEP',hard_stop=1.25,tighten_minutes=120,arm_pct=arm,lock_pct=lock))
    for arm in (.75,1.,1.25):
        for retain in (.25,.5,.75):
            result.append(dict(name=f'TRAIL_A{arm:.2f}_KEEP{retain:.2f}_STAGED',family='PROGRESS',
                mode='FRACTION',hard_stop=1.25,tighten_minutes=120,arm_pct=arm,retain_fraction=retain))
    for adverse in (.5,.75,1.):
        for stop in (0.,.25,.5):
            result.append(dict(name=f'RECOVERY_D{adverse:.2f}_STOP{stop:.2f}_STAGED',family='PROGRESS',
                mode='RECOVERY',hard_stop=1.25,tighten_minutes=120,adverse_pct=adverse,recovered_stop_pct=stop))
    for arm in (.5,.75,1.):
        for lock in (.1,.25):
            result.append(dict(name=f'STEP_A{arm:.2f}_LOCK{lock:.2f}_HARD1',family='PROGRESS',
                mode='STEP',hard_stop=1.,arm_pct=arm,lock_pct=lock))
    return result

def main():
    checks=synthetic_checks()
    ctx=common.context()
    baselines=common.controls(ctx)
    results=[]
    for rule in rules():
        item=common.evaluate(ctx,rule,exit_progress)
        results.append(item)
        print(rule['name'],item['wins'],round(item['net_profit_rupees'],2),item['positive_sessions'],
              round(item['minute_close_drawdown_rupees'],2),flush=True)
    output=common.save('progress_analysis.json',results,baselines,dict(synthetic_checks=checks,control_parity=4),
        ['Fullpostentry5minutecompletedcloses; stop changes active nextminute.',
         'Hardstop/target remain active; stopfirsttie and gapworseopen conventions retained.',
         'All percentages from original entry. Profit locks are gross before costs.',
         'All initial1.25% variants retain original120minute1%tightening; hard1% variants never start wider.',
         'No live settings changed; reusedhistory exploratory.'])
    print(output,flush=True)

if __name__=='__main__':main()
