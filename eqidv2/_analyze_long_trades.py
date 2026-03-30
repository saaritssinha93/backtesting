import pandas as pd
import numpy as np
import os, sys, json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RANK1_CSV = ROOT / "outputs_v15_sltarget_search/rank1_sSL0084_sTGT0112_lSL0077_lTGT0110/exact_rank1_sSL0084_sTGT0112_lSL0077_lTGT0110.csv"

df = pd.read_csv(RANK1_CSV)
longs = df[df['side']=='LONG'].copy()
print(f"LONG trades: {len(longs)}")
wins   = longs[longs['outcome']=='TARGET']
losses = longs[longs['outcome']=='SL']
eods   = longs[longs['outcome']=='EOD']
print(f"TARGET: {len(wins)}  SL: {len(losses)}  EOD: {len(eods)}  BE: {len(longs[longs['outcome']=='BE'])}")
print()

# ── INDICATOR DIVERGENCE ──────────────────────────────────────────────────────
ind_cols = [
    'adx_signal','rsi_signal','stochk_signal',
    'avwap_dist_atr_signal','ema20_gap_atr_signal',
    'atr_pct_signal','quality_score','india_vix',
    'nifty_rel_strength_pct','gap_pct_open','opening_range_width_pct',
]
print(f"{'Indicator':42s}  WIN_mean   LOSS_mean   DIFF")
for col in ind_cols:
    if col in longs.columns:
        wm = wins[col].mean(); lm = losses[col].mean()
        print(f"  {col:40s}  {wm:9.4f}   {lm:9.4f}   {wm-lm:+9.4f}")

print()

# ── BY SETUP ─────────────────────────────────────────────────────────────────
print("BY SETUP:")
for s in sorted(longs['setup'].unique()):
    sub = longs[longs['setup']==s]
    tw = len(sub[sub['outcome']=='TARGET'])
    tl = len(sub[sub['outcome']=='SL'])
    pf = (sub[sub['pnl_pct_gross']>0]['pnl_pct_gross'].sum() /
          max(abs(sub[sub['pnl_pct_gross']<0]['pnl_pct_gross'].sum()), 1e-6))
    avgq = sub['quality_score'].mean() if 'quality_score' in sub.columns else 0
    print(f"  {s:47s}  n={len(sub):3d}  T={tw:3d}  SL={tl:3d}  win={tw/len(sub)*100:4.0f}%  PF={pf:5.2f}  qscore={avgq:.2f}")

print()

# ── BY NIFTY MODE ─────────────────────────────────────────────────────────────
print("BY NIFTY MODE:")
for m in sorted(str(x) for x in longs['nifty_context_mode'].unique()):
    sub = longs[longs['nifty_context_mode'].astype(str)==m]
    tw = len(sub[sub['outcome']=='TARGET'])
    tl = len(sub[sub['outcome']=='SL'])
    pf = (sub[sub['pnl_pct_gross']>0]['pnl_pct_gross'].sum() /
          max(abs(sub[sub['pnl_pct_gross']<0]['pnl_pct_gross'].sum()), 1e-6))
    print(f"  {m:20s}  n={len(sub):3d}  T={tw:3d}  SL={tl:3d}  win={tw/len(sub)*100:4.0f}%  PF={pf:5.2f}")

print()

# ── NIFTY RS BUCKETED ────────────────────────────────────────────────────────
print("NIFTY RS BUCKETS (nifty_rel_strength_pct):")
rs = longs['nifty_rel_strength_pct']
buckets = [
    ('<-0.5',   rs < -0.5),
    ('-0.5-0',  (rs>=-0.5)&(rs<0)),
    ('0-0.1',   (rs>=0)&(rs<0.1)),
    ('0.1-0.3', (rs>=0.1)&(rs<0.3)),
    ('0.3-0.5', (rs>=0.3)&(rs<0.5)),
    ('0.5-1.0', (rs>=0.5)&(rs<1.0)),
    ('>=1.0',   rs>=1.0),
]
for label, mask in buckets:
    sub = longs[mask]
    if len(sub)==0: continue
    tw = len(sub[sub['outcome']=='TARGET'])
    tl = len(sub[sub['outcome']=='SL'])
    pf = (sub[sub['pnl_pct_gross']>0]['pnl_pct_gross'].sum() /
          max(abs(sub[sub['pnl_pct_gross']<0]['pnl_pct_gross'].sum()), 1e-6))
    print(f"  RS {label:12s}  n={len(sub):3d}  T={tw:3d}  SL={tl:3d}  win={tw/len(sub)*100:4.0f}%  PF={pf:5.2f}")

print()

# ── ADX BUCKETS ──────────────────────────────────────────────────────────────
print("ADX BUCKETS:")
adx = longs['adx_signal']
for lo, hi in [(0,15),(15,20),(20,25),(25,30),(30,35),(35,50),(50,999)]:
    sub = longs[(adx>=lo)&(adx<hi)]
    if len(sub)==0: continue
    tw = len(sub[sub['outcome']=='TARGET'])
    tl = len(sub[sub['outcome']=='SL'])
    pf = (sub[sub['pnl_pct_gross']>0]['pnl_pct_gross'].sum() /
          max(abs(sub[sub['pnl_pct_gross']<0]['pnl_pct_gross'].sum()), 1e-6))
    print(f"  ADX {lo:3d}-{hi:3d}  n={len(sub):3d}  T={tw:3d}  SL={tl:3d}  win={tw/len(sub)*100:4.0f}%  PF={pf:5.2f}")

print()

# ── RSI BUCKETS ──────────────────────────────────────────────────────────────
print("RSI BUCKETS:")
rsi = longs['rsi_signal']
for lo, hi in [(38,45),(45,50),(50,55),(55,60),(60,65),(65,70),(70,80)]:
    sub = longs[(rsi>=lo)&(rsi<hi)]
    if len(sub)==0: continue
    tw = len(sub[sub['outcome']=='TARGET'])
    tl = len(sub[sub['outcome']=='SL'])
    pf = (sub[sub['pnl_pct_gross']>0]['pnl_pct_gross'].sum() /
          max(abs(sub[sub['pnl_pct_gross']<0]['pnl_pct_gross'].sum()), 1e-6))
    print(f"  RSI {lo:3d}-{hi:3d}  n={len(sub):3d}  T={tw:3d}  SL={tl:3d}  win={tw/len(sub)*100:4.0f}%  PF={pf:5.2f}")

print()

# ── QUALITY SCORE BUCKETS ────────────────────────────────────────────────────
print("QUALITY SCORE BUCKETS:")
qs = longs['quality_score']
for lo, hi in [(0,3),(3,5),(5,7),(7,9),(9,11),(11,20)]:
    sub = longs[(qs>=lo)&(qs<hi)]
    if len(sub)==0: continue
    tw = len(sub[sub['outcome']=='TARGET'])
    tl = len(sub[sub['outcome']=='SL'])
    pf = (sub[sub['pnl_pct_gross']>0]['pnl_pct_gross'].sum() /
          max(abs(sub[sub['pnl_pct_gross']<0]['pnl_pct_gross'].sum()), 1e-6))
    print(f"  QS {lo:2d}-{hi:2d}   n={len(sub):3d}  T={tw:3d}  SL={tl:3d}  win={tw/len(sub)*100:4.0f}%  PF={pf:5.2f}")

print()

# ── AVWAP DIST ATR BUCKETS ────────────────────────────────────────────────────
print("AVWAP DIST ATR BUCKETS:")
av = longs['avwap_dist_atr_signal']
for lo, hi in [(0,0.5),(0.5,1.0),(1.0,1.5),(1.5,2.0),(2.0,3.0),(3.0,99)]:
    sub = longs[(av>=lo)&(av<hi)]
    if len(sub)==0: continue
    tw = len(sub[sub['outcome']=='TARGET'])
    tl = len(sub[sub['outcome']=='SL'])
    pf = (sub[sub['pnl_pct_gross']>0]['pnl_pct_gross'].sum() /
          max(abs(sub[sub['pnl_pct_gross']<0]['pnl_pct_gross'].sum()), 1e-6))
    print(f"  AVWAP_DIST {lo:.1f}-{hi:.1f}  n={len(sub):3d}  T={tw:3d}  SL={tl:3d}  win={tw/len(sub)*100:4.0f}%  PF={pf:5.2f}")

print()

# ── TIME OF DAY ───────────────────────────────────────────────────────────────
print("TIME OF DAY (entry):")
longs['entry_hour'] = pd.to_datetime(longs['entry_time_ist'], utc=True).dt.hour
longs['entry_min'] = pd.to_datetime(longs['entry_time_ist'], utc=True).dt.minute
longs['entry_slot'] = longs['entry_hour'].astype(str) + ':' + longs['entry_min'].astype(str).str.zfill(2)
for slot in sorted(longs['entry_slot'].unique()):
    sub = longs[longs['entry_slot']==slot]
    if len(sub)<3: continue
    tw = len(sub[sub['outcome']=='TARGET'])
    tl = len(sub[sub['outcome']=='SL'])
    pf = (sub[sub['pnl_pct_gross']>0]['pnl_pct_gross'].sum() /
          max(abs(sub[sub['pnl_pct_gross']<0]['pnl_pct_gross'].sum()), 1e-6))
    print(f"  {slot}  n={len(sub):3d}  T={tw:3d}  SL={tl:3d}  win={tw/len(sub)*100:4.0f}%  PF={pf:5.2f}")

print()

# ── INDIA VIX ─────────────────────────────────────────────────────────────────
print("INDIA VIX BUCKETS:")
vix = longs['india_vix']
for lo, hi in [(0,12),(12,14),(14,16),(16,18),(18,20),(20,25),(25,99)]:
    sub = longs[(vix>=lo)&(vix<hi)]
    if len(sub)==0: continue
    tw = len(sub[sub['outcome']=='TARGET'])
    tl = len(sub[sub['outcome']=='SL'])
    pf = (sub[sub['pnl_pct_gross']>0]['pnl_pct_gross'].sum() /
          max(abs(sub[sub['pnl_pct_gross']<0]['pnl_pct_gross'].sum()), 1e-6))
    print(f"  VIX {lo:2d}-{hi:2d}  n={len(sub):3d}  T={tw:3d}  SL={tl:3d}  win={tw/len(sub)*100:4.0f}%  PF={pf:5.2f}")

print()

# ── EMA GAP ATR ──────────────────────────────────────────────────────────────
print("EMA20 GAP ATR BUCKETS:")
eg = longs['ema20_gap_atr_signal']
for lo, hi in [(0,0.5),(0.5,1.0),(1.0,1.5),(1.5,2.0),(2.0,3.0),(3.0,99)]:
    sub = longs[(eg>=lo)&(eg<hi)]
    if len(sub)==0: continue
    tw = len(sub[sub['outcome']=='TARGET'])
    tl = len(sub[sub['outcome']=='SL'])
    pf = (sub[sub['pnl_pct_gross']>0]['pnl_pct_gross'].sum() /
          max(abs(sub[sub['pnl_pct_gross']<0]['pnl_pct_gross'].sum()), 1e-6))
    print(f"  EMA_GAP {lo:.1f}-{hi:.1f}  n={len(sub):3d}  T={tw:3d}  SL={tl:3d}  win={tw/len(sub)*100:4.0f}%  PF={pf:5.2f}")

print()
# ── COMBINED HIGH-CONFIDENCE FILTER ──────────────────────────────────────────
print("HIGH CONFIDENCE FILTER (RS>=0.3, ADX>=22, RSI>=50, QS>=5):")
mask = (
    (longs['nifty_rel_strength_pct'] >= 0.3) &
    (longs['adx_signal'] >= 22) &
    (longs['rsi_signal'] >= 50) &
    (longs['quality_score'] >= 5)
)
sub = longs[mask]
tw = len(sub[sub['outcome']=='TARGET']); tl = len(sub[sub['outcome']=='SL'])
pf = (sub[sub['pnl_pct_gross']>0]['pnl_pct_gross'].sum() /
      max(abs(sub[sub['pnl_pct_gross']<0]['pnl_pct_gross'].sum()), 1e-6))
print(f"  n={len(sub)}  T={tw}  SL={tl}  win={tw/len(sub)*100 if len(sub) else 0:.0f}%  PF={pf:.2f}")

print()
print("HIGH CONFIDENCE FILTER (RS>=0.5, ADX>=25, RSI>=52, QS>=6, AVWAP_DIST>=0.5):")
mask2 = (
    (longs['nifty_rel_strength_pct'] >= 0.5) &
    (longs['adx_signal'] >= 25) &
    (longs['rsi_signal'] >= 52) &
    (longs['quality_score'] >= 6) &
    (longs['avwap_dist_atr_signal'] >= 0.5)
)
sub2 = longs[mask2]
if len(sub2):
    tw2 = len(sub2[sub2['outcome']=='TARGET']); tl2 = len(sub2[sub2['outcome']=='SL'])
    pf2 = (sub2[sub2['pnl_pct_gross']>0]['pnl_pct_gross'].sum() /
           max(abs(sub2[sub2['pnl_pct_gross']<0]['pnl_pct_gross'].sum()), 1e-6))
    print(f"  n={len(sub2)}  T={tw2}  SL={tl2}  win={tw2/len(sub2)*100:.0f}%  PF={pf2:.2f}")
