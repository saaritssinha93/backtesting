"""Quick progress reader for window search results."""
from pathlib import Path
import pandas as pd, numpy as np, math

OUT = Path(__file__).resolve().parent / "outputs_v16_5min_window_search"
MARKET_DAYS = 69

def _pf(pnl):
    pos = float(pnl[pnl>0].sum()); neg = float(-pnl[pnl<0].sum())
    return pos/neg if neg>0 else (999 if pos>0 else 0)

def _dd(pnl):
    if pnl.empty: return 0.0
    eq = pnl.cumsum(); return float((eq.cummax()-eq).max())

def _show(csv_path, name):
    df = pd.read_csv(csv_path)
    s = df[df["side"].str.upper()=="SHORT"]; l = df[df["side"].str.upper()=="LONG"]
    for side, sub, label in [("COMBINED", df, ""), ("SHORT", s, ""), ("LONG", l, "")]:
        if sub.empty: print(f"  {side}: no trades"); continue
        pnl = sub["pnl_pct"].astype(float)
        dpnl = sub.groupby("trade_date")["pnl_pct"].sum()
        out = sub["outcome"].str.upper()
        n = len(sub); td = int(sub["trade_date"].nunique())
        print(f"  {side}: N={n}, TPD={n/MARKET_DAYS:.2f}, PnL={pnl.sum():.1f}%, Win={((pnl>0).mean()*100):.1f}%, DayWin={((dpnl>0).mean()*100):.1f}%, PF={_pf(pnl):.3f}, MaxDD={_dd(pnl):.2f}%  TGT={((out=='TARGET').mean()*100):.1f}% SL={((out=='SL').mean()*100):.1f}%")

for folder in sorted(OUT.iterdir()):
    csv = folder / f"{folder.name}_trades.csv"
    if csv.exists():
        print(f"\n{'='*60}\nSCENARIO: {folder.name}")
        _show(csv, folder.name)
    else:
        print(f"\n{'='*60}\nSCENARIO: {folder.name}  << STILL RUNNING >>")
