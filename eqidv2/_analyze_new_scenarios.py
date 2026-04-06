"""Quick metric calculator for new scenario exact exit CSVs."""
from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path


OUT = Path(__file__).resolve().parent / "outputs_v16_5min_long_scenario_search"
MARKET_DAYS = 69

NEW_SCENARIOS = [
    "or_gate_off_strict_rs",
    "no_avwap_dead",
    "or_gate_off_no_avwap_dead",
    "looser_rsi_or_off",
]


def _pf(pnl: pd.Series) -> float:
    pos = float(pnl[pnl > 0].sum())
    neg = float(-pnl[pnl < 0].sum())
    return pos / neg if neg > 0 else (999.0 if pos > 0 else 0.0)


def _dd(pnl: pd.Series) -> float:
    if pnl.empty:
        return 0.0
    eq = pnl.cumsum()
    return float((eq.cummax() - eq).max())


def _metrics(df: pd.DataFrame, label: str) -> None:
    if df.empty:
        print(f"  {label}: NO TRADES")
        return
    pnl = df["pnl_pct"].astype(float)
    day_pnl = df.groupby("trade_date")["pnl_pct"].sum()
    outcome = df["outcome"].str.upper()
    n = len(df)
    td = int(df["trade_date"].nunique())
    tpd = n / MARKET_DAYS
    print(
        f"  {label}: N={n}, trade_days={td}, TPD(mkt)={tpd:.2f}\n"
        f"    PnL={pnl.sum():.1f}%  avg={pnl.mean():.4f}%\n"
        f"    Win={((pnl>0).mean()*100):.1f}%  DayWin={((day_pnl>0).mean()*100):.1f}%\n"
        f"    PF={_pf(pnl):.3f}  MaxDD={_dd(pnl):.2f}%\n"
        f"    TGT%={((outcome=='TARGET').mean()*100):.1f}%  "
        f"SL%={((outcome=='SL').mean()*100):.1f}%  "
        f"EOD%={((outcome=='EOD').mean()*100):.1f}%"
    )


for scen in NEW_SCENARIOS:
    folder = OUT / scen
    csvs = sorted(folder.glob(f"{scen}_exact_sl_*.csv"))
    if not csvs:
        print(f"\n{scen}: NO EXACT CSVs YET")
        continue
    print(f"\n{'='*60}")
    print(f"SCENARIO: {scen}")
    for csv_path in csvs:
        df = pd.read_csv(csv_path)
        label = csv_path.stem.replace(f"{scen}_exact_", "")
        _metrics(df, label)

print("\n\n=== EXISTING BENCHMARK (baseline Run11-style) ===")
bsln = OUT / "baseline" / "baseline_exact_sl_0.0065_tgt_0.0090.csv"
if bsln.exists():
    _metrics(pd.read_csv(bsln), "baseline SL=0.65% TGT=0.90%")
