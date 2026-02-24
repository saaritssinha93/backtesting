# -*- coding: utf-8 -*-
"""
Created on Fri Feb 20 13:35:59 2026

@author: Saarit

"""

import pandas as pd
from pathlib import Path
import eqidv2_live_combined_analyser_csv as live

date_str = "2026-02-20"   # change date here
live.DIR_15M = str(Path.cwd() / "stocks_indicators_15min_eq")

slots = pd.date_range(f"{date_str} 09:15", f"{date_str} 15:30", freq="15min", tz="Asia/Kolkata")
state = {"count": {}, "last_signal": {}}
rows, seen = [], set()

tickers = live.list_tickers_15m()
print("tickers:", len(tickers))

for t in tickers:
    f = Path(live.DIR_15M) / f"{t}{live.END_15M}"
    df = live.read_parquet_tail(str(f), n=live.TAIL_ROWS)
    if df is None or df.empty:
        continue

    for slot in slots:
        sigs, _ = live._latest_entry_signals_for_ticker(t, df, state, slot)
        for s in sigs:
            k = (s.ticker, s.side, str(s.bar_time_ist), s.setup)
            if k in seen:
                continue
            seen.add(k)
            rows.append({
                "date": date_str,
                "ticker": s.ticker,
                "side": s.side,
                "bar_time_ist": str(s.bar_time_ist),
                "setup": s.setup,
                "entry_price": float(s.entry_price),
                "sl_price": float(s.sl_price),
                "target_price": float(s.target_price),
                "quality_score": float(s.score),
            })

out = Path("daily_signals_offmarket") / f"live_replay_{date_str}.csv"
pd.DataFrame(rows).sort_values(["bar_time_ist","side","ticker"]).to_csv(out, index=False)
print("signals:", len(rows))
print("saved:", out)



import subprocess
import pandas as pd
from pathlib import Path

date_str = "2026-02-20"  # change date

root = Path(r"C:\Users\Saarit\OneDrive\Desktop\Trading\backtesting\eqidv2\backtesting\eqidv2")
live_file = root / "eqidv2_live_combined_analyser_csv.py"
replay_csv = root / "out_eqidv2_live_signals_15m" / f"replay_signals_{date_str}.csv"

# 1) Run replay
cmd = ["python", str(live_file), "--replay-date", date_str]
print("RUN:", " ".join(cmd))
subprocess.run(cmd, check=True, cwd=str(root))

if not replay_csv.exists():
    raise SystemExit(f"Replay CSV not found: {replay_csv}")

# 2) Compare with latest runner CSV
runner_files = sorted((root / "outputs").glob("avwap_longshort_trades_ALL_DAYS_*.csv"),
                      key=lambda p: p.stat().st_mtime, reverse=True)
if not runner_files:
    raise SystemExit("No runner CSV found in outputs/")
runner_csv = runner_files[0]

rep = pd.read_csv(replay_csv)
run = pd.read_csv(runner_csv)

rep["ticker"] = rep["ticker"].astype(str).str.upper()
rep["side"] = rep["side"].astype(str).str.upper()
rep["setup"] = rep["setup"].astype(str)
rep["entry_ts"] = pd.to_datetime(rep["signal_entry_datetime_ist"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S%z")

run["date"] = pd.to_datetime(run["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
run = run[run["date"] == date_str].copy()
run["ticker"] = run["ticker"].astype(str).str.upper()
run["side"] = run["side"].astype(str).str.upper()
run["setup"] = run["setup"].astype(str)
run["entry_ts"] = pd.to_datetime(run["entry_time_ist"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S%z")

rep_set = set(zip(rep["ticker"], rep["side"], rep["setup"], rep["entry_ts"]))
run_set = set(zip(run["ticker"], run["side"], run["setup"], run["entry_ts"]))

only_run = sorted(run_set - rep_set)
only_rep = sorted(rep_set - run_set)

m = run[["ticker","side","setup","entry_ts","entry_price","sl_price","target_price"]].merge(
    rep[["ticker","side","setup","entry_ts","entry_price","sl_price","target_price"]],
    on=["ticker","side","setup","entry_ts"], how="inner", suffixes=("_run","_rep")
)

price_mismatch = 0
if not m.empty:
    cond = (
        (m["entry_price_run"] - m["entry_price_rep"]).abs() > 1e-6
    ) | (
        (m["sl_price_run"] - m["sl_price_rep"]).abs() > 1e-6
    ) | (
        (m["target_price_run"] - m["target_price_rep"]).abs() > 1e-6
    )
    price_mismatch = int(cond.sum())

print("\nRESULT")
print("runner_csv :", runner_csv)
print("replay_csv :", replay_csv)
print({
    "date": date_str,
    "runner_count": len(run_set),
    "replay_count": len(rep_set),
    "runner_only": len(only_run),
    "replay_only": len(only_rep),
    "price_mismatch": price_mismatch
})
if only_run:
    print("runner_only_sample:", only_run[:10])
if only_rep:
    print("replay_only_sample:", only_rep[:10])