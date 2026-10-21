"""Independent CSV/accounting inspection; imports no strategy/simulation code."""
import hashlib
import json
from pathlib import Path
import re
import sys

import numpy as np
import pandas as pd

root = Path("C:/TradingData/eqidv2/backtesting_result_v13_v10_g_3")
folder = Path(sys.argv[1])
checks = []

def check(name, value):
    checks.append(dict(check=name, passed=bool(value)))
    if not value:
        raise AssertionError(name)

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def load(path):
    return json.loads(path.read_text())

def csv(path):
    return pd.read_csv(path, float_precision="round_trip")

def artifact_check(directory):
    manifest = load(directory / "manifest.json")
    for name, digest in manifest["files"].items():
        check(f"artifact:{directory.name}/{name}", sha(directory / name) == digest)
    for name, digest in manifest.get("code_sha256", {}).items():
        check(f"code:{name}", sha(Path(name)) == digest)
    for href in re.findall(r'href="([^"]+)"', (directory / "report.html").read_text()):
        check(f"report_link:{directory.name}/{href}", (directory / href).is_file())
    return manifest

def metrics(ledger):
    fills = ledger.loc[ledger.portfolio_executed.eq(True)]
    return dict(executed_trades=len(fills), wins=int(fills.portfolio_net_profit_rupees.gt(0).sum()),
        losses=int(fills.portfolio_net_profit_rupees.lt(0).sum()),
        net_pnl_rs=float(fills.portfolio_net_profit_rupees.sum()))

artifact_check(folder)
summary = load(folder / "summary.json")
base = root / summary["base_source"]["run_dir"] if "base_source" in summary else folder
check("base_manifest", "base_source" not in summary or sha(base / "manifest.json") == summary["base_source"]["manifest_sha256"])
artifact_check(base)
ledger = csv(folder / ("portfolio_trades.csv" if (folder / "portfolio_trades.csv").exists() else "variant_trades.csv"))
days = csv(folder / "daily_results.csv")
baseline = csv(base / "baseline_trades.csv")
baseledger = csv(base / "variant_trades.csv")
check("unique_signal_ids", not ledger.signal_id.duplicated().any())
check("inventory_44", ledger.setup_id.nunique() == 44)
check("session_count", len(days) == summary["sessions"])
check("unique_sorted_dates", days.day.tolist() == sorted(set(days.day)))
for key, expected in metrics(ledger).items():
    check(f"summary:{key}", np.isclose(summary["current_g3"][key], expected, rtol=0, atol=1e-6))
check("gross_cost_net", np.allclose(ledger.portfolio_gross_profit_rupees-ledger.portfolio_cost_rupees,
    ledger.portfolio_net_profit_rupees, rtol=0, atol=1e-6))
fills = ledger.loc[ledger.portfolio_executed.eq(True)]
check("cost250_per_fill", fills.portfolio_cost_rupees.eq(250).all())
check("unexecuted_zero_pnl", ledger.loc[~ledger.portfolio_executed.eq(True)].portfolio_net_profit_rupees.eq(0).all())
for row in days.itertuples():
    m = metrics(ledger.loc[ledger.day.eq(row.day)])
    check(f"daily:{row.day}", row.trades == m["executed_trades"] and row.wins == m["wins"]
          and row.losses == m["losses"] and np.isclose(row.net_pnl,m["net_pnl_rs"],atol=1e-6,rtol=0))
setups = csv(folder / "setup_results.csv")
for row in setups.itertuples():
    for key, expected in metrics(ledger.loc[ledger.setup_id.eq(row.setup_id)]).items():
        check(f"setup:{row.setup_id}:{key}", np.isclose(getattr(row,key),expected,atol=1e-6,rtol=0))
check("midday_twelve", len(setups.loc[setups.added]) == 12)
base_setups = csv(base / "setup_results.csv")
check("midday_base49_each70", base_setups.loc[base_setups.added].win_rate_pct.ge(70).all())
below70 = setups.loc[setups.added & setups.win_rate_pct.lt(70),
    ["setup_id", "executed_trades", "wins", "win_rate_pct"]].to_dict("records")
check("midday_each_positive", setups.loc[setups.added].net_pnl_rs.gt(0).all())
old = baseledger.loc[baseledger.sid.isin(baseline.sid)].set_index("sid").sort_index()
prior = baseline.set_index("sid").sort_index()
check("existing_ids_preserved", old.index.equals(prior.index))
check("existing_fills_preserved", old.portfolio_executed.equals(prior.portfolio_executed))
check("existing_pnl_preserved", np.allclose(old.portfolio_net_profit_rupees,prior.portfolio_net_profit_rupees,atol=1e-6,rtol=0))
for day, trades in fills.groupby("day"):
    events=[]
    for row in trades.itertuples():
        entry,exit = pd.Timestamp(row.entry_ts),pd.Timestamp(row.exit_ts)
        check(f"intraday:{row.signal_id}", str(entry.date())==day==str(exit.date()) and entry<=exit)
        events.extend(((entry,1),(exit,-1)))
    # Simultaneous exit releases capital before a new entry; same-bar trades
    # can have entry==exit, so check active intervals at their entry instants.
    stamps=pd.to_datetime(trades.entry_ts,utc=True)
    exits=pd.to_datetime(trades.exit_ts,utc=True)
    peak=max(int(((stamps<=stamp)&(exits>stamp)).sum()) for stamp in stamps)
    check(f"capital_cap:{day}",peak<=10)
receipt=dict(status="PASS",check_count=len(checks),run_dir=str(folder),
    manifest_sha256=sha(folder/"manifest.json"),
    all_midday_setups_observed70=(len(below70)==0),midday_setups_below70=below70,checks=checks)
out=Path(__file__).with_name("historical_validation.json")
out.write_text(json.dumps(receipt,indent=2)+"\n")
print(json.dumps({k:v for k,v in receipt.items() if k!="checks"},indent=2))
