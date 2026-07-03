r"""run_discovery.py -- orchestrator for the namespaced fast-momentum LONG (~0.75%) discovery.

Full pipeline (audit -> build signal+exit cache -> edge study -> staged search):
  py -3.12 Train_and_Test/long_setup_discovery_from_raw_data/claude_engine/scripts/run_discovery.py

Skip the (slow) cache rebuild if results/signals_resolved.parquet already exists:
  py -3.12 .../claude_engine/scripts/run_discovery.py --no-build

Re-run ONLY the best candidate's evaluation (no search):
  py -3.12 .../claude_engine/scripts/run_discovery.py --best-only
"""
from __future__ import annotations
import argparse, json, runpy, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import lib_long_disc as L


def _run(script):
    print(f"\n========== {script} ==========")
    runpy.run_path(str(HERE / script), run_name="__main__")


def best_only():
    import pandas as pd
    df = pd.read_parquet(L.RESULTS / "signals_resolved.parquet")
    S = L.load_sessions()
    cfg_path = L.CAND / "BEST_NEAR_MISS_candidate_001.json"
    cfg = json.loads(cfg_path.read_text())["config"]
    for split in ("train", "test"):
        days = set(pd.Timestamp(x) for x in S[split])
        sub = df[df["_day"].isin(days)]
        for slip in (5.0, 15.0):
            m = L.evaluate(sub, {**cfg, "slip_bps": slip}, len(S[split]))
            print(f"  {split.upper():5s} @{slip:>4.0f}bps: n={m['trades']} PF={m['pf']} win={m['win_rate']}% "
                  f"exp=Rs{m['expectancy']}/tr net=Rs{m['net_pnl']:,.0f} dayDom={m['day_dom']} symDom={m['sym_dom']}")
        g = L.evaluate(sub, {**cfg, "cost_mode": "gross"}, len(S[split]))
        print(f"  {split.upper():5s} price-path(0-cost): PF={g['pf']} win={g['win_rate']}%")
    print(f"\nbest near-miss config: {json.dumps(cfg)}")
    print("VERDICT: REJECT — net-negative after realistic costs. DO NOT PROMOTE.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-build", action="store_true", help="skip cache rebuilds (02, 05)")
    ap.add_argument("--best-only", action="store_true", help="evaluate the best near-miss only")
    ap.add_argument("--ext", action="store_true", help="run ONLY the extension (wider targets + SHORT)")
    a = ap.parse_args()
    if a.best_only:
        best_only(); return
    if a.ext:
        if not a.no_build:
            _run("05_build_signals_ext.py")
        _run("06_search_ext.py")
        return
    _run("01_audit.py")
    if not a.no_build:
        _run("02_build_signals.py")
    _run("03_edge_study.py")
    _run("04_search.py")
    # extension: wider targets + limit-entry slippage + SHORT side (user-approved follow-up)
    if not a.no_build:
        _run("05_build_signals_ext.py")
    _run("06_search_ext.py")
    print("\nDONE. Reports under Train_and_Test/long_setup_discovery_from_raw_data/claude_engine/")


if __name__ == "__main__":
    main()
