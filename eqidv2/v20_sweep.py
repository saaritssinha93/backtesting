# -*- coding: utf-8 -*-
"""
v20_sweep.py — parametric grid sweep for avwap_combined_runner_v20_5min.py
============================================================================
Sweeps AVWAP-RS thresholds and momentum toggle via env vars.
Each combo runs the full backtest (~65s) and parses key metrics.
Output: a ranked comparison table.

Usage:
    python v20_sweep.py

Sweep grid (tune via COMBOS list below):
  EQIDV20_AVWAP_RS_THRESH_LONG_BOTH      : [0.30, 0.40, 0.50, 0.60]
  EQIDV20_AVWAP_RS_THRESH_SHORT_BOTH     : [0.50, 0.60, 0.70, 0.75]
  EQIDV20_RS_MOMENTUM_ENABLED            : [false, true]
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from itertools import product
from pathlib import Path
from typing import Dict, List, Optional, Tuple

PYTHON_EXE = sys.executable
BASE_DIR = Path(__file__).resolve().parent
SCRIPT = str(BASE_DIR / "avwap_combined_runner_v20_5min.py")

# V16 canonical benchmark (Run 14, 2026-04-08)
V16_BENCH = dict(
    trades=141, days=13, short_pf=2.442, long_pf=1.391, comb_pf=1.624,
    comb_daywin=76.92, comb_pnl=119.19, comb_maxdd=21.39,
    short_trades=43, long_trades=98,
)

BASE_ENV = {
    "PYTHONUNBUFFERED": "1",
    "PYTHONIOENCODING": "utf-8",
    "EQIDV2_RUNTIME_ROOT": "C:\\TradingData\\eqidv2",
    "EQIDV20_AVWAP_RS_THRESH_LONG_DIRECTIONAL":  "0.15",
    "EQIDV20_AVWAP_RS_THRESH_SHORT_DIRECTIONAL": "0.20",
    "EQIDV20_AVWAP_RS_EXHAUST_CAP":              "3.0",
    "EQIDV20_AVWAP_RS_FALLBACK_LONG":            "0.60",
    "EQIDV20_AVWAP_RS_FALLBACK_SHORT":           "0.40",
    "EQIDV20_SHORT_RAI_EXCEPTION_ENABLED":       "true",
    "EQIDV20_STRICT_5MIN_ONLY":                  "true",
    # disable charts to shorten each run by ~5s
    "EQIDV16_5MIN_ENABLE_LEGACY_CHARTS":         "0",
    "EQIDV16_5MIN_ENABLE_ENHANCED_CHARTS":       "0",
    **os.environ,
}

# ── Sweep grid ──────────────────────────────────────────────────────────────
LONG_BOTH_VALS  = [0.30, 0.40, 0.50, 0.60]
SHORT_BOTH_VALS = [0.50, 0.60, 0.70, 0.75]
MOMENTUM_VALS   = ["false", "true"]

# Build combos: momentum=false first (likely better for LONG recovery)
COMBOS: List[Dict] = []
for mom, lb, sb in product(MOMENTUM_VALS, LONG_BOTH_VALS, SHORT_BOTH_VALS):
    COMBOS.append({
        "EQIDV20_RS_MOMENTUM_ENABLED":           mom,
        "EQIDV20_AVWAP_RS_THRESH_LONG_BOTH":     str(lb),
        "EQIDV20_AVWAP_RS_THRESH_SHORT_BOTH":    str(sb),
    })


# ── Metric parser ──────────────────────────────────────────────────────────
_FLOAT = r"[-+]?\d+(?:\.\d+)?"

def _parse(text: str) -> Dict:
    m = {}

    # trades
    for label, key in [("SHORT", "short_trades"), ("LONG", "long_trades"), ("COMBINED", "comb_trades")]:
        mo = re.search(rf"={label} \(net.*?\)={10*'='}+\nTotal trades\s+:\s+(\d+)", text)
        if mo:
            m[key] = int(mo.group(1))

    # PF per section
    for label, key in [("SHORT", "short_pf"), ("LONG", "long_pf"), ("COMBINED", "comb_pf")]:
        mo = re.search(
            rf"={label} \(net.*?\)={10*'='}+.*?Profit factor\s+:\s+({_FLOAT})",
            text, re.S,
        )
        if mo:
            m[key] = float(mo.group(1))

    # Pessimistic combined metrics
    mo = re.search(
        r"\[EXIT_REALISM\] COMBINED pessimistic\s+:\s+pnl=({f})%.*?pf=({f}).*?day-win=({f})%.*?maxdd=({f})%".format(f=_FLOAT),
        text,
    )
    if mo:
        m["comb_pnl"] = float(mo.group(1))
        m["comb_pf_pess"] = float(mo.group(2))
        m["comb_daywin"] = float(mo.group(3))
        m["comb_maxdd"] = float(mo.group(4))

    mo = re.search(
        r"\[EXIT_REALISM\] SHORT pessimistic\s+:\s+pnl=({f})%.*?pf=({f}).*?day-win=({f})%.*?maxdd=({f})%".format(f=_FLOAT),
        text,
    )
    if mo:
        m["short_pnl"] = float(mo.group(1))
        m["short_pf"] = float(mo.group(2))
        m["short_daywin"] = float(mo.group(3))

    mo = re.search(
        r"\[EXIT_REALISM\] LONG pessimistic\s+:\s+pnl=({f})%.*?pf=({f}).*?day-win=({f})%.*?maxdd=({f})%".format(f=_FLOAT),
        text,
    )
    if mo:
        m["long_pnl"] = float(mo.group(1))
        m["long_pf"] = float(mo.group(2))
        m["long_daywin"] = float(mo.group(3))
        m["long_maxdd"] = float(mo.group(4))

    # Trade counts from NIFTY_CONTEXT line
    mo = re.search(r"SHORT (\d+)->(\d+).*?LONG (\d+)->(\d+)", text)
    if mo:
        m.setdefault("short_trades", int(mo.group(2)))
        m.setdefault("long_trades", int(mo.group(4)))

    # Day-wise final totals
    mo = re.search(r"TOTAL\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([\d.]+)%\s+([\+\-\d.]+)%", text)
    if mo:
        m.setdefault("comb_trades", int(mo.group(3)))

    return m


def _label(combo: Dict) -> str:
    lb = combo["EQIDV20_AVWAP_RS_THRESH_LONG_BOTH"]
    sb = combo["EQIDV20_AVWAP_RS_THRESH_SHORT_BOTH"]
    mom = combo["EQIDV20_RS_MOMENTUM_ENABLED"]
    mom_s = "mom" if mom == "true" else "nomomentum"
    return f"L{lb}_S{sb}_{mom_s}"


def run_combo(combo: Dict, idx: int, total: int) -> Tuple[Dict, float, str]:
    env = {**BASE_ENV, **combo}
    label = _label(combo)
    print(f"\n[SWEEP {idx+1}/{total}] {label}", flush=True)
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            [PYTHON_EXE, "-u", SCRIPT],
            capture_output=True,
            text=True,
            env=env,
            cwd=str(BASE_DIR),
            timeout=300,
        )
        stdout = proc.stdout + proc.stderr
    except subprocess.TimeoutExpired:
        stdout = "[TIMEOUT]"
    elapsed = time.perf_counter() - t0
    metrics = _parse(stdout)
    metrics["label"] = label
    metrics["elapsed"] = elapsed
    metrics["exit_code"] = getattr(proc, "returncode", -1)
    # Print one-line summary
    pf = metrics.get("comb_pf", metrics.get("comb_pf_pess", float("nan")))
    dw = metrics.get("comb_daywin", float("nan"))
    dd = metrics.get("comb_maxdd", float("nan"))
    pnl = metrics.get("comb_pnl", float("nan"))
    st = metrics.get("short_trades", "?")
    lt = metrics.get("long_trades", "?")
    print(
        f"  → T={metrics.get('comb_trades','?')} (S={st} L={lt}) | "
        f"PF={pf:.3f} | DayWin={dw:.1f}% | MaxDD={dd:.1f}% | PnL={pnl:.1f}% | "
        f"[{elapsed:.0f}s]",
        flush=True,
    )
    return metrics, elapsed, stdout


def print_table(results: List[Dict]) -> None:
    b = V16_BENCH
    print("\n" + "=" * 110)
    print(f"{'Label':<26} {'S':>3} {'L':>3} {'Tot':>4} {'Short_PF':>9} {'Long_PF':>8} "
          f"{'Comb_PF':>8} {'DayWin%':>8} {'MaxDD%':>7} {'SumPnL%':>8}")
    print("-" * 110)
    # v16 benchmark row
    print(
        f"{'V16 BENCHMARK':<26} {b['short_trades']:>3} {b['long_trades']:>3} {b['trades']:>4} "
        f"{b['short_pf']:>9.3f} {b['long_pf']:>8.3f} {b['comb_pf']:>8.3f} "
        f"{b['comb_daywin']:>8.2f} {b['comb_maxdd']:>7.2f} {b['comb_pnl']:>8.2f}"
    )
    print("-" * 110)

    def _sort_key(r):
        pf = r.get("comb_pf", r.get("comb_pf_pess", 0.0))
        dw = r.get("comb_daywin", 0.0)
        return (round(dw, 1), round(pf, 3))

    for r in sorted(results, key=_sort_key, reverse=True):
        pf = r.get("comb_pf", r.get("comb_pf_pess", float("nan")))
        dw = r.get("comb_daywin", float("nan"))
        dd = r.get("comb_maxdd", float("nan"))
        pnl = r.get("comb_pnl", float("nan"))
        st = r.get("short_trades", "?")
        lt = r.get("long_trades", "?")
        tot = r.get("comb_trades", "?")
        spf = r.get("short_pf", float("nan"))
        lpf = r.get("long_pf", float("nan"))

        # mark rows beating v16 on PF and DayWin
        star = ""
        if (not isinstance(pf, float) or not isinstance(dw, float)):
            star = "??"
        elif pf >= b["comb_pf"] and dw >= b["comb_daywin"]:
            star = "★★"
        elif pf >= b["comb_pf"] or dw >= b["comb_daywin"]:
            star = "★ "

        label = r.get("label", "?")
        print(
            f"{label + star:<26} {str(st):>3} {str(lt):>3} {str(tot):>4} "
            f"{spf:>9.3f} {lpf:>8.3f} {pf:>8.3f} "
            f"{dw:>8.2f} {dd:>7.2f} {pnl:>8.2f}"
        )

    print("=" * 110)
    print("★★ = beats v16 on BOTH PF and DayWin% | ★  = beats on one metric")


def main() -> None:
    print(f"V20 AVWAP-RS threshold sweep — {len(COMBOS)} combos")
    print(f"Script : {SCRIPT}")
    print(f"Python : {PYTHON_EXE}")
    print(f"V16 benchmark: PF={V16_BENCH['comb_pf']:.3f} | DayWin={V16_BENCH['comb_daywin']:.1f}% | "
          f"MaxDD={V16_BENCH['comb_maxdd']:.1f}% | Trades={V16_BENCH['trades']}")
    print()

    results: List[Dict] = []
    total_start = time.perf_counter()

    for idx, combo in enumerate(COMBOS):
        metrics, elapsed, _ = run_combo(combo, idx, len(COMBOS))
        results.append(metrics)

    print(f"\nAll {len(COMBOS)} combos done in {time.perf_counter()-total_start:.0f}s")
    print_table(results)

    # Save raw results
    import json
    out_path = BASE_DIR / "v20_sweep_results.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n[SAVED] {out_path}")


if __name__ == "__main__":
    main()
