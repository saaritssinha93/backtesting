"""conf_adherence_check.py — EOD guard: did LIVE actually trade the final_setup_conf book?

The Backtesting Result v11 session runs v11 in `live_parity` mode, which resolves the
candidates the live scanner EMITTED and compares them to the paper executor (executor
fidelity). By design it mirrors the live stream, so it CANNOT tell whether the right
book ran — on a legacy day it scores a parity % without flagging "wrong strategy".

This add-on closes that gap: it reads the day's live paper trades and checks what
fraction are `final_setup_conf` setups vs legacy/non-conf leakage, folds in the
session's parity score, and emits a combined verdict + JSON the dashboard can surface.

Run:  py -3.12 conf_adherence_check.py [--date YYYY-MM-DD]
Exit: 0 adherent (>= threshold conf), 3 leakage detected, 4 no live data.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import final_setup_conf as fc  # noqa: E402  (root config of record)

RUNTIME = Path(os.getenv("EQIDV2_RUNTIME_ROOT", r"C:\TradingData\eqidv2"))
PAPER_DIR = RUNTIME / "live_signals"
OUT_DIR = RUNTIME / "backtesting_result_v11" / "latest"
SESSION_JSON = OUT_DIR / "latest_backtesting_result_v11.json"
ADHERENCE_MIN = float(os.getenv("EQIDV2_CONF_ADHERENCE_MIN", "0.95"))


def _load_live(date: str) -> pd.DataFrame | None:
    p = PAPER_DIR / f"paper_trades_{date}_id_5min_v7.csv"
    if not p.exists():
        return None
    try:
        return pd.read_csv(p, low_memory=False)
    except Exception:
        return None


def _session_parity(date: str) -> float | None:
    """live_parity score from the session JSON, but only if it is for THIS date
    (the file holds only the latest run, so guard against showing a stale score)."""
    if not SESSION_JSON.exists():
        return None
    try:
        j = json.loads(SESSION_JSON.read_text(encoding="utf-8"))
        return j.get("parity_score_pct") if str(j.get("day")) == date else None
    except Exception:
        return None


def _write(rec: dict, date: str) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    blob = json.dumps(rec, indent=2, default=str)
    (OUT_DIR / f"conf_adherence_{date}.json").write_text(blob, encoding="utf-8")
    (OUT_DIR / "conf_adherence_latest.json").write_text(blob, encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"))
    args = ap.parse_args()
    date = args.date
    conf = {s.upper() for s in fc.FINAL_SETUP_CONF}
    parity = _session_parity(date)

    print(f"\n## Conf-book adherence — {date}\n")
    L = _load_live(date)
    if L is None:
        print(f"- WARN no live paper file for {date} (paper_trades_{date}_id_5min_v7.csv) — cannot check adherence.")
        _write({"date": date, "verdict": "NO_DATA", "checked_utc": datetime.now(timezone.utc).isoformat()}, date)
        return 4

    if L.empty or "setup" not in L.columns:
        print("- no live trades today (0 paper rows). Nothing to check.")
        _write({"date": date, "verdict": "GREEN", "live_trades": 0, "session_parity_score_pct": parity,
                "checked_utc": datetime.now(timezone.utc).isoformat()}, date)
        return 0

    L["setup"] = L["setup"].astype(str).str.upper().str.strip()
    pnl_col = next((c for c in ("net_pnl_rs", "pnl_rs", "net_pnl") if c in L.columns), None)
    L["p"] = pd.to_numeric(L[pnl_col], errors="coerce") if pnl_col else 0.0
    L["in_conf"] = L["setup"].isin(conf)

    n = len(L)
    n_conf = int(L["in_conf"].sum())
    n_legacy = n - n_conf
    adh = n_conf / n if n else 1.0
    conf_pnl = float(L.loc[L["in_conf"], "p"].sum())
    legacy_pnl = float(L.loc[~L["in_conf"], "p"].sum())
    legacy = (L.loc[~L["in_conf"]].groupby("setup")
              .agg(n=("p", "size"), pnl=("p", "sum")).sort_values("n", ascending=False))
    verdict = "GREEN" if adh >= ADHERENCE_MIN else "RED"

    print(f"- **verdict: {verdict}** — {n_conf}/{n} live trades are conf setups "
          f"({adh * 100:.0f}% adherence; threshold {ADHERENCE_MIN * 100:.0f}%).")
    print(f"- conf PnL Rs {conf_pnl:+,.0f}  |  legacy PnL Rs {legacy_pnl:+,.0f}")
    if parity is not None:
        print(f"- session live_parity score: {parity:.1f}% (executor fidelity — separate axis)")
    if n_legacy:
        print(f"- **{n_legacy} legacy/non-conf trades leaked** — live is NOT cleanly on the conf book. "
              f"Check EQIDV2_USE_FINAL_SETUP_CONF + the scheduled tasks (should run run_conf_paper_*.bat):")
        for s, r in legacy.iterrows():
            print(f"    - {s}: {int(r['n'])} trades, Rs {r['pnl']:+,.0f}")
    else:
        print("- clean: every live trade is a conf setup.")

    _write({
        "date": date, "verdict": verdict, "adherence_pct": round(adh * 100, 1),
        "live_trades": n, "conf_trades": n_conf, "legacy_trades": n_legacy,
        "conf_pnl_rs": round(conf_pnl, 0), "legacy_pnl_rs": round(legacy_pnl, 0),
        "legacy_setups": {s: int(r["n"]) for s, r in legacy.iterrows()},
        "session_parity_score_pct": parity,
        "checked_utc": datetime.now(timezone.utc).isoformat(),
    }, date)
    return 0 if verdict == "GREEN" else 3


if __name__ == "__main__":
    raise SystemExit(main())
