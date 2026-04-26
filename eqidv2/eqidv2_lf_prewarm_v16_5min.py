"""
Audit #2 (2026-04-22) — LF (live fetcher) prewarm at 09:13 IST.

Why this matters:
  The LF supervisor wakes at 09:15 and is asked to do session-startup work
  (validate 8 Kite app sessions, load instrument tokens, kick off any
  prior-day catch-up) at the same instant the live path needs throughput.
  The result is a "Too many requests" cascade against Kite at 09:20.

What this script does:
  Run once at 09:13 (before the live loop is needed) to:
    1. Open all 8 Kite app sessions, paying the auth + handshake cost upfront.
    2. Pre-resolve the NSE instrument list per app (the call that's most
       likely to throw 429 if 8 supervisors race at 09:15).
    3. Write a per-day flag file so the LF supervisor / dashboard can see
       that prewarm completed.

Failure semantics:
  Best-effort. If an app fails auth, log it and keep going — the LF
  supervisor will hit the same failure at 09:15 anyway, but at least the
  failure is visible 2 minutes earlier. Exit 0 unless 0/8 apps succeeded.

Schedule:
  schtask EQIDV2_lf_prewarm_v16_5min_0913 — Mon-Fri 09:13:00.
"""

from __future__ import annotations

import json
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

import pytz

from eqidv2_runtime_paths import RUNTIME_STATUS_DIR

# Reuse the LF script's per-app session helpers so prewarm and live use
# exactly the same auth path. This is the contract guarantee — if the LF
# supervisor would see auth fail at 09:15, prewarm sees it first at 09:13.
import eqidv2_eod_scheduler_for_5mins_data_live_minimal as lf_core


IST = pytz.timezone("Asia/Kolkata")

PREWARM_FLAG_DIR = Path(
    os.getenv("EQIDV2_LF_PREWARM_FLAG_DIR", str(RUNTIME_STATUS_DIR / "lf_prewarm"))
)


def _setup_fns():
    return [
        ("app1", lf_core.setup_kite_session_from_eqidv2_dir),
        ("app2", lf_core.setup_kite_session2_from_eqidv2_dir),
        ("app3", lf_core.setup_kite_session3_from_eqidv2_dir),
        ("app4", lf_core.setup_kite_session4_from_eqidv2_dir),
        ("app5", lf_core.setup_kite_session5_from_eqidv2_dir),
        ("app6", lf_core.setup_kite_session6_from_eqidv2_dir),
        ("app7", lf_core.setup_kite_session7_from_eqidv2_dir),
        ("app8", lf_core.setup_kite_session8_from_eqidv2_dir),
    ]


def _warm_one(name: str, setup_fn) -> dict:
    started = time.monotonic()
    info = {"app": name, "auth_ok": False, "instruments_ok": False, "n_instruments": 0}
    try:
        kc = setup_fn()
    except Exception as exc:
        info["error"] = f"auth_failed: {exc.__class__.__name__}: {exc}"
        info["elapsed_sec"] = round(time.monotonic() - started, 3)
        return info
    info["auth_ok"] = True
    try:
        rows = kc.instruments("NSE")
        info["instruments_ok"] = True
        info["n_instruments"] = len(rows) if rows is not None else 0
    except Exception as exc:
        info["error"] = f"instruments_failed: {exc.__class__.__name__}: {exc}"
    info["elapsed_sec"] = round(time.monotonic() - started, 3)
    return info


def main() -> int:
    now_ist = datetime.now(IST)
    print(f"[LF_PREWARM] starting at {now_ist.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    PREWARM_FLAG_DIR.mkdir(parents=True, exist_ok=True)
    date_str = now_ist.strftime("%Y%m%d")
    flag_path = PREWARM_FLAG_DIR / f"lf_prewarm_done_{date_str}.flag"

    results = []
    for name, fn in _setup_fns():
        info = _warm_one(name, fn)
        print(
            f"[LF_PREWARM] {name} auth={info['auth_ok']} "
            f"instr={info['instruments_ok']} n={info['n_instruments']} "
            f"elapsed={info['elapsed_sec']}s"
            + (f" err={info.get('error')}" if info.get("error") else "")
        )
        results.append(info)

    auth_ok_count = sum(1 for r in results if r["auth_ok"])
    instr_ok_count = sum(1 for r in results if r["instruments_ok"])

    payload = {
        "date":              now_ist.strftime("%Y-%m-%d"),
        "started_at_ist":    now_ist.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "completed_at_ist":  datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S%z"),
        "apps_total":        len(results),
        "apps_auth_ok":      auth_ok_count,
        "apps_instr_ok":     instr_ok_count,
        "results":           results,
    }
    tmp_path = flag_path.with_suffix(".flag.tmp")
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.flush()
        try:
            os.fsync(fh.fileno())
        except Exception:
            pass
    os.replace(tmp_path, flag_path)
    print(f"[LF_PREWARM] flag written: {flag_path}")

    # Hard fail only when literally nothing came up. Partial success is
    # still a win — the LF supervisor will retry the failed apps at 09:15.
    if auth_ok_count == 0:
        print("[LF_PREWARM] FAIL: 0/8 apps authenticated", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:
        traceback.print_exc()
        print(f"[LF_PREWARM] unexpected error: {exc}", file=sys.stderr)
        raise SystemExit(1)
