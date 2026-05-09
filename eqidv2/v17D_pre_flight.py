"""v17D Step 4.2 — pre-market sanity check.

Cron at 08:30 IST. Runs a checklist; aborts trading if any FAIL.

Checks:
    1. Universe file present and refreshed within 7 days
    2. F&O list refreshed within 7 days
    3. Sector ETF map present
    4. v17D config loads + validates
    5. Required parquet directories exist
    6. Yesterday's reconciliation file exists
    7. Kill-switch state (env + file)
    8. Disk space healthy (> 5 GB free in log dir)

Exit code:
    0 = all green, ok to trade
    1 = at least one FAIL
    2 = at least one WARN but no FAIL

Usage:
    python -m eqidv2.v17D_pre_flight \
        --indicator-dir C:/TradingData/eqidv2/stocks_indicators_5min_eq_live2 \
        --onemin-dir   C:/TradingData/eqidv2/stocks_indicators_1min_eq \
        --universe-csv eqidv2/configs/universe.csv \
        --reconcile-dir ~/v17D_logs

To wire as cron:
    30 8 * * 1-5 python -m eqidv2.v17D_pre_flight ... || mail -s "v17D PREFLIGHT FAIL" you@example.com
"""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from eqidv2 import v17D_kill_switch as ks


def _age_days(p: Path) -> float:
    if not p.exists():
        return float("inf")
    delta = datetime.now() - datetime.fromtimestamp(p.stat().st_mtime)
    return delta.total_seconds() / 86400.0


def check(
    indicator_dir: str,
    onemin_dir: str,
    universe_csv: str | None,
    reconcile_dir: str,
    config_path: str | None = None,
) -> tuple:
    """Returns (n_pass, n_warn, n_fail, lines)."""
    lines = []
    n_pass = n_warn = n_fail = 0

    def _check(label, ok, detail="", warn_only=False):
        nonlocal n_pass, n_warn, n_fail
        tag = "PASS" if ok else ("WARN" if warn_only else "FAIL")
        line = f"  [{tag}] {label}"
        if detail:
            line += f"   ({detail})"
        lines.append(line)
        if ok:
            n_pass += 1
        elif warn_only:
            n_warn += 1
        else:
            n_fail += 1

    # 1. universe file
    if universe_csv:
        u = Path(universe_csv)
        age = _age_days(u)
        _check("universe file present + fresh", age <= 7,
               f"age {age:.1f} days", warn_only=(age <= 14))

    # 2. F&O list freshness
    from eqidv2.v17D_universe_filter import DEFAULT_FO_LIST_PATH
    fo_age = _age_days(DEFAULT_FO_LIST_PATH)
    _check("F&O list refreshed within 7 days", fo_age <= 7,
           f"age {fo_age:.1f} days", warn_only=(fo_age <= 14))

    # 3. sector ETF map
    sec_path = Path(__file__).parent / "configs" / "sector_etf_map.json"
    _check("sector ETF map present", sec_path.exists(), str(sec_path))

    # 4. v17D config
    try:
        from eqidv2.v17D_config import load_config
        cfg = load_config(config_path)
        _check("v17D config loads + validates",
               True, f"version={cfg.version}, {len(cfg.setups)} setups")
    except SystemExit as e:
        _check("v17D config loads + validates", False, str(e))
    except Exception as e:
        _check("v17D config loads + validates", False, f"{type(e).__name__}: {e}")

    # 5. parquet dirs
    _check("indicator dir exists", Path(indicator_dir).is_dir(), indicator_dir)
    _check("1-min dir exists", Path(onemin_dir).is_dir(), onemin_dir)

    # 6. yesterday's reconciliation
    yest = (date.today() - timedelta(days=1))
    yest_recon = Path(reconcile_dir).expanduser() / f"reconciliation_{yest.strftime('%Y%m%d')}.txt"
    _check("yesterday's reconciliation file present",
           yest_recon.exists(), str(yest_recon),
           warn_only=True)

    # 7. kill switch state
    armed = ks.is_armed()
    _check("kill switch clear", not armed,
           "kill switch ACTIVE — manual halt in effect" if armed else "")

    # 8. disk space
    log_dir = Path(reconcile_dir).expanduser()
    if log_dir.exists():
        free_gb = shutil.disk_usage(log_dir).free / (1024 ** 3)
        _check("log disk free > 5 GB", free_gb >= 5.0,
               f"{free_gb:.1f} GB free", warn_only=(free_gb >= 1.0))

    return n_pass, n_warn, n_fail, lines


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--indicator-dir", required=True)
    p.add_argument("--onemin-dir", required=True)
    p.add_argument("--universe-csv", default=None)
    p.add_argument("--reconcile-dir", default="~/v17D_logs")
    p.add_argument("--config", default=None)
    args = p.parse_args()

    n_pass, n_warn, n_fail, lines = check(
        args.indicator_dir, args.onemin_dir,
        args.universe_csv, args.reconcile_dir, args.config,
    )

    print(f"v17D pre-flight check — {datetime.now().isoformat(timespec='seconds')}")
    print("=" * 70)
    for line in lines:
        print(line)
    print("=" * 70)
    print(f"  PASS: {n_pass}   WARN: {n_warn}   FAIL: {n_fail}")
    if n_fail > 0:
        print("\n  ABORT: pre-flight FAILED. Do not start trading.")
        sys.exit(1)
    if n_warn > 0:
        print("\n  WARNING: pre-flight passed with warnings. Review before trading.")
        sys.exit(2)
    print("\n  GREEN: ok to start trading.")
    sys.exit(0)


if __name__ == "__main__":
    main()
