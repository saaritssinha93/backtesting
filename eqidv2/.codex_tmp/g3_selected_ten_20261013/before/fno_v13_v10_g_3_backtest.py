"""Publish current G3 historical backtesting from the start through the cutoff.

The default verifies the complete V4 historical simulation and extends it with
same-contract completed daily minute replays. Legacy studies are explicit
modes. A verified normal run publishes the current historical pointer.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import tempfile
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from eqidv2_runtime_paths import runtime_dir
from research.g3_freeze import DEFAULT_FROZEN, build_frozen, export_archived_replay, load_frozen


BACKTEST_ROOT = runtime_dir("backtesting_result_v13_v10_g_3")
OCTOBER12_SOURCE = runtime_dir("fno_oi", "strategy_research", "v13_v10_g_3_0950_long_backtesting",
                               "oct12_baseline_source_20261012T1600")
IST = ZoneInfo("Asia/Kolkata")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def publish_current_backtest(root: Path, run_dir: Path) -> dict:
    """Publish a verified historical pointer without touching production pointers."""
    root, run_dir = root.resolve(), run_dir.resolve()
    relative = run_dir.relative_to(root)
    if relative.parts[:2] != ("backtests", "runs") or len(relative.parts) != 4:
        raise ValueError("Current G-3 backtest must be under backtests/runs/<day>/<run>")
    manifest_path = run_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    from research.g3_0950_long_backtest import PREFERRED_CONFIG
    expected = json.loads(PREFERRED_CONFIG.read_text(encoding="utf-8"))
    if (manifest.get("variant_id") != expected["variant_id"]
            or manifest.get("execution_authority") is not False
            or _sha(Path(manifest["code"])) != manifest.get("code_sha256")
            or any(Path(name).name != name or not (run_dir / name).is_file()
                   or _sha(run_dir / name) != digest for name, digest in manifest["files"].items())):
        raise ValueError("G-3 backtest manifest or artifact verification failed")
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    if (config != expected or summary.get("variant_id") != expected["variant_id"]
            or summary.get("scope") != "BACKTEST_ONLY"
            or summary.get("execution_authority") is not False
            or summary.get("frozen_baseline_parity") is not True
            or summary.get("existing_g3_trades_unchanged") is not True
            or summary.get("last_date") != relative.parts[2]
            or summary.get("sessions") != 49):
        raise ValueError("G-3 current backtest does not match the agreed complete profile")
    pointer = dict(schema_version="fno_g3_current_backtest_v1", role="backtest_history",
                   run_dir=relative.as_posix(), manifest_sha256=_sha(manifest_path),
                   variant_id=expected["variant_id"], session_date=summary["last_date"],
                   execution_authority=False, created_at_ist=summary["created_at_ist"])
    latest = root / "latest"
    latest.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".current_backtest.", suffix=".tmp", dir=latest)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(pointer, stream, indent=2)
            stream.write("\n")
        os.replace(temporary, latest / "current_backtest.json")
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return pointer


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--through", type=date.fromisoformat,
                        help="Normal full-history cutoff, YYYY-MM-DD (default: today IST, after close)")
    parser.add_argument("--frozen-dir", type=Path, default=DEFAULT_FROZEN)
    parser.add_argument("--build-freeze", action="store_true", help="Seal accepted archived results and verified minute inputs")
    parser.add_argument("--six-variant-study", action="store_true", help="Explicitly rerun the earlier six-arm research")
    parser.add_argument("--verify-frozen", action="store_true", help="Verify or export the archived baseline instead of running the current profile")
    parser.add_argument("--backtest-0950-long", action="store_true",
                        help="Run the isolated 09:50 LONG backtest variant; production G3 remains unchanged")
    parser.add_argument("--with-g4-late-12", action="store_true",
                        help="Backtest current G3 V3 plus all twelve G4 10:05-10:30 LONG/SHORT setups")
    parser.add_argument("--g3-v3-run", type=Path, help="Verified current G3 V3 backtest source for --with-g4-late-12")
    parser.add_argument("--g4-run", type=Path, help="Verified G4 late-slot source for --with-g4-late-12")
    parser.add_argument("--october12-run", type=Path,
                        help="Completed, isolated October 12 full G3 source replay for the 09:50 variant")
    parser.add_argument("--0950-config", dest="target_config", type=Path,
                        help="Isolated 09:50 LONG configuration; defaults to the researched 0.75%% target")
    args = parser.parse_args()
    if args.with_g4_late_12:
        if args.build_freeze or args.six_variant_study or args.verify_frozen or args.backtest_0950_long or args.target_config is not None or args.october12_run is not None:
            parser.error("--with-g4-late-12 cannot combine with other G3 study modes")
        from research.g3_g4_late_combined_backtest import G3_RUN, G4_RUN, ROOT, run
        now = datetime.now(IST)
        output = args.output_dir or (ROOT / "backtests" / "runs" / "2026-10-12" /
            f"{now:%Y%m%dT%H%M%S%f}_{secrets.token_hex(4)}_v3_plus_g4_late_12")
        if not output.resolve().is_relative_to((ROOT / "backtests" / "runs").resolve()):
            parser.error("Combined G3 backtest output must be under G3 backtests/runs")
        result = run(args.g3_v3_run or G3_RUN, args.g4_run or G4_RUN, output)
        print(f"G3 V3 + G4 late twelve: {result['sessions']} sessions; "
              f"{result['g3_plus_late_12']['executed_trades']} fills; "
              f"net P&L Rs {result['g3_plus_late_12']['net_pnl_rs']:.2f}")
        print(output.resolve())
        return
    if args.backtest_0950_long:
        if args.build_freeze or args.six_variant_study or args.verify_frozen or args.output_dir is None or args.october12_run is None:
            parser.error("--backtest-0950-long requires --output-dir and --october12-run, without other study modes")
        from research.g3_0950_long_backtest import PREFERRED_CONFIG, DEFAULT_HISTORY_PROVENANCE, DEFAULT_RECENT_PROVENANCE, run
        result = run(args.target_config or PREFERRED_CONFIG, args.output_dir, DEFAULT_HISTORY_PROVENANCE,
                     DEFAULT_RECENT_PROVENANCE, args.october12_run)
        print(f"Backtesting-only 09:50 LONG: {result['sessions']} sessions; "
              f"{result['added_0950_long']['fills']} added fills; "
              f"{result['added_0950_long']['win_rate_pct']:.2f}% added-slot win rate")
        print(args.output_dir.resolve())
        return
    if args.six_variant_study:
        if args.build_freeze or args.verify_frozen or args.output_dir is None:
            parser.error("--six-variant-study requires --output-dir and cannot combine with --build-freeze")
        from research.g3_six_confirmation_variants import run as replay
        from research.g3_results_report import run as report
        replay(args.output_dir)
        report(args.output_dir)
        return
    if args.build_freeze or args.verify_frozen:
        data = build_frozen(args.frozen_dir) if args.build_freeze else load_frozen(args.frozen_dir)
        if args.output_dir:
            data = export_archived_replay(args.output_dir, args.frozen_dir)
        print(f"Verified archived frozen G-3: {len(data['days'])} sessions; "
              f"{len(data['trades'])} selected; {int(data['trades'].portfolio_executed.sum())} executed")
        print(data["path"])
        return
    from research.g3_current_history import build, publish
    now = datetime.now(IST)
    through = args.through or now.date()
    output = args.output_dir or (BACKTEST_ROOT / "backtests" / "runs" / str(through) /
        f"{now:%Y%m%dT%H%M%S%f}_{secrets.token_hex(4)}_current_full_history")
    try:
        relative = output.resolve().relative_to(BACKTEST_ROOT.resolve())
        if relative.parts[:2] != ("backtests", "runs") or len(relative.parts) != 4:
            raise ValueError
    except ValueError:
        parser.error("Current G-3 backtest output must be under backtests/runs/<day>/<run> in the G-3 result root")
    if args.target_config is not None or args.october12_run is not None or args.g3_v3_run is not None or args.g4_run is not None:
        parser.error("The normal current history uses its pinned source; source overrides require an explicit study mode")
    from backtesting_result_v13_v10_g_3_daily import _lock
    with _lock(BACKTEST_ROOT):
        result = build(BACKTEST_ROOT, output, through)
        pointer = publish(BACKTEST_ROOT, output)
    print(f"Current G-3 backtest: {result['sessions']} sessions; {result['current_g3']['executed_trades']} fills; "
          f"{result['current_g3']['win_rate_pct']:.2f}% win rate; net Rs {result['current_g3']['net_pnl_rs']:.2f}")
    print(output.resolve())
    print(BACKTEST_ROOT / "latest" / "current_backtest.json")


if __name__ == "__main__":
    main()
