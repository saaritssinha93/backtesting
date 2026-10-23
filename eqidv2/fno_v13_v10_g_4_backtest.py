"""Run G4's G3-matched historical comparison, or an archived G4 profile.

This runner writes only to the G-4 backtest result root. G-3, dashboard, paper,
and LIVE configuration and pointers remain untouched.
"""
from __future__ import annotations

import argparse
import json
import secrets
from datetime import date, datetime
from pathlib import Path

from eqidv2_runtime_paths import runtime_dir
from research.g4_late_slot_backtest import run as run_original
from research.g4_late_slot_source import DEFAULT_CACHE as ORIGINAL_CACHE
from research.g4_extended_slot_backtest import (
    DEFAULT_CACHE, VERSION as EXTENDED_VERSION, MIDDAY_VERSION, MIDDAY_CACHE, run as run_extended,
    AFTERNOON_VERSION, AFTERNOON_CACHE,
)
from research.g4_selected_backtest import VERSION as SELECTED_VERSION, run as run_selected
from research.g4_midday_selected_backtest import (
    DEFAULT_CONFIG as MIDDAY_SELECTED_CONFIG, VERSION as MIDDAY_SELECTED_VERSION, run as run_midday_selected,
)
from research.g4_g3_mirror_backtest import (
    VERSION as MIRROR_VERSION, PREVIOUS_VERSION as MIRROR_V9,
    V8_VERSION as MIRROR_V8,
    LEGACY_VERSION as MIRROR_V7, run as run_mirror,
)
from research.g4_bounded_exit_backtest import (
    VERSION as BOUNDED_VERSION, run as run_bounded,
)
from research.g4_complete_inventory_backtest import (
    DEFAULT_CONFIG, VERSION as COMPLETE_VERSION, PREVIOUS_VERSION as COMPLETE_PREVIOUS_VERSION,
    run as run_complete,
)
from research.g4_1216_comparison import (
    run as run_1216_comparison, STUDY_LAST_DATE as COMPARISON_STUDY_LAST_DATE,
)
from fno_v13_v10_g_3_contract import IST


BACKTEST_ROOT = runtime_dir("backtesting_result_v13_v10_g_4")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--source-cache", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--through", type=date.fromisoformat, help="G3-matched history cutoff; defaults to current complete G3 history")
    parser.add_argument("--reuse-candidate-pool", type=Path, help="Verified prior G4 mirror run with the same G3 baseline")
    parser.add_argument("--approved-comparison", choices=["1216_SHORT"],
        help="Publish only the approved frozen comparison through 2026-10-16; original G4 is unchanged")
    args = parser.parse_args()
    now = datetime.now(IST)
    suffix = 'approved_1216_comparison' if args.approved_comparison else 'late_slots'
    output = args.output_dir or (BACKTEST_ROOT / "runs" / f"{now:%Y-%m-%d}" /
                                 f"{now:%Y%m%dT%H%M%S%f}_{secrets.token_hex(4)}_{suffix}")
    root = BACKTEST_ROOT.resolve()
    if not output.resolve().is_relative_to(root):
        parser.error("G4 output must remain in the isolated G-4 backtest root")
    if args.approved_comparison:
        if args.source_cache or args.reuse_candidate_pool or args.config.resolve()!=DEFAULT_CONFIG.resolve():
            parser.error("The frozen approved comparison does not accept configuration or source overrides")
        if args.through is not None and str(args.through)!=COMPARISON_STUDY_LAST_DATE:
            parser.error("The approved comparison is frozen through 2026-10-16; later sessions have not been evaluated")
        result = run_1216_comparison(output)
        print(json.dumps({key:result[key] for key in ('scope','comparison_only',
            'original_parameters_changed','study_last_date')},indent=2),flush=True)
        print(output.resolve(),flush=True)
        return
    version = json.loads(args.config.read_text(encoding="utf-8")).get("variant_id")
    if version in (BOUNDED_VERSION, COMPLETE_VERSION, COMPLETE_PREVIOUS_VERSION):
        if args.source_cache or args.reuse_candidate_pool:
            parser.error("Source-cache overrides do not apply to the bounded G4 profile")
        runner = run_complete if version in (COMPLETE_VERSION, COMPLETE_PREVIOUS_VERSION) else run_bounded
        result = runner(args.config, output, through=args.through)
        print(json.dumps({key: result[key] for key in ('sessions', 'first_date', 'last_date',
              'active_setups', 'comparison')}, indent=2), flush=True)
        print(output.resolve(), flush=True)
        return
    if version in (MIRROR_VERSION, MIRROR_V9, MIRROR_V8, MIRROR_V7):
        result = run_mirror(args.config, output, through=args.through,
                            candidate_pool_run=args.reuse_candidate_pool)
        print(json.dumps(dict(sessions=result['sessions'], last_date=result['last_date'],
                              scenarios=result['scenarios']), indent=2), flush=True)
        print(output.resolve(), flush=True)
        return
    if args.through is not None:
        parser.error("--through is supported only by the G3-matched G4 profile")
    if version == MIDDAY_SELECTED_VERSION:
        runner, cache = run_midday_selected, MIDDAY_CACHE
        keys = ("sessions", "added_setups", "total_setups", "baseline_g4", "selected_additions",
                "combined_g4", "incremental_net_pnl_rs", "baseline_trades_changed")
    elif version == SELECTED_VERSION:
        runner, cache = run_selected, DEFAULT_CACHE
        keys = ("sessions", "added_setups", "total_setups", "baseline_g4", "selected_additions",
                "combined_g4", "incremental_net_pnl_rs", "baseline_trades_changed")
    elif version in (EXTENDED_VERSION, MIDDAY_VERSION, AFTERNOON_VERSION):
        runner = run_extended
        cache = {EXTENDED_VERSION: DEFAULT_CACHE, MIDDAY_VERSION: MIDDAY_CACHE,
                 AFTERNOON_VERSION: AFTERNOON_CACHE}[version]
        keys = ("sessions", "new_setups", "tested_configurations", "training_eligible_setups",
                "chronological_check_pass_setups", "historical_fit_pass_setups")
    elif version == "V13-V10-G-4-BACKTEST-LATE-SLOTS-V1":
        runner, cache = run_original, ORIGINAL_CACHE
        keys = ("sessions", "baseline_g3", "g4_all_12", "added_late_setups", "net_pnl_delta_rs", "qualified_setups")
    else:
        parser.error(f"Unknown G4 backtest profile: {version}")
    result = runner(args.config, output, cache=args.source_cache or cache)
    print(json.dumps({key: result[key] for key in keys}, indent=2), flush=True)
    print(output.resolve(), flush=True)


if __name__ == "__main__":
    main()
