"""Run the isolated V13-V10-G-4 late-slot historical experiment.

This runner writes only to the G-4 backtest result root. G-3, dashboard, paper,
and LIVE configuration and pointers remain untouched.
"""
from __future__ import annotations

import argparse
import json
import secrets
from datetime import datetime
from pathlib import Path

from eqidv2_runtime_paths import runtime_dir
from research.g4_late_slot_backtest import run as run_original
from research.g4_late_slot_source import DEFAULT_CACHE as ORIGINAL_CACHE
from research.g4_extended_slot_backtest import (
    DEFAULT_CACHE, VERSION as EXTENDED_VERSION, MIDDAY_VERSION, MIDDAY_CACHE, run as run_extended,
)
from research.g4_selected_backtest import DEFAULT_CONFIG, VERSION as SELECTED_VERSION, run as run_selected
from fno_v13_v10_g_3_contract import IST


BACKTEST_ROOT = runtime_dir("backtesting_result_v13_v10_g_4")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--source-cache", type=Path)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    now = datetime.now(IST)
    output = args.output_dir or (BACKTEST_ROOT / "runs" / f"{now:%Y-%m-%d}" /
                                 f"{now:%Y%m%dT%H%M%S%f}_{secrets.token_hex(4)}_late_slots")
    root = BACKTEST_ROOT.resolve()
    if not output.resolve().is_relative_to(root):
        parser.error("G4 output must remain in the isolated G-4 backtest root")
    version = json.loads(args.config.read_text(encoding="utf-8")).get("variant_id")
    if version == SELECTED_VERSION:
        runner, cache = run_selected, DEFAULT_CACHE
        keys = ("sessions", "added_setups", "total_setups", "baseline_g4", "selected_additions",
                "combined_g4", "incremental_net_pnl_rs", "baseline_trades_changed")
    elif version in (EXTENDED_VERSION, MIDDAY_VERSION):
        runner, cache = run_extended, MIDDAY_CACHE if version == MIDDAY_VERSION else DEFAULT_CACHE
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
