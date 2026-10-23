"""Run G6's 51-setup backtest with its two-direction Nifty entry gate."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path
import secrets

from fno_v13_v10_g_3_contract import IST
from research.g6_g4_replica_backtest import DEFAULT_CONFIG, G6_ROOT, configuration, run
from research import g6_primary_index_direction as primary_filter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--through", type=date.fromisoformat,
                        help="Cutoff must match the latest verified complete G3 source")
    parser.add_argument("--date", dest="session_date", type=date.fromisoformat,
                        help="Replay only this closed session from its verified daily inputs; keep historical pointers unchanged")
    parser.add_argument("--direction-filter-mode", choices=primary_filter.CANONICAL_MODES,
                        help="G6 entry gate; default is the previous Nifty-futures both-directions rule. Use primary_index_both_counter_skip to restore the primary-index filter, or off for unfiltered entries.")
    parser.add_argument("--nifty-filter-mode", choices=("both_counter_skip", "off"),
                        help="Deprecated compatibility alias: both_counter_skip selects the prior Nifty-futures rule.")
    parser.add_argument("--verify-config", action="store_true",
                        help="Verify replica parameters without running or publishing a backtest")
    args = parser.parse_args()
    if args.session_date is not None and args.through is not None:
        parser.error("choose --date for one session or --through for full history, not both")
    config = configuration(args.config)
    if args.direction_filter_mode is not None and args.nifty_filter_mode is not None:
        parser.error("choose --direction-filter-mode or the legacy --nifty-filter-mode, not both")
    requested_mode = args.direction_filter_mode or args.nifty_filter_mode or config['direction_filter']['mode']
    mode = primary_filter.canonical_mode(requested_mode)
    if args.session_date is not None and mode == primary_filter.PRIMARY_FILTER:
        parser.error("--date supports the approved Nifty-futures filter or off; primary-index studies use full history")
    if args.verify_config:
        print(json.dumps(dict(variant_id=config["variant_id"],
            replica_source_variant=config["replica_source_variant"],
            active_setups=config["active_setup_count"], output_root=str(G6_ROOT),
            entry_override=config["entry_override"],
            initial_stop_pct=config["initial_stop_pct"],
            tightened_stop_pct=config["tightened_stop_pct"],
            tighten_after_minutes=config["tighten_after_minutes"],
            direction_filter_mode=mode,
            selected_policy=config['direction_filter']['alternatives'][mode],
            direction_filter_enabled=mode != primary_filter.OFF,
            nifty_direction_filter_mode=config['nifty_direction_filter']['mode'],
            nifty_direction_filter_enabled=mode == primary_filter.NIFTY_FILTER,
            execution_authority=False), indent=2))
        return
    now = datetime.now(IST)
    suffix = {primary_filter.NIFTY_FILTER: "g6_nifty_futures",
              primary_filter.PRIMARY_FILTER: "g6_primary_index",
              primary_filter.OFF: "g6_unfiltered"}[mode]
    run_root = G6_ROOT / "daily" / "runs" if args.session_date is not None else G6_ROOT / "runs"
    run_day = args.session_date or now.date()
    output = args.output_dir or run_root / str(run_day) / (
        f"{now:%Y%m%dT%H%M%S%f}_{secrets.token_hex(4)}_{suffix}")
    if not output.resolve().is_relative_to(G6_ROOT.resolve()):
        parser.error("G6 output must remain inside its isolated G6 result root")
    if args.session_date is not None:
        from research.g6_daily_backtest import run as run_daily
        result = run_daily(args.config, output, day=args.session_date, direction_filter_mode=mode)
    else:
        result = run(args.config, output, through=args.through,
                     direction_filter_mode=args.direction_filter_mode,
                     nifty_filter_mode=args.nifty_filter_mode)
    print(json.dumps({key: result[key] for key in (
        "variant_id", "sessions", "first_date", "last_date", "active_setups", "comparison")},
        indent=2))
    print(output.resolve())


if __name__ == "__main__":
    main()
