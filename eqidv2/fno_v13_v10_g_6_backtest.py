"""Run G6's 51-setup backtest with its two-direction Nifty entry gate."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path
import secrets

from fno_v13_v10_g_3_contract import IST
from research.g6_g4_replica_backtest import DEFAULT_CONFIG, G6_ROOT, configuration, run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--through", type=date.fromisoformat,
                        help="Cutoff must match the latest verified complete G3 source")
    parser.add_argument("--nifty-filter-mode", choices=("both_counter_skip", "off"),
                        help="G6 Nifty entry gate; default follows the saved G6 configuration. Use off to roll back.")
    parser.add_argument("--verify-config", action="store_true",
                        help="Verify replica parameters without running or publishing a backtest")
    args = parser.parse_args()
    config = configuration(args.config)
    if args.verify_config:
        print(json.dumps(dict(variant_id=config["variant_id"],
            replica_source_variant=config["replica_source_variant"],
            active_setups=config["active_setup_count"], output_root=str(G6_ROOT),
            initial_stop_pct=config["initial_stop_pct"],
            tightened_stop_pct=config["tightened_stop_pct"],
            tighten_after_minutes=config["tighten_after_minutes"],
            nifty_direction_filter_mode=args.nifty_filter_mode or config['nifty_direction_filter']['mode'],
            nifty_direction_filter_enabled=(args.nifty_filter_mode or config['nifty_direction_filter']['mode']) == 'both_counter_skip',
            execution_authority=False), indent=2))
        return
    now = datetime.now(IST)
    output = args.output_dir or G6_ROOT / "runs" / f"{now:%Y-%m-%d}" / (
        f"{now:%Y%m%dT%H%M%S%f}_{secrets.token_hex(4)}_g6_nifty")
    if not output.resolve().is_relative_to(G6_ROOT.resolve()):
        parser.error("G6 output must remain inside its isolated G6 result root")
    result = run(args.config, output, through=args.through,
                 nifty_filter_mode=args.nifty_filter_mode)
    print(json.dumps({key: result[key] for key in (
        "variant_id", "sessions", "first_date", "last_date", "active_setups", "comparison")},
        indent=2))
    print(output.resolve())


if __name__ == "__main__":
    main()
