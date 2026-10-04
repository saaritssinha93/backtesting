"""V13-V10-H: isolated, reproducible G control and one sizing-only challenger."""
from __future__ import annotations

import argparse
from pathlib import Path

from ai_platform.research.v13h_execution import ExecutionModel
from ai_platform.research.v13h_research import DEFAULT_OUTPUT, DEFAULT_SOURCE, run


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-run', type=Path, default=DEFAULT_SOURCE,
                        help='Checksum-inventoried G historical bundle (not a live data root)')
    parser.add_argument('--output-root', type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument('--experiment', choices=['control', 'risk_3000', 'strategy_trials'], default='risk_3000')
    parser.add_argument('--run-id')
    parser.add_argument('--tick-size', type=float, default=.05, help='Assumed uniform tick; not exchange-verified')
    parser.add_argument('--cost-bps', type=float, default=5., help='Flat round-trip entry-notional cost proxy')
    parser.add_argument('--entry-slippage-bps', type=float, default=0.)
    parser.add_argument('--exit-slippage-bps', type=float, default=0.)
    parser.add_argument('--delay-minutes', type=int, default=0)
    parser.add_argument('--holdout-start', help='Optional future date to reserve; does not collect/validate future results')
    parser.add_argument('--holdout-end')
    parser.add_argument('--sector-map', type=Path,
                        help='Optional complete dated CSV for the sector-cap research trial')
    parser.add_argument('--vix-data', type=Path,
                        help='Optional historical India VIX CSV with observed_at, available_at, india_vix')
    args = parser.parse_args(argv)
    model = ExecutionModel(cost_bps=args.cost_bps, tick_size=args.tick_size,
                           entry_slippage_bps=args.entry_slippage_bps, exit_slippage_bps=args.exit_slippage_bps,
                           delay_minutes=args.delay_minutes)
    if args.experiment == 'strategy_trials':
        from datetime import datetime, timezone
        from ai_platform.research.v13h_strategy_trials import run as run_trials
        if args.holdout_start or args.holdout_end:
            parser.error('Historical strategy trials do not reserve or validate a holdout')
        output = run_trials(args.source_run, args.output_root, model=model,
                            run_id=args.run_id or datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'),
                            sector_map=args.sector_map, vix_data=args.vix_data)
        print(f'Completed H strategy trials: {output}')
        print(f'Results: {output / "results.json"}')
        return 0
    if args.sector_map is not None or args.vix_data is not None:
        parser.error('--sector-map and --vix-data apply only to strategy_trials')
    output = run(args.source_run, args.output_root, experiment=args.experiment, model=model,
                 run_id=args.run_id, holdout_start=args.holdout_start, holdout_end=args.holdout_end)
    print(f'Completed research-only H run: {output}')
    print(f'Report: {output / "REPORT.md"}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
