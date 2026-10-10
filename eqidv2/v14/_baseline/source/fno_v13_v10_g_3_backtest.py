"""Verify/export frozen accepted G-3; six-variant research is explicit opt-in.

Default mode verifies the archived LONG 1.10x, exact-next-minute baseline.
An output directory exports that verified archived replay; it does not rebuild
the entire universe or process new trading dates.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from research.g3_freeze import DEFAULT_FROZEN, build_frozen, export_archived_replay, load_frozen


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--frozen-dir", type=Path, default=DEFAULT_FROZEN)
    parser.add_argument("--build-freeze", action="store_true", help="Seal accepted archived results and verified minute inputs")
    parser.add_argument("--six-variant-study", action="store_true", help="Explicitly rerun the earlier six-arm research")
    args = parser.parse_args()
    if args.six_variant_study:
        if args.build_freeze or args.output_dir is None:
            parser.error("--six-variant-study requires --output-dir and cannot combine with --build-freeze")
        from research.g3_six_confirmation_variants import run as replay
        from research.g3_results_report import run as report
        replay(args.output_dir)
        report(args.output_dir)
        return
    data = build_frozen(args.frozen_dir) if args.build_freeze else load_frozen(args.frozen_dir)
    if args.output_dir:
        data = export_archived_replay(args.output_dir, args.frozen_dir)
    print(f"Verified archived frozen G-3: {len(data['days'])} sessions; "
          f"{len(data['trades'])} selected; {int(data['trades'].portfolio_executed.sum())} executed")
    print(data["path"])


if __name__ == "__main__":
    main()
