"""Build read-only, path-aware V13-V10-G execution diagnostics."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ai_platform.observability.execution_research import (  # noqa: E402
    generate_execution_research,
)
from ai_platform.observability.strategy_research import discover_source_run  # noqa: E402
from eqidv2_runtime_paths import runtime_dir  # noqa: E402
from fno_strategy_registry import active_profile  # noqa: E402


DEFAULT_SOURCE_ROOT = runtime_dir(
    "fno_oi", "strategy_research", "v13_v10_g_full_history"
)
DEFAULT_PAPER_ROOT = runtime_dir("fno_oi", "v13_v10_g_live", "orders", "PAPER")
DEFAULT_OUTPUT_ROOT = runtime_dir("v13_v10_g_execution_research")
ACTIVE_PROFILE = active_profile()
if ACTIVE_PROFILE.name == "V13_V10_G_3":
    DEFAULT_SOURCE_ROOT = runtime_dir(ACTIVE_PROFILE.backtest_dir)
    DEFAULT_PAPER_ROOT = runtime_dir("fno_oi", ACTIVE_PROFILE.live_dir, "orders", "PAPER")
    DEFAULT_OUTPUT_ROOT = runtime_dir(ACTIVE_PROFILE.slug + "_execution_research")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE_ROOT,
        help="Complete historical run or parent containing complete run_* directories",
    )
    parser.add_argument(
        "--paper-root",
        type=Path,
        default=DEFAULT_PAPER_ROOT,
        help="Read-only root containing terminal PAPER order JSON states",
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if ACTIVE_PROFILE.name == "V13_V10_G_3":
        from ai_platform.observability.g3_execution_analysis import generate_g3_execution_analysis
        result = generate_g3_execution_analysis(source_root=args.source, paper_root=args.paper_root,
                                               output_root=args.output_root)
        print(json.dumps({key: result[key] for key in ("state", "strategy_version", "source_run", "run_dir", "baseline")}, indent=2))
        return 0
    source_run = discover_source_run(args.source)
    bundle = generate_execution_research(
        source_run=source_run,
        paper_root=args.paper_root,
        output_root=args.output_root,
    )
    print(json.dumps(bundle.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
