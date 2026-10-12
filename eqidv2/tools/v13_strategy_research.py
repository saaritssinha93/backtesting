"""Build read-only V13-V10-G strategy-research dashboard artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ai_platform.observability.strategy_research import (  # noqa: E402
    discover_source_run,
    generate_research_bundle,
)
from eqidv2_runtime_paths import runtime_dir  # noqa: E402
from fno_strategy_registry import active_profile  # noqa: E402


DEFAULT_SOURCE_ROOT = runtime_dir(
    "fno_oi", "strategy_research", "v13_v10_g_full_history"
)
DEFAULT_OUTPUT_ROOT = runtime_dir("v13_v10_g_strategy_research")
DEFAULT_LIVE_ROOT = runtime_dir("fno_oi", "v13_v10_g_live")
DEFAULT_REPLAY_ROOT = runtime_dir("backtesting_result_v13_v10_g", "runs")
DEFAULT_EXECUTION_RESEARCH_ROOT = runtime_dir("v13_v10_g_execution_research")
ACTIVE_PROFILE = active_profile()
if ACTIVE_PROFILE.name == "V13_V10_G_3":
    DEFAULT_SOURCE_ROOT = runtime_dir(ACTIVE_PROFILE.backtest_dir)
    DEFAULT_OUTPUT_ROOT = runtime_dir(ACTIVE_PROFILE.slug + "_strategy_research")
    DEFAULT_LIVE_ROOT = runtime_dir("fno_oi", ACTIVE_PROFILE.live_dir)
    DEFAULT_REPLAY_ROOT = DEFAULT_SOURCE_ROOT / "production" / "runs"
    DEFAULT_EXECUTION_RESEARCH_ROOT = runtime_dir(ACTIVE_PROFILE.slug + "_execution_research")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=DEFAULT_SOURCE_ROOT,
        help="Complete run directory or parent containing run_* directories",
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--live-root", type=Path, default=DEFAULT_LIVE_ROOT)
    parser.add_argument("--replay-root", type=Path, default=DEFAULT_REPLAY_ROOT)
    parser.add_argument(
        "--execution-research-root",
        type=Path,
        default=DEFAULT_EXECUTION_RESEARCH_ROOT,
    )
    parser.add_argument(
        "--registry-root",
        type=Path,
        default=REPO_ROOT / "research_outputs" / "observability_experiments",
    )
    parser.add_argument("--minimum-history", type=int, default=20)
    parser.add_argument("--minimum-context", type=int, default=5)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.minimum_history < 1 or args.minimum_context < 1:
        raise SystemExit("minimum history/context must be positive")
    if ACTIVE_PROFILE.name == "V13_V10_G_3":
        from ai_platform.observability.g3_analysis import generate_g3_analysis
        result = generate_g3_analysis(source_root=args.source, output_root=args.output_root,
                                      execution_research_root=args.execution_research_root)
        print(json.dumps({key: result[key] for key in ("state", "strategy_version", "source_run", "run_dir", "baseline")}, indent=2))
        return 0
    source_run = discover_source_run(args.source)
    bundle = generate_research_bundle(
        source_run=source_run,
        output_root=args.output_root,
        registry_root=args.registry_root,
        live_root=args.live_root,
        replay_root=args.replay_root,
        execution_research_root=args.execution_research_root,
        minimum_history=args.minimum_history,
        minimum_context=args.minimum_context,
    )
    print(json.dumps(bundle.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
