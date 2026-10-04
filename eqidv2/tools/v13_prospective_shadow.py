"""Capture or verify no-authority V13-V10-G prospective shadow evidence."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ai_platform.observability.prospective_shadow import (  # noqa: E402
    finalize_shadow_session,
    prepare_shadow_session,
    seal_shadow_decisions,
    verify_shadow_session,
)
from eqidv2_runtime_paths import runtime_dir  # noqa: E402


DEFAULT_OUTPUT_ROOT = runtime_dir("v13_v10_g_strategy_research")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="Freeze inputs before 09:15 IST")
    prepare.add_argument("--session-date", required=True, type=date.fromisoformat)
    prepare.add_argument("--dataset", required=True, type=Path)
    prepare.add_argument("--model", required=True, type=Path)
    prepare.add_argument("--strategy", required=True, type=Path)

    decisions = subparsers.add_parser(
        "seal-decisions", help="Seal a complete decision bundle before 15:30 IST"
    )
    decisions.add_argument("--session-date", required=True, type=date.fromisoformat)
    decisions.add_argument("--decisions", required=True, type=Path)

    finalize = subparsers.add_parser(
        "finalize", help="Join a complete outcome bundle after 15:30 IST"
    )
    finalize.add_argument("--session-date", required=True, type=date.fromisoformat)
    finalize.add_argument("--outcomes", required=True, type=Path)

    verify = subparsers.add_parser("verify", help="Verify final artifact and journal hashes")
    verify.add_argument("--session-date", required=True, type=date.fromisoformat)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "prepare":
        result = prepare_shadow_session(
            session_date=args.session_date,
            dataset=args.dataset,
            model=args.model,
            strategy=args.strategy,
            output_root=args.output_root,
        ).as_dict()
    elif args.command == "seal-decisions":
        result = seal_shadow_decisions(
            session_date=args.session_date,
            decisions=args.decisions,
            output_root=args.output_root,
        ).as_dict()
    elif args.command == "finalize":
        result = finalize_shadow_session(
            session_date=args.session_date,
            outcomes=args.outcomes,
            output_root=args.output_root,
        ).as_dict()
    else:
        result = verify_shadow_session(
            session_date=args.session_date,
            output_root=args.output_root,
        )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("state") not in {"INVALID"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
