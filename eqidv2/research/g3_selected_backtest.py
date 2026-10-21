"""Compatibility entry point for the complete current G3 historical replay.

Use an explicit cutoff, or a canonical backtests/runs/YYYY-MM-DD output path.
Archived selected-afternoon packages remain readable through their V7 verifier.
"""
from datetime import date
from pathlib import Path

from eqidv2_runtime_paths import runtime_dir
from research.g3_target_cutoff_backtest import FILES

ROOT = runtime_dir("backtesting_result_v13_v10_g_3")


def run(output, through=None, *, root=None, base=None, roots=None):
    from research.g3_target_cutoff_backtest import build
    root, output = Path(root or ROOT).resolve(), Path(output).resolve()
    if through is None:
        try:
            relative = output.relative_to(root)
            if relative.parts[:2] != ("backtests", "runs") or len(relative.parts) != 4:
                raise ValueError
            through = date.fromisoformat(relative.parts[2])
        except ValueError as exc:
            raise ValueError("Current G3 replay requires an explicit through date or a canonical "
                             "backtests/runs/YYYY-MM-DD output directory") from exc
    return build(root, output, through, base=base, roots=roots)


def verified_package(directory, root=None):
    from research.g3_current_history import verified_package as verify
    return verify(directory, ROOT if root is None else root)
