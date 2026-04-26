# -*- coding: utf-8 -*-
"""
V17G A/B harness.

Runs the seven configurations defined in v17f_new_design_proposal.md against
the same backtest window and emits a single comparison table.

Usage:
    py -3.12 eqidv2/v17g_ab_runner.py

Each configuration is launched as a subprocess with the appropriate
EQIDV17G_* env vars set. The actual backtest engine is the v17g runner
(`avwap_combined_runner_v17g_5min.py`); this file only orchestrates,
collects metrics, and writes the comparison report.

This is an orchestration skeleton. The runner module it invokes
(`avwap_combined_runner_v17g_5min.py`) must exist and must accept the
EQIDV17G_* env vars as documented in v17g_implementation_checklist.md.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parent
RUNNER_MODULE = REPO_ROOT / "avwap_combined_runner_v17g_5min.py"
BASELINE_RUNNER = REPO_ROOT / "avwap_combined_runner_v17f_5min.py"
OUTPUT_ROOT = REPO_ROOT / "outputs_v17g_ab"
COMPARISON_REPORT = (
    REPO_ROOT
    / "reports"
    / "strategy_comparisons"
    / "v17g_ab_comparison.md"
)


# ---------------------------------------------------------------------------
# Configurations
# ---------------------------------------------------------------------------
@dataclass
class RunSpec:
    name: str
    runner: Path
    env: Dict[str, str] = field(default_factory=dict)
    description: str = ""


def _v17g_off_env() -> Dict[str, str]:
    """All v17g toggles explicitly off — equals v17f baseline behavior
    when run through the v17g runner."""
    return {
        "EQIDV17G_FIX_LONG_PULLBACK_LAG": "0",
        "EQIDV17G_ENABLE_LONG_REVERSAL": "0",
        "EQIDV17G_SETUP_LEVEL_SIZING": "0",
        "EQIDV17G_CONSOLIDATE_SHORT_POCKETS": "0",
        "EQIDV17G_LONG_AVWAP_DIST_CAP_ON": "0",
    }


def _with(*flags: str) -> Dict[str, str]:
    env = _v17g_off_env()
    for flag in flags:
        env[flag] = "1"
    return env


CONFIGS: List[RunSpec] = [
    RunSpec(
        name="baseline",
        runner=BASELINE_RUNNER,
        env={},
        description="v17f as-is, reference run.",
    ),
    RunSpec(
        name="v17g-1",
        runner=RUNNER_MODULE,
        env=_with("EQIDV17G_FIX_LONG_PULLBACK_LAG"),
        description="Change 1 only — fix LONG pullback lag bug.",
    ),
    RunSpec(
        name="v17g-2",
        runner=RUNNER_MODULE,
        env=_with("EQIDV17G_ENABLE_LONG_REVERSAL"),
        description="Change 2 only — add LONG reversal setup.",
    ),
    RunSpec(
        name="v17g-3",
        runner=RUNNER_MODULE,
        env=_with("EQIDV17G_SETUP_LEVEL_SIZING"),
        description="Change 3 only — setup-level position sizing.",
    ),
    RunSpec(
        name="v17g-4",
        runner=RUNNER_MODULE,
        env=_with("EQIDV17G_CONSOLIDATE_SHORT_POCKETS"),
        description="Change 4 only — consolidate fragmented SHORT pockets.",
    ),
    RunSpec(
        name="v17g-5",
        runner=RUNNER_MODULE,
        env=_with("EQIDV17G_LONG_AVWAP_DIST_CAP_ON"),
        description="Change 5 only — LONG AVWAP distance cap.",
    ),
    RunSpec(
        name="v17g-all",
        runner=RUNNER_MODULE,
        env=_with(
            "EQIDV17G_FIX_LONG_PULLBACK_LAG",
            "EQIDV17G_ENABLE_LONG_REVERSAL",
            "EQIDV17G_SETUP_LEVEL_SIZING",
            "EQIDV17G_CONSOLIDATE_SHORT_POCKETS",
            "EQIDV17G_LONG_AVWAP_DIST_CAP_ON",
        ),
        description="All five changes enabled — proposed v17g-final.",
    ),
]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
def _run_single(spec: RunSpec, output_root: Path) -> Path:
    run_dir = output_root / spec.name
    run_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env.update(spec.env)
    env["EQIDV17G_AB_RUN_NAME"] = spec.name
    env["EQIDV17G_AB_OUTPUT_DIR"] = str(run_dir)

    print(f"[V17G_AB] running {spec.name}: {spec.description}")
    print(f"[V17G_AB]   runner = {spec.runner.name}")
    print(f"[V17G_AB]   env    = {spec.env}")

    cmd = [sys.executable, str(spec.runner)]
    proc = subprocess.run(cmd, env=env, cwd=str(REPO_ROOT))
    if proc.returncode != 0:
        raise RuntimeError(
            f"run {spec.name} failed with exit code {proc.returncode}"
        )
    return run_dir


# ---------------------------------------------------------------------------
# Metric collection
# ---------------------------------------------------------------------------
METRICS_OF_INTEREST = (
    "trade_count",
    "day_count",
    "day_win_pct",
    "trade_win_pct",
    "profit_factor",
    "max_drawdown_pct",
    "sharpe",
    "calmar",
    "long_trade_count",
    "short_trade_count",
    "long_pf",
    "short_pf",
    "max_consec_losing_days",
)


def _load_metrics(run_dir: Path) -> Dict[str, Optional[float]]:
    """Load metrics from the runner's standard output. Each runner is
    expected to drop a `metrics.json` file under the run output dir.
    Falls back to a CSV summary if metrics.json is missing.
    """
    metrics_json = run_dir / "metrics.json"
    if metrics_json.exists():
        with metrics_json.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        return {k: data.get(k) for k in METRICS_OF_INTEREST}
    # Fallback: leave empty so the report shows what is missing.
    return {k: None for k in METRICS_OF_INTEREST}


# ---------------------------------------------------------------------------
# Comparison report
# ---------------------------------------------------------------------------
def _write_comparison(
    results: Dict[str, Dict[str, Optional[float]]],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    headers = ["metric"] + list(results.keys())
    rows = []
    for metric in METRICS_OF_INTEREST:
        row = [metric]
        for run_name in results:
            val = results[run_name].get(metric)
            row.append("" if val is None else f"{val}")
        rows.append(row)

    lines = []
    lines.append("# V17G A/B Comparison")
    lines.append("")
    lines.append("Auto-generated by `v17g_ab_runner.py`. Do not hand-edit.")
    lines.append("")
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join(["---"] * len(headers)) + "|")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")
    lines.append("## Decision rules")
    lines.append("")
    lines.append("- per-change run advances if OOS PF >= baseline - 0.05, "
                 "OOS day_win >= baseline - 2pp, MaxDD no worse than +10% baseline")
    lines.append("- v17g-all ships if it beats baseline on >=3 of "
                 "{PF, MaxDD, Sharpe} OOS")
    lines.append("")
    output_path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> int:
    if not RUNNER_MODULE.exists():
        print(
            f"[V17G_AB] ERROR: runner module not found: {RUNNER_MODULE}",
            file=sys.stderr,
        )
        print(
            "[V17G_AB] Create avwap_combined_runner_v17g_5min.py per "
            "v17g_implementation_checklist.md before running this harness.",
            file=sys.stderr,
        )
        return 2

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    results: Dict[str, Dict[str, Optional[float]]] = {}

    for spec in CONFIGS:
        try:
            run_dir = _run_single(spec, OUTPUT_ROOT)
            results[spec.name] = _load_metrics(run_dir)
        except Exception as exc:  # noqa: BLE001 - we want all failures recorded
            print(f"[V17G_AB] {spec.name} failed: {exc}", file=sys.stderr)
            results[spec.name] = {k: None for k in METRICS_OF_INTEREST}

    _write_comparison(results, COMPARISON_REPORT)
    print(f"[V17G_AB] comparison written: {COMPARISON_REPORT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
