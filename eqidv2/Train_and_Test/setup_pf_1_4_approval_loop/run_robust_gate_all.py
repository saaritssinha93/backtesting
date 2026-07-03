"""Run the robust PF gate across the 2026-06-29 12-setup pool.

This is a research-only batch wrapper. It does not edit final_setup_conf.py.
Outputs go under:
  Train_and_Test/setup_pf_1_4_approval_loop/robust_gate_results/
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parent
ENGINE = ROOT / "_engine" / "pf_band_fitval_loop.py"
DEFAULT_POOL_ROOT = Path(r"C:\TradingData\eqidv2\setup_pools_2026_06_29")
DEFAULT_OUT = ROOT / "robust_gate_results"

SETUPS = [
    "A_MOD_BREAK_C1_LOW",
    "B_AVWAP_RECLAIM_REVERSAL",
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK",
    "B_HUGE_RED_FAILED_BOUNCE",
    "E_VWAP_LOSE_EARLY_SHORT",
    "G_HIGHER_HIGH_BREAK",
    "G_LOWER_LOW_BREAK",
    "L_BB_SQUEEZE_LONG",
    "L_DOUBLE_BOTTOM_VWAP",
    "L_PRESSURE_BURST_VWAP",
    "MR_CONTROLLED_VWAP_EXTREME_FADE_LONG",
    "MR_VWAP_EXTREME_RECLAIM_LONG",
]


def metric(summary: dict, split: str) -> dict:
    return summary.get("best_metrics", {}).get(split, {}) or {}


def short_reason(items: list[str]) -> str:
    return "; ".join(items) if items else "-"


def run_one(setup: str, args: argparse.Namespace) -> int:
    pool = args.pool_root / setup
    log_dir = args.out / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(ENGINE),
        "--setup", setup,
        "--pool", str(pool),
        "--trials", str(args.trials),
        "--time_budget_min", str(args.time_budget_min),
        "--seed", str(args.seed),
        "--out", str(args.out),
        "--gap_lambda", str(args.gap_lambda),
        "--max_mask_terms", str(args.max_mask_terms),
        "--max_pm_terms", str(args.max_pm_terms),
        "--test_pf_min", str(args.test_pf_min),
        "--max_test_day_block_p", str(args.max_test_day_block_p),
        "--min_train_target_rate", str(args.min_train_target_rate),
        "--neighborhood_pf_min", str(args.neighborhood_pf_min),
        "--dropout_pf_min", str(args.dropout_pf_min),
        "--pm_quantile_sample", str(args.pm_quantile_sample),
        "--min_trades_test", str(args.min_trades_test),
    ]
    print(f"[batch] {setup}")
    with (log_dir / f"{setup}.log").open("w", encoding="utf-8") as fh:
        fh.write(" ".join(cmd) + "\n\n")
        proc = subprocess.run(cmd, stdout=fh, stderr=subprocess.STDOUT, text=True)
    print(f"[batch] {setup} rc={proc.returncode}")
    return int(proc.returncode)


def write_scoreboard(out_dir: Path, setups: list[str]) -> Path:
    rows = []
    for setup in setups:
        path = out_dir / setup / "run_summary.json"
        if not path.exists():
            rows.append({"setup": setup, "missing": True})
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        tr = metric(data, "train_15bps")
        te = metric(data, "test_15bps")
        robust = data.get("robustness", {}) or {}
        rows.append({
            "setup": setup,
            "side": data.get("side", ""),
            "verdict": data.get("verdict", "UNKNOWN"),
            "train_n": tr.get("n"),
            "train_pf": tr.get("net_pf"),
            "train_tgt": tr.get("target_rate"),
            "train_dbp": tr.get("day_block_p"),
            "test_n": te.get("n"),
            "test_pf": te.get("net_pf"),
            "test_tgt": te.get("target_rate"),
            "test_dbp": te.get("day_block_p"),
            "neighbor": robust.get("neighbor_pass"),
            "dropout": robust.get("dropout_pass"),
            "hard": data.get("hard_reasons", []),
            "insuff": data.get("insufficient_reasons", []),
            "warnings": data.get("warnings", []),
            "missing": False,
        })

    counts = {}
    for row in rows:
        verdict = "MISSING" if row.get("missing") else row["verdict"]
        counts[verdict] = counts.get(verdict, 0) + 1

    lines = [
        "# Robust PF Gate Rerun Scoreboard",
        "",
        f"_Generated {date.today().isoformat()}. Research-only. NO final_setup_conf.py edits, NO live trades._",
        "",
        "Gate changes versus the previous PF-band run:",
        "",
        "- Search complexity capped at <=1 mask term and <=1 pre-momentum term.",
        "- FIT/VAL disagreement penalty raised through gap_lambda.",
        "- TEST confirmation uses PF >= 1.30 plus day-block p <= 0.10.",
        "- TRAIN target-fill rate must clear the configured minimum.",
        "- Neighborhood check perturbs each threshold by one TRAIN quantile step.",
        "- Term-dropout check removes each term once and requires the book to remain non-losing.",
        "- Thin OOS samples are classified as INSUFFICIENT_OOS rather than automatic rejects.",
        "",
        f"Verdict counts: {json.dumps(counts, sort_keys=True)}",
        "",
        "| setup | side | verdict | TRAIN n/PF/tgt% | TEST n/PF/p/tgt% | robust N/D | hard reasons | insufficient | warnings |",
        "|---|---|---|---:|---:|---|---|---|---|",
    ]
    for row in rows:
        if row.get("missing"):
            lines.append(f"| {row['setup']} | - | MISSING | - | - | - | - | - | - |")
            continue
        lines.append(
            f"| {row['setup']} | {row['side']} | {row['verdict']} | "
            f"{row['train_n']}/{row['train_pf']}/{row['train_tgt']} | "
            f"{row['test_n']}/{row['test_pf']}/{row['test_dbp']}/{row['test_tgt']} | "
            f"{row['neighbor']}/{row['dropout']} | "
            f"{short_reason(row['hard'])} | {short_reason(row['insuff'])} | {short_reason(row['warnings'])} |"
        )

    lines.extend([
        "",
        "Per-setup detail lives in `robust_gate_results/<SETUP>/`.",
        "Logs live in `robust_gate_results/logs/`.",
    ])
    path = out_dir / "ROBUST_SCOREBOARD.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool-root", type=Path, default=DEFAULT_POOL_ROOT)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--trials", type=int, default=220)
    ap.add_argument("--time-budget-min", type=float, default=8.0)
    ap.add_argument("--seed", type=int, default=17)
    ap.add_argument("--gap-lambda", type=float, default=0.80)
    ap.add_argument("--max-mask-terms", type=int, default=1)
    ap.add_argument("--max-pm-terms", type=int, default=1)
    ap.add_argument("--test-pf-min", type=float, default=1.30)
    ap.add_argument("--max-test-day-block-p", type=float, default=0.10)
    ap.add_argument("--min-train-target-rate", type=float, default=12.0)
    ap.add_argument("--neighborhood-pf-min", type=float, default=1.15)
    ap.add_argument("--dropout-pf-min", type=float, default=1.00)
    ap.add_argument("--pm-quantile-sample", type=int, default=1500)
    ap.add_argument("--min-trades-test", type=int, default=6)
    ap.add_argument("--setups", nargs="*", default=SETUPS)
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    rc = 0
    for setup in args.setups:
        code = run_one(setup, args)
        rc = rc or code
    scoreboard = write_scoreboard(args.out, args.setups)
    print(f"[batch] scoreboard={scoreboard}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
