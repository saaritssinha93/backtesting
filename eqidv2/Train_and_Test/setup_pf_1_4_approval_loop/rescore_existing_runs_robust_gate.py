"""Fast robust-gate re-score of the existing 12 setup approval summaries.

This does not rerun the optimizer. It applies the new statistical gate to the
already-completed run_summary.json files and writes an immediate classification.
Neighborhood/dropout checks are marked FULL_RERUN_REQUIRED because those require
new engine evaluations.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUT = ROOT / "robust_gate_existing_rescore"

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

TRAIN_PF_MIN = 1.30
TEST_PF_MIN = 1.30
TEST_DAY_BLOCK_P_MAX = 0.10
MIN_TEST_TRADES = 6
MIN_TRAIN_TRADES = 20
MIN_TRAIN_TARGET_RATE = 12.0
MAX_MASK_TERMS = 1
MAX_PM_TERMS = 1


def metric(summary: dict, split: str) -> dict:
    return summary.get("best_metrics", {}).get(split, {}) or {}


def rate(m: dict, key: str) -> float:
    n = float(m.get("n") or 0)
    return round(float(m.get(key) or 0) / n * 100.0, 1) if n > 0 else 0.0


def term_counts(summary: dict) -> tuple[int, int]:
    cfg = summary.get("best_cfg", {}) or {}
    return len(cfg.get("mask_terms", []) or []), len(cfg.get("pre_momentum_terms", []) or [])


def finite_le(value, limit: float) -> bool:
    return value is not None and float(value) <= limit


def classify(summary: dict) -> dict:
    tr = metric(summary, "train_15bps")
    te = metric(summary, "test_15bps")
    mask_n, pm_n = term_counts(summary)
    train_target_rate = rate(tr, "tgt_cnt")
    test_target_rate = rate(te, "tgt_cnt")
    hard = []
    insuff = []
    warnings = []

    if (tr.get("n") or 0) < MIN_TRAIN_TRADES:
        hard.append(f"TRAIN n<{MIN_TRAIN_TRADES}")
    if (tr.get("net_pf") or 0.0) < TRAIN_PF_MIN:
        hard.append(f"TRAIN PF<{TRAIN_PF_MIN}")
    if train_target_rate < MIN_TRAIN_TARGET_RATE:
        hard.append(f"TRAIN target-fill<{MIN_TRAIN_TARGET_RATE}%")
    if mask_n > MAX_MASK_TERMS:
        hard.append(f"mask_terms>{MAX_MASK_TERMS}")
    if pm_n > MAX_PM_TERMS:
        hard.append(f"premom_terms>{MAX_PM_TERMS}")

    if (te.get("n") or 0) < MIN_TEST_TRADES:
        insuff.append(f"TEST n<{MIN_TEST_TRADES}")
    else:
        if (te.get("net_pf") or 0.0) < TEST_PF_MIN:
            hard.append(f"TEST PF<{TEST_PF_MIN}")
        dbp = te.get("day_block_p")
        if dbp is None:
            insuff.append("TEST day-block p unavailable")
        elif not finite_le(dbp, TEST_DAY_BLOCK_P_MAX):
            hard.append(f"TEST day-block p>{TEST_DAY_BLOCK_P_MAX}")

    warnings.append("neighborhood/dropout FULL_RERUN_REQUIRED")
    if not hard and not insuff:
        verdict = "PENDING_FULL_RERUN"
    elif not hard and insuff:
        verdict = "INSUFFICIENT_OOS"
    else:
        verdict = "REJECT"
    return {
        "setup": summary.get("setup"),
        "side": summary.get("side"),
        "verdict": verdict,
        "train_n": tr.get("n"),
        "train_pf": tr.get("net_pf"),
        "train_target_rate": train_target_rate,
        "test_n": te.get("n"),
        "test_pf": te.get("net_pf"),
        "test_day_block_p": te.get("day_block_p"),
        "test_target_rate": test_target_rate,
        "mask_terms": mask_n,
        "premom_terms": pm_n,
        "hard_reasons": hard,
        "insufficient_reasons": insuff,
        "warnings": warnings,
    }


def join(items: list[str]) -> str:
    return "; ".join(items) if items else "-"


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    rows = []
    for setup in SETUPS:
        path = ROOT / setup / "run_summary.json"
        if not path.exists():
            rows.append({"setup": setup, "verdict": "MISSING", "hard_reasons": ["missing run_summary"]})
            continue
        rows.append(classify(json.loads(path.read_text(encoding="utf-8"))))

    counts = {}
    for row in rows:
        counts[row["verdict"]] = counts.get(row["verdict"], 0) + 1

    (OUT / "rescore_rows.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    lines = [
        "# Robust Gate Existing-Run Re-score",
        "",
        f"_Generated {date.today().isoformat()}. Fast re-score only; no optimizer rerun; no final config edits._",
        "",
        "This applies the cheap robust-gate pieces to the completed 12 setup runs:",
        "",
        "- complexity cap: <=1 mask term and <=1 pre-momentum term",
        "- TRAIN PF >= 1.30",
        "- TRAIN target-fill >= 12%",
        "- TEST PF >= 1.30 and day-block p <= 0.10 when TEST has enough trades",
        "- TEST n < 6 is classified as INSUFFICIENT_OOS, not hard rejection",
        "- neighborhood/dropout robustness still requires the slower full rerun",
        "",
        f"Verdict counts: `{json.dumps(counts, sort_keys=True)}`",
        "",
        "| setup | side | verdict | TRAIN n/PF/tgt% | TEST n/PF/p/tgt% | terms mask/pm | hard reasons | insufficient | warnings |",
        "|---|---|---|---:|---:|---:|---|---|---|",
    ]
    for row in rows:
        lines.append(
            f"| {row.get('setup')} | {row.get('side','-')} | {row.get('verdict')} | "
            f"{row.get('train_n','-')}/{row.get('train_pf','-')}/{row.get('train_target_rate','-')} | "
            f"{row.get('test_n','-')}/{row.get('test_pf','-')}/{row.get('test_day_block_p','-')}/{row.get('test_target_rate','-')} | "
            f"{row.get('mask_terms','-')}/{row.get('premom_terms','-')} | "
            f"{join(row.get('hard_reasons', []))} | {join(row.get('insufficient_reasons', []))} | "
            f"{join(row.get('warnings', []))} |"
        )
    lines.extend([
        "",
        "Interpretation:",
        "",
        "- `REJECT`: failed a robust gate that can be assessed from existing completed runs.",
        "- `INSUFFICIENT_OOS`: train-side evidence exists, but TEST is too thin to confirm/reject.",
        "- `PENDING_FULL_RERUN`: cheap gates are clear, but neighborhood/dropout checks still need the full engine.",
    ])
    (OUT / "ROBUST_GATE_EXISTING_RESCORE.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(OUT / "ROBUST_GATE_EXISTING_RESCORE.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
