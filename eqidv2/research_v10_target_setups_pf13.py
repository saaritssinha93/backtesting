from __future__ import annotations

import itertools
import math
from pathlib import Path

import numpy as np
import pandas as pd

import research_v10_per_setup_honest as h
import research_v10_setup_deep_dive as deep


OUT_DIR = h.OUT_DIR
TARGET_SETUPS = [
    "C_OR_BREAKOUT",
    "D_EMA20_BOUNCE",
    "E_ORB_BREAKOUT_LONG",
    "E_VWAP_BAND_FADE",
    "E_VWAP_LOSE_EARLY_SHORT",
    "L_BB_SQUEEZE_LONG",
]

NUMERIC_FEATURES = deep.BASE_NUMERIC_FEATURES + deep.DERIVED_NUMERIC_FEATURES
CATEGORICAL_FEATURES = deep.CATEGORICAL_FEATURES
QUANTILES = [0.03, 0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20, 0.25, 0.30,
             0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75, 0.80,
             0.82, 0.85, 0.88, 0.90, 0.92, 0.95, 0.97]


def _pf_value(value: float) -> float:
    if pd.isna(value):
        return -1.0
    if value == math.inf:
        return 10.0
    return float(value)


def _stats(frame: pd.DataFrame) -> dict[str, dict]:
    masks = h._split_masks(frame)
    return {k: h._metrics(frame[v]) for k, v in masks.items()}


def _min_counts(base_stats: dict[str, dict]) -> tuple[int, int]:
    train_n = int(base_stats["train"]["trades"])
    valid_n = int(base_stats["valid"]["trades"])
    min_train = max(10, int(train_n * 0.03))
    min_valid = max(3, int(valid_n * 0.03))
    return min_train, min_valid


def _score(stats: dict[str, dict], min_train: int, min_valid: int) -> float:
    train = stats["train"]
    valid = stats["valid"]
    if train["trades"] < min_train or valid["trades"] < min_valid:
        return -math.inf
    train_pf = _pf_value(train["pf"])
    valid_pf = _pf_value(valid["pf"])
    if train_pf < 1.0 or valid_pf < 1.0:
        return -math.inf
    min_pf = min(train_pf, valid_pf)
    dev_pnl = max(0.0, float(train["pnl"] + valid["pnl"]))
    size = min(math.log1p(train["trades"]), math.log1p(valid["trades"]))
    return min_pf * 10000.0 + (train_pf + valid_pf) * 100.0 + dev_pnl / 1000.0 + size * 20.0


def _classify(stats: dict[str, dict]) -> str:
    full_pf = _pf_value(stats["full"]["pf"])
    test_pf = _pf_value(stats["test"]["pf"])
    train_pf = _pf_value(stats["train"]["pf"])
    valid_pf = _pf_value(stats["valid"]["pf"])
    if full_pf >= 1.30 and train_pf >= 1.10 and valid_pf >= 1.10 and stats["test"]["trades"] >= 5 and test_pf >= 1.10:
        return "PF13_OOS_PASS"
    if full_pf >= 1.30 and train_pf >= 1.00 and valid_pf >= 1.00 and stats["test"]["trades"] >= 3 and test_pf >= 1.00:
        return "PF13_PROBATION"
    if full_pf >= 1.30:
        return "PF13_FULL_ONLY_OR_LOW_SAMPLE"
    return "FAIL_BELOW_1_3"


def _condition_pool(df: pd.DataFrame, setup: str) -> list[tuple[str, pd.Series]]:
    setup_mask = df["setup"].astype(str).eq(setup)
    train_mask = h._split_masks(df)["train"]
    train = df[setup_mask & train_mask]
    rules: list[tuple[str, pd.Series]] = [("ALL", setup_mask)]
    for col in NUMERIC_FEATURES:
        if col not in df.columns:
            continue
        train_values = pd.to_numeric(train[col], errors="coerce").dropna()
        if train_values.nunique() < 6:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        for q in QUANTILES:
            threshold = float(train_values.quantile(q))
            if not np.isfinite(threshold):
                continue
            rules.append((f"{col}>={threshold:.8g}", setup_mask & (series >= threshold)))
            rules.append((f"{col}<={threshold:.8g}", setup_mask & (series <= threshold)))
    for col in CATEGORICAL_FEATURES:
        if col not in df.columns:
            continue
        counts = train[col].astype(str).value_counts()
        for value, count in counts.items():
            if count >= max(4, int(len(train) * 0.025)):
                rules.append((f"{col}=={value}", setup_mask & df[col].astype(str).eq(value)))
    return rules


def _evaluate(df: pd.DataFrame, setup: str, variant: str, rule: str, mask: pd.Series, min_train: int, min_valid: int) -> dict:
    frame = df[mask].copy()
    stats = _stats(frame)
    score = _score(stats, min_train, min_valid)
    return {
        "setup": setup,
        "variant": variant,
        "rule": rule,
        "mask": mask,
        "frame": frame,
        "stats": stats,
        "score": score,
        "decision": _classify(stats),
    }


def _candidate_search(df: pd.DataFrame, setup: str) -> list[dict]:
    setup_mask = df["setup"].astype(str).eq(setup)
    base = _evaluate(df, setup, "baseline", "ALL", setup_mask, 1, 1)
    min_train, min_valid = _min_counts(base["stats"])
    singles = []
    candidates = [base]
    for rule, mask in _condition_pool(df, setup):
        item = _evaluate(df, setup, "single", rule, mask, min_train, min_valid)
        singles.append(item)
        candidates.append(item)

    eligible_singles = [x for x in singles if x["score"] != -math.inf]
    eligible_singles.sort(key=lambda x: (x["score"], _pf_value(x["stats"]["full"]["pf"]), x["stats"]["full"]["pnl"]), reverse=True)
    pool = eligible_singles[:120]
    seen = {x["rule"] for x in candidates}

    doubles = []
    for a, b in itertools.combinations(pool[:90], 2):
        for op, variant, mask in [
            (" AND ", "double_and", a["mask"] & b["mask"]),
            (" OR ", "double_or", a["mask"] | b["mask"]),
        ]:
            rule = f"({a['rule']}){op}({b['rule']})"
            if rule in seen:
                continue
            seen.add(rule)
            item = _evaluate(df, setup, variant, rule, mask, min_train, min_valid)
            if item["score"] != -math.inf or _pf_value(item["stats"]["full"]["pf"]) >= 1.3:
                doubles.append(item)
                candidates.append(item)

    doubles.sort(key=lambda x: (x["score"], _pf_value(x["stats"]["full"]["pf"]), x["stats"]["full"]["pnl"]), reverse=True)
    for pair in doubles[:100]:
        for single in pool[:80]:
            if single["rule"] in pair["rule"]:
                continue
            rule = f"({pair['rule']}) AND ({single['rule']})"
            if rule in seen:
                continue
            seen.add(rule)
            item = _evaluate(df, setup, "triple_and", rule, pair["mask"] & single["mask"], min_train, min_valid)
            if item["score"] != -math.inf or _pf_value(item["stats"]["full"]["pf"]) >= 1.3:
                candidates.append(item)

    candidates.sort(key=lambda x: (x["decision"] in {"PF13_OOS_PASS", "PF13_PROBATION"}, x["score"], _pf_value(x["stats"]["full"]["pf"]), x["stats"]["full"]["pnl"]), reverse=True)
    return candidates


def _rows_for_item(item: dict) -> list[dict]:
    rows = []
    for split, metrics in item["stats"].items():
        row = {
            "setup": item["setup"],
            "variant": item["variant"],
            "rule": item["rule"],
            "exit_rule": "current",
            "decision": item["decision"],
            "dev_score": item["score"],
            "split": split,
        }
        row.update(metrics)
        rows.append(row)
    return rows


def main() -> int:
    df = deep._add_derived_features(h._load_trades())
    all_rows = []
    summary_rows = []
    for setup in TARGET_SETUPS:
        print(f"[pf13] {setup}", flush=True)
        candidates = _candidate_search(df, setup)
        keep = []
        seen_sig = set()
        for item in candidates:
            sig = (item["variant"], item["rule"], item["decision"])
            if sig in seen_sig:
                continue
            seen_sig.add(sig)
            if item["decision"] != "FAIL_BELOW_1_3" or len(keep) < 12:
                keep.append(item)
            if len(keep) >= 30:
                break
        for item in keep:
            all_rows.extend(_rows_for_item(item))
        ranked = sorted(
            [x for x in candidates if x["decision"] != "FAIL_BELOW_1_3"],
            key=lambda x: (
                {"PF13_OOS_PASS": 3, "PF13_PROBATION": 2, "PF13_FULL_ONLY_OR_LOW_SAMPLE": 1}.get(x["decision"], 0),
                x["score"],
                _pf_value(x["stats"]["full"]["pf"]),
                x["stats"]["full"]["pnl"],
            ),
            reverse=True,
        )
        best = ranked[0] if ranked else candidates[0]
        row = {
            "setup": setup,
            "selected_variant": best["variant"],
            "selected_rule": best["rule"],
            "selected_exit": "current",
            "decision": best["decision"],
            "dev_score": best["score"],
        }
        for split, metrics in best["stats"].items():
            for key, value in metrics.items():
                row[f"{split}_{key}"] = value
        summary_rows.append(row)

    pd.DataFrame(all_rows).to_csv(OUT_DIR / "target_pf13_option_results.csv", index=False)
    pd.DataFrame(summary_rows).to_csv(OUT_DIR / "target_pf13_selected_by_setup.csv", index=False)
    print("[done] wrote target_pf13_selected_by_setup.csv", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
