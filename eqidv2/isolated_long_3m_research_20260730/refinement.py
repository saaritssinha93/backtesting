"""TRAIN/VALIDATION-only refinement of the compression family.

The broad research showed no edge in pullback or ORB. Compression was the only
family with a positive TRAIN region, so this file tests causal late-session
windows and simultaneous-signal ranking without altering the underlying setup.
TEST is evaluated only after one rule is frozen by TRAIN/VALIDATION.
"""

from __future__ import annotations

import json
from dataclasses import asdict

import numpy as np
import pandas as pd

import research as r


WINDOWS = {
    "12:30-14:29": (750, 869),
    "13:00-14:29": (780, 869),
    "13:30-14:29": (810, 869),
    "13:45-14:29": (825, 869),
    "14:00-14:29": (840, 869),
}
TOP_NS = (0, 10, 20)
EXITS = [
    r.ExitConfig(target, stop, 60)
    for target in (0.65, 0.75, 0.85)
    for stop in (0.70, 0.80, 0.90)
]


def ranked_indices(
    entries: pd.DataFrame,
    cfg: r.EntryConfig,
    start: int,
    end: int,
    top_n: int,
) -> np.ndarray:
    idx, score = r.config_indices(entries, cfg)
    if len(idx) == 0:
        return idx
    selected = entries.loc[idx].copy()
    selected = selected[selected["minute"].between(start, end)]
    if selected.empty:
        return np.array([], dtype=int)
    selected["_rank"] = (
        score.loc[selected.index].astype(float)
        + selected["rel_volume"].clip(upper=2.0).fillna(0) * .75
        + selected["adx_slope"].clip(lower=0, upper=10).fillna(0) * .05
        + selected["rsi_slope"].clip(lower=0, upper=15).fillna(0) * .035
        - selected["avwap_ext"].clip(lower=0).fillna(0) * 1.50
        - selected["range_atr"].clip(lower=0).fillna(0) * .20
    )
    if top_n > 0:
        selected = selected.sort_values(
            ["date", "_rank", "traded_value", "ticker"],
            ascending=[True, False, False, True],
        )
        selected = selected.groupby("date", as_index=False, sort=False).head(top_n)
    return selected.index.to_numpy(dtype=int)


def main() -> int:
    entries = pd.read_parquet(r.OUT / "valid_entries.parquet")
    z = np.load(r.OUT / "paths.npz")
    paths = {k: z[k] for k in z.files}
    configs = [c for c in r.entry_configs() if c.strategy == "compression"]
    rows: list[dict] = []
    index_cache: dict[tuple[str, str, int], np.ndarray] = {}
    for n, cfg in enumerate(configs, 1):
        for window, (start, end) in WINDOWS.items():
            for top_n in TOP_NS:
                idx = ranked_indices(entries, cfg, start, end, top_n)
                index_cache[(cfg.name, window, top_n)] = idx
                train_idx = r.split_indices(entries, idx, "train")
                val_idx = r.split_indices(entries, idx, "validation")
                for ex in EXITS:
                    train = r.metrics(r.simulate(entries, paths, train_idx, ex))
                    valid = r.metrics(r.simulate(entries, paths, val_idx, ex))
                    rows.append({
                        "config": cfg.name,
                        "profile": cfg.profile,
                        "variant": cfg.variant,
                        "regime": cfg.regime,
                        "window": window,
                        "top_n_per_signal_time": top_n,
                        **asdict(ex),
                        **{f"train_{k}": v for k, v in train.items()},
                        **{f"validation_{k}": v for k, v in valid.items()},
                    })
        if n % 12 == 0:
            print(f"[refinement] {n}/{len(configs)}")
    results = pd.DataFrame(rows)
    results.to_csv(r.OUT / "compression_refinement_train_validation.csv", index=False)
    eligible = results[
        (results["train_trades"] >= 30)
        & (results["validation_trades"] >= 8)
        & (results["train_active_days"] >= 8)
        & (results["validation_active_days"] >= 3)
        & (results["train_profit_factor"] > 1.15)
        & (results["validation_profit_factor"] > 1.10)
        & (results["train_expectancy"] > 0)
        & (results["validation_expectancy"] > 0)
    ].copy()
    if eligible.empty:
        pool = results[
            (results["train_trades"] >= 30)
            & (results["validation_trades"] >= 8)
            & (results["train_active_days"] >= 8)
            & (results["validation_active_days"] >= 3)
        ].copy()
    else:
        pool = eligible
    pool["selection_score"] = (
        np.minimum(pool["train_profit_factor"], pool["validation_profit_factor"])
        + .001 * np.minimum(pool["validation_trades"], 100)
        - .15 * abs(np.log(
            pool["validation_profit_factor"].clip(lower=.01)
            / pool["train_profit_factor"].clip(lower=.01)
        ))
    )
    frozen = pool.sort_values(
        ["selection_score", "validation_expectancy", "validation_trades"],
        ascending=False,
    ).iloc[0]
    cfg = next(c for c in configs if c.name == frozen["config"])
    ex = r.ExitConfig(
        float(frozen["target_pct"]), float(frozen["stop_pct"]),
        int(frozen["hold_min"]), str(frozen["mode"]),
    )
    idx = index_cache[(cfg.name, frozen["window"], int(frozen["top_n_per_signal_time"]))]
    test_idx = r.split_indices(entries, idx, "test")
    trades = r.simulate(entries, paths, test_idx, ex)
    test = r.metrics(trades)
    stress = r.metrics(r.simulate(entries, paths, test_idx, ex, 1.5))
    neighbors: list[dict] = []
    for target in sorted({max(.35, ex.target_pct - .10), ex.target_pct, ex.target_pct + .10}):
        for stop in sorted({max(.40, ex.stop_pct - .10), ex.stop_pct, ex.stop_pct + .10}):
            nex = r.ExitConfig(target, stop, ex.hold_min)
            for split in ("train", "validation", "test"):
                split_idx = r.split_indices(entries, idx, split)
                m = r.metrics(r.simulate(entries, paths, split_idx, nex))
                neighbors.append({
                    "target_pct": target, "stop_pct": stop, "split": split, **m
                })
    neighbor_df = pd.DataFrame(neighbors)
    neighbor_df.to_csv(r.OUT / "compression_refinement_neighbors.csv", index=False)
    final_dir = r.OUT / "refined_compression"
    final_dir.mkdir(parents=True, exist_ok=True)
    trades.to_csv(final_dir / "test_trades.csv", index=False)
    trades.groupby("session", as_index=False)["net_pnl"].sum().to_csv(
        final_dir / "test_daily.csv", index=False
    )
    payload = {
        "selection_used_test": False,
        "entry_config": asdict(cfg),
        "window": frozen["window"],
        "top_n_per_signal_time": int(frozen["top_n_per_signal_time"]),
        "exit_config": asdict(ex),
        "train": {
            k.removeprefix("train_"): frozen[k] for k in frozen.index if k.startswith("train_")
        },
        "validation": {
            k.removeprefix("validation_"): frozen[k]
            for k in frozen.index if k.startswith("validation_")
        },
        "test": test,
        "test_150pct_slippage": stress,
        "eligible_train_validation_rules": int(len(eligible)),
        "classification": (
            "ROBUST_ACCEPT"
            if test["trades"] >= 100 and test["profit_factor"] > 1.4
            and stress["profit_factor"] > 1.0
            and test["top_day_share_pct"] < 20 and test["top_ticker_share_pct"] < 10
            else "PROFITABLE_BUT_INSUFFICIENT_SAMPLE"
            if test["profit_factor"] > 1.0 and test["expectancy"] > 0
            else "REJECT"
        ),
    }
    (r.OUT / "compression_refinement_result.json").write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8"
    )
    write_report(payload, neighbor_df)
    print(json.dumps(payload, indent=2, default=str))
    return 0


def write_report(payload: dict, neighbors: pd.DataFrame) -> None:
    c, e = payload["entry_config"], payload["exit_config"]
    profitable_neighbors = neighbors[
        (neighbors["profit_factor"] > 1) & (neighbors["expectancy"] > 0)
    ].groupby("split").size().to_dict()
    lines = [
        "# Compression breakout refinement",
        "",
        f"## Classification: {payload['classification']}",
        "",
        "This rule was selected using TRAIN and VALIDATION only. TEST was evaluated after the "
        "configuration, late-session window, ranking limit, target, stop, and time exit were frozen.",
        "",
        "## Exact rule",
        "",
        f"- Base configuration: `{c['name']}` ({c['profile']} / {c['variant']} / {c['regime']})",
        f"- Signal window: {payload['window']}",
        f"- Simultaneous rank limit: {payload['top_n_per_signal_time'] or 'unlimited'}",
        f"- Target: {e['target_pct']:.2f}%",
        f"- Stop: {e['stop_pct']:.2f}%",
        f"- Time exit: {e['hold_min']} minutes, capped by 15:15",
        "",
        "| Split | Trades | PF | Expectancy | Net P&L | Active days | Top day share | Top ticker share |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label, key in (("TRAIN", "train"), ("VALIDATION", "validation"), ("TEST", "test")):
        m = payload[key]
        lines.append(
            f"| {label} | {int(m['trades'])} | {float(m['profit_factor']):.3f} | "
            f"Rs {float(m['expectancy']):,.2f} | Rs {float(m['net_profit']):,.2f} | "
            f"{int(m['active_days'])} | {float(m['top_day_share_pct']):.2f}% | "
            f"{float(m['top_ticker_share_pct']):.2f}% |"
        )
    s = payload["test_150pct_slippage"]
    lines += [
        "",
        f"At 150% of normal exit slippage, TEST PF is {s['profit_factor']:.3f}, "
        f"expectancy is Rs {s['expectancy']:,.2f}, and net P&L is Rs {s['net_profit']:,.2f}.",
        "",
        "## Honest interpretation",
        "",
        f"Profitable neighboring target/stop cases: TRAIN {profitable_neighbors.get('train', 0)}/9, "
        f"VALIDATION {profitable_neighbors.get('validation', 0)}/9, "
        f"TEST {profitable_neighbors.get('test', 0)}/9.",
        "",
        "A positive TEST result with fewer than 100 trades is not sufficient for production promotion. "
        "High day/ticker concentration also fails the framework's robustness gates. Treat this as a "
        "profitable research lead for forward paper collection, not a proven live strategy.",
    ]
    (r.OUT / "COMPRESSION_REFINEMENT_REPORT.md").write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
