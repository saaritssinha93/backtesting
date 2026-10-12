"""Render target-versus-P&L and win-rate charts from a completed sealed sweep."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import pandas as pd

from research.g3_0920_backtest import sha256


def render(run: Path, output: Path):
    manifest = json.loads((run / "manifest.json").read_text(encoding="utf-8"))
    for name in ("long_target_sweep.csv", "short_target_sweep.csv", "summary.json"):
        if sha256(run / name) != manifest["files"][name]:
            raise ValueError(f"Changed sweep artifact: {name}")
    summary = json.loads((run / "summary.json").read_text(encoding="utf-8"))
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                         "axes.spines.top": False, "axes.spines.right": False})
    fig, axes = plt.subplots(2, 2, figsize=(12, 7.6), constrained_layout=False)
    fig.patch.set_facecolor("#f6f8fb")
    for index, (side, color) in enumerate((("LONG", "#087f8c"), ("SHORT", "#b5485d"))):
        lower = side.lower()
        table = pd.read_csv(run / f"{lower}_target_sweep.csv")
        x = table[f"{lower}_target_pct"]
        metric = f"all_added_{lower}_"
        n = int(summary["selected_entry_counts"][side])
        for col, (field, ylabel) in enumerate((("net_pnl_rs", "Net P&L (INR)"), ("win_rate_pct", "Win rate (%)"))):
            ax = axes[index, col]
            ax.set_facecolor("white")
            ax.plot(x, table[metric + field], color=color, linewidth=2)
            ax.axvline(1., color="#8893a0", linewidth=1, linestyle=":", label="Previous 1% target")
            ax.set(xlabel="Profit target (%)", ylabel=ylabel, xlim=(max(0, float(x.min()) - .02), float(x.max()) + .05))
            ax.set_title(f"{side} — {n} trades", loc="left", fontweight="bold", pad=10)
            ax.grid(axis="y", color="#e3e8ef", linewidth=.7)
            if col == 0:
                ax.axhline(0, color="#8994a2", linewidth=.8)
                ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y:,.0f}"))
            else:
                ax.axhline(70, color="#667080", linewidth=1, linestyle="--", label="70% screen")
                ax.set_ylim(-3, 106)
                ax.legend(loc="lower right", frameon=False, fontsize=8)
        label = "best_long_min5_70" if side == "LONG" else "best_short_small_sample_70"
        winner = summary["winners"][label]
        if winner:
            target = winner[f"{lower}_target_pct"]
            stats = winner["periods"]["all"][f"added_{lower}"]
            axes[index, 0].scatter([target], [stats["net_pnl_rs"]], color=color, s=60, zorder=4)
            offset = -170 if target > float(x.max()) * .8 else 12
            axes[index, 0].annotate(f"Observed best >=70%: {target:.2f}%", (target, stats["net_pnl_rs"]),
                                     xytext=(offset, -25), textcoords="offset points", fontsize=9, color=color)
    fig.suptitle("09:20 profit-target sensitivity", x=.075, y=.985, ha="left", fontsize=18, fontweight="bold")
    fig.text(.075, .933, "Fixed entries and staged SL | July 29–October 12, 2026 | Historical observations", fontsize=10, color="#536071")
    fig.text(.075, .025, "INR 100,000 margin/trade × 5 exposure; 5 bps round-trip cost. SHORT has only two observations.",
             fontsize=9, color="#536071")
    fig.subplots_adjust(top=.85, bottom=.12, left=.085, right=.98, hspace=.47, wspace=.24)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=170, facecolor=fig.get_facecolor())
    plt.close(fig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    render(args.run, args.output)
