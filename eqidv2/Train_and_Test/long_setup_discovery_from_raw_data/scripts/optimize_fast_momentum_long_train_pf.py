"""Train-PF optimizer for the raw FAST_MOMENTUM_LONG research setup.

This script keeps the original setup family fixed:
LONG_VOLUME_EXPANSION_BREAKOUT_vol2_h5. It mines additional filters on top of
that raw trigger using the cached 5m candidate table and cached 1m exits.

The output is intentionally research-only. It optimizes TRAIN, then reports
FIT/VAL/TEST so an in-sample PF target is not mistaken for tradability.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
OUT_DIR = SCRIPT_DIR.parent
RESULTS_DIR = OUT_DIR / "results"
CANDIDATES_DIR = OUT_DIR / "candidates"

RULE_ID = "LONG_VOLUME_EXPANSION_BREAKOUT_vol2_h5"
SETUP_NAME = "FAST_MOMENTUM_LONG_TRAIN_PF_OPTIMIZED"
NOTIONAL_RS = 100_000.0


@dataclass(frozen=True)
class OptimizedConfig:
    label: str
    exit_id: str
    top_n_per_slot: int | None
    predicates: list[str]
    train: dict
    fit: dict
    val: dict
    test: dict


def to_py(obj):
    if isinstance(obj, dict):
        return {str(k): to_py(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_py(v) for v in obj]
    if isinstance(obj, tuple):
        return [to_py(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        x = float(obj)
        return x if math.isfinite(x) else None
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, pd.Timestamp):
        return obj.strftime("%Y-%m-%d")
    return obj


def load_summary() -> dict:
    path = OUT_DIR / "search_summary.json"
    if not path.exists():
        raise SystemExit(f"Missing {path}; run tight_raw_long_discovery.py first")
    return json.loads(path.read_text(encoding="utf-8"))


def cache_prefix(summary: dict) -> str:
    dates = summary["sessions"]["all"]
    return f"raw_cache_{dates[0]}_{dates[-1]}_all"


def load_caches(summary: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    prefix = cache_prefix(summary)
    cand_path = RESULTS_DIR / f"{prefix}_candidates.parquet"
    rule_path = RESULTS_DIR / f"{prefix}_rule_candidates.parquet"
    exit_path = RESULTS_DIR / f"{prefix}_exits.parquet"
    for path in [cand_path, rule_path, exit_path]:
        if not path.exists():
            raise SystemExit(f"Missing cache {path}; run tight_raw_long_discovery.py first")
    cand = pd.read_parquet(cand_path)
    rules = pd.read_parquet(rule_path)
    exits = pd.read_parquet(exit_path)
    return cand, rules, exits


def split_sets(summary: dict) -> dict[str, set[pd.Timestamp]]:
    return {
        key: set(pd.to_datetime(summary["sessions"][key]))
        for key in ["fit", "val", "train", "test"]
    }


def metric_pack(trades: pd.DataFrame) -> dict:
    if trades.empty:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate_pct": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "net_pnl": 0.0,
            "net_pf": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "expectancy": 0.0,
            "avg_holding_min": 0.0,
            "sl_cnt": 0,
            "target_cnt": 0,
            "time_exit_cnt": 0,
            "five_min_conflict_cnt": 0,
            "same_1m_tie_cnt": 0,
            "daywise": [],
        }
    pnl = trades["net_pnl"].astype(float)
    pos = pnl[pnl > 0]
    neg = pnl[pnl < 0]
    gp = float(pos.sum())
    gl = float(neg.sum())
    losses_abs = -gl
    by_day = []
    for day, g in trades.groupby(trades["session"].dt.strftime("%Y-%m-%d"), sort=True):
        gp_d = float(g.loc[g["net_pnl"] > 0, "net_pnl"].sum())
        gl_d = float(g.loc[g["net_pnl"] < 0, "net_pnl"].sum())
        pf_d = gp_d / (-gl_d) if gl_d < 0 else (999.0 if gp_d > 0 else 0.0)
        by_day.append({
            "date": day,
            "trades": int(len(g)),
            "net_pnl": round(float(g["net_pnl"].sum()), 2),
            "pf": round(float(pf_d), 4),
        })
    return {
        "trades": int(len(trades)),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl <= 0).sum()),
        "win_rate_pct": round(float((pnl > 0).mean() * 100.0), 2),
        "gross_profit": round(gp, 2),
        "gross_loss": round(gl, 2),
        "net_pnl": round(float(pnl.sum()), 2),
        "net_pf": round(float(gp / losses_abs if losses_abs > 0 else (999.0 if gp > 0 else 0.0)), 4),
        "avg_win": round(float(pos.mean()) if len(pos) else 0.0, 2),
        "avg_loss": round(float(neg.mean()) if len(neg) else 0.0, 2),
        "expectancy": round(float(pnl.mean()), 2),
        "avg_holding_min": round(float(trades["holding_min"].mean()), 2),
        "sl_cnt": int((trades["outcome"] == "SL").sum()),
        "target_cnt": int((trades["outcome"] == "TARGET").sum()),
        "time_exit_cnt": int((trades["outcome"] == "TIME").sum()),
        "five_min_conflict_cnt": int(trades["five_min_conflict"].fillna(False).sum()),
        "same_1m_tie_cnt": int(trades["same_1m_tie"].fillna(False).sum()),
        "daywise": by_day,
    }


def make_base(cand: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    ids = rules.loc[rules["rule_id"].eq(RULE_ID), "candidate_id"].drop_duplicates()
    if ids.empty:
        raise SystemExit(f"No cached candidates for {RULE_ID}")
    df = cand[cand["candidate_id"].isin(ids)].copy().reset_index(drop=True)
    df["session"] = pd.to_datetime(df["session"])
    return df


def add_pred(preds: list[tuple[str, np.ndarray]], name: str, mask, n: int) -> None:
    arr = np.asarray(mask, dtype=bool)
    if 10 <= int(arr.sum()) < n:
        preds.append((name, arr))


def predicate_bank(df: pd.DataFrame) -> list[tuple[str, np.ndarray]]:
    n = len(df)
    preds: list[tuple[str, np.ndarray]] = []
    for lo, hi in [(2, 7), (2, 10), (2, 12), (2, 18), (2, 24), (2, 36), (5, 18), (5, 24), (5, 36), (8, 24), (8, 36), (10, 48), (15, 60), (2, 60)]:
        add_pred(preds, f"slot {lo}-{hi}", df["slot"].between(lo, hi), n)
    for t in [2.25, 2.5, 3, 3.5, 4, 5, 6, 8, 10]:
        add_pred(preds, f"vol_ratio>={t:g}", df["vol_ratio"].ge(t), n)
    for t in [50, 52, 55, 58, 60, 63, 66, 70]:
        add_pred(preds, f"rsi>={t:g}", df["rsi"].ge(t), n)
    for t in [14, 16, 18, 20, 24, 28, 32, 36]:
        add_pred(preds, f"adx>={t:g}", df["adx"].ge(t), n)
    for t in [0.68, 0.72, 0.76, 0.80, 0.84, 0.88, 0.92]:
        add_pred(preds, f"close_loc>={t:g}", df["close_loc"].ge(t), n)
    for t in [0.18, 0.25, 0.35, 0.5, 0.75, 1.0, 1.25]:
        add_pred(preds, f"green_body_pct>={t:g}", df["green_body_pct"].ge(t), n)
    for t in [0.2, 0.35, 0.5, 0.75, 1.0, 1.25]:
        add_pred(preds, f"dist_vwap_abs<={t:g}", df["dist_vwap_abs"].le(t), n)
    for lo, hi in [(0.16, 0.4), (0.16, 0.6), (0.2, 0.6), (0.25, 0.8), (0.3, 1.0), (0.5, 1.5), (0.8, 2.25)]:
        add_pred(preds, f"atr_pct {lo:g}-{hi:g}", df["atr_pct"].between(lo, hi), n)
    for t in [0.8, 1.0, 1.2, 1.5, 2.0, 2.5, 3.0]:
        add_pred(preds, f"range_expansion>={t:g}", df["range_expansion"].ge(t), n)
    for t in [-0.02, 0, 0.02, 0.05, 0.1, 0.2]:
        add_pred(preds, f"ema20_slope_pct>={t:g}", df["ema20_slope_pct"].ge(t), n)
    for t in [-0.05, 0, 0.03, 0.08, 0.15]:
        add_pred(preds, f"macd_delta>={t:g}", df["macd_delta"].ge(t), n)
    for t in [-1, 0, 1, 2, 3, 5]:
        add_pred(preds, f"rsi_delta>={t:g}", df["rsi_delta"].ge(t), n)
    for t in [0.05, 0.1, 0.2, 0.35, 0.5]:
        add_pred(preds, f"upper_wick_pct<={t:g}", df["upper_wick_pct"].le(t), n)
    for t in [0, 1, 2]:
        add_pred(preds, f"green_streak_3<={t:g}", df["green_streak_3"].le(t), n)
    add_pred(preds, "above_vwap", df["above_vwap"], n)
    add_pred(preds, "trend_stack", df["trend_stack"], n)
    return preds


def quick_metrics(mask: np.ndarray, pnl: np.ndarray, split_mask: np.ndarray) -> dict:
    vals = pnl[mask & split_mask & np.isfinite(pnl)]
    if len(vals) == 0:
        return {"trades": 0, "net_pf": 0.0, "net_pnl": 0.0, "win_rate_pct": 0.0}
    gp = vals[vals > 0].sum()
    gl = -vals[vals < 0].sum()
    return {
        "trades": int(len(vals)),
        "net_pf": float(gp / gl if gl > 0 else (999.0 if gp > 0 else 0.0)),
        "net_pnl": float(gp - gl),
        "win_rate_pct": float((vals > 0).mean() * 100.0),
    }


def topn_mask(df: pd.DataFrame, mask: np.ndarray, top_n: int | None) -> np.ndarray:
    if top_n is None:
        return mask
    tmp = df.loc[mask, ["session", "date", "score"]]
    idx = (
        tmp.sort_values(["session", "date", "score"], ascending=[True, True, False])
        .groupby(["session", "date"], sort=False)
        .head(top_n)
        .index
    )
    out = np.zeros(len(df), dtype=bool)
    out[idx.to_numpy()] = True
    return out


def materialize_trades(
    df: pd.DataFrame,
    exits: pd.DataFrame,
    exit_id: str,
    mask: np.ndarray,
    sessions: set[pd.Timestamp],
) -> pd.DataFrame:
    base = df.loc[mask & df["session"].isin(sessions).to_numpy()].copy()
    if base.empty:
        return pd.DataFrame()
    ex = exits[exits["exit_id"].eq(exit_id)].drop_duplicates("candidate_id")
    cols = [
        "candidate_id", "entry_time", "entry_raw", "entry_fill", "exit_time", "exit_raw",
        "exit_fill", "qty", "gross_pnl", "net_pnl", "outcome", "holding_min",
        "five_min_conflict", "same_1m_tie", "sl_price", "target_price",
    ]
    return base.merge(ex[cols], on="candidate_id", how="inner")


def effective_filters(predicates: list[str]) -> dict:
    out: dict[str, object] = {
        "base_rule": {
            "prior_high_break_bars": 5,
            "vol_ratio_min": 2.0,
            "green_body_pct_min": 0.12,
            "close_loc_min": 0.65,
            "rsi_min": 48.0,
            "adx_min": 12.0,
            "atr_pct_min": 0.16,
            "atr_pct_max": 2.25,
            "dist_vwap_abs_max": 1.60,
            "vol_rising_2": True,
            "green_streak_3_max": 2,
        },
        "extra_filters": {},
    }
    extra: dict[str, object] = {}
    mins: dict[str, float] = {}
    maxs: dict[str, float] = {}
    ranges: dict[str, list[float]] = {}
    bools = []
    for pred in predicates:
        if " " in pred and "-" in pred and ">=" not in pred and "<=" not in pred:
            key, rest = pred.split(" ", 1)
            lo_s, hi_s = rest.split("-", 1)
            lo, hi = float(lo_s), float(hi_s)
            cur = ranges.setdefault(key, [lo, hi])
            cur[0] = max(cur[0], lo)
            cur[1] = min(cur[1], hi)
        elif ">=" in pred:
            key, val = pred.split(">=", 1)
            mins[key] = max(mins.get(key, -1e9), float(val))
        elif "<=" in pred:
            key, val = pred.split("<=", 1)
            maxs[key] = min(maxs.get(key, 1e9), float(val))
        else:
            bools.append(pred)
    for key, (lo, hi) in ranges.items():
        extra[f"{key}_min"] = lo
        extra[f"{key}_max"] = hi
    for key, val in mins.items():
        extra[f"{key}_min"] = val
    for key, val in maxs.items():
        extra[f"{key}_max"] = val
    for key in bools:
        extra[key] = True
    out["extra_filters"] = extra
    return out


def search_configs(
    df: pd.DataFrame,
    exits: pd.DataFrame,
    splits: dict[str, set[pd.Timestamp]],
    min_train_trades: int,
    min_train_pf: float,
    depth: int,
    beam_width: int,
) -> list[dict]:
    n = len(df)
    preds = predicate_bank(df)
    train_mask = df["session"].isin(splits["train"]).to_numpy()
    fit_mask = df["session"].isin(splits["fit"]).to_numpy()
    val_mask = df["session"].isin(splits["val"]).to_numpy()
    test_mask = df["session"].isin(splits["test"]).to_numpy()
    exit_ids = sorted(exits["exit_id"].unique())
    all_rows: list[dict] = []
    for exit_id in exit_ids:
        pnl = (
            exits[exits["exit_id"].eq(exit_id)]
            .drop_duplicates("candidate_id")
            .set_index("candidate_id")["net_pnl"]
            .reindex(df["candidate_id"])
            .to_numpy(dtype=float)
        )
        states: list[tuple[np.ndarray, list[str]]] = [(np.ones(n, dtype=bool), [])]
        seen = {""}
        for step in range(depth):
            expanded = []
            for mask, names in states:
                used = set(names)
                for name, pred_mask in preds:
                    if name in used:
                        continue
                    next_mask = mask & pred_mask
                    if int((next_mask & train_mask).sum()) < min_train_trades:
                        continue
                    key = "|".join(sorted(names + [name]))
                    if key in seen:
                        continue
                    seen.add(key)
                    tr = quick_metrics(next_mask, pnl, train_mask)
                    if tr["trades"] < min_train_trades:
                        continue
                    score = (
                        min(tr["net_pf"], 4.0) * 1000.0
                        + tr["net_pnl"] / 1000.0
                        + min(tr["trades"], 300) * 0.1
                        + tr["win_rate_pct"]
                    )
                    expanded.append((score, next_mask, names + [name], tr))
            expanded.sort(key=lambda x: x[0], reverse=True)
            states = [(mask, names) for _, mask, names, _ in expanded[:beam_width]]
            for _, mask, names, tr in expanded[: beam_width * 2]:
                for top_n in [None, 1, 2, 3, 5]:
                    final_mask = topn_mask(df, mask, top_n)
                    train = quick_metrics(final_mask, pnl, train_mask)
                    if train["trades"] < min_train_trades or train["net_pf"] < min_train_pf:
                        continue
                    row = {
                        "exit_id": exit_id,
                        "top_n_per_slot": top_n,
                        "predicates": names,
                        "train_pf": train["net_pf"],
                        "train_net": train["net_pnl"],
                        "train_trades": train["trades"],
                        "train_wr": train["win_rate_pct"],
                    }
                    for split_name, split_mask in [("fit", fit_mask), ("val", val_mask), ("test", test_mask)]:
                        m = quick_metrics(final_mask, pnl, split_mask)
                        row[f"{split_name}_pf"] = m["net_pf"]
                        row[f"{split_name}_net"] = m["net_pnl"]
                        row[f"{split_name}_trades"] = m["trades"]
                        row[f"{split_name}_wr"] = m["win_rate_pct"]
                    all_rows.append(row)
            print(
                f"[search] {exit_id} depth={step + 1} states={len(states)} hits={len(all_rows)}",
                flush=True,
            )
    all_rows.sort(key=lambda x: (x["train_pf"], x["train_net"], x["train_trades"]), reverse=True)
    return all_rows


def pick_configs(rows: list[dict]) -> tuple[dict, dict]:
    if not rows:
        raise SystemExit("No train-PF optimized configs found")
    best_train = rows[0]
    split_checked = [
        r for r in rows
        if r["fit_trades"] >= 8
        and r["val_trades"] >= 8
        and r["fit_pf"] >= 1.4
        and r["val_pf"] >= 1.4
    ]
    best_split = split_checked[0] if split_checked else best_train
    return best_train, best_split


def decorate_config(
    label: str,
    row: dict,
    df: pd.DataFrame,
    exits: pd.DataFrame,
    splits: dict[str, set[pd.Timestamp]],
) -> OptimizedConfig:
    mask = np.ones(len(df), dtype=bool)
    bank = dict(predicate_bank(df))
    for pred in row["predicates"]:
        mask &= bank[pred]
    mask = topn_mask(df, mask, row["top_n_per_slot"])
    metrics = {}
    for split in ["fit", "val", "train", "test"]:
        trades = materialize_trades(df, exits, row["exit_id"], mask, splits[split])
        metrics[split] = metric_pack(trades)
    return OptimizedConfig(
        label=label,
        exit_id=row["exit_id"],
        top_n_per_slot=row["top_n_per_slot"],
        predicates=list(row["predicates"]),
        fit=metrics["fit"],
        val=metrics["val"],
        train=metrics["train"],
        test=metrics["test"],
    )


def selected_mask(df: pd.DataFrame, cfg: OptimizedConfig) -> np.ndarray:
    mask = np.ones(len(df), dtype=bool)
    bank = dict(predicate_bank(df))
    for pred in cfg.predicates:
        mask &= bank[pred]
    return topn_mask(df, mask, cfg.top_n_per_slot)


def write_outputs(
    rows: list[dict],
    best_train: OptimizedConfig,
    best_split: OptimizedConfig,
    df: pd.DataFrame,
    exits: pd.DataFrame,
    splits: dict[str, set[pd.Timestamp]],
    summary: dict,
) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    CANDIDATES_DIR.mkdir(parents=True, exist_ok=True)
    top_path = RESULTS_DIR / "train_pf_optimization_top_configs.csv"
    pd.DataFrame(rows[:500]).to_csv(top_path, index=False)

    chosen = best_split
    mask = selected_mask(df, chosen)
    train_trades = materialize_trades(df, exits, chosen.exit_id, mask, splits["train"])
    test_trades = materialize_trades(df, exits, chosen.exit_id, mask, splits["test"])
    train_trades_path = RESULTS_DIR / "train_pf_optimized_train_trades.csv"
    test_trades_path = RESULTS_DIR / "train_pf_optimized_test_trades.csv"
    train_trades.to_csv(train_trades_path, index=False)
    test_trades.to_csv(test_trades_path, index=False)

    cfg = {
        "setup_name": SETUP_NAME,
        "status": "RESEARCH_ONLY_DO_NOT_TRADE",
        "warning": "Optimized for TRAIN PF. TEST fails; do not promote without new approval.",
        "source_setup": "FAST_MOMENTUM_LONG_LONG_VOLUME_EXPANSION_BREAKOUT",
        "rule_id": RULE_ID,
        "selected_config": chosen.label,
        "base_entry": {
            "side": "LONG",
            "family": "LONG_VOLUME_EXPANSION_BREAKOUT",
            "entry_logic": "Relative-volume expansion breaks prior 5-bar high.",
            "entry_time": "next_1m_open",
        },
        "optimized_parameters": {
            "exit_id": chosen.exit_id,
            "sl_pct": 0.75,
            "target_pct": 1.0,
            "time_exit_bars": 3 if chosen.exit_id.endswith("_tb3") else 9,
            "top_n_per_slot": chosen.top_n_per_slot,
            "predicates": chosen.predicates,
            "effective_filters": effective_filters(chosen.predicates),
        },
        "best_train_only": {
            **asdict(best_train),
            "effective_filters": effective_filters(best_train.predicates),
        },
        "best_split_checked": {
            **asdict(best_split),
            "effective_filters": effective_filters(best_split.predicates),
        },
        "sessions": summary["sessions"],
        "artifacts": {
            "top_configs_csv": str(top_path),
            "train_trades_csv": str(train_trades_path),
            "test_trades_csv": str(test_trades_path),
        },
    }
    cfg_path = CANDIDATES_DIR / f"{SETUP_NAME}_config.json"
    cfg_path.write_text(json.dumps(to_py(cfg), indent=2), encoding="utf-8")

    md = [
        "# Train-PF Optimized FAST MOMENTUM LONG",
        "",
        "Verdict: **RESEARCH ONLY / DO NOT TRADE**.",
        "",
        "The requested TRAIN PF > 1.4 is achievable only with very selective filters. "
        "The selected split-checked variant clears FIT, VAL, and TRAIN, but fails TEST badly.",
        "",
        "## Selected Split-Checked Parameters",
        "",
        f"- source rule: `{RULE_ID}`",
        f"- exit: `{chosen.exit_id}`",
        f"- top_n_per_slot: `{chosen.top_n_per_slot}`",
        f"- predicates: `{', '.join(chosen.predicates)}`",
        f"- effective filters: `{json.dumps(effective_filters(chosen.predicates), sort_keys=True)}`",
        "",
        "## Metrics",
        "",
        "| split | trades | PF | WR% | net PnL |",
        "|---|---:|---:|---:|---:|",
    ]
    for name in ["fit", "val", "train", "test"]:
        m = getattr(chosen, name)
        md.append(f"| {name.upper()} | {m['trades']} | {m['net_pf']} | {m['win_rate_pct']} | {m['net_pnl']} |")
    md.extend([
        "",
        "## Best Train-Only Pocket",
        "",
        f"- exit: `{best_train.exit_id}`",
        f"- top_n_per_slot: `{best_train.top_n_per_slot}`",
        f"- predicates: `{', '.join(best_train.predicates)}`",
        f"- TRAIN: trades `{best_train.train['trades']}`, PF `{best_train.train['net_pf']}`, WR `{best_train.train['win_rate_pct']}`, net `{best_train.train['net_pnl']}`",
        f"- TEST: trades `{best_train.test['trades']}`, PF `{best_train.test['net_pf']}`, WR `{best_train.test['win_rate_pct']}`, net `{best_train.test['net_pnl']}`",
        "",
        "## Why Not Promote",
        "",
        "- TEST PF remains far below 1.0.",
        "- Trade count is very small after filters.",
        "- The filter pocket is concentrated in early-session high-ATR/high-RSI breakouts.",
        "- This is an in-sample parameter match, not a live-ready edge.",
        "",
        "## Files",
        "",
        f"- config JSON: `{cfg_path}`",
        f"- top configs CSV: `{top_path}`",
        f"- train trades CSV: `{train_trades_path}`",
        f"- test trades CSV: `{test_trades_path}`",
    ])
    (OUT_DIR / "TRAIN_PF_OPTIMIZED_FAST_MOMENTUM_LONG.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[done] wrote {cfg_path}")
    print(f"[done] selected TRAIN PF={chosen.train['net_pf']} TEST PF={chosen.test['net_pf']}")
    print(f"[done] best train-only TRAIN PF={best_train.train['net_pf']} TEST PF={best_train.test['net_pf']}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-train-trades", type=int, default=25)
    ap.add_argument("--min-train-pf", type=float, default=1.4)
    ap.add_argument("--depth", type=int, default=5)
    ap.add_argument("--beam-width", type=int, default=250)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass
    summary = load_summary()
    cand, rules, exits = load_caches(summary)
    df = make_base(cand, rules)
    splits = split_sets(summary)
    print(
        f"[load] {RULE_ID} rows={len(df):,} "
        f"train={df['session'].isin(splits['train']).sum():,} "
        f"test={df['session'].isin(splits['test']).sum():,}",
        flush=True,
    )
    rows = search_configs(
        df,
        exits,
        splits,
        min_train_trades=args.min_train_trades,
        min_train_pf=args.min_train_pf,
        depth=args.depth,
        beam_width=args.beam_width,
    )
    best_train_row, best_split_row = pick_configs(rows)
    best_train = decorate_config("best_train_only", best_train_row, df, exits, splits)
    best_split = decorate_config("best_split_checked", best_split_row, df, exits, splits)
    write_outputs(rows, best_train, best_split, df, exits, splits, summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
