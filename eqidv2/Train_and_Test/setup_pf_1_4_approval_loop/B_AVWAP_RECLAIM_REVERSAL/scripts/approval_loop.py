from __future__ import annotations

import argparse
import csv
import json
import math
import random
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
REPO_ROOT = HERE.parents[4]
TT_DIR = REPO_ROOT / "Train_and_Test"
for p in (str(REPO_ROOT), str(TT_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

import setup_train_test as tt  # noqa: E402

try:
    import final_setup_conf as final_conf  # noqa: E402
except Exception as exc:  # pragma: no cover
    raise SystemExit(f"cannot import final_setup_conf.py: {exc}") from exc

try:
    import optuna  # type: ignore  # noqa: E402
except Exception:
    optuna = None


SETUP = "B_AVWAP_RECLAIM_REVERSAL"
MASK_FEATURES = [
    "rs_pct", "market_ret_pct", "market_abs_ret_pct", "vol_ratio", "atr_pct",
    "body_pct", "close_loc", "vwap_dist_atr", "quality_score", "ranker_score",
    "signal_range_pct", "upper_wick_pct", "lower_wick_pct", "wick_skew_pct",
    "rsi", "rsi3max", "adx", "ema20_slope", "stock_ret",
]
PM_FEATURES = [
    "pre_entry_momentum_score", "sig5_adx_calc", "sig5_rsi_dir",
    "sig5_vol_ratio20", "pre1_adx", "pre3_range_r", "pre5_mom_r",
    "pre3_close_pos",
]
QGRID = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]
SL_GRID = [0.50, 0.60, 0.70, 0.85, 0.90, 1.00, 1.10, 1.20]
TGT_GRID = [0.80, 1.00, 1.20, 1.50, 2.00, 2.50]
MIN_SLOTS = [None, "09:30", "09:45", "10:00", "10:30", "11:00"]
MAX_SLOTS = [None, "11:30", "12:30", "13:00", "14:00"]
TOP_N = [None, 1, 2, 3, 5]


def load_original_config() -> dict[str, Any]:
    cfg = None
    source = ""
    for book in ("FINAL_SETUP_CONF", "RESEARCH_WATCH_CONF"):
        data = getattr(final_conf, book, {})
        if SETUP in data:
            cfg = data[SETUP]
            source = book
            break
    if cfg is None:
        raise SystemExit(f"{SETUP} not found in final_setup_conf.py")
    exit_cfg = cfg.get("exit", {})
    return {
        "source": source,
        "status": "OK",
        "sl": float(exit_cfg.get("sl_pct", 0.70)),
        "tgt": float(exit_cfg.get("tgt_pct", 1.50)),
        "mask_terms": [list(t) for t in cfg.get("mask_terms", [])],
        "premom_terms": [list(t) for t in cfg.get("pre_momentum_terms", [])],
        "guard": dict(cfg.get("entry_guards") or {}),
        "max_positions": 20,
        "daily_loss_rs": 0.0,
        "regime_align": False,
        "regime_band": 0.0,
    }


def setup_card_text() -> str:
    path = TT_DIR / "SETUP_CARDS_AND_LIVE_CROSSCHECK.md"
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    start = None
    for i, line in enumerate(lines):
        if line.startswith("### ") and SETUP in line:
            start = i
            break
    if start is None:
        return ""
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if lines[j].startswith("### "):
            end = j
            break
    return "\n".join(lines[start:end])


def live_notes() -> str:
    path = TT_DIR / "SETUP_CARDS_AND_LIVE_CROSSCHECK.md"
    return "\n".join(
        line for line in path.read_text(encoding="utf-8", errors="replace").splitlines()
        if SETUP in line or "B_AVWAP" in line
    )


def day_series(df: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(df["_day"]).dt.normalize()


def infer_windows(pool: pd.DataFrame, train_start: str, test_start: str) -> dict[str, list[pd.Timestamp]]:
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    train_start_ts = pd.Timestamp(train_start)
    test_start_ts = pd.Timestamp(test_start)
    train = [s for s in sessions if train_start_ts <= s < test_start_ts]
    test = [s for s in sessions if s >= test_start_ts]
    if len(train) < 4:
        raise SystemExit(f"not enough TRAIN sessions: {len(train)}")
    if not test:
        raise SystemExit("no TEST sessions at or after requested test_start")
    mid = len(train) // 2
    return {"train": train, "test": test, "fit": train[:mid], "val": train[mid:], "sessions": sessions}


def subset(df: pd.DataFrame, days: list[pd.Timestamp]) -> pd.DataFrame:
    dayset = set(pd.Timestamp(d).normalize() for d in days)
    return df[day_series(df).isin(dayset)].reset_index(drop=True)


def set_runtime(cfg: dict[str, Any]) -> None:
    tt.MAX_POSITIONS = int(cfg.get("max_positions", 20))
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs", 0.0))
    tt.REGIME_ALIGN = bool(cfg.get("regime_align", False))
    tt.REGIME_BAND = float(cfg.get("regime_band", 0.0))


def config_for_eval(cfg: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "OK",
        "sl": float(cfg["sl"]),
        "tgt": float(cfg["tgt"]),
        "mask_terms": [tuple(t) for t in cfg.get("mask_terms", [])],
        "premom_terms": [tuple(t) for t in cfg.get("premom_terms", [])],
        "guard": cfg.get("guard") or None,
    }


def eval_fast(cfg: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    set_runtime(cfg)
    fam = tt.eval_family({SETUP: config_for_eval(cfg)}, df)
    return {
        "trades": int(fam.get("trades", 0)),
        "pf": float(fam.get("net_pf", 0.0)),
        "net": float(fam.get("net_pnl", 0.0)),
        "day_block_p": None if not np.isfinite(fam.get("day_block_p", np.nan)) else float(fam["day_block_p"]),
    }


def detail_metrics(cfg: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    set_runtime(cfg)
    fam = tt.eval_family({SETUP: config_for_eval(cfg)}, df)
    exits = {SETUP: (float(cfg["sl"]), float(cfg["tgt"]))}
    det = tt.book_detail(fam["book"], exits) if fam.get("trades", 0) else pd.DataFrame()
    net = det["net_pnl_rs"].astype(float) if not det.empty else pd.Series(dtype=float)
    gross = det["gross_pnl_rs"].astype(float) if not det.empty and "gross_pnl_rs" in det else net
    wins = net[net > 0]
    losses = net[net <= 0]
    gp = float(gross[gross > 0].sum()) if len(gross) else 0.0
    gl = float(-gross[gross < 0].sum()) if len(gross) else 0.0
    equity = net.cumsum() if len(net) else pd.Series(dtype=float)
    dd = equity - equity.cummax() if len(equity) else pd.Series(dtype=float)
    out = {
        "trades": int(fam.get("trades", 0)),
        "pf": round(float(fam.get("net_pf", 0.0)), 4),
        "net": round(float(fam.get("net_pnl", 0.0)), 2),
        "day_block_p": None if not np.isfinite(fam.get("day_block_p", np.nan)) else round(float(fam["day_block_p"]), 4),
        "win_rate": round(float((net > 0).mean() * 100.0), 2) if len(net) else 0.0,
        "wins": int((net > 0).sum()) if len(net) else 0,
        "losses": int((net <= 0).sum()) if len(net) else 0,
        "gross_profit": round(gp, 2),
        "gross_loss": round(gl, 2),
        "avg_win": round(float(wins.mean()), 2) if len(wins) else 0.0,
        "avg_loss": round(float(losses.mean()), 2) if len(losses) else 0.0,
        "max_drawdown": round(float(dd.min()), 2) if len(dd) else 0.0,
        "max_trades_day": int(det.groupby("trade_date").size().max()) if not det.empty and "trade_date" in det else 0,
        "top_trade_gross_profit_share": round(float(gross[gross > 0].max() / gp), 4) if gp > 0 else None,
        "top_day_net_share": None,
        "top_symbol_net_share": None,
        "outcomes": {},
        "daywise": [],
        "symbolwise": [],
        "timewise": [],
    }
    total_net = float(net.sum()) if len(net) else 0.0
    if not det.empty:
        if "outcome" in det:
            out["outcomes"] = det["outcome"].astype(str).value_counts().to_dict()
        day = det.groupby("trade_date")["net_pnl_rs"].agg(["count", "sum"]).reset_index()
        day = day.rename(columns={"count": "trades", "sum": "net"})
        out["daywise"] = day.to_dict("records")
        sym = det.groupby("ticker")["net_pnl_rs"].agg(["count", "sum"]).reset_index()
        sym = sym.rename(columns={"count": "trades", "sum": "net"}).sort_values("net")
        out["symbolwise"] = sym.to_dict("records")
        if "entry_time" in det:
            t = pd.to_datetime(det["entry_time"], errors="coerce")
            tmp = det.copy()
            tmp["_hour"] = t.dt.strftime("%H:00")
            tw = tmp.groupby("_hour")["net_pnl_rs"].agg(["count", "sum"]).reset_index()
            out["timewise"] = tw.rename(columns={"count": "trades", "sum": "net"}).to_dict("records")
        if total_net > 0:
            day_net = det.groupby("trade_date")["net_pnl_rs"].sum()
            sym_net = det.groupby("ticker")["net_pnl_rs"].sum()
            out["top_day_net_share"] = round(float(day_net.max() / total_net), 4)
            out["top_symbol_net_share"] = round(float(sym_net.max() / total_net), 4)
    return out


def quantile_map(df: pd.DataFrame, features: list[str]) -> dict[str, dict[float, float]]:
    out: dict[str, dict[float, float]] = {}
    for f in features:
        if f not in df.columns:
            continue
        s = pd.to_numeric(df[f], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(s) < 12 or s.nunique() < 3:
            continue
        out[f] = {q: round(float(s.quantile(q)), 6) for q in QGRID}
    return out


def premom_quantile_map(train: pd.DataFrame) -> dict[str, dict[float, float]]:
    sample = train
    if len(sample) > 1200:
        sample = sample.sample(n=1200, random_state=7)
    recs: list[dict[str, float]] = []
    for r in sample.itertuples():
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), 0.90, r.tt_sig_ts.isoformat())
        fd = dict(feats) if not reason else {}
        recs.append({f: fd.get(f, np.nan) for f in PM_FEATURES})
    return quantile_map(pd.DataFrame(recs), PM_FEATURES)


def sample_cfg(trial: Any, mask_q: dict[str, dict[float, float]], pm_q: dict[str, dict[float, float]],
               original: dict[str, Any]) -> dict[str, Any]:
    def cat(name: str, choices: list[Any]) -> Any:
        return trial.suggest_categorical(name, choices)
    def integ(name: str, lo: int, hi: int) -> int:
        return int(trial.suggest_int(name, lo, hi))

    cfg = {
        "status": "OK",
        "name": "trial",
        "sl": float(cat("sl", sorted(set(SL_GRID + [float(original["sl"])])))),
        "tgt": float(cat("tgt", sorted(set(TGT_GRID + [float(original["tgt"])])))),
        "mask_terms": [],
        "premom_terms": [],
        "guard": {},
        "max_positions": int(cat("max_positions", [10, 20])),
        "daily_loss_rs": float(cat("daily_loss_rs", [0.0, 2500.0, 4000.0])),
        "regime_align": bool(cat("regime_align", [False, True])),
        "regime_band": float(cat("regime_band", [0.0, 0.05, 0.10])),
    }
    if cat("keep_original_mask", [True, False]):
        cfg["mask_terms"].extend([list(t) for t in original.get("mask_terms", [])])
    n_mask = integ("n_mask", 0, 3)
    mask_feats = list(mask_q)
    for i in range(n_mask):
        if not mask_feats:
            break
        f = cat(f"mask_{i}_feat", mask_feats)
        op = cat(f"mask_{i}_op", [">=", "<="])
        q = cat(f"mask_{i}_q", QGRID)
        term = [f, op, mask_q[f][q]]
        if term not in cfg["mask_terms"]:
            cfg["mask_terms"].append(term)
    if "regime" in train_columns_global:
        rmode = cat("regime_filter", ["none", "not_bull", "not_bear", "trend_or_neutral"])
        if rmode == "not_bull":
            cfg["mask_terms"].append(["regime", "!=", "BULL"])
        elif rmode == "not_bear":
            cfg["mask_terms"].append(["regime", "!=", "BEAR"])
        elif rmode == "trend_or_neutral":
            cfg["mask_terms"].append(["regime", "!=", "BULL"])
            cfg["mask_terms"].append(["regime", "!=", "BEAR"])

    n_pm = integ("n_pm", 0, 2)
    pm_feats = list(pm_q)
    for i in range(n_pm):
        if not pm_feats:
            break
        f = cat(f"pm_{i}_feat", pm_feats)
        op = cat(f"pm_{i}_op", [">=", "<="])
        q = cat(f"pm_{i}_q", QGRID)
        term = [f, op, pm_q[f][q]]
        if term not in cfg["premom_terms"]:
            cfg["premom_terms"].append(term)

    min_slot = cat("min_slot", MIN_SLOTS)
    max_slot = cat("max_slot", MAX_SLOTS)
    if min_slot and max_slot:
        if int(min_slot[:2]) * 60 + int(min_slot[3:]) > int(max_slot[:2]) * 60 + int(max_slot[3:]):
            min_slot = None
            max_slot = None
    if min_slot:
        cfg["guard"]["min_slot"] = min_slot
    if max_slot:
        cfg["guard"]["max_slot"] = max_slot
    top_n = cat("top_n", TOP_N)
    if top_n:
        cfg["guard"]["top_n"] = int(top_n)
    return cfg


class RandTrial:
    def __init__(self, rng: random.Random):
        self.rng = rng
    def suggest_categorical(self, _name: str, choices: list[Any]) -> Any:
        return choices[self.rng.randrange(len(choices))]
    def suggest_int(self, _name: str, lo: int, hi: int) -> int:
        return self.rng.randint(lo, hi)


def score_train_side(fit: dict[str, Any], val: dict[str, Any], train: dict[str, Any],
                     min_train_trades: int) -> float:
    fit_pf = 0.0 if not np.isfinite(fit["pf"]) else float(fit["pf"])
    val_pf = 0.0 if not np.isfinite(val["pf"]) else float(val["pf"])
    train_pf = 0.0 if not np.isfinite(train["pf"]) else float(train["pf"])
    n = int(train["trades"])
    gap = abs(fit_pf - val_pf)
    balanced_pf = min(fit_pf, val_pf) - 0.50 * gap
    trade_floor = max(2, min_train_trades // 4)
    if n < min_train_trades or fit["trades"] < trade_floor or val["trades"] < trade_floor:
        return -10000.0 + balanced_pf * 100.0 + n * 0.10
    if 1.30 <= train_pf <= 1.70 and train["net"] > 0:
        return 10000.0 + balanced_pf * 100.0 + min(n, 400) * 0.20 - abs(train_pf - 1.50) * 20.0
    if train_pf < 1.30:
        return balanced_pf * 100.0 - (1.30 - train_pf) * 100.0 + min(n, 400) * 0.05
    return balanced_pf * 100.0 - (train_pf - 1.70) * 150.0 + min(n, 400) * 0.03


def md_table(headers: list[str], rows: list[list[Any]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        out.append("| " + " | ".join(str(x) for x in row) + " |")
    return "\n".join(out)


def write_outputs(base_dir: Path, original: dict[str, Any], windows: dict[str, list[pd.Timestamp]],
                  card: str, notes: str, baseline: dict[str, Any], trial_rows: list[dict[str, Any]],
                  band_rows: list[dict[str, Any]], tested_rows: list[dict[str, Any]],
                  best: dict[str, Any] | None, command: str) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)
    (base_dir / "candidates").mkdir(exist_ok=True)
    with (base_dir / "trials.csv").open("w", newline="", encoding="utf-8") as f:
        fields = list(trial_rows[0].keys()) if trial_rows else ["iteration"]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(trial_rows)

    wr = {k: ", ".join(str(x.date()) for x in windows[k]) for k in ("fit", "val", "train", "test")}
    baseline_lines = [
        f"# {SETUP} Baseline Result",
        "",
        "## Setup Card",
        "",
        "```text",
        card,
        "```",
        "",
        "## Current Config",
        "",
        f"- Source: `{original['source']}`",
        f"- Exit: SL {original['sl']} / Target {original['tgt']}",
        f"- Mask terms: `{original['mask_terms']}`",
        f"- Pre-momentum terms: `{original['premom_terms']}`",
        f"- Entry guards: `{original['guard']}`",
        "",
        "## Windows",
        "",
        f"- FIT: {wr['fit']}",
        f"- VAL: {wr['val']}",
        f"- TRAIN: {wr['train']}",
        f"- TEST: {wr['test']}",
        "",
        "## Baseline Metrics",
        "",
        md_table(["Window", "Trades", "PF", "Net", "Win %", "Max DD", "Day-block p"], [
            ["TRAIN", baseline["train"]["trades"], baseline["train"]["pf"], baseline["train"]["net"], baseline["train"]["win_rate"], baseline["train"]["max_drawdown"], baseline["train"]["day_block_p"]],
            ["TEST", baseline["test"]["trades"], baseline["test"]["pf"], baseline["test"]["net"], baseline["test"]["win_rate"], baseline["test"]["max_drawdown"], baseline["test"]["day_block_p"]],
        ]),
        "",
        "## Initial Diagnosis",
        "",
        "- Current card is below the TRAIN PF target band and below the TEST PF target.",
        "- The card has a known live/backtest mismatch: live overlay uses the inverted `vwap_dist_atr >= 0.60` rule while the card/config uses `vwap_dist_atr <= 1.0`.",
        "- TEST has only the available sessions after 2026-06-20, so any passing candidate must be treated as approval-required and sample-thin.",
    ]
    (base_dir / "BASELINE_RESULT.md").write_text("\n".join(baseline_lines) + "\n", encoding="utf-8")

    log_rows = []
    for r in trial_rows:
        log_rows.append([
            r["iteration"], r["change_group"], r["train_trades"], r["train_pf"], r["train_net"],
            r["fit_pf"], r["val_pf"], r["test_pf"], r["decision"], r["next_action"],
        ])
    iter_lines = [
        f"# {SETUP} Iteration Log",
        "",
        f"Command: `{command}`",
        "",
        md_table(["Iter", "Change", "Train n", "Train PF", "Train net", "FIT PF", "VAL PF", "TEST PF", "Decision", "Next"], log_rows),
    ]
    (base_dir / "ITERATION_LOG.md").write_text("\n".join(iter_lines) + "\n", encoding="utf-8")

    def worst(items: list[dict[str, Any]], n: int = 10) -> list[dict[str, Any]]:
        return sorted(items, key=lambda x: float(x.get("net", x.get("sum", 0.0))))[:n]
    fail = [
        f"# {SETUP} Failure Analysis",
        "",
        "## Baseline Loss Patterns",
        "",
        f"- TRAIN outcomes: `{baseline['train'].get('outcomes', {})}`",
        f"- TEST outcomes: `{baseline['test'].get('outcomes', {})}`",
        f"- Worst TRAIN days: `{worst(baseline['train'].get('daywise', []), 8)}`",
        f"- Worst TEST days: `{worst(baseline['test'].get('daywise', []), 8)}`",
        f"- Worst TRAIN symbols: `{worst(baseline['train'].get('symbolwise', []), 12)}`",
        f"- Worst TEST symbols: `{worst(baseline['test'].get('symbolwise', []), 12)}`",
        "",
        "## Common Failure Classes",
        "",
        "- Near-VWAP reclaim is still vulnerable to fake reclaim / failed follow-through.",
        "- Low TEST sample means day-wise instability is a major risk.",
        "- Candidate filters that push TRAIN PF above 1.70 are treated as overfit and rejected even when profitable.",
        "- Known live mismatch must be resolved before approval: overlay path must not trade the inverted AVWAP/VWAP distance rule.",
    ]
    (base_dir / "FAILURE_ANALYSIS.md").write_text("\n".join(fail) + "\n", encoding="utf-8")

    pass_rows = [r for r in tested_rows if r.get("approval_required")]
    cand_lines = [f"# {SETUP} Candidate Configs", ""]
    if not pass_rows:
        cand_lines.append("No candidate passed both TRAIN PF 1.30-1.70 and TEST PF > 1.40 with stability checks.")
    for i, r in enumerate(pass_rows, 1):
        cand = {
            "setup": SETUP,
            "candidate": i,
            "config": r["config"],
            "train": r["train_detail"],
            "test": r["test_detail"],
            "risk_notes": [
                "Approval required; do not promote automatically.",
                "Resolve live overlay mismatch before use.",
                "TEST sample is thin because available post-2026-06-20 data has few sessions.",
            ],
        }
        cpath = base_dir / "candidates" / f"{SETUP}_candidate_{i:03d}.json"
        cpath.write_text(json.dumps(cand, indent=2, default=str), encoding="utf-8")
        cand_lines += [
            f"## Candidate {i}",
            "",
            f"- File: `{cpath}`",
            f"- Config: `{r['config']}`",
            f"- TRAIN: n={r['train_detail']['trades']} PF={r['train_detail']['pf']} net={r['train_detail']['net']}",
            f"- TEST: n={r['test_detail']['trades']} PF={r['test_detail']['pf']} net={r['test_detail']['net']}",
            f"- Risk notes: live mismatch and TEST sample size.",
            "",
        ]
    (base_dir / "CANDIDATE_CONFIGS.md").write_text("\n".join(cand_lines) + "\n", encoding="utf-8")

    rec_lines = [
        f"# {SETUP} Approval Required Final Recommendation",
        "",
        "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES.",
        "",
    ]
    if best and best.get("approval_required"):
        rec_lines += [
            "## Recommendation",
            "",
            "Recommend approval only after resolving the live overlay mismatch and rerunning on more TEST sessions.",
            "",
            "## Proposed Config",
            "",
            "```json",
            json.dumps(best["config"], indent=2, default=str),
            "```",
        ]
    elif best:
        rec_lines += [
            "## Recommendation",
            "",
            "No approval recommended. The best found config was scored on TEST once after the loop, but it did not meet the gate.",
            "",
            "## Best Found Config (Not Selected)",
            "",
            "```json",
            json.dumps(best["config"], indent=2, default=str),
            "```",
            "",
            f"- TRAIN: n={best['train_detail']['trades']} PF={best['train_detail']['pf']} net={best['train_detail']['net']}",
            f"- TEST: n={best['test_detail']['trades']} PF={best['test_detail']['pf']} net={best['test_detail']['net']}",
        ]
    else:
        rec_lines += [
            "## Recommendation",
            "",
            "No approval recommended. No candidate met the full approval gate.",
        ]
    rec_lines += [
        "",
        "## Final Config File That Would Require Approval",
        "",
        "- `final_setup_conf.py`",
        "- `Train_and_Test/final_setup_conf.py` only if you intentionally keep the mirror in sync.",
        "",
        "## Rerun Command",
        "",
        f"`{command}`",
        "",
        "## Live/Backtest Risk",
        "",
        notes or "(none)",
    ]
    (base_dir / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(rec_lines) + "\n", encoding="utf-8")


train_columns_global: set[str] = set()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pool", default=r"C:\TradingData\eqidv2\setup_pools_2026_06_29\B_AVWAP_RECLAIM_REVERSAL")
    parser.add_argument("--out", default=str(TT_DIR / "setup_pf_1_4_approval_loop" / SETUP))
    parser.add_argument("--train_start", default="2026-05-18")
    parser.add_argument("--test_start", default="2026-06-20")
    parser.add_argument("--trials", type=int, default=75)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--time_budget_min", type=float, default=20.0)
    parser.add_argument("--min_train_trades", type=int, default=20)
    parser.add_argument("--min_test_trades", type=int, default=5)
    args = parser.parse_args()

    out_dir = Path(args.out)
    (out_dir / "scripts").mkdir(parents=True, exist_ok=True)
    (out_dir / "candidates").mkdir(parents=True, exist_ok=True)

    tt.POOL_DIR = Path(args.pool)
    tt.POOL_DIRS = [Path(args.pool)]
    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).eq(SETUP)].copy()
    if pool.empty:
        raise SystemExit(f"no rows for {SETUP} in {args.pool}")
    windows = infer_windows(pool, args.train_start, args.test_start)
    global train_columns_global

    span = windows["train"] + windows["test"]
    attached = tt.attach_entries(subset(pool, span))
    fit_df = subset(attached, windows["fit"])
    val_df = subset(attached, windows["val"])
    train_df = subset(attached, windows["train"])
    test_df = subset(attached, windows["test"])
    train_columns_global = set(train_df.columns)

    original = load_original_config()
    card = setup_card_text()
    notes = live_notes()
    baseline = {
        "train": detail_metrics(original, train_df),
        "test": detail_metrics(original, test_df),
    }

    mask_q = quantile_map(train_df, MASK_FEATURES)
    pm_q = premom_quantile_map(train_df)
    engine = f"Optuna TPE {optuna.__version__}" if optuna is not None else "Optuna unavailable; using seeded random search fallback."
    print(f"[approval-loop] setup={SETUP}")
    print(f"[approval-loop] pool={args.pool}")
    print(f"[approval-loop] TRAIN sessions={[str(x.date()) for x in windows['train']]}")
    print(f"[approval-loop] TEST sessions={[str(x.date()) for x in windows['test']]}")
    print(f"[approval-loop] rows FIT={len(fit_df)} VAL={len(val_df)} TRAIN={len(train_df)} TEST={len(test_df)}")
    print(f"[approval-loop] engine={engine}")
    print(f"[approval-loop] baseline TRAIN n={baseline['train']['trades']} pf={baseline['train']['pf']} net={baseline['train']['net']}")
    print(f"[approval-loop] baseline TEST  n={baseline['test']['trades']} pf={baseline['test']['pf']} net={baseline['test']['net']}")

    trial_rows: list[dict[str, Any]] = []
    all_trial_items: list[dict[str, Any]] = []
    train_band_rows: list[dict[str, Any]] = []
    deadline = time.time() + args.time_budget_min * 60.0
    rng = random.Random(args.seed)

    def run_trial(i: int, cfg: dict[str, Any]) -> float:
        fit = eval_fast(cfg, fit_df)
        val = eval_fast(cfg, val_df)
        train = eval_fast(cfg, train_df)
        score = score_train_side(fit, val, train, args.min_train_trades)
        in_band = 1.30 <= train["pf"] <= 1.70 and train["trades"] >= args.min_train_trades and train["net"] > 0
        decision = "TRAIN_BAND_SHORTLIST" if in_band else "REJECT_TRAIN_BAND"
        row = {
            "iteration": i,
            "change_group": "filters/guards/pre-momentum/exit",
            "sl": cfg["sl"],
            "tgt": cfg["tgt"],
            "mask_terms": json.dumps(cfg.get("mask_terms", []), default=str),
            "premom_terms": json.dumps(cfg.get("premom_terms", []), default=str),
            "guard": json.dumps(cfg.get("guard", {}), default=str),
            "max_positions": cfg.get("max_positions"),
            "daily_loss_rs": cfg.get("daily_loss_rs"),
            "fit_trades": fit["trades"],
            "fit_pf": round(fit["pf"], 4),
            "val_trades": val["trades"],
            "val_pf": round(val["pf"], 4),
            "train_trades": train["trades"],
            "train_pf": round(train["pf"], 4),
            "train_net": round(train["net"], 2),
            "test_trades": "",
            "test_pf": "not_run",
            "test_net": "",
            "decision": decision,
            "score": round(score, 4),
            "next_action": "score TEST after loop" if in_band else "adjust train-side logic",
            "config_json": json.dumps(cfg, default=str),
        }
        trial_rows.append(row)
        all_trial_items.append({"row": row, "config": cfg, "fit": fit, "val": val, "train": train, "score": score})
        if in_band:
            train_band_rows.append({"row": row, "config": cfg, "fit": fit, "val": val, "train": train, "score": score})
        return score

    baseline_cfg = dict(original)
    baseline_cfg["name"] = "baseline"
    run_trial(0, baseline_cfg)

    if optuna is not None:
        optuna.logging.set_verbosity(optuna.logging.WARNING)
        def objective(trial: Any) -> float:
            cfg = sample_cfg(trial, mask_q, pm_q, original)
            cfg["name"] = f"trial_{trial.number + 1}"
            return run_trial(int(trial.number) + 1, cfg)
        study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=args.seed))
        study.optimize(objective, n_trials=max(0, args.trials - 1), timeout=max(1, deadline - time.time()))
    else:
        for i in range(1, args.trials):
            if time.time() >= deadline:
                break
            cfg = sample_cfg(RandTrial(rng), mask_q, pm_q, original)
            cfg["name"] = f"trial_{i}"
            run_trial(i, cfg)

    train_band_rows = sorted(
        train_band_rows,
        key=lambda x: (x["train"]["trades"], -abs(x["train"]["pf"] - 1.50), x["train"]["net"]),
        reverse=True,
    )
    tested_rows: list[dict[str, Any]] = []
    test_shortlist = train_band_rows[:10]
    if not test_shortlist and all_trial_items:
        test_shortlist = [max(all_trial_items, key=lambda x: x["score"])]

    for item in test_shortlist:
        cfg = item["config"]
        tr_detail = detail_metrics(cfg, train_df)
        te_detail = detail_metrics(cfg, test_df)
        approval = (
            1.30 <= tr_detail["pf"] <= 1.70
            and te_detail["pf"] > 1.40
            and tr_detail["trades"] >= args.min_train_trades
            and te_detail["trades"] >= args.min_test_trades
            and (te_detail["top_trade_gross_profit_share"] is None or te_detail["top_trade_gross_profit_share"] <= 0.40)
            and (te_detail["top_day_net_share"] is None or te_detail["top_day_net_share"] <= 0.40)
            and (te_detail["top_symbol_net_share"] is None or te_detail["top_symbol_net_share"] <= 0.40)
            and te_detail["max_trades_day"] <= 6
        )
        tested = {
            "config": cfg,
            "train_detail": tr_detail,
            "test_detail": te_detail,
            "approval_required": approval,
            "train_band_shortlist": item in train_band_rows,
        }
        tested_rows.append(tested)
        # Backfill first matching trial row with TEST metrics.
        for row in trial_rows:
            if row.get("config_json") == json.dumps(cfg, default=str):
                row["test_trades"] = te_detail["trades"]
                row["test_pf"] = te_detail["pf"]
                row["test_net"] = te_detail["net"]
                if approval:
                    row["decision"] = "APPROVAL_REQUIRED"
                elif item in train_band_rows:
                    row["decision"] = "REJECT_TEST_OR_STABILITY"
                else:
                    row["decision"] = "BEST_FOUND_TESTED_NOT_CANDIDATE"
                row["next_action"] = "candidate file written" if approval else "keep searching / do not promote"
                break

    passing = [r for r in tested_rows if r["approval_required"]]
    best = passing[0] if passing else (tested_rows[0] if tested_rows else None)
    command = "python " + " ".join(sys.argv)
    write_outputs(out_dir, original, windows, card, notes, baseline, trial_rows, train_band_rows, tested_rows, best, command)

    summary = {
        "out_dir": str(out_dir.resolve()),
        "trials": len(trial_rows),
        "train_band_candidates": len(train_band_rows),
        "tested_train_band_candidates": len(tested_rows),
        "passing_candidates": len(passing),
        "baseline_train": {k: baseline["train"][k] for k in ("trades", "pf", "net")},
        "baseline_test": {k: baseline["test"][k] for k in ("trades", "pf", "net")},
        "best": None if not best else {
            "approval_required": best["approval_required"],
            "config": best["config"],
            "train": {k: best["train_detail"][k] for k in ("trades", "pf", "net", "day_block_p", "top_trade_gross_profit_share", "top_day_net_share", "top_symbol_net_share")},
            "test": {k: best["test_detail"][k] for k in ("trades", "pf", "net", "day_block_p", "top_trade_gross_profit_share", "top_day_net_share", "top_symbol_net_share")},
        },
    }
    (out_dir / "run_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
