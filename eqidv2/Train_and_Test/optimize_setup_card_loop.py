from __future__ import annotations

import argparse
import csv
import json
import math
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
for _p in (str(_ROOT), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt  # noqa: E402

try:
    import final_setup_conf as final_conf  # noqa: E402
except Exception as exc:  # pragma: no cover - fatal repo wiring issue
    raise SystemExit(f"[optimizer] cannot import final_setup_conf.py: {exc}") from exc

try:
    import optuna  # type: ignore  # noqa: E402
except Exception:
    optuna = None


PF_CAP_FOR_SCORE = 10.0
DEFAULT_POOL_ROOT = Path(r"C:\TradingData\eqidv2\setup_pools_2026_06_29")
DEFAULT_UNIFIED_POOL = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool")
RESULT_ROOT = _HERE / "results"
CARDS_PATH = _HERE / "SETUP_CARDS_AND_LIVE_CROSSCHECK.md"


@dataclass
class Gates:
    pf_min: float
    net_min: float
    min_trades_train: int
    min_trades_test: int
    dominance_cap: float
    max_trades_day: int
    gap_lambda: float


class Suggestor:
    def __init__(self, rng: random.Random | None = None, trial: Any | None = None):
        self.rng = rng
        self.trial = trial

    def choice(self, name: str, choices: list[Any]) -> Any:
        if not choices:
            raise ValueError(f"empty choices for {name}")
        if self.trial is not None:
            return self.trial.suggest_categorical(name, choices)
        assert self.rng is not None
        return self.rng.choice(choices)

    def randint(self, name: str, low: int, high: int) -> int:
        if self.trial is not None:
            return int(self.trial.suggest_int(name, low, high))
        assert self.rng is not None
        return self.rng.randint(low, high)


def _terms(raw: Any) -> list[list[Any]]:
    return [list(t) for t in (raw or [])]


def _runtime_defaults() -> dict[str, Any]:
    return {
        "max_positions": int(tt.MAX_POSITIONS),
        "daily_loss_rs": float(tt.DAILY_LOSS_RS),
        "regime_align": bool(tt.REGIME_ALIGN),
        "regime_band": float(tt.REGIME_BAND),
    }


def _load_original_config(setup: str) -> dict[str, Any]:
    cfg = None
    if hasattr(final_conf, "FINAL_SETUP_CONF"):
        cfg = final_conf.FINAL_SETUP_CONF.get(setup)
    if cfg is None and hasattr(final_conf, "RESEARCH_WATCH_CONF"):
        cfg = final_conf.RESEARCH_WATCH_CONF.get(setup)
    if cfg is None:
        raise SystemExit(f"[optimizer] setup {setup} not found in final_setup_conf.py")
    exit_cfg = cfg.get("exit", {})
    return {
        "name": "baseline_conf",
        "sl": float(exit_cfg.get("sl_pct")),
        "tgt": float(exit_cfg.get("tgt_pct")),
        "mask_terms": _terms(cfg.get("mask_terms")),
        "premom_terms": _terms(cfg.get("pre_momentum_terms")),
        "guard": dict(cfg.get("entry_guards") or {}),
        **_runtime_defaults(),
    }


def _setup_order_and_card(setup: str) -> tuple[list[str], str, str]:
    text = CARDS_PATH.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    starts: list[tuple[int, str]] = []
    for i, line in enumerate(lines):
        if line.startswith("### ") and "(" in line and ")" in line:
            name = line[4:].split("(", 1)[0].strip()
            if name:
                starts.append((i, name))
    if not starts:
        raise SystemExit("[optimizer] could not parse setup cards; first 80 lines:\n" + "\n".join(lines[:80]))
    order = [name for _, name in starts]
    section = ""
    if setup in order:
        idx = order.index(setup)
        start = starts[idx][0]
        end = starts[idx + 1][0] if idx + 1 < len(starts) else len(lines)
        section = "\n".join(lines[start:end])
    else:
        first_start = starts[0][0]
        first_end = starts[1][0] if len(starts) > 1 else min(len(lines), first_start + 30)
        raise SystemExit(
            f"[optimizer] setup {setup} not found in parsed cards. First card:\n"
            + "\n".join(lines[first_start:first_end])
        )
    live_lines = [line for line in lines if setup in line]
    return order, section, "\n".join(live_lines[:12])


def _next_setup(order: list[str], setup: str) -> str:
    if setup not in order:
        return ""
    i = order.index(setup)
    return order[i + 1] if i + 1 < len(order) else ""


def _pool_for_setup(setup: str, explicit: str | None) -> Path:
    if explicit:
        return Path(explicit)
    per = DEFAULT_POOL_ROOT / setup
    return per if per.exists() else DEFAULT_UNIFIED_POOL


def _day_key(df: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(df["_day"]).dt.date


def _infer_windows(pool: pd.DataFrame) -> dict[str, Any]:
    sessions = sorted(_day_key(pool).unique())
    if len(sessions) < 15:
        raise SystemExit(f"[optimizer] need at least 15 sessions; found {len(sessions)}")
    test = sessions[-5:]
    train = sessions[-15:-5]
    fit = train[:5]
    val = train[5:]
    return {"sessions": sessions, "fit": fit, "val": val, "train": train, "test": test}


def _date_range(days: list[Any]) -> str:
    return f"{days[0]}..{days[-1]}"


def _subset_days(df: pd.DataFrame, days: list[Any]) -> pd.DataFrame:
    dset = set(days)
    return df[_day_key(df).isin(dset)].reset_index(drop=True)


def _numeric_thresholds(df: pd.DataFrame, features: list[str]) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    qs = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]
    for feat in features:
        if feat not in df.columns:
            continue
        x = pd.to_numeric(df[feat], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(x) < 10 or x.nunique() < 2:
            continue
        vals = sorted({round(float(v), 6) for v in x.quantile(qs).to_numpy() if np.isfinite(v)})
        if vals:
            out[feat] = vals
    return out


def _categorical_values(df: pd.DataFrame) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for feat in ("regime",):
        if feat in df.columns:
            vals = sorted(v for v in df[feat].astype(str).str.upper().unique() if v and v != "NAN")
            if 1 < len(vals) <= 8:
                out[feat] = vals
    return out


def _premom_thresholds(rows: pd.DataFrame, sl: float, max_rows: int = 1500) -> dict[str, list[float]]:
    if rows.empty:
        return {}
    sample = rows
    if len(sample) > max_rows:
        sample = sample.sample(n=max_rows, random_state=7)
    recs: list[dict[str, float]] = []
    for r in sample.itertuples():
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), float(sl), r.tt_sig_ts.isoformat())
        if reason:
            continue
        recs.append(dict(feats))
    if not recs:
        return {}
    pm = pd.DataFrame(recs)
    return _numeric_thresholds(pm, list(tt.PREMOM_FEATURES))


def _term_key(t: list[Any]) -> tuple[str, str]:
    return str(t[0]), str(t[1])


def _param_token(text: Any) -> str:
    raw = str(text)
    return "".join(ch if ch.isalnum() else "_" for ch in raw)


def _sample_terms(
    s: Suggestor,
    prefix: str,
    original_terms: list[list[Any]],
    numeric_map: dict[str, list[float]],
    category_map: dict[str, list[str]] | None,
    max_extra: int,
    max_total: int,
) -> list[list[Any]]:
    terms: list[list[Any]] = []
    used: set[tuple[str, str]] = set()
    for i, term in enumerate(original_terms):
        feat, op, old_thr = term
        on = s.choice(f"{prefix}_orig_{i}_on", [True, False])
        if not on:
            continue
        if isinstance(old_thr, str):
            choices = sorted(set((category_map or {}).get(feat, []) + [old_thr]))
            thr = s.choice(f"{prefix}_orig_{i}_thr", choices) if choices else old_thr
        else:
            choices = sorted(set(numeric_map.get(feat, []) + [round(float(old_thr), 6)]))
            thr = s.choice(f"{prefix}_orig_{i}_thr", choices) if choices else float(old_thr)
        new = [feat, op, thr]
        terms.append(new)
        used.add(_term_key(new))

    extra_features = list(numeric_map)
    if category_map:
        extra_features += [f for f in category_map if f not in extra_features]
    n_extra = s.randint(f"{prefix}_extra_n", 0, max_extra) if extra_features else 0
    for j in range(n_extra):
        if len(terms) >= max_total:
            break
        feat = s.choice(f"{prefix}_extra_{j}_feat", extra_features)
        token = _param_token(feat)
        if category_map and feat in category_map:
            op = s.choice(f"{prefix}_extra_{j}_op_{token}", ["==", "!="])
            thr = s.choice(f"{prefix}_extra_{j}_thr_{token}", category_map[feat])
        else:
            op = s.choice(f"{prefix}_extra_{j}_op_{token}", [">=", "<="])
            thr = s.choice(f"{prefix}_extra_{j}_thr_{token}", numeric_map.get(feat, [0.0]))
        new = [feat, op, thr]
        key = _term_key(new)
        if key in used:
            continue
        terms.append(new)
        used.add(key)
    return terms[:max_total]


def _build_config(
    s: Suggestor,
    original: dict[str, Any],
    signal_num: dict[str, list[float]],
    signal_cat: dict[str, list[str]],
    premom_num: dict[str, list[float]],
    search_mode: str = "all",
) -> dict[str, Any]:
    sl_choices = sorted({0.40, 0.50, 0.60, 0.70, 0.85, 0.90, 1.00, 1.10, 1.20, 1.50, 1.80, float(original["sl"])})
    tgt_choices = sorted({0.50, 0.60, 0.80, 1.00, 1.20, 1.50, 2.00, 2.50, 3.00, float(original["tgt"])})
    if search_mode == "exit_only":
        return {
            "name": "trial",
            "sl": float(s.choice("sl", sl_choices)),
            "tgt": float(s.choice("tgt", tgt_choices)),
            "mask_terms": _terms(original["mask_terms"]),
            "premom_terms": _terms(original["premom_terms"]),
            "guard": dict(original.get("guard") or {}),
            "max_positions": int(original.get("max_positions", 20)),
            "daily_loss_rs": float(original.get("daily_loss_rs", 0.0)),
            "regime_align": bool(original.get("regime_align", False)),
            "regime_band": float(original.get("regime_band", 0.0)),
        }
    cfg = {
        "name": "trial",
        "sl": float(s.choice("sl", sl_choices)),
        "tgt": float(s.choice("tgt", tgt_choices)),
        "mask_terms": _sample_terms(s, "mask", original["mask_terms"], signal_num, signal_cat, 2, 3),
        "premom_terms": _sample_terms(s, "premom", original["premom_terms"], premom_num, None, 1, 3),
        "guard": {},
        "max_positions": int(s.choice("max_positions", [5, 10, 20])),
        "daily_loss_rs": float(s.choice("daily_loss_rs", [0.0, 1500.0, 2500.0, 4000.0])),
        "regime_align": bool(s.choice("regime_align", [False, True])),
        "regime_band": float(s.choice("regime_band", [0.0, 0.05, 0.10])),
    }
    min_slot = s.choice("guard_min_slot", [None, "09:30", "09:45", "10:00"])
    max_slot = s.choice("guard_max_slot", [None, "11:30", "12:30", "14:00"])
    top_n = s.choice("guard_top_n", [None, 1, 2, 3, 5])
    if min_slot:
        cfg["guard"]["min_slot"] = min_slot
    if max_slot:
        cfg["guard"]["max_slot"] = max_slot
    if top_n:
        cfg["guard"]["top_n"] = int(top_n)
    return cfg


def _eval_config(setup: str, cfg: dict[str, Any], df: pd.DataFrame, detail: bool = False) -> tuple[dict[str, Any], pd.DataFrame]:
    tt.MAX_POSITIONS = int(cfg.get("max_positions", 20))
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs", 0.0))
    tt.REGIME_ALIGN = bool(cfg.get("regime_align", False))
    tt.REGIME_BAND = float(cfg.get("regime_band", 0.0))
    config = {
        setup: {
            "status": "OK",
            "sl": float(cfg["sl"]),
            "tgt": float(cfg["tgt"]),
            "mask_terms": [tuple(t) for t in cfg.get("mask_terms", [])],
            "premom_terms": [tuple(t) for t in cfg.get("premom_terms", [])],
            "guard": cfg.get("guard") or None,
        }
    }
    fam = tt.eval_family(config, df)
    det = pd.DataFrame()
    if detail and fam["trades"]:
        det = tt.book_detail(fam["book"], {setup: (float(cfg["sl"]), float(cfg["tgt"]))})
    return fam, det


def _score_pf(pf: float) -> float:
    if not np.isfinite(pf):
        return PF_CAP_FOR_SCORE
    return max(0.0, min(float(pf), PF_CAP_FOR_SCORE))


def _trial_score(fit: dict[str, Any], val: dict[str, Any], min_half_trades: int, gap_lambda: float) -> tuple[float, str]:
    if int(fit["trades"]) < min_half_trades:
        return -1_000_000.0, "FIT_TOO_FEW"
    if int(val["trades"]) < min_half_trades:
        return -1_000_000.0, "VAL_TOO_FEW"
    fp, vp = _score_pf(float(fit["net_pf"])), _score_pf(float(val["net_pf"]))
    return min(fp, vp) - gap_lambda * abs(fp - vp), "OK"


def _detail_metrics(fam: dict[str, Any], detail: pd.DataFrame) -> dict[str, Any]:
    out = {
        "trades": int(fam.get("trades", 0)),
        "net_pf": round(float(fam.get("net_pf", 0.0)), 4),
        "net_pnl": round(float(fam.get("net_pnl", 0.0)), 2),
        "day_block_p": None if not np.isfinite(fam.get("day_block_p", np.nan)) else round(float(fam["day_block_p"]), 4),
        "win_pct": 0.0,
        "gross_profit": 0.0,
        "gross_loss": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "max_drawdown": 0.0,
        "max_trades_day": 0,
        "top_trade_gross_profit_share": None,
        "top_day_net_share": None,
        "top_symbol_net_share": None,
    }
    if detail.empty:
        return out
    net = detail["net_pnl_rs"].astype(float)
    gross = detail["gross_pnl_rs"].astype(float)
    wins = net[net > 0]
    losses = net[net <= 0]
    gp = float(gross[gross > 0].sum())
    gl = float(-gross[gross < 0].sum())
    out["win_pct"] = round(float((net > 0).mean() * 100.0), 2)
    out["gross_profit"] = round(gp, 2)
    out["gross_loss"] = round(gl, 2)
    out["avg_win"] = round(float(wins.mean()), 2) if len(wins) else 0.0
    out["avg_loss"] = round(float(losses.mean()), 2) if len(losses) else 0.0
    equity = net.cumsum()
    dd = equity - equity.cummax()
    out["max_drawdown"] = round(float(dd.min()), 2) if len(dd) else 0.0
    out["max_trades_day"] = int(detail.groupby("trade_date").size().max()) if "trade_date" in detail else 0
    if gp > 0:
        out["top_trade_gross_profit_share"] = round(float(gross[gross > 0].max() / gp), 4)
    total_net = float(net.sum())
    if total_net > 0:
        day_net = detail.groupby("trade_date")["net_pnl_rs"].sum()
        sym_net = detail.groupby("ticker")["net_pnl_rs"].sum()
        out["top_day_net_share"] = round(float(day_net.max() / total_net), 4)
        out["top_symbol_net_share"] = round(float(sym_net.max() / total_net), 4)
    return out


def _passes_gate(m: dict[str, Any], min_trades: int, gates: Gates) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if m["trades"] < min_trades:
        reasons.append(f"trades {m['trades']}<{min_trades}")
    if m["net_pnl"] <= gates.net_min:
        reasons.append(f"net {m['net_pnl']}<={gates.net_min}")
    if m["net_pf"] < gates.pf_min:
        reasons.append(f"PF {m['net_pf']}<{gates.pf_min}")
    share = m.get("top_trade_gross_profit_share")
    if share is not None and share > gates.dominance_cap:
        reasons.append(f"top trade gross-profit share {share}>{gates.dominance_cap}")
    if m.get("max_trades_day", 0) > gates.max_trades_day:
        reasons.append(f"max trades/day {m['max_trades_day']}>{gates.max_trades_day}")
    day_share = m.get("top_day_net_share")
    if day_share is not None and day_share > gates.dominance_cap:
        reasons.append(f"top day net share {day_share}>{gates.dominance_cap}")
    sym_share = m.get("top_symbol_net_share")
    if sym_share is not None and sym_share > gates.dominance_cap:
        reasons.append(f"top symbol net share {sym_share}>{gates.dominance_cap}")
    return not reasons, reasons


def _changed_knobs(original: dict[str, Any], best: dict[str, Any]) -> list[str]:
    changes: list[str] = []
    for k in ("sl", "tgt", "mask_terms", "premom_terms", "guard", "max_positions", "daily_loss_rs", "regime_align", "regime_band"):
        if best.get(k) != original.get(k):
            changes.append(f"{k}: {original.get(k)} -> {best.get(k)}")
    return changes


def _write_equity(detail: pd.DataFrame, path: Path, title: str) -> bool:
    if detail.empty:
        return False
    try:
        import matplotlib.pyplot as plt
    except Exception:
        return False
    d = detail.copy()
    d["_t"] = pd.to_datetime(d["entry_time"], errors="coerce")
    d = d.sort_values("_t")
    eq = d["net_pnl_rs"].astype(float).cumsum()
    plt.figure(figsize=(9, 4.5))
    plt.plot(range(1, len(eq) + 1), eq.to_numpy(), linewidth=1.8)
    plt.axhline(0, color="#888888", linewidth=0.8)
    plt.title(title)
    plt.xlabel("Trade")
    plt.ylabel("Cumulative net Rs")
    plt.tight_layout()
    plt.savefig(path, dpi=140)
    plt.close()
    return True


def _markdown_table(rows: list[list[Any]], headers: list[str]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        out.append("| " + " | ".join(str(x) for x in row) + " |")
    return "\n".join(out)


def _update_leaderboard(setup: str, status: str, baseline_train: dict[str, Any], baseline_test: dict[str, Any],
                        best_train: dict[str, Any], best_test: dict[str, Any], changed: str, next_setup: str) -> None:
    RESULT_ROOT.mkdir(parents=True, exist_ok=True)
    path = RESULT_ROOT / "LEADERBOARD.md"
    header = [
        "# Setup Optimization Leaderboard",
        "",
        "| Setup | Status | Baseline TRAIN n/PF | Baseline TEST n/PF | Best TRAIN n/PF | Best TEST n/PF | Changed knobs | Next setup |",
        "|---|---|---:|---:|---:|---:|---|---|",
    ]
    row = (
        f"| {setup} | {status} | {baseline_train['trades']} / {baseline_train['net_pf']} | "
        f"{baseline_test['trades']} / {baseline_test['net_pf']} | {best_train['trades']} / {best_train['net_pf']} | "
        f"{best_test['trades']} / {best_test['net_pf']} | {changed} | {next_setup} |"
    )
    if not path.exists():
        path.write_text("\n".join(header + [row, ""]) , encoding="utf-8")
        return
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if not any(line.startswith("| Setup | Status |") for line in lines):
        path.write_text("\n".join(header + [row, ""]) , encoding="utf-8")
        return
    replaced = False
    new_lines = []
    for line in lines:
        if line.startswith(f"| {setup} |"):
            new_lines.append(row)
            replaced = True
        else:
            new_lines.append(line)
    if not replaced:
        if new_lines and new_lines[-1].strip():
            new_lines.append(row)
        else:
            new_lines[-1:] = [row]
    path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--pool", default="")
    ap.add_argument("--trials", type=int, default=300)
    ap.add_argument("--time_budget_min", type=float, default=20.0)
    ap.add_argument("--pf_min", type=float, default=1.30)
    ap.add_argument("--net_min", type=float, default=0.0)
    ap.add_argument("--min_trades_train", type=int, default=15)
    ap.add_argument("--min_trades_test", type=int, default=5)
    ap.add_argument("--dominance_cap", type=float, default=0.40)
    ap.add_argument("--max_trades_day", type=int, default=6)
    ap.add_argument("--gap_lambda", type=float, default=0.50)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--search_mode", choices=["all", "exit_only"], default="all")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    setup = args.setup.strip().upper()
    gates = Gates(args.pf_min, args.net_min, args.min_trades_train, args.min_trades_test,
                  args.dominance_cap, args.max_trades_day, args.gap_lambda)
    order, card, live_notes = _setup_order_and_card(setup)
    next_setup = _next_setup(order, setup)
    original = _load_original_config(setup)
    pool_dir = _pool_for_setup(setup, args.pool or None)

    tt.POOL_DIR = pool_dir
    tt.POOL_DIRS = [pool_dir]
    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).eq(setup)].copy()
    if pool.empty:
        raise SystemExit(f"[optimizer] no rows for {setup} in {pool_dir}")
    windows = _infer_windows(pool)
    print(f"[optimizer] setup={setup}")
    print(f"[optimizer] pool={pool_dir}")
    print(f"[optimizer] FIT   {_date_range(windows['fit'])} sessions={windows['fit']}")
    print(f"[optimizer] VAL   {_date_range(windows['val'])} sessions={windows['val']}")
    print(f"[optimizer] TRAIN {_date_range(windows['train'])} sessions={windows['train']}")
    print(f"[optimizer] TEST  {_date_range(windows['test'])} sessions={windows['test']}")
    if optuna is None:
        print("Optuna unavailable; using seeded random search fallback.")
    else:
        optuna.logging.set_verbosity(optuna.logging.WARNING)
        print(f"[optimizer] Optuna available; using TPE sampler ({optuna.__version__}).")

    needed = _subset_days(pool, windows["train"] + windows["test"])
    print(f"[optimizer] raw rows in train+test sessions={len(needed)}")
    attached = tt.attach_entries(needed)
    print(f"[optimizer] rows with 1m entry={len(attached)}")
    fit_df = _subset_days(attached, windows["fit"])
    val_df = _subset_days(attached, windows["val"])
    train_df = _subset_days(attached, windows["train"])
    test_df = _subset_days(attached, windows["test"])
    print(f"[optimizer] attached rows FIT={len(fit_df)} VAL={len(val_df)} TRAIN={len(train_df)} TEST={len(test_df)}")

    excluded = set(getattr(tt, "DEFAULT_EXCLUDED_FEATURES", set()))
    signal_features = [f for f in tt.SIGNAL_FEATURES if f not in excluded and f != "signal_minute"]
    for term in original["mask_terms"]:
        if term[0] not in signal_features:
            signal_features.append(term[0])
    signal_num = _numeric_thresholds(train_df, signal_features)
    signal_cat = _categorical_values(train_df)
    premom_num = _premom_thresholds(train_df, float(original["sl"]))
    for term in original["premom_terms"]:
        feat, _op, thr = term
        if feat not in premom_num and not isinstance(thr, str):
            premom_num[feat] = [round(float(thr), 6)]
        elif not isinstance(thr, str):
            premom_num[feat] = sorted(set(premom_num[feat] + [round(float(thr), 6)]))

    min_half_trades = max(3, math.ceil(gates.min_trades_train / 2.0))
    rows: list[dict[str, Any]] = []
    best_cfg: dict[str, Any] | None = None
    best_score = -1_000_000_000.0
    start = time.time()
    deadline = start + float(args.time_budget_min) * 60.0

    def run_one(trial_no: int, cfg: dict[str, Any]) -> float:
        nonlocal best_cfg, best_score
        fit, _ = _eval_config(setup, cfg, fit_df, detail=False)
        val, _ = _eval_config(setup, cfg, val_df, detail=False)
        score, status = _trial_score(fit, val, min_half_trades, gates.gap_lambda)
        rec = {
            "trial": trial_no,
            "status": status,
            "score": round(float(score), 6),
            "fit_trades": int(fit["trades"]),
            "fit_pf": round(float(fit["net_pf"]), 6),
            "fit_net": round(float(fit["net_pnl"]), 2),
            "val_trades": int(val["trades"]),
            "val_pf": round(float(val["net_pf"]), 6),
            "val_net": round(float(val["net_pnl"]), 2),
            "config_json": json.dumps(cfg, sort_keys=True),
        }
        rows.append(rec)
        if score > best_score:
            best_score = score
            best_cfg = cfg
        return score

    baseline_trial = dict(original)
    baseline_trial["name"] = "baseline_conf"
    run_one(0, baseline_trial)

    if optuna is not None:
        def objective(trial: Any) -> float:
            cfg = _build_config(Suggestor(trial=trial), original, signal_num, signal_cat, premom_num, args.search_mode)
            cfg["name"] = f"trial_{trial.number}"
            trial.set_user_attr("config", cfg)
            return run_one(int(trial.number) + 1, cfg)

        sampler = optuna.samplers.TPESampler(seed=int(args.seed))
        study = optuna.create_study(direction="maximize", sampler=sampler)
        study.optimize(objective, n_trials=max(0, int(args.trials) - 1), timeout=max(1, deadline - time.time()))
    else:
        rng = random.Random(int(args.seed))
        for trial_no in range(1, int(args.trials)):
            if time.time() >= deadline:
                break
            cfg = _build_config(Suggestor(rng=rng), original, signal_num, signal_cat, premom_num, args.search_mode)
            cfg["name"] = f"trial_{trial_no}"
            run_one(trial_no, cfg)

    if best_cfg is None:
        best_cfg = baseline_trial

    # TEST is touched only after the search loop is complete.
    baseline_train_fam, baseline_train_detail = _eval_config(setup, original, train_df, detail=True)
    baseline_test_fam, baseline_test_detail = _eval_config(setup, original, test_df, detail=True)
    best_train_fam, best_train_detail = _eval_config(setup, best_cfg, train_df, detail=True)
    best_test_fam, best_test_detail = _eval_config(setup, best_cfg, test_df, detail=True)

    baseline_train = _detail_metrics(baseline_train_fam, baseline_train_detail)
    baseline_test = _detail_metrics(baseline_test_fam, baseline_test_detail)
    best_train = _detail_metrics(best_train_fam, best_train_detail)
    best_test = _detail_metrics(best_test_fam, best_test_detail)

    train_ok, train_reasons = _passes_gate(best_train, gates.min_trades_train, gates)
    test_ok, test_reasons = _passes_gate(best_test, gates.min_trades_test, gates)
    if train_ok and test_ok:
        status = "SELECTED"
    elif train_ok and not test_ok:
        status = "OVERFIT"
    else:
        status = "NOT SELECTED"

    result_dir = RESULT_ROOT / setup
    result_dir.mkdir(parents=True, exist_ok=True)
    trials_path = result_dir / "trials.csv"
    with trials_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    changed = _changed_knobs(original, best_cfg)
    best_config = {
        "setup": setup,
        "status": status,
        "score_fit_val": round(float(best_score), 6),
        "windows": {k: [str(x) for x in v] for k, v in windows.items() if k != "sessions"},
        "original_config": original,
        "best_config": best_cfg,
        "changed_knobs": changed,
        "baseline_train": baseline_train,
        "baseline_test": baseline_test,
        "best_train": best_train,
        "best_test": best_test,
        "train_gate_reasons": train_reasons,
        "test_gate_reasons": test_reasons,
        "optuna_used": optuna is not None,
        "search_mode": args.search_mode,
        "trials_run": len(rows),
    }
    (result_dir / "best_config.json").write_text(json.dumps(best_config, indent=2, default=str), encoding="utf-8")

    train_png = result_dir / "equity_train.png"
    test_png = result_dir / "equity_test.png"
    train_plot = _write_equity(best_train_detail, train_png, f"{setup} best TRAIN equity")
    test_plot = _write_equity(best_test_detail, test_png, f"{setup} best TEST equity")

    clean_verdict = "pass holds" if status == "SELECTED" else ("pass evaporates" if status == "OVERFIT" else "no train-side pass")
    changed_text = "<br>".join(changed) if changed else "none"
    metric_rows = [
        ["Baseline TRAIN", baseline_train["net_pnl"], baseline_train["net_pf"], baseline_train["trades"]],
        ["Baseline TEST", baseline_test["net_pnl"], baseline_test["net_pf"], baseline_test["trades"]],
        ["Best TRAIN", best_train["net_pnl"], best_train["net_pf"], best_train["trades"]],
        ["Best TEST", best_test["net_pnl"], best_test["net_pf"], best_test["trades"]],
    ]
    report = [
        f"# {setup} Optimization Report",
        "",
        f"Status: **{status}**",
        "",
        "## Windows",
        "",
        f"- FIT: {_date_range(windows['fit'])} ({', '.join(map(str, windows['fit']))})",
        f"- VAL: {_date_range(windows['val'])} ({', '.join(map(str, windows['val']))})",
        f"- TRAIN: {_date_range(windows['train'])}",
        f"- TEST: {_date_range(windows['test'])}",
        "",
        "TEST was evaluated only after the FIT/VAL search loop finished.",
        "",
        "## Engine",
        "",
        f"- Trials run: {len(rows)} of requested {args.trials}",
        f"- Search engine: {'Optuna TPE' if optuna is not None else 'seeded random fallback'}",
        f"- Best FIT/VAL score: {round(float(best_score), 6)}",
        f"- Command: `python Train_and_Test\\optimize_setup_card_loop.py --setup {setup} --pool {pool_dir} --trials {args.trials} --time_budget_min {args.time_budget_min:g} --search_mode {args.search_mode} --min_trades_train {args.min_trades_train} --min_trades_test {args.min_trades_test}`",
        "",
        "## Metrics",
        "",
        _markdown_table(metric_rows, ["Book", "net Rs", "PF", "trades"]),
        "",
        "## Clean OOS Verdict",
        "",
        f"- Clean TEST verdict: **{clean_verdict}**",
        f"- Train gate: {'PASS' if train_ok else 'FAIL'}" + ("" if train_ok else f" ({'; '.join(train_reasons)})"),
        f"- Test gate: {'PASS' if test_ok else 'FAIL'}" + ("" if test_ok else f" ({'; '.join(test_reasons)})"),
        "",
        "## Changed Knobs vs Original",
        "",
        changed_text,
        "",
        "## Best Config",
        "",
        "```json",
        json.dumps(best_cfg, indent=2, default=str),
        "```",
        "",
        "## Live Crosscheck / Known Mismatch Notes",
        "",
        live_notes or "(none found in setup-card doc)",
        "",
        "## Source Card",
        "",
        "```text",
        card[:5000],
        "```",
        "",
        "## Artifacts",
        "",
        f"- trials.csv: `{trials_path}`",
        f"- best_config.json: `{result_dir / 'best_config.json'}`",
        f"- equity_train.png: {'written' if train_plot else 'not written'}",
        f"- equity_test.png: {'written' if test_plot else 'not written'}",
        "",
        "No live execution was performed. No final_setup_conf.py edit was made.",
        "",
    ]
    (result_dir / "report.md").write_text("\n".join(report), encoding="utf-8")

    _update_leaderboard(
        setup, status, baseline_train, baseline_test, best_train, best_test,
        changed_text.replace("\n", " "), next_setup,
    )

    print(f"[optimizer] completed trials={len(rows)} best_score={best_score:.6f}")
    print(f"[optimizer] status={status}")
    print(f"[optimizer] baseline TRAIN n={baseline_train['trades']} pf={baseline_train['net_pf']} net={baseline_train['net_pnl']}")
    print(f"[optimizer] baseline TEST  n={baseline_test['trades']} pf={baseline_test['net_pf']} net={baseline_test['net_pnl']}")
    print(f"[optimizer] best TRAIN     n={best_train['trades']} pf={best_train['net_pf']} net={best_train['net_pnl']}")
    print(f"[optimizer] best TEST      n={best_test['trades']} pf={best_test['net_pf']} net={best_test['net_pnl']}")
    print(f"[optimizer] report={result_dir / 'report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
