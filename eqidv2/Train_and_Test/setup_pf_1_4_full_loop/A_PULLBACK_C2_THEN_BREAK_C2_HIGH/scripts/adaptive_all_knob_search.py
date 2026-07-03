"""Adaptive all-knob research loop for A_PULLBACK_C2_THEN_BREAK_C2_HIGH.

Research-only. This script deliberately writes only inside the setup folder and
does not edit final_setup_conf.py. It fixes the first full-loop gap by discovering
numeric features dynamically, including lowercase indicator columns.
"""
from __future__ import annotations

import argparse
import itertools
import json
import math
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
SCRIPT_DIR = HERE.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import full_loop_a_pullback_c2_high as fl  # noqa: E402

tt = fl.tt

SETUP = fl.SETUP
SIDE = fl.SIDE
WORK = fl.WORK
OUT_CSV = WORK / "adaptive_all_knob_iterations.csv"
OUT_APPROX_CSV = WORK / "adaptive_all_knob_approx_candidates.csv"
OUT_JSON = WORK / "adaptive_all_knob_passing_candidates.json"
OUT_MD = WORK / "ADAPTIVE_ALL_KNOB_SEARCH.md"

TRAIN_PF_MIN = 1.30
TRAIN_PF_MAX = 1.80
TEST_PF_MIN = 1.40
TEST_PF_PREFERRED = 1.50
MIN_TRAIN_TRADES = 20
MIN_TEST_TRADES = 5
MIN_FIT_VAL_TRADES = 5
MIN_TRAIN_DAYS = 6
MIN_TEST_DAYS = 3
MIN_TRAIN_SYMBOLS = 6
MIN_TEST_SYMBOLS = 3
MAX_AVG_LOSS_ABS = 1250.0

SEARCH_QS = [0.04, 0.06, 0.08, 0.10, 0.12, 0.15, 0.20, 0.25, 0.33, 0.40,
             0.50, 0.60, 0.67, 0.75, 0.80, 0.85, 0.88, 0.90, 0.92, 0.94, 0.96]
EXIT_GRID = sorted({(float(sl), float(tgt)) for sl in fl.SL_GRID for tgt in fl.TGT_GRID})
PRIORITY_EXIT_GRID = [
    (fl.BASE_EXIT["sl"], fl.BASE_EXIT["tgt"]),
    (1.20, 2.00),
    (1.10, 2.00),
    (1.20, 2.50),
    (0.85, 1.80),
    (0.85, 2.00),
    (1.00, 2.00),
    (1.10, 2.50),
    (0.70, 1.80),
    (0.90, 2.00),
    (1.40, 2.50),
    (0.60, 1.80),
    (0.50, 2.50),
    (1.00, 1.50),
    (0.70, 2.50),
]
GUARD_GRID: list[dict[str, Any]] = [
    {},
    {"min_slot": "09:30"},
    {"min_slot": "09:45"},
    {"min_slot": "10:00"},
    {"min_slot": "10:30"},
    {"min_slot": "11:00"},
    {"max_slot": "11:30"},
    {"max_slot": "12:00"},
    {"max_slot": "12:30"},
    {"max_slot": "13:00"},
    {"max_slot": "13:30"},
    {"max_slot": "14:00"},
    {"min_slot": "09:45", "max_slot": "12:30"},
    {"min_slot": "09:45", "max_slot": "13:00"},
    {"min_slot": "10:00", "max_slot": "13:00"},
    {"min_slot": "10:00", "max_slot": "14:00"},
    {"min_slot": "10:30", "max_slot": "14:00"},
    {"top_n": 1},
    {"top_n": 2},
    {"top_n": 3},
    {"max_slot": "13:00", "top_n": 1},
    {"max_slot": "14:00", "top_n": 1},
    {"max_slot": "14:00", "top_n": 2},
    {"min_slot": "10:00", "max_slot": "14:00", "top_n": 1},
]


@dataclass
class ApproxConfig:
    name: str
    sl: float
    tgt: float
    mask_terms: list[list[Any]] = field(default_factory=list)
    premom_terms: list[list[Any]] = field(default_factory=list)
    guard: dict[str, Any] = field(default_factory=dict)
    max_positions: int = 20
    daily_loss_rs: float = 0.0
    regime_align: bool = False
    regime_band: float = 0.0
    group: str = "adaptive"
    reason: str = ""
    approx: dict[str, Any] = field(default_factory=dict)
    approx_score: float = 0.0

    def to_candidate(self) -> fl.Candidate:
        return fl.Candidate(
            name=self.name,
            sl=float(self.sl),
            tgt=float(self.tgt),
            mask_terms=[list(x) for x in self.mask_terms],
            premom_terms=[list(x) for x in self.premom_terms],
            guard=dict(self.guard or {}),
            max_positions=int(self.max_positions),
            daily_loss_rs=float(self.daily_loss_rs),
            regime_align=bool(self.regime_align),
            regime_band=float(self.regime_band),
            group=self.group,
            reason=self.reason,
            old_value="dynamic adaptive search",
            new_value=json.dumps(fl._json_safe(self.config()), sort_keys=True),
        )

    def config(self) -> dict[str, Any]:
        return {
            "sl": float(self.sl),
            "tgt": float(self.tgt),
            "mask_terms": self.mask_terms,
            "premom_terms": self.premom_terms,
            "guard": self.guard or {},
            "max_positions": int(self.max_positions),
            "daily_loss_rs": float(self.daily_loss_rs),
            "regime_align": bool(self.regime_align),
            "regime_band": float(self.regime_band),
        }

    def key(self) -> str:
        return json.dumps(fl._json_safe(self.config()), sort_keys=True)


def _safe_token(value: Any, max_len: int = 90) -> str:
    text = str(value)
    text = re.sub(r"[^A-Za-z0-9_.=<>!-]+", "_", text)
    text = text.strip("_")
    return text[:max_len] or "x"


def _guard_key(guard: dict[str, Any] | None) -> str:
    return json.dumps(guard or {}, sort_keys=True)


def _metric_pf(net: np.ndarray) -> float:
    net = np.asarray(net, float)
    net = net[np.isfinite(net)]
    if len(net) == 0:
        return 0.0
    gp = float(net[net > 0].sum())
    gl = float(-net[net < 0].sum())
    if gl <= 0:
        return float("inf") if gp > 0 else 0.0
    return gp / gl


def _subset_metrics(book: pd.DataFrame, net: np.ndarray, keep: np.ndarray) -> dict[str, Any]:
    if len(book) == 0:
        keep = np.zeros(0, dtype=bool)
    keep = np.asarray(keep, bool)
    net = np.asarray(net, float)
    valid = keep & np.isfinite(net)
    n = int(valid.sum())
    if n == 0:
        return {
            "trades": 0,
            "profit_factor": 0.0,
            "net_pnl": 0.0,
            "avg_loss": 0.0,
            "avg_win": 0.0,
            "win_rate": 0.0,
            "n_days": 0,
            "n_symbols": 0,
            "top_trade_gross_profit_share": None,
            "top_day_net_share": None,
            "top_symbol_net_share": None,
        }
    vals = net[valid]
    rows = book.loc[valid]
    wins = vals[vals > 0]
    losses = vals[vals <= 0]
    gp = float(wins.sum())
    total = float(vals.sum())

    def _share(x: float, denom: float) -> float | None:
        if denom <= 0:
            return None
        return round(float(x) / denom, 4)

    days = pd.Series(vals, index=rows["_day"].to_numpy()).groupby(level=0).sum()
    syms = pd.Series(vals, index=rows["ticker"].astype(str).to_numpy()).groupby(level=0).sum()
    return {
        "trades": n,
        "profit_factor": round(float(_metric_pf(vals)), 4) if math.isfinite(_metric_pf(vals)) else float("inf"),
        "net_pnl": round(total, 2),
        "avg_loss": round(float(losses.mean()), 2) if len(losses) else 0.0,
        "avg_win": round(float(wins.mean()), 2) if len(wins) else 0.0,
        "win_rate": round(float((vals > 0).mean() * 100.0), 2),
        "n_days": int(rows["_day"].nunique()),
        "n_symbols": int(rows["ticker"].nunique()),
        "top_trade_gross_profit_share": _share(float(wins.max()) if len(wins) else 0.0, gp),
        "top_day_net_share": _share(float(days.max()) if len(days) else 0.0, total),
        "top_symbol_net_share": _share(float(syms.max()) if len(syms) else 0.0, total),
    }


def _score_metrics(fit: dict[str, Any], val: dict[str, Any], train: dict[str, Any], n_terms: int) -> float:
    if train["trades"] < MIN_TRAIN_TRADES or fit["trades"] < MIN_FIT_VAL_TRADES or val["trades"] < MIN_FIT_VAL_TRADES:
        return -1000.0 + train["trades"]
    fit_pf = min(float(fit["profit_factor"]), 5.0) if math.isfinite(float(fit["profit_factor"])) else 5.0
    val_pf = min(float(val["profit_factor"]), 5.0) if math.isfinite(float(val["profit_factor"])) else 5.0
    train_pf = min(float(train["profit_factor"]), 5.0) if math.isfinite(float(train["profit_factor"])) else 5.0
    min_pf = min(fit_pf, val_pf, train_pf)
    split_gap = abs(fit_pf - val_pf)
    if train_pf < TRAIN_PF_MIN:
        band_penalty = (TRAIN_PF_MIN - train_pf) * 2.0
    elif train_pf > TRAIN_PF_MAX:
        band_penalty = (train_pf - TRAIN_PF_MAX) * 2.4
    else:
        band_penalty = abs(train_pf - 1.55) * 0.20
    net_bonus = 0.20 if train["net_pnl"] > 0 and fit["net_pnl"] >= 0 and val["net_pnl"] >= 0 else -0.35
    count_bonus = min(train["trades"], 220) / 900.0
    thin_sample_penalty = max(0, 38 - int(train["trades"])) / 45.0
    day_symbol_penalty = max(0, MIN_TRAIN_DAYS - int(train.get("n_days", 0))) * 0.10
    day_symbol_penalty += max(0, MIN_TRAIN_SYMBOLS - int(train.get("n_symbols", 0))) * 0.08
    domination_penalty = 0.0
    for key, limit in (
        ("top_trade_gross_profit_share", 0.35),
        ("top_day_net_share", 0.40),
        ("top_symbol_net_share", 0.40),
    ):
        val_share = train.get(key)
        if val_share is not None:
            domination_penalty += max(0.0, float(val_share) - limit) * 1.4
    avg_loss_penalty = max(0.0, abs(float(train["avg_loss"])) - MAX_AVG_LOSS_ABS) / 1000.0
    complexity_penalty = max(0, n_terms - 2) * 0.08
    return (
        min_pf
        - 0.35 * split_gap
        - band_penalty
        + net_bonus
        + count_bonus
        - thin_sample_penalty
        - day_symbol_penalty
        - domination_penalty
        - avg_loss_penalty
        - complexity_penalty
    )


def _excluded_numeric_column(col: str) -> bool:
    l = col.lower()
    exact = {
        "ticker", "side", "setup", "reason", "status", "regime", "_day", "_slot",
        "tt_sig_ts", "tt_entry_iso", "tt_entry_ok", "candidate_id", "signal_id",
        "scan_session", "selection_mode", "candidate_family", "_source_pool",
    }
    if l in exact:
        return True
    blocked_prefixes = ("v6_", "v7_", "v8_", "tt_", "old_")
    if l.startswith(blocked_prefixes):
        return True
    blocked_parts = (
        "pnl", "outcome", "exit", "bars_held", "resolution", "cost", "gross",
        "entry_time", "entry_price", "target_price", "stop_price", "sl_pct",
        "target_pct", "created_at", "datetime", "bar_time", "source_day",
        "time_ist", "schema", "version", "model", "diagnostics", "shadow",
        "action", "status", "quantity", "leverage", "capital",
    )
    if any(part in l for part in blocked_parts):
        return l == "signal_minute"
    if "date" in l and l != "signal_minute":
        return True
    return False


def discover_numeric_features(df: pd.DataFrame) -> list[str]:
    features: list[str] = []
    min_valid = max(30, min(200, int(len(df) * 0.05)))
    for col in df.columns:
        if _excluded_numeric_column(str(col)):
            continue
        s = pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(s) < min_valid or s.nunique() <= 3:
            continue
        features.append(str(col))
    return sorted(features)


def numeric_feature_diagnostics(df: pd.DataFrame, selected: list[str]) -> list[dict[str, Any]]:
    selected_set = set(selected)
    rows: list[dict[str, Any]] = []
    min_valid = max(30, min(200, int(len(df) * 0.05)))
    for col in df.columns:
        if _excluded_numeric_column(str(col)):
            continue
        s = pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(s) == 0:
            reason = "all_non_numeric_or_nan"
        elif len(s) < min_valid:
            reason = f"valid<{min_valid}"
        elif s.nunique() <= 3:
            reason = "nunique<=3"
        else:
            reason = "selected" if str(col) in selected_set else "not_selected"
        rows.append({
            "column": str(col),
            "selected": str(col) in selected_set,
            "valid_train_rows": int(len(s)),
            "nunique": int(s.nunique()) if len(s) else 0,
            "reason": reason,
        })
    return sorted(rows, key=lambda r: (not r["selected"], r["column"]))


def _term_mask(term: dict[str, Any], book: pd.DataFrame, pm: pd.DataFrame | None) -> np.ndarray:
    kind = term["kind"]
    feat, op, thr = term["term"]
    if kind == "premom":
        if pm is None or feat not in pm.columns:
            return np.zeros(len(book), dtype=bool)
        x = pd.to_numeric(pm[feat], errors="coerce")
        if op == ">=":
            return (x >= float(thr)).fillna(False).to_numpy()
        if op == "<=":
            return (x <= float(thr)).fillna(False).to_numpy()
        if op == ">":
            return (x > float(thr)).fillna(False).to_numpy()
        if op == "<":
            return (x < float(thr)).fillna(False).to_numpy()
        if op == "==":
            return (x == float(thr)).fillna(False).to_numpy()
        return (x != float(thr)).fillna(False).to_numpy()

    if feat not in book.columns:
        return np.zeros(len(book), dtype=bool)
    if isinstance(thr, str):
        col = book[feat].astype(str).str.upper()
        vv = str(thr).upper()
        return (col.ne(vv) if op == "!=" else col.eq(vv)).fillna(False).to_numpy()
    x = pd.to_numeric(book[feat], errors="coerce")
    if op == ">=":
        return (x >= float(thr)).fillna(False).to_numpy()
    if op == "<=":
        return (x <= float(thr)).fillna(False).to_numpy()
    if op == ">":
        return (x > float(thr)).fillna(False).to_numpy()
    if op == "<":
        return (x < float(thr)).fillna(False).to_numpy()
    if op == "==":
        return (x == float(thr)).fillna(False).to_numpy()
    return (x != float(thr)).fillna(False).to_numpy()


def _combined_mask(terms: list[dict[str, Any]], book: pd.DataFrame, pm: pd.DataFrame | None) -> np.ndarray:
    keep = np.ones(len(book), dtype=bool)
    for t in terms:
        keep &= _term_mask(t, book, pm)
    return keep


def _compatible(terms: list[dict[str, Any]]) -> bool:
    numeric_ranges: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    seen = set()
    for t in terms:
        key = json.dumps(fl._json_safe(t), sort_keys=True)
        if key in seen:
            return False
        seen.add(key)
        feat, op, thr = t["term"]
        if isinstance(thr, str):
            continue
        fk = (t["kind"], str(feat))
        if op == ">=":
            old = numeric_ranges[fk].get("lo")
            numeric_ranges[fk]["lo"] = max(float(thr), old) if old is not None else float(thr)
        elif op == "<=":
            old = numeric_ranges[fk].get("hi")
            numeric_ranges[fk]["hi"] = min(float(thr), old) if old is not None else float(thr)
        else:
            continue
        lo = numeric_ranges[fk].get("lo")
        hi = numeric_ranges[fk].get("hi")
        if lo is not None and hi is not None and lo > hi:
            return False
    return True


def numeric_terms(book: pd.DataFrame, features: list[str], min_keep: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for feat in features:
        s = pd.to_numeric(book[feat], errors="coerce").replace([np.inf, -np.inf], np.nan)
        valid = s.dropna()
        if len(valid) < min_keep or valid.nunique() <= 3:
            continue
        thresholds = sorted({round(float(x), 6) for x in valid.quantile(SEARCH_QS).to_numpy() if np.isfinite(x)})
        for thr in thresholds:
            ge_n = int((s >= thr).sum())
            le_n = int((s <= thr).sum())
            if ge_n >= min_keep:
                out.append({"kind": "mask", "feature": feat, "term": [feat, ">=", thr]})
            if le_n >= min_keep:
                out.append({"kind": "mask", "feature": feat, "term": [feat, "<=", thr]})
    return out


def categorical_terms(book: pd.DataFrame, min_keep: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if "regime" not in book.columns:
        return out
    vals = book["regime"].astype(str).str.upper().replace({"": "UNKNOWN"})
    counts = vals.value_counts()
    for val, n in counts.items():
        if int(n) >= min_keep:
            out.append({"kind": "mask", "feature": "regime", "term": ["regime", "==", str(val)]})
        if int(len(book) - n) >= min_keep:
            out.append({"kind": "mask", "feature": "regime", "term": ["regime", "!=", str(val)]})
    return out


def premom_terms(pm: pd.DataFrame, min_keep: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for feat in fl.PREMOM_FEATURES:
        if feat not in pm.columns:
            continue
        s = pd.to_numeric(pm[feat], errors="coerce").replace([np.inf, -np.inf], np.nan)
        valid = s.dropna()
        if len(valid) < min_keep or valid.nunique() <= 3:
            continue
        thresholds = sorted({round(float(x), 6) for x in valid.quantile(SEARCH_QS).to_numpy() if np.isfinite(x)})
        for thr in thresholds:
            if int((s >= thr).sum()) >= min_keep:
                out.append({"kind": "premom", "feature": feat, "term": [feat, ">=", thr]})
            if int((s <= thr).sum()) >= min_keep:
                out.append({"kind": "premom", "feature": feat, "term": [feat, "<=", thr]})
    return out


class ApproxSearch:
    def __init__(self, windows: dict[str, pd.DataFrame], numeric_features: list[str], args: argparse.Namespace) -> None:
        self.windows = windows
        self.numeric_features = numeric_features
        self.args = args
        self.book_cache: dict[str, pd.DataFrame] = {}
        self.net_cache: dict[tuple[str, float, float], np.ndarray] = {}
        self.period_cache: dict[str, dict[str, np.ndarray]] = {}
        self.pm_cache: dict[tuple[str, float], pd.DataFrame] = {}

    def book(self, guard: dict[str, Any]) -> pd.DataFrame:
        key = _guard_key(guard)
        if key not in self.book_cache:
            rows = tt.apply_guards(self.windows["TRAIN"], guard or None)
            self.book_cache[key] = tt.dedupe_family(rows).reset_index(drop=True)
        return self.book_cache[key]

    def net(self, guard: dict[str, Any], sl: float, tgt: float) -> np.ndarray:
        key = (_guard_key(guard), float(sl), float(tgt))
        if key not in self.net_cache:
            book = self.book(guard)
            self.net_cache[key] = tt.resolve_book(book, {SETUP: (float(sl), float(tgt))})
        return self.net_cache[key]

    def period_masks(self, guard: dict[str, Any]) -> dict[str, np.ndarray]:
        key = _guard_key(guard)
        if key not in self.period_cache:
            book = self.book(guard)
            fit_days = set(self.windows["FIT"]["_day"].unique())
            val_days = set(self.windows["VAL"]["_day"].unique())
            self.period_cache[key] = {
                "FIT": book["_day"].isin(fit_days).to_numpy(),
                "VAL": book["_day"].isin(val_days).to_numpy(),
                "TRAIN": np.ones(len(book), dtype=bool),
            }
        return self.period_cache[key]

    def pm(self, guard: dict[str, Any], sl: float) -> pd.DataFrame:
        key = (_guard_key(guard), float(sl))
        if key not in self.pm_cache:
            book = self.book(guard)
            rows = []
            print(f"[adaptive] computing pre-momentum features guard={_guard_key(guard)} sl={sl} rows={len(book)}", flush=True)
            for i, r in enumerate(book.itertuples(), start=1):
                feats, reason = tt._premom(
                    r.ticker,
                    r.side,
                    r.tt_entry_iso,
                    float(r.tt_fill),
                    float(sl),
                    r.tt_sig_ts.isoformat(),
                )
                fd = dict(feats) if not reason else {}
                rows.append({f: fd.get(f, np.nan) for f in fl.PREMOM_FEATURES})
                if i % 500 == 0:
                    print(f"[adaptive] pre-momentum {i}/{len(book)}", flush=True)
            self.pm_cache[key] = pd.DataFrame(rows)
        return self.pm_cache[key]

    def eval_terms(
        self,
        guard: dict[str, Any],
        sl: float,
        tgt: float,
        terms: list[dict[str, Any]],
        pm_frame: pd.DataFrame | None = None,
    ) -> tuple[float, dict[str, Any], dict[str, Any], dict[str, Any]]:
        book = self.book(guard)
        net = self.net(guard, sl, tgt)
        periods = self.period_masks(guard)
        keep = _combined_mask(terms, book, pm_frame)
        fit = _subset_metrics(book, net, keep & periods["FIT"])
        val = _subset_metrics(book, net, keep & periods["VAL"])
        train = _subset_metrics(book, net, keep)
        score = _score_metrics(fit, val, train, len(terms))
        return score, fit, val, train


def _make_config(
    guard: dict[str, Any],
    sl: float,
    tgt: float,
    terms: list[dict[str, Any]],
    score: float,
    fit: dict[str, Any],
    val: dict[str, Any],
    train: dict[str, Any],
    group: str,
    reason: str,
) -> ApproxConfig:
    mask_terms = [list(t["term"]) for t in terms if t["kind"] == "mask"]
    pm_terms = [list(t["term"]) for t in terms if t["kind"] == "premom"]
    bits = [f"sl{sl}", f"t{tgt}"]
    if guard:
        bits.append(_safe_token(_guard_key(guard), 35))
    if terms:
        bits += [_safe_token(f"{t['kind']}:{t['term'][0]}{t['term'][1]}{t['term'][2]}", 24) for t in terms[:4]]
    name = "adaptive_" + "_".join(bits)
    return ApproxConfig(
        name=name[:180],
        sl=float(sl),
        tgt=float(tgt),
        mask_terms=mask_terms,
        premom_terms=pm_terms,
        guard=dict(guard or {}),
        group=group,
        reason=reason,
        approx={"FIT": fit, "VAL": val, "TRAIN": train},
        approx_score=float(score),
    )


def select_contexts(search: ApproxSearch, args: argparse.Namespace) -> tuple[list[tuple[dict[str, Any], float, float, dict[str, Any]]], dict[str, Any]]:
    exit_rows: list[dict[str, Any]] = []
    if args.exit_grid_limit > 0:
        grid = []
        seen_grid = set()
        for pair in PRIORITY_EXIT_GRID + EXIT_GRID:
            if pair in seen_grid:
                continue
            seen_grid.add(pair)
            grid.append(pair)
            if len(grid) >= args.exit_grid_limit:
                break
    else:
        grid = EXIT_GRID
    print(f"[adaptive] stage 1: scanning {len(grid)} SL/target pairs on no-guard book", flush=True)
    for i, (sl, tgt) in enumerate(grid, start=1):
        score, fit, val, train = search.eval_terms({}, sl, tgt, [])
        exit_rows.append({"guard": {}, "sl": sl, "tgt": tgt, "score": score, "FIT": fit, "VAL": val, "TRAIN": train})
        if i % 5 == 0 or i == len(grid):
            print(f"[adaptive] stage 1 progress {i}/{len(grid)}", flush=True)
    exit_rows.sort(key=lambda r: (r["score"], r["TRAIN"]["profit_factor"], r["TRAIN"]["trades"]), reverse=True)
    keep_exits = {(fl.BASE_EXIT["sl"], fl.BASE_EXIT["tgt"])}
    keep_exits.update((r["sl"], r["tgt"]) for r in exit_rows[: args.exit_keep])

    context_rows: list[dict[str, Any]] = []
    print(f"[adaptive] stage 2: scanning {len(GUARD_GRID)} guards x {len(keep_exits)} kept exits", flush=True)
    for guard in GUARD_GRID:
        book = search.book(guard)
        if len(book) < MIN_TRAIN_TRADES:
            continue
        for sl, tgt in sorted(keep_exits):
            score, fit, val, train = search.eval_terms(guard, sl, tgt, [])
            context_rows.append({"guard": guard, "sl": sl, "tgt": tgt, "score": score, "FIT": fit, "VAL": val, "TRAIN": train})
    context_rows.sort(key=lambda r: (r["score"], r["TRAIN"]["profit_factor"], r["TRAIN"]["trades"]), reverse=True)

    must_keep = []
    for sl, tgt in sorted(keep_exits):
        for r in context_rows:
            if r["guard"] == {} and r["sl"] == sl and r["tgt"] == tgt:
                must_keep.append(r)
                break
    selected = context_rows[: args.context_keep]
    seen = {json.dumps({"g": r["guard"], "sl": r["sl"], "tgt": r["tgt"]}, sort_keys=True) for r in selected}
    for r in must_keep:
        key = json.dumps({"g": r["guard"], "sl": r["sl"], "tgt": r["tgt"]}, sort_keys=True)
        if key not in seen:
            selected.append(r)
            seen.add(key)
    inventory = {
        "exit_scan_count": len(exit_rows),
        "kept_exits": sorted([list(x) for x in keep_exits]),
        "guard_scan_count": len(context_rows),
        "selected_context_count": len(selected),
        "top_exit_rows": exit_rows[:15],
        "top_context_rows": selected[:20],
    }
    return [(r["guard"], r["sl"], r["tgt"], r) for r in selected], inventory


def search_context_terms(search: ApproxSearch, contexts: list[tuple[dict[str, Any], float, float, dict[str, Any]]], args: argparse.Namespace) -> tuple[list[ApproxConfig], dict[str, Any]]:
    approx: list[ApproxConfig] = []
    seen: set[str] = set()
    term_inventory: dict[str, Any] = {"numeric_features": search.numeric_features, "contexts": []}

    def add(cfg: ApproxConfig) -> None:
        if cfg.approx["TRAIN"]["trades"] < MIN_TRAIN_TRADES:
            return
        key = cfg.key()
        if key in seen:
            return
        seen.add(key)
        approx.append(cfg)

    for ix, (guard, sl, tgt, context_row) in enumerate(contexts, start=1):
        book = search.book(guard)
        net = search.net(guard, sl, tgt)
        finite_n = int(np.isfinite(net).sum())
        if finite_n < MIN_TRAIN_TRADES:
            continue
        print(
            f"[adaptive] stage 3 context {ix}/{len(contexts)} guard={_guard_key(guard)} "
            f"exit={sl}/{tgt} book_rows={len(book)}",
            flush=True,
        )
        base_score, base_fit, base_val, base_train = search.eval_terms(guard, sl, tgt, [])
        add(_make_config(guard, sl, tgt, [], base_score, base_fit, base_val, base_train, "adaptive_exit_guard", "exit and guard only"))

        all_terms = numeric_terms(book, search.numeric_features, MIN_TRAIN_TRADES) + categorical_terms(book, MIN_TRAIN_TRADES)
        term_inventory["contexts"].append({
            "guard": guard,
            "sl": sl,
            "tgt": tgt,
            "book_rows": len(book),
            "mask_terms_generated": len(all_terms),
            "context_train_pf": context_row["TRAIN"]["profit_factor"],
        })

        singles: list[tuple[float, list[dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        for term in all_terms:
            score, fit, val, train = search.eval_terms(guard, sl, tgt, [term])
            if train["trades"] >= MIN_TRAIN_TRADES:
                singles.append((score, [term], fit, val, train))
                if score > -100:
                    add(_make_config(guard, sl, tgt, [term], score, fit, val, train, "adaptive_single", "single dynamic mask term"))
        singles.sort(key=lambda x: (x[0], x[4]["profit_factor"], x[4]["trades"]), reverse=True)
        beam = singles[: args.beam_terms]

        pairs: list[tuple[float, list[dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        for a, b in itertools.combinations(beam, 2):
            terms = a[1] + b[1]
            if not _compatible(terms):
                continue
            score, fit, val, train = search.eval_terms(guard, sl, tgt, terms)
            if train["trades"] >= MIN_TRAIN_TRADES:
                pairs.append((score, terms, fit, val, train))
                if score > -100:
                    add(_make_config(guard, sl, tgt, terms, score, fit, val, train, "adaptive_combo2", "two-term dynamic structural combo"))
        pairs.sort(key=lambda x: (x[0], x[4]["profit_factor"], x[4]["trades"]), reverse=True)

        triples: list[tuple[float, list[dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any]]] = []
        if args.max_terms >= 3:
            for pair in pairs[: args.beam_pairs]:
                used = {f"{t['kind']}:{t['feature']}" for t in pair[1]}
                for single in beam[: args.beam_terms_for_expansion]:
                    t = single[1][0]
                    if f"{t['kind']}:{t['feature']}" in used and len([x for x in pair[1] if x["feature"] == t["feature"]]) >= 2:
                        continue
                    terms = pair[1] + [t]
                    if not _compatible(terms):
                        continue
                    score, fit, val, train = search.eval_terms(guard, sl, tgt, terms)
                    if train["trades"] >= MIN_TRAIN_TRADES:
                        triples.append((score, terms, fit, val, train))
                        if score > -100:
                            add(_make_config(guard, sl, tgt, terms, score, fit, val, train, "adaptive_combo3", "three-term dynamic structural combo"))
            triples.sort(key=lambda x: (x[0], x[4]["profit_factor"], x[4]["trades"]), reverse=True)

        if args.max_terms >= 4:
            for triple in triples[: max(15, args.beam_pairs // 2)]:
                used_terms = {json.dumps(fl._json_safe(t), sort_keys=True) for t in triple[1]}
                for single in beam[: max(15, args.beam_terms_for_expansion // 2)]:
                    t = single[1][0]
                    if json.dumps(fl._json_safe(t), sort_keys=True) in used_terms:
                        continue
                    terms = triple[1] + [t]
                    if not _compatible(terms):
                        continue
                    score, fit, val, train = search.eval_terms(guard, sl, tgt, terms)
                    if train["trades"] >= MIN_TRAIN_TRADES and score > -100:
                        add(_make_config(guard, sl, tgt, terms, score, fit, val, train, "adaptive_combo4", "four-term dynamic structural combo"))

    approx.sort(key=lambda c: (c.approx_score, c.approx["TRAIN"]["profit_factor"], c.approx["TRAIN"]["trades"]), reverse=True)
    return approx, term_inventory


def expand_premom(search: ApproxSearch, seeds: list[ApproxConfig], args: argparse.Namespace) -> list[ApproxConfig]:
    expanded: list[ApproxConfig] = []
    seen = {s.key() for s in seeds}
    seed_slice = seeds[: args.premom_seed_limit]
    print(f"[adaptive] stage 4: expanding pre-momentum terms from {len(seed_slice)} seeds", flush=True)
    for i, seed in enumerate(seed_slice, start=1):
        if len(seed.mask_terms) + len(seed.premom_terms) >= args.max_terms:
            continue
        guard = seed.guard or {}
        pm_frame = search.pm(guard, seed.sl)
        terms = premom_terms(pm_frame, MIN_TRAIN_TRADES)
        existing = [{"kind": "mask", "feature": t[0], "term": list(t)} for t in seed.mask_terms]
        pm_scores = []
        for term in terms:
            combo = existing + [term]
            if not _compatible(combo):
                continue
            score, fit, val, train = search.eval_terms(guard, seed.sl, seed.tgt, combo, pm_frame=pm_frame)
            if train["trades"] >= MIN_TRAIN_TRADES:
                pm_scores.append((score, combo, fit, val, train))
        pm_scores.sort(key=lambda x: (x[0], x[4]["profit_factor"], x[4]["trades"]), reverse=True)
        for score, combo, fit, val, train in pm_scores[: args.premom_keep_per_seed]:
            cfg = _make_config(guard, seed.sl, seed.tgt, combo, score, fit, val, train, "adaptive_premom", "seed plus pre-entry momentum filter")
            key = cfg.key()
            if key not in seen:
                seen.add(key)
                expanded.append(cfg)
        if i % 10 == 0:
            print(f"[adaptive] pre-momentum seed {i}/{len(seed_slice)} expanded={len(expanded)}", flush=True)
    return expanded


def exact_metrics(c: fl.Candidate, windows: dict[str, pd.DataFrame], run_test: bool) -> dict[str, Any]:
    return fl.evaluate_candidate(c, windows, run_test=run_test)


def exact_train_probe_gate(metrics: dict[str, Any]) -> tuple[bool, str]:
    ok, why = fl.train_side_promising(metrics)
    if ok:
        return True, "controlled_train_gate"
    fit, val, train = metrics["FIT"], metrics["VAL"], metrics["TRAIN"]
    hot = (
        train["trades"] >= MIN_TRAIN_TRADES
        and fit["trades"] >= MIN_FIT_VAL_TRADES
        and val["trades"] >= MIN_FIT_VAL_TRADES
        and train["net_pnl"] > 0
        and fit["net_pnl"] >= 0
        and val["net_pnl"] >= 0
        and 1.80 < float(train["profit_factor"]) <= 2.40
        and float(fit["profit_factor"]) >= 1.10
        and float(val["profit_factor"]) >= 1.10
    )
    if hot:
        return True, "hot_train_probe_overfit_risk"
    return False, why


def strict_acceptance(metrics: dict[str, Any]) -> tuple[bool, str]:
    base_ok, base_why = fl.acceptance(metrics)
    reasons = [] if base_ok else [base_why]
    train = metrics["TRAIN"]
    test = metrics.get("TEST") or {}
    if train.get("n_days", 0) < MIN_TRAIN_DAYS:
        reasons.append(f"TRAIN days < {MIN_TRAIN_DAYS}")
    if test.get("n_days", 0) < MIN_TEST_DAYS:
        reasons.append(f"TEST days < {MIN_TEST_DAYS}")
    if train.get("n_symbols", 0) < MIN_TRAIN_SYMBOLS:
        reasons.append(f"TRAIN symbols < {MIN_TRAIN_SYMBOLS}")
    if test.get("n_symbols", 0) < MIN_TEST_SYMBOLS:
        reasons.append(f"TEST symbols < {MIN_TEST_SYMBOLS}")
    if abs(float(train.get("avg_loss", 0.0))) > MAX_AVG_LOSS_ABS:
        reasons.append(f"TRAIN avg loss worse than Rs {MAX_AVG_LOSS_ABS:,.0f}")
    if abs(float(test.get("avg_loss", 0.0))) > MAX_AVG_LOSS_ABS:
        reasons.append(f"TEST avg loss worse than Rs {MAX_AVG_LOSS_ABS:,.0f}")
    if float(test.get("profit_factor", 0.0)) <= TEST_PF_MIN:
        reasons.append("TEST PF <= 1.40")
    return (not reasons), "; ".join(reasons) or "APPROVAL_REQUIRED"


def _flatten_exact_row(idx: int, cfg: ApproxConfig, metrics: dict[str, Any], ran_test: bool, gate_reason: str, accepted: bool, accept_reason: str) -> dict[str, Any]:
    row = {
        "iteration": idx,
        "candidate_id": cfg.name,
        "group": cfg.group,
        "reason": cfg.reason,
        "approx_score": round(float(cfg.approx_score), 6),
        "config_json": json.dumps(fl._json_safe(cfg.config()), sort_keys=True),
        "ran_test": ran_test,
        "train_gate_reason": gate_reason,
        "keep_reject": "KEEP_APPROVAL_REQUIRED" if accepted else "REJECT",
        "failure": accept_reason,
    }
    for label in ("FIT", "VAL", "TRAIN", "TEST"):
        m = metrics.get(label) or {}
        for key in (
            "trades", "profit_factor", "net_pnl", "avg_loss", "avg_win", "win_rate",
            "sl_count", "target_count", "time_exit_count", "n_days", "n_symbols",
            "top_trade_gross_profit_share", "top_day_net_share", "top_symbol_net_share",
        ):
            row[f"{label.lower()}_{key}"] = m.get(key)
    return row


def _logic_signature(cfg: ApproxConfig) -> str:
    mask_features = [f"m:{t[0]}:{t[1]}" for t in cfg.mask_terms]
    pm_features = [f"p:{t[0]}:{t[1]}" for t in cfg.premom_terms]
    return json.dumps(
        {
            "sl": cfg.sl,
            "tgt": cfg.tgt,
            "guard": cfg.guard or {},
            "features": sorted(mask_features + pm_features),
            "overlay": {
                "max_positions": cfg.max_positions,
                "daily_loss_rs": cfg.daily_loss_rs,
                "regime_align": cfg.regime_align,
                "regime_band": cfg.regime_band,
            },
        },
        sort_keys=True,
    )


def diversify_exact_configs(configs: list[ApproxConfig], limit: int, max_per_signature: int) -> list[ApproxConfig]:
    chosen: list[ApproxConfig] = []
    counts: Counter[str] = Counter()
    for cfg in configs:
        sig = _logic_signature(cfg)
        if counts[sig] >= max_per_signature:
            continue
        chosen.append(cfg)
        counts[sig] += 1
        if len(chosen) >= limit:
            break
    if len(chosen) < limit:
        seen = {c.key() for c in chosen}
        for cfg in configs:
            if cfg.key() in seen:
                continue
            chosen.append(cfg)
            seen.add(cfg.key())
            if len(chosen) >= limit:
                break
    return chosen


def evaluate_exact(
    approx: list[ApproxConfig],
    windows: dict[str, pd.DataFrame],
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    passing: list[dict[str, Any]] = []
    exact_seen: set[str] = set()

    def eval_one(cfg: ApproxConfig, label: str) -> None:
        key = cfg.key()
        if key in exact_seen:
            return
        exact_seen.add(key)
        c = cfg.to_candidate()
        provisional = {}
        for period in ("FIT", "VAL", "TRAIN"):
            fam = fl._eval_family(c, windows[period])
            det = fl.detail_from_book(fam["book"], c)
            provisional[period] = fl.metrics_from_detail(det, fam)
        run_test, gate_reason = exact_train_probe_gate(provisional)
        if run_test:
            fam = fl._eval_family(c, windows["TEST"])
            det = fl.detail_from_book(fam["book"], c)
            provisional["TEST"] = fl.metrics_from_detail(det, fam)
        else:
            provisional["TEST"] = None
        accepted, accept_reason = strict_acceptance(provisional) if run_test else (False, gate_reason)
        if accepted:
            passing.append({"candidate": c, "config": cfg, "metrics": provisional, "accept_reason": accept_reason})
        rows.append(_flatten_exact_row(len(rows) + 1, cfg, provisional, run_test, gate_reason, accepted, accept_reason))
        print(
            f"[adaptive exact {len(rows):03d}] {label:<8} {cfg.group:<18} {cfg.name[:70]:<70} "
            f"TRAIN {fl._metric_line(provisional['TRAIN'])} | TEST {fl._metric_line(provisional['TEST'])} | "
            f"{'KEEP' if accepted else 'REJECT'} {accept_reason}",
            flush=True,
        )

    exact_configs = diversify_exact_configs(approx, args.exact_limit, args.max_exact_per_signature)
    print(
        f"[adaptive] stage 5: exact validation configs={len(exact_configs)} "
        f"(limit={args.exact_limit}, max_per_signature={args.max_exact_per_signature})",
        flush=True,
    )
    for cfg in exact_configs:
        eval_one(cfg, "base")

    base_ranked = sorted(
        rows,
        key=lambda r: (
            float(r.get("train_profit_factor") or 0.0)
            if math.isfinite(float(r.get("train_profit_factor") or 0.0))
            else 5.0,
            float(r.get("train_net_pnl") or 0.0),
            int(r.get("train_trades") or 0),
        ),
        reverse=True,
    )
    by_name = {cfg.name: cfg for cfg in approx}
    overlay_sources = [by_name[r["candidate_id"]] for r in base_ranked[: args.overlay_limit] if r["candidate_id"] in by_name]
    overlays: list[ApproxConfig] = []
    for src in overlay_sources:
        for maxpos in (10, 15):
            data = src.config()
            cfg = ApproxConfig(
                name=f"{src.name[:130]}_maxpos{maxpos}",
                sl=src.sl,
                tgt=src.tgt,
                mask_terms=[list(x) for x in src.mask_terms],
                premom_terms=[list(x) for x in src.premom_terms],
                guard=dict(src.guard),
                max_positions=maxpos,
                group="adaptive_overlay",
                reason="portfolio max-position overlay on adaptive seed",
                approx=src.approx,
                approx_score=src.approx_score - 0.02,
            )
            overlays.append(cfg)
        for dloss in (3000.0, 5000.0, 7500.0):
            cfg = ApproxConfig(
                name=f"{src.name[:130]}_dloss{int(dloss)}",
                sl=src.sl,
                tgt=src.tgt,
                mask_terms=[list(x) for x in src.mask_terms],
                premom_terms=[list(x) for x in src.premom_terms],
                guard=dict(src.guard),
                daily_loss_rs=dloss,
                group="adaptive_overlay",
                reason="daily realized-loss overlay on adaptive seed",
                approx=src.approx,
                approx_score=src.approx_score - 0.03,
            )
            overlays.append(cfg)
        for band in (0.0, 0.15, 0.30):
            cfg = ApproxConfig(
                name=f"{src.name[:130]}_regime{band}",
                sl=src.sl,
                tgt=src.tgt,
                mask_terms=[list(x) for x in src.mask_terms],
                premom_terms=[list(x) for x in src.premom_terms],
                guard=dict(src.guard),
                regime_align=True,
                regime_band=band,
                group="adaptive_overlay",
                reason="book-level regime alignment overlay on adaptive seed",
                approx=src.approx,
                approx_score=src.approx_score - 0.03,
            )
            overlays.append(cfg)

    for cfg in overlays[: args.overlay_limit * 8]:
        eval_one(cfg, "overlay")

    passing.sort(
        key=lambda p: (
            p["metrics"]["TEST"]["profit_factor"],
            p["metrics"]["TEST"]["net_pnl"],
            p["metrics"]["TEST"]["trades"],
        ),
        reverse=True,
    )
    return rows, passing


def write_approx_csv(approx: list[ApproxConfig]) -> None:
    rows = []
    for i, cfg in enumerate(approx, start=1):
        row = {
            "rank": i,
            "candidate_id": cfg.name,
            "group": cfg.group,
            "reason": cfg.reason,
            "approx_score": round(float(cfg.approx_score), 6),
            "config_json": json.dumps(fl._json_safe(cfg.config()), sort_keys=True),
        }
        for label in ("FIT", "VAL", "TRAIN"):
            m = cfg.approx.get(label) or {}
            for k in ("trades", "profit_factor", "net_pnl", "avg_loss", "n_days", "n_symbols"):
                row[f"{label.lower()}_{k}"] = m.get(k)
        rows.append(row)
    pd.DataFrame(rows).to_csv(OUT_APPROX_CSV, index=False)


def write_reports(
    manifest: dict[str, Any],
    pool: pd.DataFrame,
    numeric_features: list[str],
    feature_diagnostics: list[dict[str, Any]],
    context_inventory: dict[str, Any],
    term_inventory: dict[str, Any],
    approx: list[ApproxConfig],
    exact_rows: list[dict[str, Any]],
    passing: list[dict[str, Any]],
    args: argparse.Namespace,
) -> None:
    pd.DataFrame(exact_rows).to_csv(OUT_CSV, index=False)
    write_approx_csv(approx)
    (WORK / "candidates").mkdir(exist_ok=True)

    pass_payload = []
    for i, p in enumerate(passing, start=1):
        cid = f"{SETUP}_adaptive_candidate_{i:03d}"
        payload = {
            "candidate_id": cid,
            "setup": SETUP,
            "side": SIDE,
            "config": p["candidate"].to_config(),
            "metrics": p["metrics"],
            "approval_required": True,
            "risk_notes": "Research-only. Do not move to live/paper watch until user approves.",
        }
        pass_payload.append(payload)
        (WORK / "candidates" / f"{cid}.json").write_text(json.dumps(fl._json_safe(payload), indent=2), encoding="utf-8")
    OUT_JSON.write_text(json.dumps(fl._json_safe(pass_payload), indent=2), encoding="utf-8")

    top_exact = sorted(
        exact_rows,
        key=lambda r: (
            float(r.get("train_profit_factor") or 0.0)
            if math.isfinite(float(r.get("train_profit_factor") or 0.0))
            else 5.0,
            float(r.get("train_net_pnl") or 0.0),
            int(r.get("train_trades") or 0),
        ),
        reverse=True,
    )[:20]
    controlled = [
        r for r in exact_rows
        if r.get("train_profit_factor") is not None
        and TRAIN_PF_MIN <= float(r.get("train_profit_factor") or 0.0) <= TRAIN_PF_MAX
    ]
    controlled.sort(key=lambda r: (float(r.get("test_profit_factor") or 0.0), float(r.get("train_profit_factor") or 0.0)), reverse=True)

    fail_counts = Counter(r.get("failure") or "unknown" for r in exact_rows)
    train_gate_counts = Counter(r.get("train_gate_reason") or "unknown" for r in exact_rows)
    test_runs = sum(1 for r in exact_rows if r.get("ran_test"))
    train_sessions = manifest.get("actual_train_sessions") or []
    test_sessions = manifest.get("actual_test_sessions") or []
    fit_sessions = manifest.get("actual_fit_sessions") or []
    val_sessions = manifest.get("actual_val_sessions") or []

    lines = [
        f"# Adaptive All-Knob Search - {SETUP}",
        "",
        "## Status",
        f"- Passing candidates requiring approval: {len(passing)}",
        f"- Approximate candidates generated: {len(approx)}",
        f"- Exact candidates evaluated: {len(exact_rows)}",
        f"- TEST runs allowed by train-side gate: {test_runs}",
        f"- Output CSV: `{OUT_CSV}`",
        f"- Approx CSV: `{OUT_APPROX_CSV}`",
        f"- Passing JSON: `{OUT_JSON}`",
        "",
        "## Data Windows",
        f"- FIT: {fit_sessions[0] if fit_sessions else 'none'}..{fit_sessions[-1] if fit_sessions else 'none'} ({len(fit_sessions)} sessions)",
        f"- VAL: {val_sessions[0] if val_sessions else 'none'}..{val_sessions[-1] if val_sessions else 'none'} ({len(val_sessions)} sessions)",
        f"- TRAIN: {train_sessions[0] if train_sessions else 'none'}..{train_sessions[-1] if train_sessions else 'none'} ({len(train_sessions)} sessions)",
        f"- TEST: {test_sessions[0] if test_sessions else 'none'}..{test_sessions[-1] if test_sessions else 'none'} ({len(test_sessions)} sessions)",
        f"- Pool rows after 1-minute attach: {len(pool)}",
        "",
        "## Method",
        "- Dynamic numeric feature discovery was used instead of the older hand-written uppercase indicator list.",
        "- Result/leaky columns were excluded: resolved exits, PnL, outcomes, costs, v6/v7/v8 resolution fields, entry/exit timestamps and prices.",
        "- Approximate discovery used the repo dedupe and cached 1-minute resolution, then exact validation used `setup_train_test.eval_family` and per-trade details.",
        "- TEST was run only after controlled train-side behavior or a separately labeled hot-train overfit probe.",
        "- Passing still requires TRAIN PF 1.30..1.80, TEST PF > 1.40, positive TRAIN/TEST net, minimum trades/days/symbols, domination checks, and controlled average loss.",
        "",
        "## Dynamically Tested Numeric Columns",
        ", ".join(numeric_features) if numeric_features else "none",
        "",
        "## Numeric Feature Diagnostics",
        "Columns with too few valid TRAIN values were not swept as honest train-time filters.",
        "```json",
        json.dumps(fl._json_safe(feature_diagnostics), indent=2),
        "```",
        "",
        "## Context Inventory",
        "```json",
        json.dumps(fl._json_safe(context_inventory), indent=2),
        "```",
        "",
        "## Term Inventory Summary",
        f"- Contexts with term generation: {len(term_inventory.get('contexts', []))}",
        f"- Numeric columns: {len(numeric_features)}",
        f"- Pre-momentum seed limit: {args.premom_seed_limit}",
        "",
        "## Exact Train-Gate Counts",
        "```json",
        json.dumps(fl._json_safe(dict(train_gate_counts)), indent=2),
        "```",
        "",
        "## Exact Failure Counts",
        "```json",
        json.dumps(fl._json_safe(dict(fail_counts)), indent=2),
        "```",
        "",
        "## Top Exact Candidates By TRAIN PF",
    ]
    if not top_exact:
        lines.append("none")
    for r in top_exact:
        lines.append(
            f"- {r['candidate_id']}: TRAIN n={r.get('train_trades')} PF={r.get('train_profit_factor')} "
            f"net=Rs {float(r.get('train_net_pnl') or 0):,.0f}; "
            f"FIT PF={r.get('fit_profit_factor')} VAL PF={r.get('val_profit_factor')} "
            f"TEST PF={r.get('test_profit_factor')}; failure={r.get('failure')}"
        )
    lines += ["", "## Controlled TRAIN PF Candidates"]
    if not controlled:
        lines.append("none")
    for r in controlled[:20]:
        lines.append(
            f"- {r['candidate_id']}: TRAIN n={r.get('train_trades')} PF={r.get('train_profit_factor')} "
            f"net=Rs {float(r.get('train_net_pnl') or 0):,.0f}; TEST n={r.get('test_trades')} "
            f"PF={r.get('test_profit_factor')} net=Rs {float(r.get('test_net_pnl') or 0):,.0f}; "
            f"failure={r.get('failure')}"
        )
    lines += ["", "## Passing Candidates"]
    if not passing:
        lines.append("No adaptive candidate passed all approval gates.")
    for i, p in enumerate(passing, start=1):
        c = p["candidate"]
        lines += [
            f"### {SETUP}_adaptive_candidate_{i:03d}",
            f"- config: `{json.dumps(fl._json_safe(c.to_config()), sort_keys=True)}`",
            f"- TRAIN: {fl.fmt_metrics(p['metrics']['TRAIN'])}",
            f"- TEST: {fl.fmt_metrics(p['metrics']['TEST'])}",
            "- approval status: APPROVAL REQUIRED before final config or live/paper watch.",
            "",
        ]
    lines += [
        "## Approval Note",
        "No final setup config was edited by this script. Any candidate above is research-only until the user explicitly approves it.",
    ]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    cand_lines = [f"# Candidate Configs - {SETUP}", "", "## Adaptive All-Knob Search"]
    if not passing:
        cand_lines.append("No adaptive candidate passed TRAIN PF 1.30..1.80, TEST PF > 1.40, positive PnL, trade-count, day/symbol, average-loss, and domination checks.")
    for i, p in enumerate(passing, start=1):
        cid = f"{SETUP}_adaptive_candidate_{i:03d}"
        c = p["candidate"]
        cand_lines += [
            f"### {cid}",
            f"- exact rules/config: `{json.dumps(fl._json_safe(c.to_config()), sort_keys=True)}`",
            f"- TRAIN: {fl.fmt_metrics(p['metrics']['TRAIN'])}",
            f"- TEST: {fl.fmt_metrics(p['metrics']['TEST'])}",
            "- approval recommendation: research-only candidate; do not move until user approves.",
            "",
        ]
    (WORK / "CANDIDATE_CONFIGS.md").write_text("\n".join(cand_lines) + "\n", encoding="utf-8")

    rec_lines = [f"# Approval Required Final Recommendation - {SETUP}", "", "## Adaptive Search Result"]
    if passing:
        best = passing[0]
        c = best["candidate"]
        config_block = {
            SETUP: {
                "side": SIDE,
                "enabled": False,
                "exit": {"sl_pct": c.sl, "tgt_pct": c.tgt},
                "mask_terms": c.mask_terms,
                "pre_momentum_terms": c.premom_terms,
                "entry_guards": c.guard or {},
                "provenance": {
                    "source": str(WORK),
                    "train": best["metrics"]["TRAIN"],
                    "test": best["metrics"]["TEST"],
                    "approval_required": True,
                },
            }
        }
        rec_lines += [
            f"- best candidate: {c.name}",
            f"- TRAIN: {fl.fmt_metrics(best['metrics']['TRAIN'])}",
            f"- TEST: {fl.fmt_metrics(best['metrics']['TEST'])}",
            "",
            "## Proposed Config Block",
            "```python",
            json.dumps(fl._json_safe(config_block), indent=4),
            "```",
            "",
            "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES",
        ]
    else:
        best = top_exact[0] if top_exact else None
        rec_lines += [
            "No candidate met all approval gates.",
            "",
            "## Best Exact Fallback",
            f"- candidate: {best['candidate_id'] if best else 'none'}",
            f"- TRAIN PF: {best.get('train_profit_factor') if best else 'n/a'}",
            f"- TEST PF: {best.get('test_profit_factor') if best else 'not run'}",
            "",
            "## Final File Requiring Approval",
            f"- `{fl.REPO_ROOT / 'final_setup_conf.py'}`",
            "- No diff/patch is recommended because no adaptive candidate passed.",
            "",
            "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES",
        ]
    (WORK / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(rec_lines) + "\n", encoding="utf-8")

    addendum = [
        "",
        "## Adaptive All-Knob Addendum",
        f"- Dynamic lowercase indicator/non-indicator search completed in `{OUT_MD}`.",
        f"- Approximate candidates generated: {len(approx)}.",
        f"- Exact candidates evaluated: {len(exact_rows)}.",
        f"- Passing approval-required candidates: {len(passing)}.",
        f"- Numeric columns tested included: {', '.join(numeric_features)}.",
    ]
    summary_path = WORK / "PARAMETER_SWEEP_SUMMARY.md"
    existing = summary_path.read_text(encoding="utf-8") if summary_path.exists() else f"# Parameter Sweep Summary - {SETUP}\n"
    marker = "## Adaptive All-Knob Addendum"
    if marker in existing:
        existing = existing.split(marker)[0].rstrip() + "\n"
    summary_path.write_text(existing.rstrip() + "\n" + "\n".join(addendum) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip_build_pool", action="store_true")
    ap.add_argument("--exit_grid_limit", type=int, default=0)
    ap.add_argument("--exit_keep", type=int, default=14)
    ap.add_argument("--context_keep", type=int, default=80)
    ap.add_argument("--beam_terms", type=int, default=45)
    ap.add_argument("--beam_pairs", type=int, default=70)
    ap.add_argument("--beam_terms_for_expansion", type=int, default=30)
    ap.add_argument("--max_terms", type=int, default=4)
    ap.add_argument("--approx_limit", type=int, default=1500)
    ap.add_argument("--exact_limit", type=int, default=180)
    ap.add_argument("--overlay_limit", type=int, default=25)
    ap.add_argument("--max_exact_per_signature", type=int, default=3)
    ap.add_argument("--premom_seed_limit", type=int, default=80)
    ap.add_argument("--premom_keep_per_seed", type=int, default=3)
    return ap.parse_args()


def main() -> int:
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    args = parse_args()
    args.max_terms = max(1, min(int(args.max_terms), 4))
    print(f"[adaptive] setup={SETUP}", flush=True)
    if not args.skip_build_pool or not fl.POOL_CSV.exists():
        manifest = fl.build_pool()
    else:
        manifest = json.loads((fl.POOL_DIR / "_manifest.json").read_text(encoding="utf-8"))
    pool, manifest = fl.load_prepared_pool()
    windows = fl.split_windows(pool, manifest)
    print(
        f"[adaptive] entries: FIT={len(windows['FIT'])} VAL={len(windows['VAL'])} "
        f"TRAIN={len(windows['TRAIN'])} TEST={len(windows['TEST'])}",
        flush=True,
    )
    numeric_features = discover_numeric_features(windows["TRAIN"])
    feature_diagnostics = numeric_feature_diagnostics(windows["TRAIN"], numeric_features)
    print(f"[adaptive] dynamic numeric features={len(numeric_features)}: {numeric_features}", flush=True)

    search = ApproxSearch(windows, numeric_features, args)
    contexts, context_inventory = select_contexts(search, args)
    approx, term_inventory = search_context_terms(search, contexts, args)
    pm_expanded = expand_premom(search, approx, args)
    approx += pm_expanded
    deduped: dict[str, ApproxConfig] = {}
    for cfg in approx:
        old = deduped.get(cfg.key())
        if old is None or cfg.approx_score > old.approx_score:
            deduped[cfg.key()] = cfg
    approx = sorted(
        deduped.values(),
        key=lambda c: (c.approx_score, c.approx["TRAIN"]["profit_factor"], c.approx["TRAIN"]["trades"]),
        reverse=True,
    )[: args.approx_limit]
    print(f"[adaptive] approximate candidates kept={len(approx)}", flush=True)

    exact_rows, passing = evaluate_exact(approx, windows, args)
    write_reports(
        manifest,
        pool,
        numeric_features,
        feature_diagnostics,
        context_inventory,
        term_inventory,
        approx,
        exact_rows,
        passing,
        args,
    )
    print(f"[adaptive] wrote {OUT_MD}", flush=True)
    print(f"[adaptive] passing candidates={len(passing)}", flush=True)
    if passing:
        best = passing[0]
        print(
            f"[adaptive] best={best['candidate'].name} "
            f"TRAIN PF={best['metrics']['TRAIN']['profit_factor']} n={best['metrics']['TRAIN']['trades']} "
            f"TEST PF={best['metrics']['TEST']['profit_factor']} n={best['metrics']['TEST']['trades']}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
