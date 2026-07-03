"""Path-based rework/recovery loop for A_PULLBACK_C2_THEN_BREAK_C2_HIGH.

This is research-only. It writes artifacts inside this setup folder and never
edits final_setup_conf.py. Unlike the mask-only adaptive pass, this tests entry
and exit redesigns on 1-minute paths: confirmation entries, retest entries,
time caps, breakeven, trailing exits, top-N, and day stops.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from dataclasses import asdict, dataclass, field, replace
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
WORK = fl.WORK
PATH_DIR = WORK / "path_rework"
PATHS_PARQUET = PATH_DIR / "paths.parquet"
SUMMARY_CSV = PATH_DIR / "summary.csv"
VALIDATION_JSON = PATH_DIR / "validation.json"
ITER_CSV = WORK / "path_rework_iterations.csv"
REPORT_MD = WORK / "PATH_REWORK_RESULT.md"

SLIP = 15.0 / 1e4
NOTIONAL = 100_000.0
EOD_H, EOD_M = 15, 20
PF_LO, PF_HI, TEST_PF_MIN = 1.30, 1.80, 1.40


@dataclass
class Variant:
    name: str
    entry: str = "next_open"       # next_open | confirm | retest
    confirm_k: int = 30
    retest_depth_atr: float = 0.25
    retest_arm_min: int = 1
    sl_pct: float = 0.70
    tgt_pct: float = 0.90
    time_cap_min: int = 0
    breakeven_at: float = 0.0
    trail_pct: float = 0.0
    mask: list[list[Any]] = field(default_factory=list)
    dedupe: str = "score"          # none | first | score
    min_slot: int = 0
    max_slot: int = 0
    top_n: int = 0
    rank_col: str = "quality_score"
    max_open: int = 20
    max_trades_day: int = 0
    daily_loss_rs: float = 0.0
    max_losses_day: int = 0


def _json_safe(obj: Any) -> Any:
    return fl._json_safe(obj)


def _ts_ist(series: pd.Series) -> pd.Series:
    ts = pd.to_datetime(series, errors="coerce")
    if getattr(ts.dt, "tz", None) is None:
        return ts.dt.tz_localize("Asia/Kolkata")
    return ts.dt.tz_convert("Asia/Kolkata")


def load_pool() -> tuple[pd.DataFrame, dict[str, Any]]:
    pool, manifest = fl.load_prepared_pool()
    pool = fl.split_windows(pool, manifest)["TRAIN"]._append(
        fl.split_windows(pool, manifest)["TEST"], ignore_index=True
    )
    pool = pool.drop_duplicates(subset=["ticker", "side", "setup", "signal_time_ist"]).reset_index(drop=True)
    pool.index.name = "sid"
    pool["sig_ts"] = _ts_ist(pool["signal_time_ist"])
    pool["_day_str"] = pool["sig_ts"].dt.strftime("%Y-%m-%d")
    pool["_minute"] = pool["sig_ts"].dt.hour * 60 + pool["sig_ts"].dt.minute
    pool["_slot"] = (pool["_minute"] // 5) * 5
    if "notional" not in pool.columns:
        pool["notional"] = pd.to_numeric(pool.get("tt_fill"), errors="coerce") * pd.to_numeric(pool.get("tt_qty"), errors="coerce")
    return pool.reset_index(), manifest


def build_paths(pool: pd.DataFrame, force: bool = False) -> None:
    if PATHS_PARQUET.exists() and SUMMARY_CSV.exists() and not force:
        print(f"[path-rework] using existing paths in {PATH_DIR}", flush=True)
        return
    PATH_DIR.mkdir(parents=True, exist_ok=True)
    path_chunks: list[pd.DataFrame] = []
    summaries: list[dict[str, Any]] = []
    tickers = sorted(pool["ticker"].astype(str).str.upper().unique())
    t0 = time.time()
    for n, tk in enumerate(tickers, start=1):
        rows = pool[pool["ticker"].astype(str).str.upper() == tk]
        bars = tt.v11._load_1m_with_open(tk)
        if bars is None or bars.empty:
            continue
        bars = bars.sort_index()
        idx = bars.index
        for r in rows.itertuples():
            st = pd.Timestamp(r.sig_ts)
            if pd.isna(st):
                continue
            if st.tzinfo is None:
                st = st.tz_localize("Asia/Kolkata")
            else:
                st = st.tz_convert("Asia/Kolkata")
            eod = st.normalize() + pd.Timedelta(hours=EOD_H, minutes=EOD_M)
            sub = bars[(idx >= st) & (idx <= eod)]
            if sub.empty:
                continue
            sid = int(r.sid)
            mins = ((sub.index - st).total_seconds() // 60).astype(np.int16)
            path_chunks.append(pd.DataFrame({
                "sid": np.full(len(sub), sid, dtype=np.int32),
                "k": np.arange(len(sub), dtype=np.int16),
                "min_from_sig": mins,
                "o": sub["open"].astype(np.float32).to_numpy(),
                "h": sub["high"].astype(np.float32).to_numpy(),
                "l": sub["low"].astype(np.float32).to_numpy(),
                "c": sub["close"].astype(np.float32).to_numpy(),
            }))
            o = sub["open"].to_numpy(float)
            h = sub["high"].to_numpy(float)
            l = sub["low"].to_numpy(float)
            c = sub["close"].to_numpy(float)
            entry_open = float(o[0])
            atr_abs = float(getattr(r, "atr_pct", np.nan)) * float(getattr(r, "signal_close", np.nan))
            sig_high = float(getattr(r, "signal_high", np.nan))
            sig_low = float(getattr(r, "signal_low", np.nan))
            conf = np.nonzero(h[1:] > sig_high)[0] if np.isfinite(sig_high) else []
            ret20 = np.nonzero(l[1:] <= (float(getattr(r, "signal_close", np.nan)) - 0.20 * atr_abs))[0] if np.isfinite(atr_abs) else []
            summaries.append({
                "sid": sid,
                "ticker": tk,
                "sig_ts": str(st),
                "n_bars": int(len(sub)),
                "entry_open": entry_open,
                "eod_close": float(c[-1]),
                "mfe_pct": float(h.max() / entry_open - 1.0) * 100.0,
                "mae_pct": float(l.min() / entry_open - 1.0) * 100.0,
                "mae_before_mfe_pct": float(l[: int(np.argmax(h)) + 1].min() / entry_open - 1.0) * 100.0,
                "conf_k": int(conf[0] + 1) if len(conf) else -1,
                "retest20_k": int(ret20[0] + 1) if len(ret20) else -1,
                "atr_abs": atr_abs,
                "signal_high": sig_high,
                "signal_low": sig_low,
            })
        if n % 150 == 0 or n == len(tickers):
            print(f"[path-rework] paths {n}/{len(tickers)} tickers elapsed={time.time()-t0:.0f}s chunks={len(path_chunks)}", flush=True)
    if not path_chunks:
        raise SystemExit("[path-rework] no paths extracted")
    pd.concat(path_chunks, ignore_index=True).to_parquet(PATHS_PARQUET, index=False)
    pd.DataFrame(summaries).to_csv(SUMMARY_CSV, index=False)
    print(f"[path-rework] wrote {PATHS_PARQUET} and {SUMMARY_CSV}", flush=True)


class PathEngine:
    def __init__(self, pool: pd.DataFrame, manifest: dict[str, Any]):
        self.pool = pool.copy()
        summary = pd.read_csv(SUMMARY_CSV)
        paths = pd.read_parquet(PATHS_PARQUET)
        self.df = summary.merge(self.pool, on="sid", how="left", suffixes=("", "_pool"))
        self.df["atr_abs"] = pd.to_numeric(self.df["atr_abs"], errors="coerce")
        self.df["_day_str"] = pd.to_datetime(self.df["sig_ts"], errors="coerce").dt.strftime("%Y-%m-%d")
        self.df["_minute"] = pd.to_datetime(self.df["sig_ts"], errors="coerce").dt.hour * 60 + pd.to_datetime(self.df["sig_ts"], errors="coerce").dt.minute
        self.df["_slot"] = (self.df["_minute"] // 5) * 5
        self.paths: dict[int, tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = {}
        for sid, g in paths.groupby("sid", sort=False):
            a = g.sort_values("k")
            self.paths[int(sid)] = (
                a["min_from_sig"].to_numpy(np.int16),
                a["o"].to_numpy(float),
                a["h"].to_numpy(float),
                a["l"].to_numpy(float),
                a["c"].to_numpy(float),
            )
        train_days = list(manifest.get("actual_train_sessions") or [])
        test_days = list(manifest.get("actual_test_sessions") or [])
        fit_n = max(1, int(math.floor(len(train_days) * 0.60)))
        self.windows = {
            "FIT": set(train_days[:fit_n]),
            "VAL": set(train_days[fit_n:]),
            "TRAIN": set(train_days),
            "TEST": set(test_days),
        }

    def _resolve(self, sid: int, row: Any, v: Variant):
        if sid not in self.paths:
            return None
        mins, o, h, l, c = self.paths[sid]
        if len(mins) == 0:
            return None
        tradeable = np.nonzero((mins >= 0) & (mins <= 3))[0]
        if not len(tradeable):
            return None
        sig_high = float(row.get("signal_high", np.nan))
        sig_close = float(row.get("signal_close", np.nan))
        atr = float(row.get("atr_abs", np.nan))
        if v.entry == "next_open":
            ek = int(tradeable[0])
            ref = float(o[ek])
        elif v.entry == "confirm":
            if not np.isfinite(sig_high):
                return None
            idx = np.nonzero((mins >= 1) & (mins <= v.confirm_k) & (h > sig_high))[0]
            if not len(idx):
                return None
            ek = int(idx[0])
            ref = max(float(o[ek]), sig_high)
        elif v.entry == "retest":
            if not (np.isfinite(atr) and atr > 0 and np.isfinite(sig_close)):
                return None
            level = sig_close - v.retest_depth_atr * atr
            idx = np.nonzero((mins >= max(1, v.retest_arm_min)) & (mins <= v.confirm_k) & (l <= level))[0]
            if not len(idx):
                return None
            ek = int(idx[0])
            ref = float(level)
        else:
            raise ValueError(v.entry)
        if not np.isfinite(ref) or ref <= 0:
            return None
        fill = ref * (1.0 + SLIP)
        qty = max(1, int(NOTIONAL / ref))
        sl = fill * (1.0 - v.sl_pct / 100.0)
        tgt = fill * (1.0 + v.tgt_pct / 100.0)
        stop = sl
        hi_run = fill
        cap_idx = None
        if v.time_cap_min:
            cap_min = int(mins[ek]) + int(v.time_cap_min)
            later = np.nonzero(mins >= cap_min)[0]
            cap_idx = int(later[0]) if len(later) else len(mins) - 1
        out = "EOD"
        exit_px = float(c[-1])
        exit_min = int(mins[-1])
        for i in range(ek, len(mins)):
            hi_run = max(hi_run, float(h[i]))
            if v.breakeven_at > 0 and hi_run >= fill * (1.0 + v.breakeven_at / 100.0):
                stop = max(stop, fill)
            if v.trail_pct > 0:
                stop = max(stop, hi_run * (1.0 - v.trail_pct / 100.0))
            if float(l[i]) <= stop:
                out = "SL" if stop <= sl + 1e-9 else "TRAIL"
                exit_px = stop
                exit_min = int(mins[i])
                break
            if float(h[i]) >= tgt:
                out = "TARGET"
                exit_px = tgt
                exit_min = int(mins[i])
                break
            if cap_idx is not None and i >= cap_idx:
                out = "TIME"
                exit_px = float(c[i])
                exit_min = int(mins[i])
                break
        exit_px_slipped = exit_px * (1.0 - SLIP)
        net = float(tt._trade_net("LONG", float(fill), int(qty), out, float(exit_px_slipped), model="statutory"))
        return {
            "sid": sid,
            "entry_min_from_sig": int(mins[ek]),
            "exit_min_from_sig": int(exit_min),
            "fill": float(fill),
            "exit_px": float(exit_px_slipped),
            "qty": int(qty),
            "outcome": out,
            "net": net,
        }

    def _filtered(self, v: Variant, window: str) -> pd.DataFrame:
        d = self.df[self.df["_day_str"].isin(self.windows[window])].copy()
        if v.dedupe == "first":
            d = d.sort_values(["sig_ts", "ticker"]).groupby(["ticker", "_day_str"], sort=False).head(1)
        elif v.dedupe == "score":
            score = pd.to_numeric(d.get("quality_score", d.get("ranker_score", 0.0)), errors="coerce").fillna(0.0)
            d = d.assign(_score=score).sort_values(["_score", "sig_ts"], ascending=[False, True])
            d = d.groupby(["ticker", "_day_str"], sort=False).head(1)
        if v.min_slot:
            d = d[d["_minute"] >= v.min_slot]
        if v.max_slot:
            d = d[d["_minute"] <= v.max_slot]
        for col, op, val in v.mask:
            if col not in d.columns:
                return d.iloc[0:0]
            if isinstance(val, str):
                x = d[col].astype(str).str.upper()
                vv = val.upper()
                keep = x.ne(vv) if op == "!=" else x.eq(vv)
            else:
                x = pd.to_numeric(d[col], errors="coerce")
                if op == ">=":
                    keep = x >= float(val)
                elif op == "<=":
                    keep = x <= float(val)
                elif op == ">":
                    keep = x > float(val)
                elif op == "<":
                    keep = x < float(val)
                else:
                    keep = x == float(val)
            d = d[keep.fillna(False)]
        if v.top_n and v.rank_col in d.columns:
            d = (
                d.sort_values(v.rank_col, ascending=False)
                .groupby(["_day_str", "_slot"], sort=False)
                .head(v.top_n)
            )
        return d.sort_values("sig_ts").reset_index(drop=True)

    def evaluate(self, v: Variant, window: str, detail: bool = False) -> dict[str, Any]:
        d = self._filtered(v, window)
        trades: list[dict[str, Any]] = []
        open_until: list[tuple[str, int]] = []
        day_state: dict[str, dict[str, Any]] = {}
        for _, row in d.iterrows():
            sid = int(row["sid"])
            day = str(row["_day_str"])
            state = day_state.setdefault(day, {"n": 0, "closed": []})
            if v.max_trades_day and state["n"] >= v.max_trades_day:
                continue
            r = self._resolve(sid, row, v)
            if r is None:
                continue
            entry_abs = int(row["_minute"]) + int(r["entry_min_from_sig"])
            closed = [x for x in state["closed"] if x[0] <= entry_abs]
            if v.daily_loss_rs and sum(x[1] for x in closed) <= -abs(v.daily_loss_rs):
                continue
            if v.max_losses_day and sum(1 for x in closed if x[1] < 0) >= v.max_losses_day:
                continue
            open_until = [(dy, xm) for dy, xm in open_until if not (dy == day and xm <= entry_abs)]
            if v.max_open and sum(1 for dy, _ in open_until if dy == day) >= v.max_open:
                continue
            exit_abs = int(row["_minute"]) + int(r["exit_min_from_sig"])
            open_until.append((day, exit_abs))
            state["n"] += 1
            state["closed"].append((exit_abs, float(r["net"])))
            trades.append({
                **r,
                "ticker": row["ticker"],
                "day": day,
                "signal_time": row["sig_ts"],
                "signal_minute": int(row["_minute"]),
            })
        t = pd.DataFrame(trades)
        if t.empty:
            out = {
                "n": 0,
                "pf": 0.0,
                "net": 0.0,
                "win": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "days": 0,
                "symbols": 0,
                "dom_trade": None,
                "dom_day": None,
                "dom_sym": None,
                "sl": 0,
                "tgt": 0,
                "eod": 0,
                "time": 0,
                "trail": 0,
            }
            if detail:
                out["detail"] = t
            return out
        net = t["net"].to_numpy(float)
        gp = float(net[net > 0].sum())
        gl = float(-net[net < 0].sum())
        total = float(net.sum())
        wins = net[net > 0]
        losses = net[net <= 0]
        days = t.groupby("day")["net"].sum()
        syms = t.groupby("ticker")["net"].sum()

        def _share(x: float, denom: float) -> float | None:
            if denom <= 0:
                return None
            return round(float(x) / denom, 4)

        out = {
            "n": int(len(t)),
            "pf": round(gp / gl, 4) if gl > 0 else (float("inf") if gp > 0 else 0.0),
            "net": round(total, 2),
            "win": round(float((net > 0).mean() * 100.0), 2),
            "avg_win": round(float(wins.mean()), 2) if len(wins) else 0.0,
            "avg_loss": round(float(losses.mean()), 2) if len(losses) else 0.0,
            "days": int(t["day"].nunique()),
            "symbols": int(t["ticker"].nunique()),
            "dom_trade": _share(float(wins.max()) if len(wins) else 0.0, gp),
            "dom_day": _share(float(days.max()) if len(days) else 0.0, total),
            "dom_sym": _share(float(syms.max()) if len(syms) else 0.0, total),
            "sl": int((t["outcome"] == "SL").sum()),
            "tgt": int((t["outcome"] == "TARGET").sum()),
            "eod": int((t["outcome"] == "EOD").sum()),
            "time": int((t["outcome"] == "TIME").sum()),
            "trail": int((t["outcome"] == "TRAIL").sum()),
        }
        if detail:
            out["detail"] = t
        return out


def validate_paths(pool: pd.DataFrame, sample_n: int = 300) -> dict[str, Any]:
    if VALIDATION_JSON.exists():
        try:
            return json.loads(VALIDATION_JSON.read_text(encoding="utf-8"))
        except Exception:
            pass
    paths = pd.read_parquet(PATHS_PARQUET)
    sample = pool.sample(n=min(sample_n, len(pool)), random_state=13)
    match = 0
    total = 0
    mismatches = []
    for r in sample.itertuples():
        e = tt._entry(str(r.ticker), "LONG", pd.Timestamp(r.sig_ts).isoformat())
        if e is None:
            continue
        entry_iso, fill, qty = e
        rr = tt._resolve_full(str(r.ticker), "LONG", entry_iso, float(fill), int(qty), fl.BASE_EXIT["sl"], fl.BASE_EXIT["tgt"])
        if rr is None:
            continue
        sub = paths[paths["sid"] == int(r.sid)].sort_values("k")
        if sub.empty:
            continue
        l = sub["l"].to_numpy(float)
        h = sub["h"].to_numpy(float)
        slp = float(fill) * (1.0 - fl.BASE_EXIT["sl"] / 100.0)
        tgp = float(fill) * (1.0 + fl.BASE_EXIT["tgt"] / 100.0)
        mine = "EOD"
        for i in range(len(sub)):
            if l[i] <= slp:
                mine = "SL"
                break
            if h[i] >= tgp:
                mine = "TARGET"
                break
        total += 1
        if mine == str(rr[1]).upper():
            match += 1
        elif len(mismatches) < 10:
            mismatches.append({"sid": int(r.sid), "ticker": r.ticker, "mine": mine, "canonical": str(rr[1])})
    out = {"sampled": total, "matched": match, "match_rate": round(match / max(total, 1), 4), "mismatches": mismatches}
    VALIDATION_JSON.write_text(json.dumps(_json_safe(out), indent=2), encoding="utf-8")
    return out


def _passes_train(m: dict[str, Any]) -> bool:
    return (
        int(m["n"]) >= 20
        and m["net"] > 0
        and math.isfinite(float(m["pf"]))
        and PF_LO <= float(m["pf"]) <= PF_HI
    )


def _passes_final(train: dict[str, Any], test: dict[str, Any]) -> tuple[bool, str]:
    reasons = []
    if not _passes_train(train):
        reasons.append("TRAIN gate fail")
    if int(test["n"]) < 5:
        reasons.append("TEST n<5")
    if float(test["pf"]) <= TEST_PF_MIN:
        reasons.append("TEST PF<=1.40")
    if test["net"] <= 0:
        reasons.append("TEST net<=0")
    if test["dom_trade"] is not None and test["dom_trade"] > 0.35:
        reasons.append("TEST top_trade>35% gross profit")
    if test["dom_day"] is not None and test["dom_day"] > 0.40:
        reasons.append("TEST top_day>40% net")
    if test["dom_sym"] is not None and test["dom_sym"] > 0.40:
        reasons.append("TEST top_symbol>40% net")
    if abs(float(train["avg_loss"])) > 1250:
        reasons.append("TRAIN avg_loss too high")
    if abs(float(test["avg_loss"])) > 1250:
        reasons.append("TEST avg_loss too high")
    return (not reasons), "; ".join(reasons) or "APPROVAL_REQUIRED"


def _score(fit: dict[str, Any], val: dict[str, Any], train: dict[str, Any]) -> float:
    fit_pf = min(float(fit["pf"]), 5.0) if math.isfinite(float(fit["pf"])) else 5.0
    val_pf = min(float(val["pf"]), 5.0) if math.isfinite(float(val["pf"])) else 5.0
    train_pf = min(float(train["pf"]), 5.0) if math.isfinite(float(train["pf"])) else 5.0
    penalty = 0.0
    if train_pf < PF_LO:
        penalty += (PF_LO - train_pf) * 2.0
    elif train_pf > PF_HI:
        penalty += (train_pf - PF_HI) * 2.0
    if train["n"] < 20:
        penalty += (20 - train["n"]) / 10.0
    if train["net"] <= 0:
        penalty += 0.8
    return min(fit_pf, val_pf, train_pf) - 0.35 * abs(fit_pf - val_pf) - penalty + min(train["n"], 160) / 1000.0


def run_rework(engine: PathEngine, max_iter: int) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    passing: list[dict[str, Any]] = []
    seen: set[str] = set()
    variants: list[tuple[Variant, str, str]] = []

    def add(v: Variant, block: str, note: str) -> None:
        key = json.dumps(asdict(v), sort_keys=True, default=str)
        if key in seen:
            return
        seen.add(key)
        variants.append((v, block, note))

    base = Variant(name="base_next_open_score", dedupe="score", sl_pct=fl.BASE_EXIT["sl"], tgt_pct=fl.BASE_EXIT["tgt"])
    for dedupe in ("score", "first", "none"):
        for sl in (0.45, 0.55, 0.70, 0.85, 1.00, 1.20, 1.40):
            for tgt in (0.70, 0.90, 1.10, 1.40, 1.80, 2.20, 2.80):
                add(replace(base, name=f"A_{dedupe}_sl{sl}_t{tgt}", dedupe=dedupe, sl_pct=sl, tgt_pct=tgt), "A", "next-open exit grid")
    for tc in (30, 60, 90, 120, 180):
        add(replace(base, name=f"B_tc{tc}", sl_pct=0.85, tgt_pct=1.80, time_cap_min=tc), "B", "time cap")
    for be in (0.30, 0.50, 0.75):
        for tr in (0.0, 0.60, 0.90, 1.20):
            add(replace(base, name=f"B_be{be}_tr{tr}", sl_pct=0.85, tgt_pct=2.20, breakeven_at=be, trail_pct=tr), "B", "BE/trail")
    for ck in (5, 10, 15, 30, 60):
        for sl in (0.55, 0.70, 0.85, 1.00, 1.20):
            for tgt in (1.00, 1.40, 1.80, 2.20, 2.80):
                add(replace(base, name=f"C_conf{ck}_sl{sl}_t{tgt}", entry="confirm", confirm_k=ck, sl_pct=sl, tgt_pct=tgt), "C", "confirmation entry")
    for dep in (0.10, 0.20, 0.35, 0.50, 0.75):
        for ck in (10, 20, 30, 60):
            for sl, tgt in ((0.55, 1.10), (0.70, 1.40), (0.85, 1.80), (1.00, 2.20)):
                add(replace(base, name=f"D_ret{dep}_k{ck}_sl{sl}_t{tgt}", entry="retest", retest_depth_atr=dep, confirm_k=ck, sl_pct=sl, tgt_pct=tgt), "D", "retest entry")
    for mn, mx, tag in ((0, 690, "am"), (690, 810, "mid"), (780, 0, "late"), (0, 810, "no_late"), (600, 0, "no_open")):
        add(replace(base, name=f"E_{tag}", sl_pct=0.85, tgt_pct=1.80, min_slot=mn, max_slot=mx), "E", "time window")
    for topn in (1, 2, 3):
        for rank in ("quality_score", "vwap_dist_atr", "vol_ratio", "ranker_score"):
            add(replace(base, name=f"F_top{topn}_{rank}", sl_pct=0.85, tgt_pct=1.80, top_n=topn, rank_col=rank), "F", "top-n slot")
    for mtd in (3, 5, 10, 20):
        add(replace(base, name=f"F_mtd{mtd}", sl_pct=0.85, tgt_pct=1.80, max_trades_day=mtd), "F", "max trades/day")
    for dl in (2000.0, 3000.0, 5000.0, 7500.0):
        add(replace(base, name=f"F_dloss{int(dl)}", sl_pct=0.85, tgt_pct=1.80, daily_loss_rs=dl), "F", "realized daily loss")
    for ml in (1, 2, 3):
        add(replace(base, name=f"F_mloss{ml}", sl_pct=0.85, tgt_pct=1.80, max_losses_day=ml), "F", "max realized losing closes/day")

    # Dynamic single and pair masks around the best robust path family.
    mask_base = replace(base, name="maskbase", sl_pct=0.85, tgt_pct=2.00, max_slot=810)
    feats = [
        "market_ret_pct", "market_abs_ret_pct", "signal_minute", "upper_wick_pct",
        "lower_wick_pct", "wick_skew_pct", "vwap_dist_atr", "vol_ratio", "atr_pct",
        "body_pct", "close_loc", "rs_pct", "quality_score", "notional", "signal_range_pct",
    ]
    qgrid = [0.10, 0.20, 0.33, 0.50, 0.67, 0.80, 0.90]
    terms: list[list[Any]] = []
    tr_df = engine.df[engine.df["_day_str"].isin(engine.windows["TRAIN"])]
    for feat in feats:
        if feat not in tr_df.columns:
            continue
        s = pd.to_numeric(tr_df[feat], errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if len(s) < 50 or s.nunique() <= 3:
            continue
        for q in qgrid:
            val = round(float(s.quantile(q)), 6)
            terms.append([feat, ">=", val])
            terms.append([feat, "<=", val])
    for term in terms:
        add(replace(mask_base, name=f"G_{term[0]}{term[1]}{term[2]}", mask=[term]), "G", "single dynamic mask")
    # A few explicit structural combinations from the prior adaptive run's near misses.
    explicit = [
        [["upper_wick_pct", ">=", 0.20], ["vwap_dist_atr", "<=", 0.66]],
        [["market_ret_pct", "<=", 0.0], ["signal_minute", ">=", 795.0]],
        [["wick_skew_pct", ">=", 0.10], ["body_pct", ">=", 0.80]],
        [["vol_ratio", "<=", 2.2], ["vwap_dist_atr", "<=", 1.0]],
        [["atr_pct", ">=", 0.0025], ["market_abs_ret_pct", "<=", 0.35]],
    ]
    for i, mask in enumerate(explicit, 1):
        add(replace(mask_base, name=f"H_explicit{i}", mask=mask), "H", "explicit structural combo")

    # Evaluate first pass.
    t0 = time.time()
    for i, (v, block, note) in enumerate(variants[:max_iter], start=1):
        fit = engine.evaluate(v, "FIT")
        val = engine.evaluate(v, "VAL")
        train = engine.evaluate(v, "TRAIN")
        test = None
        verdict = "REJECT"
        reason = "train_gate_fail"
        if _passes_train(train) and fit["n"] >= 5 and val["n"] >= 5 and fit["net"] >= 0 and val["net"] >= 0:
            test = engine.evaluate(v, "TEST")
            ok, reason = _passes_final(train, test)
            verdict = "CANDIDATE" if ok else "BAND_BUT_TEST_FAIL"
            if ok:
                passing.append({"variant": v, "fit": fit, "val": val, "train": train, "test": test})
        score = _score(fit, val, train)
        rec = {
            "iter": i,
            "block": block,
            "name": v.name,
            "note": note,
            "cfg": json.dumps(_json_safe(asdict(v)), sort_keys=True),
            "score": round(score, 6),
            "verdict": verdict,
            "reason": reason,
        }
        for label, m in (("fit", fit), ("val", val), ("train", train), ("test", test or {})):
            for k in ("n", "pf", "net", "win", "avg_loss", "days", "symbols", "dom_trade", "dom_day", "dom_sym", "sl", "tgt", "eod", "time", "trail"):
                rec[f"{label}_{k}"] = m.get(k)
        rows.append(rec)
        if i % 25 == 0:
            pd.DataFrame(rows).to_csv(ITER_CSV, index=False)
            print(f"[path-rework] iter {i}/{min(len(variants), max_iter)} elapsed={time.time()-t0:.0f}s", flush=True)
    out = pd.DataFrame(rows)
    out.to_csv(ITER_CSV, index=False)
    return out, passing


def write_report(pool: pd.DataFrame, validation: dict[str, Any], rows: pd.DataFrame, passing: list[dict[str, Any]]) -> None:
    controlled = rows[(pd.to_numeric(rows["train_pf"], errors="coerce") >= PF_LO) & (pd.to_numeric(rows["train_pf"], errors="coerce") <= PF_HI)]
    top_test = rows[pd.to_numeric(rows["test_pf"], errors="coerce").notna()].copy()
    if len(top_test):
        top_test["_test_pf_num"] = pd.to_numeric(top_test["test_pf"], errors="coerce")
        top_test = top_test.sort_values(["_test_pf_num", "test_net"], ascending=False).head(15)
    top_train = rows.copy()
    top_train["_score_num"] = pd.to_numeric(top_train["score"], errors="coerce")
    top_train = top_train.sort_values("_score_num", ascending=False).head(15)
    lines = [
        f"# Path Rework Result - {SETUP}",
        "",
        "Research-only. No final config or live/paper watch change was made.",
        "",
        "## Status",
        f"- Pool signals evaluated: {len(pool)}",
        f"- Path validation vs canonical base resolver: {validation}",
        f"- Iterations: {len(rows)}",
        f"- Passing approval-required candidates: {len(passing)}",
        "",
        "## Top TRAIN/TEST Rows",
    ]
    if len(top_test):
        for _, r in top_test.iterrows():
            lines.append(
                f"- {r['name']}: TRAIN n={r.get('train_n')} PF={r.get('train_pf')} net=Rs {float(r.get('train_net') or 0):,.0f}; "
                f"TEST n={r.get('test_n')} PF={r.get('test_pf')} net=Rs {float(r.get('test_net') or 0):,.0f}; verdict={r.get('verdict')} reason={r.get('reason')}"
            )
    else:
        lines.append("No candidate reached TEST.")
    lines += ["", "## Top Robust Search Scores"]
    for _, r in top_train.iterrows():
        lines.append(
            f"- {r['name']}: FIT {r.get('fit_n')}/{r.get('fit_pf')} VAL {r.get('val_n')}/{r.get('val_pf')} "
            f"TRAIN {r.get('train_n')}/{r.get('train_pf')} net=Rs {float(r.get('train_net') or 0):,.0f}; verdict={r.get('verdict')}"
        )
    lines += ["", "## Controlled TRAIN PF Rows"]
    if len(controlled):
        tmp = controlled.copy()
        tmp["_test_pf_num"] = pd.to_numeric(tmp["test_pf"], errors="coerce").fillna(-1)
        for _, r in tmp.sort_values("_test_pf_num", ascending=False).head(20).iterrows():
            lines.append(
                f"- {r['name']}: TRAIN n={r.get('train_n')} PF={r.get('train_pf')} net=Rs {float(r.get('train_net') or 0):,.0f}; "
                f"TEST n={r.get('test_n')} PF={r.get('test_pf')} net=Rs {float(r.get('test_net') or 0):,.0f}; reason={r.get('reason')}"
            )
    else:
        lines.append("none")
    lines += ["", "## Passing Candidates"]
    if not passing:
        lines.append("No path-rework candidate passed TRAIN PF > 1.30 and TEST PF > 1.40 with positive PnL/stability gates.")
    for i, p in enumerate(passing, start=1):
        v = p["variant"]
        lines += [
            f"### {SETUP}_path_rework_candidate_{i:03d}",
            f"- config: `{json.dumps(_json_safe(asdict(v)), sort_keys=True)}`",
            f"- TRAIN: {p['train']}",
            f"- TEST: {p['test']}",
            "- approval status: APPROVAL REQUIRED before any live/paper watch.",
            "",
        ]
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    # Update recommendation with current path rework result.
    rec = [
        f"# Approval Required Final Recommendation - {SETUP}",
        "",
        "## Current Rework Result",
        f"- Adaptive all-knob filter search: 0 passing candidates.",
        f"- Path rework search: {len(passing)} passing candidates.",
        f"- Path rework report: `{REPORT_MD}`",
        "",
    ]
    if passing:
        best = passing[0]
        rec += [
            "## Best Candidate",
            f"- config: `{json.dumps(_json_safe(asdict(best['variant'])), sort_keys=True)}`",
            f"- TRAIN: {best['train']}",
            f"- TEST: {best['test']}",
            "",
            "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES",
        ]
    else:
        rec += [
            "No candidate is approved for final config or live/paper watch.",
            "",
            "DO NOT MOVE TO FINAL CONFIG UNTIL USER APPROVES",
        ]
    (WORK / "APPROVAL_REQUIRED_FINAL_RECOMMENDATION.md").write_text("\n".join(rec) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force_paths", action="store_true")
    ap.add_argument("--max_iter", type=int, default=420)
    args = ap.parse_args()
    pool, manifest = load_pool()
    print(f"[path-rework] pool rows={len(pool)} train={len(manifest.get('actual_train_sessions') or [])} test={len(manifest.get('actual_test_sessions') or [])}", flush=True)
    build_paths(pool, force=args.force_paths)
    validation = validate_paths(pool)
    print(f"[path-rework] validation={validation}", flush=True)
    engine = PathEngine(pool, manifest)
    rows, passing = run_rework(engine, max_iter=int(args.max_iter))
    write_report(pool, validation, rows, passing)
    print(f"[path-rework] wrote {REPORT_MD}", flush=True)
    print(f"[path-rework] passing candidates={len(passing)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
