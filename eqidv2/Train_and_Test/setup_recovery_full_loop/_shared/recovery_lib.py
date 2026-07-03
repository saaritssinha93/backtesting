r"""recovery_lib.py — shared research library for the setup_recovery_full_loop campaign.

Research-only. NEVER writes final_setup_conf.py. No live trading.

Provides:
  * load_attached(setup, workdir)  — pool -> tt.load_pool -> attach_entries, persisted
  * precompute_premom(...)         — 1-min pre-entry momentum features @ sl=0.90, persisted
  * precompute_exit_grid(...)      — per-row net/outcome/exit_iso for every (sl,tgt), persisted
  * precompute_mae_mfe(...)        — 1-min MAE/MFE excursions to 15:20, persisted
  * ResearchEngine                 — fast eval mirroring setup_train_test.eval_family order:
        guards -> premom(precomputed) -> dedupe_family -> mask -> portfolio overlay -> net lookup
    The ONLY approximation vs the faithful engine is premom features computed at a fixed
    sl=0.90 stop (features barely depend on the stop). Serious candidates must be
    re-confirmed via pf_band_fitval_loop.full_metrics (exact path).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
TT_DIR = _HERE.parents[1]              # Train_and_Test
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for _p in (str(REPO), str(TT_DIR), str(ENGINE_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt  # noqa: E402

SL_GRID = [0.50, 0.70, 0.90, 1.10, 1.50]
TGT_GRID = [0.60, 0.80, 1.00, 1.25, 1.50, 2.00, 2.50]

TRAIN_START = pd.Timestamp("2026-03-01")
TRAIN_END = pd.Timestamp("2026-05-30")
TEST_START = pd.Timestamp("2026-06-01")
TEST_END = pd.Timestamp("2026-07-02")
FIT_FRAC = 0.60

KEYCOLS = ["ticker", "side", "setup", "signal_time_ist"]


def _rowkey(df: pd.DataFrame) -> pd.Series:
    return (df["ticker"].astype(str) + "|" + df["side"].astype(str) + "|"
            + df["setup"].astype(str) + "|" + df["signal_time_ist"].astype(str))


def load_attached(setup: str, workdir: Path, slippage_bps: float = 15.0) -> pd.DataFrame:
    """Pool -> tt.load_pool -> attach_entries; persists tt_* columns so re-runs are cheap."""
    workdir = Path(workdir)
    pool_dir = workdir / "pools" / "pool_full"
    derived = workdir / "pools" / "derived"
    derived.mkdir(parents=True, exist_ok=True)
    cache = derived / "attached_entries.parquet"

    tt.POOL_DIRS = [pool_dir]
    tt.SLIPPAGE_BPS = float(slippage_bps)
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()
    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.upper().eq(setup)].copy()
    pool["rk"] = _rowkey(pool)

    prev = None
    if cache.exists():
        prev = pd.read_parquet(cache)
        prev = prev[prev["slippage_bps"] == float(slippage_bps)]
    if prev is not None and len(prev):
        pool = pool.merge(prev[["rk", "tt_entry_iso", "tt_fill", "tt_qty"]], on="rk", how="left")
        missing = pool["tt_entry_iso"].isna() | (pool["tt_entry_iso"] == "")
    else:
        pool["tt_entry_iso"] = None; pool["tt_fill"] = np.nan; pool["tt_qty"] = 0
        missing = pd.Series(True, index=pool.index)

    n_miss = int(missing.sum())
    if n_miss:
        print(f"[attach] computing entries for {n_miss} new rows (cached {len(pool)-n_miss})", flush=True)
        sub = pool.loc[missing]
        ent = [tt._entry(r.ticker, r.side, r.tt_sig_ts.isoformat()) for r in sub.itertuples()]
        pool.loc[missing, "tt_entry_iso"] = [e[0] if e else "" for e in ent]
        pool.loc[missing, "tt_fill"] = [e[1] if e else np.nan for e in ent]
        pool.loc[missing, "tt_qty"] = [e[2] if e else 0 for e in ent]
        keep_cols = ["rk", "tt_entry_iso", "tt_fill", "tt_qty"]
        newc = pool[keep_cols].copy(); newc["slippage_bps"] = float(slippage_bps)
        allc = pd.concat([prev, newc], ignore_index=True) if prev is not None else newc
        allc = allc.drop_duplicates(subset=["rk", "slippage_bps"], keep="last")
        allc.to_parquet(cache, index=False)

    pool["tt_entry_ok"] = pool["tt_entry_iso"].astype(str).ne("") & pool["tt_entry_iso"].notna()
    out = pool[pool["tt_entry_ok"]].copy()
    out["notional"] = out["tt_fill"] * out["tt_qty"]
    out["tt_qty"] = out["tt_qty"].astype(int)
    return out.reset_index(drop=True)


def precompute_premom(setup: str, workdir: Path, df: pd.DataFrame, sl_ref: float = 0.90) -> pd.DataFrame:
    """1-min pre-entry momentum features per row at a fixed reference stop; persisted."""
    derived = Path(workdir) / "pools" / "derived"
    cache = derived / "premom_features.parquet"
    prev = pd.read_parquet(cache) if cache.exists() else None
    have = set(prev["rk"]) if prev is not None else set()
    todo = df[~df["rk"].isin(have)]
    print(f"[premom] rows total={len(df)} cached={len(df)-len(todo)} todo={len(todo)}", flush=True)
    recs = []
    for j, r in enumerate(todo.itertuples(), 1):
        feats, reason = tt._premom(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), sl_ref,
                                   r.tt_sig_ts.isoformat())
        rec = {"rk": r.rk}
        rec["pm_blocked"] = bool(reason)
        for k, v in (dict(feats) if not reason else {}).items():
            rec[f"pm_{k}"] = float(v)
        recs.append(rec)
        if j % 1000 == 0:
            print(f"[premom] {j}/{len(todo)}", flush=True)
    if recs:
        newdf = pd.DataFrame(recs)
        alldf = pd.concat([prev, newdf], ignore_index=True) if prev is not None else newdf
        alldf = alldf.drop_duplicates(subset=["rk"], keep="last")
        alldf.to_parquet(cache, index=False)
    else:
        alldf = prev if prev is not None else pd.DataFrame(columns=["rk", "pm_blocked"])
    return alldf


def precompute_exit_grid(setup: str, workdir: Path, df: pd.DataFrame,
                         sl_grid=None, tgt_grid=None) -> pd.DataFrame:
    """Per-row (net, outcome, exit_iso) for every (sl,tgt) combo; long format; persisted."""
    sl_grid = sl_grid or SL_GRID
    tgt_grid = tgt_grid or TGT_GRID
    derived = Path(workdir) / "pools" / "derived"
    cache = derived / "exit_grid.parquet"
    prev = pd.read_parquet(cache) if cache.exists() else None
    have = set(zip(prev["rk"], prev["sl"], prev["tgt"])) if prev is not None else set()
    combos = [(s, t) for s in sl_grid for t in tgt_grid]
    recs = []
    n_done = 0
    for (sl, tgt) in combos:
        todo = [r for r in df.itertuples() if (r.rk, sl, tgt) not in have]
        if not todo:
            continue
        for r in todo:
            full = tt._resolve_full(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill),
                                    int(r.tt_qty), float(sl), float(tgt))
            if full is None:
                recs.append({"rk": r.rk, "sl": sl, "tgt": tgt, "outcome": "UNRESOLVED",
                             "exit_iso": "", "net": np.nan})
                continue
            exit_iso, outcome, exit_px = full
            net = tt._trade_net(r.side, float(r.tt_fill), int(r.tt_qty), outcome, float(exit_px))
            recs.append({"rk": r.rk, "sl": sl, "tgt": tgt, "outcome": str(outcome).upper(),
                         "exit_iso": exit_iso, "net": float(net)})
        n_done += len(todo)
        print(f"[exitgrid] sl={sl} tgt={tgt}: +{len(todo)} rows (cum {n_done})", flush=True)
        if len(recs) > 150_000:   # periodic flush
            newdf = pd.DataFrame(recs)
            prev = pd.concat([prev, newdf], ignore_index=True) if prev is not None else newdf
            prev.to_parquet(cache, index=False)
            recs = []
    if recs:
        newdf = pd.DataFrame(recs)
        prev = pd.concat([prev, newdf], ignore_index=True) if prev is not None else newdf
    if prev is None:
        prev = pd.DataFrame(columns=["rk", "sl", "tgt", "outcome", "exit_iso", "net"])
    prev = prev.drop_duplicates(subset=["rk", "sl", "tgt"], keep="last")
    prev.to_parquet(cache, index=False)
    return prev


def precompute_mae_mfe(setup: str, workdir: Path, df: pd.DataFrame) -> pd.DataFrame:
    """MAE/MFE (%) from entry to 15:20 on 1-min path (+ first-30-min variants); persisted."""
    import avwap_5min_ID_v11_backtesting as v11
    derived = Path(workdir) / "pools" / "derived"
    cache = derived / "mae_mfe.parquet"
    prev = pd.read_parquet(cache) if cache.exists() else None
    have = set(prev["rk"]) if prev is not None else set()
    todo = df[~df["rk"].isin(have)]
    print(f"[maemfe] rows total={len(df)} cached={len(df)-len(todo)} todo={len(todo)}", flush=True)
    recs = []
    for j, r in enumerate(todo.itertuples(), 1):
        bars = v11._load_1m_with_open(r.ticker)
        rec = {"rk": r.rk, "mfe_pct": np.nan, "mae_pct": np.nan,
               "mfe30_pct": np.nan, "mae30_pct": np.nan, "t_mfe_min": np.nan, "t_mae_min": np.nan}
        if bars is not None and not bars.empty:
            ets = pd.Timestamp(r.tt_entry_iso)
            eod = ets.normalize() + pd.Timedelta(hours=15, minutes=20)
            w = bars[(bars.index > ets) & (bars.index <= eod)]
            if len(w):
                fill = float(r.tt_fill)
                hi = w["high"].to_numpy(float); lo = w["low"].to_numpy(float)
                if r.side == "SHORT":
                    fav = (fill - lo) / fill * 100.0; adv = (hi - fill) / fill * 100.0
                else:
                    fav = (hi - fill) / fill * 100.0; adv = (fill - lo) / fill * 100.0
                rec["mfe_pct"] = float(np.max(fav)); rec["mae_pct"] = float(np.max(adv))
                i_f = int(np.argmax(fav)); i_a = int(np.argmax(adv))
                rec["t_mfe_min"] = float((w.index[i_f] - ets).total_seconds() / 60.0)
                rec["t_mae_min"] = float((w.index[i_a] - ets).total_seconds() / 60.0)
                w30 = min(len(w), 30)
                rec["mfe30_pct"] = float(np.max(fav[:w30])); rec["mae30_pct"] = float(np.max(adv[:w30]))
        recs.append(rec)
        if j % 1000 == 0:
            print(f"[maemfe] {j}/{len(todo)}", flush=True)
    if recs:
        newdf = pd.DataFrame(recs)
        alldf = pd.concat([prev, newdf], ignore_index=True) if prev is not None else newdf
        alldf = alldf.drop_duplicates(subset=["rk"], keep="last")
        alldf.to_parquet(cache, index=False)
    else:
        alldf = prev
    return alldf


def windows(df: pd.DataFrame):
    """TRAIN/FIT/VAL/TEST session splits per the campaign spec."""
    train = df[(df["_day"] >= TRAIN_START) & (df["_day"] <= TRAIN_END)]
    test = df[(df["_day"] >= TEST_START) & (df["_day"] <= TEST_END)]
    tr_sessions = sorted(train["_day"].dt.strftime("%Y-%m-%d").unique())
    te_sessions = sorted(test["_day"].dt.strftime("%Y-%m-%d").unique())
    n_fit = max(1, int(round(FIT_FRAC * len(tr_sessions))))
    fit_s, val_s = tr_sessions[:n_fit], tr_sessions[n_fit:]
    return {
        "TRAIN": train.reset_index(drop=True),
        "TEST": test.reset_index(drop=True),
        "FIT": train[train["_day"].dt.strftime("%Y-%m-%d").isin(fit_s)].reset_index(drop=True),
        "VAL": train[train["_day"].dt.strftime("%Y-%m-%d").isin(val_s)].reset_index(drop=True),
        "sessions": {"TRAIN": tr_sessions, "TEST": te_sessions, "FIT": fit_s, "VAL": val_s},
    }


class ResearchEngine:
    """Fast config evaluation on the precomputed matrices (mirrors eval_family order)."""

    def __init__(self, setup: str, workdir: Path, slippage_bps: float = 15.0):
        self.setup = setup
        self.workdir = Path(workdir)
        self.df = load_attached(setup, workdir, slippage_bps)
        pm = precompute_premom(setup, workdir, self.df)
        self.df = self.df.merge(pm, on="rk", how="left")
        self.grid = precompute_exit_grid(setup, workdir, self.df)
        mm = precompute_mae_mfe(setup, workdir, self.df)
        self.df = self.df.merge(mm, on="rk", how="left")
        self.grid_idx = {}
        for (sl, tgt), g in self.grid.groupby(["sl", "tgt"]):
            self.grid_idx[(round(float(sl), 2), round(float(tgt), 2))] = g.set_index("rk")
        # freshness features from the PRE-dedupe pool: fire_seq (1 = first fire of the
        # ticker's day), fresh_age_bars (5-min bars since that first fire)
        first_ts = self.df.groupby(["ticker", "_day"])["tt_sig_ts"].transform("min")
        self.df["fresh_age_bars"] = ((self.df["tt_sig_ts"] - first_ts).dt.total_seconds() / 300.0)
        self.df["fire_seq"] = self.df.groupby(["ticker", "_day"])["tt_sig_ts"].rank(method="first")
        self.w = windows(self.df)
        self._dedupe_cache: dict = {}

    # -- pipeline stages (identical order to tt.eval_family) --
    def _premom_filter(self, rows: pd.DataFrame, terms) -> pd.DataFrame:
        if rows.empty or not terms:
            return rows
        keep = ~rows["pm_blocked"].fillna(True)
        for feat, op, thr in terms:
            col = f"pm_{feat}"
            x = pd.to_numeric(rows[col], errors="coerce") if col in rows.columns else pd.Series(np.nan, index=rows.index)
            if op == ">=":
                ok = x >= thr
            elif op == "<=":
                ok = x <= thr
            elif op == ">":
                ok = x > thr
            elif op == "<":
                ok = x < thr
            else:
                ok = pd.Series(False, index=rows.index)
            keep &= ok.fillna(False)          # missing -> block
        return rows[keep]

    def eval_cfg(self, cfg: dict, wdf: pd.DataFrame, want_detail: bool = False,
                 day_block: bool = False, wname: str | None = None) -> dict:
        sl = round(float(cfg["sl"]), 2); tgt = round(float(cfg["tgt"]), 2)
        gi = self.grid_idx.get((sl, tgt))
        if gi is None:
            raise KeyError(f"exit combo ({sl},{tgt}) not in precomputed grid")
        ck = None
        rows = None
        if wname is not None:
            ck = (wname,
                  json.dumps(cfg.get("guard"), sort_keys=True, default=str),
                  json.dumps(cfg.get("premom_terms") or [], default=str),
                  json.dumps(cfg.get("prefilter_terms") or [], default=str))
            hit = self._dedupe_cache.get(ck)
            if hit is not None:
                rows = wdf[wdf["rk"].isin(hit)]
        if rows is None:
            rows = wdf[wdf["setup"] == self.setup]
            # prefilter_terms: detection-layer redesign filters (mask syntax) applied
            # BEFORE dedupe — they change WHICH row survives the slot/day collapse,
            # exactly as a redesigned detector would.
            pf_terms = cfg.get("prefilter_terms") or []
            if pf_terms:
                rows = tt.apply_mask_terms(rows, pf_terms)
            rows = tt.apply_guards(rows, cfg.get("guard"))
            rows = self._premom_filter(rows, cfg.get("premom_terms") or [])
            if len(rows):
                rows = tt.dedupe_family(rows)
            if ck is not None:
                self._dedupe_cache[ck] = set(rows["rk"]) if len(rows) else set()
        rows = tt.apply_mask_terms(rows, cfg.get("mask_terms") or [])
        m = {"n": 0, "net_pf": 0.0, "net_pnl": 0.0, "win_rate": 0.0, "n_days": 0, "n_syms": 0,
             "trades_per_day": 0.0, "sl_cnt": 0, "tgt_cnt": 0, "eod_cnt": 0, "target_rate": 0.0,
             "max_dd": 0.0, "trade_dom_gross": None, "day_dom": None, "sym_dom": None,
             "day_block_p": None, "top_day": None, "top_sym": None, "detail": None}
        if rows.empty:
            return m
        g = gi.reindex(rows["rk"])
        rows = rows.assign(_net=g["net"].to_numpy(), _outcome=g["outcome"].to_numpy(),
                           _exit_iso=g["exit_iso"].to_numpy())
        rows = rows[np.isfinite(rows["_net"].to_numpy(float))]
        rows = rows.sort_values("tt_entry_iso")
        if rows.empty:
            return m
        # portfolio overlay (position cap + daily loss), entry-time ordered
        cap = int(cfg.get("max_positions") or 20)
        dloss = abs(float(cfg.get("daily_loss_rs") or 0.0))
        if cap > 0 or dloss > 0:
            rows = rows.sort_values("tt_entry_iso").copy()
            open_pos = []; realized = {}; take = []
            for idx, e_iso, x_iso, netv in zip(rows.index, rows["tt_entry_iso"],
                                               rows["_exit_iso"], rows["_net"]):
                ets = pd.Timestamp(e_iso); day = ets.normalize()
                still = []
                for (x, n, d) in open_pos:
                    if x <= ets:
                        realized[d] = realized.get(d, 0.0) + n
                    else:
                        still.append((x, n, d))
                open_pos = still
                if dloss > 0 and realized.get(day, 0.0) <= -dloss:
                    continue
                if cap > 0 and len(open_pos) >= cap:
                    continue
                take.append(idx)
                open_pos.append((pd.Timestamp(x_iso), float(netv), day))
            rows = rows.loc[take]
        if rows.empty:
            return m
        net = rows["_net"].to_numpy(float)
        oc = rows["_outcome"].astype(str)
        wins = net[net > 0]; losses = net[net < 0]
        gp = float(wins.sum()); gl = float(-losses.sum())
        eq = net.cumsum(); dd = eq - np.maximum.accumulate(eq)
        day = rows["_day"]
        day_sum = pd.Series(net, index=day.to_numpy()).groupby(level=0).sum()
        sym_sum = pd.Series(net, index=rows["ticker"].to_numpy()).groupby(level=0).sum()
        tot = float(net.sum())
        m.update({
            "n": int(len(net)),
            "net_pf": round(gp / gl, 3) if gl > 0 else (999.0 if gp > 0 else 0.0),
            "net_pnl": round(tot, 0),
            "win_rate": round(float((net > 0).mean()) * 100, 1),
            "n_days": int(day.nunique()), "n_syms": int(rows["ticker"].nunique()),
            "sl_cnt": int((oc == "SL").sum()), "tgt_cnt": int((oc == "TARGET").sum()),
            "eod_cnt": int((oc == "EOD").sum()),
            "target_rate": round(float((oc == "TARGET").mean()) * 100, 1),
            "max_dd": round(float(dd.min()), 0) if len(dd) else 0.0,
            "trade_dom_gross": round(float(net.max()) / gp, 3) if gp > 0 else 9.99,
            "day_dom": (round(float(day_sum.max()) / tot, 3) if tot > 0 else 9.99),
            "sym_dom": (round(float(sym_sum.max()) / tot, 3) if tot > 0 else 9.99),
            "top_day": f"{pd.Timestamp(day_sum.idxmax()).date()}: Rs{day_sum.max():,.0f}",
            "top_sym": f"{sym_sum.idxmax()}: Rs{sym_sum.max():,.0f}",
        })
        m["trades_per_day"] = round(m["n"] / max(1, m["n_days"]), 2)
        if day_block:
            try:
                m["day_block_p"] = round(float(tt._day_block_p(rows, net)), 4)
            except Exception:
                m["day_block_p"] = None
        if want_detail:
            m["detail"] = rows[["rk", "ticker", "side", "_day", "tt_sig_ts", "tt_entry_iso",
                                "tt_fill", "tt_qty", "_net", "_outcome", "_exit_iso"]].copy()
        return m

    def windows_dict(self):
        return self.w


# ---------------------------------------------------------------------------
# Campaign loop helpers
# ---------------------------------------------------------------------------
PF_LO, PF_HI = 1.30, 1.80          # TRAIN acceptance band (campaign spec)
TEST_PF_MIN = 1.40
MIN_TRADES_TRAIN = 35
MIN_TRADES_TEST = 10
DOM_TRADE, DOM_DAY, DOM_SYM = 0.35, 0.40, 0.40
GAP_LAMBDA = 0.80
FV_FLOOR = 8


def band_reward(pf: float) -> float:
    """Tent peaking at PF_HI; linear climb below, 1.5x penalty above (anti-overfit)."""
    pf = min(float(pf), 10.0) if np.isfinite(pf) else 10.0
    if pf <= PF_HI:
        return pf
    return PF_HI - 1.5 * (pf - PF_HI)


def score_fit_val(mF: dict, mV: dict) -> float:
    nf, nv = mF["n"], mV["n"]
    if nf < FV_FLOOR or nv < FV_FLOOR:
        return -5.0 + min(nf, nv) / max(1, FV_FLOOR)
    pf_f = min(mF["net_pf"], 10.0); pf_v = min(mV["net_pf"], 10.0)
    sc = band_reward(min(pf_f, pf_v)) - GAP_LAMBDA * abs(pf_f - pf_v)
    if min(pf_f, pf_v) >= PF_LO:
        sc += 0.003 * min(min(nf, nv), 40)
    return sc


def dom_ok(m: dict) -> bool:
    return (m["trade_dom_gross"] is not None and m["trade_dom_gross"] <= DOM_TRADE
            and m["day_dom"] is not None and m["day_dom"] <= DOM_DAY
            and m["sym_dom"] is not None and m["sym_dom"] <= DOM_SYM)


def cfg_str(cfg: dict) -> str:
    parts = [f"SL{cfg['sl']}/T{cfg['tgt']}"]
    for key, tag in (("prefilter_terms", "pre"), ("mask_terms", "mask"), ("premom_terms", "pm")):
        ts = cfg.get(key) or []
        if ts:
            parts.append(tag + "[" + ";".join(f"{a}{o}{b}" for a, o, b in ts) + "]")
    if cfg.get("guard"):
        parts.append("g" + json.dumps(cfg["guard"], sort_keys=True))
    if (cfg.get("max_positions") or 20) != 20:
        parts.append(f"maxpos{cfg['max_positions']}")
    if cfg.get("daily_loss_rs"):
        parts.append(f"dloss{cfg['daily_loss_rs']}")
    return " ".join(parts)


class IterLog:
    """Appends every scored iteration to iteration_log.csv (one row per logical change)."""

    def __init__(self, work: Path):
        self.path = Path(work) / "iteration_log.csv"
        self.rows = []
        self.i = 0
        if self.path.exists():
            old = pd.read_csv(self.path)
            self.i = int(old["iter"].max()) if len(old) else 0
            self.rows = old.to_dict("records")

    def log(self, stage: str, group: str, change: str, cfg: dict,
            mF: dict | None, mV: dict | None, mTR: dict | None = None, mTE: dict | None = None,
            decision: str = "", note: str = "") -> int:
        self.i += 1
        r = {"iter": self.i, "stage": stage, "group": group, "change": change,
             "cfg": cfg_str(cfg),
             "fit_n": mF["n"] if mF else None, "fit_pf": mF["net_pf"] if mF else None,
             "val_n": mV["n"] if mV else None, "val_pf": mV["net_pf"] if mV else None,
             "train_n": mTR["n"] if mTR else None, "train_pf": mTR["net_pf"] if mTR else None,
             "train_net": mTR["net_pnl"] if mTR else None,
             "test_n": mTE["n"] if mTE else None, "test_pf": mTE["net_pf"] if mTE else None,
             "test_net": mTE["net_pnl"] if mTE else None,
             "score": round(score_fit_val(mF, mV), 4) if (mF and mV) else None,
             "decision": decision, "note": note,
             "cfg_json": json.dumps(cfg, default=str)}
        self.rows.append(r)
        return self.i

    def flush(self):
        pd.DataFrame(self.rows).to_csv(self.path, index=False)


def mline(m: dict) -> str:
    return (f"n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} win%={m['win_rate']} "
            f"SL/TGT/EOD={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']} tgt%={m['target_rate']} "
            f"tpd={m['trades_per_day']} days={m['n_days']} "
            f"domT/D/S={m['trade_dom_gross']}/{m['day_dom']}/{m['sym_dom']} dbp={m['day_block_p']}")
