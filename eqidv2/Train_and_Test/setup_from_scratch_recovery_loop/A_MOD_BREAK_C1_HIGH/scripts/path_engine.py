r"""path_engine.py — 1-minute path evaluator for A_MOD_BREAK_C1_HIGH redesigns.

Entry models (all fills carry +15bps adverse slippage; qty = int(100k / raw_ref_px)):
  next_open                     mirror of tt: first 1-min open after the signal
  confirm(K)                    stop-buy: first bar within K min whose high exceeds the signal-bar
                                high; fill = max(bar open, level) (gap-aware)
  retest(depth_atr, K, arm)     resting limit at signal_close - depth_atr*ATR placed after an
                                arming delay; fills when a bar's low trades <= level

Exit models (pessimistic same-bar SL priority; EOD 15:20 close; exit price slipped -15bps):
  bracket(sl_pct, tgt_pct)
  + time_cap_min (close at market after T minutes if no hit)
  + breakeven_at (SL moves to entry once price is +x% in favour)
  + trail_pct (stop ratchets to cummax(high)*(1-t/100))

Costs: statutory model via walkforward_gate.net_pnl_vectorized (same as setup_train_test).
Book: chronological; max_open concurrent cap; optional top_n per 5-min slot (rank col desc);
      optional max_trades_day; optional daily_loss_rs stop.
Windows: TRAIN 2026-03-01..05-30 (FIT=first 60% sessions, VAL=rest), TEST 2026-06-01..07-02.
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
TT_DIR = HERE.parents[3]
REPO = TT_DIR.parent
for p in (REPO, TT_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import walkforward_gate as wfg  # noqa: E402
from nse_intraday_costs import CostConfig  # noqa: E402

FNAME = "historical_all_available_pre_dedupe_live_candidates.csv"
SLIP = 15.0 / 1e4
NOTIONAL = 100_000.0
CFG = CostConfig()

TRAIN_START, TRAIN_END = "2026-03-01", "2026-05-30"
TEST_START, TEST_END = "2026-06-01", "2026-07-02"
FIT_FRAC = 0.60


@dataclass
class Variant:
    name: str
    entry: str = "next_open"          # next_open | confirm | retest
    confirm_k: int = 30               # window (minutes) for confirm/retest fills
    retest_depth_atr: float = 0.25
    retest_arm_min: int = 1
    sl_pct: float = 1.0
    tgt_pct: float = 1.5
    time_cap_min: int = 0             # 0 = off (EOD only)
    breakeven_at: float = 0.0         # 0 = off
    trail_pct: float = 0.0            # 0 = off
    mask: list = field(default_factory=list)     # [(col, op, val)]
    first_per_day: bool = True
    require_20bh: bool = True
    min_slot: int = 0                 # minutes since midnight, 0=off
    max_slot: int = 0                 # 0=off
    top_n: int = 0                    # per 5-min slot by rank_col
    rank_col: str = "vwap_dist_atr"
    max_open: int = 20
    max_trades_day: int = 0
    daily_loss_rs: float = 0.0
    max_losses_day: int = 0           # stop taking entries after N losing closes today (0=off)


class PathEngine:
    def __init__(self):
        pool = pd.read_csv(WORK / "pools" / "pool_base" / FNAME, low_memory=False)
        summ = pd.read_csv(WORK / "paths" / "summary.csv")
        paths = pd.read_parquet(WORK / "paths" / "paths.parquet")
        pool.index.name = "sid"
        df = summ.merge(pool.reset_index(), left_on="sid", right_on="sid", how="left",
                        suffixes=("", "_pool"))
        ts = pd.to_datetime(df["sig_ts"], utc=True).dt.tz_convert("Asia/Kolkata")
        df["_day"] = ts.dt.strftime("%Y-%m-%d")
        df["_minute"] = ts.dt.hour * 60 + ts.dt.minute
        df["_slot"] = df["_minute"] // 5 * 5
        df["atr_abs"] = pd.to_numeric(df["atr_pct"], errors="coerce") * pd.to_numeric(df["signal_close"], errors="coerce")
        self.df = df
        # per-sid path arrays
        self.P = {}
        for sid, g in paths.groupby("sid", sort=False):
            a = g.sort_values("k")
            self.P[int(sid)] = (a["min_from_sig"].values.astype(np.int32),
                                a["o"].values.astype(np.float64), a["h"].values.astype(np.float64),
                                a["l"].values.astype(np.float64), a["c"].values.astype(np.float64))
        days = sorted(df["_day"].unique())
        tr = [d for d in days if TRAIN_START <= d <= TRAIN_END]
        te = [d for d in days if TEST_START <= d <= TEST_END]
        nf = max(1, int(round(FIT_FRAC * len(tr))))
        self.windows = {"FIT": set(tr[:nf]), "VAL": set(tr[nf:]), "TRAIN": set(tr), "TEST": set(te)}

    # ---- entry/exit resolution for one signal under a variant ----
    def _resolve(self, sid: int, row, v: Variant):
        mins, o, h, l, c = self.P[sid]
        n = len(mins)
        if n < 2:
            return None
        sig_high = float(row.get("signal_high", np.nan))
        atr = float(row.get("atr_abs", np.nan))
        # signal_time_ist is the 5-min bar CLOSE label; first tradeable 1-min bar is
        # mins >= 1 (verified: tt._entry fills at sig_ts + 1 min, max delay 3).
        tradeable = np.nonzero((mins >= 1) & (mins <= 3))[0]
        if not len(tradeable):
            return None
        k1 = int(tradeable[0])
        # --- entry ---
        if v.entry == "next_open":
            ek, ref = k1, float(o[k1])
            fill = ref * (1 + SLIP)
        elif v.entry == "confirm":
            if not np.isfinite(sig_high):
                return None
            idx = np.nonzero((mins >= 1) & (mins <= v.confirm_k) & (h > sig_high))[0]
            if not len(idx):
                return None
            ek = int(idx[0])
            ref = max(float(o[ek]), sig_high)
            fill = ref * (1 + SLIP)
        elif v.entry == "retest":
            if not (np.isfinite(atr) and atr > 0):
                return None
            level = float(row.get("signal_close", np.nan)) - v.retest_depth_atr * atr
            if not np.isfinite(level) or level <= 0:
                return None
            elig = np.nonzero((mins >= max(1, v.retest_arm_min)) & (mins <= v.confirm_k) & (l <= level))[0]
            if not len(elig):
                return None
            ek, ref = int(elig[0]), level
            fill = ref * (1 + SLIP)
        else:
            raise ValueError(v.entry)
        qty = max(1, int(NOTIONAL / ref))

        # --- exit walk from ek ---
        sl = fill * (1 - v.sl_pct / 100.0)
        tgt = fill * (1 + v.tgt_pct / 100.0)
        be_armed = v.breakeven_at > 0
        trail = v.trail_pct > 0
        cap_k = None
        if v.time_cap_min:
            cap = mins[ek] + v.time_cap_min
            later = np.nonzero(mins >= cap)[0]
            cap_k = int(later[0]) if len(later) else n - 1
        hi_run = fill
        stop = sl
        for i in range(ek, n):
            if trail or be_armed:
                hi_run = max(hi_run, h[i])
                if be_armed and hi_run >= fill * (1 + v.breakeven_at / 100.0):
                    stop = max(stop, fill)
                if trail:
                    stop = max(stop, hi_run * (1 - v.trail_pct / 100.0))
            if l[i] <= stop:
                out, xp, xk = ("SL" if stop <= sl + 1e-9 else "TRAIL"), stop, i
                break
            if h[i] >= tgt:
                out, xp, xk = "TARGET", tgt, i
                break
            if cap_k is not None and i >= cap_k:
                out, xp, xk = "TIME", float(c[i]), i
                break
        else:
            out, xp, xk = "EOD", float(c[-1]), n - 1
        exit_px = xp * (1 - SLIP)
        return ek, fill, qty, out, exit_px, int(mins[xk]), ref

    def evaluate(self, v: Variant, window: str, detail: bool = False):
        days = self.windows[window]
        d = self.df[self.df["_day"].isin(days)]
        if v.require_20bh and "is_20bar_high" in d.columns:
            d = d[pd.to_numeric(d["is_20bar_high"], errors="coerce") >= 1.0]
        if v.first_per_day:
            d = d.sort_values("sig_ts").groupby(["ticker", "_day"], as_index=False).head(1)
        if v.min_slot:
            d = d[d["_minute"] >= v.min_slot]
        if v.max_slot:
            d = d[d["_minute"] <= v.max_slot]
        for col, op, val in v.mask:
            x = d[col]
            if isinstance(val, str):
                d = d[(x.astype(str) == val) if op == "==" else (x.astype(str) != val)]
            else:
                xn = pd.to_numeric(x, errors="coerce")
                d = d[xn >= val] if op == ">=" else d[xn <= val]
        if v.top_n and v.rank_col in d.columns:
            d = (d.sort_values(v.rank_col, ascending=False)
                 .groupby(["_day", "_slot"], sort=False).head(v.top_n))
        d = d.sort_values("sig_ts")

        trades = []
        open_until: list = []
        day_state = {}
        for _, row in d.iterrows():
            sid = int(row["sid"])
            if sid not in self.P:
                continue
            day = row["_day"]
            st = day_state.setdefault(day, {"n": 0, "closed": []})  # closed: (exit_abs_min, net)
            if v.max_trades_day and st["n"] >= v.max_trades_day:
                continue
            r = self._resolve(sid, row, v)
            if r is None:
                continue
            ek, fill, qty, out, exit_px, exit_min, ref = r
            ent_abs = row["_minute"] + (0 if v.entry == "next_open" else ek)
            # realized-only day stops: count ONLY trades already closed at this entry time
            if v.daily_loss_rs or v.max_losses_day:
                realized = [x for x in st["closed"] if x[0] <= ent_abs]
                if v.daily_loss_rs and sum(x[1] for x in realized) <= -abs(v.daily_loss_rs):
                    continue
                if v.max_losses_day and sum(1 for x in realized if x[1] < 0) >= v.max_losses_day:
                    continue
            open_until = [(dd, xm) for dd, xm in open_until if not (dd == day and xm <= ent_abs)]
            if sum(1 for dd, _ in open_until if dd == day) >= v.max_open:
                continue
            net = float(wfg.net_pnl_vectorized(np.array([fill]), np.array([exit_px]),
                                               np.array([qty]), np.array(["LONG"]), CFG)[0])
            open_until.append((day, row["_minute"] + exit_min))
            st["n"] += 1
            st["closed"].append((row["_minute"] + exit_min, net))
            trades.append({"sid": sid, "ticker": row["ticker"], "day": day,
                           "entry_min_from_sig": ek, "outcome": out, "net": net,
                           "fill": fill, "exit_px": exit_px})
        t = pd.DataFrame(trades)
        if t.empty:
            return {"n": 0, "pf": np.nan, "net": 0.0, "win": np.nan, "detail": t}
        gp = t.net[t.net > 0].sum(); gl = -t.net[t.net < 0].sum()
        daysum = t.groupby("day").net.sum()
        symsum = t.groupby("ticker").net.sum()
        rng = np.random.default_rng(11)
        obs = t.net.sum()
        flips = np.array([daysum.values * rng.choice([-1, 1], len(daysum)) for _ in range(2000)])
        day_p = float((flips.sum(axis=1) >= obs).mean())
        m = {"n": len(t), "pf": round(gp / gl, 3) if gl > 0 else np.inf,
             "net": round(obs), "win": round(100 * (t.net > 0).mean(), 1),
             "avg_win": round(t.net[t.net > 0].mean()) if (t.net > 0).any() else 0,
             "avg_loss": round(t.net[t.net < 0].mean()) if (t.net < 0).any() else 0,
             "days": t.day.nunique(), "tr_day": round(len(t) / max(1, t.day.nunique()), 2),
             "sl": int((t.outcome == "SL").sum()), "tgt": int((t.outcome == "TARGET").sum()),
             "eod": int((t.outcome == "EOD").sum()), "time": int((t.outcome == "TIME").sum()),
             "trail": int((t.outcome == "TRAIL").sum()),
             "dom_trade": round(float(t.net.max()) / gp, 3) if gp > 0 else np.nan,
             "dom_day": round(float(daysum.max()) / obs, 3) if obs > 0 else np.nan,
             "dom_sym": round(float(symsum.max()) / obs, 3) if obs > 0 else np.nan,
             "day_p": round(day_p, 4)}
        if detail:
            m["detail"] = t
        return m


if __name__ == "__main__":
    eng = PathEngine()
    v = Variant(name="baseline_mirror", entry="next_open", sl_pct=0.70, tgt_pct=1.00,
                first_per_day=False, require_20bh=False)
    for w in ("FIT", "VAL", "TRAIN", "TEST"):
        m = eng.evaluate(v, w)
        print(w, {k: m[k] for k in ("n", "pf", "net", "win", "sl", "tgt", "eod") if k in m})
