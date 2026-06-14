"""
portfolio_backtest_v11.py — consolidated portfolio backtest of the 9 ACTIVE setups in
final_setup_conf.py. For each setup: load its candidates from the right pool (clean /
raw pre-gate / tier123 overlay, per provenance.evaluated_on), apply its mask_terms +
pre_momentum_terms + entry_guards, resolve entry->EOD on 1-min data at its approved exit,
NET of NSE cost. Combine into one equal-notional (Rs100k/trade) book and report
PF / DD / daily-Sharpe / win% / monthly + the LONG-vs-SHORT split + per-setup contribution.

Reuses the G/L/T path caches (premom feats already computed); builds A/B/D/E from the clean pool.
Run:  py -3.12 portfolio_backtest_v11.py
"""
from __future__ import annotations
from pathlib import Path
import pickle
import numpy as np
import pandas as pd
import setup_train_test as stt
import avwap_5min_ID_v11_backtesting as v11
import walkforward_gate as wfg
from nse_intraday_costs import CostConfig
import final_setup_conf as fc

CLEAN = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_cleanpool")
PROP = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_traintest_pool\proposals")
CFG = CostConfig()
CACHES = {
    "G": Path(r"C:\TradingData\eqidv2\v11_G_paths_cache.pkl"),
    "L": Path(r"C:\TradingData\eqidv2\v11_L_paths_cache.pkl"),
    "T": Path(r"C:\TradingData\eqidv2\v11_T_paths_cache.pkl"),
}
CACHE_SETUP = {  # setup -> (cache key, key-in-cache)
    "G_HIGHER_HIGH_BREAK": ("G", "G_HIGHER_HIGH_BREAK"),
    "L_DOUBLE_BOTTOM_VWAP": ("L", "L_DOUBLE_BOTTOM_VWAP"),
    "L_PRESSURE_BURST_VWAP": ("L", "L_PRESSURE_BURST_VWAP"),
    "T_TREND_DAY_EMA_STAIR_SHORT": ("T", "T_TREND_DAY_EMA_STAIR_SHORT"),
}
RAW_FILE = "historical_all_available_raw_candidates.csv"
POOL_FEATS = ["vol_ratio", "rs_pct", "market_ret_pct", "vwap_dist_atr", "close_loc",
              "quality_score", "ranker_score", "atr_pct", "body_pct"]


def _load_caches():
    out = {}
    for k, p in CACHES.items():
        if p.exists():
            with open(p, "rb") as fh:
                out[k] = pickle.load(fh)
    return out


_CACHE = _load_caches()


def _clean_pool():
    stt.POOL_DIRS = [CLEAN]
    return stt.load_pool().rename(columns={"tt_sig_ts": "sig"})


_CLEANDF = None


_BUILT_CACHE = Path(r"C:\TradingData\eqidv2\v11_portfolio_built_records.pkl")
_BUILT = pickle.load(open(_BUILT_CACHE, "rb")) if _BUILT_CACHE.exists() else {}


def _build_records(setup, cfg, side):
    """Build path records (features + prices + premom if needed) for setups NOT in a cache."""
    global _CLEANDF
    if setup in _BUILT:
        return _BUILT[setup]
    need_pm = bool(cfg.get("pre_momentum_terms"))
    src = cfg.get("provenance", {}).get("evaluated_on", "")
    if src == "RAW_PRE_GATE_POOL":
        import glob, os
        frames = []
        for f in glob.glob(os.path.join(str(CLEAN), "chunk_*", RAW_FILE)):
            try:
                d = pd.read_csv(f, low_memory=False)
                frames.append(d[d["setup"].astype(str) == setup])
            except Exception:
                pass
        e = pd.concat(frames, ignore_index=True)
        e["sig"] = pd.to_datetime(e["signal_time_ist"], errors="coerce")
        e = e.dropna(subset=["sig"]).drop_duplicates(subset=["ticker", "setup", "signal_time_ist"])
    else:
        if _CLEANDF is None:
            _CLEANDF = _clean_pool()
        e = _CLEANDF[_CLEANDF["setup"].astype(str) == setup].copy()
    recs = []
    for r in e.itertuples():
        bars1 = v11._load_1m_with_open(r.ticker)
        if bars1 is None or bars1.empty:
            continue
        ent = v11._first_1m_entry(bars1, r.sig, max_delay_minutes=3)
        if ent is None:
            continue
        ets, raw_open = ent
        if raw_open <= 0:
            continue
        eod = ets.normalize() + pd.Timedelta(hours=15, minutes=20)
        sub = bars1[(bars1.index >= ets) & (bars1.index <= eod)]
        if sub.empty:
            continue
        d = r.sig
        rec = {"date": d.strftime("%Y-%m-%d"), "signal_minute": float(d.hour * 60 + d.minute),
               "regime": str(getattr(r, "regime", "")), "raw_open": float(raw_open), "side": side,
               "ticker": str(r.ticker), "highs": sub["high"].to_numpy(float),
               "lows": sub["low"].to_numpy(float), "closes": sub["close"].to_numpy(float)}
        for f in POOL_FEATS:
            v = getattr(r, f, np.nan)
            rec[f] = float(v) if pd.notna(v) else np.nan
        if need_pm:
            fill = raw_open * (0.9995 if side == "SHORT" else 1.0005)
            stop = fill * (1 + 0.9 / 100) if side == "SHORT" else fill * (1 - 0.9 / 100)
            feats, reason = v11._pre_entry_momentum_features_v11(r.ticker, side, fill, stop, ets, d)
            rec["_pm"] = feats or {}
        recs.append(rec)
    _BUILT[setup] = recs
    with open(_BUILT_CACHE, "wb") as fh:
        pickle.dump(_BUILT, fh)
    return recs


def _passes(rec, terms, premom=False):
    for f, op, v in terms:
        x = (rec.get("_pm", {}).get(f, np.nan) if premom else rec.get(f, np.nan))
        if isinstance(v, str):
            ok = (str(x) == v) if op == "==" else (str(x) != v)
        else:
            try:
                x = float(x)
            except Exception:
                return False
            if not np.isfinite(x):
                return False
            ok = (x >= v) if op == ">=" else (x <= v) if op == "<=" else (x == v) if op == "==" else (x != v)
        if not ok:
            return False
    return True


def _sim(rec, sl, tgt):
    side = rec["side"]; e = rec["raw_open"] * (0.9995 if side == "SHORT" else 1.0005)
    qty = max(1, int(100000.0 / rec["raw_open"])); h, l, c = rec["highs"], rec["lows"], rec["closes"]
    if side == "SHORT":
        slp, tgp = e * (1 + sl / 100), e * (1 - tgt / 100); slh, tgh = h >= slp, l <= tgp
    else:
        slp, tgp = e * (1 - sl / 100), e * (1 + tgt / 100); slh, tgh = l <= slp, h >= tgp
    fsl = int(np.argmax(slh)) if slh.any() else 10 ** 9
    ftg = int(np.argmax(tgh)) if tgh.any() else 10 ** 9
    xp = slp if (slh.any() and fsl <= ftg) else (tgp if tgh.any() else c[-1])
    return float(wfg.net_pnl_vectorized(np.array([e]), np.array([xp]), np.array([qty]), np.array([side]), CFG)[0])


def _pf(n):
    n = np.asarray(n, float); a, b = n[n > 0].sum(), -n[n < 0].sum()
    return float(a / b) if b > 0 else (float('inf') if a > 0 else 0.0)


def setup_trades(setup, cfg):
    side = cfg["side"]
    mask_terms = [(t[0], t[1], t[2]) for t in cfg.get("mask_terms", [])]
    pm_terms = [(t[0], t[1], t[2]) for t in cfg.get("pre_momentum_terms", [])]
    guard = cfg.get("entry_guards", {})
    min_slot = None
    if guard.get("min_slot"):
        hh, mm = guard["min_slot"].split(":"); min_slot = int(hh) * 60 + int(mm)
    sl, tgt = cfg["exit"]["sl_pct"], cfg["exit"]["tgt_pct"]

    if setup in CACHE_SETUP:
        ck, key = CACHE_SETUP[setup]
        c = _CACHE.get(ck)
        recs = (c.get(key, []) if isinstance(c, dict) else (c or []))  # G cache = list; L/T = dict
        # G/L caches don't store 'side' (hardcoded LONG); inject from cfg. Wrap feats into _pm.
        recs = [{**r, "side": side, "_pm": r} for r in recs]
    else:
        recs = _build_records(setup, cfg, side)

    rows = []
    for r in recs:
        if min_slot is not None and r.get("signal_minute", 0) < min_slot:
            continue
        if mask_terms and not _passes(r, mask_terms, premom=False):
            continue
        if pm_terms and not _passes(r, pm_terms, premom=True):
            continue
        rows.append({"date": r["date"], "setup": setup, "side": side, "net": _sim(r, sl, tgt)})
    return pd.DataFrame(rows)


def _metrics(net, dates):
    net = np.asarray(net, float)
    daily = pd.Series(net, index=pd.to_datetime(dates)).groupby(level=0).sum().sort_index()
    eq = daily.cumsum()
    dd = (eq - eq.cummax()).min()
    sharpe = (daily.mean() / daily.std() * np.sqrt(252)) if daily.std() > 0 else float('nan')
    return {"trades": len(net), "days": daily.size, "net": net.sum(), "gross_win": net[net > 0].sum(),
            "pf": _pf(net), "win": (net > 0).mean() * 100, "avg": net.mean(),
            "maxdd": dd, "sharpe": sharpe}


def main():
    PROP.mkdir(parents=True, exist_ok=True)
    active = fc.FINAL_SETUP_CONF
    longs = [k for k, v in active.items() if v["side"] == "LONG"]
    shorts = [k for k, v in active.items() if v["side"] == "SHORT"]
    print(f"ACTIVE BOOK: {len(active)} setups  ->  {len(longs)} LONG, {len(shorts)} SHORT")
    print(f"  LONG : {longs}")
    print(f"  SHORT: {shorts}\n")

    all_tr = []
    print(f"{'setup':<34} {'side':<6} {'n':>4} {'days':>4} {'net Rs':>11} {'PF':>5} {'win%':>5} {'avg':>6}")
    print("-" * 84)
    for setup, cfg in active.items():
        tr = setup_trades(setup, cfg)
        if not len(tr):
            print(f"{setup:<34} {cfg['side']:<6}  (no trades)")
            continue
        all_tr.append(tr)
        m = _metrics(tr["net"].to_numpy(), tr["date"].to_numpy())
        print(f"{setup:<34} {cfg['side']:<6} {m['trades']:>4} {m['days']:>4} {m['net']:>11,.0f} "
              f"{m['pf']:>5.2f} {m['win']:>5.0f} {m['avg']:>6.0f}")
    book = pd.concat(all_tr, ignore_index=True)
    book.to_csv(PROP / "portfolio_v11_trades.csv", index=False)

    print("\n" + "=" * 84)
    for label, sub in [("LONG book", book[book.side == "LONG"]), ("SHORT book", book[book.side == "SHORT"]),
                       ("FULL PORTFOLIO", book)]:
        m = _metrics(sub["net"].to_numpy(), sub["date"].to_numpy())
        nset = sub["setup"].nunique()
        print(f"{label:<16} setups={nset} trades={m['trades']:>4} days={m['days']:>3} | net Rs{m['net']:>10,.0f} "
              f"| PF {m['pf']:.2f} | win {m['win']:.0f}% | maxDD Rs{m['maxdd']:>9,.0f} | Sharpe {m['sharpe']:.2f}")

    print("\n=== monthly net (full portfolio, Rs) ===")
    bm = book.copy(); bm["m"] = pd.to_datetime(bm["date"]).dt.strftime("%Y-%m")
    mon = bm.groupby("m")["net"].agg(["sum", "count"])
    for mth, row in mon.iterrows():
        bar = "#" * max(0, int(row["sum"] / 2000))
        print(f"  {mth}: Rs{row['sum']:>9,.0f}  (n={int(row['count']):>3})  {bar}")
    print(f"\n  months positive: {(mon['sum'] > 0).sum()}/{len(mon)}")
    print(f"\n[portfolio] wrote portfolio_v11_trades.csv -> {PROP}")
    print("NOTE: equal-notional Rs100k/trade; setups span heterogeneous pools/windows "
          "(clean / raw pre-gate / tier123 overlay) — research view, reconcile gating before live.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
