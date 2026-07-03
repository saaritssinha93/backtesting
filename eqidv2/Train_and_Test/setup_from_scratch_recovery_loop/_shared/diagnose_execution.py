r"""diagnose_execution.py — FROM-SCRATCH RECOVERY stage 0: execution-layer
diagnostics for the 5 B-family setups. RESEARCH-ONLY; writes only inside
Train_and_Test/setup_from_scratch_recovery_loop/_shared/.

Three rounds of signal-selection search (masks / pre-momentum / guards over ~50
features) failed the PF-band goal on every setup. Before any further tuning this
script answers, with data, WHY the family loses:

  D1 COST ANATOMY   — broad detection book (family dedupe, baseline exits) at
                      0 bps price-only gross, then statutory net @5 and @15 bps/leg.
                      Distinguishes "no gross edge" from "edge eaten by costs".
  D2 MFE/MAE WALK   — per-trade max favourable / adverse excursion at 15/30/60/120
                      min from entry (1-min bars). Shows whether break-even stops,
                      time-stops or tighter/looser exits are supported by the
                      price paths, independent of any mask.
  D3 RETEST FILLS   — after the 5-min signal, how deep does price pull back toward
                      the signal bar (in ATR units) within 15/30 min? -> feasibility
                      + fill-rate of limit/retest entries instead of chasing the
                      next 1-min open.
  D4 FADE TEST      — flip the side of every detection (same dedupe, small exit
                      grid): is the OTHER side of these signals tradeable?

Run from repo root (post-market):
  py -3.12 Train_and_Test\setup_from_scratch_recovery_loop\_shared\diagnose_execution.py
"""
from __future__ import annotations

import json
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent            # _shared/
RECOVERY = _HERE.parent                            # setup_from_scratch_recovery_loop/
TT_DIR = RECOVERY.parent                           # Train_and_Test/
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for _p in (str(REPO), str(TT_DIR), str(ENGINE_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt      # noqa: E402
import pf_band_fitval_loop as eng  # noqa: E402
import avwap_5min_ID_v11_backtesting as v11  # noqa: E402

SETUPS = {
    "B_AVWAP_RECLAIM_REVERSAL": ("LONG", 0.70, 1.50),
    "B_HUGE_C1_CLOSE_RECLAIM_BREAK": ("LONG", 1.00, 1.50),
    "B_HUGE_RED_FAILED_BOUNCE": ("SHORT", 0.90, 1.25),
    "B_HUGE_FAILED_BOUNCE": ("SHORT", 1.20, 1.50),
    "B_HUGE_GREEN_PULLBACK_HOLD_THEN_BREAK": ("LONG", 1.50, 2.00),
}
TRAIN_W = ("2026-03-01", "2026-05-30")
TEST_W = ("2026-06-01", "2026-07-02")
MFE_SAMPLE = 700
HORIZONS = (15, 30, 60, 120)
FADE_EXITS = [(0.7, 1.0), (0.9, 1.25), (1.2, 1.5)]


def _pf(net):
    net = np.asarray(net, dtype=float)
    net = net[np.isfinite(net)]
    gp = net[net > 0].sum()
    gl = -net[net < 0].sum()
    return float(gp / gl) if gl > 0 else (float("inf") if gp > 0 else 0.0)


def _win(df, lo, hi):
    return df[(df["_day"] >= pd.Timestamp(lo)) & (df["_day"] <= pd.Timestamp(hi))].copy()


def broad_book(setup: str) -> pd.DataFrame:
    """Detection-level book: per-setup pool rows, family dedupe via eval_family
    with an empty config (no mask/premom/guards), baseline exits."""
    work = RECOVERY / setup
    tt.POOL_DIRS = [work / "pools" / (setup + "_enriched")]
    tt.POOL_DIR = tt.POOL_DIRS[0]
    pool = tt.load_pool()
    pool = pool[pool["setup"] == setup].copy()
    return pool


def cost_anatomy(setup: str, side: str, sl: float, tgt: float, book: pd.DataFrame) -> dict:
    out = {}
    for slip, label in ((0.0, "gross_0bps"), (5.0, "net_5bps"), (15.0, "net_15bps")):
        eng._set_slippage(slip)
        sub = tt.attach_entries(book)
        rows = []
        for r in sub.itertuples():
            res = tt._resolve_full(r.ticker, r.side, r.tt_entry_iso, float(r.tt_fill), int(r.tt_qty), sl, tgt)
            if res is None:
                continue
            _e, outcome, exit_px = res
            if slip == 0.0:
                gross = ((exit_px - r.tt_fill) if side == "LONG" else (r.tt_fill - exit_px)) * r.tt_qty
                rows.append((r.Index, gross, outcome))
            else:
                rows.append((r.Index, tt._trade_net(r.side, float(r.tt_fill), int(r.tt_qty), outcome, exit_px),
                             outcome))
        net = np.array([x[1] for x in rows])
        oc = pd.Series([x[2] for x in rows])
        out[label] = {"n": int(len(net)), "pf": round(_pf(net), 3), "sum": round(float(net.sum()), 0),
                      "tgt_rate": round(float((oc == "TARGET").mean()) * 100, 1) if len(oc) else 0.0}
    eng._set_slippage(15.0)
    return out


def mfe_mae(setup: str, side: str, book: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    sub = tt.attach_entries(book)
    if len(sub) > MFE_SAMPLE:
        sub = sub.sample(n=MFE_SAMPLE, random_state=7).sort_index()
    sub = sub.assign(day_str=sub["_day"].astype(str))
    recs = []
    for r in sub.itertuples():
        bars = v11._load_1m_with_open(r.ticker)
        if bars is None or bars.empty:
            continue
        et = pd.Timestamp(r.tt_entry_iso)
        eod = et.normalize() + pd.Timedelta(hours=15, minutes=20)
        w = bars[(bars.index >= et) & (bars.index <= eod)]
        if w.empty:
            continue
        e = float(r.tt_fill)
        rec = {"ticker": r.ticker, "day": r.day_str[:10]}
        h = w["high"].to_numpy(); l = w["low"].to_numpy()
        for hor in HORIZONS:
            k = min(len(w), hor)
            if side == "LONG":
                rec[f"mfe_{hor}"] = (h[:k].max() / e - 1) * 100
                rec[f"mae_{hor}"] = (l[:k].min() / e - 1) * 100
            else:
                rec[f"mfe_{hor}"] = (e / l[:k].min() - 1) * 100
                rec[f"mae_{hor}"] = (e / h[:k].max() - 1) * 100
        recs.append(rec)
    return pd.DataFrame(recs)


def retest_depth(setup: str, side: str, book: pd.DataFrame) -> pd.DataFrame:
    """Pullback depth (in ATR) toward the signal bar within 15/30 min AFTER the
    would-be market entry minute — i.e. how often a resting limit at
    signal_close -/+ alpha*ATR would fill."""
    sub = tt.attach_entries(book)
    if len(sub) > MFE_SAMPLE:
        sub = sub.sample(n=MFE_SAMPLE, random_state=11).sort_index()
    recs = []
    for r in sub.itertuples():
        atr_pct = float(getattr(r, "atr_pct", np.nan))
        close = float(getattr(r, "signal_close", np.nan))
        if not (np.isfinite(atr_pct) and atr_pct > 0 and np.isfinite(close) and close > 0):
            continue
        bars = v11._load_1m_with_open(r.ticker)
        if bars is None or bars.empty:
            continue
        et = pd.Timestamp(r.tt_entry_iso)
        for hor in (15, 30):
            w = bars[(bars.index >= et) & (bars.index < et + pd.Timedelta(minutes=hor))]
            if w.empty:
                continue
            if side == "LONG":
                depth = (close - float(w["low"].min())) / (close * atr_pct)
            else:
                depth = (float(w["high"].max()) - close) / (close * atr_pct)
            recs.append({"hor": hor, "depth_atr": depth})
    return pd.DataFrame(recs)


def fade_test(setup: str, side: str, book: pd.DataFrame) -> list[dict]:
    flipped = book.copy()
    flipped["side"] = "SHORT" if side == "LONG" else "LONG"
    out = []
    for sl, tgt in FADE_EXITS:
        cfg = {"sl": sl, "tgt": tgt, "mask_terms": [], "premom_terms": [], "guard": None,
               "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}
        tt.MAX_POSITIONS = 20
        tt.DAILY_LOSS_RS = 0.0
        tr = tt.eval_family({setup: cfg}, tt.attach_entries(_win(flipped, *TRAIN_W)))
        te = tt.eval_family({setup: cfg}, tt.attach_entries(_win(flipped, *TEST_W)))
        out.append({"exit": f"{sl}/{tgt}",
                    "train_n": tr["trades"], "train_pf": round(tr["net_pf"], 3),
                    "train_net": round(tr["net_pnl"], 0),
                    "test_n": te["trades"], "test_pf": round(te["net_pf"], 3),
                    "test_net": round(te["net_pnl"], 0)})
    return out


def main() -> int:
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    rng = np.random.default_rng(7)
    t0 = time.time()
    report = [f"# ROUND-4 EXECUTION DIAGNOSTICS — B family", "",
              f"_Generated {date.today().isoformat()}. Research-only. Broad detection books "
              f"(family dedupe, no masks) on the recreated enriched pools; TRAIN "
              f"{TRAIN_W[0]}..{TRAIN_W[1]}._", ""]
    blob = {}
    for setup, (side, sl, tgt) in SETUPS.items():
        print(f"[diag] === {setup} ({side}) ===")
        pool = broad_book(setup)
        tr = _win(pool, *TRAIN_W)
        print(f"[diag] TRAIN rows {len(tr)}")

        ca = cost_anatomy(setup, side, sl, tgt, tr)
        print(f"[diag] cost anatomy: {ca}")

        mm = mfe_mae(setup, side, tr, rng)
        mm.to_csv(_HERE / f"mfe_mae_{setup}.csv", index=False)
        mm_summary = {}
        for hor in HORIZONS:
            mm_summary[hor] = {
                "mfe_med": round(float(mm[f"mfe_{hor}"].median()), 3),
                "mfe_p75": round(float(mm[f"mfe_{hor}"].quantile(0.75)), 3),
                "mae_med": round(float(mm[f"mae_{hor}"].median()), 3),
                "mae_p25": round(float(mm[f"mae_{hor}"].quantile(0.25)), 3),
                "pct_mfe_ge_0.5": round(float((mm[f"mfe_{hor}"] >= 0.5).mean()) * 100, 1),
                "pct_mae_le_-0.7": round(float((mm[f"mae_{hor}"] <= -0.7).mean()) * 100, 1),
            }
        print(f"[diag] MFE/MAE: {mm_summary}")

        rt = retest_depth(setup, side, tr)
        rt_summary = {}
        for hor in (15, 30):
            g = rt[rt["hor"] == hor]["depth_atr"]
            if len(g):
                rt_summary[hor] = {"fill@0.3atr_%": round(float((g >= 0.3).mean()) * 100, 1),
                                   "fill@0.6atr_%": round(float((g >= 0.6).mean()) * 100, 1),
                                   "fill@1.0atr_%": round(float((g >= 1.0).mean()) * 100, 1),
                                   "depth_med_atr": round(float(g.median()), 3)}
        print(f"[diag] retest depth: {rt_summary}")

        fd = fade_test(setup, side, pool)
        print(f"[diag] fade: {fd}")

        blob[setup] = {"side": side, "baseline_exit": [sl, tgt], "train_rows": int(len(tr)),
                       "cost_anatomy": ca, "mfe_mae": mm_summary, "retest": rt_summary, "fade": fd}

        report += [f"## {setup} ({side}) — baseline exit {sl}/{tgt}", "",
                   "### D1 cost anatomy (broad TRAIN book)", "",
                   "| pass | n | PF | sum Rs | target-fill % |", "|---|---|---|---|---|"]
        for k, v in ca.items():
            report.append(f"| {k} | {v['n']} | {v['pf']} | {v['sum']:,.0f} | {v['tgt_rate']} |")
        report += ["", "### D2 MFE/MAE from entry (medians, % of entry px)", "",
                   "| horizon min | MFE med | MFE p75 | MAE med | MAE p25 | %MFE>=0.5 | %MAE<=-0.7 |",
                   "|---|---|---|---|---|---|---|"]
        for hor, v in mm_summary.items():
            report.append(f"| {hor} | {v['mfe_med']} | {v['mfe_p75']} | {v['mae_med']} | {v['mae_p25']} | "
                          f"{v['pct_mfe_ge_0.5']} | {v['pct_mae_le_-0.7']} |")
        report += ["", "### D3 retest/limit fill feasibility (pullback depth after signal, ATR units)", "",
                   "| window min | fill@0.3 ATR | fill@0.6 ATR | fill@1.0 ATR | median depth |",
                   "|---|---|---|---|---|"]
        for hor, v in rt_summary.items():
            report.append(f"| {hor} | {v['fill@0.3atr_%']}% | {v['fill@0.6atr_%']}% | "
                          f"{v['fill@1.0atr_%']}% | {v['depth_med_atr']} |")
        report += ["", "### D4 fade (side flipped, statutory @15bps)", "",
                   "| exit | TRAIN n/PF/net | TEST n/PF/net |", "|---|---|---|"]
        for f in fd:
            report.append(f"| {f['exit']} | {f['train_n']}/{f['train_pf']}/Rs{f['train_net']:,.0f} | "
                          f"{f['test_n']}/{f['test_pf']}/Rs{f['test_net']:,.0f} |")
        report.append("")

    (_HERE / "DIAGNOSTICS.md").write_text("\n".join(report), encoding="utf-8")
    (_HERE / "diagnostics.json").write_text(json.dumps(blob, indent=2, default=str), encoding="utf-8")
    print(f"[diag] DONE in {time.time()-t0:.0f}s -> {_HERE / 'DIAGNOSTICS.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
