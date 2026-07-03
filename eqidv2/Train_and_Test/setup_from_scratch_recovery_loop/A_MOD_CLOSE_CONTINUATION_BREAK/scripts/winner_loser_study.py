r"""winner_loser_study.py — winner-vs-loser + MFE/MAE narrative for the redesigned pool.

Builds the baseline TRAIN book (uncollapsed card @ SL0.70/T1.50, full tt pipeline), joins
signal/indicator features back to each trade, and writes WINNER_LOSER_STUDY.md with:
  * winner vs loser feature medians (signal features + key x_ indicators + flags)
  * PF by regime / hour / break-rank / freshness
  * worst days/symbols/trades
  * MFE/MAE quantiles and bracket feasibility (from mfe_mae_study.json if present)

Usage: py -3.12 winner_loser_study.py [--pool <dir>]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
WORK = HERE.parents[1]
TT_DIR = HERE.parents[3]
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for p in (REPO, TT_DIR, ENGINE_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import setup_train_test as tt  # noqa: E402
import pf_band_fitval_loop as eng  # noqa: E402

SETUP = "A_MOD_CLOSE_CONTINUATION_BREAK"
TRAIN_START = pd.Timestamp("2026-03-01")
TRAIN_END = pd.Timestamp("2026-05-30")
CFG = {"sl": 0.70, "tgt": 1.50, "mask_terms": [], "premom_terms": [], "guard": None,
       "status": "OK", "max_positions": 20, "daily_loss_rs": 0.0}

FEATS = ["rs_pct", "vol_ratio", "atr_pct", "body_pct", "close_loc", "vwap_dist_atr",
         "quality_score", "signal_range_pct", "upper_wick_pct",
         "x_rsi", "x_adx", "x_adx_slope3", "x_macd_hist_atr", "x_bb_pos", "x_stoch_k",
         "x_mfi14", "x_obv_slope5", "x_roc6", "x_range_vs_avg20", "x_vol_vs_avg20",
         "x_svwap_dist_atr", "x_day_ret_pct", "x_dist_dayhigh_atr", "x_dayrange_atr",
         "x_orh_dist_atr", "x_pdh_dist_atr", "x_gap_pct", "x_bar_i", "x_break_rank_day",
         "x_fresh_break", "x_prev_pullback", "x_first_break_of_day",
         "x_ema_stack", "x_above_pdh"]


def pf(net):
    return eng._clamp_pf(tt._pf(np.asarray(net, float)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(WORK / "pools" / "pool_enriched"))
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    tt.POOL_DIRS = [str(Path(args.pool).resolve())]
    tt.SLIPPAGE_BPS = 15.0
    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).eq(SETUP)]
    train = pool[(pool["_day"] >= TRAIN_START) & (pool["_day"] <= TRAIN_END)].reset_index(drop=True)
    train = tt.attach_entries(train)
    m = eng.full_metrics(SETUP, CFG, train)
    det = m["detail"]
    print(f"[wl] TRAIN book n={m['n']} PF={m['net_pf']}", flush=True)

    fmap = train.set_index(["ticker", train["tt_sig_ts"].astype(str)])
    rows = []
    for r in det.itertuples():
        try:
            row = fmap.loc[(r.ticker, str(pd.Timestamp(r.signal_time)))]
            if isinstance(row, pd.DataFrame):
                row = row.iloc[0]
        except Exception:
            continue
        d = {c: row.get(c) for c in FEATS}
        d["regime"] = str(row.get("regime", "?")).upper()
        d["net"] = float(r.net_pnl_rs)
        d["outcome"] = str(r.outcome)
        d["hour"] = pd.Timestamp(r.signal_time).hour
        d["trade_date"] = str(r.trade_date)
        d["ticker"] = r.ticker
        rows.append(d)
    df = pd.DataFrame(rows)
    df.to_csv(WORK / "winner_loser_train.csv", index=False)
    win, los = df[df["net"] > 0], df[df["net"] < 0]

    lines = [f"# {SETUP} (LONG) — WINNER_LOSER_STUDY (redesigned pool)", "",
             "_Generated 2026-07-03. Baseline book = uncollapsed card @ SL0.70/T1.50, "
             "15 bps/leg, statutory costs._", "",
             f"TRAIN book: n={m['n']} PF={m['net_pf']} net=Rs{m['net_pnl']:,.0f} "
             f"win%={m['win_rate']} SL/TGT/EOD={m['sl_cnt']}/{m['tgt_cnt']}/{m['eod_cnt']}", "",
             f"winners {len(win)} vs losers {len(los)}", "",
             "## Winner vs loser feature medians", "",
             "| feature | winners | losers | delta |", "|---|---|---|---|"]
    for c in FEATS:
        w = pd.to_numeric(win[c], errors="coerce").median()
        l = pd.to_numeric(los[c], errors="coerce").median()
        if pd.isna(w) and pd.isna(l):
            continue
        d = (w - l) if (pd.notna(w) and pd.notna(l)) else np.nan
        lines.append(f"| {c} | {w:.4g} | {l:.4g} | {d:+.4g} |")

    def seg(col):
        out = []
        for k, g in df.groupby(col):
            if len(g) < 15:
                continue
            out.append((str(k), len(g), pf(g['net']), float(g['net'].sum())))
        return sorted(out, key=lambda x: -x[2])

    for title, col in (("PF by regime", "regime"), ("PF by hour", "hour"),
                       ("PF by break rank (0=first of day)", "x_break_rank_day"),
                       ("PF by fresh-break flag", "x_fresh_break"),
                       ("PF by prev-pullback flag", "x_prev_pullback")):
        lines += ["", f"## {title}", "", "| value | n | PF | net Rs |", "|---|---|---|---|"]
        for k, n, p_, s in seg(col):
            lines.append(f"| {k} | {n} | {p_} | {s:,.0f} |")

    dayg = df.groupby("trade_date")["net"].sum().sort_values()
    symg = df.groupby("ticker")["net"].sum().sort_values()
    lines += ["", "## Worst days", ""] + [f"- {k}: Rs{v:,.0f}" for k, v in dayg.head(6).items()]
    lines += ["", "## Best days", ""] + [f"- {k}: Rs{v:,.0f}" for k, v in dayg.tail(3).items()]
    lines += ["", "## Worst symbols", ""] + [f"- {k}: Rs{v:,.0f}" for k, v in symg.head(6).items()]

    mfe_p = WORK / "mfe_mae_study.json"
    if mfe_p.exists():
        mm = json.loads(mfe_p.read_text())
        lines += ["", "## MFE/MAE (1-min paths, TRAIN book sample)", "",
                  f"- n = {mm['n']}",
                  f"- MFE% quantiles: {mm['mfe_pct']}",
                  f"- MAE% quantiles: {mm['mae_pct']}",
                  f"- MAE-before-MFE% quantiles: {mm['mae_before_mfe_pct']}",
                  f"- minutes to MFE: {mm['minutes_to_mfe']}",
                  f"- close-to-EOD return quantiles: {mm['close_ret_pct']}",
                  f"- by regime: {mm['by_regime']}", "",
                  "### Bracket feasibility (P[MAE-before-MFE inside SL AND MFE >= target] "
                  "vs win-rate needed for PF 1.3)", ""]
        for slk, row in mm["bracket_feasibility"].items():
            cells = ", ".join(f"{tk}: hit {v['hit']}% (need {v['wr_needed_pf1.3']}%)"
                              for tk, v in row.items())
            lines.append(f"- {slk}: {cells}")
    (WORK / "WINNER_LOSER_STUDY.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"[wl] wrote {WORK / 'WINNER_LOSER_STUDY.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
