r"""failure_study.py — Stage 2: winners-vs-losers study on the TRAIN raw deduped book.

Usage: py -3.12 failure_study.py --setup C_OR_BREAKDOWN --sl 0.90 --tgt 2.00
Writes <WORK>/WINNER_LOSER_STUDY.md + failure_segments.csv.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parent))
import recovery_lib as rl  # noqa: E402

FEATS_5M = ["vol_ratio", "quality_score", "ranker_score", "rs_pct", "atr_pct", "body_pct",
            "close_loc", "vwap_dist_atr", "adx", "rsi", "rsi3max", "ema20_slope", "macd_hist",
            "macd_hist_delta", "market_ret_pct", "stock_ret", "upper_wick_pct", "lower_wick_pct",
            "signal_range_pct", "signal_minute", "fresh_age_bars", "fire_seq",
            "mfe_pct", "mae_pct", "mfe30_pct", "mae30_pct", "t_mfe_min", "t_mae_min"]
FEATS_PM = ["pm_pre_entry_momentum_score", "pm_sig5_adx_calc", "pm_sig5_rsi_dir",
            "pm_sig5_vol_ratio20", "pm_pre1_adx", "pm_pre3_range_r", "pm_pre5_mom_r",
            "pm_pre3_close_pos", "pm_pre2_mom_r", "pm_pre10_mom_r", "pm_pre1_rsi_dir",
            "pm_sig5_body_r", "pm_pre5_vol_ratio20", "pm_pre3_vol_ratio20"]


def bucket_table(det: pd.DataFrame, feat: str, q=4):
    x = pd.to_numeric(det[feat], errors="coerce")
    if x.notna().sum() < 40 or x.nunique() < 5:
        return None
    try:
        b = pd.qcut(x, q, duplicates="drop")
    except Exception:
        return None
    rows = []
    for iv, g in det.groupby(b, observed=True):
        net = g["_net"].to_numpy(float)
        gp = net[net > 0].sum(); gl = -net[net < 0].sum()
        rows.append({"feature": feat, "bucket": str(iv), "n": len(g),
                     "pf": round(gp / gl, 2) if gl > 0 else np.inf,
                     "net": round(net.sum(), 0),
                     "win%": round((net > 0).mean() * 100, 1)})
    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--sl", type=float, default=0.90)
    ap.add_argument("--tgt", type=float, default=2.00)
    args = ap.parse_args()
    setup = args.setup.strip().upper()
    work = rl.TT_DIR / "setup_recovery_full_loop" / setup

    eng = rl.ResearchEngine(setup, work)
    w = eng.w
    cfg = {"sl": args.sl, "tgt": args.tgt, "mask_terms": [], "premom_terms": [], "guard": None,
           "max_positions": 20, "daily_loss_rs": 0.0}
    m = eng.eval_cfg(cfg, w["TRAIN"], want_detail=True, day_block=True, wname="TRAIN")
    det = m["detail"].merge(eng.df.drop(columns=[c for c in ("_net", "_outcome", "_exit_iso") if c in eng.df.columns]),
                            on="rk", how="left", suffixes=("", "_x"))
    print(f"[study] TRAIN raw deduped book: {rl.mline(m)}")

    net = det["_net"].to_numpy(float)
    det["_win"] = net > 0
    wl_rows = []
    seg_frames = []
    for f in FEATS_5M + FEATS_PM:
        if f not in det.columns:
            continue
        x = pd.to_numeric(det[f], errors="coerce")
        if x.notna().sum() < 40:
            continue
        wl_rows.append({"feature": f,
                        "win_median": round(float(x[det['_win']].median()), 4),
                        "loss_median": round(float(x[~det['_win']].median()), 4),
                        "win_mean": round(float(x[det['_win']].mean()), 4),
                        "loss_mean": round(float(x[~det['_win']].mean()), 4)})
        bt = bucket_table(det, f)
        if bt is not None:
            seg_frames.append(bt)
    wl = pd.DataFrame(wl_rows)
    seg = pd.concat(seg_frames, ignore_index=True) if seg_frames else pd.DataFrame()
    seg.to_csv(work / "failure_segments.csv", index=False)

    # categorical segments
    cat_lines = []
    for cf in ("regime", "_outcome"):
        if cf in det.columns:
            g = det.groupby(det[cf].astype(str))
            t = pd.DataFrame({"n": g.size(),
                              "net": g["_net"].sum().round(0),
                              "pf": g["_net"].apply(lambda s: round(s[s > 0].sum() / max(1e-9, -s[s < 0].sum()), 2)),
                              "win%": g["_win"].mean().mul(100).round(1)})
            cat_lines.append(f"### by {cf}\n\n{t.to_markdown()}\n")
    # time-of-day
    det["hour_bucket"] = pd.cut(pd.to_numeric(det["signal_minute"], errors="coerce"),
                                bins=[0, 690, 750, 810, 870, 2000],
                                labels=["<11:30", "11:30-12:30", "12:30-13:30", "13:30-14:30", ">14:30"])
    g = det.groupby("hour_bucket", observed=True)
    t = pd.DataFrame({"n": g.size(), "net": g["_net"].sum().round(0),
                      "pf": g["_net"].apply(lambda s: round(s[s > 0].sum() / max(1e-9, -s[s < 0].sum()), 2)),
                      "win%": g["_win"].mean().mul(100).round(1)})
    cat_lines.append(f"### by time of day\n\n{t.to_markdown()}\n")
    # weekday
    g = det.groupby(det["_day"].dt.day_name())
    t = pd.DataFrame({"n": g.size(), "net": g["_net"].sum().round(0),
                      "pf": g["_net"].apply(lambda s: round(s[s > 0].sum() / max(1e-9, -s[s < 0].sum()), 2))})
    cat_lines.append(f"### by weekday\n\n{t.to_markdown()}\n")

    # concentration + MAE/MFE by outcome
    day_sum = det.groupby(det["_day"].dt.date)["_net"].sum().sort_values()
    sym_sum = det.groupby("ticker")["_net"].sum().sort_values()
    mm = det.groupby(det["_outcome"].astype(str))[["mfe_pct", "mae_pct", "mfe30_pct", "mae30_pct",
                                                   "t_mfe_min", "t_mae_min"]].median().round(3)

    today = date.today().isoformat()
    lines = [f"# {setup} — WINNER_LOSER_STUDY (Stage 2)", "",
             f"_Generated {today}. TRAIN raw deduped book @ SL {args.sl} / Tgt {args.tgt}, 15 bps/leg._", "",
             f"**Book:** {rl.mline(m)}", "",
             "## Winners vs losers — feature medians", "", wl.to_markdown(index=False), "",
             "## Per-bucket PF (quartiles) — full table in failure_segments.csv", ""]
    if not seg.empty:
        best = seg.sort_values("pf", ascending=False).head(15)
        worst = seg.sort_values("pf").head(15)
        lines += ["### Most favorable buckets", "", best.to_markdown(index=False), "",
                  "### Most hostile buckets", "", worst.to_markdown(index=False), ""]
    lines += cat_lines
    lines += ["### MAE/MFE medians by outcome", "", mm.to_markdown(), "",
              "### Worst days", ""] + [f"- {d}: Rs{v:,.0f}" for d, v in day_sum.head(5).items()]
    lines += ["", "### Worst symbols", ""] + [f"- {s}: Rs{v:,.0f}" for s, v in sym_sum.head(5).items()]
    lines += ["", "### Best days", ""] + [f"- {d}: Rs{v:,.0f}" for d, v in day_sum.tail(3).items()]
    (work / "WINNER_LOSER_STUDY.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"[study] wrote {work / 'WINNER_LOSER_STUDY.md'} and failure_segments.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
