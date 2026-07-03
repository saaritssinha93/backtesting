r"""07_s9_threemonth.py -- evaluate the S9_MIDDAY_LOSE (SHORT) candidate over the LAST 3 MONTHS.

The TRAIN/TEST cache covers only 8 weeks; this resolves S9 signals over ~the last 63 resolvable
sessions (≈3 months ending 2026-06-29) so we see month-by-month consistency, including the
late-Mar/Apr stretch that was NEVER part of the S9 search (genuine extra out-of-sample).

Reports overall + monthly + day-wise + per-symbol + bracket-robustness, at 2/5/15 bps + gross.
Writes S9_LAST_3_MONTHS.md and results/s9_3m_trades.csv.
Run: py -3.12 .../claude_engine/scripts/07_s9_threemonth.py
"""
from __future__ import annotations
import json, sys, time
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import lib_long_disc as L

# S9 candidate (from candidates/EXT_SHORT_S9_MIDDAY_LOSE_candidate_001.json)
S9 = dict(family="S9_MIDDAY_LOSE", side="SHORT", bracket="x_125_250", slip_bps=5.0,
          min_minute=None, max_minute=660, top_n=None, rank_feat="atr_pct",
          max_per_sym_day=None, max_book_concurrent=20,
          mask=[["mom3_pct", ">=", 0.1], ["atr_pct", ">=", 0.3]])
BRACKETS = ["x_075_075", "x_100_200", "x_125_250", "x_100_300"]   # primary + robustness neighbours
IS_TRAIN_START = pd.Timestamp("2026-04-30")     # S9 search window start (anything earlier = extra OOS)
IS_TEST_START = pd.Timestamp("2026-06-15")


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    uni = L.load_universe()["tickers"]
    S = L.load_sessions()
    resolvable = [pd.Timestamp(x) for x in S["resolvable"]]
    last = resolvable[-1]
    cutoff = last - pd.DateOffset(months=3)
    signal_days = [s for s in resolvable if s >= cutoff]
    warm_start = cutoff - pd.Timedelta(days=25)
    load_days = [s for s in resolvable if s >= warm_start]
    sig_set, load_set = set(signal_days), set(load_days)
    print(f"[s9-3m] window {signal_days[0].date()}..{signal_days[-1].date()} = {len(signal_days)} sessions "
          f"(warmup from {load_days[0].date()})")

    exits = {b: dict(sl_pct=L.BRACKETS_EXT[b][0], tgt_pct=L.BRACKETS_EXT[b][1]) for b in BRACKETS}
    rows, t0 = [], time.time()
    slip = L.SLIPPAGE_BPS / 1e4
    for ti, tk in enumerate(uni):
        d5 = L.load_5m_raw(tk)
        if d5 is None:
            continue
        d5 = d5[d5["sess"].isin(load_set)]
        if len(d5) < 30:
            L.load_5m_raw.cache_clear(); continue
        feat = L.compute_features(d5)
        trig = L.short_family_triggers(feat)["S9_MIDDAY_LOSE"].to_numpy()
        emit = feat["sess"].isin(sig_set).to_numpy() & trig
        if not emit.any():
            L.load_5m_raw.cache_clear(); continue
        sub = feat[emit]
        arr = L.load_1m_raw(tk)
        if arr is None:
            L.load_5m_raw.cache_clear(); continue
        ts, op, hi, lo, cl = arr
        for r in sub.itertuples():
            ei = L._entry_index(ts, op, pd.Timestamp(r.date).value)
            if ei is None:
                continue
            e_idx, e_open = ei
            base = dict(ticker=tk, side="SHORT", signal_ts=pd.Timestamp(r.date), _day=pd.Timestamp(r.sess),
                        slot=int(r.slot), minute=int(r.minute), f_S9_MIDDAY_LOSE=True,
                        atr_pct=r.atr_pct, mom3_pct=r.mom3_pct, e_open=e_open,
                        qty=max(1, int(L.NOTIONAL_RS / e_open)))
            for b, kw in exits.items():
                res = L.resolve_path(ts, op, hi, lo, cl, e_idx, e_open, side="SHORT", **kw)
                base["out_" + b] = res["outcome"]; base["raw_" + b] = res["exit_px_raw"]
                base["held_" + b] = res["bars_held"]; base["tie_" + b] = res["tie"]
            rows.append(base)
        L.load_1m_raw.cache_clear(); L.load_5m_raw.cache_clear()
        if (ti + 1) % 50 == 0:
            print(f"  ...{ti+1}/{len(uni)} tickers, {len(rows)} S9 signals, {time.time()-t0:.0f}s")

    df = pd.DataFrame(rows)
    print(f"[s9-3m] raw S9 signals (3m, pre-selection) = {len(df):,}")
    n_sess = len(signal_days)

    # build the deployable book + per-trade net at the primary bracket/slippage
    book = L.select_book(df, S9)
    book = book.sort_values("_entry_ns")
    out = ["# S9_MIDDAY_LOSE (SHORT) — last 3 months", "",
           f"Setup `S9_MIDDAY_LOSE` SHORT, bracket **{S9['bracket']}** (SL 1.25% / target 2.50%, 1:2), "
           f"morning-only ≤11:00, mask `mom3_pct≥0.1 & atr_pct≥0.3`, no-overlap per symbol, ≤20 concurrent.",
           f"Window **{signal_days[0].date()} .. {signal_days[-1].date()}** ({n_sess} resolvable sessions). "
           f"Note: {IS_TRAIN_START.date()}–{IS_TEST_START.date()} was the S9 TRAIN (in-sample); "
           f"{IS_TEST_START.date()}–{last.date()} = TEST (OOS); **before {IS_TRAIN_START.date()} = extra OOS "
           f"never seen by the search**. Net of statutory cost + slippage.", ""]

    def metr(b, key, slip):
        if b.empty:
            return None
        net = L.attach_net(b, key, slip)
        t = pd.DataFrame({"net": net, "outcome": b["out_" + key].to_numpy(), "bars_held": b["held_" + key].to_numpy(),
                          "_day": b["_day"].to_numpy(), "ticker": b["ticker"].to_numpy(), "tie": b["tie_" + key].to_numpy()})
        return L.metrics_from_trades(t, n_sess)

    out += ["## Overall (primary bracket x_125_250)",
            "| cost | trades | PF | win% | net Rs | exp/tr | avg win | avg loss | tgt/sl/eod | dayDom | symDom | topTr | maxDD |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for slipbps in (2.0, 5.0, 15.0):
        m = metr(book, S9["bracket"], slipbps)
        out.append(f"| {slipbps:.0f} bps | {m['trades']} | {m['pf']} | {m['win_rate']} | {m['net_pnl']:,.0f} | "
                   f"{m['expectancy']} | {m['avg_win']} | {m['avg_loss']} | {m['tgt_cnt']}/{m['sl_cnt']}/{m['eod_cnt']} | "
                   f"{m['day_dom']} | {m['sym_dom']} | {m['top_trade_share']} | {m['max_dd']:,.0f} |")
    g = metr(book, S9["bracket"], 0.0)  # ~gross via attach at 0 slip (still statutory); add true-gross
    bg = book.copy()
    diff = (bg["raw_" + S9["bracket"]].to_numpy(float) - bg["e_open"].to_numpy(float)) * bg["qty"].to_numpy(float)
    truegross = -diff  # SHORT
    out.append(f"| 0 bps (statutory only) | {g['trades']} | {g['pf']} | {g['win_rate']} | {g['net_pnl']:,.0f} | "
               f"{g['expectancy']} | — | — | — | — | — | — | — |")
    out.append(f"\n_True price-path (0 cost incl. no statutory): net Rs {truegross.sum():,.0f}, "
               f"PF {truegross[truegross>0].sum()/max(1e-9,-truegross[truegross<0].sum()):.2f}._\n")

    # monthly breakdown at 5 bps
    out += ["## Month-by-month (primary bracket, 5 bps/leg)",
            "| month | sessions | trades | PF | win% | net Rs | exp/tr | in-sample? |",
            "|---|---:|---:|---:|---:|---:|---:|---|"]
    net5 = L.attach_net(book, S9["bracket"], 5.0)
    bk = book.assign(net=net5, ym=pd.to_datetime(book["_day"]).dt.to_period("M").astype(str))
    for ym, grp in bk.groupby("ym"):
        gp = grp["net"][grp["net"] > 0].sum(); gl = -grp["net"][grp["net"] < 0].sum()
        pf = gp / gl if gl > 1e-9 else float("inf")
        n_s = len(set(d for d in signal_days if str(pd.Period(d, "M")) == ym))
        first = grp["_day"].min()
        tag = ("TEST/OOS" if first >= IS_TEST_START else ("TRAIN/in-sample" if first >= IS_TRAIN_START else "extra OOS"))
        out.append(f"| {ym} | {n_s} | {len(grp)} | {pf:.3f} | {round(100*(grp['net']>0).mean(),1)} | "
                   f"{grp['net'].sum():,.0f} | {round(grp['net'].mean(),1)} | {tag} |")

    # bracket robustness over 3m
    out += ["", "## Bracket robustness over the 3 months (5 bps/leg)",
            "| bracket | trades | PF | win% | net Rs |", "|---|---:|---:|---:|---:|"]
    for b in BRACKETS:
        bb = L.select_book(df, {**S9, "bracket": b})
        m = metr(bb, b, 5.0)
        out.append(f"| {b} | {m['trades']} | {m['pf']} | {m['win_rate']} | {m['net_pnl']:,.0f} |")

    # day-wise concentration (5 bps)
    byday = bk.groupby("_day")["net"].agg(["sum", "count"]).sort_values("sum")
    out += ["", "## Day-wise net (5 bps) — worst 5 / best 5",
            "```", "WORST: " + " | ".join(f"{d.date()}:{v:,.0f}(n{int(c)})" for d, (v, c) in byday.head(5).iterrows()),
            "BEST : " + " | ".join(f"{d.date()}:{v:,.0f}(n{int(c)})" for d, (v, c) in byday.tail(5).iterrows()), "```"]
    gp = bk["net"][bk["net"] > 0].sum()
    best_day_share = byday["sum"].max() / gp if gp > 0 else 0
    pos_days = int((byday["sum"] > 0).sum()); tot_days = len(byday)
    out.append(f"\nProfitable days: **{pos_days}/{tot_days}**. Best single day = "
               f"**{best_day_share*100:.0f}%** of gross profit (concentration risk).")

    # per-symbol top
    bysym = bk.groupby("ticker")["net"].agg(["sum", "count"]).sort_values("sum")
    out += ["", "## Per-symbol net (5 bps) — worst 5 / best 5",
            "```", "WORST: " + " | ".join(f"{t}:{v:,.0f}(n{int(c)})" for t, (v, c) in bysym.head(5).iterrows()),
            "BEST : " + " | ".join(f"{t}:{v:,.0f}(n{int(c)})" for t, (v, c) in bysym.tail(5).iterrows()), "```", ""]

    m5 = metr(book, S9["bracket"], 5.0)
    out += ["## Read",
            f"- Over the full 3 months ({n_sess} sessions): **{m5['trades']} trades, PF {m5['pf']}, "
            f"win {m5['win_rate']}%, net Rs {m5['net_pnl']:,.0f} @5 bps** (≈{m5['trades']/n_sess:.1f} trades/day).",
            "- Still **not a fast scalp** — ~"
            f"{round(100*m5['eod_cnt']/max(1,m5['trades']))}% exit at EOD, avg hold {m5['avg_hold_min']} min.",
            "- **WATCH / research only — DO NOT PROMOTE WITHOUT APPROVAL.** final_setup_conf.py untouched."]
    (L.OUTDIR / "S9_LAST_3_MONTHS.md").write_text("\n".join(out), encoding="utf-8")
    bk[["_day", "ticker", "signal_ts", "slot", "minute", "atr_pct", "mom3_pct",
        "out_" + S9["bracket"], "held_" + S9["bracket"], "net"]].to_csv(L.RESULTS / "s9_3m_trades.csv", index=False)
    print(f"[s9-3m] OVERALL @5bps: n={m5['trades']} PF={m5['pf']} win={m5['win_rate']}% net=Rs{m5['net_pnl']:,.0f} "
          f"dayDom={m5['day_dom']} | wrote S9_LAST_3_MONTHS.md + results/s9_3m_trades.csv")


if __name__ == "__main__":
    main()
