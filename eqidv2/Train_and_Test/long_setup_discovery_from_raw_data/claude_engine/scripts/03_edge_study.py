r"""03_edge_study.py -- Stage 2 RAW-DATA LONG edge study around the +0.75% threshold.

Reads results/signals_resolved.parquet, uses TRAIN sessions ONLY (no TEST peeking).
Computes: bracket base rates + break-even win-rate math; per-family fast-follow-through;
feature-quintile lift on P(target-first); time-of-day; best/worst symbols & days;
overextension/failed-breakout patterns; +0.5/0.75/1.0/1.5% threshold comparison.

Writes RAW_DATA_LONG_EDGE_STUDY.md
Run: py -3.12 Train_and_Test/long_setup_discovery_from_raw_data/scripts/03_edge_study.py
"""
from __future__ import annotations
import sys
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import lib_long_disc as L

SLIP_PRIMARY = 5.0     # bps/leg realistic for top-250 liquid names
SLIP_STRESS = 15.0     # bps/leg repo default (illiquid small-cap calibration)
FAST_MIN = 15          # "fast" = target hit within 15 one-minute bars (~3 five-min bars)


def breakeven_winrate(tgt, sl, slip_bps):
    s = slip_bps / 1e4
    win = (1 + tgt / 100) * (1 - s) - (1 + s)        # per-rupee of entry
    loss = (1 - sl / 100) * (1 - s) - (1 + s)        # negative
    return -loss / (win - loss) if (win - loss) else float("nan")


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    df = pd.read_parquet(L.RESULTS / "signals_resolved.parquet")
    S = L.load_sessions()
    train_days = set(pd.Timestamp(x) for x in S["train"])
    tr = df[df["_day"].isin(train_days)].copy()
    n = len(tr)
    print(f"[edge] TRAIN union signals = {n:,}")

    out = []
    A = out.append
    A("# RAW_DATA_LONG_EDGE_STUDY — what precedes a FAST +0.75% LONG pop\n")
    A(f"Universe = top-{L.load_universe()['n']} liquid NSE names. **TRAIN sessions only** "
      f"({S['train'][0]}..{S['train'][-1]}, {len(train_days)} sessions) — TEST excluded from discovery.\n")
    A(f"Signal set = union of 10 raw 5-min LONG family triggers = **{n:,}** entries (one per ticker×signal-bar, "
      f"entered next 1-min open). Outcome resolved on 1-min bars (SL-first on same-bar tie).\n")
    A("**'TARGET-first%' = P(+target% touched before −SL%)** — the pure price-path edge (slippage-free). "
      "'fast%' = target hit within ~15 min (≤3 five-min bars). win/PF metrics are NET of statutory cost + slippage.\n")

    # ---- 1. bracket base rates + break-even -----------------------------------
    A("## 1. Bracket base rates (TRAIN union) + break-even win-rate")
    A("| bracket (SL/Tgt) | TARGET-first% | fast≤15m% | SL% | EOD% | net-win%@5bps | PF@5bps | exp@5bps Rs | net-win%@15bps | PF@15bps | breakeven-win%@5bps | @15bps |")
    A("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for key, (sl, tgt) in L.BRACKETS.items():
        oc = tr["out_" + key]
        tgt_first = 100 * (oc == "TARGET").mean()
        fast = 100 * ((oc == "TARGET") & (tr["held_" + key] <= FAST_MIN)).mean()
        slc = 100 * (oc.isin(["SL", "BE"])).mean()
        eod = 100 * (oc == "EOD").mean()
        m5 = L.attach_net(tr, key, SLIP_PRIMARY)
        m15 = L.attach_net(tr, key, SLIP_STRESS)
        def pf(x):
            gp = x[x > 0].sum(); gl = -x[x < 0].sum()
            return gp / gl if gl > 1e-9 else float("inf")
        A(f"| {sl:.2f}/{tgt:.2f} | {tgt_first:.1f} | {fast:.1f} | {slc:.1f} | {eod:.1f} | "
          f"{100*(m5>0).mean():.1f} | {pf(m5):.2f} | {m5.mean():.1f} | {100*(m15>0).mean():.1f} | {pf(m15):.2f} | "
          f"{100*breakeven_winrate(tgt,sl,SLIP_PRIMARY):.1f} | {100*breakeven_winrate(tgt,sl,SLIP_STRESS):.1f} |")
    A("")
    A("**Cost reality:** fixed per-leg slippage is a large fraction of a sub-1% target. At 15 bps/leg the "
      "break-even win-rate for 0.75/0.75 is ~70% (near-impossible); at a realistic **5 bps/leg for liquid names "
      "it is ~57%**. Larger targets (0.75/1.00) carry a LOWER break-even because the fixed cost is a smaller "
      "fraction of the target — a key lever the search exploits while staying near the tight theme.\n")

    # ---- 2. per-family fast-follow-through ------------------------------------
    A("## 2. Per-family raw edge (anchor 0.75/0.75 and 0.75/1.00, TRAIN)")
    A("| family | label | n | TARGET-first% (.75/.75) | fast≤15m% | net-win%@5bps | PF@5bps | TARGET-first% (.75/1.0) | PF@5bps(.75/1.0) |")
    A("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    fam_rows = []
    for k, lab in L.FAMILY_LABELS.items():
        sub = tr[tr["f_" + k]]
        if len(sub) == 0:
            continue
        oc = sub["out_b_075_075"]
        tf = 100 * (oc == "TARGET").mean()
        fast = 100 * ((oc == "TARGET") & (sub["held_b_075_075"] <= FAST_MIN)).mean()
        m5 = L.attach_net(sub, "b_075_075", SLIP_PRIMARY)
        def pf(x):
            gp = x[x > 0].sum(); gl = -x[x < 0].sum()
            return gp / gl if gl > 1e-9 else float("inf")
        oc2 = sub["out_b_075_100"]
        tf2 = 100 * (oc2 == "TARGET").mean()
        m52 = L.attach_net(sub, "b_075_100", SLIP_PRIMARY)
        fam_rows.append((k, tf, pf(m5), len(sub)))
        A(f"| {k} | {lab} | {len(sub):,} | {tf:.1f} | {fast:.1f} | {100*(m5>0).mean():.1f} | {pf(m5):.2f} | {tf2:.1f} | {pf(m52):.2f} |")
    A("")
    fam_rows.sort(key=lambda r: -r[1])
    A(f"Best raw target-first families: " + ", ".join(f"{k} ({tf:.0f}%)" for k, tf, _, _ in fam_rows[:4]) + ".\n")

    # ---- 3. feature-quintile lift on P(target-first) --------------------------
    A("## 3. Feature edges — P(+0.75% before −0.75%) by quintile (TRAIN union)")
    A("Monotonic rise/fall across quintiles = a usable LONG threshold. base = overall TARGET-first%.")
    base_tf = 100 * (tr["out_b_075_075"] == "TARGET").mean()
    A(f"\n- **Base TARGET-first% (0.75/0.75) = {base_tf:.1f}%**\n")
    feats = ["vol_ratio", "atr_pct", "range_pct", "body_frac", "close_loc", "upper_wick", "lower_wick",
             "rsi", "adx", "macd_hist", "ema20_slope", "mom2_pct", "mom3_pct", "vwap_dist_atr",
             "ema20_dist_atr", "compress5_atr", "green_prev3", "slot"]
    is_tgt = (tr["out_b_075_075"] == "TARGET").to_numpy()
    edge_rank = []
    A("| feature | Q1 | Q2 | Q3 | Q4 | Q5 | spread(pp) | direction |")
    A("|---|---:|---:|---:|---:|---:|---:|---|")
    for f in feats:
        x = pd.to_numeric(tr[f], errors="coerce")
        ok = x.notna()
        if ok.sum() < 500 or x[ok].nunique() < 5:
            continue
        try:
            q = pd.qcut(x[ok], 5, labels=False, duplicates="drop")
        except Exception:
            continue
        g = pd.DataFrame({"q": q, "t": is_tgt[ok.to_numpy()]}).groupby("q")["t"].mean() * 100
        if len(g) < 4:
            continue
        vals = [g.get(i, np.nan) for i in range(5)]
        spread = np.nanmax(vals) - np.nanmin(vals)
        direction = "rising" if (np.nanargmax(vals) > np.nanargmin(vals)) else "falling"
        edge_rank.append((f, spread, vals, direction))
        A(f"| {f} | " + " | ".join(f"{v:.0f}" if v == v else "-" for v in vals) + f" | {spread:.1f} | {direction} |")
    A("")
    edge_rank.sort(key=lambda r: -r[1])
    A("### Strongest single-feature edges (by quintile spread)")
    for f, sp, vals, d in edge_rank[:8]:
        best_q = int(np.nanargmax(vals)) + 1
        A(f"- **{f}**: spread {sp:.1f}pp, {d}; best in Q{best_q} ({np.nanmax(vals):.0f}% target-first).")
    A("")

    # ---- 4. time of day -------------------------------------------------------
    A("## 4. Time-of-day — fast LONG follow-through (TARGET-first%, 0.75/0.75)")
    tr["hr"] = (tr["minute"] // 60)
    g = tr.groupby("hr").apply(lambda s: pd.Series({
        "n": len(s), "tgt_first": 100 * (s["out_b_075_075"] == "TARGET").mean()}))
    A("| hour IST | n | TARGET-first% |")
    A("|---|---:|---:|")
    for hr, row in g.iterrows():
        A(f"| {int(hr):02d}:xx | {int(row['n']):,} | {row['tgt_first']:.1f} |")
    sl_g = tr.groupby(pd.cut(tr["slot"], [0, 3, 6, 12, 24, 42, 100])).apply(
        lambda s: 100 * (s["out_b_075_075"] == "TARGET").mean() if len(s) else np.nan)
    A("\nBy session-slot bucket (5-min bars from open): " +
      ", ".join(f"{str(iv)}={v:.0f}%" for iv, v in sl_g.items()) + ".\n")

    # ---- 5. best/worst symbols & days -----------------------------------------
    A("## 5. Best / worst symbols for fast LONG follow-through (min 150 signals, TRAIN)")
    sym = tr.groupby("ticker").apply(lambda s: pd.Series({
        "n": len(s), "tgt_first": 100 * (s["out_b_075_075"] == "TARGET").mean()}))
    sym = sym[sym["n"] >= 150].sort_values("tgt_first", ascending=False)
    A("Best 10: " + ", ".join(f"{t}({r['tgt_first']:.0f}%/n{int(r['n'])})" for t, r in sym.head(10).iterrows()))
    A("\nWorst 10: " + ", ".join(f"{t}({r['tgt_first']:.0f}%/n{int(r['n'])})" for t, r in sym.tail(10).iterrows()) + "\n")
    day = tr.groupby("_day").apply(lambda s: 100 * (s["out_b_075_075"] == "TARGET").mean())
    A(f"Day TARGET-first% range across TRAIN: min {day.min():.0f}% / median {day.median():.0f}% / max {day.max():.0f}% "
      f"(best {day.idxmax().date()}, worst {day.idxmin().date()}).\n")

    # ---- 6. overextension / failed-breakout patterns --------------------------
    A("## 6. Failed-breakout / overextension patterns (lower target-first% = avoid)")
    def cond_rate(mask, label):
        s = tr[mask]
        if len(s) < 200:
            return f"- {label}: n<200 (skip)"
        return f"- {label}: n={len(s):,}, TARGET-first%={100*(s['out_b_075_075']=='TARGET').mean():.1f} (base {base_tf:.1f})"
    A(cond_rate(tr["upper_wick"] >= 0.4, "long upper wick ≥0.40 (rejection)"))
    A(cond_rate(tr["vwap_dist_atr"] >= 3.0, "far above VWAP ≥3 ATR (overextended)"))
    A(cond_rate(tr["green_prev3"] >= 3, "3 prior green candles (exhaustion risk)"))
    A(cond_rate(tr["atr_pct"] <= 0.15, "very low ATR% ≤0.15 (no room)"))
    A(cond_rate(tr["atr_pct"] >= 0.8, "very high ATR% ≥0.8 (−0.75% is noise)"))
    A(cond_rate(tr["rsi"] >= 80, "RSI ≥80 (overbought)"))
    A(cond_rate(tr["close_loc"] <= 0.4, "weak close (close_loc ≤0.4)"))
    A(cond_rate(tr["vol_ratio"] < 1.0, "below-average volume (<1.0x)"))
    A("")

    # ---- 7. +0.5/0.75/1.0/1.5 threshold comparison ----------------------------
    A("## 7. Symmetric threshold comparison — P(+X% before −X%) (TRAIN union)")
    A("| ±X% | TARGET-first% | fast≤15m% | source |")
    A("|---|---:|---:|---|")
    for key, (sl, tgt) in [("b_050_050", (.5, .5)), ("b_060_060", (.6, .6)), ("b_075_075", (.75, .75))]:
        oc = tr["out_" + key]
        A(f"| ±{tgt:.2f} | {100*(oc=='TARGET').mean():.1f} | "
          f"{100*((oc=='TARGET')&(tr['held_'+key]<=FAST_MIN)).mean():.1f} | cached |")
    # resolve +1.0 and +1.5 on the fly for the TRAIN union (per ticker)
    for X in (1.0, 1.5):
        tf_cnt = fast_cnt = tot = 0
        for tk, sub in tr.groupby("ticker"):
            arr = L.load_1m_raw(tk)
            if arr is None:
                continue
            ts, op, hi, lo, cl = arr
            for r in sub.itertuples():
                ei = L._entry_index(ts, op, pd.Timestamp(r.signal_ts).value)
                if ei is None:
                    continue
                res = L.resolve_path(ts, op, hi, lo, cl, ei[0], ei[1], X, X)
                tot += 1
                if res["outcome"] == "TARGET":
                    tf_cnt += 1
                    if res["bars_held"] <= FAST_MIN:
                        fast_cnt += 1
            L.load_1m_raw.cache_clear()
        A(f"| ±{X:.2f} | {100*tf_cnt/max(1,tot):.1f} | {100*fast_cnt/max(1,tot):.1f} | resolved on-the-fly |")
    A("")
    A("**Read:** tighter targets hit far more often (more fills, as intended) but their break-even win-rate is "
      "higher (fixed cost is a bigger fraction). Wider targets convert less but need a lower win-rate to pay. "
      "The discovery searches all brackets but anchors on the tight 0.75% theme.\n")

    A("## Design implications (drive Stage 4 rules)")
    A("1. Tight symmetric 0.75% on the *raw union* is a coin-flip-minus (base ~"
      f"{base_tf:.0f}% target-first) — structure alone is not enough; must stack the top feature edges above.")
    A("2. Favor the families + feature quintiles with the highest target-first lift (Section 2-3).")
    A("3. Avoid overextension (Section 6): long upper wick, far-above-VWAP, exhausted green runs, dead-low ATR.")
    A("4. Slot/time matters (Section 4) — bias to the windows with the best fast follow-through.")
    A("5. The 0.75/1.00 bracket needs a far lower win-rate to pay; it is the most cost-robust tight variant.")
    (L.OUTDIR / "RAW_DATA_LONG_EDGE_STUDY.md").write_text("\n".join(out), encoding="utf-8")
    print("[edge] wrote RAW_DATA_LONG_EDGE_STUDY.md")


if __name__ == "__main__":
    main()
