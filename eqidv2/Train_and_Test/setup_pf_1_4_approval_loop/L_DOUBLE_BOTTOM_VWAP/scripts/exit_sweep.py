r"""exit_sweep.py — focused SL%/target% sweep for ONE setup (research-only).

"Play with SL and target and get the best match." Holds detection + (optionally) the card
gates fixed and sweeps a FINE SL x Tgt grid over the past-2-months split, reporting net-of-cost
TRAIN / TEST (and FIT/VAL) Profit Factor + trades for every exit combo. Reuses the repo pipeline
via setup_train_test (eval_family / book_detail) and pf_band_search.full_metrics, so numbers match
the band-search loop. Net of cost @15 bps/leg (realistic). No live trades; writes only under the
setup's approval-loop folder.

Two bases:
  * raw    = detection only (no mask / premom / guard)   -> isolates the pure exit effect, full sample
  * card   = card pre-momentum gates kept, exit swept     -> best exit given the card's entry filter

Run (from repo root):
  py -3.12 Train_and_Test/setup_pf_1_4_approval_loop/L_DOUBLE_BOTTOM_VWAP/scripts/exit_sweep.py \
      --setup L_DOUBLE_BOTTOM_VWAP --pool C:/TradingData/eqidv2/setup_pools_2026_06_29/L_DOUBLE_BOTTOM_VWAP \
      --train_start 2026-04-28 --test_start 2026-06-12
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_P = Path(__file__).resolve()
TT_DIR = next(par for par in _P.parents if par.name == "Train_and_Test")
REPO_ROOT = TT_DIR.parent
for _d in (str(REPO_ROOT), str(TT_DIR), str(_P.parent)):
    if _d not in sys.path:
        sys.path.insert(0, _d)

import setup_train_test as tt          # noqa: E402
import pf_band_search as pb            # noqa: E402

SL_SWEEP = [0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00, 1.10, 1.20, 1.30, 1.40, 1.50, 1.75, 2.00]
TGT_SWEEP = [0.50, 0.75, 1.00, 1.25, 1.50, 1.75, 2.00, 2.50, 3.00, 3.50]
BAND_LO, BAND_HI, TEST_PF_MIN, DOM_CAP = 1.30, 1.70, 1.40, 0.40
MIN_TR, MIN_TE = 15, 5

CARD_PREMOM = [["pre_entry_momentum_score", ">=", 79.0], ["sig5_adx_calc", ">=", 28.0]]


def _base(kind):
    if kind == "card":
        return {"mask_terms": [], "premom_terms": CARD_PREMOM, "guard": None,
                "max_positions": 20, "daily_loss_rs": 0.0}
    return {"mask_terms": [], "premom_terms": [], "guard": None,
            "max_positions": 20, "daily_loss_rs": 0.0}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--setup", required=True)
    ap.add_argument("--pool", required=True)
    ap.add_argument("--train_start", default="2026-04-28")
    ap.add_argument("--test_start", default="2026-06-12")
    ap.add_argument("--slippage_bps", type=float, default=15.0)
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass

    setup = args.setup.strip().upper()
    outdir = TT_DIR / "setup_pf_1_4_approval_loop" / setup
    outdir.mkdir(parents=True, exist_ok=True)

    tt.POOL_DIRS = [Path(args.pool)]; tt.POOL_DIR = Path(args.pool)
    pb._set_slip(args.slippage_bps)
    pool = tt.load_pool(); pool = pool[pool["setup"] == setup].copy()
    sessions = sorted(pd.Series(pool["_day"].dropna().unique()))
    ts, te = pd.Timestamp(args.train_start), pd.Timestamp(args.test_start)
    TEST_s = [s for s in sessions if s >= te]
    TRAIN_s = [s for s in sessions if ts <= s < (TEST_s[0] if TEST_s else sessions[-1] + pd.Timedelta(days=1))]
    half = len(TRAIN_s) // 2
    FIT_s, VAL_s = TRAIN_s[:half], TRAIN_s[half:]
    span = set(map(pd.Timestamp, TRAIN_s + TEST_s))
    sub = tt.attach_entries(pool[pool["_day"].isin(span)].copy())

    def _sl(ss): return sub[sub["_day"].isin(set(map(pd.Timestamp, ss)))].copy()
    FIT, VAL, TRAIN, TEST = _sl(FIT_s), _sl(VAL_s), _sl(TRAIN_s), _sl(TEST_s)

    def _d(ss): return f"{pd.Timestamp(ss[0]).date()}..{pd.Timestamp(ss[-1]).date()}"
    print(f"[exit-sweep] {setup}  TRAIN {_d(TRAIN_s)} ({len(TRAIN_s)})  TEST {_d(TEST_s)} ({len(TEST_s)})")
    print(f"[exit-sweep] grid: {len(SL_SWEEP)} SL x {len(TGT_SWEEP)} Tgt = {len(SL_SWEEP)*len(TGT_SWEEP)} combos per base")

    md = [f"# EXIT_SWEEP_RESULTS — {setup} (LONG)", "",
          f"Focused **SL% × target%** sweep, net of cost @ {args.slippage_bps:.0f} bps/leg. "
          "Detection (and, for the *card* base, the card pre-momentum gates) held fixed; only the exit varies. "
          "Reuses the repo backtest pipeline. No live trades; nothing written to final_setup_conf.py.", "",
          f"- TRAIN {_d(TRAIN_s)} ({len(TRAIN_s)} sessions) · TEST {_d(TEST_s)} ({len(TEST_s)} sessions)",
          f"- Band targets: TRAIN PF ∈ [{BAND_LO},{BAND_HI}], TEST PF > {TEST_PF_MIN}, day/sym dominance ≤ {DOM_CAP}, "
          f"TRAIN n ≥ {MIN_TR}, TEST n ≥ {MIN_TE}.", ""]

    for kind in ("raw", "card"):
        base = _base(kind)
        rows = []
        for sl in SL_SWEEP:
            for tgt in TGT_SWEEP:
                cfg = dict(base); cfg["sl"] = sl; cfg["tgt"] = tgt
                tr_n, tr_pf, _ = pb._light(setup, cfg, TRAIN)
                te_n, te_pf, _ = pb._light(setup, cfg, TEST)
                fi_n, fi_pf, _ = pb._light(setup, cfg, FIT)
                va_n, va_pf, _ = pb._light(setup, cfg, VAL)
                rows.append({"sl": sl, "tgt": tgt, "train_n": tr_n, "train_pf": round(tr_pf, 3),
                             "test_n": te_n, "test_pf": round(te_pf, 3),
                             "fit_n": fi_n, "fit_pf": round(fi_pf, 3), "val_n": va_n, "val_pf": round(va_pf, 3)})
        df = pd.DataFrame(rows)
        df.to_csv(outdir / f"exit_sweep_{kind}.csv", index=False)

        def _piv(col):
            p = df.pivot(index="sl", columns="tgt", values=col)
            return p

        # PF matrices (TRAIN, TEST) as markdown
        md += [f"## Base: {kind.upper()}" + ("  (detection only — pure exit effect, full sample)" if kind == "raw"
               else "  (card pre-mom gates kept: pre_entry_momentum_score≥79 & sig5_adx_calc≥28)"), ""]
        for col, lbl in (("train_pf", "TRAIN net PF"), ("test_pf", "TEST net PF")):
            p = _piv(col)
            hdr = "| SL \\ Tgt | " + " | ".join(f"{c:g}" for c in p.columns) + " |"
            sep = "|" + "---|" * (len(p.columns) + 1)
            md.append(f"**{lbl}** (rows = SL%, cols = target%)")
            md.append(hdr); md.append(sep)
            for sl_v, r in p.iterrows():
                md.append(f"| **{sl_v:g}** | " + " | ".join(("-" if pd.isna(v) else f"{v:g}") for v in r) + " |")
            md.append("")
        # trade-count reference (TRAIN n, TEST n) for the same grid
        for col, lbl in (("train_n", "TRAIN trades"), ("test_n", "TEST trades")):
            p = _piv(col)
            hdr = "| SL \\ Tgt | " + " | ".join(f"{c:g}" for c in p.columns) + " |"
            sep = "|" + "---|" * (len(p.columns) + 1)
            md.append(f"**{lbl}** (rows = SL%, cols = target%)")
            md.append(hdr); md.append(sep)
            for sl_v, r in p.iterrows():
                md.append(f"| **{sl_v:g}** | " + " | ".join(("-" if pd.isna(v) else f"{int(v)}") for v in r) + " |")
            md.append("")

        # ---- best matches (with full metrics for stability) ----
        # (1) best by TEST PF among meaningful samples; (2) best TRAIN PF inside the band.
        viable = df[(df["train_n"] >= MIN_TR) & (df["test_n"] >= MIN_TE)].copy()
        md.append("### Best matches")
        if viable.empty:
            md.append(f"- _No exit combo has TRAIN n≥{MIN_TR} AND TEST n≥{MIN_TE} on the {kind} base._")
        picks = []
        if not viable.empty:
            picks.append(("highest TEST PF (TRAIN n≥%d, TEST n≥%d)" % (MIN_TR, MIN_TE),
                          viable.sort_values("test_pf", ascending=False).iloc[0]))
            inband = viable[(viable["train_pf"] >= BAND_LO) & (viable["train_pf"] <= BAND_HI)]
            if not inband.empty:
                picks.append(("TRAIN PF in band, best TEST PF",
                              inband.sort_values("test_pf", ascending=False).iloc[0]))
            picks.append(("highest TRAIN PF", viable.sort_values("train_pf", ascending=False).iloc[0]))
        seen_keys = set()
        for why, r in picks:
            key = (r["sl"], r["tgt"])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            cfg = dict(base); cfg["sl"] = float(r["sl"]); cfg["tgt"] = float(r["tgt"])
            mt = pb.full_metrics(setup, cfg, TRAIN); me = pb.full_metrics(setup, cfg, TEST)
            gate = (BAND_LO <= mt["net_pf"] <= BAND_HI and me["net_pf"] > TEST_PF_MIN and
                    mt["trades"] >= MIN_TR and me["trades"] >= MIN_TE and me["net_pnl"] > 0 and
                    (me["day_dom"] is not None and me["day_dom"] <= DOM_CAP) and
                    (mt["day_dom"] is not None and mt["day_dom"] <= DOM_CAP))
            md += [f"- **{why}** → SL {r['sl']:g} / Tgt {r['tgt']:g}  "
                   f"(FIT PF {r['fit_pf']:g} n{int(r['fit_n'])}, VAL PF {r['val_pf']:g} n{int(r['val_n'])})",
                   f"  - TRAIN: n={mt['trades']} PF={mt['net_pf']} net=Rs{mt['net_pnl']:,.0f} win={mt['win_rate_pct']}% "
                   f"t/s/e={mt['tgt_cnt']}/{mt['sl_cnt']}/{mt['eod_cnt']} dayDom={mt['day_dom']} symDom={mt['sym_dom']} maxDD=Rs{mt['max_drawdown']:,.0f}",
                   f"  - TEST : n={me['trades']} PF={me['net_pf']} net=Rs{me['net_pnl']:,.0f} win={me['win_rate_pct']}% "
                   f"t/s/e={me['tgt_cnt']}/{me['sl_cnt']}/{me['eod_cnt']} dayDom={me['day_dom']} symDom={me['sym_dom']}",
                   f"  - selection gate: **{'PASS' if gate else 'FAIL'}**"]
        md.append("")
        # console
        print(f"\n[exit-sweep] base={kind}: best TEST PF rows (TRAIN n>={MIN_TR}, TEST n>={MIN_TE}):")
        if viable.empty:
            print("   (none meet the trade floors)")
        else:
            for _, r in viable.sort_values("test_pf", ascending=False).head(5).iterrows():
                print(f"   SL{r['sl']:g}/Tgt{r['tgt']:g}  TRAIN n{int(r['train_n'])} PF{r['train_pf']:g} | "
                      f"TEST n{int(r['test_n'])} PF{r['test_pf']:g}")

    (outdir / "EXIT_SWEEP_RESULTS.md").write_text("\n".join(md), encoding="utf-8")
    print(f"\n[exit-sweep] wrote {outdir/'EXIT_SWEEP_RESULTS.md'} + exit_sweep_raw.csv / exit_sweep_card.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
