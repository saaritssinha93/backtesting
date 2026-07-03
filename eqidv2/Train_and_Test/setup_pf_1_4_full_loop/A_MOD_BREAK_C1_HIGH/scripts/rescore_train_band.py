r"""rescore_train_band.py — strict TRAIN-first audit of every config the Optuna
seeds tried for A_MOD_BREAK_C1_HIGH.

Rescores every unique tried config on FULL TRAIN; only configs whose full-TRAIN PF
lands in [--pf_lo, --pf_hi] with n >= --min_train_trades are evaluated ONCE on TEST.
Windows are CLI args (no hardcoding). Mirrors the approval-loop rescore pattern.

Usage:
  py -3.12 rescore_train_band.py --pool <dir> --out <dir> \
      --train_start 2026-03-01 --test_start 2026-06-01 \
      [--pf_lo 1.30 --pf_hi 1.80 --test_pf_min 1.40] <trials.csv> [more trials.csv]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

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

SETUP = "A_MOD_BREAK_C1_HIGH"
TERM_RE = re.compile(r"^([^<>]+?)(<=|>=|==|!=)(.+)$")


def parse_terms(text: str):
    text = str(text or "").strip()
    if not text or text == "-":
        return []
    terms = []
    for part in text.split(";"):
        part = part.strip()
        if not part:
            continue
        m = TERM_RE.match(part)
        if not m:
            raise ValueError(f"cannot parse term: {part!r}")
        feat, op, val = m.group(1), m.group(2), m.group(3)
        try:
            val = float(val)
        except ValueError:
            val = str(val).strip()
        terms.append((feat, op, val))
    return terms


def parse_guard(text: str):
    text = str(text or "").strip()
    if not text or text == "-":
        return None
    return json.loads(text)


def cfg_from_row(row):
    return {
        "sl": float(row["sl"]), "tgt": float(row["tgt"]),
        "mask_terms": parse_terms(row.get("mask", "-")),
        "premom_terms": parse_terms(row.get("premom", "-")),
        "guard": parse_guard(row.get("guard", "-")),
        "status": "OK",
        "max_positions": int(float(row.get("max_positions", 20) or 20)),
        "daily_loss_rs": float(row.get("daily_loss_rs", 0.0) or 0.0),
    }


def cfg_key(cfg):
    return json.dumps({k: cfg[k] for k in ("sl", "tgt", "mask_terms", "premom_terms",
                                           "guard", "max_positions", "daily_loss_rs")},
                      sort_keys=True, default=str)


def eval_metrics(cfg, df, full=False):
    tt.MAX_POSITIONS = int(cfg.get("max_positions") or 20)
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs") or 0.0)
    fn = eng.full_metrics if full else eng.fast_metrics
    m = fn(SETUP, cfg, df)
    return {k: v for k, v in m.items() if k != "detail"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(WORK / "pools" / "pool_full"))
    ap.add_argument("--out", default=str(WORK / "rescore_train_band"))
    ap.add_argument("--train_start", default="2026-03-01")
    ap.add_argument("--test_start", default="2026-06-01")
    ap.add_argument("--min_train_trades", type=int, default=20)
    ap.add_argument("--min_test_trades", type=int, default=6)
    ap.add_argument("--pf_lo", type=float, default=1.30)
    ap.add_argument("--pf_hi", type=float, default=1.80)
    ap.add_argument("--test_pf_min", type=float, default=1.40)
    ap.add_argument("trials", nargs="+")
    args = ap.parse_args()

    tt.POOL_DIRS = [str(Path(args.pool).resolve())]
    tt.SLIPPAGE_BPS = 15.0
    tt.MAX_POSITIONS = 20
    tt.DAILY_LOSS_RS = 0.0
    tt._entry.cache_clear(); tt._resolve_full.cache_clear(); tt._premom.cache_clear()

    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.upper().eq(SETUP)].copy()
    tr_s, te_s = pd.Timestamp(args.train_start), pd.Timestamp(args.test_start)
    train = tt.attach_entries(pool[(pool["_day"] >= tr_s) & (pool["_day"] < te_s)].reset_index(drop=True))
    test = tt.attach_entries(pool[pool["_day"] >= te_s].reset_index(drop=True))

    cfgs = {}
    source_rows = 0
    for path_s in args.trials:
        path = Path(path_s)
        if not path.exists():
            print(f"[rescore] WARN missing {path}")
            continue
        tdf = pd.read_csv(path)
        source_rows += len(tdf)
        for _, row in tdf.iterrows():
            cfg = cfg_from_row(row)
            cfgs.setdefault(cfg_key(cfg), cfg)

    rows, band = [], []
    for i, cfg in enumerate(cfgs.values(), 1):
        m = eval_metrics(cfg, train, full=False)
        rec = {"idx": i, "config": cfg_key(cfg), "train_n": m["n"], "train_pf": m["net_pf"],
               "train_net": m["net_pnl"], "train_win_rate": m["win_rate"],
               "train_trades_per_day": m["trades_per_day"]}
        rows.append(rec)
        if m["n"] >= args.min_train_trades and args.pf_lo <= float(m["net_pf"]) <= args.pf_hi:
            band.append((cfg, rec))

    confirmed = []
    for cfg, rec in band:
        mtr = eval_metrics(cfg, train, full=True)
        if not (mtr["n"] >= args.min_train_trades and args.pf_lo <= float(mtr["net_pf"]) <= args.pf_hi):
            continue
        mte = eval_metrics(cfg, test, full=True)
        out = dict(rec)
        out.update({
            "train_pf_full": mtr["net_pf"], "train_n_full": mtr["n"], "train_net_full": mtr["net_pnl"],
            "train_day_block_p": mtr["day_block_p"], "train_trade_dom": mtr["trade_dom_gross"],
            "train_day_dom": mtr["day_dom"], "train_sym_dom": mtr["sym_dom"],
            "train_target_rate": mtr.get("target_rate"),
            "test_pf": mte["net_pf"], "test_n": mte["n"], "test_net": mte["net_pnl"],
            "test_day_block_p": mte["day_block_p"], "test_trade_dom": mte["trade_dom_gross"],
            "test_day_dom": mte["day_dom"], "test_sym_dom": mte["sym_dom"],
            "passes": bool(mte["n"] >= args.min_test_trades and float(mte["net_pf"]) > args.test_pf_min
                           and float(mte["net_pnl"]) > 0),
        })
        confirmed.append(out)

    outd = Path(args.out); outd.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).sort_values(["train_pf", "train_n"], ascending=[False, False]).to_csv(
        outd / "all_tried_configs_train_rescore.csv", index=False)
    pd.DataFrame(confirmed).to_csv(outd / "train_band_candidates_tested.csv", index=False)

    passing = [r for r in confirmed if r["passes"]]
    # best meaningful config regardless of band, for reporting
    big = [r for r in rows if r["train_n"] >= args.min_train_trades and pd.notna(r["train_pf"])
           and r["train_pf"] not in (float("inf"),)]
    best_big = max(big, key=lambda r: float(r["train_pf"])) if big else None
    summary = {
        "setup": SETUP, "trial_rows_read": source_rows,
        "unique_configs": len(cfgs),
        "train_entries": len(train), "test_entries": len(test),
        "windows": {"train_start": args.train_start, "test_start": args.test_start},
        "gate": {"pf_lo": args.pf_lo, "pf_hi": args.pf_hi, "test_pf_min": args.test_pf_min,
                 "min_train_trades": args.min_train_trades, "min_test_trades": args.min_test_trades},
        "train_band_count": len(band), "confirmed_count": len(confirmed),
        "passing_count": len(passing),
        "best_train_at_min_n": best_big,
        "confirmed": confirmed,
        "passing": passing,
    }
    (outd / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("unique_configs", "train_entries", "test_entries",
                                              "train_band_count", "confirmed_count", "passing_count",
                                              "best_train_at_min_n")}, indent=2, default=str))
    print(f"[rescore] wrote {outd}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
