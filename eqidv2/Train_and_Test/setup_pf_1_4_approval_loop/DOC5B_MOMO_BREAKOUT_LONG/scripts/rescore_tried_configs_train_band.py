from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd


HERE = Path(__file__).resolve()
SETUP_DIR = HERE.parents[1]
TT_DIR = SETUP_DIR.parents[1]
REPO = TT_DIR.parent
ENGINE_DIR = TT_DIR / "setup_pf_1_4_approval_loop" / "_engine"
for p in (REPO, TT_DIR, ENGINE_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import setup_train_test as tt  # noqa: E402
import pf_band_fitval_loop as eng  # noqa: E402


SETUP = "DOC5B_MOMO_BREAKOUT_LONG"
TRAIN_START = pd.Timestamp("2026-05-18")
TEST_START = pd.Timestamp("2026-06-20")
PF_LO = 1.30
PF_HI = 1.70
TEST_PF_MIN = 1.40
TERM_RE = re.compile(r"^([^<>]+)(<=|>=)([-+]?\d+(?:\.\d+)?)$")


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
        terms.append((m.group(1), m.group(2), float(m.group(3))))
    return terms


def parse_guard(text: str):
    text = str(text or "").strip()
    if not text or text == "-":
        return None
    return json.loads(text)


def cfg_from_row(row):
    return {
        "sl": float(row["sl"]),
        "tgt": float(row["tgt"]),
        "mask_terms": parse_terms(row.get("mask", "-")),
        "premom_terms": parse_terms(row.get("premom", "-")),
        "guard": parse_guard(row.get("guard", "-")),
        "status": "OK",
        "max_positions": int(float(row.get("max_positions", 20) or 20)),
        "daily_loss_rs": float(row.get("daily_loss_rs", 0.0) or 0.0),
    }


def cfg_key(cfg):
    return json.dumps(
        {
            "sl": cfg["sl"],
            "tgt": cfg["tgt"],
            "mask_terms": cfg["mask_terms"],
            "premom_terms": cfg["premom_terms"],
            "guard": cfg["guard"] or {},
            "max_positions": cfg["max_positions"],
            "daily_loss_rs": cfg["daily_loss_rs"],
        },
        sort_keys=True,
    )


def eval_metrics(cfg, df, full: bool = False):
    tt.MAX_POSITIONS = int(cfg.get("max_positions") or 20)
    tt.DAILY_LOSS_RS = float(cfg.get("daily_loss_rs") or 0.0)
    fn = eng.full_metrics if full else eng.fast_metrics
    m = fn(SETUP, cfg, df)
    return {k: v for k, v in m.items() if k != "detail"}


def fmt_cfg(cfg):
    block = eng.cfg_to_conf_block(SETUP, "LONG", cfg)
    return json.dumps(block, sort_keys=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool", default=str(TT_DIR / "doc5_long_setups" / "pool"))
    ap.add_argument("--out", default=str(SETUP_DIR / "deep_train_band_rescore"))
    ap.add_argument("--min_train_trades", type=int, default=20)
    ap.add_argument("--min_test_trades", type=int, default=6)
    ap.add_argument("--pf_lo", type=float, default=PF_LO)
    ap.add_argument("--pf_hi", type=float, default=PF_HI)
    ap.add_argument("--test_pf_min", type=float, default=TEST_PF_MIN)
    ap.add_argument("trials", nargs="*", default=[
        str(SETUP_DIR / "trials.csv"),
        str(SETUP_DIR / "deep_runs" / "mask2_pm2_seed11" / SETUP / "trials.csv"),
    ])
    args = ap.parse_args()

    tt.POOL_DIRS = [str(Path(args.pool).resolve())]
    tt.SLIPPAGE_BPS = 15.0
    tt.MAX_POSITIONS = 20
    tt.DAILY_LOSS_RS = 0.0
    tt._entry.cache_clear()
    tt._resolve_full.cache_clear()
    tt._premom.cache_clear()

    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).str.upper().eq(SETUP)].copy()
    train_raw = pool[(pool["_day"] >= TRAIN_START) & (pool["_day"] < TEST_START)].reset_index(drop=True)
    test_raw = pool[pool["_day"] >= TEST_START].reset_index(drop=True)
    train = tt.attach_entries(train_raw)
    test = tt.attach_entries(test_raw)

    cfgs = {}
    source_rows = 0
    for path_s in args.trials:
        path = Path(path_s)
        if not path.exists():
            continue
        tdf = pd.read_csv(path)
        source_rows += len(tdf)
        for _, row in tdf.iterrows():
            cfg = cfg_from_row(row)
            cfgs.setdefault(cfg_key(cfg), cfg)

    rows = []
    train_band = []
    for i, cfg in enumerate(cfgs.values(), 1):
        mtr = eval_metrics(cfg, train, full=False)
        row = {
            "idx": i,
            "config": fmt_cfg(cfg),
            "train_n": mtr["n"],
            "train_pf": mtr["net_pf"],
            "train_net": mtr["net_pnl"],
            "train_win_rate": mtr["win_rate"],
            "train_trades_per_day": mtr["trades_per_day"],
        }
        rows.append(row)
        if mtr["n"] >= args.min_train_trades and args.pf_lo <= float(mtr["net_pf"]) <= args.pf_hi:
            train_band.append((cfg, mtr, row))

    confirmed = []
    for cfg, mtr_fast, row in train_band:
        mtr = eval_metrics(cfg, train, full=True)
        if not (mtr["n"] >= args.min_train_trades and args.pf_lo <= float(mtr["net_pf"]) <= args.pf_hi):
            continue
        mte = eval_metrics(cfg, test, full=True)
        rec = dict(row)
        rec.update({
            "train_pf_full": mtr["net_pf"],
            "train_n_full": mtr["n"],
            "train_net_full": mtr["net_pnl"],
            "train_day_block_p": mtr["day_block_p"],
            "train_trade_dom": mtr["trade_dom_gross"],
            "train_day_dom": mtr["day_dom"],
            "train_sym_dom": mtr["sym_dom"],
            "test_pf": mte["net_pf"],
            "test_n": mte["n"],
            "test_net": mte["net_pnl"],
            "test_day_block_p": mte["day_block_p"],
            "test_trade_dom": mte["trade_dom_gross"],
            "test_day_dom": mte["day_dom"],
            "test_sym_dom": mte["sym_dom"],
            "passes_pf": bool(mte["n"] >= args.min_test_trades and float(mte["net_pf"]) > args.test_pf_min),
        })
        confirmed.append(rec)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).sort_values(["train_pf", "train_n"], ascending=[False, False]).to_csv(
        out / "all_tried_configs_train_rescore.csv", index=False
    )
    confirmed_df = pd.DataFrame(confirmed)
    if not confirmed_df.empty:
        confirmed_df = confirmed_df.sort_values(["test_pf", "train_pf_full"], ascending=[False, False])
    else:
        confirmed_df = pd.DataFrame(columns=[
            "idx", "config", "train_n", "train_pf", "train_net", "train_pf_full",
            "train_n_full", "train_net_full", "test_pf", "test_n", "test_net",
            "passes_pf",
        ])
    confirmed_df.to_csv(out / "train_band_candidates_tested.csv", index=False)

    best = max(rows, key=lambda r: (float(r["train_pf"]), int(r["train_n"]))) if rows else None
    passing = [r for r in confirmed if r["passes_pf"]]
    summary = {
        "setup": SETUP,
        "trial_rows_read": source_rows,
        "unique_configs_rescored_on_train": len(cfgs),
        "train_entries": len(train),
        "test_entries": len(test),
        "min_train_trades": args.min_train_trades,
        "min_test_trades": args.min_test_trades,
        "pf_lo": args.pf_lo,
        "pf_hi": args.pf_hi,
        "test_pf_min": args.test_pf_min,
        "train_band_fast_count": len(train_band),
        "train_band_confirmed_count": len(confirmed),
        "test_pf_passing_count": len(passing),
        "best_train_fast": best,
        "best_confirmed": confirmed[0] if confirmed else None,
        "passing": passing,
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    lines = [
        f"# {SETUP} - Tried Config Rescore",
        "",
        "Rescored every unique config logged by the 1x1 and 2x2 FIT/VAL searches on full TRAIN first.",
        f"TEST was evaluated only for configs whose full TRAIN PF remained inside {args.pf_lo:.2f}-{args.pf_hi:.2f} "
        f"with at least {args.min_train_trades} trades.",
        "",
        f"- Trial rows read: {source_rows}",
        f"- Unique configs TRAIN-rescored: {len(cfgs)}",
        f"- Fast TRAIN-band configs: {len(train_band)}",
        f"- Full-confirmed TRAIN-band configs tested on TEST: {len(confirmed)}",
        f"- TEST PF > 1.40 configs: {len(passing)}",
        "",
    ]
    if best:
        lines += [
            "## Best TRAIN Rescore",
            "",
            f"- TRAIN fast PF {best['train_pf']} over {best['train_n']} trades, net Rs{best['train_net']:,.0f}.",
            f"- Config: `{best['config']}`",
            "",
        ]
    if confirmed:
        top = confirmed[0]
        lines += [
            "## Best Confirmed Train-Band Candidate",
            "",
            f"- TRAIN PF {top['train_pf_full']} over {top['train_n_full']} trades, net Rs{top['train_net_full']:,.0f}.",
            f"- TEST PF {top['test_pf']} over {top['test_n']} trades, net Rs{top['test_net']:,.0f}.",
            f"- Config: `{top['config']}`",
            "",
        ]
    else:
        lines += ["## Best Confirmed Train-Band Candidate", "", "- None.", ""]
    (out / "SUMMARY.md").write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
