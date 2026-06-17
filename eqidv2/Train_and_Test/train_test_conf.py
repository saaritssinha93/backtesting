"""train_test_conf.py — evaluate the final_setup_conf book on the dynamic train/test window.

Uses each setup's EXISTING conf gate (mask_terms + pre_momentum_terms + exit + entry_guards)
— NO re-search. Reuses setup_train_test's exact entry/exit/cost pipeline, so the numbers
match how the tuner judges. Reports per-setup TRAIN vs TEST (trades / net PF / net PnL,
net of cost) and the combined book (with cross-setup family dedupe).

Window = train_test_window (TEST=last 2w, TRAIN=3mo before) unless pinned.
Run from repo root:  py -3.12 Train_and_Test\train_test_conf.py [--pool_dir <dir>] [--setups A,B,..]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
for _p in (str(_HERE.parent), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import setup_train_test as tt          # noqa: E402  (reuse load_pool/attach_entries/eval_family/...)
import final_setup_conf as fc          # noqa: E402
import eqidv2_final_conf_live_bootstrap as boot   # noqa: E402  (readmit provenance = pool basis)

DEFAULT_POOL = r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool"


def _conf_to_config(cfg: dict) -> dict:
    ex = cfg.get("exit", {})
    return {
        "sl": float(ex.get("sl_pct")), "tgt": float(ex.get("tgt_pct")),
        "mask_terms": [tuple(t) for t in cfg.get("mask_terms", [])],
        "premom_terms": [tuple(t) for t in cfg.get("pre_momentum_terms", [])],
        "guard": (cfg.get("entry_guards") or None),
        "status": "OK",
    }


def _m(d: dict) -> tuple[int, float, float, float]:
    return int(d.get("trades", 0)), float(d.get("net_pf", 0.0)), float(d.get("net_pnl", 0.0)), float(d.get("day_block_p", float("nan")))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pool_dir", default=DEFAULT_POOL)
    ap.add_argument("--setups", default="", help="comma list to restrict (default: all conf setups)")
    args = ap.parse_args()
    try:
        sys.stdout.reconfigure(line_buffering=True)   # stream per-setup rows live
    except Exception:
        pass
    tt.POOL_DIRS = [Path(args.pool_dir)]
    tt.POOL_DIR = Path(args.pool_dir)

    configs = {name: _conf_to_config(cfg) for name, cfg in fc.FINAL_SETUP_CONF.items()}
    if args.setups:
        want = {s.strip().upper() for s in args.setups.split(",") if s.strip()}
        configs = {k: v for k, v in configs.items() if k in want}
    conf_setups = set(configs)

    print(f"[conf train/test] window: TRAIN {tt.TRAIN[0]}..{tt.TRAIN[1]}  TEST {tt.TEST[0]}..{tt.TEST[1]}")
    print(f"[conf train/test] {len(configs)} setups | pool={args.pool_dir}")
    pool = tt.load_pool()
    pool = pool[pool["setup"].isin(conf_setups)].copy()
    tr, te = tt.split_train_test(pool)
    tr = tt.attach_entries(tr)
    te = tt.attach_entries(te)
    print(f"[conf train/test] resolved entries: train={len(tr)} test={len(te)}\n")

    readmit = set(boot.readmit_setups())   # 10 non-native; raw IS their live basis -> faithful here
    hdr = f"{'setup':34} | {'basis':7} | {'side':5} | {'TRAIN n/PF/netRs':>26} | {'TEST n/PF/netRs':>26}"
    print(hdr); print("-" * len(hdr))
    for name in sorted(configs):
        side = str(fc.FINAL_SETUP_CONF[name].get("side", ""))[:5]
        basis = "readmit" if name in readmit else "native*"   # native* = raw/ungated here (NOT live-faithful)
        ntr, pftr, nettr, _ = _m(tt.eval_family({name: configs[name]}, tr))
        nte, pfte, nette, dbp = _m(tt.eval_family({name: configs[name]}, te))
        te_note = "" if nte else "  <-- no TEST candidates (tier-c data ends 05-29)"
        print(f"{name:34} | {basis:7} | {side:5} | {ntr:4d} / {pftr:5.2f} / {nettr:>10,.0f} | {nte:4d} / {pfte:5.2f} / {nette:>10,.0f}{te_note}")

    print("-" * len(hdr))
    btr_n, btr_pf, btr_net, _ = _m(tt.eval_family(configs, tr))
    bte_n, bte_pf, bte_net, bte_dbp = _m(tt.eval_family(configs, te))
    print(f"{'BOOK (family-deduped)':34} | {'':5} | {btr_n:4d} / {btr_pf:5.2f} / {btr_net:>10,.0f} | "
          f"{bte_n:4d} / {bte_pf:5.2f} / {bte_net:>10,.0f}")
    print(f"\noos/is PF ratio = {(bte_pf / btr_pf if btr_pf > 0 else 0):.2f} | test day_block_p = {bte_dbp:.3f}")
    print("Net of cost (v6 model). Per-setup = isolated; BOOK = all setups with cross-setup one-ticker/day dedupe.")
    print("basis: readmit = raw IS the live basis (bypasses v8/research) -> FAITHFUL. "
          "native* = raw/ungated here; LIVE filters through v8/research first, so native* numbers are\n"
          "         NOT live-representative (pessimistic firehose) -- use the v11 conf backtest for faithful native numbers.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
