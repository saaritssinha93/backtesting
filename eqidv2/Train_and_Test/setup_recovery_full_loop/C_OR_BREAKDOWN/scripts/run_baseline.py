r"""run_baseline.py — Stage 1: baseline evaluation for C_OR_BREAKDOWN (research-only).

Evaluates on FIT/VAL/TRAIN/TEST:
  raw_detector : detection only, promoted exit 0.90/2.00
  conf_gate    : + promoted premom gate sig5_adx_calc>=39.670518 & pre1_adx<=21.368044
  conf_gate_alt: same gate, exit_alt 0.90/1.50
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[2] / "_shared"))
import recovery_lib as rl  # noqa: E402

SETUP = "C_OR_BREAKDOWN"
WORK = HERE.parents[1]

CFGS = {
    "raw_detector": {"sl": 0.90, "tgt": 2.00, "mask_terms": [], "premom_terms": [], "guard": None,
                     "max_positions": 20, "daily_loss_rs": 0.0},
    "conf_gate": {"sl": 0.90, "tgt": 2.00, "mask_terms": [],
                  "premom_terms": [("sig5_adx_calc", ">=", 39.670518), ("pre1_adx", "<=", 21.368044)],
                  "guard": None, "max_positions": 20, "daily_loss_rs": 0.0},
    "conf_gate_alt_exit": {"sl": 0.90, "tgt": 1.50, "mask_terms": [],
                           "premom_terms": [("sig5_adx_calc", ">=", 39.670518), ("pre1_adx", "<=", 21.368044)],
                           "guard": None, "max_positions": 20, "daily_loss_rs": 0.0},
}


def main() -> int:
    eng = rl.ResearchEngine(SETUP, WORK)
    w = eng.w
    out = {"setup": SETUP, "windows": {k: [w["sessions"][k][0], w["sessions"][k][-1], len(w["sessions"][k])]
                                       for k in ("FIT", "VAL", "TRAIN", "TEST")},
           "results": {}}
    for name, cfg in CFGS.items():
        res = {}
        for wn in ("FIT", "VAL", "TRAIN", "TEST"):
            m = eng.eval_cfg(cfg, w[wn], wname=wn, day_block=(wn in ("TRAIN", "TEST")))
            res[wn] = {k: v for k, v in m.items() if k != "detail"}
            print(f"[{name}] {wn:5s} {rl.mline(m)}", flush=True)
        out["results"][name] = {"cfg": {k: (list(map(list, v)) if isinstance(v, list) else v)
                                        for k, v in cfg.items()}, "metrics": res}
    (WORK / "baseline_result.json").write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"wrote {WORK / 'baseline_result.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
