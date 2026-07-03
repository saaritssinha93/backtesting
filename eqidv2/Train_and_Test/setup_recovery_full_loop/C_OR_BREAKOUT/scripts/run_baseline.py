r"""run_baseline.py — Stage 1: baseline evaluation for C_OR_BREAKOUT (research-only).

No config of record exists (never promoted; pooled PF-band campaign found no June OOS
edge). Baselines = raw detector at three representative exits.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[2] / "_shared"))
import recovery_lib as rl  # noqa: E402

SETUP = "C_OR_BREAKOUT"
WORK = HERE.parents[1]

CFGS = {
    "raw_wide": {"sl": 0.90, "tgt": 2.00, "mask_terms": [], "premom_terms": [], "guard": None,
                 "max_positions": 20, "daily_loss_rs": 0.0},
    "raw_mid": {"sl": 0.90, "tgt": 1.25, "mask_terms": [], "premom_terms": [], "guard": None,
                "max_positions": 20, "daily_loss_rs": 0.0},
    "raw_tight": {"sl": 0.70, "tgt": 1.00, "mask_terms": [], "premom_terms": [], "guard": None,
                  "max_positions": 20, "daily_loss_rs": 0.0},
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
        out["results"][name] = {"cfg": dict(cfg), "metrics": res}
    (WORK / "baseline_result.json").write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(f"wrote {WORK / 'baseline_result.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
