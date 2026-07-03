from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

import run_E_VWAP_LOSE_EARLY_SHORT_loop as base


SETUP = base.SETUP
POOL_DIR = Path(r"C:\TradingData\eqidv2\setup_pools_2026_06_29\E_VWAP_LOSE_EARLY_SHORT")
TRAIN = ("2026-04-27", "2026-06-05")
TEST = ("2026-06-08", "2026-06-12")
OUT_SUFFIX = "6wk_loop"
MIN_ABS_TRAIN_FOR_TEST = 8


def main() -> int:
    base.POOL_DIR = POOL_DIR
    base.TRAIN = TRAIN
    base.TEST = TEST
    base.tt.POOL_DIRS = [POOL_DIR]
    base.tt.POOL_DIR = POOL_DIR
    base.tt.TRAIN = TRAIN
    base.tt.TEST = TEST
    print(f"[loop] setup={SETUP}")
    print(f"[loop] pool={POOL_DIR}")
    print(f"[loop] TRAIN {TRAIN[0]}..{TRAIN[1]} TEST {TEST[0]}..{TEST[1]}")
    pool = base.tt.load_pool()
    pool = pool[pool["setup"].astype(str).eq(SETUP)].copy()
    tr, te = base.tt.split_train_test(pool)
    print(f"[loop] pool rows train={len(tr)} test={len(te)}")
    tr = base.tt.attach_entries(tr)
    te = base.tt.attach_entries(te)
    print(f"[loop] entry rows train={len(tr)} test={len(te)}")

    rows: list[dict[str, Any]] = []
    details: dict[str, dict[str, Any]] = {}
    baseline_train_pf = 0.0
    baseline_train_trades = 0
    min_train_for_test = MIN_ABS_TRAIN_FOR_TEST
    for v in base._variants():
        train_row, train_detail = base._metrics("TRAIN", v, tr)
        if v.iteration == 0:
            baseline_train_pf = float(train_row["net_pf"] or 0.0)
            baseline_train_trades = int(train_row["trades"])
            min_train_for_test = max(MIN_ABS_TRAIN_FOR_TEST, int(math.ceil(baseline_train_trades * 0.50)))
            test_row, test_detail = base._metrics("TEST", v, te)
            train_row["decision"] = "BASELINE"
            test_row["decision"] = "BASELINE"
            rows.extend([train_row, test_row])
            details[str(v.iteration)] = {
                "train_detail": train_detail.to_dict("records"),
                "test_detail": test_detail.to_dict("records"),
            }
            print(
                f"[loop] baseline train n={train_row['trades']} pf={train_row['net_pf']} "
                f"test n={test_row['trades']} pf={test_row['net_pf']} min_train_for_test={min_train_for_test}"
            )
            continue

        train_pf = float(train_row["net_pf"] or 0.0)
        train_improves = (train_pf > baseline_train_pf) and (int(train_row["trades"]) >= min_train_for_test)
        if train_improves:
            test_row, test_detail = base._metrics("TEST", v, te)
            collapse = (
                int(test_row["trades"]) < 5
                or float(test_row["net_pf"] or 0.0) < 0.80
                or float(test_row["net_pnl_rs"]) < -2500.0
            )
            train_row["decision"] = "TRAIN_PASS_TESTED"
            test_row["decision"] = "KEEP_CANDIDATE" if not collapse else "REJECT_TEST_COLLAPSE"
            rows.extend([train_row, test_row])
            details[str(v.iteration)] = {
                "train_detail": train_detail.to_dict("records"),
                "test_detail": test_detail.to_dict("records"),
            }
        else:
            train_row["decision"] = "REJECT_TRAIN_NO_IMPROVE_OR_TOO_FEW"
            rows.append(train_row)
            details[str(v.iteration)] = {"train_detail": train_detail.to_dict("records"), "test_detail": []}
        print(
            f"[loop] iter={v.iteration:02d} {v.name}: train n={train_row['trades']} "
            f"pf={train_row['net_pf']} net={train_row['net_pnl_rs']} decision={train_row['decision']}"
        )

    out = pd.DataFrame(rows)
    out_csv = base.HERE / f"{SETUP}_{OUT_SUFFIX}_metrics.csv"
    out_json = base.HERE / f"{SETUP}_{OUT_SUFFIX}_details.json"
    out.to_csv(out_csv, index=False)
    out_json.write_text(json.dumps(details, indent=2, default=str), encoding="utf-8")
    print(f"[loop] wrote {out_csv}")
    print(f"[loop] wrote {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
