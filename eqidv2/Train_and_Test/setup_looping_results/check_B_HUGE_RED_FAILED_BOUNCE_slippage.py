from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
TRAIN_DIR = HERE.parent
REPO_ROOT = TRAIN_DIR.parent
for p in (str(REPO_ROOT), str(TRAIN_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

import setup_train_test as tt  # noqa: E402


SETUP = "B_HUGE_RED_FAILED_BOUNCE"
POOL_DIR = Path(r"C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_RED_FAILED_BOUNCE")
TRAIN = ("2026-05-25", "2026-06-05")
TEST = ("2026-06-08", "2026-06-12")
CONFIG = {
    SETUP: {
        "sl": 0.90,
        "tgt": 1.25,
        "mask_terms": [],
        "premom_terms": [
            ("pre3_close_pos", "<=", 0.581797),
            ("sig5_rsi_dir", "<=", 64.104659),
            ("pre5_mom_r", "<=", 0.284145),
        ],
        "guard": None,
        "status": "OK",
    }
}


def _clear_caches() -> None:
    for name in ("_entry", "_resolve_full", "_premom"):
        fn = getattr(tt, name, None)
        if hasattr(fn, "cache_clear"):
            fn.cache_clear()


def _metrics(label: str, df: pd.DataFrame) -> dict[str, Any]:
    m = tt.eval_family(CONFIG, df)
    detail = tt.book_detail(m["book"], {SETUP: (0.90, 1.25)}) if m["trades"] else pd.DataFrame()
    net = detail["net_pnl_rs"].to_numpy(dtype=float) if not detail.empty else np.array([], dtype=float)
    pos = net[net > 0]
    neg = net[net < 0]
    return {
        "period": label,
        "trades": int(m["trades"]),
        "net_pf": round(float(m["net_pf"]), 4) if math.isfinite(float(m["net_pf"])) else None,
        "net_pnl_rs": round(float(net.sum()), 2) if len(net) else 0.0,
        "win_rate_pct": round(float((net > 0).mean()) * 100, 2) if len(net) else 0.0,
        "gross_profit_rs": round(float(pos.sum()), 2) if len(pos) else 0.0,
        "gross_loss_rs": round(float(-neg.sum()), 2) if len(neg) else 0.0,
        "outcomes": detail["outcome"].astype(str).value_counts().to_dict() if not detail.empty else {},
        "daywise": (
            detail.groupby("trade_date")["net_pnl_rs"].agg(["size", "sum"]).reset_index().to_dict("records")
            if not detail.empty
            else []
        ),
    }


def main() -> int:
    out: list[dict[str, Any]] = []
    for slippage in (5.0, 15.0):
        _clear_caches()
        tt.SLIPPAGE_BPS = slippage
        tt.POOL_DIRS = [POOL_DIR]
        tt.POOL_DIR = POOL_DIR
        tt.TRAIN = TRAIN
        tt.TEST = TEST
        pool = tt.load_pool()
        pool = pool[pool["setup"].astype(str).eq(SETUP)].copy()
        tr, te = tt.split_train_test(pool)
        tr = tt.attach_entries(tr)
        te = tt.attach_entries(te)
        out.append(
            {
                "slippage_bps_per_leg": slippage,
                "train": _metrics("TRAIN", tr),
                "test": _metrics("TEST", te),
            }
        )
    path = HERE / f"{SETUP}_rolling_slippage_check.json"
    path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print(json.dumps(out, indent=2, default=str))
    print(f"[slippage] wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
