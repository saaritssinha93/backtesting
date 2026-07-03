from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass
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
SIDE = "SHORT"
POOL_DIR = Path(r"C:\TradingData\eqidv2\setup_pools_2026_06_29\B_HUGE_RED_FAILED_BOUNCE")
TRAIN = ("2026-05-25", "2026-06-05")
TEST = ("2026-06-08", "2026-06-12")

BASELINE_SL = 0.90
BASELINE_TGT = 1.25
BASELINE_MASK: tuple[tuple[str, str, Any], ...] = ()
BASELINE_PREMOM = (
    ("pre3_close_pos", "<=", 0.581797),
    ("sig5_rsi_dir", "<=", 64.104659),
    ("pre5_mom_r", "<=", 0.284145),
)
BASELINE_GUARD: dict[str, Any] | None = None
MIN_ABS_TRAIN_FOR_TEST = 6


@dataclass(frozen=True)
class Variant:
    iteration: int
    group: str
    name: str
    hypothesis: str
    sl: float = BASELINE_SL
    tgt: float = BASELINE_TGT
    mask_terms: tuple[tuple[str, str, Any], ...] = BASELINE_MASK
    premom_terms: tuple[tuple[str, str, float], ...] = BASELINE_PREMOM
    guard: dict[str, Any] | None = BASELINE_GUARD


def _config(v: Variant) -> dict[str, dict[str, Any]]:
    return {
        SETUP: {
            "sl": float(v.sl),
            "tgt": float(v.tgt),
            "mask_terms": [tuple(t) for t in v.mask_terms],
            "premom_terms": [tuple(t) for t in v.premom_terms],
            "guard": v.guard,
            "status": "OK",
        }
    }


def _finite(x: float) -> float | None:
    return float(x) if isinstance(x, (int, float, np.floating)) and math.isfinite(float(x)) else None


def _max_drawdown(net: np.ndarray) -> float:
    net = np.asarray(net, dtype=float)
    net = net[np.isfinite(net)]
    if not len(net):
        return 0.0
    curve = np.cumsum(net)
    peak = np.maximum.accumulate(np.r_[0.0, curve])[:-1]
    dd = curve - peak
    return float(dd.min()) if len(dd) else 0.0


def _apply_until_book(df: pd.DataFrame, cfg: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, int]]:
    rows = df[df["setup"] == SETUP].copy()
    counts = {"entry_rows": int(len(rows))}
    rows = tt.apply_guards(rows, cfg.get("guard"))
    counts["after_guard"] = int(len(rows))
    rows = tt.apply_premom_terms(rows, cfg.get("premom_terms", []), float(cfg["sl"]))
    counts["after_premom"] = int(len(rows))
    deduped = tt.dedupe_family(rows)
    counts["after_dedupe"] = int(len(deduped))
    masked = tt.apply_mask_terms(deduped, cfg.get("mask_terms", []))
    counts["after_mask"] = int(len(masked))
    return masked, counts


def _metrics(label: str, v: Variant, df: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    cfg = _config(v)[SETUP]
    m = tt.eval_family(_config(v), df)
    book = m["book"].copy()
    _counts_book, counts = _apply_until_book(df, cfg)
    detail = tt.book_detail(book, {SETUP: (float(v.sl), float(v.tgt))}) if int(m["trades"]) else pd.DataFrame()
    net = detail["net_pnl_rs"].to_numpy(dtype=float) if not detail.empty else np.array([], dtype=float)
    pos = net[net > 0]
    neg = net[net < 0]
    if not detail.empty and "reason" in book.columns:
        reason = (
            pd.DataFrame({"reason": book.reset_index(drop=True)["reason"].astype(str), "net": net})
            .groupby("reason")["net"]
            .agg(["size", "sum"])
            .reset_index()
            .to_dict("records")
        )
    else:
        reason = []
    row = {
        "period": label,
        "iteration": v.iteration,
        "group": v.group,
        "name": v.name,
        "hypothesis": v.hypothesis,
        "sl": float(v.sl),
        "tgt": float(v.tgt),
        "mask_terms": json.dumps([list(t) for t in v.mask_terms]),
        "premom_terms": json.dumps([list(t) for t in v.premom_terms]),
        "guard": json.dumps(v.guard or {}),
        "trades": int(m["trades"]),
        "win_rate_pct": round(float((net > 0).mean()) * 100, 2) if len(net) else 0.0,
        "gross_profit_rs": round(float(pos.sum()), 2) if len(pos) else 0.0,
        "gross_loss_rs": round(float(-neg.sum()), 2) if len(neg) else 0.0,
        "net_pnl_rs": round(float(net.sum()), 2) if len(net) else 0.0,
        "net_pf": round(float(m["net_pf"]), 4) if math.isfinite(float(m["net_pf"])) else None,
        "avg_win_rs": round(float(pos.mean()), 2) if len(pos) else 0.0,
        "avg_loss_rs": round(float(neg.mean()), 2) if len(neg) else 0.0,
        "max_drawdown_rs": round(_max_drawdown(net), 2),
        "day_block_p": _finite(float(m["day_block_p"])),
        "entry_rows": counts["entry_rows"],
        "after_guard": counts["after_guard"],
        "after_premom": counts["after_premom"],
        "after_dedupe": counts["after_dedupe"],
        "after_mask": counts["after_mask"],
        "reason_split": json.dumps(reason, default=str),
        "daywise": json.dumps(
            detail.groupby("trade_date")["net_pnl_rs"].agg(["size", "sum"]).reset_index().to_dict("records")
            if not detail.empty
            else [],
            default=str,
        ),
        "symbolwise": json.dumps(
            detail.groupby("ticker")["net_pnl_rs"].agg(["size", "sum"]).sort_values("sum").reset_index().to_dict("records")
            if not detail.empty
            else [],
            default=str,
        ),
        "outcome_split": json.dumps(detail["outcome"].astype(str).value_counts().to_dict() if not detail.empty else {}),
    }
    return row, detail


def _base_mask_plus(*terms: tuple[str, str, Any]) -> tuple[tuple[str, str, Any], ...]:
    return BASELINE_MASK + tuple(terms)


def _base_premom_plus(*terms: tuple[str, str, float]) -> tuple[tuple[str, str, float], ...]:
    return BASELINE_PREMOM + tuple(terms)


def _variants() -> list[Variant]:
    return [
        Variant(0, "baseline", "current_conf_premom_0p90_1p25", "Current conf gate: no mask, three pre-momentum terms, exit 0.90/1.25."),
        Variant(1, "exit", "same_gate_0p90_1p00", "Tighter target to reduce EOD bleed.", tgt=1.00),
        Variant(2, "exit", "same_gate_0p90_1p50", "Let true failed-bounces run.", tgt=1.50),
        Variant(3, "exit", "same_gate_0p70_1p00", "Tighter stop and target for scalp realism.", sl=0.70, tgt=1.00),
        Variant(4, "exit", "same_gate_0p70_1p25", "Tighter stop while preserving target.", sl=0.70, tgt=1.25),
        Variant(5, "exit", "same_gate_1p10_1p25", "Allow stop noise around the failed bounce.", sl=1.10, tgt=1.25),
        Variant(6, "exit", "same_gate_1p20_1p50", "Wider risk/reward continuation profile.", sl=1.20, tgt=1.50),
        Variant(7, "premom_dropout", "raw_no_premom_gate", "Check if current gate is load-bearing.", premom_terms=()),
        Variant(8, "premom_dropout", "drop_pre3_close_pos", "Remove bounce-position constraint only.", premom_terms=(("sig5_rsi_dir", "<=", 64.104659), ("pre5_mom_r", "<=", 0.284145))),
        Variant(9, "premom_dropout", "drop_sig5_rsi_dir", "Remove RSI-direction constraint only.", premom_terms=(("pre3_close_pos", "<=", 0.581797), ("pre5_mom_r", "<=", 0.284145))),
        Variant(10, "premom_dropout", "drop_pre5_mom_r", "Remove pre5 momentum refinement only.", premom_terms=(("pre3_close_pos", "<=", 0.581797), ("sig5_rsi_dir", "<=", 64.104659))),
        Variant(11, "premom_threshold", "pre3_close_pos_le_0p45", "Demand weaker bounce close position.", premom_terms=(("pre3_close_pos", "<=", 0.45), ("sig5_rsi_dir", "<=", 64.104659), ("pre5_mom_r", "<=", 0.284145))),
        Variant(12, "premom_threshold", "pre3_close_pos_le_0p70", "Relax close-position gate for sample stability.", premom_terms=(("pre3_close_pos", "<=", 0.70), ("sig5_rsi_dir", "<=", 64.104659), ("pre5_mom_r", "<=", 0.284145))),
        Variant(13, "premom_threshold", "sig5_rsi_dir_le_55", "Require stronger short-side RSI direction.", premom_terms=(("pre3_close_pos", "<=", 0.581797), ("sig5_rsi_dir", "<=", 55.0), ("pre5_mom_r", "<=", 0.284145))),
        Variant(14, "premom_threshold", "sig5_rsi_dir_le_60", "Slightly stronger RSI direction.", premom_terms=(("pre3_close_pos", "<=", 0.581797), ("sig5_rsi_dir", "<=", 60.0), ("pre5_mom_r", "<=", 0.284145))),
        Variant(15, "premom_threshold", "pre5_mom_r_le_0p10", "Require flatter/failed pre-entry momentum.", premom_terms=(("pre3_close_pos", "<=", 0.581797), ("sig5_rsi_dir", "<=", 64.104659), ("pre5_mom_r", "<=", 0.10))),
        Variant(16, "premom_threshold", "pre5_mom_r_le_0p00", "Require no favorable pre-entry short momentum already spent.", premom_terms=(("pre3_close_pos", "<=", 0.581797), ("sig5_rsi_dir", "<=", 64.104659), ("pre5_mom_r", "<=", 0.00))),
        Variant(17, "confirmation", "add_pre1_adx_ge_30", "Require trend/volatility confirmation before entry.", premom_terms=_base_premom_plus(("pre1_adx", ">=", 30.0))),
        Variant(18, "confirmation", "add_pre1_adx_ge_39", "Stronger ADX confirmation from prior audit lead.", premom_terms=_base_premom_plus(("pre1_adx", ">=", 39.0))),
        Variant(19, "confirmation", "add_sig5_adx_ge_25", "Require signal-bar ADX confirmation.", premom_terms=_base_premom_plus(("sig5_adx_calc", ">=", 25.0))),
        Variant(20, "confirmation", "add_sig5_adx_ge_35", "Stronger signal-bar ADX confirmation.", premom_terms=_base_premom_plus(("sig5_adx_calc", ">=", 35.0))),
        Variant(21, "quality", "quality_le_37p6", "Re-test prior low-quality failed-bounce lead.", mask_terms=_base_mask_plus(("quality_score", "<=", 37.6))),
        Variant(22, "quality", "quality_le_60", "Looser low-quality failed-bounce filter.", mask_terms=_base_mask_plus(("quality_score", "<=", 60.0))),
        Variant(23, "quality", "quality_ge_60", "Opposite quality tail check.", mask_terms=_base_mask_plus(("quality_score", ">=", 60.0))),
        Variant(24, "volume", "vol_ratio_ge_2p0", "Require conviction volume on the failed bounce.", mask_terms=_base_mask_plus(("vol_ratio", ">=", 2.0))),
        Variant(25, "volume", "vol_ratio_ge_3p0", "Require stronger volume spike.", mask_terms=_base_mask_plus(("vol_ratio", ">=", 3.0))),
        Variant(26, "candle", "upper_wick_le_0p05", "Avoid rejection shapes with too much upper wick noise.", mask_terms=_base_mask_plus(("upper_wick_pct", "<=", 0.05))),
        Variant(27, "candle", "body_ge_0p70", "Require decisive red body.", mask_terms=_base_mask_plus(("body_pct", ">=", 0.70))),
        Variant(28, "time", "max_1130", "Avoid afternoon failed-bounce reversals.", guard={"max_slot": "11:30"}),
        Variant(29, "time", "max_1230", "Morning plus first lunch transition only.", guard={"max_slot": "12:30"}),
        Variant(30, "trend", "market_aligned_le_0", "Short only when index return is non-positive.", mask_terms=_base_mask_plus(("market_ret_pct", "<=", 0.0))),
        Variant(31, "trend", "market_down_le_neg_0p05", "Require weak market tape.", mask_terms=_base_mask_plus(("market_ret_pct", "<=", -0.05))),
        Variant(32, "trend", "market_abs_le_0p56", "Avoid broad index impulse/gap noise.", mask_terms=_base_mask_plus(("market_abs_ret_pct", "<=", 0.56))),
        Variant(33, "rs", "rs_lagger_le_0", "Require stock not leading market.", mask_terms=_base_mask_plus(("rs_pct", "<=", 0.0))),
        Variant(34, "rs", "rs_lagger_le_neg_0p50", "Require decisive stock-relative weakness.", mask_terms=_base_mask_plus(("rs_pct", "<=", -0.50))),
        Variant(35, "overfit_check", "quality_37p6_upperwick_0_pre1adx39_1p20_1p50", "Re-test prior maxpf overfit shape as one signal-quality group.", sl=1.20, tgt=1.50, mask_terms=(("quality_score", "<=", 37.6), ("upper_wick_pct", "<=", 0.0)), premom_terms=_base_premom_plus(("pre1_adx", ">=", 39.0))),
    ]


def main() -> int:
    tt.POOL_DIRS = [POOL_DIR]
    tt.POOL_DIR = POOL_DIR
    tt.TRAIN = TRAIN
    tt.TEST = TEST
    print(f"[loop] setup={SETUP}")
    print(f"[loop] pool={POOL_DIR}")
    print(f"[loop] TRAIN {TRAIN[0]}..{TRAIN[1]} TEST {TEST[0]}..{TEST[1]}")
    pool = tt.load_pool()
    pool = pool[pool["setup"].astype(str).eq(SETUP)].copy()
    tr, te = tt.split_train_test(pool)
    print(f"[loop] pool rows train={len(tr)} test={len(te)}")
    tr = tt.attach_entries(tr)
    te = tt.attach_entries(te)
    print(f"[loop] entry rows train={len(tr)} test={len(te)}")

    rows: list[dict[str, Any]] = []
    details: dict[str, dict[str, Any]] = {}
    baseline_train_pf = 0.0
    baseline_train_trades = 0
    min_train_for_test = MIN_ABS_TRAIN_FOR_TEST
    for v in _variants():
        train_row, train_detail = _metrics("TRAIN", v, tr)
        if v.iteration == 0:
            baseline_train_pf = float(train_row["net_pf"] or 0.0)
            baseline_train_trades = int(train_row["trades"])
            min_train_for_test = max(MIN_ABS_TRAIN_FOR_TEST, int(math.ceil(baseline_train_trades * 0.50)))
            test_row, test_detail = _metrics("TEST", v, te)
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
            test_row, test_detail = _metrics("TEST", v, te)
            collapse = (
                int(test_row["trades"]) < 3
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
    out_csv = HERE / f"{SETUP}_rolling_loop_metrics.csv"
    out_json = HERE / f"{SETUP}_rolling_loop_details.json"
    out.to_csv(out_csv, index=False)
    out_json.write_text(json.dumps(details, indent=2, default=str), encoding="utf-8")
    print(f"[loop] wrote {out_csv}")
    print(f"[loop] wrote {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
