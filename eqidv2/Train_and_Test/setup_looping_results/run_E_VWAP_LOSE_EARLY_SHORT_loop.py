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


SETUP = "E_VWAP_LOSE_EARLY_SHORT"
SIDE = "SHORT"
POOL_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool")
TRAIN = ("2026-05-25", "2026-06-05")
TEST = ("2026-06-08", "2026-06-12")

BASELINE_SL = 0.70
BASELINE_TGT = 1.00
BASELINE_MASK = (("vol_ratio", ">=", 1.8), ("vol_ratio", "<=", 3.2))
BASELINE_GUARD = {"min_slot": "09:45"}
MIN_ABS_TRAIN_FOR_TEST = 8


@dataclass(frozen=True)
class Variant:
    iteration: int
    group: str
    name: str
    hypothesis: str
    sl: float = BASELINE_SL
    tgt: float = BASELINE_TGT
    mask_terms: tuple[tuple[str, str, Any], ...] = BASELINE_MASK
    premom_terms: tuple[tuple[str, str, float], ...] = ()
    guard: dict[str, Any] | None = None


def _guard(v: Variant) -> dict[str, Any] | None:
    return BASELINE_GUARD if v.guard is None else v.guard


def _config(v: Variant) -> dict[str, dict[str, Any]]:
    return {
        SETUP: {
            "sl": float(v.sl),
            "tgt": float(v.tgt),
            "mask_terms": [tuple(t) for t in v.mask_terms],
            "premom_terms": [tuple(t) for t in v.premom_terms],
            "guard": _guard(v),
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
        "guard": json.dumps(_guard(v) or {}),
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


def _base_plus(*terms: tuple[str, str, Any]) -> tuple[tuple[str, str, Any], ...]:
    return BASELINE_MASK + tuple(terms)


def _variants() -> list[Variant]:
    return [
        Variant(0, "baseline", "current_conf_vol_band_min0945_0p70_1p00", "Current config as documented: volume band, min 09:45, no pre-momentum."),
        Variant(1, "exit", "tight_scalp_0p50_0p60", "Reduce noise exposure with smaller stop and smaller target.", 0.50, 0.60),
        Variant(2, "exit", "tight_scalp_0p60_0p80", "Tighter stop with realistic quick continuation target.", 0.60, 0.80),
        Variant(3, "exit", "baseline_stop_smaller_target_0p70_0p80", "Keep stop and take profits sooner.", 0.70, 0.80),
        Variant(4, "exit", "wider_stop_same_target_0p85_1p00", "Allow normal VWAP retest noise before target.", 0.85, 1.00),
        Variant(5, "exit", "same_stop_runner_0p70_1p20", "Keep tight invalidation but demand more payoff.", 0.70, 1.20),
        Variant(6, "exit", "wider_runner_0p90_1p25", "Test whether larger move pays for more stop room.", 0.90, 1.25),
        Variant(7, "time", "min_0950", "Avoid immediate post-09:45 churn.", guard={"min_slot": "09:50"}),
        Variant(8, "time", "min_1000", "Let open volatility settle before VWAP-loss shorts.", guard={"min_slot": "10:00"}),
        Variant(9, "time", "max_1130", "Avoid late fade/reversal behavior.", mask_terms=_base_plus(("signal_minute", "<=", 690.0))),
        Variant(10, "time", "max_1230", "Keep only morning plus first lunch transition.", mask_terms=_base_plus(("signal_minute", "<=", 750.0))),
        Variant(11, "time", "window_0950_to_1130", "Combine delayed start and morning-only cap.", mask_terms=_base_plus(("signal_minute", "<=", 690.0)), guard={"min_slot": "09:50"}),
        Variant(12, "volume", "drop_volume_band_raw_guarded", "Check whether the volume band is actually carrying the edge.", mask_terms=()),
        Variant(13, "volume", "vol_band_1p8_2p8", "Exclude possible exhaustion spikes above the old upper band.", mask_terms=(("vol_ratio", ">=", 1.8), ("vol_ratio", "<=", 2.8))),
        Variant(14, "volume", "vol_band_2p0_3p2", "Require more conviction while keeping old upper cap.", mask_terms=(("vol_ratio", ">=", 2.0), ("vol_ratio", "<=", 3.2))),
        Variant(15, "volume", "vol_band_2p0_2p8", "Middle conviction band only.", mask_terms=(("vol_ratio", ">=", 2.0), ("vol_ratio", "<=", 2.8))),
        Variant(16, "volume", "vol_band_1p5_3p2", "Relax lower volume threshold to improve sample stability.", mask_terms=(("vol_ratio", ">=", 1.5), ("vol_ratio", "<=", 3.2))),
        Variant(17, "volume", "vol_band_1p8_4p0", "Relax upper cap in case strong breaks were over-filtered.", mask_terms=(("vol_ratio", ">=", 1.8), ("vol_ratio", "<=", 4.0))),
        Variant(18, "trend", "market_aligned_le_0", "Short only when index is not positive.", mask_terms=_base_plus(("market_ret_pct", "<=", 0.0))),
        Variant(19, "trend", "market_down_le_neg_0p05", "Require explicit weak tape.", mask_terms=_base_plus(("market_ret_pct", "<=", -0.05))),
        Variant(20, "trend", "market_abs_le_0p56", "Avoid index impulse/gap noise.", mask_terms=_base_plus(("market_abs_ret_pct", "<=", 0.56))),
        Variant(21, "trend", "rs_lagger_le_neg_0p25", "Require stronger stock-relative weakness.", mask_terms=_base_plus(("rs_pct", "<=", -0.25))),
        Variant(22, "trend", "rs_lagger_le_neg_0p50", "Require decisive stock-relative weakness.", mask_terms=_base_plus(("rs_pct", "<=", -0.50))),
        Variant(23, "vwap", "overlay_vwap_only_ge_neg1p25", "Mirror the overlay's vwap_dist_atr rule without the conf volume band.", mask_terms=(("vwap_dist_atr", ">=", -1.25),)),
        Variant(24, "vwap", "vol_band_plus_vwap_ge_neg1p25", "Add not-too-extended VWAP loss guard.", mask_terms=_base_plus(("vwap_dist_atr", ">=", -1.25))),
        Variant(25, "vwap", "vol_band_plus_vwap_ge_neg1p00", "Stricter not-too-extended VWAP guard.", mask_terms=_base_plus(("vwap_dist_atr", ">=", -1.00))),
        Variant(26, "vwap", "vol_band_plus_vwap_le_neg0p20", "Require actual distance below VWAP, not marginal loss.", mask_terms=_base_plus(("vwap_dist_atr", "<=", -0.20))),
        Variant(27, "vwap", "vol_band_vwap_band_neg1p25_to_neg0p20", "Require loss below VWAP but avoid over-extension.", mask_terms=_base_plus(("vwap_dist_atr", ">=", -1.25), ("vwap_dist_atr", "<=", -0.20))),
        Variant(28, "candle", "close_loc_le_0p25", "Demand close nearer low to reduce fake breaks.", mask_terms=_base_plus(("close_loc", "<=", 0.25))),
        Variant(29, "candle", "close_loc_0p08_to_0p25", "Avoid both weak close and extreme low-tick exhaustion.", mask_terms=_base_plus(("close_loc", ">=", 0.08), ("close_loc", "<=", 0.25))),
        Variant(30, "candle", "body_ge_0p65", "Require decisive red body.", mask_terms=_base_plus(("body_pct", ">=", 0.65))),
        Variant(31, "candle", "body_ge_0p75", "Stricter decisive red body.", mask_terms=_base_plus(("body_pct", ">=", 0.75))),
        Variant(32, "volatility", "atr_le_0p0060", "Stay under the raw early gate's high-volatility zone.", mask_terms=_base_plus(("atr_pct", "<=", 0.0060))),
        Variant(33, "volatility", "atr_le_0p0045", "Avoid volatility noise.", mask_terms=_base_plus(("atr_pct", "<=", 0.0045))),
        Variant(34, "volatility", "atr_band_0p0020_0p0060", "Keep moderate volatility only.", mask_terms=_base_plus(("atr_pct", ">=", 0.0020), ("atr_pct", "<=", 0.0060))),
        Variant(35, "quality", "quality_ge_60", "Drop lowest scanner-quality breaks.", mask_terms=_base_plus(("quality_score", ">=", 60.0))),
        Variant(36, "quality", "quality_ge_80", "Higher scanner-quality threshold.", mask_terms=_base_plus(("quality_score", ">=", 80.0))),
        Variant(37, "quality", "quality_ge_100", "Very high quality only.", mask_terms=_base_plus(("quality_score", ">=", 100.0))),
        Variant(38, "confirmation", "sig5_adx_ge_20", "Require ADX trend confirmation.", premom_terms=(("sig5_adx_calc", ">=", 20.0),)),
        Variant(39, "confirmation", "sig5_adx_ge_25", "Stronger ADX trend confirmation.", premom_terms=(("sig5_adx_calc", ">=", 25.0),)),
        Variant(40, "confirmation", "sig5_vol_ratio20_ge_1p56", "Reuse old live pre-momentum volume confirmation only.", premom_terms=(("sig5_vol_ratio20", ">=", 1.5643),)),
        Variant(41, "confirmation", "old_live_premom_gate", "Check old entry-engine gate as a single confirmation group.", premom_terms=(("sig5_vol_ratio20", ">=", 1.5643), ("pre3_body_sum_r", "<=", 0.797498))),
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
    out_csv = HERE / f"{SETUP}_loop_metrics.csv"
    out_json = HERE / f"{SETUP}_loop_details.json"
    out.to_csv(out_csv, index=False)
    out_json.write_text(json.dumps(details, indent=2, default=str), encoding="utf-8")
    print(f"[loop] wrote {out_csv}")
    print(f"[loop] wrote {out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
