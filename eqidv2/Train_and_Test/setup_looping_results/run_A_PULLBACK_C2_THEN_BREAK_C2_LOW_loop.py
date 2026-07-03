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


SETUP = "A_PULLBACK_C2_THEN_BREAK_C2_LOW"
SIDE = "SHORT"
POOL_DIR = Path(r"C:\TradingData\eqidv2\outputs_ID_v11_unified_pool")
TRAIN = ("2026-05-25", "2026-06-05")
TEST = ("2026-06-08", "2026-06-12")
MIN_TRAIN_FOR_TEST = 30
BASELINE_SL = 1.20
BASELINE_TGT = 1.50


@dataclass(frozen=True)
class Variant:
    iteration: int
    group: str
    name: str
    hypothesis: str
    sl: float = BASELINE_SL
    tgt: float = BASELINE_TGT
    mask_terms: tuple[tuple[str, str, Any], ...] = ()
    premom_terms: tuple[tuple[str, str, float], ...] = ()
    guard: dict[str, Any] | None = None


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
    counts_book, counts = _apply_until_book(df, cfg)
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


def _variants() -> list[Variant]:
    return [
        Variant(0, "baseline", "current_conf_raw_1p20_1p50", "Current research-watch config: raw detection only, no guard, no premom."),
        Variant(1, "exit", "tighter_scalp_0p70_1p00", "Cut volatility noise with tighter stop and target.", 0.70, 1.00),
        Variant(2, "exit", "balanced_0p85_1p00", "Former v6-like exit may reduce EOD bleed.", 0.85, 1.00),
        Variant(3, "exit", "balanced_0p90_1p25", "Moderate stop/target for pullback continuation.", 0.90, 1.25),
        Variant(4, "exit", "small_target_1p20_1p00", "Keep wider stop but take smaller continuation move.", 1.20, 1.00),
        Variant(5, "exit", "runner_1p20_2p00", "Let stronger breaks run instead of exiting too early.", 1.20, 2.00),
        Variant(6, "time", "min_0945", "Avoid first noisy post-open signals.", guard={"min_slot": "09:45"}),
        Variant(7, "time", "min_1000", "Require opening churn to settle before shorting.", guard={"min_slot": "10:00"}),
        Variant(8, "time", "max_1230", "Avoid afternoon reversals and EOD drift.", mask_terms=(("signal_minute", "<=", 750.0),)),
        Variant(9, "time", "max_1300", "Keep only first half plus early afternoon.", mask_terms=(("signal_minute", "<=", 780.0),)),
        Variant(10, "trend", "market_aligned_le_0", "Short only when market return is not positive.", mask_terms=(("market_ret_pct", "<=", 0.0),)),
        Variant(11, "trend", "market_down_le_neg_0p05", "Require explicit weak tape.", mask_terms=(("market_ret_pct", "<=", -0.05),)),
        Variant(12, "trend", "market_abs_le_0p56", "Avoid large index impulse/gap-noise days.", mask_terms=(("market_abs_ret_pct", "<=", 0.56),)),
        Variant(13, "trend", "rs_lagger_le_neg_0p50", "Require stock to lag the market.", mask_terms=(("rs_pct", "<=", -0.50),)),
        Variant(14, "trend", "rs_lagger_le_neg_1p00", "Require stronger relative weakness.", mask_terms=(("rs_pct", "<=", -1.00),)),
        Variant(15, "volume", "vol_min_2p0", "Need conviction volume beyond raw 1.4 threshold.", mask_terms=(("vol_ratio", ">=", 2.0),)),
        Variant(16, "volume", "vol_min_3p0", "Require high volume to avoid weak breaks.", mask_terms=(("vol_ratio", ">=", 3.0),)),
        Variant(17, "volume", "vol_band_1p8_4p0", "Avoid both weak breaks and exhaustion spikes.", mask_terms=(("vol_ratio", ">=", 1.8), ("vol_ratio", "<=", 4.0))),
        Variant(18, "candle", "close_loc_le_0p20", "Demand close near low to reduce fake breaks.", mask_terms=(("close_loc", "<=", 0.20),)),
        Variant(19, "candle", "close_loc_le_0p10", "Stricter close near low.", mask_terms=(("close_loc", "<=", 0.10),)),
        Variant(20, "candle", "body_ge_0p75", "Demand decisive red body.", mask_terms=(("body_pct", ">=", 0.75),)),
        Variant(21, "candle", "body_ge_0p85", "Stricter decisive red body.", mask_terms=(("body_pct", ">=", 0.85),)),
        Variant(22, "candle", "range_ge_0p45", "Avoid tiny/noisy break candles.", mask_terms=(("signal_range_pct", ">=", 0.45),)),
        Variant(23, "volatility", "atr_le_0p0030", "Avoid high ATR chop/gap noise.", mask_terms=(("atr_pct", "<=", 0.0030),)),
        Variant(24, "volatility", "atr_band_0p0020_0p0035", "Keep moderate volatility only.", mask_terms=(("atr_pct", ">=", 0.0020), ("atr_pct", "<=", 0.0035))),
        Variant(25, "vwap", "vwap_not_extended_ge_neg4", "Avoid shorts that are already too stretched below VWAP.", mask_terms=(("vwap_dist_atr", ">=", -4.0),)),
        Variant(26, "vwap", "vwap_below_le_neg1p8", "Require meaningful VWAP loss.", mask_terms=(("vwap_dist_atr", "<=", -1.8),)),
        Variant(27, "vwap", "vwap_band_neg4_to_neg1p5", "Keep decisive but not exhausted VWAP loss.", mask_terms=(("vwap_dist_atr", ">=", -4.0), ("vwap_dist_atr", "<=", -1.5))),
        Variant(28, "quality", "quality_ge_75", "Drop low scanner-quality breaks.", mask_terms=(("quality_score", ">=", 75.0),)),
        Variant(29, "quality", "quality_ge_90", "Higher quality threshold.", mask_terms=(("quality_score", ">=", 90.0),)),
        Variant(30, "quality", "quality_ge_105", "Very high quality threshold seen in train search.", mask_terms=(("quality_score", ">=", 105.0),)),
        Variant(31, "quality", "quality_ge_123p76", "Extreme quality gate from automated TRAIN search.", mask_terms=(("quality_score", ">=", 123.7606),)),
        Variant(32, "confirmation", "sig5_adx_ge_20", "Require ADX confirmation at signal.", premom_terms=(("sig5_adx_calc", ">=", 20.0),)),
        Variant(33, "confirmation", "sig5_adx_ge_25", "Stronger ADX confirmation.", premom_terms=(("sig5_adx_calc", ">=", 25.0),)),
        Variant(34, "confirmation", "pre5_mom_ge_0p10", "Require short-side pre-entry momentum thrust.", premom_terms=(("pre5_mom_r", ">=", 0.10),)),
        Variant(35, "confirmation_on_quality", "quality_123p76_plus_sig5_adx_21p47", "Second-stage confirmation on the TRAIN-discovered high-quality subset.", mask_terms=(("quality_score", ">=", 123.7606),), premom_terms=(("sig5_adx_calc", ">=", 21.4683),)),
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
    baseline_train_pf = None
    for v in _variants():
        train_row, train_detail = _metrics("TRAIN", v, tr)
        if v.iteration == 0:
            baseline_train_pf = float(train_row["net_pf"] or 0.0)
            test_row, test_detail = _metrics("TEST", v, te)
            train_row["decision"] = "BASELINE"
            test_row["decision"] = "BASELINE"
            rows.extend([train_row, test_row])
            details[str(v.iteration)] = {
                "train_detail": train_detail.to_dict("records"),
                "test_detail": test_detail.to_dict("records"),
            }
            continue

        train_improves = (float(train_row["net_pf"] or 0.0) > float(baseline_train_pf or 0.0)) and (
            int(train_row["trades"]) >= MIN_TRAIN_FOR_TEST
        )
        if train_improves:
            test_row, test_detail = _metrics("TEST", v, te)
            collapse = (
                int(test_row["trades"]) < 5
                or float(test_row["net_pf"] or 0.0) < 0.80
                or float(test_row["net_pnl_rs"]) < -5000.0
            )
            decision = "KEEP_CANDIDATE" if not collapse else "REJECT_TEST_COLLAPSE"
            train_row["decision"] = "TRAIN_PASS_TESTED"
            test_row["decision"] = decision
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
