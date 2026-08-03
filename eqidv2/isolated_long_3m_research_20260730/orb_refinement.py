"""Honest structural refinement of the rejected opening-range breakout family.

The search uses two chronological TRAIN halves, then VALIDATION, and touches
the existing TEST segment only after freezing one rule. The family-level TEST
result is already known, so TEST is a confirmation segment rather than a
pristine never-seen holdout.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd

import pullback_refinement as common
import research as r


OUT = r.OUT / "orb_refinement"
RNG = np.random.default_rng(20260730)


@dataclass(frozen=True)
class SignalConfig:
    name: str
    variant: str
    start_minute: int
    end_minute: int
    regime: str
    min_traded_value: float
    atr_low: float
    atr_high: float
    min_breakout_extension: float
    max_breakout_extension: float
    max_avwap_extension: float
    max_range_atr: float
    max_upper_wick: float
    adx_low: float
    adx_high: float
    rsi_low: float
    rsi_high: float
    relative_volume_min: float
    close_location_min: float
    score_min: int
    top_n_per_slot: int


ANCHOR_EXITS = [
    r.ExitConfig(target, stop, hold)
    for target, stop, hold in (
        (.25, .35, 10), (.25, .50, 20), (.35, .45, 20),
        (.35, .60, 30), (.45, .45, 20), (.45, .60, 30),
        (.45, .75, 40), (.55, .55, 30), (.55, .70, 40),
        (.65, .60, 30), (.65, .75, 40), (.65, .90, 50),
        (.75, .75, 40), (.85, .90, 50),
    )
]

FULL_EXITS = [
    r.ExitConfig(target, stop, hold)
    for target in (.25, .35, .45, .55, .65, .75, .85)
    for stop in (.35, .45, .55, .65, .75, .85, .95)
    for hold in (10, 20, 30, 40, 50, 60)
]


def configs(count: int = 3000) -> list[SignalConfig]:
    windows = (
        (575, 615), (575, 630), (585, 630), (585, 645),
        (600, 645), (600, 660), (615, 675), (630, 690),
        (575, 660), (585, 690),
    )
    out: list[SignalConfig] = []
    for i in range(count):
        atr_low = float(RNG.choice((.15, .20, .25, .30, .35)))
        atr_high = float(RNG.choice((.55, .65, .75, .90, 1.10)))
        if atr_high <= atr_low + .20:
            atr_high = min(1.10, atr_low + .40)
        min_ext = float(RNG.choice((.00, .03, .06, .10, .15)))
        max_ext = float(RNG.choice((.25, .35, .50, .65, .80)))
        if max_ext <= min_ext + .10:
            max_ext = min(.80, min_ext + .30)
        adx_low = float(RNG.choice((10, 12, 14, 16, 18, 20, 22)))
        adx_high = float(RNG.choice((28, 32, 36, 40, 44, 48)))
        if adx_high <= adx_low + 8:
            adx_high = adx_low + 12
        rsi_low = float(RNG.choice((48, 50, 52, 54, 56)))
        rsi_high = float(RNG.choice((66, 68, 70, 72, 74, 78)))
        if rsi_high <= rsi_low + 10:
            rsi_high = rsi_low + 14
        start, end = windows[int(RNG.integers(0, len(windows)))]
        variant = str(RNG.choice(("or15", "or20", "or30")))
        earliest = {"or15": 575, "or20": 580, "or30": 590}[variant]
        start = max(start, earliest)
        out.append(SignalConfig(
            name=f"orb_refine_{i:04d}",
            variant=variant,
            start_minute=start,
            end_minute=end,
            regime=str(RNG.choice(("all", "not_bearish", "bullish"))),
            min_traded_value=float(RNG.choice((1_000_000, 2_500_000, 5_000_000, 10_000_000))),
            atr_low=atr_low,
            atr_high=atr_high,
            min_breakout_extension=min_ext,
            max_breakout_extension=max_ext,
            max_avwap_extension=float(RNG.choice((.30, .45, .60, .75, .90, 1.10))),
            max_range_atr=float(RNG.choice((.80, 1.10, 1.40, 1.80, 2.20, 2.70))),
            max_upper_wick=float(RNG.choice((.10, .20, .30, .40, .50))),
            adx_low=adx_low,
            adx_high=adx_high,
            rsi_low=rsi_low,
            rsi_high=rsi_high,
            relative_volume_min=float(RNG.choice((.70, .90, 1.10, 1.25, 1.50, 1.80, 2.20))),
            close_location_min=float(RNG.choice((.55, .60, .65, .70, .75))),
            score_min=int(RNG.choice((5, 6, 7, 8))),
            top_n_per_slot=int(RNG.choice((0, 5, 10, 20, 40))),
        ))
    return out


def selected_indices(entries: pd.DataFrame, cfg: SignalConfig) -> np.ndarray:
    x = entries
    breakout_extension = (x["close"] / x["break_level"] - 1.0) * 100.0
    score = (
        x["adx"].between(cfg.adx_low, cfg.adx_high).astype(np.int8)
        + x["adx_inc2"].astype(np.int8)
        + x["rsi"].between(cfg.rsi_low, cfg.rsi_high).astype(np.int8)
        + x["rsi_inc2"].astype(np.int8)
        + (
            (x["stoch_k"] > x["stoch_d"])
            & x["stoch_k"].between(25, 90)
        ).astype(np.int8)
        + 2 * x["rel_volume"].ge(cfg.relative_volume_min).astype(np.int8)
        + x["obv_up5"].astype(np.int8)
        + x["close_loc"].ge(cfg.close_location_min).astype(np.int8)
        + x["market_regime"].eq("bullish").astype(np.int8)
    )
    mask = (
        x["strategy"].eq("orb")
        & x["variant"].eq(cfg.variant)
        & x["minute"].between(cfg.start_minute, cfg.end_minute)
        & x["traded_value"].ge(cfg.min_traded_value)
        & x["atr_pct"].between(cfg.atr_low, cfg.atr_high)
        & breakout_extension.between(
            cfg.min_breakout_extension, cfg.max_breakout_extension
        )
        & x["avwap_ext"].between(-.02, cfg.max_avwap_extension)
        & x["range_atr"].le(cfg.max_range_atr)
        & x["upper_wick_frac"].le(cfg.max_upper_wick)
        & x["close_loc"].ge(cfg.close_location_min)
        & score.ge(cfg.score_min)
    )
    if cfg.regime == "not_bearish":
        mask &= x["market_regime"].ne("bearish")
    elif cfg.regime == "bullish":
        mask &= x["market_regime"].eq("bullish")
    chosen = x.loc[mask].copy()
    if chosen.empty:
        return np.array([], dtype=int)
    chosen["_rank"] = (
        score.loc[chosen.index].astype(float)
        + chosen["rel_volume"].clip(upper=3.0).fillna(0) * .5
        + breakout_extension.loc[chosen.index].clip(upper=.8).fillna(0)
        + chosen["adx_slope"].clip(lower=0, upper=12).fillna(0) * .04
        - chosen["avwap_ext"].clip(lower=0).fillna(0) * .75
        - chosen["range_atr"].clip(lower=0).fillna(0) * .15
    )
    chosen = chosen.sort_values(["ticker", "session", "date"])
    chosen = chosen.drop_duplicates(["ticker", "session"], keep="first")
    if cfg.top_n_per_slot > 0:
        chosen = chosen.sort_values(
            ["date", "_rank", "traded_value", "ticker"],
            ascending=[True, False, False, True],
        ).groupby("date", sort=False, as_index=False).head(cfg.top_n_per_slot)
    return chosen.index.to_numpy(dtype=int)


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    entries = pd.read_parquet(r.OUT / "valid_entries.parquet")
    z = np.load(r.OUT / "paths.npz")
    paths = {key: z[key] for key in z.files}
    sessions = r.sessions()
    dev_a, dev_b = set(sessions[:20]), set(sessions[20:40])
    validation, test = set(sessions[40:50]), set(sessions[50:60])
    orb_idx = entries.index[entries["strategy"].eq("orb")].to_numpy(dtype=int)

    anchor_pnl = common.pnl_arrays(entries, paths, orb_idx, ANCHOR_EXITS)
    generated = configs()
    stage1: list[dict[str, Any]] = []
    index_cache: dict[str, np.ndarray] = {}
    for n, cfg in enumerate(generated, 1):
        idx = selected_indices(entries, cfg)
        index_cache[cfg.name] = idx
        a_idx = common.fold_indices(entries, idx, dev_a)
        b_idx = common.fold_indices(entries, idx, dev_b)
        if len(a_idx) < 35 or len(b_idx) < 35:
            continue
        best: dict[str, Any] | None = None
        for ex in ANCHOR_EXITS:
            a = common.fast_metrics(anchor_pnl[ex.key], a_idx)
            b = common.fast_metrics(anchor_pnl[ex.key], b_idx)
            score = min(a["pf"], b["pf"]) - .12 * abs(
                np.log(max(a["pf"], .01) / max(b["pf"], .01))
            )
            row = {
                "config": cfg.name, **asdict(ex),
                "dev_a_trades": a["trades"], "dev_a_pf": a["pf"],
                "dev_a_expectancy": a["expectancy"], "dev_a_net": a["net"],
                "dev_b_trades": b["trades"], "dev_b_pf": b["pf"],
                "dev_b_expectancy": b["expectancy"], "dev_b_net": b["net"],
                "development_score": score,
            }
            if best is None or score > best["development_score"]:
                best = row
        if best is not None:
            stage1.append(best)
        if n % 250 == 0:
            print(f"[signal-search] {n}/{len(generated)} retained={len(stage1)}")
    stage1_df = pd.DataFrame(stage1).sort_values(
        ["development_score", "dev_a_trades", "dev_b_trades"],
        ascending=False,
    )
    stage1_df.to_csv(OUT / "development_signal_search.csv", index=False)

    top_names = stage1_df.head(120)["config"].tolist()
    full_pnl = common.pnl_arrays(entries, paths, orb_idx, FULL_EXITS)
    stage2: list[dict[str, Any]] = []
    config_map = {cfg.name: cfg for cfg in generated}
    for name in top_names:
        idx = index_cache[name]
        a_idx = common.fold_indices(entries, idx, dev_a)
        b_idx = common.fold_indices(entries, idx, dev_b)
        for ex in FULL_EXITS:
            a = common.fast_metrics(full_pnl[ex.key], a_idx)
            b = common.fast_metrics(full_pnl[ex.key], b_idx)
            if min(a["trades"], b["trades"]) < 35:
                continue
            score = min(a["pf"], b["pf"]) - .15 * abs(
                np.log(max(a["pf"], .01) / max(b["pf"], .01))
            )
            stage2.append({
                "config": name, **asdict(ex),
                "dev_a_trades": a["trades"], "dev_a_pf": a["pf"],
                "dev_a_expectancy": a["expectancy"], "dev_a_net": a["net"],
                "dev_b_trades": b["trades"], "dev_b_pf": b["pf"],
                "dev_b_expectancy": b["expectancy"], "dev_b_net": b["net"],
                "development_score": score,
            })
    stage2_df = pd.DataFrame(stage2).sort_values(
        ["development_score", "dev_a_trades", "dev_b_trades"],
        ascending=False,
    )
    stage2_df.to_csv(OUT / "development_exit_search.csv", index=False)

    validation_rows: list[dict[str, Any]] = []
    for _, row in stage2_df.head(300).iterrows():
        idx = index_cache[str(row["config"])]
        v_idx = common.fold_indices(entries, idx, validation)
        ex = r.ExitConfig(
            float(row["target_pct"]), float(row["stop_pct"]), int(row["hold_min"])
        )
        vm = common.fast_metrics(full_pnl[ex.key], v_idx)
        validation_rows.append({
            **row.to_dict(),
            "validation_trades": vm["trades"],
            "validation_pf": vm["pf"],
            "validation_expectancy": vm["expectancy"],
            "validation_net": vm["net"],
            "freeze_score": min(
                float(row["dev_a_pf"]), float(row["dev_b_pf"]), vm["pf"]
            ),
        })
    validation_df = pd.DataFrame(validation_rows).sort_values(
        ["freeze_score", "validation_expectancy", "validation_trades"],
        ascending=False,
    )
    validation_df.to_csv(OUT / "validation_freeze.csv", index=False)
    eligible = validation_df[
        (validation_df["dev_a_pf"] > 1.05)
        & (validation_df["dev_b_pf"] > 1.05)
        & (validation_df["validation_pf"] > 1.05)
        & (validation_df["dev_a_expectancy"] > 0)
        & (validation_df["dev_b_expectancy"] > 0)
        & (validation_df["validation_expectancy"] > 0)
        & (validation_df["validation_trades"] >= 15)
    ]
    pool = eligible if not eligible.empty else validation_df
    frozen = pool.iloc[0]
    cfg = config_map[str(frozen["config"])]
    ex = r.ExitConfig(
        float(frozen["target_pct"]), float(frozen["stop_pct"]), int(frozen["hold_min"])
    )
    idx = index_cache[cfg.name]
    test_idx = common.fold_indices(entries, idx, test)
    trades = r.simulate(entries, paths, test_idx, ex)
    test_metrics = r.metrics(trades)
    stress = r.metrics(r.simulate(entries, paths, test_idx, ex, 1.5))
    trades.to_csv(OUT / "frozen_test_trades.csv", index=False)

    payload = {
        "selection_used_test": False,
        "test_is_confirmation_not_pristine": True,
        "searched_signal_configs": len(generated),
        "development_exit_combinations": len(stage2_df),
        "eligible_three_fold_candidates": len(eligible),
        "signal_config": asdict(cfg),
        "exit_config": asdict(ex),
        "dev_a": {
            "trades": int(frozen["dev_a_trades"]),
            "profit_factor": float(frozen["dev_a_pf"]),
            "expectancy": float(frozen["dev_a_expectancy"]),
            "net_profit": float(frozen["dev_a_net"]),
        },
        "dev_b": {
            "trades": int(frozen["dev_b_trades"]),
            "profit_factor": float(frozen["dev_b_pf"]),
            "expectancy": float(frozen["dev_b_expectancy"]),
            "net_profit": float(frozen["dev_b_net"]),
        },
        "validation": {
            "trades": int(frozen["validation_trades"]),
            "profit_factor": float(frozen["validation_pf"]),
            "expectancy": float(frozen["validation_expectancy"]),
            "net_profit": float(frozen["validation_net"]),
        },
        "test": test_metrics,
        "test_150pct_slippage": stress,
    }
    payload["classification"] = (
        "PROFITABLE_BUT_INSUFFICIENT_OR_REUSED_TEST"
        if len(eligible) > 0
        and test_metrics["profit_factor"] > 1.0
        and test_metrics["expectancy"] > 0
        else "REJECT"
    )
    (OUT / "result.json").write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8"
    )
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

