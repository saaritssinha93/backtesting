"""Honest structural refinement of the rejected AVWAP pullback family.

Signal rules and exits are selected on two chronological TRAIN folds, checked
on VALIDATION, and only then evaluated once on the existing TEST segment.
Because the broad family TEST result is already known, the final segment is a
confirmation set rather than a pristine never-seen holdout.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import research as r


OUT = r.OUT / "pullback_refinement"
RNG = np.random.default_rng(20260730)


@dataclass(frozen=True)
class SignalConfig:
    name: str
    start_minute: int
    end_minute: int
    regime: str
    min_traded_value: float
    atr_low: float
    atr_high: float
    max_avwap_ext: float
    max_range_atr: float
    max_upper_wick: float
    pull_mode: str
    ema_tolerance: float
    vwap_tolerance: float
    adx_low: float
    adx_high: float
    rsi_low: float
    rsi_high: float
    relative_volume_min: float
    score_min: int
    top_n_per_slot: int


ANCHOR_EXITS = [
    r.ExitConfig(target, stop, hold)
    for target, stop, hold in (
        (.35, .45, 20), (.35, .60, 30), (.35, .75, 40),
        (.45, .45, 20), (.45, .60, 30), (.45, .75, 40),
        (.55, .60, 30), (.55, .75, 40), (.55, .90, 50),
        (.65, .60, 30), (.65, .75, 40), (.65, .90, 50),
        (.75, .75, 40), (.75, .90, 50),
    )
]

FULL_EXITS = [
    r.ExitConfig(target, stop, hold)
    for target in (.35, .45, .55, .65, .75, .85)
    for stop in (.45, .55, .65, .75, .85, .95)
    for hold in (20, 30, 40, 50, 60)
]


def configs(count: int = 3000) -> list[SignalConfig]:
    windows = (
        (570, 630), (585, 660), (600, 690), (630, 750), (660, 810),
        (720, 870), (780, 870), (570, 750), (600, 810), (570, 870),
    )
    out: list[SignalConfig] = []
    for i in range(count):
        atr_low = float(RNG.choice((.15, .20, .25, .30)))
        atr_high = float(RNG.choice((.55, .65, .75, .90)))
        if atr_high <= atr_low + .20:
            atr_high = min(.90, atr_low + .35)
        adx_low = float(RNG.choice((10, 12, 14, 16, 18, 20)))
        adx_high = float(RNG.choice((28, 32, 36, 40, 44)))
        if adx_high <= adx_low + 8:
            adx_high = adx_low + 12
        rsi_low = float(RNG.choice((46, 48, 50, 52, 54)))
        rsi_high = float(RNG.choice((64, 66, 68, 70, 72, 74)))
        if rsi_high <= rsi_low + 10:
            rsi_high = rsi_low + 14
        start, end = windows[int(RNG.integers(0, len(windows)))]
        out.append(SignalConfig(
            name=f"pull_refine_{i:04d}",
            start_minute=start,
            end_minute=end,
            regime=str(RNG.choice(("all", "not_bearish", "bullish"))),
            min_traded_value=float(RNG.choice((1_000_000, 2_500_000, 5_000_000, 10_000_000))),
            atr_low=atr_low,
            atr_high=atr_high,
            max_avwap_ext=float(RNG.choice((.20, .30, .40, .50, .65, .80))),
            max_range_atr=float(RNG.choice((.80, 1.10, 1.40, 1.80, 2.20))),
            max_upper_wick=float(RNG.choice((.15, .25, .35, .45, .55))),
            pull_mode=str(RNG.choice((
                "any", "ema", "vwap", "reclaim", "ema_or_reclaim",
                "vwap_or_reclaim", "both_near",
            ))),
            ema_tolerance=float(RNG.choice((.08, .12, .16, .20, .25, .30))),
            vwap_tolerance=float(RNG.choice((.08, .12, .16, .20, .25, .35))),
            adx_low=adx_low,
            adx_high=adx_high,
            rsi_low=rsi_low,
            rsi_high=rsi_high,
            relative_volume_min=float(RNG.choice((.50, .70, .90, 1.10, 1.25, 1.50))),
            score_min=int(RNG.choice((4, 5, 6, 7))),
            top_n_per_slot=int(RNG.choice((0, 10, 20, 40))),
        ))
    return out


def selected_indices(entries: pd.DataFrame, cfg: SignalConfig) -> np.ndarray:
    x = entries
    structure = {
        "any": (
            x["ema9_dist_low"].le(cfg.ema_tolerance)
            | x["vwap_dist_low"].le(cfg.vwap_tolerance)
            | x["ema9_reclaim"]
        ),
        "ema": x["ema9_dist_low"].le(cfg.ema_tolerance),
        "vwap": x["vwap_dist_low"].le(cfg.vwap_tolerance),
        "reclaim": x["ema9_reclaim"],
        "ema_or_reclaim": (
            x["ema9_dist_low"].le(cfg.ema_tolerance) | x["ema9_reclaim"]
        ),
        "vwap_or_reclaim": (
            x["vwap_dist_low"].le(cfg.vwap_tolerance) | x["ema9_reclaim"]
        ),
        "both_near": (
            x["ema9_dist_low"].le(cfg.ema_tolerance)
            & x["vwap_dist_low"].le(cfg.vwap_tolerance)
        ),
    }[cfg.pull_mode]
    score = (
        x["adx"].between(cfg.adx_low, cfg.adx_high).astype(np.int8)
        + x["adx_inc2"].astype(np.int8)
        + x["rsi"].between(cfg.rsi_low, cfg.rsi_high).astype(np.int8)
        + x["rsi_inc2"].astype(np.int8)
        + (x["stoch_k"] > x["stoch_d"]).astype(np.int8)
        + x["stoch_k"].between(25, 82).astype(np.int8)
        + x["rel_volume"].ge(cfg.relative_volume_min).astype(np.int8)
        + x["obv_up5"].astype(np.int8)
        + x["close_loc"].ge(.60).astype(np.int8)
    )
    mask = (
        x["strategy"].eq("pullback")
        & x["minute"].between(cfg.start_minute, cfg.end_minute)
        & x["traded_value"].ge(cfg.min_traded_value)
        & x["atr_pct"].between(cfg.atr_low, cfg.atr_high)
        & x["avwap_ext"].between(-.02, cfg.max_avwap_ext)
        & x["range_atr"].le(cfg.max_range_atr)
        & x["upper_wick_frac"].le(cfg.max_upper_wick)
        & structure
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
        + chosen["rel_volume"].clip(upper=2.0).fillna(0) * .5
        + chosen["rsi_slope"].clip(lower=0, upper=15).fillna(0) * .03
        - chosen["avwap_ext"].clip(lower=0).fillna(0)
        - chosen["range_atr"].clip(lower=0).fillna(0) * .15
    )
    # One trade per ticker/day is causal: keep the first qualifying signal.
    chosen = chosen.sort_values(["ticker", "session", "date"])
    chosen = chosen.drop_duplicates(["ticker", "session"], keep="first")
    if cfg.top_n_per_slot > 0:
        chosen = chosen.sort_values(
            ["date", "_rank", "traded_value", "ticker"],
            ascending=[True, False, False, True],
        ).groupby("date", sort=False, as_index=False).head(cfg.top_n_per_slot)
    return chosen.index.to_numpy(dtype=int)


def pnl_arrays(
    entries: pd.DataFrame,
    paths: dict[str, np.ndarray],
    pullback_indices: np.ndarray,
    exits: list[r.ExitConfig],
) -> dict[str, np.ndarray]:
    out: dict[str, np.ndarray] = {}
    for n, ex in enumerate(exits, 1):
        trades = r.simulate(entries, paths, pullback_indices, ex)
        values = np.full(len(entries), np.nan, dtype=np.float64)
        values[trades.index.to_numpy(dtype=int)] = trades["net_pnl"].to_numpy(float)
        out[ex.key] = values
        if n % 30 == 0 or n == len(exits):
            print(f"[pnl-cache] {n}/{len(exits)}")
    return out


def fast_metrics(pnl: np.ndarray, idx: np.ndarray) -> dict[str, float]:
    values = pnl[idx]
    values = values[np.isfinite(values)]
    if len(values) == 0:
        return {"trades": 0, "pf": 0.0, "expectancy": 0.0, "net": 0.0}
    wins = float(values[values > 0].sum())
    losses = float(-values[values < 0].sum())
    return {
        "trades": int(len(values)),
        "pf": float(wins / losses) if losses > 0 else 99.0,
        "expectancy": float(values.mean()),
        "net": float(values.sum()),
    }


def fold_indices(
    entries: pd.DataFrame, idx: np.ndarray, sessions: set[pd.Timestamp]
) -> np.ndarray:
    return np.array(
        [i for i in idx if entries.at[i, "session"] in sessions], dtype=int
    )


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    entries = pd.read_parquet(r.OUT / "valid_entries.parquet")
    z = np.load(r.OUT / "paths.npz")
    paths = {key: z[key] for key in z.files}
    sessions = r.sessions()
    dev_a = set(sessions[:20])
    dev_b = set(sessions[20:40])
    validation = set(sessions[40:50])
    test = set(sessions[50:60])
    pullback_idx = entries.index[entries["strategy"].eq("pullback")].to_numpy(dtype=int)

    anchor_pnl = pnl_arrays(entries, paths, pullback_idx, ANCHOR_EXITS)
    generated = configs()
    stage1: list[dict[str, Any]] = []
    index_cache: dict[str, np.ndarray] = {}
    for n, cfg in enumerate(generated, 1):
        idx = selected_indices(entries, cfg)
        index_cache[cfg.name] = idx
        a_idx = fold_indices(entries, idx, dev_a)
        b_idx = fold_indices(entries, idx, dev_b)
        if len(a_idx) < 60 or len(b_idx) < 60:
            continue
        best: dict[str, Any] | None = None
        for ex in ANCHOR_EXITS:
            a = fast_metrics(anchor_pnl[ex.key], a_idx)
            b = fast_metrics(anchor_pnl[ex.key], b_idx)
            score = min(a["pf"], b["pf"]) - .12 * abs(np.log(max(a["pf"], .01) / max(b["pf"], .01)))
            row = {
                "config": cfg.name,
                **asdict(ex),
                "dev_a_trades": a["trades"], "dev_a_pf": a["pf"],
                "dev_a_expectancy": a["expectancy"], "dev_a_net": a["net"],
                "dev_b_trades": b["trades"], "dev_b_pf": b["pf"],
                "dev_b_expectancy": b["expectancy"], "dev_b_net": b["net"],
                "development_score": score,
            }
            if best is None or row["development_score"] > best["development_score"]:
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
    full_pnl = pnl_arrays(entries, paths, pullback_idx, FULL_EXITS)
    stage2: list[dict[str, Any]] = []
    for name in top_names:
        cfg = next(item for item in generated if item.name == name)
        idx = index_cache[name]
        a_idx = fold_indices(entries, idx, dev_a)
        b_idx = fold_indices(entries, idx, dev_b)
        for ex in FULL_EXITS:
            a = fast_metrics(full_pnl[ex.key], a_idx)
            b = fast_metrics(full_pnl[ex.key], b_idx)
            if min(a["trades"], b["trades"]) < 60:
                continue
            score = min(a["pf"], b["pf"]) - .15 * abs(np.log(max(a["pf"], .01) / max(b["pf"], .01)))
            stage2.append({
                "config": name, **asdict(ex),
                "dev_a_trades": a["trades"], "dev_a_pf": a["pf"],
                "dev_a_expectancy": a["expectancy"], "dev_a_net": a["net"],
                "dev_b_trades": b["trades"], "dev_b_pf": b["pf"],
                "dev_b_expectancy": b["expectancy"], "dev_b_net": b["net"],
                "development_score": score,
            })
    stage2_df = pd.DataFrame(stage2).sort_values(
        ["development_score", "dev_a_trades", "dev_b_trades"], ascending=False
    )
    stage2_df.to_csv(OUT / "development_exit_search.csv", index=False)

    # Validation is used only on the strongest development candidates.
    validation_rows: list[dict[str, Any]] = []
    unique_candidates = stage2_df.drop_duplicates(
        ["config", "target_pct", "stop_pct", "hold_min"]
    ).head(250)
    for _, row in unique_candidates.iterrows():
        idx = index_cache[str(row["config"])]
        v_idx = fold_indices(entries, idx, validation)
        ex = r.ExitConfig(
            float(row["target_pct"]), float(row["stop_pct"]), int(row["hold_min"])
        )
        vm = fast_metrics(full_pnl[ex.key], v_idx)
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
        & (validation_df["validation_trades"] >= 25)
    ]
    pool = eligible if not eligible.empty else validation_df
    frozen = pool.iloc[0]
    cfg = next(item for item in generated if item.name == frozen["config"])
    ex = r.ExitConfig(
        float(frozen["target_pct"]), float(frozen["stop_pct"]), int(frozen["hold_min"])
    )
    idx = index_cache[cfg.name]
    test_idx = fold_indices(entries, idx, test)
    test_trades = r.simulate(entries, paths, test_idx, ex)
    test_metrics = r.metrics(test_trades)
    stress_metrics = r.metrics(r.simulate(entries, paths, test_idx, ex, 1.5))
    test_trades.to_csv(OUT / "frozen_test_trades.csv", index=False)

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
        "test_150pct_slippage": stress_metrics,
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

