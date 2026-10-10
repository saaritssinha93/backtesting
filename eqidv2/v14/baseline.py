"""Cash-only selection adapter and sealed G-3 execution for V14 research.

The participation callback replaces the original OI eligibility gates. Nothing
is stored as synthetic OI. All other setup thresholds, rankings, quotas, entry
fills, stops, targets, costs and portfolio behavior come from frozen G-3.
"""
from __future__ import annotations

import copy
import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

from ._baseline import execution

SNAPSHOT = Path(__file__).with_name("_baseline")
IST = "Asia/Kolkata"
PICKERS = {"max_liquidity": "traded_value", "max_volume": "volume_ratio", "max_move": "abs_price_change_pct"}
ParticipationGate = Callable[[pd.DataFrame, dict], pd.Series]


def verify_snapshot() -> dict:
    """Verify the isolated source and exact execution extraction before use."""
    manifest = json.loads((SNAPSHOT / "snapshot_manifest.json").read_text(encoding="utf-8"))
    for relative, expected in manifest["files"].items():
        source = SNAPSHOT / relative
        if not source.is_file() or hashlib.sha256(source.read_bytes()).hexdigest() != expected:
            raise RuntimeError(f"Frozen G-3 source snapshot changed: {relative}")
    return manifest


@lru_cache(maxsize=1)
def _config() -> dict:
    verify_snapshot()
    result = json.loads((SNAPSHOT / "frozen_config.json").read_text(encoding="utf-8"))
    if result["accepted_variant"] != "G3_W1_V1p1" or result["confirmation_window_minutes"] != 1:
        raise RuntimeError("V14 requires accepted next-minute G-3")
    if any(pair["expanded"]["picker"] not in PICKERS for pair in result["setup_rules"]):
        raise RuntimeError("Unexpected G-3 picker; OI ranking is not part of this baseline")
    return result


def frozen_config() -> dict:
    return copy.deepcopy(_config())


def setup_rules() -> list[dict]:
    return copy.deepcopy(_config()["setup_rules"])


def _setup_id(setup: dict) -> str:
    return setup["confirmation_end"].replace(":", "") + "_" + setup["side"]


def _gate(rows: pd.DataFrame, setup: dict, participation: ParticipationGate | pd.Series | str | None) -> pd.Series:
    sign = 1 if setup["side"] == "LONG" else -1
    passed = (
        (rows.price_change_pct * sign).ge(setup["price_change_pct"])
        & rows.volume_ratio.ge(setup["volume_ratio"])
        & rows.body_ratio.ge(setup["body_ratio"])
        & rows.wick_ratio.le(setup["max_wick_ratio"])
        & rows.traded_value.ge(setup["min_traded_value"])
    )
    if callable(participation):
        result = participation(rows, copy.deepcopy(setup))
    elif isinstance(participation, str):
        result = rows[participation]
    elif participation is not None:
        result = participation.reindex(rows.index)
    else:
        result = pd.Series(True, index=rows.index)
    if not isinstance(result, pd.Series) or not result.index.equals(rows.index):
        raise ValueError("Participation gate must return a Boolean Series with the candidate row index")
    if not result.dropna().isin([True, False]).all():
        raise ValueError("Participation gate returned non-Boolean values")
    return passed & result.fillna(False).astype(bool)


def select_orders(features: pd.DataFrame, participation_pass: ParticipationGate | pd.Series | str | None = None) -> pd.DataFrame:
    """Select from features already passing the non-OI five-minute strict gate.

    Required strict preparation: real complete bars; EMA 9/20/50 alignment;
    signed close-to-close change >=0.10%; native cash 5m volume ratio >=0.80;
    exact next-minute candle closes directionally and beyond the signal close;
    and the retained 09:25 SHORT NIFTY first-bar return <=-0.05% gate.

    ``None`` intentionally means the no-participation-filter control. A callable
    receives candidate rows and the frozen setup dictionary, once for expanded
    eligibility and once for core eligibility. Native ranking is unchanged.
    """
    config = _config()
    if features.empty:
        return features.assign(setup_id=pd.Series(dtype=str), picker=pd.Series(dtype=str),
                               max_entries=pd.Series(dtype=int), native_stop_pct=pd.Series(dtype=float),
                               native_target_pct=pd.Series(dtype=float)).copy()
    required = {"sid", "day", "tradingsymbol", "signal_ts", "confirmation_ts", "side", "hhmm_int",
                "price_change_pct", "volume_ratio", "body_ratio", "wick_ratio", "traded_value",
                "v9_1m_volume_ratio", "trigger"}
    missing = required.difference(features)
    if missing:
        raise ValueError(f"Missing strict cash features: {sorted(missing)}")
    if features.sid.duplicated().any() or not features.index.is_unique:
        raise ValueError("Signal IDs and candidate row indices must be unique")
    work = features.copy()
    for column in ("signal_ts", "confirmation_ts"):
        work[column] = pd.to_datetime(work[column], utc=True).dt.tz_convert(IST)
    if not work.confirmation_ts.sub(work.signal_ts).eq(pd.Timedelta(minutes=1)).all():
        raise ValueError("Frozen G-3 requires exact next-minute confirmation")
    if not work.side.isin(["LONG", "SHORT"]).all():
        raise ValueError("Candidate side must be LONG or SHORT")
    stamp = pd.to_datetime(work.get("v9_1m_feature_ts", work.confirmation_ts), utc=True, errors="coerce")
    if (stamp.isna() | stamp.gt(work.confirmation_ts)).any():
        raise ValueError("Noncausal or missing confirmation feature timestamp")
    work["abs_price_change_pct"] = work.price_change_pct.abs()
    thresholds = work.side.map(config["minimum_confirmation_1m_volume_ratio"])
    work = work.loc[np.isfinite(work.v9_1m_volume_ratio) & work.v9_1m_volume_ratio.ge(thresholds)].copy()
    parts = []
    for pair in config["setup_rules"]:
        core, expanded = pair["core"], pair["expanded"]
        rows = work.loc[work.hhmm_int.eq(int(expanded["signal_end"].replace(":", "")))
                        & work.side.eq(expanded["side"])].copy()
        if rows.empty:
            continue
        rows = rows.loc[_gate(rows, expanded, participation_pass)].copy()
        if rows.empty:
            continue
        picker = PICKERS[expanded["picker"]]
        ranked = rows.sort_values(["day", picker, "traded_value", "tradingsymbol"],
                                  ascending=[True, False, False, True], kind="stable")
        primary = ranked.loc[_gate(ranked, core, participation_pass)].groupby("day", sort=False).head(core["max_entries"])
        selected = []
        for _, group in ranked.groupby("day", sort=False):
            kept = group.loc[group.index.isin(primary.index)]
            additions = group.loc[~group.index.isin(kept.index)].head(expanded["max_entries"] - len(kept))
            selected.extend(kept.index.tolist() + additions.index.tolist())
        chosen = rows.loc[selected].copy()
        chosen["setup_id"] = _setup_id(expanded)
        chosen["picker"] = expanded["picker"]
        chosen["max_entries"] = expanded["max_entries"]
        chosen["configured_confirmation_end"] = expanded["confirmation_end"]
        chosen["v10_g_f_core"] = chosen.index.isin(primary.index)
        chosen["v9_rank_in_setup_day"] = ranked.groupby("day", sort=False).cumcount().add(1).reindex(chosen.index)
        chosen["v9_selected"] = True
        chosen["v9_decision"] = "SELECTED"
        chosen["baseline_version"] = config["version"]
        pair_exit = config["exit"]["setups"].get(_setup_id(expanded), config["exit"]["default"])
        chosen["native_stop_pct"] = pair_exit["stop_pct"]
        chosen["native_target_pct"] = pair_exit["target_pct"]
        parts.append(chosen)
    if not parts:
        return select_orders(features.iloc[:0], participation_pass)
    return pd.concat(parts, ignore_index=True).sort_values(
        ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"], kind="stable").reset_index(drop=True)


def native_paths(minute: pd.DataFrame, confirmation_ts, day=None) -> dict[str, np.ndarray]:
    """Extract the unchanged full cash execution path after confirmation."""
    confirmation = execution._to_ist_timestamp(confirmation_ts)
    if day is not None and pd.Timestamp(day).date() != confirmation.date():
        raise ValueError("Execution day differs from confirmation day")
    stamp = pd.to_datetime(minute.ts, utc=True).dt.tz_convert(IST)
    cutoff = confirmation.normalize() + pd.Timedelta(hours=15, minutes=15)
    use = minute.loc[stamp.gt(confirmation) & stamp.le(cutoff)]
    return {"timestamp_ns": stamp.loc[use.index].astype("int64").to_numpy(),
            **{field: use[field].to_numpy(float) for field in ("open", "high", "low", "close")}}


def simulate(orders: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]], *, cost_bps: float | None = None) -> pd.DataFrame:
    """Return the full fill/portfolio audit with retained G-3 numerical behavior.

    An explicit cost override supports separately labelled execution-cost stress
    diagnostics. The default is the exact frozen flat 5bps round trip.
    """
    config = _config()
    if cost_bps is not None and (not np.isfinite(cost_bps) or cost_bps < 0):
        raise ValueError("Cost override must be finite and nonnegative")
    if orders.empty:
        return orders.assign(filled=pd.Series(dtype=bool), portfolio_executed=pd.Series(dtype=bool),
                             portfolio_net_profit_rupees=pd.Series(dtype=float),
                             portfolio_gross_profit_rupees=pd.Series(dtype=float),
                             portfolio_cost_rupees=pd.Series(dtype=float)).copy()
    execution.validate_paths(orders, paths)
    work = orders.copy()
    targets = {key: value["target_pct"] for key, value in config["exit"]["setups"].items()}
    work["native_target_pct"] = work.setup_id.map(targets).fillna(config["exit"]["default"]["target_pct"])
    trades = execution.simulate_staged(work, paths,
        cost_bps=config["cost_bps"] if cost_bps is None else float(cost_bps),
        max_entry_delay_minutes=config["entry_expiry_minutes"])
    # Upstream portfolio preparation assumes at least one fill. Explicit empty
    # typed columns retain its behavior for a newly possible all-unfilled batch.
    for column in ("entry_ts", "exit_ts"):
        if column not in trades:
            trades[column] = pd.NaT
    for column in ("net_return_pct", "gross_return_pct", "cost_pct"):
        if column not in trades:
            trades[column] = np.nan
    trades = execution.apply_fixed_capital_model(trades,
        config["capital_per_entry_rupees"], config["leverage_factor"])
    portfolio = execution.PortfolioConfig(portfolio_capital_rupees=config["portfolio_capital_rupees"],
                                          max_positions=config["max_positions"])
    return execution.apply_portfolio_constraints(trades, portfolio)[0]
