"""G7 V5 research: regenerate selected opportunities for designated LONG slots.

The sweep varies OI and candle-body admission before the existing slot quota.
Actual OI/body values are restored after the native V15 selector has evaluated
each counterfactual threshold. Both slots rank by liquidity, so these adjusted
filter inputs cannot change the ranking measure.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

import fno_v13_replay_source as source
from research import g3_target_cutoff_contract as contract
from research import g7_g6_replica_backtest as g7
from research.g7_v5_candidate_audit import G7_NATIVE, load_day


SLOTS = {"0951_LONG": 950, "1046_LONG": 1045}
GRID = (
    ("baseline", None, None, "all"),
    ("oi_0_unconditional", 0.00, None, "all"),
    ("oi_neg010_unconditional", -0.10, None, "all"),
    ("oi_neg025_unconditional", -0.25, None, "all"),
    ("body_040_unconditional", None, 0.40, "all"),
    ("body_035_unconditional", None, 0.35, "all"),
    ("oi_0_strong_moderate", 0.00, None, "moderate"),
    ("oi_neg010_strong_moderate", -0.10, None, "moderate"),
    ("oi_neg025_strong_moderate", -0.25, None, "moderate"),
    ("body_035_strong_moderate", None, 0.35, "moderate"),
    ("oi_0_body035_strong_moderate", 0.00, 0.35, "moderate"),
)


def regime_map(market: pd.DataFrame, *, day_gain: float = .25,
               momentum: float = 0., prior: float = 55., above_open: float = 50.) -> dict[tuple[str, int], bool]:
    m = market.sort_values(["day", "clock"]).copy()
    first = (m.nifty_day_gain_pct.ge(day_gain) & m.nifty_from_open_pct.gt(0)
             & m.nifty_last15_pct.ge(momentum) & m.nifty_above_ema20
             & m.breadth_above_prior_pct.ge(prior) & m.breadth_above_open_pct.ge(above_open)
             & m.coverage_pct.ge(95))
    active = first & first.groupby(m.day).shift(fill_value=False) & m.clock.ge("09:40")
    return {(str(r.day), int(r.clock.replace(":", ""))): bool(value)
            for r, value in zip(m.itertuples(), active)}


def opening_regime_map(market: pd.DataFrame, *, day_gain: float = .25) -> dict[tuple[str, int], bool]:
    """Use only the completed 09:20 index/stock bars for morning admission."""
    first = market.loc[market.clock.eq("09:20")].copy()
    allowed = (first.nifty_day_gain_pct.ge(day_gain) & first.nifty_from_open_pct.gt(0)
               & first.breadth_above_prior_pct.ge(60)
               & first.breadth_above_open_pct.ge(50)
               & first.coverage_pct.ge(95))
    return {(str(row.day), clock): bool(ok)
            for row, ok in zip(first.itertuples(), allowed)
            for clock in (920, 925, 930, 935, 940, 945, 950)}


def opening_guarded_or_map(market: pd.DataFrame, *, day_gain: float = .25) -> dict[tuple[str, int], bool]:
    """Alternative bullish evidence with data coverage and nonnegative Nifty hard checks."""
    first = market.loc[market.clock.eq("09:20")].copy()
    nifty = first.nifty_day_gain_pct.ge(day_gain) & first.nifty_from_open_pct.gt(0)
    breadth = (first.breadth_above_prior_pct.ge(65) &
               first.breadth_above_open_pct.ge(60))
    allowed = (first.coverage_pct.ge(95) & first.nifty_day_gain_pct.ge(0)
               & (nifty | breadth))
    return {(str(row.day), clock): bool(ok)
            for row, ok in zip(first.itertuples(), allowed)
            for clock in (920, 925, 930, 935, 940, 945, 950)}


def select_day(raw: pd.DataFrame, day: str, *, oi_floor: float | None,
               body_min: float | None, gate: str,
               regime: dict[tuple[str, int], bool],
               slots: dict[str, int] | None = None,
               slot_aware: bool = False) -> pd.DataFrame:
    slots = SLOTS if slots is None else slots
    wanted = raw.loc[raw.hhmm_int.isin(slots.values())].copy()
    if wanted.empty:
        return pd.DataFrame()
    wanted["oi"] = pd.to_numeric(wanted.oi, errors="coerce").astype(float)
    wanted["oi_change_pct"] = pd.to_numeric(wanted.oi_change_pct, errors="coerce").astype(float)
    wanted["body_ratio"] = pd.to_numeric(wanted.body_ratio, errors="coerce").astype(float)
    actual = wanted[["tradingsymbol", "signal_ts", "oi", "oi_change_pct", "body_ratio"]].copy()
    actual["signal_ts"] = pd.to_datetime(actual.signal_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    active = (pd.Series(True, index=wanted.index) if gate == "all" else
              wanted.hhmm_int.map(lambda minute: regime.get((day, int(minute)), False)))
    if slot_aware:
        setup = {item["setup_id"]: item for item in g7.configuration()["active_setups"]}
        oi_requirement = wanted.hhmm_int.map({clock: float(setup[name]["oi_change_pct"])
                                              for name, clock in slots.items()})
        body_requirement = wanted.hhmm_int.map({clock: float(setup[name]["body_ratio"])
                                                for name, clock in slots.items()})
        if oi_requirement.isna().any() or body_requirement.isna().any():
            raise ValueError("Unknown selected slot threshold")
    else:
        oi_requirement = pd.Series(.05, index=wanted.index)
        body_requirement = pd.Series(.50, index=wanted.index)
    if oi_floor is not None:
        oi = pd.to_numeric(wanted.oi_change_pct, errors="coerce")
        prev = pd.to_numeric(wanted.prev_oi, errors="coerce")
        present = pd.to_numeric(wanted.oi, errors="coerce")
        eligible = (active & oi.ge(oi_floor) & oi.lt(oi_requirement) & oi.le(1.)
                    & prev.gt(0) & present.gt(0))
        epsilon = .0001 if slot_aware else 0.
        wanted.loc[eligible, "oi_change_pct"] = oi_requirement.loc[eligible] + epsilon
        wanted.loc[eligible, "oi"] = prev.loc[eligible] * (1 + (oi_requirement.loc[eligible] + epsilon) / 100)
    if body_min is not None:
        body = pd.to_numeric(wanted.body_ratio, errors="coerce")
        eligible = active & body.ge(body_min) & body.lt(body_requirement)
        wanted.loc[eligible, "body_ratio"] = body_requirement.loc[eligible]
    nifty = pd.to_numeric(wanted.nifty_first_bar_return_pct, errors="coerce").dropna().unique()
    if len(nifty) != 1:
        raise ValueError(f"Ambiguous Nifty opening context {day}")
    signals = source._strict_signals(wanted, float(nifty[0]))
    signals = signals.copy()
    signals["sid"] = 300_000_000 + int(day.replace("-", "")) * 100 + np.arange(len(signals))
    selected = contract.selection_audit(signals)
    selected = selected.loc[selected.v9_selected.eq(True) & selected.setup_id.isin(slots)].copy()
    if selected.empty:
        return selected
    restore = actual.rename(columns={"oi": "actual_oi", "oi_change_pct": "actual_oi_change_pct",
                                     "body_ratio": "actual_body_ratio"})
    selected["signal_ts"] = pd.to_datetime(selected.signal_ts, utc=True).dt.tz_convert("Asia/Kolkata")
    selected = selected.merge(restore, on=["tradingsymbol", "signal_ts"], how="left",
                              validate="one_to_one")
    if selected.actual_oi.isna().any() or selected.actual_body_ratio.isna().any():
        raise ValueError("Selected candidate lost its actual input values")
    selected["oi"] = selected.actual_oi
    selected["oi_change_pct"] = selected.actual_oi_change_pct
    selected["body_ratio"] = selected.actual_body_ratio
    selected["regime_active"] = selected.hhmm_int.map(lambda clock: regime.get((day, int(clock)), False))
    return selected.drop(columns=["actual_oi", "actual_oi_change_pct", "actual_body_ratio"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parity", type=Path, required=True)
    parser.add_argument("--market", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--experiment", choices=("intraday", "opening"), default="intraday")
    parser.add_argument("--opening-day-gain", type=float, default=.25)
    parser.add_argument("--opening-gate", choices=("and", "guarded_or"), default="and")
    parser.add_argument("--slot-aware", action="store_true")
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    parity = json.loads(args.parity.read_text(encoding="utf-8"))
    if not parity["all_matching"]:
        raise ValueError("Frozen V4 candidate parity failed")
    calendar = [r["day"] for r in parity["checks"]]
    frozen = pd.read_csv(G7_NATIVE, low_memory=False)
    market = pd.read_csv(args.market)
    if args.experiment == "opening":
        # 09:36/09:46 fail frozen V4 selection parity on eight days; the
        # opening study is restricted to slots with exact baseline identities.
        slots = {"0921_LONG": 920, "0926_LONG": 925,
                 "0941_LONG": 940, "0951_LONG": 950}
        tag = (f"opening_{int(args.opening_day_gain*1000):03d}"
               if args.opening_gate == "and" and not args.slot_aware else
               f"opening_{args.opening_gate}_{int(args.opening_day_gain*1000):03d}_slotaware")
        grid = tuple((name.replace("strong_moderate", tag),
                      oi, body, gate) for name, oi, body, gate in GRID)
        regime = (opening_guarded_or_map(market, day_gain=args.opening_day_gain)
                  if args.opening_gate == "guarded_or" else
                  opening_regime_map(market, day_gain=args.opening_day_gain))
    else:
        slots = SLOTS
        grid = GRID
        regime = regime_map(market)
    rows = []
    for day in calendar:
        raw, _ = load_day(day)
        for name, oi_floor, body_min, gate in grid:
            selected = select_day(raw, day, oi_floor=oi_floor, body_min=body_min,
                                  gate=gate, regime=regime, slots=slots,
                                  slot_aware=args.slot_aware)
            if len(selected):
                selected["variant"] = name
                rows.append(selected)
        print("SWEEP", day, flush=True)
    result = pd.concat(rows, ignore_index=True, sort=False)
    baseline = result.loc[result.variant.eq("baseline")]
    frozen_keys = set(zip(frozen.loc[frozen.setup_id.isin(slots), "day"],
                          frozen.loc[frozen.setup_id.isin(slots), "setup_id"],
                          frozen.loc[frozen.setup_id.isin(slots), "tradingsymbol"]))
    our_keys = set(zip(baseline.day.astype(str).str[:10], baseline.setup_id, baseline.tradingsymbol))
    if our_keys != frozen_keys:
        raise ValueError(f"Baseline selected inventory differs from V4: missing {sorted(frozen_keys-our_keys)[:5]}, extra {sorted(our_keys-frozen_keys)[:5]}")
    summaries = []
    for name, *_ in grid:
        sample = result.loc[result.variant.eq(name)]
        keys = set(zip(sample.day.astype(str).str[:10], sample.setup_id, sample.tradingsymbol))
        summaries.append(dict(variant=name, selected_opportunities=len(keys),
                              newly_selected=len(keys-our_keys), displaced=len(our_keys-keys),
                              changed_days=len({key[0] for key in (keys ^ our_keys)}),
                              strong_regime_selected=int(sample.regime_active.sum())))
    args.output.mkdir(parents=True)
    result.to_pickle(args.output / "selected_opportunities.pkl")
    pd.DataFrame(summaries).to_csv(args.output / "selection_summary.csv", index=False)
    (args.output / "protocol.json").write_text(json.dumps(dict(grid=grid, slots=slots,
        experiment=args.experiment, opening_day_gain=args.opening_day_gain if args.experiment == "opening" else None,
        opening_gate=args.opening_gate if args.experiment == "opening" else None,
        slot_aware=args.slot_aware,
        calendar=calendar, v4_native=str(G7_NATIVE), v4_native_sha256=source._sha(G7_NATIVE),
        exact_baseline_selection_parity=True, scope="BACKTEST_ONLY", execution_authority=False), indent=2), encoding="utf-8")
    print(pd.DataFrame(summaries).to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
