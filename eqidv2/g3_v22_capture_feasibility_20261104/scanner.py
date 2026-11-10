"""Read-only causal 1-minute alert coverage study for the sealed G3 stock cohort.

These are alerts, never G3 orders or executed trades. Full-day top-two labels
are joined only after alerts for all stocks have been constructed.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v18_20261102.data import available_days, load_day


PACKAGE = Path(__file__).resolve().parent
OUTPUT = PACKAGE / "results_full_20261104"
TOP2 = PACKAGE.parent / "g3_v20_body_cap_20261104/results_full_20261104/top2_audit.csv"
THROUGH = "2026-11-04"
SCENARIOS = {
    "exact_next_minute": dict(first_cross_cutoff="13:30", confirmation_bars=1),
    "within_five_minutes": dict(first_cross_cutoff="13:25", confirmation_bars=5),
}
THRESHOLD = 0.01


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _minute_id(day: str, symbol: str, stamp) -> str:
    return f"{day}|{symbol}|{pd.Timestamp(stamp):%H:%M}"


def _scan_side(day: str, minutes: pd.DataFrame, opens: pd.Series,
               high_times: pd.Series, low_times: pd.Series, side: str,
               scenario: str, cutoff: int, confirm_bars: int):
    clock = minutes.ts.dt.hour * 60 + minutes.ts.dt.minute
    crossing = (minutes.high.ge(opens * (1 + THRESHOLD)) if side == "LONG"
                else minutes.low.le(opens * (1 - THRESHOLD))) & clock.le(cutoff)
    first = minutes.loc[crossing].groupby("tradingsymbol", sort=False).head(1)
    if first.empty:
        return [], []
    source_idx = first.index.to_numpy(dtype=np.int64)
    symbols = first.tradingsymbol.to_numpy()
    source_close = first.close.to_numpy(float)
    chosen = np.full(len(first), -1, dtype=np.int64)
    for lag in range(1, confirm_bars + 1):
        next_idx = source_idx + lag
        next_rows = minutes.loc[next_idx]
        if not np.array_equal(next_rows.tradingsymbol.to_numpy(), symbols):
            raise ValueError(f"Confirmation crossed stock boundary: {day} {side}")
        closes, next_opens = next_rows.close.to_numpy(float), next_rows.open.to_numpy(float)
        continuation = ((closes > source_close) & (closes > next_opens)
                        if side == "LONG" else
                        (closes < source_close) & (closes < next_opens))
        take = (chosen == -1) & continuation
        chosen[take] = next_idx[take]
    extreme_times = first.tradingsymbol.map(high_times if side == "LONG" else low_times)
    crossings, alerts = [], []
    for position, (idx, row) in enumerate(zip(source_idx, first.itertuples())):
        confirmed = chosen[position] >= 0
        confirm = minutes.loc[chosen[position]] if confirmed else None
        before_extreme = bool(confirmed and confirm.ts < extreme_times.iloc[position])
        base = dict(scenario=scenario, day=day, side=side, tradingsymbol=row.tradingsymbol,
                    first_open=float(opens.iloc[idx]), first_cross_ts=row.ts,
                    first_cross_minute_id=_minute_id(day, row.tradingsymbol, row.ts),
                    first_cross_minute_index=int(idx), first_cross_open=float(row.open),
                    first_cross_high=float(row.high), first_cross_low=float(row.low),
                    first_cross_close=float(row.close), confirmed=confirmed,
                    confirmation_ts=confirm.ts if confirmed else pd.NaT,
                    before_final_extreme=before_extreme)
        crossings.append(base)
        if confirmed:
            alerts.append(dict(scenario=scenario, day=day, side=side,
                               tradingsymbol=row.tradingsymbol,
                               first_open=base["first_open"],
                               first_cross_ts=row.ts,
                               first_cross_minute_id=base["first_cross_minute_id"],
                               confirmation_ts=confirm.ts,
                               confirmation_minute_id=_minute_id(day, row.tradingsymbol, confirm.ts),
                               first_cross_minute_index=int(idx),
                               confirmation_minute_index=int(chosen[position]),
                               confirmation_lag_minutes=int(chosen[position] - idx),
                               first_cross_open=float(row.open),
                               first_cross_high=float(row.high),
                               first_cross_low=float(row.low),
                               first_cross_close=float(row.close),
                               confirmation_open=float(confirm.open),
                               confirmation_high=float(confirm.high),
                               confirmation_low=float(confirm.low),
                               confirmation_close=float(confirm.close)))
    return crossings, alerts


def run() -> None:
    if OUTPUT.exists():
        raise FileExistsError(f"Choose a new result folder; existing result is immutable: {OUTPUT}")
    days = available_days(THROUGH)
    top = pd.read_csv(TOP2, low_memory=False)
    if set(top.day) != set(days) or len(top) != 255:
        raise ValueError("Top-two case reference does not match 64 sealed sessions")
    if top.duplicated(["day", "side", "tradingsymbol"]).any():
        raise ValueError("Duplicate top-two identity")
    all_crossings, all_alerts, sources = [], [], []
    for day_index, day in enumerate(days):
        features, source_minutes, proof = load_day(day)
        minutes = source_minutes.sort_values(["tradingsymbol", "ts"]).reset_index(drop=True)
        grouped = minutes.groupby("tradingsymbol", sort=False)
        opens = grouped.open.transform("first")
        high_times = minutes.loc[grouped.high.idxmax(), ["tradingsymbol", "ts"]].set_index("tradingsymbol").ts
        low_times = minutes.loc[grouped.low.idxmin(), ["tradingsymbol", "ts"]].set_index("tradingsymbol").ts
        if not grouped.size().eq(360).all():
            raise ValueError(f"Incomplete minute stock on {day}")
        for scenario, spec in SCENARIOS.items():
            hh, mm = map(int, spec["first_cross_cutoff"].split(":"))
            cutoff = hh * 60 + mm
            for side in ("LONG", "SHORT"):
                crossings, alerts = _scan_side(day, minutes, opens, high_times, low_times,
                                                side, scenario, cutoff, spec["confirmation_bars"])
                all_crossings.extend(crossings)
                all_alerts.extend(alerts)
        sources.append(dict(day=day, loader_source=proof["loader_source"],
                            feature_stocks=proof["feature_stocks"], minute_stocks=proof["minute_stocks"],
                            breadth_universe=proof["breadth_universe"],
                            provenance_path=proof.get("provenance_path"),
                            provenance_sha256=proof.get("provenance_sha256"),
                            run_manifest=proof.get("run_manifest"),
                            run_manifest_sha256=proof.get("run_manifest_sha256")))
        print(f"{day} [{day_index+1}/{len(days)}]", flush=True)
    crossing_frame = pd.DataFrame(all_crossings)
    alert_frame = pd.DataFrame(all_alerts)
    if crossing_frame.duplicated(["scenario", "day", "side", "tradingsymbol"]).any():
        raise ValueError("Duplicate crossing identity")
    if alert_frame.duplicated(["scenario", "day", "side", "tradingsymbol"]).any():
        raise ValueError("Duplicate alert identity")
    if not alert_frame.confirmation_lag_minutes.between(1, 5).all():
        raise ValueError("Invalid confirmation lag")
    top_cases = top[["day", "side", "rank", "tradingsymbol", "move_pct", "first_3pct_time",
                     "extreme_time", "v19_status"]].copy()
    summaries, daily_rows = {}, []
    for scenario in SCENARIOS:
        crossings = crossing_frame.loc[crossing_frame.scenario.eq(scenario)]
        alerts = alert_frame.loc[alert_frame.scenario.eq(scenario)]
        joined = top_cases.merge(crossings, on=["day", "side", "tradingsymbol"],
                                 how="left", validate="one_to_one")
        joined["scenario"] = scenario
        joined["confirmed"] = joined.confirmed.fillna(False).astype(bool)
        joined["before_final_extreme"] = joined.before_final_extreme.fillna(False).astype(bool)
        top_cross = int(joined.first_cross_ts.notna().sum())
        top_alert = int(joined.confirmed.sum())
        top_before = int(joined.before_final_extreme.sum())
        summaries[scenario] = dict(threshold_pct=1.,
            first_cross_cutoff=SCENARIOS[scenario]["first_cross_cutoff"],
            confirmation_bars=SCENARIOS[scenario]["confirmation_bars"],
            universe_crossings=len(crossings), universe_alerts=len(alerts),
            universe_alerts_before_final_extreme=int(crossings.before_final_extreme.sum()),
            top2_cases=len(top_cases), top2_crossings=top_cross,
            top2_alerts=top_alert, top2_alerts_before_final_extreme=top_before,
            top2_recall_before_extreme_pct=100 * top_before / len(top_cases),
            top2_precision_among_all_alerts_pct=100 * top_alert / len(alerts),
            average_alerts_per_session=len(alerts) / len(days))
        for day in days:
            c = crossings.loc[crossings.day.eq(day)]
            a = alerts.loc[alerts.day.eq(day)]
            t = joined.loc[joined.day.eq(day)]
            daily_rows.append(dict(day=day, scenario=scenario,
                                   all_stock_crossings=len(c), all_stock_alerts=len(a),
                                   top2_cases=len(t), top2_crossings=int(t.first_cross_ts.notna().sum()),
                                   top2_alerts=int(t.confirmed.sum()),
                                   top2_alerts_before_extreme=int(t.before_final_extreme.sum())))
        if scenario == "exact_next_minute":
            top_exact = joined
        else:
            top_retry = joined
    # Joined case fields are explicitly retrospective; signal generation above
    # never references the top-two table or future extrema.
    OUTPUT.mkdir(parents=True)
    alert_frame.loc[alert_frame.scenario.eq("exact_next_minute")].to_csv(
        OUTPUT / "alerts_exact_next_minute.csv", index=False)
    alert_frame.loc[alert_frame.scenario.eq("within_five_minutes")].to_csv(
        OUTPUT / "alerts_within_five_minutes.csv", index=False)
    top_exact.to_csv(OUTPUT / "top2_cases_exact_next_minute.csv", index=False)
    top_retry.to_csv(OUTPUT / "top2_cases_within_five_minutes.csv", index=False)
    pd.DataFrame(daily_rows).to_csv(OUTPUT / "daily_counts.csv", index=False)
    (OUTPUT / "source_provenance.json").write_text(json.dumps(dict(sessions=sources,
        top2_reference=str(TOP2), top2_reference_sha256=_sha(TOP2),
        scanner_source=str(Path(__file__)), scanner_sha256=_sha(Path(__file__))),
        indent=2, default=str) + "\n", encoding="utf-8")
    summary = dict(version="G3-V22-20261104-CAPTURE-FEASIBILITY", scope="RESEARCH_ALERTS_ONLY",
                   execution_authority=False, sessions=len(days), first_day=days[0],
                   latest_sealed_day=days[-1], top2_reference_cases=len(top_cases),
                   threshold_basis="09:16 first minute open; first minute high/low crossing 1%",
                   continuation="First later 1m candle within scenario window whose close advances beyond crossing-minute close and closes directionally vs own open",
                   alert_available_at="Completed confirmation minute; no future rank used",
                   scenarios=summaries,
                   limits=["No portfolio fills or trade P&L simulated", "9:15 minute absent from source",
                           "Top-two labels and final extrema used only for retrospective evaluation"])
    (OUTPUT / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n",
                                         encoding="utf-8")
    (OUTPUT / "manifest.json").write_text(json.dumps({p.name:_sha(p) for p in OUTPUT.iterdir()
                                                    if p.is_file()}, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    run()
