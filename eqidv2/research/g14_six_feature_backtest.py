"""Full-pool, research-only V14 six-field morning strategy replay.

This does not alter the archived V14 baseline or any live/paper profile.
The four rule variants and the chronological choice are frozen in
``g14_six_feature_rule_protocol.json`` before this runner reads outcomes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from research import g14_six_feature_execution as execution
from research import g14_six_feature_rules as rules
from research import slot_win70_backtest as source
from research.g8_exit_engine import _flags, metrics


ROOT = Path(__file__).resolve().parents[1]
DATA = Path("C:/TradingData/eqidv2")
LAB = ROOT / "g3_indicator_lab_20261103/inputs"
SOURCE = DATA / "backtesting_result_v13_v10_g_14/six_feature_source"
OUTROOT = DATA / "backtesting_result_v13_v10_g_14/runs/2026-11-03"
BASELINE = OUTROOT / "morning12_config_complete_v1"
CONFIG = ROOT / "configs/strategies/v13_v10_g_14_six_feature_v2.json"
PROTOCOL = ROOT / "research/g14_six_feature_rule_protocol.json"
IST = ZoneInfo("Asia/Kolkata")
COSTS = (5.0, 9.0, 15.0)
VARIANTS = ("A_direction_only", "B_with_gap", "C_three_paths_OR", "D_strict_OR")


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def dump(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str,
                               allow_nan=False) + "\n", encoding="utf-8")


def _verify_baseline() -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    saved = read_json(BASELINE / "manifest.json")
    for name, digest in saved["files"].items():
        if sha(BASELINE / name) != digest:
            raise ValueError(f"V14 baseline artifact changed: {name}")
    daily = pd.read_csv(BASELINE / "daywise_by_cost.csv")
    daily = daily.loc[daily.strategy.eq("V13-V10-G-14")].copy()
    daily["variant"] = "V14_V1_BASELINE"
    book = pd.read_csv(BASELINE / "portfolio_trades.csv", low_memory=False)
    days = sorted(daily.day.unique().tolist())
    if len(days) != 63 or days[-1] != "2026-11-02":
        raise ValueError("V14 comparison calendar must be 63 days through November 2")
    if set(daily.cost_bps.astype(float)) != set(COSTS):
        raise ValueError("V14 comparator lacks one of the cost stresses")
    return daily, book, days


def _verify_lab() -> dict[str, dict]:
    manifest = read_json(LAB / "manifest.json")
    sessions = {entry["day"]: entry for entry in manifest["sessions"]}
    for day, item in sessions.items():
        path = LAB / day / "minutes.parquet"
        if sha(path) != item["minutes_sha256"]:
            raise ValueError(f"Frozen minute source changed: {day}")
    return sessions


def _source_days(days: list[str]) -> None:
    manifest_path = SOURCE / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError("Run research.g14_six_feature_prepare.prepare() first")
    manifest = read_json(manifest_path)
    directory = SOURCE / "candidates_by_day"
    files = {p.name: p for p in directory.glob("*.parquet")}
    expected = {f"{day}.parquet" for day in days}
    hashes = manifest.get("per_day_sha256", {})
    if set(files) != expected or set(hashes) != expected or manifest.get("days") != len(days):
        raise ValueError("Six-field candidate files or source manifest differ from the 63-day calendar")
    for name, path in files.items():
        if sha(path) != hashes[name]:
            raise ValueError(f"Frozen six-field candidate changed: {name}")
    if sha(LAB / "manifest.json") != manifest.get("source_input_manifest_sha256"):
        raise ValueError("Six-field candidate source manifest changed")
    if sha(SOURCE / "pdc.csv") != manifest.get("source_pdc_csv_sha256"):
        raise ValueError("Six-field official prior-close bundle changed")


def _zero(day: str) -> dict:
    return dict(day=day, sessions=1, selected_orders=0, native_fills=0,
                trades=0, wins=0, losses=0, breakeven_trades=0,
                win_rate_pct=0., net_pnl_rs=0., gross_pnl_rs=0., cost_rs=0.,
                profitable_days=0, losing_days=0, flat_days=1,
                worst_day_rs=0., best_day_rs=0., losing_day_sum_abs_rs=0.,
                negative_trade_sum_abs_rs=0., negative_trade_pnl_rs=0.,
                gross_winner_profit_rs=0., daily_close_drawdown_rs=0.,
                max_drawdown_rs=0., average_net_trade_rs=0., profit_factor=np.nan,
                stop_exits=0, target_exits=0)


def _daily_metric(book: pd.DataFrame, day: str) -> dict:
    return dict(metrics(book, [day]), day=day) if len(book) else _zero(day)


def _period(day: str) -> str:
    return "development" if day <= "2026-09-30" else "assessment"


def _wilson(wins: int, trades: int) -> tuple[float, float]:
    if trades <= 0:
        return np.nan, np.nan
    z = 1.959963984540054
    p = wins / trades
    denom = 1 + z*z/trades
    midpoint = (p + z*z/(2*trades)) / denom
    radius = z*np.sqrt(p*(1-p)/trades + z*z/(4*trades*trades))/denom
    return 100*(midpoint-radius), 100*(midpoint+radius)


def _aggregate(daywise: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for variant in ("V14_V1_BASELINE", *VARIANTS):
        for cost in COSTS:
            for period in ("development", "assessment", "all"):
                part = daywise.loc[daywise.variant.eq(variant) & daywise.cost_bps.eq(cost)]
                if period != "all":
                    part = part.loc[part.day.map(_period).eq(period)]
                part = part.sort_values("day")
                pnl = part.net_pnl_rs.to_numpy(float)
                equity = np.r_[0., np.cumsum(pnl)]
                trades = int(part.trades.sum())
                wins = int(part.wins.sum())
                gain = float(part.gross_winner_profit_rs.sum())
                loss = float(part.negative_trade_sum_abs_rs.sum())
                lo, hi = _wilson(wins, trades)
                rows.append(dict(variant=variant, period=period, cost_bps=cost,
                                 sessions=len(part), selected_orders=int(part.selected_orders.sum()),
                                 native_fills=int(part.native_fills.sum()), trades=trades,
                                 wins=wins, losses=int(part.losses.sum()),
                                 win_rate_pct=100*wins/trades if trades else 0.,
                                 wilson95_low_pct=lo, wilson95_high_pct=hi,
                                 net_pnl_rs=float(np.sum(pnl)),
                                 net_return_on_10l_capital_pct=float(np.sum(pnl))/10000,
                                 avg_net_trade_rs=float(np.sum(pnl))/trades if trades else 0.,
                                 profit_factor=gain/loss if loss else np.nan,
                                 profitable_days=int(np.sum(pnl > 1e-8)),
                                 losing_days=int(np.sum(pnl < -1e-8)),
                                 flat_days=int(np.sum(np.abs(pnl) <= 1e-8)),
                                 worst_day_rs=float(np.min(pnl)) if len(pnl) else 0.,
                                 max_daily_close_drawdown_rs=float(np.max(np.maximum.accumulate(equity)-equity)),
                                 losing_trade_sum_abs_rs=loss,
                                 stop_exits=int(part.stop_exits.sum()),
                                 target_exits=int(part.target_exits.sum())))
    return pd.DataFrame(rows)


def _development_choice(overall: pd.DataFrame) -> tuple[str | None, pd.DataFrame]:
    reference = overall.loc[overall.variant.eq("V14_V1_BASELINE") &
                            overall.period.eq("development") & overall.cost_bps.eq(9.)].iloc[0]
    rows = []
    for variant in VARIANTS:
        item = overall.loc[overall.variant.eq(variant) &
                           overall.period.eq("development") & overall.cost_bps.eq(9.)].iloc[0]
        failures = []
        if item.trades < 30: failures.append("fewer than 30 development trades")
        if item.win_rate_pct < 70: failures.append("development win rate below 70%")
        if item.net_pnl_rs <= 0: failures.append("nonpositive development net at 9bps")
        if item.max_daily_close_drawdown_rs > reference.max_daily_close_drawdown_rs + 1e-8:
            failures.append("development drawdown worse than V14 v1")
        rows.append(dict(variant=variant, eligible=not failures,
                         failure_reasons="; ".join(failures),
                         development_9bps_net_rs=item.net_pnl_rs,
                         development_9bps_drawdown_rs=item.max_daily_close_drawdown_rs))
    screen = pd.DataFrame(rows)
    eligible = screen.loc[screen.eligible].sort_values(
        ["development_9bps_net_rs", "development_9bps_drawdown_rs", "variant"],
        ascending=[False, True, True])
    return (str(eligible.iloc[0].variant) if len(eligible) else None), screen


def _identity(book: pd.DataFrame) -> pd.Series:
    return (book.day.astype(str).str[:10] + "|" + book.setup_id.astype(str) +
            "|" + book.tradingsymbol.astype(str))


def _trade_changes(books: pd.DataFrame, baseline: pd.DataFrame) -> pd.DataFrame:
    original = baseline.loc[_flags(baseline.portfolio_executed, "baseline executions")].copy()
    original["identity"] = _identity(original)
    if original.identity.duplicated().any():
        raise ValueError("Duplicate V14 v1 trade identity")
    original_winners = set(original.loc[original.portfolio_net_profit_rupees.gt(0), "identity"])
    original_all = set(original.identity)
    rows = []
    for variant in VARIANTS:
        part = books.loc[books.variant.eq(variant) & books.cost_bps.eq(5.)].copy()
        executed = part.loc[_flags(part.portfolio_executed, "new executions")].copy()
        executed["identity"] = _identity(executed)
        new = executed.loc[~executed.identity.isin(original_all)]
        admitted = set(executed.identity)
        rows.append(dict(variant=variant,
                         newly_executed_winners=int(new.portfolio_net_profit_rupees.gt(0).sum()),
                         newly_executed_losers=int(new.portfolio_net_profit_rupees.lt(0).sum()),
                         omitted_v14_v1_winners=len(original_winners-admitted),
                         retained_v14_v1_winners=len(original_winners & admitted)))
    return pd.DataFrame(rows)


def run(out: Path) -> dict:
    out = Path(out).resolve()
    if out.exists() or not out.is_relative_to(OUTROOT.resolve()):
        raise ValueError("Use a new output folder inside the V14 run root")
    protocol = read_json(PROTOCOL)
    if tuple(item["id"] for item in protocol["variants"]) != VARIANTS:
        raise ValueError("Frozen rule protocol changed")
    baseline_daily, baseline_book, days = _verify_baseline()
    lab = _verify_lab()
    if set(lab) != set(days):
        raise ValueError("Minute input calendar differs from V14 v1")
    _source_days(days)
    _, source_days, base_config = source.source_inputs()
    if source_days != days[:len(source_days)]:
        raise ValueError("G7 V4 original base config source calendar drift")
    out.mkdir(parents=True)
    daily = []
    books = []
    coverage = []
    for number, day in enumerate(days, 1):
        candidates = pd.read_parquet(SOURCE / "candidates_by_day" / f"{day}.parquet")
        if candidates.sid.isna().any() or candidates.sid.duplicated().any():
            raise ValueError(f"Duplicate candidate sid on {day}")
        if not candidates.day.astype(str).eq(day).all():
            raise ValueError(f"Candidate date mismatch on {day}")
        minute = pd.read_parquet(LAB / day / "minutes.parquet")
        selections = rules.evaluate_variants(candidates)
        if set(selections) != set(VARIANTS):
            raise ValueError("Rules returned a different variant inventory")
        if "execution_ready" not in candidates:
            raise ValueError("Candidate builder must disclose next-minute execution readiness")
        coverage.append(dict(day=day, full_pool=len(candidates),
                             valid_six_features=int(candidates.integrity_ok.sum()),
                             **{f"selected_{variant}":len(selections[variant]) for variant in VARIANTS},
                             **{f"execution_ready_{variant}":int(selections[variant].execution_ready.sum())
                                for variant in VARIANTS}))
        for variant in VARIANTS:
            # Decide/allocate the slot quota from completed signal fields first.
            # A selected symbol without the following confirmation cannot be
            # executed; it is removed without backfilling the quota.  Looking
            # at next-minute availability while choosing the symbol would leak
            # future information into the 5m signal decision.
            selected = selections[variant].loc[
                selections[variant].execution_ready.eq(True)].copy().reset_index(drop=True)
            per_cost = execution.simulate_allocated(selected, minute, COSTS,
                                                   base_config=base_config, policy=variant)
            for cost in COSTS:
                book = per_cost[cost]
                row = _daily_metric(book, day)
                row.update(variant=variant, cost_bps=cost, period=_period(day))
                daily.append(row)
                if len(book):
                    book = book.copy()
                    book["variant"] = variant
                    book["cost_bps"] = cost
                    books.append(book)
        print(f"{day} [{number}/{len(days)}] full={len(candidates)} valid={coverage[-1]['valid_six_features']} "
              + " ".join(f"{v}={coverage[-1][f'selected_{v}']}" for v in VARIANTS), flush=True)
    daily = pd.concat([baseline_daily, pd.DataFrame(daily)], ignore_index=True, sort=False)
    daily["period"] = daily.day.map(_period)
    all_books = pd.concat(books, ignore_index=True, sort=False) if books else pd.DataFrame()
    overall = _aggregate(daily)
    chosen, screen = _development_choice(overall)
    changes = _trade_changes(all_books, baseline_book) if len(all_books) else pd.DataFrame()
    coverage = pd.DataFrame(coverage)
    daily.to_csv(out / "daywise_by_cost.csv", index=False)
    overall.to_csv(out / "overall_by_period_and_cost.csv", index=False)
    coverage.to_csv(out / "candidate_coverage_by_day.csv", index=False)
    screen.to_csv(out / "development_rule_screen.csv", index=False)
    changes.to_csv(out / "trade_changes_vs_v14_v1.csv", index=False)
    all_books.to_csv(out / "all_variant_orders.csv", index=False)
    if len(all_books):
        executed = all_books.loc[_flags(all_books.portfolio_executed, "executed")].copy()
        executed.to_csv(out / "executed_trades.csv", index=False)
        executed["slot"] = pd.to_datetime(executed.signal_ts, utc=True).dt.tz_convert(IST).dt.strftime("%H:%M")
        slot = executed.groupby(["variant", "cost_bps", "slot", "side"], dropna=False).agg(
            trades=("sid", "size"), wins=("portfolio_net_profit_rupees", lambda x: int((x > 0).sum())),
            net_pnl_rs=("portfolio_net_profit_rupees", "sum")).reset_index()
        slot["win_rate_pct"] = 100 * slot.wins / slot.trades
        slot.to_csv(out / "slot_side_results.csv", index=False)
    source_proof = read_json(SOURCE / "manifest.json")
    summary = dict(variant_id="V13-V10-G-14-SIX-FEATURE-RESEARCH",
                   scope="BACKTEST_ONLY", execution_authority=False,
                   baseline_v14_v1=str(BASELINE),
                   sessions=len(days), first_day=days[0], last_day=days[-1],
                   rule_protocol_sha256=sha(PROTOCOL),
                   candidate_source_manifest_sha256=sha(SOURCE / "manifest.json"),
                   v14_config_sha256=sha(CONFIG),
                   selected_on_development_only=chosen,
                   selected_assessment_9bps=(overall.loc[overall.variant.eq(chosen) &
                       overall.period.eq("assessment") & overall.cost_bps.eq(9.)].iloc[0].to_dict()
                       if chosen else None),
                   method="Full prefilter stock pool, six causal selection fields, same 12 setup targets and V4 stops/protection, fresh minute fills and integer portfolio allocation at each cost.",
                   limitations=["Historical rules/exit policy already researched; assessment is not virgin future data.",
                                "09:20 OI delta measures 09:16 to 09:20 (four minutes); OI day is zero there.",
                                "Five-, nine-, fifteen-bps costs are modeled, not broker slippage observations.",
                                "No live/paper change; V14 v1 remains untouched for rollback."],
                   source_proof=source_proof)
    dump(out / "summary.json", summary)
    (out / "README.md").write_text(
        "# V14 six-field morning backtest\n\n"
        f"Source sessions: {days[0]} through {days[-1]} ({len(days)} sessions). "
        "See `summary.json`, `overall_by_period_and_cost.csv`, `daywise_by_cost.csv`, "
        "`slot_side_results.csv`, and `executed_trades.csv`. "
        "All rule thresholds were frozen in `research/g14_six_feature_rule_protocol.json` before outcomes. "
        "This run is research only and leaves the V14 v1 baseline unchanged.\n",
        encoding="utf-8")
    dump(out / "artifact_manifest.json", dict(
        files={p.name: sha(p) for p in out.iterdir() if p.is_file() and p.name != "artifact_manifest.json"},
        code={str(p.relative_to(ROOT)): sha(p) for p in (Path(__file__), PROTOCOL,
             ROOT / "research/g14_six_feature_execution.py",
             ROOT / "research/g14_six_feature_rules.py",
             ROOT / "research/g14_six_feature_prepare.py")}))
    print(overall.loc[overall.cost_bps.eq(9.)].to_string(index=False), flush=True)
    print("DEVELOPMENT-SELECTED:", chosen or "NO VARIANT QUALIFIED", flush=True)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    stamp = datetime.now(IST).strftime("%Y%m%dT%H%M%S")
    out = args.output_dir or OUTROOT / f"{stamp}_six_feature_v2"
    run(out)
    print(out.resolve())


if __name__ == "__main__":
    main()
