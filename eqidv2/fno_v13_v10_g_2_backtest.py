"""V13-v10-G-2: retained G entries/targets with one scheduled stop tightening.

This is a research-only exit sensitivity.  It reads the immutable, hash-verified
V13-v10-G full-history bundle, reconstructs the retained G selections from the
sealed signals, and replays a 1.25% stop tightened to 1.00% after 120 minutes. Targets,
entry expiry, costs, sizing, leverage and portfolio rules stay unchanged.
An explicit --relaxed-0925-long option compares the requested raw-feature
09:25 LONG exception with this original staged-stop baseline. It is not live.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
from dataclasses import asdict, replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_v13_v10_g_backtest as g


VERSION = "V13-v10-G-2"
# Kept for historical fixed-1% research callers; the current strategy uses INITIAL_STOP_PCT.
STOP_PCT = 1.0
INITIAL_STOP_PCT = 1.25
TIGHTENED_STOP_PCT = 1.0
TIGHTEN_AFTER_MINUTES = 120
MINUTE_NS = 60_000_000_000
EVIDENCE = "RETROSPECTIVE_EXIT_SENSITIVITY_REUSED_HISTORY_NOT_PROMOTED"
DEFAULT_SOURCE_BUNDLE = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_v10_g_full_history"
) / "run_20260925_cutoff_corrected_through_20260923"
DEFAULT_G_CONFIG = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g"
) / "run_20260914_opportunity_expansion/frozen_config.json"
DEFAULT_OUTPUT = Path(
    r"C:\TradingData\eqidv2\fno_oi\strategy_research\v13_corrected_v10_g_2"
) / "run_20261005_staged125_to100_120m_through_20260923"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected a JSON object: {path}")
    return value


def dump_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )


def transformed_exit(source_exit: dict[str, Any], *, legacy_fixed: bool = False) -> dict[str, Any]:
    """Keep every G target; legacy mode is only for validating historical reports."""
    initial = STOP_PCT if legacy_fixed else INITIAL_STOP_PCT
    result = copy.deepcopy(source_exit)
    result.update(
        version=VERSION,
        source_version=str(source_exit.get("version", "V13-v10-G")),
        rule=("Every active setup stop is exactly 1.00%; targets are unchanged from retained G."
              if legacy_fixed else "Start at 1.25%; tighten once to 1.00% after 120 minutes from entry; targets unchanged."),
        stop_pct=initial,
        target_policy="UNCHANGED_FROM_RETAINED_G",
        evidence=EVIDENCE,
    )
    result["default"]["stop_pct"] = initial
    for pair in result["setups"].values():
        pair["stop_pct"] = initial
    if not legacy_fixed:
        result["scheduled_tightening"] = {
            "after_minutes": TIGHTEN_AFTER_MINUTES,
            "stop_pct": TIGHTENED_STOP_PCT,
            "reference": "ACTUAL_ENTRY_PRICE",
            "activation": "FIRST_BAR_OPEN_AT_OR_AFTER_ENTRY_BAR_END_PLUS_DELAY",
        }
    validate_exit(result, source_exit, legacy_fixed=legacy_fixed)
    return result


def validate_exit(candidate: dict[str, Any], source_exit: dict[str, Any], *, legacy_fixed: bool = False) -> None:
    source_pairs = {"__DEFAULT__": source_exit["default"], **source_exit["setups"]}
    candidate_pairs = {"__DEFAULT__": candidate["default"], **candidate["setups"]}
    if set(candidate_pairs) != set(source_pairs):
        raise ValueError("G-2 exit setup keys must exactly match retained G")
    for setup_id, pair in candidate_pairs.items():
        stop = float(pair["stop_pct"])
        target = float(pair["target_pct"])
        if not np.isfinite(stop) or stop != (STOP_PCT if legacy_fixed else INITIAL_STOP_PCT):
            raise ValueError(f"G-2 initial stop drift for {setup_id}")
        if not np.isfinite(target) or target != float(source_pairs[setup_id]["target_pct"]):
            raise ValueError(f"G-2 target drift for {setup_id}")
    if candidate.get("partial_exits") is not False or candidate.get("breakeven_stop") is not False:
        raise ValueError("G-2 retains full exits without a breakeven stop")


def config(source_g: dict[str, Any], *, legacy_fixed: bool = False) -> dict[str, Any]:
    result = copy.deepcopy(source_g)
    result.update(
        version=VERSION,
        source_version=str(source_g["version"]),
        exit=transformed_exit(source_g["exit"], legacy_fixed=legacy_fixed),
        stop_change={
            "mode": "ABSOLUTE_ALL_ACTIVE_SETUPS",
            "stop_pct": STOP_PCT,
            "targets": "UNCHANGED",
        },
        evidence=EVIDENCE,
        live_configuration_changed=False,
        execution_authority=False,
    )
    if not legacy_fixed:
        result["stop_change"] = {
            "mode": "ONE_SCHEDULED_TIGHTENING_ALL_ACTIVE_SETUPS",
            "initial_stop_pct": INITIAL_STOP_PCT,
            "tightened_stop_pct": TIGHTENED_STOP_PCT,
            "tighten_after_minutes": TIGHTEN_AFTER_MINUTES,
            "targets": "UNCHANGED",
        }
    return result


def checked_settings(candidate: dict[str, Any], source_g: dict[str, Any]) -> dict[str, Any]:
    expected = config(source_g)
    if candidate != expected:
        raise ValueError("G-2 settings differ from the staged 1.25% to 1.00% stop experiment")
    validate_exit(candidate["exit"], source_g["exit"])
    return expected


def verify_bundle(root: Path) -> dict[str, Any]:
    manifest_path = root / "bundle_manifest.json"
    manifest = read_json(manifest_path)
    if manifest.get("state") != "COMPLETE":
        raise ValueError("Source bundle is not COMPLETE")
    if manifest.get("transform", {}).get("strategy_outputs_unchanged") is not True:
        raise ValueError("Source bundle does not attest unchanged strategy outputs")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or not artifacts:
        raise ValueError("Source bundle has no artifact inventory")
    mismatches = []
    for relative, record in artifacts.items():
        path = root / relative
        observed = sha256(path) if path.is_file() else None
        if observed != record.get("sha256"):
            mismatches.append(relative)
    if mismatches:
        raise RuntimeError(f"Source bundle artifact drift: {', '.join(mismatches)}")
    return manifest


def _selection_keys(frame: pd.DataFrame) -> list[tuple[int, str]]:
    return sorted(zip(frame["sid"].astype(int), frame["setup_id"].astype(str)))


def load_bundle(source: Path, g_config_path: Path) -> dict[str, Any]:
    """Load sealed artifacts without consulting mutable upstream raw files."""
    source = source.resolve()
    manifest = verify_bundle(source)
    run_metadata = read_json(source / "g_backtest/run_metadata.json")
    source_g = read_json(g_config_path)
    recorded_config_hash = str(run_metadata.get("frozen_g_config_sha256") or "")
    if sha256(g_config_path) != recorded_config_hash:
        raise RuntimeError("Retained G configuration does not match the sealed bundle")
    # This validates the complete retained-G contract, not only the exit table.
    g.checked_settings(source_g)

    dataset_manifest = read_json(source / "dataset/dataset_manifest.json")
    signals = pd.read_parquet(source / "dataset/signals.parquet")
    base = replace(
        g.v9.V9Config(),
        portfolio_capital_rupees=float(source_g["portfolio_capital_rupees"]),
        capital_per_entry_rupees=float(source_g["capital_per_entry_rupees"]),
        leverage_factor=float(source_g["leverage_factor"]),
        max_positions=source_g["max_positions"],
        cost_bps=float(source_g["cost_bps"]),
    )
    base.validate()
    change = g.SelectionChange(**source_g["selection_change"])
    orders = g.select_orders(
        signals,
        base,
        change,
        core_first=source_g["core_first"],
        morning_slots=source_g.get("morning_slots", False),
        two_bar_continuation=source_g.get("two_bar_continuation", False),
    )
    sealed_orders = pd.read_csv(source / "g_backtest/selected_trades.csv")
    if _selection_keys(orders) != _selection_keys(sealed_orders):
        raise RuntimeError("Reconstructed G selections differ from the sealed selection ledger")

    paths: dict[int, dict[str, np.ndarray]] = {}
    needed = set(orders["sid"].astype(int))
    with np.load(source / "dataset/paths.npz", allow_pickle=False) as archive:
        for name in archive.files:
            sid_text, field = name.split("_", 1)
            sid = int(sid_text)
            if sid in needed:
                paths.setdefault(sid, {})[field] = archive[name]
    g.v9.validate_paths(orders, paths)
    days = [date.fromisoformat(value) for value in dataset_manifest["days"]]
    return {
        "source": source,
        "g_config_path": g_config_path.resolve(),
        "bundle_manifest": manifest,
        "dataset_manifest": dataset_manifest,
        "source_g": source_g,
        "signals": signals,
        "orders": orders,
        "paths": paths,
        "days": days,
        "v9_config": base,
    }


RELAXED_0925_LONG = dict(oi_max_pct=1.20, minimum_volume_ratio=1.75,
                         minimum_body_ratio=.54, ignore_ema_alignment=True,
                         scope="09:25 LONG only; original selections have priority",
                         execution_authority=False)


def apply_relaxed_0925_long(original_orders, observed_features):
    """Reconsider raw rejected observations, retaining every original G2 choice.

    Ignore EMA only at 09:25 LONG, cap OI at 1.20%, require volume >=1.75
    and body >=.54. All other gates and the one-order-per-day setup quota stay.
    """
    raw = observed_features.copy()
    raw['signal_ts'] = pd.to_datetime(raw.signal_ts, utc=True).dt.tz_convert('Asia/Kolkata')
    raw['confirmation_ts'] = pd.to_datetime(raw.confirmation_ts, utc=True).dt.tz_convert('Asia/Kolkata')
    raw = raw.loc[raw.signal_ts.dt.strftime('%H:%M:%S.%f').eq('09:25:00.000000')].copy()
    raw['day'] = raw.signal_ts.dt.date
    if raw.duplicated(['day', 'tradingsymbol']).any():
        raise ValueError('Duplicate raw 09:25 stock observations')
    fields = ['oi', 'prev_oi', 'oi_change_pct', 'price_change_pct', 'volume_ratio',
              'signal_close', 'confirmation_open', 'confirmation_high', 'confirmation_low',
              'confirmation_close', 'body_ratio', 'v9_1m_upper_wick_ratio',
              'v9_1m_volume_ratio', 'traded_value']
    raw[fields] = raw[fields].apply(pd.to_numeric, errors='coerce')
    checks = dict(finite=np.isfinite(raw[fields]).all(axis=1),
        exact_confirmation=raw.confirmation_ts.sub(raw.signal_ts).eq(pd.Timedelta(minutes=1)),
        oi_increasing=raw.prev_oi.gt(0) & raw.oi.gt(raw.prev_oi),
        oi_min=raw.oi_change_pct.ge(.10), oi_max=raw.oi_change_pct.le(RELAXED_0925_LONG['oi_max_pct']),
        price=raw.price_change_pct.ge(.30), volume=raw.volume_ratio.ge(RELAXED_0925_LONG['minimum_volume_ratio']),
        confirmation_range=raw.confirmation_high.gt(raw.confirmation_low),
        confirmation_direction=raw.confirmation_close.gt(raw.confirmation_open)
                               & raw.confirmation_close.gt(raw.signal_close),
        body=raw.body_ratio.between(RELAXED_0925_LONG['minimum_body_ratio'], 1.),
        wick=raw.v9_1m_upper_wick_ratio.between(0., .60),
        confirmation_volume=raw.v9_1m_volume_ratio.ge(1.20), liquidity=raw.traded_value.ge(0))
    if 'confirmation_source_flagged' in raw:
        checks['real_confirmation'] = raw.confirmation_source_flagged.eq(False)
    if 'v9_exact_confirmation_present' in raw:
        checks['confirmation_present'] = raw.v9_exact_confirmation_present.eq(True)
    if 'v9_1m_feature_ts' in raw:
        checks['feature_clock'] = pd.to_datetime(raw.v9_1m_feature_ts, utc=True).eq(raw.confirmation_ts)
    check_frame = pd.DataFrame(checks).fillna(False)
    raw['relaxed_0925_pass'] = check_frame.all(axis=1)
    raw['relaxed_0925_failed_rules'] = pd.Series(
        [';'.join(check_frame.columns[~row]) for row in check_frame.to_numpy(bool)],
        index=raw.index, dtype=str)
    original = original_orders.copy()
    original['day'] = pd.to_datetime(original.day).dt.date
    original['relaxed_0925_added'] = False
    held = original.loc[original.setup_id.eq('0926_LONG')]
    if held.groupby('day').size().gt(1).any():
        raise ValueError('Original 09:25 LONG exceeds one-order quota')
    ranked = raw.loc[raw.relaxed_0925_pass].sort_values(
        ['day', 'traded_value', 'tradingsymbol'], ascending=[True, False, True], kind='stable')
    raw['relaxed_0925_rank'] = ranked.groupby('day').cumcount()+1
    additions = ranked.loc[~ranked.day.isin(held.day)].groupby('day', sort=False).head(1).copy()
    start = int(original.sid.max())+1 if len(original) else 0
    additions['sid'] = np.arange(start, start+len(additions))
    additions['side'] = 'LONG'
    additions['setup_id'] = '0926_LONG'
    additions['hhmm_int'] = 925
    additions['trigger'] = additions.confirmation_high
    additions['wick_ratio'] = additions.v9_1m_upper_wick_ratio
    additions['v9_1m_feature_ts'] = additions.confirmation_ts
    additions['relaxed_0925_added'] = True
    raw['relaxed_0925_added'] = raw.index.isin(additions.index)
    raw['original_slot_occupied'] = raw.day.isin(held.day)
    frames = [part for part in (original, additions) if not part.empty]
    combined = pd.concat(frames, ignore_index=True, sort=False) if frames else original
    # Older CSV selections have fixed-offset timestamps; new raw observations
    # use the named IST timezone. Normalize before downstream path validation.
    for field in ('signal_ts', 'confirmation_ts', 'v9_1m_feature_ts'):
        if field in combined:
            combined[field] = pd.to_datetime(combined[field], utc=True).dt.tz_convert('Asia/Kolkata')
    return combined.sort_values(['day','hhmm_int','side','setup_id','tradingsymbol']).reset_index(drop=True), raw


def staged_exit(path, entry_index: int, entry: float, is_long: bool, target_pct: float):
    """Causal minute replay. Times label candle ends, not candle opens.

    Use the entry candle end as the conservative timer origin (actual fill can
    be up to one minute earlier). Never apply a later candle close to its open.
    Retain the native stop-first convention for ambiguous intrabar touches.
    """
    sign = 1 if is_long else -1
    target = entry * (1 + sign * target_pct / 100)
    activation = int(path["timestamp_ns"][entry_index]) + TIGHTEN_AFTER_MINUTES * MINUTE_NS
    active_stop = INITIAL_STOP_PCT
    for j in range(entry_index, len(path["close"])):
        if int(path["timestamp_ns"][j]) - MINUTE_NS >= activation:
            active_stop = TIGHTENED_STOP_PCT
        stop = entry * (1 - sign * active_stop / 100)
        op, hi, lo = (float(path[key][j]) for key in ("open", "high", "low"))
        stop_open = op <= stop if is_long else op >= stop
        stop_hit = lo <= stop if is_long else hi >= stop
        target_hit = hi >= target if is_long else lo <= target
        if stop_hit:
            at_open = j > entry_index and stop_open
            return (j, op if at_open else stop,
                    "TIGHTENED_STOP" if active_stop < INITIAL_STOP_PCT else "STOP",
                    active_stop, "OPEN" if at_open else "INTRABAR")
        if target_hit:
            return j, target, "TARGET", active_stop, "INTRABAR"
    return len(path["close"]) - 1, float(path["close"][-1]), "TIME_EXIT_1515", active_stop, "CLOSE"


def simulate_staged(orders, paths, *, cost_bps: float, max_entry_delay_minutes: int):
    """Reuse native entry fills, then recompute all exit-dependent fields."""
    work = orders.copy()
    work["native_stop_pct"] = INITIAL_STOP_PCT
    native = g.v9.v5.simulate_native(
        work, paths, cost_bps=cost_bps, max_entry_delay_minutes=max_entry_delay_minutes
    )
    frame = native.copy()
    for ix, row in native.iterrows():
        if not row.filled:
            continue
        path = paths[int(row.sid)]
        entry, start = float(row.entry_price), int(row.entry_path_index)
        is_long = row.side == "LONG"
        sign = 1 if is_long else -1
        j, price, reason, active_stop, event = staged_exit(
            path, start, entry, is_long, float(row.native_target_pct)
        )
        gross = sign * (price / entry - 1) * 100
        excursion_end = max(start, j - 1) if event == "OPEN" else j
        mfe, mae = g.v9.v5._excursions(path, entry, start, excursion_end, is_long)
        bar_end = pd.Timestamp(int(path["timestamp_ns"][j]), tz="UTC").tz_convert("Asia/Kolkata")
        exit_ts = bar_end - pd.Timedelta(minutes=1) if event == "OPEN" else bar_end
        stop_hit = reason in ("STOP", "TIGHTENED_STOP")
        stop_level = entry * (1 - sign * active_stop / 100)
        target_level = entry * (1 + sign * float(row.native_target_pct) / 100)
        target_in_bar = path["high"][j] >= target_level if is_long else path["low"][j] <= target_level
        gap = stop_hit and event == "OPEN" and abs(price - stop_level) > 1e-8
        changes = dict(
            exit_path_index=j, exit_price=price, exit_ts=exit_ts,
            exit_bar_end_ts=str(bar_end), exit_execution_ts=str(exit_ts), exit_event=event,
            exit_reason=reason, gross_return_pct=gross, net_return_pct=gross-cost_bps/100,
            mfe_pct=max(mfe, gross), mae_pct=min(mae, gross),
            holding_minutes=(exit_ts-pd.Timestamp(row.entry_ts)).total_seconds()/60,
            initial_stop_pct=INITIAL_STOP_PCT, active_stop_pct_at_exit=active_stop,
            stop_hit=stop_hit, target_hit=reason == "TARGET", first_target_hit=reason == "TARGET",
            runner_target_hit=reason == "TARGET",
            same_bar_ambiguous=bool(stop_hit and event == "INTRABAR" and target_in_bar),
            exit_gap_through=gap, exit_gap_bps=abs(price/stop_level-1)*10000 if gap else 0.0,
        )
        for key, value in changes.items():
            frame.loc[ix, key] = value
    return frame


def evaluate(dataset: dict[str, Any], settings: dict[str, Any]):
    source_g = dataset["source_g"]
    settings = checked_settings(settings, source_g)
    orders = dataset["orders"].copy()
    exits = settings["exit"]
    target_map = {key: float(pair["target_pct"]) for key, pair in exits["setups"].items()}
    orders["native_stop_pct"] = INITIAL_STOP_PCT
    orders["native_target_pct"] = orders["setup_id"].map(target_map).fillna(
        float(exits["default"]["target_pct"])
    )
    g.v9.validate_paths(orders, dataset["paths"])
    base = dataset["v9_config"]
    trades = simulate_staged(
        orders,
        dataset["paths"],
        cost_bps=base.cost_bps,
        max_entry_delay_minutes=int(settings["entry_expiry_minutes"]),
    )
    trades = g.v9.v5.apply_fixed_capital_model(
        trades, base.capital_per_entry_rupees, base.leverage_factor
    )
    trades["v10_g_2_stop_pct"] = INITIAL_STOP_PCT
    trades["v10_g_2_tightened_stop_pct"] = TIGHTENED_STOP_PCT
    trades["v10_g_2_tighten_after_minutes"] = TIGHTEN_AFTER_MINUTES
    trades["v10_g_2_target_pct"] = trades["native_target_pct"]
    trades["v10_g_2_reward_risk"] = (
        trades["v10_g_2_target_pct"] / trades["v10_g_2_stop_pct"]
    )
    ledger, summary = g.v9.v6.apply_portfolio_constraints(
        trades, base.portfolio_config()
    )
    summary.update(
        version=VERSION,
        source_version=source_g["version"],
        settings=settings,
        evidence=EVIDENCE,
        initial_stop_pct=INITIAL_STOP_PCT,
        tightened_stop_pct=TIGHTENED_STOP_PCT,
        tighten_after_minutes=TIGHTEN_AFTER_MINUTES,
        target_policy="UNCHANGED_FROM_RETAINED_G",
        selection_count=len(orders),
        selection_identity_unchanged=True,
        partial_exits=False,
        breakeven_stop=False,
        square_off="15:15 Asia/Kolkata",
        live_configuration_changed=False,
        execution_authority=False,
    )
    return trades, ledger, summary


def _metric_rows(frame: pd.DataFrame, group: str) -> dict[str, Any]:
    pnl = pd.to_numeric(frame["portfolio_net_profit_rupees"], errors="coerce").fillna(0.0)
    gross = pd.to_numeric(frame["portfolio_gross_profit_rupees"], errors="coerce").fillna(0.0)
    cost = pd.to_numeric(frame["portfolio_cost_rupees"], errors="coerce").fillna(0.0)
    positive, negative = pnl[pnl > 1e-9], pnl[pnl < -1e-9]
    return {
        "group": group,
        "trades": len(frame),
        "wins": int((pnl > 1e-9).sum()),
        "losses": int((pnl < -1e-9).sum()),
        "win_rate_pct": float((pnl > 1e-9).mean() * 100) if len(frame) else 0.0,
        "profit_factor": float(positive.sum() / -negative.sum()) if len(negative) else None,
        "gross_pnl_rupees": float(gross.sum()),
        "cost_rupees": float(cost.sum()),
        "net_pnl_rupees": float(pnl.sum()),
        "average_trade_rupees": float(pnl.mean()) if len(frame) else 0.0,
        "median_trade_rupees": float(pnl.median()) if len(frame) else 0.0,
    }


def _executed(ledger: pd.DataFrame) -> pd.DataFrame:
    return ledger.loc[ledger["portfolio_executed"].eq(True)].copy()


def build_breakdowns(ledger: pd.DataFrame, days: list[date]) -> dict[str, pd.DataFrame]:
    executed = _executed(ledger)
    executed["day"] = pd.to_datetime(executed["day"])
    sessions = pd.DatetimeIndex(pd.to_datetime(days))
    daily = pd.DataFrame(index=sessions)
    selected_dates = pd.to_datetime(ledger["day"])
    daily["selected_orders"] = ledger.groupby(selected_dates).size().reindex(sessions, fill_value=0)
    daily["trades"] = executed.groupby("day").size().reindex(sessions, fill_value=0)
    daily["wins"] = executed.groupby("day")["portfolio_net_profit_rupees"].apply(
        lambda values: int((values > 1e-9).sum())
    ).reindex(sessions, fill_value=0)
    daily["losses"] = executed.groupby("day")["portfolio_net_profit_rupees"].apply(
        lambda values: int((values < -1e-9).sum())
    ).reindex(sessions, fill_value=0)
    for target, source in (
        ("gross_pnl_rupees", "portfolio_gross_profit_rupees"),
        ("cost_rupees", "portfolio_cost_rupees"),
        ("net_pnl_rupees", "portfolio_net_profit_rupees"),
    ):
        daily[target] = executed.groupby("day")[source].sum().reindex(sessions, fill_value=0.0)
    daily["cumulative_net_pnl_rupees"] = daily["net_pnl_rupees"].cumsum()
    peak = daily["cumulative_net_pnl_rupees"].cummax().clip(lower=0.0)
    daily["drawdown_rupees"] = peak - daily["cumulative_net_pnl_rupees"]
    daily.index.name = "day"
    daily = daily.reset_index()

    def grouped(column: str) -> pd.DataFrame:
        rows = [_metric_rows(group, str(key)) for key, group in executed.groupby(column, sort=True)]
        return pd.DataFrame(rows)

    month_rows = []
    for month, group in executed.groupby(executed["day"].dt.to_period("M"), sort=True):
        row = _metric_rows(group, str(month))
        row["sessions"] = int((sessions.to_period("M") == month).sum())
        row["selected_orders"] = int(
            ledger.loc[pd.to_datetime(ledger["day"]).dt.to_period("M").eq(month)].shape[0]
        )
        month_rows.append(row)
    return {
        "daily": daily,
        "monthly": pd.DataFrame(month_rows),
        "side": grouped("side"),
        "setup": grouped("setup_id").sort_values("net_pnl_rupees", ascending=False),
        "exit": grouped("exit_reason").sort_values("trades", ascending=False),
    }


def _fmt_money(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}Rs {abs(value):,.2f}"


def _fmt_pf(value: Any) -> str:
    return "N/A" if value is None or pd.isna(value) else f"{float(value):.4f}"


def write_report(
    output: Path,
    dataset: dict[str, Any],
    ledger: pd.DataFrame,
    summary: dict[str, Any],
    breakdowns: dict[str, pd.DataFrame],
) -> Path:
    days = dataset["days"]
    control = dataset.get("control_ledger")
    if control is None:
        control = pd.read_csv(dataset["source"] / "g_backtest/portfolio_trades.csv")
    control_summary = dataset.get("control_summary") or read_json(dataset["source"] / "g_backtest/summary.json")
    control = control.copy()
    ledger = ledger.copy()
    control["day"] = pd.to_datetime(control["day"]).dt.strftime("%Y-%m-%d")
    ledger["day"] = pd.to_datetime(ledger["day"]).dt.strftime("%Y-%m-%d")
    control_metric = g.r.metric(control, days)
    candidate_metric = g.r.metric(ledger, days)
    control_detail = _metric_rows(_executed(control), "V13-v10-G")
    candidate_detail = _metric_rows(_executed(ledger), VERSION)
    comparison = pd.DataFrame([
        {"strategy": "V13-v10-G", **control_metric},
        {"strategy": VERSION, **candidate_metric},
    ])
    comparison["net_delta_vs_g_rupees"] = comparison["net_profit_rupees"] - float(
        control_metric["net_profit_rupees"]
    )
    comparison.to_csv(output / "g_vs_g2_comparison.csv", index=False)

    identity = ["sid", "setup_id", "day", "tradingsymbol", "side"]
    outcome = [
        "filled", "exit_reason", "portfolio_executed", "portfolio_net_profit_rupees",
        "native_stop_pct", "native_target_pct",
    ]
    changes = control[identity + outcome].merge(
        ledger[["day", "sid", "setup_id", *outcome]],
        on=["day", "sid", "setup_id"],
        how="outer",
        validate="one_to_one",
        suffixes=("_g", "_g2"),
        indicator=True,
    )
    if not changes["_merge"].eq("both").all():
        raise RuntimeError("G/G-2 trade identity mismatch during comparison")
    changes["net_delta_rupees"] = (
        pd.to_numeric(changes["portfolio_net_profit_rupees_g2"], errors="coerce").fillna(0.0)
        - pd.to_numeric(changes["portfolio_net_profit_rupees_g"], errors="coerce").fillna(0.0)
    )
    changes["exit_changed"] = changes["exit_reason_g"].fillna("").ne(
        changes["exit_reason_g2"].fillna("")
    )
    changes["avoided_g_stop"] = changes["exit_reason_g"].eq("STOP") & ~changes[
        "exit_reason_g2"
    ].isin(["STOP", "TIGHTENED_STOP"])
    material_changes = changes.loc[
        changes["exit_changed"] | changes["net_delta_rupees"].abs().gt(0.01)
    ].sort_values(["day", "sid"], kind="stable")
    material_changes.to_csv(output / "trade_change_analysis.csv", index=False)

    stop_rows = []
    source_exit = dataset["source_g"]["exit"]
    for setup_id, pair in sorted(source_exit["setups"].items()):
        stop_rows.append({
            "setup_id": setup_id,
            "side": setup_id.split("_", 1)[1],
            "g_stop_pct": float(pair["stop_pct"]),
            "g2_initial_stop_pct": INITIAL_STOP_PCT,
            "g2_tightened_stop_pct": TIGHTENED_STOP_PCT,
            "tighten_after_minutes": TIGHTEN_AFTER_MINUTES,
            "target_pct_unchanged": float(pair["target_pct"]),
            "g2_initial_reward_risk": float(pair["target_pct"]) / INITIAL_STOP_PCT,
        })
    pd.DataFrame(stop_rows).to_csv(output / "stop_comparison.csv", index=False)

    for name, frame in breakdowns.items():
        frame.to_csv(output / f"{name}_results.csv", index=False)

    delta = candidate_metric["net_profit_rupees"] - control_metric["net_profit_rupees"]
    lines = [
        f"# {VERSION} full backtest — staged 1.25% to 1.00% after 120 minutes",
        "",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "Research-only retrospective exit sensitivity. G selections and targets are unchanged; "
        "every stop starts at 1.25% and tightens once to 1.00% after 120 minutes from entry. "
        "Live and paper configurations were not changed.",
        "",
        "## Headline comparison",
        "",
        "| Metric | V13-v10-G | V13-v10-G-2 |",
        "|---|---:|---:|",
    ]
    fields = [
        ("Selected orders", "selected_orders", ".0f"),
        ("Executed trades", "trades", ".0f"),
        ("Wins", "wins", ".0f"),
        ("Losses", "losses", ".0f"),
        ("Win rate", "win_rate_pct", ".2f"),
        ("Profit factor", "profit_factor", ".4f"),
        ("Net P&L", "net_profit_rupees", ".2f"),
        ("Daily-close maximum drawdown", "daily_close_drawdown_rupees", ".2f"),
        ("Median trade", "median_trade_rupees", ".2f"),
    ]
    for label, key, spec in fields:
        left, right = control_metric[key], candidate_metric[key]
        if key in {"net_profit_rupees", "daily_close_drawdown_rupees", "median_trade_rupees"}:
            left_text, right_text = _fmt_money(float(left)), _fmt_money(float(right))
        elif key == "win_rate_pct":
            left_text, right_text = f"{left:{spec}}%", f"{right:{spec}}%"
        else:
            left_text, right_text = f"{left:{spec}}", f"{right:{spec}}"
        lines.append(f"| {label} | {left_text} | {right_text} |")
    lines.extend([
        f"| Gross P&L | {_fmt_money(control_detail['gross_pnl_rupees'])} | "
        f"{_fmt_money(candidate_detail['gross_pnl_rupees'])} |",
        f"| Modeled costs | {_fmt_money(control_detail['cost_rupees'])} | "
        f"{_fmt_money(candidate_detail['cost_rupees'])} |",
        f"| Average trade | {_fmt_money(control_detail['average_trade_rupees'])} | "
        f"{_fmt_money(candidate_detail['average_trade_rupees'])} |",
        f"| Net return on Rs 10 lakh capital | "
        f"{control_metric['net_profit_rupees'] / 10_000:.2f}% | "
        f"{candidate_metric['net_profit_rupees'] / 10_000:.2f}% |",
        f"| Peak concurrent positions | {int(control_summary['peak_concurrent_positions'])} | "
        f"{int(summary['peak_concurrent_positions'])} |",
        f"| Peak reserved capital | {_fmt_money(float(control_summary['peak_reserved_capital_rupees']))} | "
        f"{_fmt_money(float(summary['peak_reserved_capital_rupees']))} |",
        f"| Peak modeled gross exposure | {_fmt_money(float(control_summary['peak_gross_exposure_rupees']))} | "
        f"{_fmt_money(float(summary['peak_gross_exposure_rupees']))} |",
        f"| Peak open initial risk | {_fmt_money(float(control_summary['peak_open_initial_risk_rupees']))} | "
        f"{_fmt_money(float(summary['peak_open_initial_risk_rupees']))} |",
    ])
    avoided = material_changes.loc[material_changes["avoided_g_stop"]]
    g_stop_count = int(_executed(control)["exit_reason"].eq("STOP").sum())
    became_target = int(avoided["exit_reason_g2"].eq("TARGET").sum())
    became_time = int(avoided["exit_reason_g2"].eq("TIME_EXIT_1515").sum())
    post_mask_g = pd.to_datetime(control["day"]).gt(pd.Timestamp("2026-09-11"))
    post_mask_g2 = pd.to_datetime(ledger["day"]).gt(pd.Timestamp("2026-09-11"))
    post_g = _metric_rows(_executed(control.loc[post_mask_g]), "G post-2026-09-11")
    post_g2 = _metric_rows(_executed(ledger.loc[post_mask_g2]), "G-2 post-2026-09-11")
    daily = breakdowns["daily"]
    best_day = daily.loc[daily["net_pnl_rupees"].idxmax()]
    worst_day = daily.loc[daily["net_pnl_rupees"].idxmin()]
    lines.extend([
        "",
        f"G-2 net change versus G: **{_fmt_money(float(delta))}**.",
        f"Of G's {g_stop_count} stop exits, {len(avoided)} were avoided: {became_target} later hit their "
        f"unchanged targets and {became_time} reached the 15:15 exit. The remaining "
        f"{g_stop_count - len(avoided)} stopped trades exited under the staged stop rule.",
        "",
        f"Post-2026-09-11 comparison: G produced {post_g['trades']} trades, "
        f"{post_g['win_rate_pct']:.2f}% wins, PF {_fmt_pf(post_g['profit_factor'])}, and "
        f"{_fmt_money(post_g['net_pnl_rupees'])}; G-2 produced {post_g2['trades']} trades, "
        f"{post_g2['win_rate_pct']:.2f}% wins, PF {_fmt_pf(post_g2['profit_factor'])}, and "
        f"{_fmt_money(post_g2['net_pnl_rupees'])}.",
        "",
        f"Best G-2 session: {pd.Timestamp(best_day['day']).date()}, "
        f"{_fmt_money(float(best_day['net_pnl_rupees']))}. Worst G-2 session: "
        f"{pd.Timestamp(worst_day['day']).date()}, "
        f"{_fmt_money(float(worst_day['net_pnl_rupees']))}.",
        "",
        "## Monthly results",
        "",
        "| Month | Sessions | Selected | Trades | W-L | Win rate | PF | Net P&L |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for row in breakdowns["monthly"].itertuples(index=False):
        lines.append(
            f"| {row.group} | {row.sessions} | {row.selected_orders} | {row.trades} | "
            f"{row.wins}-{row.losses} | {row.win_rate_pct:.2f}% | {_fmt_pf(row.profit_factor)} | "
            f"{_fmt_money(row.net_pnl_rupees)} |"
        )
    for title, key in (("Side results", "side"), ("Setup results", "setup"), ("Exit results", "exit")):
        lines.extend([
            "",
            f"## {title}",
            "",
            "| Group | Trades | W-L | Win rate | PF | Net P&L |",
            "|---|---:|---:|---:|---:|---:|",
        ])
        for row in breakdowns[key].itertuples(index=False):
            lines.append(
                f"| {row.group} | {row.trades} | {row.wins}-{row.losses} | "
                f"{row.win_rate_pct:.2f}% | {_fmt_pf(row.profit_factor)} | "
                f"{_fmt_money(row.net_pnl_rupees)} |"
            )
    lines.extend([
        "",
        "## Assumptions and evidence",
        "",
        f"- Window: {days[0]} through {days[-1]}, {len(days)} eligible sessions.",
        "- Initial stop 1.25%, tightened once to 1.00% after 120 minutes; both measured from actual entry price. Targets unchanged.",
        "- Timer uses the entry candle end as a conservative fill-time proxy; activation occurs at the first eligible candle open.",
        "- Gap-through stops fill at the adverse open; intrabar stop/target ties remain stop-first.",
        "- Open-exit excursions exclude later exit-candle extrema; other candle extrema can include pre/post-fill movement.",
        "- Rs 1,00,000 allocated per filled trade, modeled 5x exposure, Rs 10,00,000 portfolio capital.",
        "- Flat 5 bps modeled round-trip cost, 10-minute entry expiry and 15:15 IST square-off.",
        "- The retained 15:15 research exit is not a broker-realistic MIS deployment cutoff; a separate earlier-cutoff replay is required before live use.",
        "- Full exits; no partial exit and no breakeven stop.",
        "- Daily-close drawdown is realized end-of-day drawdown, not intraday mark-to-market drawdown.",
        f"- Evidence: `{EVIDENCE}`. This reused history and is not an untouched holdout.",
        "- The immutable source bundle was verified against every artifact hash before replay.",
        "- No live or paper setting was changed; this output has no execution authority.",
        "",
        "## Artifacts",
        "",
        "- `selected_trades.csv`: order-level G-2 replay.",
        "- `portfolio_trades.csv`: chronological portfolio ledger.",
        "- `summary.json`: engine summary and fixed settings.",
        "- `frozen_config.json`: exact G-2 configuration.",
        "- `daily_results.csv`, `monthly_results.csv`, `side_results.csv`, `setup_results.csv`, `exit_results.csv`.",
        "- `g_vs_g2_comparison.csv` and `stop_comparison.csv`.",
        "- `trade_change_analysis.csv`: every materially changed trade and P&L delta.",
        "- `provenance.json`: input and output hashes.",
        "",
    ])
    report = output / "V13_V10_G_2_FULL_RESULTS.md"
    report.write_text("\n".join(lines), encoding="utf-8")
    return report


def save_run(
    output: Path,
    dataset: dict[str, Any],
    settings: dict[str, Any],
    trades: pd.DataFrame,
    ledger: pd.DataFrame,
    summary: dict[str, Any],
) -> Path:
    output.mkdir(parents=True, exist_ok=True)
    g.r.save(output, trades, ledger, summary)
    dump_json(output / "frozen_config.json", settings)
    breakdowns = build_breakdowns(ledger, dataset["days"])
    report = write_report(output, dataset, ledger, summary, breakdowns)
    outputs = sorted(path for path in output.iterdir() if path.is_file() and path.name != "provenance.json")
    provenance = {
        "schema_version": "V13_V10_G_2_BACKTEST_V1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "strategy": VERSION,
        "source_bundle": str(dataset["source"]),
        "source_bundle_manifest_sha256": sha256(dataset["source"] / "bundle_manifest.json"),
        "source_g_config": str(dataset["g_config_path"]),
        "source_g_config_sha256": sha256(dataset["g_config_path"]),
        "code": str(Path(__file__).resolve()),
        "code_sha256": sha256(Path(__file__).resolve()),
        "selection_identity_unchanged": True,
        "initial_stop_pct": INITIAL_STOP_PCT,
        "tightened_stop_pct": TIGHTENED_STOP_PCT,
        "tighten_after_minutes": TIGHTEN_AFTER_MINUTES,
        "targets_unchanged": True,
        "live_configuration_changed": False,
        "execution_authority": False,
        "complete_daily_extensions": dataset.get("extension_evidence", []),
        "artifacts": {
            path.name: {"bytes": path.stat().st_size, "sha256": sha256(path)}
            for path in outputs
        },
    }
    dump_json(output / "provenance.json", provenance)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-bundle", type=Path, default=DEFAULT_SOURCE_BUNDLE)
    parser.add_argument("--source-g-config", type=Path, default=DEFAULT_G_CONFIG)
    parser.add_argument("--config-json", type=Path)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--include-complete-extensions", action="store_true",
                        help="Include sealed Sep 24-30 daily replays; exclude incomplete Oct 1")
    parser.add_argument("--relaxed-0925-long", action="store_true",
                        help="Compare the requested 09:25 LONG relaxation with original G2; extension comparison also includes complete Oct 5")
    args = parser.parse_args()
    if args.relaxed_0925_long:
        if args.config_json:
            raise ValueError("Relaxed comparison uses fixed G2 exits; --config-json is not supported")
        import fno_v13_v10_g_2_0925_comparison as comparison
        comparison.run(None if args.output_dir == DEFAULT_OUTPUT else args.output_dir,
                       source_bundle=args.source_bundle, source_g_config=args.source_g_config,
                       include_extensions=args.include_complete_extensions)
        return 0
    dataset = load_bundle(args.source_bundle, args.source_g_config)
    settings = config(dataset["source_g"])
    if args.config_json:
        settings = checked_settings(read_json(args.config_json), dataset["source_g"])
    trades, ledger, summary = evaluate(dataset, settings)
    if args.include_complete_extensions:
        import fno_v13_v10_g_2_extend_results as ext

        if dataset["days"][-1] != date(2026, 9, 23):
            raise ValueError("Complete extensions require the base ending 2026-09-23")
        extension = ext._load_daily_extensions(
            dict(source_g=dataset["source_g"], base=dataset["v9_config"]),
            ext.DEFAULT_DAILY_ROOT, staged=True,
        )
        trades = pd.concat([trades, extension["g2"]], ignore_index=True, sort=False)
        ledger, combined = g.v9.v6.apply_portfolio_constraints(
            trades, dataset["v9_config"].portfolio_config()
        )
        summary.update(combined)
        summary["selection_count"] = len(trades)
        control_trades = pd.concat([
            pd.read_csv(dataset["source"] / "g_backtest/selected_trades.csv"), extension["g"]
        ], ignore_index=True, sort=False)
        dataset["control_ledger"], dataset["control_summary"] = g.v9.v6.apply_portfolio_constraints(
            control_trades, dataset["v9_config"].portfolio_config()
        )
        dataset["days"] = [*dataset["days"], *ext.COMPLETE_EXTENSION_DAYS]
        dataset["extension_evidence"] = extension["evidence"]
        if args.output_dir == DEFAULT_OUTPUT:
            args.output_dir = DEFAULT_OUTPUT.with_name("run_20261005_staged125_to100_120m_through_20260930")
    report = save_run(args.output_dir, dataset, settings, trades, ledger, summary)
    print(json.dumps({
        "metrics": g.r.metric(ledger, dataset["days"]),
        "report": str(report),
        "output_dir": str(args.output_dir),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
