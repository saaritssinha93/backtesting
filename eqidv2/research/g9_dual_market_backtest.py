"""Reproducible G7 V2 selected-pool research for the supplied OI/OFI/CVD design.

The available input cache has cash OHLC and futures OI, not paired order books
or trade prints. Full-model execution is therefore unavailable. Candle-only
experiments below never fabricate flow features. Candidate selection is the
inherited G7 selection, observable only at its one-minute confirmation.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd

from research import g7_g6_replica_backtest as g7
from research.g8_exit_engine import allocate_variant, metrics as inherited_metrics, _flags

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs/strategies/v13_v10_g_9_g7_v2_dual_market_v1.json"
OUTPUT_ROOT = g7.G7_ROOT.parent / "backtesting_result_v13_v10_g_9_dual_market"
SPEC = Path("C:/Users/Saarit/Downloads/NSE_Dual_Market_OI_OFI_CVD_Trading_Strategy.md")
VERSION = "V13-V10-G-9-G7-V2-DUAL-MARKET-V1"
IST = "Asia/Kolkata"
MINUTE = 60_000_000_000


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, default=str, allow_nan=False) + "\n", encoding="utf-8")


def metrics(book, days):
    result = inherited_metrics(book, days)
    cohort = book.loc[book.day.isin(days) & _flags(book.portfolio_executed, "portfolio_executed")]
    result["stop_exits"] += int(cohort.exit_reason.eq("STOP_GAP").sum())
    return result


def definition(path=CONFIG):
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if value["variant_id"] != VERSION or value["execution_authority"] is not False:
        raise ValueError("Unexpected research identity")
    if value["scope"] != "BACKTEST_ONLY" or value["g7_v2"] != g7.configuration(g7.V2_CONFIG):
        raise ValueError("G9 must preserve the exact resolved G7 V2 configuration")
    settings = value["candle_ablation"]
    for field in ("stop_pct", "target_pct", "risk_equity_pct", "top_k", "max_positions", "candidate_ttl_minutes"):
        if not np.isfinite(settings[field]) or settings[field] <= 0:
            raise ValueError("Invalid ablation setting: " + field)
    if settings["fill_delay_minutes"] < 1 or int(settings["fill_delay_minutes"]) != settings["fill_delay_minutes"]:
        raise ValueError("Candle fills require at least one complete minute of delay")
    return value


def candidate_audit(orders, nifty_audit, settings):
    """No future execution fields participate in this gate or its rank."""
    out = orders[["sid", "day", "setup_id", "tradingsymbol", "side", "signal_ts",
                  "confirmation_ts", "price_change_pct", "oi_change_pct", "oi", "prev_oi"]].copy()
    numeric = ["price_change_pct", "oi_change_pct", "oi", "prev_oi"]
    for name in numeric:
        out[name] = pd.to_numeric(out[name], errors="coerce")
    finite = np.isfinite(out[numeric]).all(axis=1)
    signal = pd.to_datetime(out.signal_ts, utc=True).dt.tz_convert(IST)
    known = pd.to_datetime(out.confirmation_ts, utc=True).dt.tz_convert(IST)
    if signal.isna().any() or known.isna().any() or (known < signal).any():
        raise ValueError("Invalid candidate observation time")
    out["candidate_available_at"] = known.astype(str)
    out["candidate_expires_at"] = (signal + pd.Timedelta(minutes=settings["candidate_ttl_minutes"])).astype(str)
    # Expiry is measured from the original five-minute signal, not reset after
    # G7's confirmation. That leaves only a narrow subsequent trigger window.
    out["long_in_window"] = out.side.eq("LONG") & signal.dt.strftime("%H:%M").between(
        settings["earliest_selection"], settings["new_entries_end"])
    direction = nifty_audit.set_index("sid").both_counter_allowed
    out["g7_direction_allowed"] = out.sid.map(_flags(direction, "both_counter_allowed"))
    if out.g7_direction_allowed.isna().any():
        raise ValueError("Missing G7 direction evidence")
    out["oi_spot_positive"] = finite & (
        pd.to_numeric(out.price_change_pct, errors="coerce").gt(0)
        & pd.to_numeric(out.oi_change_pct, errors="coerce").gt(0)
        & pd.to_numeric(out.oi, errors="coerce").gt(pd.to_numeric(out.prev_oi, errors="coerce"))
        & pd.to_numeric(out.prev_oi, errors="coerce").gt(0))
    out["eligible"] = out.long_in_window & out.g7_direction_allowed & out.oi_spot_positive
    out["oi_rank"] = np.nan
    eligible = out.loc[out.eligible].sort_values(
        ["candidate_available_at", "oi_change_pct", "tradingsymbol", "sid"],
        ascending=[True, False, True, True], kind="stable")
    ranks = eligible.groupby("candidate_available_at").cumcount() + 1
    out.loc[eligible.index, "oi_rank"] = ranks
    out["top_k_allowed"] = out.eligible & out.oi_rank.le(settings["top_k"])
    out["rejection_reason"] = np.select(
        [~out.long_in_window, ~out.g7_direction_allowed, ~out.oi_spot_positive,
         ~out.top_k_allowed],
        ["SHORT_OR_OUTSIDE_WINDOW", "G7_NIFTY_GATE", "OI_OR_SPOT_NOT_POSITIVE", "OUTSIDE_TOP_K"],
        default="ELIGIBLE_CANDLE_ABLATION")
    out["full_model_status"] = "UNAVAILABLE_MISSING_PAIRED_BOOKS_AND_TRADE_PRINTS"
    return out


def add_futures_gate(audit, context, settings):
    """Add exact same-contract futures candle context before top-K ranking."""
    out = audit.merge(context.reset_index(drop=True), on="sid", how="left", validate="one_to_one")
    if len(out) != len(audit) or out.futures_context_available.isna().any():
        raise ValueError("Incomplete futures context audit")
    returns = pd.to_numeric(out.futures_ret_5m, errors="coerce")
    out["futures_price_positive"] = (_flags(out.futures_context_available, "futures_context_available")
                                     & np.isfinite(returns) & returns.gt(0))
    out["eligible_dual_price"] = out.eligible & out.futures_price_positive
    out["oi_rank"] = np.nan
    eligible = out.loc[out.eligible_dual_price].sort_values(
        ["candidate_available_at", "oi_change_pct", "tradingsymbol", "sid"],
        ascending=[True, False, True, True], kind="stable")
    out.loc[eligible.index, "oi_rank"] = eligible.groupby("candidate_available_at").cumcount()+1
    out["top_k_allowed"] = out.eligible_dual_price & out.oi_rank.le(settings["top_k"])
    out.loc[out.eligible & ~out.futures_price_positive, "rejection_reason"] = "FUTURES_CANDLE_CONTEXT_NOT_POSITIVE_OR_UNAVAILABLE"
    out.loc[out.eligible_dual_price & ~out.top_k_allowed, "rejection_reason"] = "OUTSIDE_TOP_K"
    out.loc[out.top_k_allowed, "rejection_reason"] = "ELIGIBLE_DUAL_PRICE_ABLATION"
    return out


def strict_model_audit(orders, context, full_config):
    from research.g9_dual_market_features import GateConfig, candidate_rejection_reasons

    config = GateConfig(max_oi_age_seconds=full_config["max_oi_age_seconds"],
                        max_quote_age_seconds=full_config["max_quote_age_seconds"],
                        max_spread_bps=full_config["max_spread_bps"],
                        min_trade_side_coverage=full_config["min_classification_coverage"])
    futures = context.set_index("sid", drop=False)
    records = []
    for order in orders.to_dict("records"):
        end = pd.Timestamp(order["signal_ts"]).to_pydatetime()
        row = dict(symbol=order["tradingsymbol"], future_contract=order["futures_tradingsymbol"],
                   oi_start_contract=order["futures_tradingsymbol"], oi_end_contract=order["futures_tradingsymbol"],
                   window_start=(pd.Timestamp(end)-pd.Timedelta(minutes=5)).to_pydatetime(),
                   window_end=end, available_at=pd.Timestamp(order["confirmation_ts"]).to_pydatetime(),
                   futures_return_5m=float(futures.loc[order["sid"], "futures_ret_5m"]),
                   spot_return_5m=float(order["price_change_pct"])/100,
                   oi_delta_5m=float(order["oi"])-float(order["prev_oi"]),
                   oi_delta_percent=float(order["oi_change_pct"]), oi_age_seconds=None,
                   futures_flow=None, spot_flow=None)
        reasons = candidate_rejection_reasons(row, config)
        if not reasons:
            raise ValueError("Full model improperly accepted a candle-only candidate")
        records.append(dict(sid=order["sid"], day=order["day"], symbol=order["tradingsymbol"],
                            full_model_allowed=False, rejection_reasons="|".join(reasons)))
    return pd.DataFrame(records)


def validate_path(path):
    t = np.asarray(path["timestamp_ns"], dtype="int64")
    if not len(t) or (np.diff(t) != MINUTE).any():
        raise ValueError("Execution path must have consecutive one-minute candle ends")
    values = {key: np.asarray(path[key], dtype=float) for key in ("open", "high", "low", "close")}
    if any(len(v) != len(t) or not np.isfinite(v).all() or (v <= 0).any() for v in values.values()):
        raise ValueError("Invalid execution OHLC")
    if ((values["high"] < np.maximum(values["open"], values["close"]))
            | (values["low"] > np.minimum(values["open"], values["close"]))
            | (values["low"] > values["high"])).any():
        raise ValueError("Inconsistent execution OHLC")
    return t, values


def e1_trade(order, path, settings):
    """Completed close breakout, delayed open fill, stop-first OHLC exits.

    Cached bars are END labelled. For a trigger ending T and delay=1, use the
    OPEN of the candle ending T+2 (open time T+1). Both trigger and fill must
    respect TTL from the five-minute signal. Exit timestamps conservatively
    use the bar end, since the intrabar crossing time is unknown.
    """
    t, ohlc = validate_path(path)
    confirmation = pd.Timestamp(order["confirmation_ts"])
    signal = pd.Timestamp(order["signal_ts"])
    expires = signal + pd.Timedelta(minutes=settings["candidate_ttl_minutes"])
    cutoff = pd.Timestamp(str(order["day"])[:10] + " " + settings["force_flat"], tz=IST)
    entry_cutoff = pd.Timestamp(str(order["day"])[:10] + " " + settings["new_entries_end"], tz=IST)
    if t[0] != confirmation.value + MINUTE or t[-1] != cutoff.value:
        raise ValueError("Cached path does not cover confirmation through exact forced exit")
    if not np.isfinite(float(order["confirmation_high"])) or float(order["confirmation_high"]) <= 0:
        raise ValueError("Missing previous completed one-minute high")
    delay = int(settings["fill_delay_minutes"])
    result = dict(filled=False, entry_ts=pd.NaT, exit_ts=pd.NaT, entry_price=np.nan,
                  exit_price=np.nan, entry_path_index=np.nan, exit_path_index=np.nan,
                  gross_return_pct=np.nan, trigger_ts=pd.NaT,
                  exit_reason="NO_E1_TRIGGER_BEFORE_EXPIRY", initial_stop_pct=settings["stop_pct"],
                  stop_pct=settings["stop_pct"], target_pct=settings["target_pct"])
    for i in range(len(t)):
        if t[i] > expires.value:
            break
        previous_high = float(order["confirmation_high"]) if i == 0 else ohlc["high"][i-1]
        if ohlc["close"][i] <= previous_high:
            continue
        fill_index = i + delay + 1
        if fill_index >= len(t):
            break
        entry_ns = t[fill_index] - MINUTE
        if entry_ns > min(expires.value, entry_cutoff.value) or entry_ns >= cutoff.value:
            result["exit_reason"] = "TRIGGER_BUT_FILL_EXPIRED"
            break
        entry = float(ohlc["open"][fill_index])
        stop, target = entry * (1-settings["stop_pct"]/100), entry * (1+settings["target_pct"]/100)
        exit_index, exit_price, reason = len(t)-1, float(ohlc["close"][-1]), "FORCE_FLAT_1515"
        for j in range(fill_index, len(t)):
            op, hi, lo = (float(ohlc[key][j]) for key in ("open", "high", "low"))
            if op <= stop:
                exit_index, exit_price, reason = j, op, "STOP_GAP"
                break
            if op >= target:
                # Do not grant favorable opening-price improvement to targets.
                exit_index, exit_price, reason = j, target, "TARGET"
                break
            if lo <= stop:  # Includes bars touching both thresholds.
                exit_index, exit_price, reason = j, stop, "STOP"
                break
            if hi >= target:
                exit_index, exit_price, reason = j, target, "TARGET"
                break
        result.update(filled=True, entry_ts=pd.Timestamp(entry_ns, tz="UTC").tz_convert(IST),
                      exit_ts=pd.Timestamp(t[exit_index], tz="UTC").tz_convert(IST),
                      trigger_ts=pd.Timestamp(t[i], tz="UTC").tz_convert(IST),
                      entry_price=entry, exit_price=exit_price, entry_path_index=fill_index,
                      exit_path_index=exit_index, gross_return_pct=100*(exit_price/entry-1), exit_reason=reason)
        return result
    return result


def replay_e1(orders, paths, audit, settings):
    gate = audit.set_index("sid").top_k_allowed
    records = []
    for order in orders.to_dict("records"):
        row = {k: order[k] for k in ("sid", "day", "setup_id", "tradingsymbol", "side", "signal_ts", "confirmation_ts")}
        if gate.loc[order["sid"]]:
            row.update(e1_trade(order, paths[int(order["sid"])], settings))
        else:
            row.update(filled=False, entry_ts=pd.NaT, exit_ts=pd.NaT, entry_price=np.nan,
                       exit_price=np.nan, entry_path_index=np.nan, gross_return_pct=np.nan,
                       exit_reason="CANDIDATE_REJECTED")
        row["g8_baseline_filled"] = row["filled"]
        row["g8_quantity_scale"] = 1.
        records.append(row)
    return pd.DataFrame(records)


def integer_risk_book(native, base_config, settings, cost_bps, policy):
    """Fixed initial-equity risk, whole shares, sequential capacity accounting.

    0.25% is stop-loss risk plus modeled round-trip costs, without compounding.
    No spread/depth/partial-fill claims are possible from these candles.
    """
    book = native.copy(deep=True)
    book["portfolio_executed"] = False
    book["quantity"] = 0
    book["portfolio_reject_reason"] = "NO_ENTRY"
    for field in ("portfolio_net_profit_rupees", "portfolio_gross_profit_rupees", "portfolio_cost_rupees",
                  "portfolio_capital_rupees", "portfolio_exposure_rupees", "risk_budget_rupees"):
        book[field] = 0.
    capital, leverage = float(base_config["portfolio_capital_rs"]), float(base_config["leverage"])
    risk_budget = capital * settings["risk_equity_pct"] / 100
    fills = book.loc[book.filled].copy()
    fills["sort_time"] = pd.to_datetime(fills.entry_ts, utc=True)
    fills = fills.sort_values(["sort_time", "sid"], kind="stable")
    positions = []
    for index, row in fills.iterrows():
        entry_time = pd.Timestamp(row.entry_ts)
        positions = [p for p in positions if p["exit"] >= entry_time]
        # Conservative same-boundary ordering: exits at T release capacity only
        # after entries at T, because actual intrabar execution order is unknown.
        reason = None
        if len(positions) >= settings["max_positions"]:
            reason = "MAX_POSITIONS"
        elif any(p["symbol"] == row.tradingsymbol for p in positions):
            reason = "SYMBOL_ALREADY_OPEN"
        remaining = capital - sum(p["capital"] for p in positions)
        risk_per_share = float(row.entry_price) * (settings["stop_pct"]/100 + cost_bps/10000)
        qty = int(np.floor(min(risk_budget/risk_per_share, max(0., remaining)*leverage/float(row.entry_price))))
        if qty <= 0:
            reason = reason or "INSUFFICIENT_CAPITAL"
        if reason:
            book.at[index, "portfolio_reject_reason"] = reason
            continue
        exposure = qty*float(row.entry_price)
        used = exposure/leverage
        gross = qty*(float(row.exit_price)-float(row.entry_price))
        cost = exposure*cost_bps/10000
        for key, val in dict(portfolio_executed=True, quantity=qty, portfolio_reject_reason="",
                             portfolio_net_profit_rupees=gross-cost, portfolio_gross_profit_rupees=gross,
                             portfolio_cost_rupees=cost, portfolio_capital_rupees=used,
                             portfolio_exposure_rupees=exposure, risk_budget_rupees=risk_budget).items():
            book.at[index, key] = val
        positions.append(dict(exit=pd.Timestamp(row.exit_ts), symbol=row.tradingsymbol, capital=used))
    book["research_policy"] = policy
    book["scope"], book["execution_authority"] = "BACKTEST_ONLY", False
    book["strategy_version"] = VERSION
    return book


def protection_snapshot():
    paths = [ROOT / "configs/strategies/active_profile.json", ROOT / "fno_strategy_registry.py",
             ROOT / "fno_v13_v10_g_9_backtest.py", ROOT / "research/g9_backtest.py",
             ROOT / "configs/strategies/v13_v10_g_9_g8_risk_permission_v1.json"]
    for name in ("backtesting_result_v13_v10_g_7", "backtesting_result_v13_v10_g_8", "backtesting_result_v13_v10_g_9"):
        directory = OUTPUT_ROOT.parent / name
        paths.extend(directory.glob("latest*.json"))
        paths.extend((directory / "latest").glob("*.json"))
    return {str(p): sha(p) for p in paths if p.is_file()}


def run(output, *, config_path=CONFIG, spec_path=SPEC):
    from research.g9_dual_market_sources import load_sources, futures_context

    config = definition(config_path)
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(OUTPUT_ROOT.resolve()):
        raise ValueError("Choose a NEW directory inside the isolated G9 dual-market result root")
    if not Path(spec_path).is_file() or sha(spec_path) != config["strategy_document_sha256"]:
        raise ValueError("Attached strategy file differs from the configured research specification")
    protected = protection_snapshot()
    source = load_sources()
    if source["config"] != config["g7_v2"]:
        raise ValueError("Cached inputs do not use the copied G7 V2 configuration")
    base, settings, days = source["config"], config["candle_ablation"], source["days"]
    print(f"Verified G7 V2: {len(source['orders'])} selected candidates / {len(days)} sessions", flush=True)
    audit = candidate_audit(source["orders"], source["audit"], settings)
    context, context_provenance = futures_context(source["orders"])
    audit = add_futures_gate(audit, context, settings)
    strict_audit = strict_model_audit(source["orders"], context.reset_index(drop=True), config["full_model"])
    decisions = audit.set_index("sid")
    books = {"g7_v2_baseline": source["baseline"].copy()}
    for name, allowed in {
        "g7_v2_long_window_control": decisions.long_in_window & decisions.g7_direction_allowed,
        "g9_oi_spot_g7_execution": decisions.eligible,
        "g9_oi_dual_price_g7_execution": decisions.eligible_dual_price,
    }.items():
        books[name] = allocate_variant(source["unfiltered"], allowed, base, name, cost_bps=base["model_cost_bps_round_trip"])
    native = replay_e1(source["orders"], source["paths"], audit, settings)
    execution_config = {**base, "variant_id": VERSION, "max_positions": settings["max_positions"], "max_positions_per_symbol": 1}
    books["g9_e1_fixed_exposure_5bps"] = allocate_variant(native, decisions.top_k_allowed, execution_config,
                                                           "g9_e1_fixed_exposure_5bps", cost_bps=5.)
    for cost in config["cost_scenarios_bps"]:
        name = f"g9_e1_risk_sized_{cost:g}bps"
        books[name] = integer_risk_book(native, base, settings, cost, name)
    for name, book in books.items():
        book["research_policy"] = name
        book["scope"], book["execution_authority"] = "BACKTEST_ONLY", False
        if name != "g7_v2_baseline":
            book["strategy"], book["strategy_version"] = "V13-V10-G-9-DUAL-MARKET", VERSION
    periods = {"all": days, "development_first35": days[:35], "later_sessions": days[35:],
               "october": [d for d in days if d.startswith("2026-10")], "latest_session": days[-1:]}
    overall = pd.DataFrame([dict(policy=name, period=label, **metrics(book, subset))
                            for name, book in books.items() for label, subset in periods.items()])
    daily = pd.DataFrame([dict(policy=name, day=day, **metrics(book, [day]))
                          for name, book in books.items() for day in days])
    monthly = pd.DataFrame([dict(policy=name, month=month, **metrics(book, [d for d in days if d.startswith(month)]))
                            for name, book in books.items() for month in sorted({d[:7] for d in days})])
    folds = pd.DataFrame([dict(policy=name, fold=i+1, first_date=cohort[0], last_date=cohort[-1], **metrics(book, cohort))
                          for name, book in books.items()
                          for i, cohort in enumerate([days[j:j+7] for j in range(35, len(days), 7)])])
    wide = daily.pivot(index="day", columns="policy", values="net_pnl_rs").reset_index()
    wide["g9_oi_spot_change_from_long_control_rs"] = wide.g9_oi_spot_g7_execution-wide.g7_v2_long_window_control
    limitations = [
        "Full dual-market model is UNAVAILABLE; zero executed trades is a missing-data result, not strategy performance.",
        "No genuine paired OFI/CVD, per-event sequence or receive timestamps, verified OI freshness, spread or executable depth in these inputs.",
        "OI/spot control uses futures OI and cash price. Dual-price and E1 ablations additionally require exact same-contract positive futures candle return and OI parity; real-time freshness remains unverified.",
        "G7 preselected opportunities only, including its confirmation, filters and Nifty gate. This is not a fresh all-stock five-minute scanner.",
        "G7 selection becomes observable at confirmation (signal+1m). First E1 close available is signal+2m; a full-minute delay fills at signal+3m if within TTL.",
        "Prior G7 setup/target selection used inspected history; inherited after-close stock-day eligibility exclusions remain. Chronological slices are not untouched out-of-sample proof.",
        "57 available sessions do not constitute every exchange session in the date interval; absent sessions are not filled with zero returns.",
        "G7/fixed-exposure controls retain theoretical fractional shares, Rs100,000 margin per entry and 5x exposure. Risk ablation uses integer cash shares and fixed initial-equity risk.",
        "5/15/30bps are modeled round-trip cost scenarios, not verified date-specific brokerage/tax schedules; spread, impact, latency and partial fills are not event replayed.",
        "Each risk-sized cost scenario recomputes quantity to keep stop risk plus modeled costs within 0.25% of starting equity; these scenarios change both costs and exposure.",
        "Drawdown is measured at daily close; intraday mark-to-market drawdown may be larger. No live or paper execution authority.",
        "Full model feature primitives are tested separately; feed integration, event-driven fills, training calibration and subscription simulation remain unavailable.",
    ]
    summary = dict(schema_version="g9_dual_market_research_v1", variant_id=VERSION, scope="BACKTEST_ONLY",
                   execution_authority=False, complete=True, first_date=days[0], last_date=days[-1], days=days,
                   sessions=len(days), active_setups=51, selected_candidates=len(audit),
                   eligible_candle_candidates=int(audit.top_k_allowed.sum()), e1_native_fills=int(native.filled.sum()),
                   full_model_status="UNAVAILABLE_MISSING_EVENT_DATA", full_model_performance_evaluable=False,
                   full_model_result=dict(examined_candidates=len(strict_audit), admitted_candidates=0,
                                          executed_trades=0, net_pnl_rs=None, profit_factor=None, net_expectancy_rs=None),
                   reproduction_command="python fno_v13_v10_g_9_dual_market_backtest.py",
                   baseline_allocation_verified=True, chronological_slices_are_untouched_holdout=False,
                   comparison=overall.loc[overall.period.eq("all")].to_dict("records"), limitations=limitations,
                   source_provenance=source["provenance"], strategy_document_sha256=sha(spec_path))
    output.mkdir(parents=True)
    dump(output / "config.json", config)
    dump(output / "source_provenance.json", source["provenance"])
    dump(output / "data_capability_audit.json", source["capability_audit"].to_dict("records"))
    source["capability_audit"].to_csv(output / "data_capability_audit.csv", index=False)
    dump(output / "futures_context_provenance.json", context_provenance)
    shutil.copyfile(spec_path, output / "NSE_Dual_Market_OI_OFI_CVD_Trading_Strategy.md")
    for name, frame in {"candidate_audit": audit, "overall_comparison": overall, "daywise_all_policies": daily,
                        "daywise_comparison": wide, "monthly_comparison": monthly, "chronological_slices": folds,
                        "e1_native_candidates": native, "full_model_rejections": strict_audit,
                        "futures_context": context.reset_index(drop=True)}.items():
        frame.to_csv(output / (name + ".csv"), index=False)
    for name, book in books.items():
        (output / name).mkdir()
        book.to_csv(output / name / "portfolio_trades.csv", index=False)
    for name, evidence in context_provenance["source_files"].items():
        if evidence.get("present") and sha(name) != evidence["sha256"]:
            raise ValueError("Futures candle source changed during the run: " + name)
    if protection_snapshot() != protected:
        raise ValueError("A protected configuration or previous experiment pointer changed during this run")
    summary["protected_files_sha256_unchanged"] = protected
    dump(output / "summary.json", summary)
    columns = ["policy", "trades", "win_rate_pct", "net_pnl_rs", "profit_factor", "max_drawdown_rs", "average_net_trade_rs"]
    body = ("<!doctype html><html><head><meta charset='utf-8'><title>G9 dual-market research</title>"
            "<style>body{font:15px/1.55 system-ui;margin:32px;max-width:1500px;color:#192736;background:#f5f7fa}"
            "table{border-collapse:collapse;background:white}td,th{padding:9px;border:1px solid #cdd6e0;text-align:right}"
            "td:first-child,th:first-child{text-align:left}.notice{padding:16px;background:#fff1d0}</style></head><body>"
            "<h1>G9: G7 V2 clone and dual-market feasibility</h1>"
            f"<p>{days[0]} to {days[-1]} · {len(days)} available sessions · {len(audit)} selected G7 opportunities</p>"
            "<p class='notice'><b>Full OFI/CVD strategy unavailable:</b> no paired order-book and trade-print history. "
            "The nonzero results below are explicitly candle-only experiments.</p>"
            "<p><a href='daywise_comparison.csv'>Daywise comparison</a> · <a href='overall_comparison.csv'>All metrics</a> · "
            "<a href='candidate_audit.csv'>Candidate decisions</a> · <a href='summary.json'>Reproducibility and limitations</a></p>"
            "<h2>Full period</h2>" + overall.loc[overall.period.eq("all"), columns].to_html(index=False, float_format=lambda x: f"{x:,.2f}")
            + "<h2>Later chronological sessions (previously inspected history)</h2>"
            + overall.loc[overall.period.eq("later_sessions"), columns].to_html(index=False, float_format=lambda x: f"{x:,.2f}")
            + "<h2>Interpretation</h2><p>The long-window control isolates direction and session restrictions. "
            "The OI/spot subset retains G7 entries/exits. E1 variants change timing, exits and position limits; "
            "risk-sized variants also change exposure. Their differences are not evidence of OFI/CVD value.</p>"
            "<p>Reproduce: <code>python fno_v13_v10_g_9_dual_market_backtest.py</code>. "
            "Configuration: <a href='config.json'>copied G7 V2 and research overlay</a>.</p>"
            + "<ul>" + "".join("<li>"+html.escape(x)+"</li>" for x in limitations) + "</ul></body></html>")
    (output / "report.html").write_text(body, encoding="utf-8")
    code_files = [Path(__file__), Path(config_path), ROOT / "fno_v13_v10_g_9_dual_market_backtest.py",
                  ROOT / "research/g9_dual_market_sources.py", ROOT / "research/g9_dual_market_features.py",
                  ROOT / "research/g8_sources.py", ROOT / "research/g8_exit_engine.py"]
    dump(output / "manifest.json", dict(schema_version=summary["schema_version"],
         code={str(p): sha(p) for p in code_files},
         files={p.relative_to(output).as_posix(): sha(p) for p in output.rglob("*") if p.is_file()}))
    verify(output)
    print(overall.loc[overall.period.eq("all"), columns].to_string(index=False), flush=True)
    print("VERIFIED " + str(output), flush=True)
    return summary


def verify(output):
    output = Path(output).resolve()
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    for name, expected in manifest["files"].items():
        path = (output / name).resolve()
        if not path.is_relative_to(output) or sha(path) != expected:
            raise ValueError("Result artifact changed: " + name)
    for name, expected in manifest["code"].items():
        if sha(name) != expected:
            raise ValueError("Research implementation changed: " + name)
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    rows = pd.read_csv(output / "overall_comparison.csv")
    daily = pd.read_csv(output / "daywise_all_policies.csv")
    for row in rows.loc[rows.period.eq("all")].itertuples():
        book = pd.read_csv(output / row.policy / "portfolio_trades.csv", low_memory=False)
        actual = metrics(book, summary["days"])
        for key in ("trades", "wins", "losses", "net_pnl_rs", "cost_rs", "max_drawdown_rs"):
            if not np.isclose(actual[key], getattr(row, key), atol=1e-6, rtol=0):
                raise ValueError("Accounting mismatch: " + row.policy + " " + key)
        if not np.isclose(daily.loc[daily.policy.eq(row.policy), "net_pnl_rs"].sum(), actual["net_pnl_rs"], atol=1e-6, rtol=0):
            raise ValueError("Daily P&L reconciliation failed")
        if row.policy.startswith("g9_e1"):
            fills = book.loc[_flags(book.portfolio_executed, "portfolio_executed")]
            if len(fills):
                entry, trigger = (pd.to_datetime(fills[x], utc=True) for x in ("entry_ts", "trigger_ts"))
                configuration = json.loads((output / "config.json").read_text(encoding="utf-8"))
                expiry = pd.to_datetime(fills.signal_ts, utc=True)+pd.Timedelta(minutes=configuration["candle_ablation"]["candidate_ttl_minutes"])
                if ((entry <= trigger) | (entry > expiry)).any():
                    raise ValueError("E1 temporal contract broken")
    if summary["full_model_performance_evaluable"]:
        raise ValueError("Candle history cannot evaluate the full model")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--strategy-document", type=Path, default=SPEC)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--verify", type=Path, help="Verify an existing run and its accounting")
    args = parser.parse_args()
    if args.verify:
        verify(args.verify)
        print("VERIFIED", args.verify)
        return
    now = pd.Timestamp.now(tz=IST)
    output = args.output_dir or OUTPUT_ROOT / "runs" / str(now.date()) / (now.strftime("%Y%m%dT%H%M%S%f")+"_g7v2_oi_ofi_cvd")
    run(output, config_path=args.config, spec_path=args.strategy_document)


if __name__ == "__main__":
    main()
