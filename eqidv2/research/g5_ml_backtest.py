"""Frozen G4 replica, past-session ML decisions, and complete portfolio replay.

Run ``python fno_v13_v10_g_5_backtest.py``. The default configuration is saved
before any candidate evaluation. No production registry or pointer is written.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import html
import json
from pathlib import Path
import shutil

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from eqidv2_runtime_paths import runtime_dir
from research import g4_bounded_exit_backtest as bounded
from research import g4_delayed_entry_research as prior
from research import g4_failed_breakout_research as sealed
from research import g4_five_minute_reversal_action as reversal
from research import g4_g3_mirror_backtest as base
from research import g4_iterative_dataset as dataset
from research import g4_percent_trail_exit as trailing
from research.g3_sl_target_simulator import simulate_exit

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "configs/research/g5_ml_walkforward_v1.json"
G5_ROOT = runtime_dir("backtesting_result_v13_v10_g_5")
FEATURES = (
    "atr_pct", "ema_stretch_atr", "vwap_stretch_atr", "momentum_3_atr",
    "ema_slope_3_atr", "log_relvol20", "log_relvol_time", "body_ratio",
    "rejection_wick", "side_long", "time_fraction", "target_r",
)
VARIANTS = (
    "control", "uniform_half", "ml_size", "ml_skip", "adaptive_exit",
    "combined_size", "combined_skip", "fixed_protective", "fixed_percentage_trail",
)
ENGINE = sealed.bounded.history.simulator.g.v9
MONEY_COLUMNS = (
    "capital_per_entry_rupees", "exposure_per_entry_rupees",
    "unleveraged_pre_cost_profit_rupees", "unleveraged_cost_rupees",
    "unleveraged_net_profit_rupees", "pre_cost_profit_rupees", "cost_rupees",
    "net_profit_rupees",
)


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def build_entry_features(orders, contexts):
    """Read an explicit allowlist at confirmation close; outcomes never enter X."""
    records = []
    if orders.sid.duplicated().any():
        raise ValueError("Duplicate order SID")
    for row in orders.to_dict("records"):
        sid, stamp = int(row["sid"]), pd.Timestamp(row["confirmation_ts"])
        if stamp.tzinfo is None or row["side"] not in ("LONG", "SHORT"):
            raise ValueError("Timezone-aware confirmation and valid side required")
        stamp = stamp.tz_convert("Asia/Kolkata")
        context = contexts[sid]
        observed = context.loc[context.ts.le(stamp)]
        if observed.empty or pd.Timestamp(observed.iloc[-1].ts) != stamp:
            raise ValueError("Exact confirmation context required")
        bar = observed.iloc[-1]
        if any(not np.isclose(float(bar[name]), float(row["confirmation_" + name]),
                              rtol=0, atol=.001) for name in ("open", "high", "low", "close")):
            raise ValueError("Confirmation differs from frozen order")
        sign = 1 if row["side"] == "LONG" else -1
        atr = float(bar.atr14)
        close = float(bar.close)
        valid_denominator = np.isfinite(atr) and atr > 0 and close > 0
        atr_denominator = atr if valid_denominator else np.nan
        with np.errstate(invalid="ignore", divide="ignore"):
            values = dict(
                atr_pct=atr / close * 100,
                ema_stretch_atr=sign * (close - float(bar.ema20)) / atr_denominator,
                vwap_stretch_atr=sign * (close - float(bar.session_vwap)) / atr_denominator,
                momentum_3_atr=sign * float(bar.roc3_atr),
                ema_slope_3_atr=sign * float(bar.ema_slope3_atr),
                log_relvol20=float(np.log1p(float(bar.relvol20))),
                log_relvol_time=float(np.log1p(float(bar.relvol_time))),
                body_ratio=float(bar.body_ratio),
                rejection_wick=float(bar.upper_wick_ratio if sign == 1 else bar.lower_wick_ratio),
                side_long=float(sign == 1),
                time_fraction=(stamp.hour * 60 + stamp.minute - 555) / 360,
                target_r=float(row["native_target_pct"]) / 1.2,
            )
        flag = bar.get("gap_filled", False)
        flagged = str(flag).lower() in ("true", "1", "1.0", "yes")
        valid = valid_denominator and not flagged and np.isfinite(list(values.values())).all()
        records.append(dict(sid=sid, day=stamp.date().isoformat(),
            decision_ts=stamp.isoformat(), feature_ts=pd.Timestamp(bar.ts).isoformat(),
            feature_valid=bool(valid), feature_reason="OK" if valid else "MISSING_OR_FLAGGED_INPUT",
            **values))
    return pd.DataFrame(records).set_index("sid", drop=False)


def folds(days, initial=30, block=5):
    if list(days) != sorted(set(days)) or initial < 20 or block < 1:
        raise ValueError("Unique chronological sessions and a valid split are required")
    for number, start in enumerate(range(initial, len(days), block)):
        yield number, list(days[:start]), list(days[start:start + block])


def fit_model(features, labels, train_days, config):
    """Inner validation and every preprocessing fit use prior sessions only."""
    y = labels.reindex(features.index)
    usable = features.day.isin(train_days) & features.feature_valid & y.notna()
    train = features.loc[usable]
    target = y.loc[usable].astype(float)
    if (len(train) < config["minimum_training_fills"] or
            target.lt(0).sum() < config["minimum_training_losses"]):
        return None, dict(reason="INSUFFICIENT_PRIOR_FILLS", training_fills=len(train),
                          alpha=None, inner_scores=[])
    scores = []
    block = config["test_block_sessions"]
    first_validation = len(train_days) - config["inner_validation_blocks"] * block
    for alpha in config["ridge_alphas"]:
        errors = []
        for start in range(first_validation, len(train_days), block):
            fit = train.day.isin(train_days[:start])
            validation = train.day.isin(train_days[start:start + block])
            if fit.sum() < 40 or not validation.any():
                continue
            model = make_pipeline(StandardScaler(), Ridge(alpha=alpha))
            model.fit(train.loc[fit, list(FEATURES)], target.loc[fit])
            prediction = model.predict(train.loc[validation, list(FEATURES)])
            errors.extend(np.square(prediction - target.loc[validation].to_numpy()).tolist())
        scores.append(dict(alpha=float(alpha), mse=float(np.mean(errors)) if errors else None,
                           validation_fills=len(errors)))
    eligible = [item for item in scores if item["mse"] is not None]
    if not eligible:
        return None, dict(reason="INSUFFICIENT_INNER_VALIDATION", training_fills=len(train),
                          alpha=None, inner_scores=scores)
    best = min(eligible, key=lambda item: (item["mse"], -item["alpha"]))
    model = make_pipeline(StandardScaler(), Ridge(alpha=best["alpha"]))
    model.fit(train[list(FEATURES)], target)
    return model, dict(reason="FITTED_PRIOR_SESSIONS", training_fills=len(train),
                       alpha=best["alpha"], inner_scores=scores)


def serialize_model(model, audit, train_days):
    value = dict(features=list(FEATURES), training_start=train_days[0],
                 training_end=train_days[-1], audit=audit)
    if model is None:
        value["enabled"] = False
    else:
        scale, ridge = model.steps[0][1], model.steps[1][1]
        value.update(enabled=True, mean=scale.mean_.tolist(), scale=scale.scale_.tolist(),
                     coefficients=ridge.coef_.tolist(), intercept=float(ridge.intercept_))
    return value


def score_saved_model(features, model):
    """Portable numeric model: no pickle or label columns needed for scoring."""
    result = pd.Series(np.nan, index=features.index, dtype=float)
    if model["features"] != list(FEATURES):
        raise ValueError("Model feature schema differs")
    if not model["enabled"]:
        return result
    valid = features.feature_valid & features[list(FEATURES)].notna().all(axis=1)
    x = features.loc[valid, list(FEATURES)].to_numpy(float)
    result.loc[valid] = ((x - np.asarray(model["mean"])) / np.asarray(model["scale"])) @ np.asarray(
        model["coefficients"]) + model["intercept"]
    return result


def native_catalogue(original, paths, config, delay=0):
    """Simulate a fixed exit catalogue; future outcomes are labels, never inputs."""
    records = {name: [] for name in config["exit_catalogue"]}
    for source in original.to_dict("records"):
        path = paths[int(source["sid"])]
        found = prior.delayed_entry(source, path, delay_bars=delay)
        for policy in records:
            row = source.copy()
            row.update(filled=found is not None, position_open=False)
            if found is None:
                row.update(entry_ts=pd.NaT, exit_ts=pd.NaT, entry_price=np.nan,
                           exit_price=np.nan, gross_return_pct=np.nan,
                           net_return_pct=np.nan, cost_pct=np.nan, exit_reason="UNFILLED")
            else:
                index, entry, trigger, gap, overshoot = found
                row.update(entry_path_index=index, entry_price=entry, trigger_used=trigger,
                    entry_gap_through=gap, entry_overshoot_bps=overshoot,
                    entry_ts=pd.Timestamp(int(path["timestamp_ns"][index]), tz="UTC").tz_convert(base.production.IST))
                if policy == "baseline":
                    outcome = simulate_exit(path, index, entry, row["side"] == "LONG", 1.2,
                        float(row["native_target_pct"]), tighten_after_minutes=120, tightened_stop_pct=1.)
                elif policy == "percentage_trail":
                    outcome = trailing.simulate_percentage_trailing_exit(path, index, entry,
                        row["side"] == "LONG", float(row["native_target_pct"]), config["percentage_trail"])
                elif policy == "protective":
                    # Reuse the already-tested native immediate-entry reversal semantics.
                    # Delayed entries need their own fill-aware signal loop below.
                    outcome = protective_outcome(row, path, index, entry)
                else:
                    raise ValueError("Unregistered exit policy")
                row.update(outcome)
                row.update(cost_pct=.05, net_return_pct=outcome["gross_return_pct"] - .05)
            row["g5_exit_policy"] = policy
            records[policy].append(row)
    return {name: pd.DataFrame(rows) for name, rows in records.items()}


def protective_outcome(row, path, index, entry):
    """Two 0.30% adverse closes in first five confirmation minutes; next-open exit."""
    native = simulate_exit(path, index, entry, row["side"] == "LONG", 1.2,
        float(row["native_target_pct"]), tighten_after_minutes=120, tightened_stop_pct=1.)
    count, sign = 0, 1 if row["side"] == "LONG" else -1
    for current in range(index, min(5, int(native["exit_path_index"]))):
        count = count + 1 if sign * (float(path["close"][current]) / entry - 1) * 100 <= -.3 else 0
        if count < 2:
            continue
        following = current + 1
        opening = float(path["open"][following])
        if sign * (opening / entry - 1) * 100 <= -1.2 or sign * (
                opening / entry - 1) * 100 >= float(row["native_target_pct"]):
            break
        return reversal.failed._open_result(native, path, index, entry, sign == 1,
                                           following, opening, "G5_PROTECTIVE_EXIT")
    return native


def allocate(native, multiplier, config, cost_bps=5.):
    """Recompute all monetary fields and chronological allocation for each variant."""
    work = native.copy()
    factor = pd.Series(multiplier, index=work.sid).reindex(work.sid).to_numpy(float)
    if not np.isfinite(factor).all() or not np.isin(factor, [0., .5, 1.]).all():
        raise ValueError("G5 permits only zero, half or normal position size")
    work["g5_native_filled"] = work.filled.eq(True)
    work["g5_multiplier"] = factor
    work["filled"] = work.filled.eq(True) & (factor > 0)
    work["cost_pct"] = np.where(work.filled, cost_bps / 100, np.nan)
    work["net_return_pct"] = work.gross_return_pct - work.cost_pct
    sized = ENGINE.v5.apply_fixed_capital_model(work, config["capital_per_entry_rs"], config["leverage"])
    for field in MONEY_COLUMNS:
        sized[field] = sized[field].to_numpy(float) * factor
    settings = ENGINE.V9Config(portfolio_capital_rupees=config["portfolio_capital_rs"],
        capital_per_entry_rupees=config["capital_per_entry_rs"], leverage_factor=config["leverage"],
        max_positions=config["max_positions"], cost_bps=cost_bps)
    book, allocation = ENGINE.v6.apply_portfolio_constraints(sized, settings.portfolio_config())
    book["day"] = book.day.astype(str).str[:10]
    skipped = book.g5_multiplier.eq(0)
    book.loc[skipped, "portfolio_status"] = "G5_SKIPPED"
    book.loc[skipped, "portfolio_reject_reason"] = "PREDICTED_NET_R_NOT_POSITIVE"
    book["execution_authority"] = False
    book["scope"] = "BACKTEST_ONLY"
    return book, allocation


def choose_exit(books, train_days, config):
    baseline = books["baseline"].loc[books["baseline"].day.isin(train_days)].set_index("sid")
    original = baseline.portfolio_net_profit_rupees
    base_stats = bounded.stats(books["baseline"], train_days)
    audits, eligible = [], []
    rules = config["exit_selection"]
    for policy, book in books.items():
        candidate = book.loc[book.day.isin(train_days)].set_index("sid").reindex(baseline.index)
        pnl = candidate.portfolio_net_profit_rupees
        stats = bounded.stats(book, train_days)
        winner_sum = float(original.loc[original.gt(0)].sum())
        retention = float(pnl.loc[original.gt(0)].sum() / winner_sum) if winner_sum else 0.
        changed = int((pnl - original).abs().gt(1e-6).sum())
        qualifies = (policy != "baseline" and base_stats["trades"] >= config["minimum_training_fills"]
            and changed >= rules["minimum_changed_trades"]
            and retention >= rules["minimum_original_winner_profit_retention"]
            and stats["net_pnl_rs"] > base_stats["net_pnl_rs"] + 1e-6
            and stats["negative_trade_sum_abs_rs"] < base_stats["negative_trade_sum_abs_rs"] - 1e-6
            and stats["daily_close_drawdown_rs"] <= base_stats["daily_close_drawdown_rs"] + 1e-6)
        audits.append(dict(policy=policy, qualifies=bool(qualifies), changed_trades=changed,
                           winner_profit_retention=retention, **stats))
        if qualifies:
            eligible.append((stats["net_pnl_rs"], policy))
    selected = sorted(eligible, key=lambda item: (-item[0], item[1]))[0][1] if eligible else "baseline"
    return selected, audits


def walkforward(features, labels, books, days, config, model_dir):
    predictions = features[["sid", "day", "decision_ts", "feature_ts", "feature_valid", "feature_reason"]].copy()
    predictions["fold"] = -1
    predictions["phase"] = "warmup"
    predictions["prediction_net_r"] = np.nan
    predictions["train_end"] = ""
    predictions["alpha"] = np.nan
    predictions["exit_policy"] = "baseline"
    predictions["model_reason"] = "WARMUP_BASELINE"
    audits, exit_audits = [], []
    for fold, train_days, test_days in folds(days, config["initial_train_sessions"], config["test_block_sessions"]):
        model, audit = fit_model(features, labels, train_days, config)
        saved = serialize_model(model, audit, train_days)
        selected_exit, selections = choose_exit(books, train_days, config)
        if not config["ml_enabled"]:
            saved["enabled"], selected_exit = False, "baseline"
        write_json(model_dir / f"fold_{fold}.json", saved)
        mask = predictions.day.isin(test_days)
        values = score_saved_model(features.loc[mask], saved)
        predictions.loc[mask, "prediction_net_r"] = values
        predictions.loc[mask, "fold"] = fold
        predictions.loc[mask, "phase"] = "evaluation"
        predictions.loc[mask, "train_end"] = train_days[-1]
        predictions.loc[mask, "alpha"] = audit["alpha"]
        predictions.loc[mask, "exit_policy"] = selected_exit
        predictions.loc[mask, "model_reason"] = audit["reason"] if config["ml_enabled"] else "ML_DISABLED"
        audits.append(dict(fold=fold, train_start=train_days[0], train_end=train_days[-1],
            test_start=test_days[0], test_end=test_days[-1], training_sessions=len(train_days),
            test_sessions=len(test_days), selected_exit=selected_exit, **audit))
        exit_audits.extend(dict(fold=fold, train_end=train_days[-1], **item) for item in selections)
        print(f"Fold {fold}: through {train_days[-1]}, alpha={audit['alpha']}, exit={selected_exit}", flush=True)
    model, audit = fit_model(features, labels, days, config)
    saved = serialize_model(model, audit, days)
    saved["enabled"] = bool(saved["enabled"] and config["ml_enabled"])
    saved["next_block_exit_policy"] = choose_exit(books, days, config)[0] if config["ml_enabled"] else "baseline"
    saved["execution_authority"] = False
    write_json(model_dir / "next_block_shadow_model.json", saved)
    predictions["ml_size_multiplier"] = np.where(predictions.prediction_net_r.le(
        config["decision_threshold_net_r"]), .5, 1.)
    predictions["ml_skip_multiplier"] = np.where(predictions.prediction_net_r.le(
        config["decision_threshold_net_r"]), 0., 1.)
    return predictions, audits, exit_audits


def variant_native(catalogue, predictions, name):
    policies = predictions.exit_policy.copy()
    if name == "fixed_protective":
        policies[:] = "protective"
    elif name == "fixed_percentage_trail":
        policies[:] = "percentage_trail"
    elif name not in ("adaptive_exit", "combined_size", "combined_skip"):
        policies[:] = "baseline"
    frames = []
    for policy in catalogue:
        selected = set(predictions.loc[policies.eq(policy), "sid"])
        frames.append(catalogue[policy].loc[catalogue[policy].sid.isin(selected)])
    return pd.concat(frames, ignore_index=True).set_index("sid", drop=False).loc[
        predictions.index].reset_index(drop=True)


def multiplier_for(predictions, name):
    if name == "uniform_half":
        return pd.Series(.5, index=predictions.index)
    if name in ("ml_size", "combined_size"):
        return predictions.ml_size_multiplier
    if name in ("ml_skip", "combined_skip"):
        return predictions.ml_skip_multiplier
    return pd.Series(1., index=predictions.index)


def minute_equity(book, paths, days):
    """Minute-close portfolio marks; transaction costs recognized at entry."""
    result, carry = [], 0.
    for day in days:
        grid = pd.date_range(day + " 09:16", day + " 15:15", freq="min", tz="Asia/Kolkata")
        stamps = grid.as_unit("ns").asi8
        net = np.zeros(len(grid))
        selected = book.loc[book.day.eq(day) & book.portfolio_executed.eq(True)]
        for row in selected.itertuples():
            path = paths[int(row.sid)]
            entered, exited = pd.Timestamp(row.entry_ts).value, pd.Timestamp(row.exit_ts).value
            active = (stamps >= entered) & (stamps < exited)
            indices = np.searchsorted(path["timestamp_ns"], stamps[active])
            if len(indices) and np.any(np.asarray(path["timestamp_ns"])[indices] != stamps[active]):
                raise ValueError("Missing minute mark")
            sign = 1 if row.side == "LONG" else -1
            net[active] += sign * (np.asarray(path["close"])[indices] / row.entry_price - 1) * row.exposure_per_entry_rupees - row.cost_rupees
            net[stamps >= exited] += row.portfolio_net_profit_rupees
        expected = float(selected.portfolio_net_profit_rupees.sum())
        if not np.isclose(net[-1], expected, atol=1e-6, rtol=0):
            raise ValueError("Minute equity does not reconcile to realized P&L")
        result.append(pd.DataFrame(dict(day=day, timestamp=grid.astype(str), equity_pnl_rs=carry + net,
                                        session_pnl_rs=net)))
        carry += expected
    return pd.concat(result, ignore_index=True)


def metrics(book, days, minute_marks):
    result = bounded.stats(book, days)
    selected = book.loc[book.day.isin(days) & book.portfolio_executed.eq(True)]
    pnl = selected.portfolio_net_profit_rupees
    exposure = float(selected.exposure_per_entry_rupees.sum())
    marks = minute_marks.loc[minute_marks.day.isin(days), "equity_pnl_rs"].to_numpy(float)
    curve = np.r_[0., marks - (marks[0] if len(marks) else 0.)]
    result.update(total_trade_exposure_rs=exposure,
        net_per_million_trade_exposure_rs=float(pnl.sum() / exposure * 1e6) if exposure else 0.,
        profit_factor=float(pnl[pnl > 0].sum() / -pnl[pnl < 0].sum()) if (pnl < 0).any() else None,
        worst_day_rs=float(selected.groupby("day").portfolio_net_profit_rupees.sum().reindex(days, fill_value=0.).min()),
        minute_mark_drawdown_rs=float(np.max(np.maximum.accumulate(curve) - curve)),
        average_position_multiplier=float(selected.g5_multiplier.mean()) if len(selected) else 0.)
    return result


def block_interval(delta, config):
    values = np.asarray(delta, dtype=float)
    settings = config["bootstrap"]
    rng = np.random.default_rng(settings["seed"])
    count, block = settings["repetitions"], settings["block_sessions"]
    starts = rng.integers(0, len(values), size=(count, int(np.ceil(len(values) / block))))
    indices = (starts[:, :, None] + np.arange(block)) % len(values)
    totals = values[indices.reshape(count, -1)[:, :len(values)]].sum(axis=1)
    lo, hi = np.quantile(totals, [.025, .975])
    return dict(lower_95_delta_rs=float(lo), upper_95_delta_rs=float(hi),
                positive_fraction=float(np.mean(totals > 0)))


def acceptance(candidate, control, config):
    if control["net_pnl_rs"] <= 0:
        return "INCONCLUSIVE_NONPOSITIVE_BASELINE"
    limits = config["acceptance"]
    if (candidate["net_pnl_rs"] >= limits["growth_minimum_net_ratio"] * control["net_pnl_rs"]
        and candidate["negative_trade_sum_abs_rs"] <= limits["growth_maximum_loss_amount_ratio"] * control["negative_trade_sum_abs_rs"]
        and candidate["daily_close_drawdown_rs"] <= limits["growth_maximum_drawdown_ratio"] * control["daily_close_drawdown_rs"]):
        return "HISTORICAL_GROWTH_SCREEN_PASS"
    if (candidate["net_pnl_rs"] >= limits["defensive_minimum_net_ratio"] * control["net_pnl_rs"]
        and candidate["daily_close_drawdown_rs"] <= limits["defensive_maximum_drawdown_ratio"] * control["daily_close_drawdown_rs"]):
        return "HISTORICAL_DEFENSIVE_SCREEN_PASS"
    return "DOES_NOT_PASS"


def report_html(summary, comparison, daily, fold_table):
    columns = ["variant", "trades", "losses", "net_pnl_rs", "net_change_rs",
               "negative_trade_sum_abs_rs", "daily_close_drawdown_rs", "minute_mark_drawdown_rs", "screen"]
    def table(frame):
        return '<div class="scroll">' + frame.to_html(index=False, border=0, float_format=lambda x: f"{x:,.2f}") + '</div>'
    evaluation = comparison.loc[comparison.stress.eq("base_5bps") & comparison.period.eq("evaluation")]
    october = comparison.loc[comparison.stress.eq("base_5bps") & comparison.period.isin(["october_15", "october_16"])]
    return ('<!doctype html><html><head><meta charset="utf-8"><title>G5 ML walk-forward results</title>'
        '<style>body{font:15px/1.5 system-ui;max-width:1500px;margin:auto;padding:30px;background:#101827;color:#e6edf6}'
        'a{color:#8ed0ff}table{border-collapse:collapse;font-size:13px}td,th{padding:8px;border-bottom:1px solid #405069;text-align:right;white-space:nowrap}'
        '.scroll{overflow:auto}h1,h2{color:#b9dcff}</style></head><body><h1>G5: G4 replica and ML research</h1>'
        '<p>Exact G4 control parity passed. 53 previously reviewed sessions; first 30 train, next 23 evaluate. '
        'Models refit every five sessions using prior data only. These results are retrospective; future shadow validation remains pending.</p>'
        '<h2>Primary evaluation: September 11–October 16, 2026</h2>' + table(evaluation[columns]) +
        '<h2>15 / 16 October diagnostics</h2>' + table(october[["variant", "period", "trades", "losses", "net_pnl_rs", "net_change_rs"]]) +
        '<h2>Training and evaluation boundaries</h2>' + table(fold_table.drop(columns=["inner_scores"])) +
        '<h2>All costs and timing stresses</h2>' + table(comparison.loc[comparison.period.eq("evaluation"), ["variant", "stress", "net_pnl_rs", "net_change_rs", "negative_trade_sum_abs_rs", "daily_close_drawdown_rs"]]) +
        '<h2>Daily results</h2>' + table(daily.loc[daily.stress.eq("base_5bps"), ["day", "variant", "trades", "losses", "net_pnl_rs"]]) +
        '<h2>Interpretation and limitations</h2><ul>' + ''.join('<li>' + html.escape(item) + '</li>' for item in summary["limitations"]) +
        '</ul><p><a href="comparison.csv">Comparisons</a> · <a href="daily_comparison.csv">Daily CSV</a> · '
        '<a href="predictions.csv">Predictions</a> · <a href="folds.csv">Folds</a> · <a href="trade_changes.csv">Trade changes</a> · '
        '<a href="summary.json">Summary</a> · <a href="ROLLBACK.md">Rollback</a></p></body></html>')


def run(output, source=prior.DEFAULT_DATASET, config_path=DEFAULT_CONFIG, ml_disabled=False):
    output = Path(output).resolve()
    if output.exists() or not output.is_relative_to(G5_ROOT.resolve()):
        raise ValueError("A fresh directory inside the isolated G5 root is required")
    config = json.loads(Path(config_path).read_text(encoding="utf-8"))
    if config != json.loads(DEFAULT_CONFIG.read_text(encoding="utf-8")):
        raise ValueError("Use the preregistered V1 configuration; changed protocols need a new version")
    if ml_disabled:
        config["ml_enabled"] = False
    output.mkdir(parents=True)
    write_json(output / "config.json", config)
    model_dir = output / "models"
    model_dir.mkdir()
    original, contexts, paths, proof = dataset.load(source)
    days = list(proof["days"])
    if len(days) != 53 or len(original) != 543 or not proof["native_control_exactly_reproduced"]:
        raise ValueError("Unexpected or unverified G4 cohort")
    original["day"] = original.day.astype(str).str[:10]
    catalogue = native_catalogue(original, paths, config)
    ones = pd.Series(1., index=original.sid)
    policy_books = {name: allocate(native, ones, config)[0] for name, native in catalogue.items()}
    sealed.parity(policy_books["baseline"], original)
    print("Exact native G4 and portfolio parity passed", flush=True)
    features = build_entry_features(original, contexts)
    features.to_csv(output / "features.csv", index=False)
    labels = catalogue["baseline"].set_index("sid").net_return_pct / config["initial_stop_pct"]
    pd.DataFrame(dict(sid=labels.index, label_net_r=labels.to_numpy())).to_csv(output / "labels.csv", index=False)
    predictions, fold_audits, exit_audits = walkforward(features, labels, policy_books, days, config, model_dir)
    predictions.to_csv(output / "predictions.csv", index=False)
    fold_table = pd.DataFrame(fold_audits)
    fold_table.to_csv(output / "folds.csv", index=False)
    pd.DataFrame(exit_audits).to_csv(output / "exit_selection.csv", index=False)
    delayed = native_catalogue(original, paths, config, delay=config["latency_stress_delay_bars"])
    stresses = [(f"base_{int(cost)}bps", cost, catalogue) for cost in config["cost_bps"]]
    stresses.append(("one_bar_entry_delay_5bps", 5., delayed))
    periods = dict(all_including_warmup=days, evaluation=days[config["initial_train_sessions"]:],
                   october_15=["2026-10-15"], october_16=["2026-10-16"])
    comparisons, daily_rows, trade_changes, fold_results, intervals = [], [], [], [], []
    primary_books = {}
    for stress, cost, native_set in stresses:
        stress_books, marks = {}, {}
        for name in VARIANTS:
            native = variant_native(native_set, predictions, name)
            book, allocation = allocate(native, multiplier_for(predictions, name), config, cost)
            book["strategy_version"] = config["variant_id"] + "-" + name
            stress_books[name] = book
            marks[name] = minute_equity(book, paths, days)
            folder = output / stress / name
            folder.mkdir(parents=True)
            book.to_csv(folder / "portfolio_trades.csv", index=False)
            write_json(folder / "allocation.json", allocation)
            marks[name].to_csv(folder / "minute_equity.csv", index=False)
            saved = base.read_frozen_csv(folder / "portfolio_trades.csv")
            if not np.isclose(saved.portfolio_net_profit_rupees.sum(), book.portfolio_net_profit_rupees.sum(), atol=1e-6, rtol=0):
                raise ValueError("Saved trade ledger does not reconcile")
            if ml_disabled and name in ("ml_size", "ml_skip", "adaptive_exit", "combined_size", "combined_skip"):
                sealed.parity(book, stress_books["control"])
        control = stress_books["control"]
        for name, book in stress_books.items():
            delta = prior._changes(control, book, name)
            delta["stress"] = stress
            trade_changes.append(delta)
            for period, cohort in periods.items():
                stats = metrics(book, cohort, marks[name])
                reference = metrics(control, cohort, marks["control"])
                rows = delta.loc[delta.day_baseline.isin(cohort)]
                comparisons.append(dict(variant=name, stress=stress, period=period, **stats,
                    net_change_rs=stats["net_pnl_rs"] - reference["net_pnl_rs"],
                    baseline_losers_skipped=int((rows.portfolio_net_profit_rupees_baseline.lt(0) & ~rows.portfolio_executed_variant.eq(True)).sum()),
                    baseline_winners_skipped=int((rows.portfolio_net_profit_rupees_baseline.gt(0) & ~rows.portfolio_executed_variant.eq(True)).sum()),
                    screen=acceptance(stats, reference, config) if name != "control" else "REFERENCE"))
            daily_rows.extend(dict(variant=name, stress=stress, day=day, **bounded.stats(book, [day])) for day in days)
            if stress == "base_5bps":
                for fold, _, cohort in folds(days, config["initial_train_sessions"], config["test_block_sessions"]):
                    stats, reference = bounded.stats(book, cohort), bounded.stats(control, cohort)
                    fold_results.append(dict(variant=name, fold=fold, first_day=cohort[0], last_day=cohort[-1],
                        **stats, net_change_rs=stats["net_pnl_rs"] - reference["net_pnl_rs"]))
                test = periods["evaluation"]
                def daily_pnl(value):
                    return value.groupby("day").portfolio_net_profit_rupees.sum().reindex(test, fill_value=0.)
                intervals.append(dict(variant=name, **block_interval(daily_pnl(book) - daily_pnl(control), config)))
        if stress == "base_5bps":
            primary_books = stress_books
        print(stress + " portfolio replay complete", flush=True)
    comparison, daily = pd.DataFrame(comparisons), pd.DataFrame(daily_rows)
    comparison.to_csv(output / "comparison.csv", index=False)
    daily.to_csv(output / "daily_comparison.csv", index=False)
    pd.concat(trade_changes, ignore_index=True).to_csv(output / "trade_changes.csv", index=False)
    pd.DataFrame(fold_results).to_csv(output / "fold_results.csv", index=False)
    pd.DataFrame(intervals).to_csv(output / "bootstrap_intervals.csv", index=False)
    setup_rows = []
    for name, book in primary_books.items():
        for setup in sorted(book.setup_id.unique()):
            setup_rows.append(dict(variant=name, setup_id=setup, **bounded.stats(
                book.loc[book.setup_id.eq(setup)], periods["evaluation"])))
    pd.DataFrame(setup_rows).to_csv(output / "setup_results.csv", index=False)
    summary = dict(variant_id=config["variant_id"], scope="BACKTEST_ONLY", execution_authority=False,
        configuration_changed=False, ml_enabled=config["ml_enabled"], parity_passed=True,
        source_dataset=str(Path(source).resolve()), source_manifest_sha256=base.sha256(Path(source) / "manifest.json"),
        days=days, evaluation_days=periods["evaluation"], features=list(FEATURES),
        feature_invalid_orders=int((~features.feature_valid).sum()), folds=fold_audits,
        primary_results=comparison.loc[comparison.stress.eq("base_5bps") & comparison.period.eq("evaluation")].to_dict("records"),
        bootstrap_intervals=intervals, forward_validation="PENDING_NEW_SESSIONS",
        limitations=[
            "All 53 sessions were previously inspected and helped select G4. Chronological ML splits do not erase that strategy-selection bias.",
            "Only 23 sessions evaluate ML; 30 earlier sessions are warmup. Aggregate all-history results include baseline warmup and are secondary.",
            "All candidate trials and failed screens are retained. Historical screen passes do not authorize promotion.",
            "Labels use native filled orders including portfolio-rejected fills; unfilled orders are not losses. Decisions use confirmation-time features only.",
            "Missing feature inputs retain baseline sizing. No replacement candidate is selected after a skip.",
            "Protective policy exits only filled trades after two 0.30% adverse closes in first five confirmation minutes; no pending-order cancellation.",
            "Trailing uses 0.50% behind the best completed close, activates at max(0.60%, half target), floors at +0.05%, effective next bar.",
            "Minute close marks omit unknown intraminute portfolio extremes. Cost stress is a linear notional charge; broker fees, market impact, and whole-lot rounding remain unmodeled.",
            "One-bar delay is a separate coarse execution stress, not an estimate of seconds-level latency. Predictions and chosen exit policies stay frozen during stress tests.",
            "Bootstrap bands describe resampling of observed daily differences; they are not proof of future profitability.",
            "Existing sources combine equity price paths and futures OI selection evidence; G5 has no live or executable-futures validation.",
            "New-session shadow collection needs future data. Review after at least 30 new sessions and 200 completed eligible fills, with more data if inconclusive."
        ])
    write_json(output / "summary.json", summary)
    (output / "report.html").write_text(report_html(summary, comparison, daily, fold_table), encoding="utf-8")
    (output / "ROLLBACK.md").write_text(
        "# G5 rollback\n\nG4 configuration, registry, dashboard, and live pointers were not modified.\n\n"
        "To verify baseline behavior, run `python fno_v13_v10_g_5_backtest.py --ml-disabled`. "
        "This produces a fresh isolated run and asserts that all adaptive variants match its control exactly.\n\n"
        "Keep this immutable run for audit. Models are numeric JSON with training cutoff and feature order. "
        "`models/next_block_shadow_model.json` is trained through the last recorded session and has no execution authority.\n\n"
        "Score a later order/context snapshot with `python -m research.g5_ml_shadow --model MODEL.json "
        "--orders ORDERS.csv --contexts CONTEXTS.parquet --output OUTPUT.csv`. Only strictly later sessions are accepted.\n",
        encoding="utf-8")
    code = [Path(__file__), Path(config_path), ROOT / "fno_v13_v10_g_5_backtest.py",
            ROOT / "research/g5_ml_shadow.py", Path(prior.__file__), Path(trailing.__file__),
            Path(reversal.__file__), Path(dataset.__file__), Path(ENGINE.v5.__file__), Path(ENGINE.v6.__file__)]
    archive = output / "code"
    archive.mkdir()
    for path in code:
        shutil.copy2(path, archive / path.name)
    write_json(output / "manifest.json", dict(files={p.relative_to(output).as_posix(): base.sha256(p)
        for p in output.rglob("*") if p.is_file()}, code_sha256={str(p.resolve()): base.sha256(p) for p in code}))
    print(comparison.loc[comparison.stress.eq("base_5bps") & comparison.period.eq("evaluation"),
        ["variant", "trades", "losses", "net_pnl_rs", "net_change_rs", "negative_trade_sum_abs_rs", "daily_close_drawdown_rs", "screen"]].to_string(index=False), flush=True)
    print(output, flush=True)
    return comparison


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--source", type=Path, default=prior.DEFAULT_DATASET)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--ml-disabled", action="store_true")
    args = parser.parse_args()
    output = args.output_dir or G5_ROOT / "research" / datetime.now().strftime("%Y%m%dT%H%M%S_ml_walkforward_v1")
    run(output, args.source, args.config, args.ml_disabled)


if __name__ == "__main__":
    main()
