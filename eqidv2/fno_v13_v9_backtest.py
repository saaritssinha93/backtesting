"""V13-v9 selection research on the immutable V13-v5/V6 execution control.

Cash equity supplies execution prices; mapped stock futures supply OI. This is
not a futures-contract or options-premium execution engine. The default is the
incumbent higher-frequency V13-v5 setup book and exits, replayed with V6 capital
constraints. Alternative rules act on every eligible candidate BEFORE top-N
selection. No rule reads forward returns, exits, MFE, MAE or portfolio outcomes.

Five-minute features end at the signal close; one-minute features end at the
confirmation close. Entry starts in the following minute and keeps V13's S+10
limit. The inherited stop-first OHLC ambiguity policy is deliberately retained.
Flat round-trip basis-point costs reproduce V13 accounting; cost sensitivity
and 1x exposure can be requested explicitly. No claim of verified itemized
fees, integer-share execution or out-of-sample improvement is implied.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

import fno_v13_corrected_v5_backtest as v5
import fno_v13_v6_portfolio_backtest as v6


SCHEMA_VERSION = "FNO_V13_V9_SELECTION_RESEARCH_V1"
EVIDENCE_STATUS = "RESEARCH_ONLY_INCUMBENT_FALLBACK_UNTIL_VALIDATED"
EXPECTED_SOURCE_HASHES = {
    "fno_v13_corrected_v5_backtest.py": "d8bb7769b8442a48c0b1a902bc2bf46d29edb2138c9881c9fa38f73d40616e58",
    "fno_v13_v6_portfolio_backtest.py": "1d1297ce155c9014ff7d23a8f3763a2d29b2ba90c0f95dcc26a3464290c7cce5",
    "fno_v5_hybrid_backtest.py": "725e3174dec9855224e76c47164ae8445195c1b1214b1e228c5bd37c561849b4",
}
PICKER_COLUMNS = {
    "max_oi": "oi_change_pct", "max_volume": "volume_ratio",
    "max_move": "abs_price_change_pct", "max_body": "body_ratio",
    "max_liquidity": "traded_value",
}
RANKING_MODES = ("native", "1m_body", "5m_ema_strength")


@dataclass(frozen=True)
class V9Config:
    name: str = "V13_V6_CONTROL"
    require_1m_ema_alignment: bool = False
    min_5m_body_ratio: float | None = None
    min_1m_body_ratio: float | None = None
    max_1m_wick_ratio: float | None = None
    max_1m_range_pct: float | None = None
    max_5m_range_pct: float | None = None
    min_5m_volume_ratio: float | None = None
    max_signed_5m_vwap_extension_pct: float | None = None
    ranking: str = "native"
    maximum_holding_minutes: int | None = None
    cost_bps: float = 5.0
    capital_per_entry_rupees: float = 100_000.0
    leverage_factor: float = 5.0
    portfolio_capital_rupees: float = 300_000.0
    max_positions: int | None = 3
    max_positions_per_symbol: int | None = None

    def validate(self) -> None:
        if not self.name:
            raise ValueError("Configuration name cannot be empty")
        if self.ranking not in RANKING_MODES:
            raise ValueError(f"ranking must be one of {RANKING_MODES}; arbitrary/outcome columns are prohibited")
        for field in ("min_5m_body_ratio", "min_1m_body_ratio", "max_1m_wick_ratio"):
            value = getattr(self, field)
            if value is not None and (not np.isfinite(value) or not 0 <= value <= 1):
                raise ValueError(f"{field} must be between zero and one")
        for field in ("max_1m_range_pct", "max_5m_range_pct", "min_5m_volume_ratio",
                      "max_signed_5m_vwap_extension_pct"):
            value = getattr(self, field)
            if value is not None and (not np.isfinite(value) or value <= 0):
                raise ValueError(f"{field} must be positive and finite")
        if not np.isfinite(self.cost_bps) or self.cost_bps < 0:
            raise ValueError("cost_bps must be nonnegative and finite")
        for field in ("capital_per_entry_rupees", "leverage_factor"):
            value = getattr(self, field)
            if not np.isfinite(value) or value <= 0:
                raise ValueError(f"{field} must be positive and finite")
        if self.maximum_holding_minutes not in (None, 180):
            raise ValueError("Only the original EOD exit or independent V7 180-minute comparator is allowed")
        self.portfolio_config().validate()

    def portfolio_config(self) -> v6.PortfolioConfig:
        return v6.PortfolioConfig(
            portfolio_capital_rupees=self.portfolio_capital_rupees,
            max_positions=self.max_positions,
            max_positions_per_symbol=self.max_positions_per_symbol,
        )

    @property
    def uses_features(self) -> bool:
        return any((self.require_1m_ema_alignment, self.min_5m_body_ratio is not None,
                    self.min_1m_body_ratio is not None, self.max_1m_range_pct is not None,
                    self.max_1m_wick_ratio is not None, self.max_5m_range_pct is not None,
                    self.min_5m_volume_ratio is not None,
                    self.max_signed_5m_vwap_extension_pct is not None,
                    self.ranking != "native"))


def experiment_configs() -> dict[str, V9Config]:
    """Eight individually registered hypotheses; no combinations or exit tuning."""
    variants = {
        "WICK_1M_MAX_035": {"max_1m_wick_ratio": 0.35},
        "BODY_1M_MIN_060": {"min_1m_body_ratio": 0.60},
        "VWAP_EXTENSION_5M_MAX_150": {"max_signed_5m_vwap_extension_pct": 1.50},
        "EMA_1M_ALIGNED": {"require_1m_ema_alignment": True},
        "VOLUME_5M_MIN_150": {"min_5m_volume_ratio": 1.50},
        "RANGE_5M_MAX_150": {"max_5m_range_pct": 1.50},
        "RANK_1M_BODY": {"ranking": "1m_body"},
        "RANK_5M_EMA_SPREAD": {"ranking": "5m_ema_strength"},
    }
    return {name: V9Config(name=name, **fields) for name, fields in variants.items()}


def source_hashes() -> dict[str, str]:
    root = Path(__file__).resolve().parent
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
            for name in EXPECTED_SOURCE_HASHES}


def validate_configuration() -> dict[str, str]:
    observed = source_hashes()
    for name, expected in EXPECTED_SOURCE_HASHES.items():
        if observed[name] != expected:
            raise RuntimeError(f"Immutable V13 source drift: {name}; expected {expected}, observed {observed[name]}")
    v5.validate_configuration()
    return observed


def _dates(values: Iterable[Any]) -> list[str]:
    return sorted({pd.Timestamp(value).date().isoformat() for value in values})


def _day_mask(signals: pd.DataFrame, days: Iterable[Any]) -> pd.Series:
    return pd.to_datetime(signals["day"]).dt.strftime("%Y-%m-%d").isin(_dates(days))


def _feature_requirements(cfg: V9Config) -> tuple[set[str], set[str]]:
    five: set[str] = set()
    one: set[str] = set()
    if cfg.min_5m_body_ratio is not None:
        five.add("v9_5m_body_ratio")
    if cfg.min_1m_body_ratio is not None or cfg.ranking == "1m_body":
        one.add("v9_1m_body_ratio")
    if cfg.max_1m_range_pct is not None:
        one.add("v9_1m_range_pct")
    if cfg.max_1m_wick_ratio is not None:
        one.add("wick_ratio")
    if cfg.max_5m_range_pct is not None:
        five.add("v9_5m_range_pct")
    if cfg.min_5m_volume_ratio is not None:
        five.add("v9_5m_volume_ratio")
    if cfg.max_signed_5m_vwap_extension_pct is not None:
        five.add("v9_5m_distance_vwap_pct")
    if cfg.require_1m_ema_alignment:
        one.update(("v9_1m_ema9", "v9_1m_ema20"))
    if cfg.ranking == "5m_ema_strength":
        five.update(("v9_5m_ema9", "v9_5m_ema20", "signal_close"))
    return five, one


def _validate_feature_clock(rows: pd.DataFrame, cfg: V9Config) -> None:
    five, one = _feature_requirements(cfg)
    required = five | one
    required |= {"v9_5m_feature_ts"} if five else set()
    required |= {"v9_1m_feature_ts"} if one else set()
    missing = sorted(required.difference(rows.columns))
    if missing:
        raise ValueError(f"Causal feature inputs missing: {missing}")
    confirmation = pd.to_datetime(rows["confirmation_ts"], utc=True)
    signal = (pd.to_datetime(rows["signal_ts"], utc=True) if "signal_ts" in rows
              else confirmation - pd.Timedelta(minutes=1))
    for needed, column, cutoff in (
        (five, "v9_5m_feature_ts", signal), (one, "v9_1m_feature_ts", confirmation),
    ):
        if not needed:
            continue
        stamp = pd.to_datetime(rows[column], utc=True, errors="coerce")
        present = rows[list(needed)].notna().any(axis=1)
        if (present & (stamp.isna() | stamp.gt(cutoff))).any():
            raise ValueError(f"Noncausal or undocumented feature timestamp: {column}")


def _true(values: pd.Series) -> pd.Series:
    return values.eq(True) | values.astype(str).str.lower().eq("true")


def _filter_and_score(rows: pd.DataFrame, cfg: V9Config) -> pd.DataFrame:
    out = rows.copy()
    out["v9_filter_pass"] = True
    out["v9_filter_reject_reason"] = ""
    out["v9_rank_score"] = np.nan
    if not cfg.uses_features or out.empty:
        return out
    _validate_feature_clock(out, cfg)
    checks: list[tuple[str, pd.Series]] = []
    if cfg.require_1m_ema_alignment:
        ema9 = pd.to_numeric(out["v9_1m_ema9"], errors="coerce")
        ema20 = pd.to_numeric(out["v9_1m_ema20"], errors="coerce")
        aligned = pd.Series(np.where(out["side"].eq("LONG"),
            ema9.gt(ema20), ema9.lt(ema20)), index=out.index)
        aligned &= np.isfinite(ema9) & np.isfinite(ema20)
        checks.append(("ONE_MINUTE_EMA_NOT_ALIGNED_OR_MISSING", aligned))
    for field, threshold in (("v9_5m_body_ratio", cfg.min_5m_body_ratio),
                             ("v9_1m_body_ratio", cfg.min_1m_body_ratio)):
        if threshold is not None:
            checks.append((f"{field.upper()}_BELOW_MIN_OR_MISSING",
                           pd.to_numeric(out[field], errors="coerce").ge(threshold)))
    if cfg.max_1m_range_pct is not None:
        values = pd.to_numeric(out["v9_1m_range_pct"], errors="coerce")
        checks.append(("ONE_MINUTE_RANGE_TOO_LARGE_OR_MISSING",
                       values.ge(0) & values.le(cfg.max_1m_range_pct)))
    if cfg.max_1m_wick_ratio is not None:
        values = pd.to_numeric(out["wick_ratio"], errors="coerce")
        checks.append(("ONE_MINUTE_WICK_TOO_LARGE_OR_MISSING",
                       values.ge(0) & values.le(cfg.max_1m_wick_ratio)))
    if cfg.max_5m_range_pct is not None:
        values = pd.to_numeric(out["v9_5m_range_pct"], errors="coerce")
        checks.append(("FIVE_MINUTE_RANGE_TOO_LARGE_OR_MISSING",
                       values.ge(0) & values.le(cfg.max_5m_range_pct)))
    if cfg.min_5m_volume_ratio is not None:
        values = pd.to_numeric(out["v9_5m_volume_ratio"], errors="coerce")
        checks.append(("FIVE_MINUTE_VOLUME_TOO_SMALL_OR_MISSING",
                       values.ge(cfg.min_5m_volume_ratio)))
    if cfg.max_signed_5m_vwap_extension_pct is not None:
        sign = np.where(out["side"].eq("LONG"), 1.0, -1.0)
        values = pd.to_numeric(out["v9_5m_distance_vwap_pct"], errors="coerce") * sign
        checks.append(("FIVE_MINUTE_VWAP_EXTENSION_TOO_LARGE_OR_MISSING",
                       np.isfinite(values) & values.le(cfg.max_signed_5m_vwap_extension_pct)))
    if cfg.ranking == "1m_body":
        out["v9_rank_score"] = pd.to_numeric(out["v9_1m_body_ratio"], errors="coerce")
    elif cfg.ranking == "5m_ema_strength":
        sign = np.where(out["side"].eq("LONG"), 1.0, -1.0)
        ema9 = pd.to_numeric(out["v9_5m_ema9"], errors="coerce")
        ema20 = pd.to_numeric(out["v9_5m_ema20"], errors="coerce")
        close = pd.to_numeric(out["signal_close"], errors="coerce")
        out["v9_rank_score"] = (ema9 - ema20) / close.where(close.gt(0)) * 100 * sign
    if cfg.ranking != "native":
        checks.append(("RANK_FEATURE_MISSING_OR_NONFINITE", np.isfinite(out["v9_rank_score"])))
    for reason, passed in checks:
        failed = ~passed.fillna(False)
        first_fail = failed & out["v9_filter_pass"]
        out.loc[first_fail, "v9_filter_reject_reason"] = reason
        out.loc[failed, "v9_filter_pass"] = False
    return out


def _setup_metadata(rows: pd.DataFrame, setup: Any) -> pd.DataFrame:
    out = rows.copy()
    out["setup_id"] = setup.setup_id
    out["native_stop_pct"] = setup.stop_pct
    out["native_target_pct"] = setup.target_pct
    out["configured_confirmation_end"] = setup.confirmation_end
    out["picker"] = setup.picker
    out["max_entries"] = setup.max_entries
    return out


def selection_audit(signals: pd.DataFrame, cfg: V9Config | None = None) -> pd.DataFrame:
    """Return every native setup-eligible row, including V9 filters and top-N rejects.

    Input signals must already have the native strict confirmation, index and
    contract gates applied by the V9 data builder. Broader selection/rejection
    stages are retained separately by that builder.
    """
    cfg = cfg or V9Config()
    cfg.validate()
    signals = signals.reset_index(drop=True)
    parts: list[pd.DataFrame] = []
    for setup in v5.profile_setups(v5.PROFILES["higher_frequency"]):
        rows = v5.replay._eligible(signals, setup)
        if rows.empty:
            continue
        rows = _filter_and_score(rows, cfg)
        rows["v9_selected"] = False
        rows["v9_rank_in_setup_day"] = np.nan
        picker = PICKER_COLUMNS[setup.picker]
        if picker == "abs_price_change_pct":
            rows[picker] = rows["price_change_pct"].abs()
        passed = rows.loc[rows["v9_filter_pass"]].copy()
        columns = ["day"] + (["v9_rank_score"] if cfg.ranking != "native" else [])
        columns += [picker, "traded_value", "tradingsymbol"]
        columns = list(dict.fromkeys(columns))
        directions = [name in ("day", "tradingsymbol") for name in columns]
        ranked = passed.sort_values(columns, ascending=directions, kind="stable")
        ranks = ranked.groupby("day", sort=False).cumcount() + 1
        rows.loc[ranked.index, "v9_rank_in_setup_day"] = ranks.astype(float)
        if cfg.ranking == "native":
            # The incumbent and every pure-filter variant use the immutable
            # native picker AFTER filtering, allowing previously ranked-out
            # rows to replace removed candidates.
            selected = v5.replay.select_setup_rows(passed, setup)
        else:
            selected = ranked.groupby("day", sort=False, as_index=False).head(setup.max_entries)
        rows.loc[selected.index, "v9_selected"] = True
        rows["v9_decision"] = np.select(
            [~rows["v9_filter_pass"], rows["v9_selected"]],
            ["V9_FILTER_REJECTED", "SELECTED"], default="RANKED_OUT")
        rows["v9_configuration"] = cfg.name
        parts.append(_setup_metadata(rows, setup))
    if not parts:
        return pd.DataFrame(columns=[*signals.columns, "setup_id", "v9_selected",
            "v9_filter_pass", "v9_filter_reject_reason", "v9_rank_score",
            "v9_rank_in_setup_day", "v9_decision", "v9_configuration"])
    return pd.concat(parts, ignore_index=True).sort_values(
        ["day", "hhmm_int", "side", "setup_id", "tradingsymbol"], kind="stable"
    ).reset_index(drop=True)


def select_orders(signals: pd.DataFrame, cfg: V9Config | None = None) -> pd.DataFrame:
    audit = selection_audit(signals, cfg)
    return audit.loc[audit["v9_selected"].eq(True)].reset_index(drop=True)


def validate_paths(orders: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]]) -> None:
    """Fail closed on missing minutes; missing prices never remove losing trades."""
    for row in orders.drop_duplicates("sid").itertuples(index=False):
        sid = int(row.sid)
        if sid not in paths:
            raise RuntimeError(f"Missing raw execution path for selected sid={sid}")
        path = paths[sid]
        required = {"timestamp_ns", "open", "high", "low", "close"}
        if required.difference(path):
            raise RuntimeError(f"Incomplete path fields for sid={sid}")
        stamp = np.asarray(path["timestamp_ns"], dtype=np.int64)
        confirmation = v5._to_ist_timestamp(row.confirmation_ts)
        cutoff = confirmation.normalize() + pd.Timedelta(hours=15, minutes=15)
        expected = pd.date_range(confirmation + pd.Timedelta(minutes=1), cutoff, freq="min").asi8
        if not len(stamp) or not np.array_equal(stamp, expected):
            raise RuntimeError(f"Non-continuous or mistimed path for sid={sid}; expected confirmation+1 through exact 15:15")
        prices = [np.asarray(path[field], dtype=float) for field in ("open", "high", "low", "close")]
        if any(len(values) != len(stamp) for values in prices):
            raise RuntimeError(f"Mismatched path array lengths for sid={sid}")
        if any(not np.isfinite(values).all() or (values <= 0).any() for values in prices):
            raise RuntimeError(f"Invalid OHLC price in path for sid={sid}")
        opening, high, low, close = prices
        if ((high < np.maximum(opening, close)) | (low > np.minimum(opening, close)) | (high < low)).any():
            raise RuntimeError(f"Inconsistent OHLC bars for sid={sid}")


def replay_candidates(orders: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]],
                      cfg: V9Config | None = None) -> pd.DataFrame:
    """Native exits on selected or counterfactual candidates; does not allocate capital."""
    cfg = cfg or V9Config()
    cfg.validate()
    validate_paths(orders, paths)
    exit_spec = replace(v5.PROFILES["higher_frequency"].exit,
                        maximum_holding_minutes=cfg.maximum_holding_minutes)
    out = v5.simulate_scaleout(orders, paths, exit_spec, cost_bps=cfg.cost_bps,
                              max_entry_delay_minutes=v5.MAX_ENTRY_DELAY_MINUTES)
    # Native simulator emits only the fields encountered. Stable empty/unfilled
    # schemas let the unchanged V6 portfolio function handle zero-fill periods.
    for name, default in (("filled", False), ("entry_ts", pd.NaT), ("exit_ts", pd.NaT),
                          ("gross_return_pct", np.nan), ("net_return_pct", np.nan),
                          ("cost_pct", np.nan), ("initial_stop_pct", exit_spec.initial_stop_pct)):
        if name not in out:
            out[name] = default
    out = v5.apply_fixed_capital_model(out, cfg.capital_per_entry_rupees, cfg.leverage_factor)
    out["configured_setup_rules"] = len(v5.profile_setups(v5.PROFILES["higher_frequency"]))
    out["v9_configuration"] = cfg.name
    out["v9_schema_version"] = SCHEMA_VERSION
    return out


def evaluate(signals: pd.DataFrame, paths: dict[int, dict[str, np.ndarray]],
             days: Iterable[Any], cfg: V9Config | None = None
             ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Reselect from candidate signals and perform a NEW V6 portfolio replay."""
    cfg = cfg or V9Config()
    cfg.validate()
    hashes = validate_configuration()
    session_days = _dates(days)
    subset = signals.loc[_day_mask(signals, session_days)].copy().reset_index(drop=True)
    decisions = selection_audit(subset, cfg)
    selected = decisions.loc[decisions["v9_selected"].eq(True)].reset_index(drop=True)
    audit = replay_candidates(selected, paths, cfg)
    ledger, summary = v6.apply_portfolio_constraints(audit, cfg.portfolio_config())
    summary.update({
        "schema_version": SCHEMA_VERSION, "name": cfg.name, "v9_config": asdict(cfg),
        "evidence_status": EVIDENCE_STATUS, "source_hashes": hashes,
        "sessions": len(session_days), "first_day": session_days[0] if session_days else None,
        "last_day": session_days[-1] if session_days else None,
        "strict_input_signals": len(subset), "native_setup_eligible": len(decisions),
        "v9_filter_rejected": int((~decisions["v9_filter_pass"].astype(bool)).sum()),
        "selected_orders": len(audit), "flat_round_trip_cost_bps": cfg.cost_bps,
        "net_return_on_initial_portfolio_pct": summary["net_profit_rupees"] / cfg.portfolio_capital_rupees * 100,
        "unconstrained_net_profit_rupees": float(audit["net_profit_rupees"].sum()),
        "selection_policy": "CAUSAL_FILTER_THEN_TOP_N_THEN_NEW_PORTFOLIO_REPLAY",
    })
    return audit, ledger, summary


def assert_control_parity(expected: pd.DataFrame, observed: pd.DataFrame,
                          *, tolerance: float = 1e-7) -> dict[str, Any]:
    """Compare semantic order keys, fills, exits and P&L despite regenerated SIDs."""
    keys = ["day", "tradingsymbol", "side", "setup_id"]
    fields = ["filled", "exit_reason", "net_profit_rupees"]
    for optional in ("entry_ts", "exit_ts", "entry_price", "exit_price", "portfolio_executed"):
        if optional in expected and optional in observed:
            fields.append(optional)
    normalized = []
    for frame in (expected, observed):
        work = frame.loc[:, keys + fields].copy()
        work["day"] = pd.to_datetime(work["day"]).dt.strftime("%Y-%m-%d")
        if work.duplicated(keys).any():
            raise RuntimeError("Control parity requires unique semantic order keys")
        normalized.append(work)
    merged = normalized[0].merge(normalized[1], on=keys, how="outer", suffixes=("_expected", "_observed"), indicator=True)
    if not merged["_merge"].eq("both").all():
        raise RuntimeError("Control parity failed: selected order set differs")
    maximum = 0.0
    for field in fields:
        left, right = merged[f"{field}_expected"], merged[f"{field}_observed"]
        if field in ("filled", "portfolio_executed"):
            equal = _true(left).eq(_true(right))
        elif field in ("entry_ts", "exit_ts"):
            left, right = pd.to_datetime(left, utc=True), pd.to_datetime(right, utc=True)
            equal = left.eq(right) | (left.isna() & right.isna())
        elif field == "exit_reason":
            equal = left.fillna("").eq(right.fillna(""))
        else:
            left, right = pd.to_numeric(left), pd.to_numeric(right)
            equal = pd.Series(np.isclose(left, right, rtol=0, atol=tolerance, equal_nan=True), index=left.index)
            if field == "net_profit_rupees" and len(left):
                maximum = float((left - right).abs().max())
        if not equal.all():
            raise RuntimeError(f"Control parity failed: {field} differs")
    return {"passed": True, "selected_orders": len(merged),
            "maximum_absolute_pnl_delta_rupees": maximum, "tolerance_rupees": tolerance}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--config-json", type=Path)
    args = parser.parse_args(argv)
    import fno_v13_v9_data as data
    dataset = data.load_dataset(args.dataset_dir)
    cfg = V9Config(**json.loads(args.config_json.read_text(encoding="utf-8"))) if args.config_json else V9Config()
    audit, ledger, summary = evaluate(dataset["signals"], dataset["paths"], dataset["days"], cfg)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    audit.to_csv(args.output_dir / "v9_selected_trades.csv", index=False)
    ledger.to_csv(args.output_dir / "v9_portfolio_trades.csv", index=False)
    selection_audit(dataset["signals"], cfg).to_csv(args.output_dir / "v9_selection_audit.csv", index=False)
    (args.output_dir / "v9_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
