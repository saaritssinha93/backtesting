"""Capital-constrained portfolio replay for the frozen V13-v5 trade ledger.

V13-v6 deliberately leaves signal selection and per-trade exits unchanged.  It
adds the missing portfolio state: capital reservation, gross-exposure and
initial-risk limits, concurrent-position limits, and deterministic ordering for
orders that become fillable at the same time.

The source ledger is never overwritten.  Each CLI run is published into a new
run-id directory with its manifest written last.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import fno_oi_common as common


SCHEMA_VERSION = "FNO_V13_V6_PORTFOLIO_V1"
DEFAULT_SOURCE = (
    common.FNO_ROOT
    / "strategy_research"
    / "v13_corrected_v5"
    / "higher_frequency"
    / "fno_v13_corrected_v5_higher_frequency_trades.csv"
)
DEFAULT_OUTPUT_ROOT = common.FNO_ROOT / "strategy_research" / "v13_corrected_v6"


@dataclass(frozen=True)
class PortfolioConfig:
    portfolio_capital_rupees: float
    max_positions: int | None = None
    max_positions_per_symbol: int | None = None
    max_gross_exposure_rupees: float | None = None
    max_open_risk_rupees: float | None = None

    def validate(self) -> None:
        if not np.isfinite(self.portfolio_capital_rupees) or self.portfolio_capital_rupees <= 0:
            raise ValueError("portfolio_capital_rupees must be positive and finite")
        for name in ("max_positions", "max_positions_per_symbol"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be positive when configured")
        for name in ("max_gross_exposure_rupees", "max_open_risk_rupees"):
            value = getattr(self, name)
            if value is not None and (not np.isfinite(value) or value <= 0):
                raise ValueError(f"{name} must be positive and finite when configured")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _timestamps(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for column in ("entry_ts", "exit_ts", "confirmation_ts"):
        if column in out:
            out[column] = pd.to_datetime(out[column], errors="coerce", utc=True).dt.tz_convert(
                common.IST
            )
    return out


def _numeric(frame: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in frame:
        return pd.Series(default, index=frame.index, dtype=float)
    return pd.to_numeric(frame[column], errors="coerce").fillna(default)


def _picker_priority(frame: pd.DataFrame) -> pd.Series:
    """Recreate each setup's documented picker value for deterministic ties."""

    picker = frame.get("picker", pd.Series("", index=frame.index)).astype(str)
    liquidity = _numeric(frame, "traded_value")
    volume = _numeric(frame, "volume_ratio")
    move = _numeric(frame, "abs_price_change_pct")
    if "abs_price_change_pct" not in frame and "price_change_pct" in frame:
        move = _numeric(frame, "price_change_pct").abs()
    return pd.Series(
        np.select(
            [picker.eq("max_liquidity"), picker.eq("max_volume"), picker.eq("max_move")],
            [liquidity, volume, move],
            default=0.0,
        ),
        index=frame.index,
        dtype=float,
    )


def prepare_source_ledger(source: pd.DataFrame) -> pd.DataFrame:
    required = {
        "sid",
        "day",
        "tradingsymbol",
        "filled",
        "entry_ts",
        "exit_ts",
        "capital_per_entry_rupees",
        "exposure_per_entry_rupees",
        "net_profit_rupees",
    }
    missing = sorted(required.difference(source.columns))
    if missing:
        raise ValueError(f"V13-v5 ledger is missing required columns: {missing}")
    out = _timestamps(source)
    out["filled"] = out["filled"].astype(str).str.lower().eq("true") | source[
        "filled"
    ].eq(True)
    filled = out["filled"]
    if out.loc[filled, ["entry_ts", "exit_ts"]].isna().any().any():
        raise ValueError("Filled source trades require valid entry_ts and exit_ts")
    if (out.loc[filled, "exit_ts"] < out.loc[filled, "entry_ts"]).any():
        raise ValueError("Filled source trades cannot exit before entry")
    out["portfolio_priority_value"] = _picker_priority(out)
    out["portfolio_source_row"] = np.arange(len(out), dtype=int)
    return out


def _trade_amounts(row: Any) -> tuple[float, float, float]:
    capital = float(row.capital_per_entry_rupees)
    exposure = float(row.exposure_per_entry_rupees)
    stop_pct = float(getattr(row, "initial_stop_pct", 0.0) or 0.0)
    estimated_cost = float(getattr(row, "cost_rupees", 0.0) or 0.0)
    risk = exposure * stop_pct / 100.0 + max(0.0, estimated_cost)
    if not all(np.isfinite(value) and value >= 0 for value in (capital, exposure, risk)):
        raise ValueError(f"Non-finite portfolio amount for sid={row.sid}")
    return capital, exposure, risk


def apply_portfolio_constraints(
    source: pd.DataFrame, config: PortfolioConfig
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Accept/reject frozen V13-v5 fills using causal portfolio state."""

    config.validate()
    ledger = prepare_source_ledger(source)
    ledger["portfolio_executed"] = False
    ledger["portfolio_status"] = np.where(
        ledger["filled"], "PENDING_PORTFOLIO_CHECK", "SOURCE_UNFILLED"
    )
    ledger["portfolio_reject_reason"] = np.where(
        ledger["filled"], "", "SOURCE_UNFILLED"
    )
    state_columns = (
        "portfolio_open_positions_before",
        "portfolio_reserved_capital_before_rupees",
        "portfolio_gross_exposure_before_rupees",
        "portfolio_open_risk_before_rupees",
        "portfolio_trade_capital_rupees",
        "portfolio_trade_exposure_rupees",
        "portfolio_trade_initial_risk_rupees",
    )
    for column in state_columns:
        ledger[column] = 0.0

    candidates = ledger.loc[ledger["filled"]].copy()
    candidates["_confirmation_sort"] = candidates.get(
        "confirmation_ts", candidates["entry_ts"]
    ).fillna(candidates["entry_ts"])
    candidates["_setup_sort"] = candidates.get(
        "setup_id", pd.Series("", index=candidates.index)
    ).fillna("").astype(str)
    candidates = candidates.sort_values(
        [
            "entry_ts",
            "_confirmation_sort",
            "_setup_sort",
            "portfolio_priority_value",
            "tradingsymbol",
            "sid",
        ],
        ascending=[True, True, True, False, True, True],
        kind="stable",
    )

    active: list[dict[str, Any]] = []
    peaks = {"positions": 0, "capital": 0.0, "exposure": 0.0, "risk": 0.0}
    reject_counts: dict[str, int] = {}
    for row in candidates.itertuples(index=True):
        entry_ts = row.entry_ts
        active = [position for position in active if position["exit_ts"] > entry_ts]
        open_positions = len(active)
        reserved = float(sum(position["capital"] for position in active))
        gross_exposure = float(sum(position["exposure"] for position in active))
        open_risk = float(sum(position["risk"] for position in active))
        capital, exposure, risk = _trade_amounts(row)
        ledger.loc[row.Index, list(state_columns)] = [
            open_positions,
            reserved,
            gross_exposure,
            open_risk,
            capital,
            exposure,
            risk,
        ]

        symbol_positions = sum(
            position["symbol"] == str(row.tradingsymbol) for position in active
        )
        reason = ""
        if reserved + capital > config.portfolio_capital_rupees + 1e-9:
            reason = "INSUFFICIENT_PORTFOLIO_CAPITAL"
        elif config.max_positions is not None and open_positions >= config.max_positions:
            reason = "MAX_POSITIONS_REACHED"
        elif (
            config.max_positions_per_symbol is not None
            and symbol_positions >= config.max_positions_per_symbol
        ):
            reason = "MAX_SYMBOL_POSITIONS_REACHED"
        elif (
            config.max_gross_exposure_rupees is not None
            and gross_exposure + exposure > config.max_gross_exposure_rupees + 1e-9
        ):
            reason = "MAX_GROSS_EXPOSURE_REACHED"
        elif (
            config.max_open_risk_rupees is not None
            and open_risk + risk > config.max_open_risk_rupees + 1e-9
        ):
            reason = "MAX_OPEN_RISK_REACHED"

        if reason:
            ledger.loc[row.Index, "portfolio_status"] = "REJECTED"
            ledger.loc[row.Index, "portfolio_reject_reason"] = reason
            reject_counts[reason] = reject_counts.get(reason, 0) + 1
            continue

        ledger.loc[row.Index, "portfolio_executed"] = True
        ledger.loc[row.Index, "portfolio_status"] = "EXECUTED"
        active.append(
            {
                "exit_ts": row.exit_ts,
                "symbol": str(row.tradingsymbol),
                "capital": capital,
                "exposure": exposure,
                "risk": risk,
            }
        )
        peaks["positions"] = max(peaks["positions"], len(active))
        peaks["capital"] = max(peaks["capital"], reserved + capital)
        peaks["exposure"] = max(peaks["exposure"], gross_exposure + exposure)
        peaks["risk"] = max(peaks["risk"], open_risk + risk)

    ledger["portfolio_net_profit_rupees"] = np.where(
        ledger["portfolio_executed"], _numeric(ledger, "net_profit_rupees"), 0.0
    )
    ledger["portfolio_gross_profit_rupees"] = np.where(
        ledger["portfolio_executed"], _numeric(ledger, "pre_cost_profit_rupees"), 0.0
    )
    ledger["portfolio_cost_rupees"] = np.where(
        ledger["portfolio_executed"], _numeric(ledger, "cost_rupees"), 0.0
    )
    accepted = ledger.loc[ledger["portfolio_executed"]]
    pnl = _numeric(accepted, "net_profit_rupees")
    gains = float(pnl.loc[pnl > 0].sum())
    losses = float(-pnl.loc[pnl < 0].sum())
    daily = ledger.groupby("day", sort=True)["portfolio_net_profit_rupees"].sum()
    curve = daily.cumsum()
    drawdown = curve - np.maximum.accumulate(np.r_[0.0, curve.to_numpy(float)])[1:]
    summary = {
        "schema_version": SCHEMA_VERSION,
        "source_rows": int(len(ledger)),
        "source_filled_trades": int(ledger["filled"].sum()),
        "portfolio_executed_trades": int(ledger["portfolio_executed"].sum()),
        "portfolio_rejected_trades": int(ledger["portfolio_status"].eq("REJECTED").sum()),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "net_profit_rupees": float(pnl.sum()),
        "profit_factor": gains / losses if losses else (float("inf") if gains else float("nan")),
        "maximum_drawdown_rupees": float(drawdown.min()) if len(drawdown) else 0.0,
        "peak_concurrent_positions": int(peaks["positions"]),
        "peak_reserved_capital_rupees": float(peaks["capital"]),
        "peak_gross_exposure_rupees": float(peaks["exposure"]),
        "peak_open_initial_risk_rupees": float(peaks["risk"]),
        "reject_counts": reject_counts,
        "config": asdict(config),
    }
    return ledger, summary


def daily_frame(ledger: pd.DataFrame) -> pd.DataFrame:
    grouped = ledger.groupby("day", sort=True).agg(
        source_selected=("sid", "size"),
        source_filled=("filled", "sum"),
        portfolio_executed=("portfolio_executed", "sum"),
        portfolio_rejected=("portfolio_status", lambda values: int(pd.Series(values).eq("REJECTED").sum())),
        net_profit_rupees=("portfolio_net_profit_rupees", "sum"),
    )
    out = grouped.reset_index()
    out["cumulative_net_profit_rupees"] = out["net_profit_rupees"].cumsum()
    peak = np.maximum.accumulate(np.r_[0.0, out["cumulative_net_profit_rupees"].to_numpy(float)])[1:]
    out["drawdown_rupees"] = out["cumulative_net_profit_rupees"] - peak
    return out


def _optional_positive(value: int) -> int | None:
    return None if value == 0 else value


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--portfolio-capital-rupees", type=float, required=True)
    parser.add_argument("--max-positions", type=int, default=0, help="0 means capital-only")
    parser.add_argument("--max-positions-per-symbol", type=int, default=0, help="0 means unlimited")
    parser.add_argument("--max-gross-exposure-rupees", type=float)
    parser.add_argument("--max-open-risk-rupees", type=float)
    parser.add_argument("--run-id")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    source_path = args.source.resolve()
    if not source_path.is_file():
        raise FileNotFoundError(f"Missing V13-v5 source ledger: {source_path}")
    config = PortfolioConfig(
        portfolio_capital_rupees=float(args.portfolio_capital_rupees),
        max_positions=_optional_positive(int(args.max_positions)),
        max_positions_per_symbol=_optional_positive(int(args.max_positions_per_symbol)),
        max_gross_exposure_rupees=args.max_gross_exposure_rupees,
        max_open_risk_rupees=args.max_open_risk_rupees,
    )
    source = pd.read_csv(source_path)
    ledger, summary = apply_portfolio_constraints(source, config)
    generated = common.now_ist()
    run_id = args.run_id or generated.strftime("v13_v6_%Y%m%dT%H%M%S_IST")
    if any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for character in run_id):
        raise ValueError("run-id may contain only letters, digits, underscore and hyphen")
    run_dir = args.output_root.resolve() / run_id
    if run_dir.exists():
        raise FileExistsError(f"Refusing to overwrite existing V13-v6 run: {run_dir}")
    run_dir.mkdir(parents=True)
    trades_path = run_dir / "fno_v13_v6_portfolio_trades.csv"
    daily_path = run_dir / "fno_v13_v6_portfolio_daily.csv"
    summary_path = run_dir / "fno_v13_v6_portfolio_summary.json"
    manifest_path = run_dir / "manifest.json"
    common.atomic_write_csv(ledger, trades_path)
    common.atomic_write_csv(daily_frame(ledger), daily_path)
    common.atomic_write_json(summary_path, summary)
    source_days = pd.to_datetime(source.get("day"), errors="coerce")
    data_through_date = (
        source_days.max().date().isoformat() if source_days.notna().any() else None
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "run_id": run_id,
        "generated_at_ist": generated.isoformat(timespec="seconds"),
        "data_through_date": data_through_date,
        "source": str(source_path),
        "source_sha256": _sha256(source_path),
        "config": asdict(config),
        "outputs": {
            "trades": {"path": str(trades_path), "sha256": _sha256(trades_path)},
            "daily": {"path": str(daily_path), "sha256": _sha256(daily_path)},
            "summary": {"path": str(summary_path), "sha256": _sha256(summary_path)},
        },
    }
    common.atomic_write_json(manifest_path, manifest)
    print(json.dumps(summary, indent=2, sort_keys=True, allow_nan=True))
    print(f"[V13-v6 portfolio][RUN] {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
