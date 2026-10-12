"""Read-only, standard-library data adapter for Dashboard Flow.

Only retained V13-V10-G, G-2 and G-3 backtest CSV files are eligible. Public
run IDs resolve through discovery; callers cannot supply a filesystem path.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import defaultdict
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any
from fno_strategy_registry import active_profile_name, g3_live_activation_authorized, PROFILES
from fno_v13_v10_g_3_contract import confirmation_volume_minimum, setup_for, supported_identity


DATA_ROOT = Path(r"C:\TradingData\eqidv2")
_SOURCES = (
    ("g", "V13-V10-G", "V13-V10-G backtesting", "backtesting_result_v13_v10_g", "runs/*/*/daily_results.csv"),
    ("g-full", "V13-V10-G", "V13-V10-G full backtest", "fno_oi/strategy_research/v13_v10_g_full_history", "run_*/g_backtest/portfolio_trades.csv"),
    ("g2", "V13-V10-G-2", "V13-V10-G-2 backtesting", "fno_oi/strategy_research/v13_corrected_v10_g_2", "run_*/daily_results.csv"),
    ("g3", "V13-V10-G-3", "V13-V10-G-3 backtesting", "backtesting_result_v13_v10_g_3", "runs/*/*/daily_results.csv"),
)
_G3_RESEARCH_ROOT = "fno_oi/strategy_research/v13_v10_g_3_0950_long_backtesting"
_G3_RESEARCH_TARGETS = {
    "V13-V10-G-3-BT-0950-LONG-V1": 1.0,
    "V13-V10-G-3-BT-0950-LONG-T075-V1": 0.75,
}
_STRATEGIES = {"V13-V10-G-3", "V13-V10-G-2", "V13-V10-G"}


def _uses_current_g3_0950_rules(config: dict) -> bool:
    """Say rules were promoted only when this saved configuration still matches."""
    spec = config.get("new_setup")
    setup = setup_for("09:50", "LONG")
    if not isinstance(spec, dict) or setup is None:
        return False
    fields = {
        "signal_end": setup.signal_end, "confirmation_end": setup.confirmation_end,
        "side": setup.side, "mode": setup.mode, "max_entries": setup.max_entries,
        "picker": setup.picker, "price_change_pct": setup.price_change_pct,
        "oi_change_pct": setup.oi_change_pct, "five_minute_volume_ratio_min": setup.volume_ratio,
        "one_minute_body_ratio_min": setup.body_ratio,
        "one_minute_upper_wick_ratio_max": setup.max_wick_ratio,
        "one_minute_volume_ratio_min": confirmation_volume_minimum("LONG", setup_id=setup.setup_id),
        "initial_stop_pct": setup.stop_pct, "target_pct": setup.target_pct,
        "require_green_candle": True, "require_close_above_signal_close": True,
    }
    return all(spec.get(name) == value for name, value in fields.items())


@lru_cache(maxsize=128)
def _artifact_hash(path: str, size: int, modified_ns: int) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _checked_hash(path: Path, expected: str) -> bool:
    if not expected or not path.is_file():
        return False
    stat = path.stat()
    return _artifact_hash(str(path.resolve()), stat.st_size, stat.st_mtime_ns) == expected


def _verified_g3_runs() -> list[tuple[dict, Path]]:
    """Discover only promoted, hash-verified production/history packages."""
    root = DATA_ROOT / "backtesting_result_v13_v10_g_3"
    found = []
    pointers = []
    for filename, role in (("current_history.json", "cumulative_history"), ("current_run.json", "production_daily")):
        pointers.append((root / "latest" / filename, role, None))
        pointers.extend((path, role, path.parent.name)
                        for path in sorted((root / "latest" / "archived").glob("*/" + filename)))
    seen = set()
    for pointer_path, role, archived_version in pointers:
        try:
            if not _inside(pointer_path, DATA_ROOT) or not pointer_path.is_file():
                continue
            pointer = json.loads(pointer_path.read_text(encoding="utf-8-sig"))
            run = root / pointer["run_dir"]
            if not _inside(run, root) or Path(pointer["run_dir"]).is_absolute():
                continue
            manifest_path = run / "run_manifest.json"
            if not _checked_hash(manifest_path, pointer["manifest_sha256"]):
                continue
            manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
            expected_tree = "history" if role == "cumulative_history" else "production"
            if (manifest.get("schema_version") != "fno_g3_production_run_v1"
                    or manifest.get("execution_authority") is not False
                    or pointer.get("session_date") != manifest.get("session_date")
                    or run.relative_to(root).parts[:2] != (expected_tree, "runs")):
                continue
            version = manifest.get("strategy_version")
            fingerprint = manifest.get("strategy_fingerprint")
            if (not supported_identity(version, fingerprint, allow_legacy=True)
                    or (archived_version is not None and archived_version != version)
                    or manifest.get("complete") is not True
                    or manifest.get("state", manifest.get("status")) not in {"SUCCESS", "SUCCESS_WITH_EXCLUDED_SESSIONS"}
                    or manifest.get("role") != role or pointer.get("role") != role
                    or any(record.get("strategy_version") != version
                           or record.get("strategy_fingerprint") != fingerprint
                           for record in (pointer, manifest))):
                continue
            coverage = manifest.get("coverage") or {}
            if manifest.get("state") == "SUCCESS_WITH_EXCLUDED_SESSIONS" and not coverage.get("excluded_sessions"):
                continue
            artifacts = manifest["artifacts"]
            paths = {}
            for key in ("daily_results", "portfolio_trades"):
                record = artifacts[key]
                path = run / record["path"]
                if Path(record["path"]).is_absolute() or not _inside(path, run) or not _checked_hash(path, record["sha256"]):
                    raise ValueError("Invalid promoted artifact")
                paths[key] = path
            days = sorted({_day(_text(row, "day", "date")) for row in _rows(paths["daily_results"], [])} - {""})
            if not days:
                continue
            identity = run.relative_to(root).as_posix()
            if identity in seen:
                continue
            seen.add(identity)
            current_strategy = supported_identity(version, fingerprint)
            source_label = "G-3 verified cumulative backtest" if role == "cumulative_history" else "G-3 production daily backtest"
            if not current_strategy:
                source_label = f"Archived {version} verified result"
            found.append(({
                "id": "g3p-" + hashlib.sha256(identity.encode()).hexdigest()[:16],
                "label": "V13-V10-G-3 · " + run.name,
                "strategy": "V13-V10-G-3", "run_name": run.name,
                "source_label": source_label,
                "kind": "production_history" if role == "cumulative_history" else "production_daily",
                "role": role, "verified": True,
                "strategy_matches_active": current_strategy,
                "strategy_version": version, "strategy_fingerprint": fingerprint,
                "period_start": days[0], "period_end": days[-1], "sessions": len(days),
                "coverage": coverage,
                "_run_dir": str(run),
                "_ledger": str(paths["portfolio_trades"].relative_to(run)),
            }, paths["daily_results"]))
        except (OSError, ValueError, TypeError, KeyError, ImportError, AttributeError):
            # An invalid current pointer never inherits the appearance of success
            # from another version or an unverified directory beside it.
            continue
    return found


def _inside(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except (OSError, ValueError):
        return False


def _number(value: Any) -> float | None:
    try:
        result = float(str(value).strip().replace(",", ""))
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _numeric(row: dict, *keys: str) -> float | None:
    for key in keys:
        if row.get(key) not in (None, ""):
            return _number(row[key])
    return None


def _text(row: dict, *keys: str) -> str:
    for key in keys:
        if row.get(key) not in (None, ""):
            return str(row[key]).strip()
    return ""


def _day(value: str) -> str:
    try:
        return date.fromisoformat(value[:10]).isoformat()
    except (TypeError, ValueError):
        return ""


def _sum(values) -> float | None:
    values = list(values)
    if any(value is None for value in values):
        return None
    try:
        result = math.fsum(values)
    except (OverflowError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _rows(path: Path, warnings: list[str]) -> list[dict]:
    if not path.is_file() or not _inside(path, DATA_ROOT):
        return []
    try:
        with path.open(newline="", encoding="utf-8-sig") as stream:
            return list(csv.DictReader(stream))
    except (OSError, UnicodeError, csv.Error):
        warnings.append(f"The saved {path.name} could not be read.")
        return []


def _full_sessions(run: Path, warnings: list[str]) -> list[str]:
    """Use the saved eligible sessions, including sessions with no executions."""
    metadata_path = run / "run_metadata.json"
    sessions_path = run.parent / "dataset" / "source_session_eligibility.csv"
    if not all(path.is_file() and _inside(path, DATA_ROOT) for path in (metadata_path, sessions_path)):
        return []
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8-sig"))
        first, last = _day(metadata.get("first_session", "")), _day(metadata.get("last_session", ""))
        count = _number(metadata.get("session_count"))
    except (OSError, UnicodeError, ValueError, AttributeError):
        return []
    if not first or not last or first > last or not count:
        return []
    sessions = sorted({
        day for row in _rows(sessions_path, warnings)
        if _text(row, "eligible").lower() in {"true", "1", "1.0", "yes"}
        and (day := _day(_text(row, "day"))) and first <= day <= last
    })
    # Incomplete exports cannot turn missing sessions into zero-trade results.
    return sessions if len(sessions) == count and sessions[0] == first and sessions[-1] == last else []


def _verified_g3_0950_runs(root: Path, manifests, *, current: bool = False) -> list[tuple[dict, Path]]:
    """Verify complete 09:50 historical runs without changing the live book."""
    found = []
    if not _inside(root, DATA_ROOT):
        return found
    for manifest_path in manifests:
        run = manifest_path.parent
        try:
            if not _inside(run, root):
                continue
            manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
            files = manifest["files"]
            if (manifest.get("variant_id") not in _G3_RESEARCH_TARGETS
                    or (current and manifest["variant_id"] != "V13-V10-G-3-BT-0950-LONG-T075-V1")
                    or manifest.get("execution_authority") is not False
                    or not {"summary.json", "daily_results.csv", "variant_trades.csv", "config.json"}.issubset(files)
                    or any(Path(name).name != name or not _inside(run / name, run)
                           or not _checked_hash(run / name, digest) for name, digest in files.items())):
                continue
            summary = json.loads((run / "summary.json").read_text(encoding="utf-8-sig"))
            config = json.loads((run / "config.json").read_text(encoding="utf-8-sig"))
            if (summary.get("variant_id") != manifest["variant_id"]
                    or summary.get("scope") != "BACKTEST_ONLY"
                    or summary.get("execution_authority") is not False
                    or config.get("scope") != "BACKTEST_ONLY"
                    or ("variant_id" in config and config["variant_id"] != manifest["variant_id"])
                    or ("new_setup" in config and
                        config["new_setup"].get("target_pct") != _G3_RESEARCH_TARGETS[manifest["variant_id"]])):
                continue
            if current and (summary.get("frozen_baseline_parity") is not True
                    or summary.get("existing_g3_trades_unchanged") is not True
                    or summary.get("sessions") != 49
                    or config != json.loads((Path(__file__).resolve().parent / "configs/strategies/v13_v10_g_3_backtest_0950_long_target075_v1.json").read_text(encoding="utf-8"))
                    or not _checked_hash(Path(manifest["code"]), manifest.get("code_sha256"))):
                continue
            rows = _rows(run / "daily_results.csv", [])
            days = [_day(_text(row, "day")) for row in rows]
            variant = summary["with_0950_long"]
            if (not days or "" in days or len(set(days)) != len(days)
                    or days != sorted(days) or len(days) != summary.get("sessions")
                    or days[0] != summary.get("first_date") or days[-1] != summary.get("last_date")
                    or _sum(_numeric(row, "variant_fills") for row in rows) != variant["fills"]
                    or _sum(_numeric(row, "variant_wins") for row in rows) != variant["wins"]
                    or not math.isclose(_sum(_numeric(row, "variant_net_pnl_rs") for row in rows),
                                        variant["net_pnl_rs"], abs_tol=0.02)):
                continue
            identity = run.relative_to(root).as_posix()
            promoted_rules = _uses_current_g3_0950_rules(config)
            found.append(({
                "id": ("g3b-" if current else "g3r-") + hashlib.sha256(identity.encode()).hexdigest()[:16],
                "label": "V13-V10-G-3 · " + run.name,
                "strategy": "V13-V10-G-3", "run_name": run.name,
                "source_label": "G-3 09:50 LONG research backtest · no execution authority",
                "kind": "research_variant", "role": "backtest_history" if current else "backtest_only",
                "integrity_verified": True, "verified": current,
                "promoted_rules": promoted_rules,
                "target_pct": _G3_RESEARCH_TARGETS[manifest["variant_id"]],
                "period_start": days[0], "period_end": days[-1], "sessions": len(days),
                "created_at_ist": summary.get("created_at_ist"),
                "_run_dir": str(run), "_ledger": "variant_trades.csv",
            }, run / "daily_results.csv"))
            if current:
                found[-1][0]["source_label"] = (
                    "G-3 current 09:50 LONG historical backtest using production V2 rules"
                    if promoted_rules else "G-3 current 09:50 LONG historical backtest; no execution authority")
        except (OSError, ValueError, TypeError, KeyError, AttributeError, OverflowError):
            continue
    return found


def _verified_g3_research_runs() -> list[tuple[dict, Path]]:
    root = DATA_ROOT / _G3_RESEARCH_ROOT
    return _verified_g3_0950_runs(root, root.glob("run_*/manifest.json"))


def _verified_g3_current_backtest() -> list[tuple[dict, Path]]:
    root = DATA_ROOT / "backtesting_result_v13_v10_g_3"
    pointer_path = root / "latest" / "current_backtest.json"
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8-sig"))
        relative = Path(pointer["run_dir"])
        if (pointer.get("schema_version") != "fno_g3_current_backtest_v1"
                or pointer.get("role") != "backtest_history"
                or pointer.get("execution_authority") is not False
                or relative.is_absolute() or relative.parts[:2] != ("backtests", "runs")
                or len(relative.parts) != 4 or relative.parts[2] != pointer.get("session_date")):
            return []
        run = root / relative
        manifest_path = run / "manifest.json"
        if not _inside(run, root) or not _checked_hash(manifest_path, pointer["manifest_sha256"]):
            return []
        found = _verified_g3_0950_runs(root, [manifest_path], current=True)
        if (len(found) != 1 or found[0][0]["period_end"] != pointer["session_date"]
                or pointer.get("variant_id") != "V13-V10-G-3-BT-0950-LONG-T075-V1"):
            return []
        return found
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return []


def _discover() -> list[tuple[dict, Path]]:
    found = _verified_g3_runs() + _verified_g3_current_backtest() + _verified_g3_research_runs()
    for prefix, strategy, source_label, relative, pattern in _SOURCES:
        root = DATA_ROOT / relative
        if not _inside(root, DATA_ROOT):
            continue
        try:
            paths = sorted(root.glob(pattern), reverse=True)
        except OSError:
            continue
        for daily_path in paths:
            if not daily_path.is_file() or not _inside(daily_path, root):
                continue
            full_history = prefix == "g-full"
            sessions = _full_sessions(daily_path.parent, []) if full_history else sorted({
                day for row in _rows(daily_path, [])
                if (day := _day(_text(row, "day", "date")))
            })
            if full_history and not sessions:
                continue
            run = daily_path.parent.parent if full_history else daily_path.parent
            identity = run.relative_to(root).as_posix()
            run_id = prefix + "-" + hashlib.sha256(identity.encode()).hexdigest()[:16]
            meta = {
                "id": run_id,
                "label": f"{strategy} · {run.name}",
                "strategy": strategy,
                "run_name": run.name,
                "source_label": source_label,
                "kind": "full_history" if full_history else "daily" if prefix == "g" else "backtest",
                "period_start": sessions[0] if sessions else None,
                "period_end": sessions[-1] if sessions else None,
                "sessions": len(sessions),
            }
            found.append((meta, daily_path))
    # Preserve authoritative history before daily or research variants, then
    # choose the latest covered session within that source category.
    family_order = {"V13-V10-G-3": 3, "V13-V10-G-2": 2, "V13-V10-G": 1}
    found.sort(key=lambda entry: (family_order[entry[0]["strategy"]],
        4 if entry[0].get("role") in {"backtest_history", "cumulative_history"} else
        3 if entry[0].get("role") == "production_daily" else
        2 if entry[0]["kind"] == "full_history" else
        1 if entry[0]["kind"] == "research_variant" else 0,
        entry[0]["period_end"] or "",
        1 if entry[0].get("role") == "backtest_history" else 0,
        entry[0].get("created_at_ist") or entry[0]["run_name"]), reverse=True)
    return found


def _load_trades(run: Path, warnings: list[str], ledger: str | None = None) -> list[dict]:
    path = run / ledger if ledger else next((run / name for name in ("trades_with_pullbacks.csv", "portfolio_trades.csv") if (run / name).is_file()), None)
    if path is None or not _inside(path, run):
        return []
    trades = []
    for index, row in enumerate(_rows(path, warnings)):
        # Portfolio sheets include rejected/unfilled orders; those are not trades.
        flag = _text(row, "portfolio_executed", "filled")
        if flag and flag.lower() not in {"true", "1", "1.0", "yes"}:
            continue
        if _text(row, "exit_reason").upper() == "UNFILLED":
            continue
        day = _day(_text(row, "day", "date", "entry_time_ist", "entry_ts"))
        if not day:
            continue
        trade_id = _text(row, "trade_id", "signal_id", "sid") or str(index + 1)
        setup = _text(row, "setup_id", "setup")
        if not setup and "|" in trade_id:
            setup = trade_id.split("|")[1]
        trades.append({
            "id": trade_id,
            "date": day,
            "symbol": _text(row, "tradingsymbol", "ticker", "symbol"),
            "side": _text(row, "side").upper(),
            "setup": setup,
            "entry_time": _text(row, "entry_time_ist", "entry_ts", "entry_time"),
            "exit_time": _text(row, "exit_time_ist", "exit_ts", "exit_time"),
            "entry_price": _numeric(row, "entry_price", "filled_price"),
            "exit_price": _numeric(row, "exit_price"),
            "net_pnl": _numeric(row, "portfolio_net_profit_rupees", "net_pnl", "net_profit_rupees", "net_pnl_rupees"),
            "gross_pnl": _numeric(row, "portfolio_gross_profit_rupees", "gross_pnl", "gross_profit_rupees", "pre_cost_profit_rupees", "gross_pnl_rupees"),
            "cost": _numeric(row, "portfolio_cost_rupees", "cost", "cost_rupees"),
            "exit_reason": _text(row, "exit_reason", "trade_exit_outcome", "outcome"),
        })
    return sorted(trades, key=lambda row: (row["date"], row["entry_time"], row["id"]))


def _load_daily(path: Path, trades: list[dict], warnings: list[str]) -> list[dict]:
    return _normalize_daily(_rows(path, warnings), trades, warnings)


def _load_full_daily(run: Path, trades: list[dict], warnings: list[str]) -> list[dict]:
    by_day = defaultdict(list)
    for trade in trades:
        by_day[trade["date"]].append(trade)
    rows = []
    for day in _full_sessions(run, warnings):
        executions = by_day[day]
        row = {"day": day, "trades": len(executions)}
        for field in ("net_pnl", "gross_pnl", "cost"):
            row[field] = _sum(trade[field] for trade in executions)
        for field, sign in (("wins", 1), ("losses", -1)):
            row[field] = sum(trade["net_pnl"] * sign > 0 for trade in executions) if all(trade["net_pnl"] is not None for trade in executions) else None
        rows.append(row)
    return _normalize_daily(rows, trades, warnings)


def _normalize_daily(rows: list[dict], trades: list[dict], warnings: list[str]) -> list[dict]:
    grouped = defaultdict(list)
    for row in rows:
        day = _day(_text(row, "day", "date", "session_date"))
        if not day:
            warnings.append("A daily row with an invalid date was excluded.")
            continue
        grouped[day].append(row)
    by_day = defaultdict(list)
    for trade in trades:
        by_day[trade["date"]].append(trade)
    daily = []
    cumulative, peak = 0.0, 0.0
    for day, rows in sorted(grouped.items()):
        item = {"date": day}
        for field, aliases in {
            "trades": ("trades", "fills"),
            "wins": ("wins",),
            "losses": ("losses",),
            "net_pnl": ("net_pnl", "net_profit_rupees", "net_pnl_rupees"),
            "gross_pnl": ("gross_pnl", "gross_profit_rupees", "gross_pnl_rupees", "pre_cost_profit_rupees"),
            "cost": ("cost", "cost_rupees"),
        }.items():
            item[field] = _sum(_numeric(row, *aliases) for row in rows)
        day_trades = by_day[day]
        if item["trades"] is None and day_trades:
            item["trades"] = len(day_trades)
        complete = item["trades"] == len(day_trades)
        if complete:
            for field in ("gross_pnl", "cost"):
                if item[field] is None:
                    item[field] = _sum(trade[field] for trade in day_trades)
            if all(trade["net_pnl"] is not None for trade in day_trades):
                for field, sign in (("wins", 1), ("losses", -1)):
                    if item[field] is None:
                        item[field] = sum(trade["net_pnl"] * sign > 0 for trade in day_trades)
        if item["net_pnl"] is None or cumulative is None:
            cumulative = None
            item["drawdown"] = None
        else:
            cumulative = _sum((cumulative, item["net_pnl"]))
            if cumulative is not None:
                peak = max(peak, cumulative)
                item["drawdown"] = _number(cumulative - peak)
            else:
                item["drawdown"] = None
        item["cumulative_net_pnl"] = cumulative
        daily.append(item)
    return daily


def _summary(daily: list[dict], trades: list[dict], warnings: list[str]) -> dict:
    result = {field: _sum(row[field] for row in daily) for field in ("trades", "wins", "losses", "net_pnl", "gross_pnl", "cost")}
    count = result["trades"]
    known_outcomes = count is not None and result["wins"] is not None and result["losses"] is not None
    result["breakeven"] = max(0, count - result["wins"] - result["losses"]) if known_outcomes else None
    result["win_rate_pct"] = _number(result["wins"] / count * 100) if count and result["wins"] is not None else None
    result["average_trade"] = _number(result["net_pnl"] / count) if count and result["net_pnl"] is not None else None
    net_values = [row["net_pnl"] for row in daily]
    valid_days = [value for value in net_values if value is not None]
    all_days_known = len(valid_days) == len(daily)
    result.update({
        "sessions": len(daily),
        "period_start": daily[0]["date"] if daily else None,
        "period_end": daily[-1]["date"] if daily else None,
        "best_day": max(valid_days) if valid_days and all_days_known else None,
        "worst_day": min(valid_days) if valid_days and all_days_known else None,
        "winning_days": sum(value > 0 for value in valid_days) if all_days_known else None,
        "losing_days": sum(value < 0 for value in valid_days) if all_days_known else None,
        "flat_days": sum(value == 0 for value in valid_days) if all_days_known else None,
        "max_drawdown": abs(min((row["drawdown"] for row in daily), default=0)) if all_days_known and all(row["drawdown"] is not None for row in daily) else None,
        "profit_factor": None,
        "profit_factor_basis": None,
        "long_trades": None,
        "short_trades": None,
    })
    trade_total = _sum(trade["net_pnl"] for trade in trades)
    complete = count == len(trades) and trade_total is not None and result["net_pnl"] is not None and math.isclose(trade_total, result["net_pnl"], abs_tol=0.02, rel_tol=1e-9)
    if complete:
        result["long_trades"] = sum(trade["side"] in {"LONG", "BUY"} for trade in trades)
        result["short_trades"] = sum(trade["side"] in {"SHORT", "SELL"} for trade in trades)
        if trades:
            gains = _sum(max(trade["net_pnl"], 0) for trade in trades)
            losses = _sum(-min(trade["net_pnl"], 0) for trade in trades)
            ratio = gains / losses if gains is not None and losses else None
            result["profit_factor"] = _number(ratio)
            result["profit_factor_basis"] = "net trade P&L"
            if not losses:
                warnings.append("Profit factor is undefined because the saved trades contain no net losses.")
    elif count:
        warnings.append("The trade ledger does not fully reconcile to the daily results; trade profit factor is unavailable.")
    if not all_days_known:
        warnings.append("Some daily P&L values are missing or non-finite; affected totals and cumulative metrics are unavailable.")
    return result


def load_flow_data(run_id: str | None = None, strategy: str | None = None) -> dict:
    """Return normalized historical evidence; reject IDs absent from discovery."""
    if strategy is not None and (strategy not in _STRATEGIES or run_id is not None):
        raise ValueError("Choose one known strategy or one saved run.")
    discovered = _discover()
    runs = [{k: v for k, v in metadata.items() if not k.startswith("_")} for metadata, _ in discovered]
    selected = next((entry for entry in discovered if entry[0]["id"] == run_id), None) if run_id else next((entry for entry in discovered if strategy is None or entry[0]["strategy"] == strategy), None)
    if run_id and selected is None:
        raise ValueError("Unknown dashboard run.")
    warnings: list[str] = []
    if not run_id and (strategy is None or strategy == "V13-V10-G-3") and active_profile_name() == "V13_V10_G_3":
        for name, label in (("status.json", "Daily backtest"), ("history_status.json", "History rebuild")):
            status_path = DATA_ROOT / "backtesting_result_v13_v10_g_3" / name
            try:
                attempt = json.loads(status_path.read_text(encoding="utf-8-sig"))
                state = str(attempt.get("state", attempt.get("status", "")))
                if state and state not in {"SUCCESS", "SUCCESS_WITH_EXCLUDED_SESSIONS"}:
                    warnings.append(f"{label} attempt for {attempt.get('session_date', 'the requested period')}: {state}. Only the last verified complete package can be displayed.")
            except (OSError, ValueError, AttributeError):
                pass
    if not run_id and (strategy is None or strategy == "V13-V10-G-3") and active_profile_name() == "V13_V10_G_3" and (selected is None or not selected[0].get("verified")):
        selected = None
        warnings.append("Active G-3 has no verified complete backtest. Historical runs remain available from the run selector.")
    if selected:
        metadata, path = selected
        if metadata.get("strategy_matches_active") is False:
            warnings.append(f"This saved result uses {metadata['strategy_version']}. Current paper/live decisions use {PROFILES['V13_V10_G_3'].version}; the saved history has not been relabeled or recomputed.")
        trades = _load_trades(Path(metadata.get("_run_dir", path.parent)), warnings, metadata.get("_ledger"))
        excluded_sessions = metadata.get("coverage", {}).get("excluded_sessions", [])
        if excluded_sessions:
            warnings.append(f"{len(excluded_sessions)} source sessions were excluded from this backtest. See the coverage report for dates and reasons; these are not zero-trade days.")
        if metadata["kind"] == "full_history":
            daily = _load_full_daily(path.parent, trades, warnings)
        elif metadata["kind"] == "research_variant":
            rows = [{"day": row.get("day"), "trades": row.get("variant_fills"),
                     "wins": row.get("variant_wins"),
                     "losses": _number(row.get("variant_fills")) - _number(row.get("variant_wins")),
                     "net_pnl": row.get("variant_net_pnl_rs")}
                    for row in _rows(path, warnings)]
            daily = _normalize_daily(rows, trades, warnings)
            if metadata.get("promoted_rules") and active_profile_name() == "V13_V10_G_3":
                live_state = ("LIVE execution authorization is active."
                              if g3_live_activation_authorized() else
                              "LIVE order execution is currently disabled.")
                warnings.append("This saved historical simulation uses the 09:50 LONG / 0.75% target rules configured in G-3 V2 for PAPER, daily backtesting, and the LIVE rule book. Its modeled fills are not PAPER or broker orders; the saved run cannot place orders. " + live_state)
            else:
                warnings.append("This saved G-3 research backtest has no order execution authority. Its results are preserved for review.")
        else:
            daily = _load_daily(path, trades, warnings)
        # Keep the ledger in the same date scope as the daily summary.
        dates = {row["date"] for row in daily}
        excluded = sum(trade["date"] not in dates for trade in trades)
        if excluded:
            warnings.append("Trades outside the saved daily result dates were excluded.")
        trades = [trade for trade in trades if trade["date"] in dates]
    else:
        metadata, trades, daily = None, [], []
    if not daily:
        warnings.append("No saved daily backtest results are available for this selection.")
    summary = _summary(daily, trades, warnings)
    return {
        "state": "ready" if daily else "empty",
        "runs": runs,
        "selected_run": {k: v for k, v in metadata.items() if not k.startswith("_")} if metadata else None,
        "daily": daily,
        "trades": trades,
        "summary": summary,
        "warnings": list(dict.fromkeys(warnings)),
        "currency": "INR",
        "data_mode": "Verified G-3 backtest" if metadata and metadata.get("verified") else "Historical backtest",
    }
