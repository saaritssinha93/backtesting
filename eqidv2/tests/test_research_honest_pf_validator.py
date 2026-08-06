from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

import research_honest_pf_validator as validator


def _robust_trades() -> tuple[pd.DataFrame, list[pd.Timestamp]]:
    sessions = list(pd.bdate_range("2026-01-01", periods=60))
    rows: list[dict[str, object]] = []
    for day in sessions:
        # Five trades/day: three +100 winners and two -50 losers.  Net PF 3.0,
        # and the edge is evenly spread over both chronological halves.
        for pnl in (100.0, 100.0, 100.0, -50.0, -50.0):
            cost = 10.0
            rows.append(
                {
                    "trade_date": str(day.date()),
                    "net_pnl_rs": pnl,
                    "gross_pnl_rs": pnl + cost,
                    "cost_rs": cost,
                }
            )
    return validator.prepare_trades(pd.DataFrame(rows)), sessions


def test_profit_factor_edges() -> None:
    assert validator.profit_factor([100.0, -50.0, 0.0]) == 2.0
    assert np.isinf(validator.profit_factor([100.0, 0.0]))
    assert validator.profit_factor([0.0, 0.0]) == 0.0


def test_full_gate_can_research_qualify_but_never_production_approve() -> None:
    trades, sessions = _robust_trades()

    result = validator.evaluate(
        trades,
        sessions,
        session_source="unit_test",
        bootstrap_draws=2_000,
    )

    assert result["qualification_pass"] is True
    assert all(result["checks"].values())
    assert result["overall"]["trades"] == 300
    assert result["overall"]["profit_factor"] == 3.0
    assert result["production_approved"] is False
    assert result["promotion_action"] == "NONE_RESEARCH_ONLY"
    assert result["decision"] == "RESEARCH_QUALIFIED"


def test_chronological_half_gate_catches_one_sided_history() -> None:
    trades, sessions = _robust_trades()
    first_days = set(sessions[:30])
    first_mask = trades["_session_date"].isin(first_days)
    trades.loc[first_mask, "net_pnl_rs"] = -10.0
    trades.loc[first_mask, "gross_pnl_rs"] = 0.0

    result = validator.evaluate(
        trades,
        sessions,
        session_source="unit_test",
        bootstrap_draws=500,
    )

    assert result["qualification_pass"] is False
    assert result["checks"]["first_half_net_pnl_positive"] is False
    assert result["checks"]["first_half_net_pf_at_least_1p10"] is False


def test_day_cluster_bootstrap_is_deterministic() -> None:
    trades, sessions = _robust_trades()

    first = validator.day_cluster_bootstrap_pf(trades, sessions, draws=777)
    second = validator.day_cluster_bootstrap_pf(trades, sessions, draws=777)

    assert first == second
    assert first["seed"] == validator.BOOTSTRAP_SEED
    assert first["lower_profit_factor"] == pytest.approx(3.0)


def test_stress_uses_exact_gross_minus_1p25_cost_formula() -> None:
    trades, sessions = _robust_trades()

    result = validator.evaluate(
        trades,
        sessions,
        session_source="unit_test",
        bootstrap_draws=100,
    )

    # Winners become 97.5 and losers -52.5 after the 25% cost stress.
    expected_pf = (60 * 3 * 97.5) / (60 * 2 * 52.5)
    assert result["cost_plus_25pct_stress"]["profit_factor"] == pytest.approx(
        expected_pf
    )
    assert result["cost_plus_25pct_stress"]["formula"] == (
        "gross_pnl_rs - 1.25 * cost_rs"
    )


def test_explicit_audit_sessions_and_report_writes(tmp_path) -> None:
    trades, sessions = _robust_trades()
    audit = tmp_path / "slot_audit.csv"
    pd.DataFrame(
        {
            "date": [str(day.date()) for day in sessions for _ in range(2)],
            "slot": ["09:20", "10:20"] * len(sessions),
        }
    ).to_csv(audit, index=False)

    loaded, source, warnings = validator.load_sessions(
        trades=trades, sessions_path=audit
    )
    result = validator.evaluate(
        trades,
        loaded,
        session_source=source,
        warnings=warnings,
        bootstrap_draws=100,
    )
    json_path = tmp_path / "report.json"
    markdown_path = tmp_path / "report.md"
    validator.write_reports(result, json_path, markdown_path)

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert len(loaded) == 60
    assert payload["production_approved"] is False
    assert "Production approval is always **false**" in markdown_path.read_text(
        encoding="utf-8"
    )


def test_missing_cost_inputs_fail_closed() -> None:
    with pytest.raises(ValueError, match="missing required"):
        validator.prepare_trades(
            pd.DataFrame(
                {
                    "trade_date": ["2026-01-01"],
                    "net_pnl_rs": [100.0],
                }
            )
        )


def test_trade_outside_explicit_sessions_fails_closed(tmp_path) -> None:
    trades, _ = _robust_trades()
    audit = tmp_path / "audit.csv"
    pd.DataFrame({"date": ["2026-01-01"]}).to_csv(audit, index=False)

    with pytest.raises(ValueError, match="outside"):
        validator.load_sessions(trades=trades, sessions_path=audit)


def test_explicit_window_filters_combined_trades_inclusively_before_session_check(
    tmp_path,
) -> None:
    trades, sessions = _robust_trades()
    extra = validator.prepare_trades(
        pd.DataFrame(
            [
                {
                    "trade_date": "2025-12-31",
                    "net_pnl_rs": 1.0,
                    "gross_pnl_rs": 2.0,
                    "cost_rs": 1.0,
                },
                {
                    "trade_date": "2026-04-01",
                    "net_pnl_rs": 1.0,
                    "gross_pnl_rs": 2.0,
                    "cost_rs": 1.0,
                },
            ]
        )
    )
    combined = pd.concat([extra.iloc[[0]], trades, extra.iloc[[1]]], ignore_index=True)
    start = str(sessions[0].date())
    end = str(sessions[-1].date())

    filtered = validator.filter_trades_to_window(combined, start, end)
    audit = tmp_path / "window_sessions.csv"
    pd.DataFrame({"date": [str(day.date()) for day in sessions]}).to_csv(
        audit, index=False
    )
    loaded, _, _ = validator.load_sessions(
        trades=filtered,
        sessions_path=audit,
        start_date=start,
        end_date=end,
    )

    assert len(filtered) == 300
    assert filtered["_session_date"].min() == sessions[0]
    assert filtered["_session_date"].max() == sessions[-1]
    assert loaded == sessions


def test_session_audit_excludes_false_included_rows_and_deduplicates_slots(
    tmp_path,
) -> None:
    trades, sessions = _robust_trades()
    rows: list[dict[str, object]] = []
    for day in sessions:
        rows.extend(
            [
                {"date": str(day.date()), "slot": "09:20", "included": True},
                {"date": str(day.date()), "slot": "10:20", "included": True},
            ]
        )
    rows.append({"date": "2025-10-21", "slot": "09:20", "included": False})
    audit = tmp_path / "session_audit.csv"
    pd.DataFrame(rows).to_csv(audit, index=False)

    loaded, source, _ = validator.load_sessions(trades=trades, sessions_path=audit)

    assert loaded == sessions
    assert len(loaded) == 60
    assert pd.Timestamp("2025-10-21") not in loaded
    assert "inclusion_columns=included" in source


def test_session_audit_rejects_ambiguous_inclusion_values(tmp_path) -> None:
    trades, _ = _robust_trades()
    audit = tmp_path / "ambiguous_audit.csv"
    pd.DataFrame(
        {
            "date": ["2026-01-01", "2026-01-02"],
            "included": ["true", "unknown"],
        }
    ).to_csv(audit, index=False)

    with pytest.raises(ValueError, match="ambiguous"):
        validator.load_sessions(trades=trades, sessions_path=audit)
