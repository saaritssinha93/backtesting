"""Capital-only ablation of the sealed eight-session early-entry study.

This is an offline research runner. It has no live execution authority.
"""
from __future__ import annotations

import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from research import g3_early_entry_study_20261023 as study

OUTPUT = study.OUTPUT.parent / "no_capital_limit"


def capital_usage(ledger):
    taken = ledger.loc[ledger.portfolio_executed.eq(True)]
    events = []
    for row in taken.to_dict("records"):
        entry = row.get("entry_open_ts")
        entry = pd.Timestamp(entry) if pd.notna(entry) else pd.Timestamp(row["entry_ts"])
        end = max(pd.Timestamp(row["exit_ts"]), entry + pd.Timedelta(nanoseconds=1))
        events.extend([(entry.value, 1), (end.value, -1)])
    def peak(entries_first):
        count = highest = 0
        for _, delta in sorted(events, key=lambda x: (x[0], -x[1] if entries_first else x[1])):
            count += delta
            highest = max(highest, count)
        return highest
    nominal, conservative = peak(False), peak(True)
    return dict(peak_positions=nominal, same_minute_conservative_peak_positions=conservative,
                peak_margin_rupees=nominal*100000, peak_gross_exposure_rupees=nominal*500000)


def metrics(ledger, day, family, limit):
    r = study.metrics(ledger, family, day)
    r.pop("portfolio_return_pct")  # An unlimited portfolio is not funded by a fixed Rs1m.
    r["capital_limit"] = "10_positions" if limit == 10 else "unlimited"
    r.update(capital_usage(ledger))
    capital_rejected = ledger.portfolio_status.eq("CAPITAL_REJECTED")
    if 'portfolio_reject_reason' in ledger:
        capital_rejected |= ledger.portfolio_reject_reason.eq('INSUFFICIENT_PORTFOLIO_CAPITAL')
    r["capital_rejections"] = int(capital_rejected.sum())
    return r


def run():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    evidence = json.loads((study.HISTORY / "summary.json").read_text())["daily_path_evidence"]
    evidence.sort(key=lambda x: Path(x["run_dir"]).parent.name, reverse=True)
    previous = pd.read_csv(study.OUTPUT / "daywise_comparison.csv")
    result = []; requested = []; provenance = []
    for item in evidence:
        day, features, minute_frame, meta = study.build_day(item, study.OUTPUT)
        minutes = {s: f.sort_values("ts").copy() for s, f in minute_frame.groupby("tradingsymbol")}
        orders = study.qualify(features, "turn", .4)
        paths = study.source.selected_paths(orders, minutes, day)
        study.execution.g.v9.validate_paths(orders, paths)
        simulated = study.execution.simulate_staged(orders, paths, cost_bps=5., max_entry_delay_minutes=10)
        books = []
        for family, quota in (("turn_vol0.4_breakout_qall", 0), ("turn_vol0.4_breakout_q1", 1)):
            for limit in (10, None):
                ledger = study.allocate(simulated, quota, max_positions=limit)
                r = metrics(ledger, day, family, limit)
                if limit == 10:
                    control = previous.loc[previous.day.eq(str(day)) & previous.variant.eq(family)].iloc[0]
                    assert int(control.trades) == r["trades"]
                    assert np.isclose(control.net_pnl_rupees, r["net_pnl_rupees"], rtol=1e-9, atol=1e-6)
                ledger["variant"] = family; ledger["capital_limit"] = r["capital_limit"]
                result.append(r); books.append(ledger)
        baseline = study.contract.select_orders(features)
        paths = study.source.selected_paths(baseline, minutes, day)
        limited, _ = study.replay.simulate(baseline, paths, [day])
        unlimited = study.execution.simulate_staged(baseline, paths, cost_bps=5., max_entry_delay_minutes=10)
        unlimited["portfolio_executed"] = unlimited.filled.eq(True)
        unlimited["portfolio_status"] = np.where(unlimited.portfolio_executed, "EXECUTED", "UNFILLED_EXPIRED")
        unlimited["portfolio_net_profit_rupees"] = np.where(unlimited.portfolio_executed, unlimited.net_return_pct*5000., 0.)
        # Native G3 permits repeated stock entries; removing capital must not change that rule.
        for limit, ledger in ((10, limited), (None, unlimited)):
            r = metrics(ledger, day, "G3_V14_original", limit)
            ledger["variant"] = "G3_V14_original"; ledger["capital_limit"] = r["capital_limit"]
            result.append(r); books.append(ledger)
        all_orders = pd.concat(books, ignore_index=True)
        all_orders.to_parquet(OUTPUT / f"orders_{day}.parquet", index=False)
        if str(day) == "2026-10-23":
            requested.append(all_orders.loc[all_orders.tradingsymbol.isin(study.REQUESTED)])
        provenance.append(meta)
        study.write_json(OUTPUT / "daywise_results.json", result)
        print(f"[NO CAP] {day}: " + json.dumps([{k:r[k] for k in ('variant','trades','net_pnl_rupees','peak_positions','requested_trades')} for r in result[-6:] if r['capital_limit']=='unlimited']), flush=True)
    daily = pd.DataFrame(result).sort_values(["variant", "capital_limit", "day"])
    summary = []
    for (family, limit), frame in daily.groupby(["variant", "capital_limit"]):
        for period, group in (("all_available", frame), ("prior_days", frame.loc[frame.day.lt("2026-10-23")]), ("2026-10-23", frame.loc[frame.day.eq("2026-10-23")])):
            total_trades = int(group.trades.sum()); wins = int(group.wins.sum())
            summary.append(dict(variant=family, capital_limit=limit, period=period, days=len(group),
                trades=total_trades, wins=wins, losses=int(group.losses.sum()),
                win_rate_pct=100*wins/total_trades if total_trades else 0.,
                net_pnl_rupees=float(group.net_pnl_rupees.sum()), losing_days=int(group.net_pnl_rupees.lt(0).sum()),
                worst_day_rupees=float(group.net_pnl_rupees.min()), peak_positions=int(group.peak_positions.max()),
                peak_margin_rupees=float(group.peak_margin_rupees.max()),
                requested_trades=int(group.requested_trades.fillna(0).sum())))
    summary = pd.DataFrame(summary)
    targets = pd.concat(requested, ignore_index=True)
    selected = targets.loc[targets.variant.eq('turn_vol0.4_breakout_qall') & targets.capital_limit.eq('unlimited') & targets.portfolio_executed.eq(True)].copy()
    selected = selected[['tradingsymbol','side','entry_ts','entry_price','exit_ts','exit_reason','net_return_pct','portfolio_net_profit_rupees']]
    study.write_json(OUTPUT / "summary.json", summary.to_dict("records"))
    study.write_json(OUTPUT / "requested_executions.json", selected.to_dict("records"))
    study.write_json(OUTPUT / "provenance.json", provenance)
    study.write_json(OUTPUT / "config.json", dict(scope="RESEARCH_ONLY", execution_authority=False,
        code_sha256=study.sha(__file__), allocator_sha256=study.sha(study.__file__),
        source_study=str(study.OUTPUT), changed_parameter="MAX_CONCURRENT_POSITIONS: 10 versus unlimited",
        unchanged="Rules, quota, ranking, per-trade size, one executed symbol/day for experimental rules, target/SL and costs",
        margin_per_trade_rupees=100000, exposure_per_trade_rupees=500000, cost_bps=5,
        evidence_days=sorted(daily.day.unique()),
        caveats=["Only eight sealed sessions, not the entire 57-session baseline history",
            "Historical hypothetical fills; no extra slippage, latency, impact, or quantity rounding",
            "Breakout timestamps are candle ends; same-minute execution sequence is unknown",
            "No return percentage on the old Rs1m portfolio denominator for unlimited-capital results"]))
    page = '<!doctype html><meta charset="utf-8"><title>G3 early entries without capital limit</title><style>body{font:14px system-ui;margin:32px}table{border-collapse:collapse;margin-bottom:24px}td,th{border:1px solid #ccc;padding:6px;text-align:right}th:first-child,td:first-child{text-align:left}</style><h1>G3 early entries: capital-limit comparison</h1>'
    page += '<p>Eight sealed sessions: October 13, 14, 15, 16, 19, 21, 22 and 23, 2026. This is not the entire 57-session historical baseline. Only the concurrent-position/capital limit changes. Fixed exposure is Rs 500,000 per trade, requiring modeled margin Rs 100,000. Costs are 5 basis points per trade. Experimental rules keep one executed entry per stock/day; original G3 retains its repeat-entry policy. Slot quotas remain unchanged. No live or paper configuration changed.</p>'
    page += '<h2>Summary</h2>'+summary.round(2).to_html(index=False)
    page += '<h2>Requested names, broad early-entry rule, unlimited capital</h2>'+selected.round(4).to_html(index=False)
    page += '<h2>Daywise</h2>'+daily.round(2).to_html(index=False)
    page += '<p>Peak capital is simultaneous open-position margin, not cumulative trade turnover. It excludes cash buffers and broker-specific margin constraints. Minute OHLC cannot prove intra-minute fill order. Equal-timestamp exits release capital before new fills; the conservative peak-position column reverses those ties. Unlimited capital removes allocation rejection, not losing trades or execution uncertainty. No percentage return is computed against the old Rs 1,000,000 account.</p>'
    (OUTPUT / "report.html").write_text(page, encoding="utf-8")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    run()
