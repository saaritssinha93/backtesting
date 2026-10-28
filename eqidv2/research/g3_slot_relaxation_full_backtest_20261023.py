"""Offline full-history slot-relaxation and capital-limit comparison."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from research import g3_early_entry_study_20261023 as study
from research.g3_early_no_capital_limit_20261023 import capital_usage

OUTPUT = study.OUTPUT.parent / "full_history_relaxation"
PROFILES = ("baseline", "targeted_OI", "targeted_volume", "combined_morning", "combined_morning_two_0931")


def normalize_broad_opening(features):
    """Price-only opening eligibility, identical across historical sources.

    The broad experiment treats OI as diagnostic, so missing opening OI must
    not silently suppress a verified cash-price signal in just some sources.
    Baseline/targeted frames retain their original fail-closed OI checks.
    """
    frame = features.copy()
    opening = frame.hhmm_int.eq(920)
    cash_open = pd.to_numeric(frame['open'], errors='coerce')
    cash_close = pd.to_numeric(frame['close'], errors='coerce')
    valid = np.isfinite(cash_open) & np.isfinite(cash_close) & cash_open.gt(0) & cash_close.gt(0)
    frame.loc[opening, 'price_change_pct'] = np.nan
    selected = opening & valid
    frame.loc[selected, 'price_change_pct'] = (cash_close.loc[selected] / cash_open.loc[selected] - 1) * 100
    return frame


def financial_metrics(ledger, day, variant, cap):
    taken = ledger.loc[ledger.portfolio_executed.eq(True)]
    pnl = taken.portfolio_net_profit_rupees
    requested = sum(study.REQUESTED.get(r.tradingsymbol) == r.side for r in taken.itertuples())
    result = dict(day=str(day), variant=variant, capital_limit=cap, orders=len(ledger), trades=len(taken),
        wins=int(pnl.gt(0).sum()), losses=int(pnl.lt(0).sum()), net_pnl_rupees=float(pnl.sum()),
        winning_trade_pnl_rupees=float(pnl.loc[pnl.gt(0)].sum()),
        losing_trade_pnl_rupees=float(pnl.loc[pnl.lt(0)].sum()),
        requested_trades=int(requested) if str(day) == "2026-10-23" else None)
    result.update(capital_usage(ledger))
    return result


def original_policy_ledgers(orders, paths, day):
    if orders.empty:
        empty = study.allocate(orders, 0)
        return {'10_positions': empty.copy(), 'unlimited': empty.copy()}
    limited, _ = study.replay.simulate(orders, paths, [day])
    # The native replay helper labels its output as active G3. Restore the
    # actual research selector identity instead of mislabeling a variant.
    for field in ('strategy_version','strategy_fingerprint'):
        values = orders[field].dropna().unique()
        if len(values) != 1:
            raise ValueError(f'Mixed order identities: {field}')
        limited[field] = values[0]
    limited['execution_authority'] = False
    unlimited = study.execution.simulate_staged(orders, paths, cost_bps=5., max_entry_delay_minutes=10)
    unlimited["portfolio_executed"] = unlimited.filled.eq(True)
    unlimited["portfolio_status"] = np.where(unlimited.portfolio_executed, "EXECUTED", "UNFILLED_EXPIRED")
    unlimited["portfolio_net_profit_rupees"] = np.where(unlimited.portfolio_executed, unlimited.net_return_pct*5000., 0.)
    return {"10_positions": limited, "unlimited": unlimited}


def check_selection(actual, expected):
    keys = ["tradingsymbol", "setup_id", "side"]
    fields = keys + ["trigger", "native_target_pct"]
    left = actual[fields].sort_values(keys).reset_index(drop=True)
    right = expected[fields].sort_values(keys).reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(left, right, check_dtype=False, check_exact=False, rtol=1e-6, atol=1e-6)
    except AssertionError as exc:
        return dict(matches=False, actual_orders=len(left), saved_orders=len(right), error=str(exc)[:2500],
            actual=left.to_dict("records"), expected=right.to_dict("records"))
    return dict(matches=True, actual_orders=len(left), saved_orders=len(right))


def summarize(daily):
    records = []
    for (variant, cap), book in daily.groupby(["variant", "capital_limit"]):
        periods = [("all_available", book), ("through_2026-10-22", book.loc[book.day.lt("2026-10-23")]),
                   ("2026-10-23", book.loc[book.day.eq("2026-10-23")])]
        periods += list(book.groupby(book.day.str[:7]))
        for period, frame in periods:
            if frame.empty:
                continue
            frame = frame.sort_values("day")
            n = int(frame.trades.sum()); w = int(frame.wins.sum())
            gains = float(frame.winning_trade_pnl_rupees.sum()); losses = float(frame.losing_trade_pnl_rupees.sum())
            curve = frame.net_pnl_rupees.cumsum()
            high = curve.cummax().clip(lower=0)
            records.append(dict(variant=variant, capital_limit=cap, period=period, days=len(frame),
                trades=n, wins=w, losses=int(frame.losses.sum()), win_rate_pct=100*w/n if n else 0.,
                net_pnl_rupees=float(frame.net_pnl_rupees.sum()), mean_trade_pnl_rupees=float(frame.net_pnl_rupees.sum()/n) if n else 0.,
                losing_days=int(frame.net_pnl_rupees.lt(0).sum()), worst_day_rupees=float(frame.net_pnl_rupees.min()),
                maximum_closed_day_drawdown_rupees=float((high-curve).max()),
                profit_factor=gains/abs(losses) if losses else None,
                peak_positions=int(frame.peak_positions.max()), peak_margin_rupees=int(frame.peak_margin_rupees.max()),
                requested_trades=int(frame.requested_trades.fillna(0).sum())))
    return pd.DataFrame(records)


def report(output, daily, summary, controls, targets):
    mismatches = [r for r in controls if not r['selection']['matches'] or not r['pnl_matches']]
    page = '<!doctype html><meta charset="utf-8"><title>G3 historical slot relaxation</title><style>body{font:14px system-ui;margin:32px}table{border-collapse:collapse;margin:18px 0}th,td{border:1px solid #ccc;padding:6px;text-align:right}td:first-child,th:first-child{text-align:left}</style><h1>G3 historical slot-relaxation comparison</h1>'
    page += f'<p>Observed sessions: {daily.day.nunique()}, {daily.day.min()} to {daily.day.max()}. All results are retrospective, research only. Live and paper settings were not changed. Saved-baseline control mismatches: {len(mismatches)}.</p>'
    page += '<p>Fixed per-entry exposure Rs 500,000, modeled margin Rs 100,000, round-trip costs 5 basis points. Unlimited capital removes only capital rejection; it does not remove the configured slot quota. Targeted variants retain original G3 targets, stops, ranking and repeat-entry policy. Broad early-entry variants use the separate fixed 1.5% target, 1.25% initial stop, 1% stop after 120 minutes, and one executed trade per symbol/day. These are not equivalent exit policies.</p>'
    page += '<p>Consistent broad-model opening policy: the 09:20 signal uses verified cash open-to-close movement regardless of opening OI availability, on every date. Missing/nonpositive cash prices remain ineligible. This corrects the earlier eight-session study, whose recent source also suppressed opening price signals when OI was unavailable. Baseline and targeted slot changes retain original opening-OI requirements.</p>'
    descriptions = pd.DataFrame([
        ('targeted_OI','09:21 short OI floor 0%; 09:31 short floor -3%; 09:36 short floor +0.05%. Positive finite current/prior OI and +1% ceiling retained.'),
        ('targeted_volume','Confirmation volume ratio: 09:21 short 0.60; 09:31 short 0.90; 09:36 short 0.50. For 09:36 short, 5-minute volume minimum 0.40.'),
        ('combined_morning','Both preceding changes, plus 09:21 short body minimum 0.30 and Nifty opening return ceiling +0.05%.'),
        ('combined_morning_two_0931','Combined changes plus two selections instead of one at 09:31 short. Other quotas and ranking remain unchanged.'),
        ('turn_vol0.4_breakout_qall','Separate price-led early-turn model: every 5-minute slot, OI diagnostic only, 5-minute move at least 0.20%, volume ratio at least 0.40, confirmed breakout, all qualifying orders.'),
        ('turn_vol0.4_breakout_q1','Same separate model, but only one ranked candidate per side per slot.')],columns=['Variant','Changed conditions'])
    page += '<h2>Tested changes</h2>'+descriptions.to_html(index=False)
    page += '<h2>Unlimited-capital summary</h2>'+summary.loc[summary.capital_limit.eq('unlimited') & summary.period.eq('all_available')].round(2).to_html(index=False)
    page += '<h2>Capital-limited controls</h2>'+summary.loc[summary.capital_limit.eq('10_positions') & summary.period.eq('all_available')].round(2).to_html(index=False)
    page += '<h2>Monthly unlimited-capital results</h2>'+summary.loc[summary.capital_limit.eq('unlimited') & summary.period.str.fullmatch(r'2026-\d{2}')].round(2).to_html(index=False)
    page += '<h2>23 October requested-stock executions</h2>'+targets.round(4).to_html(index=False)
    page += '<h2>Daywise unlimited-capital net P&amp;L</h2>'+daily.loc[daily.capital_limit.eq('unlimited')].pivot(index='day', columns='variant', values='net_pnl_rupees').round(2).to_html()
    page += '<h2>Limitations</h2><p>The earlier history uses the same legacy monthly stock-universe mapping as the existing backtest. For some early dates the available constituent file was dated later; this is not a fully point-in-time, survivorship-free universe. Some reconstructed opening inputs may differ from earlier saved sources; any control discrepancy is recorded and must be resolved before claiming exact parity. One-minute OHLC does not establish intrabar trigger order. Modeled margin excludes broker buffers, impact, and additional slippage. No unlimited-capital percentage is divided by the old Rs 1,000,000 account. Variants were motivated by known missed examples; this is not untouched out-of-sample validation.</p>'
    page += '<details><summary>All summary periods</summary>'+summary.round(2).to_html(index=False)+'</details>'
    (output / 'report.html').write_text(page, encoding='utf-8')


def main():
    from research import g3_slot_relaxation_20261023 as slots
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=OUTPUT)
    parser.add_argument('--days', nargs='*')
    parser.add_argument('--allow-baseline-mismatch', action='store_true')
    parser.add_argument('--sealed-only', action='store_true')
    args = parser.parse_args(); args.output.mkdir(parents=True, exist_ok=True)
    study.write_json(args.output / 'profiles.json', {p:slots.profile_configuration(p) for p in PROFILES})
    if args.sealed_only:
        source = study
        evidence = json.loads((study.HISTORY / 'summary.json').read_text())['daily_path_evidence']
        if args.days:
            evidence = [x for x in evidence if Path(x['run_dir']).parent.name in args.days]
        iterator = (study.build_day(x, study.OUTPUT) for x in evidence)
    else:
        from research import g3_early_full_history_source_20261023 as source
        iterator = source.iter_days(days=args.days)
    saved = pd.read_csv(study.HISTORY / 'selected_orders.csv', usecols=['day','tradingsymbol','setup_id','side','trigger','native_target_pct'])
    saved_daily = pd.read_csv(study.HISTORY / 'daily_results.csv').set_index('day')
    all_metrics = []; controls = []; proofs = []; named = []
    for day, features, minute_frame, proof in iterator:
        print(f'[FULL] evaluate {day}: {len(features)} feature rows', flush=True)
        minutes = {s:f.sort_values('ts').copy() for s,f in minute_frame.groupby('tradingsymbol')}
        books = []
        original = study.contract.select_orders(features)
        selection_control = check_selection(original, saved.loc[saved.day.eq(str(day))])
        if not selection_control['matches'] and not args.allow_baseline_mismatch:
            study.write_json(args.output / f'failed_control_{day}.json', selection_control)
            raise ValueError(f'Saved baseline selection mismatch on {day}; no claim of parity')
        for profile in PROFILES:
            orders = slots.select_orders(features, profile)
            if profile == 'baseline':
                assert check_selection(orders, original)['matches'], 'Pure selector must match native contract'
            orders = orders.copy(); orders['sid'] = np.arange(len(orders))
            paths = study.source.selected_paths(orders, minutes, day)
            study.execution.g.v9.validate_paths(orders, paths)
            ledgers = original_policy_ledgers(orders, paths, day)
            for cap, ledger in ledgers.items():
                variant = 'G3_V14_original' if profile == 'baseline' else profile
                metric = financial_metrics(ledger, day, variant, cap)
                if profile == 'baseline' and cap == '10_positions':
                    control = dict(day=str(day), selection=selection_control,
                        actual_pnl=metric['net_pnl_rupees'], saved_pnl=float(saved_daily.loc[str(day),'net_pnl']))
                    control['pnl_matches'] = bool(np.isclose(control['actual_pnl'],control['saved_pnl'],rtol=1e-7,atol=.01))
                    controls.append(control)
                    if not control['pnl_matches'] and not args.allow_baseline_mismatch:
                        study.write_json(args.output / f'failed_control_{day}.json', control)
                        raise ValueError(f'Saved baseline PnL mismatch on {day}')
                ledger['variant'] = variant; ledger['capital_limit'] = cap
                all_metrics.append(metric); books.append(ledger)
        candidates = study.qualify(normalize_broad_opening(features), 'turn', .4)
        paths = study.source.selected_paths(candidates, minutes, day)
        study.execution.g.v9.validate_paths(candidates, paths)
        simulated = study.execution.simulate_staged(candidates, paths, cost_bps=5., max_entry_delay_minutes=10)
        for quota in (0, 1):
            for cap, limit in (('10_positions',10),('unlimited',None)):
                ledger = study.allocate(simulated, quota, max_positions=limit)
                variant = f'turn_vol0.4_breakout_q{quota or "all"}'
                all_metrics.append(financial_metrics(ledger, day, variant, cap))
                ledger['variant'] = variant; ledger['capital_limit'] = cap; books.append(ledger)
        combined = pd.concat(books, ignore_index=True)
        combined.to_parquet(args.output / f'orders_{day}.parquet', index=False)
        if str(day) == '2026-10-23':
            hit = combined.portfolio_executed.eq(True) & combined.tradingsymbol.isin(study.REQUESTED) & combined.capital_limit.eq('unlimited')
            named.append(combined.loc[hit,['variant','tradingsymbol','side','setup_id','entry_ts','entry_price','exit_reason','net_return_pct','portfolio_net_profit_rupees']])
        proofs.append(proof)
        study.write_json(args.output / 'daywise_results.json', all_metrics)
        study.write_json(args.output / 'controls.json', controls)
        print(f'[FULL] completed {day}', flush=True)
    daily = pd.DataFrame(all_metrics).sort_values(['variant','capital_limit','day'])
    summary = summarize(daily)
    targets = pd.concat(named,ignore_index=True) if named else pd.DataFrame()
    study.write_json(args.output / 'summary.json', summary.to_dict('records'))
    study.write_json(args.output / 'requested_executions.json', targets.to_dict('records'))
    study.write_json(args.output / 'provenance.json', proofs)
    study.write_json(args.output / 'config.json', dict(scope='RESEARCH_ONLY',execution_authority=False,
        code_sha256=study.sha(__file__), selector_sha256=study.sha(slots.__file__), source_sha256=study.sha(source.__file__),
        profiles=list(PROFILES), baseline_version=study.contract.STRATEGY_VERSION,
        days=sorted(daily.day.unique()), unchanged_live_and_paper=True,
        execution_limits=[10,None], margin_per_entry=100000,exposure_per_entry=500000,cost_bps=5,
        broad_opening_price_policy='verified_cash_open_to_close_independent_of_OI_on_all_dates'))
    report(args.output,daily,summary,controls,targets)
    print(summary.loc[summary.period.eq('all_available')].to_string(index=False))


if __name__ == '__main__':
    main()
