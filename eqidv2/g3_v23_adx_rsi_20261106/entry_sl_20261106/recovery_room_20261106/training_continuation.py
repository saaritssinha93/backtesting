"""Training-only continuation diagnostics after native stops, never trade rules.

The native exit bar's remainder has unknown chronology and is excluded.
Later target touches and adverse excursions are descriptive; they do not
establish that any wider stop could have retained a trade profitably.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from g3_v23_adx_rsi_20261106.loss_control_20261106 import inputs
from g3_v23_adx_rsi_20261106 import run as original

PACKAGE = Path(__file__).resolve().parent
OUTPUT = PACKAGE / 'training_continuation'
MINUTE_NS = 60_000_000_000


def analyze(data):
    """Inspect only first44 native stopped trades; do not inspect later labels."""
    days = data['days'][:44]
    if len(days) != 44 or days[-1] != '2026-10-05':
        raise ValueError('Training session inventory changed')
    training = data['trades'].loc[data['trades'].day.isin(days)].copy()
    stopped = training.loc[training.exit_reason.isin(['STOP', 'TIGHTENED_STOP'])]
    meta = data['parity'].set_index('sid')
    rows = []
    for trade in stopped.itertuples(index=False):
        source = meta.loc[trade.sid]
        path = data['paths'][int(trade.sid)]
        j = int(source.exit_index)
        entry = float(trade.entry_price)
        sign = 1 if trade.side == 'LONG' else -1
        target_pct = float(source.native_target_pct)
        target = entry * (1 + sign * target_pct / 100)
        indices = np.arange(j + 1, len(path['close']))
        high = np.asarray(path['high'], dtype=float)[indices]
        low = np.asarray(path['low'], dtype=float)[indices]
        close = np.asarray(path['close'], dtype=float)[indices]
        favorable = sign * ((high if sign == 1 else low) / entry - 1) * 100
        adverse = -sign * ((low if sign == 1 else high) / entry - 1) * 100
        close_return = sign * (close / entry - 1) * 100
        touches = (high >= target if sign == 1 else low <= target)
        target_candidates = np.flatnonzero(touches)
        target_at = int(target_candidates[0]) if len(target_candidates) else None
        close_at = np.flatnonzero(close_return >= target_pct - 1e-10)
        native_adverse = max(0., -sign * (float(trade.exit_price) / entry - 1) * 100)
        stamp = pd.Timestamp(trade.exit_ts)
        first_target = (pd.Timestamp(int(path['timestamp_ns'][indices[target_at]]), tz='UTC').tz_convert('Asia/Kolkata')
                        if target_at is not None else None)
        pre_target_adverse = max(native_adverse, float(np.max(adverse[:target_at])) if target_at else 0.)
        through_target_adverse = (max(pre_target_adverse, float(adverse[target_at]))
                                  if target_at is not None else None)
        row = dict(sid=int(trade.sid), day=trade.day, tradingsymbol=trade.tradingsymbol,
            side=trade.side, native_exit_reason=trade.exit_reason,
            entry_price=entry, native_exit_price=float(trade.exit_price),
            entry_ts=trade.entry_ts, native_exit_ts=trade.exit_ts,
            native_target_pct=target_pct, native_exit_adverse_pct=native_adverse,
            continuation_bars=len(indices), later_target_touched=target_at is not None,
            later_target_closed=bool(len(close_at)), first_target_bar_end=str(first_target) if first_target is not None else None,
            minutes_to_target_touch_lower=(max(0., (first_target - stamp).total_seconds()/60-1)
                                           if first_target is not None else None),
            minutes_to_target_touch_upper=((first_target-stamp).total_seconds()/60 if first_target is not None else None),
            post_exit_adverse_max_pct=max(0.,float(np.max(adverse))) if len(indices) else None,
            post_exit_favorable_max_pct=max(0.,float(np.max(favorable))) if len(indices) else None,
            adverse_before_target_bar_lower_pct=pre_target_adverse if target_at is not None else None,
            adverse_through_target_bar_upper_pct=through_target_adverse,
            final_close_return_pct=float(close_return[-1]) if len(indices) else None,
            later_close_above_cost_break_even=bool((close_return > .05 + 1e-10).any()),
            native_exit_bar_excluded=True)
        # Barrier order is measured only after the excluded native-exit bar.
        # Same-bar target/barrier touches cannot be chronologically resolved.
        for label, barrier in [('125', 1.25), ('150', 1.50)]:
            hit_indices = np.flatnonzero(adverse >= barrier - 1e-10)
            barrier_at = int(hit_indices[0]) if len(hit_indices) else None
            row['later_adverse_' + label + '_touched'] = barrier_at is not None
            row['target_before_adverse_' + label + '_in_continuation'] = (
                target_at is not None and (barrier_at is None or target_at < barrier_at))
            row['first_target_and_adverse_' + label + '_same_bar'] = (
                target_at is not None and barrier_at is not None and target_at == barrier_at)
            original_bar_adverse = -sign * ((float(path['low'][j]) if sign == 1 else float(path['high'][j])) / entry - 1) * 100
            row['excluded_native_exit_bar_also_reached_' + label] = original_bar_adverse >= barrier - 1e-10
        rows.append(row)
    frame = pd.DataFrame(rows)
    summary = []
    for name, group in [('all_native_stops', frame), *list(frame.groupby('native_exit_reason', sort=True))]:
        recovered = group.loc[group.later_target_touched]
        summary.append(dict(group=name, native_stops=len(group), later_target_touched=int(group.later_target_touched.sum()),
            later_target_closed=int(group.later_target_closed.sum()),
            later_close_above_cost_break_even=int(group.later_close_above_cost_break_even.sum()),
            target_touch_pct=float(group.later_target_touched.mean()*100),
            target_touch_minutes_median_upper=float(recovered.minutes_to_target_touch_upper.median()) if len(recovered) else None,
            post_exit_adverse_median_pct=float(group.post_exit_adverse_max_pct.median()),
            post_exit_adverse_q90_pct=float(group.post_exit_adverse_max_pct.quantile(.9)),
            target_before_125_in_continuation=int(group.target_before_adverse_125_in_continuation.sum()),
            target_before_150_in_continuation=int(group.target_before_adverse_150_in_continuation.sum()),
            target_125_same_bar=int(group.first_target_and_adverse_125_same_bar.sum()),
            target_150_same_bar=int(group.first_target_and_adverse_150_same_bar.sum())))
    return frame, pd.DataFrame(summary), days, training.exit_reason.value_counts().to_dict()


def run(output=OUTPUT):
    output = Path(output).resolve()
    if output.exists() or output == PACKAGE or not output.is_relative_to(PACKAGE):
        raise ValueError('Use a fresh continuation diagnostic folder')
    data = inputs.load()
    frame, summary, days, counts = analyze(data)
    output.mkdir(parents=True)
    frame.to_csv(output/'native_stop_continuation.csv',index=False)
    summary.to_csv(output/'summary.csv',index=False)
    config = dict(scope='BACKTEST_ONLY_TRAINING_DIAGNOSTIC',execution_authority=False,
        training_days=days,training_fills=sum(counts.values()),native_exit_reason_counts=counts,
        later_outcomes_inspected=False,continuation_starts='First full bar after native exit bar',
        already_declared_next_models=['native_125_to100_after120m','constant_125',
                                     '125_to100_after240m','constant_150'],
        caveat='Target touches are not rescued-trade outcomes. Wider stops can incur larger losses; target/barrier chronology may be ambiguous, and the excluded native exit bar may already cross a wider barrier.')
    (output/'config.json').write_text(json.dumps(config,indent=2)+'\n')
    lines=['# Native-stop continuation: first44 sessions only','',
        'This diagnostic inspects prices after the baseline had already stopped out. It does not alter entries or create a rescue rule. Only sessions through5October2026 are inspected; no validation or final-seven outcomes select a threshold.','',
        '| Native exit | Stopped trades | Later target touch | Later target close | Later close above cost break-even | Median minutes to target (upper bound) | Median later adverse move |',
        '|:--|--:|--:|--:|--:|--:|--:|']
    for r in summary.itertuples():
        lines.append(f'| {r.group} | {r.native_stops} | {r.later_target_touched} | {r.later_target_closed} | '
                     f'{r.later_close_above_cost_break_even} | {r.target_touch_minutes_median_upper:.1f} | {r.post_exit_adverse_median_pct:.3f}% |')
    lines += ['', 'The continuation excludes the native exit bar, whose remainder cannot be ordered from one-minute OHLC. Target touch time is reported as a one-minute interval. Adverse excursion through the first target bar is an upper bound: its high/low may occur after the target touch. Barrier-order flags apply only to the remaining full bars; an excluded native exit bar may already have crossed a wider stop.', '',
              'The four planned full-entry replays were declared before this diagnostic: original1.25%→1%after120minutes, constant1.25%,1.25%→1%after240minutes, and constant1.50%. They must be replayed for every entry with correct stop-first and gap handling. These diagnostic recoveries must not select individual trades or be counted as profitable rescues.', '',
              '[Summary](summary.csv) · [Every stopped trade](native_stop_continuation.csv) · [Configuration](config.json)','']
    (output/'REPORT.md').write_text('\n'.join(lines),encoding='utf-8')
    provenance=dict(source_manifests=data['source_manifests'],code_sha256={str(Path(__file__)):original.sha(__file__)})
    (output/'provenance.json').write_text(json.dumps(provenance,indent=2)+'\n')
    (output/'manifest.json').write_text(json.dumps({p.name:original.sha(p) for p in output.iterdir() if p.is_file()},indent=2)+'\n')
    print(summary.to_string(index=False),flush=True)
    return config


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=OUTPUT)
    run(parser.parse_args().output)
