import json
import time
from pathlib import Path
from datetime import datetime

import pandas as pd

import avwap_combined_runner as r
from avwap_v11_refactored.avwap_common import default_short_config, default_long_config, compute_backtest_metrics


OUT_DIR = Path('outputs')
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _build_cfg(min_bars: int):
    short_cfg = default_short_config(reports_dir=OUT_DIR)
    long_cfg = default_long_config(reports_dir=OUT_DIR)

    short_cfg.lag_bars_short_a_mod_break_c1_low = int(r.SHORT_LAG_BARS_A_MOD_BREAK_C1_LOW)
    short_cfg.lag_bars_short_a_pullback_c2_break_c2_low = int(r.SHORT_LAG_BARS_A_PULLBACK_C2_BREAK_C2_LOW)
    short_cfg.lag_bars_short_b_huge_failed_bounce = int(r.SHORT_LAG_BARS_B_HUGE_FAILED_BOUNCE)

    long_cfg.lag_bars_long_a_mod_break_c1_high = int(r.LONG_LAG_BARS_A_MOD_BREAK_C1_HIGH)
    long_cfg.lag_bars_long_a_pullback_c2_break_c2_high = int(r.LONG_LAG_BARS_A_PULLBACK_C2_BREAK_C2_HIGH)
    long_cfg.lag_bars_long_b_huge_pullback_hold_break = int(r.LONG_LAG_BARS_B_HUGE_PULLBACK_HOLD_BREAK)

    if r.FORCE_LIVE_PARITY_MIN_BARS_LEFT:
        short_cfg.min_bars_left_after_entry = 0
        long_cfg.min_bars_left_after_entry = 0

    if r.FORCE_LIVE_PARITY_DISABLE_TOPN:
        short_cfg.enable_topn_per_day = False
        long_cfg.enable_topn_per_day = False

    if r.FINAL_SIGNAL_WINDOW_OVERRIDE:
        short_cfg.use_time_windows = bool(r.FINAL_SHORT_USE_TIME_WINDOWS)
        long_cfg.use_time_windows = bool(r.FINAL_LONG_USE_TIME_WINDOWS)
        short_cfg.signal_windows = list(r.FINAL_SHORT_SIGNAL_WINDOWS)
        long_cfg.signal_windows = list(r.FINAL_LONG_SIGNAL_WINDOWS)

    # Align with live scanner behavior
    short_cfg.allow_incomplete_tail = True
    long_cfg.allow_incomplete_tail = True

    short_cfg.min_bars_for_scan = int(min_bars)
    long_cfg.min_bars_for_scan = int(min_bars)

    return short_cfg, long_cfg


def _safe_pf(v):
    try:
        x = float(v)
    except Exception:
        return None
    if x == float('inf'):
        return 'inf'
    return x


def run_case(min_bars: int):
    t0 = time.time()

    short_cfg, long_cfg = _build_cfg(min_bars)

    short_df = r._run_side_parallel('SHORT', short_cfg, r.MAX_WORKERS)
    long_df = r._run_side_parallel('LONG', long_cfg, r.MAX_WORKERS)

    dir_5m = r._resolve_5min_dir()
    suffix_5m = '.parquet'

    if not short_df.empty:
        short_df = r._resolve_exits_5min(short_df, dir_5m, suffix_5m, short_cfg.parquet_engine)
    if not long_df.empty:
        long_df = r._resolve_exits_5min(long_df, dir_5m, suffix_5m, long_cfg.parquet_engine)

    if not short_df.empty:
        short_df = r._add_notional_pnl(short_df)
        short_df = r._sort_trades_for_output(short_df)
    if not long_df.empty:
        long_df = r._add_notional_pnl(long_df)
        long_df = r._sort_trades_for_output(long_df)

    if short_df.empty and long_df.empty:
        combined = pd.DataFrame()
    elif short_df.empty:
        combined = long_df.copy()
    elif long_df.empty:
        combined = short_df.copy()
    else:
        combined = pd.concat([short_df, long_df], ignore_index=True)

    if not combined.empty:
        combined = r._add_notional_pnl(combined)
        combined = r._sort_trades_for_output(combined)

    m_short = compute_backtest_metrics(short_df)
    m_long = compute_backtest_metrics(long_df)
    m_combined = compute_backtest_metrics(combined)

    net_pnl_rs = float(combined['pnl_rs'].sum()) if (not combined.empty and 'pnl_rs' in combined.columns) else 0.0
    gross_pnl_rs = float(combined['pnl_rs_gross'].sum()) if (not combined.empty and 'pnl_rs_gross' in combined.columns) else 0.0
    roi_pct = (net_pnl_rs / float(r.PORTFOLIO_START_CAPITAL_RS) * 100.0) if r.PORTFOLIO_START_CAPITAL_RS else 0.0

    elapsed = time.time() - t0

    return {
        'min_bars_for_scan': int(min_bars),
        'elapsed_sec': round(elapsed, 2),
        'rows': {
            'short': int(len(short_df)),
            'long': int(len(long_df)),
            'combined': int(len(combined)),
        },
        'notional': {
            'net_pnl_rs': round(net_pnl_rs, 2),
            'gross_pnl_rs': round(gross_pnl_rs, 2),
            'roi_pct_on_start_capital': round(roi_pct, 4),
            'start_capital_rs': float(r.PORTFOLIO_START_CAPITAL_RS),
        },
        'metrics_combined': {
            'total_trades': int(m_combined.total_trades),
            'profit_factor': _safe_pf(m_combined.profit_factor),
            'sum_pnl_pct': round(float(m_combined.sum_pnl_pct), 6),
            'avg_pnl_pct': round(float(m_combined.avg_pnl_pct), 6),
            'hit_rate_pct': round(float(m_combined.hit_rate_pct), 6),
            'target_count': int(m_combined.target_count),
            'sl_count': int(m_combined.sl_count),
            'be_count': int(m_combined.be_count),
            'eod_count': int(m_combined.eod_count),
            'max_drawdown_pct': round(float(m_combined.max_drawdown_pct), 6),
            'sharpe_daily': round(float(getattr(m_combined, 'sharpe_daily', 0.0)), 6),
            'sortino_daily': round(float(getattr(m_combined, 'sortino_daily', 0.0)), 6),
            'calmar_ratio': round(float(getattr(m_combined, 'calmar_ratio', 0.0)), 6),
        },
        'metrics_short': {
            'total_trades': int(m_short.total_trades),
            'profit_factor': _safe_pf(m_short.profit_factor),
            'sum_pnl_pct': round(float(m_short.sum_pnl_pct), 6),
            'hit_rate_pct': round(float(m_short.hit_rate_pct), 6),
        },
        'metrics_long': {
            'total_trades': int(m_long.total_trades),
            'profit_factor': _safe_pf(m_long.profit_factor),
            'sum_pnl_pct': round(float(m_long.sum_pnl_pct), 6),
            'hit_rate_pct': round(float(m_long.hit_rate_pct), 6),
        },
    }


def _delta(a, b):
    return {
        'entries_delta': int(b['rows']['combined'] - a['rows']['combined']),
        'net_pnl_rs_delta': round(float(b['notional']['net_pnl_rs']) - float(a['notional']['net_pnl_rs']), 2),
        'roi_pct_delta': round(float(b['notional']['roi_pct_on_start_capital']) - float(a['notional']['roi_pct_on_start_capital']), 6),
        'profit_factor_delta': (
            None
            if isinstance(a['metrics_combined']['profit_factor'], str) or isinstance(b['metrics_combined']['profit_factor'], str)
            else round(float(b['metrics_combined']['profit_factor']) - float(a['metrics_combined']['profit_factor']), 6)
        ),
        'sum_pnl_pct_delta': round(float(b['metrics_combined']['sum_pnl_pct']) - float(a['metrics_combined']['sum_pnl_pct']), 6),
    }


def main():
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    print('[ABTEST] Running baseline min_bars_for_scan=5 ...', flush=True)
    res5 = run_case(5)

    print('[ABTEST] Running variant min_bars_for_scan=3 ...', flush=True)
    res3 = run_case(3)

    summary = {
        'generated_at': ts,
        'assumptions': {
            'runner': 'avwap_combined_runner.py',
            'all_tickers_all_days': True,
            'use_5min_exit_resolution': True,
            'allow_incomplete_tail_for_parity': True,
            'only_changed_parameter': 'min_bars_for_scan',
            'comparison': '3_vs_5',
        },
        'min_bars_5': res5,
        'min_bars_3': res3,
        'delta_3_minus_5': _delta(res5, res3),
    }

    out_json = OUT_DIR / f'min_bars_scan_abtest_{ts}.json'
    out_json.write_text(json.dumps(summary, indent=2), encoding='utf-8')

    print(f"[ABTEST] Saved: {out_json}", flush=True)
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == '__main__':
    main()
