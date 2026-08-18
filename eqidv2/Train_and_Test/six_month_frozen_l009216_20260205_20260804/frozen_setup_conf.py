"""Post-hoc in-sample V12 LONG research candidate.

This configuration used the full 2026-07-06 through 2026-08-04 month
for local refinement.  It has no untouched holdout and is not approved for
production.
"""

PRODUCTION_APPROVED = False
RESEARCH_ONLY = True
POSTHOC_IN_SAMPLE = True
REQUIRES_FRESH_HOLDOUT = True

SETUP_NAME = 'ONE_MONTH_PREFILTER_LONG_V9'
CONFIG_ID = 'L009216_PULLBACK_BOUNCE'
CONFIG_SHA256 = '7d5f02566c6bffc649395d22cc925dd3083079b3d63b308cf0d12258c3438310'
FAMILY = 'PULLBACK_BOUNCE'

PREFILTER_JOB_CHANGED = False
PREFILTER_PRIMARY_SIDE = "LONG"
PREFILTER_RANK_MIN = 200
PREFILTER_RANK_MAX = 300

SIGNAL_TIMEFRAME = "5min_completed_bar"
ENTRY_TIMEFRAME = "exact_next_available_1min"
EXIT_TIMEFRAME = "exact_1min_with_conservative_5min_gap_fallback"
STOP_LOSS_PCT = 1.0
TARGET_PCT = 2.0
ONE_TICKER_PER_DAY = True
DAILY_CAP = 15
STATUTORY_COSTS = True
V12_RISK_SIZING = True
PAPER_ENTRY_SLIPPAGE_BPS = 5.0
RISK_EQUITY_RS = 200000.0
RISK_PCT_PER_TRADE = 0.25
RISK_MIN_NOTIONAL_RS = 50000.0
RISK_MAX_NOTIONAL_RS = 150000.0
INTRADAY_LEVERAGE = 5.0

ENTRY_SELECTION = "first chronological passing signal per ticker/day"
ENTRY_TIE_BREAK = ("signal_time_ist", "selection_rank", "ticker")
STOP_TARGET_SAME_BAR_POLICY = "STOP_FIRST"
ONE_MINUTE_GAP_POLICY = "CONSERVATIVE_5MIN_FALLBACK"
MISSING_FEATURE_POLICY = "FAIL_CLOSED"
PREFILTER_MEMBERSHIP_POLICY = "LONG at signal hour; same hourly list valid within that hour"

RULE = {'config_id': 'L009216_PULLBACK_BOUNCE', 'family': 'PULLBACK_BOUNCE', 'rank_min': 200, 'rank_max': 300, 'signal_minute_min': 600, 'signal_minute_max': 780, 'atr_pct_min': 0.35, 'session_return_min': 1.0, 'vwap_dist_atr_min': 0.9, 'close_position_min': 0.65, 'range_pct_min': 0.25, 'ret_5m_min': 0.15, 'ret_5m_max': 0.35, 'ret_15m_min': None, 'ret_30m_min': None, 'ret_60m_min': None, 'return_acceleration_min': None, 'adx_min': None, 'rsi_min': None, 'rsi_max': None, 'volume_ratio20_min': None, 'upper_wick_pct_max': None, 'running_high_distance_atr_min': None, 'running_high_distance_atr_max': None, 'ema20_dist_atr_min': 1.0, 'ema20_dist_atr_max': None, 'ema50_dist_atr_min': None, 'score_margin_min': 0.05, 'previous_ret_5m_max': -0.1, 'previous_vwap_dist_atr_max': None, 'require_contiguous_previous': True, 'require_bullish_reversal': False, 'require_vwap_reclaim': False}
