"""v17D adapter — Signal -> v17C trades-CSV row.

Bridges the v17D setup library (which emits Signal objects) into the
existing v17C trades-CSV pipeline. Two flows:

    signal_to_row(signal, exit_result, leverage, position_size_rs,
                  cost_model_pct, sector=None) -> dict
        Build a complete row matching v17C's avwap_longshort_trades CSV
        schema. exit_result is what v17D_exit_resolver.resolve() returned.

    rows_to_dataframe(rows) -> pd.DataFrame
        Convert a list of row-dicts into a DataFrame ordered by the
        canonical v17C column order.

Schema produced (matches v17C output for downstream governor / sizing
compatibility):

    Identity:
        ticker, side, setup, trade_date

    Signal-time fields:
        signal_time_ist, entry_time_ist, entry_price,
        bars_from_open, entry_hour

    Diagnostic features (for filter chains):
        rsi_signal, atr_pct_signal, adx_signal, stochk_signal,
        ema20_gap_atr_signal, avwap_dist_atr_signal,
        entry_bar_vol_ratio, quality_score

    Exit fields:
        exit_time_ist, exit_price, outcome, pnl_pct_price, pnl_pct, pnl_rs,
        bars_held, sl_pct, tgt_pct, sl_price, tgt_price

    Cost & sizing:
        cost_pct, position_size_rs, leverage

    v17D-specific:
        sector, suggested_sl_pct, suggested_tgt_pct, source ("v17D")
"""
from __future__ import annotations

import pandas as pd

from eqidv2.v17D_setup_library.common import Signal
from eqidv2.v17D_exit_resolver import ResolutionResult

CSV_COLUMN_ORDER = [
    # Identity
    "trade_date", "ticker", "side", "setup",
    # Signal time
    "signal_time_ist", "entry_time_ist", "entry_price",
    "bars_from_open", "entry_hour",
    # Diagnostic features
    "rsi_signal", "atr_pct_signal", "adx_signal", "stochk_signal",
    "ema20_gap_atr_signal", "avwap_dist_atr_signal",
    "entry_bar_vol_ratio", "quality_score",
    # Exit
    "exit_time_ist", "exit_price", "outcome",
    "pnl_pct_price", "pnl_pct", "pnl_rs",
    "bars_held", "sl_pct", "tgt_pct", "sl_price", "tgt_price",
    # Costs & sizing
    "cost_pct", "position_size_rs", "leverage",
    # v17D-specific
    "sector", "suggested_sl_pct", "suggested_tgt_pct", "source",
]


def signal_to_row(
    signal: Signal,
    exit_result: ResolutionResult | None,
    *,
    leverage: float = 5.0,
    position_size_rs: float = 20000.0,
    cost_pct: float = 0.0,
    sector: str | None = None,
) -> dict:
    """Build the v17C-compatible row dict from a Signal + exit resolution.

    If exit_result is None (no bars available), the row is still emitted with
    NaN exit fields so downstream audits can detect the gap (rather than
    silently dropping the trade).
    """
    sl_pct = float(signal.suggested_sl_pct)
    tgt_pct = float(signal.suggested_tgt_pct)
    entry = float(signal.entry_price)
    if signal.side == "LONG":
        sl_price = entry * (1 - sl_pct / 100)
        tgt_price = entry * (1 + tgt_pct / 100)
    else:
        sl_price = entry * (1 + sl_pct / 100)
        tgt_price = entry * (1 - tgt_pct / 100)

    sig_ts = pd.Timestamp(signal.signal_time_ist)
    if sig_ts.tz is None:
        sig_ts = sig_ts.tz_localize("Asia/Kolkata")
    trade_date = sig_ts.date()
    # Entry bar follows signal bar by 5 min (F12 entry-bar exits in v17C
    # would use signal bar itself; for v17D library, default to next bar).
    entry_ts = sig_ts + pd.Timedelta(minutes=5)

    row = {
        "trade_date": trade_date,
        "ticker": signal.ticker,
        "side": signal.side,
        "setup": signal.setup,
        "signal_time_ist": sig_ts.isoformat(),
        "entry_time_ist": entry_ts.isoformat(),
        "entry_price": entry,
        "bars_from_open": signal.bars_from_open,
        "entry_hour": signal.entry_hour,
        "rsi_signal": signal.rsi_signal,
        "atr_pct_signal": signal.atr_pct_signal,
        "adx_signal": signal.adx_signal,
        "stochk_signal": signal.stochk_signal,
        "ema20_gap_atr_signal": signal.ema20_gap_atr_signal,
        "avwap_dist_atr_signal": signal.avwap_dist_atr_signal,
        "entry_bar_vol_ratio": signal.entry_bar_vol_ratio,
        "quality_score": signal.quality_score,
        "sl_pct": sl_pct,
        "tgt_pct": tgt_pct,
        "sl_price": sl_price,
        "tgt_price": tgt_price,
        "cost_pct": cost_pct,
        "position_size_rs": position_size_rs,
        "leverage": leverage,
        "sector": sector or "UNKNOWN",
        "suggested_sl_pct": sl_pct,
        "suggested_tgt_pct": tgt_pct,
        "source": "v17D",
    }

    if exit_result is None:
        row.update({
            "exit_time_ist": None,
            "exit_price": float("nan"),
            "outcome": None,
            "pnl_pct_price": float("nan"),
            "pnl_pct": float("nan"),
            "pnl_rs": float("nan"),
            "bars_held": 0,
        })
    else:
        # Apply costs to price PnL, then leverage
        net_price = exit_result.pnl_pct_price - cost_pct
        net_lev = net_price * leverage
        pnl_rs = net_lev * position_size_rs / 100.0
        row.update({
            "exit_time_ist": exit_result.exit_time_ist.isoformat(),
            "exit_price": exit_result.exit_price,
            "outcome": exit_result.outcome,
            "pnl_pct_price": exit_result.pnl_pct_price,
            "pnl_pct": net_lev,
            "pnl_rs": pnl_rs,
            "bars_held": exit_result.bars_held,
        })

    return row


def rows_to_dataframe(rows: list) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=CSV_COLUMN_ORDER)
    df = pd.DataFrame(rows)
    # Reorder; missing columns become NaN
    for col in CSV_COLUMN_ORDER:
        if col not in df.columns:
            df[col] = float("nan")
    return df[CSV_COLUMN_ORDER]


if __name__ == "__main__":
    import numpy as np
    from eqidv2.v17D_exit_resolver import resolve

    sig = Signal(
        ticker="TEST", setup="TC_1_PULLBACK_EMA20", side="LONG",
        signal_time_ist=pd.Timestamp("2026-05-08 09:30", tz="Asia/Kolkata"),
        entry_price=500.0, bars_from_open=3, entry_hour=9.5,
        rsi_signal=55.0, atr_pct_signal=0.005, adx_signal=28.0,
        suggested_sl_pct=0.75, suggested_tgt_pct=1.00,
    )
    idx = pd.DatetimeIndex(
        [pd.Timestamp("2026-05-08 09:35", tz="Asia/Kolkata")
         + pd.Timedelta(minutes=i) for i in range(60)]
    )
    bars = pd.DataFrame({
        "high": np.full(60, 502.0), "low": np.full(60, 498.0),
        "close": np.full(60, 500.0),
    }, index=idx)
    bars.iloc[20, bars.columns.get_loc("high")] = 506.0  # tgt hit
    res = resolve(bars, "LONG", 500.0, sig.signal_time_ist + pd.Timedelta(minutes=5),
                  0.75, 1.00)
    row = signal_to_row(sig, res, cost_pct=0.30, sector="IT")
    df = rows_to_dataframe([row])
    print(df.to_string(index=False))
