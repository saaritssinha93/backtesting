"""Causal Hilega Milega indicators and research setup definitions.

The setup is an RSI regime/crossover model described in the supplied Upsurge
Club transcript.  Its core is RSI(9), EMA(3) of RSI, and WMA(21) of RSI.  A
standard WMA weights recent RSI observations; it does not consume volume.

This module deliberately separates indicator calculation from setup flags so
the rules can be tested without silently changing the project's live book.
All signals are based on completed bars and use no future values.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import pandas as pd


LONG_RSI50_REVERSAL: Final = "L_HM_RSI50_REVERSAL"
SHORT_RSI50_REVERSAL: Final = "S_HM_RSI50_REVERSAL"
LONG_BB20_PULLBACK: Final = "L_HM_BB20_PULLBACK"
SHORT_BB20_PULLBACK: Final = "S_HM_BB20_PULLBACK"

SETUP_FLAG_COLUMNS: Final[dict[str, str]] = {
    LONG_RSI50_REVERSAL: "HM_SETUP_L_RSI50_REVERSAL",
    SHORT_RSI50_REVERSAL: "HM_SETUP_S_RSI50_REVERSAL",
    LONG_BB20_PULLBACK: "HM_SETUP_L_BB20_PULLBACK",
    SHORT_BB20_PULLBACK: "HM_SETUP_S_BB20_PULLBACK",
}

RSI_COLUMN: Final = "HM_RSI_9"
EMA_COLUMN: Final = "HM_RSI_EMA_3"
WMA_COLUMN: Final = "HM_RSI_WMA_21"
BB_MID_COLUMN: Final = "HM_BB_MID_20"
BB_UPPER_COLUMN: Final = "HM_BB_UPPER_20"
BB_LOWER_COLUMN: Final = "HM_BB_LOWER_20"


@dataclass(frozen=True)
class HilegaMilegaConfig:
    rsi_period: int = 9
    ema_period: int = 3
    wma_period: int = 21
    midline: float = 50.0
    bb_period: int = 20
    bb_stddev: float = 2.0

    def __post_init__(self) -> None:
        for name in ("rsi_period", "ema_period", "wma_period", "bb_period"):
            if int(getattr(self, name)) <= 0:
                raise ValueError(f"{name} must be positive")
        if not np.isfinite(self.midline):
            raise ValueError("midline must be finite")
        if not np.isfinite(self.bb_stddev) or self.bb_stddev <= 0:
            raise ValueError("bb_stddev must be positive and finite")


DEFAULT_CONFIG: Final = HilegaMilegaConfig()


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype(float)


def _require_columns(frame: pd.DataFrame, columns: tuple[str, ...]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"Hilega Milega input is missing columns: {missing}")


def _validate_time_order(frame: pd.DataFrame) -> None:
    if "date" not in frame.columns or frame.empty:
        return
    timestamps = pd.to_datetime(frame["date"], errors="coerce")
    if timestamps.isna().any():
        raise ValueError("Hilega Milega input contains invalid dates")
    if not timestamps.is_monotonic_increasing:
        raise ValueError("Hilega Milega input must be ordered oldest to newest")


def wilder_rsi(close: pd.Series, period: int = 9) -> pd.Series:
    """Return Wilder RSI with an SMA seed and recursive RMA updates."""
    if period <= 0:
        raise ValueError("period must be positive")
    values = _numeric(close)
    delta = values.diff()
    gains = delta.clip(lower=0.0).to_numpy(dtype=float)
    losses = (-delta.clip(upper=0.0)).to_numpy(dtype=float)
    result = np.full(len(values), np.nan, dtype=float)

    avg_gain = np.nan
    avg_loss = np.nan
    for index in range(period, len(values)):
        gain = gains[index]
        loss = losses[index]
        if not (np.isfinite(gain) and np.isfinite(loss)):
            avg_gain = np.nan
            avg_loss = np.nan
            continue
        if not (np.isfinite(avg_gain) and np.isfinite(avg_loss)):
            gain_window = gains[index - period + 1 : index + 1]
            loss_window = losses[index - period + 1 : index + 1]
            if not (np.isfinite(gain_window).all() and np.isfinite(loss_window).all()):
                continue
            avg_gain = float(gain_window.mean())
            avg_loss = float(loss_window.mean())
        else:
            avg_gain = ((period - 1) * avg_gain + gain) / period
            avg_loss = ((period - 1) * avg_loss + loss) / period

        if avg_gain == 0.0 and avg_loss == 0.0:
            result[index] = 50.0
        elif avg_loss == 0.0:
            result[index] = 100.0
        elif avg_gain == 0.0:
            result[index] = 0.0
        else:
            relative_strength = avg_gain / avg_loss
            result[index] = 100.0 - 100.0 / (1.0 + relative_strength)
    return pd.Series(result, index=close.index, name=RSI_COLUMN)


def weighted_moving_average(values: pd.Series, period: int) -> pd.Series:
    """Return a linearly weighted MA; the newest observation has most weight."""
    if period <= 0:
        raise ValueError("period must be positive")
    numeric = _numeric(values)
    weights = np.arange(1.0, period + 1.0, dtype=float)
    denominator = float(weights.sum())
    return numeric.rolling(period, min_periods=period).apply(
        lambda window: float(np.dot(window, weights) / denominator),
        raw=True,
    )


def add_hilega_milega_indicators(
    frame: pd.DataFrame,
    config: HilegaMilegaConfig = DEFAULT_CONFIG,
) -> pd.DataFrame:
    """Add RSI/EMA/WMA and Bollinger features without mutating ``frame``."""
    _require_columns(frame, ("open", "high", "low", "close"))
    _validate_time_order(frame)
    out = frame.copy()
    close = _numeric(out["close"])
    rsi = wilder_rsi(close, config.rsi_period)
    out[RSI_COLUMN] = rsi
    out[EMA_COLUMN] = rsi.ewm(
        span=config.ema_period,
        adjust=False,
        min_periods=config.ema_period,
    ).mean()
    out[WMA_COLUMN] = weighted_moving_average(rsi, config.wma_period)

    bb_mid = close.rolling(config.bb_period, min_periods=config.bb_period).mean()
    bb_std = close.rolling(config.bb_period, min_periods=config.bb_period).std(ddof=0)
    out[BB_MID_COLUMN] = bb_mid
    out[BB_UPPER_COLUMN] = bb_mid + config.bb_stddev * bb_std
    out[BB_LOWER_COLUMN] = bb_mid - config.bb_stddev * bb_std
    return out


def add_hilega_milega_setup_flags(
    frame: pd.DataFrame,
    config: HilegaMilegaConfig = DEFAULT_CONFIG,
) -> pd.DataFrame:
    """Add four entry setups plus warning, momentum, and exit diagnostics."""
    _require_columns(
        frame,
        (
            "open",
            "high",
            "low",
            "close",
            RSI_COLUMN,
            EMA_COLUMN,
            WMA_COLUMN,
            BB_MID_COLUMN,
            BB_UPPER_COLUMN,
            BB_LOWER_COLUMN,
        ),
    )
    out = frame.copy()
    open_price = _numeric(out["open"])
    high = _numeric(out["high"])
    low = _numeric(out["low"])
    close = _numeric(out["close"])
    rsi = _numeric(out[RSI_COLUMN])
    ema = _numeric(out[EMA_COLUMN])
    wma = _numeric(out[WMA_COLUMN])
    bb_mid = _numeric(out[BB_MID_COLUMN])

    finite_lines = rsi.notna() & ema.notna() & wma.notna()
    bullish_alignment = finite_lines & rsi.gt(ema) & rsi.gt(wma)
    bearish_alignment = finite_lines & rsi.lt(ema) & rsi.lt(wma)
    bullish_slope = ema.diff().gt(0.0) & wma.diff().gt(0.0)
    bearish_slope = ema.diff().lt(0.0) & wma.diff().lt(0.0)
    bullish_trend = bullish_alignment & bullish_slope
    bearish_trend = bearish_alignment & bearish_slope

    line_max = pd.concat([rsi, ema, wma], axis=1).max(axis=1)
    line_min = pd.concat([rsi, ema, wma], axis=1).min(axis=1)
    out["HM_LINE_DISTANCE"] = (line_max - line_min).where(finite_lines)
    out["HM_BULLISH_ALIGNMENT"] = bullish_trend.fillna(False)
    out["HM_BEARISH_ALIGNMENT"] = bearish_trend.fillna(False)
    out["HM_NO_TRADE"] = (~(bullish_trend | bearish_trend)).fillna(True)

    cross_up_50 = rsi.shift(1).le(config.midline) & rsi.gt(config.midline)
    cross_down_50 = rsi.shift(1).ge(config.midline) & rsi.lt(config.midline)
    rsi_cross_up_ema = rsi.shift(1).le(ema.shift(1)) & rsi.gt(ema)
    rsi_cross_up_wma = rsi.shift(1).le(wma.shift(1)) & rsi.gt(wma)
    rsi_cross_down_ema = rsi.shift(1).ge(ema.shift(1)) & rsi.lt(ema)
    rsi_cross_down_wma = rsi.shift(1).ge(wma.shift(1)) & rsi.lt(wma)

    out["HM_BOTTOM_FORMING_WARNING"] = (
        rsi.lt(config.midline)
        & bullish_slope
        & (rsi_cross_up_ema | rsi_cross_up_wma)
        & rsi.gt(ema)
        & rsi.gt(wma)
    ).fillna(False)
    out["HM_TOP_FORMING_WARNING"] = (
        rsi.gt(config.midline)
        & bearish_slope
        & (rsi_cross_down_ema | rsi_cross_down_wma)
        & rsi.lt(ema)
        & rsi.lt(wma)
    ).fillna(False)

    long_price_confirmation = (
        close.gt(open_price)
        & close.gt(high.shift(1))
        & close.gt(bb_mid)
    )
    # The transcript states the previous-high close for a bottom.  The
    # previous-low condition below is its explicit, testable short-side mirror.
    short_price_confirmation = (
        close.lt(open_price)
        & close.lt(low.shift(1))
        & close.lt(bb_mid)
    )
    out[SETUP_FLAG_COLUMNS[LONG_RSI50_REVERSAL]] = (
        cross_up_50 & bullish_trend & long_price_confirmation
    ).fillna(False)
    out[SETUP_FLAG_COLUMNS[SHORT_RSI50_REVERSAL]] = (
        cross_down_50 & bearish_trend & short_price_confirmation
    ).fillna(False)

    established_bullish_regime = rsi.shift(1).gt(config.midline) & rsi.gt(config.midline)
    established_bearish_regime = rsi.shift(1).lt(config.midline) & rsi.lt(config.midline)
    bb_mid_reclaim = low.le(bb_mid) & close.gt(bb_mid) & close.gt(open_price)
    bb_mid_reject = high.ge(bb_mid) & close.lt(bb_mid) & close.lt(open_price)
    out[SETUP_FLAG_COLUMNS[LONG_BB20_PULLBACK]] = (
        established_bullish_regime & bullish_trend & bb_mid_reclaim
    ).fillna(False)
    out[SETUP_FLAG_COLUMNS[SHORT_BB20_PULLBACK]] = (
        established_bearish_regime & bearish_trend & bb_mid_reject
    ).fillna(False)

    out["HM_EXIT_LONG_WMA_CROSS"] = (
        wma.shift(1).le(rsi.shift(1)) & wma.gt(rsi)
    ).fillna(False)
    out["HM_EXIT_SHORT_WMA_CROSS"] = (
        wma.shift(1).ge(rsi.shift(1)) & wma.lt(rsi)
    ).fillna(False)
    out["HM_LONG_INITIAL_STOP"] = low
    out["HM_SHORT_INITIAL_STOP"] = high
    out["HM_LONG_BB_TARGET"] = _numeric(out[BB_UPPER_COLUMN])
    # The lower-band short target is the symmetric implementation because the
    # transcript only states the upper-band target explicitly for longs.
    out["HM_SHORT_BB_TARGET"] = _numeric(out[BB_LOWER_COLUMN])
    return out


def add_hilega_milega_features(
    frame: pd.DataFrame,
    config: HilegaMilegaConfig = DEFAULT_CONFIG,
) -> pd.DataFrame:
    """Add all causal indicators and setup flags to one instrument's bars."""
    return add_hilega_milega_setup_flags(
        add_hilega_milega_indicators(frame, config),
        config,
    )


def setup_fired(row: pd.Series, setup: str) -> bool:
    """Return a strict bool for a named setup flag, failing closed."""
    if setup not in SETUP_FLAG_COLUMNS:
        raise KeyError(f"Unknown Hilega Milega setup: {setup}")
    value = row.get(SETUP_FLAG_COLUMNS[setup], False)
    return bool(value) if pd.notna(value) else False


ALL_SETUPS: Final[tuple[str, ...]] = tuple(SETUP_FLAG_COLUMNS)

