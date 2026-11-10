"""Hash-verified, session-reset Wilder ADX14 at each V21 confirmation.

All indicator inputs stop at the completed confirmation candle.  ADX is a
trend-strength statistic; its companion +DI and -DI supply direction.  No
future bars, prior-session gaps, or trade outcomes enter these features.
This is a new session-reset definition, distinct from the legacy snapshot's
continuous EWM ``ADX`` column; there is no asserted parity with that column.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from research import g7_v4_extremes
from g3_v21_expanded_20261104.refinement_20261104.g7_v4_confirmations_20261104 import stock_features


PERIOD = 14
FIRST_ADX_BAR = 2 * PERIOD - 1
FIRST_DELTA3_BAR = FIRST_ADX_BAR + 3
FIELDS = ("adx14", "plus_di14", "minus_di14", "adx_delta3")


def wilder_adx14(window: pd.DataFrame) -> pd.DataFrame:
    """Calculate Wilder's DMI/ADX on one contiguous one-minute session.

    Index 0 is the first session bar.  The first smoothed TR and DM sums use
    transitions 1..14; DX is available at index 14.  The first ADX averages
    DX indices 14..27 and is therefore available at index 27 (28 bars).
    """
    if not isinstance(window, pd.DataFrame) or not {"high", "low", "close"}.issubset(window):
        raise ValueError("ADX requires one session of high/low/close bars")
    high = pd.to_numeric(window.high, errors="raise").to_numpy(float)
    low = pd.to_numeric(window.low, errors="raise").to_numpy(float)
    close = pd.to_numeric(window.close, errors="raise").to_numpy(float)
    n = len(window)
    if not np.isfinite(np.column_stack((high, low, close))).all() or (high < low).any():
        raise ValueError("Invalid ADX high/low/close source")
    plus_di = np.full(n, np.nan, dtype=float)
    minus_di = np.full(n, np.nan, dtype=float)
    adx = np.full(n, np.nan, dtype=float)
    dx = np.full(n, np.nan, dtype=float)
    tr = np.zeros(n, dtype=float)
    plus_dm = np.zeros(n, dtype=float)
    minus_dm = np.zeros(n, dtype=float)
    for i in range(1, n):
        upward = high[i] - high[i - 1]
        downward = low[i - 1] - low[i]
        plus_dm[i] = upward if upward > downward and upward > 0 else 0.0
        minus_dm[i] = downward if downward > upward and downward > 0 else 0.0
        tr[i] = max(high[i] - low[i], abs(high[i] - close[i - 1]),
                    abs(low[i] - close[i - 1]))
    if n > PERIOD:
        tr_sum = float(tr[1:PERIOD + 1].sum())
        plus_sum = float(plus_dm[1:PERIOD + 1].sum())
        minus_sum = float(minus_dm[1:PERIOD + 1].sum())
        for i in range(PERIOD, n):
            if i > PERIOD:
                tr_sum = tr_sum - tr_sum / PERIOD + tr[i]
                plus_sum = plus_sum - plus_sum / PERIOD + plus_dm[i]
                minus_sum = minus_sum - minus_sum / PERIOD + minus_dm[i]
            if tr_sum <= 0:
                plus_di[i] = minus_di[i] = 0.0
            else:
                plus_di[i] = 100.0 * plus_sum / tr_sum
                minus_di[i] = 100.0 * minus_sum / tr_sum
            di_total = plus_di[i] + minus_di[i]
            dx[i] = 100.0 * abs(plus_di[i] - minus_di[i]) / di_total if di_total > 0 else 0.0
            if i == FIRST_ADX_BAR:
                adx[i] = float(np.mean(dx[PERIOD:FIRST_ADX_BAR + 1]))
            elif i > FIRST_ADX_BAR:
                adx[i] = (adx[i - 1] * (PERIOD - 1) + dx[i]) / PERIOD
    delta3 = np.full(n, np.nan, dtype=float)
    if n > FIRST_DELTA3_BAR:
        delta3[FIRST_DELTA3_BAR:] = adx[FIRST_DELTA3_BAR:] - adx[FIRST_ADX_BAR:-3]
    for field in (plus_di, minus_di, adx):
        if np.isfinite(field).any() and (np.nanmin(field) < -1e-10 or np.nanmax(field) > 100 + 1e-10):
            raise ValueError("ADX or DI outside [0, 100]")
    return pd.DataFrame({"plus_di14": plus_di, "minus_di14": minus_di,
                         "adx14": adx, "adx_delta3": delta3}, index=window.index)


def build_adx_audit(orders: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """Return one pre-entry ADX feature row per selected V21 order SID.

    ``orders`` are the strict V21 selected native orders, not outcome-filtered
    trade rows.  The source lookup and hash checks exactly match the existing
    G7 stock audit.  Missing early-session ADX is explicit and never filled.
    """
    if orders.empty:
        return pd.DataFrame(columns=["sid", "day", "setup_id", "tradingsymbol", "side",
                                     "confirmation_ts", "adx_available_at", "adx_ready",
                                     "adx_delta3_ready", *FIELDS, "adx_stock_source_sha256"]), []
    grouped, source_proofs = stock_features._inventory(orders)
    results: list[dict] = []
    proofs: list[dict] = []
    for path, rows in grouped.items():
        proof = source_proofs[path]
        frame, proof = stock_features._prepared_frame(path, rows, proof)
        components = stock_features._extension_series(frame)
        by_day: dict[str, list[dict]] = {}
        for row in rows:
            stock_features._current_confirmation(row, frame, components)
            by_day.setdefault(str(row["day"])[:10], []).append(row)
        for day, day_rows in by_day.items():
            latest = max(g7_v4_extremes.timestamp(row["confirmation_ts"]) for row in day_rows)
            window = g7_v4_extremes.checked_session(frame, latest)
            values = wilder_adx14(window)
            positions = {stamp: i for i, stamp in enumerate(window.ts)}
            for row in day_rows:
                stamp = g7_v4_extremes.timestamp(row["confirmation_ts"])
                i = positions.get(stamp)
                expected_i = int((stamp - stamp.normalize()).total_seconds() / 60) - (9 * 60 + 16)
                if i is None or i != expected_i:
                    raise ValueError(f"ADX confirmation minute missing or misaligned: {row['sid']}")
                current = values.iloc[i]
                ready = bool(i >= FIRST_ADX_BAR and np.isfinite(current.adx14)
                             and np.isfinite(current.plus_di14) and np.isfinite(current.minus_di14))
                delta_ready = bool(i >= FIRST_DELTA3_BAR and np.isfinite(current.adx_delta3))
                if i >= FIRST_ADX_BAR and not ready:
                    raise ValueError(f"Missing ready ADX at confirmation: {row['sid']}")
                results.append(dict(sid=int(row["sid"]), day=day,
                                    setup_id=str(row["setup_id"]),
                                    tradingsymbol=str(row["tradingsymbol"]), side=str(row["side"]),
                                    confirmation_ts=stamp.isoformat(),
                                    adx_available_at=stamp.isoformat(),
                                    adx_session_bars=i + 1,
                                    adx_ready=ready, adx_delta3_ready=delta_ready,
                                    adx14=float(current.adx14) if ready else np.nan,
                                    plus_di14=float(current.plus_di14) if ready else np.nan,
                                    minus_di14=float(current.minus_di14) if ready else np.nan,
                                    adx_delta3=float(current.adx_delta3) if delta_ready else np.nan,
                                    adx_stock_source_sha256=proof["stock_sha256"]))
        proofs.append(dict(proof, adx_period=PERIOD,
                           adx_definition="WILDER_SESSION_RESET_FIRST_ADX_AT_BAR_28",
                           adx_selected_sids=len(rows),
                           adx_source_rows=int(len(frame)),
                           adx_candidate_days=sorted(by_day)))
    audit = pd.DataFrame(results).sort_values("sid", kind="stable").reset_index(drop=True)
    if (len(audit) != len(orders) or audit.sid.duplicated().any() or
            set(audit.sid.astype(int)) != set(orders.sid.astype(int))):
        raise ValueError("ADX audit does not cover every selected order exactly once")
    available = pd.to_datetime(audit.adx_available_at, utc=True)
    confirmation = pd.to_datetime(audit.confirmation_ts, utc=True)
    if available.isna().any() or confirmation.isna().any() or not available.le(confirmation).all():
        raise ValueError("ADX feature timestamp is later than confirmation")
    if "entry_ts" in orders:
        entry = pd.to_datetime(orders.set_index("sid").loc[audit.sid, "entry_ts"], utc=True,
                               errors="coerce").reset_index(drop=True)
        if entry.notna().any() and not available.loc[entry.notna()].lt(entry.loc[entry.notna()]).all():
            raise ValueError("ADX confirmation feature is not before native entry")
    return audit, proofs
