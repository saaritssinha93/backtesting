"""Production 09:25 LONG addition, without changing original G selections."""
from __future__ import annotations

import numpy as np
import pandas as pd

from fno_v13_v10_g_policy import RELAXED_0925_LONG, enabled_for_session


def apply_relaxed_0925_long(original_orders, observed_features, *, session_date):
    """Fill the existing one-order quota only if the original book left it empty.

    Observations are raw, before EMA/OI strict gating. The dated policy applies
    only to the requested session; historical reruns retain their original book.
    """
    original = original_orders.copy()
    if not enabled_for_session(session_date) or observed_features.empty:
        return original, pd.DataFrame()
    raw = observed_features.copy()
    for column in ("signal_ts", "confirmation_ts"):
        raw[column] = pd.to_datetime(raw[column], utc=True, errors="coerce").dt.tz_convert("Asia/Kolkata")
    raw = raw.loc[raw.signal_ts.dt.strftime("%H:%M:%S.%f").eq("09:25:00.000000")].copy()
    raw["day"] = raw.signal_ts.dt.date
    if not raw.day.eq(session_date).all():
        raise ValueError("Relaxed observations escaped the requested session")
    if raw.duplicated(["day", "tradingsymbol"]).any():
        raise ValueError("Duplicate raw 09:25 stock observations")
    fields = ["oi", "prev_oi", "oi_change_pct", "price_change_pct", "volume_ratio",
              "signal_close", "confirmation_open", "confirmation_high", "confirmation_low",
              "confirmation_close", "body_ratio", "v9_1m_upper_wick_ratio",
              "v9_1m_volume_ratio", "traded_value"]
    raw[fields] = raw[fields].apply(pd.to_numeric, errors="coerce")
    checks = dict(
        finite=np.isfinite(raw[fields]).all(axis=1),
        exact_confirmation=raw.confirmation_ts.sub(raw.signal_ts).eq(pd.Timedelta(minutes=1)),
        oi_increasing=raw.prev_oi.gt(0) & raw.oi.gt(raw.prev_oi),
        oi_min=raw.oi_change_pct.ge(.10),
        oi_max=raw.oi_change_pct.le(RELAXED_0925_LONG["oi_max_pct"]),
        price=raw.price_change_pct.ge(.30),
        volume=raw.volume_ratio.ge(RELAXED_0925_LONG["minimum_volume_ratio"]),
        confirmation_range=raw.confirmation_high.gt(raw.confirmation_low),
        confirmation_ohlc=raw[["confirmation_open", "confirmation_high", "confirmation_low", "confirmation_close"]].gt(0).all(axis=1)
                          & raw.confirmation_high.ge(raw[["confirmation_open", "confirmation_close"]].max(axis=1))
                          & raw.confirmation_low.le(raw[["confirmation_open", "confirmation_close"]].min(axis=1)),
        signal_price=raw.signal_close.gt(0),
        confirmation_direction=raw.confirmation_close.gt(raw.confirmation_open)
                               & raw.confirmation_close.gt(raw.signal_close),
        body=raw.body_ratio.between(RELAXED_0925_LONG["minimum_body_ratio"], 1.),
        wick=raw.v9_1m_upper_wick_ratio.between(0., .60),
        confirmation_volume=raw.v9_1m_volume_ratio.ge(1.20),
        liquidity=raw.traded_value.ge(0),
    )
    if "confirmation_source_flagged" in raw:
        checks["real_confirmation"] = raw.confirmation_source_flagged.eq(False)
    if "v9_exact_confirmation_present" in raw:
        checks["confirmation_present"] = raw.v9_exact_confirmation_present.eq(True)
    if "v9_1m_feature_ts" in raw:
        checks["feature_clock"] = pd.to_datetime(raw.v9_1m_feature_ts, utc=True, errors="coerce").eq(raw.confirmation_ts)
    check_frame = pd.DataFrame(checks).fillna(False)
    raw["relaxed_0925_pass"] = check_frame.all(axis=1)
    raw["relaxed_0925_failed_rules"] = pd.Series(
        [";".join(check_frame.columns[~row]) for row in check_frame.to_numpy(bool)], index=raw.index, dtype=str)
    original["day"] = pd.to_datetime(original.day).dt.date
    if not original.day.eq(session_date).all():
        raise ValueError("Original orders escaped the requested session")
    original["relaxed_0925_added"] = False
    held = original.loc[original.setup_id.eq("0926_LONG")]
    if len(held) > 1:
        raise ValueError("Original 09:25 LONG exceeds one-order quota")
    ranked = raw.loc[raw.relaxed_0925_pass].sort_values(
        ["traded_value", "tradingsymbol"], ascending=[False, True], kind="stable")
    ranked["relaxed_0925_rank"] = np.arange(1, len(ranked) + 1)
    raw["relaxed_0925_rank"] = ranked.relaxed_0925_rank
    additions = ranked.iloc[:0 if len(held) else 1].copy()
    additions["sid"] = np.arange(int(original.sid.max()) + 1 if len(original) else 0,
                                  (int(original.sid.max()) + 1 if len(original) else 0) + len(additions))
    additions["side"] = "LONG"
    additions["setup_id"] = "0926_LONG"
    additions["hhmm"] = "0925"
    additions["hhmm_int"] = 925
    additions["trigger"] = additions.confirmation_high
    additions["wick_ratio"] = additions.v9_1m_upper_wick_ratio
    additions["v9_1m_feature_ts"] = additions.confirmation_ts
    additions["relaxed_0925_added"] = True
    raw["relaxed_0925_added"] = raw.index.isin(additions.index)
    raw["original_slot_occupied"] = bool(len(held))
    frames = [part for part in (original, additions) if not part.empty]
    combined = pd.concat(frames, ignore_index=True, sort=False) if frames else original
    for column in ("signal_ts", "confirmation_ts", "v9_1m_feature_ts"):
        if column in combined:
            combined[column] = pd.to_datetime(combined[column], utc=True).dt.tz_convert("Asia/Kolkata")
    return combined.sort_values(["day", "hhmm_int", "side", "setup_id", "tradingsymbol"], kind="stable").reset_index(drop=True), raw
