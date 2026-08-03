"""Isolated causal research for the three LONG families in intraday_long_strategies.md.

This module only reads the repository/data stores. Every output is written
under this script's own directory.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


VERSION = "isolated_long_3m_20260730_v1"
HERE = Path(__file__).resolve().parent
OUT = HERE / "outputs"
DATA5 = Path(r"C:\TradingData\eqidv2\stocks_indicators_5min_eq_live2")
DATA1 = Path(r"C:\TradingData\eqidv2\stocks_indicators_1min_eq")
NIFTY5 = DATA5 / "NIFTY50_INDEX_stocks_indicators_5min.parquet"
FIVE_SUFFIX = "_stocks_indicators_5min.parquet"
ONE_SUFFIX = "_stocks_indicators_1min.parquet"
N_SESSIONS = 60
TRAIN_N = 40
VALID_N = 10
MAX_PATH_MIN = 60
ENTRY_SLIPPAGE_BPS = 5.0
EXIT_SLIPPAGE_BPS = 5.0
NOTIONAL_RS = 100_000.0
TICK_SIZE = 0.05
FORCED_EXIT_MIN = 15 * 60 + 15

FIVE_COLS = [
    "date", "open", "high", "low", "close", "volume", "gap_filled",
    "opening_snapshot",
]
ONE_COLS = ["date", "open", "high", "low", "close", "volume"]


@dataclass(frozen=True)
class EntryConfig:
    name: str
    strategy: str
    profile: str
    variant: str
    regime: str
    min_traded_value: float
    adx_lo: float
    adx_hi: float
    rsi_lo: float
    rsi_hi: float
    rv_min: float
    score_min: int
    pull_ema_tol: float = 0.20
    pull_vwap_tol: float = 0.25
    max_extension: float = 0.75


@dataclass(frozen=True)
class ExitConfig:
    target_pct: float
    stop_pct: float
    hold_min: int
    mode: str = "fixed"

    @property
    def key(self) -> str:
        return f"{self.mode}_t{self.target_pct:.2f}_s{self.stop_pct:.2f}_h{self.hold_min}"


def _dt(values: pd.Series) -> pd.Series:
    out = pd.to_datetime(values, errors="coerce")
    try:
        out = out.dt.tz_localize(None)
    except TypeError:
        out = out.dt.tz_convert(None)
    return out


def _read(path: Path, cols: list[str]) -> pd.DataFrame:
    available = pq.ParquetFile(path).schema.names
    wanted = [c for c in cols if c in available]
    return pd.read_parquet(path, columns=wanted)


def sessions() -> list[pd.Timestamp]:
    d = pd.read_parquet(NIFTY5, columns=["date"])
    x = _dt(d["date"]).dt.normalize().dropna().drop_duplicates().sort_values()
    return list(x.tail(N_SESSIONS))


def symbol_files() -> tuple[dict[str, Path], dict[str, Path]]:
    five = {
        p.name[: -len(FIVE_SUFFIX)]: p
        for p in DATA5.glob(f"*{FIVE_SUFFIX}")
        if p.name not in {
            "NIFTY50_INDEX_stocks_indicators_5min.parquet",
            "NIFTY50_stocks_indicators_5min.parquet",
            "NIFTY_50_stocks_indicators_5min.parquet",
            "NIFTY500_stocks_indicators_5min.parquet",
            "NIFTY 500_stocks_indicators_5min.parquet",
        }
    }
    one = {p.name[: -len(ONE_SUFFIX)]: p for p in DATA1.glob(f"*{ONE_SUFFIX}")}
    common = sorted(set(five) & set(one))
    return ({k: five[k] for k in common}, {k: one[k] for k in common})


def _wilder(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()


def add_features(raw: pd.DataFrame) -> pd.DataFrame:
    d = raw.copy()
    d["date"] = _dt(d["date"])
    d = d.dropna(subset=["date"]).sort_values("date").drop_duplicates("date", keep="last")
    for c in ("open", "high", "low", "close", "volume"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d["session"] = d["date"].dt.normalize()
    d["minute"] = d["date"].dt.hour * 60 + d["date"].dt.minute
    prev = d["close"].shift()
    tr = pd.concat(
        [(d["high"] - d["low"]).abs(), (d["high"] - prev).abs(), (d["low"] - prev).abs()],
        axis=1,
    ).max(axis=1)
    d["atr"] = _wilder(tr, 14)
    d["atr_pct"] = d["atr"] / d["close"] * 100.0
    delta = d["close"].diff()
    gain = _wilder(delta.clip(lower=0), 14)
    loss = _wilder((-delta).clip(lower=0), 14)
    rs = gain / loss.replace(0, np.nan)
    d["rsi"] = 100.0 - (100.0 / (1.0 + rs))
    up = d["high"].diff()
    down = -d["low"].diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    plus_di = 100.0 * _wilder(plus_dm, 14) / d["atr"].replace(0, np.nan)
    minus_di = 100.0 * _wilder(minus_dm, 14) / d["atr"].replace(0, np.nan)
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    d["adx"] = _wilder(dx, 14)
    d["ema9"] = d["close"].ewm(span=9, adjust=False, min_periods=9).mean()
    d["ema20"] = d["close"].ewm(span=20, adjust=False, min_periods=20).mean()
    d["ema20_slope3"] = d["ema20"] / d["ema20"].shift(3) - 1.0
    low14 = d["low"].rolling(14, min_periods=14).min()
    high14 = d["high"].rolling(14, min_periods=14).max()
    d["stoch_k"] = 100.0 * (d["close"] - low14) / (high14 - low14).replace(0, np.nan)
    d["stoch_d"] = d["stoch_k"].rolling(3, min_periods=3).mean()
    direction = np.sign(d["close"].diff()).fillna(0)
    d["obv"] = (direction * d["volume"].fillna(0)).cumsum()
    d["obv_up5"] = d["obv"] > d.groupby("session")["obv"].shift(5)
    d["adx_inc2"] = (d["adx"] > d["adx"].shift(1)) & (d["adx"].shift(1) > d["adx"].shift(2))
    d["adx_inc3"] = d["adx_inc2"] & (d["adx"].shift(2) > d["adx"].shift(3))
    d["rsi_inc2"] = (d["rsi"] > d["rsi"].shift(1)) & (d["rsi"].shift(1) > d["rsi"].shift(2))
    d["adx_slope"] = d["adx"] - d["adx"].shift(2)
    d["rsi_slope"] = d["rsi"] - d["rsi"].shift(2)
    typical = (d["high"] + d["low"] + d["close"]) / 3.0
    pv = typical * d["volume"].fillna(0)
    cum_pv = pv.groupby(d["session"]).cumsum()
    cum_v = d["volume"].fillna(0).groupby(d["session"]).cumsum()
    fallback = typical.groupby(d["session"]).expanding().mean().reset_index(level=0, drop=True)
    d["avwap"] = (cum_pv / cum_v.replace(0, np.nan)).fillna(fallback)
    d["avwap_ext"] = (d["close"] / d["avwap"] - 1.0) * 100.0
    slot_median = d.groupby("minute", sort=False)["volume"].transform(
        lambda s: s.shift(1).rolling(10, min_periods=5).median()
    )
    d["rel_volume"] = d["volume"] / slot_median.replace(0, np.nan)
    rng = (d["high"] - d["low"]).replace(0, np.nan)
    d["range_atr"] = rng / d["atr"].replace(0, np.nan)
    d["close_loc"] = (d["close"] - d["low"]) / rng
    d["upper_wick_frac"] = (d["high"] - d[["open", "close"]].max(axis=1)) / rng
    d["traded_value"] = d["close"] * d["volume"]
    d["ema9_dist_low"] = ((d["low"] / d["ema9"]) - 1.0).abs() * 100.0
    d["vwap_dist_low"] = ((d["low"] / d["avwap"]) - 1.0).abs() * 100.0
    d["ema9_reclaim"] = (d["close"].shift(1) < d["ema9"].shift(1)) & (d["close"] >= d["ema9"])
    d["prev_high6"] = d.groupby("session")["high"].transform(
        lambda s: s.shift(1).rolling(6, min_periods=6).max()
    )
    d["prev_high10"] = d.groupby("session")["high"].transform(
        lambda s: s.shift(1).rolling(10, min_periods=10).max()
    )
    mid20 = d.groupby("session")["close"].transform(
        lambda s: s.rolling(20, min_periods=20).mean()
    )
    std20 = d.groupby("session")["close"].transform(
        lambda s: s.rolling(20, min_periods=20).std()
    )
    width = 4.0 * std20 / mid20.replace(0, np.nan)
    q25 = width.groupby(d["session"]).transform(
        lambda s: s.shift(1).rolling(20, min_periods=20).quantile(0.25)
    )
    d["bb_compressed"] = width.groupby(d["session"]).shift(1) <= q25
    atr_l1 = d.groupby("session")["atr"].shift(1)
    atr_l2 = d.groupby("session")["atr"].shift(2)
    atr_l3 = d.groupby("session")["atr"].shift(3)
    atr_l4 = d.groupby("session")["atr"].shift(4)
    d["atr_decline3"] = (
        (atr_l1 < atr_l2) & (atr_l2 < atr_l3) & (atr_l3 < atr_l4)
    )
    prior_high5 = d.groupby("session")["high"].transform(
        lambda s: s.shift(1).rolling(5, min_periods=5).max()
    )
    prior_low5 = d.groupby("session")["low"].transform(
        lambda s: s.shift(1).rolling(5, min_periods=5).min()
    )
    d["narrow_compressed"] = d["atr_decline3"] & ((prior_high5 - prior_low5) <= 2.5 * atr_l1)
    for cutoff, label in ((570, "or15"), (575, "or20"), (585, "or30")):
        opening = d["high"].where(d["minute"].between(555, cutoff))
        d[label] = opening.groupby(d["session"]).transform("max")
    d["valid"] = (
        d[["open", "high", "low", "close", "volume"]].notna().all(axis=1)
        & (d["close"] > 0)
        & (d["high"] >= d[["open", "close", "low"]].max(axis=1))
        & (d["low"] <= d[["open", "close", "high"]].min(axis=1))
        & ~d.get("gap_filled", pd.Series(False, index=d.index)).fillna(False).astype(bool)
        & ~d.get("opening_snapshot", pd.Series(False, index=d.index)).fillna(False).astype(bool)
    )
    return d


KEEP = [
    "ticker", "date", "session", "minute", "open", "high", "low", "close", "volume",
    "atr", "atr_pct", "rsi", "adx", "ema9", "ema20", "ema20_slope3", "stoch_k",
    "stoch_d", "obv_up5", "adx_inc2", "adx_inc3", "rsi_inc2", "adx_slope",
    "rsi_slope", "avwap", "avwap_ext", "rel_volume", "range_atr", "close_loc",
    "upper_wick_frac", "traded_value", "ema9_dist_low", "vwap_dist_low",
    "ema9_reclaim", "prev_high6", "prev_high10", "bb_compressed",
    "narrow_compressed", "or15", "or20", "or30",
]


def candidate_rows(d: pd.DataFrame, wanted_sessions: set[pd.Timestamp]) -> pd.DataFrame:
    base = (
        d["session"].isin(wanted_sessions)
        & d["valid"]
        & d["close"].ge(50.0)
        & d["traded_value"].ge(250_000.0)
        & d["atr_pct"].between(0.10, 1.50)
        & d["close"].ge(d["avwap"])
        & d["ema9"].gt(d["ema20"])
        & d["ema20_slope3"].gt(0)
        & d["avwap_ext"].between(-0.02, 1.10)
        & d["range_atr"].le(2.7)
        & d["upper_wick_frac"].le(0.65)
    )
    frames: list[pd.DataFrame] = []
    pull = base & d["minute"].between(570, 885) & (
        d["ema9_dist_low"].le(0.35) | d["vwap_dist_low"].le(0.40) | d["ema9_reclaim"]
    )
    if pull.any():
        x = d.loc[pull, KEEP].copy()
        x["strategy"] = "pullback"
        x["variant"] = "pullback"
        x["break_level"] = np.nan
        frames.append(x)
    for label, cutoff in (("or15", 570), ("or20", 575), ("or30", 585)):
        level = d[label]
        extension = (d["close"] / level - 1.0) * 100.0
        mask = base & d["minute"].between(cutoff + 5, 690) & extension.between(0.01, 0.80)
        if mask.any():
            x = d.loc[mask, KEEP].copy()
            x["strategy"] = "orb"
            x["variant"] = label
            x["break_level"] = level.loc[mask]
            frames.append(x)
    for comp_name, comp in (("bb", d["bb_compressed"]), ("narrow", d["narrow_compressed"])):
        for lookback, level in ((6, d["prev_high6"]), (10, d["prev_high10"])):
            extension = (d["close"] / level - 1.0) * 100.0
            mask = base & d["minute"].between(600, 870) & comp & extension.between(0.0, 0.80)
            if mask.any():
                x = d.loc[mask, KEEP].copy()
                x["strategy"] = "compression"
                x["variant"] = f"{comp_name}{lookback}"
                x["break_level"] = level.loc[mask]
                frames.append(x)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["signal_id"] = (
        out["ticker"].astype(str) + "|" + out["date"].astype(str) + "|"
        + out["strategy"] + "|" + out["variant"]
    )
    return out


def build_candidates(rebuild: bool = False) -> tuple[pd.DataFrame, dict[str, Any]]:
    OUT.mkdir(parents=True, exist_ok=True)
    cache = OUT / "candidate_features.parquet"
    quality_path = OUT / "data_quality.json"
    if cache.exists() and quality_path.exists() and not rebuild:
        return pd.read_parquet(cache), json.loads(quality_path.read_text(encoding="utf-8"))
    sess = sessions()
    wanted = set(sess)
    five, one = symbol_files()
    all_candidates: list[pd.DataFrame] = []
    breadth: list[pd.DataFrame] = []
    excluded: list[dict[str, Any]] = []
    accepted = 0
    for n, (ticker, path) in enumerate(five.items(), 1):
        try:
            raw = _read(path, FIVE_COLS)
            raw["date"] = _dt(raw["date"])
            coverage = int(raw["date"].dt.normalize().isin(wanted).groupby(raw["date"].dt.normalize()).any().sum())
            if coverage < 48:
                excluded.append({"ticker": ticker, "reason": "coverage", "sessions": coverage})
                continue
            warm_start = sess[0] - pd.Timedelta(days=45)
            raw = raw[raw["date"] >= warm_start]
            d = add_features(raw)
            d["ticker"] = ticker
            b = d[d["session"].isin(wanted) & d["valid"] & d["avwap"].notna()]
            if not b.empty:
                breadth.append(
                    b.assign(above=(b["close"] >= b["avwap"]).astype(np.int16))
                    .groupby("date", as_index=False)
                    .agg(above=("above", "sum"), total=("above", "size"))
                )
            c = candidate_rows(d, wanted)
            if not c.empty:
                all_candidates.append(c)
            accepted += 1
        except Exception as exc:
            excluded.append({"ticker": ticker, "reason": f"read_or_feature:{type(exc).__name__}"})
        if n % 100 == 0:
            print(f"[features] {n}/{len(five)} accepted={accepted} candidate_parts={len(all_candidates)}")
    candidates = pd.concat(all_candidates, ignore_index=True) if all_candidates else pd.DataFrame()
    b = pd.concat(breadth, ignore_index=True).groupby("date", as_index=False).sum()
    b["breadth"] = b["above"] / b["total"].replace(0, np.nan)
    candidates = candidates.merge(b[["date", "breadth"]], on="date", how="left")
    nifty = _read(NIFTY5, FIVE_COLS)
    nifty = add_features(nifty)
    nifty["nifty_up"] = (nifty["close"] > nifty["ema20"]) & (nifty["ema9"] > nifty["ema20"])
    candidates = candidates.merge(nifty[["date", "nifty_up"]], on="date", how="left")
    nifty_up = candidates["nifty_up"].astype("boolean").fillna(False).astype(bool)
    candidates["market_regime"] = np.select(
        [
            nifty_up & candidates["breadth"].ge(0.55),
            (~nifty_up) | candidates["breadth"].lt(0.45),
        ],
        ["bullish", "bearish"],
        default="neutral",
    )
    broad_candidate_rows = len(candidates)
    # Resolve only rows that can be selected by at least one declared entry
    # configuration. This preserves the complete search space while avoiding
    # millions of unnecessary one-minute path resolutions.
    union_indices: set[int] = set()
    candidates = candidates.reset_index(drop=True)
    for cfg in entry_configs():
        idx, _ = config_indices(candidates, cfg)
        union_indices.update(idx.tolist())
    candidates = candidates.loc[sorted(union_indices)].reset_index(drop=True)
    candidates.to_parquet(cache, index=False)
    quality = {
        "version": VERSION,
        "five_min_files": len(five),
        "one_min_files": len(one),
        "accepted_tickers": accepted,
        "excluded_count": len(excluded),
        "excluded": excluded,
        "broad_candidate_rows": broad_candidate_rows,
        "candidate_rows": len(candidates),
        "sessions": [str(x.date()) for x in sess],
        "train": [str(sess[0].date()), str(sess[TRAIN_N - 1].date())],
        "validation": [str(sess[TRAIN_N].date()), str(sess[TRAIN_N + VALID_N - 1].date())],
        "test": [str(sess[TRAIN_N + VALID_N].date()), str(sess[-1].date())],
    }
    quality_path.write_text(json.dumps(quality, indent=2), encoding="utf-8")
    pd.DataFrame(excluded).to_csv(OUT / "excluded_tickers.csv", index=False)
    return candidates, quality


def resolve_entries(
    candidates: pd.DataFrame, one_files: dict[str, Path], rebuild: bool = False
) -> tuple[pd.DataFrame, dict[str, np.ndarray], dict[str, int]]:
    entry_cache = OUT / "valid_entries.parquet"
    path_cache = OUT / "paths.npz"
    reject_path = OUT / "entry_reject_counts.json"
    if entry_cache.exists() and path_cache.exists() and reject_path.exists() and not rebuild:
        z = np.load(path_cache)
        return pd.read_parquet(entry_cache), {k: z[k] for k in z.files}, json.loads(reject_path.read_text())
    records: list[dict[str, Any]] = []
    opens: list[np.ndarray] = []
    highs: list[np.ndarray] = []
    lows: list[np.ndarray] = []
    closes: list[np.ndarray] = []
    offsets: list[np.ndarray] = []
    reject: dict[str, int] = {}
    grouped = candidates.groupby("ticker", sort=True)
    for n, (ticker, group) in enumerate(grouped, 1):
        p = one_files.get(str(ticker))
        if p is None:
            reject["missing_1m_file"] = reject.get("missing_1m_file", 0) + len(group)
            continue
        try:
            m = _read(p, ONE_COLS)
            m["date"] = _dt(m["date"])
            m = m.dropna(subset=["date", "open", "high", "low", "close"]).sort_values("date")
            m = m.drop_duplicates("date", keep="last")
            dates = m["date"].to_numpy(dtype="datetime64[ns]")
            op = m["open"].to_numpy(float)
            hi = m["high"].to_numpy(float)
            lo = m["low"].to_numpy(float)
            cl = m["close"].to_numpy(float)
            for idx, r in group.iterrows():
                signal = np.datetime64(pd.Timestamp(r["date"]))
                start = signal + np.timedelta64(1, "m")
                end = signal + np.timedelta64(3, "m")
                a = int(np.searchsorted(dates, start, side="left"))
                b = int(np.searchsorted(dates, end, side="right"))
                trigger = math.ceil((float(r["high"]) + TICK_SIZE) / TICK_SIZE - 1e-9) * TICK_SIZE
                cancel = float(r["low"])
                if r["strategy"] in {"orb", "compression"} and pd.notna(r["break_level"]):
                    cancel = max(cancel, float(r["break_level"]))
                fill_i = -1
                fill_raw = np.nan
                reason = "not_triggered"
                for j in range(a, b):
                    if lo[j] <= cancel:
                        reason = "cancel_level_before_or_ambiguous_trigger"
                        break
                    if hi[j] >= trigger:
                        fill_raw = max(trigger, op[j])
                        if (fill_raw / trigger - 1.0) * 100.0 > 0.20:
                            reason = "entry_gap_over_0.20pct"
                            break
                        fill_i = j
                        reason = ""
                        break
                if fill_i < 0:
                    reject[reason] = reject.get(reason, 0) + 1
                    continue
                entry_price = fill_raw * (1.0 + ENTRY_SLIPPAGE_BPS / 10_000.0)
                forced = pd.Timestamp(dates[fill_i]).normalize() + pd.Timedelta(minutes=FORCED_EXIT_MIN)
                path_end = min(pd.Timestamp(dates[fill_i]) + pd.Timedelta(minutes=MAX_PATH_MIN), forced)
                e = int(np.searchsorted(dates, np.datetime64(path_end), side="right"))
                length = max(0, e - fill_i)
                if length == 0:
                    reject["missing_exit_path"] = reject.get("missing_exit_path", 0) + 1
                    continue
                pad = MAX_PATH_MIN + 1
                po = np.full(pad, np.nan, dtype=np.float32)
                ph = np.full(pad, np.nan, dtype=np.float32)
                pl = np.full(pad, np.nan, dtype=np.float32)
                pc = np.full(pad, np.nan, dtype=np.float32)
                pt = np.full(pad, -1, dtype=np.int16)
                use = min(length, pad)
                po[:use], ph[:use], pl[:use], pc[:use] = (
                    op[fill_i:fill_i + use], hi[fill_i:fill_i + use],
                    lo[fill_i:fill_i + use], cl[fill_i:fill_i + use],
                )
                pt[:use] = (
                    (dates[fill_i:fill_i + use] - dates[fill_i]) / np.timedelta64(1, "m")
                ).astype(np.int16)
                rec = r.to_dict()
                rec.update({
                    "candidate_index": int(idx),
                    "entry_time": pd.Timestamp(dates[fill_i]),
                    "planned_trigger": trigger,
                    "entry_raw": fill_raw,
                    "entry_price": entry_price,
                    "cancel_level": cancel,
                    "qty": max(1, math.floor(NOTIONAL_RS / entry_price)),
                })
                records.append(rec)
                opens.append(po); highs.append(ph); lows.append(pl); closes.append(pc); offsets.append(pt)
        except Exception as exc:
            reject[f"one_minute_error:{type(exc).__name__}"] = reject.get(
                f"one_minute_error:{type(exc).__name__}", 0
            ) + len(group)
        if n % 100 == 0:
            print(f"[entries] {n}/{len(grouped)} valid={len(records)}")
    entries = pd.DataFrame(records).reset_index(drop=True)
    paths = {
        "open": np.vstack(opens) if opens else np.empty((0, MAX_PATH_MIN + 1), np.float32),
        "high": np.vstack(highs) if highs else np.empty((0, MAX_PATH_MIN + 1), np.float32),
        "low": np.vstack(lows) if lows else np.empty((0, MAX_PATH_MIN + 1), np.float32),
        "close": np.vstack(closes) if closes else np.empty((0, MAX_PATH_MIN + 1), np.float32),
        "offset": np.vstack(offsets) if offsets else np.empty((0, MAX_PATH_MIN + 1), np.int16),
    }
    entries.to_parquet(entry_cache, index=False)
    np.savez_compressed(path_cache, **paths)
    reject_path.write_text(json.dumps(reject, indent=2), encoding="utf-8")
    return entries, paths, reject


PROFILES = {
    "pullback": {
        "loose": (14, 40, 48, 72, 0.90, 5),
        "normal": (16, 36, 50, 68, 1.15, 6),
        "strict": (18, 32, 52, 66, 1.30, 7),
    },
    "orb": {
        "loose": (16, 42, 50, 74, 1.00, 5),
        "normal": (18, 36, 53, 72, 1.20, 6),
        "strict": (20, 34, 55, 70, 1.35, 7),
    },
    "compression": {
        "loose": (12, 34, 48, 72, 0.90, 4),
        "normal": (14, 30, 50, 70, 1.10, 5),
        "strict": (14, 28, 52, 68, 1.25, 6),
    },
}


def entry_configs() -> list[EntryConfig]:
    out: list[EntryConfig] = []
    n = 0
    for strategy in ("pullback", "orb", "compression"):
        variants = {
            "pullback": ["pullback"],
            "orb": ["or15", "or20", "or30"],
            "compression": ["bb6", "bb10", "narrow6", "narrow10"],
        }[strategy]
        for profile, vals in PROFILES[strategy].items():
            adx_lo, adx_hi, rsi_lo, rsi_hi, rv, score = vals
            for variant in variants:
                for regime in ("all", "not_bearish", "bullish"):
                    for liquidity in (1_000_000.0, 2_500_000.0):
                        n += 1
                        out.append(EntryConfig(
                            name=f"{strategy}_{n:03d}", strategy=strategy, profile=profile,
                            variant=variant, regime=regime, min_traded_value=liquidity,
                            adx_lo=adx_lo, adx_hi=adx_hi, rsi_lo=rsi_lo, rsi_hi=rsi_hi,
                            rv_min=rv, score_min=score,
                            pull_ema_tol={"loose": .30, "normal": .20, "strict": .15}[profile],
                            pull_vwap_tol={"loose": .35, "normal": .25, "strict": .20}[profile],
                            max_extension={"loose": .90, "normal": .75, "strict": .60}[profile],
                        ))
    return out


def config_indices(entries: pd.DataFrame, cfg: EntryConfig) -> tuple[np.ndarray, pd.Series]:
    x = entries
    common = (
        x["strategy"].eq(cfg.strategy)
        & x["variant"].eq(cfg.variant)
        & x["traded_value"].ge(cfg.min_traded_value)
        & x["atr_pct"].between(0.15, 0.90)
        & x["avwap_ext"].le(cfg.max_extension)
        & x["range_atr"].le(2.2)
        & x["upper_wick_frac"].le(0.45)
    )
    if cfg.regime == "not_bearish":
        common &= x["market_regime"].ne("bearish")
    elif cfg.regime == "bullish":
        common &= x["market_regime"].eq("bullish")
    adx_ok = x["adx"].between(cfg.adx_lo, cfg.adx_hi)
    rsi_ok = x["rsi"].between(cfg.rsi_lo, cfg.rsi_hi)
    stoch_ok = (x["stoch_k"] > x["stoch_d"]) & x["stoch_k"].between(20, 88)
    if cfg.strategy == "pullback":
        structure = (
            x["ema9_dist_low"].le(cfg.pull_ema_tol)
            | x["vwap_dist_low"].le(cfg.pull_vwap_tol)
            | x["ema9_reclaim"]
        )
        score = (
            adx_ok.astype(int) + x["adx_inc2"].astype(int)
            + rsi_ok.astype(int) + x["rsi_inc2"].astype(int)
            + (x["stoch_k"] > x["stoch_d"]).astype(int)
            + x["stoch_k"].between(30, 78).astype(int)
            + x["rel_volume"].ge(cfg.rv_min).astype(int)
            + x["obv_up5"].astype(int) + x["close_loc"].ge(.60).astype(int)
        )
    elif cfg.strategy == "orb":
        structure = pd.Series(True, index=x.index)
        score = (
            adx_ok.astype(int) + x["adx_inc2"].astype(int)
            + rsi_ok.astype(int) + x["rsi_inc2"].astype(int)
            + stoch_ok.astype(int) + 2 * x["rel_volume"].ge(cfg.rv_min).astype(int)
            + x["obv_up5"].astype(int) + x["close_loc"].ge(.65).astype(int)
            + x["market_regime"].eq("bullish").astype(int)
        )
    else:
        structure = pd.Series(True, index=x.index)
        score = (
            adx_ok.astype(int) + x["adx_inc3"].astype(int)
            + rsi_ok.astype(int) + x["rsi_inc2"].astype(int)
            + stoch_ok.astype(int) + x["rel_volume"].ge(cfg.rv_min).astype(int)
            + x["obv_up5"].astype(int) + x["close_loc"].ge(.60).astype(int)
        )
    mask = common & structure & (score >= cfg.score_min)
    chosen = x.loc[mask].assign(_score=score.loc[mask]).sort_values(
        ["ticker", "session", "date", "_score"], ascending=[True, True, True, False]
    )
    chosen = chosen.drop_duplicates(["ticker", "session"], keep="first")
    return chosen.index.to_numpy(dtype=int), score


def exit_grid(strategy: str) -> list[ExitConfig]:
    holds = {
        "pullback": (20, 30, 40),
        "orb": (30, 40, 50),
        "compression": (40, 50, 60),
    }[strategy]
    return [
        ExitConfig(t, s, h)
        for t in (0.35, 0.45, 0.55, 0.65, 0.75, 0.85)
        for s in (0.40, 0.50, 0.60, 0.70, 0.80, 0.90)
        for h in holds
    ]


def _costs(entry: np.ndarray, exit_: np.ndarray, qty: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    buy = entry * qty
    sell = exit_ * qty
    turnover = buy + sell
    brokerage = np.minimum(20.0, .0003 * buy) + np.minimum(20.0, .0003 * sell)
    stt = .00025 * sell
    exch = .0000297 * turnover
    sebi = .000001 * turnover
    ipft = .000001 * turnover
    stamp = .00003 * buy
    gst = .18 * (brokerage + exch + sebi + ipft)
    total = brokerage + stt + exch + sebi + ipft + stamp + gst
    return total, (exit_ - entry) * qty - total


def simulate(
    entries: pd.DataFrame,
    paths: dict[str, np.ndarray],
    indices: np.ndarray,
    cfg: ExitConfig,
    extra_slippage_mult: float = 1.0,
) -> pd.DataFrame:
    if len(indices) == 0:
        return pd.DataFrame()
    e = entries.loc[indices]
    entry = e["entry_price"].to_numpy(float)
    qty = e["qty"].to_numpy(float)
    hi = paths["high"][indices].astype(float)
    lo = paths["low"][indices].astype(float)
    op = paths["open"][indices].astype(float)
    cl = paths["close"][indices].astype(float)
    off = paths["offset"][indices]
    valid = np.isfinite(cl) & (off >= 0) & (off <= cfg.hold_min)
    target_pct = np.full(len(indices), cfg.target_pct, float)
    stop_pct = np.full(len(indices), cfg.stop_pct, float)
    valid_stop = np.ones(len(indices), dtype=bool)
    if cfg.mode == "atr":
        atrp = e["atr_pct"].to_numpy(float)
        if e["strategy"].iloc[0] == "pullback":
            target_pct = np.clip(.75 * atrp, .35, .55)
            stop_pct = np.clip(1.10 * atrp, .55, .80)
        elif e["strategy"].iloc[0] == "orb":
            target_pct = np.clip(.90 * atrp, .45, .70)
            stop_pct = np.clip(1.10 * atrp, .60, .85)
        else:
            target_pct = np.clip(1.00 * atrp, .55, .80)
            stop_pct = np.clip(.90 * atrp, .50, .70)
    elif cfg.mode == "structural":
        structural = (entry - e["cancel_level"].to_numpy(float)) / entry * 100.0 + .05
        stop_pct = np.minimum(stop_pct, structural)
        valid_stop = stop_pct >= .30
    tp = entry * (1.0 + target_pct / 100.0)
    sp = entry * (1.0 - stop_pct / 100.0)
    target_hit = (hi >= tp[:, None]) & valid
    stop_hit = (lo <= sp[:, None]) & valid
    event = target_hit | stop_hit
    has = event.any(axis=1)
    first = np.where(has, event.argmax(axis=1), 0)
    rows = np.arange(len(indices))
    is_stop = has & stop_hit[rows, first]
    is_target = has & ~is_stop
    last = np.maximum(0, valid.sum(axis=1) - 1)
    event_i = np.where(has, first, last)
    exit_raw = cl[rows, event_i]
    exit_raw = np.where(is_target, tp, exit_raw)
    exit_raw = np.where(is_stop, np.minimum(sp, op[rows, event_i]), exit_raw)
    slip = EXIT_SLIPPAGE_BPS * extra_slippage_mult / 10_000.0
    exit_price = np.where(is_target, exit_raw, exit_raw * (1.0 - slip))
    total_cost, net = _costs(entry, exit_price, qty)
    reason = np.where(is_stop, "STOP", np.where(is_target, "TARGET", "TIME"))
    result = e[[
        "signal_id", "ticker", "session", "strategy", "variant", "date", "entry_time",
        "planned_trigger", "entry_price", "qty", "market_regime", "adx", "rsi",
        "stoch_k", "stoch_d", "avwap_ext", "rel_volume", "atr_pct", "obv_up5",
        "range_atr", "upper_wick_frac",
    ]].copy()
    result["target_pct"] = target_pct
    result["stop_pct"] = stop_pct
    result["hold_min"] = cfg.hold_min
    result["exit_price"] = exit_price
    result["exit_reason"] = reason
    result["exit_offset_min"] = off[rows, event_i]
    result["gross_pnl"] = (exit_price - entry) * qty
    result["cost_rs"] = total_cost
    result["net_pnl"] = net
    result["mfe_pct"] = (np.nanmax(np.where(valid, hi, np.nan), axis=1) / entry - 1.0) * 100.0
    result["mae_pct"] = (np.nanmin(np.where(valid, lo, np.nan), axis=1) / entry - 1.0) * 100.0
    result = result.loc[valid_stop].copy()
    return result


def metrics(trades: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {
            "trades": 0, "net_profit": 0.0, "profit_factor": 0.0,
            "expectancy": 0.0, "win_rate": 0.0, "max_drawdown": 0.0,
            "active_days": 0, "profitable_days_pct": 0.0,
        }
    p = trades["net_pnl"].astype(float)
    wins = p[p > 0].sum()
    losses = -p[p < 0].sum()
    daily = trades.groupby("session")["net_pnl"].sum().sort_index()
    curve = p.cumsum()
    dd = curve - curve.cummax()
    positive_days = daily[daily > 0].sum()
    ticker = trades.groupby("ticker")["net_pnl"].sum()
    positive_tickers = ticker[ticker > 0].sum()
    return {
        "trades": int(len(trades)),
        "net_profit": round(float(p.sum()), 2),
        "gross_profit": round(float(wins), 2),
        "gross_loss": round(float(losses), 2),
        "profit_factor": round(float(wins / losses), 4) if losses > 0 else 99.0,
        "expectancy": round(float(p.mean()), 3),
        "win_rate": round(float((p > 0).mean() * 100.0), 3),
        "max_drawdown": round(float(dd.min()), 2),
        "active_days": int(daily.size),
        "profitable_days_pct": round(float((daily > 0).mean() * 100.0), 2),
        "top_day_share_pct": round(float(daily.max() / positive_days * 100.0), 2) if positive_days > 0 else 0.0,
        "top_ticker_share_pct": round(float(ticker.max() / positive_tickers * 100.0), 2) if positive_tickers > 0 else 0.0,
        "cost_rs": round(float(trades["cost_rs"].sum()), 2),
    }


def split_indices(entries: pd.DataFrame, indices: np.ndarray, split: str) -> np.ndarray:
    sess = sessions()
    ranges = {
        "train": set(sess[:TRAIN_N]),
        "validation": set(sess[TRAIN_N:TRAIN_N + VALID_N]),
        "test": set(sess[TRAIN_N + VALID_N:]),
    }
    return np.array([i for i in indices if entries.at[i, "session"] in ranges[split]], dtype=int)


def anchor_exits(strategy: str) -> list[ExitConfig]:
    return {
        "pullback": [ExitConfig(.45, .65, 30), ExitConfig(.55, .60, 30), ExitConfig(.45, .50, 40)],
        "orb": [ExitConfig(.55, .70, 40), ExitConfig(.65, .60, 40), ExitConfig(.45, .50, 50)],
        "compression": [ExitConfig(.65, .60, 50), ExitConfig(.55, .60, 50), ExitConfig(.75, .70, 60)],
    }[strategy]


def research(rebuild: bool = False) -> int:
    candidates, quality = build_candidates(rebuild)
    _, one = symbol_files()
    entries, paths, rejects = resolve_entries(candidates, one, rebuild)
    print(f"[research] candidates={len(candidates)} valid_entries={len(entries)}")
    config_map = {c.name: c for c in entry_configs()}
    anchor_rows: list[dict[str, Any]] = []
    config_idx: dict[str, np.ndarray] = {}
    for n, cfg in enumerate(config_map.values(), 1):
        idx, _ = config_indices(entries, cfg)
        config_idx[cfg.name] = idx
        train_idx = split_indices(entries, idx, "train")
        anchor_metrics = [metrics(simulate(entries, paths, train_idx, ex)) for ex in anchor_exits(cfg.strategy)]
        best = max(anchor_metrics, key=lambda m: (m["profit_factor"], m["expectancy"]))
        anchor_rows.append({"config": cfg.name, "strategy": cfg.strategy, **best})
        if n % 25 == 0:
            print(f"[anchors] {n}/{len(config_map)}")
    anchor_df = pd.DataFrame(anchor_rows)
    anchor_df.to_csv(OUT / "entry_anchor_results.csv", index=False)
    shortlisted: list[str] = []
    for strategy in ("pullback", "orb", "compression"):
        part = anchor_df[(anchor_df["strategy"] == strategy) & (anchor_df["trades"] >= 150)]
        part = part.sort_values(["profit_factor", "expectancy", "trades"], ascending=False).head(18)
        shortlisted.extend(part["config"].tolist())
    grid_rows: list[dict[str, Any]] = []
    for n, name in enumerate(shortlisted, 1):
        cfg = config_map[name]
        train_idx = split_indices(entries, config_idx[name], "train")
        for ex in exit_grid(cfg.strategy):
            m = metrics(simulate(entries, paths, train_idx, ex))
            grid_rows.append({
                "config": name, "strategy": cfg.strategy, "exit": ex.key,
                **asdict(ex), **{f"train_{k}": v for k, v in m.items()},
            })
        print(f"[exit-grid] {n}/{len(shortlisted)} {name}")
    grid = pd.DataFrame(grid_rows)
    grid.to_csv(OUT / "train_exit_grid.csv", index=False)
    eligible = grid[
        (grid["train_trades"] >= 200)
        & (grid["train_profit_factor"] >= 1.05)
        & (grid["train_expectancy"] > 0)
    ].copy()
    if eligible.empty:
        eligible = grid.sort_values(["train_profit_factor", "train_expectancy"], ascending=False).groupby(
            "strategy", as_index=False
        ).head(20)
    else:
        eligible = eligible.sort_values(
            ["train_profit_factor", "train_expectancy"], ascending=False
        ).groupby("strategy", as_index=False).head(30)
    validation_rows: list[dict[str, Any]] = []
    for _, r in eligible.iterrows():
        cfg = config_map[r["config"]]
        ex = ExitConfig(float(r["target_pct"]), float(r["stop_pct"]), int(r["hold_min"]), str(r["mode"]))
        val_idx = split_indices(entries, config_idx[cfg.name], "validation")
        vm = metrics(simulate(entries, paths, val_idx, ex))
        validation_rows.append({
            **r.to_dict(), **{f"validation_{k}": v for k, v in vm.items()}
        })
    validation = pd.DataFrame(validation_rows)
    validation.to_csv(OUT / "validation_results.csv", index=False)
    finalists: list[dict[str, Any]] = []
    for strategy in ("pullback", "orb", "compression"):
        part = validation[validation["strategy"] == strategy].copy()
        stable = part[
            (part["train_profit_factor"] >= 1.10)
            & (part["train_expectancy"] > 0)
            & (part["validation_trades"] >= 40)
            & (part["validation_profit_factor"] >= 1.05)
            & (part["validation_expectancy"] > 0)
        ]
        pool = stable if not stable.empty else part
        if pool.empty:
            continue
        pool = pool.assign(
            selection_score=np.minimum(pool["train_profit_factor"], pool["validation_profit_factor"])
            + .0005 * np.minimum(pool["train_trades"], 1000)
        ).sort_values(["selection_score", "validation_expectancy"], ascending=False)
        finalists.append(pool.iloc[0].to_dict())
    # TEST is touched only here, after finalists have been frozen.
    final_payload: list[dict[str, Any]] = []
    for row in finalists:
        cfg = config_map[row["config"]]
        ex = ExitConfig(float(row["target_pct"]), float(row["stop_pct"]), int(row["hold_min"]), str(row["mode"]))
        test_idx = split_indices(entries, config_idx[cfg.name], "test")
        trades = simulate(entries, paths, test_idx, ex)
        tm = metrics(trades)
        stress = metrics(simulate(entries, paths, test_idx, ex, extra_slippage_mult=1.5))
        # Required alternate exit models from the brief.
        alternates = {}
        for mode in ("atr", "structural"):
            alt = ExitConfig(ex.target_pct, ex.stop_pct, ex.hold_min, mode)
            alternates[mode] = metrics(simulate(entries, paths, test_idx, alt))
        strategy_dir = OUT / "finalists" / cfg.strategy
        strategy_dir.mkdir(parents=True, exist_ok=True)
        trades.to_csv(strategy_dir / "test_trades.csv", index=False)
        daily = trades.groupby("session", as_index=False)["net_pnl"].sum()
        daily.to_csv(strategy_dir / "test_daily.csv", index=False)
        ticker = trades.groupby("ticker", as_index=False).agg(
            trades=("net_pnl", "size"), net_pnl=("net_pnl", "sum")
        ).sort_values("net_pnl", ascending=False)
        ticker.to_csv(strategy_dir / "test_ticker.csv", index=False)
        final_payload.append({
            "entry_config": asdict(cfg), "exit_config": asdict(ex),
            "train": {k.removeprefix("train_"): row[k] for k in row if k.startswith("train_")},
            "validation": {
                k.removeprefix("validation_"): row[k] for k in row if k.startswith("validation_")
            },
            "test": tm, "slippage_150pct": stress, "alternate_exits_test": alternates,
        })
    accepted = []
    for f in final_payload:
        t, s = f["test"], f["slippage_150pct"]
        if (
            t["trades"] >= 100 and t["profit_factor"] > 1.40 and t["expectancy"] > 0
            and t["top_ticker_share_pct"] < 10 and t["top_day_share_pct"] < 20
            and s["profit_factor"] > 1.0 and s["expectancy"] > 0
        ):
            accepted.append(f["entry_config"]["strategy"])
    payload = {
        "version": VERSION,
        "decision": "PROFITABLE_ROBUST_STRATEGIES_FOUND" if accepted else "NO_STRATEGY_PASSED_ALL_GATES",
        "accepted_strategies": accepted,
        "split": {
            "train": quality["train"], "validation": quality["validation"], "test": quality["test"]
        },
        "quality": quality,
        "entry_reject_counts": rejects,
        "selection": {
            "entry_configs": len(config_map), "shortlisted": len(shortlisted),
            "train_exit_combinations": len(grid), "validation_combinations": len(validation),
        },
        "finalists": final_payload,
    }
    (OUT / "research_results.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    write_report(payload)
    print(f"[done] decision={payload['decision']} accepted={accepted}")
    return 0


def write_report(payload: dict[str, Any]) -> None:
    lines = [
        "# Three-month intraday LONG research",
        "",
        f"## Decision: {payload['decision']}",
        "",
        "This isolated harness does not import existing repository code for execution and writes only "
        "beneath its own directory. Unrelated concurrent workspace changes are left untouched.",
        "",
        "## Chronological split",
        "",
        f"- TRAIN: {payload['split']['train'][0]} to {payload['split']['train'][1]} (40 sessions)",
        f"- VALIDATION: {payload['split']['validation'][0]} to {payload['split']['validation'][1]} (10 sessions)",
        f"- Untouched TEST: {payload['split']['test'][0]} to {payload['split']['test'][1]} (10 sessions)",
        "",
        "Entry configurations and broad target/stop/time-exit regions were screened on TRAIN. "
        "VALIDATION selected one frozen finalist per strategy family. TEST was evaluated only afterward.",
        "",
        "## Execution assumptions",
        "",
        "- Completed five-minute signals only; entry can begin one minute later.",
        "- Trigger is signal high plus one NSE tick, valid for three one-minute bars.",
        "- Cancel first when trigger and invalidation are ambiguous in the same minute.",
        "- Reject gaps over 0.20%; apply 5 bps adverse entry slippage.",
        "- Resolve target/stop using one-minute OHLC; same-minute ties go to stop.",
        "- Stop gaps fill at the worse open; non-target exits receive 5 bps adverse slippage.",
        "- Forced exit no later than 15:15 and Zerodha-style 2026 statutory costs.",
        "- Constant approximately Rs 100,000 notional per trade for comparable strategy returns.",
        "",
        "## Frozen finalists",
        "",
        "| Strategy | Profile/variant | Target | Stop | Time | Train PF | Validation PF | Test trades | Test PF | Test expectancy | Test net | 150% slip PF | Verdict |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    accepted = set(payload["accepted_strategies"])
    for f in payload["finalists"]:
        c, e, tr, va, te, stress = (
            f["entry_config"], f["exit_config"], f["train"], f["validation"],
            f["test"], f["slippage_150pct"],
        )
        verdict = "ACCEPT" if c["strategy"] in accepted else "REJECT"
        lines.append(
            f"| {c['strategy']} | {c['profile']}/{c['variant']} | {e['target_pct']:.2f}% | "
            f"{e['stop_pct']:.2f}% | {e['hold_min']}m | {float(tr.get('profit_factor', 0)):.3f} | "
            f"{float(va.get('profit_factor', 0)):.3f} | {int(te['trades'])} | "
            f"{te['profit_factor']:.3f} | Rs {te['expectancy']:,.2f} | Rs {te['net_profit']:,.2f} | "
            f"{stress['profit_factor']:.3f} | {verdict} |"
        )
    lines += [
        "",
        "Acceptance requires untouched TEST PF above 1.40, positive expectancy after all costs, "
        "at least 100 TEST trades, low ticker/day concentration, and survival under 50% extra slippage.",
        "",
        "## Search coverage",
        "",
        f"- Accepted ticker files: {payload['quality']['accepted_tickers']}",
        f"- Broad structural candidate rows: {payload['quality'].get('broad_candidate_rows', payload['quality']['candidate_rows'])}",
        f"- Rows in the union of all selectable configurations: {payload['quality']['candidate_rows']}",
        f"- Entry configurations: {payload['selection']['entry_configs']}",
        f"- Shortlisted entry configurations: {payload['selection']['shortlisted']}",
        f"- TRAIN target/stop/time combinations: {payload['selection']['train_exit_combinations']}",
        f"- VALIDATION combinations: {payload['selection']['validation_combinations']}",
        "- Fixed exit grid: targets 0.35%-0.85%, stops 0.40%-0.90%, three nearby time exits per family.",
        "- ATR-adjusted and structural stops are reported separately for each frozen TEST finalist.",
        "",
        "Detailed configurations, metrics, rejected entry counts, trades, daily summaries, ticker "
        "summaries, and the full TRAIN/VALIDATION grids are stored beside this report.",
    ]
    (OUT / "RESEARCH_REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["research", "report"], nargs="?", default="research")
    p.add_argument("--rebuild", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass
    if args.mode == "report":
        path = OUT / "research_results.json"
        if not path.exists():
            raise SystemExit("No research_results.json; run research first.")
        write_report(json.loads(path.read_text(encoding="utf-8")))
        return 0
    return research(args.rebuild)


if __name__ == "__main__":
    raise SystemExit(main())
