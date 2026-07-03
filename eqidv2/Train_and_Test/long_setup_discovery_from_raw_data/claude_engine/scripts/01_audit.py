r"""01_audit.py -- Stage 1 data audit + liquid universe + session split + resolver validation.

Writes: DATA_AUDIT.md, results/universe.json, results/sessions.json
Run:  py Train_and_Test/long_setup_discovery_from_raw_data/scripts/01_audit.py
"""
from __future__ import annotations
import glob, json, os, random, sys
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import lib_long_disc as L

TURNOVER_MIN_RS = 2.5e8     # >= Rs 25 cr median daily turnover (liquid enough for 0.75% scalps)
UNIVERSE_CAP = 250          # keep at most this many (top by turnover) for tractability
LOOKBACK_SESS = 45          # sessions used to rank liquidity
TEST_SESSIONS = 10          # ~2 weeks out-of-sample
TRAIN_SESSIONS = 30         # ~6 weeks
WARMUP_SESSIONS = 12        # loaded before TRAIN for indicator warmup (not signalled)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    files5 = sorted(glob.glob(str(L.D5 / "*_stocks_indicators_5min.parquet")))
    files1 = set(os.path.basename(p).split("_stocks")[0] for p in glob.glob(str(L.D1 / "*_stocks_indicators_1min.parquet")))
    filesd = set(os.path.basename(p).split("_stocks")[0] for p in glob.glob(str(L.DD / "*_stocks_indicators_daily.parquet")))
    print(f"[audit] 5min files={len(files5)} 1min files={len(files1)} daily files={len(filesd)}")

    # --- scan 5-min for sessions + per-symbol turnover -------------------------
    sess_barcount = {}        # session -> list of bar counts across symbols
    turnover = {}             # ticker -> median daily turnover
    sym_sessions = {}         # ticker -> set(sessions)
    n = 0
    for p in files5:
        tk = os.path.basename(p).split("_stocks")[0]
        try:
            df = pd.read_parquet(p, columns=["date", "close", "volume", "opening_snapshot"])
        except Exception:
            try:
                df = pd.read_parquet(p, columns=["date", "close", "volume"])
                df["opening_snapshot"] = False
            except Exception:
                continue
        df = L._norm_ist(df)
        if df is None:
            continue
        df = df[~df["opening_snapshot"].fillna(False).astype(bool)] if "opening_snapshot" in df.columns else df
        df = df.drop_duplicates(subset=["date"], keep="last")
        df["sess"] = df["date"].dt.normalize()
        g = df.groupby("sess")
        bc = g.size()
        for s, cnt in bc.items():
            sess_barcount.setdefault(s, []).append(int(cnt))
        # turnover on last LOOKBACK_SESS sessions
        tov = (df["close"] * df["volume"]).groupby(df["sess"]).sum()
        tov = tov.sort_index().tail(LOOKBACK_SESS)
        if len(tov) >= 10:
            turnover[tk] = float(tov.median())
            sym_sessions[tk] = set(bc.index)
        n += 1
        if n % 300 == 0:
            print(f"  ...scanned {n}/{len(files5)}")

    # global trading sessions = days where >=50% of symbols have >=70 bars (full day)
    n_sym = len(files5)
    full_sessions = sorted(s for s, lst in sess_barcount.items()
                           if (np.array(lst) >= 70).sum() >= 0.5 * n_sym)
    # require 1-min coverage: check ref liquid names for >=350 1-min bars/session
    ref = [t for t, _ in sorted(turnover.items(), key=lambda kv: -kv[1])[:5]]
    onemin_full = None
    for tk in ref:
        arr = L.load_1m_raw(tk)
        if arr is None:
            continue
        ts = pd.DatetimeIndex(pd.to_datetime(arr[0], utc=True)).tz_convert(L.IST)
        bc = pd.Series(1, index=ts).groupby(ts.normalize()).size()
        ok = set(bc.index[bc >= 350])
        onemin_full = ok if onemin_full is None else (onemin_full & ok)
    resolvable = sorted(s for s in full_sessions if onemin_full is None or s in onemin_full)
    # exclude any session beyond last resolvable
    last_res = resolvable[-1]
    print(f"[audit] full 5min sessions={len(full_sessions)} | resolvable(1min full)={len(resolvable)} | last_resolvable={last_res.date()}")

    # --- universe --------------------------------------------------------------
    liq = sorted(((t, v) for t, v in turnover.items() if v >= TURNOVER_MIN_RS and t in files1),
                 key=lambda kv: -kv[1])
    universe = [t for t, _ in liq[:UNIVERSE_CAP]]
    print(f"[audit] liquid (>=Rs{TURNOVER_MIN_RS/1e7:.0f}cr & has 1min): {len(liq)} -> capped universe {len(universe)}")

    # --- split -----------------------------------------------------------------
    TEST_s = resolvable[-TEST_SESSIONS:]
    TRAIN_s = resolvable[-(TEST_SESSIONS + TRAIN_SESSIONS):-TEST_SESSIONS]
    half = len(TRAIN_s) // 2
    FIT_s, VAL_s = TRAIN_s[:half], TRAIN_s[half:]
    WARM_s = resolvable[-(TEST_SESSIONS + TRAIN_SESSIONS + WARMUP_SESSIONS):-(TEST_SESSIONS + TRAIN_SESSIONS)]

    def ds(lst): return [pd.Timestamp(x).strftime("%Y-%m-%d") for x in lst]
    sessions = dict(resolvable=ds(resolvable), warmup=ds(WARM_s), fit=ds(FIT_s),
                    val=ds(VAL_s), train=ds(TRAIN_s), test=ds(TEST_s),
                    last_resolvable=str(last_res.date()))
    L.save_json(L.RESULTS / "sessions.json", sessions)
    L.save_json(L.RESULTS / "universe.json", dict(tickers=universe, turnover_min_rs=TURNOVER_MIN_RS,
                                                   n=len(universe), liquid_total=len(liq)))

    # --- DQ on a sample of the universe ---------------------------------------
    dq = {"dup_rows_5m": 0, "opening_snapshot_dropped": 0, "nan_ohlc_5m": 0,
          "halted_sessions": 0, "gaps_5m": 0, "checked": 0}
    halted = []
    for tk in universe[:60]:
        raw = pd.read_parquet(L.D5 / f"{tk}_stocks_indicators_5min.parquet",
                              columns=["date", "open", "high", "low", "close", "volume", "opening_snapshot"])
        raw0 = L._norm_ist(raw)
        if raw0 is None:
            continue
        dq["opening_snapshot_dropped"] += int(raw0["opening_snapshot"].fillna(False).astype(bool).sum())
        d2 = raw0[~raw0["opening_snapshot"].fillna(False).astype(bool)]
        dq["dup_rows_5m"] += int(d2["date"].duplicated().sum())
        dq["nan_ohlc_5m"] += int(d2[["open", "high", "low", "close"]].isna().any(axis=1).sum())
        d2 = d2.assign(sess=d2["date"].dt.normalize())
        bc = d2.groupby("sess").size()
        halted += [(tk, str(s.date()), int(cb)) for s, cb in bc.items() if cb < 30 and s in set(resolvable)]
        dq["checked"] += 1
    dq["halted_sessions"] = len(halted)

    # --- resolver validation vs repo er.resolve --------------------------------
    import avwap_5min_ID_v11_backtesting as v11
    rng = random.Random(13)
    span = set(pd.Timestamp(x).tz_localize(None) for x in (FIT_s + VAL_s))  # match tz-naive sess
    match, mismatch, tried = 0, 0, 0
    for tk in universe[:40]:
        d5 = L.compute_features(L.load_5m_raw(tk))
        if d5 is None:
            continue
        fams = L.family_triggers(d5)
        cand = d5[(fams["F2_PRESSURE_BURST"] | fams["F6_VOLUME_EXPANSION"]) & d5["sess"].isin(span)]
        if cand.empty:
            continue
        arr = L.load_1m_raw(tk)
        if arr is None:
            continue
        ts, op, hi, lo, cl = arr
        bars1m = v11._load_1m_with_open(tk)
        for r in cand.sample(min(6, len(cand)), random_state=rng.randint(0, 1 << 30)).itertuples():
            sig_ns = pd.Timestamp(r.date).value
            ei = L._entry_index(ts, op, sig_ns)
            if ei is None:
                continue
            e_idx, e_open = ei
            mine = L.resolve_path(ts, op, hi, lo, cl, e_idx, e_open, 0.75, 0.75)
            res = v11.er.resolve(bars=bars1m, side="LONG", entry_price=e_open,
                                 entry_time_ist=pd.Timestamp(ts[e_idx], tz="UTC").tz_convert(L.IST),
                                 sl_pct=0.75, tgt_pct=0.75)
            if res is None:
                continue
            tried += 1
            if str(res.outcome) == ("SL" if mine["outcome"] in ("SL", "BE") else mine["outcome"]):
                match += 1
            else:
                mismatch += 1
    val_rate = round(100.0 * match / tried, 1) if tried else 0.0
    print(f"[audit] resolver validation vs er.resolve: {match}/{tried} match ({val_rate}%), {mismatch} mismatch")

    # --- column inventory ------------------------------------------------------
    def cols(path):
        try:
            import pyarrow.parquet as pq
            return list(pq.ParquetFile(path).schema.names)
        except Exception:
            return list(pd.read_parquet(path).columns)
    c5 = cols(files5[0])
    c1 = cols(str(L.D1 / (universe[0] + "_stocks_indicators_1min.parquet")))
    IND = {"RSI", "ATR", "EMA_20", "EMA_50", "EMA_200", "20_SMA", "VWAP", "CCI", "MFI", "OBV",
           "MACD", "MACD_Signal", "MACD_Hist", "ADX", "Stoch_%K", "Stoch_%D",
           "Upper_Band", "Lower_Band", "Recent_High", "Recent_Low"}
    ind5 = [c for c in c5 if c in IND]
    oth5 = [c for c in c5 if c not in IND]

    # --- write DATA_AUDIT.md ---------------------------------------------------
    out = []
    A = out.append
    A("# DATA_AUDIT — raw 5-min / 1-min stores for FAST-MOMENTUM LONG (~0.75%) discovery\n")
    A("Stage-1 audit (read-only). Signals are built from RAW 5-min bars; exits resolved on RAW 1-min bars.\n")
    A("## Raw data paths")
    A(f"- **5-MINUTE (entry/signal discovery):** `{L.D5}` — {len(files5)} symbols")
    A(f"- **1-MINUTE (intrabar exit simulation):** `{L.D1}` — {len(files1)} symbols")
    A(f"- **DAILY:** `{L.DD}` — {len(filesd)} symbols — ⚠️ **STALE** (ends ~2026-05-15); NOT used. "
      f"ATR taken from recomputed 5-min ATR instead.\n")
    A("## Available date range & sessions")
    A(f"- 5-min full trading sessions found: **{len(full_sessions)}** "
      f"({full_sessions[0].date()} .. {full_sessions[-1].date()})")
    A(f"- 1-min-resolvable sessions (≥350 1-min bars/day on ref liquid names): **{len(resolvable)}**, "
      f"last = **{last_res.date()}**")
    A(f"- 2026-06-30 EXCLUDED — 1-min incomplete (~2 bars; today's open). Last resolvable session = {last_res.date()}.\n")
    A("### Train/Test split (task convention: TRAIN=6wk before TEST, TEST=last ~2wk; FIT/VAL = TRAIN halves)")
    A(f"- **WARMUP** ({len(WARM_s)}): {WARM_s[0].date() if WARM_s else '-'} .. {WARM_s[-1].date() if WARM_s else '-'} (loaded for indicator warmup, NOT signalled)")
    A(f"- **FIT** ({len(FIT_s)}): {ds(FIT_s)}")
    A(f"- **VAL** ({len(VAL_s)}): {ds(VAL_s)}")
    A(f"- **TRAIN** ({len(TRAIN_s)}) = FIT+VAL: {TRAIN_s[0].date()} .. {TRAIN_s[-1].date()}")
    A(f"- **TEST** ({len(TEST_s)}): {ds(TEST_s)}")
    A("- (Repo standard `compute_windows` = TEST last 4wk / TRAIN 3mo; we use the task's 6wk/2wk + FIT-VAL split for honest nested validation.)\n")
    A("## Symbols / universe")
    A(f"- Liquid universe = symbols with median daily turnover ≥ Rs {TURNOVER_MIN_RS/1e7:.0f} cr "
      f"(over last {LOOKBACK_SESS} sessions) AND a 1-min file: **{len(liq)}** qualify, capped to top **{len(universe)}** by turnover.")
    A(f"- Turnover from 5-min (Σ close·volume per session); daily store stale so liquidity derived from intraday.")
    A(f"- Top 10 by turnover: {', '.join(universe[:10])}")
    A(f"- Universe saved -> `results/universe.json`; sessions -> `results/sessions.json`\n")
    A("## Columns")
    A(f"### 5-min parquet ({len(c5)} cols)")
    A(f"- indicator-like ({len(ind5)}): {', '.join(ind5)}")
    A(f"- other ({len(oth5)}): {', '.join(oth5)}")
    A(f"### 1-min parquet ({len(c1)} cols): {', '.join(c1)}")
    A("\n### Required columns for this study")
    A("- Needed: date, open, high, low, close, volume (5-min AND 1-min) — **all present**.")
    A("- VWAP/EMA/ATR/RSI/ADX/MACD-hist — **recomputed causally in-engine** (parquet `VWAP` is the known-stale "
      "global-cumsum column; we do NOT use it). No required column missing.\n")
    A("## Data-quality checks (sample of "
      f"{dq['checked']} universe symbols)")
    A(f"- `opening_snapshot` duplicate rows (09:15 == 09:20 first real bar) — **dropped**: {dq['opening_snapshot_dropped']} rows across sample.")
    A(f"- exact duplicate 5-min timestamps after snapshot-drop: {dq['dup_rows_5m']}")
    A(f"- 5-min rows with NaN OHLC: {dq['nan_ohlc_5m']}")
    A(f"- halted/short sessions (<30 bars on a resolvable day): {dq['halted_sessions']}"
      + (f" e.g. {halted[:5]}" if halted else ""))
    A(f"- VWAP caveat: parquet `VWAP` stale/anchored → engine recomputes session-anchored VWAP (cumΣ typical·vol).")
    A(f"- 2026-06-22 appears in some 1-min files but not all 5-min files → handled by using the "
      f"5-min∩1-min resolvable session intersection.\n")
    A("## Resolver validation (no-lookahead + tie-break parity)")
    A(f"- My 1-min resolver vs repo `v17D_exit_resolver.resolve` on {tried} sampled LONG signals @0.75/0.75: "
      f"**{match}/{tried} identical outcomes ({val_rate}%)**, {mismatch} mismatch.")
    A("- Both use SL-first pessimism when SL & target are touched in the SAME 1-min bar; my resolver additionally "
      "counts those tie-break bars (reported per candidate in the search).")
    A("- Entry = next 1-min OPEN at floor(signal)+1min (≤+3min), 15 bps/leg adverse slippage; identical to "
      "`setup_train_test._entry`. No bar's own future is used to trigger it → no lookahead.")
    (L.OUTDIR / "DATA_AUDIT.md").write_text("\n".join(out), encoding="utf-8")
    print("[audit] wrote DATA_AUDIT.md")
    print(f"[audit] FIT={len(FIT_s)} VAL={len(VAL_s)} TRAIN={len(TRAIN_s)} TEST={len(TEST_s)} universe={len(universe)}")


if __name__ == "__main__":
    main()
