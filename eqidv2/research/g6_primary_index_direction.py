"""Effective-dated per-stock primary-index direction gate for G6.

The gate rejects an entry only when both the latest exact completed five-minute
cash-index candle and the same candle's close versus the 09:20 session open
oppose the trade.  Membership is resolved as of the order day; the 2026-10-21
workbook is never projected backward across the 2026-09-30 reconstitution.
"""
from __future__ import annotations

from datetime import date
import json
import os
from pathlib import Path
import re
import shutil
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import numpy as np
import pandas as pd

from research import g4_failed_breakout_research as sealed
from research import g4_nifty_direction_backtest as nifty


ROOT = Path(__file__).resolve().parents[1]
MEMBERSHIP_CONFIG = ROOT / "configs/research/g6_primary_index_effective_membership_v1.json"
WORKBOOK = ROOT / "outputs/fno_index_mapping_20261021/nse_fno_stock_index_mapping_2026-10-21.xlsx"
INDEX_ROOT = Path(os.environ.get(
    "EQIDV2_G6_PRIMARY_INDEX_5M_DIR",
    r"C:\TradingData\eqidv2\backtesting_result_v13_v10_g_6\inputs\primary_index_20261021"))

PRIMARY_FILTER = "primary_index_both_counter_skip"
NIFTY_FILTER = "nifty_both_counter_skip"
NIFTY50_SPOT_FILTER = "nifty50_spot_both_counter_skip"
LEGACY_NIFTY_FILTER = "both_counter_skip"
OFF = "off"
MODES = (PRIMARY_FILTER, NIFTY_FILTER, OFF, LEGACY_NIFTY_FILTER)
CANONICAL_MODES = (PRIMARY_FILTER, NIFTY_FILTER, OFF)
ENGINE = sealed.bounded.history.simulator.g.v9
IST = "Asia/Kolkata"

INDEX_SPECS = {
    "Nifty 50": {
        "slug": "nifty50",
        "file": "G6_NIFTY50_stocks_indicators_5min.parquet",
        "sha256": "986fe5358e11e804122e2b2a49d096019b36a80a1adacc5defc50c6cc05a7c37",
        "instrument_token": 256265,
        "source_symbol": "NIFTY 50",
    },
    "Nifty Next 50": {
        "slug": "nifty_next50",
        "file": "G6_NIFTY_NEXT50_stocks_indicators_5min.parquet",
        "sha256": "f58d4c583c12e4cf3828c3a88873e22fefaa850a1f402293f738dc3234289e2d",
        "instrument_token": 270857,
        "source_symbol": "NIFTY NEXT 50",
    },
    "Nifty Midcap 150": {
        "slug": "nifty_midcap150",
        "file": "G6_NIFTY_MIDCAP150_stocks_indicators_5min.parquet",
        "sha256": "e831ee48dc63850bd424ba7b1197b8c39455d2fdcbcbabebeff3bfc7f9c89a65",
        "instrument_token": 266249,
        "source_symbol": "NIFTY MIDCAP 150",
    },
    "Nifty Smallcap 250": {
        "slug": "nifty_smallcap250",
        "file": "G6_NIFTY_SMALLCAP250_stocks_indicators_5min.parquet",
        "sha256": "85448177c384b6a8fc79d6536af1b92a14696170fc261109bedc5cefb4d3999f",
        "instrument_token": 267273,
        "source_symbol": "NIFTY SMLCAP 250",
    },
    "Nifty Microcap 250": {
        "slug": "nifty_microcap250",
        "file": "G6_NIFTY_MICROCAP250_stocks_indicators_5min.parquet",
        "sha256": "fafc3d3b313582e818f502b1e2becba955af0b9e069a6a456ec34265c016c333",
        "instrument_token": 290569,
        "source_symbol": "NIFTY MICROCAP250",
    },
}


def _read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def canonical_mode(mode):
    return NIFTY_FILTER if mode == LEGACY_NIFTY_FILTER else mode


def allowed(side, slot_change_pct, session_change_pct):
    """Flat or mixed index directions allow the entry on either side."""
    return nifty.allow_direction(side, slot_change_pct) or nifty.allow_direction(
        side, session_change_pct)


def _cell_value(cell, shared, namespace):
    kind = cell.attrib.get("t")
    value = cell.find("x:v", namespace)
    if value is None:
        inline = cell.find("x:is/x:t", namespace)
        return "" if inline is None else (inline.text or "")
    text = value.text or ""
    return shared[int(text)] if kind == "s" else text


def read_stock_mapping_workbook(path=WORKBOOK, *, expected_sha=None):
    """Read the stock and primary-index columns without an openpyxl dependency."""
    path = Path(path).resolve()
    digest = nifty.base.sha256(path)
    if expected_sha is not None and digest != expected_sha:
        raise ValueError("Primary-index workbook SHA-256 changed")
    namespace = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with ZipFile(path) as archive:
        shared = []
        if "xl/sharedStrings.xml" in archive.namelist():
            tree = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for item in tree.findall("x:si", namespace):
                shared.append("".join((node.text or "") for node in item.findall(".//x:t", namespace)))
        sheet = ET.fromstring(archive.read("xl/worksheets/sheet1.xml"))
    rows = {}
    for row in sheet.findall(".//x:sheetData/x:row", namespace):
        number = int(row.attrib["r"])
        values = {}
        for cell in row.findall("x:c", namespace):
            column = re.match(r"[A-Z]+", cell.attrib["r"]).group(0)
            values[column] = _cell_value(cell, shared, namespace).strip()
        rows[number] = values
    if rows.get(9, {}).get("A") != "Symbol" or rows.get(9, {}).get("D") != "Primary size index":
        raise ValueError("Unexpected stock-mapping workbook schema")
    records = [dict(symbol=rows[number].get("A", "").upper(),
                    primary_index=rows[number].get("D", ""))
               for number in range(10, 223)]
    frame = pd.DataFrame(records)
    if len(frame) != 213 or frame.symbol.eq("").any() or frame.symbol.duplicated().any():
        raise ValueError("Expected 213 unique mapped F&O symbols")
    if set(frame.primary_index) != set(INDEX_SPECS):
        raise ValueError("Unexpected or missing primary size-index bucket")
    return frame.sort_values("symbol").reset_index(drop=True), digest


def effective_mapping(*, workbook=WORKBOOK, membership_config=MEMBERSHIP_CONFIG):
    config = _read_json(membership_config)
    snapshot = config["current_snapshot"]
    current, workbook_sha = read_stock_mapping_workbook(
        workbook, expected_sha=snapshot["workbook_sha256"])
    observed_counts = current.primary_index.value_counts().to_dict()
    if observed_counts != snapshot["primary_counts"]:
        raise ValueError("Primary-index workbook bucket counts changed")
    transitions = {row["symbol"]: row for row in config["primary_index_transitions"]}
    if len(transitions) != 12 or not set(transitions).issubset(set(current.symbol)):
        raise ValueError("Effective membership transitions are incomplete")
    current_by_symbol = current.set_index("symbol").primary_index
    for symbol, transition in transitions.items():
        if current_by_symbol[symbol] != transition["from_2026_09_30"]:
            raise ValueError("Transition does not match current workbook: " + symbol)
    records = []
    for row in current.itertuples(index=False):
        before = transitions.get(row.symbol, {}).get("before", row.primary_index)
        records.extend([
            dict(symbol=row.symbol, primary_index=before,
                 current_snapshot_index=row.primary_index,
                 effective_from="2026-07-29", effective_through="2026-09-29",
                 regime="before_2026_09_30_reconstitution",
                 mapping_basis=("official_2026_09_30_transition_reversed"
                                if row.symbol in transitions else "unchanged_across_audited_events")),
            dict(symbol=row.symbol, primary_index=row.primary_index,
                 current_snapshot_index=row.primary_index,
                 effective_from="2026-09-30", effective_through="2026-10-19",
                 regime="from_2026_09_30_reconstitution",
                 mapping_basis="current_snapshot_after_official_reconstitution"),
        ])
    result = pd.DataFrame(records).sort_values(["symbol", "effective_from"]).reset_index(drop=True)
    return result, config, workbook_sha


def mapping_for_orders(orders, mapping):
    if orders.sid.duplicated().any():
        raise ValueError("Duplicate G6 order SID")
    records = []
    grouped = {symbol: part.reset_index(drop=True) for symbol, part in mapping.groupby("symbol")}
    for row in orders.itertuples():
        symbol = str(row.tradingsymbol).strip().upper()
        day = str(row.day)
        candidates = grouped.get(symbol)
        if candidates is None:
            raise ValueError("Missing primary-index mapping: " + symbol)
        hit = candidates.loc[candidates.effective_from.le(day) & candidates.effective_through.ge(day)]
        if len(hit) != 1:
            raise ValueError(f"Expected one effective primary index: {symbol} {day}")
        value = hit.iloc[0]
        records.append(dict(sid=int(row.sid), symbol=symbol, day=day,
            primary_index=value.primary_index, current_snapshot_index=value.current_snapshot_index,
            mapping_regime=value.regime, mapping_effective_from=value.effective_from,
            mapping_effective_through=value.effective_through, mapping_basis=value.mapping_basis))
    result = pd.DataFrame(records)
    if len(result) != len(orders) or result.sid.duplicated().any():
        raise ValueError("Effective mapping does not cover every selected order")
    return result


def _normalise_index_frame(raw, index_name, required_days):
    required = {"date", "open", "high", "low", "close"}
    if not required.issubset(raw.columns):
        raise ValueError("Cash-index input schema changed: " + index_name)
    frame = raw.copy()
    frame["ts"] = pd.to_datetime(frame["date"], utc=True).dt.tz_convert(IST).dt.as_unit("ns")
    if "opening_snapshot" in frame:
        frame = frame.loc[~frame.opening_snapshot.fillna(False).astype(bool)]
    clock = frame.ts.dt.strftime("%H:%M")
    frame = frame.loc[clock.between("09:20", "15:30")].copy()
    frame["day"] = frame.ts.dt.strftime("%Y-%m-%d")
    frame = frame.loc[frame.day.isin(required_days)].sort_values("ts").reset_index(drop=True)
    if frame.ts.duplicated().any():
        raise ValueError("Duplicate cash-index bar: " + index_name)
    if not np.isfinite(frame[["open", "high", "low", "close"]].to_numpy(dtype=float)).all():
        raise ValueError("Non-finite cash-index OHLC: " + index_name)
    if (frame[["open", "high", "low", "close"]].to_numpy(dtype=float) <= 0).any():
        raise ValueError("Non-positive cash-index OHLC: " + index_name)
    expected_clocks = pd.date_range("09:20", "15:30", freq="5min").strftime("%H:%M").tolist()
    for day in sorted(required_days):
        part = frame.loc[frame.day.eq(day)]
        if part.ts.dt.strftime("%H:%M").tolist() != expected_clocks:
            raise ValueError(f"Incomplete exact cash-index session: {index_name} {day}")
    return frame[["day", "ts", "open", "high", "low", "close"]].copy()


def load_index_inputs(days, *, index_root=INDEX_ROOT):
    days = sorted({str(day) for day in days})
    proofs, inputs = [], {}
    for index_name, spec in INDEX_SPECS.items():
        path = Path(index_root) / spec["file"]
        digest = nifty.base.sha256(path)
        if digest != spec["sha256"]:
            raise ValueError("Pinned cash-index input changed: " + index_name)
        sidecar = path.with_name(path.name + ".source.json")
        source = _read_json(sidecar)
        if (int(source.get("instrument_token", -1)) != spec["instrument_token"] or
                source.get("source_symbol") != spec["source_symbol"]):
            raise ValueError("Cash-index source identity changed: " + index_name)
        frame = _normalise_index_frame(pd.read_parquet(path), index_name, days)
        inputs[index_name] = {day: frame.loc[frame.day.eq(day)].reset_index(drop=True)
                              for day in days}
        proofs.append(dict(primary_index=index_name, slug=spec["slug"],
            instrument_token=spec["instrument_token"], source_symbol=spec["source_symbol"],
            path=str(path.resolve()), sha256=digest, source_metadata_path=str(sidecar.resolve()),
            source_metadata_sha256=nifty.base.sha256(sidecar), sessions=len(days),
            first_day=days[0], last_day=days[-1], bars=len(frame)))
    return pd.DataFrame(proofs), inputs


def direction_at(frame, decision):
    decision = pd.Timestamp(decision)
    if decision.tzinfo is None:
        raise ValueError("Timezone-aware index decision required")
    decision = decision.tz_convert(IST)
    expected = decision.floor("5min")
    exact = frame.loc[frame.ts.eq(expected)]
    opening_ts = decision.normalize() + pd.Timedelta(hours=9, minutes=20)
    opening = frame.loc[frame.ts.eq(opening_ts)]
    if len(exact) != 1 or len(opening) != 1:
        return dict(status="MISSING_EXACT_INDEX_BAR", index_bar_ts=str(expected),
                    slot_change_pct=np.nan, session_change_pct=np.nan)
    bar = exact.iloc[0]
    values = np.asarray([bar.open, bar.close, opening.iloc[0].open], dtype=float)
    if not np.isfinite(values).all() or (values <= 0).any():
        raise ValueError("Invalid cash-index direction price")
    return dict(status="OK", index_bar_ts=str(expected), index_open=float(bar.open),
        index_close=float(bar.close), index_session_open=float(opening.iloc[0].open),
        slot_change_pct=float((bar.close / bar.open - 1) * 100),
        session_change_pct=float((bar.close / opening.iloc[0].open - 1) * 100))


def _build_audit(orders, order_mapping, proofs, inputs, *, forced_index=None,
                 mapping_index_field="primary_index"):
    mapping_by_sid = order_mapping.set_index("sid")
    proof_by_index = proofs.set_index("primary_index")
    records = []
    for row in orders.itertuples():
        mapped = mapping_by_sid.loc[int(row.sid)]
        index_name = forced_index or getattr(mapped, mapping_index_field)
        proof = proof_by_index.loc[index_name]
        frame = inputs[index_name][str(row.day)]
        values = direction_at(frame, row.confirmation_ts)
        if not np.isfinite([values["slot_change_pct"], values["session_change_pct"]]).all():
            raise ValueError(f"Incomplete primary-index context: {row.day} {row.setup_id} {row.tradingsymbol}")
        if pd.Timestamp(values["index_bar_ts"]) > pd.Timestamp(row.confirmation_ts):
            raise ValueError("Future cash-index candle used")
        records.append(dict(sid=int(row.sid), day=str(row.day), setup_id=row.setup_id,
            tradingsymbol=row.tradingsymbol, side=row.side,
            confirmation_ts=str(pd.Timestamp(row.confirmation_ts)),
            mapped_primary_index=mapped.primary_index,
            current_snapshot_index=mapped.current_snapshot_index,
            decision_index=index_name,
            mapping_regime=mapped.mapping_regime,
            mapping_effective_from=mapped.mapping_effective_from,
            mapping_effective_through=mapped.mapping_effective_through,
            index_instrument_token=int(proof.instrument_token), index_source_symbol=proof.source_symbol,
            index_source_sha256=proof.sha256, **values,
            both_counter_allowed=allowed(row.side, values["slot_change_pct"], values["session_change_pct"])))
    audit = pd.DataFrame(records)
    if len(audit) != len(orders) or audit.sid.duplicated().any() or set(audit.sid) != set(orders.sid):
        raise ValueError("Incomplete cash-index decision audit")
    return audit


def order_audits(orders):
    mapping, membership, workbook_sha = effective_mapping()
    order_mapping = mapping_for_orders(orders, mapping)
    proofs, inputs = load_index_inputs(orders.day.unique())
    primary = _build_audit(orders, order_mapping, proofs, inputs)
    nifty50_spot = _build_audit(orders, order_mapping, proofs, inputs, forced_index="Nifty 50")
    snapshot_naive = _build_audit(
        orders, order_mapping, proofs, inputs, mapping_index_field="current_snapshot_index")
    metadata = dict(membership_config=membership, workbook_sha256=workbook_sha)
    return (primary, nifty50_spot, snapshot_naive, proofs, inputs, mapping,
            order_mapping, metadata)


def apply_filter(unfiltered, audit, config, *, mode=PRIMARY_FILTER):
    decision = audit.set_index("sid").both_counter_allowed
    permitted = unfiltered.sid.map(decision)
    if permitted.isna().any() or set(decision.index) != set(unfiltered.sid):
        raise ValueError("Missing or extraneous cash-index decision")
    work = unfiltered.copy()
    work["g6_native_filled"] = work.filled.eq(True)
    work["g6_direction_allowed"] = permitted.to_numpy(bool)
    work["filled"] = work.filled.eq(True) & permitted
    sized = ENGINE.v5.apply_fixed_capital_model(
        work, config["capital_per_entry_rs"], config["leverage"])
    settings = ENGINE.V9Config(
        portfolio_capital_rupees=config["portfolio_capital_rs"],
        capital_per_entry_rupees=config["capital_per_entry_rs"],
        leverage_factor=config["leverage"], max_positions=10,
        cost_bps=config["model_cost_bps_round_trip"])
    book, allocation = ENGINE.v6.apply_portfolio_constraints(sized, settings.portfolio_config())
    blocked = ~permitted.to_numpy(bool)
    status = ("G6_PRIMARY_INDEX_BOTH_COUNTER_BLOCKED" if mode == PRIMARY_FILTER
              else "G6_NIFTY50_SPOT_BOTH_COUNTER_BLOCKED")
    reason = ("BOTH_PRIMARY_INDEX_DIRECTIONS_OPPOSE" if mode == PRIMARY_FILTER
              else "BOTH_NIFTY50_SPOT_DIRECTIONS_OPPOSE")
    book.loc[blocked, "portfolio_status"] = status
    book.loc[blocked, "portfolio_reject_reason"] = reason
    book["strategy"] = "V13-V10-G-6"
    book["scope"] = "BACKTEST_ONLY"
    book["execution_authority"] = False
    book["g6_direction_filter_mode"] = mode
    return book, allocation


def write_evidence(output, primary_audit, nifty50_spot_audit, snapshot_naive_audit,
                   proofs, inputs, effective, order_mapping, metadata):
    output = Path(output)
    primary_audit.to_csv(output / "primary_index_direction_audit.csv", index=False)
    nifty50_spot_audit.to_csv(output / "nifty50_spot_direction_audit.csv", index=False)
    snapshot_naive_audit.to_csv(output / "snapshot_naive_direction_audit.csv", index=False)
    proofs.to_csv(output / "primary_index_source_proof.csv", index=False)
    effective.to_csv(output / "primary_index_effective_mapping.csv", index=False)
    order_mapping.to_csv(output / "primary_index_order_mapping.csv", index=False)
    shutil.copy2(WORKBOOK, output / "primary_index_mapping_source.xlsx")
    (output / "primary_index_membership_config.json").write_text(
        json.dumps(metadata["membership_config"], indent=2) + "\n", encoding="utf-8")
    root = output / "primary_index_inputs"
    root.mkdir()
    for index_name, days in inputs.items():
        folder = root / INDEX_SPECS[index_name]["slug"]
        folder.mkdir()
        for day, frame in days.items():
            frame.to_parquet(folder / (day + ".parquet"), index=False)


def _verify_audit(output, audit_name, *, expected_index=None, expected_index_column=None):
    output = Path(output)
    audit = pd.read_csv(output / audit_name)
    proofs = pd.read_csv(output / "primary_index_source_proof.csv")
    if audit.sid.duplicated().any():
        raise ValueError("Duplicate SID in saved cash-index audit")
    for row in audit.itertuples():
        index_name = expected_index or (getattr(row, expected_index_column)
                                        if expected_index_column else row.mapped_primary_index)
        if row.decision_index != index_name:
            raise ValueError("Saved decision index differs from effective mapping")
        proof = proofs.loc[proofs.primary_index.eq(index_name)]
        if len(proof) != 1 or row.index_source_sha256 != proof.sha256.iloc[0]:
            raise ValueError("Saved cash-index provenance differs")
        slug = INDEX_SPECS[index_name]["slug"]
        frame = pd.read_parquet(output / "primary_index_inputs" / slug / (row.day + ".parquet"))
        values = direction_at(frame, row.confirmation_ts)
        for key in ("slot_change_pct", "session_change_pct"):
            np.testing.assert_allclose(values[key], getattr(row, key), rtol=0, atol=1e-10)
        if bool(row.both_counter_allowed) != allowed(
                row.side, values["slot_change_pct"], values["session_change_pct"]):
            raise ValueError("Saved cash-index decision differs")
    return audit


def verify_saved(output, summary):
    """Recompute effective membership and every saved cash-index decision."""
    output = Path(output)
    if "primary_index_reference_policy" not in summary:
        return 0
    membership_path = output / "primary_index_membership_config.json"
    saved_config = _read_json(membership_path)
    expected_config = _read_json(MEMBERSHIP_CONFIG)
    if saved_config != expected_config:
        raise ValueError("Saved effective-membership configuration differs")
    rebuilt, _, workbook_sha = effective_mapping(
        workbook=output / "primary_index_mapping_source.xlsx",
        membership_config=membership_path)
    saved_mapping = pd.read_csv(output / "primary_index_effective_mapping.csv", dtype=str)
    pd.testing.assert_frame_equal(saved_mapping.reset_index(drop=True),
                                  rebuilt.astype(str).reset_index(drop=True), check_dtype=False)
    if workbook_sha != summary["primary_index_mapping_workbook_sha256"]:
        raise ValueError("Saved primary-index workbook proof differs")
    primary = _verify_audit(output, "primary_index_direction_audit.csv")
    _verify_audit(output, "nifty50_spot_direction_audit.csv", expected_index="Nifty 50")
    _verify_audit(output, "snapshot_naive_direction_audit.csv",
                  expected_index_column="current_snapshot_index")
    order_mapping = pd.read_csv(output / "primary_index_order_mapping.csv")
    if set(order_mapping.sid) != set(primary.sid):
        raise ValueError("Saved effective order mapping is incomplete")
    policy = summary["primary_index_reference_policy"]
    book = nifty.base.read_frozen_csv(output / policy / "portfolio_trades.csv")
    if set(book.sid) != set(primary.sid) or len(book) != len(primary):
        raise ValueError("Primary-index ledger does not match its decision audit")
    permitted = book.sid.map(primary.set_index("sid").both_counter_allowed).astype(bool)
    if (book.portfolio_executed.eq(True) & ~permitted).any():
        raise ValueError("Primary-index-blocked entry received capital")
    comparison = nifty.base.read_frozen_csv(output / "comparison.csv")
    recorded = comparison.loc[comparison.policy.eq(policy) & comparison.period.eq("all")]
    if len(recorded) != 1 or not np.isclose(
            book.portfolio_net_profit_rupees.sum(), recorded.net_pnl_rs.iloc[0], rtol=0, atol=1e-6):
        raise ValueError("Primary-index profit differs from comparison")
    return len(primary)
