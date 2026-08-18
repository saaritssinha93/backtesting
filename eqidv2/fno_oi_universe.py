from __future__ import annotations

import argparse
import sys
from datetime import date
from typing import Any

import pandas as pd

import fno_oi_common as common
import fno_oi_hybrid_data as hybrid


SESSION = "fno_oi_universe"
REPORT_DIR = common.FNO_ROOT / "universe_reports"


def refresh_universe(
    session_date: date,
    *,
    timeout_sec: float = 8.0,
    max_apps: int = 8,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    credentials = common.discover_kite_credentials(max_apps=max_apps)
    failures: list[str] = []
    records = None
    source_client = None
    source_app = ""
    for credential in credentials:
        common.publish_heartbeat(
            SESSION,
            "RUNNING",
            phase="INSTRUMENT_MASTER",
            app=credential.app_name,
            session_date_ist=session_date.isoformat(),
        )
        try:
            client = common.make_kite_client(credential, timeout_sec=timeout_sec)
            records = client.instruments("NFO")
            source_app = credential.app_name
            if records:
                source_client = client
                break
            failures.append(f"{credential.app_name}:empty_master")
        except Exception as exc:
            failures.append(f"{credential.app_name}:{type(exc).__name__}:{exc}")

    if not records:
        raise RuntimeError(
            "Unable to download the NFO instrument master from any authenticated app: "
            + " | ".join(failures)
        )

    if source_client is None:
        raise RuntimeError("NFO instrument source client was not retained.")
    nse_records = source_client.instruments("NSE")
    if not nse_records:
        raise RuntimeError("NSE instrument master is empty; equity mapping cannot be built.")

    master = common.normalize_futures_master(records, session_date=session_date)
    universe = common.select_near_month(master)
    mapped, excluded = hybrid.attach_equity_instruments(
        universe, pd.DataFrame(nse_records)
    )
    unmapped_stock = excluded.loc[
        excluded["reason"].ne("INDEX_FUTURE_HAS_NO_CASH_EQUITY")
    ] if not excluded.empty else excluded
    if not unmapped_stock.empty:
        sample = unmapped_stock.head(10).to_dict("records")
        raise RuntimeError(f"Stock-future equity mapping is incomplete: {sample}")
    mapping_columns = [
        "tradingsymbol",
        "futures_tradingsymbol",
        "futures_instrument_token",
        "futures_lot_size",
        "futures_tick_size",
        "equity_symbol",
        "equity_instrument_token",
        "equity_tick_size",
        "equity_exchange",
        "data_contract",
    ]
    universe = universe.merge(
        mapped[mapping_columns], on="tradingsymbol", how="left", validate="one_to_one"
    )
    universe["data_contract"] = hybrid.DATA_CONTRACT_VERSION
    summary = common.persist_universe(master, universe, session_date=session_date)
    summary.update(
        {
            "source_app": source_app,
            "credential_count": len(credentials),
            "failed_app_attempts": failures,
            "mapped_stock_equities": int(len(mapped)),
            "excluded_index_futures": int(
                excluded["reason"].eq("INDEX_FUTURE_HAS_NO_CASH_EQUITY").sum()
            ) if not excluded.empty else 0,
            "data_contract": hybrid.DATA_CONTRACT_VERSION,
        }
    )
    common.atomic_write_json(common.UNIVERSE_DIR / "latest_universe_summary.json", summary)
    return master, universe, summary


def render_report(
    master: pd.DataFrame,
    universe: pd.DataFrame,
    summary: dict[str, Any],
) -> str:
    expiries = (
        master.groupby(master["expiry"].dt.strftime("%Y-%m-%d"), sort=True)
        .size()
        .rename("contracts")
    )
    lines = [
        "# FnO Futures Universe",
        "",
        f"Session date: {summary['session_date']}",
        f"Published: {summary['published_at_ist']}",
        f"Source app: {summary.get('source_app', '')}",
        f"Active NFO-FUT contracts: {summary['active_futures']}",
        f"Near-month underlyings: {summary['near_month_contracts']}",
        f"Permanent contract registry: {summary['contract_registry_records']} records",
        f"Stock futures: {summary['stock_futures']}",
        f"Index futures: {summary['index_futures']}",
        f"Mapped stock equities: {summary.get('mapped_stock_equities', 0)}",
        f"Hybrid data contract: {summary.get('data_contract', '')}",
        f"Universe SHA256: {summary['universe_sha256']}",
        "",
        "Expiry distribution:",
        "",
        "Expiry | Contracts",
        "--- | ---:",
    ]
    lines.extend(f"{expiry} | {int(count)}" for expiry, count in expiries.items())
    lines.extend(
        [
            "",
            "Near-month sample:",
            "",
            "Underlying | Contract | Expiry | Lot | Type",
            "--- | --- | --- | ---: | ---",
        ]
    )
    for row in universe.head(25).itertuples(index=False):
        kind = "INDEX" if bool(row.is_index_future) else "STOCK"
        lines.append(
            f"{row.underlying} | {row.tradingsymbol} | "
            f"{pd.Timestamp(row.expiry).strftime('%Y-%m-%d')} | {int(row.lot_size)} | {kind}"
        )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Archive the daily NFO-FUT master and select one near-month contract per underlying."
    )
    parser.add_argument("--session-date", default="")
    parser.add_argument("--timeout-sec", type=float, default=8.0)
    parser.add_argument("--max-apps", type=int, default=8)
    parser.add_argument("--allow-non-trading-day", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    session_date = (
        date.fromisoformat(args.session_date) if args.session_date else common.now_ist().date()
    )
    if (
        not args.allow_non_trading_day
        and not common.is_trading_day(session_date, common.load_holidays())
    ):
        common.publish_status(
            SESSION,
            "SKIPPED_NON_TRADING_DAY",
            session_date_ist=session_date.isoformat(),
        )
        print(f"[SKIP] {session_date.isoformat()} is not a trading day.", flush=True)
        return 0

    common.publish_status(
        SESSION,
        "RUNNING",
        phase="START",
        session_date_ist=session_date.isoformat(),
    )
    try:
        master, universe, summary = refresh_universe(
            session_date,
            timeout_sec=args.timeout_sec,
            max_apps=args.max_apps,
        )
        report = render_report(master, universe, summary)
        dated_report = REPORT_DIR / f"fno_oi_universe_{session_date.isoformat()}.md"
        latest_report = common.LATEST_DIR / "latest_fno_oi_universe.md"
        common.atomic_write_text(dated_report, report)
        common.atomic_write_text(latest_report, report)
        common.publish_status(
            SESSION,
            "SUCCESS",
            phase="DONE",
            session_date_ist=session_date.isoformat(),
            active_futures=summary["active_futures"],
            near_month_contracts=summary["near_month_contracts"],
            source_app=summary["source_app"],
            output=latest_report,
        )
        print(
            "[SUCCESS] FnO universe "
            f"active={summary['active_futures']} near_month={summary['near_month_contracts']} "
            f"source={summary['source_app']}",
            flush=True,
        )
        return 0
    except Exception as exc:
        common.publish_status(
            SESSION,
            "FAILED",
            heartbeat_state="CRASHED",
            phase="FAILED",
            session_date_ist=session_date.isoformat(),
            error=f"{type(exc).__name__}: {exc}",
        )
        print(f"[FATAL] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
