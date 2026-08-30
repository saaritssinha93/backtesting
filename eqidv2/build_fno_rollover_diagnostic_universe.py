"""Build the isolated retrospective SEP rollover universe for Aug 24-25.

This does not rewrite or relabel expired AUG futures.  It intersects the
frozen Aug 11 stock universe with the current SEP contract master, preserves
the frozen cash-equity mapping, and records the resulting provenance as a
research-only reconstruction.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import fno_oi_backtest_provenance as provenance
import fno_oi_common as common


FROZEN_AUG_UNIVERSE = (
    common.UNIVERSE_DIR / "near_month_2026-08-11.parquet"
)
SEP_SOURCE_UNIVERSE = common.UNIVERSE_DIR / "near_month_2026-08-26.parquet"
OUTPUT_ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / "v8_v9_last_10_backtests"
    / "retrospective_rollover_universe"
)
OUTPUT_UNIVERSE = OUTPUT_ROOT / "near_month_2026-08-24.parquet"
OUTPUT_MANIFEST = OUTPUT_ROOT / "reconstruction_manifest.json"

EXPECTED_MAPPED_COUNT = 207
EXPECTED_EXCLUDED = {"DALBHARAT"}
EQUITY_COLUMNS = (
    "equity_symbol",
    "equity_instrument_token",
    "equity_tick_size",
    "equity_exchange",
)


def _stock_rows(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.loc[~frame["is_index_future"].fillna(False).astype(bool)].copy()


def build() -> dict[str, object]:
    frozen = pd.read_parquet(FROZEN_AUG_UNIVERSE)
    sep_source = pd.read_parquet(SEP_SOURCE_UNIVERSE)
    frozen_stocks = _stock_rows(frozen)
    sep_stocks = _stock_rows(sep_source)
    sep_stocks = sep_stocks.loc[
        sep_stocks["tradingsymbol"]
        .astype(str)
        .str.contains("26SEP", case=False, na=False)
    ].copy()

    frozen_symbols = set(
        frozen_stocks["equity_symbol"].astype(str).str.strip().str.upper()
    )
    sep_stocks["equity_symbol"] = (
        sep_stocks["equity_symbol"].astype(str).str.strip().str.upper()
    )
    selected = sep_stocks.loc[sep_stocks["equity_symbol"].isin(frozen_symbols)].copy()
    selected = selected.sort_values(["equity_symbol", "tradingsymbol"], kind="stable")
    selected = selected.drop_duplicates("equity_symbol", keep="first")

    observed = set(selected["equity_symbol"])
    excluded = frozen_symbols - observed
    if len(selected) != EXPECTED_MAPPED_COUNT or excluded != EXPECTED_EXCLUDED:
        raise RuntimeError(
            "Unexpected retrospective rollover intersection: "
            f"mapped={len(selected)}, excluded={sorted(excluded)}"
        )

    frozen_mapping = frozen_stocks.set_index(
        frozen_stocks["equity_symbol"].astype(str).str.strip().str.upper()
    )
    for column in EQUITY_COLUMNS:
        selected[column] = selected["equity_symbol"].map(frozen_mapping[column])
        if selected[column].isna().any():
            raise RuntimeError(f"Frozen equity mapping is incomplete for {column}")

    selected["master_date"] = pd.Timestamp("2026-08-24")
    selected = selected.loc[:, list(sep_source.columns)].reset_index(drop=True)

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    if OUTPUT_UNIVERSE.exists():
        existing = pd.read_parquet(OUTPUT_UNIVERSE)
        try:
            pd.testing.assert_frame_equal(existing, selected, check_dtype=False)
        except AssertionError as exc:
            raise FileExistsError(
                f"Refusing to replace a different reconstructed universe: {OUTPUT_UNIVERSE}"
            ) from exc
    else:
        common.atomic_write_parquet(selected, OUTPUT_UNIVERSE)

    mapped, universe_record = provenance.load_backtest_universe(
        universe_path=OUTPUT_UNIVERSE,
        universe_date="2026-08-24",
        contract_month_contains="26SEP",
        require_persisted_mapping=True,
    )
    payload: dict[str, object] = {
        "schema_version": "fno_retrospective_rollover_universe_v1",
        "classification": "RETROSPECTIVELY_RECONSTRUCTED_ROLLOVER_DIAGNOSTIC",
        "research_only": True,
        "promotion_eligible": False,
        "point_in_time_universe": False,
        "effective_sessions": ["2026-08-24", "2026-08-25"],
        "contract_month_filter": "26SEP",
        "roll_policy": "AUG_THROUGH_2026_08_21_THEN_SEP_FROM_2026_08_24",
        "frozen_stock_symbol_source": str(FROZEN_AUG_UNIVERSE.resolve()),
        "frozen_stock_symbol_source_sha256": provenance.sha256_file(
            FROZEN_AUG_UNIVERSE
        ),
        "sep_contract_source": str(SEP_SOURCE_UNIVERSE.resolve()),
        "sep_contract_source_sha256": provenance.sha256_file(SEP_SOURCE_UNIVERSE),
        "sep_contract_source_master_date": "2026-08-26",
        "cash_mapping_policy": "PRESERVE_FROZEN_2026_08_11_EQUITY_MAPPING",
        "mapped_stock_futures": int(len(mapped)),
        "excluded_after_roll": sorted(EXPECTED_EXCLUDED),
        "limitations": [
            "NO_DATED_2026_08_24_OR_2026_08_25_UNIVERSE_WAS_ARCHIVED",
            "SEP_CONTRACT_IDENTITIES_RETROSPECTIVELY_RECOVERED_FROM_2026_08_26_MASTER",
            "NOT_ELIGIBLE_FOR_PROMOTION_OR_POINT_IN_TIME_PERFORMANCE_CLAIMS",
        ],
        "universe": universe_record,
    }
    if OUTPUT_MANIFEST.exists():
        existing_manifest = json.loads(OUTPUT_MANIFEST.read_text(encoding="utf-8"))
        if existing_manifest != payload:
            raise FileExistsError(
                f"Refusing to replace a different reconstruction manifest: {OUTPUT_MANIFEST}"
            )
    else:
        common.atomic_write_json(OUTPUT_MANIFEST, payload)
    return payload


def main() -> int:
    print(json.dumps(build(), indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
