"""G7 adapter for G6's sealed effective-dated primary-index inputs."""
from __future__ import annotations

from research import g6_primary_index_direction as source


MEMBERSHIP_CONFIG = source.MEMBERSHIP_CONFIG
WORKBOOK = source.WORKBOOK
INDEX_ROOT = source.INDEX_ROOT
INDEX_SPECS = source.INDEX_SPECS
PRIMARY_FILTER = source.PRIMARY_FILTER
NIFTY_FILTER = source.NIFTY_FILTER
NIFTY50_SPOT_FILTER = source.NIFTY50_SPOT_FILTER
LEGACY_NIFTY_FILTER = source.LEGACY_NIFTY_FILTER
OFF = source.OFF
MODES = source.MODES
CANONICAL_MODES = source.CANONICAL_MODES

canonical_mode = source.canonical_mode
allowed = source.allowed
effective_mapping = source.effective_mapping
mapping_for_orders = source.mapping_for_orders
load_index_inputs = source.load_index_inputs
order_audits = source.order_audits
write_evidence = source.write_evidence
verify_saved = source.verify_saved


def apply_filter(unfiltered, audit, config, *, mode=PRIMARY_FILTER):
    book, allocation = source.apply_filter(unfiltered, audit, config, mode=mode)
    book = book.rename(columns={
        "g6_native_filled": "g7_native_filled",
        "g6_direction_allowed": "g7_direction_allowed",
        "g6_direction_filter_mode": "g7_direction_filter_mode",
    })
    book.loc[book.portfolio_status.eq("G6_PRIMARY_INDEX_BOTH_COUNTER_BLOCKED"),
             "portfolio_status"] = "G7_PRIMARY_INDEX_BOTH_COUNTER_BLOCKED"
    book.loc[book.portfolio_status.eq("G6_NIFTY50_SPOT_BOTH_COUNTER_BLOCKED"),
             "portfolio_status"] = "G7_NIFTY50_SPOT_BOTH_COUNTER_BLOCKED"
    book["strategy"] = "V13-V10-G-7"
    book["scope"] = "BACKTEST_ONLY"
    book["execution_authority"] = False
    return book, allocation
