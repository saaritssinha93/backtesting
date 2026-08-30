"""V9-Honest diagnostic launcher for the Aug 24-25 SEP futures roll."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

import fno_oi_common as common
import fno_rollover_diagnostic_config as rollover
import fno_v8_windowed_1m_entry_backtest as engine
import fno_v9_honest_v8_backtest as parent


ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / "v8_v9_last_10_backtests"
    / "rolling_diagnostic"
    / "sep_block"
    / "v9_honest"
)


def configure_engine() -> None:
    parent.configure_engine()
    rollover.configure(
        engine,
        root=ROOT,
        report_name="latest_fno_v9_honest_rollover_diagnostic.md",
        launcher_path=Path(__file__),
    )


def main(argv: Sequence[str] | None = None) -> int:
    configure_engine()
    args = parent._inject_v9_variant(sys.argv[1:] if argv is None else argv)
    return engine.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
