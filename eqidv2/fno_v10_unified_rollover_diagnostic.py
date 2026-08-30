"""V10 unified diagnostic launcher for the Aug 24-25 SEP futures roll."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Sequence

import fno_oi_common as common
import fno_rollover_diagnostic_config as rollover
import fno_v10_unified_5m_1m_backtest as parent
import fno_v8_windowed_1m_entry_backtest as engine


ROOT = (
    common.FNO_ROOT
    / "strategy_research"
    / "v10_unified_5m_1m_v1"
    / "rollover_diagnostic"
)


def configure_engine() -> None:
    parent.configure_engine()
    rollover.configure(
        engine,
        root=ROOT,
        report_name="latest_fno_v10_unified_rollover_diagnostic.md",
        launcher_path=Path(__file__),
    )


def main(argv: Sequence[str] | None = None) -> int:
    configure_engine()
    args = parent._inject_v10_variant(sys.argv[1:] if argv is None else argv)
    return engine.main(args)


if __name__ == "__main__":
    raise SystemExit(main())
