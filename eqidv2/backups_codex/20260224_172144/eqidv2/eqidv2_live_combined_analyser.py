# -*- coding: utf-8 -*-
"""
eqidv2_live_combined_analyser.py
================================

Compatibility shim that keeps this module path fully aligned with
`eqidv2_live_combined_analyser_csv.py`.

All signal logic, scheduler behavior, and CSV output schema are sourced from
the CSV analyser so both files stay in parity.
"""

from __future__ import annotations

from eqidv2_live_combined_analyser_csv import *  # noqa: F401,F403


if __name__ == "__main__":
    main()
