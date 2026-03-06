# -*- coding: utf-8 -*-
"""
Compatibility launcher for V8 sweep runner.

This file exists so that workflows referring to:
    avwap_runner_combined_v8_sweep.py
delegate to:
    avwap_combined_runner_v8_sweep.py
"""

from __future__ import annotations

from avwap_combined_runner_v8_sweep import main


if __name__ == "__main__":
    main()
