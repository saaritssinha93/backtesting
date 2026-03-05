# -*- coding: utf-8 -*-
"""
Compatibility launcher for V7 sweep runner.

This file exists because some workflows refer to:
    avwap_runner_combined_v7_sweep.py

It delegates to:
    avwap_combined_runner_v7_sweep.py
"""

from __future__ import annotations

from avwap_combined_runner_v7_sweep import main


if __name__ == "__main__":
    main()
