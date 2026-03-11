# -*- coding: utf-8 -*-
"""
Compatibility launcher for the top-level V11 playbook runner.

This mirrors the v7/v6 layout where a similarly named file exists inside
avwap_v11_refactored, while the orchestrator lives at project root.
"""

from __future__ import annotations

from avwap_combined_runner_v11_newlong import main


if __name__ == "__main__":
    main()
