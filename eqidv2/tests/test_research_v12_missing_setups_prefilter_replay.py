from __future__ import annotations

import unittest

from research_v12_missing_setups_prefilter_replay import (
    TARGET_SETUPS,
    aggregate_breadth_contributions,
    canonical_scan_state,
    scan_state_digest,
    setups_for_scope,
    ticker_dates_from_daily_unions,
)


class MissingSetupSupplementPureHelperTests(unittest.TestCase):
    def test_ticker_payloads_load_each_ticker_once_and_keep_only_eligible_dates(self) -> None:
        payloads = ticker_dates_from_daily_unions(
            {
                "2026-06-05": frozenset({"B", "C"}),
                "2026-06-04": frozenset({"A", "B"}),
            }
        )
        self.assertEqual(
            payloads,
            [
                ("A", ("2026-06-04",)),
                ("B", ("2026-06-04", "2026-06-05")),
                ("C", ("2026-06-05",)),
            ],
        )

    def test_scan_state_digest_is_order_independent_but_flag_sensitive(self) -> None:
        first = {
            "setup_conf_module": "final_setup_conf_v12",
            "target_setups": ["S9", "DOC"],
            "allowed_setups": ["DOC", "S9"],
            "excluded_setups": ["Z", "A"],
            "enable_s9_midday_lose": True,
        }
        reordered = {
            **first,
            "target_setups": ["DOC", "S9"],
            "allowed_setups": ["S9", "DOC"],
            "excluded_setups": ["A", "Z"],
        }
        changed = {**reordered, "enable_s9_midday_lose": False}
        self.assertEqual(canonical_scan_state(first), canonical_scan_state(reordered))
        self.assertEqual(scan_state_digest(first), scan_state_digest(reordered))
        self.assertNotEqual(scan_state_digest(first), scan_state_digest(changed))

    def test_breadth_aggregation_uses_valid_observation_denominator(self) -> None:
        rows = [
            {"slot_ist": "2026-06-04T14:00:00+05:30", "above": 1, "total": 1},
            {"slot_ist": "2026-06-04 14:00:00+05:30", "above": 0, "total": 1},
            {"slot_ist": "2026-06-04T14:05:00+05:30", "above": 1, "total": 1},
        ]
        result = aggregate_breadth_contributions(rows)
        self.assertEqual(result["2026-06-04T14:00:00+05:30"]["above"], 1)
        self.assertEqual(result["2026-06-04T14:00:00+05:30"]["total"], 2)
        self.assertEqual(result["2026-06-04T14:00:00+05:30"]["market_breadth"], 0.5)
        self.assertEqual(result["2026-06-04T14:05:00+05:30"]["market_breadth"], 1.0)

    def test_setup_scope_is_exact_and_missing_scope_validates_book(self) -> None:
        active = [*TARGET_SETUPS, "B_SETUP", "A_SETUP"]
        self.assertEqual(setups_for_scope("missing", active), TARGET_SETUPS)
        self.assertEqual(
            setups_for_scope("all_active", active),
            tuple(sorted(active)),
        )
        with self.assertRaisesRegex(ValueError, "missing supplemental setups"):
            setups_for_scope("missing", ["A_SETUP"])


if __name__ == "__main__":
    unittest.main()
