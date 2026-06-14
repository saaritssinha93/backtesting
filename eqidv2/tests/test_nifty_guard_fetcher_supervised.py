import unittest
from unittest.mock import patch

import eqidv2_nifty_guard_fetcher_supervised_v16_5min as nf


class NiftyGuardFetcherSupervisedTests(unittest.TestCase):
    def test_fetches_proxy_marker_first_then_true_index_as_skip_marker(self):
        calls = []

        def fake_run_one_fetch(symbol, aliases, label, skip_marker=False):
            calls.append((symbol, aliases, label, skip_marker))
            return 0

        patches = [
            patch.object(nf, "NIFTY_SYMBOL", "NIFTYBEES"),
            patch.object(nf, "NIFTY_ALIASES", "NIFTYBEES,NIFTYBEES_PROXY"),
            patch.object(nf, "NIFTY_INDEX_ENABLED", True),
            patch.object(nf, "NIFTY_INDEX_SYMBOL", "NIFTY 50"),
            patch.object(nf, "NIFTY_INDEX_ALIASES", "NIFTY,NIFTY50,NIFTY_50,NIFTY 50"),
            patch.object(nf, "NIFTY500_ENABLED", False),
            patch.object(nf, "_run_one_fetch", fake_run_one_fetch),
        ]
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6]:
            rc = nf._run_fetcher_once()

        self.assertEqual(rc, 0)
        self.assertEqual(
            calls,
            [
                ("NIFTYBEES", "NIFTYBEES,NIFTYBEES_PROXY", "NIFTYBEES_PROXY", False),
                ("NIFTY 50", "NIFTY,NIFTY50,NIFTY_50,NIFTY 50", "NIFTY50_INDEX", True),
            ],
        )

    def test_true_index_failure_is_soft_failure_after_proxy_success(self):
        calls = []

        def fake_run_one_fetch(symbol, aliases, label, skip_marker=False):
            calls.append(label)
            return 1 if label == "NIFTY50_INDEX" else 0

        with patch.object(nf, "NIFTY_INDEX_ENABLED", True), patch.object(nf, "NIFTY500_ENABLED", False), patch.object(nf, "_run_one_fetch", fake_run_one_fetch):
            rc = nf._run_fetcher_once()

        self.assertEqual(rc, 0)
        self.assertEqual(calls, ["NIFTYBEES_PROXY", "NIFTY50_INDEX"])

    def test_proxy_failure_blocks_secondary_fetches(self):
        calls = []

        def fake_run_one_fetch(symbol, aliases, label, skip_marker=False):
            calls.append(label)
            return 7

        with patch.object(nf, "NIFTY_INDEX_ENABLED", True), patch.object(nf, "NIFTY500_ENABLED", True), patch.object(nf, "_run_one_fetch", fake_run_one_fetch):
            rc = nf._run_fetcher_once()

        self.assertEqual(rc, 7)
        self.assertEqual(calls, ["NIFTYBEES_PROXY"])


if __name__ == "__main__":
    unittest.main()
