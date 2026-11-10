"""Data invariants that matter for full-pool ranking and frozen-source parity."""
from datetime import date
from pathlib import Path
import unittest

import pandas as pd

from . import data, policy


class DataTests(unittest.TestCase):
    def test_full_saved_pool_reproduces_original_selection(self):
        audit, _, _ = data._saved_history()
        self.assertEqual(len(audit), 6537)
        eligible = audit.loc[audit.v9_filter_pass.eq(True)].copy()
        self.assertEqual(len(eligible), 1445)
        eligible['v24_core_pass'] = [data.selection._passes(
            row, data.selection.CORE[row['setup_id']], date.fromisoformat(row['day']))
            for row in eligible.to_dict('records')]
        restored, _ = policy.select(eligible, 'baseline_v23')
        original = data.strict.apply(audit.loc[audit.v9_selected.eq(True)].copy())
        self.assertEqual(sorted(restored.sid.tolist()), sorted(original.sid.tolist()))
        self.assertTrue((eligible.v9_selected.eq(False)).any())
        self.assertTrue((eligible.v24_core_pass & eligible.v10_g_f_core.eq(False)).any())

    def test_outcomes_rejected_but_source_gap_flag_is_allowed(self):
        frame = pd.DataFrame([dict(sid=1, day='2026-07-29', setup_id='0926_LONG',
            tradingsymbol='X', v9_filter_pass=True, v9_selected=True,
            v24_core_pass=False, gap_filled=False)])
        record = dict(audit=frame, minutes=pd.DataFrame({'tradingsymbol': ['X']}),
                      baseline_selected_sids=[1])
        data._validate(record, 0, '2026-07-29')
        frame['portfolio_net_profit_rupees'] = 5000
        with self.assertRaisesRegex(ValueError, 'outcomes'):
            data._validate(record, 0, '2026-07-29')

    def test_cache_cannot_write_to_older_versions(self):
        with self.assertRaisesRegex(ValueError, 'isolated V24'):
            data.load_day(0, '2026-07-29', Path(data.ROOT / 'g3_v18_20261102'))

    def test_sid_cohort_is_fixed(self):
        with self.assertRaisesRegex(ValueError, 'Day index'):
            data.load_day(1, '2026-07-29', data.PACKAGE / 'unused_test_cache')
        self.assertEqual(data.days[-2:], ['2026-11-06', '2026-11-09'])
        self.assertEqual(len(data.days), 68)

    def test_cached_replay_pins_payloads_and_provenance_not_mutable_ancestors(self):
        proof = dict(loader_source='HASH_VERIFIED_PRESELECTION_RESEARCH_CACHE',
            feature_path='features.parquet', features_sha256='feature-hash',
            minute_path='minutes.parquet', minutes_sha256='minute-hash',
            provenance_path='provenance.json', provenance_sha256='document-hash',
            breadth_universe=dict(path='universe.parquet', sha256='universe-hash'),
            original_inputs=[dict(path='mutable_futures.parquet', sha256='historic-ancestor-hash')])
        consumed = data._consumed_cache_inputs(proof)
        self.assertEqual(set(consumed.values()),
                         {'feature-hash', 'minute-hash', 'document-hash', 'universe-hash'})
        self.assertNotIn(str(Path('mutable_futures.parquet').resolve()), consumed)
        # Ancestry remains in the hashed provenance document, not silently deleted.
        self.assertEqual(data._identities(proof)[str(Path('mutable_futures.parquet').resolve())],
                         'historic-ancestor-hash')
        del proof['minutes_sha256']
        with self.assertRaisesRegex(ValueError, 'Missing actual cache input'):
            data._consumed_cache_inputs(proof)

    def test_all_manifested_historical_coverage_and_only_known_gap(self):
        _, proofs, _ = data._saved_history()
        gaps = {}
        for day, proof in proofs.items():
            coverage = data._expansion_coverage(proof['expansion'], pinned_historical=True)
            if coverage['missing_appended_rows']:
                gaps[day] = coverage['missing_appended_rows']
                self.assertEqual(coverage['source_issue_symbols'],
                                 ['CUB', 'DATAPATTNS', 'NAVINFLUOR', 'SYRMA'])
        self.assertEqual(gaps, {'2026-10-28': 116})
        with self.assertRaisesRegex(ValueError, 'Incomplete newly rebuilt'):
            data._expansion_coverage(proofs['2026-10-28']['expansion'], pinned_historical=False)
        broken = dict(proofs['2026-10-28']['expansion'], missing_append_rows=115)
        with self.assertRaisesRegex(ValueError, 'row accounting'):
            data._expansion_coverage(broken, pinned_historical=True)


if __name__ == '__main__':
    unittest.main()
