import unittest
import numpy as np
import pandas as pd
from g3_v23_adx_rsi_20261106.entry_sl_20261106.study import excursions,fit_models,ceil_step


class StudyTests(unittest.TestCase):
    def test_entry_and_exit_extremes_are_only_upper_bounds(self):
        path={'low':np.array([90.,99.,80.]),'high':np.array([120.,101.,130.]),'close':np.array([100.,100.,110.])}
        result=excursions(path,0,2,100.,101.,True)
        self.assertAlmostEqual(result['mae_lower_pct'],1.)
        self.assertAlmostEqual(result['mae_upper_pct'],20.)
        self.assertAlmostEqual(result['mfe_lower_pct'],1.)
        self.assertAlmostEqual(result['mfe_upper_pct'],30.)

    def test_short_excursions_and_same_bar_trade(self):
        path={'low':np.array([80.]),'high':np.array([120.]),'close':np.array([100.])}
        result=excursions(path,0,0,100.,101.,False)
        self.assertAlmostEqual(result['mae_lower_pct'],1.)
        self.assertAlmostEqual(result['mfe_lower_pct'],0.)
        self.assertAlmostEqual(result['mae_upper_pct'],20.)

    def test_quantization_rounds_up_without_floating_extra_step(self):
        self.assertAlmostEqual(ceil_step(.868438,.05),.90)
        self.assertAlmostEqual(ceil_step(1.,.05),1.)

    def test_fit_rejects_later_labels(self):
        features=pd.DataFrame([dict(sid=i,day='2026-10-05',atr14_pct=.1) for i in range(50)])
        labels=pd.DataFrame([dict(sid=i,day='2026-10-06',baseline_winner=True,mae_lower_pct=.5,mae_upper_pct=.5) for i in range(50)])
        with self.assertRaises(ValueError):
            fit_models(features,labels,['2026-10-05'])

    def test_later_feature_rows_cannot_change_fit(self):
        features=pd.DataFrame([dict(sid=i,day='2026-10-05',atr14_pct=.1+i*.001) for i in range(100)])
        labels=pd.DataFrame([dict(sid=i,day='2026-10-05',baseline_winner=True,mae_lower_pct=.5,mae_upper_pct=.5) for i in range(100)])
        first=fit_models(features,labels,['2026-10-05'])
        extra=pd.DataFrame([dict(sid=1001,day='2026-11-05',atr14_pct=999.)])
        self.assertEqual(first,fit_models(pd.concat([features,extra]),labels,['2026-10-05']))


if __name__=='__main__':
    unittest.main()
