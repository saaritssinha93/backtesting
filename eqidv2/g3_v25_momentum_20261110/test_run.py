import unittest
from unittest.mock import patch
import pandas as pd
from . import run,policy


class SelectionTests(unittest.TestCase):
    def test_later_outcomes_cannot_change_validation_choice(self):
        validation=['2026-10-01','2026-10-02']
        arms=['baseline_v24']+[run.arm_name(f,t) for f in policy.FAMILIES for t in policy.TARGETS]
        books={a:pd.DataFrame({'day':['2026-10-01','2026-10-02','2026-11-09'],run.PNL:[100,200,-500]}) for a in arms}
        orders={a:b[['day']].copy() for a,b in books.items()}
        first=run.choose(books,orders,validation)
        for index,book in enumerate(books.values()):
            book.loc[book.day.eq('2026-11-09'),run.PNL]=10_000_000*(index+1)
        second=run.choose(books,orders,validation)
        self.assertEqual(first,second)
        self.assertIsNone(first['validation_chosen'])

    def test_every_criterion_is_required(self):
        days=[f'2026-10-{n:02d}' for n in range(1,11)]
        arms=['baseline_v24']+[run.arm_name(f,t) for f in policy.FAMILIES for t in policy.TARGETS]
        books={a:pd.DataFrame({'day':days*4,run.PNL:[100.]*40}) for a in arms}
        orders={a:b[['day']].copy() for a,b in books.items()}
        result=run.choose(books,orders,days)
        self.assertIsNotNone(result['validation_chosen'])
        self.assertTrue(all(r['qualified'] for r in result['variants']))
        for a in arms[1:]:books[a][run.PNL]=50.
        changed=run.choose(books,orders,days)
        self.assertIsNone(changed['validation_chosen'])
        self.assertTrue(all(not r['checks']['mean_at_least_baseline'] for r in changed['variants']))


if __name__=='__main__':unittest.main()
