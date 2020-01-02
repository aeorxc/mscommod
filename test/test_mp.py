import pandas as pd
from mscommod import mp
import unittest


class TestMP(unittest.TestCase):

    def test_getts(self):
        res = mp.ts('BRN-Q0F', 'ICE_EuroFutures')
        self.assertEqual(res.loc[pd.to_datetime('2017-03-31'),'settlement_price(BRN-Q0F)'], 53.32)

    def test_getfwd(self):
        res = mp.fwd('BRN', 'ICE_EuroFutures_continuous')
        self.assertEqual(res.loc[pd.to_datetime('2009-11-16'),'settlement_price(BRN_001_Month)'], 78.76)

    def test_getfeed(self):
        res = mp.feeds()
        print(res)


if __name__ == '__main__':
    unittest.main()