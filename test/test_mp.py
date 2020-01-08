import pandas as pd
from mscommod import mp
import unittest


class TestMP(unittest.TestCase):

    def test_getSources(self):
        res = mp.getSources()
        self.assertIn('ICE', res)

    def test_getFeeds(self):
        res = mp.getFeeds('ICE')
        self.assertIn('ICE_Canada_FuturesPrices', res)

    def getKeyNames(self):
        res = mp.getKeyNames('ICE_EuroFutures')
        self.assertIn('AF30Q', res)

    def test_getts1(self):
        res = mp.ts('BRN-Q0F', 'ICE_EuroFutures')
        self.assertEqual(res.loc[pd.to_datetime('2017-03-31'),'settlement_price(BRN-Q0F)'], 53.32)

    def test_getts2(self):
        res = mp.ts('PJABA00', 'Platts_EB', key='Code')
        self.assertEqual(res.loc[pd.to_datetime('2019-11-04'),'close(PJABA00)'], 635.25)


    def test_getfwd(self):
        res = mp.fwd('BRN', 'ICE_EuroFutures_continuous')
        self.assertEqual(res.loc[pd.to_datetime('2009-11-16'),'settlement_price(BRN_001_Month)'], 78.76)


if __name__ == '__main__':
    unittest.main()