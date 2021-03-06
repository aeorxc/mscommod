import pandas as pd
from mscommod import mp
import unittest


class TestMP(unittest.TestCase):

    def test_feed(self):
        res = mp.feed('Platts_EB')
        self.assertEqual(res['name'].drop_duplicates().iloc[0], 'Platts_EB')
        self.assertEqual(res['dataSource'].drop_duplicates().iloc[0], 'Platts')

    def test_feedkey(self):
        res = mp.feedkey('Platts_EB')
        self.assertEqual(res, 'Code')

        res = mp.feedkey('ICE_EuroFutures')
        self.assertEqual(res, 'Symbol')

    def test_search(self):
        res = mp.search('AAIBI00')
        self.assertEqual(len(res['results']), 1)
        self.assertEqual(res['results'][0]['feed'], 'Platts_AE')

    def test_search2(self):
        res = mp.search('BRN_001_Month')
        self.assertEqual(len(res['results']), 2)
        self.assertEqual(res['results'][0]['feed'], 'ICE_EuroFutures_continuous')

    def test_query(self):
        q = """
        var $Source = morn.Product.create("CME_NymexFutures_EOD", ["NG"], ["Settlement_Price"] );
        var Source = forward_curve($Source, get_curve_date($Source, today()), "Month");
        Source = drop_nans(Source);
        """
        res = mp.query(q)
        self.assertEqual(type(res.index), pd.core.indexes.datetimes.DatetimeIndex)

    def test_query_fail(self):
        q = """
        var $Source = morn.Product.create("CME_NymexFutures_FAKE", ["NGFAKE"], ["Settlement_Price"] );
        var Source = forward_curve($Source, get_curve_date($Source, today()), "Month");
        Source = drop_nans(Source);
        """
        res = mp.query(q)
        self.assertEqual(res, None)

    def test_query1(self):
        q = """
    var $TIME_ZONE = TimeZone.get('Europe/London');
    var $RUN_DATE = "2020-02-10";
    var $TODAY = today($TIME_ZONE);
    var $NOW = now($TIME_ZONE);
    var $FP = morn.Product.create('ICE_EuroFutures', ['G'], 'Settlement_Price');
    var $Brent = morn.Product.create('ICE_EuroFutures', ['BRN'], 'Settlement_Price');
    var Brent = forward_curve($Brent, $RUN_DATE, "month");
    Brent = drop_nans(Brent);
    var GasOil = forward_curve($FP, $RUN_DATE, "month");
    GasOil = drop_nans(GasOil);
    var Crack = cell_diff(cell_quotient(GasOil, 7.45), Brent);
    as.csv(union(Crack));
        """
        res = mp.query(q)
        self.assertEqual(type(res.index), pd.core.indexes.datetimes.DatetimeIndex)

    def test_clean_column(self):
        res = mp.series('BRN-Q0F', 'ICE_EuroFutures', clean_columns='symbol')
        self.assertEqual(res.columns[0], 'BRN-Q0F')

    def test_getts1(self):
        res = mp.series('BRN-Q0F', 'ICE_EuroFutures')
        self.assertEqual(res.loc[pd.to_datetime('2017-03-31'),'settlement_price(BRN-Q0F)'], 53.32)

    def test_getts1a(self):
        res = mp.series('BRN_001_Month',)# 'ICE_EuroFutures_continuous')
        self.assertEqual(res.loc[pd.to_datetime('2020-01-02'),'settlement_price(BRN_001_Month)'], 66.25)

    def test_getts1b(self):
        res = mp.series('BRN_001_Month', 'ICE_EuroFutures_continuous', column='settlement_price')
        self.assertEqual(res.loc[pd.to_datetime('2020-01-02'),'settlement_price(BRN_001_Month)'], 66.25)

    def test_getts1c(self):
        res = mp.series('CL_001_Month', column='settlement_price')
        self.assertEqual(res.loc[pd.to_datetime('2020-01-02'),'settlement_price(CL_001_MONTH)'], 61.18)

    def test_getts2(self):
        res = mp.series('PJABA00', 'Platts_EB', feedkeyname='Code')
        self.assertEqual(res.loc[pd.to_datetime('2019-11-04'),'close(PJABA00)'], 635.25)

    def test_getts3(self):
        res = mp.series('AAIBI00')
        self.assertEqual(res.loc[pd.to_datetime('2019-12-01'), 'close(AAIBI00)'], 713.65)

    def test_getts4(self):
        res = mp.series('Country=AT&Product=GASDIES&Flow=CLOSTLV&Unit=KBBL', feedname='Jodi_GlobalOil')
        self.assertEqual(res.loc[pd.to_datetime('2019-01-01')].iloc[0], 7818.08)

    def test_getCurve(self):
        res = mp.curve('BRN', 'ICE_EuroFutures')
        self.assertEqual(res.columns[0], 'BRN')

        res = mp.curve('BRN')
        self.assertEqual(res.columns[0], 'BRN')

    def test_getCurveHist(self):
        res = mp.curve('BRN', 'ICE_EuroFutures', curvedate='2020-01-02')
        self.assertEqual(res.columns[0], 'BRN')
        self.assertEqual(res['BRN']['2020-03-01'], 66.25)


if __name__ == '__main__':
    unittest.main()