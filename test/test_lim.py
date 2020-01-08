import pandas as pd
from mscommod import lim
import unittest


class TestMP(unittest.TestCase):

    def test_getSymbol1(self):
        res = lim.getSymbol('AAQZV00', clean=False)
        self.assertIn('ask(AAQZV00)', res.columns)

    # def test_getSymbol2(self):
    #     res = lim.getSymbol('BRN')
    #     self.assertIn('ask(AAQZV00)', res.columns)



if __name__ == '__main__':
    unittest.main()