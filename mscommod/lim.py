# Wrapper around market place to get it in a similar place to LIM type functionality
import pandas as pd
from mscommod import mp
from mscommod import symbolcache
from functools import lru_cache


_platts_prefixes = [
'PA',
'AA',
'PU',
'F1',
'PH',
'PJ',
'PG',
'PO',
'PP',
]


def _isPlattsSymbol(contract):
    return len(contract) == 7 and contract[:2] in _platts_prefixes


def _isArgusSymbol(self, contract):
    if '.' in contract:
        contract = contract.split('.')[0]

    if len(contract) == 9 and contract.startswith('PA'):
        return True

    return False


@lru_cache(maxsize=None)
def allFeedSymbols(feed):
    feedcomp = mp.getFeeds(feed)
    return {x:mp.getKeyNames(x) for x in feedcomp}


@lru_cache(maxsize=None)
def _symbolKey(symbol):
    """
    Given a feed find the default key, eg for Platts its Code, for ICE its Symbol
    """
    feed = _symbolFeed(symbol)
    key = mp.feedKey(feed)
    return key


@lru_cache(maxsize=None)
def _symbolFeed(symbol):
    """
    Given a symbol get the feed it could belong to
    """

    for x in symbolcache.c:
        if symbol in symbolcache.c[x]:
            return x

    if _isPlattsSymbol(symbol):
        allsymbols = allFeedSymbols('Platts')
        filtfeed = {x: x for x in allsymbols if symbol in allsymbols[x]}
        if len(filtfeed) == 1:
            return list(filtfeed.keys())[0]


def cleanres(symbol, df):
    if _isPlattsSymbol(symbol):
        df = df[[x for x in df if 'close' in x]]
        df = df.rename(columns={df.columns[0]:'close'})

    return df


def getSymbol(symbol, clean=True):
    feed, key = _symbolFeed(symbol), _symbolKey(symbol)
    res = mp.ts(symbol, feed, key)

    if clean:
        res = cleanres(symbol, res)
    return res


if __name__ == '__main__':
    # print(_symbolFeed('AAQZV00'))
    # print(getSymbol('AAQZV00'))
    print(allFeedSymbols('Platts_RI'))
    # print(getSymbol('BRN10G'))


