import pandas as pd
import io, os
import requests
from requests.auth import HTTPBasicAuth
from functools import lru_cache
import logging
from mscommod import symbolcache


feed_url = 'https://mp.morningstarcommodity.com/lds/feeds/{}'
ts_url = 'https://mp.morningstarcommodity.com/lds/feeds/{}/ts'
curve_url = 'https://mp.morningstarcommodity.com/lds/feeds/{}/curve?'
feed_utl = 'https://mp.morningstarcommodity.com/lds/users/{}/feeds'
search_url = "https://mp.morningstarcommodity.com/lds/search/v2/search"
query_url = "https://mp.morningstarcommodity.com/lds/formulas/run/js"

mpUserName = os.environ['MPUSERNAME'].replace('"', '')
mpPassword = os.environ['MPPASSWORD'].replace('"', '')
auth = requests.auth.HTTPBasicAuth(mpUserName, mpPassword)

headers_csv = {
        'Accept': "text/csv",
        'cache-control': "no-cache"
}

headers_json = {
    'Content-Type': 'application/json'
}


@lru_cache(maxsize=None)
def feed(feed):
    u = feed_url.format(feed)
    r = requests.get(u, auth=requests.auth.HTTPBasicAuth(mpUserName, mpPassword))
    d = pd.DataFrame(r.json())
    df = d.apply(lambda x: pd.Series(x['fields']), 1)
    df = df.rename(columns={'type':'fieldType'})
    d = pd.merge(d, df, left_index=True, right_index=True)
    return d


@lru_cache(maxsize=None)
def feedkey(feedname):
    # some caching
    if feedname.startswith('Platts'):
        return 'Code'

    if feedname.startswith('ICE'):
        if feedname.endswith('_continuous'):
            return 'Contract'
        else:
            return 'Symbol'

    df = feed(feedname)
    df = df[df['fieldType'].str.lower() == 'k']
    if len(df) == 1:
        return df['fieldName'].iloc[0]
    elif len(df) > 1: # for more than 1 key
        return list(df[df['fieldType'].str.lower() == 'k']['fieldName'].values)


@lru_cache(maxsize=None)
def search(symbol):
    j = dict()
    criteriaons = []
    d1 = dict()
    d1["type"] = "QUERY"
    d1["value"] = symbol
    criteriaons.append(d1)

    j["criterions"] = criteriaons
    j["entitlement"] = "all"
    j["isFuturesOnly"] = "false"
    j["isExcludeFutures"] = "false"
    j["hideExpiredContracts"] = "true"
    r = requests.post(search_url, json=j, auth=requests.auth.HTTPBasicAuth(mpUserName, mpPassword), headers=headers_json)

    if r.status_code == 200:
        return r.json()


def _find_symbol_metadata(symbol, feedname=None, feedkeyname=None, inc_feeddata=False):
    # check in symbolcache first
    if feedname is None:
        for feednamec in symbolcache.c:
            if symbol in symbolcache.c[feednamec]:
                feedname = feednamec
                break

    # if not in symbolcache then search
    if feedname is None:
        searchres = search(symbol)
        if isinstance(searchres, dict) and len(searchres['results']) >= 1:
            if 'feed' in searchres['results'][0]:
                feedname = searchres['results'][0]['feed']

    if feedkeyname is None:
        feedkeyname = feedkey(feedname)

    if inc_feeddata:
        feeddata = feed(feedname)
        return feedname, feedkeyname, feeddata

    return feedname, feedkeyname


def series(symbol, feedname=None, feedkeyname=None, column=None):
    feedname, feedkeyname = _find_symbol_metadata(symbol, feedname, feedkeyname)
    u = ts_url.format(feedname)
    if isinstance(feedkeyname, str):
        u = '{}?{}={}'.format(u, feedkeyname, symbol)
    elif isinstance(feedkeyname, list):
        u = '{}?{}'.format(u, symbol) # todo make a better extract of symbol components
    if column is not None:
        u = '{}&cols={}'.format(u, column)

    r = requests.get(u, auth=auth, headers=headers_csv)

    if r.status_code == 200:
        df = pd.read_csv(io.StringIO(r.text), index_col='Date')
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        df = df.dropna(how='all')
        return df
    else:
        logging.warning(r.text)


def query(querystr):
    r = requests.request("POST", query_url, headers=headers_csv, data=querystr, auth=auth)
    df = pd.read_csv(io.StringIO(r.text), header=None)
    df = df.set_index(0)
    df.index = pd.to_datetime(df.index)
    return df


def curve(symbol, feedname=None, feedkeyname=None, column='Settlement_Price', curvedate='today()'):
    feedname, feedkeyname = _find_symbol_metadata(symbol, feedname, feedkeyname)

    if curvedate != 'today()':
        curvedate = "'{}'".format(curvedate)

    q = """
        var $prod = morn.Product.create("{}", ["{}"], ["{}"]);
        var Source = forward_curve($prod, get_curve_date($prod, {}), "Month");
        Source = drop_nans(Source);
        """
    q = q.format(feedname, symbol, column, curvedate)
    res = query(q)
    res = res.rename(columns={res.columns[0]:symbol})
    return res


if __name__ == '__main__':
    # fwd('BRN', feed='ICE_EuroFutures_continuous')
    # getKeyNames('ICE_EuroFutures')
    print('test')