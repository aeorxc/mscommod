import pandas as pd
import io
import json
import requests
import urllib.parse
import logging
import datetime
import re
import os
import numpy
import xml.etree.ElementTree as ET
from requests.auth import HTTPBasicAuth

feed_url = 'https://mp.morningstarcommodity.com/lds/feeds/{}'
ts_url = 'https://mp.morningstarcommodity.com/lds/feeds/{}/ts?{}={}'
fwd_url = 'https://mp.morningstarcommodity.com/lds/feeds/{}/ts?Contract={}'
feed_utl = 'https://mp.morningstarcommodity.com/lds/users/{}/feeds'

mpUserName = os.environ['MPUSERNAME'].replace('"', '')
mpPassword = os.environ['MPPASSWORD'].replace('"', '')

headers = {
        'Accept': "text/csv",
        'cache-control': "no-cache"
}

def url_to_dataframe(u, headers=headers):
    r = requests.get(u, auth=requests.auth.HTTPBasicAuth(mpUserName, mpPassword), headers=headers)
    df = pd.read_csv(io.StringIO(r.text), index_col='Date')
    df.index = pd.to_datetime(df.index)
    return df


def ts(symbol, feed, key='Symbol'):
    u = ts_url.format(feed, key, symbol)
    df = url_to_dataframe(u)
    return df


def feed(feed):
    u = feed_url.format(feed)
    r = requests.get(u, auth=requests.auth.HTTPBasicAuth(mpUserName, mpPassword))
    d = pd.DataFrame(r.json())


def fwd(symbol, feed, contract='001_Month'):
    symbol = '{}_{}'.format(symbol, contract)
    u = fwd_url.format(feed, symbol)
    df = url_to_dataframe(u)
    return df


def mpLogger(msg):
    logging.info(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%Z") + ' ' + msg)



def getSources(pattern='.*'):
    """
    Returns an array of available Marketplace data sources entitled for the user

    :Example:
    >>> getSources()
    ['Gfi', 'WallStreetHorizon', 'Eoxlive', ...

    :Example:
    >>> getSources('Wall.*')
    ['WallStreetHorizon']

    """
    global mpUserName
    global mpPassword

    # URL to request the sources
    mpurl = "https://mp.morningstarcommodity.com/lds/users/" + mpUserName + "/feeds"
    # change the username and password here - must be in quotes
    r = requests.get(mpurl, auth=HTTPBasicAuth(mpUserName, mpPassword))
    mpSources = []
    for x in r.json():
        if x["dataSource"] not in mpSources:
            mpSources.append(x["dataSource"])

    mpSources.sort()
    r = re.compile(pattern)
    return list(filter(r.match, mpSources))


def getFeeds(source, pattern='.*', type=0):
    """
    Returns an array of available feeds for a specific Marketplace Source.

    :param source: The Marketplace souce
    :param type: Optional, 0 = return all feeds, 1 = return Forward only, 2 = return Non-Forward only

    :Example:
    >>> getFeeds('ICE')    # returns all ICE feeds
    ['Ice_Cot', 'Ice_ClearedPower', 'Ice_ClearedOil', ...

    >>> getFeeds('ICE', 0) # returns all ICE feeds
    ['Ice_Cot', 'Ice_ClearedPower', 'Ice_ClearedOil', ...

    >>> getFeeds('ICE', 1) # returns only ICE feeds with Forward data.
    ['Ice_ClearedPower', 'Ice_ClearedOil', 'Ice_ClearedGas', ...

    >>> getFeeds('ICE', 2) # returns only ICE feeds no Forward data.
    ['Ice_Cot', 'Ice_PowerIndices', 'Ice_GasIndices', ...

    """
    global mpUserName
    global mpPassword
    mpurl = "https://mp.morningstarcommodity.com/lds/users/" + mpUserName + "/feeds"
    # change the username and password here - must be in quotes
    r = requests.get(mpurl, auth=HTTPBasicAuth(mpUserName, mpPassword))
    df = pd.DataFrame(r.json())['name']
    df.sort_values()
    return df.values


def getCurveNames(feed):
    """
    Returns an array of curve names available for a feed

    :param feed: name of feed

    :Example:
    >>> getCurveNames('ICE_EuroFutures')
    ['AF1', 'AF2', 'AF3'...

    """
    global mpUserName
    global mpPassword
    availableCurves = "https://mp.morningstarcommodity.com/lds/feeds/" + feed + "/contractroots"
    r = requests.get(availableCurves, auth=requests.auth.HTTPBasicAuth(mpUserName, mpPassword))
    if r.headers['content-type'] == 'application/json':
        mpCurves = r.json()["root"]
        mpCurves.sort()
        return mpCurves
    else:
        msg = []
        root = ET.fromstring(r.content)
        for errors in root:
            msg.append(errors.tag + ": " + errors.text)
        return msg


def extractValueColumnsNumber(s):
    return s['keys'][0]['value']


def getKeyNames(feed):
    """
    Returns an array of keys used in a feed

    :param feed: name of feed

    :Example:
    >>> getKeyNames('ICE_EuroFutures')
    ['Symbol']

    """
    global mpUserName
    global mpPassword
    availableCurves = "https://mp.morningstarcommodity.com/lds/feeds/" + feed + "/keys?limit=1000"
    r = requests.get(availableCurves, auth=HTTPBasicAuth(mpUserName, mpPassword))
    allKeys = []
    if r.headers['content-type'] == 'application/json':
        df = pd.DataFrame(r.json())
        df['key'] = df.apply(lambda x: extractValueColumnsNumber(x), 1)
        keys = list(df['key'].values)
        keys.sort()
        allKeys.append(keys)
        return allKeys[0]
    else:
        msg = []
        root = ET.fromstring(r.content)
        for errors in root:
            msg.append(errors.tag + ": " + errors.text)
        return msg


def getColumns(feed):
    """
    Returns an array of columns used in a feed

    :param feed: name of feed

    :Example:
    >>> getColumns('ICE_EuroFutures')


    ['Settlement_Price']

    """
    global mpUserName
    global mpPassword
    availableCurves = "https://mp.morningstarcommodity.com/lds/feeds/" + feed
    r = requests.get(availableCurves, auth=HTTPBasicAuth(mpUserName, mpPassword))
    cols = []
    if r.headers['content-type'] == 'application/json':
        for x in range(len(r.json()['fields'])):
            if r.json()['fields'][x]['type'].upper() == 'V':
                cols.append(r.json()['fields'][x]['fieldName'])
        return cols
    else:
        msg = []
        root = ET.fromstring(r.content)
        for errors in root:
            msg.append(errors.tag + ": " + errors.text)
        return msg




if __name__ == '__main__':
    fwd('BRN', feed='ICE_EuroFutures_continuous')
