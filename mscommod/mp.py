import pandas as pd
import requests
import io, os

ts_url = 'https://mp.morningstarcommodity.com/lds/feeds/{}/ts?Symbol={}'
fwd_url = 'https://mp.morningstarcommodity.com/lds/feeds/{}/ts?Contract={}'
feed_utl = 'https://mp.morningstarcommodity.com/lds/users/{}/feeds'

mpUserName = os.environ['MPUSERNAME'].replace('"', '')
mpPassword = os.environ['MPPASSWORD'].replace('"', '')

headers = {
        'Accept': "text/csv",
        'cache-control': "no-cache"
}


def url_to_dataframe(u):
    r = requests.get(u, auth=requests.auth.HTTPBasicAuth(mpUserName, mpPassword), headers=headers)
    df = pd.read_csv(io.StringIO(r.text), index_col='Date')
    df.index = pd.to_datetime(df.index)
    return df


def ts(symbol, feed):
    u = ts_url.format(feed, symbol)
    df = url_to_dataframe(u)
    return df


def fwd(symbol, feed, contract='001_Month'):
    symbol = '{}_{}'.format(symbol, contract)
    u = fwd_url.format(feed, symbol)
    df = url_to_dataframe(u)
    return df


def feeds():
    u = feed_utl.format(mpUserName)
    r = requests.get(u, auth=requests.auth.HTTPBasicAuth(mpUserName, mpPassword), headers=headers)
    # df = pd.read_csv(io.StringIO(r.text))
    return r#df


if __name__ == '__main__':
    fwd('BRN', feed='ICE_EuroFutures_continuous')