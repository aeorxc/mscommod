import pandas as pd
import requests
import os
from requests.auth import HTTPBasicAuth

mpUserName = os.environ['MPUSERNAME'].replace('"', '')
mpPassword = os.environ['MPPASSWORD'].replace('"', '')


def run():
    u = 'https://mp.morningstarcommodity.com/lds/users/gibran.afzal@rwe.com/feeds'
    r = requests.get(u, auth=requests.auth.HTTPBasicAuth(mpUserName, mpPassword))
    feeds = pd.DataFrame(r.json())

    dfs= []
    for feed in feeds['name']:
        print(feed)
        u = 'https://mp.morningstarcommodity.com/lds/feeds/{}'.format(feed)
        r = requests.get(u, auth=requests.auth.HTTPBasicAuth(mpUserName, mpPassword))
        try:
            fields = pd.DataFrame(r.json())
            dfs.append(fields)
        except Exception as ex:
            print(ex)

if __name__ == '__main__':
    run()