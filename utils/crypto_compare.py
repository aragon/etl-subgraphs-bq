'''
https://min-api.cryptocompare.com/documentation
https://min-api.cryptocompare.com/pricing

Api limit: 
- 100,000 reqs/month
- 50,000 reqs/day
- 2,500 reqs/min -> 40 per second
- 50 per second

Api request cost: 
- eth_blockNumber = 1 request
- erc20/{address}/price = 3 requests

As of 23/02/2022:
- 18,442 transfers
- ~11k were stable or ETH

18,442 / 40 = 450 -> 450/60 = 7.5 min
'''
import collections
import requests
import os
import json

METHODS_ENUM = {
    1 : "ETH_price_by_ts",
    }


class CryptoCompare:
    def __init__(self, api_key, cache=False):
        self.api_key = api_key
        self.api_url = os.getenv('CRYPTO_COMPARE_API_URL') 
        self.headers = {
            "accept" : "application/json",
            "authorization": f"Apikey {self.api_key}"
            }

    def query(self, method, **kwargs):
        if method == METHODS_ENUM.get(1):
            ts = kwargs.get('ts')
            url = f"{self.api_url}data/pricehistorical?fsym=ETH&tsyms=USD&ts={ts}"
        
        r = requests.get(url, headers=self.headers)
        r_parsed = Response(r, method).parse()
        return r_parsed


class Response:
    def __init__(self, r, method):
        self.r = r
        self.method = method
        self.content = json.loads(r.content)
        self.status_code = r.status_code
    
    def parse(self):
        output = None
        if self.method == METHODS_ENUM.get(1):
            output = flatten(self.content)
        
        return output

    
def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)
    


#### TEMP CODE FOR LOCAL DEV
# Set creds
# os.environ['CRYPTO_COMPARE_API_URL'] =  'https://min-api.cryptocompare.com/'
# os.environ['CRYPTO_COMPARE_API_KEY'] = str(open('.key/crypto_compare').read())

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'aragon-analytics-6fffcc116564.json'

# m = CryptoCompare(os.environ['CRYPTO_COMPARE_API_KEY'])
# ts = "1540855996"

# price = m.query("ETH_price_by_ts", ts=ts)

# print(price)