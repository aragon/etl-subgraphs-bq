# https://docs.moralis.io/misc/rate-limit
'''
https://docs.moralis.io/misc/rate-limit
https://moralis.io/pricing/

Api limit: 
- 1,500 reqs/min
- 10,000,000 reqs/month *

Api request cost: 
- eth_blockNumber = 1 request
- erc20/{address}/price = 3 requests

As of 23/02/2022:
- 18,442 transfers
- ~11k were stable or ETH

18,442 (block time) + 18,442 * 3 = 73,768 max requests (50 min)
8k * 4 = 32,000 (21 min)
'''
import collections
from argparse import ONE_OR_MORE
import requests
import os
import json

METHODS_ENUM = {
    1 : "dateToBlock",
    2 : "erc20_price"
    }

class Moralis:
    def __init__(self, api_key, cache=False):
        self.api_key = api_key
        self.api_url = os.getenv('MORALIS_API_URL') 
        self.headers = {
            "accept" : "application/json",
            "X-API-Key": self.api_key
            }
    
    def query(self, method, **kwargs):
        # Set Ethereum as default - https://docs.moralis.io/moralis-server/web3-sdk/intro#supported-chains
        chain = kwargs.get('chain', 'eth') 
        
        if method not in METHODS_ENUM.values():
            raise Exception(f"Method '{method}' not included in METHODS_ENUM")

        if method == METHODS_ENUM.get(1):
            date = kwargs.get('date')
            url = f"{self.api_url}dateToBlock?chain={chain}&date={date}"
        
        if method == METHODS_ENUM.get(2):
            block = kwargs.get('block')
            address = kwargs.get('address')
            url = f"{self.api_url}erc20/{address}/price?chain={chain}"
            if block:
                url += f"&to_block={block}"
        
        r = requests.get(url, headers=self.headers)
        r_parsed = Response(r, method).parse()
        return r_parsed


class Response:
    def __init__(self, r, method):
        self.r = r
        self.method = method
        self.content = getattr(r, 'content', "{}")
        self.content = json.loads(self.content)
        self.status_code = getattr(r, 'status_code', None)
        self.message = self.content.get('message')
    
    def parse(self):
        output = None
        if self.method == METHODS_ENUM.get(1):
            output = self.content.get("block") 
        
        if self.method == METHODS_ENUM.get(2):
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
# os.environ['MORALIS_API_URL'] =  'https://deep-index.moralis.io/api/v2/'
# os.environ['MORALIS_API_KEY'] = str(open('.key/moralis').read())

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'aragon-analytics-6fffcc116564.json'

# m = Moralis(os.environ['MORALIS_API_KEY'])
# date_int = '1645344471'
# address = '0x89ab32156e46f46d02ade3fecbe5fc4243b9aaed'

# block = m.query("dateToBlock", date=date_int)

# price = m.query("erc20_price", address=address, block=block)

# print(price)