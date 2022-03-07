from audioop import add
from .moralis import Moralis
from .crypto_compare import CryptoCompare
from .timer import RateLimited
import os
import yaml
import pandas as pd

TOKEN_SYMBOL_COL = os.getenv('TOKEN_SYMBOL_COL')
TOKEN_ADDRESS_COL = os.getenv('TOKEN_ADDRESS_COL')
ID_COL = os.getenv('ID_COL')
MORALIS_MAX_REQ_PER_MINUTE = float(os.getenv('MORALIS_MAX_REQ_PER_MINUTE'))
CRYPTO_COMPARE_MAX_REQ_PER_SEC = float(os.getenv('CRYPTO_COMPARE_MAX_REQ_PER_SEC'))
DATE_RANGE_COL = os.getenv('DATE_RANGE_COL')
LOCAL_CACHE_FILE = os.getenv('LOCAL_CACHE_FILE', '')

MORALIS_MAX_REQ_PER_SEC = MORALIS_MAX_REQ_PER_MINUTE / 60
MORALIS_NO_PRICE_MESSAGE = os.getenv('MORALIS_NO_PRICE_MESSAGE')
SEP = ';'

m = Moralis(os.environ['MORALIS_APY_KEY'])
cc = CryptoCompare(os.environ['CRYPTO_COMPARE_API_URL'])
# Load tokens data
with open(os.environ['TOKENS_YAML']) as f:
        tokens_dict = yaml.safe_load(f)
        STABLE_SYMBOLS = tokens_dict.get('STABLE_SYMBOLS')
        STABLE_ADDRESSES = tokens_dict.get('STABLE_ADDRESSES')
        ETH_SYMBOLS = tokens_dict.get('ETH_SYMBOLS')
class FinanceParser:
    def __init__(self, df):
        self.df = df
        self.cache = None
        self.cache_set = set()
        if os.path.isfile(LOCAL_CACHE_FILE):
            pass
            # self.cache = pd.read_csv(LOCAL_CACHE_FILE, sep=SEP)
            # self.cache_set = set()
            # for _, row in self.cache.iterrows():
            #     self.cache_set.add((row[TOKEN_ADDRESS_COL], row[DATE_RANGE_COL]))


    def get_transactions_prices(self):
        # Filer out all transactions realted to stables and ETH
        self.df_erc20 = self.df.loc[
        (~self.df[TOKEN_SYMBOL_COL].isin(STABLE_SYMBOLS)) &
        (~self.df[TOKEN_SYMBOL_COL].isin(ETH_SYMBOLS)) &
        (~self.df[TOKEN_ADDRESS_COL].isin(STABLE_ADDRESSES))
        ]
        self.df_others = (self.df.loc[~self.df[ID_COL].isin(
            self.df_erc20[ID_COL])])
            
        df = self.get_erc20_transactions_data()
        df.to_csv(LOCAL_CACHE_FILE, sep=SEP)

        df = pd.concat([df, self.df_others])

        df = self.get_eth_price_by_ts(df)
        df.to_csv('df_prices_eth.csv', sep=SEP)

        return df
    
    @RateLimited(MORALIS_MAX_REQ_PER_SEC)
    def get_erc20_transactions_data(self):
        data_list = []
        _df = self.df_erc20.copy()
        unique_addresses = _df[TOKEN_ADDRESS_COL].unique()
        _df = _df.sort_values(
            by=DATE_RANGE_COL,
            ascending=False)
        for a in unique_addresses:
            tmp_df = _df.loc[_df[TOKEN_ADDRESS_COL]==a]
            continue_bool= True
            for _, row in tmp_df.iterrows():
                date = row[DATE_RANGE_COL]
                address = row[TOKEN_ADDRESS_COL]
                data = {
                    "token_id":address,
                     DATE_RANGE_COL:date
                }
                # Check if present in cache
                if ((row[TOKEN_ADDRESS_COL], row[DATE_RANGE_COL]) 
                in self.cache_set):
                    continue
                if continue_bool:
                    # If API has data for this contract address at this date   
                    block = m.query(
                            method="dateToBlock", 
                            date=date
                            )
                    data.update({
                        "block":block, 
                    })
                    price = m.query(
                            method="erc20_price", 
                            block=block,
                            address=address
                            )
                    # Merge dicts
                    data = {**data, **price}
                    if price.get('message', None) == MORALIS_NO_PRICE_MESSAGE:
                        # Flag contrac and date to avoid next API calls
                        continue_bool = False
                else:
                    data.update({
                        "message":MORALIS_NO_PRICE_MESSAGE, 
                    })
                data_list.append(data)
        
        _df_prices = pd.DataFrame(data_list)

        return _df.merge(_df_prices, on=[TOKEN_ADDRESS_COL, DATE_RANGE_COL], how='left')


    @RateLimited(CRYPTO_COMPARE_MAX_REQ_PER_SEC)
    def get_eth_price_by_ts(self, df):
        data = []
        for _, row in df.iterrows():
            ts = row[DATE_RANGE_COL]
            
            eth_data = cc.query(
                method='ETH_price_by_ts',
                ts=ts
            )
            eth_data.update({
                DATE_RANGE_COL:ts,
            })
            data.append(eth_data)
        _df_prices = pd.DataFrame(data)

        return pd.concat([df, _df_prices], axis=1)        