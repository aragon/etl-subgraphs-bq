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
        if os.path.isfile(LOCAL_CACHE_FILE):
            pass
            self.cache = pd.read_csv(LOCAL_CACHE_FILE, sep=';')

    def get_transactions_prices(self):
        self.df_erc20 = self.df.loc[
        (~self.df[TOKEN_SYMBOL_COL].isin(STABLE_SYMBOLS)) &
        (~self.df[TOKEN_SYMBOL_COL].isin(ETH_SYMBOLS)) &
        (~self.df[TOKEN_ADDRESS_COL].isin(ETH_SYMBOLS))
        ]
        self.df_others = self.df.loc[~self.df[ID_COL].isin(self.df_erc20[ID_COL])]

        # TMP
        unique_addresses = self.df_erc20[TOKEN_ADDRESS_COL].unique()
        rows = []
        for a in unique_addresses:
            if isinstance(self.cache, pd.DataFrame) and a in self.cache[TOKEN_ADDRESS_COL].values:
                continue
            tmp_df = self.df_erc20.loc[self.df_erc20[TOKEN_ADDRESS_COL]==a]
            max = (tmp_df[DATE_RANGE_COL].max())
            min = (tmp_df[DATE_RANGE_COL].min())
            rows.append(
                tmp_df.loc[tmp_df[DATE_RANGE_COL]==min]
                )
            rows.append(
                tmp_df.loc[tmp_df[DATE_RANGE_COL]==max]
                )

        if rows:
            tmp_dfs = pd.concat(rows)
            tmp_dfs.to_csv('tmp_dfs.csv')
        else:
            tmp_dfs = self.cache.copy()
        self.df_erc20 = pd.concat([self.df_erc20, tmp_dfs])

        df = self.get_erc20_transactions_data()
        df.to_csv('df_prices.csv')

        df = self.get_eth_price_by_ts(df)
        df.to_csv('df_prices_eth.csv')

        return df
    
    @RateLimited(MORALIS_MAX_REQ_PER_SEC)
    def get_erc20_transactions_data(self):
        data = []
        _df = self.df_erc20.copy()
        for _, row in _df.iterrows():
            date = row[DATE_RANGE_COL]
            address = row[TOKEN_ADDRESS_COL]
            price = None
            block = m.query(
                    method="dateToBlock", 
                    date=date
                    )
            price = m.query(
                    method="erc20_price", 
                    block=block,
                    address=address
                    )
            price.update({
                "token_id":address,
                "block":block,
                "ts":date
            })
            print(price)
            data.append(price)
        _df_prices = pd.DataFrame(data)

        return pd.concat([_df, _df_prices])


    @RateLimited(CRYPTO_COMPARE_MAX_REQ_PER_SEC)
    def get_eth_price_by_ts(df):
        data = []
        for _, row in df.iterrows():
            ts = row[DATE_RANGE_COL]
            
            eth_data = cc.query(
                method='ETH_price_by_ts',
                ts=ts
            )
            eth_data.update({
                "ts":ts,
            })
            print(eth_data)
            data.append(eth_data)
        _df_prices = pd.DataFrame(data)

        return pd.concat([df, _df_prices])        