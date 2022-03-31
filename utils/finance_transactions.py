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
DATE_RANGE_COL = os.getenv('DATE_RANGE_COL')
MORALIS_MAX_REQ_PER_SEC = MORALIS_MAX_REQ_PER_MINUTE / 60
MORALIS_NO_PRICE_MESSAGE = os.getenv('MORALIS_NO_PRICE_MESSAGE')
SEP = ';'

m = Moralis(os.environ['MORALIS_APY_KEY'])
# Load tokens data
with open(os.environ['TOKENS_YAML']) as f:
        tokens_dict = yaml.safe_load(f)
        STABLE_SYMBOLS = tokens_dict.get('STABLE_SYMBOLS')
        STABLE_ADDRESSES = tokens_dict.get('STABLE_ADDRESSES')
        ETH_SYMBOLS = tokens_dict.get('ETH_SYMBOLS')
class FinanceParser:
    def __init__(self, df):
        self.df = df

    def get_token_prices(self):
        return _get_token_prices(self.df)

@RateLimited(MORALIS_MAX_REQ_PER_SEC)
def _get_token_prices(df):
    '''expects a df with unique contract addresses'''
    data_list = []
    for _, row in df.iterrows():
            address = row[TOKEN_ADDRESS_COL]
            token_name = row[TOKEN_SYMBOL_COL]
            data = {
                "token_id":address,
                "token_name":token_name,
                }

            price = m.query(
                        method="erc20_price",
                        address=address
                        )
            # Merge dicts
            data = {**data, **price}
            data_list.append(data)
    
    _df_prices = pd.DataFrame(data_list)

    return _df_prices


@RateLimited(MORALIS_MAX_REQ_PER_SEC)
def get_erc20_transactions_data_by_date(df_erc20):
    data_list = []
    _df = df_erc20.copy()
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