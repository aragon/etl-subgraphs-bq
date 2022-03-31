from .moralis import Moralis
from .timer import RateLimited
import os

MORALIS_MAX_REQ_PER_MINUTE = os.getenv('MORALIS_MAX_REQ_PER_MINUTE')
CRYPTO_COMPARE_MAX_REQ_PER_SEC = os.getenv('CRYPTO_COMPARE_MAX_REQ_PER_SEC')

MORALIS_MAX_REQ_PER_SEC = MORALIS_MAX_REQ_PER_MINUTE / 60

# TODO: ADD LOCAL CACHE
@RateLimited(MORALIS_MAX_REQ_PER_SEC)
def get_erc20_transactions_data(df):
    pass

@RateLimited(CRYPTO_COMPARE_MAX_REQ_PER_SEC)
def get_eth_price_by_ts(df):
    pass