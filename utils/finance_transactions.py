from .moralis import Moralis
from .timer import RateLimited

MORALIS_MAX_REQ_PER_MINUTE = 1500 # TODO: SEND TO ENV_VARS
MORALIS_MAX_REQ_PER_SEC = MORALIS_MAX_REQ_PER_MINUTE / 60
CRYPTO_COMPARE_MAX_REQ_PER_SEC = 40 # https://min-api.cryptocompare.com/pricing # TODO: SEND TO ENV_VARS

# TODO: ADD LOCAL CACHE
@RateLimited(MORALIS_MAX_REQ_PER_SEC)
def get_erc20_transactions_data(df):
    pass

@RateLimited(CRYPTO_COMPARE_MAX_REQ_PER_SEC)
def get_eth_price_by_ts(df):
    pass