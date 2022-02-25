from .moralis import Moralis
from .timer import RateLimited

MAX_REQ_PER_SEC = 25
MAX_REQ_PER_MINUTE = 1500
MAX_REQ_PER_SEC = MAX_REQ_PER_MINUTE / 60


@RateLimited(MAX_REQ_PER_SEC)
def get_transactions_data():
    pass
# cache

