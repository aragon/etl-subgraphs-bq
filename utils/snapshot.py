'''
Official docs: https://docs.snapshot.org/strategies
https://snapshot.org/#/strategies
- https://snapshot.org/#/strategy/erc20-balance-of
- https://snapshot.org/#/strategy/erc721

'''
import pandas as pd
import ast
from etherscan import Etherscan
import os

STRATEGIES = "['erc20-balance-of', 'erc721']"
#STRATEGIES = ast.literal_eval(os.getenv('STRATEGIES'))
e = Etherscan("S85IISU9D7QUDP9HRHF7I5VKS6VTC9595Q")

class Spaces():
    def __init__(
        self,
        df : pd.DataFrame):

        self.df = df

    def get_vp(self):
        self.df["total_voting_power"] = df["strategies"].apply(
            lambda x: Strategies(x).get_total_vp()
        )
        return self.df

class Strategies(list):
    def __init__(
        self,
        strategies : str):
        self.strategies = ast.literal_eval(strategies)

    def get_total_vp(self):
        total_vp = 0
        for index, s in enumerate(self.strategies):
            s = Strategy(s, index)
            vp = s.get_vp()
            total_vp += vp

        return total_vp
    
class Strategy(dict):
    def __init__(
        self,
        strategy:dict,
        index:int):

        self.strategy = strategy
        self.index = index
        self.name = self.get("name", "")
        self.params = self.get("params", {})

    def get_vp(self):
        vp = 0
        if self.name in STRATEGIES:
            address = self.params.get("address")
            decimals = self.params.get("decimals")
            symbol = self.params.get("symbol")
            if address:
                _vp = e.get_total_supply_by_contract_address(
                contract_address=address)
                if _vp:
                    vp += vp
        return vp
    
os.environ['LOCAL_ETHERSCAN_API_KEY'] = '.key/etherscan'
os.environ['ETHERSCAN_API_KEY'] = str(open(
    os.getenv('LOCAL_ETHERSCAN_API_KEY')).read())

df = pd.read_csv("df_spaces_new.csv")
df = df.head(30)
df["total_voting_power"] = df["strategies"].apply(
    lambda x: Strategies(x).get_total_vp())

print(df["total_voting_power"])