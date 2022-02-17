'''
Official docs: https://docs.snapshot.org/strategies
https://snapshot.org/#/strategies
- https://snapshot.org/#/strategy/erc20-balance-of
- https://snapshot.org/#/strategy/erc721

'''
import pandas as pd
import ast
from .etherscan import Ether
import os

STRATEGIES = "['erc20-balance-of', 'erc721']"
#STRATEGIES = ast.literal_eval(os.getenv('STRATEGIES'))
e = Ether(os.getenv('ETHERSCAN_API_KEY'))

class Spaces:
    def __init__(
        self,
        df : pd.DataFrame):

        self.df = df

    def _get_spaces_strategies(self):
        fields = []
        self.df.strategies = self.df.strategies.apply(
            lambda x: ast.literal_eval(x))

        for y in self.df.strategies:
            _fields = []
            for x in y:
                name = x.get("name", "")
                params = x.get("params", {})
                for k, v in params.items():
                    field = {"_".join([name, k]) : v}
                    _fields.append(field)
            fields.append(_fields)


class Strategies:
    def __init__(
        self,
        strategies : list):
        self.strategies = strategies

    def get_total_vp(self):
        total_vp = 0
        for index, s in enumerate(self.strategies:)
            s = Strategy(s, index)
            vp = s.get_vp()
            total_vp += vp

        return total_vp
    
class Strategy:
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
            vp += e.get_total_supply_by_contract_address(
                contract_address=address
            )
        pass
    


df = pd.read_csv("df_spaces_new.csv")

df = Spaces(df)._get_spaces_strategies()