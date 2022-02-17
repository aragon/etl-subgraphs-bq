from etherscan import Etherscan


class Ether:
    def __init__(
        self,
        api_key : str):

        self.api_key = api_key
        self.eth = Etherscan(self.api_key) 

    def get_eth_balance(self, address):
        return self.eth.get_eth_balance(
            address=address
            )
    
    def get_total_supply_by_contract_address(
        self, contract_address
        ):
        return self.eth.get_total_supply_by_contract_address(
            contract_address=contract_address
            )

    
