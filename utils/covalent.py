from .covalent_api import Session, ClassA
import pandas as pd

class Query:
    def __init__(self, api_key):
        if not api_key or len(api_key) > 1:
            raise 'Invalide Covalent key.'
        self.session = Session(
            server_url='https://api.covalenthq.com',
            api_key=api_key
            )

    def _init_class_a(self):
        self.class_a = ClassA(self.session)

    def get_log_events_by_contract_address(
        self,
        chain_id,
        address,
        starting_block,
        page_size=10**4,
        ending_block='latest'):
        # https://www.covalenthq.com/docs/api/#get-/v1/{chain_id}/events/address/{address}/       
        self._init_class_a()

        r = self.class_a.get_log_events_by_contract_address(
            chain_id=chain_id,
            address=address,
            starting_block=starting_block,
            ending_block=ending_block,
            page_size=page_size
            )

        return Response(r)
        

class Response:
    def __init__(self, response):
        self.response = response
        self.data = response.get('data')
        self.error = response.get('error')
        self.error_code = response.get('error_code')
        self.error_message = response.get('error_message')
        if self.data:
            self.items = self.data.get('items')
            self.len = len(self.items)
        else:
            self.items = None
        
        
        
    def _get_params(self):
        # TODO: modularize
        rows_list = []
        if self.items:    
            self.df_items = pd.DataFrame(self.items)
            for _, row in self.df_items.iterrows():
                base_dict = row.to_dict()
                base_dict.pop('decoded', None)
                decoded_dict = row['decoded']
                if decoded_dict:
                    base_dict["function_name"] = decoded_dict.get('name', None)
                    base_dict["function_signature"] = decoded_dict.get('signature', None)
                    for _param_dict in decoded_dict['params']:
                        _base_dict = base_dict.copy()
                        # Update name to clarify data meaning
                        _param_dict = {
                            'param_'+k : v for k, v in 
                            _param_dict.items()
                        }
                        _base_dict.update(_param_dict)
                        rows_list.append(_base_dict)
                else:
                    rows_list.append(base_dict)
        return rows_list

    def _get_params_2(self):
        # TODO: modularize
        self.df_items = pd.DataFrame(self.items)
        rows_list = []
        for _, row in self.df_items.iterrows():
            base_dict = row.to_dict()
            base_dict.pop('decoded', None)
            decoded_dict = row['decoded']
            if decoded_dict:
                base_dict["function_name"] = decoded_dict.get('name', None)
                base_dict["function_signature"] = decoded_dict.get('signature', None)
                params = decoded_dict['params']
                _base_dict = base_dict.copy()
                # Create empty list for every param attribute (name, type, indexed, decoded, value as of 2021/09/08)
                _param_dict = {'param_'+k : [] for p in params for k, v in p.items() }
                # Append ordered params
                for p in params:
                    for param_name, param_value in p.items(): 
                        _param_dict['param_' + param_name].append(param_value)
                # Create row
                _base_dict.update(_param_dict)
                rows_list.append(_base_dict)
            else:
                rows_list.append(base_dict)

        
        return rows_list

    def get_df(self):
        rows_list = self._get_params_2()
        df = pd.DataFrame(rows_list)
        #df = df.drop('raw_log_topics', axis=1)
        return df
            
         