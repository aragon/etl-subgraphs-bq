import json
import requests
from pathlib import Path
import re
from .utils import flatten, flatten_2
class GraphQuery:
    QUERY_FIRST = '1000'
    QUERY_SKIP = 'null'
    def __init__(
        self, 
        query_path:Path,
        api_url:str,
        gt_statement:str=None,
        gt_value:str='0',
        query_first:str=QUERY_FIRST,
        query_skip:str=QUERY_SKIP):

        if not isinstance(query_path, Path):
            query_path = Path(query_path)
        
        self.query_path = query_path
        self.api_url = api_url
        
        with open(query_path) as f:
            self.query_txt = f.read()

        # Get first query word (entity queried)
        self.q_name = re.findall(r'\w+', self.query_txt)[0]

        self.query_first = query_first
        self.query_skip = query_skip
        self.gt_statement = gt_statement
        self.gt_value = f'"{gt_value}"'

    def _post_request(self, query_txt):
        r = requests.post(
                self.api_url,
                json={'query': query_txt})
        return r

    def _parse_response(self, r):
        data = json.loads(r.text).get('data')
        errors = json.loads(r.text).get('errors')
        if errors:
            raise ValueError(f'{r.text}')
        
        data = data.get(self.q_name)
        data = self._flatten(data)
        return data


    def _flatten(self, data):
        data = [flatten(d) for d in data]
        return data
    
    def post(
        self, 
        paginate=False,
        date_filter=False):
        '''
        If `paginate` = True, string text is expected to
        have $first and $skip params to be replaced accordingly.
        It is assumes only one entity per query.

        - first: max. response length
        - skip: number of responses to skip

        If `date_filter` = True, string text is expected to
        have a date filter statement such as `where:{createdAt_gt:$createdAt_gt})`.
        In this case `gt_statement` being `$createdAt_gt`.
        `gt_value` will depend on last table update, being `0` by default.
        '''
        if date_filter:
            self.query_txt = self.query_txt.replace(
                self.gt_statement, self.gt_value)
        
        if paginate:
            return self._post_paginated()

        else: # Single query
            r = self._post_request(self.query_txt)
            data = self._parse_response(r)
            return data
    
    def _post_paginated(self):
        data_list = []
        # Initialize for first iteration 
        r_len = self.query_first
        _skip = self.query_skip
        # Continue until responses are smaller than max. available
        while (str(r_len) == self.query_first and 
        # 5000 is max for skip value in The Graph
        # Check if first iteration or less than limit
        (_skip == self.QUERY_SKIP or int(_skip) <= 5000)):
            temp_q_text = (
                self.query_txt
                .replace('$first', self.query_first)
                .replace('$skip', str(_skip))
                )
            r = self._post_request(temp_q_text)
            data = self._parse_response(r)
            if isinstance(data, list): # to avoid str
                data_list.extend(data)
            r_len = len(data)
            # Update params for pagination
            if _skip == self.query_skip:
                # First iteration
                _skip = int(self.query_first)
            else:
                # Iter to next pagination
                _skip += int(self.query_first)
        
        return data_list