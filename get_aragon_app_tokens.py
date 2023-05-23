import json
import requests
import pandas as pd
from utils.utils import flatten
# Load the GraphQL query from file
with open('queries/app_tokens.graphql', 'r') as f:
    query = f.read()

# Define the GraphQL endpoint URL
url = 'https://subgraph.satsuma-prod.com/qHR2wGfc5RLi6/aragon/osx-mainnet/api'
url = 'https://subgraph.satsuma-prod.com/qHR2wGfc5RLi6/aragon/osx-polygon/api'

# Send the GraphQL query and retrieve the result
response = requests.post(url, json={'query': query})
result = response.json().get('data', {})
data = result.get(list(result.keys())[0], {})
data = [flatten(x) for x in data]
df_data = pd.DataFrame(data)
# Convert the result to a SQL insert statement and save to file

# values_statement = ',\n'.join([f"({', '.join([f'\'{row[col_name]}\'' for col_name in df_data.columns])})" 
#                                for row in df_data.iterrows()])
# columns_statement = ',\n'.join([f"{col_name}" for col_name in df_data.columns])

# table_statement = f"FROM\n(\nVALUES\n{values_statement}\n) as tmp ({columns_statement})"


# with open('result.sql', 'w') as f:
#     for row in result:
#         table_name = 'table'
#         columns = row.keys()
#         values = [json.dumps(row[column]) for column in columns]
#         insert_statement = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
#         f.write(insert_statement + '\n')

