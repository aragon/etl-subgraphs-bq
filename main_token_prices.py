import pandas as pd
from pathlib import Path
import os 
from dotenv import load_dotenv
from utils.the_graph import GraphQuery
from utils.argparser import args
import re


def main(testing_mode=True):
    from utils.bq import BQ_table # Import GCP after setting env_vars (credentials)

    API_URL = os.getenv('API_URL')
    BQ_DATASET = os.getenv('BQ_DATASET')
    
    if testing_mode: SPACES_BQ_TABLE = "test_" + SPACES_BQ_TABLE

    # Load Spaces
    spaces_table = BQ_table(BQ_DATASET, SPACES_BQ_TABLE)

    last_update:str = '0'
    df_spaces_base = pd.DataFrame()
    if spaces_table.exists:
            df_spaces_base = spaces_table.select_all()
            ids = set(df_spaces_base['id'])
    
    if not spaces_table.exists:
        spaces_table.create_table_from_df(
            df=df_spaces_new,
            partitioned_day=True)

    if not df_spaces_new.empty:
        errs = spaces_table.uplaoad_df_to_bq(df_spaces_new)
        if errs:
            print(f'Spaces: Execution ended with {len(errs)} errors. Check Logging.')
        print(f"""Space: Table: {spaces_table.table_id}. Shape: {df_spaces_new.shape}""")

'''

    - get last timestamp (createdAt)
    - get transactions at createdAt+1
    - for each transaction
    - - if not ETH or STABLE
		- get block height query 
		- get price for contract address at height
    - - if ETH
        - get USD price
    - - if STABLE
        - get ETH price
    - upload new rows
'''
    return lastt


# Set creds
os.environ['MORALIS_APY_KEY'] = str(open(os.getenv('LOCAL_MORALIS_API_KEY')).read())
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('LOCAL_GOOGLE_APPLICATION_CREDENTIALS')

