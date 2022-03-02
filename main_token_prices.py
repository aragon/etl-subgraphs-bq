import pandas as pd
from pathlib import Path
import os 
from dotenv import load_dotenv
from utils.the_graph import GraphQuery
from utils.argparser import args
from utils.finance_transactions import get_transactions_data
from utils.timer import RateLimited
import re


def main(testing_mode=True):
    from utils.bq import BQ_table # Import GCP after setting env_vars (credentials)

    API_URL = os.getenv('API_URL')
    BQ_DATASET = os.getenv('BQ_DATASET')
    BQ_TABLE_ORIG = os.getenv('BQ_TABLE_ORIG')
    BQ_TABLE_OUT = os.getenv('BQ_TABLE_OUT')
    DATE_RANGE_COL = os.getenv('DATE_RANGE_COL')

    
    if testing_mode: BQ_TABLE_OUT = "test_" + BQ_TABLE_OUT

    table_orig = BQ_table(BQ_DATASET, BQ_TABLE_ORIG)
    table_out = BQ_table(BQ_DATASET, BQ_TABLE_OUT)

    last_update:str = '0'
    if table_out.exists:
            last_update = table_out.get_last_block(DATE_RANGE_COL)
            last_update = str(last_update) if pd.notnull(last_update) else '0'
    ## Get base df
    df_spaces_base = table_orig.select_all_gt_block(DATE_RANGE_COL, last_update)
    
    ### QUERY createdAt+1
    df = get_transactions_data(df_spaces_base)

    if df.empty:
        return f'No new rows to add to Table: {table_out.table_id}.'

    if not table_out.exists:
        table_out.create_table_from_df(
            df=df,
            partitioned_day=True)

    errs = table_out.uplaoad_df_to_bq(df)
    if errs:
        return f'Execution ended with {len(errs)} errors. Check Logging.'
    return f'Execution succeded. Table: {table_out.table_id}. Shape: {df.shape}'



'''
    - get last timestamp (createdAt)
    - get transactions at createdAt+1
    - group transactions between ETH, stable and others
    - - if others
		- get block height query 
		- get price for contract address at height
    - - if ETH
        - get USD price
    - - if STABLE
        - get ETH price
    - upload new rows

    return last update
    insert empty row if empty?
'''

# Set creds
os.environ['MORALIS_APY_KEY'] = str(open(os.getenv('LOCAL_MORALIS_API_KEY')).read())
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('LOCAL_GOOGLE_APPLICATION_CREDENTIALS')

