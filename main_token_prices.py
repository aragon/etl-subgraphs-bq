from dotenv import load_dotenv
from utils.argparser import args
# Set creds
_ENV_VARS_PATH = './env_vars/mainnet_client_finance_transactions_prices.env'
ENV_VARS_PATH = args.env_vars if args.env_vars != None else _ENV_VARS_PATH
print('ENV_VARS_PATH:', ENV_VARS_PATH)
load_dotenv(dotenv_path=ENV_VARS_PATH, override=True)
import pandas as pd
import os
import time
from datetime import datetime,timezone


def main(testing_mode=True):
    print(time.strftime("%Y/%m/%d-%H:%M:%S"))

    # Import after setting env_vars (credentials)
    from utils.bq import BQ_table 
    from utils.finance_transactions import FinanceParser

    BQ_DATASET = os.getenv('BQ_DATASET')
    BQ_TABLE_ORIG = os.getenv('BQ_TABLE_ORIG')
    BQ_TABLE_OUT = os.getenv('BQ_TABLE_OUT')
    DATE_RANGE_COL = os.getenv('DATE_RANGE_COL')
   
    if testing_mode: BQ_TABLE_OUT = "test_" + BQ_TABLE_OUT

    table_orig = BQ_table(BQ_DATASET, BQ_TABLE_ORIG)
    table_out = BQ_table(BQ_DATASET, BQ_TABLE_OUT)

    now_utc = datetime.now(timezone.utc)
    ts = int(now_utc.timestamp()*1000000)

    last_update:str = '0'
    if table_out.exists:
            last_update = table_out.get_last_block(DATE_RANGE_COL)
            last_update = str(last_update) if pd.notnull(last_update) else '0'
    
    print("last_update: ", last_update)
    ## Get base df from createdAt+1
    df_base = table_orig.select_all()
    
    fp = FinanceParser(df_base)
    df = fp.get_token_prices()
    df['timestamp_utc'] = ts
    
    if df.empty:
        return f'No new rows to add to Table: {table_out.table_id}.'

    if not table_out.exists:
        table_out.create_table_from_df(
            df=df,
            partitioned_day=True)

    errs = table_out.uplaoad_df_to_bq(df)
    
    print(time.strftime("%Y/%m/%d-%H:%M:%S"))

    if errs:
        return f'Execution ended with {len(errs)} errors. Check Logging.'
    return f'Execution succeded. Table: {table_out.table_id}. Shape: {df.shape}'

if args.local:
    # Set google creds
    os.environ['MORALIS_APY_KEY'] = str(open(os.getenv('LOCAL_MORALIS_API_KEY')).read())
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('LOCAL_GOOGLE_APPLICATION_CREDENTIALS')
    args.testing = False

print(main(testing_mode=args.testing))