import pandas as pd
from pathlib import Path
import os 
from dotenv import load_dotenv
from utils.the_graph import GraphQuery, unnest_balances
from utils.argparser import args
from datetime import datetime,timezone

def main(testing_mode=True):
    from utils.bq import BQ_table # Import GCP after setting env_vars (credentials)

    PROCESS_NAME = os.getenv('PROCESS_NAME')
    QUERY_PATH = Path(os.getenv('QUERY_DIR')).joinpath(PROCESS_NAME+'.graphql')
    API_URL = os.getenv('API_URL')
    BQ_DATASET = os.getenv('BQ_DATASET')
    BQ_TABLE = os.getenv('BQ_TABLE')
    DATE_RANGE_ATTR = os.getenv('DATE_RANGE_ATTR')
    DATE_RANGE_COL = os.getenv('DATE_RANGE_COL')
    
    if testing_mode: BQ_TABLE = "test_" + BQ_TABLE

    table = BQ_table(BQ_DATASET, BQ_TABLE)

    # SET TIMESTAMP INT UTC
    now_utc = datetime.now(timezone.utc)
    ts = int(now_utc.timestamp()*1000000)

    print("Timesptamp: ", ts)
    
    query = GraphQuery(
        query_path=QUERY_PATH,
        api_url=API_URL,
        query_first='1000',
    )

    data = query.post(
        paginate=True,
        date_filter=False)

    df = pd.DataFrame(data)
    
    if 'vault_balances' in PROCESS_NAME:
        df = unnest_balances(df)

    df['timestamp_utc'] = ts
    df['token_decimals'] = df['token_decimals'].replace({None: 0})
    df['balance'] = df['balance'].replace({None: 0})


    if df.empty:
        return f'No new rows to add to Table: {table.table_id}.'

    if not table.exists:
        table.create_table_from_df(
            df=df,
            partitioned_day=True)

    errs = table.uplaoad_df_to_bq(df)
    if errs:
        return f'Execution ended with {len(errs)} errors. Check Logging.'
    return f'Execution succeded. Table: {table.table_id}. Shape: {df.shape}'

#_ENV_VARS_PATH = './env_vars/mainnet_client_finance_vault_balances.env'
_ENV_VARS_PATH = './env_vars/mainnet_client_finance_token_balances.env'

ENV_VARS_PATH = args.env_vars if args.env_vars != None else _ENV_VARS_PATH
print('ENV_VARS_PATH:', ENV_VARS_PATH)
load_dotenv(dotenv_path=ENV_VARS_PATH, override=True)

if args.local:
    # Set google creds
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('LOCAL_GOOGLE_APPLICATION_CREDENTIALS')    
    args.testing = False

print(main(testing_mode=False))
print('GOOGLE_APPLICATION_CREDENTIALS: ', os.environ['GOOGLE_APPLICATION_CREDENTIALS'])