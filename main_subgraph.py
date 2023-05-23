import pandas as pd
from pathlib import Path
import os 
from dotenv import load_dotenv
from utils.the_graph import GraphQuery
from utils.argparser import args


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

    last_update:str = '0'
    if table.exists and DATE_RANGE_COL:
            last_update = table.get_last_block(DATE_RANGE_COL)
            last_update = str(last_update) if pd.notnull(last_update) else '0'
            print("last update: ", last_update)
    
    query = GraphQuery(
        query_path=QUERY_PATH,
        api_url=API_URL,
        query_first='1000',
        gt_statement=DATE_RANGE_ATTR,
        gt_value=str(int(last_update)+1)
    )

    data = query.post(
        paginate=True,
        date_filter=True)

    df = pd.DataFrame(data)

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

#_ENV_VARS_PATH = './env_vars/court_guardians.env'
#_ENV_VARS_PATH = './env_vars/govern_daos.env'
#_ENV_VARS_PATH = './env_vars/govern_executions.env'
#_ENV_VARS_PATH = './env_vars/court_disputes.env'
#_ENV_VARS_PATH = './env_vars/mainnet_client_daos.env'
#_ENV_VARS_PATH = './env_vars/mainnet_client_voting.env'
#_ENV_VARS_PATH = './env_vars/harmony_client_daos.env'
#_ENV_VARS_PATH = './env_vars/harmony_client_voting.env'
#_ENV_VARS_PATH = './env_vars/mainnet_client_casts.env'
#_ENV_VARS_PATH = './env_vars/mainnet_client_daos_names.env'
#_ENV_VARS_PATH = './env_vars/polygon_client_daos_names.env'
#_ENV_VARS_PATH = './env_vars/mainnet_client_finance_transactions.env'
#_ENV_VARS_PATH = './env_vars/mainnet_client_voting.env'
#_ENV_VARS_PATH = './env_vars/ens_domains.env'
#_ENV_VARS_PATH = './env_vars/mainnet_client_dandelion_voting.env'
#_ENV_VARS_PATH = './env_vars/mainnet_client_casts.env'
_ENV_VARS_PATH = './env_vars/aragon_app_mainnet_tokens.env'

ENV_VARS_PATH = args.env_vars if args.env_vars != None else _ENV_VARS_PATH
print('ENV_VARS_PATH:', ENV_VARS_PATH)
load_dotenv(dotenv_path=ENV_VARS_PATH, override=True)

if args.local:
    # Set google creds
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('LOCAL_GOOGLE_APPLICATION_CREDENTIALS')    
    args.testing = False

print(main(testing_mode=False))
print('GOOGLE_APPLICATION_CREDENTIALS: ', os.environ['GOOGLE_APPLICATION_CREDENTIALS'])