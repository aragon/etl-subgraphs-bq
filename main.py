import argparse
import pandas as pd
from pathlib import Path
import os 
from utils._local_utils import load_env_vars
from utils.the_graph import GraphQuery

## Will only be used in local execution
ENV_VARS_PATH = 'env_vars/court_guardians.yaml'
#ENV_VARS_PATH = 'env_vars/govern_daos.yaml'
#ENV_VARS_PATH = 'env_vars/govern_executions.yaml'

def main(env_vars_path=ENV_VARS_PATH, testing_mode=True):
    print(f'Setting env_vars: {env_vars_path}')
    print(f'TESTING_MODE: {testing_mode}')
    load_env_vars(env_vars_path) 
    # Import GCP after setting env_vars (credentials)
    from utils.bq import BQ_table

    PROCESS_NAME = os.getenv('PROCESS_NAME')
    QUERY_PATH = Path(os.getenv('QUERY_DIR')).joinpath(PROCESS_NAME+'.graphql')
    API_URL = os.getenv('API_URL')
    BQ_DATASET = os.getenv('BQ_DATASET')
    BQ_TABLE = PROCESS_NAME
    
    if testing_mode: BQ_TABLE = "test_" + BQ_TABLE

    query = GraphQuery(
        query_path=QUERY_PATH,
        api_url=API_URL,
        query_first='1000'
    )

    data = query.post(paginate=True)

    df = pd.DataFrame(data)

    table = BQ_table(BQ_DATASET, BQ_TABLE)

    if not table.exists:
        table.create_table_from_df(
            df=df,
            partitioned_day=True)

    errs = table.uplaoad_df_to_bq(df)
    if errs:
        return f'Execution ended with {len(errs)} errors. Check Logging.'
    
    
    return f'Execution succeded. Table: {table.table_id}. Shape: {df.shape}'


print(main(testing_mode=True))