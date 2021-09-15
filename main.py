import argparse
import pandas as pd
from pathlib import Path
import os 
from dotenv import load_dotenv
from utils.the_graph import GraphQuery


# Create the parser
parser = argparse.ArgumentParser()
# Add argument to differenciate between local executions
parser.add_argument('--local', dest='local', action='store_true')
parser.add_argument('--no-local', dest='local', action='store_false')
parser.set_defaults(local=True)

# Add argument to point to testing tables
parser.add_argument('--testing', dest='testing', action='store_true')
parser.add_argument('--no-testing', dest='testing', action='store_false')
parser.set_defaults(testing=True)

# Add argument to define env_vars to use
parser.add_argument('--env_vars', type=str, required=False, default=None) # relative path
args = parser.parse_args()
print("args: ", args)

def main(testing_mode=True):
    from utils.bq import BQ_table # Import GCP after setting env_vars (credentials)

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

if args.local:
    # Set local env-vars
    _ENV_VARS_PATH = './env_vars/court_guardians.env'
    ENV_VARS_PATH = args.env_vars if args.env_vars != None else _ENV_VARS_PATH
    print('ENV_VARS_PATH:', ENV_VARS_PATH)
    load_dotenv(dotenv_path=ENV_VARS_PATH, override=True)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('LOCAL_GOOGLE_APPLICATION_CREDENTIALS')    


print(main(testing_mode=args.testing))
print('GOOGLE_APPLICATION_CREDENTIALS: ', os.environ['GOOGLE_APPLICATION_CREDENTIALS'])