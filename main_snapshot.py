import pandas as pd
from pathlib import Path
import os 
from dotenv import load_dotenv
from utils.the_graph import GraphQuery
from utils.argparser import args


def main(testing_mode=True):
    from utils.bq import BQ_table # Import GCP after setting env_vars (credentials)

    SPACES_QUERY_PATH = Path(os.getenv('SPACES_QUERY_PATH'))
    API_URL = os.getenv('API_URL')
    BQ_DATASET = os.getenv('BQ_DATASET')
    SPACES_BQ_TABLE = os.getenv('SPACES_BQ_TABLE')
    #DATE_RANGE_ATTR = os.getenv('DATE_RANGE_ATTR')
    #DATE_RANGE_COL = os.getenv('DATE_RANGE_COL')
    
    if testing_mode: SPACES_BQ_TABLE = "test_" + SPACES_BQ_TABLE

    spaces_table = BQ_table(BQ_DATASET, SPACES_BQ_TABLE)

    last_update:str = '0'
    df_spaces_base = pd.DataFrame()
    if spaces_table.exists:
            df_spaces_base = spaces_table.select_all()
            ids = set(df_spaces_base['id'])
    
    query = GraphQuery(
        query_path=SPACES_QUERY_PATH,
        api_url=API_URL,
        query_first='64',
        query_skip='0'
        )

    print("Spaces: quering started.")
    data = query.post(
        paginate=True)
    
    df_spaces_new = pd.DataFrame(data)
    
    if not df_spaces_base.empty:
        df_spaces_new = df_spaces_new[~df_spaces_new['id'].isin(ids)]

    print(f"Spaces: new fields to add {df_spaces_new.shape[0]}")

    if not spaces_table.exists:
        spaces_table.create_table_from_df(
            df=df_spaces_new,
            partitioned_day=True)

    errs = spaces_table.uplaoad_df_to_bq(df_spaces_new)
    if errs:
        print(f'Spaces: Execution ended with {len(errs)} errors. Check Logging.')
    
    print(f"""Space: Table: {spaces_table.table_id}. Shape: {df_spaces_new.shape}""")


_ENV_VARS_PATH = './env_vars/snapshot.env'

ENV_VARS_PATH = args.env_vars if args.env_vars != None else _ENV_VARS_PATH
print('ENV_VARS_PATH:', ENV_VARS_PATH)
load_dotenv(dotenv_path=ENV_VARS_PATH, override=True)

if args.local:
    # Set google creds
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('LOCAL_GOOGLE_APPLICATION_CREDENTIALS')    
    args.testing = False

print(main(testing_mode=args.testing))
print('GOOGLE_APPLICATION_CREDENTIALS: ', os.environ['GOOGLE_APPLICATION_CREDENTIALS'])