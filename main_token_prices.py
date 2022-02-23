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

    SPACES_QUERY_PATH = Path(os.getenv('SPACES_QUERY_PATH'))
    PROPOSALS_QUERY_PATH = Path(os.getenv('PROPOSALS_QUERY_PATH'))
    VOTES_QUERY_PATH = Path(os.getenv('VOTES_QUERY_PATH'))
    SPACES_BQ_TABLE = os.getenv('SPACES_BQ_TABLE')
    PROPOSALS_BQ_TABLE = os.getenv('PROPOSALS_BQ_TABLE')
    PROPOSALS_DATE_RANGE_ATTR = os.getenv('PROPOSALS_DATE_RANGE_ATTR')
    PROPOSALS_DATE_RANGE_COL = os.getenv('PROPOSALS_DATE_RANGE_COL')
    VOTES_BQ_TABLE = os.getenv('VOTES_BQ_TABLE')
    VOTES_DATE_RANGE_ATTR = os.getenv('VOTES_DATE_RANGE_ATTR')
    VOTES_DATE_RANGE_COL = os.getenv('VOTES_DATE_RANGE_COL')
    
    if testing_mode: SPACES_BQ_TABLE = "test_" + SPACES_BQ_TABLE

    # Load Spaces
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

    if not df_spaces_new.empty:
        errs = spaces_table.uplaoad_df_to_bq(df_spaces_new)
        if errs:
            print(f'Spaces: Execution ended with {len(errs)} errors. Check Logging.')
        print(f"""Space: Table: {spaces_table.table_id}. Shape: {df_spaces_new.shape}""")


    # Load Proposals
    print("Proposals: quering started.")
    proposals_table = BQ_table(BQ_DATASET, PROPOSALS_BQ_TABLE)

    last_update = 0
    if proposals_table.exists:
            last_update_tmp = proposals_table.get_last_block(PROPOSALS_DATE_RANGE_COL)
            last_update = last_update_tmp if last_update_tmp != None else last_update
            print(f"Proposals: last update:{last_update}")

    query = GraphQuery(
        query_path=PROPOSALS_QUERY_PATH,
        api_url=API_URL,
        query_first='1000',
        query_skip='0',
        gt_statement=PROPOSALS_DATE_RANGE_ATTR,
        gt_value=int(last_update)+1
        )

    data = query.post(
        paginate=True,
        date_filter=True)

    df_proposals_new = pd.DataFrame(data)
    lambda_replace = lambda x: x.replace('\n',"").replace('**',"")

    if not df_proposals_new.empty:
        df_proposals_new['body'] = (df_proposals_new['body']
                                    .apply(lambda_replace))

    print(f"Proposals: new fields to add {df_proposals_new.shape[0]}")

    if not proposals_table.exists:
        proposals_table.create_table_from_df(
            df=df_proposals_new,
            partitioned_day=True)

    if not df_proposals_new.empty:
        errs = proposals_table.uplaoad_df_to_bq(df_proposals_new)
        if errs:
            print(f'Proposals: Execution ended with {len(errs)} errors. Check Logging.')
        print(f"""Proposals: Table: {proposals_table.table_id}. Shape: {df_proposals_new.shape}""")

    # Load Votes
    print("Votes: quering started.")
    votes_table = BQ_table(BQ_DATASET, VOTES_BQ_TABLE)

    last_update = 0
    if votes_table.exists:
            last_update_tmp = votes_table.get_last_block(VOTES_DATE_RANGE_COL)
            last_update = last_update_tmp if last_update_tmp != None else last_update
            print(f"Votes: last update at database:{last_update}")

    query = GraphQuery(
        query_path=VOTES_QUERY_PATH,
        api_url=API_URL,
        query_first='1000',
        query_skip='0',
        gt_statement=VOTES_DATE_RANGE_ATTR,
        gt_value=int(last_update)+1
        )

    data = query.post(
        paginate=True,
        date_filter=True)

    df_votes_new = pd.DataFrame(data)
    lambda_replace = lambda x: x.replace('\n',"").replace('**',"")

    print(f"Votes: new fields to add {df_votes_new.shape[0]}")
    lastt = int(df_votes_new[VOTES_DATE_RANGE_COL].max())
    print(f'Votes: Last update to upload: {lastt}')

    if not votes_table.exists:
        votes_table.create_table_from_df(
            df=df_votes_new,
            partitioned_day=True)

    if not df_votes_new.empty:
        errs = votes_table.uplaoad_df_to_bq(df_votes_new)
        if errs:
            print(f'Votes: Execution ended with {len(errs)} errors. Check Logging.')
        print(f"""Votes: Table: {votes_table.table_id}. Shape: {df_votes_new.shape}""")

    return lastt


# Set creds
os.environ['MORALIS_APY_KEY'] = str(open(os.getenv('LOCAL_MORALIS_API_KEY')).read())
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('LOCAL_GOOGLE_APPLICATION_CREDENTIALS')

