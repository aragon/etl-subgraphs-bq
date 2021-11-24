from utils.covalent import Query
import os
from dotenv import load_dotenv
from utils.argparser import args
from utils.utils import correct_env_var
import pandas as pd
import yaml

def main(testing_mode=True, check_last_block=False):

    from utils.bq import BQ_table # load after env-vars due to google creds.

    COVALENT_API_KEY = os.getenv('COVALENT_API_KEY')
    CHAIN_ID = os.getenv('CHAIN_ID')
    BQ_DATASET = os.getenv('BQ_DATASET')
    BQ_TABLE = os.getenv('BQ_TABLE')
    CONTRACT_ADDRESS = os.getenv('CONTRACT_ADDRESS')
    STARTING_BLOCK = os.getenv('STARTING_BLOCK', False)
    TO_CHECK_BQ_DATASET = os.getenv('TO_CHECK_BQ_DATASET')
    TO_CHECK_BQ_TABLE = os.getenv('TO_CHECK_BQ_TABLE')
    TO_CHECK_COL_STARTING_BLOCK = os.getenv('TO_CHECK_COL_STARTING_BLOCK')
    FUNCTION_NAME_TO_QUERY = os.getenv('FUNCTION_NAME_TO_QUERY')
    

    with open('topics.yaml') as f:
        topics_dict = yaml.safe_load(f)
    
    function_hash = topics_dict.get(FUNCTION_NAME_TO_QUERY, None)

    assert STARTING_BLOCK != None or TO_CHECK_COL_STARTING_BLOCK != None, (
        'You should define either STARTING_BLOCK or'
        ' TO_CHECK_COL_STARTING_BLOCK in env vars')

    assert function_hash != None

    if STARTING_BLOCK:
        starting_block = STARTING_BLOCK
    else:
        # Assign by default
        starting_block = 0
   
    if TO_CHECK_COL_STARTING_BLOCK and check_last_block:
        table_base = BQ_table(TO_CHECK_BQ_DATASET, TO_CHECK_BQ_TABLE)
        if table_base.exists:
            last_block_row = table_base.get_last_row_by_block(TO_CHECK_COL_STARTING_BLOCK)
            last_block = last_block_row[TO_CHECK_COL_STARTING_BLOCK][0]
            if pd.notnull(last_block):
                # Override if provided starting_block
                starting_block = str(int(last_block) + 1) # Add 1 so as to not repeat last block

    
    if testing_mode: BQ_TABLE = "test_" + BQ_TABLE

    # Create session
    q = Query(
        api_key=COVALENT_API_KEY
    )

    blockchain_last_block = q.get_last_block(chain_id=CHAIN_ID).data['items'][0]['height']
    ending_block = str(min(blockchain_last_block, int(starting_block) + 10**6)) # 1M is API limit
    
    r = q.get_log_events_by_topic_hash(
        chain_id=CHAIN_ID,
        topic=function_hash,
        starting_block=starting_block,
        ending_block=ending_block,
    )
    print(f"- starting_block: {starting_block} | ending_block: {ending_block}")


    df = r.get_df()


    table = BQ_table(BQ_DATASET, BQ_TABLE)
    if df.empty:
        if check_last_block:
            row_df_to_add = pd.DataFrame([{col: None for col in last_block_row}])
            row_df_to_add[TO_CHECK_COL_STARTING_BLOCK] = ending_block
            errs = table.uplaoad_df_to_bq(df)
            return f'Inserted empty row with ending_block: {ending_block}.'
        return f'No new rows to add to Table: {table.table_id}.'

    if not table.exists:
        table.create_table_from_df(
            df=df,
            partitioned_day=True
        )

    errs = table.uplaoad_df_to_bq(df)
    if errs:
        return f'Execution ended with {len(errs)} errors. Check Logging. Table: {table.table_id}'

    return f'Execution succeded. Table: {table.table_id}. Shape: {df.shape}'
    

#_ENV_VARS_PATH = './env_vars/mumbai_client_daos.env'
_ENV_VARS_PATH = './env_vars/polygon_client_daos.env'
#_ENV_VARS_PATH = './env_vars/bsc_testnet_client_daos.env'
#_ENV_VARS_PATH = './env_vars/fuji_client_daos.env'
ENV_VARS_PATH = args.env_vars if args.env_vars != None else _ENV_VARS_PATH
print('ENV_VARS_PATH:', ENV_VARS_PATH)
load_dotenv(dotenv_path=ENV_VARS_PATH, override=True)

if args.local:
    # Set creds
    os.environ['COVALENT_API_KEY'] = str(open(os.getenv('LOCAL_COVALENT_KEY_PATH')).read())
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('LOCAL_GOOGLE_APPLICATION_CREDENTIALS')
    args.check_last_block = True
    args.testing = False
    

print(main(
    testing_mode=args.testing,
    check_last_block=args.check_last_block
    ))