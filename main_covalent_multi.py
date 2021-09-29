from utils.covalent import Query
import os
import pandas as pd
from dotenv import load_dotenv
from utils.argparser import args

def main(testing_mode=True, check_last_block=True):

    from utils.bq import BQ_table # load after env-vars due to google creds.

    COVALENT_API_KEY = os.getenv('COVALENT_API_KEY')
    CHAIN_ID = os.getenv('CHAIN_ID')
    BQ_DATASET = os.getenv('BQ_DATASET')
    BQ_TABLE = os.getenv('BQ_TABLE')

    TO_CHECK_BQ_DATASET = os.getenv('TO_CHECK_BQ_DATASET')
    TO_CHECK_BQ_TABLE = os.getenv('TO_CHECK_BQ_TABLE')
    TO_CHECK_COL_ID = os.getenv('TO_CHECK_COL_ID')
    TO_CHECK_COL_STARTING_BLOCK = os.getenv('TO_CHECK_COL_STARTING_BLOCK')

    if testing_mode: BQ_TABLE = "test_" + BQ_TABLE

    # Create session
    q = Query(
        api_key=COVALENT_API_KEY
    )

    df_base = BQ_table(TO_CHECK_BQ_DATASET, TO_CHECK_BQ_TABLE).select_all()
    table = BQ_table(BQ_DATASET, BQ_TABLE)

    if table.exists and check_last_block:
        df_table = table.select_all()

    df_events_list = []
    covalent_errs = []
    for _, row in df_base.iterrows():
        address = row[TO_CHECK_COL_ID]
        starting_block = str(row[TO_CHECK_COL_STARTING_BLOCK])

        if table.exists and check_last_block:
            # Check last block
            _starting_block = df_table[df_table[TO_CHECK_COL_ID]==address][TO_CHECK_COL_STARTING_BLOCK].max()

            if pd.notnull(_starting_block):
                starting_block = str(int(_starting_block) + 1) # Add 1 so as to not repeat last block

        ending_block = str(int(starting_block) + 10**6) # 1M is limit, pagintaion will be implemented later

        r = q.get_log_events_by_contract_address(
            chain_id=CHAIN_ID,
            address=address,
            starting_block=starting_block,
            ending_block=ending_block,
        )

        df = r.get_df()
        if r.error_message:
            covalent_errs.append(r.error_message)
        df[TO_CHECK_COL_ID] = address
        df_events_list.append(df)

    df_events = pd.concat(df_events_list)

    if df_events.empty:
        return f'No new events to add to Table: {table.table_id}.'
    else:
        if not table.exists:
            table.create_table_from_df(
                df=df_events,
                partitioned_day=True
            )

        errs = table.uplaoad_df_to_bq(df_events)
        return_value = ''
        if errs:
            return_value += f'Execution ended with {len(errs)} BQ errors. Table: {table.table_id}'
        if covalent_errs:
            return_value += f'Execution ended with {len(covalent_errs)} Covalent errors.'
            return_value += covalent_errs[0]
            return_value += covalent_errs[-1]

        if not errs:
            return_value += f'Execution succeded. Table: {table.table_id}. Shape: {df_events.shape}'

        return return_value



#_ENV_VARS_PATH = 'env_vars/polygon_client_daos_events.env'
_ENV_VARS_PATH = 'env_vars/polygon_client_voting_events.env'
    
ENV_VARS_PATH = args.env_vars if args.env_vars != None else _ENV_VARS_PATH
print('ENV_VARS_PATH:', ENV_VARS_PATH)
load_dotenv(dotenv_path=ENV_VARS_PATH, override=True)

if args.local:
    # Set creds
    os.environ['COVALENT_API_KEY'] = str(open(os.getenv('LOCAL_COVALENT_KEY_PATH')).read())
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('LOCAL_GOOGLE_APPLICATION_CREDENTIALS')    

print(main(
    testing_mode=args.testing,
    check_last_block=args.check_last_block
    ))