from utils.covalent import Query
import os
from dotenv import load_dotenv
from utils.argparser import args


def main(testing_mode=True):
    print(f"Loading env_vars from {ENV_VARS_PATH}")
    load_dotenv(dotenv_path=ENV_VARS_PATH, override=True)

    from utils.bq import BQ_table # load after env-vars due to google creds.

    COVALENT_KEY = os.getenv('COVALENT_KEY')
    CHAIN_ID = os.getenv('CHAIN_ID')
    BQ_DATASET = os.getenv('BQ_DATASET')
    BQ_TABLE = os.getenv('BQ_TABLE')
    CONTRACT_ADDRESS = os.getenv('CONTRACT_ADDRESS')
    STARTING_BLOCK = os.getenv('STARTING_BLOCK')
    ending_block = str(int(STARTING_BLOCK) + 10**6) # 1M is limit, pagintaion will be implemented later

    if testing_mode: BQ_TABLE = "test_" + BQ_TABLE

    # Create session
    q = Query(
        api_key=COVALENT_KEY
    )

    r = q.get_log_events_by_contract_address(
        chain_id=CHAIN_ID,
        address=CONTRACT_ADDRESS,
        starting_block=STARTING_BLOCK,
        ending_block=ending_block,
    )


    df = r.get_df()

    table = BQ_table(BQ_DATASET, BQ_TABLE)

    if not table.exists:
        table.create_table_from_df(
            df=df,
            partitioned_day=True
        )

    errs = table.uplaoad_df_to_bq(df)
    if errs:
        print(f'Execution ended with {len(errs)} errors. Check Logging. Table: {table.table_id}')

    print(f'Execution succeded. Table: {table.table_id}. Shape: {df.shape}')
    print()

_ENV_VARS_PATH = './env_vars/mumbai_client_daos.env'
#_ENV_VARS_PATH = './env_vars/polygon_client_daos.env'
ENV_VARS_PATH = args.env_vars if args.env_vars != None else _ENV_VARS_PATH
print('ENV_VARS_PATH:', ENV_VARS_PATH)
load_dotenv(dotenv_path=ENV_VARS_PATH, override=True)

if args.local:
    # Set creds
    os.environ['COVALENT_KEY'] = str(open(os.getenv('LOCAL_COVALENT_KEY_PATH')).read())

print(main(testing_mode=args.testing))