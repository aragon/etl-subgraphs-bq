import logging
import typing
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
from pathlib import Path
import os
import numpy as np
from .utils import correct_env_var

BQ_ETH_LOGS = "`bigquery-public-data.crypto_ethereum.logs`"

def reassing_key_env_var():
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', False)
    if GOOGLE_APPLICATION_CREDENTIALS and '~' in GOOGLE_APPLICATION_CREDENTIALS:
        # As it's relative path to home, reset env var
        GOOGLE_APPLICATION_CREDENTIALS = Path(GOOGLE_APPLICATION_CREDENTIALS).expanduser().as_posix()
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS


correct_env_var('GOOGLE_APPLICATION_CREDENTIALS')

# Construct a BigQuery client object.
client = bigquery.Client()
project = client.project

class BQ_table:
    def __init__(
        self,
        dataset_name : str,
        table_name : str):

        self.dataset_name = dataset_name
        self.table_name = table_name
        self.project = project

        self.table_id = ".".join([
            self.project, 
            self.dataset_name, 
            self.table_name])

        self._check_exists()
    
    def _check_exists(self):
        try:
            self._assign()    
        except Exception:
            self.exists = False

    
    def _assign(self):
        # Create dataset if it doesn't exist
        client.create_dataset(self.dataset_name, timeout=30, exists_ok=True)
        table_ref = client.dataset(self.dataset_name).table(self.table_name)
        self.table = client.get_table(table_ref)  # API request
        self.exists = True
        self.schema = {schema.name : schema.field_type for schema in self.table.schema}

    def _change_dtypes(self, df):
        '''
        Allign df dtypes to the schema one.
        Specially, transform bools to str when needed.
        '''
        schema_dict = self.schema
        for col in df.columns:
            dtype = SCHEMA_DTYPES.get(schema_dict.get(col))
            if dtype:
                df[col] = df[col].astype(dtype)

        return df   
    
    
    
    def uplaoad_df_to_bq(
        self,
        df : pd.DataFrame):

        # Replace pd nulls with None (for inserting in BQ)
        #df = df.where(pd.notnull(df), None) 
        df = df.replace({np.nan: None})

        df = self._change_dtypes(df)
        if df.index.is_unique == False:
            df = df.reset_index(drop=True)
        rows_list = [row_dict 
        for index, row_dict
        in df.to_dict(orient="index").items()]

        return self.insert_rows(rows_list) 

    def download_table(self, limit=100, print_query=True):
        query = f"""
                SELECT * 
                FROM `{self.table_id}` 
                """

        if limit:
            query += f"LIMIT {limit}"
        
        query_job = client.query(query) # Make an API request.
        if print_query: print(query)

        return query_job.result().to_dataframe()

    
    def insert_rows(
        self, 
        rows_list : typing.List[dict]
        ) -> list:
        
        self._assign()

        rows_n = len(rows_list)
        response_list = client.insert_rows(self.table, rows_list)
        errors_n = len(response_list)
        if errors_n > 0:
            logging.error(f"""
            Big Query insert errors - 
            From {rows_n} to be inserted, {errors_n} failed.""")
            logging.error(f"""First error example - 
            {response_list[0]}""")

            if errors_n > 1:
                logging.error(f"""Last error example - 
                {response_list[-1]}""")
        else:
            logging.info(f"""Inserted {rows_n} rows to {self.project}.{self.dataset_name}.{self.table_name}""")
        
        return response_list

    def _create_schema_from_df(self, df):
        self.schema = create_bq_schema(df)
        self.schema_list = []
        for key, d_type in self.schema.items():
                self.schema_list.append(
                    bigquery.SchemaField(
                        key,
                        d_type,
                        mode="NULLABLE"))
    
    def create_table_from_df(
        self, 
        df, 
        partitioned_day=False):

        df = self._avoid_duplicates(df)
        self._create_schema_from_df(df)
        table = bigquery.Table(
            self.table_id, 
            schema=self.schema_list)
        if partitioned_day:
            # https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.TimePartitioning.html
            # https://cloud.google.com/bigquery/docs/creating-partitioned-tables#python
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=None,  # If not set, the table is partitioned by pseudo column `_PARTITIONTIME`
                expiration_ms=None,
            )

            
        table = client.create_table(table) # API request


    def _avoid_duplicates(self, df):
        # Workaround to avoid issues while uploading schema to BigQuery
        cols_new = []
        _tracker_dict = {}
        for col in df.columns:
            _col_lower = col.lower()
            if _col_lower in _tracker_dict: # If will have issues
                col = col + '_' + str(_tracker)
            cols_new.append(col)
            _tracker = _tracker_dict.get(_col_lower, 0) + 1
            _tracker_dict[_col_lower] = _tracker
        df.columns = cols_new
        return df

    
    def delete_table(self):
        client.delete_table(
            self.table_id, 
            not_found_ok=True)
        self._check_exists()
        
        return self.exists

    def select_all(self):
        query = f"""
            SELECT * 
            FROM {self.table_id} 
            """
        return self._query_job(query)

    def get_last_block_by_address(self, block_col, col_id, address):
        query = f"""         
            SELECT max({block_col}) max_block_height
            FROM {self.table_id} 
            WHERE {col_id} = '{address}'
            """
        df_response = self._query_job(query)
        return self._get_last_block(df_response)

    def get_last_row_by_block(self, block_col):
        query = f"""         
            SELECT *
            FROM {self.table_id} 
            ORDER BY {block_col} DESC
            LIMIT 1
            """
        df_response = self._query_job(query)
        return df_response
    
    def get_last_block(self, block_col):
        query = f"""         
            SELECT max(CAST({block_col} as INT64)) max_block_height
            FROM {self.table_id} 
            """
        df_response = self._query_job(query)
        return self._get_last_block(df_response)

    def select_all_gt_block(self, block_col, min_block):
        query = f"""         
            SELECT *
            FROM {self.table_id} 
            WHERE CAST ({block_col} AS NUMERIC) > {min_block}
            ORDER BY {block_col} ASC
            """
        df_response = self._query_job(query)
        return df_response

    def _get_last_block(self, df):
        _last_block = df.iloc[0][0]
        last_block = str(int(_last_block)) if not pd.isnull(_last_block) else None
        return last_block


    def _query_job(self, query):
        query_job = client.query(query)  # Make an API request.
        return query_job.result().to_dataframe()   



########## Utils
SCHEMA_DTYPES = {
    'STRING' : str,
    #'FLOAT' : float,
    #'INTEGER' : int,
}


def create_bq_schema(df : pd.DataFrame) -> dict:
    '''
    Take df and return a dict with form:
        {col_name : bq_dtpye}
        Being bq_dtpyes: [STRING, FLOAT, BOOL, INTEGER]
    '''
    schema_dict = {}
    for col in df.columns:
        dtype = 'STRING' # base case
        mode = 'NULLABLE'
        s = df[col]
        if pd.api.types.is_float_dtype(s):
            dtype = 'FLOAT'
        elif pd.api.types.is_bool_dtype(s):
            dtype = 'BOOL'
        elif pd.api.types.is_integer_dtype(s):
            dtype = 'INTEGER'
        schema_dict[col] = dtype

    return schema_dict



def create_bq_view(
        dataset_name:str, 
        view_name:str, 
        view_statement:str):

        shared_dataset_ref = client.dataset(dataset_name)
        view_ref = shared_dataset_ref.table(view_name)
        view = bigquery.Table(view_ref)
        view.view_query = view_statement
        view = client.create_table(view)  # API request
        print("Successfully created view at {}".format(view.full_table_id))
        


def get_logs(
    address:list, 
    func_sign:list, 
    limit=100, 
    print_query=True):
        
    query = f"""
            SELECT * 
            FROM {BQ_ETH_LOGS}, 
            UNNEST(topics) t 
            WHERE 
            t in (  "{','.join(func_sign)}"   )
            AND address in ( "{','.join(address)}" ) 
            AND DATE(block_timestamp) >= "2020-01-01"
            """

    if limit:
        query += f"LIMIT {limit}"
    
    query_job = client.query(query)  # Make an API request.
    if print_query: print(query)
    return query_job.result().to_dataframe()

def insert_rows(
    dataset_name : str,
    table_name : str, 
    rows_list : typing.List[dict]) -> list:

    rows_n = len(rows_list)
    table_ref = client.dataset(dataset_name).table(table_name)
    table = client.get_table(table_ref)
    response_list = client.insert_rows(table, rows_list)
    errors_n = len(response_list)
    if errors_n > 0:
        logging.error(f"""
        Big Query insert errors - 
        From {rows_n} to be inserted, {errors_n} failed.""")
        logging.error(f"""First error example - 
        {response_list[0]}""")

        if errors_n > 1:
            logging.error(f"""Last error example - 
            {response_list[-1]}""")
    
    return response_list

