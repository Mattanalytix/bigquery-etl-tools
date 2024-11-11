"""Module for testing bigquery_etl_tools_package_tup core functions"""

import os
from datetime import datetime, timezone
import polars as pl
from google.cloud import storage


from bigquery_etl_tools_package_tup import dataframe_to_bigquery
from bigquery_etl_tools_package_tup.bigquery_utils import table_exists


BUCKET_NAME = os.environ['BUCKET']
DATASET_NAME = os.environ['DATASET']


storage_client = storage.Client()
bucket = storage_client.get_bucket(BUCKET_NAME)

test_df = pl.DataFrame(
    {
        "A": [1, 2, 3, 4, 5],
        "fruits": ["banana", "banana", "apple", "apple", "banana"],
        "B": [5, 4, 3, 2, 1],
        "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
        "test_dt": [datetime(2024, 1, 1)] * 5
    }
)


def test_dataframe_to_bigquery_csv():
    """Test the dataframe_to_bigquery function with a csv file"""
    table_name = 'dataframe_to_bigquery_test_csv'
    table_id = f'{DATASET_NAME}.{table_name}'
    file_type = 'csv'
    now_ts = int(round(datetime.now(timezone.utc).timestamp()))
    blob_name = f'bigquery_etl_tools/tests/{now_ts}_{table_name}.{file_type}'
    blob, table = dataframe_to_bigquery(
        dataframe=test_df,
        bucket_name=BUCKET_NAME,
        blob_name=blob_name,
        table_id=table_id,
        file_type=file_type
    )
    assert blob.exists(), f'Blob {blob.name} does not exist'
    assert table_exists(table), f'Table {table.table_id} does not exist'
    assert datetime.timestamp(table.modified) - now_ts > 0, 'Table not updated'


def test_dataframe_to_bigquery_json():
    """Test the dataframe_to_bigquery function with a json file"""
    table_name = 'dataframe_to_bigquery_test_json'
    table_id = f'{DATASET_NAME}.{table_name}'
    file_type = 'json'
    now_ts = int(round(datetime.now(timezone.utc).timestamp()))
    blob_name = f'bigquery_etl_tools/tests/{now_ts}_{table_name}.{file_type}'
    blob, table = dataframe_to_bigquery(
        dataframe=test_df,
        bucket_name=BUCKET_NAME,
        blob_name=blob_name,
        table_id=table_id,
        file_type=file_type
    )
    assert blob.exists(), f'Blob {blob.name} does not exist'
    assert table_exists(table), f'Table {table.table_id} does not exist'
