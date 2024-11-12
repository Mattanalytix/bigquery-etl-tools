"""Module for testing bigquery_etl_tools core functions"""

import os
from datetime import datetime, timezone
import polars as pl
from google.cloud import storage, bigquery


from bigquery_etl_tools import (
    dataframe_to_bigquery,
    autodetect_dataframe_schema
)
from bigquery_etl_tools.bigquery_utils import table_exists


BUCKET_NAME = os.environ['BUCKET']
DATASET_NAME = os.environ['DATASET']
BLOB_DIR = 'bigquery_etl_tools/ci_jobs'

bigquery_client = bigquery.Client()
storage_client = storage.Client()

bucket = storage_client.get_bucket(BUCKET_NAME)

test_df = pl.DataFrame(
    {
        "A": [1, 2, 3, 4, 5],
        "fruits": ["banana", "banana", "apple", "apple", "banana"],
        "B": [5, 4, 3, 2, 1],
        "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
        "bool_test": [True, False, True, False, True],
        "test_dt": [datetime(2024, 1, 1)] * 5
    }
)


def test_dataframe_to_bigquery_csv():
    """Test the dataframe_to_bigquery function with a csv file"""
    table_name = 'dataframe_to_bigquery_test_csv'
    table_id = f'{DATASET_NAME}.{table_name}'
    file_type = 'csv'
    now_ts = int(round(datetime.now(timezone.utc).timestamp()))
    blob_name = f'{BLOB_DIR}/{now_ts}_{table_name}.{file_type}'
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
    blob_name = f'{BLOB_DIR}/{now_ts}_{table_name}.{file_type}'
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


def test_autodetect_dataframe_schema():
    """Test the autodetect_dataframe_schema function"""
    table_name = 'dataframe_to_bigquery_test_csv'
    table_id = f'{DATASET_NAME}.{table_name}'
    file_type = 'csv'
    now_ts = int(round(datetime.now(timezone.utc).timestamp()))
    blob_name = f'{BLOB_DIR}/{now_ts}_{table_name}.{file_type}'
    filepath = autodetect_dataframe_schema(
        dataframe=test_df,
        bucket_name=BUCKET_NAME,
        blob_name=blob_name,
        table_id=table_id,
        file_type=file_type
    )
    schema = bigquery_client.schema_from_json(filepath)
    assert os.path.exists(filepath), f'File {filepath} does not exist'
    assert table_exists(table_id), f'Table {table_id} does not exist'
    assert len(schema) == test_df.shape[1], f"""Schema field length
        [{len(schema)}] does not match dataframe [{test_df.shape[1]}]"""
