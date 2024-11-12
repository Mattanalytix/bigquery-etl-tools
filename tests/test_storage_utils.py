"""Module for testing storage_utils.py"""

import os
import logging
from datetime import datetime
import polars as pl
from google.cloud import storage

from bigquery_etl_tools.config import FILE_TYPE_CONFIG


BUCKET_NAME = os.environ['BUCKET']

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


def test_validate_file_type_config():
    """Validate the FILE_TYPE_CONFIG has been configured correctly for each
       file type"""
    for k, file_type in FILE_TYPE_CONFIG.items():
        logging.info('Validating configuration values for %s', k)
        assert 'dataframe_write_function' in file_type
        assert len(file_type['dataframe_write_function']) == 2
        assert 'blob_type' in file_type
