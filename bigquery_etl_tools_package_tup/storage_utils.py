"""Module providing helper functions for working with google cloud storage"""

import logging
from google.cloud import storage
from google.cloud.storage import Blob
import polars as pl


FILE_TYPE_CONFIG = {
    'csv': {
        'dataframe_write_function': [pl.DataFrame.write_csv, {
            'datetime_format': '%Y-%m-%d %H:%M:%S', 'date_format': '%Y-%m-%d'
        }],
        'blob_type': 'text/csv'  
    },
    'json': {
        'dataframe_write_function': [pl.DataFrame.write_ndjson, {}],
        'blob_type': 'text/json'
    }
}


def dataframe_to_storage(
        storage_client: storage.Client,
        dataframe: pl.DataFrame,
        bucket_name: str,
        blob_name: str,
        file_type: str
    ) -> None:
    """
    Upload a dataframe to google cloud storage
    """
    bucket = storage_client.get_bucket(bucket_name)
    blob = Blob(blob_name, bucket)

    config = FILE_TYPE_CONFIG[file_type]
    pl.DataFrame.my_write_function = config['dataframe_write_function'][0]

    blob.upload_from_string(
        data = dataframe.my_write_function(**config['dataframe_write_function'][1]),
        content_type = config['blob_type']
    )
