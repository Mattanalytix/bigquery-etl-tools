"""Module providing helper functions for working with google cloud storage"""

import logging
import polars as pl
from google.cloud import storage
from google.cloud.storage import Blob
from google.cloud.exceptions import Forbidden

from .config import FILE_TYPE_CONFIG


# google.cloud.storage wrappers
def get_bucket_wrapper(
        storage_client: storage.Client,
        bucket_name: str
        ) -> storage.Bucket:
    """
    Wrapper around the google.cloud.storage.Client.get_bucket method
    @param storage_client The google.cloud.storage.Client object
    @param bucket_name The name of the bucket to retrieve
    @return The bucket object
    """
    try:
        return storage_client.get_bucket(bucket_name)
    except Forbidden as e:
        logging.error("""Bucket does not exist %s:%s or user does not
                      have access""", storage_client.project, bucket_name)
        raise e


def get_bucket_blob_wrapper(
        storage_client: storage.Client,
        bucket_name: str,
        blob_name: str
        ) -> tuple[storage.Bucket, Blob]:
    """
    Wrapper function to retrieve a bucket and blob object
    @param storage_client The google.cloud.storage.Client object
    @param bucket_name The name of the bucket to retrieve
    @param blob_name The name of the blob to retrieve
        (e.g. full/path/to/blob.csv)
    @return A tuple of the bucket and blob objects
    """
    logging.info('Retrieving blob %s:%s',
                 storage_client.project, blob_name)
    bucket = get_bucket_wrapper(storage_client, bucket_name)
    blob = Blob(blob_name, bucket)
    blob_exists = 'FOUND' if blob.exists() else 'NOT FOUND'
    logging.info('Blob %s in bucket %s:%s', blob_exists,
                 storage_client.project, blob.name)

    return bucket, blob


# CORE FUNCTIONS
def dataframe_to_storage(
        storage_client: storage.Client,
        dataframe: pl.DataFrame,
        bucket_name: str,
        blob_name: str,
        file_type: str
        ) -> storage.Blob:
    """
    Upload a dataframe to google cloud storage
    @param storage_client The google.cloud.storage.Client object
    @param dataframe The dataframe to upload
    @param bucket_name The name of the bucket to upload to
    @param blob_name The name of the blob to upload to
    @param file_type The type of file to upload (csv, json)
    """
    _, blob = get_bucket_blob_wrapper(storage_client, bucket_name, blob_name)

    config = FILE_TYPE_CONFIG[file_type]
    pl.DataFrame.my_write_function = config['dataframe_write_function'][0]

    logging.info('Uploading (%s, %s) dataframe to blob %s:%s',
                 dataframe.shape[0], dataframe.shape[1],
                 storage_client.project, blob.name)

    blob.upload_from_string(
        data=dataframe.my_write_function(
            **config['dataframe_write_function'][1]),
        content_type=config['blob_type']
    )

    return blob
