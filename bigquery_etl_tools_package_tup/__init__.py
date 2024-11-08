"""Core module for bigquery_etl_tools_package_tup"""

import logging
import polars as pl
from google.cloud import bigquery, storage

from .config import FILE_TYPE_CONFIG
from .storage_utils import dataframe_to_storage
from .bigquery_utils import storage_to_bigquery


def dataframe_to_bigquery(
        dataframe: pl.DataFrame,
        bucket_name: str,
        blob_name: str,
        dataset_name: str,
        table_name: str,
        file_type: str,
        job_config: bigquery.LoadJobConfig = bigquery.LoadJobConfig(
            write_disposition='WRITE_TRUNCATE',
            autodetect=True
        )
        ) -> tuple[storage.Blob, bigquery.TableReference]:
    """
    Load a dataframe into a bigquery table, via cloud storage
    @param dataframe The dataframe to load
    @param bucket_name The name of the bucket to load from
    @param blob_name The name of the blob to load from
    @param dataset_name The name of the dataset to load into
    @param table_name The name of the table to load into
    @param file_type The type of file to load (csv, json)
    @param job_config The job config
    @return A tuple of the blob and load job
    """

    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    blob = dataframe_to_storage(
        storage_client,
        dataframe,
        bucket_name,
        blob_name,
        file_type
    )

    for k, v in FILE_TYPE_CONFIG.items():
        if v['blob_type'] == blob.content_type:
            logging.info("""Setting file type to %s as detected from
                         the blob %s""", k, blob.name)
            job_config.source_format = v['bigquery_format']
            break

    table = storage_to_bigquery(
        blob,
        bigquery_client,
        dataset_name,
        table_name,
        job_config
    )

    return blob, table
