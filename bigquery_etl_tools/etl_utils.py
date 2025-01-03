"""Core module for bigquery_etl_tools_package_tup"""

import os
import logging
import polars as pl
from google.cloud import bigquery, storage

from .config import get_source_format_from_blob
from .storage_utils import dataframe_to_storage
from .bigquery_utils import storage_to_bigquery


def _inner_dataframe_to_bigquery(
        storage_client: storage.Client,
        bigquery_client: bigquery.Client,
        dataframe: pl.DataFrame,
        bucket_name: str,
        blob_name: str,
        table_id: str,
        file_type: str = 'csv',
        job_config: bigquery.LoadJobConfig = bigquery.LoadJobConfig(
            write_disposition='WRITE_TRUNCATE',
            autodetect=True
        )
        ) -> tuple[storage.Blob, bigquery.Table]:
    """
    Inner method to load a dataframe into a bigquery table, via cloud storage
    @param storage_client The google.cloud.storage.Client object
    @param bigquery_client The google.cloud.bigquery.Client object
    @param dataframe The dataframe to load
    @param bucket_name The name of the bucket to load from
    @param blob_name The name of the blob to load from
        (e.g. full/path/to/blob.csv)
    @param table_id The id of the table format dataset.table
    @param file_type The type of file to load (csv, json)
    @param job_config The job config
    @return A tuple of the blob and load job
    """

    blob = dataframe_to_storage(
        storage_client,
        dataframe,
        bucket_name,
        blob_name,
        file_type
    )

    job_config.source_format = get_source_format_from_blob(blob)

    table = storage_to_bigquery(
        blob,
        bigquery_client,
        table_id,
        job_config
    )

    return blob, table


def dataframe_to_bigquery(
        dataframe: pl.DataFrame,
        bucket_name: str,
        blob_name: str,
        table_id: str,
        file_type: str = 'csv',
        job_config: bigquery.LoadJobConfig = bigquery.LoadJobConfig(
            write_disposition='WRITE_TRUNCATE',
            autodetect=True
        ),
        storage_client: storage.Client = storage.Client(),
        bigquery_client: bigquery.Client = bigquery.Client()  
        ) -> tuple[storage.Blob, bigquery.Table]:
    """
    Load a dataframe into a bigquery table, via cloud storage
    @param dataframe The dataframe to load
    @param bucket_name The name of the bucket to load from
    @param blob_name The name of the blob to load from
        (e.g. full/path/to/blob.csv)
    @param table_id The id of the table format dataset.table
    @param file_type The type of file to load (csv, json)
    @param job_config The job config
    @param storage_client a google.cloud.storage.Client object
    @param bigquery_client a google.cloud.bigquery.Client object
    @return A tuple of the blob and load job
    """
    blob, table = _inner_dataframe_to_bigquery(
        storage_client,
        bigquery_client,
        dataframe,
        bucket_name,
        blob_name,
        table_id,
        file_type,
        job_config
    )

    return blob, table


def autodetect_dataframe_schema(
        dataframe: pl.DataFrame,
        bucket_name: str,
        blob_name: str,
        table_id: str,
        file_type: str = 'csv',
        destination_dir: str = 'data/compiled',
        storage_client: storage.Client = storage.Client(),
        bigquery_client: bigquery.Client = bigquery.Client()        
    ) -> str:
    """
    Autodetect the bigquery schema of a dataframe and write it to a local file
    @param dataframe The dataframe to autodetect
    @param bucket_name The name of the bucket to load from
    @param blob_name The name of the blob to load from
        (e.g. full/path/to/blob.csv)
    @param table_id The id of the table format dataset.table
    @param file_type The type of file to load (csv, json)
    @param destination_dir The directory to write the schema to
    @param storage_client a google.cloud.storage.Client object
    @param bigquery_client a google.cloud.bigquery.Client object
    @return The path to the local file containing the schema
    """
    logging.info('Sampling 100 rows from the dataframe')
    dataframe_sample = dataframe.head(100)

    _, table = _inner_dataframe_to_bigquery(
        storage_client,
        bigquery_client,
        dataframe_sample,
        bucket_name,
        blob_name,
        table_id,
        file_type
    )

    os.makedirs(destination_dir, exist_ok=True)
    file_path = f'{destination_dir}/AUTODETECT_{table.table_id}.json'
    logging.info('Writing schema to local file: %s', file_path)
    bigquery_client.schema_to_json(table.schema, file_path)

    return file_path
