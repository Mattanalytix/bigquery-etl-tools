import polars as pl
from google.cloud import bigquery, storage

from .config import get_source_format_from_blob
from .storage_utils import dataframe_to_storage
from .bigquery_utils import storage_to_bigquery
from .etl_utils import (
    dataframe_to_bigquery,
    autodetect_dataframe_schema
)


class BigqueryEtlClient:
    """
    A client for interacting with the bigquery etl tools package
    @param project_name The name of the project to use
    @param bucket_name The name of the bucket to use
    """
    def __init__(
            self,
            bucket_name: str,
            project_name: str = None
            ):
        if project_name is None:
            self.storage_client = storage.Client()
            self.bigquery_client = bigquery.Client()
        else:
            self.storage_client = storage.Client(project_name)
            self.bigquery_client = bigquery.Client(project_name)
        self.bucket_name = bucket_name
        self.project_name = self.storage_client.project
    
    def dataframe_to_storage(
        self,
        dataframe: pl.DataFrame,
        blob_name: str,
        file_type: str
        ) -> storage.Blob:
        """
        Upload a dataframe to google cloud storage
        @param dataframe The dataframe to upload
        @param blob_name The name of the blob to upload to
        @param file_type The type of file to upload (csv, json)
        """
        return dataframe_to_storage(
            self.storage_client,
            dataframe,
            self.bucket_name,
            blob_name,
            file_type
        )
    
    def storage_to_bigquery(
        self,
        blob: storage.Blob,
        table_id: str,
        job_config: bigquery.LoadJobConfig
        ) -> bigquery.Table:
        """
        Load a blob into a bigquery table
        @param blob The blob to load
        @param table_id The id of the table format dataset.table
        @param job_config The job config
        @return The load job
        """
        return storage_to_bigquery(
            blob,
            self.bigquery_client,
            table_id,
            job_config
        )
    
    def dataframe_to_bigquery(
        self,
        dataframe: pl.DataFrame,
        blob_name: str,
        table_id: str,
        file_type: str = 'csv',
        job_config: bigquery.LoadJobConfig = bigquery.LoadJobConfig(
            write_disposition='WRITE_TRUNCATE',
            autodetect=True
        )
        ) -> tuple[storage.Blob, bigquery.Table]:
        """
        Load a dataframe into a bigquery table, via cloud storage
        @param dataframe The dataframe to load
        @param blob_name The name of the blob to load from
            (e.g. full/path/to/blob.csv)
        @param table_id The id of the table format dataset.table
        @param file_type The type of file to load (csv, json)
        @param job_config The job config
        @return A tuple of the blob and load job
        """
        return dataframe_to_bigquery(
            dataframe,
            self.bucket_name,
            blob_name,
            table_id,
            file_type = file_type,
            job_config = job_config,
            storage_client = self.storage_client,
            bigquery_client = self.bigquery_client
        )

    def autodetect_dataframe_schema(
        self,
        dataframe: pl.DataFrame,
        blob_name: str,
        table_id: str,
        file_type: str = 'csv',
        destination_dir: str = 'data/compiled'
        ) -> str:
        """
        Autodetect the bigquery schema of a dataframe and write it to a local file
        @param dataframe The dataframe to autodetect
        @param blob_name The name of the blob to load from
            (e.g. full/path/to/blob.csv)
        @param table_id The id of the table format dataset.table
        @param file_type The type of file to load (csv, json)
        @param destination_dir The directory to write the schema to
        @return The path to the local file containing the schema
        """
        return autodetect_dataframe_schema(
            dataframe,
            self.bucket_name,
            blob_name,
            table_id,
            file_type = file_type,
            destination_dir = destination_dir,
            storage_client = self.storage_client,
            bigquery_client = self.bigquery_client
        )
