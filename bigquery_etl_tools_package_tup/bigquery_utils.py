"""Module providing helper functions for working with bigquery"""

import logging

from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound


def storage_to_bigquery(
        blob: storage.Blob,
        bigquery_client: bigquery.Client,
        table_id: str,
        job_config: bigquery.LoadJobConfig
        ) -> bigquery.Table:
    """
    Load a blob into a bigquery table
    @param blob The blob to load
    @param bigquery_client The bigquery client
    @param table_id The id of the table format dataset.table
    @param job_config The job config
    @return The load job
    """
    uri = f'gs://{blob.bucket.name}/{blob.name}'

    logging.info('Loading %s into %s', uri, table_id)

    load_job = bigquery_client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )

    load_job.result()

    destination_table = bigquery_client.get_table(table_id)
    logging.info("Loaded {} rows.".format(destination_table.num_rows))

    return destination_table


def table_exists(
        table_id: str
        ) -> bool:
    """
    Check that a bigquery table exists
    @param table_id The table id
    @return True if the table exists, False otherwise
    """
    client = bigquery.Client()
    try:
        client.get_table(table_id)  # Make an API request.
        logging.info("Table {} already exists.".format(table_id))
        return True
    except NotFound:
        logging.info("Table {} is not found.".format(table_id))
        return False
