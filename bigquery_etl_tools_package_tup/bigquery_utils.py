"""Module providing helper functions for working with bigquery"""

from google.cloud import storage, bigquery


def storage_to_bigquery(
        blob: storage.Blob,
        bigquery_client: bigquery.Client,
        dataset_name: str,
        table_name: str,
        job_config: bigquery.LoadJobConfig
        ) -> bigquery.TableReference:
    """
    Load a blob into a bigquery table
    @param blob The blob to load
    @param bigquery_client The bigquery client
    @param dataset_name The name of the dataset
    @param table_name The name of the table
    @param job_config The job config
    @return The load job
    """
    uri = f'gs://{blob.bucket.name}/{blob.name}'
    dataset_ref = bigquery_client.dataset(dataset_id=dataset_name)
    table_ref = dataset_ref.table(table_id=table_name)

    _ = bigquery_client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config
    )

    return table_ref
