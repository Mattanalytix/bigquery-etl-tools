"""Module containing configuration for file types"""

import logging
from google.cloud import bigquery, storage
import polars as pl


FILE_TYPE_CONFIG = {
    'csv': {
        'dataframe_write_function': [pl.DataFrame.write_csv, {
            'datetime_format': '%Y-%m-%d %H:%M:%S', 'date_format': '%Y-%m-%d'
        }],
        'blob_type': 'text/csv',
        'bigquery_format': bigquery.SourceFormat.CSV
    },
    'json': {
        'dataframe_write_function': [pl.DataFrame.write_ndjson, {}],
        'blob_type': 'text/json',
        'bigquery_format': bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    }
}


def get_source_format_from_blob(blob: storage.Blob) -> bigquery.SourceFormat:
    """
    Get the source format from a blob based on the type of blob
    as defined in the FILE_TYPE_CONFIG
    @param blob The blob to get the source format from
    @return The bigquery source format for the file type (csv, json)
    """
    for k, v in FILE_TYPE_CONFIG.items():
        if v['blob_type'] == blob.content_type:
            logging.info("""Setting file type to %s as detected from
                         the blob %s""", k, blob.name)
            return v['bigquery_format']
