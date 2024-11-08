"""Module containing configuration for file types"""

from google.cloud import bigquery
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