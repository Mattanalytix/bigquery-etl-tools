"""Module for testing file_utils.py"""

from bigquery_etl_tools.file_utils import split_bucket_blob


# Test split_bucket_blob function

def test_split_bucket_blob_bucket_only():
    """Test the split_bucket_blob function with a bucket only"""
    bucket_blob = 'bucketname'
    assert split_bucket_blob(bucket_blob) == ('bucketname', '')


def test_split_bucket_blob_correct_format():
    """Test the split_bucket_blob function with a bucket and blob"""
    bucket_blob = 'bucketname:path/to/blob'
    assert split_bucket_blob(bucket_blob) == ('bucketname', 'path/to/blob')
