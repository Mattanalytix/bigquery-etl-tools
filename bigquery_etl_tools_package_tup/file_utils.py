"""Module providing helper functions for working with files related to etl
processes and google cloud platform"""

import logging


def split_bucket_blob(bucket_dir: str) -> tuple[str, str]:
    """
    Split a google cloud storage path into the bucket name and blob directory.
    Name must be in the format bucketname:blobdir
    @param bucket_dir The path to the dir in the bucket in the
        form bucketname:blobdir
    @return A tuple of the bucket name and blob directory
    """
    split = bucket_dir.split(':')

    if len(split) == 1:
        logging.warning("""Warning: No : found in bucket_dir,
                        returning only %s as bucketname""", split[0])
        return (split[0], '')

    return split[0], split[1]
