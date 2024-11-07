from setuptools import setup, find_packages
from os import path

from google.cloud import bigquery, storage


working_directory = path.abspath(path.dirname(__file__))

with open(path.join(working_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='bigquery_etl_tools_package_tup',
    version='0.0.1',
    url='',
    author='Mattanalytix',
    author_email='matthewh@mattanalytix.com',
    description='ETL tools for BigQuery',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    install_requires=[
        bigquery,
        storage,
        polars
    ]
)