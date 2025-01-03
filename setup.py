from setuptools import setup, find_packages
from os import path


working_directory = path.abspath(path.dirname(__file__))

with open(path.join(working_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='bigquery_etl_tools',
    version='0.0.4',
    url='https://github.com/Mattanalytix/bigquery-etl-tools',
    author='mattanalytix',
    author_email='info@mattanalytix.com',
    description='BigQuery ETL Tools',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    install_requires=[],
)
