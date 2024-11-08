# BigQuery Ingest Utils

This package contains helper utilities to make it easier to ingest data into BigQuery. The main functionality is to help ingest data from local dataframes or files. The main assumption of this package is that your data can be loaded into a `polars` dataframe and uploaded from there into bigquery, either directly or via cloud storage.

**Note:** This package is biased towards certain data structures and file configurations, at the benefit of saved time to get something working.

## Local Development

To use the package locally, authenticate with GCP using OAuth:

```cmd
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

Setup environment for local development:

```
conda create -n bigquery_etl_tools pip
conda activate bigquery_etl_tools
pip install -r requirements.txt
```

Optionally, install `ipykernel` if developing in jupyter notebooks:

```
pip install ipykernel
```

## Testing

The tests for this package are created using [pytest](https://docs.pytest.org/), for some of the tests you will need to specifiy variables which are specfic to your GCP environment. To do that create a `.env` file in the root of the repository and set the following values:

```.env
BUCKET=bucketname
```

To run pytest run the following command on the command line:

```cmd
pytest # full run

pytest tests/MODULE_NAME.py # module run
```

@TODO examples