import pandas
from pandas import DataFrame
from os import path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import os
from datetime import datetime, timedelta

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


# google cloud authentication
project_id = 'de-zoomcamp-413811'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_acct_creds.json'


@data_loader
def load_from_google_cloud_storage(*args, **kwargs):

    # reading date and taxi type from pipeline variables
    taxi_type = kwargs.get('taxi_type')
   
    now = kwargs.get('execution_date')
    # now = datetime(2019, 1, 1) # for testing on historical data
    month = now.month
    year = now.year

    # bucket name matches bucket defined in variables.tf
    bucket_name = 'ny_taxi_storage_413811'
    gcs_path = f'{bucket_name}/{taxi_type}/{year}/{month}'

    # generate GCS object
    gcs = pa.fs.GcsFileSystem()

    # specify partitioning based on taxi type
    partitioning = ds.partitioning(
        schema=pa.schema([('pickup_date', pa.date64())]),
        flavor='hive'
    )

    # read the parquet files at the gcs path (month level) and combine
    pq_from_gcs = ds.dataset(
        gcs_path,
        filesystem=gcs,
        format='parquet',
        partitioning=partitioning)

    # cast dataset to pandas df
    df = pq_from_gcs.to_table().to_pandas()
    df = df.drop(columns=['pickup_date'])

    return df

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'