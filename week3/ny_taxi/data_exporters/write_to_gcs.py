from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import datetime, timedelta

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# google cloud authentication
project_id = 'de-zoomcamp-413811'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_acct_creds.json'

@data_exporter
def export_data(data, *args, **kwargs):

    # create parquet dataset from df 
    table = pa.Table.from_pandas(data)

    # generate GCS object
    gcs = pa.fs.GcsFileSystem()

    # set parameters for path

    # reading date and taxi type from pipeline variables
    taxi_type = kwargs.get('taxi_type')
    now = kwargs.get('execution_date')
    # now = datetime(2019, 3, 1) # for testing on historical data

    month = now.month
    year = now.year

    # bucket name matches bucket defined in variables.tf
    bucket_name = 'ny_taxi_storage_413811'
    root_path = f'{bucket_name}/{taxi_type}/{year}/{month}'

    # write dataset to specified path
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['pickup_date'],
        basename_template = 'trips{i}.parquet',
        filesystem = gcs
    )