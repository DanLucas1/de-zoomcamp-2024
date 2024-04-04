from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

import pyarrow as pa
import pyarrow.parquet as pq
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_acct_creds.json'

bucket_name = 'mage-zoomcamp-test-dl2345'
project_id = 'de-zoomcamp-413811'

table_name = 'nyc_taxi_data'

root_path = f'{bucket_name}/{table_name}'


@data_exporter
def export_data(data, *args, **kwargs):

    # create parquet dataset from pandas df 
    table = pa.Table.from_pandas(data)

    # generate Google Cloud Storage object
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem = gcs
    )