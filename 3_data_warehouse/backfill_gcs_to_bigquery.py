import os
import pandas as pd
from pandas import DataFrame
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from google.cloud import bigquery
import pandas_gbq
from backfill_schemas import bq_schemas

# google cloud authentication
project_id = 'de-zoomcamp-413811'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_acct_creds.json'


def load_from_google_cloud_storage(taxi_type, now):
    
    # specify bucket name and path to read from (matches bucket defined in variables.tf)
    bucket_name = 'ny_taxi_storage_413811'
    gcs_path = f'{bucket_name}/{taxi_type}/{now.year}/{now.month}'

    # generate GCS object
    gcs = pa.fs.GcsFileSystem()

    # specify partitioning column for pyarrow dataset read
    partitioning = ds.partitioning(
        schema=pa.schema([('pickup_date', pa.date32())]),
        flavor='hive'
    )

    # read the parquet files at the gcs path (month level) and combine
    pq_from_gcs = ds.dataset(
        gcs_path,
        filesystem=gcs,
        format='parquet',
        partitioning=partitioning)

    # write dataset to pandas df and drop the pickup_date column (used for path creation only)
    df = pq_from_gcs.to_table().to_pandas()
    df = df.drop(columns=['pickup_date'])

    return df


def export_data_to_big_query(df: DataFrame, taxi_type, now) -> None:

    ## ---- Setup for partitioning in bigquery table creation ----

    # identify partitioning column based on taxi_type
    partition_options = {
        'yellow': 'tpep_pickup_datetime',
        'green': 'lpep_pickup_datetime',
        'fhv': 'pickup_datetime'}
    trip_datetime = partition_options.get(taxi_type)

    # specify bigquery table ID for each taxi type
    table_id = f'ny_taxi_dataset.{taxi_type}_trips'


    ## ---- Write tables to bigquery ----

    # initialize bigquery client
    client = bigquery.Client()

    # access imported bigquery schema
    bq_schema=bq_schemas.get(taxi_type)

    # define partitioning and clustering
    table = bigquery.Table(f'{project_id}.{table_id}', schema=bq_schema)
    table.time_partitioning = bigquery.TimePartitioning(  # set partitioning field
        type_=bigquery.TimePartitioningType.DAY,
        field=trip_datetime
    )
    table.clustering_fields = ['pu_location_id']  # set clustering field

    # create the table to write data to
    client.create_table(table, exists_ok=True)

    try:
        pandas_gbq.to_gbq(
            df,
            table_id,
            project_id=project_id,
            # chunksize=10000, # depreciated
            if_exists='append')
        print(f'{now.year}-{now.month}: wrote {df.shape[0]} records to dataset {project_id}.{table_id}')
    except Exception as E:
        print(f'could not write data to BigQuery: {E}')

# function to generate list of monthly dates for looping backfills
def timeframe(start_date, end_date):
    date_range = list(pd.date_range(start=start_date, end=end_date, freq='MS'))
    return date_range

if __name__ == '__main__':

    # copy gcs parquet files to bigquery for 2019 (all types)
    for date in timeframe('2019-01-01', '2019-12-01'):
        for taxi_type in ['yellow', 'fhv', 'green']:
            taxi_df = load_from_google_cloud_storage(taxi_type, date)
            export_data_to_big_query(taxi_df, taxi_type, date)
            del taxi_df

    # copy gcs parquet files to bigquery for 2020 (yellow and green only)
    for date in timeframe('2020-01-01', '2020-12-01'):
        for taxi_type in ['yellow', 'green']:
            taxi_df = load_from_google_cloud_storage(taxi_type, date)
            export_data_to_big_query(taxi_df, taxi_type, date)
            del taxi_df