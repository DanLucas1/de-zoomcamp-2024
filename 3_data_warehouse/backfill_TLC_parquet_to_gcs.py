import io
import os
import numpy as np
import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datetime import datetime, timedelta
import re
from backfill_schemas import pa_schemas


# google cloud authentication
project_id = 'de-zoomcamp-413811'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_acct_creds.json'


# function to clean up column names
def column_cleanup(col_name):
    col_name = re.sub(r'(?<!_)ID', r'_ID', col_name)
    col_name = re.sub(r'(?<!_)PU', r'PU_', col_name)
    col_name = re.sub(r'(?<!_)DO', r'DO_', col_name)
    col_name = col_name.lower()
    return col_name

def TLC_parquet_to_df(taxi_type, now):

    # ---- Setup for file read: pickup date filtering ----

    # set start date = first of this month, and end date = first of next month
    start_date = datetime(now.year, now.month, 1)
    if now.month == 12:
        end_date = datetime(now.year + 1, 1, 1)
    else:
        end_date = datetime(now.year, now.month + 1, 1)
   
    # dynamic column choice using taxi_type
    date_columns = {
        'yellow': ('tpep_pickup_datetime', 'tpep_dropoff_datetime'),
        'green': ('lpep_pickup_datetime', 'lpep_dropoff_datetime'),
        'fhv': ('pickup_datetime', 'dropOff_datetime')}
    pickup, dropoff = date_columns.get(taxi_type)

    # filter appropriate column for taxi type using start and end dates
    date_filters = [
        (pickup, ">=", pa.scalar(start_date)),
        (pickup, "<", pa.scalar(end_date)),
        (dropoff, "<=", pa.scalar(datetime(2050, 1, 1))), # very loose filtering to avoid "out of pd.Timestamp" range errors 
        (dropoff, ">=", pa.scalar(datetime(2000, 1, 1)))
        ]

    # ---- Read Parquet file from URL using pandas ----

    # read parquet data from NY TLC site directly to pandas df
    try:
        df = pd.read_parquet(
            f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{now:%Y-%m}.parquet',
            engine='pyarrow',
            filters = date_filters
            )
        print(f'loaded {taxi_type} taxi data from {now:%Y-%m} - {df.shape[0]} trips')
    except Exception as E:
        print('Could not read data:', E)

    # generate a date-only pickup column for partitioning in cloud storage
    df['pickup_date'] = df[pickup].dt.date # this method gives us the cleanest file structure names and isn't used elsewhere 
    return df

def dataframe_cleanup(df):
    
    # set column names to snake case
    df.columns = [column_cleanup(col) for col in df.columns]

    # convert missing pickup and dropoff IDs to 264 (unknown) and set to integer dtype
    df['pu_location_id'] = df['pu_location_id'].fillna(264).astype('Int32')
    df['do_location_id'] = df['do_location_id'].fillna(264).astype('Int32')

    # convert store_and_fwd_flag to a more efficient dtype (yellow/green only, FHV does not have this column)
    if 'store_and_fwd_flag' in df.columns:
        df.store_and_fwd_flag = df.store_and_fwd_flag.map({'Y': 1, 'N': 0}).astype('Int32')

    return df

def parquet_table_to_gcs(df, taxi_type, now):

    # Create pyarrow table from DataFrame using imported schema
    pa_schema = pa_schemas.get(taxi_type)
    table = pa.Table.from_pandas(df, schema=pa_schema)

    # ---- Set up for writing to cloud storage  ----

    # generate GCS object
    gcs = pa.fs.GcsFileSystem()

    # bucket name matches bucket defined in variables.tf
    bucket_name = 'ny_taxi_storage_413811'
    root_path = f'{bucket_name}/{taxi_type}/{now.year}/{now.month}'

    # write dataset to specified path in gcs
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['pickup_date'],
        basename_template = 'trips{i}.parquet',
        filesystem = gcs,
        schema = pa_schema
    )

    print(f'wrote {taxi_type} taxi data from {now:%Y-%m} to google cloud storage')

# function to generate list of monthly dates for looping backfills
def timeframe(start_date, end_date):
    date_range = list(pd.date_range(start=start_date, end=end_date, freq='MS'))
    return date_range

if __name__ == '__main__':

    # copy parquet to gcs for 2019 (all types)
    for taxi_type in ['yellow', 'fhv', 'green']:
        for date in timeframe('2019-01-01', '2019-12-01'):
            taxi_df = TLC_parquet_to_df(taxi_type, date)
            clean_taxi_df = dataframe_cleanup(taxi_df)
            parquet_table_to_gcs(clean_taxi_df, taxi_type, date)
            del taxi_df
            del clean_taxi_df

    # copy parquet to gcs for 2020 (yellow and green only)
    for taxi_type in ['yellow', 'green']:
        for date in timeframe('2020-01-01', '2020-12-01'):
            taxi_df = TLC_parquet_to_df(taxi_type, date)
            clean_taxi_df = dataframe_cleanup(taxi_df)
            parquet_table_to_gcs(clean_taxi_df, taxi_type, date)
            del taxi_df
            del clean_taxi_df