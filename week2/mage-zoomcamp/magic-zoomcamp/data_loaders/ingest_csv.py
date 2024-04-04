import io
import pandas as pd
import requests
import os
from datetime import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# single dataset loading function
def load_data_from_api(date, *args, **kwargs):
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{date:%Y-%m}.csv.gz'

    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float
    }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    return pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)

# generate a list of datetime objects to be interpolated in requests string (todo: scalable for range of multiple years)
def timeframe(year, start_month, end_month):
    month_values = []
    for month in range(start_month, end_month+1):
        date_obj = datetime(year=year, month=month, day=1)
        month_values.append(date_obj)
    return month_values

# function to loop through datasets and concat
def combine_datasets(datasets):
    base = None
    add = None

    for monthly_data in datasets:
        add = load_data_from_api(monthly_data)
        add = add.assign(
            data_month = monthly_data.month,
            data_year = monthly_data.year)

        if base is None:
            base = add
        else:
            base = pd.concat([base, add]).reset_index()
        print(f'added data from {monthly_data:%Y-%m}:', add.shape[0], 'rows')
    return base

@data_loader
def load(*args, **kwargs):
    return combine_datasets(timeframe(2020, 10, 12))
    # return combine_datasets(timeframe(2022, 1, 12))

# @test
# def test_output(output, *args) -> None:
#     assert output.shape[0] > 1, 'no data'
#     assert output is not None, 'The output is undefined'