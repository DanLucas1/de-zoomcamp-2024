import io
import numpy as np
import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from datetime import datetime, timedelta

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from mage_ai.data_preparation.variable_manager import set_global_variable

@data_loader
def load_all(*args, **kwargs):

    # get taxi type and runtime
    taxi_type = kwargs.get('taxi_type')
    now = kwargs.get('execution_date')
    # now = datetime(2019, 2, 1) # for testing on historical data

    ## date filtering for parquet read
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
    pickup, dropoff = date_columns.get(f'{taxi_type}')

    # create filters object for pyarrow read
    date_filters = [
        (pickup, ">=", pa.scalar(start_date)),
        (pickup, "<", pa.scalar(end_date)),
        (dropoff, "<=", pa.scalar(datetime(2050, 1, 1))), # no strict filtering here, just avoiding out of pd.Timestamp errors 
        (dropoff, ">=", pa.scalar(datetime(2000, 1, 1)))
        ]    

    # read data from NY TLC site
    try:
        df = pd.read_parquet(
            f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{now:%Y-%m}.parquet',
            engine='pyarrow',
            filters = date_filters)
        print(df.shape)
        print(f'loaded {taxi_type} taxi data from {now:%Y-%m} - {df.shape[0]} trips')
    except Exception as E:
        print('Could not read data:', E)

    # generate a date-only pickup column for partitioning
    df['pickup_date'] = df[pickup].dt.date
    print('dates converted')
    return df

@test
def test_output(output, *args, **kwargs) -> None:
    assert output is not None, 'The output is undefined'