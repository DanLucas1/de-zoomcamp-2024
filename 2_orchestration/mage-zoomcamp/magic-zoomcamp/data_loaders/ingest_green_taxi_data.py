import io
import numpy as np
import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# df = pq.read_table(source=source).to_pandas()


def read_file(date):
    try:
        df = pd.read_parquet(
            f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{date:%Y-%m}.parquet',
            engine='pyarrow')
    except Exception as E:
        print('Could not read data:', E)
    return df


def timeframe(year, start_month, end_month):
    get_months = []
    for month in range(start_month, end_month+1):
        date_obj = datetime(year=year, month=month, day=1)
        get_months.append(date_obj)
    return get_months


# function to loop through datasets and concat
def combine_datasets(datasets):
    base = None
    add = None

    for monthly_data in datasets:
        add = read_file(monthly_data)
        add = add.assign(
            data_month = monthly_data.month,
            data_year = monthly_data.year)

        if base is None:
            base = add
        else:
            base = pd.concat([base, add])
        print(f'loaded data from {monthly_data:%Y-%m}:', add.shape[0], 'rows')
    return base.reset_index()


@data_loader
def load_all(*args, **kwargs):
    pass
    datasets = timeframe(2022, 1, 12)
    results = combine_datasets(datasets)
    return results

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    assert output.data_year.nunique() == 1, 'data outside year range'
    assert output.data_year.unique()[0] == 2022, 'data from wrong year'
    assert np.array_equal(np.sort(output.data_month.unique()), np.arange(1, 13)), 'incorrect number of months or month values'