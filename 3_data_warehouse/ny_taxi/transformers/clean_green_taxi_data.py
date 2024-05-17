if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd
import re

# function to clean up columns (specific to TLC trips data but works for all taxi types)
def column_cleanup(col_name):
    col_name = re.sub(r'(?<!_)ID', r'_ID', col_name)
    col_name = re.sub(r'(?<!_)PU', r'PU_', col_name)
    col_name = re.sub(r'(?<!_)DO', r'DO_', col_name)
    col_name = col_name.lower()
    return col_name


@transformer
def transform(data, *args, **kwargs):

    # set column names to snake case
    data.columns = [column_cleanup(col) for col in data.columns]

    # convert missing pickup and dropoff IDs to 264 (unknown) and cast to int32
    data['pu_location_id'] = data['pu_location_id'].fillna(264).astype('int32')
    data['do_location_id'] = data['do_location_id'].fillna(264).astype('int32')

    # convert low-cardinality integers stored as floats
    data.ratecode_id = data.ratecode_id.astype('Int32')
    data.payment_type = data.payment_type.astype('Int32')
    data.trip_type = data.trip_type.astype('Int32')

    # convert store_and_fwd_flag to a more efficient dtype
    data.store_and_fwd_flag = data.store_and_fwd_flag.map({'Y': 1, 'N': 0}).astype(pd.Int8Dtype())

    return data

@test
def test_output(output, *args) -> None:

    assert output is not None, 'The output is undefined'

    # check range of pickup/dropoff IDs
    assert output['pu_location_id'].max() <= 265, 'pu_location_id out of range'
    assert output['pu_location_id'].min() >= 1, 'pu_location_id out of range'
    assert output['do_location_id'].max() <= 265, 'do_location_id out of range'
    assert output['do_location_id'].min() >= 1, 'do_location_id out of range'
   