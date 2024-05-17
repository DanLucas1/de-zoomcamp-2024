from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame
import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


# convert to snake case function
def convert (camel_input):
    words = re.findall(r'[A-Z]?[a-z]+|[A-Z]{2,}(?=[A-Z][a-z]|\d|\W|$)|\d+', camel_input)
    return '_'.join(map(str.lower, words))

@transformer
def filter_nonzero(df):
    df = df.loc[(df.passenger_count > 0) & (df.trip_distance > 0)]
    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date
    df.columns = [convert(col) for col in df.columns]
    return df

@test
def test_output(output, *args) -> None:
    assert output.passenger_count.min() > 0, 'passenger_count error: 0 passengers'
    assert output.trip_distance.min() > 0, 'trip_distance error: 0 miles'
    assert output.vendor_id.isin([1,2]).all(), 'invalid vendor ID'
    assert output is not None, 'The output is undefined'