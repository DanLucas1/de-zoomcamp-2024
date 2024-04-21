from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
import os
from os import path
import pandas_gbq
from google.cloud import bigquery
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_acct_creds.json'

# define schemas for each taxi_type
schemas = {
    'yellow': [
        bigquery.SchemaField('vendor_id', 'INTEGER'),
        bigquery.SchemaField('tpep_pickup_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('tpep_dropoff_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('passenger_count', 'FLOAT'),
        bigquery.SchemaField('trip_distance', 'FLOAT'),
        bigquery.SchemaField('pu_location_id', 'INTEGER'),
        bigquery.SchemaField('do_location_id', 'INTEGER'),
        bigquery.SchemaField('ratecode_id', 'INTEGER'),
        bigquery.SchemaField('store_and_fwd_flag', 'INTEGER'),
        bigquery.SchemaField('payment_type', 'INTEGER'),
        bigquery.SchemaField('fare_amount', 'FLOAT'),
        bigquery.SchemaField('extra', 'FLOAT'),
        bigquery.SchemaField('mta_tax', 'FLOAT'),
        bigquery.SchemaField('improvement_surcharge', 'FLOAT'),
        bigquery.SchemaField('tip_amount', 'FLOAT'),
        bigquery.SchemaField('tolls_amount', 'FLOAT'),
        bigquery.SchemaField('total_amount', 'FLOAT'),
        bigquery.SchemaField('congestion_surcharge', 'FLOAT'),
        bigquery.SchemaField('airport_fee', 'FLOAT')
    ],
    'green': [
        bigquery.SchemaField('vendor_id', 'INTEGER'),
        bigquery.SchemaField('lpep_pickup_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('lpep_dropoff_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('store_and_fwd_flag', 'INTEGER'),
        bigquery.SchemaField('ratecode_id', 'INTEGER'),
        bigquery.SchemaField('pu_location_id', 'INTEGER'),
        bigquery.SchemaField('do_location_id', 'INTEGER'),
        bigquery.SchemaField('passenger_count', 'FLOAT'),
        bigquery.SchemaField('trip_distance', 'FLOAT'),
        bigquery.SchemaField('fare_amount', 'FLOAT'),
        bigquery.SchemaField('extra', 'FLOAT'),
        bigquery.SchemaField('mta_tax', 'FLOAT'),
        bigquery.SchemaField('tip_amount', 'FLOAT'),
        bigquery.SchemaField('tolls_amount', 'FLOAT'),
        bigquery.SchemaField('ehail_fee', 'FLOAT'),
        bigquery.SchemaField('improvement_surcharge', 'FLOAT'),
        bigquery.SchemaField('total_amount', 'FLOAT'),
        bigquery.SchemaField('payment_type', 'INTEGER'),
        bigquery.SchemaField('trip_type', 'INTEGER'),
        bigquery.SchemaField('congestion_surcharge', 'FLOAT')
    ],
    'fhv': [
        bigquery.SchemaField('dispatching_base_num', 'STRING'),
        bigquery.SchemaField('pickup_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('dropoff_datetime', 'TIMESTAMP'),
        bigquery.SchemaField('pu_location_id', 'INTEGER'),
        bigquery.SchemaField('do_location_id', 'INTEGER'),
        bigquery.SchemaField('sr_flag', 'FLOAT'),
        bigquery.SchemaField('affiliated_base_number', 'STRING')
    ]
}

@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:

    ## ---- SETUP ----

    # load parameters from kwargs
    project_id = kwargs.get('project_id')
    dataset = kwargs.get('bq_dataset_name')
    taxi_type = kwargs.get('taxi_type')

    # identify partitioning column based on taxi_type
    partition_options = {
        'yellow': 'tpep_pickup_datetime',
        'green': 'lpep_pickup_datetime',
        'fhv': 'pickup_datetime'}
    trip_datetime = partition_options.get(taxi_type)

    # specify bigquery table ID
    table_id = f'{dataset}.{taxi_type}_trips'

    ## ---- WRITE TO BIGQUERY ----

    # initialize bigquery client
    client = bigquery.Client()

    # define partitioning and clustering
    table = bigquery.Table(f'{project_id}.{table_id}', schema=schemas.get(taxi_type, None))
    table.time_partitioning = bigquery.TimePartitioning(  # define partitioning field
        type_=bigquery.TimePartitioningType.DAY,
        field=trip_datetime
    )
    table.clustering_fields = ['pu_location_id']  # define clustering field

    # create the table
    client.create_table(table, exists_ok=True)

    try:
        pandas_gbq.to_gbq(
            df,
            table_id,
            project_id=project_id,
            # chunksize=10000,
            if_exists='append')
        print(f'wrote {df.shape[0]} records to dataset {project_id}.{table_id}')
    except Exception as E:
        print(f'could not write data to BigQuery: {E}')