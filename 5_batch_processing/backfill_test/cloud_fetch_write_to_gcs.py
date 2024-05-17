import pyspark
import requests
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, year, month, dayofmonth
import re
import subprocess
import os

# establish start and end dates
now = datetime.now()

# ----- FOR TESTING: -----
now = datetime(2020, 2, 1)
taxi_type = 'green'
# ------------------------

start_date = datetime(now.year, now.month, 1)
end_date = start_date + relativedelta(months=1)

# dataset schema
green_schema = types.StructType([
    types.StructField('VendorID', types.LongType(), True),
    types.StructField('lpep_pickup_datetime', types.TimestampType(), True),
    types.StructField('lpep_dropoff_datetime', types.TimestampType(), True),
    types.StructField('store_and_fwd_flag', types.StringType(), True),
    types.StructField('RatecodeID', types.DoubleType(), True),
    types.StructField('PULocationID', types.LongType(), True),
    types.StructField('DOLocationID', types.LongType(), True),
    types.StructField('passenger_count', types.DoubleType(), True),
    types.StructField('trip_distance', types.DoubleType(), True),
    types.StructField('fare_amount', types.DoubleType(), True),
    types.StructField('extra', types.DoubleType(), True),
    types.StructField('mta_tax', types.DoubleType(), True),
    types.StructField('tip_amount', types.DoubleType(), True),
    types.StructField('tolls_amount', types.DoubleType(), True),
    types.StructField('ehail_fee', types.IntegerType(), True),
    types.StructField('improvement_surcharge', types.DoubleType(), True),
    types.StructField('total_amount', types.DoubleType(), True),
    types.StructField('payment_type', types.DoubleType(), True),
    types.StructField('trip_type', types.DoubleType(), True),
    types.StructField('congestion_surcharge', types.DoubleType(), True)
])


# create spark session
spark = SparkSession.builder \
    .appName('Read TLC parquet to GCS') \
    .getOrCreate()

# URL and GCS path setup
url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{now:%Y-%m}.parquet'
gcs_staging = f'gs://ny_taxi_storage_413811/staging/tripdata.parquet'
gcs_storage = f'gs://ny_taxi_storage_413811/{taxi_type}2/'


# Download data
response = requests.get(url)
if response.status_code == 200:
    with open('/tmp/temp_data.parquet', 'wb') as file:
        file.write(response.content)
    
    # Use gsutil to upload if local write is used
    subprocess.run(['gsutil', 'cp', '/tmp/temp_data.parquet', gcs_staging], check=True)
    
    # Read the data into a DataFrame directly from GCS
    trips = spark.read.parquet(gcs_staging, schema = green_schema)
else:
    print(f"Failed to download the file: Status code {response.status_code}")


# function to clean up column names
def column_cleanup(col_name):
    col_name = re.sub(r'(?<!_)ID', r'_ID', col_name)
    col_name = re.sub(r'(?<!_)PU', r'PU_', col_name)
    col_name = re.sub(r'(?<!_)DO', r'DO_', col_name)
    col_name = col_name.lower()
    return col_name

# apply cleanup function to green trips data
trips = trips.select(*[col(c).alias(column_cleanup(c)) for c in trips.columns])


# dynamic column choice using taxi_type
date_columns = {
    'yellow': ('tpep_pickup_datetime', 'tpep_dropoff_datetime'),
    'green': ('lpep_pickup_datetime', 'lpep_dropoff_datetime'),
    'fhv': ('pickup_datetime', 'dropOff_datetime')}
pickup, dropoff = date_columns.get(taxi_type)

# remove out-of-range dates
trips = trips.filter((col(pickup) >= start_date) & (col(dropoff) < end_date))

# replace missing values for location IDs with "unknown" ID
trips = trips.na.fill({'pu_location_id': 264, 'do_location_id': 264})

# add datepart columns for partitioning
trips = trips \
    .withColumn("year", year(col(pickup))) \
    .withColumn("month", month(col(pickup))) \
    .withColumn("day", dayofmonth(col(pickup)))

# write to final cloud storage location with datepart partitioning
trips.write.format('parquet').mode('append').partitionBy('year', 'month', 'day').save(gcs_storage)


# read back from cloud storage
trips = spark.read.parquet(os.path.join(gcs_storage, '*/*/*'))

# specify staging bucket to use
bucket = "ny_taxi_storage_413811"
spark.conf.set('temporaryGcsBucket', bucket)

# write to bigquery
trips.write.format('bigquery') \
  .option('table', 'green_taxi_spark_test.trips') \
  .save()

# Stop Spark session
spark.stop()