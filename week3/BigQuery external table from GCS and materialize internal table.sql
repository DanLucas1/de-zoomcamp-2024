CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-413811.ny_taxi_dataset.yellow_trips_ext`
  (vendor_id INT64,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count FLOAT64,
  trip_distance FLOAT64,
  pu_location_id INT64,
  do_location_id INT64,
  ratecode_id INT64,
  store_and_fwd_flag INT64,
  payment_type INT64,
  fare_amount FLOAT64,
  extra FLOAT64,
  mta_tax FLOAT64,
  improvement_surcharge FLOAT64,
  tip_amount FLOAT64,
  tolls_amount FLOAT64,
  total_amount FLOAT64,
  congestion_surcharge FLOAT64,
  airport_fee FLOAT64)
  OPTIONS(
    format ="parquet",
    uris = ['gs://ny_taxi_storage_413811/yellow/*.parquet']);


CREATE OR REPLACE TABLE `de-zoomcamp-413811.ny_taxi_dataset.yellow_trips`
  PARTITION BY
    DATE(tpep_pickup_datetime)
  CLUSTER BY
    vendor_id
AS
  SELECT * FROM de-zoomcamp-413811.ny_taxi_dataset.ext;