
-- creating external table from GCS files
CREATE OR REPLACE EXTERNAL TABLE `green_taxi_2022.external_data_test`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoomcamp-test-dl2345/*'] -- for batched files
  --   uris = ['gs://mage-zoomcamp-test-dl2345/green_taxi_data.parquet'] -- alt version for using a for single file
);


-- materialize external table in dataset
CREATE OR REPLACE TABLE green_taxi_2022.internal_data_test AS
SELECT * FROM green_taxi_2022.external_data_test;


-- count total records and total records with 0 fare
SELECT COUNT(*) AS total
FROM green_taxi_2022.internal_data_test
WHERE fare_amount = 0;


-- count distinct PUlocationID: measuring internal v. external resource consumption
SELECT COUNT (DISTINCT PULocationID) AS PULocCount
FROM green_taxi_2022.external_data_test;

SELECT COUNT (DISTINCT PULocationID) AS PULocCount
FROM green_taxi_2022.internal_data_test;


-- materialize a clustered & partitioned table
CREATE OR REPLACE TABLE green_taxi_2022.internal_data_optimized
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID
AS SELECT * FROM green_taxi_2022.external_data_test;


-- testing performance of partitioned/clustered and unoptimized tables
SELECT COUNT (DISTINCT PULocationID) AS PULocIDCount
FROM green_taxi_2022.internal_data_optimized
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' and '2022-06-30';

SELECT COUNT (DISTINCT PULocationID) AS PULocIDCount
FROM green_taxi_2022.internal_data_test
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' and '2022-06-30';
