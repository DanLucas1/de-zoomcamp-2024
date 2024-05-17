#!/bin/bash

echo "ingest zone lookup data"
python ingestion_script.py \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=zone_lookup \
    --url="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"

echo "ingest 2019 trip data"
python ingestion_script.py \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=trips_201909 \
    --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"