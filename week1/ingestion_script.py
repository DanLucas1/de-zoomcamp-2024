import pandas as pd
import os
from sqlalchemy import create_engine
from time import time
import traceback
import argparse


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url


    # download the CSV
    if url.endswith('.csv.gz'):
          csv_name = 'output.csv.gz'
    elif url.endswith('.csv'):
          csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")


    set_dtypes = {'VendorID': pd.Int64Dtype(),
                'passenger_count': pd.Int64Dtype(),
                'trip_distance': 'float64',
                'RatecodeID': pd.Int64Dtype(),
                'store_and_fwd_flag': 'object',
                'PULocationID': pd.Int64Dtype(),
                'DOLocationID': pd.Int64Dtype(),
                'payment_type': pd.Int64Dtype(),
                'fare_amount': 'float64',
                'extra': 'float64',
                'mta_tax': 'float64',
                'tip_amount': 'float64',
                'tolls_amount': 'float64',
                'improvement_surcharge': 'float64',
                'total_amount': 'float64',
                'congestion_surcharge': 'float64'}
    

    # connect to postgres: [type of db]  user:pass@hostname :port/db_name
    # engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')


    df_iter = pd.read_csv(csv_name,
                    dtype=set_dtypes,
                    parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime'],
                    iterator=True,
                    chunksize=50000)

    # add table header
    # df = next(df_iter)
    # df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')



    batchnum = 1
    exists = 'replace'

    while True:
        try:
            start = time()
            df = next(df_iter)
            rows = df.shape[0]
            df.to_sql(name='yellow_taxi_data', con=engine, if_exists=exists)
            duration = time() - start
            print(f'inserted batch {batchnum}, {rows} rows: {duration:.2f} seconds')
            exists = 'append'
            batchnum += 1
        except StopIteration:
            print(f'All batches processed. Total batches: {batchnum}')
            break  # Exit the loop when there are no more items in the iterator
        except Exception as e:
            traceback_str = traceback.format_exc()  # This captures the traceback as a string
            print(f'Error in batch {batchnum}: {e}\nTraceback:\n{traceback_str}')
            break  # Optionally break out of the loop to prevent the kernel from crashing



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest CSV data to postgres db")

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args=parser.parse_args()

    main(args)