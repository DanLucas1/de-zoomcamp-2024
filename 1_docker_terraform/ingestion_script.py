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

    # connect to postgres: [type of db]  user:pass@hostname :port/db_name
    # engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # find date and time columns to use as parse_dates argument in full CSV read
    csv_headers = pd.read_csv(csv_name, nrows=0).columns
    date_substr = ["date", "time"]
    datetime_cols = [col for col in csv_headers if any(sub in col for sub in date_substr)]

    # create iterator to read CSV
    df_iter = pd.read_csv(csv_name,
                          parse_dates = datetime_cols,
                          iterator=True,
                          chunksize=50000)

    # add table header - covered in replace/append logic below
    # df = next(df_iter)
    # df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    batchnum = 1
    exists = 'replace'

    while True:
        try:
            start = time()
            df = next(df_iter)
            rows = df.shape[0]
            df.to_sql(name=table_name, con=engine, if_exists=exists)
            duration = time() - start
            print(f'inserted batch {batchnum}, {rows} rows: {duration:.2f} seconds')
            exists = 'append'
            batchnum += 1
        except StopIteration:
            print(f'All batches processed. Total batches: {batchnum}')
            break  # exit the loop when there are no more items in the iterator
        except Exception as e:
            traceback_str = traceback.format_exc()  # capture the traceback as a string
            print(f'Error in batch {batchnum}: {e}\nTraceback:\n{traceback_str}')
            break  # break out of the loop to prevent the kernel from crashing

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