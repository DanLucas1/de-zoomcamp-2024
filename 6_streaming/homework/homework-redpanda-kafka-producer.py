import json
import time 
from kafka import KafkaProducer
import pandas as pd
from pprint import pprint
from datetime import datetime

# create json serializer
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# define server and connect
server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

# confirm connection
if producer.bootstrap_connected():
    print('connected to server')
else:
    print('connection failure')
    exit()


# read in data extract from CSV
url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz'
usecols = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
    ]

df = pd.read_csv(
    url,
    compression='gzip',
    usecols=usecols)

df = df.sort_values('lpep_pickup_datetime', ascending=True)


# send all rows as messages and time results
t0 = time.time()

topic_name = 'green-trips'

# n = 2000
# df = df.iloc[:n] # use n rows for testing

for row in df.itertuples(index=False):
    message = {col: getattr(row, col) for col in row._fields}
    producer.send(topic_name, value=message, timestamp_ms=int(time.time_ns()/1000))

    # show time elapsed
    time.sleep(0.1)
    tx = time.time() - t0
    print(f'{tx:.2f} seconds elapsed')

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')