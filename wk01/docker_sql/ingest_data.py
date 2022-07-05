import pandas as pd
import argparse
from sqlalchemy import create_engine
from time import time
import os

pd.__version__

def main(params):

  user = params.user
  password = params.password
  host = params.host
  port = params.port
  db = params.db
  url = params.url
  table_name = params.table_name
  
  csv_name = 'output.csv'

  os.system(f'curl {url} --output {csv_name}')

  engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

  df_iter = pd.read_csv(f'{csv_name}', iterator=True, chunksize=100000)

  df = next(df_iter)

  df.tpep_pickup_datetime = pd.to_datetime(df['tpep_pickup_datetime'])
  df.tpep_dropoff_datetime = pd.to_datetime(df['tpep_dropoff_datetime'])

  df.head(n=0).to_sql(name=f'{table_name}', con=engine, if_exists='replace')
  
  df.to_sql(name=f'{table_name}', con=engine, if_exists='append')

if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

  parser.add_argument('--user', help='username for db')
  parser.add_argument('--password', help='password for db')
  parser.add_argument('--host', help='host for db')
  parser.add_argument('--port', help='port for db')
  parser.add_argument('--db', help='db name')
  parser.add_argument('--table-name', help='table name')
  parser.add_argument('--url', help='url of the csv file')

  args = parser.parse_args()
  
  main(args)




