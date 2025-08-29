import pandas as pd
pd.__version__
from sqlalchemy import create_engine
from time import time
import argparse

def main(params):
	user = params.user
	password = params.password
	host = params.host
	port = params.port
	db = params.db
	table = params.table
	csv = params.csv

	engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
	df = pd.read_csv(csv, nrows=100)
	df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
	df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
	df.head(n=0).to_sql(table, engine, if_exists='replace', index=False)


	df_iter = pd.read_csv(csv, iterator=True, chunksize=100000, dtype = {'store_and_fwd_flag': 'str'})

	while True:
		t_start = time()
		try:
			df = next(df_iter)
		except StopIteration:
			print("All chunks processed")
			break
		df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
		df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
		df.to_sql(table, engine, if_exists='append', index=False)
		t_end = time()
		print(f"Chunk uploaded in {t_end - t_start} seconds")

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Ingest a CSV file to a PostgresDB Table')
	
	parser.add_argument('--user', help='username for postgres')
	parser.add_argument('--password', help='password for postgres')
	parser.add_argument('--host', help='host for postgres')
	parser.add_argument('--port', help='port for postgres')
	parser.add_argument('--db', help='database name for postgres')
	parser.add_argument('--table', help='name of the table where we will write the results to')
	parser.add_argument('--csv', help='path to the csv file')

	args = parser.parse_args()
	main(args)