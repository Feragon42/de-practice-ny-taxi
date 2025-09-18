import pandas as pd
pd.__version__
from sqlalchemy import create_engine, true, inspect, text
from time import time
import argparse
import os
import requests
import dask.dataframe as dd
from bs4 import BeautifulSoup as bs

def main(params):
	user = params.user
	password = params.password
	host = params.host
	port = params.port
	db = params.db
	source_url = params.source_url

	print("Starting the dimension upload process to Postgres")

	engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}',isolation_level="AUTOCOMMIT")
	print(f"Connection successful to postgresql://{user}:{password}@{host}:{port}/{db}")

	with engine.connect() as connection:
		files_url = obtain_dimension_file_url(source_url)
		if len(files_url) > 0:
			upload_dimension_to_db(files_url[0], connection)


def obtain_dimension_file_url(source_url):
	files_url = []

	response = requests.get(source_url)
	soup = bs(response.content, 'html.parser')
	for link in soup.find_all('a'):
		url = link.get('href')
		if url.endswith('.csv') and 'zone_lookup' in url:
			files_url.append(url)

	print(f"Get {len(files_url)} dimension files")

	return files_url

def upload_dimension_to_db(file_url, connection):
	try:
		request = requests.get(file_url)
		file_name = 'taxi_zones.csv'
		with open(file_name, 'wb') as file:
			file.write(request.content)
		print(f"Dimension file downloaded successfully: {file_name}")
	except Exception as e:
		print(f"Error downloading the dimension file: {file_name}, error: {e}")
		return False

	try:
		print(f"Starting the dimension upload process for file: {file_name}")
		start_time = time()
		df = dd.read_csv(file_name)
		df = df.compute()
		df.columns = [col.lower() for col in df.columns] ##Apply this in the taxi trips tables too
		df.to_sql(name='taxi_zones', con=connection, if_exists='replace', index=False)
		end_time = time()
		print(f"Dimension upload process finished for file: {file_name}, took {end_time - start_time} seconds")

		os.remove(file_name)

		return True
	except Exception as e:
		print(f"Error during the dimension upload process for file: {file_name}, error: {e}")
		return False

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Upload dimension tables to Postgres')

	parser.add_argument('--user', help='username for postgres', default='root')
	parser.add_argument('--password', help='password for postgres', default='root')
	parser.add_argument('--host', help='host for postgres', default='localhost')
	parser.add_argument('--port', help='port for postgres', default='5432')
	parser.add_argument('--db', help='database name for postgres', default='ny_taxi')
	parser.add_argument('--source_url', help='URL to the source page', default='https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page')

	args = parser.parse_args()

	main(args)