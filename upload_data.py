import pandas as pd
pd.__version__
from sqlalchemy import create_engine, true
from sqlalchemy import inspect
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
	period = params.period

	print(f"Starting the ingestion process from web to Postgres, for period: {period}")
	print(f"parameters: user={user}, password={password}, host={host}, port={port}, db={db}, source_url={source_url}")

	tables = {"yellow": "ny_taxi_yellow", "green": "ny_taxi_green", "fhv": "ny_taxi_fhv", "fhvhv": "ny_taxi_fhv_hv"}

	engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
	print(f"Connection successful to postgresql://{user}:{password}@{host}:{port}/{db}")
	
	file_url_list = obtain_source_file_url(source_url,period)
	if len(file_url_list) > 0:
		file_name_list, download_status = manage_files(file_url_list, 'download')
		if download_status == True and len(file_name_list) > 0:
			upload_status = False
			with engine.connect() as engine:
				upload_status = upload_files_to_db(file_name_list, engine, tables, period)
			if upload_status == True:
				manage_files(file_name_list, 'delete')
	else:
		print("No files found")
		return

def obtain_source_file_url(source_url, period):
	files_url = []

	response = requests.get(source_url)
	soup = bs(response.content, 'html.parser')
	for link in soup.find_all('a'):
		url = link.get('href')
		if url.endswith('.parquet') and period in url:
			files_url.append(url)

	return files_url

def manage_files(files_url, actions='download'):
	file_names_list = []
	operation_done = False
	if actions == 'download':
		try:
			for url in files_url:
				response = requests.get(url)
				file_name = url.split('/')[-1]
				file_names_list.append(file_name)
				with open(file_name, 'wb') as f:
					f.write(response.content)
				print(f"Downloaded {file_name}")
			operation_done = True
		except Exception as e:
			print(f"Error downloading files: {e}")
			operation_done = False
		return file_names_list, operation_done
	elif actions == 'delete':
		try:
			for file_name in files_url:
				os.remove(file_name)
				print(f"Deleted {file_name}")
			operation_done = True
		except Exception as e:
			print(f"Error deleting files: {e}")
			operation_done = False
		return file_names_list, operation_done

def upload_files_to_db(files_list, connection, tables, period):
	try:
		for file in files_list:
			table_name = tables[file.split('_')[0]]
			# print(f"Processing file {file} into table {table_name}")
			# print(f"Checking if table {table_name} exists")
			# if inspect(connection).has_table(table_name):
			# 	print(f"Table {table_name} exists")
			# else:
			# 	print(f"Table {table_name} does not exist, proceeding to create it.")
			# 	table_created = create_table_in_db(connection, table_name, file)
			# 	if table_created == False:
			# 		print(f"Table {table_name} could not be created. Exiting.")
			# 		return False
			#I don't need to create the table beforehand

			print(f"Uploading data from {file} to table {table_name}")
			df = dd.read_parquet(file, engine="pyarrow")
			df = df.repartition(partition_size="100MB")
			for partition in df.partitions:
				t_start = time()
				chunk = partition.compute()
				print(f"Chunk size: {len(chunk)}")
				chunk.to_sql(name=table_name, con=connection, if_exists='append', index=False)
				#The if_exists='append' does nothing if the table have 0 rows, so I preffer that this create it
				t_end = time()
				print(f"Chunk from {file} uploaded in {t_end - t_start} seconds")
			connection.execute(f"INSERT INTO upload_controller (table_name, period, upload_time, row_count) \
				SELECT '{table_name}' AS table_name, '{period}' AS period, CURRENT_TIMESTAMP, COUNT(*) AS ROW_COUNT \
				FROM {table_name}")
		return True
	except Exception as e:
		print(f"Error uploading files to DB: {e}")
		return False

# def create_table_in_db(connection, table, file):
# 	try:
# 		df = dd.read_parquet(file, engine="pyarrow", chunksize=100_000)
# 		df.head(n=0).to_sql(name=table, con=connection, if_exists='fail', index=False)
# 		return True
# 	except Exception as e:
# 		print(f"Error creating table {table}: {e}")
# 		return False

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Ingest a CSV file to a PostgresDB Table')

	today = pd.Timestamp.now()
	default_period = today.strftime("%Y-%m")
	
	parser.add_argument('--user', help='username for postgres', default='root')
	parser.add_argument('--password', help='password for postgres', default='root')
	parser.add_argument('--host', help='host for postgres', default='localhost')
	parser.add_argument('--port', help='port for postgres', default='5432')
	parser.add_argument('--db', help='database name for postgres', default='ny_taxi')
	parser.add_argument('--source_url', help='URL to the source page', default='https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page')
	parser.add_argument('--period', help='Period to be ingested in format YYYY-MM', default=default_period)

	args = parser.parse_args()
	main(args)