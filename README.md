# Learning Excersice using Python, Postgre and Dockers

## Objective: 
Build a dashboard to explore information about NYC Taxi trips using the official datasets published by the [New York City Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## Progress:
### V0.1
I set up the working environment using two Docker containers: one for the **PostgreSQL** database and another one for **pgAdmin**.

After that, I worked in a Jupyter Notebook to design the initial logic for uploading NYC Yellow Taxi `.csv` data into the PostgreSQL database.  
For this task, I used **pandas** to read the file and create the DataFrame, and **SQLAlchemy** to manage the database connection and DDL commands.  
When inserting the data, it was important to consider the large file size, so I implemented chunked ingestion using a pandas iterator.  

Once the ingestion logic was ready, I automated the process further by using **requests** and **gzip** to download the `.gz` file and extract the `.csv`.  

Finally, I migrated the Python logic into a functional `.py` script to run inside a Docker image. I also added parameterization with **argparse**, allowing variables to be defined through the `CMD` command. With this setup, I was able to build the image, run the container, and let Python handle both the download and the data insertion.  

<img width="3582" height="1839" alt="Picture1" src="https://github.com/user-attachments/assets/13cd2769-116b-437a-ae89-7c76468f7e51" />
<img width="1868" height="948" alt="image" src="https://github.com/user-attachments/assets/73d6432a-bef0-40ac-8b5f-bee236d34b4d" />
<img width="1253" height="708" alt="image" src="https://github.com/user-attachments/assets/ce69cd37-8d1d-4447-8856-6a982357fb9a" />


#### Next Steps (V0.2)
~~- Improve containerization and dockerize more components of the solution.  ~~
~~- Update `upload_data.py` logic to use **Parquet** instead of CSV. (This first version was based on a 2021 tutorial that still used CSV, but TLC now provides Parquet files.)  ~~
~~- Standardize the solution to ingest all available Parquet datasets.  ~~
- Explore the data and start designing a simple hand-made dashboard.


---

### V0.2
In this version, a scraping ingest flow was implemented.

![flow](https://github.com/user-attachments/assets/a33b69e7-805c-4329-bbe9-6c466c278643)

Using **argparse**, parameters were added to the Python script to let the user specify the connection information, the target webpage URL (in case it changes in the future) and the desire data extraction period.
The code makes a request to the NYC webpage to retrieve its DOM, then parses it with **BeautifulSoup**, extracting the URLs of all parquet files and selecting only those that match the specified period.
Afterwards, the parquet files are processed with **dask** using the **pyarrow** engine, and partitioned into 100mb chunks, ready to be appended to their respective tables in the PostgresSQL database.
Finally, the Python script was packaged into a Docker image for easy and consistent execution.

<img width="1108" height="617" alt="Screenshot 2025-09-08 091116" src="https://github.com/user-attachments/assets/6b179d70-9c56-4980-99ba-f8fc7c4643eb" />
<img width="1107" height="615" alt="Screenshot 2025-09-08 093529" src="https://github.com/user-attachments/assets/214a3c82-0ac4-4dfb-a0b3-039fcf626757" />


#### Next Steps (V0.3)
- Explore the data and start designing a simple dashboard.
