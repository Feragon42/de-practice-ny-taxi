# Learning Excersice using Python, Postgre and Dockers

## Objective: 
Build a dashboard that explore the information of the NY Taxi's trips in the official datasets published by [New York City Taxi and Limousine Commission]('https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page')

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

#### Next Steps (V0.2)
- Improve containerization and dockerize more components of the solution.  
- Update `upload_data.py` logic to use **Parquet** instead of CSV. (This first version was based on a 2021 tutorial that still used CSV, but TLC now provides Parquet files.)  
- Standardize the solution to ingest all available Parquet datasets.  
- Explore the data and start designing a simple hand-made dashboard. 
