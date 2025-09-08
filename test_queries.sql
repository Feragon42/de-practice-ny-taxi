SELECT * FROM ny_taxi_data_legacy;
SELECT MAX(passenger_count) FROM ny_taxi_data_legacy;

SELECT count(*) FROM ny_taxi_green;
SELECT 1 FROM ny_taxi_green LIMIT 1;

drop table ny_taxi_green; 


##Create upload control table
CREATE TABLE IF NOT EXISTS UPLOAD_CONTROLLER (
	table_name VARCHAR,
	period VARCHAR,
	upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	ROW_COUNT INTEGER,
	PRIMARY KEY (table_name, period)
);

select * from upload_controller;


INSERT INTO upload_controller
SELECT 'ny_taxi_yellow' AS table_name, '2025-02' AS period, CURRENT_TIMESTAMP, COUNT(*) AS ROW_COUNT
FROM ny_taxi_yellow where tpep_pickup_datetime >= '2025-02-01';
-- INSERT INTO upload_controller
-- SELECT 'ny_taxi_green' AS table_name, '2025-01' AS period, CURRENT_TIMESTAMP, COUNT(*) AS ROW_COUNT
-- FROM ny_taxi_green
-- union
-- SELECT 'ny_taxi_yellow' AS table_name, '2025-01' AS period, CURRENT_TIMESTAMP, COUNT(*) AS ROW_COUNT
-- FROM ny_taxi_yellow
-- UNION
-- select 'ny_taxi_fhv' AS table_name, '2025-01' AS period, CURRENT_TIMESTAMP, COUNT(*) AS ROW_COUNT
-- FROM ny_taxi_fhv
-- union
-- select 'ny_taxi_fhv_hv' AS table_name, '2025-01' AS period, CURRENT_TIMESTAMP, COUNT(*) AS ROW_COUNT
-- FROM ny_taxi_fhv_hv;

DELETE FROM upload_controller WHERE period!='2025-01';

### Exploring the tables structures
SELECT * FROM NY_TAXI_YELLOW;
WITH duration as (
	SELECT tpep_dropoff_datetime-tpep_pickup_datetime TRIP_DURATION, trip_distance, fare_amount FROM NY_TAXI_YELLOW LIMIT 100
),
 AGG as (
	SELECT SUM(TRIP_DURATION) as TOTAL_TRIP_DURATION, SUM(trip_distance) as TOTAL_TRIP_DISTANCE, SUM(fare_amount) as TOTAL_FARE_AMOUNT 
	FROM duration
 )
SELECT TOTAL_TRIP_DISTANCE/extract(MINUTE FROM TOTAL_TRIP_DURATION) AS MILES_PER_MINUTE,
 	   TOTAL_FARE_AMOUNT/extract(MINUTE FROM TOTAL_TRIP_DURATION) AS FARE_PER_MINUTE,
	   TOTAL_FARE_AMOUNT/TOTAL_TRIP_DISTANCE AS FARE_PER_MILE
  FROM AGG