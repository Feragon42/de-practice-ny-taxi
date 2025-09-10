SELECT * FROM ny_taxi_data_legacy;
SELECT MAX(passenger_count) FROM ny_taxi_data_legacy;

SELECT count(*) FROM ny_taxi_green;
SELECT distinct period FROM ny_taxi_yellow;
SELECT distinct period FROM ny_taxi_fhv_hv;
SELECT distinct period FROM ny_taxi_fhv;
SELECT distinct period FROM ny_taxi_green;
SELECT 1 FROM ny_taxi_green LIMIT 1;

select * from ny_taxi_green limit 5;
delete from ny_taxi_green where period='2025-02';
SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'ny_taxi_green'


##Create upload control table
CREATE TABLE IF NOT EXISTS UPLOAD_CONTROLLER (
	table_name VARCHAR,
	period VARCHAR,
	upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	ROW_COUNT INTEGER,
	PRIMARY KEY (table_name, period)
);

select * from upload_controller;

--INSERT INTO upload_controller
SELECT 'ny_taxi_yellow' AS table_name, '2025-02' AS period, CURRENT_TIMESTAMP, COUNT(*) AS ROW_COUNT
FROM ny_taxi_yellow 
where DATE_TRUNC('month', tpep_pickup_datetime) = DATE_TRUNC('month', '2025-02-01'::DATE);
delete
FROM ny_taxi_yellow 
where period = '2025-02';

commit;

select * FROM ny_taxi_yellow;
select * FROM ny_taxi_yellow where period = '2025-02';
select * FROM ny_taxi_green;
select * FROM ny_taxi_fhv;
select * FROM ny_taxi_fhv_hv;

alter table ny_taxi_fhv_hv add column period VARCHAR(7);
update ny_taxi_fhv_hv set period='2025-01';


insert into upload_controller values ('ny_taxi_fhv_hv', '2025-02', CURRENT_TIMESTAMP, 0);
insert into upload_controller values ('ny_taxi_fhv', '2025-02', CURRENT_TIMESTAMP, 0);

INSERT INTO upload_controller
SELECT 'ny_taxi_green' AS table_name, period, CURRENT_TIMESTAMP, COUNT(*) AS ROW_COUNT
FROM ny_taxi_green group by period
union
SELECT 'ny_taxi_yellow' AS table_name, period, CURRENT_TIMESTAMP, COUNT(*) AS ROW_COUNT
FROM ny_taxi_yellow group by period
UNION
select 'ny_taxi_fhv' AS table_name, period, CURRENT_TIMESTAMP, COUNT(*) AS ROW_COUNT
FROM ny_taxi_fhv group by period
union
select 'ny_taxi_fhv_hv' AS table_name, period, CURRENT_TIMESTAMP, COUNT(*) AS ROW_COUNT
FROM ny_taxi_fhv_hv group by period;

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