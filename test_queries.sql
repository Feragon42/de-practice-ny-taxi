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

select * from upload_controller ORDER BY table_name, period;
delete from upload_controller where table_name in ('ny_taxi_fhv_hv', 'ny_taxi_fhv') and period = '2025-02';
delete from upload_controller where table_name in ('ny_taxi_yellow') and period = '2025-01';
delete from ny_taxi_yellow where period = '2025-01';

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
	SELECT DATE(tpep_dropoff_datetime) AS dropoff_date, DATE(tpep_pickup_datetime) AS pickup_date, tpep_dropoff_datetime-tpep_pickup_datetime TRIP_DURATION, trip_distance, fare_amount FROM NY_TAXI_YELLOW LIMIT 100
),
 AGG as (
	SELECT SUM(TRIP_DURATION) as TOTAL_TRIP_DURATION, SUM(trip_distance) as TOTAL_TRIP_DISTANCE, SUM(fare_amount) as TOTAL_FARE_AMOUNT 
	FROM duration
 )
SELECT TOTAL_TRIP_DISTANCE/extract(MINUTE FROM TOTAL_TRIP_DURATION) AS MILES_PER_MINUTE,
	   TOTAL_FARE_AMOUNT/extract(MINUTE FROM TOTAL_TRIP_DURATION) AS FARE_PER_MINUTE,
	   TOTAL_FARE_AMOUNT/TOTAL_TRIP_DISTANCE AS FARE_PER_MILE
  FROM AGG



------------Homework

SELECT * FROM NY_TAXI_GREEN;

--During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:
SELECT CASE WHEN TRIP_DISTANCE <= 1 THEN '1- Up to 1 mile'
			WHEN TRIP_DISTANCE > 1 AND TRIP_DISTANCE <= 3 THEN '2- Between 1 and 3 miles'
			WHEN TRIP_DISTANCE > 3 AND TRIP_DISTANCE <= 7 THEN '3- Between 3 and 7 miles'
			WHEN TRIP_DISTANCE > 7 AND TRIP_DISTANCE <= 10 THEN '4- Between 7 and 10 miles'
			ELSE '5- Over 10 miles' END AS DISTANCE_RANGE,
	   COUNT(*) AS NUMBER_OF_TRIPS
  FROM NY_TAXI_GREEN 
 WHERE period = '2019-10'
 GROUP BY DISTANCE_RANGE ORDER BY DISTANCE_RANGE;


--Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.
SELECT LPEP_PICKUP_DATETIME AS PICKUP_DAY, MAX(TRIP_DISTANCE) AS LONGEST_TRIP_DISTANCE
  FROM NY_TAXI_GREEN
 WHERE period = '2019-10'
 GROUP BY PICKUP_DAY
 ORDER BY LONGEST_TRIP_DISTANCE DESC
 LIMIT 1;

--Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?
SELECT zone, sum(total_amount) AS NUMBER_OF_TRIPS
  FROM NY_TAXI_GREEN
  LEFT JOIN taxi_zones
	ON "PULocationID" = locationid
 WHERE lpep_pickup_datetime::date = '2019-10-18'
  AND "PULocationID" IS NOT NULL
 GROUP BY zone
 HAVING sum(total_amount) > 13000
 ORDER BY NUMBER_OF_TRIPS DESC;
--Rename the columns later. And change the upload_data script to reflect the changes.

--For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?
SELECT dozones.zone, max(tip_amount) AS max_tip
  FROM ny_taxi_green
 INNER JOIN taxi_zones puzones
	ON "PULocationID" = puzones.locationid
 INNER JOIN taxi_zones dozones
	ON "DOLocationID" = dozones.locationid
 WHERE puzones.zone = 'East Harlem North'
   AND period = '2019-10'
 GROUP BY dozones.zone
 ORDER BY max_tip desc
 LIMIT 1;
