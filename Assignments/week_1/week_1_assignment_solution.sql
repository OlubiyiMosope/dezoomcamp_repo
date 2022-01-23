-- QUESTION 3: Number of taxi trips on January 15
SELECT COUNT(*) FROM yellow_taxi_trips
WHERE tpep_pickup_datetime::TEXT LIKE '2021-01-15%';

-- QUESTION 4: Day in January with Largest tip.
SELECT tpep_pickup_datetime
FROM yellow_taxi_trips
WHERE tip_amount = (SELECT MAX(tip_amount) FROM yellow_taxi_trips);

-- QUESTION 5: 
-- What was the most popular destination for passengers picked up in central park on Jan. 14? 
-- Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"
SELECT "Zone", COUNT("Zone")
FROM yellow_taxi_trips
JOIN taxi_zones_lookup
ON yellow_taxi_trips."DOLocationID" = taxi_zones_lookup."LocationID"
WHERE tpep_pickup_datetime::TEXT LIKE '2021-01-14%' 
GROUP BY "Zone"
ORDER BY COUNT("Zone") desc
LIMIT 1;


-- QUESTION 6: 
-- What's the pickup-dropoff pair with the largest average price for a ride 
-- (calculated based on total_amount)? Enter two zone names separated by a slash. 
-- For example: "Jamaica Bay / Clinton East" 
-- If any of the zone names are unknown (missing), write "Unknown". 
-- For example, "Unknown / Clinton East".

SELECT 
	CONCAT(zpu."Zone", ' / ', zdo."Zone") AS pu_do,
 	AVG(total_amount)
FROM 
	yellow_taxi_trips t JOIN taxi_zones_lookup zpu
		ON t."PULocationID" = zpu."LocationID"
	JOIN taxi_zones_lookup zdo
		ON t."DOLocationID" = zdo."LocationID"
GROUP BY pu_do
ORDER BY
	AVG(total_amount) DESC
LIMIT 1;
