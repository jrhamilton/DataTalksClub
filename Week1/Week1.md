# HomeWork Week 1

### HW 1 & 2)
Self explanatory
No Code.


### HW 3)
#### How many taxi trips were totally made on January 15?
```SQL
SELECT
    COUNT (*)
FROM
    green_taxi_trips
WHERE
    lpep_dropoff_datetime < '2019-01-16 00:00:00'
    AND lpep_pickup_datetime > '2019-01-14 23:59:59';
```

### HW 4)
#### Which was the day with the largest trip distance?
```SQL
SELECT
    CAST(lpep_pickup_datetime AS DATE) as day,
    MAX(trip_distance) AS max_distance
FROM
    green_taxi_trips
GROUP BY
    day
ORDER BY
    max_distance DESC
LIMIT 1;
```

### HW 5)
#### In 2019-01-01 how many trips had 2 and 3 passengers?
```SQL
SELECT
    passenger_count,
    COUNT(passenger_count)
FROM
    green_taxi_trips
WHERE
    date_trunc('day', lpep_pickup_datetime) = '2019-01-01 00:00:00'
  AND
    passenger_count = 2
  OR
    date_trunc('day', lpep_pickup_datetime) = '2019-01-01 00:00:00'
  AND
    passenger_count = 3
GROUP BY
    passenger_count
```

### HW 6)
#### For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?
```SQL
SELECT
    MAX(t.tip_amount) as max_tip,
    zdo."Zone" as do_zone
FROM
    green_taxi_trips t LEFT JOIN zones zpu
    ON t."PULocationID" = zpu."LocationID"
    JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE
    zpu."Zone" = 'Astoria'
GROUP BY
    zdo."Zone"
ORDER BY max_tip DESC LIMIT 1;
```
