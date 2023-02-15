# WEEK 3

## Q0:
(Ingest script: _https://github.com/jrhamilton/DataTalksClub/Week3/workdir/flows/0/web_to_gcs.py_)[https://github.com/jrhamilton/DataTalksClub/Week3/workdir/flows/0/web_to_gcs.py]

## Q1:
```SQL
CREATE OR REPLACE EXTERNAL TABLE `datatalksclub-375802.dtczoomcamp.fhv_trips`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-datatalks-taxi/data/tag/fhv/fhv_tripdata_2019-*.csv.gz']
);
```

## Q2:
```SQL
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `datatalksclub-375802.dtczoomcamp.fhv_trips`;
```

```SQL
CREATE OR REPLACE TABLE `datatalksclub-375802.dtczoomcamp.fhv_trips_nonpartitioned`
AS SELECT * FROM `datatalksclub-375802.dtczoomcamp.fhv_trips`;
```

```SQL
SELECT COUNT(DISTINCT(affiliated_base_number)) FROM `datatalksclub-375802.dtczoomcamp.fhv_trips_nonpartitioned`;
```

## Q3:
```SQL
SELECT COUNT(*) FROM `datatalksclub-375802.dtczoomcamp.fhv_trips`
WHERE `PUlocationID`IS NULL
AND `DOlocationID` IS NULL;
```

## Q4:
```
CREATE OR REPLACE TABLE `datatalksclub-375802.dtczoomcamp.fhv_trips_partitioned`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY `affiliated_base_number` AS (
  SELECT * FROM `datatalksclub-375802.dtczoomcamp.fhv_trips`
);
```

## Q5
```SQL
SELECT DISTINCT `affiliated_base_number`
FROM  `datatalksclub-375802.dtczoomcamp.fhv_trips_nonpartitioned`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
```

```SQL
SELECT DISTINCT `affiliated_base_number`
FROM  `datatalksclub-375802.dtczoomcamp.fhv_trips_partitioned`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
```
