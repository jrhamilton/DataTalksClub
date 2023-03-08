# Data Warehouse and Big Quer
https://www.youtube.com/watch?v=jrHljAoD6nM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=25

## Data Warehouse
- OLAP vs OLTP
- What is a data warehouse
- BigQuery
    * Cost
    * Partitions and Clustering
    * Best practices
    * Internals
    * ML in BQ


##
OLAP vs OLTP
OLTP: Online Transaction Processing
OLAP: Online Analytica Processing

Purpose:
    - OLTP: Control and run essential businesses operations in real time. Seen in backend services
    - OLAP: Plan, solve problems, support decisions, discover hidden insights.

Data updates:
    - OLTP: Short, fast upates inflated by user
    - OLAP: data preiodically refreshed with scheduled, long-running batch jobs.

Database design:
    - OLTP: Normalized databases for efficiency.
    - OLAP: Denormalized databases for analyses.

Space requirements:
    - OLTP: Generally small if historical data is archived
    - OLAP: Generally large due to aggregating large datasets

Backup and Recovery:
    - OLTP: Regular backups required to ensure business ciontinuuty and meet legal and governance requirements
    - OLAP: Lost data can be reloaded from OLTP databases as needed in leu of regular backups

Productivity:
    - OLTP: Increses productivity of end users
    - OLAP: Increases of business managers, data analysts, and executives

Data View:
    - OLTP: Lists day-to-day business transactions
    - OLAP: Multi-dimensional view of enterprise data

User examples:
    - OLTP Customer facing personnel clerks, online shoppers
    - OLAP: Knowledge workers such as data analysts, business analysts, and executives


## What is a Data Warehouse
- OLAP solution
- Used for reporting and data analysis


## BigQuery
- Data Warehouse solution
- Serverless data warehouse
    * There are no servers to manage of datase software to install
- Software as well as infrasturcure inclduding:
    * Scalability and High-Availability
- Built-in features like:
    * mahchine learning
    * geospatial analysis
    * business inteligence
- BigQuery maximizes flexibility by separating the compute engine that analyzes your data from your storage.


# BigQuery UI
- BQ has cache

click a table:
run a command from the tab.
Can then save results in CSV or VSCode


## BigQuery
- On demand pricing
    - 1 TB of data process is $5
- Fla rate pricing
    - Based on number of pre requested slots
    - 100 slots - $2,000/month - 400 TB data processed on demand pricing
- Doesn't make since to use flat rate pricing unless going above 200 TB

## BigQuery closer look UI
BQ allows to create external tables from imported sources like our datasets from cloud sources.

BQ already knows types. Very importance because you don't have to define schema

## Partition in BQ - VidTime(~ 13:00)
Generally when we creat dataset. Generally we certain columsn like date, tags, titles and others.

- Data can start out raw then is partitioned 
    * like say by date.

```SQL
-- Create a non paritioned table from external table
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_partitioned
PARTITION ON
    DATE(tpep_pickup_datetime) AS
SELECT * FROM taxi-rides-ny.nytaxi_external_yellow_tripdata;
```
Run this partition queyr and BQ will already know the 'Details'

The query page shows how much data will be processed.

## Clustering -VidTime(19:40)
Cluster by date first like above then by tag (like android, linux, etc)
