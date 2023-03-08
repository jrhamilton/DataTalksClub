# Anatomy of a Spark Cluster

## Spark Cluster
Local Computer | Cluster

Master -> Executors

Master creates instructions for executors
Master assigns task to executor

## DataFrames
Executor pulls from parquet file (DataFame)

Hadoop and HDFS are popular 

With Hadoop and HDFS data lakes are 

## Summarize
A Driver (CPU, Airflow or Prefect, etc) submits a job to Spark Master

Master communicates everything keeps track of which machines (executors) are healthy and management

Executors are machines that do the computations.

DF's are the parquet files in cloud storage (Data Centers)
