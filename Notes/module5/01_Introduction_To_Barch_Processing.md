# Batch Processing

## Processing Data
    1. Barch
    2. Streaming

## Batch
Database and in database is a job for a day then takes all the data and does the processing.

## Stream
Data processed in real time

## This module is Batch
Batch Jobs:
    1. Weekly
    2. Daily
    3. Hourly
    4. Smaller granularity

## Technologies
    * Python Scripts
        - Kubernetes
        - AWS Batch jobs
    * SQL to transform
    * SPARK
    * Flink

## Workflow
    Lake CSV -> Python -> SQL (DBT) -> Spak -> Python

## Advantages of Batch
    - Easy to manage
    - Retry
    - Scale

## Disadvantage
    - Delay
