# Partitioning and Clustering
https://www.youtube.com/watch?v=-CqXf7vhhDs&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=26

## BigQuery Partition
Can choose by:
- Time-unit column
- Ingestion time (_ PARTITIONTIME)
- Integer range parittioning
- When using Time unit or ingestion time:
    * Daily (Default)
    * Hourly
    * Monthy or yearly
- Number of partitions limit is 4000

## BigQuery Clustering
- Coluns you specify are used to colocate related data
- Order of the column is important
- The order of the specified columns etermeines the sortorder of the data
- Clustering impores
    * Filter queries
    * Aggregate queries
- Talbe with data size < 1 GB, don't show significant improvement with partitioning and clustering
- You can specify up to four clustering columns

### clustering columns must be top-level non-repeated columns
- DATE
- BOOL
- GEOGRAPHY
- INT64
- NUMERIC
- BIGNUMERIC
- STRING
- TIMESTAMP
- DATETIME

## Partition vs Clustering
Clustering:
    - Cost benefit unknonw
    - You need more granularity than partitioning alone allows
    - Your queries commonly use filters or aggregation against multiple particular columns
    - The cardinality of the number fo values in a column or group of columsn is large
Partitioning:
    - Cost knonw upfront
    - You need parition0level management
    - Filter or aggregate or single column

## Clustering over Partitioning
- Partitioning results in a small amount of data per partition (approximately less than 1 GB)
- Parittioninig results in large number of partitionis beyond the limits on parititoned tables
- Partioining results in your mutation operations modifying the majority of paritioins in the table frequently (for example, every few minutes)

## Automatic reclustering - Time(6:00)
- As Data is added to a clustered table:
    *
    *
- To maintain the performance of characteristics of a clustered table
    *
    *
