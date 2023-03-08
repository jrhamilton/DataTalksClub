# Data Lake

## Overview
- What is a Data Lake
- Data Lake vs Data warehouse
- Gotcha of Data Lake
- ETL vs ELT
- Cloud provider Data Lake

### Data Lake
A data lake is a big repository that holds big data from many resources.
Can be structured, simi-structured or unstructored.
The idea is to supply to scientists, engineers, etc.
Generally use metadata for faster access.
Should be secure and can scale.
Try to have hardware that is inexpensive.

### Data Lake vs Data Warehouse
#### Data Lake
Generally data is unstructured and usually in the area of terabytes in a data lake.
Target users are scientists and analysts.
Use cases are stream processing, machine learning and real time analytics.
#### Data Warehouse
Data is generally structured for business analysts.
Data size is generally small.
Use cases are batch processing or polling.

### How did Data Lake get started?
- Companies realized value of data.
- We want to store and access data quickly. Don't wait develop structure and relations.
- Idea is to store as quickly as possible.
- Generally it was realized that data is useful when project starts but later in project life cycle so store as quickly as possible.
- There is an increase of data scientists.
- R&D on data products.
- Need cheap storage of Big Data.

### ETL vs ELT
- Defs:
    * ETL: Export, Transform & Load
    * ELT: Export, Load & Transform
- ETL is mainly used for small amounts of data: Data Warehouse.
    * Idea of ETL is _Schema on Write_ where you define the schema and relationships then write the data.
- ELT is usef for large amounts of data: Data Lake
    * Idea of ELT is _Schema on Read_ where you write the data first then define the schema on read.

### Gotchas on Data Lake
- Well known that Data Lakes start on good intentions but turn into Data Swamp
- ^ Due to no versioning.
    * Example is in folder writing data as avlo, then in same folder writing data as parquet making very hard for consumers to consume this data.
- No metadata associated.
- Joins not possible.

### Cloud provie for data lake:
- GCP: Cloud storage
- AWS: S3
- AZURE: AZURE BLOB
- Ide k

