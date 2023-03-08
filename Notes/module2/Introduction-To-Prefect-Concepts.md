# Introduction to Prefect Concepts
### _References_
_Notes_: https://github.com/discdiver/prefect-zoomcamp/tree/main/flows/01_start
_Video_: https://www.youtube.com/watch?v=cdtN6dhp708&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=19

Prefect orchestration is going to allow observablity and orchestration with python code.

## Make sure Conda is installed or another environment product
Create a requirements.txt file with following:
```
andas==1.5.2
prefect==2.7.7
prefect-sqlalchemy==0.2.2
pefect-gcp[cloud_storage]==0.2.3
protobuf==4.21.11
pyarrow==10.0.1
pandas-gbq==0.18.1
psycopg2-binary=2.9.5
sqlalchemy==1.4.46
```
Then create environment and activate environment:
`conda create -n zoom python=3.9`
`conda activate zoom`

Now inside environment, install requirements:
`pip install -r requirements.txt`

## IF USING DOCKER INSTEAD (*BUT DON"T DO THIS*)
create docker-compose file:
```
services:
  pgdatabase:
    image: postgres:13
    hostname: pgdatabase
    environment:
      - POSTGRES_USER=dataclub
      - POSTGRES_PASSWORD=dataclub
      - POSTGRES_DB=ny_taxi
    volumes:
      - "./ny_taxi_postgres_data:/var/lib/postgresql/data:rw"
    ports:
      - 5432:5432
  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=j@localhostmail.com
      - PGADMIN_DEFAULT_PASSWORD=root
    ports:
      - "8080:80"
```
Then run docker compose up to to the background
`docker compose up -d`

Take not of the network name, in this instance it was conda_default

run the following command:
```
docker run -it \
  --network=conda_default \
  taxi_ingest:v001 \
    --user=dataclub \
    --password=dataclub \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-01.csv.gz
```

### BUT LIKE I SAID, DON'T USE DOCKER FOR THIS

Now instead of running all these commands we can start using an orchestration tool and in this case we will use Prefect.
Add `from prefect import flow` near the top of the ingest_data.py file so it looks like this:
```python
#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow

def main(params):
...
```

A Preflect flow is the most basic Prefect object that is a container for workflow logic and allows you to interact and understand the sate of the workflow. Flows are more like functions, they take inputs, perform work, and return an output.

Start by using a @flow decorator to a mian_flow function like `@flow(name=Ingess Flow")`:
```python
...
@flow(name="Ingest Flow")
def main(args):
    user = params.user || "postgres"
    password params.password || "admin"
    host = params.host || "localhost"
    port = params.port || "5432"
    db = params.db || "ny_taxi"
    table_name = params.table_name || "green_taxi_trips"
    url = params.url
    file = params.file

    ingest_data(args)
...
```
The file was redesigned a bit but what was formally main is now ingest_data and now the reference to ingest_data is inside the new flow of main().

Now change the old main to ingest_data and add a task decorator above it:
_Previously_
```python
...
main(args):
...
```
_Now_:
@task
ingest_data(...):
    // _Run the main function_
```
Tasks are not required by flow but the do receive metadata about upstreams ...

Now we can add arguments to the task:
`@task(log_prints=True, retries=3)`

...

## CUT TO THE CHASE.. HERE IS THE WHOLE FILE:
```python
#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
#from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash,
      cache_expiration=timedelta(days=1))
def file_extract(url, filename):
    if url is None:
        csv_name = filename
        if csv_name.endswith('csv.gz'):
            compression = 'gzip'
        else:
            compression = None
    else:
        if url.endswith('csv.gz'):
            cvs_name = 'output.csv.gz'
            compression = 'gzip'
        else:
            csv_name = 'output.csv'
            compression = None

        os.system(f"wget {url} -O {csv_name}")

    return (csv_name, compression)


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash,
      cache_expiration=timedelta(days=1))
def extract_data(csv_name, compression):
    df_iter = pd.read_csv(csv_name, iterator=True, compression=compression)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    return df


@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task(log_prints=True, retries=3)
def load_data(user, password, host, port, db, table_name, df):
    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(postgres_url)

    print(df.head(n=3))

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Ingest Flow")
def main(params):
    user = params.user if params.user != None else "postgres"
    password = params.password if params.password != None else "admin"
    host = params.host if params.host != None else "localhost"
    port = params.port if params.port != None else "5432"
    db = params.db if params.db != None else "ny_taxi"
    table_name = params.table_name if params.table_name != None else "green_taxi_trips"
    url = params.url # if params.url != None else "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-01.csv.gz"
    filename = params.filename

    (csv_name, compression) = file_extract(url, filename)
    raw_data = extract_data(csv_name, compression)
    data = transform_data(raw_data)
    load_data(user, password, host, port, db, table_name, data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest csv data to Postgres')

    # user, password, host, pot, database name, table name, url of csv

    parser.add_argument('--user', help='user name fr postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgre')
    parser.add_argument('--table_name', help='name of the table writing resuls to')
    parser.add_argument('--url', help='url of the csv file if by url')
    parser.add_argument('--filename', help='filename of csv file if by local file')

    args = parser.parse_args()

    main(args)
```

I modified mine a bit to allow for filename _OR_ url so that I wasn't making internet network downloads every 2 minutes while working out bugs.

I started out working with Docker because I didn't want to lose what I learned but working through this I relealized that I was going to need to _fall in line_ and use Conda Environment. Probably could use Linux Containers as well but Conda Env would be much lighter.

However, after going through the the session a bit, I realized that using Docker would have been ok. I just would have had to figure out how to make the ingest_data docker instance to perist then run the `prefect orion start` command.

Run `prefect orion start` to view the prefect orion page at the url emitted by the command.

We can add subflows for example adding a function to log a table name input. In my case I'll log if url or filename is used:
```python
...
@flow(name="Subflow", log_prints=True)
def log_subflow_how_file_obtained(url, filename):
    if url != None:
        f = "URL"
    else:
        f = "Local File"

    print(f"Logging for Subflow capturing file obtained by: {f}")


@flow(name="Ingest Flow")
```

Then add the reference:
```python
...
@flow(name="Ingest Flow")
def main(params):
    ...
    url = params.url
    filename = params.filename

    log_subflow_how_file_obtained(url, filename)

    (csv_name, compression) = file_extract(url, filename)
    raw_data = extract_data(csv_name, compression)
    data = transform_data(raw_data)
    load_data(user, password, host, port, db, table_name, data)
...
```

Check Prefect Orion page to see the Flow and Subflow

## Blocks, Notifications and TaskRun Concurrency

TaskRun Concurrency can be configured on tasks by adding a tag to the task and hten setting a limit through a CLI comman.

Notifications can be set for notification when something goes wrong.

Blocks are a primitive within Prefect that enables the storage of ocnfiguration and provide an interface with interacing with external systems. Blocks can be built by pre-made configurations or created on your own.
Blocks are immutable so they can be reused across multiple flows.
Blocks can als build upon blocks or be installed as part of Integration collection which is prebuilt tasks and blocks that are pip installable.
Great thing about the immutability is that you can update code in one central place and won't need to update code in multiple places.

One example is SqlAlchemy
* in Conda environment run `pip install sqlalchemy`
* Add `from prefect_sqlalchemy import SqlAlchemyConnector` near top of file
* From the Orion page select SqlAlchmy Block:
* Give name
* Select the Postgres option
* psychopg2 option
* Fill in all the sql login details.
* at the top of load_data function add: `connection_block = SqlAlchemyConnector.load("postgres-connector")`
    - "postgres-connector" is the Block name given in the 'Give name' step above.
* load_data function will look like the following with the postgres_url connection lines removed:
```python
@task(log_prints=True, retries=3)
def load_data(user, password, host, port, db, table_name, df):
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin='false') as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')
```

Now all the following parameters are no longer necessary and can be removed throughout the file:
user, password, host, database and port.
For example, the load data function signature now looks like this:
```python
@task(log_prints=True, retries=3)
def load-data(table_name, df):
    ...
```

Now run `python ingest_data.py`
Check Prefect Orion page.
- Make sure there is no space before the bucket name: *Facepalm*
