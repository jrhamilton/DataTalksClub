# ETL with GCP and Prefect

Create file: etl_web_to_gcs.py

Will have one main flow that will call other functions


### GCS
* Go to your console.
* Go to Project and make a bucket.
* Click 'Create'
* Give a globally unique name and keep whatever defaults.

Register Prefect GCP blocks from GCS Bucket
Run:
`prefect block register -a prefect_gcp`
We will be using the:
* GCP Credentials
* GCP Bucket

- Now in the Prefect Orion page, go to Blocks and search for GCS Bucket and add
it.
- Give a unique name to Block.
- Add the name of bucket created in GCS Console.
- Add credentials click add.
    * name: datatalks-gcp-creds
    * Go to GCP and get Service Account by going to:
        - Hamburger meun -> IAM & Admin -> Service Accounts
        - Click + button to Create Service Account
        - Give a name
        - Give it a role _if you can_:
            1. BigQuery Admin
            2. Storage Admin
        - Next click on the hamburger menu next to the new credential to give
          it keys.
            * JSON key type.
        - Download key automatically. DO NOT PUT IN ANY PUBLIC REPOSITORY!
        - click CREATE
- Click CREATE for main Block.

