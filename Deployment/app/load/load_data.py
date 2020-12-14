# coding=utf-8
import os
import sys
from datetime import datetime, timedelta
from google.cloud import bigquery, storage



def upload_blob(bucket_name, path_local, filename):
    bucket_name = bucket_name
    source_file_name = '{}/{}'.format(path_local,filename)
    destination_blob_name = '{}'.format(filename)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


def load_csv_into_bq(bucket_name, filename, csv_delimiter, dataset_bq, table_name):
    client = bigquery.Client()
 
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        encoding="utf-8",
        field_delimiter=csv_delimiter,
        autodetect=False
    )

    uri = 'gs://{BUCKET_GCP}/{FILENAME}'.format(BUCKET_GCP=bucket_name,FILENAME=filename)
    table_id = '{DATASET_BQ}.{TABLE_NAME}'.format(DATASET_BQ=dataset_bq,TABLE_NAME=table_name)
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    ) 
    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))



def execute_sql_bq(dataset_bq, table_name, query_sql):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
                                          write_disposition='WRITE_TRUNCATE'
                                        )
    # Set the destination table
    table_ref = client.dataset(dataset_bq).table(table_name)
    job_config.destination = table_ref

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        query_sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location="US",
        job_config=job_config,

    )  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print("Query results loaded to table {}".format(table_ref.path))


