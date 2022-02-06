import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq


PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'trips_data_all')
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow')

# https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
FILE_NAME = 'taxi+_zone_lookup.csv'
URL = f'https://s3.amazonaws.com/nyc-tlc/misc/{FILE_NAME}'
OUTPUT_FILE_NAME = AIRFLOW_HOME + f'/{FILE_NAME}'

PARQUET_FILE_NAME = FILE_NAME.replace('.csv', '.parquet')
PARQUET_FILE_NAME_TEMPLATE = f'{AIRFLOW_HOME}/{PARQUET_FILE_NAME}'


def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error('Sorry, can only accept source files in csv format.')
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

zones_data_dag = DAG(
    dag_id='zones_data_dag',
    start_date=datetime.utcnow(),
    schedule_interval=None,
    default_args=default_args,
    max_active_runs=3,
    catchup=False,
    tags=['dtc-de-homework'],
)


with zones_data_dag:

    download_zones_data = BashOperator(
        task_id='download_zones_data',
        bash_command=f'curl -sSLf {URL} > {OUTPUT_FILE_NAME}'
    )

    format_zones_to_parquet = PythonOperator(
        task_id='format_zones_to_parquet',
        python_callable=format_to_parquet,
        op_kwargs={
            'src_file': OUTPUT_FILE_NAME
        }
    )

    upload_zones_to_gcs = PythonOperator(
        task_id='upload_zones_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs={
            'bucket': BUCKET,
            'object_name': f'raw/{PARQUET_FILE_NAME}',
            'local_file': f'{PARQUET_FILE_NAME_TEMPLATE}',
        }
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{PARQUET_FILE_NAME}"],
            },
        },
    )

    download_zones_data >> format_zones_to_parquet >> upload_zones_to_gcs >> bigquery_external_table_task
