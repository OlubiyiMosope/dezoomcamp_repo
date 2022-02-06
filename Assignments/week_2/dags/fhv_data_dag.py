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

# https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2019-01.csv
FHV_URL_PREFIX = 'https://nyc-tlc.s3.amazonaws.com/trip+data'
FHV_FILE_NAME = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
FHV_URL_TEMPLATE = f'{FHV_URL_PREFIX}/{FHV_FILE_NAME}'
FHV_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'

FHV_PARQUET_FILE_NAME = FHV_FILE_NAME.replace('.csv', '.parquet')
FHV_PARQUET_FILE_NAME_TEMPLATE = f'{AIRFLOW_HOME}/{FHV_PARQUET_FILE_NAME}'


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
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    #  end of workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


fhv_dag = DAG(
    dag_id='fhv_data_dag',
    start_date=datetime(2019, 9, 1),
    schedule_interval="0 6 2 * *",
    end_date=datetime(2019, 12, 31),
    max_active_runs=3,
    default_args=default_args,
    tags=['dtc-de-homework'],
    catchup=True,
)

with fhv_dag:
    download_fhv_dataset_task = BashOperator(
        task_id="download_fhv_dataset_task",
        bash_command=f"curl -sSLf {FHV_URL_TEMPLATE} > {FHV_OUTPUT_FILE_TEMPLATE}"
    )

    format_fhv_to_parquet_task = PythonOperator(
        task_id="format_fhv_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": FHV_OUTPUT_FILE_TEMPLATE,
        },
    )

    local_fhv_to_gcs_task = PythonOperator(
        task_id="local_fhv_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{FHV_PARQUET_FILE_NAME}",
            "local_file": FHV_PARQUET_FILE_NAME_TEMPLATE,
        },
    )

    bigquery_fhv_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_fhv_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{FHV_PARQUET_FILE_NAME}"],
            },
        },
    )

    remove_temporary_fhv_files = BashOperator(
        task_id='remove_temporary_fhv_files',
        bash_command=f'rm {FHV_OUTPUT_FILE_TEMPLATE} {FHV_PARQUET_FILE_NAME_TEMPLATE}'
    )

    download_fhv_dataset_task >> format_fhv_to_parquet_task >> local_fhv_to_gcs_task >> \
        bigquery_fhv_external_table_task >> remove_temporary_fhv_files
