from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import storage

from datetime import datetime
import json
import requests
import os

# CONFIG
PROJECT_ID = "serious-music-295616"
RAW_BUCKET = f"ev-raw-{PROJECT_ID}"

# Base path resolves to repo root (one level up from /dags)
BASE_PATH = os.path.dirname(os.path.dirname(__file__))
SQL_PATH = "/home/airflow/gcs/data/sql"

def load_sql(name: str) -> str:
    with open(os.path.join(SQL_PATH, name), "r") as f:
        return f.read()

# STEP 1 — FETCH TfL → GCS
def fetch_tfl_data():
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    urls = {
        "charge_stations": "https://api.tfl.gov.uk/Place/Type/ChargeStation",
        "charge_connectors": "https://api.tfl.gov.uk/Place/Type/ChargeConnector",
    }

    storage_client = storage.Client()
    bucket = storage_client.bucket(RAW_BUCKET)

    for name, url in urls.items():
        r = requests.get(url)
        r.raise_for_status()
        blob = bucket.blob(f"tfl/{name}/{name}_{ts}.json")
        blob.upload_from_string(r.text, content_type="application/json")

# STEP 2 — LOAD JSON → XCOM
def load_json_from_gcs(prefix: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(RAW_BUCKET)
    blobs = sorted(bucket.list_blobs(prefix=prefix), key=lambda b: b.updated, reverse=True)
    content = blobs[0].download_as_string().decode()
    return json.loads(content)

def push_stations_to_xcom(**kwargs):
    kwargs['ti'].xcom_push("stations_json", load_json_from_gcs("tfl/charge_stations/"))

def push_connectors_to_xcom(**kwargs):
    kwargs['ti'].xcom_push("connectors_json", load_json_from_gcs("tfl/charge_connectors/"))

# STEP 3 — BQ LOADS (STAGING)
def bq_load_stations(**kwargs):
    rows = kwargs['ti'].xcom_pull(key='stations_json')
    sql = load_sql("stg_ev_stations.sql")
    job = {
        "query": {
            "query": sql,
            "useLegacySql": False,
            "parameterMode": "NAMED",
            "queryParameters": [
                {
                    "name": "json_data",
                    "parameterType": {"type": "ARRAY", "arrayType": {"type": "JSON"}},
                    "parameterValue": {"arrayValues": [{"value": json.dumps(r)} for r in rows]}
                }
            ]
        }
    }
    BigQueryHook().insert_job(project_id=PROJECT_ID, configuration=job)

def bq_load_connectors(**kwargs):
    rows = kwargs['ti'].xcom_pull(key='connectors_json')
    sql = load_sql("stg_ev_connectors.sql")
    job = {
        "query": {
            "query": sql,
            "useLegacySql": False,
            "parameterMode": "NAMED",
            "queryParameters": [
                {
                    "name": "json_data",
                    "parameterType": {"type": "ARRAY", "arrayType": {"type": "JSON"}},
                    "parameterValue": {"arrayValues": [{"value": json.dumps(r)} for r in rows]}
                }
            ]
        }
    }
    BigQueryHook().insert_job(project_id=PROJECT_ID, configuration=job)

# STEP 4 — BQ LOAD MART
def bq_load_mart(**kwargs):
    sql = load_sql("mart_ev_charger.sql")
    job = {"query": {"query": sql, "useLegacySql": False}}
    BigQueryHook().insert_job(project_id=PROJECT_ID, configuration=job)


# DAG DEFINITON
default_args = {
    "owner": "lakshang",
    "email_on_failure": False,
}

with DAG(
    dag_id="ev_tfl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["ev", "tfl", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")
    fetch = PythonOperator(task_id="fetch_tfl_data", python_callable=fetch_tfl_data)

    push_stations = PythonOperator(task_id="push_stations", python_callable=push_stations_to_xcom)
    push_connectors = PythonOperator(task_id="push_connectors", python_callable=push_connectors_to_xcom)

    load_stations = PythonOperator(task_id="load_stations", python_callable=bq_load_stations)
    load_connectors = PythonOperator(task_id="load_connectors", python_callable=bq_load_connectors)

    load_mart = PythonOperator(task_id="load_mart", python_callable=bq_load_mart)

    dq = EmptyOperator(task_id="run_dq_checks")
    end = EmptyOperator(task_id="end")

    start >> fetch >> [push_stations, push_connectors]
    push_stations >> load_stations
    push_connectors >> load_connectors
    [load_stations, load_connectors] >> load_mart >> dq >> end
