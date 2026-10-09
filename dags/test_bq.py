from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

with DAG("test_bq", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
    BigQueryInsertJobOperator(
        task_id="run_query",
        configuration={"query": {"query": "SELECT 1", "useLegacySql": False}},
        gcp_conn_id="google_cloud_default",
        location="US",
    )