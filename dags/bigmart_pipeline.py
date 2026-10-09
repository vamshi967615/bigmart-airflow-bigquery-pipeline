from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import DAG, task

PROJECT = "imposing-bee-510900-u7"
LOCATION = "asia-south1"
RAW_TABLE = f"{PROJECT}.vamshi_123.my_csv_data"

BASE_DIR = Path(__file__).parent
CSV_PATH = BASE_DIR / "data" / "BigMart_Sales.csv"
SQL_DIR = BASE_DIR / "sql"


def run_sql(task_id: str, filename: str) -> BigQueryInsertJobOperator:
    return BigQueryInsertJobOperator(
        task_id=task_id,
        project_id=PROJECT,
        location=LOCATION,
        configuration={"query": {
            "query": (SQL_DIR / filename).read_text(),
            "useLegacySql": False,
        }},
    )


default_args = {
    "owner": "vamshi",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bigmart_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["bigquery", "bigmart"],
) as dag:

    @task
    def load_raw() -> int:
        from google.cloud import bigquery

        if not CSV_PATH.exists():
            raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

        client = BigQueryHook(gcp_conn_id="google_cloud_default",
                              location=LOCATION).get_client(project_id=PROJECT)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("Item_Identifier", "STRING"),
                bigquery.SchemaField("Item_Weight", "FLOAT"),
                bigquery.SchemaField("Item_Fat_Content", "STRING"),
                bigquery.SchemaField("Item_Visibility", "FLOAT"),
                bigquery.SchemaField("Item_Type", "STRING"),
                bigquery.SchemaField("Item_MRP", "FLOAT"),
                bigquery.SchemaField("Outlet_Identifier", "STRING"),
                bigquery.SchemaField("Outlet_Establishment_Year", "INTEGER"),
                bigquery.SchemaField("Outlet_Size", "STRING"),
                bigquery.SchemaField("Outlet_Location_Type", "STRING"),
                bigquery.SchemaField("Outlet_Type", "STRING"),
                bigquery.SchemaField("Item_Outlet_Sales", "FLOAT"),
            ],
        )

        with open(CSV_PATH, "rb") as f:
            job = client.load_table_from_file(
                f, RAW_TABLE, job_config=job_config, location=LOCATION)
        job.result()

        rows = client.get_table(RAW_TABLE).num_rows
        if rows == 0:
            raise ValueError("Load finished but the table has 0 rows")
        print(f"Loaded {rows} rows into {RAW_TABLE}")
        return rows

    build_silver = run_sql("build_silver", "01_silver.sql")
    dq_checks = run_sql("dq_checks", "02_dq_checks.sql")
    build_gold = run_sql("build_gold", "03_gold.sql")

    load_raw() >> build_silver >> dq_checks >> build_gold
