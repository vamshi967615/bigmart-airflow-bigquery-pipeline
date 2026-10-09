from airflow.sdk import dag, task
from datetime import datetime
import random


@dag(
    dag_id="branching_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def branching_dag():

    @task.python
    def extract_task(**kwargs):
        print("Extracting Data.....")
        ti = kwargs['ti']
        extracted_data_dict = {"api_extracted_data": [1, 2, 3],
                               "db_extracted_data": [4, 5, 6],
                               "gs_extracted_data": [7, 8, 9]}
        ti.xcom_push(key='return_value', value=extracted_data_dict)

    @task.branch
    def branch_task(**kwargs):
        # Decide which single transform path to run.
        # Returns the task_id of the branch to follow; the others get skipped.
        selected_source = random.choice(["api", "db", "gs"])
        print(f"Branching to: {selected_source}")
        return f"transform_task_{selected_source}"

    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs['ti']
        api_extracted_data = ti.xcom_pull(task_ids='extract_task')['api_extracted_data']
        print("Transforming API Data.....")
        transformed_api_data = [i * 2 for i in api_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_api_data)

    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs['ti']
        db_extracted_data = ti.xcom_pull(task_ids='extract_task')['db_extracted_data']
        print("Transforming DB Data.....")
        transformed_db_data = [i * 2 for i in db_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_db_data)

    @task.python
    def transform_task_gs(**kwargs):
        ti = kwargs['ti']
        gs_extracted_data = ti.xcom_pull(task_ids='extract_task')['gs_extracted_data']
        print("Transforming GS Data.....")
        transformed_gs_data = [i * 2 for i in gs_extracted_data]
        ti.xcom_push(key='return_value', value=transformed_gs_data)

    # trigger_rule is needed because two of the three upstream tasks are skipped
    @task.bash(trigger_rule="none_failed_min_one_success")
    def load_task(**kwargs):
        ti = kwargs['ti']
        api_data = ti.xcom_pull(task_ids='transform_task_api')
        db_data = ti.xcom_pull(task_ids='transform_task_db')
        gs_data = ti.xcom_pull(task_ids='transform_task_gs')
        return f'echo "Loading data -> API: {api_data} | DB: {db_data} | GS: {gs_data}"'

    # Defining task dependencies
    extract = extract_task()
    branch = branch_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_gs = transform_task_gs()
    load = load_task()

    extract >> branch >> [transform_api, transform_db, transform_gs] >> load


branching_dag()