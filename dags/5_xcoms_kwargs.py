from airflow.sdk import dag, task
from datetime import datetime


@dag(
    dag_id="XCOMS_dag_manual",
    start_date=datetime(2026, 9, 30),
    schedule=None,
    catchup=False
)
def XCOMS_dag_manual():

    @task
    def first_task(**kwargs):

        print("Extracting data: This is the first task")

        fetched_data = {"data": [1, 2, 3, 4, 5]}

        # Push data into XCom
        kwargs["ti"].xcom_push(
            key="fetched_data",
            value=fetched_data
        )

    @task
    def second_task(**kwargs):

        print("Transform data: This is the second task")

        # Pull data from first_task
        ti = kwargs["ti"]

        fetched_data = ti.xcom_pull(
            task_ids="first_task",
            key="fetched_data"
        )

        print("Received data:", fetched_data)

        data = fetched_data["data"]

        transformed_data = [x * 2 for x in data]

        transformed_data_dict = {
            "transf_data": transformed_data
        }

        # Push transformed data
        ti.xcom_push(
            key="transformed_data",
            value=transformed_data_dict
        )

    @task
    def third_task(**kwargs):

        print("Load data: This is the third task")

        ti = kwargs["ti"]

        # Pull data from second_task
        load_data = ti.xcom_pull(
            task_ids="second_task",
            key="transformed_data"
        )

        print("Load data:", load_data)

    # XCom flow
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


XCOMS_dag_manual()