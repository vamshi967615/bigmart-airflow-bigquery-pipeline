from airflow.sdk import dag, task
from datetime import datetime

@dag(dag_id="XCOMS_dag_auto")
def XCOMS_dag_AUTO():

    @task
    def first_task():
        print("Extracting data: This is the first task")

        fetched_data = {"data": [1, 2, 3, 4, 5]}

        return fetched_data

    @task
    def second_task(data: dict):
        print("Transform data: This is the second task")

        fetched_data = data["data"]

        transformed_data = [fetched_data*2]

        transformed_data_dict = {"transf_data": transformed_data}

        return transformed_data_dict

    @task
    def third_task(data: dict):
        print("Load data: This is the third task")

        load_data = data

        print(load_data)

        return load_data

    # XCom flow
    data_from_first = first_task()
    data_from_second = second_task(data_from_first)
    third_task(data_from_second)

XCOMS_dag_AUTO()