from airflow.sdk import dag, task
from datetime import datetime

@dag(dag_id="versioned_dag")
def versioned_dag():

    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task
    def third_task():
        print("This is the third task")
    @task
    def version_task():
        print("This is a version task. DAG version 2.0")    

    # call the tasks and set the order
    first_task() >> second_task() >> third_task() >> version_task()

versioned_dag()