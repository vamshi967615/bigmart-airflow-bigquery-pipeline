from airflow.sdk import dag, task
from datetime import datetime

@dag(dag_id="first_orchestrator_dag")
def first_orchestrator_dag():

    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task
    def third_task():
        print("This is the third task")

    # call the tasks and set the order
    first_task() >> second_task() >> third_task()

first_orchestrator_dag()


