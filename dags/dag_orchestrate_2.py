from airflow.sdk import dag, task
from datetime import datetime

@dag(dag_id="second_orchestrator_dag")
def second_orchestrator_dag():

    @task
    def second_orchestrator_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task
    def third_task():
        print("This is the third task")

    # call the tasks and set the order
    second_orchestrator_task() >> second_task() >> third_task()

second_orchestrator_dag()


