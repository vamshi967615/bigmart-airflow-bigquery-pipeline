from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

@dag(dag_id="operators_dag")
def operators_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.bash
    def bash_task_modern():
        return "echo https://airflow.apache.org/"

    bash_task_oldschool = BashOperator(
        task_id="bash_task_oldschool",
        bash_command="echo https://airflow.apache.org/"
    )

    # call the tasks and set the order
    first_task() >> second_task() >> bash_task_modern()

operators_dag()