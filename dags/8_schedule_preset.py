from airflow.sdk import dag, task
import pendulum


@dag(
    dag_id="first_schedule_dag",
    start_date=pendulum.datetime(2026, 9, 30, tz="Asia/Kolkata"),
    schedule="@daily",
    catchup=False,
)
def first_schedule_dag():

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


first_schedule_dag()