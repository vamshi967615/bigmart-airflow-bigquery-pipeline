from airflow.sdk import dag, task
from datetime import timedelta
import pendulum
from airflow.timetables.trigger import DeltaTriggerTimetable


@dag(
    dag_id="delta_schedule_dag",
    start_date=pendulum.datetime(2026, 9, 30, tz="Asia/Kolkata"),
    end_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    schedule=DeltaTriggerTimetable(timedelta(hours=1)),  # run every 1 hour
    catchup=False,
)
def delta_schedule_dag():

    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")

    @task
    def third_task():
        print("This is the third task")

    first_task() >> second_task() >> third_task()


delta_schedule_dag()