from airflow.sdk import dag, task
import pendulum
from airflow.timetables.trigger import CronTriggerTimetable


@dag(
    dag_id="corn_schedule_dag",

    start_date=pendulum.datetime(
        2026, 9, 30, tz="Asia/Kolkata"
    ),

    end_date=pendulum.datetime(
        2026, 10, 1, tz="Asia/Kolkata"
    ),

    schedule=CronTriggerTimetable(
        "0,15 * * * MON-FRI",
        timezone="Asia/Kolkata"
    ),

    catchup=False,
)
def corn_schedule_dag():

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


corn_schedule_dag()