from datetime import datetime

import pendulum
from airflow.sdk import dag, task
from airflow.timetables.events import EventsTimetable

# Run the DAG only on these specific dates (timezone-aware)
special_dates = EventsTimetable(
    event_dates=[
        pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
        pendulum.datetime(2026, 10, 2, tz="Asia/Kolkata"),
        pendulum.datetime(2026, 10, 6, tz="Asia/Kolkata"),
        pendulum.datetime(2026, 1, 30, tz="Asia/Kolkata"),
    ]
)


@dag(
    schedule=special_dates,
    start_date=datetime(2026, 1, 1, tzinfo=pendulum.timezone("Asia/Kolkata")),
    end_date=datetime(2026, 1, 31, tzinfo=pendulum.timezone("Asia/Kolkata")),
    catchup=True,
)
def special_dates_dag():

    @task.python
    def special_event_task(**kwargs):
        execution_date = kwargs["logical_date"]
        print(f"Running task for special event on {execution_date}")

    special_event = special_event_task()


# Instantiating the DAG
special_dates_dag()