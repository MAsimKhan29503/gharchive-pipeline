from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="hello_dag",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
)
def hello_dag():
    @task
    def say_hello():
        print("Hello from Airflow!")

    say_hello()


hello_dag()