from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="simple-dag",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="simple DAG of all",
    start_date=datetime(2024, 1, 1),
    schedule="0,10,20,30,40,50 * * * *",
    catchup=False,
    tags=["lutergs"]
)
def operator():
    @task(task_id="simple_print")
    def pass_simple_print_str(ti=None):
        ti.xcom_push(key="print_str", value="hello world!")

    simple_task = pass_simple_print_str()

    bash_print = BashOperator(
        task_id="print_string_task",
        bash_command='echo "received string is {{ task_instance.xcom_pull(task_ids="simple_print", key="print_str") }}"',
        do_xcom_push=False,
    )

    simple_task >> bash_print

operator()
