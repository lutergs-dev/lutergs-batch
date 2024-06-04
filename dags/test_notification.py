import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.date_time import DateTimeSensorAsync

from lutergs_pwa_alarm_trigger_hook import LuterGSPwaAlarmHook





@dag(
    dag_id="test-alarmer",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    description="test topic alarm trigger",
    start_date=datetime.datetime(2024, 1, 1),
    schedule="0 10,20 * * *",
    catchup=False,
    tags=["lutergs", "test"]
)
def operator():

    @task(task_id="set_wait_time")
    def _set_wait_time(ti=None):
        wait_complete_datetime = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=30)
        wait_complete_minus_15_second = wait_complete_datetime - datetime.timedelta(seconds=15)

        ti.xcom_push(key="wait_complete_datetime", value=wait_complete_datetime)
        ti.xcom_push(key="wait_complete_minus_15_second", value=wait_complete_minus_15_second)

    set_wait_time = _set_wait_time()

    wait_until_time_minus_15_second = DateTimeSensorAsync(
        task_id="wait_until_time_minus_15_second",
        target_time='{{ task_instance.xcom_pull(task_ids="set_wait_time", key="wait_complete_minus_15_second") }}'
    )

    wait_until_time = DateTimeSensorAsync(
        task_id="wait_until_time",
        target_time='{{ task_instance.xcom_pull(task_ids="set_wait_time", key="wait_complete_datetime") }}'
    )

    @task(task_id="set_message")
    def _set_message(ti=None):
        ti.xcom_push(key="message", value="테스트 메시지입니다!")

    set_message = _set_message()

    @task(task_id="trigger_test_alarm")
    def _trigger_test_alarm(ti=None):
        sunrise_alarm_id = Variable.get("LUTERGS_PWA_TEST_TOPIC", deserialize_json=False)

        message = ti.xcom_pull(task_ids="set_message", key="message")

        hook = LuterGSPwaAlarmHook(
            topic_uuid=sunrise_alarm_id,
            alarm_request=LuterGSPwaAlarmHook.LuterGSPwaAlarmRequest(
                title="일출입니다!",
                message=message,
                image_url=None
            )
        )
        hook.get_conn()

    trigger_test_alarm = _trigger_test_alarm()

    set_wait_time >> wait_until_time_minus_15_second
    wait_until_time_minus_15_second >> [wait_until_time, set_message]
    set_message >> trigger_test_alarm


operator()
