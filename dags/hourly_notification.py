import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

from lutergs_pwa_alarm_trigger_hook import LuterGSPwaAlarmHook


@dag(
    dag_id="hourly-alarmer",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=1),
    },
    description="sunrise trigger alarm",
    start_date=datetime.datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["lutergs"]
)
def operator():

    @task(task_id="trigger_hourly_alarm")
    def _trigger_hourly_alarm():
        hourly_alarm_id = Variable.get("LUTERGS_PWA_HOURLY_TOPIC", deserialize_json=False)

        hook = LuterGSPwaAlarmHook(
            topic_uuid=hourly_alarm_id,
            alarm_request=LuterGSPwaAlarmHook.LuterGSPwaAlarmRequest(
                title="정각 알림",
                message=f"{datetime.datetime.now().hour}시입니다"
            )
        )
        hook.get_conn()

    trigger_hourly_alarm = _trigger_hourly_alarm()

    trigger_hourly_alarm


operator()
