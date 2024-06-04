import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sensors.date_time import DateTimeSensorAsync

from openweather_hook import OpenWeatherLocationInfoHook
from lutergs_pwa_alarm_trigger_hook import LuterGSPwaAlarmHook


def get_nearest_weather(openweather_response: dict):
    sunrise_epoch_second = openweather_response["current"]["sunrise"]
    hourly = openweather_response["hourly"]
    diff = sunrise_epoch_second
    closest_hour_forecast = hourly[0]
    for hour in hourly:
        temp_diff = abs(hour["dt"] - sunrise_epoch_second)
        if temp_diff < diff:
            diff = temp_diff
            closest_hour_forecast = hour
        else:
            break
    return closest_hour_forecast["weather"]



@dag(
    dag_id="sunrise-alarmer",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    description="sunrise trigger alarm",
    start_date=datetime.datetime(2024, 1, 1),
    schedule="0 4 * * *",
    catchup=False,
    tags=["lutergs"]
)
def operator():

    @task(task_id="set sunrise info")
    def _get_openweather_info(ti=None):
        hook = OpenWeatherLocationInfoHook(latitude=37.56, longitude=127.00)
        weather_response = hook.get_conn()

        sunrise_epoch_second = weather_response["current"]["sunrise"]

        sunrise_datetime = datetime.datetime.fromtimestamp(sunrise_epoch_second, datetime.timezone.utc)  - datetime.timedelta(seconds=2)
        sunrise_datetime_minus_1_minute = sunrise_datetime - datetime.timedelta(minutes=1)

        ti.xcom_push(key="sunrise_datetime", value=sunrise_datetime)
        ti.xcom_push(key="sunrise_datetime_minus_1_minute", value=sunrise_datetime_minus_1_minute)

    get_openweather_info = _get_openweather_info()

    wait_until_sunrise_minus_1_minute = DateTimeSensorAsync(
        task_id="wait until sunrise_minus_1_minute",
        target_time='{{ task_instance.xcom.pull(task_ids="set sunrise info", key="sunrise_datetime_minus_1_minute") }}'
    )

    wait_until_sunrise = DateTimeSensorAsync(
        task_id="wait until sunrise",
        target_time='{{ task_instance.xcom.pull(task_ids="set sunrise info", key="sunrise_datetime") }}'
    )

    @task(task_id="set sunrise forecast")
    def _get_current_forecast(ti=None):
        hook = OpenWeatherLocationInfoHook(latitude=37.56, longitude=127.00)
        weather_response = hook.get_conn()["current"]["weather"]

        ti.xcom_push(key="current_forecast", value=weather_response)

    get_current_forecast = _get_current_forecast()

    @task(task_id="trigger sunrise alarm")
    def _trigger_sunrise_alarm(ti=None):
        sunrise_alarm_id = Variable.get("LUTERGS_PWA_SUNRISE_TOPIC", deserialize_json=False)

        current_weather = ti.xcom_pull(task_ids="set current forecast", key="current_forecast")

        hook = LuterGSPwaAlarmHook(
            topic_uuid=sunrise_alarm_id,
            alarm_request=LuterGSPwaAlarmHook.LuterGSPwaAlarmRequest(
                title="일출입니다!",
                message=f"해가 떠오릅니다. 지금은 {current_weather["description"]} 날씨입니다.",
                image_url=f'https://openweathermap.org/img/wn/{current_weather["icon"]}@2x.png'
            )
        )
        hook.get_conn()

    trigger_sunrise_alarm = _trigger_sunrise_alarm()

    get_openweather_info >> wait_until_sunrise_minus_1_minute
    wait_until_sunrise_minus_1_minute >> [wait_until_sunrise, get_current_forecast]
    get_current_forecast >> trigger_sunrise_alarm


operator()