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

    @task(task_id="set_sunrise_wait_time")
    def _set_sunrise_wait_time(ti=None):
        hook = OpenWeatherLocationInfoHook(latitude=37.56, longitude=127.00)
        weather_response = hook.get_conn().json()

        sunrise_epoch_second = weather_response["current"]["sunrise"]

        sunrise_datetime = datetime.datetime.fromtimestamp(sunrise_epoch_second, datetime.timezone.utc) - datetime.timedelta(seconds=2)
        sunrise_datetime_minus_2_minute = sunrise_datetime - datetime.timedelta(minutes=2)

        ti.xcom_push(key="sunrise_datetime", value=sunrise_datetime)
        ti.xcom_push(key="sunrise_datetime_minus_2_minute", value=sunrise_datetime_minus_2_minute)

    set_sunrise_wait_time = _set_sunrise_wait_time()

    wait_until_sunrise_minus_2_minute = DateTimeSensorAsync(
        task_id="wait_until_sunrise_minus_2_minute",
        target_time='{{ task_instance.xcom_pull(task_ids="set_sunrise_info", key="sunrise_datetime_minus_2_minute") }}'
    )

    wait_until_sunrise = DateTimeSensorAsync(
        task_id="wait_until_sunrise",
        target_time='{{ task_instance.xcom_pull(task_ids="set_sunrise_info", key="sunrise_datetime") }}',
        poke_interval=datetime.timedelta(seconds=30)
    )

    @task(task_id="set_sunrise_forecast")
    def _set_current_forecast(ti=None):
        hook = OpenWeatherLocationInfoHook(latitude=37.56, longitude=127.00)
        weather_response = hook.get_conn().json()
        current_weather_forecast = weather_response["current"]["weather"][0]

        ti.xcom_push(key="current_forecast", value=current_weather_forecast)

    set_current_forecast = _set_current_forecast()

    @task(task_id="trigger_sunrise_alarm")
    def _trigger_sunrise_alarm(ti=None):
        sunrise_alarm_id = Variable.get("LUTERGS_PWA_SUNRISE_TOPIC", deserialize_json=False)

        current_weather = ti.xcom_pull(task_ids="set_current_forecast", key="current_forecast")

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

    set_sunrise_wait_time >> wait_until_sunrise_minus_2_minute
    wait_until_sunrise_minus_2_minute >> [wait_until_sunrise, set_current_forecast]
    wait_until_sunrise >> trigger_sunrise_alarm


operator()
